//! `marketrec import tardis`: a day of Tardis.dev's raw Binance Spot recording, turned into
//! frames, so that the same code derives the same tables from it.
//!
//! Tardis.dev records the streams marketrec reads (`<symbol>@depth@100ms`, `@bookTicker`,
//! `@trade`, in microseconds since 2025-02-28) and the REST depth snapshots it fetches, which
//! it marks `"generated":true`. Its raw API serves them by the minute: each line is the
//! local receive time in ISO 8601, a space, and the message as received. An empty line marks
//! a lost connection. The first day of each month is served without a key; for other days
//! the key is read from `TARDIS_API_KEY`.
//!
//! - Each message becomes a text frame byte for byte, with Tardis's receive time. Times are
//!   kept in order: a line older than the one before it gets the time of the one before.
//! - The start, and each empty line, open a connection: `connected` (and `disconnected` for
//!   the one before). The end of the day closes the last one with reason `stopped`.
//! - A snapshot becomes the answer to the request the pipeline makes for that connection,
//!   which the first depth update after the connection causes (its request ID is that
//!   update's receive time, `docs/FRAMES.md`). A snapshot received before any update waits
//!   for it and follows it one nanosecond later.
//!
//! The receive times are Tardis's, in Tokyo: the lag in the manifest is theirs, and the data
//! has no round trips of a link, so a latency for the fill simulator must be given.

use std::fmt;
use std::fs;
use std::io::{self, BufRead, Read};
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;
use serde_json::value::RawValue;

use crate::frames::zst::{LEVEL, compress_file};
use crate::frames::{FrameKind, Header, OwnedFrame};
use crate::json::TimeUnit;
use crate::rebuild::{RebuildReport, rebuild_day};
use crate::record::recorder_id;
use crate::record::writer::Rotator;
use crate::time::day_start;

pub const DEFAULT_BASE: &str = "https://api.tardis.dev/v1";
/// Minutes per request; the API serves up to 10.
pub const SLICE_MINUTES: u32 = 10;
const CHANNELS: [&str; 4] = ["depth", "depthSnapshot", "bookTicker", "trade"];

#[derive(Debug)]
pub enum ImportError {
    BadDate(String),
    /// The day directory already holds frames: an import never mixes with a recording.
    Exists(std::path::PathBuf),
    /// Tardis.dev returned no messages of the symbol for that day.
    NoData,
    BadLine {
        line: u64,
        reason: String,
    },
    Http(reqwest::Error),
    Status {
        code: u16,
        body: String,
    },
    Encoding(String),
    Io(io::Error),
    Rebuild(crate::rebuild::RebuildError),
}

impl fmt::Display for ImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ImportError::BadDate(d) => write!(f, "{d} is not a date (YYYY-MM-DD)"),
            ImportError::Exists(p) => write!(
                f,
                "{} already holds frames; an import goes into a day without any",
                p.display()
            ),
            ImportError::NoData => write!(f, "Tardis.dev returned no messages of the symbol"),
            ImportError::Rebuild(e) => write!(f, "{e}"),
            ImportError::BadLine { line, reason } => write!(f, "line {line}: {reason}"),
            ImportError::Http(e) => write!(f, "{e}"),
            ImportError::Status {
                code: 401 | 403,
                body,
            } => write!(
                f,
                "Tardis.dev refused the request ({body}); without a key it serves only the \
                 first day of each month: set TARDIS_API_KEY for other days"
            ),
            ImportError::Status { code, body } => write!(f, "Tardis.dev answered {code}: {body}"),
            ImportError::Encoding(e) => write!(f, "{e}"),
            ImportError::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ImportError {}

impl From<io::Error> for ImportError {
    fn from(e: io::Error) -> Self {
        ImportError::Io(e)
    }
}

impl From<reqwest::Error> for ImportError {
    fn from(e: reqwest::Error) -> Self {
        ImportError::Http(e)
    }
}

/// `2026-11-01T00:00:00.1422608Z` in ns since the epoch.
pub fn parse_time(s: &str) -> Option<i64> {
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    let day = day_start(date)?;
    let (hms, frac) = time.split_once('.').unwrap_or((time, ""));
    let mut parts = hms.splitn(3, ':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let sec: i64 = parts.next()?.parse().ok()?;
    if h > 23 || m > 59 || sec > 60 || frac.len() > 9 || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let ns: i64 = if frac.is_empty() {
        0
    } else {
        frac.parse::<i64>().ok()? * 10_i64.pow(9 - frac.len() as u32)
    };
    Some(day + ((h * 60 + m) * 60 + sec) * 1_000_000_000 + ns)
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    stream: &'a str,
    #[serde(borrow)]
    data: &'a RawValue,
}

#[derive(Deserialize)]
struct EventTime {
    #[serde(rename = "E")]
    e: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImportStats {
    pub lines: u64,
    pub messages: u64,
    pub snapshots: u64,
    pub connections: u64,
    /// Messages of other symbols or streams, left out.
    pub skipped: u64,
}

impl fmt::Display for ImportStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} lines: {} messages, {} snapshots, {} connections, {} left out",
            self.lines, self.messages, self.snapshots, self.connections, self.skipped
        )
    }
}

/// Lines of the raw API in, frames out.
pub struct Converter {
    stream_prefix: String,
    conn: u64,
    connected: bool,
    first_depth: Option<i64>,
    held: Option<Vec<u8>>,
    last_ts: i64,
    time_unit: Option<TimeUnit>,
    pending: Vec<OwnedFrame>,
    pub stats: ImportStats,
}

impl Converter {
    pub fn new(symbol: &str) -> Self {
        Self {
            stream_prefix: format!("{}@", symbol.to_lowercase()),
            conn: 0,
            connected: false,
            first_depth: None,
            held: None,
            last_ts: i64::MIN,
            time_unit: None,
            pending: Vec::new(),
            stats: ImportStats::default(),
        }
    }

    /// The feed's time unit, known from the first message with an event time. Frames are
    /// held back until then, because the frames file header names it.
    pub fn time_unit(&self) -> Option<TimeUnit> {
        self.time_unit
    }

    /// Feed one line; frames go to `out`, with the feed's time unit, once that is known.
    pub fn line(
        &mut self,
        line: &str,
        out: &mut impl FnMut(OwnedFrame, TimeUnit) -> io::Result<()>,
    ) -> Result<(), ImportError> {
        self.stats.lines += 1;
        let line = line.trim_end_matches(['\r', '\n']);
        if line.trim().is_empty() {
            if self.connected {
                let ts = self.last_ts;
                self.close(ts, "tardis.dev: connection lost");
            }
            return self.flush(out);
        }
        let bad = |reason: &str| ImportError::BadLine {
            line: self.stats.lines,
            reason: reason.to_owned(),
        };
        let (time, message) = line.split_once(' ').ok_or_else(|| bad("no time"))?;
        let ts = parse_time(time).ok_or_else(|| bad("time is not ISO 8601 UTC"))?;
        let ts = ts.max(self.last_ts);
        let env: Envelope = serde_json::from_str(message)
            .map_err(|e| bad(&format!("not a combined-stream message: {e}")))?;
        let Some(name) = env.stream.strip_prefix(&self.stream_prefix) else {
            self.stats.skipped += 1;
            return Ok(());
        };
        if !self.connected {
            self.conn += 1;
            self.stats.connections += 1;
            self.connected = true;
            self.first_depth = None;
            self.held = None;
            self.emit(
                FrameKind::Connected,
                ts,
                self.conn,
                b"tardis.dev raw replay".to_vec(),
            );
        }
        if self.time_unit.is_none()
            && let Ok(EventTime { e: Some(e) }) = serde_json::from_str(env.data.get())
        {
            self.time_unit = Some(if e >= 100_000_000_000_000 {
                TimeUnit::Micro
            } else {
                TimeUnit::Milli
            });
        }
        if name == "depthSnapshot" {
            self.stats.snapshots += 1;
            let body = env.data.get().as_bytes().to_vec();
            match self.first_depth {
                Some(id) => self.emit(FrameKind::Snapshot, ts, id as u64, body),
                None => self.held = Some(body),
            }
        } else if name.starts_with("depth") || name == "bookTicker" || name == "trade" {
            self.stats.messages += 1;
            self.emit(FrameKind::Text, ts, self.conn, message.as_bytes().to_vec());
            if name.starts_with("depth") && self.first_depth.is_none() {
                self.first_depth = Some(ts);
                if let Some(body) = self.held.take() {
                    self.emit(FrameKind::Snapshot, ts + 1, ts as u64, body);
                }
            }
        } else {
            self.stats.skipped += 1;
        }
        self.flush(out)
    }

    /// End of the data: the last connection stops.
    pub fn finish(
        &mut self,
        out: &mut impl FnMut(OwnedFrame, TimeUnit) -> io::Result<()>,
    ) -> Result<(), ImportError> {
        if self.connected {
            let ts = self.last_ts;
            self.close(ts, crate::net::ws::STOPPED);
        }
        if self.time_unit.is_none() && !self.pending.is_empty() {
            self.time_unit = Some(TimeUnit::Micro); // no event time seen: nothing depends on it
        }
        self.flush(out)
    }

    fn close(&mut self, ts: i64, reason: &str) {
        self.emit(
            FrameKind::Disconnected,
            ts,
            self.conn,
            reason.as_bytes().to_vec(),
        );
        self.connected = false;
    }

    fn emit(&mut self, kind: FrameKind, ts: i64, aux: u64, payload: Vec<u8>) {
        self.last_ts = self.last_ts.max(ts);
        self.pending.push(OwnedFrame {
            kind,
            recv_ts: self.last_ts,
            aux,
            payload,
        });
    }

    fn flush(
        &mut self,
        out: &mut impl FnMut(OwnedFrame, TimeUnit) -> io::Result<()>,
    ) -> Result<(), ImportError> {
        if let Some(unit) = self.time_unit {
            for f in self.pending.drain(..) {
                out(f, unit)?;
            }
        }
        Ok(())
    }
}

/// Feed every line of a reader.
pub fn convert_lines(
    conv: &mut Converter,
    input: impl BufRead,
    out: &mut impl FnMut(OwnedFrame, TimeUnit) -> io::Result<()>,
) -> Result<(), ImportError> {
    for line in input.lines() {
        conv.line(&line?, out)?;
    }
    Ok(())
}

/// The raw API of Tardis.dev for one exchange.
pub struct Source {
    http: reqwest::Client,
    base: String,
    key: Option<String>,
}

impl Source {
    pub fn new(base: &str, key: Option<String>) -> Result<Self, ImportError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .user_agent(concat!("marketrec/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            http,
            base: base.trim_end_matches('/').to_owned(),
            key: key.filter(|k| !k.is_empty()),
        })
    }

    /// `minutes` minutes of `symbol` from minute `offset` of `date`, decompressed. Retried a
    /// few times on errors that may pass: lost connections, 429 and 5xx.
    pub async fn slice(
        &self,
        date: &str,
        offset: u32,
        minutes: u32,
        symbol: &str,
    ) -> Result<Vec<u8>, ImportError> {
        let lower = symbol.to_lowercase();
        let filters: Vec<String> = CHANNELS
            .iter()
            .map(|c| format!(r#"{{"channel":"{c}","symbols":["{lower}","{symbol}"]}}"#))
            .collect();
        let filters = format!("[{}]", filters.join(","));
        let url = format!(
            "{}/data-feeds/binance?from={date}&offset={offset}&sliceSize={minutes}&filters={}",
            self.base,
            percent_encode(&filters)
        );
        let mut wait = Duration::from_secs(1);
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.get(&url).await {
                Ok(body) => return Ok(body),
                Err(e) if attempt < 5 && retryable(&e) => {
                    tokio::time::sleep(wait).await;
                    wait *= 2;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn get(&self, url: &str) -> Result<Vec<u8>, ImportError> {
        let mut req = self.http.get(url).header("Accept-Encoding", "zstd");
        if let Some(k) = &self.key {
            req = req.bearer_auth(k);
        }
        let resp = req.send().await?;
        let status = resp.status();
        let encoding = resp
            .headers()
            .get("content-encoding")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();
        let body = resp.bytes().await?;
        if !status.is_success() {
            let text = String::from_utf8_lossy(&body);
            return Err(ImportError::Status {
                code: status.as_u16(),
                body: text.chars().take(300).collect(),
            });
        }
        match encoding.as_str() {
            "" | "identity" => Ok(body.to_vec()),
            "zstd" => {
                // one zstd frame per minute: the decoder reads them one after the other
                let mut out = Vec::new();
                zstd::stream::read::Decoder::new(&body[..])?.read_to_end(&mut out)?;
                Ok(out)
            }
            other => Err(ImportError::Encoding(format!(
                "Tardis.dev answered in {other}, asked for zstd"
            ))),
        }
    }
}

pub struct ImportReport {
    pub stats: ImportStats,
    pub files: usize,
    pub raw_bytes: u64,
    pub compressed_bytes: u64,
    pub rebuilt: RebuildReport,
}

impl fmt::Display for ImportReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "imported      {}", self.stats)?;
        writeln!(
            f,
            "frames        {} hourly files, {:.1} MB compressed to {:.1} MB",
            self.files,
            self.raw_bytes as f64 / 1e6,
            self.compressed_bytes as f64 / 1e6
        )?;
        write!(f, "{}", self.rebuilt)
    }
}

/// Import one day: download it slice by slice, write the frames as the recorder would
/// (hourly files, compressed when done), then derive and verify the day's tables.
pub fn tardis_day(
    base: &str,
    key: Option<String>,
    date: &str,
    symbol: &str,
    out: &Path,
    steps: (String, String),
    allow_today: bool,
) -> Result<ImportReport, ImportError> {
    if day_start(date).is_none() {
        return Err(ImportError::BadDate(date.to_owned()));
    }
    let root = out.join("binance").join(symbol);
    let day = root.join(date);
    let frames_dir = day.join("frames");
    if frames_dir.is_dir() && fs::read_dir(&frames_dir)?.next().is_some() {
        return Err(ImportError::Exists(day));
    }
    let src = Source::new(base, key)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut conv = Converter::new(symbol);
    let recorder = format!("tardis.dev raw replay, imported by {}", recorder_id());
    let mut rot: Option<Rotator> = None;
    let mut write = |f: OwnedFrame, unit: TimeUnit| -> io::Result<()> {
        let r = rot.get_or_insert_with(|| {
            Rotator::new(
                &root,
                Header {
                    symbol: symbol.to_owned(),
                    price_step: steps.0.clone(),
                    qty_step: steps.1.clone(),
                    time_unit: unit,
                    recorder: recorder.clone(),
                },
            )
        });
        r.write(&f).map(|_| ())
    };
    for offset in (0..24 * 60).step_by(SLICE_MINUTES as usize) {
        let body = rt.block_on(src.slice(date, offset, SLICE_MINUTES, symbol))?;
        convert_lines(&mut conv, &body[..], &mut write)?;
        let done = offset + SLICE_MINUTES;
        if done.is_multiple_of(60) {
            eprintln!("imported      {:02}:00 UTC, {}", done / 60, conv.stats);
        }
    }
    conv.finish(&mut write)?;
    let Some(rot) = rot else {
        return Err(ImportError::NoData);
    };
    let files = rot.finish()?;
    let (mut raw, mut packed) = (0, 0);
    for f in &files {
        let c = compress_file(f, LEVEL)?;
        raw += c.raw_bytes;
        packed += c.compressed_bytes;
    }
    let rebuilt = rebuild_day(&day, allow_today).map_err(ImportError::Rebuild)?;
    Ok(ImportReport {
        stats: conv.stats,
        files: files.len(),
        raw_bytes: raw,
        compressed_bytes: packed,
        rebuilt,
    })
}

fn retryable(e: &ImportError) -> bool {
    match e {
        ImportError::Http(_) => true,
        ImportError::Status { code, .. } => *code == 429 || *code >= 500,
        _ => false,
    }
}

fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::net::ws::STOPPED;
    use crate::testkit::{DAY0, MS, SEC, Session};
    use crate::time::Utc;
    use crate::verify::Outcome;

    fn iso(ns: i64) -> String {
        let t = Utc::from_ns(ns);
        format!(
            "{}T{:02}:{:02}:{:02}.{:07}Z",
            t.date(),
            t.hour,
            t.minute,
            t.second,
            ns.rem_euclid(SEC) / 100
        )
    }

    /// A session as Tardis.dev serves it: messages with their times, an empty line for each
    /// lost connection, no connection events, and each snapshot as a generated message,
    /// either when it came or (`early`) just before the connection's first depth update.
    fn tardis_lines(s: &Session, early: bool) -> String {
        let mut out = String::new();
        let mut snapshots: Vec<(i64, String)> = Vec::new();
        let snap = |body: &[u8]| {
            format!(
                r#"{{"stream":"btcusdt@depthSnapshot","generated":true,"data":{}}}"#,
                String::from_utf8_lossy(body)
            )
        };
        let mut first_depth_seen = false;
        let frames = &s.frames;
        for (i, f) in frames.iter().enumerate() {
            match f.kind {
                FrameKind::Connected => first_depth_seen = false,
                FrameKind::Disconnected if f.payload != STOPPED.as_bytes() => out.push('\n'),
                FrameKind::Snapshot if !early => {
                    out += &format!("{} {}\n", iso(f.recv_ts), snap(&f.payload));
                }
                FrameKind::Text => {
                    let text = String::from_utf8_lossy(&f.payload);
                    if early && !first_depth_seen && text.contains("@depth@") {
                        first_depth_seen = true;
                        // the snapshot this connection got, sent ahead of the first update
                        if let Some(sn) = frames[i..].iter().find(|g| g.kind == FrameKind::Snapshot)
                        {
                            snapshots.push((f.recv_ts, snap(&sn.payload)));
                            out += &format!("{} {}\n", iso(f.recv_ts - MS), snap(&sn.payload));
                        }
                    }
                    out += &format!("{} {}\n", iso(f.recv_ts), text);
                }
                _ => {}
            }
        }
        out
    }

    fn convert(text: &str) -> (Vec<OwnedFrame>, ImportStats) {
        let mut conv = Converter::new("BTCUSDT");
        let mut frames = Vec::new();
        let mut push = |f: OwnedFrame, u: TimeUnit| {
            assert_eq!(u, TimeUnit::Micro);
            frames.push(f);
            Ok(())
        };
        convert_lines(&mut conv, text.as_bytes(), &mut push).unwrap();
        conv.finish(&mut push).unwrap();
        (frames, conv.stats)
    }

    fn session() -> Session {
        let t0 = DAY0 + 3 * crate::time::NS_PER_HOUR;
        let mut s = Session::new(61);
        s.connect(t0);
        s.run(t0 + MS, t0 + 20 * SEC, 40 * MS);
        s.disconnect(t0 + 20 * SEC, "connection reset");
        s.connect(t0 + 21 * SEC);
        s.run(t0 + 21 * SEC + MS, t0 + 40 * SEC, 40 * MS);
        s
    }

    #[test]
    fn messages_become_frames_and_snapshots_answer_the_first_update() {
        let s = session();
        for early in [false, true] {
            let (frames, stats) = convert(&tardis_lines(&s, early));
            assert_eq!(stats.connections, 2);
            assert_eq!(stats.snapshots, 2, "early: {early}");
            let kinds: Vec<FrameKind> = frames.iter().map(|f| f.kind).collect();
            assert_eq!(kinds[0], FrameKind::Connected);
            assert_eq!(
                kinds
                    .iter()
                    .filter(|k| **k == FrameKind::Disconnected)
                    .count(),
                2,
                "a lost connection and the stop"
            );
            let last = frames.last().unwrap();
            assert_eq!(
                (last.kind, last.payload.as_slice()),
                (FrameKind::Disconnected, STOPPED.as_bytes())
            );
            // each snapshot answers the request of its connection's first depth update
            for (i, f) in frames
                .iter()
                .enumerate()
                .filter(|(_, f)| f.kind == FrameKind::Snapshot)
            {
                let conn_start = frames[..i]
                    .iter()
                    .rposition(|g| g.kind == FrameKind::Connected)
                    .unwrap();
                let first = frames[conn_start..]
                    .iter()
                    .find(|g| {
                        g.kind == FrameKind::Text && g.payload.windows(7).any(|w| w == b"@depth@")
                    })
                    .unwrap();
                assert_eq!(f.aux, first.recv_ts as u64);
                assert!(f.recv_ts > first.recv_ts);
            }
            assert!(
                frames.windows(2).all(|w| w[0].recv_ts <= w[1].recv_ts),
                "in time order"
            );
            // the text frames are the messages, byte for byte
            let texts: Vec<&[u8]> = frames
                .iter()
                .filter(|f| f.kind == FrameKind::Text)
                .map(|f| f.payload.as_slice())
                .collect();
            let sent: Vec<&[u8]> = s
                .frames
                .iter()
                .filter(|f| f.kind == FrameKind::Text)
                .map(|f| f.payload.as_slice())
                .collect();
            assert_eq!(texts, sent);
        }
    }

    #[test]
    fn other_symbols_and_streams_are_left_out_and_bad_lines_named() {
        let line = format!(
            "{} {}",
            iso(DAY0),
            r#"{"stream":"ethusdt@trade","data":{"E":1795219200000000}}"#
        );
        let (frames, stats) = convert(&line);
        assert!(frames.is_empty());
        assert_eq!(stats.skipped, 1);
        let mut conv = Converter::new("BTCUSDT");
        let err = conv.line("garbage", &mut |_, _| Ok(())).unwrap_err();
        assert!(err.to_string().starts_with("line 1:"), "{err}");
    }

    /// A stand-in for the raw API: the body for minute offset 0 (zstd, two frames, as the
    /// API sends one per minute), an empty body for the others; or one status for all.
    fn serve(body: Vec<u8>, status: u16) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        let half = body.len() / 2;
        let cut = body[half..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(body.len(), |p| half + p + 1);
        let mut packed = zstd::encode_all(&body[..cut], 3).unwrap();
        packed.extend(zstd::encode_all(&body[cut..], 3).unwrap());
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let mut req = Vec::new();
                let mut buf = [0u8; 4096];
                while !req.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = std::io::Read::read(&mut stream, &mut buf).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    req.extend_from_slice(&buf[..n]);
                }
                let req = String::from_utf8_lossy(&req).into_owned();
                log.lock().unwrap().push(req.clone());
                let first = req.contains("offset=0&");
                let payload: &[u8] = if status == 200 && first { &packed } else { b"" };
                let enc = if status == 200 && first {
                    "Content-Encoding: zstd\r\n"
                } else {
                    ""
                };
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status} X\r\n{enc}Content-Length: {}\r\nConnection: close\r\n\r\n",
                    payload.len()
                );
                let _ = stream.write_all(payload);
            }
        });
        (format!("http://{addr}/v1"), seen)
    }

    #[test]
    fn a_day_is_downloaded_written_as_frames_and_derived() {
        let s = session();
        let (base, seen) = serve(tardis_lines(&s, true).into_bytes(), 200);
        let dir = tempfile::tempdir().unwrap();
        let steps = ("0.01000000".to_owned(), "0.00001000".to_owned());
        let r = tardis_day(
            &base,
            None,
            "2026-11-21",
            "BTCUSDT",
            dir.path(),
            steps.clone(),
            true,
        )
        .unwrap();
        assert_eq!(r.stats.connections, 2);
        assert_eq!(r.files, 1, "one hour of data");
        assert_eq!(
            r.rebuilt.verify.check("rebuild").unwrap().outcome,
            Outcome::Pass,
            "{}",
            r.rebuilt.verify
        );
        assert_eq!(
            r.rebuilt.verify.check("continuity").unwrap().outcome,
            Outcome::Pass,
            "{}",
            r.rebuilt.verify
        );
        let day = dir.path().join("binance/BTCUSDT/2026-11-21");
        let m = crate::tables::read::read_manifest(&day).unwrap();
        assert_eq!(
            m.quality["synced"], 2,
            "the book is valid on both connections"
        );
        assert_eq!(m.quality.get("snapshot_too_old"), None);
        let requests = seen.lock().unwrap();
        assert_eq!(requests.len(), 144, "ten minutes a request");
        assert!(
            requests[0].contains("accept-encoding: zstd")
                || requests[0].contains("Accept-Encoding: zstd")
        );
        assert!(requests[0].contains("sliceSize=10&filters=%5B%7B%22channel%22%3A%22depth%22"));
        assert!(
            !requests[0].to_lowercase().contains("authorization"),
            "no key, no header"
        );
        drop(requests);
        // the frames are the recorder's: a second import into the same day is refused
        let again = tardis_day(
            &base,
            None,
            "2026-11-21",
            "BTCUSDT",
            dir.path(),
            steps,
            true,
        );
        assert!(matches!(again, Err(ImportError::Exists(_))));
    }

    #[test]
    fn a_refusal_names_the_key() {
        let (base, _) = serve(Vec::new(), 401);
        let dir = tempfile::tempdir().unwrap();
        let steps = ("0.01000000".to_owned(), "0.00001000".to_owned());
        let err = tardis_day(
            &base,
            None,
            "2026-11-02",
            "BTCUSDT",
            dir.path(),
            steps,
            true,
        )
        .err()
        .unwrap();
        assert!(err.to_string().contains("TARDIS_API_KEY"), "{err}");
    }

    #[test]
    fn times_parse_to_the_nanosecond() {
        let day = day_start("2026-11-01").unwrap();
        assert_eq!(
            parse_time("2026-11-01T00:00:00.1422608Z"),
            Some(day + 142_260_800)
        );
        assert_eq!(
            parse_time("2026-11-01T13:05:12Z"),
            Some(day + (13 * 3600 + 5 * 60 + 12) * 1_000_000_000)
        );
        assert_eq!(
            parse_time("2026-11-01T00:00:00.123456789Z"),
            Some(day + 123_456_789)
        );
        for bad in [
            "2026-11-01 00:00:00Z",
            "2026-11-01T00:00:00",
            "2026-11-01T24:00:00Z",
            "2026-11-01T00:00:00.1234567890Z",
            "2026-11-01T00:00:00.12a4Z",
        ] {
            assert_eq!(parse_time(bad), None, "{bad}");
        }
    }

    #[test]
    fn filters_are_percent_encoded() {
        assert_eq!(
            percent_encode(r#"[{"a":"b c"}]"#),
            "%5B%7B%22a%22%3A%22b%20c%22%7D%5D"
        );
    }
}
