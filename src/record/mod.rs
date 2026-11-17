//! Live recording: three threads and two queues, as in the design.
//!
//! ```text
//! network thread    WebSocket session + snapshot fetcher (tokio, one thread)
//!       |  SPSC queue, never blocks: a full queue drops frames and leaves an overflow marker
//! decode thread     Pipeline::on_frame: decode, book sync, cross-checks, quality events
//!       |  SPSC queue, never blocks: the same rule
//! writer thread     frames files, rotated every UTC hour
//! ```
//!
//! The network thread only moves bytes; slow disks or a busy decoder never delay reading
//! the socket. Snapshot requests go back from the decode thread to the network thread, and
//! their answers come forward through the same queue as the market data, so the decoder
//! sees one ordered stream, and that stream is what the frames files store: replaying them
//! reproduces the live run exactly.
//!
//! Stopping (Ctrl+C, SIGTERM or the configured duration) flows downstream: the session
//! closes and reports `disconnected`, the queues drain, the last file is synced and renamed.

pub mod disk;
pub mod fetch;
pub mod queue;
pub mod writer;

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc as std_mpsc};
use std::thread;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};

use crate::book::{LadderBook, RefBook};
use crate::frames::{FrameKind, Header, OwnedFrame};
use crate::json::TimeUnit;
use crate::net::rest::{Rest, RestError};
use crate::net::ws::{self, BackoffConfig, WsConfig, now_ns};
use crate::pipeline::{Pipeline, QualityEvent, Sink, Stats};
use crate::sync::SyncConfig;
use crate::tables::Tables;
use fetch::FetchConfig;
use writer::Rotator;

#[derive(Debug, Clone)]
pub struct RecordConfig {
    /// Symbol as Binance writes it, for example `BTCUSDT`.
    pub symbol: String,
    /// Data root; files go to `<out>/binance/<symbol>/<date>/frames/`.
    pub out: PathBuf,
    pub ws_base: String,
    pub rest_base: String,
    /// Stop after this long; `None` runs until Ctrl+C or SIGTERM.
    pub duration: Option<Duration>,
    pub fetch: FetchConfig,
    /// Capacity of each of the two queues, in frames.
    pub queue_capacity: usize,
    pub stale_after: Duration,
    /// Pause between round-trip pings.
    pub probe_every: Duration,
    /// Age at which a connection is replaced make-before-break, and how long the two
    /// deliver together.
    pub rotate_after: Duration,
    pub overlap: Duration,
    pub backoff: BackoffConfig,
    pub status_every: Duration,
    /// Stop on Ctrl+C and SIGTERM. Off in tests.
    pub handle_signals: bool,
    /// zstd level for finished hourly frames files; `None` keeps them uncompressed.
    pub compress_level: Option<i32>,
    /// Refuse to start, and stop cleanly, below this much free space on the data disk.
    pub min_free_bytes: u64,
    /// How free space is measured; a test can put its own here.
    pub disk_free: fn(&Path) -> Option<u64>,
}

impl RecordConfig {
    pub fn new(symbol: &str, out: PathBuf) -> Self {
        Self {
            symbol: symbol.to_ascii_uppercase(),
            out,
            ws_base: ws::DEFAULT_BASE.into(),
            rest_base: crate::net::rest::DEFAULT_BASE.into(),
            duration: None,
            fetch: FetchConfig::default(),
            queue_capacity: 1 << 16,
            stale_after: Duration::from_secs(15),
            probe_every: ws::PROBE_EVERY,
            rotate_after: ws::ROTATE_AFTER,
            overlap: ws::OVERLAP,
            backoff: BackoffConfig::default(),
            status_every: Duration::from_secs(10),
            handle_signals: true,
            compress_level: Some(crate::frames::zst::LEVEL),
            min_free_bytes: disk::MIN_FREE,
            disk_free: disk::free_bytes,
        }
    }
}

/// Progress, delivered on the calling thread.
#[derive(Debug, Clone)]
pub enum Progress {
    Status(Box<Status>),
    Quality(i64, QualityEvent),
    /// Something that ends the recording early, for example a disk error.
    Fatal(String),
    /// The recording stops on purpose before its end, for example because the disk fills.
    Stopping(String),
}

#[derive(Debug, Clone, Default)]
pub struct Status {
    pub elapsed: Duration,
    pub stats: Stats,
    pub bytes_written: u64,
    pub file: Option<PathBuf>,
    pub network_queue_max: usize,
    pub writer_queue_max: usize,
    pub writer_dropped: u64,
    /// Free space on the data disk, if it can be told.
    pub disk_free: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct RecordSummary {
    pub header: Header,
    pub symbol_status: String,
    pub stats: Stats,
    /// Frames files, in order.
    pub files: Vec<PathBuf>,
    /// Parquet parts and manifests.
    pub tables: Vec<PathBuf>,
    /// Bytes of frames before and after compression, for the files that were compressed.
    pub compressed: (u64, u64),
    /// Files left uncompressed because compressing them failed, with the reason.
    pub not_compressed: Vec<(PathBuf, String)>,
    pub bytes_written: u64,
    pub network_dropped: u64,
    pub writer_dropped: u64,
    pub network_queue_max: usize,
    pub writer_queue_max: usize,
    /// Why the recording stopped early on purpose: the disk filled up.
    pub low_disk: Option<String>,
}

#[derive(Debug)]
pub enum RecordError {
    Rest(RestError),
    BadRules(String),
    Io(std::io::Error),
    /// A worker thread failed.
    Thread(String),
    /// Less free space on the data disk than the configured minimum.
    LowDisk {
        free: u64,
        min: u64,
    },
}

impl fmt::Display for RecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecordError::Rest(e) => write!(f, "{e}"),
            RecordError::BadRules(e) => write!(f, "symbol rules: {e}"),
            RecordError::Io(e) => write!(f, "{e}"),
            RecordError::Thread(e) => write!(f, "{e}"),
            RecordError::LowDisk { free, min } => write!(
                f,
                "only {} free on the data disk, below the minimum of {}",
                disk::gb(*free),
                disk::gb(*min)
            ),
        }
    }
}

impl std::error::Error for RecordError {}

impl From<std::io::Error> for RecordError {
    fn from(e: std::io::Error) -> Self {
        RecordError::Io(e)
    }
}

#[derive(Default)]
struct Shared {
    stats: Stats,
    bytes_written: u64,
    file: Option<PathBuf>,
    writer_dropped: u64,
}

fn overflow(dropped: u64, queue: &str) -> OwnedFrame {
    OwnedFrame {
        kind: FrameKind::Overflow,
        recv_ts: now_ns(),
        aux: dropped,
        payload: queue.as_bytes().to_vec(),
    }
}

struct LiveSink {
    requests: mpsc::UnboundedSender<u64>,
    progress: std_mpsc::Sender<Progress>,
}

impl Sink for LiveSink {
    fn request_snapshot(&mut self, id: u64) {
        let _ = self.requests.send(id);
    }

    fn quality(&mut self, recv_ts: i64, event: &QualityEvent) {
        let _ = self
            .progress
            .send(Progress::Quality(recv_ts, event.clone()));
    }
}

/// Record until stopped. `progress` runs on the calling thread.
pub fn record(
    cfg: RecordConfig,
    mut progress: impl FnMut(Progress),
) -> Result<RecordSummary, RecordError> {
    if let Some(free) = (cfg.disk_free)(&cfg.out)
        && free < cfg.min_free_bytes
    {
        return Err(RecordError::LowDisk {
            free,
            min: cfg.min_free_bytes,
        });
    }
    let rest = Rest::new(&cfg.rest_base, Duration::from_secs(10)).map_err(RecordError::Rest)?;
    let info = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(rest.symbol_info(&cfg.symbol))
        .map_err(RecordError::Rest)?;
    let header = Header {
        symbol: info.symbol.clone(),
        price_step: info.tick_size.clone(),
        qty_step: info.step_size.clone(),
        time_unit: TimeUnit::Micro,
        recorder: recorder_id(),
    };
    header
        .grids()
        .map_err(|e| RecordError::BadRules(e.to_string()))?;
    // the hot path uses the fast book; the writer derives the tables with the reference
    // book, so every live run also checks one against the other (see the tests)
    let pipeline = Pipeline::new(&header, LadderBook::new(), SyncConfig::default())
        .map_err(|e| RecordError::BadRules(e.to_string()))?;

    let started = Instant::now();
    let root = cfg.out.join("binance").join(&header.symbol);
    let (mut net_tx, mut net_rx, net_hw) = queue::channel::<OwnedFrame>(cfg.queue_capacity);
    let (mut wr_tx, mut wr_rx, wr_hw) = queue::channel::<OwnedFrame>(cfg.queue_capacity);
    let shared = Arc::new(Mutex::new(Shared::default()));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let (progress_tx, progress_rx) = std_mpsc::channel::<Progress>();
    let (req_tx, req_rx) = mpsc::unbounded_channel::<u64>();

    let writer = {
        let (shared, progress_tx, header) = (shared.clone(), progress_tx.clone(), header.clone());
        let level = cfg.compress_level;
        thread::Builder::new()
            .name("marketrec-writer".into())
            .spawn(
                move || -> Result<(Vec<PathBuf>, Vec<PathBuf>, u64, Compression), String> {
                    let mut rot = Rotator::new(&root, header.clone());
                    let compressor = Compressor::start(level)?;
                    // the tables are derived from the frames by a pipeline of their own, exactly
                    // as `marketrec convert` derives them offline
                    let mut tables = Tables::new(&root, header.clone(), &recorder_id());
                    let mut derive = Pipeline::new(&header, RefBook::new(), SyncConfig::default())
                        .map_err(|e| e.to_string())?;
                    let mut last = Instant::now();
                    let fail = |msg: String| {
                        let _ = progress_tx.send(Progress::Fatal(msg.clone()));
                        Err(msg)
                    };
                    while let Some(frame) = wr_rx.recv() {
                        match rot.write(&frame) {
                            Ok(Some(closed)) => compressor.add(closed),
                            Ok(None) => {}
                            Err(e) => return fail(format!("writing frames failed: {e}")),
                        }
                        derive.on_frame(frame.as_frame(), &mut tables);
                        if let Some(e) = tables.error() {
                            return fail(format!("writing tables failed: {e}"));
                        }
                        if last.elapsed() >= Duration::from_millis(500) {
                            last = Instant::now();
                            let mut s = shared.lock().expect("lock");
                            s.bytes_written = rot.bytes();
                            s.file = rot.current_path().map(|p| p.to_owned());
                        }
                    }
                    let bytes = rot.bytes();
                    let frames = rot
                        .finish()
                        .map_err(|e| format!("finishing frames file failed: {e}"))?;
                    if let Some(last) = frames.last() {
                        compressor.add(last.clone());
                    }
                    let tables = tables
                        .finish()
                        .map_err(|e| format!("finishing tables failed: {e}"))?;
                    let compression = compressor.finish();
                    let frames = frames.iter().map(|p| compression.final_path(p)).collect();
                    Ok((frames, tables, bytes, compression))
                },
            )?
    };

    let decode = {
        let (shared, stop_flag) = (shared.clone(), stop_flag.clone());
        let mut sink = LiveSink {
            requests: req_tx,
            progress: progress_tx.clone(),
        };
        let mut pipeline = pipeline;
        thread::Builder::new()
            .name("marketrec-decode".into())
            .spawn(move || -> (Stats, u64) {
                let mut last = Instant::now();
                while let Some(frame) = net_rx.recv() {
                    pipeline.on_frame(frame.as_frame(), &mut sink);
                    wr_tx.push(frame, |n| overflow(n, "writer queue"));
                    if wr_tx.receiver_gone() {
                        stop_flag.store(true, Ordering::Relaxed);
                    }
                    if last.elapsed() >= Duration::from_millis(500) {
                        last = Instant::now();
                        let mut s = shared.lock().expect("lock");
                        s.stats = pipeline.stats_so_far();
                        s.writer_dropped = wr_tx.total_dropped();
                    }
                }
                (pipeline.finish().clone(), wr_tx.total_dropped())
            })?
    };

    let network = {
        let stop_flag = stop_flag.clone();
        let ws_cfg = WsConfig {
            url: ws::stream_url(&cfg.ws_base, &header.symbol, TimeUnit::Micro),
            stale_after: cfg.stale_after,
            probe_every: cfg.probe_every,
            rotate_after: cfg.rotate_after,
            overlap: cfg.overlap,
            connect_timeout: Duration::from_secs(10),
            backoff: cfg.backoff,
            seed: now_ns() as u64,
        };
        let (symbol, fetch_cfg, duration, signals, cap) = (
            header.symbol.clone(),
            cfg.fetch,
            cfg.duration,
            cfg.handle_signals,
            cfg.queue_capacity,
        );
        thread::Builder::new().name("marketrec-net".into()).spawn(
            move || -> Result<u64, String> {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| e.to_string())?;
                rt.block_on(async move {
                    let (frame_tx, mut frame_rx) = mpsc::channel::<OwnedFrame>(cap);
                    let (stop_tx, stop_rx) = watch::channel(false);
                    let session = tokio::spawn(ws::run(ws_cfg, frame_tx.clone(), stop_rx.clone()));
                    let fetcher = tokio::spawn(fetch::run(
                        rest, symbol, fetch_cfg, req_rx, frame_tx, stop_rx,
                    ));
                    let stopper = tokio::spawn(async move {
                        wait_for_stop(duration, signals, &stop_flag).await;
                        let _ = stop_tx.send(true);
                    });
                    while let Some(frame) = frame_rx.recv().await {
                        net_tx.push(frame, |n| overflow(n, "network queue"));
                        if net_tx.receiver_gone() {
                            break;
                        }
                    }
                    stopper.abort();
                    let _ = session.await;
                    let _ = fetcher.await;
                    Ok(net_tx.total_dropped())
                })
            },
        )?
    };

    // the calling thread reports progress until the network thread is done
    let mut next_status = Instant::now() + cfg.status_every;
    let mut fatal = None;
    let mut low_disk = None;
    while !network.is_finished() {
        let wait = next_status
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(100));
        match progress_rx.recv_timeout(wait) {
            Ok(Progress::Fatal(msg)) => {
                stop_flag.store(true, Ordering::Relaxed);
                fatal.get_or_insert(msg.clone());
                progress(Progress::Fatal(msg));
            }
            Ok(p) => progress(p),
            Err(_) => {}
        }
        if Instant::now() >= next_status {
            next_status += cfg.status_every;
            let disk_free = (cfg.disk_free)(&cfg.out);
            if let Some(free) = disk_free.filter(|&f| f < cfg.min_free_bytes)
                && low_disk.is_none()
            {
                let msg = format!(
                    "only {} free on the data disk, below the minimum of {}: stopping",
                    disk::gb(free),
                    disk::gb(cfg.min_free_bytes)
                );
                stop_flag.store(true, Ordering::Relaxed);
                progress(Progress::Stopping(msg.clone()));
                low_disk = Some(msg);
            }
            let s = shared.lock().expect("lock");
            progress(Progress::Status(Box::new(Status {
                elapsed: started.elapsed(),
                stats: s.stats.clone(),
                bytes_written: s.bytes_written,
                file: s.file.clone(),
                network_queue_max: net_hw.get(),
                writer_queue_max: wr_hw.get(),
                writer_dropped: s.writer_dropped,
                disk_free,
            })));
        }
    }
    let network_dropped = network
        .join()
        .map_err(|_| RecordError::Thread("network thread panicked".into()))?
        .map_err(RecordError::Thread)?;
    let (stats, writer_dropped) = decode
        .join()
        .map_err(|_| RecordError::Thread("decode thread panicked".into()))?;
    let written = writer
        .join()
        .map_err(|_| RecordError::Thread("writer thread panicked".into()))?;
    drop(progress_tx);
    for p in progress_rx.try_iter() {
        progress(p);
    }
    let (files, tables, bytes_written, compression) = written.map_err(RecordError::Thread)?;
    if let Some(msg) = fatal {
        return Err(RecordError::Thread(msg));
    }
    Ok(RecordSummary {
        header,
        symbol_status: info.status,
        stats,
        files,
        tables,
        compressed: (compression.raw_bytes, compression.compressed_bytes),
        not_compressed: compression.failed,
        bytes_written,
        network_dropped,
        writer_dropped,
        network_queue_max: net_hw.get(),
        writer_queue_max: wr_hw.get(),
        low_disk,
    })
}

/// Compresses finished hourly frames files on a thread of its own, so the writer never
/// waits for it.
struct Compressor {
    tx: Option<std_mpsc::Sender<PathBuf>>,
    thread: Option<thread::JoinHandle<Compression>>,
}

#[derive(Debug, Default)]
struct Compression {
    done: Vec<(PathBuf, PathBuf)>,
    failed: Vec<(PathBuf, String)>,
    raw_bytes: u64,
    compressed_bytes: u64,
}

impl Compression {
    fn final_path(&self, raw: &Path) -> PathBuf {
        self.done
            .iter()
            .find(|(r, _)| r == raw)
            .map_or_else(|| raw.to_owned(), |(_, z)| z.clone())
    }
}

impl Compressor {
    fn start(level: Option<i32>) -> Result<Self, String> {
        let Some(level) = level else {
            return Ok(Self {
                tx: None,
                thread: None,
            });
        };
        let (tx, rx) = std_mpsc::channel::<PathBuf>();
        let thread = thread::Builder::new()
            .name("marketrec-compress".into())
            .spawn(move || {
                let mut c = Compression::default();
                for raw in rx {
                    match crate::frames::zst::compress_file(&raw, level) {
                        Ok(z) => {
                            c.raw_bytes += z.raw_bytes;
                            c.compressed_bytes += z.compressed_bytes;
                            c.done.push((raw, z.path));
                        }
                        Err(e) => c.failed.push((raw, e.to_string())),
                    }
                }
                c
            })
            .map_err(|e| e.to_string())?;
        Ok(Self {
            tx: Some(tx),
            thread: Some(thread),
        })
    }

    fn add(&self, path: PathBuf) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(path);
        }
    }

    /// Wait for the files sent so far.
    fn finish(mut self) -> Compression {
        drop(self.tx.take());
        self.thread
            .take()
            .and_then(|t| t.join().ok())
            .unwrap_or_default()
    }
}

/// Program, version and the commit it was built from.
pub fn recorder_id() -> String {
    format!(
        "marketrec {} ({})",
        env!("CARGO_PKG_VERSION"),
        option_env!("MARKETREC_GIT").unwrap_or("unknown commit")
    )
}

async fn wait_for_stop(duration: Option<Duration>, signals: bool, flag: &AtomicBool) {
    let deadline = async {
        match duration {
            Some(d) => tokio::time::sleep(d).await,
            None => std::future::pending().await,
        }
    };
    let polled = async {
        while !flag.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::select! {
        _ = deadline => {}
        _ = polled => {}
        _ = shutdown_signal(), if signals => {}
    }
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    match signal(SignalKind::terminate()) {
        Ok(mut term) => {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = &self.stats;
        let secs = self.elapsed.as_secs_f64().max(1e-9);
        let file = self
            .file
            .as_ref()
            .and_then(|p| p.file_name())
            .map_or("-".into(), |n| n.to_string_lossy().into_owned());
        write!(
            f,
            "{:>7.0} s | valid book {:5.1}% | lag {:>7} | rtt {:>7} | {:6.0} frames/s | depth {} ticker {} trades {} | gaps {} resyncs {} reconnects {} late {} | queue max {}/{} | written {:.1} MB {file} | disk {}",
            secs,
            s.synced_share() * 100.0,
            s.last_lag_ns.map_or("-".into(), crate::timing::duration_ms),
            s.last_rtt_ns.map_or("-".into(), crate::timing::duration_ms),
            s.frames as f64 / secs,
            s.depth,
            s.book_tickers,
            s.trades,
            s.gaps,
            s.resyncs,
            s.reconnects,
            s.late_intervals,
            self.network_queue_max,
            self.writer_queue_max,
            self.bytes_written as f64 / 1e6,
            self.disk_free
                .map_or("?".into(), |b| format!("{} free", disk::gb(b))),
        )
    }
}

impl fmt::Display for RecordSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = &self.stats;
        let h = &self.header;
        writeln!(
            f,
            "symbol        {} ({}), price step {}, qty step {}",
            h.symbol, self.symbol_status, h.price_step, h.qty_step
        )?;
        writeln!(
            f,
            "frames        {} received, {} lost in the network queue, {} lost in the writer queue",
            s.frames, self.network_dropped, self.writer_dropped
        )?;
        writeln!(
            f,
            "queues        largest backlog {} (network), {} (writer)",
            self.network_queue_max, self.writer_queue_max
        )?;
        writeln!(
            f,
            "depth         {} diffs: {} applied, {} ignored, {} buffered",
            s.depth, s.depth_applied, s.depth_ignored, s.depth_buffered
        )?;
        writeln!(
            f,
            "book ticker   {} messages, {} compared with the book, {} mismatches",
            s.book_tickers, s.bbo_compared, s.bbo_mismatches
        )?;
        writeln!(f, "trades        {}", s.trades)?;
        writeln!(
            f,
            "snapshots     {} requested, {} received, {} failed",
            s.snapshot_requests, s.snapshots, s.snapshot_failures
        )?;
        writeln!(
            f,
            "continuity    {} gaps, {} resyncs, {} reconnects, {} undecodable frames",
            s.gaps, s.resyncs, s.reconnects, s.undecodable
        )?;
        writeln!(
            f,
            "valid book    {:.2}% of the time",
            s.synced_share() * 100.0
        )?;
        writeln!(
            f,
            "lag           {}",
            crate::timing::report_line(&s.lag, s.late_intervals, s.late_ns)
        )?;
        if s.duplicates > 0 {
            writeln!(
                f,
                "duplicates    {} messages delivered first by another connection, dropped",
                s.duplicates
            )?;
        }
        writeln!(f, "link          {}", crate::timing::rtt_line(&s.rtt))?;
        writeln!(
            f,
            "written       {:.1} MB of frames in {} file(s), {} table file(s)",
            self.bytes_written as f64 / 1e6,
            self.files.len(),
            self.tables.len()
        )?;
        let (raw, zst) = self.compressed;
        if zst > 0 {
            writeln!(
                f,
                "compressed    {:.1} MB of frames to {:.1} MB ({:.1}x)",
                raw as f64 / 1e6,
                zst as f64 / 1e6,
                raw as f64 / zst as f64
            )?;
        }
        for (p, why) in &self.not_compressed {
            writeln!(f, "not compressed {}: {why}", p.display())?;
        }
        if let Some(why) = &self.low_disk {
            writeln!(f, "stopped       {why}")?;
        }
        for p in &self.files {
            writeln!(f, "              {}", p.display())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tokio_tungstenite::tungstenite::Message;

    use super::*;
    use crate::net::rest::tests::{EXCHANGE_INFO, mock_http_thread};
    use crate::net::ws::tests::{Script, server_thread};
    use crate::replay::replay_file;

    fn depth(first: u64, last: u64, bid: &str) -> Message {
        Message::text(format!(
            r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":NOW_US,"s":"BTCUSDT","U":{first},"u":{last},"b":[["97123.45000000","{bid}"]],"a":[]}}}}"#
        ))
    }

    fn config(dir: &std::path::Path, rest: String, ws: String) -> RecordConfig {
        let mut cfg = RecordConfig::new("btcusdt", dir.to_owned());
        cfg.rest_base = rest;
        cfg.ws_base = ws;
        cfg.duration = Some(Duration::from_millis(1500));
        cfg.handle_signals = false;
        cfg.status_every = Duration::from_millis(200);
        cfg.min_free_bytes = 0; // the tests must not depend on this machine's disk
        cfg.backoff = BackoffConfig {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(50),
            stable_after: Duration::from_secs(60),
        };
        cfg
    }

    const SNAPSHOT: &str = r#"{"lastUpdateId":1000,"bids":[["97123.45000000","0.25000000"],["97123.44000000","0.50000000"]],"asks":[["97123.46000000","0.30000000"]]}"#;

    /// The core promise: what was recorded replays to exactly the live result.
    #[test]
    fn live_run_and_replay_of_its_files_agree() {
        let dir = tempfile::tempdir().unwrap();
        let rest = mock_http_thread(vec![
            ("/api/v3/exchangeInfo", "200 OK", "", EXCHANGE_INFO),
            (
                "/api/v3/depth",
                "200 OK",
                "x-mbx-used-weight-1m: 250\r\n",
                SNAPSHOT,
            ),
        ]);
        let mut msgs = vec![
            depth(998, 1000, "0.25000000"),
            depth(1001, 1002, "0.20000000"),
        ];
        msgs.push(Message::text(r#"{"stream":"btcusdt@bookTicker","data":{"u":1002,"s":"BTCUSDT","b":"97123.45000000","B":"0.20000000","a":"97123.46000000","A":"0.30000000"}}"#));
        for i in 0..40 {
            let first = 1003 + 2 * i;
            msgs.push(depth(first, first + 1, &format!("0.{:02}000000", 10 + i)));
            msgs.push(Message::text(format!(r#"{{"stream":"btcusdt@trade","data":{{"e":"trade","E":1795219200000000,"s":"BTCUSDT","t":{i},"p":"97123.45000000","q":"0.00010000","T":1795219200000000,"m":true,"M":true}}}}"#)));
        }
        let ws_url = server_thread(vec![Script::SendSlowlyThenWait(
            msgs,
            Duration::from_millis(10),
        )]);
        let ws_base = ws_url.trim_end_matches("/stream").to_owned();

        let mut statuses = 0;
        let mut status_valid_ns = 0;
        let mut events = Vec::new();
        let summary = record(config(dir.path(), rest, ws_base), |p| match p {
            Progress::Status(st) => {
                statuses += 1;
                status_valid_ns = st.stats.synced_ns;
            }
            Progress::Quality(_, e) => events.push(e.kind()),
            Progress::Fatal(m) | Progress::Stopping(m) => panic!("{m}"),
        })
        .unwrap();

        let s = &summary.stats;
        assert_eq!((s.depth, s.trades, s.book_tickers), (42, 40, 1));
        assert_eq!(
            (s.snapshot_requests, s.snapshots, s.gaps, s.resyncs),
            (1, 1, 0, 0)
        );
        assert_eq!((s.bbo_compared, s.bbo_mismatches), (1, 0));
        assert!(s.synced_ns > 0);
        assert_eq!((summary.network_dropped, summary.writer_dropped), (0, 0));
        assert_eq!(events, ["connected", "synced", "disconnected"]);
        assert!(statuses >= 3, "a status every 200 ms: {statuses}");
        assert!(
            status_valid_ns > 0,
            "status lines count the valid time while it lasts"
        );
        assert_eq!(summary.files.len(), 1);
        let path = &summary.files[0];
        assert!(
            path.to_string_lossy().ends_with(".frames.zst"),
            "finished files are compressed: {}",
            path.display()
        );
        assert!(summary.compressed.1 > 0 && summary.compressed.1 < summary.compressed.0);
        assert!(summary.not_compressed.is_empty());
        assert_eq!(
            std::fs::read_dir(path.parent().unwrap()).unwrap().count(),
            1,
            "the raw file is gone"
        );
        assert!(path.starts_with(dir.path().join("binance/BTCUSDT")));
        assert!(path.to_string_lossy().contains("/frames/"));

        let replayed = replay_file(path).unwrap();
        assert_eq!(
            &replayed.stats, s,
            "replay of the recorded file matches the live run"
        );
        assert_eq!(replayed.event_kinds(), events);
        assert_eq!(replayed.header, summary.header);

        // the tables written live are the tables `convert` builds from the frames
        use crate::tables::read::{read_manifest, read_table};
        use crate::tables::schema::Table;
        let live_day = path.parent().unwrap().parent().unwrap();
        let out = tempfile::tempdir().unwrap();
        crate::tables::convert::convert(path, out.path(), "test").unwrap();
        let converted = out
            .path()
            .join("binance/BTCUSDT")
            .join(live_day.file_name().unwrap());
        for table in Table::ALL {
            let live = read_table(live_day, table).unwrap();
            assert_eq!(
                live,
                read_table(&converted, table).unwrap(),
                "{}",
                table.name()
            );
        }
        let (a, b) = (
            read_manifest(live_day).unwrap(),
            read_manifest(&converted).unwrap(),
        );
        assert_eq!((&a.rows, &a.quality), (&b.rows, &b.quality));
        assert_eq!((a.covered_ns, a.synced_ns), (b.covered_ns, b.synced_ns));
        assert_eq!(a.rows["depth_diff"], 42, "one level per diff");
        assert_eq!(a.rows["trades"], 40);
        assert!(a.rows["book_l20"] > 0);
        assert_eq!(a.runs[0].recorder, recorder_id());
        assert_eq!(summary.tables.len(), 6, "a part per table and the manifest");

        let report = crate::verify::verify_day(live_day, &Default::default());
        assert_eq!(report.outcome(), crate::verify::Outcome::Pass, "{report}");
    }

    /// The ТЗ's stage 5: a connection replaced during a recording costs no gap, no
    /// snapshot and no rebuild.
    #[test]
    fn a_connection_replaced_mid_recording_costs_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let rest = mock_http_thread(vec![
            ("/api/v3/exchangeInfo", "200 OK", "", EXCHANGE_INFO),
            ("/api/v3/depth", "200 OK", "", SNAPSHOT),
        ]);
        // the same stream on both connections; the second sends faster to catch up
        let stream: Vec<Message> = std::iter::once(depth(998, 1000, "0.25000000"))
            .chain((0..80).map(|i| {
                let first = 1001 + 2 * i;
                depth(first, first + 1, &format!("0.{:02}000000", 10 + i))
            }))
            .collect();
        // after 20 updates the exchange announces the end of connection 1
        let mut first = stream.clone();
        first.insert(
            21,
            Message::text(
                r#"{"stream":"!serverShutdown","data":{"e":"serverShutdown","E":1,"T":2}}"#,
            ),
        );
        let ws_url = server_thread(vec![
            Script::SendSlowlyThenWait(first, Duration::from_millis(10)),
            Script::SendSlowlyThenWait(stream, Duration::from_millis(3)),
        ]);
        let mut cfg = config(
            dir.path(),
            rest,
            ws_url.trim_end_matches("/stream").to_owned(),
        );
        cfg.overlap = Duration::from_millis(100);
        let mut events = Vec::new();
        let summary = record(cfg, |p| {
            if let Progress::Quality(_, e) = p {
                events.push(e.to_string());
            }
        })
        .unwrap();
        let s = &summary.stats;
        assert_eq!(
            (s.gaps, s.resyncs, s.reconnects, s.snapshot_requests),
            (0, 0, 0, 1),
            "{events:?}"
        );
        assert_eq!(s.depth, 81, "every update once");
        assert!(s.duplicates > 0);
        let kinds: Vec<&str> = events
            .iter()
            .map(|e| e.split(' ').next().unwrap())
            .collect();
        assert_eq!(
            kinds,
            [
                "connected",
                "synced",
                "server_shutdown",
                "connected",
                "disconnected",
                "disconnected"
            ],
            "{events:?}"
        );
        assert!(
            events[5].contains("conn=2 reason=\"stopped\""),
            "{events:?}"
        );
        assert!(
            events[4].contains("conn=1 reason=\"handover\""),
            "{events:?}"
        );
        let day = summary.files[0].parent().unwrap().parent().unwrap();
        let report = crate::verify::verify_day(day, &Default::default());
        assert_eq!(report.outcome(), crate::verify::Outcome::Pass, "{report}");
    }

    #[test]
    fn a_full_disk_refuses_the_start_and_a_filling_one_stops_the_recording_cleanly() {
        use std::sync::atomic::AtomicU64;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = config(
            dir.path(),
            "http://127.0.0.1:9".into(),
            "ws://127.0.0.1:9".into(),
        );
        cfg.min_free_bytes = u64::MAX;
        let err = record(cfg, |_| {}).unwrap_err();
        assert!(matches!(err, RecordError::LowDisk { .. }), "{err}");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);

        // free space falls below the limit during a recording
        static FREE: AtomicU64 = AtomicU64::new(u64::MAX);
        fn fake(_: &Path) -> Option<u64> {
            Some(FREE.load(Ordering::Relaxed))
        }
        let rest = mock_http_thread(vec![
            ("/api/v3/exchangeInfo", "200 OK", "", EXCHANGE_INFO),
            ("/api/v3/depth", "200 OK", "", SNAPSHOT),
        ]);
        let msgs: Vec<Message> = (0..300)
            .map(|i| depth(1001 + i, 1001 + i, "0.10000000"))
            .collect();
        let ws_url = server_thread(vec![Script::SendSlowlyThenWait(
            msgs,
            Duration::from_millis(10),
        )]);
        let mut cfg = config(
            dir.path(),
            rest,
            ws_url.trim_end_matches("/stream").to_owned(),
        );
        cfg.duration = Some(Duration::from_secs(20));
        cfg.min_free_bytes = 1_000_000_000;
        cfg.disk_free = fake;
        let mut stopping = Vec::new();
        let started = Instant::now();
        let summary = record(cfg, |p| match p {
            Progress::Status(s) => {
                if s.disk_free == Some(u64::MAX) && s.stats.depth > 20 {
                    FREE.store(500_000_000, Ordering::Relaxed);
                }
            }
            Progress::Stopping(m) => stopping.push(m),
            _ => {}
        })
        .unwrap();
        assert!(started.elapsed() < Duration::from_secs(10), "stopped early");
        assert_eq!(stopping.len(), 1, "{stopping:?}");
        assert!(stopping[0].contains("only 0.5 GB free"), "{stopping:?}");
        assert!(summary.low_disk.is_some());
        let day = summary.files[0].parent().unwrap().parent().unwrap();
        let report = crate::verify::verify_day(day, &Default::default());
        assert_eq!(
            report.outcome(),
            crate::verify::Outcome::Pass,
            "a clean stop: {report}"
        );
    }

    #[test]
    fn unknown_symbol_fails_before_anything_starts() {
        let dir = tempfile::tempdir().unwrap();
        let rest = mock_http_thread(vec![(
            "/api/v3/exchangeInfo",
            "400 Bad Request",
            "",
            r#"{"code":-1121,"msg":"Invalid symbol."}"#,
        )]);
        let err = record(config(dir.path(), rest, "ws://127.0.0.1:9".into()), |_| {}).unwrap_err();
        assert!(err.to_string().contains("Invalid symbol"), "{err}");
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            0,
            "no files"
        );
    }

    #[test]
    fn unreachable_stream_still_stops_cleanly_and_records_the_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let rest = mock_http_thread(vec![("/api/v3/exchangeInfo", "200 OK", "", EXCHANGE_INFO)]);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let dead = format!("ws://{}", listener.local_addr().unwrap());
        drop(listener);
        let mut cfg = config(dir.path(), rest, dead);
        cfg.duration = Some(Duration::from_millis(300));
        let summary = record(cfg, |_| {}).unwrap();
        assert_eq!(
            summary.stats.reconnects, 0,
            "failed attempts never delivered data"
        );
        assert!(
            summary.stats.frames >= 2,
            "every failed attempt is a disconnected frame"
        );
        let replayed = replay_file(&summary.files[0]).unwrap();
        assert!(replayed.event_kinds().iter().all(|k| *k == "disconnected"));
    }
}
