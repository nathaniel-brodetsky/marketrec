//! Frames file: the raw input of the hot path, recorded so that it can be replayed exactly.
//!
//! The network thread hands the decoding thread a sequence of frames: WebSocket messages,
//! REST snapshots and connection events, each stamped with the local receive time. A frames
//! file stores that sequence verbatim, so `replay` runs the same code on the same input as
//! the live recorder did. The byte layout is specified in `docs/FRAMES.md`.
//!
//! Integrity: every record carries a CRC-32 of its contents. A reader returns every intact
//! record and then stops with an explicit error; it never returns altered data. A file cut
//! short by a crash ends with `Truncated`, a damaged one with `Corrupt`.

pub mod text;

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::decimal::{Grid, ParseError};
use crate::json::TimeUnit;

pub const MAGIC: [u8; 8] = *b"MRFRAMES";
pub const VERSION: u16 = 1;
/// Upper bound for one payload. A REST snapshot of 5000 levels per side is about 300 KiB.
pub const MAX_PAYLOAD: u32 = 16 << 20;
pub const MAX_HEADER: u32 = 1 << 20;

/// Bytes before the payload: length (4), kind (1), receive time (8), aux (8).
const PREFIX: usize = 4 + 1 + 8 + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum FrameKind {
    /// WebSocket text message (JSON feed). `aux` is the connection number.
    Text = 1,
    /// WebSocket binary message (SBE feed). `aux` is the connection number.
    Binary = 2,
    /// Body of a REST depth snapshot response. `aux` is the snapshot request ID.
    Snapshot = 3,
    /// A connection was established. `aux` is its number, the payload is the URL.
    Connected = 4,
    /// A connection ended. `aux` is its number, the payload is the reason.
    Disconnected = 5,
}

impl FrameKind {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            1 => FrameKind::Text,
            2 => FrameKind::Binary,
            3 => FrameKind::Snapshot,
            4 => FrameKind::Connected,
            5 => FrameKind::Disconnected,
            _ => return None,
        })
    }
}

/// What every frame of the file refers to. Stored once, at the start of the file, as JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    pub symbol: String,
    /// Tick size as Binance writes it, for example "0.01000000".
    pub price_step: String,
    /// Step size as Binance writes it, for example "0.00001000".
    pub qty_step: String,
    /// Unit of the time fields inside the feed messages.
    pub time_unit: TimeUnit,
    /// Program and version that wrote the file.
    pub recorder: String,
}

impl Header {
    pub fn grids(&self) -> Result<(Grid, Grid), ParseError> {
        Ok((
            Grid::parse(self.price_step.as_bytes())?,
            Grid::parse(self.qty_step.as_bytes())?,
        ))
    }
}

/// One record, borrowed from the reader's buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frame<'a> {
    pub kind: FrameKind,
    /// Local receive time, nanoseconds since the Unix epoch.
    pub recv_ts: i64,
    pub aux: u64,
    pub payload: &'a [u8],
}

/// One record with its own payload, for tests and conversions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedFrame {
    pub kind: FrameKind,
    pub recv_ts: i64,
    pub aux: u64,
    pub payload: Vec<u8>,
}

impl OwnedFrame {
    pub fn as_frame(&self) -> Frame<'_> {
        Frame {
            kind: self.kind,
            recv_ts: self.recv_ts,
            aux: self.aux,
            payload: &self.payload,
        }
    }
}

impl Frame<'_> {
    pub fn to_owned(&self) -> OwnedFrame {
        OwnedFrame {
            kind: self.kind,
            recv_ts: self.recv_ts,
            aux: self.aux,
            payload: self.payload.to_vec(),
        }
    }
}

#[derive(Debug)]
pub enum FramesError {
    Io(io::Error),
    BadMagic,
    UnsupportedVersion(u16),
    BadHeader(String),
    /// The file ends inside the record that starts at `offset`.
    Truncated {
        offset: u64,
    },
    /// The record at `offset` is damaged.
    Corrupt {
        offset: u64,
        reason: &'static str,
    },
}

impl fmt::Display for FramesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FramesError::Io(e) => write!(f, "i/o: {e}"),
            FramesError::BadMagic => write!(f, "not a frames file"),
            FramesError::UnsupportedVersion(v) => write!(
                f,
                "frames format version {v} is not supported (this build reads {VERSION})"
            ),
            FramesError::BadHeader(e) => write!(f, "bad header: {e}"),
            FramesError::Truncated { offset } => {
                write!(f, "file ends inside the record at byte {offset}")
            }
            FramesError::Corrupt { offset, reason } => {
                write!(f, "record at byte {offset} is damaged: {reason}")
            }
        }
    }
}

impl std::error::Error for FramesError {}

impl From<io::Error> for FramesError {
    fn from(e: io::Error) -> Self {
        FramesError::Io(e)
    }
}

fn checksum(prefix_without_len: &[u8], payload: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(prefix_without_len);
    h.update(payload);
    h.finalize()
}

/// Writes the frames format to any byte sink. Wrap files in a `BufWriter`.
pub struct FramesWriter<W: Write> {
    inner: W,
}

impl<W: Write> FramesWriter<W> {
    pub fn new(mut inner: W, header: &Header) -> io::Result<Self> {
        let json = serde_json::to_vec(header)?;
        let len = u32::try_from(json.len())
            .ok()
            .filter(|&n| n <= MAX_HEADER)
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "header too large"))?;
        inner.write_all(&MAGIC)?;
        inner.write_all(&VERSION.to_le_bytes())?;
        inner.write_all(&len.to_le_bytes())?;
        inner.write_all(&json)?;
        Ok(Self { inner })
    }

    pub fn write_frame(&mut self, frame: Frame<'_>) -> io::Result<()> {
        let len = u32::try_from(frame.payload.len())
            .ok()
            .filter(|&n| n <= MAX_PAYLOAD)
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "payload too large"))?;
        let mut prefix = [0u8; PREFIX];
        prefix[0..4].copy_from_slice(&len.to_le_bytes());
        prefix[4] = frame.kind as u8;
        prefix[5..13].copy_from_slice(&frame.recv_ts.to_le_bytes());
        prefix[13..21].copy_from_slice(&frame.aux.to_le_bytes());
        let crc = checksum(&prefix[4..], frame.payload);
        self.inner.write_all(&prefix)?;
        self.inner.write_all(frame.payload)?;
        self.inner.write_all(&crc.to_le_bytes())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

/// A frames file being written. It lives under `<path>.tmp` and appears under `<path>` only
/// after `finish`, flushed and synced; an existing file is never overwritten.
pub struct FramesFile {
    writer: FramesWriter<BufWriter<File>>,
    tmp: PathBuf,
    path: PathBuf,
}

impl FramesFile {
    pub fn create(path: &Path, header: &Header) -> io::Result<Self> {
        if path.exists() {
            return Err(io::Error::new(
                ErrorKind::AlreadyExists,
                format!("{} already exists", path.display()),
            ));
        }
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        let file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        let writer = FramesWriter::new(BufWriter::with_capacity(1 << 20, file), header)?;
        Ok(Self {
            writer,
            tmp,
            path: path.to_owned(),
        })
    }

    pub fn write_frame(&mut self, frame: Frame<'_>) -> io::Result<()> {
        self.writer.write_frame(frame)
    }

    /// Flush, sync, then rename to the final name. Returns the final path.
    pub fn finish(self) -> io::Result<PathBuf> {
        let file = self
            .writer
            .into_inner()
            .into_inner()
            .map_err(|e| e.into_error())?;
        file.sync_all()?;
        drop(file);
        if self.path.exists() {
            return Err(io::Error::new(
                ErrorKind::AlreadyExists,
                format!("{} already exists", self.path.display()),
            ));
        }
        fs::rename(&self.tmp, &self.path)?;
        #[cfg(unix)]
        if let Some(dir) = self.path.parent().filter(|d| !d.as_os_str().is_empty()) {
            File::open(dir)?.sync_all()?; // make the rename itself durable
        }
        Ok(self.path)
    }
}

/// Reads the frames format. `next_frame` lends each payload from one reused buffer.
pub struct FramesReader<R: Read> {
    inner: R,
    header: Header,
    payload: Vec<u8>,
    offset: u64,
    failed: bool,
}

impl<R: Read> FramesReader<R> {
    pub fn new(mut inner: R) -> Result<Self, FramesError> {
        let mut fixed = [0u8; 14];
        if read_full(&mut inner, &mut fixed)? < fixed.len() {
            return Err(FramesError::BadMagic);
        }
        if fixed[0..8] != MAGIC {
            return Err(FramesError::BadMagic);
        }
        let version = u16::from_le_bytes([fixed[8], fixed[9]]);
        if version != VERSION {
            return Err(FramesError::UnsupportedVersion(version));
        }
        let len = u32::from_le_bytes(fixed[10..14].try_into().expect("4 bytes"));
        if len > MAX_HEADER {
            return Err(FramesError::BadHeader("header length out of range".into()));
        }
        let mut json = vec![0u8; len as usize];
        if read_full(&mut inner, &mut json)? < json.len() {
            return Err(FramesError::BadHeader("file ends inside the header".into()));
        }
        let header: Header =
            serde_json::from_slice(&json).map_err(|e| FramesError::BadHeader(e.to_string()))?;
        Ok(Self {
            inner,
            header,
            payload: Vec::new(),
            offset: 14 + u64::from(len),
            failed: false,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Byte offset of the next record.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// The next record, `Ok(None)` at a clean end of file. After an error, every further
    /// call returns `Ok(None)`: nothing past a damaged record is trusted.
    pub fn next_frame(&mut self) -> Result<Option<Frame<'_>>, FramesError> {
        if self.failed {
            return Ok(None);
        }
        match self.read_record() {
            Ok(true) => {}
            Ok(false) => return Ok(None),
            Err(e) => {
                self.failed = true;
                return Err(e);
            }
        }
        let p = &self.payload;
        let kind = FrameKind::from_u8(p[0]).expect("checked in read_record");
        let recv_ts = i64::from_le_bytes(p[1..9].try_into().expect("8 bytes"));
        let aux = u64::from_le_bytes(p[9..17].try_into().expect("8 bytes"));
        Ok(Some(Frame {
            kind,
            recv_ts,
            aux,
            payload: &p[17..],
        }))
    }

    /// Reads one record into `self.payload` as kind, receive time, aux and payload.
    fn read_record(&mut self) -> Result<bool, FramesError> {
        let start = self.offset;
        let mut len_bytes = [0u8; 4];
        match read_full(&mut self.inner, &mut len_bytes)? {
            0 => return Ok(false),
            4 => {}
            _ => return Err(FramesError::Truncated { offset: start }),
        }
        let len = u32::from_le_bytes(len_bytes);
        if len > MAX_PAYLOAD {
            return Err(FramesError::Corrupt {
                offset: start,
                reason: "payload length out of range",
            });
        }
        let body = PREFIX - 4 + len as usize;
        self.payload.clear();
        self.payload.resize(body + 4, 0);
        if read_full(&mut self.inner, &mut self.payload)? < body + 4 {
            return Err(FramesError::Truncated { offset: start });
        }
        let stored = u32::from_le_bytes(self.payload[body..].try_into().expect("4 bytes"));
        if checksum(&self.payload[..PREFIX - 4], &self.payload[PREFIX - 4..body]) != stored {
            return Err(FramesError::Corrupt {
                offset: start,
                reason: "checksum mismatch",
            });
        }
        if FrameKind::from_u8(self.payload[0]).is_none() {
            return Err(FramesError::Corrupt {
                offset: start,
                reason: "unknown frame kind",
            });
        }
        self.payload.truncate(body);
        self.offset += 4 + body as u64 + 4;
        Ok(true)
    }
}

/// Like `read_exact`, but reports how many bytes were read before end of file.
fn read_full(r: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    pub(crate) fn header() -> Header {
        Header {
            symbol: "BTCUSDT".into(),
            price_step: "0.01000000".into(),
            qty_step: "0.00001000".into(),
            time_unit: TimeUnit::Micro,
            recorder: "marketrec test".into(),
        }
    }

    fn sample() -> Vec<OwnedFrame> {
        vec![
            OwnedFrame {
                kind: FrameKind::Connected,
                recv_ts: 10,
                aux: 1,
                payload: b"wss://example/stream".to_vec(),
            },
            OwnedFrame {
                kind: FrameKind::Text,
                recv_ts: 11,
                aux: 1,
                payload: br#"{"stream":"btcusdt@trade","data":{}}"#.to_vec(),
            },
            OwnedFrame {
                kind: FrameKind::Snapshot,
                recv_ts: 12,
                aux: 7,
                payload: br#"{"lastUpdateId":5,"bids":[],"asks":[]}"#.to_vec(),
            },
            OwnedFrame {
                kind: FrameKind::Binary,
                recv_ts: -3,
                aux: u64::MAX,
                payload: vec![0, 255, 1, 2],
            },
            OwnedFrame {
                kind: FrameKind::Disconnected,
                recv_ts: 13,
                aux: 1,
                payload: Vec::new(),
            },
        ]
    }

    fn encode(frames: &[OwnedFrame]) -> Vec<u8> {
        let mut w = FramesWriter::new(Vec::new(), &header()).unwrap();
        for f in frames {
            w.write_frame(f.as_frame()).unwrap();
        }
        w.into_inner()
    }

    /// All intact frames, and the error that stopped reading, if any.
    fn decode(bytes: &[u8]) -> (Vec<OwnedFrame>, Option<FramesError>) {
        let mut r = match FramesReader::new(bytes) {
            Ok(r) => r,
            Err(e) => return (Vec::new(), Some(e)),
        };
        let mut out = Vec::new();
        loop {
            match r.next_frame() {
                Ok(Some(f)) => out.push(f.to_owned()),
                Ok(None) => return (out, None),
                Err(e) => return (out, Some(e)),
            }
        }
    }

    fn header_len() -> usize {
        14 + serde_json::to_vec(&header()).unwrap().len()
    }

    #[test]
    fn roundtrip() {
        let bytes = encode(&sample());
        let r = FramesReader::new(&bytes[..]).unwrap();
        assert_eq!(r.header(), &header());
        assert_eq!(r.header().grids().unwrap().0, Grid::parse(b"0.01").unwrap());
        let (frames, err) = decode(&bytes);
        assert_eq!(frames, sample());
        assert!(err.is_none());
    }

    #[test]
    fn every_cut_returns_the_complete_records_then_truncated() {
        let frames = sample();
        let bytes = encode(&frames);
        let mut ends = vec![header_len()];
        for f in &frames {
            ends.push(ends.last().unwrap() + PREFIX + f.payload.len() + 4);
        }
        assert_eq!(*ends.last().unwrap(), bytes.len());
        for cut in header_len()..bytes.len() {
            let (got, err) = decode(&bytes[..cut]);
            let complete = ends.iter().filter(|&&e| e <= cut).count() - 1;
            assert_eq!(got, frames[..complete], "cut at {cut}");
            if ends.contains(&cut) {
                assert!(err.is_none(), "cut at a record boundary is a clean end");
            } else {
                assert!(
                    matches!(err, Some(FramesError::Truncated { .. })),
                    "cut at {cut}: {err:?}"
                );
            }
        }
    }

    #[test]
    fn header_errors() {
        let bytes = encode(&sample());
        assert!(matches!(
            FramesReader::new(&b"NOTFRAMES....."[..]),
            Err(FramesError::BadMagic)
        ));
        assert!(matches!(
            FramesReader::new(&bytes[..5]),
            Err(FramesError::BadMagic)
        ));
        let mut v2 = bytes.clone();
        v2[8] = 2;
        assert!(matches!(
            FramesReader::new(&v2[..]),
            Err(FramesError::UnsupportedVersion(2))
        ));
        assert!(matches!(
            FramesReader::new(&bytes[..20]),
            Err(FramesError::BadHeader(_))
        ));
    }

    #[test]
    fn oversized_length_is_rejected_without_allocating_it() {
        let mut bytes = encode(&sample()[..1]);
        let at = header_len();
        bytes[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let (got, err) = decode(&bytes);
        assert!(got.is_empty());
        assert!(matches!(
            err,
            Some(FramesError::Corrupt {
                reason: "payload length out of range",
                ..
            })
        ));
    }

    #[test]
    fn nothing_is_returned_after_an_error() {
        let mut bytes = encode(&sample());
        let at = header_len() + PREFIX + 2; // inside the first payload
        bytes[at] ^= 1;
        let mut r = FramesReader::new(&bytes[..]).unwrap();
        assert!(
            matches!(r.next_frame(), Err(FramesError::Corrupt { offset, .. }) if offset == header_len() as u64)
        );
        assert!(matches!(r.next_frame(), Ok(None)));
    }

    #[test]
    fn writer_rejects_oversized_payload() {
        let mut w = FramesWriter::new(Vec::new(), &header()).unwrap();
        let big = vec![0u8; MAX_PAYLOAD as usize + 1];
        let f = Frame {
            kind: FrameKind::Text,
            recv_ts: 0,
            aux: 0,
            payload: &big,
        };
        assert_eq!(
            w.write_frame(f).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }

    #[test]
    fn file_appears_only_after_finish_and_is_never_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("day.frames");
        let mut f = FramesFile::create(&path, &header()).unwrap();
        for fr in sample() {
            f.write_frame(fr.as_frame()).unwrap();
        }
        assert!(!path.exists());
        assert!(dir.path().join("day.frames.tmp").exists());
        assert_eq!(f.finish().unwrap(), path);
        assert!(!dir.path().join("day.frames.tmp").exists());
        let (frames, err) = decode(&fs::read(&path).unwrap());
        assert_eq!(frames, sample());
        assert!(err.is_none());
        assert_eq!(
            FramesFile::create(&path, &header()).err().unwrap().kind(),
            ErrorKind::AlreadyExists
        );
    }

    fn kind() -> impl Strategy<Value = FrameKind> {
        prop_oneof![
            Just(FrameKind::Text),
            Just(FrameKind::Binary),
            Just(FrameKind::Snapshot),
            Just(FrameKind::Connected),
            Just(FrameKind::Disconnected),
        ]
    }

    fn frames() -> impl Strategy<Value = Vec<OwnedFrame>> {
        proptest::collection::vec(
            (
                kind(),
                any::<i64>(),
                any::<u64>(),
                proptest::collection::vec(any::<u8>(), 0..64),
            )
                .prop_map(|(kind, recv_ts, aux, payload)| OwnedFrame {
                    kind,
                    recv_ts,
                    aux,
                    payload,
                }),
            0..20,
        )
    }

    proptest! {
        #[test]
        fn random_roundtrip(fs in frames()) {
            let (got, err) = decode(&encode(&fs));
            prop_assert_eq!(got, fs);
            prop_assert!(err.is_none());
        }

        // A damaged file never yields altered data: what is returned is a prefix of the
        // original records, and the damage is reported.
        #[test]
        fn flipped_bit_is_never_silent(fs in frames(), pos in any::<prop::sample::Index>(), bit in 0u8..8) {
            prop_assume!(!fs.is_empty());
            let mut bytes = encode(&fs);
            let at = header_len() + pos.index(bytes.len() - header_len());
            bytes[at] ^= 1 << bit;
            let (got, err) = decode(&bytes);
            prop_assert!(err.is_some(), "damage at byte {} went unnoticed", at);
            prop_assert!(got.len() < fs.len());
            prop_assert_eq!(&got[..], &fs[..got.len()]);
        }
    }
}
