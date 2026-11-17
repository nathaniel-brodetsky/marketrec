//! `marketrec bench`: how fast the decode path runs on recorded data, stage by stage.
//!
//! The frames of a recorded day (up to a limit) are loaded into memory and run through:
//! - the JSON decoder alone, per kind of message;
//! - the whole pipeline with the reference book and with the ladder book: throughput, the
//!   time of every frame (p50, p99, p99.9, max) and allocations per frame;
//! - the book updates alone: the exact sequence of snapshots and diffs the pipeline applied,
//!   replayed on each book, with the time of every diff, and a check that both books end
//!   in the same state.
//!
//! Numbers are only meaningful from a release build on a quiet machine; the report says so
//! when it runs from a debug build.

use std::fmt;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::alloc_count;
use crate::book::{BookError, LadderBook, OrderBook, RefBook, Side};
use crate::event::{DepthDiff, Event, Level};
use crate::frames::{FrameKind, Header, OwnedFrame};
use crate::json::JsonDecoder;
use crate::pipeline::{Pipeline, Sink};
use crate::replay::{Input, ReplayError};
use crate::sync::SyncConfig;
use crate::tables::convert::input_files;

#[derive(Debug, Clone)]
pub struct BenchOptions {
    /// Frames loaded from the day, at most.
    pub max_frames: usize,
    /// Runs of each measurement; the best one counts.
    pub repeat: usize,
}

impl Default for BenchOptions {
    fn default() -> Self {
        Self {
            max_frames: 2_000_000,
            repeat: 3,
        }
    }
}

#[derive(Debug)]
pub enum BenchError {
    NoFrames(PathBuf),
    Read(PathBuf, ReplayError),
    Mixed(PathBuf),
    BadHeader(String),
}

impl fmt::Display for BenchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BenchError::NoFrames(p) => write!(f, "no frames files in {}", p.display()),
            BenchError::Read(p, e) => write!(f, "{}: {e}", p.display()),
            BenchError::Mixed(p) => write!(f, "{}: another symbol or other steps", p.display()),
            BenchError::BadHeader(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for BenchError {}

/// Time per frame or per update, ns.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Spread {
    pub p50: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
}

impl Spread {
    fn of(mut ns: Vec<u64>) -> Option<Self> {
        if ns.is_empty() {
            return None;
        }
        ns.sort_unstable();
        let at = |q: f64| ns[((q * ns.len() as f64) as usize).min(ns.len() - 1)];
        Some(Self {
            p50: at(0.5),
            p99: at(0.99),
            p999: at(0.999),
            max: ns[ns.len() - 1],
        })
    }
}

#[derive(Debug, Clone)]
pub struct DecodeTime {
    pub kind: &'static str,
    pub messages: usize,
    pub mean_ns: f64,
}

#[derive(Debug, Clone)]
pub struct PipelineTime {
    pub book: &'static str,
    pub frames_per_s: f64,
    pub per_frame: Spread,
    /// `None` when the counting allocator is not installed.
    pub allocations_per_frame: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct BookTime {
    pub book: &'static str,
    pub per_diff: Spread,
}

#[derive(Debug, Clone)]
pub struct BenchReport {
    pub header: Header,
    /// The day directory's name.
    pub date: String,
    pub frames: usize,
    pub files: usize,
    pub span_ns: i64,
    pub decode: Vec<DecodeTime>,
    pub pipelines: Vec<PipelineTime>,
    pub snapshots: usize,
    pub diffs: usize,
    pub levels_per_diff: f64,
    pub books: Vec<BookTime>,
    /// Both books hold the same levels after the replayed updates.
    pub books_agree: bool,
    pub debug_build: bool,
}

struct Null;

impl Sink for Null {}

/// What the pipeline did to its book, to be done again to each book alone.
#[derive(Debug, Clone)]
enum BookOp {
    Clear,
    Snapshot(u64, Vec<Level>, Vec<Level>),
    Apply(DepthDiff),
}

/// A book that notes every change before passing it on.
#[derive(Default)]
struct Recording {
    inner: RefBook,
    ops: Vec<BookOp>,
}

impl OrderBook for Recording {
    fn clear(&mut self) {
        self.ops.push(BookOp::Clear);
        self.inner.clear();
    }

    fn update_id(&self) -> u64 {
        self.inner.update_id()
    }

    fn set_update_id(&mut self, id: u64) {
        self.inner.set_update_id(id);
    }

    fn set(&mut self, side: Side, price: i64, qty: i64) -> Result<(), BookError> {
        self.inner.set(side, price, qty)
    }

    fn best(&self, side: Side) -> Option<Level> {
        self.inner.best(side)
    }

    fn len(&self, side: Side) -> usize {
        self.inner.len(side)
    }

    fn top(&self, side: Side, n: usize, out: &mut Vec<Level>) {
        self.inner.top(side, n, out);
    }

    fn apply(&mut self, diff: &DepthDiff) -> Result<(), BookError> {
        self.ops.push(BookOp::Apply(diff.clone()));
        self.inner.apply(diff)
    }

    fn load_snapshot(
        &mut self,
        update_id: u64,
        bids: &[Level],
        asks: &[Level],
    ) -> Result<(), BookError> {
        self.ops
            .push(BookOp::Snapshot(update_id, bids.to_vec(), asks.to_vec()));
        self.inner.load_snapshot(update_id, bids, asks)
    }
}

fn load(day: &Path, max: usize) -> Result<(Header, Vec<OwnedFrame>, usize), BenchError> {
    let dir = day.join("frames");
    let files = if dir.is_dir() {
        input_files(&dir).map_err(|e| BenchError::Read(dir.clone(), ReplayError::Io(e)))?
    } else {
        Vec::new()
    };
    if files.is_empty() {
        return Err(BenchError::NoFrames(dir));
    }
    let mut header: Option<Header> = None;
    let mut frames = Vec::new();
    let mut used = 0;
    for path in &files {
        if frames.len() >= max {
            break;
        }
        used += 1;
        let mut input = Input::open(path).map_err(|e| BenchError::Read(path.clone(), e))?;
        let h = input.header().clone();
        match &header {
            None => header = Some(h),
            Some(first)
                if (&first.symbol, &first.price_step, &first.qty_step)
                    != (&h.symbol, &h.price_step, &h.qty_step) =>
            {
                return Err(BenchError::Mixed(path.clone()));
            }
            Some(_) => {}
        }
        input
            .for_each_while(|f| {
                frames.push(f.to_owned());
                frames.len() < max
            })
            .map_err(|e| BenchError::Read(path.clone(), e))?;
    }
    Ok((header.expect("one file at least"), frames, used))
}

fn decode_times(
    header: &Header,
    frames: &[OwnedFrame],
    repeat: usize,
) -> Result<Vec<DecodeTime>, BenchError> {
    let (price, qty) = header
        .grids()
        .map_err(|e| BenchError::BadHeader(e.to_string()))?;
    let mut decoder = JsonDecoder::new(&header.symbol, price, qty, header.time_unit);
    let mut classes: [(&'static str, Vec<&[u8]>); 3] = [
        ("depth", Vec::new()),
        ("book ticker", Vec::new()),
        ("trade", Vec::new()),
    ];
    for f in frames.iter().filter(|f| f.kind == FrameKind::Text) {
        let class = match decoder.decode(&f.payload) {
            Ok(Event::Depth(_)) => 0,
            Ok(Event::BookTicker(_)) => 1,
            Ok(Event::Trade(_)) => 2,
            _ => continue,
        };
        classes[class].1.push(&f.payload);
    }
    let mut out = Vec::new();
    for (kind, msgs) in classes {
        if msgs.is_empty() {
            continue;
        }
        let mut best = f64::INFINITY;
        for _ in 0..repeat.max(1) {
            let t = Instant::now();
            for m in &msgs {
                let _ = black_box(decoder.decode(black_box(m)));
            }
            best = best.min(t.elapsed().as_nanos() as f64 / msgs.len() as f64);
        }
        out.push(DecodeTime {
            kind,
            messages: msgs.len(),
            mean_ns: best,
        });
    }
    Ok(out)
}

fn pipeline_time<B: OrderBook>(
    name: &'static str,
    header: &Header,
    frames: &[OwnedFrame],
    repeat: usize,
    book: impl Fn() -> B,
) -> Result<PipelineTime, BenchError> {
    let new = || {
        Pipeline::new(header, book(), SyncConfig::default())
            .map_err(|e| BenchError::BadHeader(e.to_string()))
    };
    let mut best = f64::INFINITY;
    for _ in 0..repeat.max(1) {
        let mut p = new()?;
        let t = Instant::now();
        for f in frames {
            p.on_frame(black_box(f.as_frame()), &mut Null);
        }
        best = best.min(t.elapsed().as_secs_f64());
    }
    // once more, timing every frame and counting allocations after a warm-up tenth
    let mut p = new()?;
    let mut times = Vec::with_capacity(frames.len());
    let warm = frames.len() / 10;
    let mut allocs_before = 0;
    for (i, f) in frames.iter().enumerate() {
        if i == warm {
            allocs_before = alloc_count::this_thread();
        }
        let t = Instant::now();
        p.on_frame(f.as_frame(), &mut Null);
        times.push(t.elapsed().as_nanos() as u64);
    }
    let measured = (frames.len() - warm).max(1) as f64;
    let allocations_per_frame = alloc_count::active()
        .then(|| (alloc_count::this_thread() - allocs_before) as f64 / measured);
    Ok(PipelineTime {
        book: name,
        frames_per_s: frames.len() as f64 / best.max(1e-12),
        per_frame: Spread::of(times).unwrap_or(Spread {
            p50: 0,
            p99: 0,
            p999: 0,
            max: 0,
        }),
        allocations_per_frame,
    })
}

/// The times of every diff, and the bids and asks the book ends with.
type OpsRun = (Vec<u64>, Vec<Level>, Vec<Level>);

/// Replay the book operations on `book`, timing every diff.
fn replay_ops(ops: &[BookOp], book: &mut impl OrderBook) -> OpsRun {
    let mut times = Vec::new();
    for op in ops {
        match op {
            BookOp::Clear => book.clear(),
            BookOp::Snapshot(id, b, a) => {
                let _ = book.load_snapshot(*id, b, a);
            }
            BookOp::Apply(d) => {
                let t = Instant::now();
                let _ = black_box(book.apply(black_box(d)));
                times.push(t.elapsed().as_nanos() as u64);
            }
        }
    }
    let (mut b, mut a) = (Vec::new(), Vec::new());
    book.top(Side::Bid, usize::MAX, &mut b);
    book.top(Side::Ask, usize::MAX, &mut a);
    (times, b, a)
}

pub fn bench_day(day: &Path, opts: &BenchOptions) -> Result<BenchReport, BenchError> {
    let (header, frames, files) = load(day, opts.max_frames)?;
    let span_ns = match (frames.first(), frames.last()) {
        (Some(a), Some(b)) => b.recv_ts - a.recv_ts,
        _ => 0,
    };
    let decode = decode_times(&header, &frames, opts.repeat)?;
    let pipelines = vec![
        pipeline_time("reference", &header, &frames, opts.repeat, RefBook::new)?,
        pipeline_time("ladder", &header, &frames, opts.repeat, LadderBook::new)?,
    ];

    let mut rec = Pipeline::new(&header, Recording::default(), SyncConfig::default())
        .map_err(|e| BenchError::BadHeader(e.to_string()))?;
    for f in &frames {
        rec.on_frame(f.as_frame(), &mut Null);
    }
    let ops = &rec.book().ops;
    let snapshots = ops
        .iter()
        .filter(|o| matches!(o, BookOp::Snapshot(..)))
        .count();
    let (diffs, levels) = ops.iter().fold((0usize, 0usize), |(n, l), o| match o {
        BookOp::Apply(d) => (n + 1, l + d.bids.len() + d.asks.len()),
        _ => (n, l),
    });
    let mut books = Vec::new();
    let mut best: [Option<OpsRun>; 2] = [None, None];
    for _ in 0..opts.repeat.max(1) {
        for (i, run) in [
            replay_ops(ops, &mut RefBook::new()),
            replay_ops(ops, &mut LadderBook::new()),
        ]
        .into_iter()
        .enumerate()
        {
            let total = |t: &Vec<u64>| t.iter().sum::<u64>();
            if best[i].as_ref().is_none_or(|b| total(&run.0) < total(&b.0)) {
                best[i] = Some(run);
            }
        }
    }
    let [Some(reference), Some(ladder)] = best else {
        unreachable!("at least one run");
    };
    let books_agree = (&reference.1, &reference.2) == (&ladder.1, &ladder.2);
    for (name, run) in [("reference", reference), ("ladder", ladder)] {
        if let Some(per_diff) = Spread::of(run.0) {
            books.push(BookTime {
                book: name,
                per_diff,
            });
        }
    }
    Ok(BenchReport {
        date: day
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
        header,
        frames: frames.len(),
        files,
        span_ns,
        decode,
        pipelines,
        snapshots,
        diffs,
        levels_per_diff: if diffs > 0 {
            levels as f64 / diffs as f64
        } else {
            0.0
        },
        books,
        books_agree,
        debug_build: cfg!(debug_assertions),
    })
}

/// `0.41 µs` below a millisecond, `3.4 ms` above.
fn us(ns: f64) -> String {
    if ns < 1_000_000.0 {
        format!("{:.2} µs", ns / 1000.0)
    } else {
        format!("{:.1} ms", ns / 1e6)
    }
}

impl fmt::Display for Spread {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "p50 {}, p99 {}, p99.9 {}, max {}",
            us(self.p50 as f64),
            us(self.p99 as f64),
            us(self.p999 as f64),
            us(self.max as f64)
        )
    }
}

impl fmt::Display for BenchReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let span = self.span_ns as f64 / 1e9;
        writeln!(
            f,
            "input         {} {}: {} frames from {} file{} spanning {}",
            self.header.symbol,
            self.date,
            self.frames,
            self.files,
            if self.files == 1 { "" } else { "s" },
            if span >= 3600.0 {
                format!("{:.1} h", span / 3600.0)
            } else {
                format!("{span:.0} s")
            }
        )?;
        let decode: Vec<String> = self
            .decode
            .iter()
            .map(|d| format!("{} {} x {}", d.kind, d.messages, us(d.mean_ns)))
            .collect();
        writeln!(f, "decode        {} (mean per message)", decode.join(", "))?;
        for (i, p) in self.pipelines.iter().enumerate() {
            let allocs = p
                .allocations_per_frame
                .map_or("allocations not counted".into(), |a| {
                    format!("{a:.3} allocations per frame")
                });
            writeln!(
                f,
                "{}{:<10} {:>9.0} frames/s; per frame {}; {allocs}",
                if i == 0 {
                    "pipeline      "
                } else {
                    "              "
                },
                p.book,
                p.frames_per_s,
                p.per_frame
            )?;
        }
        write!(
            f,
            "book updates  {} diffs of {:.1} levels after {} snapshot{}; ",
            self.diffs,
            self.levels_per_diff,
            self.snapshots,
            if self.snapshots == 1 { "" } else { "s" }
        )?;
        let books: Vec<String> = self
            .books
            .iter()
            .map(|b| format!("{} {}", b.book, b.per_diff))
            .collect();
        writeln!(f, "{}", books.join("; "))?;
        writeln!(
            f,
            "              the books end {}",
            if self.books_agree {
                "identical"
            } else {
                "DIFFERENT"
            }
        )?;
        if self.debug_build {
            write!(
                f,
                "note          a debug build: times are not meaningful, use cargo run --release"
            )?;
        } else {
            write!(
                f,
                "note          one machine, one run; times include about 20 ns of clock reading per frame"
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::writer::Rotator;
    use crate::testkit::{DAY0, MS, SEC, Session, header};

    /// A recorded day with varied lags, probes, a gap and a reconnect.
    fn session() -> Session {
        let mut s = Session::new(41);
        s.connect(DAY0);
        s.run(DAY0 + MS, DAY0 + 20 * SEC, 10 * MS);
        for k in 1..60 {
            s.probe(DAY0 + k * SEC + 3 * MS, (100 + k) * MS);
        }
        s.lag = 1700 * MS;
        s.run(DAY0 + 20 * SEC, DAY0 + 30 * SEC, 10 * MS);
        s.lag = 140 * MS;
        s.run(DAY0 + 30 * SEC, DAY0 + 40 * SEC, 10 * MS);
        s.lose(1);
        s.run(DAY0 + 40 * SEC, DAY0 + 60 * SEC, 10 * MS);
        s.frames.sort_by_key(|f| f.recv_ts);
        s
    }

    /// Decoding and the pipeline allocate nothing per frame once their buffers have grown,
    /// whatever the messages, their lags or the round trips: with the ladder book a frame
    /// costs no allocation at all. (The reference book allocates a tree node for each new
    /// price level.)
    #[test]
    fn with_the_ladder_book_a_frame_allocates_nothing() {
        let mut s = Session::new(42);
        s.connect(DAY0);
        // warm-up: the book, the buffers and both histograms get their memory
        s.run(DAY0 + MS, DAY0 + 5 * SEC, 10 * MS);
        s.probe(DAY0 + 5 * SEC - 3 * MS, 100 * MS);
        let warm = s.frames.len();
        for (k, lag) in [0, 300, 1700, 40, 2600, 140, 900].into_iter().enumerate() {
            let t = DAY0 + (5 + 5 * k as i64) * SEC;
            s.lag = lag * MS;
            s.run(t, t + 5 * SEC, 10 * MS);
            s.probe(t + 7 * MS, (50 + 30 * k as i64) * MS);
        }
        let mut p = Pipeline::new(&header(), LadderBook::new(), SyncConfig::default()).unwrap();
        for f in &s.frames[..warm] {
            p.on_frame(f.as_frame(), &mut Null);
        }
        assert!(alloc_count::active(), "the unit tests count allocations");
        let before = alloc_count::this_thread();
        for f in &s.frames[warm..] {
            p.on_frame(f.as_frame(), &mut Null);
        }
        let n = alloc_count::this_thread() - before;
        assert_eq!(n, 0, "allocations in {} frames", s.frames.len() - warm);
        let st = p.stats();
        assert!(st.late_intervals >= 2 && st.rtt.count() == 8 && st.trades > 1000);
    }

    #[test]
    fn bench_reports_every_stage_on_a_recorded_day() {
        let s = session();
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("binance/BTCUSDT");
        let mut rot = Rotator::new(&root, header());
        for f in &s.frames {
            rot.write(f).unwrap();
        }
        rot.finish().unwrap();
        let day = root.join("2026-11-21");
        let r = bench_day(
            &day,
            &BenchOptions {
                max_frames: 1_000_000,
                repeat: 1,
            },
        )
        .unwrap();
        assert_eq!(r.frames, s.frames.len());
        assert_eq!(r.decode.len(), 3);
        assert_eq!(
            r.decode.iter().map(|d| d.messages).sum::<usize>(),
            s.frames
                .iter()
                .filter(|f| f.kind == FrameKind::Text)
                .count()
        );
        assert_eq!(r.pipelines.len(), 2);
        assert!(r.pipelines.iter().all(|p| p.frames_per_s > 0.0));
        let ladder = &r.pipelines[1];
        assert!(ladder.allocations_per_frame.unwrap() < 0.05, "{ladder:?}");
        assert_eq!(r.snapshots, 2, "the start and the resync after the gap");
        assert!(r.diffs > 4000);
        assert!(r.books_agree);
        assert_eq!(r.books.len(), 2);
        let text = r.to_string();
        assert!(text.contains("the books end identical"), "{text}");
        assert!(text.contains("pipeline      reference"), "{text}");

        // a limit takes the first frames only
        let r = bench_day(
            &day,
            &BenchOptions {
                max_frames: 1000,
                repeat: 1,
            },
        )
        .unwrap();
        assert_eq!(r.frames, 1000);
    }

    #[test]
    fn a_day_without_frames_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = bench_day(dir.path(), &BenchOptions::default()).unwrap_err();
        assert!(err.to_string().contains("no frames files"), "{err}");
    }
}
