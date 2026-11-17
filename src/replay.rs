//! Replay: run a recorded frames file through the live pipeline and report what happened.
//!
//! Accepts the binary form and the JSON Lines form (detected by the magic bytes). The
//! frames go through exactly the code the recorder runs: `Pipeline` with the reference
//! book. A file that ends inside a record (a crash while recording) is replayed up to that
//! point and reported as truncated; a damaged record stops the replay and is reported as
//! corrupt.

use std::fmt;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

use crate::book::{OrderBook, RefBook, Side};
use crate::decimal::{Grid, write_atoms};
use crate::event::Level;
use crate::frames::text::{TextError, TextReader};
use crate::frames::{Frame, FramesError, FramesReader, Header, MAGIC};
use crate::pipeline::{Pipeline, PipelineError, QualityEvent, Sink, Stats};
use crate::sync::{State, SyncConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputForm {
    Binary,
    Text,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ending {
    Clean,
    /// The file ends inside the record at this byte offset.
    Truncated {
        offset: u64,
    },
    Corrupt {
        offset: u64,
        reason: &'static str,
    },
}

#[derive(Debug)]
pub enum ReplayError {
    Io(io::Error),
    /// The file header cannot be read.
    Frames(FramesError),
    Text(TextError),
    Pipeline(PipelineError),
}

impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplayError::Io(e) => write!(f, "{e}"),
            ReplayError::Frames(e) => write!(f, "{e}"),
            ReplayError::Text(e) => write!(f, "{e}"),
            ReplayError::Pipeline(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ReplayError {}

impl From<io::Error> for ReplayError {
    fn from(e: io::Error) -> Self {
        ReplayError::Io(e)
    }
}

/// Everything a replay found out.
#[derive(Debug, Clone)]
pub struct Report {
    pub form: InputForm,
    pub header: Header,
    pub stats: Stats,
    pub state: State,
    pub update_id: u64,
    pub best_bid: Option<Level>,
    pub best_ask: Option<Level>,
    /// Snapshot request IDs in the order the pipeline issued them.
    pub requests: Vec<u64>,
    /// Quality events with their receive times.
    pub events: Vec<(i64, QualityEvent)>,
    /// The book the last time it was valid, also when it is not valid at the end.
    pub last_valid: Option<ValidBook>,
    pub ending: Ending,
}

impl Report {
    pub fn event_kinds(&self) -> Vec<&'static str> {
        self.events.iter().map(|(_, e)| e.kind()).collect()
    }
}

#[derive(Default)]
struct Collect {
    requests: Vec<u64>,
    events: Vec<(i64, QualityEvent)>,
    last_valid: Option<ValidBook>,
}

/// The book at the last frame after which it was valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidBook {
    pub recv_ts: i64,
    pub update_id: u64,
    pub best_bid: Option<Level>,
    pub best_ask: Option<Level>,
}

impl Sink for Collect {
    fn after_frame(&mut self, recv_ts: i64, _connected: bool, book: Option<&dyn OrderBook>) {
        if let Some(b) = book {
            self.last_valid = Some(ValidBook {
                recv_ts,
                update_id: b.update_id(),
                best_bid: b.best(Side::Bid),
                best_ask: b.best(Side::Ask),
            });
        }
    }

    fn request_snapshot(&mut self, id: u64) {
        self.requests.push(id);
    }

    fn quality(&mut self, recv_ts: i64, event: &QualityEvent) {
        self.events.push((recv_ts, event.clone()));
    }
}

/// A frames file in either form, read frame by frame.
pub struct Input<R: BufRead> {
    form: InputForm,
    header: Header,
    reader: Reader<R>,
}

enum Reader<R: BufRead> {
    Binary(FramesReader<R>),
    Text(TextReader<R>),
}

impl Input<BufReader<File>> {
    pub fn open(path: &Path) -> Result<Self, ReplayError> {
        Input::new(BufReader::with_capacity(1 << 20, File::open(path)?))
    }
}

impl<R: BufRead> Input<R> {
    /// Detects the form by the magic bytes and reads the header.
    pub fn new(mut input: R) -> Result<Self, ReplayError> {
        if input.fill_buf()?.starts_with(&MAGIC) {
            let reader = FramesReader::new(input).map_err(ReplayError::Frames)?;
            Ok(Self {
                form: InputForm::Binary,
                header: reader.header().clone(),
                reader: Reader::Binary(reader),
            })
        } else {
            let reader = TextReader::new(input).map_err(ReplayError::Text)?;
            Ok(Self {
                form: InputForm::Text,
                header: reader.header().clone(),
                reader: Reader::Text(reader),
            })
        }
    }

    pub fn form(&self) -> InputForm {
        self.form
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Hand every frame to `f`. A binary file that ends inside a record or has a damaged
    /// record stops there and says so in the `Ending`; a bad line in the text form is an
    /// error, since text files are written by hand or by `frames dump`, never by a crash.
    pub fn for_each(&mut self, mut f: impl FnMut(Frame<'_>)) -> Result<Ending, ReplayError> {
        self.for_each_while(|frame| {
            f(frame);
            true
        })
    }

    /// `for_each` that stops, with a clean ending, when `f` returns false.
    pub fn for_each_while(
        &mut self,
        mut f: impl FnMut(Frame<'_>) -> bool,
    ) -> Result<Ending, ReplayError> {
        match &mut self.reader {
            Reader::Binary(r) => loop {
                match r.next_frame() {
                    Ok(Some(frame)) => {
                        if !f(frame) {
                            return Ok(Ending::Clean);
                        }
                    }
                    Ok(None) => return Ok(Ending::Clean),
                    Err(FramesError::Truncated { offset }) => {
                        return Ok(Ending::Truncated { offset });
                    }
                    Err(FramesError::Corrupt { offset, reason }) => {
                        return Ok(Ending::Corrupt { offset, reason });
                    }
                    Err(e) => return Err(ReplayError::Frames(e)),
                }
            },
            Reader::Text(r) => {
                while let Some(frame) = r.next_frame().map_err(ReplayError::Text)? {
                    if !f(frame.as_frame()) {
                        break;
                    }
                }
                Ok(Ending::Clean)
            }
        }
    }
}

pub fn replay_file(path: &Path) -> Result<Report, ReplayError> {
    replay(Input::open(path)?)
}

pub fn replay<R: BufRead>(mut input: Input<R>) -> Result<Report, ReplayError> {
    let mut run = Run::new(input.header())?;
    let ending = input.for_each(|frame| run.pipeline.on_frame(frame, &mut run.sink))?;
    Ok(run.finish(input.form(), input.header().clone(), ending))
}

struct Run {
    pipeline: Pipeline<RefBook>,
    sink: Collect,
}

impl Run {
    fn new(header: &Header) -> Result<Self, ReplayError> {
        let pipeline = Pipeline::new(header, RefBook::new(), SyncConfig::default())
            .map_err(ReplayError::Pipeline)?;
        Ok(Self {
            pipeline,
            sink: Collect::default(),
        })
    }

    fn finish(mut self, form: InputForm, header: Header, ending: Ending) -> Report {
        let stats = self.pipeline.finish().clone();
        let synced = self.pipeline.state() == State::Synced;
        let book = self.pipeline.book();
        Report {
            form,
            header,
            stats,
            state: self.pipeline.state(),
            update_id: if synced { book.update_id() } else { 0 },
            best_bid: if synced { book.best(Side::Bid) } else { None },
            best_ask: if synced { book.best(Side::Ask) } else { None },
            requests: self.sink.requests,
            events: self.sink.events,
            last_valid: self.sink.last_valid,
            ending,
        }
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let h = &self.header;
        let s = &self.stats;
        let span = match (s.first_ts, s.last_ts) {
            (Some(a), Some(b)) => (b - a) as f64 / 1e9,
            _ => 0.0,
        };
        let form = match self.form {
            InputForm::Binary => "binary",
            InputForm::Text => "text",
        };
        let unit = match h.time_unit {
            crate::json::TimeUnit::Milli => "milliseconds",
            crate::json::TimeUnit::Micro => "microseconds",
        };
        let resync_diffs = s.depth - s.depth_applied - s.depth_ignored - s.depth_buffered;
        writeln!(
            f,
            "symbol        {}, price step {}, qty step {}, feed times in {unit}",
            h.symbol, h.price_step, h.qty_step
        )?;
        writeln!(
            f,
            "input         {form} frames file, {} frames over {span:.3} s",
            s.frames
        )?;
        writeln!(
            f,
            "depth         {} diffs: {} applied, {} ignored, {} buffered, {resync_diffs} started a resync",
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
            "valid book    {:.1}% of the time",
            s.synced_share() * 100.0
        )?;
        match h.grids() {
            Ok((price, qty)) if self.state == State::Synced => writeln!(
                f,
                "final book    update {}: bid {}, ask {}",
                self.update_id,
                level(self.best_bid, price, qty),
                level(self.best_ask, price, qty)
            )?,
            Ok((price, qty)) => {
                let stopped = self.events.last().is_some_and(|(_, e)| {
                    matches!(e, QualityEvent::Disconnected { reason, .. } if reason == crate::net::ws::STOPPED)
                });
                let why = if stopped {
                    "recording stopped".to_owned()
                } else {
                    format!("not synchronized ({:?})", self.state)
                };
                match self.last_valid {
                    Some(v) => writeln!(
                        f,
                        "final book    {why}; last valid at update {}: bid {}, ask {}",
                        v.update_id,
                        level(v.best_bid, price, qty),
                        level(v.best_ask, price, qty)
                    )?,
                    None => writeln!(f, "final book    {why}; never valid")?,
                }
            }
            Err(_) => writeln!(f, "final book    not synchronized ({:?})", self.state)?,
        }
        match &self.ending {
            Ending::Clean => write!(f, "end           clean"),
            Ending::Truncated { offset } => write!(
                f,
                "end           file ends inside the record at byte {offset} (normal after a crash)"
            ),
            Ending::Corrupt { offset, reason } => {
                write!(f, "end           DAMAGED record at byte {offset}: {reason}")
            }
        }
    }
}

fn level(l: Option<Level>, price: Grid, qty: Grid) -> String {
    match l {
        Some(l) => format!("{} x {}", decimal(l.price, price), decimal(l.qty, qty)),
        None => "-".to_owned(),
    }
}

/// Steps back to the decimal form Binance uses.
pub fn decimal(steps: i64, grid: Grid) -> String {
    let mut out = String::new();
    match grid.to_atoms(steps) {
        Some(atoms) => write_atoms(atoms, &mut out).expect("writing to a String cannot fail"),
        None => out.push('?'),
    }
    out
}
