//! `marketrec convert`: derive the tables from frames files offline, with exactly the code
//! the live recorder runs, into a directory that holds no tables for the symbol yet.

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::{Tables, TablesError};
use crate::book::RefBook;
use crate::frames::Header;
use crate::pipeline::{Pipeline, Stats};
use crate::replay::{Ending, Input, ReplayError};
use crate::sync::SyncConfig;

#[derive(Debug)]
pub enum ConvertError {
    Io(io::Error),
    NoInput(PathBuf),
    Read(PathBuf, ReplayError),
    /// An input file has another symbol or other steps than the first one.
    Mixed(PathBuf),
    /// The output already holds tables for this symbol.
    Exists(PathBuf),
    /// A file starts before the previous one ends: the inputs are not one recording. The
    /// tables of the files before it are complete and stay in place.
    Overlap {
        path: PathBuf,
        starts: i64,
        previous_ends: i64,
    },
    Tables(TablesError),
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConvertError::Io(e) => write!(f, "{e}"),
            ConvertError::NoInput(p) => write!(f, "no frames files in {}", p.display()),
            ConvertError::Read(p, e) => write!(f, "{}: {e}", p.display()),
            ConvertError::Mixed(p) => write!(
                f,
                "{}: another symbol or other steps than the first file",
                p.display()
            ),
            ConvertError::Exists(p) => write!(
                f,
                "{} already exists; convert into a new directory",
                p.display()
            ),
            ConvertError::Overlap {
                path,
                starts,
                previous_ends,
            } => write!(
                f,
                "{} starts at {starts}, before the previous file ends ({previous_ends}): the inputs are not one recording; tables of the files before it were written",
                path.display()
            ),
            ConvertError::Tables(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ConvertError {}

impl From<io::Error> for ConvertError {
    fn from(e: io::Error) -> Self {
        ConvertError::Io(e)
    }
}

#[derive(Debug)]
pub struct ConvertSummary {
    pub header: Header,
    pub inputs: Vec<(PathBuf, Ending)>,
    pub stats: Stats,
    pub written: Vec<PathBuf>,
}

impl ConvertSummary {
    /// Inputs that did not end cleanly.
    pub fn damaged(&self) -> impl Iterator<Item = &(PathBuf, Ending)> {
        self.inputs
            .iter()
            .filter(|(_, e)| matches!(e, Ending::Corrupt { .. }))
    }
}

impl fmt::Display for ConvertSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = &self.stats;
        writeln!(
            f,
            "converted     {} file{}, {} frames of {}",
            self.inputs.len(),
            if self.inputs.len() == 1 { "" } else { "s" },
            s.frames,
            self.header.symbol
        )?;
        for (path, ending) in &self.inputs {
            match ending {
                Ending::Clean => {}
                Ending::Truncated { offset } => writeln!(
                    f,
                    "              {} ends inside the record at byte {offset}",
                    path.display()
                )?,
                Ending::Corrupt { offset, reason } => writeln!(
                    f,
                    "              {} DAMAGED at byte {offset}: {reason}; the rest of it is skipped",
                    path.display()
                )?,
            }
        }
        writeln!(
            f,
            "rows          {} depth updates, {} best bid and ask updates, {} trades",
            s.depth, s.book_tickers, s.trades
        )?;
        writeln!(
            f,
            "continuity    {} gaps, {} resyncs, {} reconnects; valid book {:.1}% of the time \
             from the first frame to the last",
            s.gaps,
            s.resyncs,
            s.reconnects,
            s.synced_share() * 100.0
        )?;
        write!(f, "written       {} files", self.written.len())
    }
}

/// A frames file, or every `*.frames`, `*.frames.zst` and `*.jsonl` file under a directory in
/// path order,
/// which is time order for the recorder's `<date>/frames/<HH-MM-SS>.frames` layout. A
/// `*.frames.tmp` file is the unfinished file of a recorder that crashed; it is included,
/// since it is the only record of that hour and reads up to the crash.
pub fn input_files(input: &Path) -> io::Result<Vec<PathBuf>> {
    if !input.is_dir() {
        return Ok(vec![input.to_owned()]);
    }
    let mut out = Vec::new();
    let mut stack = vec![input.to_owned()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .extension()
                .is_some_and(|e| e == "frames" || e == "jsonl")
                || path.to_string_lossy().ends_with(".frames.tmp")
                || path.to_string_lossy().ends_with(".frames.zst")
            {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

/// Run every input through one pipeline into tables under `<out>/binance/<symbol>`. One
/// pipeline for all files means a session split across hourly files is one session, as it
/// was live.
pub fn convert(input: &Path, out: &Path, recorder: &str) -> Result<ConvertSummary, ConvertError> {
    let files = input_files(input)?;
    if files.is_empty() {
        return Err(ConvertError::NoInput(input.to_owned()));
    }
    convert_files(files, out, recorder, i64::MIN, i64::MAX)
}

/// `convert` of a list of files in the given order. Nothing is written for frames received
/// before `skip_before`, but they are run through the pipeline, so the tables from then on
/// are what a conversion of all the files gives. Conversion stops after the first frame
/// received at or after `stop_after`, which closes the time before it.
pub fn convert_files(
    files: Vec<PathBuf>,
    out: &Path,
    recorder: &str,
    skip_before: i64,
    stop_after: i64,
) -> Result<ConvertSummary, ConvertError> {
    let Some(first) = files.first() else {
        return Err(ConvertError::NoInput(out.to_owned()));
    };
    let first = first.clone();
    // check every header before writing anything
    let mut header: Option<Header> = None;
    for path in &files {
        let input = Input::open(path).map_err(|e| ConvertError::Read(path.clone(), e))?;
        let h = input.header();
        match &header {
            None => header = Some(h.clone()),
            Some(first)
                if (&first.symbol, &first.price_step, &first.qty_step)
                    != (&h.symbol, &h.price_step, &h.qty_step) =>
            {
                return Err(ConvertError::Mixed(path.clone()));
            }
            Some(_) => {}
        }
    }
    let header = header.expect("at least one input");
    let root = out.join("binance").join(&header.symbol);
    if root.exists() {
        return Err(ConvertError::Exists(root));
    }
    let mut pipeline = Pipeline::new(&header, RefBook::new(), SyncConfig::default())
        .map_err(|e| ConvertError::Read(first, ReplayError::Pipeline(e)))?;
    let mut tables = Tables::new(&root, header.clone(), recorder).skip_before(skip_before);
    let mut inputs = Vec::new();
    let mut last_ts: Option<i64> = None;
    let mut done = false;
    for path in files {
        if done {
            break;
        }
        let read = |e| ConvertError::Read(path.clone(), e);
        let mut input = Input::open(&path).map_err(read)?;
        let mut overlap = None;
        let mut first = true;
        let ending = input
            .for_each_while(|frame| {
                if first {
                    first = false;
                    if let Some(prev) = last_ts.filter(|&prev| frame.recv_ts < prev) {
                        overlap = Some((frame.recv_ts, prev));
                        return false;
                    }
                }
                last_ts = Some(last_ts.map_or(frame.recv_ts, |t| t.max(frame.recv_ts)));
                pipeline.on_frame(frame, &mut tables);
                done = frame.recv_ts >= stop_after;
                !done
            })
            .map_err(read)?;
        if let Some((starts, previous_ends)) = overlap {
            tables.finish().map_err(ConvertError::Tables)?;
            return Err(ConvertError::Overlap {
                path,
                starts,
                previous_ends,
            });
        }
        inputs.push((path, ending));
        if tables.error().is_some() {
            break; // `finish` returns it
        }
    }
    let stats = pipeline.finish().clone();
    let written = tables.finish().map_err(ConvertError::Tables)?;
    Ok(ConvertSummary {
        header,
        inputs,
        stats,
        written,
    })
}
