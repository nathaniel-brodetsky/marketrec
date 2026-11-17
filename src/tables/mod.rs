//! Parquet tables derived from frames: the data contract read by the research side.
//!
//! The frames files are the primary record. The tables are a function of them: the writer
//! thread runs the same `Pipeline` over the frames it stores and feeds this `Tables` sink,
//! and `marketrec convert` does the same offline, so tables can always be rebuilt from the
//! frames, for example after a schema change.
//!
//! Layout (see `SCHEMA.md`): `<root>/<date>/<table>/part-NNN.parquet` plus
//! `<root>/<date>/manifest.json`. A part covers at most one UTC hour, is written as a hidden
//! `.part-NNN.parquet.tmp` and renamed when closed, and is never changed afterwards.

mod builders;
pub mod convert;
pub mod manifest;
pub mod read;
pub mod schema;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;

use crate::book::{OrderBook, Side};
use crate::event::{BookTicker, DepthDiff, Level, Trade};
use crate::frames::Header;
use crate::pipeline::{QualityEvent, Sink};
use crate::sync::DiffOutcome;
use crate::time::{NS_PER_DAY, NS_PER_HOUR, Utc, hour_start};
use builders::Builders;
use manifest::DayManifest;
use schema::{L20, SCHEMA_VERSION, Table};

/// `book_l20` sampling period: one row per 100 ms of receive time while the book is valid.
pub const L20_PERIOD_NS: i64 = 100_000_000;
/// Rows per Arrow batch handed to the Parquet writer.
const BATCH_ROWS: usize = 8192;

/// Which feed the rows come from: `json` now, `sbe` later.
pub const SOURCE_JSON: &str = "json";

struct Part {
    writer: ArrowWriter<File>,
    tmp: PathBuf,
    path: PathBuf,
}

/// One day of one run: open parts, buffered rows, counters for the manifest.
struct Day {
    header: Header,
    date: String,
    /// Midnight that begins the day, ns.
    start: i64,
    dir: PathBuf,
    hour: i64,
    parts: BTreeMap<Table, Part>,
    builders: Builders,
    manifest: DayManifest,
}

#[derive(Debug)]
pub enum TablesError {
    Io(io::Error),
    Parquet(parquet::errors::ParquetError),
    Arrow(arrow_schema::ArrowError),
    Manifest(String),
}

impl std::fmt::Display for TablesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TablesError::Io(e) => write!(f, "tables: {e}"),
            TablesError::Parquet(e) => write!(f, "tables: parquet: {e}"),
            TablesError::Arrow(e) => write!(f, "tables: arrow: {e}"),
            TablesError::Manifest(e) => write!(f, "tables: manifest: {e}"),
        }
    }
}

impl std::error::Error for TablesError {}

impl From<io::Error> for TablesError {
    fn from(e: io::Error) -> Self {
        TablesError::Io(e)
    }
}

impl From<parquet::errors::ParquetError> for TablesError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        TablesError::Parquet(e)
    }
}

impl From<arrow_schema::ArrowError> for TablesError {
    fn from(e: arrow_schema::ArrowError) -> Self {
        TablesError::Arrow(e)
    }
}

/// Time accounting for the manifest: receive time up to `ts` has been credited, and the
/// state since then is `covered` (a stream connection open) and `synced` (book valid).
#[derive(Debug, Clone, Copy)]
struct Clock {
    ts: i64,
    covered: bool,
    synced: bool,
}

impl Clock {
    /// Credit the time from `self.ts` to `to` to `m`; all of it lies in `m`'s day.
    fn credit(&mut self, m: &mut DayManifest, to: i64) {
        if to <= self.ts {
            return;
        }
        if self.covered || self.synced {
            m.covered_ns += to - self.ts;
        }
        if self.synced {
            m.synced_ns += to - self.ts;
        }
        self.ts = to;
    }
}

/// Builds the tables from pipeline output. Errors are kept and returned by `finish`, since
/// `Sink` methods cannot fail; after the first error nothing more is written.
pub struct Tables {
    root: PathBuf,
    header: Header,
    recorder: String,
    day: Option<Day>,
    error: Option<TablesError>,
    next_l20: i64,
    clock: Option<Clock>,
    written: Vec<PathBuf>,
    levels: Vec<Level>,
    /// Update ID of the book after the last frame, if it was valid.
    last_book: Option<u64>,
    /// Nothing is written for frames received before this time.
    skip_before: i64,
}

impl Tables {
    /// `root` is the symbol directory, for example `data/binance/BTCUSDT`.
    pub fn new(root: &Path, header: Header, recorder: &str) -> Self {
        Self {
            root: root.to_owned(),
            header,
            recorder: recorder.to_owned(),
            day: None,
            error: None,
            next_l20: i64::MIN,
            clock: None,
            written: Vec::new(),
            levels: Vec::with_capacity(L20),
            last_book: None,
            skip_before: i64::MIN,
        }
    }

    /// Write nothing for frames received before `ts`, but follow their state, so the tables
    /// from `ts` on are what a run over all frames writes. `verify` rebuilds one day this
    /// way from the frames of that day and the day before.
    pub fn skip_before(mut self, ts: i64) -> Self {
        self.skip_before = ts;
        self
    }

    /// Close every open part and write the manifest. Returns the files written.
    pub fn finish(mut self) -> Result<Vec<PathBuf>, TablesError> {
        if self.error.is_none() {
            let r = self.close_day();
            self.keep(r);
        }
        match self.error {
            Some(e) => Err(e),
            None => Ok(self.written),
        }
    }

    /// The first error, if writing has failed.
    pub fn error(&self) -> Option<&TablesError> {
        self.error.as_ref()
    }

    fn keep(&mut self, r: Result<(), TablesError>) {
        if let Err(e) = r {
            self.error.get_or_insert(e);
        }
    }

    /// Make sure the open day and parts match `ts`: a new UTC hour closes the parts, a new
    /// day also writes the manifest.
    fn roll(&mut self, ts: i64) -> Result<(), TablesError> {
        let date = Utc::from_ns(ts).date();
        if let Some(day) = self.day.as_mut()
            && day.date != date
        {
            if let Some(clock) = self.clock.as_mut() {
                clock.credit(&mut day.manifest, ts.min(day.start + NS_PER_DAY));
            }
            self.close_day()?;
        }
        if self.day.is_none() {
            let start = ts.div_euclid(NS_PER_DAY) * NS_PER_DAY;
            if let Some(clock) = self.clock.as_mut() {
                // time on days without any frame is not credited anywhere
                clock.ts = clock.ts.max(start);
            }
            let dir = self.root.join(&date);
            let mut manifest = DayManifest::new(&self.header, &date, &self.recorder);
            manifest.runs[0].book_at_start = self.last_book;
            manifest
                .check_file(&dir.join("manifest.json"))
                .map_err(TablesError::Manifest)?;
            let header = self.header.clone();
            self.day = Some(Day {
                header,
                date,
                start,
                dir,
                hour: hour_start(ts),
                parts: BTreeMap::new(),
                builders: Builders::default(),
                manifest,
            });
        }
        let hour = hour_start(ts);
        let day = self.day.as_mut().expect("opened above");
        if day.hour != hour {
            close_parts(day, &mut self.written)?;
            day.hour = hour;
        }
        Ok(())
    }

    fn close_day(&mut self) -> Result<(), TablesError> {
        let Some(mut day) = self.day.take() else {
            return Ok(());
        };
        close_parts(&mut day, &mut self.written)?;
        if day
            .manifest
            .runs
            .last()
            .is_some_and(|r| r.first_recv_ts.is_some())
        {
            fs::create_dir_all(&day.dir)?;
            let path = day.dir.join("manifest.json");
            day.manifest
                .merge_into_file(&path)
                .map_err(TablesError::Manifest)?;
            self.written.push(path);
        }
        Ok(())
    }

    fn with_day(&mut self, ts: i64, f: impl FnOnce(&mut Day)) {
        if self.error.is_some() || ts < self.skip_before {
            return;
        }
        let r = self.roll(ts);
        self.keep(r);
        if self.error.is_some() {
            return;
        }
        let day = self.day.as_mut().expect("rolled");
        day.manifest.touch(ts);
        f(day);
        let r = flush_full(day);
        self.keep(r);
    }
}

pub(crate) fn props(table: Table, header: &Header) -> WriterProperties {
    let meta = vec![
        KeyValue::new(
            "marketrec.schema_version".into(),
            SCHEMA_VERSION.to_string(),
        ),
        KeyValue::new("marketrec.table".into(), table.name().to_owned()),
        KeyValue::new("marketrec.symbol".into(), header.symbol.clone()),
        KeyValue::new("marketrec.price_step".into(), header.price_step.clone()),
        KeyValue::new("marketrec.qty_step".into(), header.qty_step.clone()),
    ];
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("valid level"),
        ))
        .set_max_row_group_row_count(Some(1 << 20))
        .set_key_value_metadata(Some(meta))
        .build()
}

/// The next free `part-NNN.parquet` in `dir`, counting finished and unfinished parts.
fn next_part(dir: &Path) -> io::Result<PathBuf> {
    let mut n = 0;
    if dir.exists() {
        for entry in fs::read_dir(dir)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            if let Some(num) = name
                .trim_start_matches('.')
                .strip_prefix("part-")
                .and_then(|r| r.split('.').next())
                .and_then(|r| r.parse::<u32>().ok())
            {
                n = n.max(num + 1);
            }
        }
    }
    Ok(dir.join(format!("part-{n:03}.parquet")))
}

fn write_batch(
    day: &mut Day,
    table: Table,
    batch: RecordBatch,
    header: &Header,
) -> Result<(), TablesError> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    if !day.parts.contains_key(&table) {
        let dir = day.dir.join(table.name());
        fs::create_dir_all(&dir)?;
        let path = next_part(&dir)?;
        // hidden while unfinished: tools that read a whole directory skip dot files
        let tmp = dir.join(format!(
            ".{}.tmp",
            path.file_name().expect("file").to_string_lossy()
        ));
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        let writer = ArrowWriter::try_new(file, table.schema(), Some(props(table, header)))?;
        day.parts.insert(table, Part { writer, tmp, path });
    }
    day.manifest.add_rows(table, batch.num_rows() as u64);
    day.parts
        .get_mut(&table)
        .expect("inserted")
        .writer
        .write(&batch)?;
    Ok(())
}

fn flush_full(day: &mut Day) -> Result<(), TablesError> {
    for table in Table::ALL {
        if day.builders.len(table) >= BATCH_ROWS {
            let batch = day.builders.finish(table)?;
            let header = day.header.clone();
            write_batch(day, table, batch, &header)?;
        }
    }
    Ok(())
}

fn close_parts(day: &mut Day, written: &mut Vec<PathBuf>) -> Result<(), TablesError> {
    let header = day.header.clone();
    for table in Table::ALL {
        let batch = day.builders.finish(table)?;
        write_batch(day, table, batch, &header)?;
    }
    for (table, mut part) in std::mem::take(&mut day.parts) {
        part.writer.finish()?;
        part.writer.inner_mut().sync_all()?;
        drop(part.writer);
        fs::rename(&part.tmp, &part.path)?;
        #[cfg(unix)]
        if let Some(dir) = part.path.parent() {
            File::open(dir)?.sync_all()?;
        }
        let rel = format!(
            "{}/{}",
            table.name(),
            part.path.file_name().expect("file").to_string_lossy()
        );
        day.manifest.add_file(table, rel);
        written.push(part.path);
    }
    Ok(())
}

impl Sink for Tables {
    fn depth(&mut self, recv_ts: i64, diff: &DepthDiff, _outcome: DiffOutcome) {
        self.with_day(recv_ts, |d| d.builders.depth(recv_ts, diff, SOURCE_JSON));
    }

    fn book_ticker(&mut self, recv_ts: i64, t: &BookTicker) {
        self.with_day(recv_ts, |d| d.builders.book_ticker(recv_ts, t, SOURCE_JSON));
    }

    fn trade(&mut self, recv_ts: i64, t: &Trade) {
        self.with_day(recv_ts, |d| d.builders.trade(recv_ts, t, SOURCE_JSON));
    }

    fn quality(&mut self, recv_ts: i64, event: &QualityEvent) {
        self.with_day(recv_ts, |d| {
            d.manifest.count_quality(event.kind());
            let detail = event.to_string();
            let detail = detail
                .strip_prefix(event.kind())
                .unwrap_or(&detail)
                .trim_start();
            d.builders
                .quality(recv_ts, event.kind(), detail, SOURCE_JSON);
        });
    }

    fn after_frame(&mut self, recv_ts: i64, connected: bool, book: Option<&dyn OrderBook>) {
        let valid = book.map(|b| b.update_id());
        let now = Clock {
            ts: recv_ts,
            covered: connected,
            synced: valid.is_some(),
        };
        if self.error.is_some() || recv_ts < self.skip_before {
            // follow the state only; time before `skip_before` is not credited
            self.clock = Some(Clock {
                ts: self.skip_before,
                ..now
            });
            self.last_book = valid;
            return;
        }
        let r = self.roll(recv_ts); // a day opened here starts with the book before this frame
        self.keep(r);
        self.last_book = valid;
        let Some(day) = self.day.as_mut().filter(|_| self.error.is_none()) else {
            return;
        };
        day.manifest.touch(recv_ts);
        match self.clock.as_mut() {
            Some(c) => {
                c.credit(&mut day.manifest, recv_ts);
                *c = Clock {
                    ts: c.ts.max(recv_ts),
                    ..now
                };
            }
            None => self.clock = Some(now),
        }
        let Some(book) = book else { return };
        if recv_ts < self.next_l20 {
            return;
        }
        self.next_l20 = (recv_ts.div_euclid(L20_PERIOD_NS) + 1) * L20_PERIOD_NS;
        let mut levels = std::mem::take(&mut self.levels);
        self.with_day(recv_ts, |d| {
            let mut row = d.builders.l20_row(recv_ts, book.update_id());
            for (i, side) in [Side::Bid, Side::Ask].into_iter().enumerate() {
                book.top(side, L20, &mut levels);
                row.side(i, &levels);
            }
            row.finish(SOURCE_JSON);
        });
        self.levels = levels;
    }
}

/// Hours since the day began, for tests and tools.
pub fn hour_of_day(ts: i64) -> i64 {
    ts.rem_euclid(NS_PER_DAY) / NS_PER_HOUR
}
