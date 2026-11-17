//! `marketrec verify`: check one recorded day against its own manifest, against the rules
//! of the update stream, and against the frames it was derived from.
//!
//! Checks, each `PASS`, `WARN` or `FAIL`:
//! - `manifest`: readable, this contract version, names the directory it is in.
//! - `parts`: every listed part exists, has the table's schema and the manifest's symbol
//!   and steps; parts the manifest does not list, and unfinished parts, are reported.
//! - `rows`: rows per table equal the manifest's counts.
//! - `hours`: every part holds one UTC hour of this day, receive times in order.
//! - `continuity`: an independent re-check of the update stream with the tables alone.
//!   While the quality journal says the book is valid, every update must continue the
//!   previous one, and every `book_l20` row must be well formed and carry the update ID the
//!   stream has reached.
//! - `freshness`: how late the depth updates arrived after their exchange time, and the
//!   intervals in which the data was more than a second behind (a valid but stale book).
//! - `rebuild`: the tables built again from the frames (this day's and the day before's)
//!   equal the stored ones row for row.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::replay::Ending;
use crate::tables::convert::{convert_files, input_files};
use crate::tables::manifest::DayManifest;
use crate::tables::read::{TableStream, first_difference, parts, read_manifest};
use crate::tables::schema::{L20, SCHEMA_VERSION, Table};
use crate::time::{NS_PER_DAY, Utc, day_start, hour_start};
use crate::timing::{MsHistogram, duration_ms};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Outcome {
    Pass,
    Warn,
    Fail,
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Outcome::Pass => "PASS",
            Outcome::Warn => "WARN",
            Outcome::Fail => "FAIL",
        })
    }
}

#[derive(Debug, Clone)]
pub struct Check {
    pub name: &'static str,
    pub outcome: Outcome,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct VerifyReport {
    pub day: PathBuf,
    pub checks: Vec<Check>,
}

impl VerifyReport {
    /// The worst outcome of all checks.
    pub fn outcome(&self) -> Outcome {
        self.checks
            .iter()
            .map(|c| c.outcome)
            .max()
            .unwrap_or(Outcome::Pass)
    }

    pub fn check(&self, name: &str) -> Option<&Check> {
        self.checks.iter().find(|c| c.name == name)
    }

    fn push(&mut self, name: &'static str, outcome: Outcome, detail: impl Into<String>) {
        self.checks.push(Check {
            name,
            outcome,
            detail: detail.into(),
        });
    }
}

impl fmt::Display for VerifyReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "day         {}", self.day.display())?;
        for c in &self.checks {
            let mut lines = c.detail.lines();
            writeln!(
                f,
                "{} {:<11}{}",
                c.outcome,
                c.name,
                lines.next().unwrap_or("")
            )?;
            for line in lines {
                writeln!(f, "{:16}{line}", "")?;
            }
        }
        write!(f, "result      {}", self.outcome())
    }
}

#[derive(Debug, Clone, Default)]
pub struct VerifyOptions {
    /// Skip the rebuild from frames, the slow part.
    pub quick: bool,
    /// Where to build the tables again; a new directory under the system's temporary
    /// directory if `None`. It is removed afterwards.
    pub scratch: Option<PathBuf>,
}

/// What the parts check found, for the checks after it.
#[derive(Default)]
struct PartsFound {
    /// Unlisted finished parts, unfinished parts: traces of a run that did not end cleanly.
    crash_traces: Vec<String>,
    /// Rows per table counted from the files' metadata.
    rows: Vec<(Table, u64)>,
}

pub fn verify_day(day: &Path, opts: &VerifyOptions) -> VerifyReport {
    let mut report = VerifyReport {
        day: day.to_owned(),
        checks: Vec::new(),
    };
    let Some(m) = check_manifest(day, &mut report) else {
        return report;
    };
    let found = check_parts(day, &m, &mut report);
    check_rows(&m, &found, &mut report);
    check_hours(day, &m, &mut report);
    check_continuity(day, &m, &mut report);
    check_freshness(day, &m, &mut report);
    if !opts.quick {
        check_rebuild(day, &m, &found, opts, &mut report);
    }
    report
}

fn listed(day: &Path, m: &DayManifest, table: Table) -> Vec<PathBuf> {
    m.files
        .get(table.name())
        .map(|v| v.iter().map(|rel| day.join(rel)).collect())
        .unwrap_or_default()
}

fn dir_name(p: Option<&Path>) -> String {
    p.and_then(|p| p.file_name())
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn check_manifest(day: &Path, report: &mut VerifyReport) -> Option<DayManifest> {
    let m = match read_manifest(day) {
        Ok(m) => m,
        Err(e) => {
            report.push("manifest", Outcome::Fail, format!("cannot be read: {e}"));
            return None;
        }
    };
    let mut problems = Vec::new();
    if m.schema_version != SCHEMA_VERSION {
        problems.push(format!(
            "schema version {}, this build reads {SCHEMA_VERSION}",
            m.schema_version
        ));
    }
    let name = dir_name(Some(day));
    if name != m.date {
        problems.push(format!("the directory {name} holds the date {}", m.date));
    }
    if day_start(&m.date).is_none() {
        problems.push(format!("{} is not a date", m.date));
    }
    let symbol = dir_name(day.parent());
    if symbol != m.symbol {
        problems.push(format!(
            "the directory {symbol} holds the symbol {}",
            m.symbol
        ));
    }
    if m.runs.is_empty() {
        problems.push("no runs".into());
    }
    if problems.is_empty() {
        report.push(
            "manifest",
            Outcome::Pass,
            format!(
                "{} {}, schema {}, steps {} and {}, {} run{}, valid book {:.1}% of connected time",
                m.symbol,
                m.date,
                m.schema_version,
                m.price_step,
                m.qty_step,
                m.runs.len(),
                if m.runs.len() == 1 { "" } else { "s" },
                m.synced_share * 100.0
            ),
        );
        Some(m)
    } else {
        report.push("manifest", Outcome::Fail, problems.join("\n"));
        None
    }
}

fn check_parts(day: &Path, m: &DayManifest, report: &mut VerifyReport) -> PartsFound {
    let mut found = PartsFound::default();
    let (mut problems, mut count) = (Vec::new(), 0);
    for table in Table::ALL {
        let listed = listed(day, m, table);
        let on_disk = parts(day, table).unwrap_or_default();
        for p in on_disk.iter().filter(|p| !listed.contains(p)) {
            found.crash_traces.push(format!(
                "{} is not in the manifest",
                p.strip_prefix(day).unwrap_or(p).display()
            ));
        }
        if let Ok(entries) = fs::read_dir(day.join(table.name())) {
            for e in entries.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                if name.starts_with('.') && name.ends_with(".tmp") {
                    found
                        .crash_traces
                        .push(format!("{}/{name} was never finished", table.name()));
                }
            }
        }
        let mut rows = 0;
        for path in &listed {
            count += 1;
            match part_info(path, table, m) {
                Ok(n) => rows += n,
                Err(e) => problems.push(e),
            }
        }
        found.rows.push((table, rows));
    }
    if !problems.is_empty() {
        report.push("parts", Outcome::Fail, problems.join("\n"));
    } else if !found.crash_traces.is_empty() {
        let mut detail = format!(
            "{count} listed parts are sound, but a run did not end cleanly (a crash?): its tables are incomplete; `marketrec convert` rebuilds them from the frames"
        );
        for t in &found.crash_traces {
            detail.push('\n');
            detail.push_str(t);
        }
        report.push("parts", Outcome::Warn, detail);
    } else {
        report.push(
            "parts",
            Outcome::Pass,
            format!(
                "{count} parts, all listed in the manifest, readable, with the schema, symbol and steps it names"
            ),
        );
    }
    found
}

/// Rows of one part from its footer, after checking its schema and metadata.
fn part_info(path: &Path, table: Table, m: &DayManifest) -> Result<u64, String> {
    let shown = path.display();
    let file = fs::File::open(path).map_err(|e| format!("{shown}: {e}"))?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| format!("{shown}: {e}"))?;
    let fields = |s: &arrow_schema::Schema| s.fields().iter().cloned().collect::<Vec<_>>();
    if fields(builder.schema()) != fields(&table.schema()) {
        return Err(format!(
            "{shown}: columns differ from the {} schema",
            table.name()
        ));
    }
    let meta = builder.metadata().file_metadata();
    let kv = |key: &str| {
        meta.key_value_metadata()
            .and_then(|kv| kv.iter().find(|k| k.key == key))
            .and_then(|k| k.value.clone())
            .unwrap_or_default()
    };
    let expected = [
        ("marketrec.schema_version", m.schema_version.to_string()),
        ("marketrec.table", table.name().to_owned()),
        ("marketrec.symbol", m.symbol.clone()),
        ("marketrec.price_step", m.price_step.clone()),
        ("marketrec.qty_step", m.qty_step.clone()),
    ];
    for (key, want) in expected {
        let got = kv(key);
        if got != want {
            return Err(format!(
                "{shown}: {key} is {got:?}, the manifest says {want:?}"
            ));
        }
    }
    Ok(meta.num_rows() as u64)
}

fn check_rows(m: &DayManifest, found: &PartsFound, report: &mut VerifyReport) {
    let mut problems = Vec::new();
    let mut counts = Vec::new();
    for &(table, rows) in &found.rows {
        let want = m.rows.get(table.name()).copied().unwrap_or(0);
        if rows != want {
            problems.push(format!(
                "{}: {rows} rows in the files, {want} in the manifest",
                table.name()
            ));
        }
        counts.push(format!("{} {rows}", table.name()));
    }
    if problems.is_empty() {
        report.push("rows", Outcome::Pass, counts.join(", "));
    } else {
        report.push("rows", Outcome::Fail, problems.join("\n"));
    }
}

fn i64_col(b: &RecordBatch, i: usize) -> &[i64] {
    b.column(i).as_primitive::<Int64Type>().values()
}

fn check_hours(day: &Path, m: &DayManifest, report: &mut VerifyReport) {
    let Some(start) = day_start(&m.date) else {
        return;
    };
    let (mut problems, mut backwards) = (Vec::new(), 0u64);
    for table in Table::ALL {
        for path in listed(day, m, table) {
            let mut hour: Option<i64> = None;
            let mut prev = i64::MIN;
            for batch in TableStream::new(table, vec![path.clone()], Some(vec![0])) {
                let Ok(batch) = batch else {
                    break; // reported by `parts`
                };
                for &ts in i64_col(&batch, 0) {
                    let h = hour_start(ts);
                    if !(start..start + NS_PER_DAY).contains(&ts) {
                        problems.push(format!(
                            "{}: a row at {ts} is not on this day",
                            path.display()
                        ));
                        break;
                    }
                    if *hour.get_or_insert(h) != h {
                        problems.push(format!("{}: rows from more than one hour", path.display()));
                        break;
                    }
                    if ts < prev {
                        backwards += 1;
                    }
                    prev = ts;
                }
            }
        }
    }
    problems.dedup();
    if !problems.is_empty() {
        report.push("hours", Outcome::Fail, problems.join("\n"));
    } else if backwards > 0 {
        report.push(
            "hours",
            Outcome::Warn,
            format!("every part holds one hour, but receive times go back {backwards} times: the clock was stepped back"),
        );
    } else {
        report.push(
            "hours",
            Outcome::Pass,
            "every part holds one hour of this day, receive times in order",
        );
    }
}

/// One step of the merged time line of a day.
#[derive(Clone, Copy)]
enum Step {
    RunStart(usize),
    Quality(usize),
    Update { first: i64, last: i64 },
    Book { update_id: i64, sound: bool },
}

impl Step {
    /// Order at equal receive times: a run starts first; quality events are caused by the
    /// frame at their time, so they come before the updates of that time; the book is
    /// sampled after the frame.
    fn rank(&self) -> u8 {
        match self {
            Step::RunStart(_) => 0,
            Step::Quality(_) => 1,
            Step::Update { .. } => 2,
            Step::Book { .. } => 3,
        }
    }
}

/// Bids strictly falling, asks strictly rising, levels positive, zeros only after the last
/// level, and the book not crossed.
fn sound_book(bid_px: &[i64], bid_qty: &[i64], ask_px: &[i64], ask_qty: &[i64]) -> bool {
    fn side(px: &[i64], qty: &[i64], better: impl Fn(i64, i64) -> bool) -> Option<Option<i64>> {
        let n = px.iter().position(|&p| p == 0).unwrap_or(px.len());
        let levels_ok = px[..n].iter().zip(&qty[..n]).all(|(&p, &q)| p > 0 && q > 0);
        let rest_zero = px[n..].iter().chain(&qty[n..]).all(|&v| v == 0);
        let ordered = px[..n].windows(2).all(|w| better(w[0], w[1]));
        (levels_ok && rest_zero && ordered).then(|| px.first().copied().filter(|&p| p > 0))
    }
    let (Some(bid), Some(ask)) = (
        side(bid_px, bid_qty, |a, b| a > b),
        side(ask_px, ask_qty, |a, b| a < b),
    ) else {
        return false;
    };
    match (bid, ask) {
        (Some(b), Some(a)) => b < a,
        _ => true,
    }
}

fn update_id(detail: &str) -> Option<i64> {
    detail
        .split_whitespace()
        .find_map(|kv| kv.strip_prefix("update_id="))
        .and_then(|v| v.parse().ok())
}

fn check_continuity(day: &Path, m: &DayManifest, report: &mut VerifyReport) {
    match continuity(day, m) {
        Ok((outcome, detail)) => report.push("continuity", outcome, detail),
        Err(e) => report.push(
            "continuity",
            Outcome::Fail,
            format!("cannot be checked: {e}"),
        ),
    }
}

fn continuity(day: &Path, m: &DayManifest) -> Result<(Outcome, String), String> {
    let err = |e: crate::tables::TablesError| e.to_string();
    let mut line: Vec<(i64, Step)> = Vec::new();
    let mut runs: Vec<_> = m.runs.iter().collect();
    runs.sort_by_key(|r| r.first_recv_ts);
    for (i, r) in runs.iter().enumerate() {
        if let Some(ts) = r.first_recv_ts {
            line.push((ts, Step::RunStart(i)));
        }
    }

    let mut quality: Vec<(String, String)> = Vec::new();
    for batch in TableStream::new(Table::Quality, listed(day, m, Table::Quality), None) {
        let batch = batch.map_err(err)?;
        let (kind, detail) = (
            batch.column(1).as_string::<i32>(),
            batch.column(2).as_string::<i32>(),
        );
        for (i, &ts) in i64_col(&batch, 0).iter().enumerate() {
            line.push((ts, Step::Quality(quality.len())));
            quality.push((kind.value(i).to_owned(), detail.value(i).to_owned()));
        }
    }

    // one step per update message: its rows share the receive time and both IDs
    let mut prev: Option<(i64, i64, i64)> = None;
    for batch in TableStream::new(
        Table::DepthDiff,
        listed(day, m, Table::DepthDiff),
        Some(vec![0, 2, 3]),
    ) {
        let batch = batch.map_err(err)?;
        let (ts, first, last) = (i64_col(&batch, 0), i64_col(&batch, 1), i64_col(&batch, 2));
        for i in 0..batch.num_rows() {
            let key = (ts[i], first[i], last[i]);
            if prev != Some(key) {
                line.push((
                    ts[i],
                    Step::Update {
                        first: first[i],
                        last: last[i],
                    },
                ));
                prev = Some(key);
            }
        }
    }

    let mut books = 0u64;
    for batch in TableStream::new(Table::BookL20, listed(day, m, Table::BookL20), None) {
        let batch = batch.map_err(err)?;
        let list = |c: usize| batch.column(c).as_fixed_size_list();
        let (bp, bq, ap, aq) = (list(2), list(3), list(4), list(5));
        let ids = i64_col(&batch, 1);
        for (i, &ts) in i64_col(&batch, 0).iter().enumerate() {
            let vals = |l: &arrow_array::FixedSizeListArray| {
                l.value(i).as_primitive::<Int64Type>().values().to_vec()
            };
            let (a, b, c, d) = (vals(bp), vals(bq), vals(ap), vals(aq));
            let sound = a.len() == L20 && sound_book(&a, &b, &c, &d);
            line.push((
                ts,
                Step::Book {
                    update_id: ids[i],
                    sound,
                },
            ));
            books += 1;
        }
    }
    line.sort_by_key(|(ts, step)| (*ts, step.rank())); // stable: file order within a rank

    let (mut valid, mut local) = (false, 0i64);
    let (mut applied, mut old, mut intervals, mut gaps) = (0u64, 0u64, 0u64, 0u64);
    let mut missed: Vec<String> = Vec::new();
    let mut outside: Vec<String> = Vec::new();
    let mut wrong_id: Vec<String> = Vec::new();
    let mut unsound: Vec<String> = Vec::new();
    let mut unreadable: Vec<String> = Vec::new();
    let at = |ts: i64| {
        let t = Utc::from_ns(ts);
        format!(
            "{:02}:{:02}:{:02}.{:09}",
            t.hour,
            t.minute,
            t.second,
            ts.rem_euclid(1_000_000_000)
        )
    };
    for (ts, step) in line {
        match step {
            Step::RunStart(i) => match runs[i].book_at_start {
                Some(id) => {
                    valid = true;
                    local = id as i64;
                    intervals += 1;
                }
                None => valid = false,
            },
            Step::Quality(i) => {
                let (kind, detail) = &quality[i];
                match kind.as_str() {
                    "synced" | "resync" => match update_id(detail) {
                        Some(id) => {
                            valid = true;
                            local = id;
                            intervals += 1;
                        }
                        None => {
                            unreadable.push(format!("{} {kind} {detail:?}", at(ts)));
                            valid = false;
                        }
                    },
                    "gap" => {
                        gaps += 1;
                        valid = false;
                    }
                    "invalid_book" | "reconnect" | "overflow" => valid = false,
                    "disconnected" if detail.contains("reason=\"stopped\"") => valid = false,
                    _ => {}
                }
            }
            Step::Update { first, last } if valid => {
                if last <= local {
                    old += 1;
                } else if first > local + 1 {
                    missed.push(format!(
                        "{}: update {first}-{last} after {local}, no gap recorded",
                        at(ts)
                    ));
                    local = last; // keep checking from here
                } else {
                    applied += 1;
                    local = last;
                }
            }
            Step::Update { .. } => {}
            Step::Book { update_id, sound } => {
                if !sound {
                    unsound.push(format!("{}: update {update_id}", at(ts)));
                }
                if !valid {
                    outside.push(format!("{}: update {update_id}", at(ts)));
                } else if update_id != local {
                    wrong_id.push(format!(
                        "{}: row says {update_id}, the updates reach {local}",
                        at(ts)
                    ));
                }
            }
        }
    }

    let mut problems = Vec::new();
    for (what, list) in [
        (
            "updates that do not continue the book, without a recorded gap",
            &missed,
        ),
        ("book_l20 rows while the book was not valid", &outside),
        ("book_l20 rows at another update than the stream", &wrong_id),
        ("book_l20 rows that are not a sound book", &unsound),
        ("synced events without a readable update ID", &unreadable),
    ] {
        if !list.is_empty() {
            problems.push(format!("{} {what}, first: {}", list.len(), list[0]));
        }
    }
    if problems.is_empty() {
        Ok((
            Outcome::Pass,
            format!(
                "{applied} updates continue the book in {intervals} valid interval{} ({gaps} gap{} recorded, none missed, {old} already applied); {books} book rows agree with the updates",
                if intervals == 1 { "" } else { "s" },
                if gaps == 1 { "" } else { "s" },
            ),
        ))
    } else {
        Ok((Outcome::Fail, problems.join("\n")))
    }
}

/// Late intervals from the quality journal: from `late` to the next `on_time`, or to the
/// end of the connection, or to the end of the day's data.
fn late_intervals(day: &Path, m: &DayManifest) -> Result<Vec<(i64, i64)>, String> {
    let end_of_day = m
        .runs
        .iter()
        .filter_map(|r| r.last_recv_ts)
        .max()
        .unwrap_or(i64::MAX);
    let mut out = Vec::new();
    let mut open: Option<i64> = None;
    for batch in TableStream::new(Table::Quality, listed(day, m, Table::Quality), None) {
        let batch = batch.map_err(|e| e.to_string())?;
        let (kind, detail) = (
            batch.column(1).as_string::<i32>(),
            batch.column(2).as_string::<i32>(),
        );
        for (i, &ts) in i64_col(&batch, 0).iter().enumerate() {
            match kind.value(i) {
                "late" => {
                    open.get_or_insert(ts);
                }
                "on_time" | "reconnect" => out.extend(open.take().map(|s| (s, ts))),
                "disconnected" if detail.value(i).contains("reason=\"stopped\"") => {
                    out.extend(open.take().map(|s| (s, ts)))
                }
                _ => {}
            }
        }
    }
    out.extend(open.map(|s| (s, end_of_day)));
    Ok(out)
}

/// Round trips inside and outside the late intervals.
fn rtt_split(
    day: &Path,
    m: &DayManifest,
    late: &[(i64, i64)],
) -> Result<(MsHistogram, MsHistogram), String> {
    let (mut inside, mut outside) = (MsHistogram::default(), MsHistogram::default());
    for batch in TableStream::new(Table::LinkRtt, listed(day, m, Table::LinkRtt), None) {
        let batch = batch.map_err(|e| e.to_string())?;
        for (&ts, &rtt) in i64_col(&batch, 0).iter().zip(i64_col(&batch, 2)) {
            if late.iter().any(|&(a, b)| (a..=b).contains(&ts)) {
                inside.record(rtt);
            } else {
                outside.record(rtt);
            }
        }
    }
    Ok((inside, outside))
}

/// Why the data was late, from the round trips: a slow link, or delay before it.
fn diagnose(inside: &MsHistogram, outside: &MsHistogram) -> String {
    let Some(in50) = inside.quantile_ms(0.5) else {
        return "no round-trip probe fell inside the late intervals".into();
    };
    let mut line = format!(
        "round trip inside them p50 {} ({} probes)",
        duration_ms(in50 as i64 * 1_000_000),
        inside.count()
    );
    match outside.quantile_ms(0.5) {
        Some(out50) => {
            line += &format!(", elsewhere p50 {}", duration_ms(out50 as i64 * 1_000_000));
            if in50 >= 3 * out50 && in50 >= out50 + 200 {
                line +=
                    ": the connection itself was slow (the network, or the exchange's send buffer)";
            } else {
                line += ": the link answered as usual, so the data was late before it entered the connection, at the exchange";
            }
        }
        None => line += "; no probe outside them to compare with",
    }
    line
}

fn check_freshness(day: &Path, m: &DayManifest, report: &mut VerifyReport) {
    let lag = &m.lag;
    if lag.count() == 0 && lag.below_zero == 0 {
        report.push("freshness", Outcome::Pass, "no depth updates");
        return;
    }
    let mut detail = format!(
        "depth updates arrived {} after their exchange time",
        lag.summary()
    );
    if m.rtt.count() > 0 {
        detail += &format!("; round trip of the link {}", m.rtt.summary());
    }
    let mut outcome = Outcome::Pass;
    let late = m.quality.get("late").copied().unwrap_or(0);
    if late > 0 || m.late_ns > 0 {
        outcome = Outcome::Warn;
        detail += &format!(
            "\nmore than 1 s behind the exchange {late} time{} for {:.1} s in all: the book was correct there but stale; `late` and `on_time` in quality mark the intervals",
            if late == 1 { "" } else { "s" },
            m.late_ns as f64 / 1e9
        );
        match late_intervals(day, m).and_then(|late| rtt_split(day, m, &late)) {
            Ok((inside, outside)) => detail += &format!("\n{}", diagnose(&inside, &outside)),
            Err(e) => detail += &format!("\nround trips not read: {e}"),
        }
    }
    if lag.below_zero > 0 {
        outcome = Outcome::Warn;
        detail += &format!(
            "\n{} updates arrived before their exchange time: this machine's clock is behind, so lags are understated",
            lag.below_zero
        );
    }
    report.push("freshness", outcome, detail);
}

/// The frames that derive one day exactly: the day before's, for the state at midnight;
/// the day's own; and the day after's, whose first frame closes the day's time.
pub(crate) struct DayInputs {
    pub files: Vec<PathBuf>,
    pub n_prev: usize,
    pub n_this: usize,
    start: i64,
}

impl DayInputs {
    /// `None` if the day has no frames.
    pub fn find(day: &Path, start: i64) -> Option<Self> {
        let frames_of = |dir: &Path| -> Vec<PathBuf> {
            if dir.is_dir() {
                input_files(dir).unwrap_or_default()
            } else {
                Vec::new()
            }
        };
        let sibling = |offset: i64| {
            day.parent()
                .map(|p| frames_of(&p.join(Utc::from_ns(start + offset).date()).join("frames")))
                .unwrap_or_default()
        };
        let this = frames_of(&day.join("frames"));
        if this.is_empty() {
            return None;
        }
        let (prev, next) = (sibling(-NS_PER_DAY), sibling(NS_PER_DAY));
        Some(Self {
            n_prev: prev.len(),
            n_this: this.len(),
            files: prev.into_iter().chain(this).chain(next).collect(),
            start,
        })
    }

    /// Derive the day's tables under `out` (as `out/binance/<symbol>/<date>`), writing
    /// nothing for other days.
    pub fn convert(
        &self,
        out: &Path,
        recorder: &str,
    ) -> Result<crate::tables::convert::ConvertSummary, crate::tables::convert::ConvertError> {
        convert_files(
            self.files.clone(),
            out,
            recorder,
            self.start,
            self.start + NS_PER_DAY,
        )
    }
}

/// A directory removed when dropped.
pub(crate) struct Scratch(pub PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn check_rebuild(
    day: &Path,
    m: &DayManifest,
    found: &PartsFound,
    opts: &VerifyOptions,
    report: &mut VerifyReport,
) {
    let Some(start) = day_start(&m.date) else {
        return;
    };
    let Some(inputs) = DayInputs::find(day, start) else {
        report.push(
            "rebuild",
            Outcome::Warn,
            "no frames files in frames/: the tables cannot be checked against their source",
        );
        return;
    };
    // without the day before, the book at midnight is unknown to the rebuild
    let state_unknown = inputs.n_prev == 0 && m.runs.iter().any(|r| r.book_at_start.is_some());

    let base = opts.scratch.clone().unwrap_or_else(std::env::temp_dir);
    let scratch = Scratch(base.join(format!(
        "marketrec-verify-{}-{}",
        std::process::id(),
        crate::net::ws::now_ns()
    )));
    let (n_prev, n_this) = (inputs.n_prev, inputs.n_this);
    let mut crash = found.crash_traces.clone();
    for f in &inputs.files {
        if f.to_string_lossy().ends_with(".tmp") {
            crash.push(format!("{} was never finished", f.display()));
        }
    }
    let summary = match inputs.convert(&scratch.0, "marketrec verify") {
        Ok(s) => s,
        Err(e) => {
            report.push(
                "rebuild",
                Outcome::Fail,
                format!("the frames cannot be converted: {e}"),
            );
            return;
        }
    };
    for (path, ending) in &summary.inputs {
        match ending {
            Ending::Clean => {}
            Ending::Truncated { .. } => {
                crash.push(format!("{} ends inside a record", path.display()))
            }
            Ending::Corrupt { offset, reason } => crash.push(format!(
                "{} is damaged at byte {offset}: {reason}",
                path.display()
            )),
        }
    }
    let rebuilt = scratch.0.join("binance").join(&m.symbol).join(&m.date);

    let tables: &[Table] = if state_unknown {
        &[Table::DepthDiff, Table::BookTicker, Table::Trades]
    } else {
        &Table::ALL
    };
    let mut differ = Vec::new();
    for &table in tables {
        let stored = TableStream::new(table, listed(day, m, table), None);
        let again = TableStream::new(table, parts(&rebuilt, table).unwrap_or_default(), None);
        match first_difference(stored, again) {
            Ok(None) => {}
            Ok(Some(row)) => differ.push(format!("{}: from row {row} on", table.name())),
            Err(e) => differ.push(format!("{}: {e}", table.name())),
        }
    }
    if !state_unknown && differ.is_empty() {
        match read_manifest(&rebuilt) {
            Ok(r) => {
                if (r.covered_ns, r.synced_ns, r.late_ns) != (m.covered_ns, m.synced_ns, m.late_ns)
                {
                    differ.push(format!(
                        "manifest: connected {} ns, valid {} ns, late {} ns; rebuilt {}, {}, {}",
                        m.covered_ns, m.synced_ns, m.late_ns, r.covered_ns, r.synced_ns, r.late_ns
                    ));
                }
                if r.lag != m.lag {
                    differ.push("manifest: the lag histogram differs".into());
                }
                if r.rtt != m.rtt {
                    differ.push("manifest: the round-trip histogram differs".into());
                }
            }
            Err(e) => differ.push(format!("rebuilt manifest: {e}")),
        }
    }

    let older: Vec<String> = m
        .runs
        .iter()
        .filter(|r| r.derivation < crate::tables::DERIVATION)
        .map(|r| format!("{}, derivation {}", r.recorder, r.derivation))
        .collect();
    let files_of = |n: usize| format!("{n} frames file{}", if n == 1 { "" } else { "s" });
    let mut source = format!("the day's {}", files_of(n_this));
    if n_prev > 0 {
        source += &format!(" and {} of the day before", files_of(n_prev));
    }
    if differ.is_empty() {
        if state_unknown {
            report.push(
                "rebuild",
                Outcome::Warn,
                format!("depth_diff, book_ticker and trades rebuilt from {source} are identical; book_l20 and quality not compared: the day continues a run whose frames of the day before are missing"),
            );
        } else {
            report.push(
                "rebuild",
                Outcome::Pass,
                format!("every table rebuilt from {source} is identical, row for row"),
            );
        }
    } else if !crash.is_empty() || !older.is_empty() {
        let why = match (crash.is_empty(), older.is_empty()) {
            (false, true) => "as expected after a crash".to_owned(),
            (true, false) => format!(
                "and the day holds tables derived by older rules ({}), so the difference can come from the code",
                older.join("; ")
            ),
            _ => format!(
                "as expected after a crash and with tables derived by older rules ({})",
                older.join("; ")
            ),
        };
        let mut detail = format!(
            "tables rebuilt from {source} differ, {why}: {}\n`marketrec rebuild --day {}` derives the day again from its frames",
            differ.join("; "),
            day.display()
        );
        for c in crash {
            detail.push('\n');
            detail.push_str(&c);
        }
        report.push("rebuild", Outcome::Warn, detail);
    } else {
        report.push(
            "rebuild",
            Outcome::Fail,
            format!(
                "tables rebuilt from {source} differ:\n{}",
                differ.join("\n")
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array};
    use parquet::arrow::ArrowWriter;

    use super::*;
    use crate::book::RefBook;
    use crate::net::ws::STOPPED;
    use crate::pipeline::Pipeline;
    use crate::record::writer::Rotator;
    use crate::sync::SyncConfig;
    use crate::tables::Tables;
    use crate::tables::read::read_part;
    use crate::testkit::{DAY0, MS, SEC, Session, header};
    use crate::time::NS_PER_HOUR;

    const MIN: i64 = 60 * SEC;
    const MIDNIGHT: i64 = DAY0 + 24 * NS_PER_HOUR;

    /// What the recorder's writer thread does: frames files and the tables derived from them.
    /// Without `finish` the run ends as in a crash.
    fn record(frames: &[crate::frames::OwnedFrame], root: &Path, finish: bool) {
        let mut rot = Rotator::new(root, header());
        let mut tables = Tables::new(root, header(), "marketrec test");
        let mut p = Pipeline::new(&header(), RefBook::new(), SyncConfig::default()).unwrap();
        for f in frames {
            rot.write(f).unwrap();
            p.on_frame(f.as_frame(), &mut tables);
        }
        if finish {
            rot.finish().unwrap();
            tables.finish().unwrap();
        }
    }

    /// A session over midnight with a gap on each day.
    fn two_days() -> Session {
        let mut s = Session::new(21);
        s.connect(MIDNIGHT - 2 * MIN);
        s.run(MIDNIGHT - 2 * MIN + MS, MIDNIGHT - MIN, 40 * MS);
        s.lose(2);
        s.run(MIDNIGHT - MIN, MIDNIGHT + MIN, 40 * MS);
        s.lose(1);
        s.run(MIDNIGHT + MIN, MIDNIGHT + 2 * MIN, 40 * MS);
        s.disconnect(MIDNIGHT + 2 * MIN, STOPPED);
        s
    }

    fn quick() -> VerifyOptions {
        VerifyOptions {
            quick: true,
            scratch: None,
        }
    }

    fn outcomes(r: &VerifyReport) -> Vec<(&str, Outcome)> {
        r.checks.iter().map(|c| (c.name, c.outcome)).collect()
    }

    /// Replace a part with `f` applied to its rows, keeping its metadata.
    fn rewrite(path: &Path, table: Table, f: impl FnOnce(RecordBatch) -> RecordBatch) {
        let part = read_part(path, table).unwrap();
        let batch = f(part.batch);
        let props = crate::tables::props(table, &header());
        let mut w =
            ArrowWriter::try_new(fs::File::create(path).unwrap(), table.schema(), Some(props))
                .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    fn without_row(b: RecordBatch, row: usize) -> RecordBatch {
        let keep: arrow_array::BooleanArray = (0..b.num_rows()).map(|i| Some(i != row)).collect();
        arrow_select::filter::filter_record_batch(&b, &keep).unwrap()
    }

    fn set_i64(b: RecordBatch, col: usize, row: usize, v: i64) -> RecordBatch {
        let mut vals = i64_col(&b, col).to_vec();
        vals[row] = v;
        let mut cols: Vec<ArrayRef> = b.columns().to_vec();
        cols[col] = Arc::new(Int64Array::from(vals));
        RecordBatch::try_new(b.schema(), cols).unwrap()
    }

    fn set_manifest(day: &Path, f: impl FnOnce(&mut DayManifest)) {
        let mut m = read_manifest(day).unwrap();
        f(&mut m);
        fs::write(
            day.join("manifest.json"),
            serde_json::to_string_pretty(&m).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn a_clean_recording_passes_every_check_on_both_days() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("binance/BTCUSDT");
        record(&two_days().frames, &root, true);
        for date in ["2026-11-21", "2026-11-22"] {
            let r = verify_day(&root.join(date), &VerifyOptions::default());
            assert_eq!(r.outcome(), Outcome::Pass, "{date}\n{r}");
            assert_eq!(r.checks.len(), 7);
        }
        let r = verify_day(&root.join("2026-11-22"), &VerifyOptions::default());
        let c = &r.check("continuity").unwrap().detail;
        assert!(
            c.contains("2 valid intervals (1 gap recorded, none missed"),
            "valid from midnight, and again after the gap: {c}"
        );
        assert!(
            r.check("rebuild")
                .unwrap()
                .detail
                .contains("from the day's 1 frames file and 1 frames file of the day before"),
            "{r}"
        );
    }

    #[test]
    fn an_update_missing_from_depth_diff_is_found() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("binance/BTCUSDT");
        record(&two_days().frames, &root, true);
        let day = root.join("2026-11-22");
        let part = day.join("depth_diff/part-000.parquet");
        let rows = read_part(&part, Table::DepthDiff).unwrap().batch.num_rows();
        // drop every row of one message of a single update, the smallest possible loss
        let b = read_part(&part, Table::DepthDiff).unwrap().batch;
        let mid = (rows / 3..rows)
            .find(|&i| i64_col(&b, 2)[i] == i64_col(&b, 3)[i])
            .unwrap();
        let (ts, last) = (i64_col(&b, 0)[mid], i64_col(&b, 3)[mid]);
        let keep: arrow_array::BooleanArray = (0..rows)
            .map(|i| Some(!(i64_col(&b, 0)[i] == ts && i64_col(&b, 3)[i] == last)))
            .collect();
        let dropped = rows - keep.true_count();
        rewrite(&part, Table::DepthDiff, |b| {
            arrow_select::filter::filter_record_batch(&b, &keep).unwrap()
        });
        set_manifest(&day, |m| {
            *m.rows.get_mut("depth_diff").unwrap() -= dropped as u64;
        });
        let r = verify_day(&day, &VerifyOptions::default());
        assert_eq!(r.check("rows").unwrap().outcome, Outcome::Pass, "{r}");
        let c = r.check("continuity").unwrap();
        assert_eq!(c.outcome, Outcome::Fail, "{r}");
        assert!(c.detail.contains("no gap recorded"), "{}", c.detail);
        let rb = r.check("rebuild").unwrap();
        assert_eq!(rb.outcome, Outcome::Fail, "{r}");
        assert!(rb.detail.contains("depth_diff: from row"), "{}", rb.detail);
    }

    #[test]
    fn book_rows_that_disagree_with_the_updates_are_found() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("binance/BTCUSDT");
        record(&two_days().frames, &root, true);
        let day = root.join("2026-11-22");
        let part = day.join("book_l20/part-000.parquet");
        rewrite(&part, Table::BookL20, |b| {
            let id = i64_col(&b, 1)[10];
            set_i64(b, 1, 10, id - 1)
        });
        let r = verify_day(&day, &quick());
        let c = r.check("continuity").unwrap();
        assert_eq!(c.outcome, Outcome::Fail);
        assert!(
            c.detail.contains("1 book_l20 rows at another update"),
            "{}",
            c.detail
        );

        // a crossed book: best ask below best bid
        rewrite(&part, Table::BookL20, |b| {
            let mut cols: Vec<ArrayRef> = b.columns().to_vec();
            cols[4] = cols[2].clone(); // asks := bids
            RecordBatch::try_new(b.schema(), cols).unwrap()
        });
        let r = verify_day(&day, &quick());
        let c = &r.check("continuity").unwrap().detail;
        assert!(c.contains("rows that are not a sound book"), "{c}");
    }

    #[test]
    fn book_rows_while_the_book_was_invalid_are_found() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("binance/BTCUSDT");
        record(&two_days().frames, &root, true);
        let day = root.join("2026-11-22");
        // remove the resync: the rows after it are now outside a valid interval
        let part = day.join("quality/part-000.parquet");
        let q = read_part(&part, Table::Quality).unwrap().batch;
        let kinds = q.column(1).as_string::<i32>();
        let resync = (0..q.num_rows())
            .find(|&i| kinds.value(i) == "resync")
            .unwrap();
        rewrite(&part, Table::Quality, |b| without_row(b, resync));
        set_manifest(&day, |m| *m.rows.get_mut("quality").unwrap() -= 1);
        let r = verify_day(&day, &quick());
        let c = &r.check("continuity").unwrap().detail;
        assert!(
            c.contains("book_l20 rows while the book was not valid"),
            "{c}"
        );
    }

    #[test]
    fn a_changed_value_is_found_by_the_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("binance/BTCUSDT");
        record(&two_days().frames, &root, true);
        let day = root.join("2026-11-22");
        rewrite(&day.join("trades/part-000.parquet"), Table::Trades, |b| {
            let p = i64_col(&b, 4)[7];
            set_i64(b, 4, 7, p + 1)
        });
        let r = verify_day(&day, &VerifyOptions::default());
        assert_eq!(
            r.checks
                .iter()
                .filter(|c| c.outcome != Outcome::Pass)
                .count(),
            1,
            "{r}"
        );
        let rb = r.check("rebuild").unwrap();
        assert_eq!(rb.outcome, Outcome::Fail);
        assert!(rb.detail.contains("trades: from row 7 on"), "{}", rb.detail);
    }

    #[test]
    fn two_runs_converted_into_one_pass() {
        // convert makes one run of two: the stop ends the valid book, not a new run
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("binance/BTCUSDT");
        let t0 = DAY0 + 7 * NS_PER_HOUR;
        for (seed, start) in [(24, t0), (25, t0 + MIN)] {
            let mut s = Session::new(seed);
            s.ex.id += seed * 1000; // a later run sees later update IDs
            s.connect(start);
            s.run(start + MS, start + 20 * SEC, 40 * MS);
            s.disconnect(start + 20 * SEC, STOPPED);
            record(&s.frames, &root, true);
        }
        let day = root.join("2026-11-21");
        assert_eq!(
            verify_day(&day, &VerifyOptions::default()).outcome(),
            Outcome::Pass
        );
        let out = dir.path().join("converted");
        crate::tables::convert::convert(&day.join("frames"), &out, "test").unwrap();
        let converted = out.join("binance/BTCUSDT/2026-11-21");
        assert_eq!(read_manifest(&converted).unwrap().runs.len(), 1);
        let r = verify_day(&converted, &quick());
        assert_eq!(r.outcome(), Outcome::Pass, "{r}");
    }

    #[test]
    fn first_difference_ignores_how_rows_are_split_into_batches() {
        let batch = |v: &[i64]| -> RecordBatch {
            let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "x",
                arrow_schema::DataType::Int64,
                false,
            )]));
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(v.to_vec()))]).unwrap()
        };
        let ok = |v: Vec<RecordBatch>| v.into_iter().map(Ok);
        let a = || ok(vec![batch(&[1, 2, 3]), batch(&[]), batch(&[4, 5])]);
        let same = ok(vec![batch(&[1]), batch(&[2, 3, 4, 5])]);
        assert_eq!(first_difference(a(), same).unwrap(), None);
        let changed = ok(vec![batch(&[1, 2]), batch(&[3, 9, 5])]);
        assert_eq!(first_difference(a(), changed).unwrap(), Some(3));
        let shorter = ok(vec![batch(&[1, 2, 3, 4])]);
        assert_eq!(first_difference(a(), shorter).unwrap(), Some(4));
        let longer = ok(vec![batch(&[1, 2, 3, 4, 5, 6])]);
        assert_eq!(first_difference(a(), longer).unwrap(), Some(5));
    }

    #[test]
    fn a_manifest_that_disagrees_with_the_files_fails() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("binance/BTCUSDT");
        record(&two_days().frames, &root, true);
        let day = root.join("2026-11-21");
        set_manifest(&day, |m| *m.rows.get_mut("trades").unwrap() += 1);
        let r = verify_day(&day, &quick());
        assert_eq!(r.check("rows").unwrap().outcome, Outcome::Fail);
        assert!(r.check("rows").unwrap().detail.contains("trades: "), "{r}");

        set_manifest(&day, |m| m.qty_step = "0.00010000".into());
        let r = verify_day(&day, &quick());
        assert_eq!(r.check("parts").unwrap().outcome, Outcome::Fail);
        assert!(
            r.check("parts")
                .unwrap()
                .detail
                .contains("marketrec.qty_step"),
            "{r}"
        );

        set_manifest(&day, |m| m.date = "2026-11-20".into());
        let r = verify_day(&day, &quick());
        assert_eq!(outcomes(&r), [("manifest", Outcome::Fail)], "stops there");
    }

    #[test]
    fn a_missing_part_fails() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("binance/BTCUSDT");
        record(&two_days().frames, &root, true);
        let day = root.join("2026-11-21");
        fs::remove_file(day.join("trades/part-000.parquet")).unwrap();
        let r = verify_day(&day, &quick());
        assert_eq!(r.check("parts").unwrap().outcome, Outcome::Fail, "{r}");
    }

    #[test]
    fn a_crashed_run_is_a_warning_that_names_the_way_out() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("binance/BTCUSDT");
        let t0 = DAY0 + 5 * NS_PER_HOUR;
        let mut a = Session::new(22);
        a.connect(t0);
        a.run(t0 + MS, t0 + 20 * SEC, 40 * MS);
        record(&a.frames, &root, false); // crash: nothing finished
        let mut b = Session::new(23);
        b.connect(t0 + MIN);
        b.run(t0 + MIN + MS, t0 + 2 * MIN, 40 * MS);
        b.disconnect(t0 + 2 * MIN, STOPPED);
        record(&b.frames, &root, true);

        let day = root.join("2026-11-21");
        let r = verify_day(&day, &VerifyOptions::default());
        assert_eq!(r.outcome(), Outcome::Warn, "{r}");
        assert_eq!(
            r.check("parts").unwrap().outcome,
            Outcome::Pass,
            "the crashed run had not written a part yet"
        );
        let rb = r.check("rebuild").unwrap();
        assert_eq!(rb.outcome, Outcome::Warn, "{r}");
        assert!(
            rb.detail.contains("marketrec rebuild --day"),
            "{}",
            rb.detail
        );
        assert!(
            rb.detail.contains(".frames.tmp was never finished"),
            "{}",
            rb.detail
        );
        assert_eq!(r.check("continuity").unwrap().outcome, Outcome::Pass, "{r}");

        // the way out: derive the day again from its frames, the crashed run's included
        let r = crate::rebuild::rebuild_day(&day, true).unwrap().verify;
        assert_eq!(r.outcome(), Outcome::Pass, "{r}");
    }

    #[test]
    fn an_unfinished_or_unlisted_part_is_a_warning() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("binance/BTCUSDT");
        record(&two_days().frames, &root, true);
        let day = root.join("2026-11-21");
        fs::write(day.join("trades/.part-001.parquet.tmp"), b"PAR1").unwrap();
        fs::copy(
            day.join("trades/part-000.parquet"),
            day.join("trades/part-002.parquet"),
        )
        .unwrap();
        let r = verify_day(&day, &quick());
        let p = r.check("parts").unwrap();
        assert_eq!(p.outcome, Outcome::Warn, "{r}");
        assert!(
            p.detail
                .contains("trades/.part-001.parquet.tmp was never finished"),
            "{}",
            p.detail
        );
        assert!(
            p.detail.contains("part-002.parquet is not in the manifest"),
            "{}",
            p.detail
        );
    }

    #[test]
    fn without_the_day_before_only_the_messages_are_compared() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("binance/BTCUSDT");
        record(&two_days().frames, &root, true);
        fs::remove_dir_all(root.join("2026-11-21/frames")).unwrap();
        let r = verify_day(&root.join("2026-11-22"), &VerifyOptions::default());
        let rb = r.check("rebuild").unwrap();
        assert_eq!(rb.outcome, Outcome::Warn, "{r}");
        assert!(
            rb.detail.contains("frames of the day before are missing"),
            "{}",
            rb.detail
        );
        assert_eq!(r.check("continuity").unwrap().outcome, Outcome::Pass, "{r}");
    }

    #[test]
    fn book_checks() {
        let z = [0; L20];
        let mut bp = z;
        let mut bq = z;
        let mut ap = z;
        let mut aq = z;
        assert!(sound_book(&bp, &bq, &ap, &aq), "an empty book is sound");
        (bp[0], bp[1], bq[0], bq[1]) = (100, 99, 1, 1);
        (ap[0], aq[0]) = (101, 1);
        assert!(sound_book(&bp, &bq, &ap, &aq));
        ap[0] = 100;
        assert!(!sound_book(&bp, &bq, &ap, &aq), "crossed");
        ap[0] = 101;
        bp[1] = 100;
        assert!(!sound_book(&bp, &bq, &ap, &aq), "bids not falling");
        bp[1] = 99;
        bq[1] = 0;
        assert!(!sound_book(&bp, &bq, &ap, &aq), "a level without quantity");
        bq[1] = 1;
        bp[3] = 50;
        assert!(!sound_book(&bp, &bq, &ap, &aq), "a level after the end");
    }

    #[test]
    fn report_lines() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("binance/BTCUSDT");
        record(&two_days().frames, &root, true);
        let text = verify_day(&root.join("2026-11-21"), &quick()).to_string();
        let lines: Vec<&str> = text.lines().collect();
        assert!(lines[0].starts_with("day         "));
        assert!(
            lines[1].starts_with("PASS manifest   BTCUSDT 2026-11-21"),
            "{text}"
        );
        assert_eq!(lines.last(), Some(&"result      PASS"));
    }
}
