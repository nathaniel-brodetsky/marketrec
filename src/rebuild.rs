//! `marketrec rebuild`: derive a recorded day's tables again from its frames, in place.
//!
//! For a day whose tables a crash left incomplete, or that older rules derived. The frames
//! stay as they are; the tables are built next to the day (on the same disk, so the swap is
//! a rename), with the day before's frames for the state at midnight, then swapped in and
//! the old ones removed. The day is verified afterwards. No recorder may be writing to the
//! day meanwhile, which is why today's date is refused unless asked for.

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::tables::convert::{ConvertError, ConvertSummary};
use crate::tables::schema::Table;
use crate::time::{Utc, day_start};
use crate::verify::{DayInputs, Scratch, VerifyOptions, VerifyReport, verify_day};

#[derive(Debug)]
pub enum RebuildError {
    /// The directory is not `<root>/binance/<symbol>/<date>`.
    NotADay(PathBuf),
    /// Today's date, which a running recorder may still write.
    Today(String),
    NoFrames(PathBuf),
    Convert(ConvertError),
    Io(io::Error),
}

impl fmt::Display for RebuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RebuildError::NotADay(p) => {
                write!(
                    f,
                    "{} is not a day directory (<symbol>/<date>)",
                    p.display()
                )
            }
            RebuildError::Today(d) => write!(
                f,
                "{d} is today (UTC) and a recorder may still write it; stop the recorder and add --today"
            ),
            RebuildError::NoFrames(p) => write!(f, "no frames files in {}", p.display()),
            RebuildError::Convert(e) => write!(f, "{e}"),
            RebuildError::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for RebuildError {}

impl From<io::Error> for RebuildError {
    fn from(e: io::Error) -> Self {
        RebuildError::Io(e)
    }
}

#[derive(Debug)]
pub struct RebuildReport {
    pub summary: ConvertSummary,
    pub verify: VerifyReport,
}

impl fmt::Display for RebuildReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", self.summary)?;
        write!(f, "{}", self.verify)
    }
}

/// Derive `day` again from its frames and swap the new tables in.
pub fn rebuild_day(day: &Path, allow_today: bool) -> Result<RebuildReport, RebuildError> {
    let name = |p: Option<&Path>| {
        p.and_then(Path::file_name)
            .map(|n| n.to_string_lossy().into_owned())
    };
    let (Some(date), Some(symbol), Some(symbol_root)) =
        (name(Some(day)), name(day.parent()), day.parent())
    else {
        return Err(RebuildError::NotADay(day.to_owned()));
    };
    let Some(start) = day_start(&date) else {
        return Err(RebuildError::NotADay(day.to_owned()));
    };
    if !allow_today && Utc::from_ns(crate::net::ws::now_ns()).date() == date {
        return Err(RebuildError::Today(date));
    }
    let inputs =
        DayInputs::find(day, start).ok_or_else(|| RebuildError::NoFrames(day.join("frames")))?;

    // on the same disk as the day, so the new tables move in by renaming
    let scratch = Scratch(symbol_root.join(format!(".rebuild-{date}-{}", std::process::id())));
    let summary = inputs
        .convert(&scratch.0, &crate::record::recorder_id())
        .map_err(RebuildError::Convert)?;
    let rebuilt = scratch.0.join("binance").join(&symbol).join(&date);

    // the old tables go aside first, then the new ones move in; frames are not touched
    let aside = day.join(format!(".replaced-{}", crate::net::ws::now_ns()));
    fs::create_dir(&aside)?;
    let entries: Vec<&str> = Table::ALL
        .iter()
        .map(|t| t.name())
        .chain(["manifest.json"])
        .collect();
    for e in &entries {
        if day.join(e).exists() {
            fs::rename(day.join(e), aside.join(e))?;
        }
    }
    for e in &entries {
        if rebuilt.join(e).exists() {
            fs::rename(rebuilt.join(e), day.join(e))?;
        }
    }
    fs::remove_dir_all(&aside)?;
    drop(scratch);
    let verify = verify_day(day, &VerifyOptions::default());
    Ok(RebuildReport { summary, verify })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::RefBook;
    use crate::net::ws::STOPPED;
    use crate::pipeline::Pipeline;
    use crate::record::writer::Rotator;
    use crate::sync::SyncConfig;
    use crate::tables::Tables;
    use crate::tables::read::read_manifest;
    use crate::testkit::{DAY0, MS, SEC, Session, header};
    use crate::time::NS_PER_HOUR;
    use crate::verify::Outcome;

    // the tests' day is a fixed date, which is today once a year: `true` keeps them from
    // depending on the date they run on; the refusal itself is tested below

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

    #[test]
    fn a_day_left_incomplete_by_a_crash_is_derived_again_and_passes() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("binance/BTCUSDT");
        let t0 = DAY0 + 5 * NS_PER_HOUR;
        let mut a = Session::new(51);
        a.connect(t0);
        a.run(t0 + MS, t0 + 20 * SEC, 40 * MS);
        record(&a.frames, &root, false); // crash
        let mut b = Session::new(52);
        b.connect(t0 + 60 * SEC);
        b.run(t0 + 61 * SEC, t0 + 90 * SEC, 40 * MS);
        b.disconnect(t0 + 90 * SEC, STOPPED);
        record(&b.frames, &root, true);
        let day = root.join("2026-11-21");
        assert_eq!(
            verify_day(&day, &VerifyOptions::default()).outcome(),
            Outcome::Warn
        );

        let r = rebuild_day(&day, true).unwrap();
        assert_eq!(r.verify.outcome(), Outcome::Pass, "{}", r.verify);
        let m = read_manifest(&day).unwrap();
        assert_eq!(m.quality["connected"], 2, "both runs are in the tables now");
        assert_eq!(m.runs[0].derivation, crate::tables::DERIVATION);
        let left: Vec<String> = fs::read_dir(&root)
            .unwrap()
            .chain(fs::read_dir(&day).unwrap())
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with('.'))
            .collect();
        assert!(left.is_empty(), "no scratch left: {left:?}");
        assert!(day.join("frames").is_dir(), "frames untouched");
    }

    #[test]
    fn tables_from_older_rules_are_a_warning_until_derived_again() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("binance/BTCUSDT");
        let t0 = DAY0 + 7 * NS_PER_HOUR;
        let mut s = Session::new(53);
        s.connect(t0);
        s.run(t0 + MS, t0 + 10 * SEC, 40 * MS);
        s.lag = 2 * SEC; // the current rules record a late interval here
        s.run(t0 + 10 * SEC, t0 + 15 * SEC, 40 * MS);
        s.lag = 0;
        s.run(t0 + 15 * SEC, t0 + 20 * SEC, 40 * MS);
        s.disconnect(t0 + 20 * SEC, STOPPED);
        record(&s.frames, &root, true);
        let day = root.join("2026-11-21");
        // as if older rules had written it: no late events, no derivation recorded
        let q = day.join("quality/part-000.parquet");
        let part = crate::tables::read::read_part(&q, Table::Quality).unwrap();
        let kinds = arrow_array::cast::AsArray::as_string::<i32>(part.batch.column(1));
        let keep: arrow_array::BooleanArray = (0..part.batch.num_rows())
            .map(|i| Some(!matches!(kinds.value(i), "late" | "on_time")))
            .collect();
        let batch = arrow_select::filter::filter_record_batch(&part.batch, &keep).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(
            fs::File::create(&q).unwrap(),
            Table::Quality.schema(),
            Some(crate::tables::props(Table::Quality, &header())),
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let mut m = read_manifest(&day).unwrap();
        *m.rows.get_mut("quality").unwrap() -= 2;
        m.runs[0].derivation = 0;
        fs::write(
            day.join("manifest.json"),
            serde_json::to_string(&m).unwrap(),
        )
        .unwrap();

        let r = verify_day(&day, &VerifyOptions::default());
        let rb = r.check("rebuild").unwrap();
        assert_eq!(rb.outcome, Outcome::Warn, "{r}");
        assert!(
            rb.detail.contains("derived by older rules"),
            "{}",
            rb.detail
        );
        assert!(
            rb.detail.contains("marketrec rebuild --day"),
            "{}",
            rb.detail
        );

        let r = rebuild_day(&day, true).unwrap();
        assert_eq!(
            r.verify.outcome(),
            Outcome::Warn,
            "the late interval is real"
        );
        assert_eq!(r.verify.check("rebuild").unwrap().outcome, Outcome::Pass);
        assert_eq!(read_manifest(&day).unwrap().quality["late"], 1);
    }

    #[test]
    fn today_and_odd_directories_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let today = Utc::from_ns(crate::net::ws::now_ns()).date();
        let day = dir.path().join("binance/BTCUSDT").join(&today);
        fs::create_dir_all(day.join("frames")).unwrap();
        assert!(matches!(
            rebuild_day(&day, false),
            Err(RebuildError::Today(_))
        ));
        assert!(matches!(
            rebuild_day(&day, true),
            Err(RebuildError::NoFrames(_))
        ));
        assert!(matches!(
            rebuild_day(&dir.path().join("notadate"), false),
            Err(RebuildError::NotADay(_))
        ));
    }
}
