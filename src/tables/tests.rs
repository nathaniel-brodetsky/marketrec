//! Tables built from simulated recordings, checked against the simulated exchange.

use std::path::{Path, PathBuf};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int8Type, Int64Type};
use arrow_array::{Array, RecordBatch};

use super::read::{parts, read_manifest, read_table};
use super::schema::{L20, Table};
use super::{L20_PERIOD_NS, Tables};
use crate::book::RefBook;
use crate::event::Level;
use crate::frames::OwnedFrame;
use crate::net::ws::STOPPED;
use crate::pipeline::{Pipeline, Stats};
use crate::sync::SyncConfig;
use crate::testkit::{DAY0, MS, SEC, Session, header};
use crate::time::{NS_PER_HOUR, hour_start};

const MIN: i64 = 60 * SEC;

/// Derive the tables from `frames` into `root`, as the writer thread does.
fn derive(frames: &[OwnedFrame], root: &Path) -> Stats {
    let mut tables = Tables::new(root, header(), "marketrec test");
    let mut p = Pipeline::new(&header(), RefBook::new(), SyncConfig::default()).unwrap();
    for f in frames {
        p.on_frame(f.as_frame(), &mut tables);
    }
    tables.finish().unwrap();
    p.finish().clone()
}

fn i64s(b: &RecordBatch, col: &str) -> Vec<i64> {
    b.column_by_name(col)
        .unwrap()
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

fn strs(b: &RecordBatch, col: &str) -> Vec<String> {
    let a = b.column_by_name(col).unwrap().as_string::<i32>();
    (0..a.len()).map(|i| a.value(i).to_owned()).collect()
}

/// One `book_l20` row as (bids, asks), zero levels left out.
fn l20(b: &RecordBatch, row: usize) -> (Vec<Level>, Vec<Level>) {
    let list = |col: &str| -> Vec<i64> {
        let a = b.column_by_name(col).unwrap().as_fixed_size_list();
        assert!(a.is_valid(row));
        a.value(row).as_primitive::<Int64Type>().values().to_vec()
    };
    let side = |px: Vec<i64>, qty: Vec<i64>| -> Vec<Level> {
        assert_eq!(px.len(), L20);
        px.into_iter()
            .zip(qty)
            .filter(|&(p, _)| p != 0)
            .map(|(price, qty)| Level { price, qty })
            .collect()
    };
    (
        side(list("bid_px"), list("bid_qty")),
        side(list("ask_px"), list("ask_qty")),
    )
}

fn quality(day: &Path) -> Vec<(i64, String)> {
    let q = read_table(day, Table::Quality).unwrap();
    i64s(&q, "ts").into_iter().zip(strs(&q, "kind")).collect()
}

fn names(paths: Vec<PathBuf>) -> Vec<String> {
    paths
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
        .collect()
}

/// The manifest agrees with the files next to it.
fn assert_manifest_matches_files(day: &Path) {
    let m = read_manifest(day).unwrap();
    for table in Table::ALL {
        let rows = read_table(day, table).unwrap().num_rows() as u64;
        assert_eq!(m.rows[table.name()], rows, "{}", table.name());
        let listed: Vec<String> = m.files.get(table.name()).cloned().unwrap_or_default();
        let found: Vec<String> = names(parts(day, table).unwrap())
            .into_iter()
            .map(|n| format!("{}/{n}", table.name()))
            .collect();
        assert_eq!(listed, found, "{}", table.name());
    }
}

#[test]
fn book_l20_samples_the_true_book_every_100ms_once_synced() {
    let mut s = Session::new(1);
    let t0 = DAY0 + 12 * NS_PER_HOUR;
    s.connect(t0);
    s.run(t0 + MS, t0 + 10 * SEC, 30 * MS);
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("binance/BTCUSDT");
    let stats = derive(&s.frames, &root);
    let day = root.join("2026-11-21");

    let q = quality(&day);
    let kinds: Vec<&str> = q.iter().map(|(_, k)| k.as_str()).collect();
    assert_eq!(kinds, ["connected", "synced"]);
    let synced_at = q[1].0;

    // first frame of every 100 ms bucket, from the moment the book became valid
    let mut expected = Vec::new();
    for f in s.frames.iter().filter(|f| f.recv_ts >= synced_at) {
        let bucket = f.recv_ts.div_euclid(L20_PERIOD_NS);
        if expected
            .last()
            .is_none_or(|&t: &i64| t.div_euclid(L20_PERIOD_NS) != bucket)
        {
            expected.push(f.recv_ts);
        }
    }
    let b = read_table(&day, Table::BookL20).unwrap();
    assert_eq!(i64s(&b, "recv_ts"), expected);
    assert!(b.num_rows() > 90, "about one row per 100 ms over ~10 s");

    for (row, id) in i64s(&b, "last_update_id").into_iter().enumerate() {
        let truth = &s.ex.tops[&(id as u64)];
        assert_eq!(&l20(&b, row), truth, "row {row}, update {id}");
    }

    let m = read_manifest(&day).unwrap();
    let last = s.frames.last().unwrap().recv_ts;
    assert_eq!(m.covered_ns, last - t0, "connected from the first frame");
    assert_eq!(m.synced_ns, last - synced_at);
    assert_eq!(m.synced_ns, stats.synced_ns, "same as the pipeline's count");
    assert_eq!(m.quality["synced"], 1);
    assert_manifest_matches_files(&day);
}

#[test]
fn depth_and_trade_rows_hold_exactly_what_was_sent() {
    let mut s = Session::new(2);
    let t0 = DAY0 + 3 * NS_PER_HOUR;
    s.connect(t0);
    s.run(t0 + MS, t0 + 3 * SEC, 20 * MS);
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("binance/BTCUSDT");
    derive(&s.frames, &root);
    let day = root.join("2026-11-21");

    let d = read_table(&day, Table::DepthDiff).unwrap();
    let side = d.column_by_name("side").unwrap().as_primitive::<Int8Type>();
    let got: Vec<(i64, i64, i64, i64, i8, i64, i64)> = (0..d.num_rows())
        .map(|i| {
            (
                i64s(&d, "recv_ts")[i],
                i64s(&d, "event_ts")[i],
                i64s(&d, "first_update_id")[i],
                i64s(&d, "final_update_id")[i],
                side.value(i),
                i64s(&d, "price")[i],
                i64s(&d, "qty")[i],
            )
        })
        .collect();
    let mut want = Vec::new();
    for (ts, first, last, bids, asks) in &s.depth {
        let event_ts = ts / 1000 * 1000; // sent in microseconds
        for (side, levels) in [(1i8, bids), (-1, asks)] {
            for &(p, q) in levels {
                want.push((*ts, event_ts, *first as i64, *last as i64, side, p, q));
            }
        }
    }
    assert_eq!(
        got, want,
        "every level of every diff, buffered ones included"
    );
    assert!(strs(&d, "source").iter().all(|s| s == "json"));

    let t = read_table(&day, Table::Trades).unwrap();
    let maker = t
        .column_by_name("is_buyer_maker")
        .unwrap()
        .as_boolean()
        .clone();
    let got: Vec<_> = (0..t.num_rows())
        .map(|i| {
            (
                i64s(&t, "recv_ts")[i],
                i64s(&t, "event_ts")[i],
                i64s(&t, "trade_ts")[i],
                i64s(&t, "trade_id")[i],
                i64s(&t, "price")[i],
                i64s(&t, "qty")[i],
                maker.value(i),
            )
        })
        .collect();
    assert_eq!(got, s.trades);
    assert!(!s.trades.is_empty());

    let bt = read_table(&day, Table::BookTicker).unwrap();
    for (i, id) in i64s(&bt, "update_id").into_iter().enumerate() {
        let (bids, asks) = &s.ex.tops[&(id as u64)];
        assert_eq!(
            (
                i64s(&bt, "bid_px")[i],
                i64s(&bt, "bid_qty")[i],
                i64s(&bt, "ask_px")[i],
                i64s(&bt, "ask_qty")[i]
            ),
            (bids[0].price, bids[0].qty, asks[0].price, asks[0].qty)
        );
    }
}

#[test]
fn parts_close_every_hour_and_days_split_at_midnight() {
    let mut s = Session::new(3);
    let connect = DAY0 + 22 * NS_PER_HOUR + 59 * MIN + 50 * SEC;
    s.connect(connect);
    s.run(connect + MS, DAY0 + 23 * NS_PER_HOUR + 10 * SEC, 500 * MS);
    let midnight = DAY0 + 24 * NS_PER_HOUR;
    s.run(midnight - 10 * SEC, midnight + 10 * SEC, 500 * MS);
    s.disconnect(midnight + 10 * SEC, STOPPED);

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("binance/BTCUSDT");
    let stats = derive(&s.frames, &root);
    let (day1, day2) = (root.join("2026-11-21"), root.join("2026-11-22"));

    for table in [Table::DepthDiff, Table::BookL20] {
        assert_eq!(
            names(parts(&day1, table).unwrap()),
            ["part-000.parquet", "part-001.parquet"],
            "hours 22 and 23"
        );
        assert_eq!(names(parts(&day2, table).unwrap()), ["part-000.parquet"]);
    }
    for (day, hours) in [
        (
            &day1,
            vec![DAY0 + 22 * NS_PER_HOUR, DAY0 + 23 * NS_PER_HOUR],
        ),
        (&day2, vec![midnight]),
    ] {
        for (path, hour) in parts(day, Table::DepthDiff).unwrap().iter().zip(hours) {
            let part = super::read::read_part(path, Table::DepthDiff).unwrap();
            assert!(
                i64s(&part.batch, "recv_ts")
                    .iter()
                    .all(|&t| hour_start(t) == hour),
                "{} holds one hour",
                path.display()
            );
            assert_eq!(part.metadata["marketrec.table"], "depth_diff");
            assert_eq!(part.metadata["marketrec.schema_version"], "1");
            assert_eq!(part.metadata["marketrec.qty_step"], "0.00001000");
        }
        assert_manifest_matches_files(day);
    }

    let (m1, m2) = (read_manifest(&day1).unwrap(), read_manifest(&day2).unwrap());
    let synced_at = quality(&day1)
        .into_iter()
        .find(|(_, k)| k == "synced")
        .unwrap()
        .0;
    assert_eq!(m1.covered_ns, midnight - connect);
    assert_eq!(m1.synced_ns, midnight - synced_at);
    assert_eq!((m2.covered_ns, m2.synced_ns), (10 * SEC, 10 * SEC));
    assert_eq!(m1.synced_ns + m2.synced_ns, stats.synced_ns);
    assert_eq!(
        m2.runs[0].first_recv_ts,
        Some(midnight),
        "a frame at 00:00:00"
    );
    assert_eq!(
        quality(&day2),
        [(midnight + 10 * SEC, "disconnected".to_owned())]
    );
}

#[test]
fn no_book_rows_between_a_gap_and_the_resync() {
    let mut s = Session::new(4);
    let t0 = DAY0 + 6 * NS_PER_HOUR;
    s.connect(t0);
    s.run(t0 + MS, t0 + 3 * SEC, 20 * MS);
    s.lose(3);
    s.run(t0 + 3 * SEC, t0 + 6 * SEC, 20 * MS);
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("binance/BTCUSDT");
    let stats = derive(&s.frames, &root);
    let day = root.join("2026-11-21");

    let q = quality(&day);
    let kinds: Vec<&str> = q.iter().map(|(_, k)| k.as_str()).collect();
    assert_eq!(kinds, ["connected", "synced", "gap", "resync"]);
    let (gap, resync) = (q[2].0, q[3].0);
    assert_eq!(resync - gap, 50 * MS, "one snapshot delay");

    let b = read_table(&day, Table::BookL20).unwrap();
    let ts = i64s(&b, "recv_ts");
    assert!(ts.iter().all(|&t| t < gap || t >= resync));
    assert!(ts.iter().any(|&t| t >= resync));
    for (row, id) in i64s(&b, "last_update_id").into_iter().enumerate() {
        assert_eq!(&l20(&b, row), &s.ex.tops[&(id as u64)]);
    }

    let m = read_manifest(&day).unwrap();
    assert_eq!(m.synced_ns, stats.synced_ns);
    assert_eq!(
        m.covered_ns - m.synced_ns,
        (synced(&q) - t0) + (resync - gap)
    );
    assert_eq!((m.quality["gap"], m.quality["resync"]), (1, 1));
}

fn synced(q: &[(i64, String)]) -> i64 {
    q.iter().find(|(_, k)| k == "synced").unwrap().0
}

#[test]
fn a_second_run_adds_parts_and_merges_the_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("binance/BTCUSDT");
    let day = root.join("2026-11-21");
    let t0 = DAY0 + NS_PER_HOUR;
    let mut covered = 0;
    for (seed, start) in [(5, t0), (6, t0 + 30 * SEC)] {
        let mut s = Session::new(seed);
        s.connect(start);
        s.run(start + MS, start + 5 * SEC, 100 * MS);
        s.disconnect(start + 5 * SEC, STOPPED);
        derive(&s.frames, &root);
        covered += 5 * SEC;
    }
    assert_eq!(
        names(parts(&day, Table::DepthDiff).unwrap()),
        ["part-000.parquet", "part-001.parquet"]
    );
    let m = read_manifest(&day).unwrap();
    assert_eq!(m.runs.len(), 2);
    assert_eq!(
        m.covered_ns, covered,
        "the pause between runs is not covered"
    );
    assert_eq!((m.quality["connected"], m.quality["disconnected"]), (2, 2));
    assert_manifest_matches_files(&day);

    // a run with other steps must not add to this day, not even one part
    let mut other = header();
    other.qty_step = "0.00010000".into();
    let mut tables = Tables::new(&root, other.clone(), "marketrec test");
    let mut p = Pipeline::new(&other, RefBook::new(), SyncConfig::default()).unwrap();
    let mut s = Session::new(7);
    s.connect(t0 + MIN);
    s.run(t0 + MIN + MS, t0 + MIN + SEC, 100 * MS);
    for f in &s.frames {
        p.on_frame(f.as_frame(), &mut tables);
    }
    let err = tables.finish().unwrap_err().to_string();
    assert!(err.contains("describes other data"), "{err}");
    assert_eq!(parts(&day, Table::DepthDiff).unwrap().len(), 2);
    assert_eq!(read_manifest(&day).unwrap(), m);
}

#[test]
fn convert_equals_the_live_derivation_and_refuses_to_overwrite() {
    let mut s = Session::new(8);
    let t0 = DAY0 + 9 * NS_PER_HOUR;
    s.connect(t0);
    s.run(t0 + MS, t0 + 2 * SEC, 25 * MS);
    s.lose(1);
    s.run(t0 + 2 * SEC, t0 + 4 * SEC, 25 * MS);
    s.disconnect(t0 + 4 * SEC, "connection reset");
    s.connect(t0 + 5 * SEC);
    s.run(t0 + 5 * SEC + MS, t0 + 7 * SEC, 25 * MS);

    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("live/binance/BTCUSDT");
    derive(&s.frames, &live);

    // the frames as the recorder writes them: binary, split into two files
    let frames_dir = dir.path().join("frames");
    std::fs::create_dir_all(&frames_dir).unwrap();
    let half = s.frames.len() / 2;
    for (name, chunk) in [
        ("09-00-00", &s.frames[..half]),
        ("09-00-03", &s.frames[half..]),
    ] {
        let path = frames_dir.join(format!("{name}.frames"));
        let mut w = crate::frames::FramesFile::create(&path, &header()).unwrap();
        for f in chunk {
            w.write_frame(f.as_frame()).unwrap();
        }
        w.finish().unwrap();
    }
    let out = dir.path().join("converted");
    let summary = super::convert::convert(&frames_dir, &out, "marketrec test").unwrap();
    assert_eq!(summary.inputs.len(), 2);
    assert_eq!(summary.stats.frames as usize, s.frames.len());

    let (a, b) = (
        live.join("2026-11-21"),
        out.join("binance/BTCUSDT/2026-11-21"),
    );
    for table in Table::ALL {
        assert_eq!(
            read_table(&a, table).unwrap(),
            read_table(&b, table).unwrap(),
            "{}",
            table.name()
        );
    }
    let (ma, mb) = (read_manifest(&a).unwrap(), read_manifest(&b).unwrap());
    assert_eq!(
        (&ma.rows, &ma.files, &ma.quality),
        (&mb.rows, &mb.files, &mb.quality)
    );
    assert_eq!((ma.covered_ns, ma.synced_ns), (mb.covered_ns, mb.synced_ns));
    assert_eq!(ma.quality["reconnect"], 1);

    let err = super::convert::convert(&frames_dir, &out, "marketrec test").unwrap_err();
    assert!(err.to_string().contains("already exists"), "{err}");
}

/// Rows of `b` whose `col` is at least `ts`.
fn since(b: &RecordBatch, col: &str, ts: i64) -> RecordBatch {
    let keep: arrow_array::BooleanArray = i64s(b, col).iter().map(|&t| Some(t >= ts)).collect();
    arrow_select::filter::filter_record_batch(b, &keep).unwrap()
}

#[test]
fn a_replay_from_the_middle_matches_the_live_tables_from_the_next_connection_on() {
    let mut s = Session::new(9);
    let t0 = DAY0 + 10 * NS_PER_HOUR;
    s.connect(t0);
    s.run(t0 + MS, t0 + 20 * SEC, 50 * MS);
    s.disconnect(t0 + 20 * SEC, "connection reset");
    s.connect(t0 + 21 * SEC);
    s.run(t0 + 21 * SEC + MS, t0 + 40 * SEC, 50 * MS);
    let cut = s
        .frames
        .iter()
        .position(|f| f.recv_ts >= t0 + 10 * SEC)
        .unwrap();
    let cut_ts = s.frames[cut].recv_ts;

    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (dir.path().join("a"), dir.path().join("b"));
    derive(&s.frames, &a);
    derive(&s.frames[cut..], &b);
    let (a, b) = (a.join("2026-11-21"), b.join("2026-11-21"));

    // messages are stored whatever the book's state
    for table in [Table::DepthDiff, Table::BookTicker, Table::Trades] {
        let (x, y) = (
            read_table(&a, table).unwrap(),
            read_table(&b, table).unwrap(),
        );
        assert_eq!(since(&x, "recv_ts", cut_ts), y, "{}", table.name());
    }
    // the book: none until the replay's first snapshot, then exactly the live one
    let resync = quality(&a)
        .into_iter()
        .filter(|(_, k)| k == "synced")
        .nth(1)
        .unwrap()
        .0;
    assert!(resync > t0 + 21 * SEC);
    let (x, y) = (
        read_table(&a, Table::BookL20).unwrap(),
        read_table(&b, Table::BookL20).unwrap(),
    );
    assert_eq!(i64s(&y, "recv_ts").first(), Some(&resync));
    assert_eq!(since(&x, "recv_ts", resync), y);
    let from_conn2 = |q: Vec<(i64, String)>| -> Vec<(i64, String)> {
        q.into_iter().filter(|(t, _)| *t >= t0 + 21 * SEC).collect()
    };
    assert_eq!(from_conn2(quality(&b)), from_conn2(quality(&a)));
}

#[test]
fn a_day_rebuilt_from_the_frames_since_the_day_before_equals_the_live_day() {
    let mut s = Session::new(10);
    let midnight = DAY0 + 24 * NS_PER_HOUR;
    s.connect(midnight - MIN);
    s.run(midnight - MIN + MS, midnight + 30 * SEC, 40 * MS);
    s.lose(2);
    s.run(midnight + 30 * SEC, midnight + MIN, 40 * MS);
    s.disconnect(midnight + MIN, STOPPED);

    let dir = tempfile::tempdir().unwrap();
    let (live, rebuilt) = (dir.path().join("live"), dir.path().join("rebuilt"));
    derive(&s.frames, &live);
    let mut tables = Tables::new(&rebuilt, header(), "marketrec test").skip_before(midnight);
    let mut p = Pipeline::new(&header(), RefBook::new(), SyncConfig::default()).unwrap();
    for f in &s.frames {
        p.on_frame(f.as_frame(), &mut tables);
    }
    tables.finish().unwrap();

    assert!(
        !rebuilt.join("2026-11-21").exists(),
        "nothing before midnight"
    );
    let (a, b) = (live.join("2026-11-22"), rebuilt.join("2026-11-22"));
    for table in Table::ALL {
        assert_eq!(
            read_table(&a, table).unwrap(),
            read_table(&b, table).unwrap(),
            "{}",
            table.name()
        );
    }
    let (ma, mb) = (read_manifest(&a).unwrap(), read_manifest(&b).unwrap());
    assert_eq!((ma.covered_ns, ma.synced_ns), (mb.covered_ns, mb.synced_ns));
    assert_eq!(ma.covered_ns, MIN, "connected all day long, until the stop");
    assert_eq!(ma.runs, mb.runs);

    // the book at midnight is the book after the last message of the day before
    let last_before = s.depth.iter().rev().find(|d| d.0 < midnight).unwrap().2;
    assert_eq!(ma.runs[0].book_at_start, Some(last_before));
    let first_day = read_manifest(&live.join("2026-11-21")).unwrap();
    assert_eq!(
        first_day.runs[0].book_at_start, None,
        "the run began that day"
    );
}

#[test]
fn a_diff_without_levels_is_one_row_with_side_zero() {
    let mut s = Session::new(11);
    s.connect(DAY0);
    s.run(DAY0 + MS, DAY0 + SEC, 100 * MS);
    let id = s.ex.id;
    s.push(crate::testkit::text(
        DAY0 + SEC,
        format!(
            r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":{},"u":{},"b":[],"a":[]}}}}"#,
            id + 1,
            id + 2
        ),
    ));
    let dir = tempfile::tempdir().unwrap();
    derive(&s.frames, dir.path());
    let d = read_table(&dir.path().join("2026-11-21"), Table::DepthDiff).unwrap();
    let last = d.num_rows() - 1;
    let side = d.column_by_name("side").unwrap().as_primitive::<Int8Type>();
    assert_eq!(side.value(last), 0);
    assert_eq!(
        (
            i64s(&d, "first_update_id")[last],
            i64s(&d, "final_update_id")[last],
            i64s(&d, "price")[last],
            i64s(&d, "qty")[last]
        ),
        (id as i64 + 1, id as i64 + 2, 0, 0)
    );
    assert_eq!(side.iter().filter(|s| *s == Some(0)).count(), 1);
}
