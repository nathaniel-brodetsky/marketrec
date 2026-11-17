//! The data contract: `SCHEMA.md` against the code, and tables built by `convert` from the
//! fixtures against what replay reports about the same frames.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_schema::DataType;
use marketrec::frames::text;
use marketrec::pipeline::QualityEvent;
use marketrec::replay::replay_file;
use marketrec::sync::State;
use marketrec::tables::convert::{ConvertError, convert};
use marketrec::tables::read::{days, read_manifest, read_table};
use marketrec::tables::schema::{L20, SCHEMA_VERSION, Table};

fn repo(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

fn fixtures() -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(repo("tests/fixtures"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "jsonl"))
        .collect();
    v.sort();
    assert_eq!(v.len(), 12);
    v
}

fn type_name(t: &DataType) -> String {
    match t {
        DataType::Int64 => "int64".into(),
        DataType::Int8 => "int8".into(),
        DataType::Utf8 => "string".into(),
        DataType::Boolean => "bool".into(),
        DataType::FixedSizeList(item, n) => format!("{}[{n}]", type_name(item.data_type())),
        other => panic!("type {other} is not in the contract"),
    }
}

/// Columns per `### table` section of SCHEMA.md, as (name, type).
fn documented() -> BTreeMap<String, Vec<(String, String)>> {
    let doc = fs::read_to_string(repo("SCHEMA.md")).unwrap();
    let mut out: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    let mut table = None;
    for line in doc.lines() {
        if let Some(name) = line.strip_prefix("### ") {
            table = Some(name.trim().to_owned());
        } else if line.starts_with("## ") {
            table = None;
        } else if let (Some(t), Some(row)) = (&table, line.strip_prefix("| `")) {
            let cells: Vec<&str> = row.split('|').map(str::trim).collect();
            let col = cells[0].trim_end_matches('`').to_owned();
            out.entry(t.clone())
                .or_default()
                .push((col, cells[1].to_owned()));
        }
    }
    out
}

#[test]
fn schema_md_is_the_schema_in_the_code() {
    let doc = documented();
    let names: Vec<&str> = Table::ALL.iter().map(|t| t.name()).collect();
    assert_eq!(doc.keys().map(String::as_str).collect::<Vec<_>>(), {
        let mut n = names.clone();
        n.sort();
        n
    });
    for table in Table::ALL {
        let code: Vec<(String, String)> = table
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), type_name(f.data_type())))
            .collect();
        assert_eq!(doc[table.name()], code, "SCHEMA.md, table {}", table.name());
    }
    let text = fs::read_to_string(repo("SCHEMA.md")).unwrap();
    assert!(text.starts_with(&format!("# Data contract, version {SCHEMA_VERSION}\n")));
    let kinds = QualityEvent::KINDS
        .iter()
        .map(|k| format!("`{k}`"))
        .collect::<Vec<_>>()
        .join(", ");
    let listed = text
        .split_once("Kinds: ")
        .unwrap()
        .1
        .split_once(".\n")
        .unwrap()
        .0
        .replace('\n', " ");
    assert_eq!(listed, kinds, "quality kinds in SCHEMA.md");
}

fn day_of(out: &Path) -> PathBuf {
    let d = days(&out.join("binance/BTCUSDT")).unwrap();
    assert_eq!(d.len(), 1, "every fixture is one day");
    d.into_iter().next().unwrap()
}

/// Levels in the depth messages of a fixture, counted from the JSON.
fn depth_levels(path: &Path) -> usize {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .skip(1)
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter_map(|v| v.get("payload_json").cloned())
        .filter(|p| p["stream"].as_str().is_some_and(|s| s.contains("@depth")))
        .map(|p| {
            p["data"]["b"].as_array().map_or(0, Vec::len)
                + p["data"]["a"].as_array().map_or(0, Vec::len)
        })
        .sum()
}

#[test]
fn every_fixture_converts_to_tables_that_agree_with_replay() {
    for path in fixtures() {
        let name = path.file_stem().unwrap().to_string_lossy().into_owned();
        let out = tempfile::tempdir().unwrap();
        let summary = convert(&path, out.path(), "test").unwrap_or_else(|e| panic!("{name}: {e}"));
        let r = replay_file(&path).unwrap();
        assert_eq!(summary.stats, r.stats, "{name}");
        let day = day_of(out.path());
        let m = read_manifest(&day).unwrap();
        let rows = |t: Table| read_table(&day, t).unwrap().num_rows() as u64;

        assert_eq!(rows(Table::Trades), r.stats.trades, "{name}");
        assert_eq!(rows(Table::BookTicker), r.stats.book_tickers, "{name}");
        if r.stats.undecodable == 0 {
            assert_eq!(
                rows(Table::DepthDiff) as usize,
                depth_levels(&path),
                "{name}"
            );
        }
        let q = read_table(&day, Table::Quality).unwrap();
        let kinds = q.column_by_name("kind").unwrap().as_string::<i32>();
        let ts = q.column_by_name("ts").unwrap().as_primitive::<Int64Type>();
        let got: Vec<(i64, &str)> = (0..q.num_rows())
            .map(|i| (ts.value(i), kinds.value(i)))
            .collect();
        let want: Vec<(i64, &str)> = r.events.iter().map(|(t, e)| (*t, e.kind())).collect();
        assert_eq!(got, want, "{name}");

        for t in Table::ALL {
            assert_eq!(m.rows[t.name()], rows(t), "{name} {}", t.name());
        }
        assert_eq!(m.synced_ns, r.stats.synced_ns, "{name}");
        assert_eq!(
            m.quality.values().sum::<u64>() as usize,
            r.events.len(),
            "{name}"
        );

        // the last book row is the book replay ends with, when nothing broke it after
        let b = read_table(&day, Table::BookL20).unwrap();
        if r.state == State::Synced && b.num_rows() > 0 {
            let last = b.num_rows() - 1;
            let ids = b
                .column_by_name("last_update_id")
                .unwrap()
                .as_primitive::<Int64Type>();
            if ids.value(last) as u64 == r.update_id {
                let first = |col: &str| {
                    let a = b
                        .column_by_name(col)
                        .unwrap()
                        .as_fixed_size_list()
                        .value(last);
                    let v = a.as_primitive::<Int64Type>().values().to_vec();
                    assert_eq!(v.len(), L20);
                    v[0]
                };
                let bid = r.best_bid.unwrap();
                let ask = r.best_ask.unwrap();
                assert_eq!(
                    (
                        first("bid_px"),
                        first("bid_qty"),
                        first("ask_px"),
                        first("ask_qty")
                    ),
                    (bid.price, bid.qty, ask.price, ask.qty),
                    "{name}"
                );
            }
        }
    }
}

#[test]
fn binary_and_text_frames_give_identical_tables() {
    let dir = tempfile::tempdir().unwrap();
    let src = repo("tests/fixtures/reconnect.jsonl");
    let bin = dir.path().join("reconnect.frames");
    text::pack_file(std::io::BufReader::new(fs::File::open(&src).unwrap()), &bin).unwrap();
    let (a, b) = (dir.path().join("a"), dir.path().join("b"));
    convert(&src, &a, "test").unwrap();
    convert(&bin, &b, "test").unwrap();
    for t in Table::ALL {
        assert_eq!(
            read_table(&day_of(&a), t).unwrap(),
            read_table(&day_of(&b), t).unwrap(),
            "{}",
            t.name()
        );
    }
}

#[test]
fn inputs_of_different_instruments_are_refused_before_anything_is_written() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in");
    fs::create_dir(&input).unwrap();
    let src = fs::read_to_string(repo("tests/fixtures/normal_start.jsonl")).unwrap();
    fs::write(input.join("a.jsonl"), &src).unwrap();
    fs::write(
        input.join("b.jsonl"),
        src.replacen(
            "\"qty_step\":\"0.00001000\"",
            "\"qty_step\":\"0.00010000\"",
            1,
        ),
    )
    .unwrap();
    let out = dir.path().join("out");
    let err = convert(&input, &out, "test").unwrap_err();
    assert!(
        matches!(err, ConvertError::Mixed(ref p) if p.ends_with("b.jsonl")),
        "{err}"
    );
    assert!(!out.exists(), "nothing written");
}

#[test]
fn files_that_overlap_in_time_are_not_one_recording() {
    let out = tempfile::tempdir().unwrap();
    let err = convert(&repo("tests/fixtures"), out.path(), "test").unwrap_err();
    let ConvertError::Overlap { path, .. } = &err else {
        panic!("{err}")
    };
    assert!(
        path.ends_with("broken_frame.jsonl"),
        "the second file: {err}"
    );
    // the first file's tables are complete
    let m = read_manifest(&day_of(out.path())).unwrap();
    assert_eq!(m.quality["bbo_mismatch"], 1);
}
