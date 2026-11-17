//! Replay of the scenarios in `tests/fixtures`, one per case of the recorder's test table.
//! Each fixture is a readable frames file; see `tests/fixtures/README.md`.

use std::fs::{self, File};
use std::io::BufReader;
use std::path::PathBuf;

use marketrec::event::Level;
use marketrec::frames::text;
use marketrec::replay::{Ending, InputForm, Report, replay_file};
use marketrec::sync::State;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("{name}.jsonl"))
}

/// Receive time of a fixture frame `ms` milliseconds after the start, which is also the ID
/// of a snapshot request that frame caused.
fn at(ms: u64) -> u64 {
    1_795_219_200_000_000_000 + ms * 1_000_000
}

fn replay(name: &str) -> Report {
    let r = replay_file(&fixture(name)).unwrap_or_else(|e| panic!("{name}: {e}"));
    assert_eq!(r.form, InputForm::Text);
    assert_eq!(r.ending, Ending::Clean);
    r
}

/// Price in cents and quantity in units of 0.00001, as the fixtures use.
fn lv(price: i64, qty: i64) -> Option<Level> {
    Some(Level { price, qty })
}

fn assert_book(r: &Report, update_id: u64, bid: Option<Level>, ask: Option<Level>) {
    assert_eq!(r.state, State::Synced);
    assert_eq!((r.update_id, r.best_bid, r.best_ask), (update_id, bid, ask));
}

#[test]
fn normal_start() {
    let r = replay("normal_start");
    assert_eq!(r.event_kinds(), ["connected", "synced"]);
    assert_eq!(r.requests, [at(5)], "asked at the first diff");
    assert_book(&r, 1007, lv(9712346, 1000), lv(9712347, 90000));
    let s = &r.stats;
    assert_eq!((s.depth_buffered, s.depth_applied, s.trades), (2, 2, 1));
    assert_eq!(
        (s.bbo_compared, s.bbo_mismatches),
        (2, 0),
        "both tickers match the book at their update ids"
    );
}

#[test]
fn snapshot_older_than_buffer_is_requested_again() {
    let r = replay("stale_snapshot");
    assert_eq!(r.event_kinds(), ["connected", "snapshot_too_old", "synced"]);
    assert_eq!(
        r.requests,
        [at(5), at(60)],
        "asked again when the old snapshot came"
    );
    assert_book(&r, 1004, lv(9712345, 20000), lv(9712346, 30000));
}

#[test]
fn buffered_events_older_than_snapshot_are_dropped() {
    let r = replay("old_events_in_buffer");
    assert_eq!(r.event_kinds(), ["connected", "synced"]);
    // the first buffered diff set the best bid to 9.99999; the snapshot replaced it
    assert_book(&r, 1003, lv(9712345, 25000), lv(9712346, 30000));
}

#[test]
fn missed_event_causes_gap_and_resync() {
    let r = replay("gap");
    assert_eq!(r.event_kinds(), ["connected", "synced", "gap", "resync"]);
    assert_eq!(
        r.requests,
        [at(5), at(305)],
        "asked again at the diff after the gap"
    );
    assert_eq!((r.stats.gaps, r.stats.resyncs), (1, 1));
    assert_book(&r, 1009, lv(9712345, 15000), lv(9712346, 31000));
}

#[test]
fn duplicate_event_is_not_applied_twice() {
    let r = replay("duplicate");
    assert_eq!(r.event_kinds(), ["connected", "synced"]);
    assert_eq!(r.stats.depth_ignored, 1);
    assert_book(&r, 1006, lv(9712345, 30000), lv(9712346, 10000));
}

#[test]
fn out_of_order_events_cause_gap_and_resync() {
    let r = replay("out_of_order");
    assert_eq!(r.event_kinds(), ["connected", "synced", "gap", "resync"]);
    assert_book(&r, 1008, lv(9712345, 30000), lv(9712346, 10000));
}

#[test]
fn zero_quantity_removes_the_level() {
    let r = replay("zero_quantity");
    assert_book(&r, 1005, lv(9712344, 50000), lv(9712346, 30000));
    assert_eq!((r.stats.bbo_compared, r.stats.bbo_mismatches), (2, 0));
}

#[test]
fn reconnect_rebuilds_the_book() {
    let r = replay("reconnect");
    assert_eq!(
        r.event_kinds(),
        [
            "connected",
            "synced",
            "disconnected",
            "reconnect",
            "connected",
            "synced"
        ]
    );
    assert_eq!(
        (r.stats.reconnects, r.stats.resyncs, r.stats.gaps),
        (1, 0, 0)
    );
    assert_book(&r, 1012, lv(9712345, 10000), lv(9712346, 20000));
}

#[test]
fn broken_frame_is_logged_and_processing_continues() {
    let r = replay("broken_frame");
    assert_eq!(
        r.event_kinds(),
        ["connected", "synced", "undecodable", "gap", "resync"]
    );
    assert_eq!(r.stats.undecodable, 1);
    assert_book(&r, 1007, lv(9712345, 15000), lv(9712346, 30000));
}

#[test]
fn best_bid_and_ask_disagreeing_with_the_book_are_reported() {
    let r = replay("bbo_mismatch");
    assert_eq!(r.event_kinds(), ["connected", "synced", "bbo_mismatch"]);
    let (_, ev) = &r.events[2];
    assert_eq!(
        ev.to_string(),
        "bbo_mismatch update_id=1005 book=9712345x20000/9712346x40000 ticker=9712345x20000/9712346x41000 (steps)"
    );
}

#[test]
fn crossed_book_is_rebuilt() {
    let r = replay("crossed_book");
    assert_eq!(
        r.event_kinds(),
        ["connected", "synced", "invalid_book", "resync"]
    );
    assert_book(&r, 1006, lv(9712347, 1000), lv(9712360, 150000));
}

#[test]
fn server_shutdown_notice_is_logged() {
    let r = replay("server_shutdown");
    assert_eq!(r.event_kinds(), ["connected", "synced", "server_shutdown"]);
}

const ALL: [&str; 12] = [
    "normal_start",
    "stale_snapshot",
    "old_events_in_buffer",
    "gap",
    "duplicate",
    "out_of_order",
    "zero_quantity",
    "reconnect",
    "broken_frame",
    "bbo_mismatch",
    "crossed_book",
    "server_shutdown",
];

#[test]
fn every_fixture_is_covered() {
    let mut names: Vec<String> = fs::read_dir(fixture("x").parent().unwrap())
        .unwrap()
        .filter_map(|e| e.unwrap().file_name().into_string().ok())
        .filter_map(|n| n.strip_suffix(".jsonl").map(str::to_owned))
        .collect();
    names.sort();
    let mut all: Vec<&str> = ALL.to_vec();
    all.sort();
    assert_eq!(names, all);
}

#[test]
fn fixtures_are_canonical_text() {
    // packing and dumping again reproduces each fixture byte for byte
    for name in ALL {
        let original = fs::read_to_string(fixture(name)).unwrap();
        let mut bin = Vec::new();
        text::pack(original.as_bytes(), &mut bin).unwrap();
        let mut again = Vec::new();
        text::dump(&bin[..], &mut again).unwrap();
        assert_eq!(String::from_utf8(again).unwrap(), original, "{name}");
    }
}

#[test]
fn binary_and_text_forms_replay_identically() {
    let dir = tempfile::tempdir().unwrap();
    for name in ALL {
        let bin = dir.path().join(format!("{name}.frames"));
        text::pack_file(BufReader::new(File::open(fixture(name)).unwrap()), &bin).unwrap();
        let (a, b) = (replay(name), replay_file(&bin).unwrap());
        assert_eq!(b.form, InputForm::Binary);
        assert_eq!(
            (a.state, a.update_id, a.best_bid, a.best_ask),
            (b.state, b.update_id, b.best_bid, b.best_ask),
            "{name}"
        );
        assert_eq!(
            (&a.stats, &a.requests, &a.events),
            (&b.stats, &b.requests, &b.events),
            "{name}"
        );
    }
}

#[test]
fn truncated_recording_is_replayed_up_to_the_cut() {
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("cut.frames");
    text::pack_file(BufReader::new(File::open(fixture("gap")).unwrap()), &bin).unwrap();
    let bytes = fs::read(&bin).unwrap();
    let cut = dir.path().join("cut2.frames");
    fs::write(&cut, &bytes[..bytes.len() - 10]).unwrap();
    let r = replay_file(&cut).unwrap();
    assert!(matches!(r.ending, Ending::Truncated { .. }));
    assert_eq!(
        r.event_kinds(),
        ["connected", "synced", "gap", "resync"],
        "all but the last frame were replayed"
    );
    assert_eq!(r.update_id, 1008);
}

#[test]
fn damaged_recording_stops_at_the_damage() {
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("x.frames");
    text::pack_file(
        BufReader::new(File::open(fixture("normal_start")).unwrap()),
        &bin,
    )
    .unwrap();
    let mut bytes = fs::read(&bin).unwrap();
    let at = bytes.len() - 30; // inside the last record
    bytes[at] ^= 0x40;
    let bad = dir.path().join("bad.frames");
    fs::write(&bad, &bytes).unwrap();
    let r = replay_file(&bad).unwrap();
    assert!(matches!(
        r.ending,
        Ending::Corrupt {
            reason: "checksum mismatch",
            ..
        }
    ));
    let frames_in_fixture = fs::read_to_string(fixture("normal_start"))
        .unwrap()
        .lines()
        .count() as u64
        - 1;
    assert_eq!(
        r.stats.frames,
        frames_in_fixture - 1,
        "every frame but the damaged last one"
    );
}

#[test]
fn after_a_stop_the_summary_shows_the_last_valid_book() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stopped.jsonl");
    let mut text = fs::read_to_string(fixture("normal_start")).unwrap();
    text.push_str(&format!(
        "{{\"kind\":\"disconnected\",\"recv_ts\":{},\"aux\":1,\"payload\":\"stopped\"}}\n",
        at(400)
    ));
    fs::write(&path, text).unwrap();
    let r = replay_file(&path).unwrap();
    assert_eq!(
        r.state,
        State::Resyncing,
        "the book is not maintained after a stop"
    );
    let v = r.last_valid.unwrap();
    assert_eq!(
        (v.update_id, v.best_bid, v.best_ask),
        (1007, lv(9712346, 1000), lv(9712347, 90000))
    );
    let summary = r.to_string();
    assert!(
        summary.contains("final book    recording stopped; last valid at update 1007: bid 97123.46000000 x 0.01000000, ask 97123.47000000 x 0.90000000"),
        "{summary}"
    );
}
