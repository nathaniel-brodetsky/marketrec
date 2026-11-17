# marketrec

Market-data recorder for Binance Spot. Goal: keep a synchronized order book, detect every
gap and write the data to Parquet files with a fixed schema.

Status: under construction. Recording the JSON feed works end to end and is meant to run
for days: a live run writes frames files and Parquet tables, replaces its connection
before the exchange's 24-hour limit without a gap, measures how late the data arrives and
why, and `verify` checks a recorded day down to rebuilding every table from the frames.
The binary SBE feed is next. This file lists only what is implemented and tested.

## Data

Every recording keeps two forms of the same data:

- **frames** (`<day>/frames/*.frames.zst`): the raw input of the decoder, byte for byte as
  received, with receive times, compressed once each hour is finished. The primary record.
  Format: [`docs/FRAMES.md`](docs/FRAMES.md).
- **tables** (`<day>/<table>/part-NNN.parquet` and `manifest.json`): depth diffs, best bid
  and ask, trades, the top 20 levels every 100 ms, the quality journal and the link's
  round trips, in integer steps. Derived from the frames by the same code live and offline. Contract:
  [`SCHEMA.md`](SCHEMA.md), checked against the code by a test.

## Build

```bash
cargo build --release
cargo test
```

## Try it without network

Twelve recorded scenarios (gaps, reconnects, stale snapshots, a broken frame, a crossed
book and others) are in `tests/fixtures`, readable as text:

```bash
cargo run -- replay --input tests/fixtures/gap.jsonl --events
```

```text
1795219200000000000 connected conn=1
1795219200180000000 synced update_id=1003
1795219200305000000 gap expected=1006 got=1007
1795219200360000000 resync update_id=1008

symbol        BTCUSDT, price step 0.01000000, qty step 0.00001000, feed times in microseconds
input         text frames file, 9 frames over 0.405 s
depth         5 diffs: 2 applied, 0 ignored, 2 buffered, 1 started a resync
book ticker   1 messages, 1 compared with the book, 0 mismatches
trades        0
snapshots     2 requested, 2 received, 0 failed
continuity    1 gaps, 1 resyncs, 0 reconnects, 0 undecodable frames
valid book    42.0% of the time
lag           depth updates p50 2 ms, p99 2 ms, max 2 ms
final book    update 1009: bid 97123.45000000 x 0.15000000, ask 97123.46000000 x 0.31000000
end           clean
```

The same file as Parquet tables:

```bash
cargo run -- convert --input tests/fixtures/gap.jsonl --out /tmp/demo
```

```text
converted     1 file, 9 frames of BTCUSDT
rows          5 depth diffs, 1 book tickers, 0 trades
continuity    1 gaps, 1 resyncs, 0 reconnects; valid book 42.0% of the time
written       5 files
```

`book_l20` in `/tmp/demo/binance/BTCUSDT/2026-11-21` then has no row between the gap at
.305 s and the resync at .360 s. With the frames next to the tables, as the recorder leaves
them, the day can be verified:

```bash
mkdir /tmp/demo/binance/BTCUSDT/2026-11-21/frames
cp tests/fixtures/gap.jsonl /tmp/demo/binance/BTCUSDT/2026-11-21/frames/
cargo run -- verify --day /tmp/demo/binance/BTCUSDT/2026-11-21
```

```text
day         /tmp/demo/binance/BTCUSDT/2026-11-21
PASS manifest   BTCUSDT 2026-11-21, schema 1, steps 0.01000000 and 0.00001000, 1 run, valid book 42.0% of connected time
PASS parts      4 parts, all listed in the manifest, readable, with the schema, symbol and steps it names
PASS rows       depth_diff 5, book_ticker 1, trades 0, book_l20 4, quality 4
PASS hours      every part holds one hour of this day, receive times in order
PASS continuity 2 updates continue the book in 2 valid intervals (1 gap recorded, none missed, 0 already applied); 4 book rows agree with the updates
PASS freshness  depth updates arrived p50 2 ms, p99 2 ms, max 2 ms after their exchange time
PASS rebuild    every table rebuilt from the day's 1 frames file is identical, row for row
result      PASS
```

## Commands

| Command | Purpose | State |
|---------|---------|-------|
| `replay` | feed recorded frames through the live code without network; summary and quality events | implemented |
| `frames dump`, `frames pack` | convert frames files to readable JSON Lines and back | implemented |
| `frames compress` | compress finished frames files to `.frames.zst`, each checked byte for byte before the original goes | implemented |
| `info` | trading rules of a symbol from Binance (tick size, step size, status) | implemented |
| `snapshot` | fetch a depth snapshot and print its best levels, decoded with the symbol's rules | implemented |
| `listen` | connect to the live stream for a while and count messages per stream; records nothing | implemented |
| `record` | record a symbol from Binance into hourly frames files and Parquet tables until Ctrl+C, with status lines and quality events | implemented (JSON feed) |
| `convert` | build the Parquet tables from frames files, exactly as `record` does live | implemented |
| `verify` | check a recorded day: manifest against files, continuity of the update stream re-checked from the tables alone, freshness, and a rebuild of every table from the frames compared row for row; exit 1 on failure | implemented |
| `bench` | measure the decode path on a recorded day: the decoder per kind of message, the whole pipeline with each book (throughput, time per frame, allocations per frame), the book updates alone | implemented |

## What is implemented

| Part | Where | Checked by |
|------|-------|------------|
| Decimal strings to integer steps, no floating point, no allocation | `src/decimal.rs` | unit tests and property tests |
| Decoder for the JSON combined stream: depth diffs, best bid and ask, trades, server shutdown notices; one pass over each frame, strings borrowed, depth levels parsed straight into reused vectors, so decoding does not allocate | `src/json.rs` | unit tests on sample frames, property tests on random and truncated input |
| Reference order book (`BTreeMap`), invariants: positive prices, non-negative quantities, book not crossed | `src/book/` | unit tests, property test against a hash-map model |
| Fast order book on sorted vectors, the best level last: binary search, short moves near the top, memory reserved once; snapshots built in one sort. The recorder's hot path uses it, the writer derives the tables with the reference book, so every live run compares the two | `src/book/ladder.rs` | property test: any sequence of sets, diffs, snapshots and clears, valid or not, leaves it holding exactly what the reference book holds and failing where it fails; the whole pipeline gives identical output with either book on a long session with a gap and a reconnect; mutation-tested |
| Book synchronization (snapshot plus diffs) as Binance documents it: stale snapshots, gaps, duplicates, out-of-order diffs, reconnects | `src/sync.rs` | unit test per case; property test with dropped, duplicated and reordered diffs: a synced book always equals the exchange book |
| Compression of finished hourly frames files with zstd on a thread of its own: the copy is read back and compared before the original is removed; every reader opens both forms | `src/frames/zst.rs` | round trip, a target that exists is never overwritten, a flipped byte fails the checksum, nothing is left behind on failure; the live recording test checks its files are compressed and replays them |
| Frames file: raw hot-path input (messages, snapshots, connection events) with receive times; CRC-32 per record, atomic write, specified in `docs/FRAMES.md` | `src/frames/` | round trips, every truncation point, random bit flips are always detected |
| Decoding pipeline: frames to events, book sync, cross-check of the book against the exchange's best bid and ask at equal update IDs, counters including time with a valid book | `src/pipeline.rs` | scenario tests per case, determinism test, random frames never panic |
| Replay of recorded frames through the live code, with a summary and the quality events; truncated files replay up to the cut, damaged ones stop at the damage. Snapshot requests are named by the receive time of the frame that caused them, so a replay that starts in the middle of a recording (one hourly file) joins the live result at the next connection | `src/replay.rs`, `src/sync.rs` | one integration test per scenario in `tests/fixtures`; text and binary forms give identical results; a replay from the middle equals the live tables from the next connection on |
| REST client for symbol rules and depth snapshots: timeouts, HTTP errors with the exchange's message, `Retry-After` on 429 and 418, used IP weight; bodies kept verbatim for recording | `src/net/rest.rs` | tests against a local HTTP server: success, invalid symbol, rate limit, missing filter, timeout, refused connection |
| WebSocket session: link events and messages as frames with receive times; pings answered; silent connection dropped; reconnect with exponential backoff and jitter. A connection is replaced make-before-break an hour before Binance's 24-hour limit and on the exchange's shutdown notice: the successor connects while the old one still delivers, and the old one closes once the successor has delivered for 5 s, so the book carries on without a gap or a new snapshot (stage 5 of the plan) | `src/net/ws.rs` | tests against a local WebSocket server: close and reconnect, ping and pong, stale link, refused connection, consumer gone; takeover on a shutdown notice, at the age limit, after a failed successor; a recording whose connection is replaced midway keeps one snapshot, no gap and passes `verify` |
| Parquet tables per `SCHEMA.md`: `depth_diff`, `book_ticker`, `trades`, `book_l20` (sampled every 100 ms of receive time, only while the book is valid), `quality`, `link_rtt`; hourly parts written atomically; a day manifest with row counts, files, quality counts and the share of time with a valid book, merged across runs | `src/tables/` | rows checked against a simulated exchange (every level, trade and sampled book), hour and midnight boundaries, a gap leaves no book rows, merging runs, refusing mixed data; tables from the live recorder equal `convert` of its frames; CI reads them back with pyarrow (`scripts/check_tables.py`) |
| Verification of a recorded day: seven checks, each PASS, WARN or FAIL. The continuity check needs no frames: it walks the quality journal and the update IDs and fails on an update that skips IDs without a recorded gap, and on a `book_l20` row outside a valid interval, at another update ID than the stream or not a sound book. The rebuild runs the frames of the day and the day before through the pipeline and compares every table, streaming. A crashed run is a warning that names the fix | `src/verify.rs` | a clean live run passes everything; tampered tables are caught: one lost update, a changed price, a book row at the wrong update, a crossed book, a removed resync, a manifest that disagrees, a missing part; crashed runs, unlisted parts and missing frames of the day before give warnings; mutation-tested |
| Freshness and the link: the lag of every depth update behind its exchange time, in a histogram with three significant digits that adds up exactly across runs and days; `late` and `on_time` quality events when updates arrive more than 1 s late and within 0.5 s again; time in late intervals per day; lag in status lines and summaries, and a `freshness` check in `verify` that warns about stale intervals and about a clock behind the exchange. The session sends its own ping every 5 s and records the round trip (`rtt` frames, `link_rtt` table, no clock agreement needed); `verify` compares round trips inside and outside late intervals and says whether the link was slow or the data was late before it, at the exchange | `src/timing.rs` | thresholds and hysteresis, histogram merge equals one histogram (property test), a simulated 3 s stall gives one late interval of exactly 5 s in quality, manifest and `verify`; late time split at midnight; pings answered by a local server keep a quiet link alive and come back as round trips; a simulated stall with a normal and with a slow round trip gets the two different diagnoses; mutation-tested |
| Several connections at once: the pipeline knows which connections deliver data; while a new one takes over from an old one, every message is kept from whichever connection brought it first and the copies are dropped and counted; closing one connection while another is open leaves the book valid | `src/pipeline.rs` | a takeover in the middle of a synced session; property test: two complete copies of the stream, batched differently and interleaved in any order, give the exchange's book with every update forwarded once and no gap; tables of a takeover equal those of one connection; mutation-tested |
| Benchmark of the decode path and a counting allocator (per thread, installed in the binary and the unit tests): the hot path is shown to make no allocation per frame once warmed up, whatever the messages, lags and round trips | `src/bench.rs`, `src/alloc_count.rs` | a test asserts zero allocations over thousands of frames with late intervals and probes; `bench` on a recorded day reports every stage and checks that both books end identical |
| Operation: free space on the data disk checked before the start and with every status line, with a clean stop and exit code 3 below the limit; systemd units for a recorder per symbol (restart on failure but not on a full disk, hardened) and a nightly `verify` of the day before | `src/record/disk.rs`, `deploy/` | a recording refused on a full disk writes nothing; a disk that fills during a recording stops it early and cleanly, and its day passes `verify`; units checked with `systemd-analyze verify` |
| Recorder: network, decode and writer threads joined by never-blocking SPSC queues (a full queue drops frames and leaves an `overflow` marker); snapshot requests answered by REST with pacing, `Retry-After` and weight limits; hourly frames files; the writer derives the tables from what it stores; clean stop on Ctrl+C or SIGTERM | `src/record/` | end-to-end test against local servers: a live run and the replay of its files give identical counters, and its tables equal `convert` of its files |

## Running 24/7

`deploy/` has systemd units: one recorder per symbol, and a timer that verifies the day
before every night at 00:30 UTC.

```bash
cargo build --release
sudo install -m 755 target/release/marketrec /usr/local/bin/
sudo useradd --system --home-dir /var/lib/marketrec --create-home --shell /usr/sbin/nologin marketrec
sudo install -m 644 deploy/marketrec@.service deploy/marketrec-verify@.service deploy/marketrec-verify@.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now marketrec@BTCUSDT marketrec-verify@BTCUSDT.timer
journalctl -u marketrec@BTCUSDT -f
```

What happens over days:

- **The 24-hour limit and shutdown notices.** The connection is replaced make-before-break,
  with no gap and no new snapshot.
- **Hourly files.** Each finished frames file is compressed and checked; the tables get one
  part per hour, and `manifest.json` is updated when the day ends.
- **A crash or a lost network.** systemd restarts the recorder after 10 s, and the new run
  adds to the day it starts in. The crashed run's unfinished files remain; `verify` reports
  them as a warning, and `marketrec convert` rebuilds that day's tables from the frames.
- **The disk.** The recorder does not start, and stops cleanly, below 2 GB free
  (`--min-free-gb`). It then exits with code 3, which the unit does not restart.
- **Every night.** `marketrec-verify@` checks the day before; the report is in
  `journalctl -u marketrec-verify@BTCUSDT`.

## Performance

`bench` on a synthetic 10-minute day shaped like the first live BTCUSDT run
(`scripts/synthetic_day.py`), release build, in a 2-core cloud container:

```text
decode        depth 6000 x 3.39 µs, book ticker 102000 x 0.42 µs, trade 42000 x 0.44 µs (mean per message)
pipeline      reference    1523968 frames/s; per frame p50 0.50 µs, p99 6.59 µs, p99.9 12.52 µs, max 1.6 ms; 0.022 allocations per frame
              ladder       1602094 frames/s; per frame p50 0.44 µs, p99 5.36 µs, p99.9 13.04 µs, max 945.33 µs; 0.000 allocations per frame
book updates  5999 diffs of 35.3 levels after 1 snapshot; reference p50 2.14 µs, p99 2.88 µs, p99.9 16.29 µs, max 29.57 µs; ladder p50 1.62 µs, p99 2.12 µs, p99.9 14.56 µs, max 20.87 µs
              the books end identical
```

What it says: one core decodes about 1.5 million frames a second against about 260 a second
live, so the recorder has a margin of several thousand. Decoding the JSON of a depth update
is the largest cost per message, several times a book update; the ladder book is about a
quarter faster than the reference book on the updates themselves. The maximum per frame is
the 5000-level snapshot. Runs on this container vary by about 10%; numbers from a desktop
will differ, and `bench` on a real recorded day is the measurement that counts.

## License

MIT
