# marketrec

Market-data recorder for Binance Spot. Goal: keep a synchronized order book, detect every
gap and write the data to Parquet files with a fixed schema.

Status: under construction. Recording the JSON feed works end to end: a live run writes
frames files and Parquet tables, replaying the frames reproduces the live result exactly,
and `verify` checks a recorded day, down to rebuilding every table from the frames and
comparing row for row. The binary SBE feed and the performance work are next. This file
lists only what is implemented and tested.

## Data

Every recording keeps two forms of the same data:

- **frames** (`<day>/frames/*.frames`): the raw input of the decoder, byte for byte as
  received, with receive times. The primary record. Format: [`docs/FRAMES.md`](docs/FRAMES.md).
- **tables** (`<day>/<table>/part-NNN.parquet` and `manifest.json`): depth diffs, best bid
  and ask, trades, the top 20 levels every 100 ms and the quality journal, in integer
  steps. Derived from the frames by the same code live and offline. Contract:
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
PASS rebuild    every table rebuilt from the day's 1 frames file is identical, row for row
result      PASS
```

## Commands

| Command | Purpose | State |
|---------|---------|-------|
| `replay` | feed recorded frames through the live code without network; summary and quality events | implemented |
| `frames dump`, `frames pack` | convert frames files to readable JSON Lines and back | implemented |
| `info` | trading rules of a symbol from Binance (tick size, step size, status) | implemented |
| `snapshot` | fetch a depth snapshot and print its best levels, decoded with the symbol's rules | implemented |
| `listen` | connect to the live stream for a while and count messages per stream; records nothing | implemented |
| `record` | record a symbol from Binance into hourly frames files and Parquet tables until Ctrl+C, with status lines and quality events | implemented (JSON feed) |
| `convert` | build the Parquet tables from frames files, exactly as `record` does live | implemented |
| `verify` | check a recorded day: manifest against files, continuity of the update stream re-checked from the tables alone, and a rebuild of every table from the frames compared row for row; exit 1 on failure | implemented |
| `bench` | performance experiments on a recorded day | not yet |

## What is implemented

| Part | Where | Checked by |
|------|-------|------------|
| Decimal strings to integer steps, no floating point, no allocation | `src/decimal.rs` | unit tests and property tests |
| Decoder for the JSON combined stream: depth diffs, best bid and ask, trades, server shutdown notices | `src/json.rs` | unit tests on sample frames, property tests on random and truncated input |
| Reference order book (`BTreeMap`), invariants: positive prices, non-negative quantities, book not crossed | `src/book/` | unit tests, property test against a hash-map model |
| Book synchronization (snapshot plus diffs) as Binance documents it: stale snapshots, gaps, duplicates, out-of-order diffs, reconnects | `src/sync.rs` | unit test per case; property test with dropped, duplicated and reordered diffs: a synced book always equals the exchange book |
| Frames file: raw hot-path input (messages, snapshots, connection events) with receive times; CRC-32 per record, atomic write, specified in `docs/FRAMES.md` | `src/frames/` | round trips, every truncation point, random bit flips are always detected |
| Decoding pipeline: frames to events, book sync, cross-check of the book against the exchange's best bid and ask at equal update IDs, counters including time with a valid book | `src/pipeline.rs` | scenario tests per case, determinism test, random frames never panic |
| Replay of recorded frames through the live code, with a summary and the quality events; truncated files replay up to the cut, damaged ones stop at the damage. Snapshot requests are named by the receive time of the frame that caused them, so a replay that starts in the middle of a recording (one hourly file) joins the live result at the next connection | `src/replay.rs`, `src/sync.rs` | one integration test per scenario in `tests/fixtures`; text and binary forms give identical results; a replay from the middle equals the live tables from the next connection on |
| REST client for symbol rules and depth snapshots: timeouts, HTTP errors with the exchange's message, `Retry-After` on 429 and 418, used IP weight; bodies kept verbatim for recording | `src/net/rest.rs` | tests against a local HTTP server: success, invalid symbol, rate limit, missing filter, timeout, refused connection |
| WebSocket session: link events and messages as frames with receive times; pings answered; silent connection dropped; reconnect with exponential backoff and jitter; immediate reconnect on the exchange's shutdown notice | `src/net/ws.rs` | tests against a local WebSocket server: close and reconnect, ping and pong, stale link, shutdown notice, refused connection, consumer gone |
| Parquet tables per `SCHEMA.md`: `depth_diff`, `book_ticker`, `trades`, `book_l20` (sampled every 100 ms of receive time, only while the book is valid), `quality`; hourly parts written atomically; a day manifest with row counts, files, quality counts and the share of time with a valid book, merged across runs | `src/tables/` | rows checked against a simulated exchange (every level, trade and sampled book), hour and midnight boundaries, a gap leaves no book rows, merging runs, refusing mixed data; tables from the live recorder equal `convert` of its frames; CI reads them back with pyarrow (`scripts/check_tables.py`) |
| Verification of a recorded day: six checks, each PASS, WARN or FAIL. The continuity check needs no frames: it walks the quality journal and the update IDs and fails on an update that skips IDs without a recorded gap, and on a `book_l20` row outside a valid interval, at another update ID than the stream or not a sound book. The rebuild runs the frames of the day and the day before through the pipeline and compares every table, streaming. A crashed run is a warning that names the fix | `src/verify.rs` | a clean live run passes everything; tampered tables are caught: one lost update, a changed price, a book row at the wrong update, a crossed book, a removed resync, a manifest that disagrees, a missing part; crashed runs, unlisted parts and missing frames of the day before give warnings; mutation-tested |
| Recorder: network, decode and writer threads joined by never-blocking SPSC queues (a full queue drops frames and leaves an `overflow` marker); snapshot requests answered by REST with pacing, `Retry-After` and weight limits; hourly frames files; the writer derives the tables from what it stores; clean stop on Ctrl+C or SIGTERM | `src/record/` | end-to-end test against local servers: a live run and the replay of its files give identical counters, and its tables equal `convert` of its files |

## License

MIT
