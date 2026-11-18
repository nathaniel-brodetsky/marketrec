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
rows          5 depth updates, 1 best bid and ask updates, 0 trades
continuity    1 gaps, 1 resyncs, 0 reconnects; valid book 42.0% of the time from the first frame to the last
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
| `rebuild` | derive a recorded day's tables again from its frames, in place, and verify it: after a crash, or when the rules of derivation have changed | implemented |
| `import tardis` | a day of Tardis.dev's raw Binance Spot recording, written as frames and derived into tables like a recording of our own (below) | implemented |

## What is implemented

| Part | Where | Checked by |
|------|-------|------------|
| Decimal strings to integer steps, no floating point, no allocation | `src/decimal.rs` | unit tests and property tests |
| Decoder for the JSON combined stream: depth diffs, best bid and ask, trades, server shutdown notices; one pass over each frame, strings borrowed, depth levels parsed straight into reused vectors, so decoding does not allocate | `src/json.rs` | unit tests on sample frames, property tests on random and truncated input |
| Reference order book (`BTreeMap`), invariants: positive prices, non-negative quantities, book not crossed | `src/book/` | unit tests, property test against a hash-map model |
| A second order book on sorted vectors, the best level last: a galloping search from the best end, memory reserved once, no allocation per update, snapshots built in one sort. Measured on a recorded BTCUSDT day it is slower at the median than the reference book (0.84 against 0.47 µs per update), because real updates reach deep into the 5000-level book where the vector moves many levels; the recorder uses the reference book, and `bench` keeps measuring both | `src/book/ladder.rs` | property test: any sequence of sets, diffs, snapshots and clears, valid or not, leaves it holding exactly what the reference book holds and failing where it fails; the whole pipeline gives identical output with either book; deterministic tests of the search over a 5000-level book; mutation-tested |
| Book synchronization (snapshot plus diffs) as Binance documents it: stale snapshots, gaps, duplicates, out-of-order diffs, reconnects | `src/sync.rs` | unit test per case; property test with dropped, duplicated and reordered diffs: a synced book always equals the exchange book |
| Compression of finished hourly frames files with zstd on a thread of its own: the copy is read back and compared before the original is removed; every reader opens both forms | `src/frames/zst.rs` | round trip, a target that exists is never overwritten, a flipped byte fails the checksum, nothing is left behind on failure; the live recording test checks its files are compressed and replays them |
| Frames file: raw hot-path input (messages, snapshots, connection events) with receive times; CRC-32 per record, atomic write, specified in `docs/FRAMES.md` | `src/frames/` | round trips, every truncation point, random bit flips are always detected |
| Decoding pipeline: frames to events, book sync, cross-check of the book against the exchange's best bid and ask at equal update IDs, counters including time with a valid book | `src/pipeline.rs` | scenario tests per case, determinism test, random frames never panic |
| Replay of recorded frames through the live code, with a summary and the quality events; truncated files replay up to the cut, damaged ones stop at the damage. Snapshot requests are named by the receive time of the frame that caused them, so a replay that starts in the middle of a recording (one hourly file) joins the live result at the next connection | `src/replay.rs`, `src/sync.rs` | one integration test per scenario in `tests/fixtures`; text and binary forms give identical results; a replay from the middle equals the live tables from the next connection on |
| REST client for symbol rules and depth snapshots: timeouts, HTTP errors with the exchange's message, `Retry-After` on 429 and 418, used IP weight; bodies kept verbatim for recording | `src/net/rest.rs` | tests against a local HTTP server: success, invalid symbol, rate limit, missing filter, timeout, refused connection |
| WebSocket session: link events and messages as frames with receive times; pings answered; silent connection dropped; reconnect with exponential backoff and jitter. A connection is replaced make-before-break an hour before Binance's 24-hour limit and on the exchange's shutdown notice: the successor connects while the old one still delivers, and the old one closes once the successor has delivered for 5 s, so the book carries on without a gap or a new snapshot (stage 5 of the plan) | `src/net/ws.rs` | tests against a local WebSocket server: close and reconnect, ping and pong, stale link, refused connection, consumer gone; takeover on a shutdown notice, at the age limit, after a failed successor; a recording whose connection is replaced midway keeps one snapshot, no gap and passes `verify` |
| Parquet tables per `SCHEMA.md`: `depth_diff`, `book_ticker`, `trades`, `book_l20` (sampled every 100 ms of receive time, only while the book is valid), `quality`, `link_rtt`; hourly parts written atomically; a day manifest with row counts, files, quality counts and the share of time with a valid book, merged across runs | `src/tables/` | rows checked against a simulated exchange (every level, trade and sampled book), hour and midnight boundaries, a gap leaves no book rows, merging runs, refusing mixed data; tables from the live recorder equal `convert` of its frames; CI reads them back with pyarrow (`scripts/check_tables.py`) |
| Verification of a recorded day: seven checks, each PASS, WARN or FAIL. The continuity check needs no frames: it walks the quality journal and the update IDs and fails on an update that skips IDs without a recorded gap, and on a `book_l20` row outside a valid interval, at another update ID than the stream or not a sound book. The rebuild runs the frames of the day and the day before through the pipeline and compares every table, streaming. A crashed run, or tables derived by older rules (each run records its `derivation`), is a warning that names the fix: `marketrec rebuild` | `src/verify.rs` | a clean live run passes everything; tampered tables are caught: one lost update, a changed price, a book row at the wrong update, a crossed book, a removed resync, a manifest that disagrees, a missing part; crashed runs, unlisted parts and missing frames of the day before give warnings; mutation-tested |
| Freshness and the link: the lag of every depth update behind its exchange time, in a histogram with three significant digits that adds up exactly across runs and days; `late` and `on_time` quality events when updates arrive more than 1 s late and within 0.5 s again; time in late intervals per day; lag in status lines and summaries, and a `freshness` check in `verify` that warns about stale intervals and about a clock behind the exchange. The session sends its own ping every 5 s and records the round trip (`rtt` frames, `link_rtt` table, no clock agreement needed); `verify` compares round trips inside and outside late intervals and says whether the link was slow or the data was late before it, at the exchange | `src/timing.rs` | thresholds and hysteresis, histogram merge equals one histogram (property test), a simulated 3 s stall gives one late interval of exactly 5 s in quality, manifest and `verify`; late time split at midnight; pings answered by a local server keep a quiet link alive and come back as round trips; a simulated stall with a normal and with a slow round trip gets the two different diagnoses; mutation-tested |
| Several connections at once: the pipeline knows which connections deliver data; while a new one takes over from an old one, every message is kept from whichever connection brought it first and the copies are dropped and counted; closing one connection while another is open leaves the book valid | `src/pipeline.rs` | a takeover in the middle of a synced session; property test: two complete copies of the stream, batched differently and interleaved in any order, give the exchange's book with every update forwarded once and no gap; tables of a takeover equal those of one connection; mutation-tested |
| Benchmark of the decode path and a counting allocator (per thread, installed in the binary and the unit tests): decoding and the pipeline allocate nothing per frame once their buffers have grown; with the ladder book a frame costs no allocation at all, the reference book allocates a tree node for each new price level | `src/bench.rs`, `src/alloc_count.rs` | a test asserts zero allocations over thousands of frames with late intervals and probes; `bench` on a recorded day reports every stage and checks that both books end identical |
| Operation: free space on the data disk checked before the start and with every status line, with a clean stop and exit code 3 below the limit; systemd units for a recorder per symbol (restart on failure but not on a full disk, hardened) and a nightly `verify` of the day before | `src/record/disk.rs`, `deploy/` | a recording refused on a full disk writes nothing; a disk that fills during a recording stops it early and cleanly, and its day passes `verify`; units checked with `systemd-analyze verify` |
| Recorder: network, decode and writer threads joined by never-blocking SPSC queues (a full queue drops frames and leaves an `overflow` marker); snapshot requests answered by REST with pacing, `Retry-After` and weight limits; hourly frames files; the writer derives the tables from what it stores; clean stop on Ctrl+C or SIGTERM | `src/record/` | end-to-end test against local servers: a live run and the replay of its files give identical counters, and its tables equal `convert` of its files |

## Days recorded by Tardis.dev

Tardis.dev records the same Binance Spot streams (`depth@100ms`, `bookTicker`, `trade`, in
microseconds since 2025-02-28) and the REST depth snapshots it fetches, from Tokyo, and
serves the first day of each month without a key. `marketrec import tardis` turns such a
day into frames and from there into the same tables, so that everything downstream,
`verify` and the research side included, reads it like a day of our own:

```bash
cargo run --release -- import tardis --date 2026-11-01 --symbol BTCUSDT --out data
```

- Each message becomes a text frame byte for byte, with Tardis's receive time; messages
  they received at the same time are spread one nanosecond apart, so that every frame has
  a time of its own as in a recording of ours. An empty
  line in their data, a lost connection, closes one connection and opens the next; the
  end of the day closes the last with reason `stopped`, one nanosecond after its last
  message.
- Their snapshot of a connection becomes the answer to the request our pipeline makes for
  it, which the connection's first depth update causes. A snapshot received before that
  update follows it one nanosecond later. Our book synchronization then checks it like
  any other: a snapshot that does not fit the updates is reported, not forced.
- A failed request is tried again for about four minutes, waiting longer each time. If the
  download fails anyway, the frames written so far are removed, so the day can simply be
  imported again.
- The steps come from Binance's current rules unless `--price-step` and `--qty-step` are
  given. Other days need a key, read from the environment variable `TARDIS_API_KEY`.
- The receive times are theirs: the lag in the manifest is that of a machine in Tokyo, and
  there are no round trips of a link of ours. The raw data stays in `data/`, which is not
  part of the repository.

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
  them as a warning, and `marketrec rebuild --day <dir>` derives that day's tables again
  from the frames.
- **An upgrade.** Each run in a manifest records the version of the rules that derived its
  tables. When `verify` finds a difference on a day derived by older rules, it says so and
  is a warning, not a failure; `marketrec rebuild` derives the day with the current rules.
- **The disk.** The recorder does not start, and stops cleanly, below 2 GB free
  (`--min-free-gb`). It then exits with code 3, which the unit does not restart.
- **Every night.** `marketrec-verify@` checks the day before; the report is in
  `journalctl -u marketrec-verify@BTCUSDT`.

## Performance

`bench` on a recorded BTCUSDT day (two runs, 221 371 frames), release build, on a desktop:

```text
decode        depth 11956 x 1.75 µs, book ticker 154058 x 0.25 µs, trade 55232 x 0.26 µs (mean per message)
pipeline      reference    2360485 frames/s; per frame p50 0.27 µs, p99 3.69 µs, p99.9 25.29 µs, max 877.47 µs; 0.052 allocations per frame
              ladder       2246529 frames/s; per frame p50 0.27 µs, p99 4.32 µs, p99.9 26.07 µs, max 607.04 µs; 0.001 allocations per frame
book updates  11924 diffs of 29.2 levels after 2 snapshots; reference p50 0.47 µs, p99 15.04 µs, p99.9 30.54 µs, max 50.03 µs; ladder p50 0.84 µs, p99 17.29 µs, p99.9 37.20 µs, max 88.62 µs
              the books end identical
```

What it says: one core runs about 2.3 million frames a second through the pipeline, and the
live feed brings about 110 to 260 a second, a margin of four orders of magnitude. Decoding the
JSON of a depth update is the largest cost per message. The ladder book, which had been a
quarter faster on synthetic updates near the best price (`scripts/synthetic_day.py`), is
slower on real ones, which reach deep into the book; the recorder uses the reference book.
The maximum per frame is a 5000-level snapshot.

## License

MIT
