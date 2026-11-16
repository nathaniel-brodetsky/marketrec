# marketrec

Market-data recorder for Binance Spot. Goal: keep a synchronized order book, detect every
gap and write the data to Parquet files with a fixed schema.

Status: under construction. The offline core works: decoding, book synchronization, the
recording format and replay. Live recording from the network is not built yet. This file
lists only what is implemented and tested.

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

## Commands

| Command | Purpose | State |
|---------|---------|-------|
| `replay` | feed recorded frames through the live code without network; summary and quality events | implemented |
| `frames dump`, `frames pack` | convert frames files to readable JSON Lines and back | implemented |
| `record` | record a symbol from Binance | not yet |
| `verify` | check a recorded day for continuity and consistency | not yet |
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
| Replay of recorded frames through the live code, with a summary and the quality events; truncated files replay up to the cut, damaged ones stop at the damage | `src/replay.rs` | one integration test per scenario in `tests/fixtures`; text and binary forms give identical results |

## License

MIT
