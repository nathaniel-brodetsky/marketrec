# marketrec

Market-data recorder for Binance Spot. Goal: keep a synchronized order book, detect every
gap and write the data to Parquet files with a fixed schema.

Status: under construction. Commands exist but do nothing yet; this file lists only what
is implemented and tested.

## Build

```bash
cargo build --release
cargo test
```

## Commands

| Command  | Purpose                                                   | State           |
|----------|-----------------------------------------------------------|-----------------|
| `record` | record a symbol from Binance                              | not implemented |
| `replay` | feed recorded frames through the hot path, no network  | not implemented |
| `verify` | check a recorded day for continuity and consistency       | not implemented |
| `bench`  | performance experiments on a recorded day                 | not implemented |

## What is implemented

| Part | Where | Checked by |
|------|-------|------------|
| Decimal strings to integer steps, no floating point, no allocation | `src/decimal.rs` | unit tests and property tests |
| Decoder for the JSON combined stream: depth diffs, best bid and ask, trades, server shutdown notices | `src/json.rs` | unit tests on sample frames, property tests on random and truncated input |
| Reference order book (`BTreeMap`), invariants: positive prices, non-negative quantities, book not crossed | `src/book/` | unit tests, property test against a hash-map model |

## License

MIT
