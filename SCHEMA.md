# Data contract, version 1

`marketrec` writes these files; the research side only reads them. The version is stored in
every Parquet file (key `marketrec.schema_version`) and in every `manifest.json`. A change
to any table below is a new version.

## Layout

```text
data/
  binance/
    BTCUSDT/
      2026-11-21/
        frames/13-05-12.frames          raw input of the decoder, see docs/FRAMES.md
        depth_diff/part-000.parquet
        book_ticker/part-000.parquet
        trades/part-000.parquet
        book_l20/part-000.parquet
        quality/part-000.parquet
        link_rtt/part-000.parquet
        manifest.json
```

- A day directory holds data whose receive time falls on that UTC date.
- A part covers at most one UTC hour. It is written as the hidden file
  `.part-NNN.parquet.tmp` and renamed when complete, so tools that read a whole directory
  never see an unfinished part. A finished part never changes.
- Several runs may write to one day, one at a time: each adds parts with the next free
  number and merges its counts into `manifest.json`. A run refuses a day directory whose
  manifest names another symbol, other steps or another schema version.
- The tables are derived from the frames files. `marketrec convert` rebuilds them from frames
  at any time; the live recorder produces exactly the same rows.

## Common rules

- Times are int64 nanoseconds since the Unix epoch, UTC. `recv_ts` is the local receive time
  of the message; exchange times (`event_ts`, `trade_ts`) come from Binance in microseconds
  and are scaled to nanoseconds.
- Prices and quantities are int64 counts of the symbol's steps: price = `price` x
  `price_step`, quantity = `qty` x `qty_step`. The steps are in `manifest.json` and in the
  Parquet key-value metadata. No value passes through floating point.
- Update and trade IDs are int64.
- `source` is the feed the row came from: `json` (and `sbe` once that feed is recorded).
- Between a `gap`, `reconnect`, `overflow` or `invalid_book` event and the next `resync` in
  `quality`, the book was not valid: analysis must exclude that interval, not bridge it.
  `book_l20` has no rows there.

## Tables

### depth_diff

One row per changed price level of a depth update (`<symbol>@depth@100ms`). All rows of
one update share `recv_ts`, `first_update_id` and `final_update_id`. Every update received
is here, including those received while the book was being rebuilt; an update without
levels is one row with `side` 0, because it still moves the update IDs on. While a new
connection takes over from an old one, both deliver the same stream for a while; every
table keeps the copy that arrived first, and the manifest counts the others in
`duplicates`.

| column | type | meaning |
|--------|------|---------|
| `recv_ts` | int64 | local receive time, ns |
| `event_ts` | int64 | exchange event time, ns |
| `first_update_id` | int64 | first update ID in the message (`U`) |
| `final_update_id` | int64 | last update ID in the message (`u`) |
| `side` | int8 | 1 bid, -1 ask, 0 for an update without levels (then `price` and `qty` are 0) |
| `price` | int64 | price in steps |
| `qty` | int64 | new quantity at that price in steps; 0 removes the level |
| `source` | string | feed |

### book_ticker

Best bid and ask after the order book update `update_id` (`<symbol>@bookTicker`).

| column | type | meaning |
|--------|------|---------|
| `recv_ts` | int64 | local receive time, ns |
| `update_id` | int64 | order book update ID, same sequence as `depth_diff` |
| `bid_px` | int64 | best bid price in steps |
| `bid_qty` | int64 | best bid quantity in steps |
| `ask_px` | int64 | best ask price in steps |
| `ask_qty` | int64 | best ask quantity in steps |
| `source` | string | feed |

### trades

One trade (`<symbol>@trade`).

| column | type | meaning |
|--------|------|---------|
| `recv_ts` | int64 | local receive time, ns |
| `event_ts` | int64 | exchange event time, ns |
| `trade_ts` | int64 | trade time, ns |
| `trade_id` | int64 | trade ID |
| `price` | int64 | price in steps |
| `qty` | int64 | quantity in steps |
| `is_buyer_maker` | bool | true if the buyer's order was resting, so the aggressor sold |
| `source` | string | feed |

### book_l20

The best 20 levels per side of the synchronized local book, sampled at the first frame of
every 100 ms of receive time while the book is valid. Arrays are best first; missing levels
are 0.

| column | type | meaning |
|--------|------|---------|
| `recv_ts` | int64 | receive time of the frame after which the book was sampled, ns |
| `last_update_id` | int64 | update ID of the book at that moment |
| `bid_px` | int64[20] | bid prices in steps |
| `bid_qty` | int64[20] | bid quantities in steps |
| `ask_px` | int64[20] | ask prices in steps |
| `ask_qty` | int64[20] | ask quantities in steps |
| `source` | string | feed |

### quality

Every data-quality event, in order.

| column | type | meaning |
|--------|------|---------|
| `ts` | int64 | receive time of the frame that caused it, ns |
| `kind` | string | see below |
| `detail` | string | human-readable details |
| `source` | string | feed |

Kinds: `connected`, `disconnected`, `gap`, `snapshot_too_old`, `invalid_book`,
`buffer_overflow`, `reconnect`, `overflow`, `synced`, `resync`, `bbo_mismatch`,
`undecodable`, `snapshot_failed`, `server_shutdown`, `late`, `on_time`.

The kinds and their details are listed under [Quality kinds](#quality-kinds).

### link_rtt

Round trips of the recorder's own WebSocket pings, one every 5 s per connection. Both ends of
a round trip are this machine's clock, so no clock agreement is needed: compared with the
lag of the data, it tells whether late data was delayed on the link or before it.

| column | type | meaning |
|--------|------|---------|
| `recv_ts` | int64 | receive time of the pong, ns |
| `conn` | int64 | connection number within the run |
| `rtt_ns` | int64 | round trip, ns |
| `source` | string | feed |

## Quality kinds

The book is valid from a `synced` or `resync` event until the next `gap`, `invalid_book`,
`reconnect`, `overflow`, or `disconnected` with reason `stopped`. Details are `key=value`
pairs; the update ID in `synced` and `resync` is part of the contract (`verify` reads it).

| kind | book after it | meaning | detail |
|------|---------------|---------|--------|
| connected | unchanged | a stream connection opened | `conn=N` |
| disconnected | invalid if it was the connection delivering data | a connection ended, or a connection attempt failed | `conn=N reason="..."`; reason `stopped`: the recording ended on purpose |
| gap | invalid | a diff did not continue the book | `expected=ID got=U` |
| snapshot_too_old | still invalid | the snapshot is older than the first buffered diff; another is requested | `snapshot=ID first_buffered=U` |
| invalid_book | invalid | a diff or snapshot gave a crossed or malformed book | text |
| buffer_overflow | still invalid | too many diffs buffered while waiting for a snapshot; the buffer was dropped | `len=N` |
| reconnect | invalid | the connection that delivered data ended | |
| overflow | invalid | frames were lost in a full queue inside the recorder | `dropped=N` |
| synced | valid | the first valid book on this connection | `update_id=ID` |
| resync | valid | the book is valid again after a loss on this connection | `update_id=ID` |
| bbo_mismatch | unchanged | the book disagrees with the exchange's best bid and ask at the same update ID | `update_id=ID book=BID/ASK ticker=...` in steps |
| undecodable | unchanged | a frame could not be decoded | `kind=K reason="..."` |
| snapshot_failed | unchanged | a snapshot could not be fetched; requested again while diffs wait | `id=REQUEST reason="..."` |
| server_shutdown | unchanged | the exchange announced that it will close the connection | `shutdown_ts=NS` |
| late | unchanged, but behind the market | a depth update arrived more than 1 s after its exchange time | `lag_ms=N` |
| on_time | unchanged | a depth update arrived within 0.5 s again; ends a `late` interval | `lag_ms=N late_for_ms=N max_lag_ms=N` |

A `late` interval ends at the next `on_time`, or when its connection ends. In it the book can
be valid, so continuity holds, but it shows the market as it was seconds before. Lags are
receive time minus exchange time, so they include this machine's clock offset.

## manifest.json

One per day directory.

| field | meaning |
|-------|---------|
| `schema_version` | this contract's version |
| `symbol`, `date`, `price_step`, `qty_step` | what the directory holds |
| `host` | machine that wrote the tables |
| `runs` | each run that wrote to this day: `recorder` (version and commit), `first_recv_ts` and `last_recv_ts`, `book_at_start`: the book's update ID when the run's data for this day began, if the book was valid then (a run that continues over midnight), else null, and `derivation`: the version of the rules that derived the tables from the frames (0: written before it was recorded). Rows from runs of different derivations can differ for the same frames; `marketrec rebuild` derives a whole day again with the current rules |
| `rows` | rows per table |
| `files` | parts per table, relative to the day directory |
| `quality` | count of quality events per kind |
| `covered_ns` | receive time with a stream connection open |
| `synced_ns` | the part of it with a valid book |
| `synced_share` | `synced_ns / covered_ns` |
| `late_ns` | time in `late` intervals |
| `lag` | lag of every depth update behind its exchange time: `counts` per bucket in ms (exact below 1000 ms, then three significant digits, so days and runs add up exactly), `below_zero` (updates that arrived before their exchange time) and `max_ns` |
| `lag_p50_ms`, `lag_p99_ms` | median and 99th percentile of `lag`, lower bounds of their buckets |
| `rtt`, `rtt_p50_ms`, `rtt_p99_ms` | the same for the round trips in `link_rtt` |
| `duplicates` | messages dropped because another connection had delivered them first |

## Reading

Python with pyarrow, prices and quantities back to decimals:

```python
import json
import pyarrow.parquet as pq

day = "data/binance/BTCUSDT/2026-11-21"
m = json.load(open(f"{day}/manifest.json"))
trades = pq.read_table(f"{day}/trades").to_pandas()
trades["price"] = trades.price * float(m["price_step"])  # for display; keep steps for maths
quality = pq.read_table(f"{day}/quality").to_pandas()
```
