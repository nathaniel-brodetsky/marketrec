# Replay scenarios

Each file is a frames file in its text form (`docs/FRAMES.md`): a header line, then the
frames in arrival order. Symbol BTCUSDT, tick 0.01, step 0.00001, feed times in
microseconds, receive times from 2026-11-21 00:00:00 UTC. The snapshot answering request
`n` is the `snapshot` frame with `"aux":n`.

Run one: `cargo run -- replay --input tests/fixtures/gap.jsonl --events`.
Expected results are asserted in `tests/replay.rs`.

| File | What happens | Expected |
|------|--------------|----------|
| `normal_start.jsonl` | diffs buffered, snapshot at 1000, live diffs; book tickers arrive before their depth batch | synced; both tickers match the book |
| `stale_snapshot.jsonl` | first snapshot (990) is older than the first buffered diff (1001) | `snapshot_too_old`, second request, synced |
| `old_events_in_buffer.jsonl` | buffered diffs up to 1000 are already in the snapshot at 1000 | dropped; the book shows the snapshot's levels |
| `gap.jsonl` | diff 1006 never arrives | `gap`, new snapshot, `resync` |
| `duplicate.jsonl` | the same diff arrives twice | applied once |
| `out_of_order.jsonl` | 1006-1007 arrives before 1004-1005 | `gap`, `resync` |
| `zero_quantity.jsonl` | the best bid is set to 0 | level removed, ticker confirms |
| `reconnect.jsonl` | the connection drops, a new one opens | `disconnected`, `reconnect`, `resync` |
| `broken_frame.jsonl` | a depth message is cut off | `undecodable`; its loss shows as a `gap`; `resync` |
| `bbo_mismatch.jsonl` | the book ticker disagrees with the book at the same update ID | `bbo_mismatch` |
| `crossed_book.jsonl` | a diff would put the best bid above the best ask | `invalid_book`, `resync` |
| `server_shutdown.jsonl` | the exchange announces it will close the connection | `server_shutdown` logged |
