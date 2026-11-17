# Frames file format, version 1

A frames file is the exact input of the decoding thread: WebSocket messages, REST
snapshots and connection events in arrival order, each with its local receive time.
`marketrec replay` feeds such a file through the same code as the live recorder.

All integers are little-endian.

## File

| Offset | Size | Field                                   |
|--------|------|-----------------------------------------|
| 0      | 8    | magic `MRFRAMES`                        |
| 8      | 2    | format version, `1`                     |
| 10     | 4    | header length `H`, at most 1 MiB        |
| 14     | H    | header, a UTF-8 JSON object (see below) |
| 14 + H | ...  | records until end of file               |

Header fields: `symbol`, `price_step` and `qty_step` (decimal strings as in Binance
exchangeInfo), `time_unit` of the feed (`milli` or `micro`), `recorder` (program and
version). Readers reject other format versions.

## Record

| Size | Field                                                       |
|------|-------------------------------------------------------------|
| 4    | payload length `N`, at most 16 MiB                          |
| 1    | kind                                                        |
| 8    | `recv_ts`: local receive time, ns since the Unix epoch, i64 |
| 8    | `aux`: meaning depends on the kind, u64                     |
| N    | payload                                                     |
| 4    | CRC-32 (IEEE) of kind, `recv_ts`, `aux` and payload         |

| Kind | Name           | `aux`               | Payload                  |
|------|----------------|---------------------|--------------------------|
| 1    | `text`         | connection number   | WebSocket text message   |
| 2    | `binary`       | connection number   | WebSocket binary message |
| 3    | `snapshot`     | snapshot request ID: the `recv_ts` of the frame that caused the request | REST depth response body, or `{"error":...}` if the fetch failed |
| 4    | `connected`    | connection number   | URL                      |
| 5    | `disconnected` | connection number   | reason; `stopped` if the recorder stopped on purpose, `handover` if a successor connection took over |
| 6    | `overflow`     | frames dropped      | name of the full queue   |
| 7    | `rtt`          | connection number   | send time of the recorder's own ping, i64 little-endian ns; the round trip is `recv_ts` minus it |

## Compression

A finished file may be stored compressed as `<name>.frames.zst`: one zstd frame, with its
content checksum, holding exactly the bytes of `<name>.frames`. The recorder compresses
every hourly file once it is finished (`record --no-compress` keeps them raw), and
`marketrec frames compress` does the same for older files. The compressed copy is written
as `.tmp`, read back and compared byte for byte with the original, and only then renamed
and the original removed. Every command that reads frames opens both forms; a damaged
compressed file fails its checksum and is reported, never read as other data.

## Integrity

- A reader returns intact records in order and stops at the first problem:
  `Truncated` if the file ends inside a record (normal after a crash), `Corrupt` if a
  length is out of range, the checksum does not match or the kind is unknown. Nothing
  after a damaged record is returned, and a damaged record is never returned altered.
- A file is written as `<name>.tmp` and renamed to `<name>` after it is flushed and
  synced. A finished file is never modified or overwritten.

## Text form

`marketrec frames dump` and `marketrec frames pack` convert to and from JSON Lines
without loss: a header line `{"header":{...}}`, then one object per record with `kind`,
`recv_ts`, `aux` and at most one payload field:

- `payload_json`: a JSON object or array on one line, embedded verbatim;
- `payload`: any other UTF-8 text, as a JSON string;
- `payload_hex`: any other bytes;
- no payload field: empty payload.

Test fixtures use this form; `marketrec replay` reads both forms.
