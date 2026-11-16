//! Decoder for the Binance Spot JSON combined stream.
//!
//! Expected frames, one per WebSocket text message (format as documented by Binance):
//!
//! ```text
//! {"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":..,"s":"BTCUSDT","U":..,"u":..,"b":[["px","qty"]],"a":[..]}}
//! {"stream":"btcusdt@bookTicker","data":{"u":..,"s":"BTCUSDT","b":"px","B":"qty","a":"px","A":"qty"}}
//! {"stream":"btcusdt@trade","data":{"e":"trade","E":..,"s":"BTCUSDT","t":..,"p":"px","q":"qty","T":..,"m":true,"M":true}}
//! {"stream":"!serverShutdown","data":{"e":"serverShutdown","E":..,"T":..}}
//! ```
//!
//! Strings are borrowed from the frame. Price and quantity arrays of a depth message are
//! still collected into temporary vectors here; making that path allocation-free is a later,
//! separately tested step.

use std::fmt;

use serde::Deserialize;
use serde_json::value::RawValue;

use crate::decimal::{Grid, ParseError};
use crate::event::{BookTicker, DepthDiff, Event, Level, ServerShutdown, Trade};

/// Unit of the time fields in the feed: milliseconds by default, microseconds when the
/// stream URL has `timeUnit=MICROSECOND`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Milli,
    Micro,
}

impl TimeUnit {
    fn to_ns(self, value: u64) -> Option<i64> {
        let factor = match self {
            TimeUnit::Milli => 1_000_000,
            TimeUnit::Micro => 1_000,
        };
        i64::try_from(value.checked_mul(factor)?).ok()
    }
}

#[derive(Debug)]
pub enum DecodeError {
    /// Not valid JSON, or a required field is missing or of the wrong type.
    Json(serde_json::Error),
    /// A stream this decoder does not handle.
    UnknownStream,
    /// The message is for another symbol.
    WrongSymbol,
    /// A price or quantity that is not a valid number of steps.
    Number {
        field: &'static str,
        err: ParseError,
    },
    /// A time that does not fit into nanoseconds as i64.
    TimeOverflow,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Json(e) => write!(f, "json: {e}"),
            DecodeError::UnknownStream => write!(f, "unknown stream"),
            DecodeError::WrongSymbol => write!(f, "message for another symbol"),
            DecodeError::Number { field, err } => write!(f, "field {field}: {err}"),
            DecodeError::TimeOverflow => write!(f, "time out of range"),
        }
    }
}

impl std::error::Error for DecodeError {}

impl From<serde_json::Error> for DecodeError {
    fn from(e: serde_json::Error) -> Self {
        DecodeError::Json(e)
    }
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    stream: &'a str,
    #[serde(borrow)]
    data: &'a RawValue,
}

#[derive(Deserialize)]
struct DepthRaw<'a> {
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s", borrow)]
    symbol: &'a str,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "b", borrow)]
    bids: Vec<[&'a str; 2]>,
    #[serde(rename = "a", borrow)]
    asks: Vec<[&'a str; 2]>,
}

#[derive(Deserialize)]
struct BookTickerRaw<'a> {
    #[serde(rename = "u")]
    update_id: u64,
    #[serde(rename = "s", borrow)]
    symbol: &'a str,
    #[serde(rename = "b", borrow)]
    bid_px: &'a str,
    #[serde(rename = "B", borrow)]
    bid_qty: &'a str,
    #[serde(rename = "a", borrow)]
    ask_px: &'a str,
    #[serde(rename = "A", borrow)]
    ask_qty: &'a str,
}

#[derive(Deserialize)]
struct TradeRaw<'a> {
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s", borrow)]
    symbol: &'a str,
    #[serde(rename = "t")]
    trade_id: u64,
    #[serde(rename = "p", borrow)]
    price: &'a str,
    #[serde(rename = "q", borrow)]
    qty: &'a str,
    #[serde(rename = "T")]
    trade_time: u64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
}

#[derive(Deserialize)]
struct ShutdownRaw {
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "T")]
    shutdown_time: u64,
}

/// Decoder for one symbol. Reuse one decoder for the whole stream: its depth buffers keep
/// their capacity between messages.
pub struct JsonDecoder {
    symbol: String,
    price: Grid,
    qty: Grid,
    unit: TimeUnit,
    depth: DepthDiff,
}

impl JsonDecoder {
    /// `symbol` as Binance writes it in messages, for example `BTCUSDT`.
    pub fn new(symbol: &str, price: Grid, qty: Grid, unit: TimeUnit) -> Self {
        Self {
            symbol: symbol.to_owned(),
            price,
            qty,
            unit,
            depth: DepthDiff::default(),
        }
    }

    /// Decode one frame of the combined stream.
    pub fn decode(&mut self, frame: &[u8]) -> Result<Event<'_>, DecodeError> {
        let env: Envelope<'_> = serde_json::from_slice(frame)?;
        let data = env.data.get();
        // "btcusdt@depth@100ms" -> "depth@100ms"; "!serverShutdown" has no symbol part
        let kind = env
            .stream
            .split_once('@')
            .map_or(env.stream, |(_, rest)| rest);
        match kind {
            "!serverShutdown" => {
                let raw: ShutdownRaw = serde_json::from_str(data)?;
                Ok(Event::ServerShutdown(ServerShutdown {
                    event_ts: self.ns(raw.event_time)?,
                    shutdown_ts: self.ns(raw.shutdown_time)?,
                }))
            }
            "depth" | "depth@100ms" => {
                let raw: DepthRaw<'_> = serde_json::from_str(data)?;
                self.check_symbol(raw.symbol)?;
                let event_ts = self.ns(raw.event_time)?;
                fill_levels(&mut self.depth.bids, &raw.bids, self.price, self.qty)?;
                fill_levels(&mut self.depth.asks, &raw.asks, self.price, self.qty)?;
                self.depth.event_ts = event_ts;
                self.depth.first_update_id = raw.first_update_id;
                self.depth.final_update_id = raw.final_update_id;
                Ok(Event::Depth(&self.depth))
            }
            "bookTicker" => {
                let raw: BookTickerRaw<'_> = serde_json::from_str(data)?;
                self.check_symbol(raw.symbol)?;
                Ok(Event::BookTicker(BookTicker {
                    update_id: raw.update_id,
                    bid_px: steps(self.price, raw.bid_px, "b")?,
                    bid_qty: steps(self.qty, raw.bid_qty, "B")?,
                    ask_px: steps(self.price, raw.ask_px, "a")?,
                    ask_qty: steps(self.qty, raw.ask_qty, "A")?,
                }))
            }
            "trade" => {
                let raw: TradeRaw<'_> = serde_json::from_str(data)?;
                self.check_symbol(raw.symbol)?;
                Ok(Event::Trade(Trade {
                    event_ts: self.ns(raw.event_time)?,
                    trade_ts: self.ns(raw.trade_time)?,
                    trade_id: raw.trade_id,
                    price: steps(self.price, raw.price, "p")?,
                    qty: steps(self.qty, raw.qty, "q")?,
                    is_buyer_maker: raw.is_buyer_maker,
                }))
            }
            _ => Err(DecodeError::UnknownStream),
        }
    }

    fn ns(&self, value: u64) -> Result<i64, DecodeError> {
        self.unit.to_ns(value).ok_or(DecodeError::TimeOverflow)
    }

    fn check_symbol(&self, symbol: &str) -> Result<(), DecodeError> {
        if symbol == self.symbol {
            Ok(())
        } else {
            Err(DecodeError::WrongSymbol)
        }
    }
}

fn steps(grid: Grid, s: &str, field: &'static str) -> Result<i64, DecodeError> {
    grid.to_steps(s.as_bytes())
        .map_err(|err| DecodeError::Number { field, err })
}

fn fill_levels(
    out: &mut Vec<Level>,
    raw: &[[&str; 2]],
    price: Grid,
    qty: Grid,
) -> Result<(), DecodeError> {
    out.clear();
    for [p, q] in raw {
        out.push(Level {
            price: steps(price, p, "price level")?,
            qty: steps(qty, q, "level quantity")?,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn decoder(unit: TimeUnit) -> JsonDecoder {
        let tick = Grid::parse(b"0.01").unwrap();
        let step = Grid::parse(b"0.00001").unwrap();
        JsonDecoder::new("BTCUSDT", tick, step, unit)
    }

    const DEPTH: &str = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1763300000123,"s":"BTCUSDT","U":157,"u":160,"b":[["97123.45000000","0.00120000"],["97123.44000000","0.00000000"]],"a":[["97123.46000000","1.50000000"]]}}"#;
    const BOOK_TICKER: &str = r#"{"stream":"btcusdt@bookTicker","data":{"u":400900217,"s":"BTCUSDT","b":"97123.45000000","B":"0.00120000","a":"97123.46000000","A":"1.50000000"}}"#;
    const TRADE: &str = r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1763300000123,"s":"BTCUSDT","t":12345,"p":"97123.45000000","q":"0.00010000","T":1763300000120,"m":true,"M":true}}"#;
    const SHUTDOWN: &str = r#"{"stream":"!serverShutdown","data":{"e":"serverShutdown","E":1770123456789,"T":1770124000000}}"#;

    #[test]
    fn depth_update() {
        let mut d = decoder(TimeUnit::Milli);
        let Event::Depth(diff) = d.decode(DEPTH.as_bytes()).unwrap() else {
            panic!("not a depth event")
        };
        assert_eq!(diff.event_ts, 1_763_300_000_123_000_000);
        assert_eq!((diff.first_update_id, diff.final_update_id), (157, 160));
        assert_eq!(
            diff.bids,
            vec![
                Level {
                    price: 9_712_345,
                    qty: 120
                },
                Level {
                    price: 9_712_344,
                    qty: 0
                }
            ]
        );
        assert_eq!(
            diff.asks,
            vec![Level {
                price: 9_712_346,
                qty: 150_000
            }]
        );
    }

    #[test]
    fn depth_buffers_are_cleared_between_messages() {
        let mut d = decoder(TimeUnit::Milli);
        d.decode(DEPTH.as_bytes()).unwrap();
        let small = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":161,"u":161,"b":[],"a":[["97123.47000000","0.00001000"]]}}"#;
        let Event::Depth(diff) = d.decode(small.as_bytes()).unwrap() else {
            panic!("not a depth event")
        };
        assert!(diff.bids.is_empty());
        assert_eq!(
            diff.asks,
            vec![Level {
                price: 9_712_347,
                qty: 1
            }]
        );
    }

    #[test]
    fn plain_depth_stream_name() {
        let mut d = decoder(TimeUnit::Milli);
        let frame = DEPTH.replace("btcusdt@depth@100ms", "btcusdt@depth");
        assert!(matches!(d.decode(frame.as_bytes()), Ok(Event::Depth(_))));
    }

    #[test]
    fn book_ticker() {
        let mut d = decoder(TimeUnit::Milli);
        assert_eq!(
            d.decode(BOOK_TICKER.as_bytes()).unwrap(),
            Event::BookTicker(BookTicker {
                update_id: 400_900_217,
                bid_px: 9_712_345,
                bid_qty: 120,
                ask_px: 9_712_346,
                ask_qty: 150_000,
            })
        );
    }

    #[test]
    fn trade() {
        let mut d = decoder(TimeUnit::Milli);
        assert_eq!(
            d.decode(TRADE.as_bytes()).unwrap(),
            Event::Trade(Trade {
                event_ts: 1_763_300_000_123_000_000,
                trade_ts: 1_763_300_000_120_000_000,
                trade_id: 12345,
                price: 9_712_345,
                qty: 10,
                is_buyer_maker: true,
            })
        );
    }

    #[test]
    fn server_shutdown() {
        let mut d = decoder(TimeUnit::Milli);
        assert_eq!(
            d.decode(SHUTDOWN.as_bytes()).unwrap(),
            Event::ServerShutdown(ServerShutdown {
                event_ts: 1_770_123_456_789_000_000,
                shutdown_ts: 1_770_124_000_000_000_000,
            })
        );
    }

    #[test]
    fn microsecond_feed() {
        let mut d = decoder(TimeUnit::Micro);
        let frame = TRADE.replace("1763300000123", "1763300000123456");
        let Event::Trade(t) = d.decode(frame.as_bytes()).unwrap() else {
            panic!("not a trade")
        };
        assert_eq!(t.event_ts, 1_763_300_000_123_456_000);
    }

    #[test]
    fn errors() {
        let mut d = decoder(TimeUnit::Milli);
        let wrong_symbol = TRADE.replace("\"s\":\"BTCUSDT\"", "\"s\":\"ETHUSDT\"");
        assert!(matches!(
            d.decode(wrong_symbol.as_bytes()),
            Err(DecodeError::WrongSymbol)
        ));
        let partial_depth = DEPTH.replace("btcusdt@depth@100ms", "btcusdt@depth20@100ms");
        assert!(matches!(
            d.decode(partial_depth.as_bytes()),
            Err(DecodeError::UnknownStream)
        ));
        let unknown = TRADE.replace("btcusdt@trade", "btcusdt@aggTrade");
        assert!(matches!(
            d.decode(unknown.as_bytes()),
            Err(DecodeError::UnknownStream)
        ));
        let off_grid = TRADE.replace("97123.45000000", "97123.45500000");
        assert!(matches!(
            d.decode(off_grid.as_bytes()),
            Err(DecodeError::Number {
                field: "p",
                err: ParseError::OffGrid
            })
        ));
        let missing = TRADE.replace("\"t\":12345,", "");
        assert!(matches!(
            d.decode(missing.as_bytes()),
            Err(DecodeError::Json(_))
        ));
        assert!(matches!(
            d.decode(b"{\"result\":null,\"id\":1}"),
            Err(DecodeError::Json(_))
        ));
        assert!(matches!(
            d.decode(&DEPTH.as_bytes()[..50]),
            Err(DecodeError::Json(_))
        ));
        let huge_time = TRADE.replace("1763300000123", "99999999999999999");
        assert!(matches!(
            d.decode(huge_time.as_bytes()),
            Err(DecodeError::TimeOverflow)
        ));
    }

    proptest! {
        #[test]
        fn never_panics_on_bytes(bytes in proptest::collection::vec(any::<u8>(), 0..300)) {
            let mut d = decoder(TimeUnit::Milli);
            let _ = d.decode(&bytes);
        }

        #[test]
        fn never_panics_on_truncated_frames(cut in 0usize..DEPTH.len()) {
            let mut d = decoder(TimeUnit::Milli);
            let _ = d.decode(&DEPTH.as_bytes()[..cut]);
        }
    }
}
