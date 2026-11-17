//! Test support: a simulated exchange, and a recorder session that answers the pipeline's
//! snapshot requests as the live recorder does. The frames it produces are frames a live
//! run could have recorded, and the exchange keeps the true book after every message, so
//! derived data can be checked against the truth rather than against the code under test.

use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::book::RefBook;
use crate::event::Level;
use crate::frames::{FrameKind, Header, OwnedFrame};
use crate::json::TimeUnit;
use crate::pipeline::{Pipeline, Sink};
use crate::sync::SyncConfig;

/// 2026-11-21 00:00:00 UTC.
pub const DAY0: i64 = 1_795_219_200_000_000_000;
pub const MS: i64 = 1_000_000;
pub const SEC: i64 = 1_000_000_000;

pub fn header() -> Header {
    Header {
        symbol: "BTCUSDT".into(),
        price_step: "0.01000000".into(),
        qty_step: "0.00001000".into(),
        time_unit: TimeUnit::Micro,
        recorder: "marketrec test".into(),
    }
}

/// Best 20 levels per side, best first.
pub type Top = (Vec<Level>, Vec<Level>);

/// Prices in cents around 10000.00, quantities in steps of 0.00001. Bids stay below
/// 10000.00 and asks at or above it, so the book never crosses.
pub struct Exchange {
    rng: u64,
    pub id: u64,
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
    /// The book after every message, by the message's last update ID.
    pub tops: HashMap<u64, Top>,
    trade_id: u64,
}

const MID: i64 = 1_000_000;

impl Exchange {
    pub fn new(seed: u64) -> Self {
        let mut ex = Self {
            rng: seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
            id: 1000,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            tops: HashMap::new(),
            trade_id: 0,
        };
        for i in 0..50 {
            ex.bids.insert(MID - 1 - i, 1000 + i);
            ex.asks.insert(MID + i, 2000 + i);
        }
        ex.tops.insert(ex.id, ex.top());
        ex
    }

    fn below(&mut self, n: u64) -> u64 {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        self.rng % n
    }

    pub fn top(&self) -> Top {
        let lv = |(&price, &qty): (&i64, &i64)| Level { price, qty };
        (
            self.bids.iter().rev().take(20).map(lv).collect(),
            self.asks.iter().take(20).map(lv).collect(),
        )
    }

    /// One depth message of 1 to 3 updates. Returns `(U, u, bids, asks)`.
    #[allow(clippy::type_complexity)]
    pub fn diff(&mut self) -> (u64, u64, Vec<(i64, i64)>, Vec<(i64, i64)>) {
        let first = self.id + 1;
        let n = 1 + self.below(3);
        let (mut bids, mut asks) = (BTreeMap::new(), BTreeMap::new());
        for _ in 0..n {
            let bid = self.below(2) == 0;
            let off = self.below(60) as i64;
            let qty = if self.below(4) == 0 {
                0
            } else {
                1 + self.below(100_000) as i64
            };
            let (price, book, msg) = if bid {
                (MID - 1 - off, &mut self.bids, &mut bids)
            } else {
                (MID + off, &mut self.asks, &mut asks)
            };
            msg.insert(price, qty);
            if qty == 0 {
                book.remove(&price);
            } else {
                book.insert(price, qty);
            }
        }
        self.id += n;
        self.tops.insert(self.id, self.top());
        (
            first,
            self.id,
            bids.into_iter().collect(),
            asks.into_iter().collect(),
        )
    }

    pub fn snapshot_json(&self) -> String {
        let side = |levels: Vec<(i64, i64)>| {
            levels
                .iter()
                .map(|&(p, q)| format!(r#"["{}","{}"]"#, px(p), qty(q)))
                .collect::<Vec<_>>()
                .join(",")
        };
        format!(
            r#"{{"lastUpdateId":{},"bids":[{}],"asks":[{}]}}"#,
            self.id,
            side(self.bids.iter().rev().map(|(&p, &q)| (p, q)).collect()),
            side(self.asks.iter().map(|(&p, &q)| (p, q)).collect())
        )
    }

    fn next_trade(&mut self) -> u64 {
        self.trade_id += 1;
        self.trade_id
    }
}

pub fn px(cents: i64) -> String {
    format!("{}.{:02}", cents / 100, cents % 100)
}

pub fn qty(steps: i64) -> String {
    format!("{}.{:05}", steps / 100_000, steps % 100_000)
}

fn levels(l: &[(i64, i64)]) -> String {
    l.iter()
        .map(|&(p, q)| format!(r#"["{}","{}"]"#, px(p), qty(q)))
        .collect::<Vec<_>>()
        .join(",")
}

pub fn text(ts: i64, payload: String) -> OwnedFrame {
    OwnedFrame {
        kind: FrameKind::Text,
        recv_ts: ts,
        aux: 0,
        payload: payload.into_bytes(),
    }
}

#[derive(Default)]
struct Requests(Vec<u64>);

impl Sink for Requests {
    fn request_snapshot(&mut self, id: u64) {
        self.0.push(id);
    }
}

/// A depth message as sent: `(recv_ts, U, u, bids, asks)`.
pub type SentDepth = (i64, u64, u64, Vec<(i64, i64)>, Vec<(i64, i64)>);

/// What a `trades` row must hold: `(recv_ts, event_ts, trade_ts, id, price, qty, is_buyer_maker)`.
pub type SentTrade = (i64, i64, i64, i64, i64, i64, bool);

/// Frames of a recording: messages from the exchange, and snapshots fetched when the
/// recorder's pipeline asks for them, `snapshot_delay` later.
pub struct Session {
    pub frames: Vec<OwnedFrame>,
    pub depth: Vec<SentDepth>,
    pub trades: Vec<SentTrade>,
    pub ex: Exchange,
    pub snapshot_delay: i64,
    recorder: Pipeline<RefBook>,
    requests: Requests,
    pending: VecDeque<(i64, u64)>,
    conn: u64,
    messages: u64,
}

impl Session {
    pub fn new(seed: u64) -> Self {
        Self {
            frames: Vec::new(),
            depth: Vec::new(),
            trades: Vec::new(),
            ex: Exchange::new(seed),
            snapshot_delay: 50 * MS,
            recorder: Pipeline::new(&header(), RefBook::new(), SyncConfig::default())
                .expect("valid header"),
            requests: Requests::default(),
            pending: VecDeque::new(),
            conn: 0,
            messages: 0,
        }
    }

    pub fn push(&mut self, frame: OwnedFrame) {
        self.recorder.on_frame(frame.as_frame(), &mut self.requests);
        for id in self.requests.0.drain(..) {
            self.pending
                .push_back((frame.recv_ts + self.snapshot_delay, id));
        }
        self.frames.push(frame);
    }

    /// Deliver the snapshots due by `ts`.
    pub fn deliver(&mut self, ts: i64) {
        while let Some(&(due, id)) = self.pending.front() {
            if due > ts {
                break;
            }
            self.pending.pop_front();
            let body = self.ex.snapshot_json();
            self.push(OwnedFrame {
                kind: FrameKind::Snapshot,
                recv_ts: due,
                aux: id,
                payload: body.into_bytes(),
            });
        }
    }

    pub fn connect(&mut self, ts: i64) {
        self.conn += 1;
        self.push(OwnedFrame {
            kind: FrameKind::Connected,
            recv_ts: ts,
            aux: self.conn,
            payload: b"wss://test/stream".to_vec(),
        });
    }

    pub fn disconnect(&mut self, ts: i64, reason: &str) {
        self.pending.clear(); // the live fetcher's answers would be for the old session
        self.push(OwnedFrame {
            kind: FrameKind::Disconnected,
            recv_ts: ts,
            aux: self.conn,
            payload: reason.as_bytes().to_vec(),
        });
    }

    /// A depth message every `every` ns in `[from, to)`; after every second one a book
    /// ticker, after every third a trade.
    pub fn run(&mut self, from: i64, to: i64, every: i64) {
        let mut ts = from;
        while ts < to {
            self.deliver(ts);
            let (first, last, bids, asks) = self.ex.diff();
            self.push(text(
                ts,
                format!(
                    r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":{},"s":"BTCUSDT","U":{first},"u":{last},"b":[{}],"a":[{}]}}}}"#,
                    ts / 1000,
                    levels(&bids),
                    levels(&asks)
                ),
            ));
            self.depth.push((ts, first, last, bids, asks));
            if self.messages % 2 == 1 {
                let (b, a) = self.ex.top();
                self.push(text(
                    ts,
                    format!(
                        r#"{{"stream":"btcusdt@bookTicker","data":{{"u":{},"s":"BTCUSDT","b":"{}","B":"{}","a":"{}","A":"{}"}}}}"#,
                        self.ex.id,
                        px(b[0].price),
                        qty(b[0].qty),
                        px(a[0].price),
                        qty(a[0].qty)
                    ),
                ));
            }
            if self.messages % 3 == 2 {
                let id = self.ex.next_trade();
                let m = id.is_multiple_of(2);
                let (price, q) = (MID + (id % 7) as i64, 1 + id as i64);
                let (e, t) = (ts / 1000, ts / 1000 - 1);
                self.trades
                    .push((ts, e * 1000, t * 1000, id as i64, price, q, m));
                self.push(text(
                    ts,
                    format!(
                        r#"{{"stream":"btcusdt@trade","data":{{"e":"trade","E":{},"s":"BTCUSDT","t":{id},"p":"{}","q":"{}","T":{},"m":{m},"M":true}}}}"#,
                        e,
                        px(price),
                        qty(q),
                        t
                    ),
                ));
            }
            self.messages += 1;
            ts += every;
        }
    }

    /// The exchange moves on by `n` messages that never arrive.
    pub fn lose(&mut self, n: usize) {
        for _ in 0..n {
            self.ex.diff();
        }
    }
}
