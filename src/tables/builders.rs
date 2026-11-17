//! Column builders, one set per table, turned into Arrow batches.

use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, FixedSizeListBuilder, Int8Builder, Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::ArrowError;

use super::schema::{L20, Table};
use crate::event::{BookTicker, DepthDiff, Level, Trade};

/// Update and trade IDs are unsigned on the wire and stored as int64, which is what every
/// analysis tool reads well; Binance IDs are far below 2^63.
fn id(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

#[derive(Default)]
struct Depth {
    recv_ts: Int64Builder,
    event_ts: Int64Builder,
    first: Int64Builder,
    last: Int64Builder,
    side: Int8Builder,
    price: Int64Builder,
    qty: Int64Builder,
    source: StringBuilder,
}

#[derive(Default)]
struct Ticker {
    recv_ts: Int64Builder,
    update_id: Int64Builder,
    bid_px: Int64Builder,
    bid_qty: Int64Builder,
    ask_px: Int64Builder,
    ask_qty: Int64Builder,
    source: StringBuilder,
}

#[derive(Default)]
struct Trades {
    recv_ts: Int64Builder,
    event_ts: Int64Builder,
    trade_ts: Int64Builder,
    trade_id: Int64Builder,
    price: Int64Builder,
    qty: Int64Builder,
    is_buyer_maker: BooleanBuilder,
    source: StringBuilder,
}

struct L20Columns {
    recv_ts: Int64Builder,
    last_update_id: Int64Builder,
    /// bid_px, bid_qty, ask_px, ask_qty
    lists: [FixedSizeListBuilder<Int64Builder>; 4],
    source: StringBuilder,
}

impl Default for L20Columns {
    fn default() -> Self {
        let list = || FixedSizeListBuilder::new(Int64Builder::new(), L20 as i32);
        Self {
            recv_ts: Int64Builder::new(),
            last_update_id: Int64Builder::new(),
            lists: [list(), list(), list(), list()],
            source: StringBuilder::new(),
        }
    }
}

#[derive(Default)]
struct Quality {
    ts: Int64Builder,
    kind: StringBuilder,
    detail: StringBuilder,
    source: StringBuilder,
}

#[derive(Default)]
pub(super) struct Builders {
    depth: Depth,
    ticker: Ticker,
    trades: Trades,
    l20: L20Columns,
    quality: Quality,
}

/// One `book_l20` row being filled: call `side` for bids (0) and asks (1), then `finish`.
pub(super) struct L20Row<'a>(&'a mut L20Columns);

impl L20Row<'_> {
    /// The best levels of one side, best first; missing levels are written as 0.
    pub fn side(&mut self, side: usize, levels: &[Level]) {
        let (px, qty) = self.0.lists.split_at_mut(2 * side + 1);
        let (px, qty) = (&mut px[2 * side], &mut qty[0]);
        for i in 0..L20 {
            let l = levels.get(i).copied().unwrap_or(Level { price: 0, qty: 0 });
            px.values().append_value(l.price);
            qty.values().append_value(l.qty);
        }
        px.append(true);
        qty.append(true);
    }

    pub fn finish(self, source: &str) {
        self.0.source.append_value(source);
    }
}

impl Builders {
    pub fn len(&self, table: Table) -> usize {
        match table {
            Table::DepthDiff => self.depth.recv_ts.len(),
            Table::BookTicker => self.ticker.recv_ts.len(),
            Table::Trades => self.trades.recv_ts.len(),
            Table::BookL20 => self.l20.source.len(),
            Table::Quality => self.quality.ts.len(),
        }
    }

    /// Everything buffered for `table` as one batch; the builders start empty again.
    pub fn finish(&mut self, table: Table) -> Result<RecordBatch, ArrowError> {
        let columns: Vec<ArrayRef> = match table {
            Table::DepthDiff => {
                let b = &mut self.depth;
                vec![
                    Arc::new(b.recv_ts.finish()),
                    Arc::new(b.event_ts.finish()),
                    Arc::new(b.first.finish()),
                    Arc::new(b.last.finish()),
                    Arc::new(b.side.finish()),
                    Arc::new(b.price.finish()),
                    Arc::new(b.qty.finish()),
                    Arc::new(b.source.finish()),
                ]
            }
            Table::BookTicker => {
                let b = &mut self.ticker;
                vec![
                    Arc::new(b.recv_ts.finish()),
                    Arc::new(b.update_id.finish()),
                    Arc::new(b.bid_px.finish()),
                    Arc::new(b.bid_qty.finish()),
                    Arc::new(b.ask_px.finish()),
                    Arc::new(b.ask_qty.finish()),
                    Arc::new(b.source.finish()),
                ]
            }
            Table::Trades => {
                let b = &mut self.trades;
                vec![
                    Arc::new(b.recv_ts.finish()),
                    Arc::new(b.event_ts.finish()),
                    Arc::new(b.trade_ts.finish()),
                    Arc::new(b.trade_id.finish()),
                    Arc::new(b.price.finish()),
                    Arc::new(b.qty.finish()),
                    Arc::new(b.is_buyer_maker.finish()),
                    Arc::new(b.source.finish()),
                ]
            }
            Table::BookL20 => {
                let b = &mut self.l20;
                let [a, c, d, e] = &mut b.lists;
                vec![
                    Arc::new(b.recv_ts.finish()),
                    Arc::new(b.last_update_id.finish()),
                    Arc::new(a.finish()),
                    Arc::new(c.finish()),
                    Arc::new(d.finish()),
                    Arc::new(e.finish()),
                    Arc::new(b.source.finish()),
                ]
            }
            Table::Quality => {
                let b = &mut self.quality;
                vec![
                    Arc::new(b.ts.finish()),
                    Arc::new(b.kind.finish()),
                    Arc::new(b.detail.finish()),
                    Arc::new(b.source.finish()),
                ]
            }
        };
        RecordBatch::try_new(table.schema(), columns)
    }

    /// One row per changed level: bids with side 1, asks with side -1.
    /// One row per level; a diff without levels gets one row with side 0, because it still
    /// moves the update IDs on and continuity is checked from this table.
    pub fn depth(&mut self, recv_ts: i64, diff: &DepthDiff, source: &str) {
        const NONE: &[Level] = &[Level { price: 0, qty: 0 }];
        let b = &mut self.depth;
        let sides: [(i8, &[Level]); 2] = if diff.bids.is_empty() && diff.asks.is_empty() {
            [(0, NONE), (0, &[])]
        } else {
            [(1, &diff.bids), (-1, &diff.asks)]
        };
        for (side, levels) in sides {
            for l in levels {
                b.recv_ts.append_value(recv_ts);
                b.event_ts.append_value(diff.event_ts);
                b.first.append_value(id(diff.first_update_id));
                b.last.append_value(id(diff.final_update_id));
                b.side.append_value(side);
                b.price.append_value(l.price);
                b.qty.append_value(l.qty);
                b.source.append_value(source);
            }
        }
    }

    pub fn book_ticker(&mut self, recv_ts: i64, t: &BookTicker, source: &str) {
        let b = &mut self.ticker;
        b.recv_ts.append_value(recv_ts);
        b.update_id.append_value(id(t.update_id));
        b.bid_px.append_value(t.bid_px);
        b.bid_qty.append_value(t.bid_qty);
        b.ask_px.append_value(t.ask_px);
        b.ask_qty.append_value(t.ask_qty);
        b.source.append_value(source);
    }

    pub fn trade(&mut self, recv_ts: i64, t: &Trade, source: &str) {
        let b = &mut self.trades;
        b.recv_ts.append_value(recv_ts);
        b.event_ts.append_value(t.event_ts);
        b.trade_ts.append_value(t.trade_ts);
        b.trade_id.append_value(id(t.trade_id));
        b.price.append_value(t.price);
        b.qty.append_value(t.qty);
        b.is_buyer_maker.append_value(t.is_buyer_maker);
        b.source.append_value(source);
    }

    pub fn quality(&mut self, ts: i64, kind: &str, detail: &str, source: &str) {
        let b = &mut self.quality;
        b.ts.append_value(ts);
        b.kind.append_value(kind);
        b.detail.append_value(detail);
        b.source.append_value(source);
    }

    pub fn l20_row(&mut self, recv_ts: i64, last_update_id: u64) -> L20Row<'_> {
        self.l20.recv_ts.append_value(recv_ts);
        self.l20.last_update_id.append_value(id(last_update_id));
        L20Row(&mut self.l20)
    }
}
