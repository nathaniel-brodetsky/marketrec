//! Arrow schemas of the Parquet tables. The authoritative description is `SCHEMA.md`;
//! a test keeps the two in step.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

/// Version of the data contract. Written into every Parquet file and every manifest.
pub const SCHEMA_VERSION: u32 = 1;

/// Levels per side in `book_l20`.
pub const L20: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Table {
    DepthDiff,
    BookTicker,
    Trades,
    BookL20,
    Quality,
    LinkRtt,
}

impl Table {
    pub const ALL: [Table; 6] = [
        Table::DepthDiff,
        Table::BookTicker,
        Table::Trades,
        Table::BookL20,
        Table::Quality,
        Table::LinkRtt,
    ];

    /// Directory name inside a day directory.
    pub fn name(self) -> &'static str {
        match self {
            Table::DepthDiff => "depth_diff",
            Table::BookTicker => "book_ticker",
            Table::Trades => "trades",
            Table::BookL20 => "book_l20",
            Table::Quality => "quality",
            Table::LinkRtt => "link_rtt",
        }
    }

    pub fn schema(self) -> SchemaRef {
        let i64f = |n: &str| Field::new(n, DataType::Int64, false);
        let level_list = |n: &str| {
            Field::new(
                n,
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Int64, true)),
                    L20 as i32,
                ),
                false,
            )
        };
        let source = Field::new("source", DataType::Utf8, false);
        let fields = match self {
            Table::DepthDiff => vec![
                i64f("recv_ts"),
                i64f("event_ts"),
                i64f("first_update_id"),
                i64f("final_update_id"),
                Field::new("side", DataType::Int8, false),
                i64f("price"),
                i64f("qty"),
                source,
            ],
            Table::BookTicker => vec![
                i64f("recv_ts"),
                i64f("update_id"),
                i64f("bid_px"),
                i64f("bid_qty"),
                i64f("ask_px"),
                i64f("ask_qty"),
                source,
            ],
            Table::Trades => vec![
                i64f("recv_ts"),
                i64f("event_ts"),
                i64f("trade_ts"),
                i64f("trade_id"),
                i64f("price"),
                i64f("qty"),
                Field::new("is_buyer_maker", DataType::Boolean, false),
                source,
            ],
            Table::BookL20 => vec![
                i64f("recv_ts"),
                i64f("last_update_id"),
                level_list("bid_px"),
                level_list("bid_qty"),
                level_list("ask_px"),
                level_list("ask_qty"),
                source,
            ],
            Table::Quality => vec![
                i64f("ts"),
                Field::new("kind", DataType::Utf8, false),
                Field::new("detail", DataType::Utf8, false),
                source,
            ],
            Table::LinkRtt => vec![i64f("recv_ts"), i64f("conn"), i64f("rtt_ns"), source],
        };
        Arc::new(Schema::new(fields))
    }
}
