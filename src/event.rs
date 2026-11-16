//! Market events after decoding, shared by every feed decoder.
//!
//! Prices and quantities are whole numbers of steps (see `decimal`), times are nanoseconds
//! since the Unix epoch in UTC as given by the exchange.

/// One price level: price and quantity in steps. Quantity 0 removes the level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub price: i64,
    pub qty: i64,
}

/// Changes to the order book between two update IDs, inclusive.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DepthDiff {
    pub event_ts: i64,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// Best bid and ask after the book update with this ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookTicker {
    pub update_id: u64,
    pub bid_px: i64,
    pub bid_qty: i64,
    pub ask_px: i64,
    pub ask_qty: i64,
}

/// One trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trade {
    pub event_ts: i64,
    pub trade_ts: i64,
    pub trade_id: u64,
    pub price: i64,
    pub qty: i64,
    /// True when the buyer was the resting order, that is the aggressor sold.
    pub is_buyer_maker: bool,
}

/// The exchange announces that this connection will be closed at `shutdown_ts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerShutdown {
    pub event_ts: i64,
    pub shutdown_ts: i64,
}

/// A decoded message. A depth diff is borrowed from the decoder, whose buffers are reused.
#[derive(Debug, PartialEq, Eq)]
pub enum Event<'a> {
    Depth(&'a DepthDiff),
    BookTicker(BookTicker),
    Trade(Trade),
    ServerShutdown(ServerShutdown),
}
