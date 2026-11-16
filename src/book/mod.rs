//! Local order book.
//!
//! `OrderBook` is the interface every book implements. `RefBook` is the reference: simple
//! and obviously correct, used as the ground truth in tests. Faster books are added later
//! and must produce the same state as `RefBook` for any sequence of updates.

mod reference;

use std::fmt;

pub use reference::RefBook;

use crate::event::{DepthDiff, Level};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookError {
    /// A price that is zero or negative.
    InvalidPrice { side: Side, price: i64 },
    /// A negative quantity. Zero is valid and removes the level.
    InvalidQty { side: Side, price: i64, qty: i64 },
    /// Best bid at or above best ask: the local book no longer matches the exchange.
    Crossed { bid: i64, ask: i64 },
}

impl fmt::Display for BookError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BookError::InvalidPrice { side, price } => {
                write!(f, "{side:?} price {price} is not positive")
            }
            BookError::InvalidQty { side, price, qty } => {
                write!(f, "{side:?} level {price} has negative quantity {qty}")
            }
            BookError::Crossed { bid, ask } => write!(f, "crossed book: bid {bid} >= ask {ask}"),
        }
    }
}

impl std::error::Error for BookError {}

pub trait OrderBook {
    /// Remove every level and reset the update ID to 0.
    fn clear(&mut self);

    /// Update ID of the last applied change.
    fn update_id(&self) -> u64;

    fn set_update_id(&mut self, id: u64);

    /// Set one level. Quantity 0 removes it; removing a missing level does nothing.
    fn set(&mut self, side: Side, price: i64, qty: i64) -> Result<(), BookError>;

    fn best(&self, side: Side) -> Option<Level>;

    /// Number of levels on one side.
    fn len(&self, side: Side) -> usize;

    /// The best `n` levels of one side, best first, written into `out` (cleared first).
    fn top(&self, side: Side, n: usize, out: &mut Vec<Level>);

    fn is_empty(&self) -> bool {
        self.len(Side::Bid) == 0 && self.len(Side::Ask) == 0
    }

    /// Apply every level of a diff, then take its final update ID. Sequencing is not
    /// checked here: deciding whether a diff may be applied is the sync state machine's job.
    /// On error the book is left partially updated and must be resynchronized.
    fn apply(&mut self, diff: &DepthDiff) -> Result<(), BookError> {
        for l in &diff.bids {
            self.set(Side::Bid, l.price, l.qty)?;
        }
        for l in &diff.asks {
            self.set(Side::Ask, l.price, l.qty)?;
        }
        self.set_update_id(diff.final_update_id);
        Ok(())
    }

    /// Replace the whole book with a snapshot.
    fn load_snapshot(
        &mut self,
        update_id: u64,
        bids: &[Level],
        asks: &[Level],
    ) -> Result<(), BookError> {
        self.clear();
        for l in bids {
            self.set(Side::Bid, l.price, l.qty)?;
        }
        for l in asks {
            self.set(Side::Ask, l.price, l.qty)?;
        }
        self.set_update_id(update_id);
        Ok(())
    }

    /// The book is not crossed. Quantities and prices are checked when levels are set.
    fn check(&self) -> Result<(), BookError> {
        match (self.best(Side::Bid), self.best(Side::Ask)) {
            (Some(b), Some(a)) if b.price >= a.price => Err(BookError::Crossed {
                bid: b.price,
                ask: a.price,
            }),
            _ => Ok(()),
        }
    }
}

/// Shared argument checks for `OrderBook::set` implementations.
pub(crate) fn validate(side: Side, price: i64, qty: i64) -> Result<(), BookError> {
    if price <= 0 {
        return Err(BookError::InvalidPrice { side, price });
    }
    if qty < 0 {
        return Err(BookError::InvalidQty { side, price, qty });
    }
    Ok(())
}
