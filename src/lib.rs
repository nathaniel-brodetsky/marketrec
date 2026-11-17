//! marketrec: market-data recorder for Binance Spot.

pub mod book;
pub mod cli;
pub mod decimal;
pub mod event;
pub mod frames;
pub mod json;
pub mod net;
pub mod pipeline;
pub mod record;
pub mod replay;
pub mod sync;
pub mod tables;
#[cfg(test)]
pub(crate) mod testkit;
pub mod time;
pub mod verify;
