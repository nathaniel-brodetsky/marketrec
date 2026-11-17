//! Network access to Binance. Everything here is I/O; decisions live in `sync` and
//! `pipeline`, which never touch the network.

pub mod listen;
pub mod rest;
pub mod ws;
