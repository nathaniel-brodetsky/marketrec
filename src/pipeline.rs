//! The decoding thread's work for one symbol: frames in, market events and quality out.
//!
//! `Pipeline::on_frame` is the hot path. The live recorder calls it for every frame the
//! network thread hands over; `replay` calls it for every frame of a recorded file. It is
//! deterministic: the same frames always produce the same calls on the `Sink`, including
//! the same snapshot request IDs, which is why a recorded snapshot (its `aux` is the ID)
//! is accepted again on replay.
//!
//! Besides decoding and book synchronization it checks the book against the exchange's
//! own best bid and ask: the book-ticker stream and the depth stream share update IDs, so
//! when the book reaches the ID of a book-ticker message both must show the same levels.

use crate::book::{OrderBook, Side};
use crate::decimal::ParseError;
use crate::event::{BookTicker, DepthDiff, Event, Level, Trade};
use crate::frames::{Frame, FrameKind, Header};
use crate::json::JsonDecoder;
use crate::sync::{Action, BookSync, DiffOutcome, Quality, State, SyncConfig};
use crate::timing::{LagChange, LateTracker, MsHistogram};

/// Data-quality events, in the order they happen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QualityEvent {
    Connected {
        conn: u64,
    },
    Disconnected {
        conn: u64,
        reason: String,
    },
    /// From the book synchronization: gap, stale snapshot, invalid book, overflow, reconnect.
    Sync(Quality),
    /// The book is valid as of `update_id`. `first`: the first time on this connection
    /// (`synced`); otherwise it is valid again after a loss within it (`resync`). Both
    /// depend only on the frames since the connection started, so every replay agrees.
    Synced {
        update_id: u64,
        first: bool,
    },
    /// The book disagrees with the exchange's best bid and ask at the same update ID.
    BboMismatch {
        update_id: u64,
        book_bid: Option<Level>,
        book_ask: Option<Level>,
        ticker: BookTicker,
    },
    /// A frame that could not be decoded. Processing continues.
    Undecodable {
        kind: FrameKind,
        reason: String,
    },
    /// The snapshot for request `id` could not be decoded; another one is requested.
    SnapshotFailed {
        id: u64,
        reason: String,
    },
    /// The exchange will close the connection at `shutdown_ts`.
    ServerShutdown {
        shutdown_ts: i64,
    },
    /// A depth update arrived more than a second after its exchange time: the book may be
    /// valid but is behind the market (`crate::timing`).
    Late {
        lag_ns: i64,
    },
    /// Updates arrive within half a second again.
    OnTime {
        lag_ns: i64,
        late_for_ns: i64,
        max_lag_ns: i64,
    },
}

impl QualityEvent {
    /// Every kind, in the order `SCHEMA.md` lists them.
    pub const KINDS: [&'static str; 16] = [
        "connected",
        "disconnected",
        "gap",
        "snapshot_too_old",
        "invalid_book",
        "buffer_overflow",
        "reconnect",
        "overflow",
        "synced",
        "resync",
        "bbo_mismatch",
        "undecodable",
        "snapshot_failed",
        "server_shutdown",
        "late",
        "on_time",
    ];

    /// Short stable name, the vocabulary of the quality journal.
    pub fn kind(&self) -> &'static str {
        match self {
            QualityEvent::Connected { .. } => "connected",
            QualityEvent::Disconnected { .. } => "disconnected",
            QualityEvent::Sync(Quality::Gap { .. }) => "gap",
            QualityEvent::Sync(Quality::SnapshotTooOld { .. }) => "snapshot_too_old",
            QualityEvent::Sync(Quality::Invalid(_)) => "invalid_book",
            QualityEvent::Sync(Quality::BufferOverflow { .. }) => "buffer_overflow",
            QualityEvent::Sync(Quality::Reconnect) => "reconnect",
            QualityEvent::Sync(Quality::Overflow { .. }) => "overflow",
            QualityEvent::Synced { first: true, .. } => "synced",
            QualityEvent::Synced { first: false, .. } => "resync",
            QualityEvent::BboMismatch { .. } => "bbo_mismatch",
            QualityEvent::Undecodable { .. } => "undecodable",
            QualityEvent::SnapshotFailed { .. } => "snapshot_failed",
            QualityEvent::ServerShutdown { .. } => "server_shutdown",
            QualityEvent::Late { .. } => "late",
            QualityEvent::OnTime { .. } => "on_time",
        }
    }
}

impl std::fmt::Display for QualityEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let lv = |l: &Option<Level>| l.map_or("-".to_owned(), |l| format!("{}x{}", l.price, l.qty));
        write!(f, "{}", self.kind())?;
        match self {
            QualityEvent::Connected { conn } => write!(f, " conn={conn}"),
            QualityEvent::Disconnected { conn, reason } => {
                write!(f, " conn={conn} reason={reason:?}")
            }
            QualityEvent::Sync(Quality::Gap { expected, got }) => {
                write!(f, " expected={expected} got={got}")
            }
            QualityEvent::Sync(Quality::SnapshotTooOld {
                snapshot,
                first_buffered,
            }) => {
                write!(f, " snapshot={snapshot} first_buffered={first_buffered}")
            }
            QualityEvent::Sync(Quality::Invalid(e)) => write!(f, " {e}"),
            QualityEvent::Sync(Quality::BufferOverflow { len }) => write!(f, " len={len}"),
            QualityEvent::Sync(Quality::Reconnect) => Ok(()),
            QualityEvent::Sync(Quality::Overflow { dropped }) => write!(f, " dropped={dropped}"),
            QualityEvent::Synced { update_id, .. } => write!(f, " update_id={update_id}"),
            QualityEvent::BboMismatch {
                update_id,
                book_bid,
                book_ask,
                ticker,
            } => write!(
                f,
                " update_id={update_id} book={}/{} ticker={}x{}/{}x{} (steps)",
                lv(book_bid),
                lv(book_ask),
                ticker.bid_px,
                ticker.bid_qty,
                ticker.ask_px,
                ticker.ask_qty
            ),
            QualityEvent::Undecodable { kind, reason } => {
                write!(f, " kind={} reason={reason:?}", kind.name())
            }
            QualityEvent::SnapshotFailed { id, reason } => write!(f, " id={id} reason={reason:?}"),
            QualityEvent::ServerShutdown { shutdown_ts } => write!(f, " shutdown_ts={shutdown_ts}"),
            QualityEvent::Late { lag_ns } => write!(f, " lag_ms={}", lag_ns / 1_000_000),
            QualityEvent::OnTime {
                lag_ns,
                late_for_ns,
                max_lag_ns,
            } => write!(
                f,
                " lag_ms={} late_for_ms={} max_lag_ms={}",
                lag_ns / 1_000_000,
                late_for_ns / 1_000_000,
                max_lag_ns / 1_000_000
            ),
        }
    }
}

/// Receives the pipeline's output. Every method has an empty default.
pub trait Sink {
    /// The book needs a REST snapshot; answer with a `Snapshot` frame whose `aux` is `id`.
    fn request_snapshot(&mut self, _id: u64) {}
    /// Every depth diff as received, with what the synchronization did with it.
    fn depth(&mut self, _recv_ts: i64, _diff: &DepthDiff, _outcome: DiffOutcome) {}
    fn book_ticker(&mut self, _recv_ts: i64, _ticker: &BookTicker) {}
    fn trade(&mut self, _recv_ts: i64, _trade: &Trade) {}
    fn quality(&mut self, _recv_ts: i64, _event: &QualityEvent) {}
    /// A message was dropped: another connection had delivered it first.
    fn duplicate(&mut self, _recv_ts: i64) {}
    /// A round-trip probe came back on connection `conn` (`crate::timing`).
    fn rtt(&mut self, _recv_ts: i64, _conn: u64, _rtt_ns: i64) {}
    /// After every frame: the link's state, and the book if it is valid.
    fn after_frame(&mut self, _recv_ts: i64, _state: FrameState, _book: Option<&dyn OrderBook>) {}
}

/// The link after a frame, for time accounting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FrameState {
    /// A stream connection is open.
    pub connected: bool,
    /// Its data arrives more than a second late (`crate::timing`).
    pub late: bool,
}

/// Counters for one run. Times are local receive times in nanoseconds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stats {
    pub frames: u64,
    pub depth: u64,
    pub depth_applied: u64,
    pub depth_ignored: u64,
    pub depth_buffered: u64,
    pub book_tickers: u64,
    pub trades: u64,
    pub snapshot_requests: u64,
    pub snapshots: u64,
    pub snapshot_failures: u64,
    pub gaps: u64,
    pub reconnects: u64,
    /// Times the book became valid again after having been valid before.
    pub resyncs: u64,
    pub undecodable: u64,
    /// Frames lost in full queues before they reached the pipeline.
    pub overflow_frames: u64,
    pub bbo_compared: u64,
    pub bbo_mismatches: u64,
    pub first_ts: Option<i64>,
    pub last_ts: Option<i64>,
    /// Time with a valid book, between the first and the last frame.
    pub synced_ns: i64,
    /// Lag of every depth update behind its exchange time.
    pub lag: MsHistogram,
    /// The lag of the latest depth update.
    pub last_lag_ns: Option<i64>,
    /// `late` intervals and the time in them.
    pub late_intervals: u64,
    pub late_ns: i64,
    /// Round trips of the recorder's own pings, and the latest one.
    pub rtt: MsHistogram,
    pub last_rtt_ns: Option<i64>,
    /// Messages dropped because another connection had delivered them first.
    pub duplicates: u64,
}

impl Stats {
    /// Share of the run with a valid book, 0 to 1.
    pub fn synced_share(&self) -> f64 {
        match (self.first_ts, self.last_ts) {
            (Some(a), Some(b)) if b > a => self.synced_ns as f64 / (b - a) as f64,
            _ => 0.0,
        }
    }
}

#[derive(Debug)]
pub enum PipelineError {
    /// The header's price or quantity step is not a valid decimal.
    BadStep(ParseError),
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineError::BadStep(e) => write!(f, "bad step in header: {e}"),
        }
    }
}

impl std::error::Error for PipelineError {}

/// Book-ticker messages waiting for the book to reach their update ID, by ID modulo size.
const PENDING_TICKERS: usize = 64;

pub struct Pipeline<B: OrderBook> {
    decoder: JsonDecoder,
    sync: BookSync<B>,
    actions: Vec<Action>,
    pending: [Option<BookTicker>; PENDING_TICKERS],
    stats: Stats,
    synced_since: Option<i64>,
    /// The book has been valid on the current connection.
    synced_on_connection: bool,
    /// The connection that delivers data now.
    lines: Lines,
    late: LateTracker,
}

/// The connections that deliver data, and how far each kind of message has come, to drop
/// the copies one connection delivers of what another already did. While a new connection
/// takes over from an old one both deliver the same stream for a while; whichever brings a
/// message first wins. Messages on one connection are never dropped, so a single connection
/// works as if this were not here.
#[derive(Debug, Default)]
struct Lines {
    open: Vec<u64>,
    /// Highest ID forwarded, and on which connection: final update ID of depth updates,
    /// trade ID, update ID of book tickers.
    depth: Option<(u64, u64)>,
    trade: Option<(u64, u64)>,
    ticker: Option<(u64, u64)>,
}

impl Lines {
    /// Whether the message `id` on `conn` is new: not one another connection delivered.
    fn admit(mark: &mut Option<(u64, u64)>, conn: u64, id: u64) -> bool {
        match *mark {
            Some((max, by)) if by != conn && id <= max => false,
            Some((max, _)) if id <= max => true,
            _ => {
                *mark = Some((id, conn));
                true
            }
        }
    }

    fn is_open(&self, conn: u64) -> bool {
        self.open.contains(&conn)
    }
}

impl<B: OrderBook> Pipeline<B> {
    pub fn new(header: &Header, book: B, cfg: SyncConfig) -> Result<Self, PipelineError> {
        let (price, qty) = header.grids().map_err(PipelineError::BadStep)?;
        Ok(Self::with_decoder(
            JsonDecoder::new(&header.symbol, price, qty, header.time_unit),
            book,
            cfg,
        ))
    }

    pub fn with_decoder(decoder: JsonDecoder, book: B, cfg: SyncConfig) -> Self {
        Self {
            decoder,
            sync: BookSync::new(book, cfg),
            actions: Vec::with_capacity(8),
            pending: [None; PENDING_TICKERS],
            stats: Stats::default(),
            synced_since: None,
            synced_on_connection: false,
            lines: Lines::default(),
            late: LateTracker::default(),
        }
    }

    pub fn state(&self) -> State {
        self.sync.state()
    }

    pub fn book(&self) -> &B {
        self.sync.book()
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// The counters as of the last frame, with a valid interval still running counted up
    /// to that frame. `stats` counts an interval only once it has ended.
    pub fn stats_so_far(&self) -> Stats {
        let mut s = self.stats.clone();
        if let (Some(since), Some(last)) = (self.synced_since, s.last_ts) {
            s.synced_ns += elapsed(since, last);
        }
        if let (Some(since), Some(last)) = (self.late.since(), s.last_ts) {
            s.late_ns += elapsed(since, last);
        }
        s
    }

    pub fn on_frame(&mut self, frame: Frame<'_>, sink: &mut impl Sink) {
        let ts = frame.recv_ts;
        self.stats.frames += 1;
        self.stats.first_ts.get_or_insert(ts);
        self.stats.last_ts = Some(self.stats.last_ts.map_or(ts, |t| t.max(ts)));
        self.actions.clear();
        self.sync.set_clock(u64::try_from(ts).unwrap_or(0));
        match frame.kind {
            FrameKind::Text => {
                // a connection that delivers data is open, even if a replay began after it
                // opened
                if !self.lines.is_open(frame.aux) {
                    self.lines.open.push(frame.aux);
                }
                self.on_text(ts, frame.aux, frame.payload, sink)
            }
            FrameKind::Snapshot => self.on_snapshot(ts, frame.aux, frame.payload, sink),
            FrameKind::Connected => {
                sink.quality(ts, &QualityEvent::Connected { conn: frame.aux });
                if self.lines.open.is_empty() {
                    self.end_late(ts);
                    self.synced_on_connection = false;
                    self.sync.on_connected(&mut self.actions);
                }
                // with another connection open this one takes over: the book carries on
                if !self.lines.is_open(frame.aux) {
                    self.lines.open.push(frame.aux);
                }
            }
            FrameKind::Disconnected => {
                let reason = String::from_utf8_lossy(frame.payload).into_owned();
                sink.quality(
                    ts,
                    &QualityEvent::Disconnected {
                        conn: frame.aux,
                        reason,
                    },
                );
                // a failed connection attempt never delivered data, and while another
                // connection is open the book carries on with it: nothing to rebuild
                let was_open = self.lines.is_open(frame.aux);
                self.lines.open.retain(|&c| c != frame.aux);
                if was_open && self.lines.open.is_empty() {
                    self.end_late(ts);
                    if frame.payload == crate::net::ws::STOPPED.as_bytes() {
                        self.sync.on_stop(); // the end of a recording is not a lost connection
                    } else {
                        self.sync.on_reconnect(&mut self.actions);
                    }
                }
            }
            FrameKind::Binary => {
                self.stats.undecodable += 1;
                let reason = "binary frames are not supported yet".to_owned();
                sink.quality(
                    ts,
                    &QualityEvent::Undecodable {
                        kind: FrameKind::Binary,
                        reason,
                    },
                );
            }
            FrameKind::Rtt => match <[u8; 8]>::try_from(frame.payload) {
                Ok(sent) => {
                    let rtt = ts - i64::from_le_bytes(sent);
                    self.stats.rtt.record(rtt);
                    self.stats.last_rtt_ns = Some(rtt);
                    sink.rtt(ts, frame.aux, rtt);
                }
                Err(_) => {
                    self.stats.undecodable += 1;
                    let reason = format!("rtt payload of {} bytes, not 8", frame.payload.len());
                    sink.quality(
                        ts,
                        &QualityEvent::Undecodable {
                            kind: FrameKind::Rtt,
                            reason,
                        },
                    );
                }
            },
            FrameKind::Overflow => {
                self.stats.overflow_frames += frame.aux;
                self.sync.on_overflow(frame.aux, &mut self.actions);
            }
        }
        self.drain_actions(ts, sink);
        let book = (self.sync.state() == State::Synced).then(|| self.sync.book() as &dyn OrderBook);
        let state = FrameState {
            connected: !self.lines.open.is_empty(),
            late: self.late.is_late(),
        };
        sink.after_frame(ts, state, book);
    }

    /// The connection ended or a new one began: a late interval ends with it.
    fn end_late(&mut self, ts: i64) {
        if let Some(since) = self.late.end() {
            self.stats.late_ns += elapsed(since, ts);
        }
    }

    /// Close the run: count the time since the last change of state up to the last frame.
    pub fn finish(&mut self) -> &Stats {
        if let (Some(since), Some(last)) = (self.synced_since.take(), self.stats.last_ts) {
            self.stats.synced_ns += elapsed(since, last);
        }
        if let (Some(since), Some(last)) = (self.late.end(), self.stats.last_ts) {
            self.stats.late_ns += elapsed(since, last);
        }
        &self.stats
    }

    fn on_text(&mut self, ts: i64, conn: u64, payload: &[u8], sink: &mut impl Sink) {
        let Self {
            decoder,
            sync,
            actions,
            stats,
            late,
            lines,
            ..
        } = self;
        let event = decoder.decode(payload);
        let new = match &event {
            Ok(Event::Depth(d)) => Lines::admit(&mut lines.depth, conn, d.final_update_id),
            Ok(Event::Trade(t)) => Lines::admit(&mut lines.trade, conn, t.trade_id),
            Ok(Event::BookTicker(t)) => Lines::admit(&mut lines.ticker, conn, t.update_id),
            _ => true,
        };
        if !new {
            stats.duplicates += 1;
            sink.duplicate(ts);
            return;
        }
        match event {
            Err(e) => {
                stats.undecodable += 1;
                sink.quality(
                    ts,
                    &QualityEvent::Undecodable {
                        kind: FrameKind::Text,
                        reason: e.to_string(),
                    },
                );
            }
            Ok(Event::Depth(diff)) => {
                stats.depth += 1;
                let outcome = sync.on_diff(diff, actions);
                match outcome {
                    DiffOutcome::Applied => stats.depth_applied += 1,
                    DiffOutcome::Ignored => stats.depth_ignored += 1,
                    DiffOutcome::Buffered => stats.depth_buffered += 1,
                    DiffOutcome::Resync => {}
                }
                sink.depth(ts, diff, outcome);
                let lag = ts - diff.event_ts;
                stats.lag.record(lag);
                stats.last_lag_ns = Some(lag);
                match late.on_update(ts, lag) {
                    Some(LagChange::Late { lag_ns }) => {
                        stats.late_intervals += 1;
                        sink.quality(ts, &QualityEvent::Late { lag_ns });
                    }
                    Some(LagChange::OnTime {
                        lag_ns,
                        late_for_ns,
                        max_lag_ns,
                    }) => {
                        stats.late_ns += late_for_ns;
                        sink.quality(
                            ts,
                            &QualityEvent::OnTime {
                                lag_ns,
                                late_for_ns,
                                max_lag_ns,
                            },
                        );
                    }
                    None => {}
                }
                if outcome == DiffOutcome::Applied {
                    self.check_pending_ticker(ts, sink);
                }
            }
            Ok(Event::BookTicker(t)) => {
                stats.book_tickers += 1;
                sink.book_ticker(ts, &t);
                self.on_ticker(ts, t, sink);
            }
            Ok(Event::Trade(t)) => {
                stats.trades += 1;
                sink.trade(ts, &t);
            }
            Ok(Event::ServerShutdown(s)) => {
                sink.quality(
                    ts,
                    &QualityEvent::ServerShutdown {
                        shutdown_ts: s.shutdown_ts,
                    },
                );
            }
        }
    }

    fn on_snapshot(&mut self, ts: i64, id: u64, body: &[u8], sink: &mut impl Sink) {
        match self.decoder.decode_snapshot(body) {
            Ok(snap) => {
                self.stats.snapshots += 1;
                self.sync.on_snapshot(id, &snap, &mut self.actions);
            }
            Err(e) => {
                self.stats.snapshot_failures += 1;
                sink.quality(
                    ts,
                    &QualityEvent::SnapshotFailed {
                        id,
                        reason: e.to_string(),
                    },
                );
                self.sync.on_snapshot_failed(id, &mut self.actions);
            }
        }
    }

    fn drain_actions(&mut self, ts: i64, sink: &mut impl Sink) {
        for i in 0..self.actions.len() {
            match self.actions[i] {
                Action::RequestSnapshot { id } => {
                    self.stats.snapshot_requests += 1;
                    sink.request_snapshot(id);
                }
                Action::Synced { update_id } => {
                    let first = !self.synced_on_connection;
                    self.synced_on_connection = true;
                    if !first {
                        self.stats.resyncs += 1;
                    }
                    sink.quality(ts, &QualityEvent::Synced { update_id, first });
                }
                Action::Quality(q) => {
                    match q {
                        Quality::Gap { .. } => self.stats.gaps += 1,
                        Quality::Reconnect => self.stats.reconnects += 1,
                        _ => {}
                    }
                    sink.quality(ts, &QualityEvent::Sync(q));
                }
            }
        }
        self.actions.clear();
        // time accounting: the state can only change while processing a frame
        match (self.sync.state() == State::Synced, self.synced_since) {
            (true, None) => self.synced_since = Some(ts),
            (false, Some(since)) => {
                self.stats.synced_ns += elapsed(since, ts);
                self.synced_since = None;
            }
            _ => {}
        }
        if self.sync.state() == State::Synced {
            self.check_pending_ticker(ts, sink);
        }
    }

    fn on_ticker(&mut self, ts: i64, t: BookTicker, sink: &mut impl Sink) {
        let synced = self.sync.state() == State::Synced;
        let book_id = self.sync.book().update_id();
        if synced && t.update_id == book_id {
            self.compare(ts, &t, sink);
        } else if !synced || t.update_id > book_id {
            self.pending[slot(t.update_id)] = Some(t);
        }
    }

    fn check_pending_ticker(&mut self, ts: i64, sink: &mut impl Sink) {
        let id = self.sync.book().update_id();
        let i = slot(id);
        if let Some(t) = self.pending[i]
            && t.update_id == id
        {
            self.pending[i] = None;
            self.compare(ts, &t, sink);
        }
    }

    fn compare(&mut self, ts: i64, t: &BookTicker, sink: &mut impl Sink) {
        self.stats.bbo_compared += 1;
        let book = self.sync.book();
        let (bid, ask) = (book.best(Side::Bid), book.best(Side::Ask));
        let as_level = |price, qty| (price > 0).then_some(Level { price, qty });
        if bid != as_level(t.bid_px, t.bid_qty) || ask != as_level(t.ask_px, t.ask_qty) {
            self.stats.bbo_mismatches += 1;
            let ev = QualityEvent::BboMismatch {
                update_id: t.update_id,
                book_bid: bid,
                book_ask: ask,
                ticker: *t,
            };
            sink.quality(ts, &ev);
        }
    }
}

/// Receive times come from a monotonic clock; a damaged file must still not overflow.
fn elapsed(from: i64, to: i64) -> i64 {
    to.saturating_sub(from).max(0)
}

fn slot(update_id: u64) -> usize {
    (update_id % PENDING_TICKERS as u64) as usize
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::book::RefBook;
    use crate::frames::OwnedFrame;
    use crate::json::TimeUnit;

    fn header() -> Header {
        Header {
            symbol: "BTCUSDT".into(),
            price_step: "0.01000000".into(),
            qty_step: "0.00001000".into(),
            time_unit: TimeUnit::Milli,
            recorder: "test".into(),
        }
    }

    /// Everything the pipeline emits, as text, in order.
    #[derive(Default)]
    struct Log(Vec<String>);

    impl Sink for Log {
        fn request_snapshot(&mut self, id: u64) {
            self.0.push(format!("request {id}"));
        }
        fn depth(&mut self, _: i64, d: &DepthDiff, outcome: DiffOutcome) {
            self.0.push(format!(
                "depth {}-{} {outcome:?}",
                d.first_update_id, d.final_update_id
            ));
        }
        fn book_ticker(&mut self, _: i64, t: &BookTicker) {
            self.0.push(format!("ticker {}", t.update_id));
        }
        fn trade(&mut self, _: i64, t: &Trade) {
            self.0.push(format!("trade {}", t.trade_id));
        }
        fn quality(&mut self, ts: i64, e: &QualityEvent) {
            self.0.push(format!("quality@{ts} {e:?}"));
        }
    }

    fn text(ts: i64, payload: String) -> OwnedFrame {
        OwnedFrame {
            kind: FrameKind::Text,
            recv_ts: ts,
            aux: 1,
            payload: payload.into_bytes(),
        }
    }

    /// Prices are given in cents, quantities in steps of 0.00001.
    fn depth(
        ts: i64,
        first: u64,
        last: u64,
        bids: &[(i64, i64)],
        asks: &[(i64, i64)],
    ) -> OwnedFrame {
        let lv = |l: &[(i64, i64)]| {
            l.iter()
                .map(|&(p, q)| {
                    format!(
                        r#"["{}.{:02}","{}.{:05}"]"#,
                        p / 100,
                        p % 100,
                        q / 100_000,
                        q % 100_000
                    )
                })
                .collect::<Vec<_>>()
                .join(",")
        };
        text(
            ts,
            format!(
                r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":{first},"u":{last},"b":[{}],"a":[{}]}}}}"#,
                lv(bids),
                lv(asks)
            ),
        )
    }

    fn ticker(ts: i64, id: u64, bid: (i64, i64), ask: (i64, i64)) -> OwnedFrame {
        let px = |p: i64| format!("{}.{:02}", p / 100, p % 100);
        let q = |q: i64| format!("{}.{:05}", q / 100_000, q % 100_000);
        text(
            ts,
            format!(
                r#"{{"stream":"btcusdt@bookTicker","data":{{"u":{id},"s":"BTCUSDT","b":"{}","B":"{}","a":"{}","A":"{}"}}}}"#,
                px(bid.0),
                q(bid.1),
                px(ask.0),
                q(ask.1)
            ),
        )
    }

    fn trade(ts: i64, id: u64) -> OwnedFrame {
        text(
            ts,
            format!(
                r#"{{"stream":"btcusdt@trade","data":{{"e":"trade","E":1,"s":"BTCUSDT","t":{id},"p":"100.00","q":"0.00100","T":1,"m":false,"M":true}}}}"#
            ),
        )
    }

    fn snapshot(ts: i64, request: u64, last: u64) -> OwnedFrame {
        let body = format!(
            r#"{{"lastUpdateId":{last},"bids":[["99.00","0.00005"],["98.00","0.00007"]],"asks":[["101.00","0.00002"],["102.00","0.00003"]]}}"#
        );
        OwnedFrame {
            kind: FrameKind::Snapshot,
            recv_ts: ts,
            aux: request,
            payload: body.into_bytes(),
        }
    }

    fn link(kind: FrameKind, ts: i64, conn: u64, payload: &str) -> OwnedFrame {
        OwnedFrame {
            kind,
            recv_ts: ts,
            aux: conn,
            payload: payload.as_bytes().to_vec(),
        }
    }

    fn run(frames: &[OwnedFrame]) -> (Pipeline<RefBook>, Log) {
        let mut p = Pipeline::new(&header(), RefBook::new(), SyncConfig::default()).unwrap();
        let mut log = Log::default();
        for f in frames {
            p.on_frame(f.as_frame(), &mut log);
        }
        p.finish();
        (p, log)
    }

    fn normal_start() -> Vec<OwnedFrame> {
        vec![
            link(FrameKind::Connected, 0, 1, "wss://example"),
            depth(10, 99, 100, &[], &[]),
            depth(20, 101, 102, &[(9900, 6)], &[]),
            ticker(25, 102, (9900, 6), (10100, 2)),
            snapshot(30, 10, 100), // answers the request made at 10
            trade(35, 7),
            depth(40, 103, 104, &[], &[(10100, 0)]),
            ticker(45, 104, (9900, 6), (10200, 3)),
            depth(100, 105, 105, &[(9950, 1)], &[]),
        ]
    }

    #[test]
    fn kinds_lists_every_kind_once() {
        use crate::book::BookError;
        let t = BookTicker {
            update_id: 1,
            bid_px: 1,
            bid_qty: 1,
            ask_px: 2,
            ask_qty: 1,
        };
        let all = [
            QualityEvent::Connected { conn: 1 },
            QualityEvent::Disconnected {
                conn: 1,
                reason: String::new(),
            },
            QualityEvent::Sync(Quality::Gap {
                expected: 1,
                got: 3,
            }),
            QualityEvent::Sync(Quality::SnapshotTooOld {
                snapshot: 1,
                first_buffered: 3,
            }),
            QualityEvent::Sync(Quality::Invalid(BookError::Crossed { bid: 2, ask: 1 })),
            QualityEvent::Sync(Quality::BufferOverflow { len: 1 }),
            QualityEvent::Sync(Quality::Reconnect),
            QualityEvent::Sync(Quality::Overflow { dropped: 1 }),
            QualityEvent::Synced {
                update_id: 1,
                first: true,
            },
            QualityEvent::Synced {
                update_id: 1,
                first: false,
            },
            QualityEvent::BboMismatch {
                update_id: 1,
                book_bid: None,
                book_ask: None,
                ticker: t,
            },
            QualityEvent::Undecodable {
                kind: FrameKind::Binary,
                reason: String::new(),
            },
            QualityEvent::SnapshotFailed {
                id: 1,
                reason: String::new(),
            },
            QualityEvent::ServerShutdown { shutdown_ts: 1 },
            QualityEvent::Late { lag_ns: 1 },
            QualityEvent::OnTime {
                lag_ns: 1,
                late_for_ns: 1,
                max_lag_ns: 1,
            },
        ];
        let kinds: Vec<&str> = all.iter().map(QualityEvent::kind).collect();
        assert_eq!(kinds, QualityEvent::KINDS);
    }

    #[test]
    fn normal_flow() {
        let (p, log) = run(&normal_start());
        assert_eq!(p.state(), State::Synced);
        assert_eq!(p.book().update_id(), 105);
        assert_eq!(
            log.0,
            vec![
                "quality@0 Connected { conn: 1 }",
                "depth 99-100 Buffered",
                "request 10",
                "depth 101-102 Buffered",
                "ticker 102",
                "quality@30 Synced { update_id: 102, first: true }",
                "trade 7",
                "depth 103-104 Applied",
                "ticker 104",
                "depth 105-105 Applied",
            ]
        );
        let s = p.stats();
        assert_eq!(
            (s.frames, s.depth, s.depth_applied, s.depth_buffered),
            (9, 4, 2, 2)
        );
        assert_eq!(
            (s.book_tickers, s.trades, s.snapshots, s.snapshot_requests),
            (2, 1, 1, 1)
        );
        assert_eq!(
            (s.bbo_compared, s.bbo_mismatches, s.gaps, s.resyncs),
            (2, 0, 0, 0)
        );
        assert_eq!(
            s.synced_ns, 70,
            "synced from the snapshot at 30 to the last frame at 100"
        );
        assert!((s.synced_share() - 0.7).abs() < 1e-9);
    }

    #[test]
    fn requests_are_named_by_the_receive_time_of_their_cause() {
        let mut frames = normal_start();
        frames.push(depth(110, 107, 107, &[], &[])); // gap
        let (_, log) = run(&frames);
        let requests: Vec<&String> = log.0.iter().filter(|l| l.starts_with("request")).collect();
        assert_eq!(requests, ["request 10", "request 110"]);
    }

    #[test]
    fn replay_from_the_middle_of_a_connection_joins_at_the_next_one() {
        let mut frames = normal_start();
        frames.push(link(FrameKind::Disconnected, 110, 1, "connection reset"));
        frames.push(link(FrameKind::Connected, 120, 2, "wss://example"));
        frames.push(depth(130, 300, 301, &[(9800, 4)], &[]));
        frames.push(snapshot(140, 130, 300));
        frames.push(depth(150, 302, 302, &[], &[(10300, 1)]));
        let (full, full_log) = run(&frames);
        // without the connection's start the replay cannot sync, and must not get stuck
        let (part, part_log) = run(&frames[6..]);
        assert_eq!(part.state(), State::Synced);
        assert_eq!(part.book().update_id(), full.book().update_id());
        let after = |log: &[String]| -> Vec<String> {
            let i = log
                .iter()
                .position(|l| l.contains("Connected { conn: 2 }"))
                .unwrap();
            log[i..].to_vec()
        };
        assert_eq!(after(&part_log.0), after(&full_log.0));
        assert_eq!(
            part.stats().reconnects,
            1,
            "connection 1 delivered data in the replay, so the replay saw it end"
        );
    }

    #[test]
    fn stats_so_far_count_the_valid_time_still_running() {
        let mut p = Pipeline::new(&header(), RefBook::new(), SyncConfig::default()).unwrap();
        for f in normal_start() {
            p.on_frame(f.as_frame(), &mut Log::default());
        }
        assert_eq!(p.stats().synced_ns, 0, "the interval has not ended");
        assert_eq!(
            p.stats_so_far().synced_ns,
            70,
            "from the snapshot at 30 to 100"
        );
        assert_eq!(p.finish().synced_ns, 70);
    }

    #[test]
    fn late_and_on_time_follow_the_lag_of_depth_updates() {
        const SEC: i64 = 1_000_000_000;
        const MS: i64 = 1_000_000;
        // a depth update received at `ts` that the exchange stamped `lag` earlier (in ms)
        let d = |ts: i64, lag: i64, id: u64| {
            text(
                ts,
                format!(
                    r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":{},"s":"BTCUSDT","U":{id},"u":{id},"b":[],"a":[]}}}}"#,
                    (ts - lag) / MS
                ),
            )
        };
        let frames = vec![
            link(FrameKind::Connected, 0, 1, "wss://example"),
            d(SEC, 100 * MS, 1),
            d(2 * SEC, 1500 * MS, 2),
            d(3 * SEC, 3000 * MS, 3),
            d(4 * SEC, 700 * MS, 4), // between the thresholds: still late
            d(5 * SEC, 200 * MS, 5),
            d(6 * SEC, 2000 * MS, 6),
            link(FrameKind::Disconnected, 8 * SEC, 1, "connection reset"),
        ];
        let (p, log) = run(&frames);
        let quality: Vec<&String> = log
            .0
            .iter()
            .filter(|l| l.contains("Late") || l.contains("OnTime"))
            .collect();
        assert_eq!(
            quality,
            [
                "quality@2000000000 Late { lag_ns: 1500000000 }",
                "quality@5000000000 OnTime { lag_ns: 200000000, late_for_ns: 3000000000, max_lag_ns: 3000000000 }",
                "quality@6000000000 Late { lag_ns: 2000000000 }",
            ]
        );
        let s = p.stats();
        assert_eq!(s.late_intervals, 2);
        assert_eq!(
            s.late_ns,
            3 * SEC + 2 * SEC,
            "the second ends with its connection"
        );
        assert_eq!(s.lag.count(), 6);
        assert_eq!(s.lag.max_ns, 3000 * MS);
        assert_eq!(s.last_lag_ns, Some(2000 * MS));

        // a late interval still running is counted by stats_so_far, and by finish
        let mut p = Pipeline::new(&header(), RefBook::new(), SyncConfig::default()).unwrap();
        for f in &frames[..7] {
            p.on_frame(f.as_frame(), &mut Log::default());
        }
        assert_eq!(p.stats().late_ns, 3 * SEC);
        assert_eq!(p.stats_so_far().late_ns, 3 * SEC);
        p.on_frame(d(7 * SEC, 1500 * MS, 7).as_frame(), &mut Log::default());
        assert_eq!(p.stats_so_far().late_ns, 4 * SEC);
        assert_eq!(p.finish().late_ns, 4 * SEC);
        assert_eq!(
            QualityEvent::OnTime {
                lag_ns: 200 * MS,
                late_for_ns: 3 * SEC,
                max_lag_ns: 3 * SEC
            }
            .to_string(),
            "on_time lag_ms=200 late_for_ms=3000 max_lag_ms=3000"
        );
    }

    /// A depth message: `U`, `u`, bids and asks.
    type DepthMsg = (u64, u64, Vec<(i64, i64)>, Vec<(i64, i64)>);

    /// A frame on connection `conn`.
    fn on(conn: u64, mut f: OwnedFrame) -> OwnedFrame {
        f.aux = conn;
        f
    }

    #[test]
    fn a_second_connection_takes_over_without_touching_the_book() {
        let mut frames = normal_start(); // connection 1, synced at 102, book at 105
        frames.push(link(FrameKind::Connected, 101, 2, "wss://example"));
        frames.push(on(2, depth(102, 104, 105, &[], &[(10100, 0)]))); // copy of what 1 sent
        frames.push(on(2, depth(103, 106, 107, &[(9960, 2)], &[]))); // 2 is ahead now
        frames.push(on(1, depth(104, 106, 107, &[(9960, 2)], &[]))); // the copy from 1
        frames.push(on(2, trade(105, 7))); // a copy of the trade 1 delivered at 35
        frames.push(on(2, trade(106, 8)));
        frames.push(on(1, trade(107, 8)));
        frames.push(link(FrameKind::Disconnected, 110, 1, "handover"));
        frames.push(on(2, depth(120, 108, 108, &[(9970, 1)], &[])));
        let (p, log) = run(&frames);
        assert_eq!(p.state(), State::Synced);
        assert_eq!(p.book().update_id(), 108);
        let s = p.stats();
        assert_eq!((s.reconnects, s.resyncs, s.gaps), (0, 0, 0));
        assert_eq!(s.duplicates, 4, "two depth copies, two trade copies");
        let depth: Vec<&String> = log.0.iter().filter(|l| l.starts_with("depth 10")).collect();
        assert_eq!(
            depth,
            [
                "depth 101-102 Buffered",
                "depth 103-104 Applied",
                "depth 105-105 Applied",
                "depth 106-107 Applied",
                "depth 108-108 Applied"
            ],
            "every update once"
        );
        let trades: Vec<&String> = log.0.iter().filter(|l| l.starts_with("trade")).collect();
        assert_eq!(trades, ["trade 7", "trade 8"]);
        assert!(!log.0.iter().any(|l| l.contains("Reconnect")));

        // the last connection closing is a lost connection as before
        frames.push(link(FrameKind::Disconnected, 130, 2, "reset"));
        let (p, _) = run(&frames);
        assert_eq!((p.stats().reconnects, p.state()), (1, State::Resyncing));
    }

    proptest! {
        /// Two connections deliver the same updates, each in order and complete, batched
        /// differently and interleaved in any way: the book equals the exchange's, every
        /// update counts once, and no gap is ever seen.
        #[test]
        fn two_copies_of_the_stream_make_one_book(
            updates in proptest::collection::vec((any::<bool>(), 0i64..40, 0i64..4), 1..60),
            cuts_a in proptest::collection::vec(any::<bool>(), 60),
            cuts_b in proptest::collection::vec(any::<bool>(), 60),
            order in proptest::collection::vec(any::<bool>(), 120),
        ) {
            // the exchange: the snapshot book at 100, then updates 101, 102, ...
            let mut truth: [std::collections::BTreeMap<i64, i64>; 2] =
                [[(9900, 5), (9800, 7)].into(), [(10100, 2), (10200, 3)].into()];
            let level = |&(bid, off, q): &(bool, i64, i64)| {
                if bid { (0usize, 9000 + off * 10, q) } else { (1usize, 10300 + off * 10, q) }
            };
            for u in &updates {
                let (side, price, q) = level(u);
                if q == 0 { truth[side].remove(&price); } else { truth[side].insert(price, q); }
            }
            // one connection's messages: consecutive updates batched where `cuts` says,
            // each batch carrying the final quantity of every level it changed
            let batches = |cuts: &[bool]| -> Vec<DepthMsg> {
                let mut out = Vec::new();
                let mut start = 0;
                for i in 0..updates.len() {
                    if i + 1 == updates.len() || cuts[i] {
                        let mut sides = [std::collections::BTreeMap::new(), std::collections::BTreeMap::new()];
                        for u in &updates[start..=i] {
                            let (side, price, q) = level(u);
                            sides[side].insert(price, q);
                        }
                        let [b, a] = sides;
                        out.push((101 + start as u64, 101 + i as u64, b.into_iter().collect(), a.into_iter().collect()));
                        start = i + 1;
                    }
                }
                out
            };
            let (a, b) = (batches(&cuts_a), batches(&cuts_b));
            let mut frames = vec![
                link(FrameKind::Connected, 1, 1, "wss://example"),
                on(1, depth(10, 100, 100, &[], &[])),
                snapshot(11, 10, 100),
                link(FrameKind::Connected, 12, 2, "wss://example"),
            ];
            let (mut ia, mut ib, mut ts) = (0, 0, 20);
            for &take_a in order.iter().chain(std::iter::repeat(&true)) {
                let (conn, list, i) = match (take_a, ia < a.len(), ib < b.len()) {
                    (_, false, false) => break,
                    (true, true, _) | (false, true, false) => (1, &a, &mut ia),
                    _ => (2, &b, &mut ib),
                };
                let (first, last, bids, asks) = &list[*i];
                *i += 1;
                frames.push(on(conn, depth(ts, *first, *last, bids, asks)));
                ts += 1;
            }
            let (p, log) = run(&frames);
            prop_assert_eq!(p.state(), State::Synced, "{:?}", log.0);
            prop_assert_eq!(p.stats().gaps, 0);
            prop_assert_eq!(p.book().update_id(), 100 + updates.len() as u64);
            for (side, s) in [Side::Bid, Side::Ask].into_iter().enumerate() {
                let mut got = Vec::new();
                p.book().top(s, 1000, &mut got);
                let mut want: Vec<Level> = truth[side].iter().map(|(&price, &qty)| Level { price, qty }).collect();
                if side == 0 { want.reverse(); }
                prop_assert_eq!(got, want);
            }
            // each update forwarded once: the final IDs of what reached the sink only grow
            let finals: Vec<u64> = log
                .0
                .iter()
                .filter_map(|l| l.strip_prefix("depth "))
                .map(|l| l.split(['-', ' ']).nth(1).unwrap().parse().unwrap())
                .collect();
            prop_assert!(finals.windows(2).all(|w| w[0] < w[1]), "{:?}", finals);
            prop_assert_eq!(
                p.stats().depth + p.stats().duplicates,
                1 + (a.len() + b.len()) as u64,
                "every message is either forwarded or dropped as a copy"
            );
        }
    }

    #[test]
    fn the_fast_book_gives_the_reference_result_on_a_long_session() {
        use crate::book::LadderBook;
        use crate::testkit::{DAY0, MS, SEC, Session};
        let mut s = Session::new(31);
        s.connect(DAY0);
        s.run(DAY0 + MS, DAY0 + 20 * SEC, 20 * MS);
        s.lose(2);
        s.run(DAY0 + 20 * SEC, DAY0 + 40 * SEC, 20 * MS);
        s.disconnect(DAY0 + 40 * SEC, "reset");
        s.connect(DAY0 + 41 * SEC);
        s.run(DAY0 + 41 * SEC + MS, DAY0 + 60 * SEC, 20 * MS);
        let h = crate::testkit::header();
        let (mut a, mut b) = (
            Pipeline::new(&h, RefBook::new(), SyncConfig::default()).unwrap(),
            Pipeline::new(&h, LadderBook::new(), SyncConfig::default()).unwrap(),
        );
        let (mut la, mut lb) = (Log::default(), Log::default());
        for f in &s.frames {
            a.on_frame(f.as_frame(), &mut la);
            b.on_frame(f.as_frame(), &mut lb);
        }
        assert_eq!(la.0, lb.0);
        assert_eq!(a.finish(), b.finish());
        let (mut x, mut y) = (Vec::new(), Vec::new());
        for side in [Side::Bid, Side::Ask] {
            a.book().top(side, usize::MAX, &mut x);
            b.book().top(side, usize::MAX, &mut y);
            assert_eq!(x, y);
        }
        assert!(a.stats().bbo_compared > 100 && a.stats().gaps == 1);
    }

    #[test]
    fn bbo_mismatch_is_reported() {
        let mut frames = normal_start();
        frames[7] = ticker(45, 104, (9900, 6), (10200, 4)); // the book has 3 at 102.00
        let (p, log) = run(&frames);
        assert_eq!(p.stats().bbo_mismatches, 1);
        assert!(
            log.0
                .iter()
                .any(|l| l.starts_with("quality@45 BboMismatch { update_id: 104"))
        );
    }

    #[test]
    fn ticker_ahead_of_the_book_is_compared_when_the_book_gets_there() {
        // the usual live order: the real-time ticker arrives before the 100 ms depth batch
        let mut frames = normal_start();
        frames.push(ticker(105, 107, (9950, 1), (10200, 9)));
        frames.push(depth(110, 106, 107, &[], &[(10200, 9)]));
        let (p, log) = run(&frames);
        assert_eq!(p.book().update_id(), 107);
        assert_eq!((p.stats().bbo_compared, p.stats().bbo_mismatches), (3, 0));
        let mut wrong = normal_start();
        wrong.push(ticker(105, 107, (9950, 1), (10200, 8)));
        wrong.push(depth(110, 106, 107, &[], &[(10200, 9)]));
        let (p, _) = run(&wrong);
        assert_eq!((p.stats().bbo_compared, p.stats().bbo_mismatches), (3, 1));
        assert!(
            log.0
                .ends_with(&["ticker 107".to_owned(), "depth 106-107 Applied".to_owned()])
        );
    }

    #[test]
    fn ticker_that_never_matches_a_book_id_is_not_compared() {
        let mut frames = normal_start();
        frames[7] = ticker(45, 103, (9900, 6), (10200, 3)); // 103 is inside the diff 103-104
        let (p, _) = run(&frames);
        assert_eq!(p.stats().bbo_compared, 1);
    }

    #[test]
    fn gap_and_resync() {
        let mut frames = normal_start();
        frames.push(depth(110, 107, 107, &[], &[])); // 106 is missing
        frames.push(snapshot(120, 110, 107));
        frames.push(depth(130, 108, 108, &[], &[]));
        let (p, log) = run(&frames);
        assert_eq!(p.state(), State::Synced);
        assert_eq!(p.book().update_id(), 108);
        let s = p.stats();
        assert_eq!((s.gaps, s.resyncs, s.snapshot_requests), (1, 1, 2));
        assert_eq!(s.synced_ns, (110 - 30) + (130 - 120));
        assert!(
            log.0
                .contains(&"quality@110 Sync(Gap { expected: 106, got: 107 })".to_owned())
        );
        assert!(
            log.0
                .contains(&"quality@120 Synced { update_id: 107, first: false }".to_owned())
        );
    }

    #[test]
    fn reconnect_rebuilds_the_book() {
        let mut frames = normal_start();
        frames.push(link(FrameKind::Disconnected, 110, 1, "connection reset"));
        frames.push(link(FrameKind::Connected, 120, 2, "wss://example"));
        frames.push(depth(130, 300, 301, &[], &[]));
        frames.push(snapshot(140, 130, 300));
        let (p, log) = run(&frames);
        assert_eq!(p.book().update_id(), 301);
        assert_eq!((p.stats().reconnects, p.stats().resyncs), (1, 0));
        assert!(
            log.0
                .contains(&"quality@140 Synced { update_id: 301, first: true }".to_owned()),
            "the first valid book of a connection is `synced`"
        );
        assert!(log.0.contains(
            &r#"quality@110 Disconnected { conn: 1, reason: "connection reset" }"#.to_owned()
        ));
        assert!(log.0.contains(&"quality@110 Sync(Reconnect)".to_owned()));
    }

    #[test]
    fn deliberate_stop_ends_the_valid_time_without_counting_a_reconnect() {
        let mut frames = normal_start();
        frames.push(link(FrameKind::Disconnected, 150, 1, "stopped"));
        frames.push(trade(200, 8));
        let (p, log) = run(&frames);
        assert_eq!(p.stats().reconnects, 0);
        assert_eq!(
            p.stats().synced_ns,
            150 - 30,
            "valid from the snapshot until the stop"
        );
        assert!(!log.0.iter().any(|l| l.contains("Reconnect")));
    }

    #[test]
    fn failed_connection_attempts_do_not_reset_the_book() {
        let mut frames = normal_start();
        frames.push(link(
            FrameKind::Disconnected,
            110,
            7,
            "connect failed: refused",
        ));
        let (p, log) = run(&frames);
        assert_eq!(
            p.state(),
            State::Synced,
            "connection 7 never delivered data"
        );
        assert_eq!(p.stats().reconnects, 0);
        assert!(
            log.0.contains(
                &r#"quality@110 Disconnected { conn: 7, reason: "connect failed: refused" }"#
                    .to_owned()
            )
        );
    }

    #[test]
    fn overflow_marker_rebuilds_the_book() {
        let mut frames = normal_start();
        frames.push(link(FrameKind::Overflow, 110, 42, "network queue"));
        frames.push(depth(120, 200, 201, &[], &[]));
        frames.push(snapshot(130, 120, 200));
        let (p, log) = run(&frames);
        assert_eq!(p.book().update_id(), 201);
        assert_eq!((p.stats().overflow_frames, p.stats().resyncs), (42, 1));
        assert!(
            log.0
                .contains(&"quality@110 Sync(Overflow { dropped: 42 })".to_owned())
        );
    }

    #[test]
    fn broken_frames_do_not_stop_processing() {
        let mut frames = normal_start();
        frames.insert(6, text(36, "{not json".into()));
        frames.insert(7, link(FrameKind::Binary, 37, 1, "\u{0}"));
        let (p, log) = run(&frames);
        assert_eq!(p.stats().undecodable, 2);
        assert_eq!(p.book().update_id(), 105);
        assert!(
            log.0
                .iter()
                .any(|l| l.starts_with("quality@36 Undecodable { kind: Text"))
        );
    }

    #[test]
    fn failed_snapshot_is_requested_again() {
        let mut frames = normal_start();
        frames.insert(
            4,
            OwnedFrame {
                kind: FrameKind::Snapshot,
                recv_ts: 28,
                aux: 10,
                payload: br#"{"code":-1003,"msg":"Too many requests"}"#.to_vec(),
            },
        );
        frames[5] = snapshot(30, 28, 100); // asked again at the failure
        let (p, log) = run(&frames);
        assert_eq!(p.state(), State::Synced);
        assert_eq!(
            (p.stats().snapshot_failures, p.stats().snapshot_requests),
            (1, 2)
        );
        assert!(
            log.0
                .iter()
                .any(|l| l.starts_with("quality@28 SnapshotFailed { id: 10,"))
        );
    }

    #[test]
    fn server_shutdown_is_reported() {
        let mut frames = normal_start();
        frames.push(text(200, r#"{"stream":"!serverShutdown","data":{"e":"serverShutdown","E":1770123456789,"T":1770124000000}}"#.into()));
        let (_, log) = run(&frames);
        assert_eq!(
            log.0.last().unwrap(),
            "quality@200 ServerShutdown { shutdown_ts: 1770124000000000000 }"
        );
    }

    #[test]
    fn same_frames_same_output() {
        let mut frames = normal_start();
        frames.push(depth(110, 107, 107, &[], &[]));
        frames.push(snapshot(120, 110, 107));
        let (a, log_a) = run(&frames);
        let (b, log_b) = run(&frames);
        assert_eq!(log_a.0, log_b.0);
        assert_eq!(a.stats(), b.stats());
    }

    #[test]
    fn events_print_with_their_journal_name() {
        let gap = QualityEvent::Sync(Quality::Gap {
            expected: 106,
            got: 107,
        });
        assert_eq!(gap.to_string(), "gap expected=106 got=107");
        assert_eq!(
            QualityEvent::Synced {
                update_id: 7,
                first: false
            }
            .to_string(),
            "resync update_id=7"
        );
    }

    #[test]
    fn bad_header_step_is_an_error() {
        let mut h = header();
        h.price_step = "0".into();
        assert!(Pipeline::new(&h, RefBook::new(), SyncConfig::default()).is_err());
    }

    proptest! {
        #[test]
        fn never_panics(frames in proptest::collection::vec(
            (0u8..6, any::<i64>(), 0u64..4, proptest::collection::vec(any::<u8>(), 0..80)), 0..40))
        {
            let mut p = Pipeline::new(&header(), RefBook::new(), SyncConfig::default()).unwrap();
            for (k, ts, aux, payload) in frames {
                if let Some(kind) = FrameKind::from_u8(k) {
                    p.on_frame(Frame { kind, recv_ts: ts, aux, payload: &payload }, &mut Log::default());
                }
            }
            p.finish();
        }
    }
}
