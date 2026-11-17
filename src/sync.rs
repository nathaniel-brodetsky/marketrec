//! Order book synchronization: snapshot plus diffs, as Binance Spot documents it.
//!
//! States:
//! - `Buffering`: at start. Diffs are buffered; a snapshot is requested after the first one.
//! - `Synced`: the book matches the exchange as of its update ID; diffs are applied live.
//! - `Resyncing`: after a gap, an invalid book or a reconnect. Same as `Buffering`, but the
//!   preceding data is known to be broken.
//!
//! Procedure (Binance Spot, "How to manage a local order book correctly"):
//! 1. Buffer diffs; note `U` of the first one; request a snapshot.
//! 2. If the snapshot's `lastUpdateId` is below that `U`, the snapshot is too old: request again.
//! 3. Drop buffered diffs with `u <= lastUpdateId`, load the snapshot, apply the rest.
//! 4. Live: ignore a diff with `u <= book id` (already applied; Binance ignores only
//!    `u < book id`, which is equivalent because a level update sets, not adds); a diff with
//!    `U > book id + 1` is a gap: drop the book and start again; otherwise apply it.
//!
//! The state machine does no I/O. It reports what the caller must do through `Action`s,
//! which also carry the data-quality events.
//!
//! Snapshot requests are named by the caller's clock (`set_clock`): the pipeline sets it to
//! the receive time of the frame being processed, so a request's ID depends only on the
//! frame that caused it. A replay that starts in the middle of a recording therefore asks
//! for the same IDs as the live run once both have seen the same connection start, and from
//! there on accepts the same recorded snapshots.

use crate::book::{BookError, OrderBook};
use crate::event::{DepthDiff, Snapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Buffering,
    Synced,
    Resyncing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quality {
    /// A diff did not start right after the book: `expected` is book id + 1, `got` is its `U`.
    Gap { expected: u64, got: u64 },
    /// The snapshot is older than the first buffered diff; another one is requested.
    SnapshotTooOld { snapshot: u64, first_buffered: u64 },
    /// A diff or snapshot produced an invalid book.
    Invalid(BookError),
    /// The diff buffer reached its limit while waiting for a snapshot and was dropped.
    BufferOverflow { len: usize },
    /// The connection was lost; the book is rebuilt from scratch.
    Reconnect,
    /// `dropped` frames were lost in a full queue; the book is rebuilt from scratch.
    Overflow { dropped: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Fetch a REST snapshot and pass it to `on_snapshot` with this `id`.
    RequestSnapshot {
        id: u64,
    },
    /// The book became valid again as of `update_id`.
    Synced {
        update_id: u64,
    },
    Quality(Quality),
}

/// What happened to one diff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffOutcome {
    Buffered,
    Applied,
    /// Already contained in the book.
    Ignored,
    /// The book was dropped; synchronization starts again.
    Resync,
}

#[derive(Debug, Clone, Copy)]
pub struct SyncConfig {
    /// Diffs kept while waiting for a snapshot.
    pub max_buffer: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self { max_buffer: 10_000 }
    }
}

pub struct BookSync<B> {
    book: B,
    state: State,
    buffer: Vec<DepthDiff>,
    /// ID of the snapshot request whose answer is awaited; answers to other IDs are stale.
    awaiting: Option<u64>,
    /// Lowest ID the next request may take; IDs strictly increase.
    next_request: u64,
    clock: u64,
    cfg: SyncConfig,
}

impl<B: OrderBook> BookSync<B> {
    pub fn new(mut book: B, cfg: SyncConfig) -> Self {
        book.clear();
        Self {
            book,
            state: State::Buffering,
            buffer: Vec::new(),
            awaiting: None,
            next_request: 1,
            clock: 0,
            cfg,
        }
    }

    /// The current time; the next snapshot request takes it as its ID (or the previous ID
    /// plus one, if that is larger). Without a clock IDs count 1, 2, 3, ...
    pub fn set_clock(&mut self, now: u64) {
        self.clock = now;
    }

    pub fn state(&self) -> State {
        self.state
    }

    /// The local book. Valid only in `State::Synced`.
    pub fn book(&self) -> &B {
        &self.book
    }

    pub fn on_diff(&mut self, diff: &DepthDiff, out: &mut Vec<Action>) -> DiffOutcome {
        match self.state {
            State::Synced => self.apply_live(diff, out),
            State::Buffering | State::Resyncing => {
                self.buffer_diff(diff, out);
                DiffOutcome::Buffered
            }
        }
    }

    pub fn on_snapshot(&mut self, id: u64, snap: &Snapshot, out: &mut Vec<Action>) {
        if self.awaiting != Some(id) || self.state == State::Synced {
            return; // an answer to a request that is no longer current
        }
        self.awaiting = None;
        let Some(first) = self.buffer.first() else {
            return; // nothing buffered yet: the next diff requests a fresh snapshot
        };
        if snap.last_update_id < first.first_update_id {
            out.push(Action::Quality(Quality::SnapshotTooOld {
                snapshot: snap.last_update_id,
                first_buffered: first.first_update_id,
            }));
            self.request(out);
            return;
        }
        let loaded = self
            .book
            .load_snapshot(snap.last_update_id, &snap.bids, &snap.asks)
            .and_then(|()| self.book.check());
        if let Err(e) = loaded {
            out.push(Action::Quality(Quality::Invalid(e)));
            self.restart(None, State::Resyncing);
            return;
        }
        self.state = State::Synced;
        let buffered = std::mem::take(&mut self.buffer);
        for (i, d) in buffered.iter().enumerate() {
            if d.final_update_id <= snap.last_update_id {
                continue;
            }
            if self.apply_live(d, out) == DiffOutcome::Resync {
                // diffs after the failing one are still valid future data
                for rest in &buffered[i + 1..] {
                    self.buffer_diff(rest, out);
                }
                return;
            }
        }
        out.push(Action::Synced {
            update_id: self.book.update_id(),
        });
    }

    /// The snapshot for request `id` could not be fetched or decoded: ask again if diffs are
    /// waiting, otherwise the next diff asks.
    pub fn on_snapshot_failed(&mut self, id: u64, out: &mut Vec<Action>) {
        if self.awaiting != Some(id) {
            return;
        }
        self.awaiting = None;
        if !self.buffer.is_empty() {
            self.request(out);
        }
    }

    /// A new connection starts. Live, the end of the previous one has already restarted
    /// synchronization and this does nothing. If the book is still valid, that end was never
    /// seen: the book is dropped as on a reconnect. A replay that started in the middle of a
    /// connection drops what it collected without a snapshot, silently, and from here on
    /// behaves as the live run did.
    pub fn on_connected(&mut self, out: &mut Vec<Action>) {
        if self.state == State::Synced {
            self.on_reconnect(out);
        } else if !self.buffer.is_empty() || self.awaiting.is_some() {
            self.restart(None, self.state);
        }
    }

    /// The connection was lost: everything is rebuilt from the next diff.
    pub fn on_reconnect(&mut self, out: &mut Vec<Action>) {
        out.push(Action::Quality(Quality::Reconnect));
        self.restart(None, State::Resyncing);
    }

    /// Recording stops on purpose: the book is no longer maintained, which is not a loss.
    pub fn on_stop(&mut self) {
        self.restart(None, State::Resyncing);
    }

    /// Frames were lost before reaching this point: everything is rebuilt from the next diff.
    pub fn on_overflow(&mut self, dropped: u64, out: &mut Vec<Action>) {
        out.push(Action::Quality(Quality::Overflow { dropped }));
        self.restart(None, State::Resyncing);
    }

    fn apply_live(&mut self, diff: &DepthDiff, out: &mut Vec<Action>) -> DiffOutcome {
        let id = self.book.update_id();
        if diff.final_update_id <= id {
            return DiffOutcome::Ignored;
        }
        if diff.first_update_id > id + 1 {
            out.push(Action::Quality(Quality::Gap {
                expected: id + 1,
                got: diff.first_update_id,
            }));
            self.restart(Some((diff, out)), State::Resyncing);
            return DiffOutcome::Resync;
        }
        if let Err(e) = self.book.apply(diff).and_then(|()| self.book.check()) {
            out.push(Action::Quality(Quality::Invalid(e)));
            self.restart(None, State::Resyncing);
            return DiffOutcome::Resync;
        }
        DiffOutcome::Applied
    }

    fn buffer_diff(&mut self, diff: &DepthDiff, out: &mut Vec<Action>) {
        if self.buffer.len() >= self.cfg.max_buffer {
            out.push(Action::Quality(Quality::BufferOverflow {
                len: self.buffer.len(),
            }));
            self.buffer.clear();
        }
        self.buffer.push(diff.clone());
        if self.awaiting.is_none() {
            self.request(out);
        }
    }

    /// Drop the book and the buffer; any snapshot in flight becomes stale. A diff that
    /// revealed a gap is kept: it is valid data from the future and starts the new buffer.
    fn restart(&mut self, keep: Option<(&DepthDiff, &mut Vec<Action>)>, state: State) {
        self.book.clear();
        self.buffer.clear();
        self.awaiting = None;
        self.state = state;
        if let Some((diff, out)) = keep {
            self.buffer_diff(diff, out);
        }
    }

    fn request(&mut self, out: &mut Vec<Action>) {
        let id = self.next_request.max(self.clock);
        self.next_request = id + 1;
        self.awaiting = Some(id);
        out.push(Action::RequestSnapshot { id });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use proptest::prelude::*;

    use super::*;
    use crate::book::{RefBook, Side};
    use crate::event::Level;

    fn lv(price: i64, qty: i64) -> Level {
        Level { price, qty }
    }

    fn diff(first: u64, last: u64, bids: &[(i64, i64)], asks: &[(i64, i64)]) -> DepthDiff {
        DepthDiff {
            event_ts: 0,
            first_update_id: first,
            final_update_id: last,
            bids: bids.iter().map(|&(p, q)| lv(p, q)).collect(),
            asks: asks.iter().map(|&(p, q)| lv(p, q)).collect(),
        }
    }

    fn snapshot(id: u64) -> Snapshot {
        Snapshot {
            last_update_id: id,
            bids: vec![lv(99, 5), lv(98, 7)],
            asks: vec![lv(101, 2), lv(102, 3)],
        }
    }

    fn new_sync() -> BookSync<RefBook> {
        BookSync::new(RefBook::new(), SyncConfig::default())
    }

    fn requested(out: &[Action]) -> Option<u64> {
        out.iter().rev().find_map(|a| match a {
            Action::RequestSnapshot { id } => Some(*id),
            _ => None,
        })
    }

    /// Feed diffs, answer the snapshot request with `snap`, return all actions.
    fn start(s: &mut BookSync<RefBook>, diffs: &[DepthDiff], snap: &Snapshot) -> Vec<Action> {
        let mut out = Vec::new();
        for d in diffs {
            s.on_diff(d, &mut out);
        }
        let id = requested(&out).expect("a snapshot must be requested");
        s.on_snapshot(id, snap, &mut out);
        out
    }

    #[test]
    fn normal_start() {
        let mut s = new_sync();
        let out = start(
            &mut s,
            &[
                diff(99, 100, &[], &[]),
                diff(101, 102, &[(99, 6)], &[]),
                diff(103, 103, &[], &[(101, 0)]),
            ],
            &snapshot(100),
        );
        assert_eq!(s.state(), State::Synced);
        assert!(out.contains(&Action::Synced { update_id: 103 }));
        assert_eq!(s.book().update_id(), 103);
        assert_eq!(s.book().best(Side::Bid), Some(lv(99, 6)));
        assert_eq!(s.book().best(Side::Ask), Some(lv(102, 3)));
    }

    #[test]
    fn first_diff_overlapping_the_snapshot_is_applied() {
        let mut s = new_sync();
        start(&mut s, &[diff(95, 105, &[(97, 1)], &[])], &snapshot(100));
        assert_eq!(s.state(), State::Synced);
        assert_eq!(s.book().update_id(), 105);
        assert_eq!(s.book().len(Side::Bid), 3);
    }

    #[test]
    fn snapshot_older_than_buffer_is_requested_again() {
        let mut s = new_sync();
        let mut out = Vec::new();
        s.on_diff(&diff(200, 201, &[], &[]), &mut out);
        let first = requested(&out).unwrap();
        out.clear();
        s.on_snapshot(first, &snapshot(150), &mut out);
        assert_eq!(
            out[0],
            Action::Quality(Quality::SnapshotTooOld {
                snapshot: 150,
                first_buffered: 200
            })
        );
        let second = requested(&out).expect("must ask again");
        assert_ne!(second, first);
        assert_eq!(s.state(), State::Buffering);
        out.clear();
        s.on_snapshot(second, &snapshot(201), &mut out);
        assert_eq!(s.state(), State::Synced);
        assert_eq!(s.book().update_id(), 201);
    }

    #[test]
    fn buffered_diffs_older_than_snapshot_are_dropped() {
        let mut s = new_sync();
        // the first two diffs touch level 98, but the snapshot already includes them
        start(
            &mut s,
            &[
                diff(90, 95, &[(98, 1)], &[]),
                diff(96, 100, &[(98, 2)], &[]),
                diff(101, 101, &[], &[]),
            ],
            &snapshot(100),
        );
        assert_eq!(s.state(), State::Synced);
        let mut bids = Vec::new();
        s.book().top(Side::Bid, 10, &mut bids);
        assert_eq!(bids, vec![lv(99, 5), lv(98, 7)]);
    }

    #[test]
    fn gap_after_sync_restarts() {
        let mut s = new_sync();
        start(&mut s, &[diff(100, 101, &[], &[])], &snapshot(100));
        let mut out = Vec::new();
        assert_eq!(
            s.on_diff(&diff(102, 103, &[], &[]), &mut out),
            DiffOutcome::Applied
        );
        assert_eq!(
            s.on_diff(&diff(105, 106, &[(97, 4)], &[]), &mut out),
            DiffOutcome::Resync
        );
        assert_eq!(
            out[0],
            Action::Quality(Quality::Gap {
                expected: 104,
                got: 105
            })
        );
        let id = requested(&out).expect("must request a snapshot right away");
        assert_eq!(s.state(), State::Resyncing);
        assert!(s.book().is_empty());
        out.clear();
        s.on_snapshot(id, &snapshot(105), &mut out);
        assert_eq!(s.state(), State::Synced);
        assert_eq!(
            s.book().update_id(),
            106,
            "the diff that revealed the gap is applied after the snapshot"
        );
        assert_eq!(s.book().best(Side::Bid), Some(lv(99, 5)));
        assert_eq!(s.book().len(Side::Bid), 3);
    }

    #[test]
    fn duplicate_diff_is_not_applied_again() {
        let mut s = new_sync();
        start(&mut s, &[diff(100, 101, &[], &[])], &snapshot(100));
        let mut out = Vec::new();
        let d = diff(102, 103, &[(99, 1)], &[]);
        assert_eq!(s.on_diff(&d, &mut out), DiffOutcome::Applied);
        s.on_diff(&diff(104, 104, &[(99, 9)], &[]), &mut out);
        assert_eq!(s.on_diff(&d, &mut out), DiffOutcome::Ignored);
        assert_eq!(s.book().best(Side::Bid), Some(lv(99, 9)));
        assert!(out.is_empty());
    }

    #[test]
    fn out_of_order_diffs_cause_a_gap() {
        let mut s = new_sync();
        start(&mut s, &[diff(100, 101, &[], &[])], &snapshot(100));
        let mut out = Vec::new();
        assert_eq!(
            s.on_diff(&diff(104, 105, &[], &[]), &mut out),
            DiffOutcome::Resync
        );
        assert_eq!(
            s.on_diff(&diff(102, 103, &[], &[]), &mut out),
            DiffOutcome::Buffered
        );
        assert!(matches!(
            out[0],
            Action::Quality(Quality::Gap {
                expected: 102,
                got: 104
            })
        ));
    }

    #[test]
    fn gap_inside_the_buffer_is_found_during_replay() {
        let mut s = new_sync();
        let out = start(
            &mut s,
            &[
                diff(100, 101, &[], &[]),
                diff(103, 104, &[], &[]),
                diff(105, 105, &[], &[]),
            ],
            &snapshot(100),
        );
        assert!(out.contains(&Action::Quality(Quality::Gap {
            expected: 102,
            got: 103
        })));
        assert_eq!(s.state(), State::Resyncing);
        assert!(!out.iter().any(|a| matches!(a, Action::Synced { .. })));
        // the diffs after the hole are kept and a new snapshot is requested
        let id = requested(&out).unwrap();
        let mut out = Vec::new();
        s.on_snapshot(id, &snapshot(104), &mut out);
        assert_eq!(s.book().update_id(), 105);
    }

    #[test]
    fn stale_snapshot_answer_is_ignored() {
        let mut s = new_sync();
        let mut out = Vec::new();
        s.on_diff(&diff(100, 101, &[], &[]), &mut out);
        let old = requested(&out).unwrap();
        s.on_reconnect(&mut out);
        out.clear();
        s.on_snapshot(old, &snapshot(100), &mut out);
        assert_eq!(s.state(), State::Resyncing);
        assert!(out.is_empty());
    }

    #[test]
    fn failed_snapshot_is_requested_again() {
        let mut s = new_sync();
        let mut out = Vec::new();
        s.on_diff(&diff(100, 101, &[], &[]), &mut out);
        let first = requested(&out).unwrap();
        out.clear();
        s.on_snapshot_failed(first + 5, &mut out);
        assert!(
            out.is_empty(),
            "a failure for another request changes nothing"
        );
        s.on_snapshot_failed(first, &mut out);
        let second = requested(&out).expect("must ask again");
        out.clear();
        s.on_snapshot(second, &snapshot(100), &mut out);
        assert_eq!(s.state(), State::Synced);
    }

    #[test]
    fn reconnect_rebuilds_from_scratch() {
        let mut s = new_sync();
        start(&mut s, &[diff(100, 101, &[], &[])], &snapshot(100));
        let mut out = Vec::new();
        s.on_reconnect(&mut out);
        assert_eq!(out, vec![Action::Quality(Quality::Reconnect)]);
        assert_eq!(s.state(), State::Resyncing);
        assert!(s.book().is_empty());
        let out = start(&mut s, &[diff(500, 501, &[], &[])], &snapshot(500));
        assert!(out.contains(&Action::Synced { update_id: 501 }));
    }

    #[test]
    fn crossed_book_restarts() {
        let mut s = new_sync();
        start(&mut s, &[diff(100, 101, &[], &[])], &snapshot(100));
        let mut out = Vec::new();
        assert_eq!(
            s.on_diff(&diff(102, 102, &[(101, 1)], &[]), &mut out),
            DiffOutcome::Resync
        );
        assert!(matches!(
            out[0],
            Action::Quality(Quality::Invalid(BookError::Crossed { .. }))
        ));
        assert_eq!(s.state(), State::Resyncing);
    }

    #[test]
    fn buffer_overflow_is_reported() {
        let mut s = BookSync::new(RefBook::new(), SyncConfig { max_buffer: 3 });
        let mut out = Vec::new();
        for i in 0..4 {
            s.on_diff(&diff(100 + i, 100 + i, &[], &[]), &mut out);
        }
        assert!(out.contains(&Action::Quality(Quality::BufferOverflow { len: 3 })));
        assert_eq!(
            out.iter()
                .filter(|a| matches!(a, Action::RequestSnapshot { .. }))
                .count(),
            1
        );
    }

    // Random exchange history with faults: dropped, duplicated and swapped diffs, snapshots
    // answered with a delay. Whenever the local book claims to be synced, it must equal the
    // exchange book as of the same update ID.
    #[derive(Debug, Clone)]
    enum Fault {
        None,
        Drop,
        Duplicate,
        SwapWithNext,
    }

    /// One exchange step: IDs consumed, level changes (bid?, distance from 100, qty), how the
    /// diff is delivered, and after how many diffs a snapshot requested now is answered.
    type Step = (u64, Vec<(bool, i64, i64)>, Fault, usize);

    /// Returns how many delivered diffs found the book synced.
    fn simulate(steps: &[Step]) -> Result<usize, TestCaseError> {
        // exchange history
        let mut truth = RefBook::new();
        truth.set_update_id(1000);
        let mut at: HashMap<u64, RefBook> = HashMap::from([(1000, truth.clone())]);
        let mut diffs = Vec::new();
        for (len, changes, f, _) in steps {
            let first = truth.update_id() + 1;
            let mut d = diff(first, first + len - 1, &[], &[]);
            for &(is_bid, offset, qty) in changes {
                // bids below 100, asks above: the true book never crosses
                if is_bid {
                    d.bids.push(lv(100 - offset, qty));
                } else {
                    d.asks.push(lv(100 + offset, qty));
                }
            }
            truth.apply(&d).unwrap();
            at.insert(d.final_update_id, truth.clone());
            diffs.push((d, f.clone()));
        }
        // delivery with faults
        let mut delivered = Vec::new();
        let mut i = 0;
        while i < diffs.len() {
            let (d, f) = &diffs[i];
            match f {
                Fault::None => delivered.push(d.clone()),
                Fault::Drop => {}
                Fault::Duplicate => {
                    delivered.push(d.clone());
                    delivered.push(d.clone());
                }
                Fault::SwapWithNext if i + 1 < diffs.len() => {
                    delivered.push(diffs[i + 1].0.clone());
                    delivered.push(d.clone());
                    i += 1;
                }
                Fault::SwapWithNext => delivered.push(d.clone()),
            }
            i += 1;
        }
        // run; a snapshot carries the newest exchange state seen so far
        let mut s = new_sync();
        let mut out = Vec::new();
        let mut pending: Option<(u64, usize)> = None;
        let mut newest = 1000;
        let mut synced = 0;
        for (k, d) in delivered.iter().enumerate() {
            newest = newest.max(d.final_update_id);
            out.clear();
            s.on_diff(d, &mut out);
            if let Some(id) = requested(&out) {
                pending = Some((id, k + steps[k % steps.len()].3));
            }
            if let Some((id, when)) = pending
                && when <= k
            {
                pending = None;
                let book = &at[&newest];
                let mut snap = Snapshot {
                    last_update_id: newest,
                    ..Snapshot::default()
                };
                book.top(Side::Bid, usize::MAX, &mut snap.bids);
                book.top(Side::Ask, usize::MAX, &mut snap.asks);
                out.clear();
                s.on_snapshot(id, &snap, &mut out);
                if let Some(id) = requested(&out) {
                    pending = Some((id, k));
                }
            }
            if s.state() == State::Synced {
                synced += 1;
                let id = s.book().update_id();
                prop_assert_eq!(
                    s.book(),
                    &at[&id],
                    "synced book differs from the exchange at update {}",
                    id
                );
            }
        }
        Ok(synced)
    }

    #[test]
    fn simulation_without_faults_gets_synced() {
        let steps: Vec<Step> = (0..50)
            .map(|i| {
                (
                    1 + i % 3,
                    vec![(i % 2 == 0, 1 + (i as i64 % 7), 2)],
                    Fault::None,
                    1,
                )
            })
            .collect();
        let synced = simulate(&steps).unwrap();
        assert!(
            synced >= 45,
            "only {synced} of 50 diffs found the book synced"
        );
    }

    fn fault() -> impl Strategy<Value = Fault> {
        prop_oneof![
            8 => Just(Fault::None),
            1 => Just(Fault::Drop),
            1 => Just(Fault::Duplicate),
            1 => Just(Fault::SwapWithNext),
        ]
    }

    fn step() -> impl Strategy<Value = Step> {
        (
            1u64..4,
            proptest::collection::vec((any::<bool>(), 1i64..20, 0i64..4), 0..4),
            fault(),
            0usize..4,
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(300))]
        #[test]
        fn synced_book_always_equals_the_exchange(steps in proptest::collection::vec(step(), 1..120)) {
            simulate(&steps)?;
        }
    }

    #[test]
    fn request_ids_follow_the_clock_and_always_increase() {
        let mut sync = BookSync::new(RefBook::new(), SyncConfig::default());
        let mut out = Vec::new();
        sync.set_clock(500);
        sync.on_diff(&diff(5, 6, &[], &[]), &mut out);
        sync.on_snapshot_failed(500, &mut out);
        sync.set_clock(400); // a clock that went back still gives a new ID
        sync.on_diff(&diff(7, 7, &[], &[]), &mut out);
        sync.on_snapshot_failed(501, &mut out);
        let ids: Vec<u64> = out
            .iter()
            .filter_map(|a| match a {
                Action::RequestSnapshot { id } => Some(*id),
                _ => None,
            })
            .collect();
        assert_eq!(ids, [500, 501, 502]);
    }

    #[test]
    fn a_new_connection_drops_what_has_no_snapshot_and_a_book_whose_end_was_missed() {
        let mut sync = BookSync::new(RefBook::new(), SyncConfig::default());
        let mut out = Vec::new();
        sync.on_diff(&diff(5, 6, &[], &[]), &mut out);
        out.clear();
        sync.on_connected(&mut out);
        assert!(out.is_empty(), "silent: nothing valid is lost");
        sync.on_snapshot(1, &snapshot(6), &mut out);
        assert_eq!(sync.state(), State::Buffering, "the old request is stale");

        let mut fresh = BookSync::new(RefBook::new(), SyncConfig::default());
        fresh.on_connected(&mut out);
        assert!(out.is_empty());

        sync.on_diff(&diff(7, 8, &[], &[]), &mut out);
        sync.on_snapshot(2, &snapshot(7), &mut out);
        assert_eq!(sync.state(), State::Synced);
        out.clear();
        sync.on_connected(&mut out);
        assert_eq!(out, [Action::Quality(Quality::Reconnect)]);
        assert_eq!(sync.state(), State::Resyncing);
    }
}
