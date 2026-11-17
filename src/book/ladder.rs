//! Order book on sorted vectors, the best level at the end of each.
//!
//! Most updates touch the levels near the best price. With the best level last, finding a
//! level is a binary search over a contiguous array, and inserting or removing one moves
//! only the few levels better than it. The memory is reserved once, so updates do not
//! allocate after the book has reached its usual depth. It must hold exactly what
//! `RefBook` holds for any sequence of updates; the tests below check that.

use super::{BookError, OrderBook, Side, validate};
use crate::event::Level;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LadderBook {
    /// Ascending prices: the best bid is last.
    bids: Vec<Level>,
    /// Descending prices: the best ask is last.
    asks: Vec<Level>,
    update_id: u64,
}

/// Levels per side reserved up front: a 5000-level snapshot plus room to move.
pub const DEFAULT_CAPACITY: usize = 8192;

impl LadderBook {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(levels: usize) -> Self {
        Self {
            bids: Vec::with_capacity(levels),
            asks: Vec::with_capacity(levels),
            update_id: 0,
        }
    }

    fn side_mut(&mut self, side: Side) -> &mut Vec<Level> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }

    fn side(&self, side: Side) -> &Vec<Level> {
        match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        }
    }

    /// Position of `price` in a side, or where it would go. The search gallops from the
    /// best end (1, 2, 4, ... levels back) and then bisects the last stride, so a level `k`
    /// places from the best costs about 2 log2 k comparisons whatever the depth of the book.
    fn find(levels: &[Level], side: Side, price: i64) -> Result<usize, usize> {
        // the levels worse than `price` form a prefix: find where it ends
        let worse = |l: &Level| match side {
            Side::Bid => l.price < price,
            Side::Ask => l.price > price,
        };
        let (mut lo, mut hi, mut step) = (0, levels.len(), 1);
        // invariant: levels[hi..] are not worse
        while hi > 0 {
            let probe = hi.saturating_sub(step);
            if worse(&levels[probe]) {
                lo = probe + 1;
                break;
            }
            hi = probe;
            step *= 2;
        }
        let k = lo + levels[lo..hi].partition_point(worse);
        match levels.get(k) {
            Some(l) if l.price == price => Ok(k),
            _ => Err(k),
        }
    }

    /// Replace one side with `levels` given in any order, as setting them one after the
    /// other would: the last quantity for a price wins and 0 removes it.
    fn load_side(&mut self, side: Side, levels: &[Level]) {
        let v = self.side_mut(side);
        v.clear();
        v.extend_from_slice(levels);
        // a stable sort keeps the order of equal prices: the last one is the one to keep
        v.sort_by(|a, b| match side {
            Side::Bid => a.price.cmp(&b.price),
            Side::Ask => b.price.cmp(&a.price),
        });
        let mut w = 0;
        for r in 0..v.len() {
            let last_of_price = r + 1 == v.len() || v[r + 1].price != v[r].price;
            if last_of_price && v[r].qty != 0 {
                v[w] = v[r];
                w += 1;
            }
        }
        v.truncate(w);
    }
}

impl OrderBook for LadderBook {
    fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.update_id = 0;
    }

    fn update_id(&self) -> u64 {
        self.update_id
    }

    fn set_update_id(&mut self, id: u64) {
        self.update_id = id;
    }

    fn set(&mut self, side: Side, price: i64, qty: i64) -> Result<(), BookError> {
        validate(side, price, qty)?;
        let found = Self::find(self.side(side), side, price);
        let v = self.side_mut(side);
        match (found, qty) {
            (Ok(i), 0) => {
                v.remove(i);
            }
            (Ok(i), q) => v[i].qty = q,
            (Err(_), 0) => {}
            (Err(i), q) => v.insert(i, Level { price, qty: q }),
        }
        Ok(())
    }

    fn best(&self, side: Side) -> Option<Level> {
        self.side(side).last().copied()
    }

    fn len(&self, side: Side) -> usize {
        self.side(side).len()
    }

    fn top(&self, side: Side, n: usize, out: &mut Vec<Level>) {
        out.clear();
        out.extend(self.side(side).iter().rev().take(n).copied());
    }

    /// Checks every level first, then builds each side in one sort instead of thousands of
    /// inserts. On an invalid level it returns the error `set` would have returned first;
    /// the book is then left empty, which the caller must treat as invalid anyway.
    fn load_snapshot(
        &mut self,
        update_id: u64,
        bids: &[Level],
        asks: &[Level],
    ) -> Result<(), BookError> {
        self.clear();
        for (side, levels) in [(Side::Bid, bids), (Side::Ask, asks)] {
            for l in levels {
                validate(side, l.price, l.qty)?;
            }
        }
        self.load_side(Side::Bid, bids);
        self.load_side(Side::Ask, asks);
        self.set_update_id(update_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::book::RefBook;
    use crate::event::DepthDiff;

    fn lv(price: i64, qty: i64) -> Level {
        Level { price, qty }
    }

    /// Everything a reader can see of a book.
    fn state(b: &impl OrderBook) -> (u64, Vec<Level>, Vec<Level>) {
        let (mut bids, mut asks) = (Vec::new(), Vec::new());
        b.top(Side::Bid, usize::MAX, &mut bids);
        b.top(Side::Ask, usize::MAX, &mut asks);
        assert_eq!(b.best(Side::Bid), bids.first().copied());
        assert_eq!(b.best(Side::Ask), asks.first().copied());
        assert_eq!(
            (b.len(Side::Bid), b.len(Side::Ask)),
            (bids.len(), asks.len())
        );
        (b.update_id(), bids, asks)
    }

    #[test]
    fn best_level_is_last_in_memory_and_first_in_top() {
        let mut b = LadderBook::new();
        b.load_snapshot(
            100,
            &[lv(98, 7), lv(99, 5), lv(97, 1)],
            &[lv(102, 3), lv(101, 2)],
        )
        .unwrap();
        assert_eq!(b.bids.last(), Some(&lv(99, 5)));
        assert_eq!(b.asks.last(), Some(&lv(101, 2)));
        let mut out = Vec::new();
        b.top(Side::Bid, 2, &mut out);
        assert_eq!(out, [lv(99, 5), lv(98, 7)]);
    }

    #[test]
    fn updates_do_not_allocate_within_capacity() {
        let mut b = LadderBook::with_capacity(64);
        let (pb, pa) = (b.bids.as_ptr(), b.asks.as_ptr());
        for i in 0..64 {
            b.set(Side::Bid, 1000 - i, 1).unwrap();
            b.set(Side::Ask, 2000 + i, 1).unwrap();
        }
        b.clear();
        b.load_snapshot(1, &[lv(5, 1)], &[lv(6, 1)]).unwrap();
        assert_eq!(
            (b.bids.as_ptr(), b.asks.as_ptr()),
            (pb, pa),
            "the same memory"
        );
    }

    #[test]
    fn in_a_snapshot_the_last_quantity_for_a_price_wins() {
        let mut many: Vec<Level> = (1..=60).map(|q| lv(7, q)).collect();
        many.extend((1..=60).map(|q| lv(9, 61 - q)));
        many.push(lv(8, 3));
        many.push(lv(8, 0));
        let (mut fast, mut reference) = (LadderBook::new(), RefBook::new());
        fast.load_snapshot(1, &many, &[]).unwrap();
        reference.load_snapshot(1, &many, &[]).unwrap();
        assert_eq!(state(&fast), state(&reference));
        assert_eq!(state(&fast).1, [lv(9, 1), lv(7, 60)]);
        let mut fast = LadderBook::new();
        fast.load_snapshot(1, &[], &many).unwrap();
        assert_eq!(state(&fast).2, [lv(7, 60), lv(9, 1)]);
    }

    #[test]
    fn search_finds_every_place_in_a_deep_book() {
        let mut b = LadderBook::new();
        let bids: Vec<Level> = (1..=5000).map(|i| lv(2 * i, 1)).collect();
        let asks: Vec<Level> = (1..=5000).map(|i| lv(20_000 + 2 * i, 1)).collect();
        b.load_snapshot(1, &bids, &asks).unwrap();
        for p in 0..10_003 {
            let want = p % 2 == 0 && (2..=10_000).contains(&p);
            assert_eq!(
                LadderBook::find(&b.bids, Side::Bid, p).is_ok(),
                want,
                "bid {p}"
            );
            let a = 20_000 + p;
            assert_eq!(
                LadderBook::find(&b.asks, Side::Ask, a).is_ok(),
                want,
                "ask {a}"
            );
        }
    }

    fn side() -> impl Strategy<Value = Side> {
        prop_oneof![Just(Side::Bid), Just(Side::Ask)]
    }

    fn levels() -> impl Strategy<Value = Vec<Level>> {
        proptest::collection::vec((-2i64..40, -1i64..4), 0..30)
            .prop_map(|v| v.into_iter().map(|(p, q)| lv(p, q)).collect())
    }

    #[derive(Debug, Clone)]
    enum Op {
        Set(Side, i64, i64),
        Snapshot(u64, Vec<Level>, Vec<Level>),
        Diff(u64, Vec<Level>, Vec<Level>),
        Clear,
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            8 => (side(), -2i64..40, -1i64..4).prop_map(|(s, p, q)| Op::Set(s, p, q)),
            1 => (0u64..1000, levels(), levels()).prop_map(|(id, b, a)| Op::Snapshot(id, b, a)),
            3 => (0u64..1000, levels(), levels()).prop_map(|(id, b, a)| Op::Diff(id, b, a)),
            1 => Just(Op::Clear),
        ]
    }

    proptest! {
        /// Any sequence of updates, valid or not, leaves the ladder holding what the
        /// reference book holds and failing where it fails. After a failure both books
        /// are invalid by contract, so both are cleared.
        #[test]
        fn holds_exactly_what_the_reference_book_holds(ops in proptest::collection::vec(op(), 0..200)) {
            let (mut fast, mut reference) = (LadderBook::with_capacity(4), RefBook::new());
            for op in ops {
                let (a, b) = match op {
                    Op::Set(s, p, q) => (fast.set(s, p, q), reference.set(s, p, q)),
                    Op::Snapshot(id, b, a) => {
                        (fast.load_snapshot(id, &b, &a), reference.load_snapshot(id, &b, &a))
                    }
                    Op::Diff(id, bids, asks) => {
                        let d = DepthDiff { event_ts: 0, first_update_id: id, final_update_id: id, bids, asks };
                        (fast.apply(&d), reference.apply(&d))
                    }
                    Op::Clear => {
                        fast.clear();
                        reference.clear();
                        (Ok(()), Ok(()))
                    }
                };
                prop_assert_eq!(&a, &b);
                if a.is_err() {
                    fast.clear();
                    reference.clear();
                }
                prop_assert_eq!(state(&fast), state(&reference));
                prop_assert_eq!(fast.check(), reference.check());
            }
        }
    }
}
