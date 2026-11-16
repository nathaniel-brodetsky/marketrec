//! Reference order book on `BTreeMap`: price -> quantity, one map per side.

use std::collections::BTreeMap;

use super::{BookError, OrderBook, Side, validate};
use crate::event::Level;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RefBook {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    update_id: u64,
}

impl RefBook {
    pub fn new() -> Self {
        Self::default()
    }

    fn side(&self, side: Side) -> &BTreeMap<i64, i64> {
        match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        }
    }

    fn side_mut(&mut self, side: Side) -> &mut BTreeMap<i64, i64> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }
}

impl OrderBook for RefBook {
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
        let levels = self.side_mut(side);
        if qty == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, qty);
        }
        Ok(())
    }

    fn best(&self, side: Side) -> Option<Level> {
        let entry = match side {
            Side::Bid => self.bids.last_key_value(),
            Side::Ask => self.asks.first_key_value(),
        };
        entry.map(|(&price, &qty)| Level { price, qty })
    }

    fn len(&self, side: Side) -> usize {
        self.side(side).len()
    }

    fn top(&self, side: Side, n: usize, out: &mut Vec<Level>) {
        out.clear();
        let level = |(&price, &qty): (&i64, &i64)| Level { price, qty };
        match side {
            Side::Bid => out.extend(self.bids.iter().rev().take(n).map(level)),
            Side::Ask => out.extend(self.asks.iter().take(n).map(level)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use proptest::prelude::*;

    use super::*;
    use crate::event::DepthDiff;

    fn lv(price: i64, qty: i64) -> Level {
        Level { price, qty }
    }

    fn book() -> RefBook {
        let mut b = RefBook::new();
        b.load_snapshot(
            100,
            &[lv(99, 5), lv(98, 7), lv(97, 1)],
            &[lv(101, 2), lv(102, 3)],
        )
        .unwrap();
        b
    }

    #[test]
    fn snapshot_sets_levels_and_update_id() {
        let b = book();
        assert_eq!(b.update_id(), 100);
        assert_eq!(b.best(Side::Bid), Some(lv(99, 5)));
        assert_eq!(b.best(Side::Ask), Some(lv(101, 2)));
        assert_eq!((b.len(Side::Bid), b.len(Side::Ask)), (3, 2));
        assert!(b.check().is_ok());
    }

    #[test]
    fn top_is_best_first() {
        let b = book();
        let mut out = Vec::new();
        b.top(Side::Bid, 2, &mut out);
        assert_eq!(out, vec![lv(99, 5), lv(98, 7)]);
        b.top(Side::Ask, 10, &mut out);
        assert_eq!(out, vec![lv(101, 2), lv(102, 3)]);
    }

    #[test]
    fn zero_removes_and_missing_removal_is_noop() {
        let mut b = book();
        b.set(Side::Bid, 99, 0).unwrap();
        assert_eq!(b.best(Side::Bid), Some(lv(98, 7)));
        b.set(Side::Ask, 150, 0).unwrap();
        assert_eq!(b.len(Side::Ask), 2);
    }

    #[test]
    fn quantity_is_replaced_not_added() {
        let mut b = book();
        b.set(Side::Ask, 101, 9).unwrap();
        assert_eq!(b.best(Side::Ask), Some(lv(101, 9)));
    }

    #[test]
    fn invalid_levels_are_rejected() {
        let mut b = book();
        assert_eq!(
            b.set(Side::Bid, 0, 1),
            Err(BookError::InvalidPrice {
                side: Side::Bid,
                price: 0
            })
        );
        assert_eq!(
            b.set(Side::Ask, 101, -1),
            Err(BookError::InvalidQty {
                side: Side::Ask,
                price: 101,
                qty: -1
            })
        );
        assert_eq!(b, book(), "a rejected level must not change the book");
    }

    #[test]
    fn crossed_book_is_detected() {
        let mut b = book();
        b.set(Side::Bid, 101, 1).unwrap();
        assert_eq!(b.check(), Err(BookError::Crossed { bid: 101, ask: 101 }));
    }

    #[test]
    fn apply_diff() {
        let mut b = book();
        let diff = DepthDiff {
            event_ts: 0,
            first_update_id: 101,
            final_update_id: 104,
            bids: vec![lv(99, 0), lv(100, 4)],
            asks: vec![lv(101, 1), lv(103, 8)],
        };
        b.apply(&diff).unwrap();
        assert_eq!(b.update_id(), 104);
        let mut out = Vec::new();
        b.top(Side::Bid, 10, &mut out);
        assert_eq!(out, vec![lv(100, 4), lv(98, 7), lv(97, 1)]);
        b.top(Side::Ask, 10, &mut out);
        assert_eq!(out, vec![lv(101, 1), lv(102, 3), lv(103, 8)]);
    }

    #[test]
    fn clear_resets_everything() {
        let mut b = book();
        b.clear();
        assert!(b.is_empty());
        assert_eq!(b.update_id(), 0);
        assert_eq!(b.best(Side::Bid), None);
        assert!(b.check().is_ok());
    }

    fn side_strategy() -> impl Strategy<Value = Side> {
        prop_oneof![Just(Side::Bid), Just(Side::Ask)]
    }

    proptest! {
        // The reference book against an even simpler model: a hash map sorted on demand.
        #[test]
        fn matches_hash_map_model(ops in proptest::collection::vec((side_strategy(), 1i64..60, 0i64..5), 0..300)) {
            let mut b = RefBook::new();
            let mut model: HashMap<(Side, i64), i64> = HashMap::new();
            for (side, price, qty) in ops {
                b.set(side, price, qty).unwrap();
                if qty == 0 { model.remove(&(side, price)); } else { model.insert((side, price), qty); }
            }
            for side in [Side::Bid, Side::Ask] {
                let mut expected: Vec<Level> = model.iter()
                    .filter(|((s, _), _)| *s == side)
                    .map(|((_, p), q)| lv(*p, *q))
                    .collect();
                expected.sort_by_key(|l| l.price);
                if side == Side::Bid { expected.reverse(); }
                let mut out = Vec::new();
                b.top(side, usize::MAX, &mut out);
                prop_assert_eq!(&out, &expected);
                prop_assert_eq!(b.best(side), expected.first().copied());
                prop_assert_eq!(b.len(side), expected.len());
                prop_assert!(out.iter().all(|l| l.qty > 0));
            }
        }
    }
}
