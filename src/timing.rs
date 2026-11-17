//! Timing of the feed: how late the data arrives, and how long the link takes to answer.
//!
//! Lag: the receive time of a depth update minus the time the exchange stamped it. Depth
//! updates come every 100 ms, so their lag is a steady clock of freshness.
//!
//! A valid book can still be stale: during a stall the updates arrive seconds late and in
//! order, and the book is correct but behind the market. The pipeline marks such intervals
//! as `late` when an update arrives more than `LATE_AFTER_NS` after its exchange time, and
//! `on_time` when one arrives within `ON_TIME_BELOW_NS` again; the gap between the two
//! thresholds keeps a lag near the limit from flapping.
//!
//! The lag includes the offset between this machine's clock and the exchange's. An update
//! that seems to arrive before it was stamped is counted apart, in `below_zero`: it means
//! this clock is behind.
//!
//! Round trip: the recorder sends its own WebSocket ping every few seconds with the send
//! time in the payload, and the exchange's pong comes back on the same connection. Both
//! times are this machine's, so the round trip needs no clock agreement. It tells late
//! data apart: a normal round trip while data is late means the data was late before it
//! entered the connection, at the exchange; a slow round trip means the connection itself
//! was congested, on the network or in the exchange's send buffer.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// An update this late starts a `late` interval.
pub const LATE_AFTER_NS: i64 = 1_000_000_000;
/// An update within this lag ends it.
pub const ON_TIME_BELOW_NS: i64 = 500_000_000;

const NS_PER_MS: i64 = 1_000_000;

/// Durations in milliseconds with three significant digits (at most 1% too low), so
/// histograms of different runs and days can be added up exactly. The counts live in one
/// array allocated on the first sample, so recording never allocates again; in JSON only
/// the buckets in use appear, keyed by their lower bound in ms.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(from = "HistogramRepr", into = "HistogramRepr")]
pub struct MsHistogram {
    dense: Option<Box<[u64]>>,
    /// Negative samples: for lags, updates received before their exchange time.
    pub below_zero: u64,
    /// The largest sample, exact, in ns.
    pub max_ns: i64,
}

/// The JSON form of `MsHistogram`.
#[derive(Serialize, Deserialize)]
struct HistogramRepr {
    /// Samples per bucket; the key is the bucket's lower bound in ms.
    counts: BTreeMap<u64, u64>,
    below_zero: u64,
    max_ns: i64,
}

impl From<HistogramRepr> for MsHistogram {
    fn from(r: HistogramRepr) -> Self {
        let mut h = MsHistogram {
            dense: None,
            below_zero: r.below_zero,
            max_ns: r.max_ns,
        };
        for (lower, n) in r.counts {
            h.add(index(lower), n);
        }
        h
    }
}

impl From<MsHistogram> for HistogramRepr {
    fn from(h: MsHistogram) -> Self {
        HistogramRepr {
            counts: h.counts().collect(),
            below_zero: h.below_zero,
            max_ns: h.max_ns,
        }
    }
}

impl PartialEq for MsHistogram {
    fn eq(&self, other: &Self) -> bool {
        (self.below_zero, self.max_ns) == (other.below_zero, other.max_ns)
            && self.counts().eq(other.counts())
    }
}

impl Eq for MsHistogram {}

impl std::fmt::Debug for MsHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MsHistogram")
            .field("counts", &self.counts().collect::<BTreeMap<_, _>>())
            .field("below_zero", &self.below_zero)
            .field("max_ns", &self.max_ns)
            .finish()
    }
}

/// Exact buckets below 1000 ms, then 900 per decade (100 to 999 times 10^d ms) up to 10^9
/// ms, about 11 days; anything longer lands in the last bucket.
const EXACT: usize = 1000;
const PER_DECADE: usize = 900;
const DECADES: usize = 6;
const BUCKETS: usize = EXACT + PER_DECADE * DECADES;

fn index(ms: u64) -> usize {
    if ms < EXACT as u64 {
        return ms as usize;
    }
    let (mut m, mut d) = (ms, 0usize);
    while m >= 1000 {
        m /= 10;
        d += 1;
    }
    if d > DECADES {
        return BUCKETS - 1;
    }
    EXACT + (d - 1) * PER_DECADE + (m as usize - 100)
}

/// Lower bound in ms of bucket `i`.
fn lower_bound(i: usize) -> u64 {
    if i < EXACT {
        return i as u64;
    }
    let j = i - EXACT;
    (100 + (j % PER_DECADE) as u64) * 10u64.pow((j / PER_DECADE + 1) as u32)
}

/// Lower bound of the bucket holding `ms`: exact below 1000 ms, then 3 significant digits.
#[cfg(test)]
fn bucket(ms: u64) -> u64 {
    lower_bound(index(ms))
}

impl MsHistogram {
    fn add(&mut self, i: usize, n: u64) {
        let dense = self
            .dense
            .get_or_insert_with(|| vec![0; BUCKETS].into_boxed_slice());
        dense[i] += n;
    }

    pub fn record(&mut self, lag_ns: i64) {
        if lag_ns < 0 {
            self.below_zero += 1;
            return;
        }
        self.add(index((lag_ns / NS_PER_MS) as u64), 1);
        self.max_ns = self.max_ns.max(lag_ns);
    }

    pub fn merge(&mut self, other: &MsHistogram) {
        for (lower, n) in other.counts() {
            self.add(index(lower), n);
        }
        self.below_zero += other.below_zero;
        self.max_ns = self.max_ns.max(other.max_ns);
    }

    /// The buckets in use: lower bound in ms and count, in order.
    pub fn counts(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.dense
            .iter()
            .flat_map(|d| d.iter().enumerate())
            .filter(|&(_, &n)| n > 0)
            .map(|(i, &n)| (lower_bound(i), n))
    }

    /// Samples counted, `below_zero` excluded.
    pub fn count(&self) -> u64 {
        self.dense.as_ref().map_or(0, |d| d.iter().sum())
    }

    /// The value in ms below which a share `q` (0 to 1) of the samples fell, as the lower
    /// bound of its bucket. `None` without samples.
    pub fn quantile_ms(&self, q: f64) -> Option<u64> {
        let n = self.count();
        if n == 0 {
            return None;
        }
        let rank = ((q.clamp(0.0, 1.0) * n as f64).ceil() as u64).max(1);
        let mut seen = 0;
        let mut last = None;
        for (lower, c) in self.counts() {
            seen += c;
            last = Some(lower);
            if seen >= rank {
                return Some(lower);
            }
        }
        last
    }

    /// `p50 140 ms, p99 260 ms, max 16.6 s`, or `none` without updates.
    pub fn summary(&self) -> String {
        match (self.quantile_ms(0.5), self.quantile_ms(0.99)) {
            (Some(p50), Some(p99)) => format!(
                "p50 {}, p99 {}, max {}",
                duration_ms(p50 as i64 * NS_PER_MS),
                duration_ms(p99 as i64 * NS_PER_MS),
                duration_ms(self.max_ns)
            ),
            _ => "none".into(),
        }
    }
}

/// The summary line of a run: lag of depth updates and the late intervals.
pub fn report_line(lag: &MsHistogram, late_intervals: u64, late_ns: i64) -> String {
    let mut line = format!("depth updates {}", lag.summary());
    if late_intervals > 0 {
        line += &format!(
            "; more than 1 s behind {late_intervals} time{} for {:.1} s",
            if late_intervals == 1 { "" } else { "s" },
            late_ns as f64 / 1e9
        );
    }
    if lag.below_zero > 0 {
        line += &format!(
            "; {} arrived before their exchange time: this clock is behind",
            lag.below_zero
        );
    }
    line
}

/// The summary line of the link: round trips of the recorder's pings.
pub fn rtt_line(rtt: &MsHistogram) -> String {
    match rtt.count() {
        0 => "no round-trip probe answered".into(),
        n => format!("round trip {}, {n} probes", rtt.summary()),
    }
}

/// `140 ms` below ten seconds, `16.6 s` above.
pub fn duration_ms(ns: i64) -> String {
    if ns < 10_000 * NS_PER_MS {
        format!("{} ms", ns / NS_PER_MS)
    } else {
        format!("{:.1} s", ns as f64 / 1e9)
    }
}

/// The `late` / `on_time` state of one connection.
#[derive(Debug, Clone, Default)]
pub struct LateTracker {
    since: Option<i64>,
    max_ns: i64,
}

/// What a depth update's lag changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LagChange {
    Late {
        lag_ns: i64,
    },
    OnTime {
        lag_ns: i64,
        late_for_ns: i64,
        max_lag_ns: i64,
    },
}

impl LateTracker {
    pub fn is_late(&self) -> bool {
        self.since.is_some()
    }

    /// Since when the data has been late.
    pub fn since(&self) -> Option<i64> {
        self.since
    }

    pub fn on_update(&mut self, recv_ts: i64, lag_ns: i64) -> Option<LagChange> {
        match self.since {
            None if lag_ns > LATE_AFTER_NS => {
                self.since = Some(recv_ts);
                self.max_ns = lag_ns;
                Some(LagChange::Late { lag_ns })
            }
            None => None,
            Some(since) => {
                self.max_ns = self.max_ns.max(lag_ns);
                (lag_ns < ON_TIME_BELOW_NS).then(|| {
                    self.since = None;
                    LagChange::OnTime {
                        lag_ns,
                        late_for_ns: recv_ts - since,
                        max_lag_ns: self.max_ns,
                    }
                })
            }
        }
    }

    /// The connection ended: a late interval ends with it. Returns when it began.
    pub fn end(&mut self) -> Option<i64> {
        self.since.take()
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    const MS: i64 = NS_PER_MS;

    #[test]
    fn buckets_keep_three_digits() {
        assert_eq!(bucket(0), 0);
        assert_eq!(bucket(999), 999);
        assert_eq!(bucket(1000), 1000);
        assert_eq!(bucket(1234), 1230);
        assert_eq!(bucket(16_551), 16_500);
        assert_eq!(bucket(123_456), 123_000);
    }

    #[test]
    fn every_bucket_index_maps_back_to_its_lower_bound() {
        for i in 0..BUCKETS {
            assert_eq!(index(lower_bound(i)), i, "bucket {i}");
        }
        assert_eq!(bucket(999_999_999), 999_000_000);
        assert_eq!(
            bucket(5_000_000_000),
            999_000_000,
            "beyond 11 days: the last bucket"
        );
    }

    #[test]
    fn json_lists_only_the_buckets_in_use() {
        let mut h = MsHistogram::default();
        h.record(140 * MS);
        h.record(140 * MS + 5);
        h.record(16_551 * MS);
        h.record(-1);
        let json = serde_json::to_string(&h).unwrap();
        assert_eq!(
            json,
            r#"{"counts":{"140":2,"16500":1},"below_zero":1,"max_ns":16551000000}"#
        );
        let back: MsHistogram = serde_json::from_str(&json).unwrap();
        assert_eq!(back, h);
        assert_eq!(
            MsHistogram::default(),
            serde_json::from_str(r#"{"counts":{},"below_zero":0,"max_ns":0}"#).unwrap()
        );
    }

    #[test]
    fn quantiles_and_summary() {
        let mut h = MsHistogram::default();
        for ms in 1..=100 {
            h.record(ms * MS);
        }
        h.record(16_551 * MS + 600_000);
        h.record(-5 * MS);
        assert_eq!((h.count(), h.below_zero), (101, 1));
        assert_eq!(h.quantile_ms(0.5), Some(51));
        assert_eq!(h.quantile_ms(0.99), Some(100));
        assert_eq!(h.quantile_ms(1.0), Some(16_500));
        assert_eq!(h.summary(), "p50 51 ms, p99 100 ms, max 16.6 s");
        assert_eq!(MsHistogram::default().summary(), "none");
    }

    #[test]
    fn late_starts_above_a_second_and_ends_below_half_a_second() {
        let mut t = LateTracker::default();
        assert_eq!(t.on_update(0, 999 * MS), None);
        assert_eq!(
            t.on_update(10, 1500 * MS),
            Some(LagChange::Late { lag_ns: 1500 * MS })
        );
        assert_eq!(t.on_update(20, 7000 * MS), None);
        assert_eq!(
            t.on_update(30, 700 * MS),
            None,
            "between the thresholds: still late"
        );
        assert_eq!(
            t.on_update(40, 140 * MS),
            Some(LagChange::OnTime {
                lag_ns: 140 * MS,
                late_for_ns: 30,
                max_lag_ns: 7000 * MS
            })
        );
        assert!(!t.is_late());
        t.on_update(50, 2000 * MS);
        assert_eq!(t.end(), Some(50));
        assert!(!t.is_late());
    }

    proptest! {
        #[test]
        fn merging_equals_recording_everything_in_one(
            a in proptest::collection::vec(-1_000i64..100_000_000_000, 0..200),
            b in proptest::collection::vec(-1_000i64..100_000_000_000, 0..200),
        ) {
            let (mut x, mut y, mut all) = Default::default();
            for &v in &a { MsHistogram::record(&mut x, v); MsHistogram::record(&mut all, v); }
            for &v in &b { MsHistogram::record(&mut y, v); MsHistogram::record(&mut all, v); }
            let mut merged: MsHistogram = x;
            merged.merge(&y);
            prop_assert_eq!(&merged, &all);
            // a bucket is at most 1% below the value it holds
            for &v in a.iter().chain(&b).filter(|&&v| v >= 0) {
                let ms = (v / MS) as u64;
                prop_assert!(bucket(ms) <= ms && ms - bucket(ms) <= ms / 100);
            }
        }
    }
}
