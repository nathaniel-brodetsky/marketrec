//! Bounded single-producer single-consumer queue between threads, never blocking the
//! producer.
//!
//! Built on `rtrb`, a wait-free ring buffer. If the queue is full the item is dropped and
//! counted; before the next item that fits, a marker with the count is enqueued, so the
//! consumer always learns where data is missing. The consumer spins briefly when the queue
//! is empty, then parks; the producer wakes it after each push.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::{self, Thread};
use std::time::Duration;

pub struct Sender<T> {
    prod: rtrb::Producer<T>,
    wake: Arc<OnceLock<Thread>>,
    dropped: u64,
    total_dropped: u64,
    high_water: Arc<AtomicUsize>,
}

pub struct Receiver<T> {
    cons: rtrb::Consumer<T>,
    wake: Arc<OnceLock<Thread>>,
}

/// Largest number of items the queue has held at once.
#[derive(Clone)]
pub struct HighWater(Arc<AtomicUsize>);

impl HighWater {
    pub fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>, HighWater) {
    let (prod, cons) = rtrb::RingBuffer::new(capacity);
    let wake = Arc::new(OnceLock::new());
    let high_water = Arc::new(AtomicUsize::new(0));
    let tx = Sender {
        prod,
        wake: wake.clone(),
        dropped: 0,
        total_dropped: 0,
        high_water: high_water.clone(),
    };
    (tx, Receiver { cons, wake }, HighWater(high_water))
}

impl<T> Sender<T> {
    /// Enqueue `item`, or drop and count it if the queue is full. Returns whether it was
    /// enqueued. `marker` builds the item that reports how many were dropped.
    pub fn push(&mut self, item: T, marker: impl FnOnce(u64) -> T) -> bool {
        if self.dropped > 0 {
            if self.prod.slots() < 2 {
                self.dropped += 1;
                self.total_dropped += 1;
                return false;
            }
            let lost = std::mem::take(&mut self.dropped);
            let _ = self.prod.push(marker(lost));
        }
        match self.prod.push(item) {
            Ok(()) => {
                let used = self.prod.buffer().capacity() - self.prod.slots();
                self.high_water.fetch_max(used, Ordering::Relaxed);
                if let Some(t) = self.wake.get() {
                    t.unpark();
                }
                true
            }
            Err(_) => {
                self.dropped += 1;
                self.total_dropped += 1;
                false
            }
        }
    }

    /// Items dropped so far because the queue was full.
    pub fn total_dropped(&self) -> u64 {
        self.total_dropped
    }

    /// The receiver has been dropped: nothing will be read any more.
    pub fn receiver_gone(&self) -> bool {
        self.prod.is_abandoned()
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if let Some(t) = self.wake.get() {
            t.unpark(); // let the consumer notice that the queue is closed
        }
    }
}

impl<T> Receiver<T> {
    /// The next item; waits for one. `None` once the sender is gone and the queue is empty.
    pub fn recv(&mut self) -> Option<T> {
        let _ = self.wake.set(thread::current());
        let mut spins = 0u32;
        loop {
            if let Ok(v) = self.cons.pop() {
                return Some(v);
            }
            if self.cons.is_abandoned() {
                return self.cons.pop().ok();
            }
            if spins < 200 {
                spins += 1;
                std::hint::spin_loop();
            } else {
                // the timeout only covers a wake-up lost between the check and the park
                thread::park_timeout(Duration::from_millis(1));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    enum Item {
        Data(u32),
        Lost(u64),
    }

    #[test]
    fn full_queue_drops_and_reports_the_loss_before_the_next_item() {
        let (mut tx, mut rx, hw) = channel::<Item>(4);
        for i in 0..10 {
            tx.push(Item::Data(i), Item::Lost);
        }
        assert_eq!(tx.total_dropped(), 6);
        assert_eq!(hw.get(), 4);
        let first: Vec<Item> = (0..4).map(|_| rx.recv().unwrap()).collect();
        assert_eq!(
            first,
            [Item::Data(0), Item::Data(1), Item::Data(2), Item::Data(3)]
        );
        tx.push(Item::Data(10), Item::Lost);
        assert_eq!(rx.recv(), Some(Item::Lost(6)));
        assert_eq!(rx.recv(), Some(Item::Data(10)));
    }

    #[test]
    fn across_threads_in_order_and_closes() {
        let (mut tx, mut rx, _) = channel::<Item>(1024);
        let consumer = thread::spawn(move || {
            let mut got = Vec::new();
            while let Some(Item::Data(v)) = rx.recv() {
                got.push(v);
            }
            got
        });
        for i in 0..100_000 {
            while !tx.push(Item::Data(i), Item::Lost) {
                // a test that must not lose data waits; the recorder never does
                thread::yield_now();
                tx.dropped = 0;
            }
        }
        drop(tx);
        let got = consumer.join().unwrap();
        assert_eq!(got.len(), 100_000);
        assert!(got.windows(2).all(|w| w[0] + 1 == w[1]));
    }
}
