//! Counting allocator: every allocation made by a thread is counted for that thread, so a
//! measurement on one thread is not disturbed by the others. The binary installs it (the
//! cost is one thread-local increment per allocation), and so do the unit tests; `bench`
//! and the tests use it to show that the hot path does not allocate.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

pub struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

fn count() {
    // const-initialized and without a destructor: valid at any point of a thread's life
    let _ = ALLOCATIONS.try_with(|c| c.set(c.get() + 1));
}

// SAFETY: every call goes to the system allocator unchanged; counting has no other effect.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count();
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// Allocations made by the calling thread so far; always 0 if `Counting` is not the global
/// allocator.
pub fn this_thread() -> u64 {
    ALLOCATIONS.try_with(Cell::get).unwrap_or(0)
}

/// Whether `Counting` is the global allocator of this program.
pub fn active() -> bool {
    let before = this_thread();
    drop(std::hint::black_box(Box::new(0u64)));
    this_thread() > before
}
