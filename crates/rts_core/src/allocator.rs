use std::alloc::{GlobalAlloc, Layout, System};
use std::ops::Sub;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct TrackingAllocator {
    inner: System,
}

static ALLOC_COUNT:   AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES:   AtomicUsize = AtomicUsize::new(0);
static DEALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static DEALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator { inner: System };

impl TrackingAllocator {
    /// Zero all counters. Call immediately before the code-under-test.
    pub fn reset() {
        ALLOC_COUNT.store(0, Ordering::Relaxed);
        ALLOC_BYTES.store(0, Ordering::Relaxed);
        DEALLOC_COUNT.store(0, Ordering::Relaxed);
        DEALLOC_BYTES.store(0, Ordering::Relaxed);
    }

    /// Read current counter values atomically (each counter read separately —
    /// Relaxed ordering is intentional; we accept non-atomic snapshots for stats).
    pub fn snapshot() -> AllocSnapshot {
        AllocSnapshot {
            alloc_count:   ALLOC_COUNT.load(Ordering::Relaxed),
            alloc_bytes:   ALLOC_BYTES.load(Ordering::Relaxed),
            dealloc_count: DEALLOC_COUNT.load(Ordering::Relaxed),
            dealloc_bytes: DEALLOC_BYTES.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time reading of the tracking allocator counters.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllocSnapshot {
    pub alloc_count:   usize,
    pub alloc_bytes:   usize,
    pub dealloc_count: usize,
    pub dealloc_bytes: usize,
}

impl Sub for AllocSnapshot {
    type Output = AllocSnapshot;

    /// Compute the delta between two snapshots (wrapping on underflow).
    fn sub(self, rhs: AllocSnapshot) -> AllocSnapshot {
        AllocSnapshot {
            alloc_count:   self.alloc_count.wrapping_sub(rhs.alloc_count),
            alloc_bytes:   self.alloc_bytes.wrapping_sub(rhs.alloc_bytes),
            dealloc_count: self.dealloc_count.wrapping_sub(rhs.dealloc_count),
            dealloc_bytes: self.dealloc_bytes.wrapping_sub(rhs.dealloc_bytes),
        }
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        self.inner.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        self.inner.dealloc(ptr, layout)
    }
}
