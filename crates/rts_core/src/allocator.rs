use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct TrackingAllocator {
    inner: System,
}

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static FREE_COUNT:  AtomicU64 = AtomicU64::new(0);

impl TrackingAllocator {
    pub const fn new() -> Self {
        Self { inner: System }
    }

    pub fn reset() {
        ALLOC_COUNT.store(0, Ordering::Relaxed);
        ALLOC_BYTES.store(0, Ordering::Relaxed);
        FREE_COUNT.store(0, Ordering::Relaxed);
    }

    pub fn snapshot() -> AllocSnapshot {
        AllocSnapshot {
            alloc_count: ALLOC_COUNT.load(Ordering::Relaxed),
            alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
            free_count:  FREE_COUNT.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AllocSnapshot {
    pub alloc_count: u64,
    pub alloc_bytes: u64,
    pub free_count:  u64,
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        self.inner.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        FREE_COUNT.fetch_add(1, Ordering::Relaxed);
        self.inner.dealloc(ptr, layout)
    }
}
