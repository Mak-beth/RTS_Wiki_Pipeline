use std::alloc::{GlobalAlloc, Layout, System};
use std::ops::Sub;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A `GlobalAlloc` wrapper around the system allocator that counts every
/// allocation and deallocation via lock-free `AtomicUsize` counters.
///
/// # Usage
///
/// Declare in the pipeline binary (gated behind the `track-alloc` feature):
///
/// ```rust,ignore
/// #[cfg(feature = "track-alloc")]
/// #[global_allocator]
/// static ALLOC: rts_core::allocator::TrackingAllocator =
///     rts_core::allocator::TrackingAllocator::new();
/// ```
///
/// Then take snapshots around the code under test:
///
/// ```rust,ignore
/// let before = TrackingAllocator::snapshot();
/// let _ = parse_event(&buf);
/// let delta = TrackingAllocator::snapshot() - before;
/// assert_eq!(delta.alloc_count, 0, "parse_event should not allocate");
/// ```
///
/// # Measurement limitations
///
/// The counters are global — they capture allocations on **all threads**.
/// In a multi-threaded runtime, snapshot deltas will include noise from
/// concurrent threads. Use a `current_thread` Tokio runtime or a dedicated
/// single-threaded benchmark to get noise-free per-call measurements.
pub struct TrackingAllocator {
    pub(crate) inner: System,
}

static ALLOC_COUNT:   AtomicUsize = AtomicUsize::new(0);
static ALLOC_BYTES:   AtomicUsize = AtomicUsize::new(0);
static DEALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static DEALLOC_BYTES: AtomicUsize = AtomicUsize::new(0);

impl TrackingAllocator {
    /// Create a new `TrackingAllocator`.  Intended for use as a
    /// `static` — the `const fn` ensures zero-cost initialisation.
    pub const fn new() -> Self {
        Self { inner: System }
    }

    /// Reset all counters to zero.  Call immediately before the code under
    /// test to establish a clean baseline.
    pub fn reset() {
        ALLOC_COUNT.store(0, Ordering::Relaxed);
        ALLOC_BYTES.store(0, Ordering::Relaxed);
        DEALLOC_COUNT.store(0, Ordering::Relaxed);
        DEALLOC_BYTES.store(0, Ordering::Relaxed);
    }

    /// Read current counter values into an [`AllocSnapshot`].
    ///
    /// Each counter is read independently with `Relaxed` ordering — the
    /// snapshot is not atomic across all four fields, which is acceptable
    /// for statistical measurement.  Subtract two snapshots to obtain the
    /// delta for a code region.
    pub fn snapshot() -> AllocSnapshot {
        AllocSnapshot {
            alloc_count:   ALLOC_COUNT.load(Ordering::Relaxed),
            alloc_bytes:   ALLOC_BYTES.load(Ordering::Relaxed),
            dealloc_count: DEALLOC_COUNT.load(Ordering::Relaxed),
            dealloc_bytes: DEALLOC_BYTES.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time reading of all four tracking allocator counters.
///
/// Subtract an earlier snapshot from a later one (via [`Sub`]) to get the
/// delta for the enclosed code region.  All arithmetic is wrapping to handle
/// counter roll-over gracefully.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllocSnapshot {
    /// Total number of heap allocations since the last reset (or program start).
    pub alloc_count:   usize,
    /// Total bytes allocated since the last reset.
    pub alloc_bytes:   usize,
    /// Total number of heap deallocations since the last reset.
    pub dealloc_count: usize,
    /// Total bytes deallocated since the last reset.
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
