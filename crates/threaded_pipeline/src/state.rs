use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub static DEGRADED:            AtomicBool = AtomicBool::new(false);
pub static OVERFLOW_COUNT:      AtomicU64  = AtomicU64::new(0);
pub static DEADLINE_MISS_COUNT: AtomicU64  = AtomicU64::new(0);
pub static EVENTS_INGESTED:     AtomicU64  = AtomicU64::new(0);

pub fn is_degraded() -> bool {
    DEGRADED.load(Ordering::Relaxed)
}
