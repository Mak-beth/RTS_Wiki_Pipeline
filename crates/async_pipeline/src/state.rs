use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub static DEGRADED:            AtomicBool = AtomicBool::new(false);
pub static OVERFLOW_COUNT:      AtomicU64  = AtomicU64::new(0);
pub static DEADLINE_MISS_COUNT: AtomicU64  = AtomicU64::new(0);
pub static EVENTS_INGESTED:     AtomicU64  = AtomicU64::new(0);

pub fn is_degraded() -> bool {
    DEGRADED.load(Ordering::Relaxed)
}

/// Controls optional latency injection for demo mode.
///
/// When `inject_window` is `None` (all non-demo runs) every worker skips
/// the injection block entirely with a single branch miss — zero hot-path
/// overhead.
pub struct StressConfig {
    /// Elapsed-time window (relative to `program_start`) during which
    /// injection is active.  `None` disables injection permanently.
    pub inject_window: Option<(Duration, Duration)>,
    /// Reference instant captured at pipeline start.
    pub program_start: Instant,
    /// A 3 ms spin-loop is injected on every `every_nth`-th packet.
    pub every_nth:     u64,
    /// Packet counter shared across all worker tasks.
    pub counter:       Arc<AtomicU64>,
}

impl StressConfig {
    /// Build the demo stress schedule: inject every 20th packet between
    /// t=15s and t=25s, using the supplied start instant as the reference.
    pub fn demo(program_start: Instant) -> Arc<Self> {
        Arc::new(Self {
            inject_window: Some((Duration::from_secs(15), Duration::from_secs(25))),
            program_start,
            every_nth: 20,
            counter:   Arc::new(AtomicU64::new(0)),
        })
    }

    /// No-op config: injection permanently disabled.
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            inject_window: None,
            program_start: Instant::now(),
            every_nth:     1,
            counter:       Arc::new(AtomicU64::new(0)),
        })
    }
}
