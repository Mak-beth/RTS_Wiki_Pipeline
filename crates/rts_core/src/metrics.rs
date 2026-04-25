use hdrhistogram::Histogram;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct LatencyHistogram {
    inner: Mutex<Histogram<u64>>,
}

impl LatencyHistogram {
    /// `max_us`: maximum trackable value in microseconds.
    pub fn new(max_us: u64) -> Self {
        Self {
            inner: Mutex::new(
                Histogram::<u64>::new_with_max(max_us, 3)
                    .expect("invalid histogram params"),
            ),
        }
    }

    pub fn record(&self, value_us: u64) {
        let mut h = self.inner.lock();
        let cap = h.high();
        let _ = h.record(value_us.min(cap));
    }

    pub fn percentiles(&self) -> Percentiles {
        let h = self.inner.lock();
        Percentiles {
            p50:   h.value_at_quantile(0.50),
            p90:   h.value_at_quantile(0.90),
            p99:   h.value_at_quantile(0.99),
            count: h.len(),
        }
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct Percentiles {
    pub p50:   u64,
    pub p90:   u64,
    pub p99:   u64,
    pub count: u64,
}

pub struct Counter(AtomicU64);

impl Counter {
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}
