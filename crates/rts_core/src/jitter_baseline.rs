use hdrhistogram::Histogram;
use std::time::{Duration, Instant};

/// OS scheduling jitter statistics measured by repeated short sleeps.
#[derive(Debug, Clone, Copy)]
pub struct OsJitterReport {
    /// Median wake-latency overshoot in microseconds.
    pub p50_us: u64,
    /// 90th-percentile wake-latency overshoot in microseconds.
    pub p90_us: u64,
    /// 99th-percentile wake-latency overshoot in microseconds.
    /// Deadlines below this value are physically unachievable on this OS/hardware
    /// combination and must be noted as such in the report.
    pub p99_us: u64,
    /// Worst observed wake-latency overshoot in microseconds.
    pub max_us: u64,
}

/// Measure the OS scheduler's baseline wake-up jitter.
///
/// Spawns a dedicated thread that calls `sleep(100µs)` `samples` times and
/// records the delta between the requested and actual wake time.  The thread
/// is isolated from the calling thread's workload so the measurement reflects
/// OS scheduling noise rather than application contention.
pub fn measure_os_jitter(samples: u32) -> OsJitterReport {
    let target_duration = Duration::from_micros(100);

    let handle = std::thread::spawn(move || {
        let mut hist = Histogram::<u64>::new_with_max(1_000_000, 3)
            .expect("jitter histogram init failed");

        for _ in 0..samples {
            let before = Instant::now();
            std::thread::sleep(target_duration);
            let elapsed = before.elapsed();

            let overshoot_us = if elapsed > target_duration {
                (elapsed - target_duration).as_micros() as u64
            } else {
                0
            };

            let cap = hist.high();
            let _ = hist.record(overshoot_us.min(cap));
        }

        hist
    });

    let hist = handle.join().expect("jitter measurement thread panicked");

    OsJitterReport {
        p50_us: hist.value_at_quantile(0.50),
        p90_us: hist.value_at_quantile(0.90),
        p99_us: hist.value_at_quantile(0.99),
        max_us: hist.max(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_report_is_sane() {
        let report = measure_os_jitter(200);

        // All fields must be non-negative (they are u64, so this is always true,
        // but we also verify the histogram produced actual data).
        assert!(
            report.max_us > 0,
            "max jitter was 0µs — suggests the sleep loop did not run"
        );

        // Fundamental percentile ordering must hold.
        assert!(
            report.p50_us <= report.p90_us,
            "p50={} > p90={} — histogram ordering violated",
            report.p50_us, report.p90_us
        );
        assert!(
            report.p90_us <= report.p99_us,
            "p90={} > p99={} — histogram ordering violated",
            report.p90_us, report.p99_us
        );
        assert!(
            report.p99_us <= report.max_us,
            "p99={} > max={} — histogram ordering violated",
            report.p99_us, report.max_us
        );
    }
}
