use hdrhistogram::Histogram;
use serde_json::{json, Value};
use std::time::Instant;

/// Timing data collected for a single processed event.
#[derive(Debug, Clone)]
pub struct LatencySample {
    /// Moment the raw SSE bytes were received from the network layer.
    pub ingest_time: Instant,
    /// Moment the event was dequeued and processing began.
    pub dequeue_time: Instant,
    /// Moment processing completed.
    pub complete_time: Instant,
    /// The scheduled (expected) start time for this task slot.
    pub expected_start: Instant,
    /// True when the edit was made by a human (not a bot).
    pub was_human: bool,
}

/// Aggregates latency, drift, and processing-time measurements into HDR histograms,
/// split by event priority so human-edit and bot-edit paths can be compared.
pub struct HistogramAggregator {
    e2e_latency_human:      Histogram<u64>,
    e2e_latency_bot:        Histogram<u64>,
    drift_human:            Histogram<u64>,
    drift_bot:              Histogram<u64>,
    processing_time_human:  Histogram<u64>,
    processing_time_bot:    Histogram<u64>,
}

impl HistogramAggregator {
    pub fn new() -> Self {
        let mk = || {
            Histogram::<u64>::new_with_max(60_000_000, 3)
                .expect("histogram initialisation failed")
        };
        Self {
            e2e_latency_human:     mk(),
            e2e_latency_bot:       mk(),
            drift_human:           mk(),
            drift_bot:             mk(),
            processing_time_human: mk(),
            processing_time_bot:   mk(),
        }
    }

    /// Record one sample. All derived durations are in microseconds.
    pub fn record(&mut self, s: &LatencySample) {
        let e2e = s.complete_time.duration_since(s.ingest_time).as_micros() as u64;

        let drift = if s.dequeue_time >= s.expected_start {
            s.dequeue_time.duration_since(s.expected_start).as_micros() as u64
        } else {
            0
        };

        let proc_time = s.complete_time.duration_since(s.dequeue_time).as_micros() as u64;

        if s.was_human {
            let cap = self.e2e_latency_human.high();
            let _ = self.e2e_latency_human.record(e2e.min(cap));
            let cap = self.drift_human.high();
            let _ = self.drift_human.record(drift.min(cap));
            let cap = self.processing_time_human.high();
            let _ = self.processing_time_human.record(proc_time.min(cap));
        } else {
            let cap = self.e2e_latency_bot.high();
            let _ = self.e2e_latency_bot.record(e2e.min(cap));
            let cap = self.drift_bot.high();
            let _ = self.drift_bot.record(drift.min(cap));
            let cap = self.processing_time_bot.high();
            let _ = self.processing_time_bot.record(proc_time.min(cap));
        }
    }

    /// Return a JSON object with p50/p90/p99/p999/max/count for every histogram.
    /// All values are in microseconds.
    pub fn emit_summary(&self) -> Value {
        fn stats(h: &Histogram<u64>) -> Value {
            json!({
                "p50":   h.value_at_quantile(0.500),
                "p90":   h.value_at_quantile(0.900),
                "p99":   h.value_at_quantile(0.990),
                "p999":  h.value_at_quantile(0.999),
                "max":   h.max(),
                "count": h.len(),
            })
        }
        json!({
            "e2e_latency_human":     stats(&self.e2e_latency_human),
            "e2e_latency_bot":       stats(&self.e2e_latency_bot),
            "drift_human":           stats(&self.drift_human),
            "drift_bot":             stats(&self.drift_bot),
            "processing_time_human": stats(&self.processing_time_human),
            "processing_time_bot":   stats(&self.processing_time_bot),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn percentile_ordering_and_count() {
        let mut agg = HistogramAggregator::new();
        let base = Instant::now();

        // Record 100 human samples with e2e times of 51µs … 150µs.
        for i in 1..=100u64 {
            let sample = LatencySample {
                ingest_time:    base,
                dequeue_time:   base + Duration::from_micros(50),
                complete_time:  base + Duration::from_micros(50 + i),
                expected_start: base + Duration::from_micros(50),
                was_human:      true,
            };
            agg.record(&sample);
        }

        let summary = agg.emit_summary();
        let p50   = summary["e2e_latency_human"]["p50"].as_u64().unwrap();
        let p90   = summary["e2e_latency_human"]["p90"].as_u64().unwrap();
        let p99   = summary["e2e_latency_human"]["p99"].as_u64().unwrap();
        let count = summary["e2e_latency_human"]["count"].as_u64().unwrap();

        assert_eq!(count, 100, "expected 100 samples");
        // With values 51..150µs: p50≈100, p90≈140, p99≈149
        assert!(p50 >= 95  && p50 <= 105, "p50={p50} out of expected range 95–105");
        assert!(p90 >= 135 && p90 <= 145, "p90={p90} out of expected range 135–145");
        assert!(p99 >= 145 && p99 <= 150, "p99={p99} out of expected range 145–150");
        // Ordering invariant
        assert!(p50 <= p90, "p50={p50} > p90={p90}");
        assert!(p90 <= p99, "p90={p90} > p99={p99}");
    }

    #[test]
    fn bot_samples_do_not_pollute_human_histogram() {
        let mut agg = HistogramAggregator::new();
        let base = Instant::now();
        let bot_sample = LatencySample {
            ingest_time:    base,
            dequeue_time:   base + Duration::from_micros(50),
            complete_time:  base + Duration::from_micros(60),
            expected_start: base + Duration::from_micros(50),
            was_human:      false,
        };
        agg.record(&bot_sample);

        let summary = agg.emit_summary();
        assert_eq!(summary["e2e_latency_human"]["count"].as_u64().unwrap(), 0);
        assert_eq!(summary["e2e_latency_bot"]["count"].as_u64().unwrap(), 1);
    }
}
