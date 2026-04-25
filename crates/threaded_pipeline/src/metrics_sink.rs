// THREAD: metrics_sink

use crate::state::{is_degraded, DEGRADED};
use crossbeam_channel::{Receiver, RecvTimeoutError};
use rts_core::{HistogramAggregator, LatencySample};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub fn run(
    chan_metrics: Receiver<LatencySample>,
    summary_path: String,
    shutdown:     Arc<AtomicBool>,
) {
    let mut agg = HistogramAggregator::new();
    let mut rolling: VecDeque<u64> = VecDeque::with_capacity(1001);
    let mut consecutive_normal: u32 = 0;

    loop {
        match chan_metrics.recv_timeout(Duration::from_millis(100)) {
            Ok(sample) => {
                agg.record(&sample);

                let e2e_us = sample.complete_time
                    .duration_since(sample.ingest_time)
                    .as_micros() as u64;

                if rolling.len() >= 1000 {
                    rolling.pop_front();
                }
                rolling.push_back(e2e_us);

                if rolling.len() == 1000 {
                    let mut sorted: Vec<u64> = rolling.iter().copied().collect();
                    sorted.sort_unstable();
                    let p99 = sorted[989];

                    if !is_degraded() && p99 > 5_000 {
                        DEGRADED.store(true, Ordering::Relaxed);
                        consecutive_normal = 0;
                        tracing::warn!(
                            target: "mode_transition",
                            to = "degraded",
                            p99_us = p99
                        );
                    } else if is_degraded() && p99 < 3_000 {
                        consecutive_normal += 1;
                        if consecutive_normal >= 500 {
                            DEGRADED.store(false, Ordering::Relaxed);
                            consecutive_normal = 0;
                            tracing::info!(
                                target: "mode_transition",
                                to = "normal",
                                p99_us = p99
                            );
                        }
                    } else if !is_degraded() {
                        consecutive_normal = 0;
                    }
                }
            }

            Err(RecvTimeoutError::Timeout) => {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
            }

            Err(RecvTimeoutError::Disconnected) => break,
        }
    }

    // Drain any remaining samples before writing the summary.
    while let Ok(sample) = chan_metrics.try_recv() {
        agg.record(&sample);
    }

    let summary = agg.emit_summary();
    std::fs::write(
        &summary_path,
        serde_json::to_string_pretty(&summary).unwrap(),
    )
    .expect("failed to write threaded summary JSON");

    tracing::info!(target: "summary_written", path = %summary_path);
}
