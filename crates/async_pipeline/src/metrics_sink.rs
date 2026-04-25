use crate::state::{is_degraded, DEGRADED};
use rts_core::{HistogramAggregator, LatencySample};
use std::collections::VecDeque;
use std::sync::atomic::Ordering;

pub async fn run(
    mut chan_metrics: tokio::sync::mpsc::Receiver<LatencySample>,
    summary_path:     String,
    mut shutdown:     tokio::sync::oneshot::Receiver<()>,
) {
    let mut agg = HistogramAggregator::new();
    let mut rolling: VecDeque<u64> = VecDeque::with_capacity(1001);
    let mut consecutive_normal: u32 = 0;

    loop {
        tokio::select! {
            sample = chan_metrics.recv() => {
                let sample = match sample {
                    Some(s) => s,
                    None    => break, // all senders dropped
                };

                agg.record(&sample);

                let e2e_us = sample.complete_time
                    .duration_since(sample.ingest_time)
                    .as_micros() as u64;

                if rolling.len() >= 1000 {
                    rolling.pop_front();
                }
                rolling.push_back(e2e_us);

                // Fail-safe evaluation on every sample once the window is full.
                if rolling.len() == 1000 {
                    let mut sorted: Vec<u64> = rolling.iter().copied().collect();
                    sorted.sort_unstable();
                    let p99 = sorted[989]; // 99th percentile of 1 000 samples

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

            _ = &mut shutdown => break,
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
    .expect("failed to write summary JSON");

    tracing::info!(target: "summary_written", path = %summary_path);
}
