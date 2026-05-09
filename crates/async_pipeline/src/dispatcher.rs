use crate::ingestion::BoundedRing;
use crate::state::is_degraded;
use rts_core::{parse_event, Priority};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::Sender;

pub async fn run(
    ring:       Arc<BoundedRing>,
    chan_human: Sender<(String, Instant, Instant)>,
    chan_bot:   Sender<(String, Instant, Instant)>,
) {
    loop {
        match ring.pop().await {
            Some((data, ingest_time)) => {
                // ── Zero-copy scope ──────────────────────────────────────────
                // parse_event borrows from `data` to read the bot flag.
                // The owned String is forwarded to workers, which re-parse to
                // get their own borrowed WikiEvent. This avoids cloning string
                // data while respecting Rust's ownership model.
                // ─────────────────────────────────────────────────────────────

                #[cfg(feature = "track-alloc")]
                let snap_before = rts_core::allocator::TrackingAllocator::snapshot();

                let priority = match parse_event(&data) {
                    Ok(event) => Priority::from(&event),
                    Err(e) => {
                        tracing::debug!(parse_error = %e);
                        continue;
                    }
                };

                #[cfg(feature = "track-alloc")]
                {
                    let snap_after = rts_core::allocator::TrackingAllocator::snapshot();
                    let delta = snap_after - snap_before;
                    tracing::info!(
                        target: "alloc_audit",
                        allocs = delta.alloc_count,
                        bytes  = delta.alloc_bytes
                    );
                }

                // Degrade mode: silently drop bot events to shed load.
                if is_degraded() && priority == Priority::Bot {
                    tracing::trace!("dropping bot event in degraded mode");
                    continue;
                }

                let enqueue_time = Instant::now();
                match priority {
                    Priority::Human => {
                        if chan_human.try_send((data, ingest_time, enqueue_time)).is_err() {
                            tracing::warn!("human channel full, dropping event");
                        }
                    }
                    Priority::Bot => {
                        if chan_bot.try_send((data, ingest_time, enqueue_time)).is_err() {
                            tracing::warn!("bot channel full, dropping event");
                        }
                    }
                }
            }

            // Ring empty - yield to other tasks briefly without using
            // tokio::time::sleep, which has a 15.6ms tick floor on Windows.
            None => {
                tokio::task::yield_now().await;
            }
        }
    }
}
