// THREAD: dispatcher
// Mirrors async_pipeline dispatcher logic using blocking crossbeam channels.

use crate::ingestion::BoundedRing;
use crate::state::is_degraded;
use crossbeam_channel::Sender;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub fn run(
    ring:       Arc<BoundedRing>,
    chan_human: Sender<(String, Instant, Instant)>,
    chan_bot:   Sender<(String, Instant, Instant)>,
    shutdown:   Arc<AtomicBool>,
) {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let (data, ingest_time) = match ring.pop() {
            Some(item) => item,
            None => {
                std::thread::sleep(Duration::from_micros(100));
                continue;
            }
        };

        // ── Zero-copy scope ───────────────────────────────────────────────────
        // parse_event borrows from `data` to read the bot flag.
        // The owned String is forwarded to workers, which re-parse to get their
        // own borrowed WikiEvent. This avoids cloning string data while
        // respecting Rust's ownership model.
        // ─────────────────────────────────────────────────────────────────────

        #[cfg(feature = "track-alloc")]
        let snap_before = rts_core::allocator::TrackingAllocator::snapshot();

        let is_bot = match rts_core::parse_event(&data) {
            Ok(event) => event.bot,
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

        if is_degraded() && is_bot {
            tracing::trace!(target: "bot_dropped_degraded", "bot event dropped in degraded mode");
            continue;
        }

        let enqueue_time = Instant::now();
        let payload = (data, ingest_time, enqueue_time);
        if is_bot {
            if chan_bot.try_send(payload).is_err() {
                tracing::warn!(target: "channel_full", priority = "bot");
            }
        } else if chan_human.try_send(payload).is_err() {
            tracing::warn!(target: "channel_full", priority = "human");
        }
    }
}
