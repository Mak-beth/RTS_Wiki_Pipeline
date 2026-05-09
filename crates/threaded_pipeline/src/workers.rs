// THREAD: human_worker_N  (4 threads by default)
// THREAD: bot_worker      (1 thread)
//
// All worker threads run the same function.
// Human workers receive from both channels (biased toward human).
// The bot worker receives only from chan_bot; chan_human is a never-ready
// receiver so the select! human branch is permanently disabled.

use crate::state::DEADLINE_MISS_COUNT;
use crossbeam_channel::{select, Receiver};
use rts_core::{parse_event, LatencySample, Leaderboard};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub fn run_worker_loop(
    id:           usize,
    chan_human:   Receiver<(String, Instant, Instant)>,
    chan_bot:     Receiver<(String, Instant, Instant)>,
    chan_metrics: crossbeam_channel::Sender<LatencySample>,
    leaderboard:  Arc<dyn Leaderboard>,
    shutdown:     Arc<AtomicBool>,
) {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Biased select: human channel is always checked first.
        let (raw, ingest_time, enqueue_time, was_human) = select! {
            recv(chan_human) -> msg => match msg {
                Ok((r, t, e)) => (r, t, e, true),
                Err(_)        => break,
            },
            recv(chan_bot) -> msg => match msg {
                Ok((r, t, e)) => (r, t, e, false),
                Err(_)        => break,
            },
            default(Duration::from_micros(100)) => continue,
        };

        let dequeue_time   = Instant::now();
        let expected_start = enqueue_time;

        if let Ok(event) = parse_event(&raw) {
            leaderboard.record(event.server_name);
        }

        // Simulate 100 µs of processing work (blocking sleep on OS thread).
        std::thread::sleep(Duration::from_micros(100));

        let complete_time = Instant::now();
        let e2e_us = complete_time.duration_since(ingest_time).as_micros() as u64;

        if e2e_us > 2_000 {
            DEADLINE_MISS_COUNT.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                target: "deadline_miss",
                e2e_us,
                was_human,
                worker_id = id,
            );
        }

        let _ = chan_metrics.try_send(LatencySample {
            ingest_time,
            dequeue_time,
            complete_time,
            expected_start,
            was_human,
        });
    }
}
