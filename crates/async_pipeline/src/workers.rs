use crate::state::{DEADLINE_MISS_COUNT, StressConfig};
use rts_core::{parse_event, LatencySample, Leaderboard};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};

/// Multiple human-worker tasks share the same channel receiver via
/// `Arc<Mutex<Receiver>>`.  Each worker locks, receives one item, releases the
/// lock, then processes the item without holding the lock — so all workers
/// can receive concurrently once the previous item has been dequeued.
pub async fn run_human_worker(
    id:           usize,
    chan_human:   Arc<Mutex<mpsc::Receiver<(String, Instant, Instant)>>>,
    chan_metrics: mpsc::Sender<LatencySample>,
    leaderboard:  Arc<dyn Leaderboard>,
    stress:       Arc<StressConfig>,
) {
    loop {
        let item = {
            let mut rx = chan_human.lock().await;
            rx.recv().await
        };

        let (raw, ingest_time, enqueue_time) = match item {
            Some(v) => v,
            None    => break, // sender dropped — pipeline shutting down
        };

        process(raw, ingest_time, enqueue_time, true, id,
                &chan_metrics, &leaderboard, &stress).await;
    }
}

pub async fn run_bot_worker(
    mut chan_bot:  mpsc::Receiver<(String, Instant, Instant)>,
    chan_metrics:  mpsc::Sender<LatencySample>,
    leaderboard:   Arc<dyn Leaderboard>,
    stress:        Arc<StressConfig>,
) {
    while let Some((raw, ingest_time, enqueue_time)) = chan_bot.recv().await {
        process(raw, ingest_time, enqueue_time, false, 0,
                &chan_metrics, &leaderboard, &stress).await;
    }
}

async fn process(
    raw:          String,
    ingest_time:  Instant,
    enqueue_time: Instant,
    was_human:    bool,
    worker_id:    usize,
    chan_metrics: &mpsc::Sender<LatencySample>,
    leaderboard:  &Arc<dyn Leaderboard>,
    stress:       &Arc<StressConfig>,
) {
    // ── Demo latency injection ────────────────────────────────────────────────
    // When inside the injection window, spin-busy for 3 ms on every `every_nth`
    // packet.  Outside the window (or when injection is disabled) the branch
    // is a single compare against `None` — zero hot-path overhead.
    if let Some((win_start, win_end)) = stress.inject_window {
        let elapsed = stress.program_start.elapsed();
        if elapsed >= win_start && elapsed < win_end {
            let n = stress.counter.fetch_add(1, Ordering::Relaxed) + 1;
            if n % stress.every_nth == 0 {
                let target = Instant::now() + Duration::from_millis(3);
                while Instant::now() < target {
                    std::hint::spin_loop();
                }
            }
        }
    }

    let dequeue_time   = Instant::now();
    let expected_start = enqueue_time;

    // Re-parse to get a borrowed WikiEvent for the leaderboard update.
    if let Ok(event) = parse_event(&raw) {
        leaderboard.record(event.server_name);
    }

    // Simulate 100 µs of processing work via busy-spin.
    // Note: tokio::time::sleep on Windows has a 15.6ms tick floor,
    // making it unsuitable for sub-millisecond real-time work.
    let target = Instant::now() + Duration::from_micros(100);
    while Instant::now() < target {
        std::hint::spin_loop();
    }

    let complete_time = Instant::now();
    let e2e_us = complete_time.duration_since(ingest_time).as_micros() as u64;

    if e2e_us > 2_000 {
        DEADLINE_MISS_COUNT.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            target: "deadline_miss",
            e2e_us,
            was_human,
            worker_id,
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
