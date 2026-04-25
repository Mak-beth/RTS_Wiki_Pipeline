use crate::state::DEADLINE_MISS_COUNT;
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
    chan_human:   Arc<Mutex<mpsc::Receiver<(String, Instant)>>>,
    chan_metrics: mpsc::Sender<LatencySample>,
    leaderboard:  Arc<dyn Leaderboard>,
) {
    loop {
        let item = {
            let mut rx = chan_human.lock().await;
            rx.recv().await
        };

        let (raw, ingest_time) = match item {
            Some(v) => v,
            None    => break, // sender dropped — pipeline shutting down
        };

        process(raw, ingest_time, true, id, &chan_metrics, &leaderboard).await;
    }
}

pub async fn run_bot_worker(
    mut chan_bot:  mpsc::Receiver<(String, Instant)>,
    chan_metrics:  mpsc::Sender<LatencySample>,
    leaderboard:   Arc<dyn Leaderboard>,
) {
    while let Some((raw, ingest_time)) = chan_bot.recv().await {
        process(raw, ingest_time, false, 0, &chan_metrics, &leaderboard).await;
    }
}

async fn process(
    raw:          String,
    ingest_time:  Instant,
    was_human:    bool,
    worker_id:    usize,
    chan_metrics: &mpsc::Sender<LatencySample>,
    leaderboard:  &Arc<dyn Leaderboard>,
) {
    let dequeue_time   = Instant::now();
    let expected_start = dequeue_time;

    // Re-parse to get a borrowed WikiEvent for the leaderboard update.
    if let Ok(event) = parse_event(&raw) {
        leaderboard.record(event.server_name);
    }

    // Simulate 100 µs of processing work.
    tokio::time::sleep(Duration::from_micros(100)).await;

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
