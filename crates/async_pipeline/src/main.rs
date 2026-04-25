mod dispatcher;
mod ingestion;
mod metrics_sink;
mod state;
mod workers;

use clap::Parser;
use rts_core::{AtomicLeaderboard, Leaderboard, MutexLeaderboard, RwLockLeaderboard};
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "track-alloc")]
#[global_allocator]
static ALLOC: rts_core::allocator::TrackingAllocator =
    rts_core::allocator::TrackingAllocator::new();

#[derive(Parser)]
#[command(name = "async_pipeline", about = "Wikipedia Recent Changes — async pipeline")]
struct Args {
    #[arg(
        long,
        default_value = "https://stream.wikimedia.org/v2/stream/recentchange"
    )]
    sse_url: String,

    /// Run for this many seconds then exit (omit to run until Ctrl-C).
    #[arg(long)]
    duration: Option<u64>,

    /// Synchronisation primitive used for the top-3 leaderboard.
    #[arg(long, default_value = "mutex", value_parser = ["mutex", "rwlock", "atomic"])]
    leaderboard_impl: String,

    /// Capacity of the bounded ring and per-priority channels.
    #[arg(long, default_value_t = 1024)]
    channel_capacity: usize,

    /// Number of concurrent human-edit worker tasks.
    #[arg(long, default_value_t = 4)]
    human_workers: usize,

    /// If set, capture up to 500 raw SSE lines to this file for benchmarks.
    #[arg(long)]
    capture_sample: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // ── 1. Logging ────────────────────────────────────────────────────────────
    std::fs::create_dir_all("logs").expect("failed to create logs/");
    let log_file = std::fs::File::create("logs/async_run.jsonl")
        .expect("failed to create logs/async_run.jsonl");

    tracing_subscriber::fmt()
        .json()
        .with_writer(std::sync::Mutex::new(log_file))
        .with_max_level(tracing::Level::INFO)
        .init();

    // ── 2. OS jitter baseline ─────────────────────────────────────────────────
    tracing::info!(target: "startup", "measuring OS jitter baseline (500 samples)…");
    let jitter = rts_core::measure_os_jitter(500);
    tracing::info!(
        target: "os_jitter",
        p50_us = jitter.p50_us,
        p90_us = jitter.p90_us,
        p99_us = jitter.p99_us,
        max_us = jitter.max_us,
        note   = "DeadlineMiss events below p99 are OS scheduling noise"
    );

    // ── 3. Leaderboard ────────────────────────────────────────────────────────
    let leaderboard: Arc<dyn Leaderboard> = match args.leaderboard_impl.as_str() {
        "rwlock" => Arc::new(RwLockLeaderboard::new()),
        "atomic" => Arc::new(AtomicLeaderboard::new()),
        _        => Arc::new(MutexLeaderboard::new()),
    };

    // ── 4. Channels ───────────────────────────────────────────────────────────
    let ring = Arc::new(ingestion::BoundedRing::new(args.channel_capacity));

    let (chan_human_tx, chan_human_rx) =
        tokio::sync::mpsc::channel::<(String, std::time::Instant)>(args.channel_capacity);
    let (chan_bot_tx, chan_bot_rx) =
        tokio::sync::mpsc::channel::<(String, std::time::Instant)>(args.channel_capacity);
    let (chan_metrics_tx, chan_metrics_rx) =
        tokio::sync::mpsc::channel::<rts_core::LatencySample>(4096);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Human workers share a single receiver via a Mutex.
    let chan_human_shared =
        Arc::new(tokio::sync::Mutex::new(chan_human_rx));

    // ── 5. Spawn tasks ────────────────────────────────────────────────────────
    tokio::spawn(ingestion::run(
        ingestion::IngestionConfig {
            sse_url:      args.sse_url.clone(),
            capacity:     args.channel_capacity,
            capture_path: args.capture_sample.clone(),
        },
        Arc::clone(&ring),
    ));

    tokio::spawn(dispatcher::run(
        Arc::clone(&ring),
        chan_human_tx,
        chan_bot_tx,
    ));

    for id in 0..args.human_workers {
        tokio::spawn(workers::run_human_worker(
            id,
            Arc::clone(&chan_human_shared),
            chan_metrics_tx.clone(),
            Arc::clone(&leaderboard),
        ));
    }

    tokio::spawn(workers::run_bot_worker(
        chan_bot_rx,
        chan_metrics_tx.clone(),
        Arc::clone(&leaderboard),
    ));

    tokio::spawn(metrics_sink::run(
        chan_metrics_rx,
        "logs/async_summary.json".to_string(),
        shutdown_rx,
    ));

    tracing::info!(
        target: "startup",
        sse_url        = %args.sse_url,
        leaderboard    = %args.leaderboard_impl,
        human_workers  = args.human_workers,
        channel_cap    = args.channel_capacity,
        duration_secs  = ?args.duration,
        "pipeline running"
    );

    // ── 6. Wait for shutdown ──────────────────────────────────────────────────
    if let Some(secs) = args.duration {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(secs)) => {
                tracing::info!(target: "shutdown", "duration elapsed");
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!(target: "shutdown", "ctrl-c received");
            }
        }
    } else {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!(target: "shutdown", "ctrl-c received");
    }

    // ── 7. Flush ──────────────────────────────────────────────────────────────
    let _ = shutdown_tx.send(());
    tokio::time::sleep(Duration::from_millis(500)).await;

    let top3 = leaderboard.top_3();
    tracing::info!(target: "final_leaderboard", ?top3);
}
