mod dispatcher;
mod ingestion;
mod metrics_sink;
mod state;
mod workers;

use clap::Parser;
use ingestion::BoundedRing;
use rts_core::{AtomicLeaderboard, Leaderboard, MutexLeaderboard, RwLockLeaderboard};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[cfg(feature = "track-alloc")]
#[global_allocator]
static ALLOC: rts_core::allocator::TrackingAllocator =
    rts_core::allocator::TrackingAllocator::new();

#[derive(Parser)]
#[command(name = "threaded_pipeline", about = "Wikipedia Recent Changes — threaded pipeline")]
struct Args {
    #[arg(
        long,
        default_value = "https://stream.wikimedia.org/v2/stream/recentchange"
    )]
    sse_url: String,

    /// Run for this many seconds then exit (omit to run until Ctrl-C).
    #[arg(long)]
    duration: Option<u64>,

    #[arg(long, default_value = "mutex", value_parser = ["mutex", "rwlock", "atomic"])]
    leaderboard_impl: String,

    #[arg(long, default_value_t = 1024)]
    channel_capacity: usize,

    #[arg(long, default_value_t = 4)]
    human_workers: usize,

    #[arg(long)]
    capture_sample: Option<String>,
}

fn main() {
    let args = Args::parse();

    // ── 1. Logging ────────────────────────────────────────────────────────────
    std::fs::create_dir_all("logs").expect("failed to create logs/");
    let log_file = std::fs::File::create("logs/threaded_run.jsonl")
        .expect("failed to create logs/threaded_run.jsonl");

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

    // ── 4. Shared state ───────────────────────────────────────────────────────
    let shutdown:       Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    let reconnect_flag: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    let condvar: Arc<(Mutex<bool>, Condvar)> = Arc::new((Mutex::new(false), Condvar::new()));
    let ring = Arc::new(BoundedRing::new(args.channel_capacity));

    // ── 5. Channels ───────────────────────────────────────────────────────────
    let (human_tx, human_rx) =
        crossbeam_channel::bounded::<(String, Instant, Instant)>(args.channel_capacity);
    let (bot_tx, bot_rx) =
        crossbeam_channel::bounded::<(String, Instant, Instant)>(args.channel_capacity);
    let (metrics_tx, metrics_rx) =
        crossbeam_channel::bounded::<rts_core::LatencySample>(4096);

    // ── 6. Spawn threads ──────────────────────────────────────────────────────
    let mut handles = Vec::new();

    // Watchdog thread
    {
        let condvar    = Arc::clone(&condvar);
        let reconnect  = Arc::clone(&reconnect_flag);
        let shutdown   = Arc::clone(&shutdown);
        handles.push(std::thread::Builder::new()
            .name("watchdog".into())
            .spawn(move || ingestion::run_watchdog(condvar, reconnect, shutdown))
            .expect("failed to spawn watchdog"));
    }

    // Ingestion thread
    {
        let ring       = Arc::clone(&ring);
        let condvar    = Arc::clone(&condvar);
        let reconnect  = Arc::clone(&reconnect_flag);
        let shutdown   = Arc::clone(&shutdown);
        handles.push(std::thread::Builder::new()
            .name("ingestion".into())
            .spawn(move || ingestion::run_ingestion(
                ingestion::IngestionConfig {
                    sse_url:      args.sse_url.clone(),
                    capacity:     args.channel_capacity,
                    capture_path: args.capture_sample.clone(),
                },
                ring, condvar, reconnect, shutdown,
            ))
            .expect("failed to spawn ingestion"));
    }

    // Dispatcher thread
    {
        let ring     = Arc::clone(&ring);
        let shutdown = Arc::clone(&shutdown);
        handles.push(std::thread::Builder::new()
            .name("dispatcher".into())
            .spawn(move || dispatcher::run(ring, human_tx, bot_tx, shutdown))
            .expect("failed to spawn dispatcher"));
    }

    // Human worker threads — crossbeam Receiver is Clone (MPMC)
    for id in 0..args.human_workers {
        let human_rx     = human_rx.clone();
        let bot_rx       = bot_rx.clone();
        let metrics_tx   = metrics_tx.clone();
        let leaderboard  = Arc::clone(&leaderboard);
        let shutdown     = Arc::clone(&shutdown);
        handles.push(std::thread::Builder::new()
            .name(format!("human_worker_{id}"))
            .spawn(move || {
                workers::run_worker_loop(id, human_rx, bot_rx, metrics_tx, leaderboard, shutdown)
            })
            .expect("failed to spawn human worker"));
    }

    // Bot worker — never-ready dummy human receiver so select! skips it
    {
        let dummy_rx:    crossbeam_channel::Receiver<(String, Instant, Instant)> =
            crossbeam_channel::never();
        let bot_rx       = bot_rx.clone();
        let metrics_tx   = metrics_tx.clone();
        let leaderboard  = Arc::clone(&leaderboard);
        let shutdown     = Arc::clone(&shutdown);
        handles.push(std::thread::Builder::new()
            .name("bot_worker".into())
            .spawn(move || {
                workers::run_worker_loop(99, dummy_rx, bot_rx, metrics_tx, leaderboard, shutdown)
            })
            .expect("failed to spawn bot worker"));
    }

    // Metrics sink thread
    {
        let shutdown = Arc::clone(&shutdown);
        handles.push(std::thread::Builder::new()
            .name("metrics_sink".into())
            .spawn(move || {
                metrics_sink::run(metrics_rx, "logs/threaded_summary.json".into(), shutdown)
            })
            .expect("failed to spawn metrics sink"));
    }

    // ── 7. Ctrl-C handler ─────────────────────────────────────────────────────
    {
        let shutdown_ctrlc = Arc::clone(&shutdown);
        ctrlc::set_handler(move || {
            tracing::info!(target: "shutdown", "ctrl-c received");
            shutdown_ctrlc.store(true, Ordering::Relaxed);
        })
        .expect("failed to set ctrl-c handler");
    }

    tracing::info!(
        target: "startup",
        leaderboard   = %args.leaderboard_impl,
        human_workers = args.human_workers,
        channel_cap   = args.channel_capacity,
        duration_secs = ?args.duration,
        "threaded pipeline running"
    );

    // ── 8. Wait for shutdown ──────────────────────────────────────────────────
    let start = Instant::now();
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        if let Some(secs) = args.duration {
            if start.elapsed().as_secs() >= secs {
                tracing::info!(target: "shutdown", "duration elapsed");
                shutdown.store(true, Ordering::Relaxed);
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // ── 9. Flush ──────────────────────────────────────────────────────────────
    // Wake watchdog so it exits promptly without waiting up to 10 s.
    {
        let (lock, cvar) = &*condvar;
        let mut g = lock.lock().unwrap();
        *g = true;
        cvar.notify_all();
    }

    std::thread::sleep(Duration::from_secs(1));

    let top3 = leaderboard.top_3();
    tracing::info!(target: "final_leaderboard", ?top3);

    for h in handles {
        let _ = h.join();
    }
}
