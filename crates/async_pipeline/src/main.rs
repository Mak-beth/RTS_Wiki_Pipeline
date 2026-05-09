mod dispatcher;
mod ingestion;
mod metrics_sink;
mod state;
mod workers;

use clap::Parser;
use rts_core::{AtomicLeaderboard, Leaderboard, MutexLeaderboard, RwLockLeaderboard};
use std::sync::Arc;
use std::time::{Duration, Instant};

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

    /// Run with a deterministic mock stream instead of the live Wikipedia SSE
    /// stream.  Produces 2000 events/second with seed 42 — identical across runs.
    #[arg(long)]
    mock: bool,

    /// Run a scripted 60-second demonstration that exercises all four pipeline
    /// phases: baseline → latency injection → stream silence → recovery.
    /// Implies `--mock` and overrides `--duration` to 60 s.
    #[arg(long)]
    demo: bool,
}

// With track-alloc, use a single-thread runtime so the cooperative scheduler
// never preempts the dispatcher between snap_before and snap_after. This
// eliminates cross-thread allocator noise from concurrent ingestion/worker tasks,
// giving a clean per-call measurement of parse_event allocations.
#[cfg_attr(not(feature = "track-alloc"), tokio::main)]
#[cfg_attr(feature = "track-alloc", tokio::main(flavor = "current_thread"))]
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
        tokio::sync::mpsc::channel::<(String, std::time::Instant, std::time::Instant)>(args.channel_capacity);
    let (chan_bot_tx, chan_bot_rx) =
        tokio::sync::mpsc::channel::<(String, std::time::Instant, std::time::Instant)>(args.channel_capacity);
    let (chan_metrics_tx, chan_metrics_rx) =
        tokio::sync::mpsc::channel::<rts_core::LatencySample>(4096);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Human workers share a single receiver via a Mutex.
    let chan_human_shared =
        Arc::new(tokio::sync::Mutex::new(chan_human_rx));

    // ── 5. Demo / mock config ─────────────────────────────────────────────────
    // `--demo` implies `--mock` and overrides the run duration to 60 s.
    let program_start = Instant::now();
    let (run_mock, run_duration) = if args.demo {
        (true, Duration::from_secs(60))
    } else {
        (args.mock, Duration::from_secs(args.duration.unwrap_or(60)))
    };

    let stress = if args.demo {
        state::StressConfig::demo(program_start)
    } else {
        state::StressConfig::disabled()
    };

    // Silence window for Phase 3 (25 s – 38 s) — only active in demo mode.
    let silence_window: Option<(Duration, Duration)> = if args.demo {
        Some((Duration::from_secs(25), Duration::from_secs(38)))
    } else {
        None
    };

    // ── 6. Spawn tasks ────────────────────────────────────────────────────────
    if run_mock {
        let eps: u64 = 2000;
        eprintln!("[ingestion] mode = {} ({eps} eps)",
                  if args.demo { "demo" } else { "mock" });
        tokio::spawn(ingestion::run_mock(
            Arc::clone(&ring), eps, run_duration, silence_window,
        ));
    } else {
        eprintln!("[ingestion] mode = live (stream rate)");
        tokio::spawn(ingestion::run(
            ingestion::IngestionConfig {
                sse_url:      args.sse_url.clone(),
                capacity:     args.channel_capacity,
                capture_path: args.capture_sample.clone(),
            },
            Arc::clone(&ring),
        ));
    }

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
            Arc::clone(&stress),
        ));
    }

    tokio::spawn(workers::run_bot_worker(
        chan_bot_rx,
        chan_metrics_tx.clone(),
        Arc::clone(&leaderboard),
        Arc::clone(&stress),
    ));

    // ── 7. Demo banner task ───────────────────────────────────────────────────
    if args.demo {
        let ts = tokio::time::Instant::now();
        tokio::spawn(async move {
            let banners: &[(u64, &str)] = &[
                (0,  "[demo] Phase 1 ( 0-15s): baseline 2000 eps — expect NORMAL"),
                (15, "[demo] Phase 2 (15-25s): injecting 3 ms spin every 20 packets — expect DEGRADED"),
                (25, "[demo] Phase 3 (25-38s): stream silenced — mock watchdog fires at ~35s"),
                (38, "[demo] Phase 4 (38-60s): stream resumed — expect RECOVERY → NORMAL"),
            ];
            for &(secs, msg) in banners {
                tokio::time::sleep_until(ts + Duration::from_secs(secs)).await;
                eprintln!("{msg}");
            }
        });
    }

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
        demo           = args.demo,
        mock           = run_mock,
        duration_secs  = run_duration.as_secs(),
        "pipeline running"
    );

    // ── 8. Wait for shutdown ──────────────────────────────────────────────────
    if run_mock || args.duration.is_some() {
        tokio::select! {
            _ = tokio::time::sleep(run_duration) => {
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
