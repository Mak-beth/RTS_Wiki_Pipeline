// THREAD: ingestion
// Uses reqwest::blocking::Client to read the SSE stream on a dedicated OS thread.
// This blocks on I/O, which reduces throughput vs the async pipeline's
// non-blocking I/O. This is an intentional architectural difference — see
// HONEST_LIMITATIONS.md.

use crate::state::{EVENTS_INGESTED, OVERFLOW_COUNT};
use crossbeam_queue::ArrayQueue;
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

pub struct IngestionConfig {
    pub sse_url:      String,
    pub capacity:     usize,
    pub capture_path: Option<String>,
}

/// Bounded ring buffer backed by a lock-free ArrayQueue.
/// Oldest-drop semantics: force_push evicts the front item when the queue is full.
pub struct BoundedRing {
    queue: Arc<ArrayQueue<(String, Instant)>>,
}

impl BoundedRing {
    pub fn new(capacity: usize) -> Self {
        Self { queue: Arc::new(ArrayQueue::new(capacity)) }
    }

    /// Push item. Returns `true` if the oldest item was evicted to make room.
    pub fn push(&self, item: (String, Instant)) -> bool {
        // force_push: returns Some(evicted) if queue was full, None otherwise.
        self.queue.force_push(item).is_some()
    }

    pub fn pop(&self) -> Option<(String, Instant)> {
        self.queue.pop()
    }
}

/// THREAD: mock_ingestion
///
/// Deterministic mock that replaces the live SSE stream.  Produces events at
/// `events_per_second` using `MockStream` (seed 42) and exits after `duration`
/// or when `shutdown` is set, whichever comes first.
pub fn run_mock(
    ring:               Arc<BoundedRing>,
    events_per_second:  u64,
    duration:           Duration,
    shutdown:           Arc<AtomicBool>,
) {
    let interval = Duration::from_micros(1_000_000 / events_per_second.max(1));
    let mut mock = rts_core::MockStream::new();
    let start    = Instant::now();

    while start.elapsed() < duration && !shutdown.load(Ordering::Relaxed) {
        let json        = mock.next_event();
        let ingest_time = Instant::now();
        EVENTS_INGESTED.fetch_add(1, Ordering::Relaxed);

        let dropped = ring.push((json, ingest_time));
        if dropped {
            let cnt = OVERFLOW_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::warn!(target: "overflow", overflow_count = cnt);
        }

        let target = Instant::now() + interval;
        while Instant::now() < target {
            std::hint::spin_loop();
        }
    }
}

// THREAD: watchdog
// Shares a Mutex<bool> + Condvar with the ingestion thread.
// Ingestion calls notify_one() on each received event.
// Watchdog calls wait_timeout(10s). On timeout: sets reconnect_flag, logs NetworkReset.
pub fn run_watchdog(
    condvar:        Arc<(Mutex<bool>, Condvar)>,
    reconnect_flag: Arc<AtomicBool>,
    shutdown:       Arc<AtomicBool>,
) {
    let (lock, cvar) = &*condvar;
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        let guard = lock.lock().unwrap();
        let (guard, result) = cvar.wait_timeout(guard, Duration::from_secs(10)).unwrap();
        drop(guard);

        if result.timed_out() {
            reconnect_flag.store(true, Ordering::Relaxed);
            tracing::warn!(
                target: "network_reset",
                "watchdog: 10 s without event, triggering reconnect"
            );
        }
    }
}

pub fn run_ingestion(
    config:         IngestionConfig,
    ring:           Arc<BoundedRing>,
    condvar:        Arc<(Mutex<bool>, Condvar)>,
    reconnect_flag: Arc<AtomicBool>,
    shutdown:       Arc<AtomicBool>,
) {
    let client = reqwest::blocking::Client::builder()
        .timeout(None)
        .user_agent("rts_wiki_pipeline/0.1 (assignment; threaded)")
        .build()
        .expect("failed to build blocking reqwest client");

    let mut backoff_secs: u64 = 1;
    let mut captured:     usize = 0;

    'outer: loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        tracing::info!(
            target: "ingestion",
            url = %config.sse_url,
            ring_capacity = config.capacity,
            backoff_secs,
            "connecting (blocking) to SSE stream"
        );

        let response = match client
            .get(&config.sse_url)
            .header("Accept", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .send()
        {
            Ok(r)  => r,
            Err(e) => {
                tracing::error!(target: "ingestion", connect_error = %e);
                std::thread::sleep(Duration::from_secs(backoff_secs));
                backoff_secs = (backoff_secs * 2).min(30);
                continue;
            }
        };

        backoff_secs = 1;
        let reader = BufReader::new(response);

        for line in reader.lines() {
            if shutdown.load(Ordering::Relaxed) {
                break 'outer;
            }

            if reconnect_flag.swap(false, Ordering::Relaxed) {
                tracing::info!(target: "network_reset", reason = "watchdog");
                break; // drop to outer loop and reconnect
            }

            let line = match line {
                Ok(l)  => l,
                Err(_) => break,
            };

            // Signal watchdog on every received line (including SSE heartbeat
            // `:` comments) so a healthy-but-quiet stream doesn't look stale.
            {
                let (lock, cvar) = &*condvar;
                let mut triggered = lock.lock().unwrap();
                *triggered = true;
                cvar.notify_one();
            }

            if !line.starts_with("data:") {
                continue;
            }

            let data = line["data:".len()..].trim().to_owned();
            if data.is_empty() {
                continue;
            }

            let ingest_time = Instant::now();
            EVENTS_INGESTED.fetch_add(1, Ordering::Relaxed);

            let dropped = ring.push((data.clone(), ingest_time));
            if dropped {
                let cnt = OVERFLOW_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
                tracing::warn!(target: "overflow", overflow_count = cnt);
            }

            // Sample capture — append-open per write is fine at SSE rates.
            if captured < 500 {
                if let Some(ref path) = config.capture_path {
                    use std::io::Write;
                    if let Ok(mut f) = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(path)
                    {
                        let _ = writeln!(f, "{}", data);
                        captured += 1;
                    }
                }
            }
        }

        // Brief pause before reconnect
        if !shutdown.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_secs(backoff_secs));
            backoff_secs = (backoff_secs * 2).min(30);
        }
    }
}
