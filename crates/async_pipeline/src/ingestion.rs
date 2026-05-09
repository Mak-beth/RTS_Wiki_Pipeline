use crate::state::{EVENTS_INGESTED, OVERFLOW_COUNT};
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use reqwest::Client;
use std::collections::VecDeque;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

pub struct IngestionConfig {
    pub sse_url:      String,
    pub capacity:     usize,
    pub capture_path: Option<String>,
}

/// Bounded ring buffer shared between the ingestion task (producer) and the
/// dispatcher task (consumer).  Oldest-drop semantics: when the ring is full,
/// the oldest item is discarded to make room for the incoming item.
pub struct BoundedRing {
    inner:    tokio::sync::Mutex<VecDeque<(String, Instant)>>,
    capacity: usize,
}

impl BoundedRing {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner:    tokio::sync::Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    /// Push an item.  Returns `true` if the oldest item was dropped to make room.
    pub async fn push(&self, item: (String, Instant)) -> bool {
        let mut q = self.inner.lock().await;
        let dropped = if q.len() >= self.capacity {
            q.pop_front();
            true
        } else {
            false
        };
        q.push_back(item);
        dropped
    }

    /// Pop the oldest item, or `None` if the ring is empty.
    pub async fn pop(&self) -> Option<(String, Instant)> {
        self.inner.lock().await.pop_front()
    }
}

/// Deterministic mock ingestion — replaces the live SSE stream with a
/// seeded synthetic generator for reproducible benchmark runs.
///
/// Produces events at `events_per_second` and exits cleanly after `duration`.
/// Uses the same ring push / overflow accounting as the live `run` function.
///
/// `silence_window`: if `Some((start, end))`, no events are pushed while the
/// elapsed time is inside `[start, end)`.  After 10 s of silence the mock
/// watchdog fires a `network_reset` log, matching the live-stream behaviour.
pub async fn run_mock(
    ring:               Arc<BoundedRing>,
    events_per_second:  u64,
    duration:           Duration,
    silence_window:     Option<(Duration, Duration)>,
) {
    let interval = Duration::from_micros(1_000_000 / events_per_second.max(1));
    let mut mock = rts_core::MockStream::new();
    let start    = Instant::now();

    // Tracks when we first entered the silence window (for the mock watchdog).
    let mut silence_enter:   Option<Instant> = None;
    let mut watchdog_fired:  bool            = false;

    while start.elapsed() < duration {
        let elapsed = start.elapsed();

        // Check whether we are inside the silence window.
        let silenced = match silence_window {
            Some((s, e)) => elapsed >= s && elapsed < e,
            None         => false,
        };

        if silenced {
            // Record when silence began (first loop iteration inside window).
            let entered = *silence_enter.get_or_insert_with(Instant::now);

            // Mock watchdog: fire once after 10 s of continuous silence.
            if !watchdog_fired && entered.elapsed() >= Duration::from_secs(10) {
                watchdog_fired = true;
                tracing::warn!(
                    target: "network_reset",
                    "mock watchdog: no events for 10 s inside silence window"
                );
            }

            tokio::time::sleep(interval).await;
            continue;
        } else {
            // Reset watchdog state once we leave the silence window.
            silence_enter  = None;
            watchdog_fired = false;
        }

        let json        = mock.next_event();
        let ingest_time = Instant::now();
        EVENTS_INGESTED.fetch_add(1, Ordering::Relaxed);

        let dropped = ring.push((json, ingest_time)).await;
        if dropped {
            let cnt = OVERFLOW_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::warn!(target: "overflow", overflow_count = cnt);
        }

        tokio::time::sleep(interval).await;
    }
}

pub async fn run(config: IngestionConfig, ring: Arc<BoundedRing>) {
    let client = Client::builder()
        .user_agent("rts_wiki_pipeline/0.1 (assignment; contact via github)")
        .build()
        .expect("failed to build reqwest client");

    // Capture file setup — sync BufWriter is acceptable here: writes are tiny
    // and low-frequency (≤ 500 events total).
    let mut capture: Option<std::io::BufWriter<std::fs::File>> =
        config.capture_path.as_deref().map(|p| {
            std::io::BufWriter::new(
                std::fs::File::create(p).expect("failed to create capture file"),
            )
        });
    let mut captured: usize = 0;

    let mut backoff_secs: u64 = 1;

    loop {
        tracing::info!(
            target: "ingestion",
            url = %config.sse_url,
            ring_capacity = config.capacity,
            backoff_secs,
            "connecting to SSE stream"
        );

        let response = match client.get(&config.sse_url).send().await {
            Ok(r)  => r,
            Err(e) => {
                tracing::warn!(target: "ingestion", error = %e, "initial connection failed");
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(30);
                continue;
            }
        };

        let mut stream = response.bytes_stream().eventsource();

        loop {
            match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
                // Good event
                Ok(Some(Ok(event))) => {
                    backoff_secs = 1; // healthy connection — reset backoff

                    if event.data.is_empty() {
                        continue;
                    }

                    // Capture up to 500 raw lines (sync write — see comment above)
                    if captured < 500 {
                        if let Some(ref mut w) = capture {
                            let _ = writeln!(w, "{}", event.data);
                            captured += 1;
                            if captured == 500 {
                                let _ = w.flush();
                                tracing::info!(
                                    target: "capture_complete",
                                    path = ?config.capture_path,
                                    "500 samples captured"
                                );
                            }
                        }
                    }

                    let ingest_time = Instant::now();
                    EVENTS_INGESTED.fetch_add(1, Ordering::Relaxed);

                    let dropped = ring.push((event.data, ingest_time)).await;
                    if dropped {
                        let cnt = OVERFLOW_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
                        tracing::warn!(target: "overflow", overflow_count = cnt);
                    }
                }

                // Stream-level error
                Ok(Some(Err(e))) => {
                    tracing::warn!(target: "ingestion", error = %e, "SSE stream error");
                    break;
                }

                // Stream ended cleanly
                Ok(None) => {
                    tracing::warn!(target: "ingestion", "SSE stream ended");
                    break;
                }

                // 10-second watchdog fired — reconnect
                Err(_timeout) => {
                    tracing::warn!(
                        target: "network_reset",
                        "watchdog timeout after 10s, reconnecting"
                    );
                    break;
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
        backoff_secs = (backoff_secs * 2).min(30);
    }
}
