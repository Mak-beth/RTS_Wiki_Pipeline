# Phase 2 — Async Pipeline (Tokio)

## What Was Built
The first of two production pipelines. A fully async, non-blocking event
processing system using the Tokio runtime, consuming the live Wikipedia
SSE stream and enforcing real-time constraints under cooperative scheduling.

## Contributions

### Five-Stage Async Architecture
```
[SSE Ingestion] → BoundedRing → [Dispatcher] → chan_human → [Human Workers x4]
                                              → chan_bot   → [Bot Worker x1]
                                                          → chan_metrics → [Metrics Sink]
```

### Ingestion Task
- Connects to `https://stream.wikimedia.org/v2/stream/recentchange` via
  `reqwest` streaming and `eventsource-stream`
- Custom `BoundedRing` (Tokio Mutex + VecDeque) implements oldest-packet-drop
  backpressure: when full, drops the oldest item and logs a timestamped
  `OverflowEvent`
- Watchdog: `tokio::time::timeout(10s)` wrapping each receive — on timeout,
  logs `NetworkReset` and reconnects with exponential backoff (1s → 2s → 4s
  → max 30s)
- `--capture-sample` flag: writes the first 500 raw SSE events to a file
  for use in Phase 4 benchmarks

### Dispatcher Task
- Dequeues from `BoundedRing`, calls `parse_event` to determine priority
- Routes owned `String` to `chan_human` or `chan_bot`
- In DEGRADED mode, drops bot events before routing
- Allocation audit hooks (`#[cfg(feature = "track-alloc")]`) wrap the
  `parse_event` call to measure parsing-stage heap allocations

### Worker Pool
- 4 Tokio tasks on `chan_human`, 1 on `chan_bot`
- Biased channel selection enforces human-first priority
- 100µs simulated processing via `tokio::time::sleep`
- 2ms deadline check: if `complete_time - ingest_time > 2ms`, logs
  `DeadlineMiss { e2e_us, was_human }`
- Updates shared leaderboard with `event.server_name`

### Metrics Sink
- Accumulates `LatencySample` events into `HistogramAggregator`
- Maintains rolling window of 1000 e2e values for fail-safe logic
- On shutdown: flushes p50/p90/p99 summary to `logs/async_summary.json`

### Fail-Safe / Degraded Mode
- Rolling p99 > 5ms → sets global `DEGRADED: AtomicBool`, logs
  `ModeTransition { to: "degraded" }`
- Degraded mode: bot events dropped at dispatcher, leaderboard updates skipped
- Recovery: rolling p99 < 3ms for 500 consecutive samples →
  `ModeTransition { to: "normal" }`
- Hysteresis (different enter/exit thresholds) prevents oscillation

### OS Jitter Context
- Logs `OsJitterReport` at startup
- Inline comment in source: deadline misses below OS jitter p99 are
  physically unachievable on a general-purpose kernel

### CLI
```
--sse-url             (default: Wikipedia stream)
--duration            (default: run until Ctrl-C)
--leaderboard-impl    mutex | rwlock | atomic
--channel-capacity    (default: 1024)
--human-workers       (default: 4)
--capture-sample      path to write 500 raw events
```

## Run Results (60-second live run)
- Events processed: ~2000 human + ~400 bot
- OS jitter p99: 1200µs
- e2e human p50: 5547µs, p99: 29919µs
- Human deadline misses: 1495/1616 (92.5%)
- Root cause: `tokio::time::sleep(100µs)` uses ~1ms timer wheel resolution,
  making 100µs processing take ~3ms in practice

## Files Created
```
crates/async_pipeline/src/main.rs
crates/async_pipeline/src/ingestion.rs
crates/async_pipeline/src/dispatcher.rs
crates/async_pipeline/src/workers.rs
crates/async_pipeline/src/metrics_sink.rs
crates/async_pipeline/src/state.rs
logs/sample_stream.jsonl  (500 captured events)
```
