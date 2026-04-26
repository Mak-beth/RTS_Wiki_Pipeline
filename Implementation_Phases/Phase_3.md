# Phase 3 — Threaded Pipeline (std::thread)

## What Was Built
The second production pipeline, functionally identical to the async pipeline
but implemented using OS threads and blocking primitives. Zero Tokio
dependency. Exists specifically to provide a valid architectural comparison
against the async pipeline.

## Contributions

### Thread Map
```
THREAD: watchdog     — Condvar-based, fires NetworkReset after 10s silence
THREAD: ingestion    — reqwest::blocking, reads SSE line by line
THREAD: dispatcher   — crossbeam channel consumer, priority routing
THREAD: human_worker_0..3  — 4 threads on chan_human
THREAD: bot_worker   — 1 thread on chan_bot (with dummy human receiver)
THREAD: metrics_sink — crossbeam recv_timeout loop, writes summary on exit
```

### Key Architectural Differences from Async Pipeline
| Concern | Async Pipeline | Threaded Pipeline |
|---|---|---|
| HTTP client | reqwest + eventsource-stream | reqwest::blocking |
| Concurrency | tokio::spawn | std::thread::spawn |
| Channels | tokio::sync::mpsc | crossbeam_channel::bounded |
| Priority select | tokio::select! biased | crossbeam select! biased |
| Backpressure ring | VecDeque + Tokio Mutex | crossbeam_queue::ArrayQueue |
| Watchdog | tokio::time::timeout | std::sync::Condvar |
| Shutdown | tokio oneshot channel | AtomicBool + ctrlc crate |

### Real Bug Found and Fixed During This Phase
`reqwest::blocking` requires explicit SSE headers that `eventsource-stream`
handles automatically. Without `Accept: text/event-stream` and
`Cache-Control: no-cache`, Wikimedia's endpoint returns HTTP 200 but never
sends data. The ingestion thread was silently blocking. Fixed by adding
explicit headers to the request builder. Documented in `HONEST_LIMITATIONS.md`.

### Condvar Watchdog
- Ingestion thread calls `condvar.notify_one()` on every received line
  (including SSE heartbeat `:` comment lines) — prevents false reconnects
  on active but momentarily quiet streams
- Watchdog thread calls `condvar.wait_timeout(10s)` — on timeout, sets
  `reconnect_flag: AtomicBool` which ingestion checks between lines

### Priority Enforcement
```rust
select! {
    recv(chan_human) -> msg => { /* process as human */ },
    recv(chan_bot)   -> msg => { /* process as bot   */ },
    default(Duration::from_micros(100)) => continue,
}
```
Bot worker receives a disconnected dummy human receiver so the same
`run_worker_loop` function handles both priority levels.

## Run Results (60-second live run)
- Events processed: ~1739 human + ~876 bot
- OS jitter p99: 894µs
- e2e human p50: 1013µs, p99: 5011µs
- Human deadline misses: 76/1739 (4.4%)
- Processing time p50: 533µs (vs async 2955µs)
- `std::thread::sleep(100µs)` achieves ~533µs actual — much closer to
  the 100µs target than Tokio's timer wheel

## Key Finding Established
The threaded pipeline meets the 2ms deadline 95.6% of the time vs 7.5%
for async. Root cause is Tokio timer resolution, not workload difference.
This is the primary real-time systems finding of the project.

## Files Created
```
crates/threaded_pipeline/src/main.rs
crates/threaded_pipeline/src/ingestion.rs
crates/threaded_pipeline/src/dispatcher.rs
crates/threaded_pipeline/src/workers.rs
crates/threaded_pipeline/src/metrics_sink.rs
crates/threaded_pipeline/src/state.rs
BEHAVIORAL_DIFF.md
HONEST_LIMITATIONS.md (updated)
logs/threaded_run.jsonl
logs/threaded_summary.json
```
