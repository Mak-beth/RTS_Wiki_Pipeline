# Async vs Threaded Pipeline — Behavioural Differences

## Intentional Differences (architectural choices, not defects)

| Dimension | `async_pipeline` | `threaded_pipeline` | Reason |
|-----------|-----------------|---------------------|--------|
| **Concurrency model** | Tokio task-per-role, M:N scheduling | `std::thread` one-thread-per-role, 1:1 OS mapping | Core assignment contrast |
| **HTTP client** | `reqwest` async + `eventsource-stream` (native SSE parser) | `reqwest::blocking` + `BufReader::lines()` manual SSE parser | No Tokio allowed in threaded crate |
| **Watchdog mechanism** | `tokio::time::timeout(10s)` wrapping `stream.next()` | `Condvar::wait_timeout(10s)` + `AtomicBool reconnect_flag` | Async requires cooperative cancellation; threads use OS primitives |
| **Worker channel pattern** | `Arc<tokio::sync::Mutex<mpsc::Receiver>>` (shared single receiver) | `crossbeam_channel::Receiver::clone()` (true MPMC, no lock needed) | Tokio `mpsc::Receiver` is not Clone; crossbeam is |
| **Bot worker dummy receiver** | `tokio::sync::oneshot::channel()` sender dropped → closed channel | `crossbeam_channel::never()` | Tokio has no `never()` equivalent; `never()` prevents false `Err(Disconnected)` in `select!` |
| **Shutdown signal** | `tokio::sync::broadcast` / `CancellationToken` (async-aware) | `Arc<AtomicBool>` polled in tight 100 ms loop | Tokio's cooperative cancellation doesn't transfer to OS threads |
| **Simulated processing delay** | `tokio::time::sleep(100 µs)` (yields to scheduler) | `std::thread::sleep(100 µs)` (blocks OS thread) | Async sleep yields CPU; blocking sleep holds the OS thread — intentional to demonstrate head-of-line blocking |
| **Ring buffer** | `tokio::sync::Mutex<VecDeque>` | `crossbeam_queue::ArrayQueue` (lock-free) | Lock-free ring removes mutex contention on the hot path in the threaded version |
| **Log file** | `logs/async_run.jsonl` | `logs/threaded_run.jsonl` | Independent runs for comparison |
| **Summary file** | `logs/async_summary.json` | `logs/threaded_summary.json` | Independent runs for comparison |

## Timing / Performance Differences (expected, not bugs)

- **Higher p99 latency in threaded pipeline.** `std::thread::sleep(100 µs)` holds an OS thread; Windows scheduler granularity (~1 ms) means the actual sleep is longer than the async equivalent, pushing p99 up.
- **More deadline misses per event count.** The 4 human worker threads share 4 OS scheduler slots. Under load, the scheduler may not wake them within the 2 ms deadline window. The async pipeline's tasks are multiplexed by Tokio and can be rescheduled sub-millisecond.
- **No overflow observed.** The lock-free `ArrayQueue::force_push` is faster than the mutex-guarded `VecDeque`, so the 1024-slot ring rarely fills during normal SSE rates.

## Identical Behaviour (by design)

- Priority routing: human edits always take the fast path; bot edits are dropped in degraded mode.
- Fail-safe / degraded mode thresholds: p99 > 5 000 µs → degraded; p99 < 3 000 µs for 500 samples → normal.
- Deadline-miss threshold: 2 000 µs e2e.
- Leaderboard semantics: `top_3()` returns descending count, ascending name as tiebreaker.
- OS jitter baseline measurement at startup (500 samples, 100 µs target sleep).
- Structured JSON logging via `tracing` with identical `target:` field names for Python analysis.
- `track-alloc` feature gate for `TrackingAllocator`.
