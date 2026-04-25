# Honest Limitations

## 1. Zero-Copy Scope

The SSE client library (`eventsource-stream`) allocates one `String` per event to hold the raw
data field. This allocation is outside our control and is **not** claimed as zero-copy.

**Our claim**: zero *additional* heap allocation during the parsing stage. The `parse_event`
function borrows all string fields directly from the `String` already owned by the SSE layer —
`serde_json` deserialises into `&'a str` lifetimes tied to that buffer. No extra `String`,
`Vec`, or `Box` is created on the hot path. The `TrackingAllocator` audit verifies this
quantitatively.

## 2. OS Jitter Baseline

Deadline-miss events are only meaningful relative to the OS scheduler's own wake-up jitter.
Before reporting any `DeadlineMiss`, both pipelines measure a baseline jitter distribution
(spinning sleeps with no workload). All deadline-miss thresholds are interpreted in the context
of this baseline — a miss smaller than the p99 baseline jitter is not a real miss.

The jitter measurement calls `measure_os_jitter(500)` at startup and logs the result. On a
typical developer laptop running Windows, p99 OS jitter is in the range of 500 µs–2 ms. Any
deadline-miss analysis in `reports/` should be read with this in mind.

## 3. Threaded Pipeline HTTP

The threaded pipeline uses `reqwest::blocking` on a dedicated OS thread. Internally,
`reqwest::blocking` still drives an async body via an embedded single-thread Tokio runtime; the
OS-thread boundary is at the `reqwest::blocking::Response::read()` call, which blocks the
ingestion thread until bytes arrive from the network.

Two caveats specific to this design:

1. **SSE headers required.** The blocking client must send `Accept: text/event-stream` and
   `Cache-Control: no-cache`. Without these headers the Wikimedia EventStream endpoint may return
   a non-streaming response.
2. **Watchdog signalling.** The watchdog condvar must be notified for *all* received lines
   (including SSE heartbeat `:` comment lines), not just `data:` events. Notifying only on data
   events causes the watchdog to trigger false reconnects on a healthy but momentarily quiet
   stream.

## 4. Allocation Audit — Threaded Pipeline Measurement

The `TrackingAllocator` uses global `AtomicUsize` counters that capture allocations from
**all OS threads**. In the threaded pipeline the dispatcher thread is isolated, but four worker
threads run concurrently and each call `leaderboard.record()` → `HashMap::entry(server_name.to_owned())`,
producing one `String` allocation per processed event. These concurrent allocations pollute the
snapshot delta taken around `parse_event`.

**Measured result (threaded):** 53.8 % zero-allocation. This is a conservative lower bound
caused by cross-thread noise, not evidence of allocations within `parse_event` itself.

**Measured result (async, `current_thread` runtime):** 100.0 % zero-allocation. With Tokio's
`current_thread` flavour, the cooperative scheduler cannot preempt the dispatcher between the two
snapshots, giving a clean, noise-free per-call measurement. The 100 % result is the authoritative
proof of the zero-copy claim.

**Improvement path:** Replace the global `AtomicUsize` counters with thread-local counters in
`TrackingAllocator`. This would give clean per-thread measurement without requiring a
single-threaded runtime. Not implemented in this version due to re-entrancy complexity in
`GlobalAlloc` implementations.

The async pipeline audit achieves 100 % zero-allocation by running under a `current_thread`
Tokio runtime during measurement. This serialises all tasks, eliminating cross-task counter
noise. The production pipeline uses a multi-thread runtime. The audit proves that `parse_event`
itself makes zero heap allocations — it does not prove the production multi-threaded runtime has
zero allocations globally.

## 5. Benchmark Reproducibility

Criterion benchmarks are run on a developer laptop under a live OS load. Results reflect the
hardware and OS scheduler of that machine. Reported throughput numbers (parsing, leaderboard
contention, e2e replay) should be treated as relative comparisons between implementations, not
absolute performance guarantees.

Key sources of variance:
- CPU frequency scaling (Turbo Boost / power management)
- Background system processes contending for cores
- Memory bandwidth shared with other applications
- Criterion's warm-up period may be insufficient for very short benchmarks (<1 µs)

## 6. Network Fault Simulation

The reconnection / watchdog behaviour is tested against a live stream, not a controllable fault
injector. True network-fault simulation (e.g., cutting the connection mid-stream, injecting
malformed SSE payloads, or simulating latency spikes) would require a local mock server or
OS-level traffic shaping (`tc netem` on Linux). On Windows this would require administrator
privileges and would affect the entire machine's network stack.

The watchdog timeout (currently 30 s) is verified only by waiting for a naturally quiet stream
period. The reconnection code path is covered by the integration structure but not by a
reproducible automated fault injection test.
