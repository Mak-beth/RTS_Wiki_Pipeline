# Honest Limitations

## Zero-Copy Scope

The SSE client library (`eventsource-stream`) allocates one `String` per event to hold the raw
data field. This allocation is outside our control and is **not** claimed as zero-copy.

**Our claim**: zero *additional* heap allocation during the parsing stage. The `parse_event`
function borrows all string fields directly from the `String` already owned by the SSE layer —
`serde_json` deserialises into `&'a str` lifetimes tied to that buffer. No extra `String`,
`Vec`, or `Box` is created on the hot path. The `TrackingAllocator` audit verifies this
quantitatively.

## OS Jitter Baseline

Deadline-miss events are only meaningful relative to the OS scheduler's own wake-up jitter.
Before reporting any `DeadlineMiss`, both pipelines measure a baseline jitter distribution
(spinning sleeps with no workload). All deadline-miss thresholds are interpreted in the context
of this baseline — a miss smaller than p99 baseline jitter is not a real miss.

## Threaded Pipeline HTTP

The threaded pipeline uses `reqwest::blocking` on a dedicated OS thread. Internally `reqwest::blocking`
still drives an async body via an embedded single-thread Tokio runtime; the OS-thread boundary is at the
`reqwest::blocking::Response::read()` call, which blocks the ingestion thread until bytes arrive from the
network.

Two caveats specific to this design:

1. **SSE headers required.** The blocking client must send `Accept: text/event-stream` and `Cache-Control:
   no-cache`. Without these headers, the Wikimedia EventStream endpoint may return a non-streaming response.
2. **Watchdog signalling.** The watchdog condvar must be notified for *all* received lines (including SSE
   heartbeat `:` comment lines), not just `data:` events. Notifying only on data events causes the watchdog
   to trigger false reconnects on a healthy but momentarily quiet stream.

## Network Fault Simulation

Fault injection uses a controllable local mock server (not OS-level `Disable-NetAdapter`),
which would require administrator privileges and affect the entire machine.
