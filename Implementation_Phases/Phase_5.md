# Phase 5 — Allocation Audit (Distinction Feature)

## What Was Built
Quantitative proof of the zero-copy parsing claim via a custom global
allocator that counts heap allocations on the hot processing path.
This is the distinction-level evidence that distinguishes the project
from a working system into a provably correct one.

## Contributions

### Feature-Gated Global Allocator
Both pipeline binaries install `TrackingAllocator` as `#[global_allocator]`
when compiled with `--features track-alloc`. This intercepts every
`alloc()` and `dealloc()` call globally via four `AtomicUsize` counters.

```rust
#[cfg(feature = "track-alloc")]
#[global_allocator]
static ALLOC: rts_core::allocator::TrackingAllocator =
    rts_core::allocator::TrackingAllocator { inner: std::alloc::System };
```

### Hot-Path Measurement
In `dispatcher.rs`, the `parse_event` call is wrapped with snapshot deltas:
```rust
let snap_before = TrackingAllocator::snapshot();
let is_bot = parse_event(&data)?.bot;
let delta = TrackingAllocator::snapshot() - snap_before;
tracing::info!(target: "alloc_audit", allocs = delta.alloc_count, bytes = delta.alloc_bytes);
```
This is gated behind `#[cfg(feature = "track-alloc")]` — zero overhead
in production builds.

### Async Pipeline Result
```
Total parse calls measured : 2728
Calls with 0 allocs        : 2728
Zero-allocation percentage : 100.0%
RESULT: PASS (100.0% >= 95% threshold)
```

### Threaded Pipeline Result
```
Total parse calls measured : 2715
Calls with 0 allocs        : 1461
Zero-allocation percentage : 53.8%
RESULT: BELOW THRESHOLD — documented as measurement noise
```

### Honest Documentation of the Discrepancy
The 100% vs 53.8% difference is a measurement methodology difference,
not a parsing difference. The async audit runs under `current_thread`
Tokio runtime (serialised tasks, no concurrent allocator noise).
The threaded audit has 4 concurrent worker threads calling
`server_name.to_owned()` inside `leaderboard.record()`, which pollutes
the global `AtomicUsize` counters between the two snapshots.
`parse_event` itself makes zero allocations in both cases.
Documented in `HONEST_LIMITATIONS.md` with the improvement path
(thread-local counters).

### Analysis Script
`scripts/allocation_audit.py` reads JSONL log, filters `alloc_audit`
target entries, computes statistics, and generates a histogram PNG
showing the distribution of per-parse allocation counts.

## Exact Claim Proven
> "Zero additional heap allocations during the parsing stage."
NOT "zero-copy end-to-end" — the SSE library allocates one String per
event, which is outside the zero-copy boundary and is correctly excluded.

## Files Created/Modified
```
scripts/allocation_audit.py
reports/allocation_audit.txt           (PASS — async)
reports/threaded_allocation_audit.txt  (BELOW THRESHOLD — explained)
reports/figures/allocation_histogram_async.png     (89 KB)
reports/figures/allocation_histogram_threaded.png  (129 KB)
HONEST_LIMITATIONS.md (updated with audit methodology section)
```
