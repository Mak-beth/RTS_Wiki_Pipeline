# Phase 1 — Core Types, Zero-Copy Parser & Shared Infrastructure (rts_core)

## What Was Built
The shared library crate that both pipelines depend on entirely.
Every type, parser, allocator, metric, and leaderboard used in the
project lives here. This phase established the technical foundation
that distinction-level features depend on.

## Contributions

### Zero-Copy Event Parsing
- Defined `WikiEvent<'a>` with `#[serde(borrow)]` on all five string
  fields (`user`, `server_name`, `wiki`, `title`, `event_type`)
- No `String`, `Vec`, or `Cow` in the struct — all fields are `&'a str`
  borrowing directly from the source JSON buffer
- Implemented `parse_event<'a>(buf: &'a str) -> Result<WikiEvent<'a>, ParseError>`
  using `serde_json::from_str` for borrowed deserialization
- Written a pointer-address unit test that proves each returned `&str`
  field's memory address falls within the bounds of the source buffer —
  quantitative proof of the zero-copy claim

### Priority Scheduling Foundation
- Defined `enum Priority { Human, Bot }`
- Implemented `From<&WikiEvent<'_>> for Priority` based on the `bot` field
- This enum drives all priority routing decisions in both pipelines

### Custom Tracking Allocator
- Implemented `TrackingAllocator` wrapping `std::alloc::System`
- Four global `AtomicUsize` counters track allocations and deallocations
- `AllocSnapshot` struct with `Sub` implementation for delta measurements
- `reset()` and `snapshot()` public API for wrapping hot-path calls
- Entire module gated behind `#[cfg(feature = "track-alloc")]` — zero
  overhead in production builds

### HDR Histogram Metrics
- `LatencySample` struct capturing five `Instant` timestamps per event
- `HistogramAggregator` with six separate `hdrhistogram::Histogram<u64>`
  instances: e2e latency, scheduling drift, and processing time, each
  split by human/bot priority
- `emit_summary()` outputs JSON with p50, p90, p99, p99.9, max, and count
  for every histogram

### Three Leaderboard Implementations
- Common `Leaderboard` trait: `record(&str)` and `top_3() -> Vec<(String, u64)>`
- `MutexLeaderboard` — `parking_lot::Mutex<HashMap<String, u64>>`
- `RwLockLeaderboard` — `parking_lot::RwLock<HashMap<String, u64>>`
- `AtomicLeaderboard` — `DashMap<String, AtomicU64>` for lock-free writes
- Deterministic `top_3` ordering via secondary sort on domain name as tiebreaker
- 8-thread concurrency test confirms all three implementations produce
  identical results under contention

### OS Jitter Baseline
- `measure_os_jitter(samples: u32) -> OsJitterReport`
- Spawns a dedicated thread, sleeps 100µs per iteration, records the
  overshoot (actual - requested) into an HDR histogram
- Produces p50/p90/p99/max of OS scheduling jitter
- Both pipelines log this at startup — provides the physical floor below
  which deadline misses are OS noise, not pipeline failures

## Bug Fixes Applied
- Removed `#[global_allocator]` from `allocator.rs` (would cause duplicate
  declaration when pipeline binaries install their own instance in Phase 5)
- Changed `timestamp: i64` to `timestamp: Option<i64>` to handle Wikipedia
  SSE events where the timestamp field is absent or non-integer

## Test Results
```
test parser::tests::parse_borrows_from_source       ok
test parser::tests::parse_correct_values            ok
test parser::tests::priority_from_bot_false         ok
test parser::tests::priority_from_bot_true          ok
test parser::tests::parse_error_on_invalid_json     ok
test metrics::tests::percentile_ordering_and_count  ok
test metrics::tests::bot_samples_do_not_pollute_human_histogram ok
test leaderboard::tests::all_three_implementations_agree ok
test jitter_baseline::tests::jitter_report_is_sane  ok

9 passed; 0 failed
```

## Files Created
```
crates/rts_core/src/event.rs
crates/rts_core/src/parser.rs
crates/rts_core/src/allocator.rs
crates/rts_core/src/metrics.rs
crates/rts_core/src/leaderboard.rs
crates/rts_core/src/jitter_baseline.rs
crates/rts_core/src/lib.rs
```
