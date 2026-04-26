# Phase 7 — Documentation

## What Was Built
Complete project documentation: inline Rust doc comments on all public
API items, an honest limitations document, and a full README with
architecture diagram, grading criterion map, and reproduction instructions.

## Contributions

### Rust API Documentation
Added `///` doc comments to every public item in `rts_core`:
- `WikiEvent<'a>` — struct and each field with lifetime explanation
- `Priority` — enum and each variant
- `ParseError` — each variant with error context
- `parse_event` — full `# Errors` section, zero-copy guarantee stated
- `TrackingAllocator` — usage example, measurement limitation noted
- `AllocSnapshot` — field-level docs, `Sub` implementation explained
- `OsJitterReport` — each field with interpretation note
- `measure_os_jitter` — parameters, return value, isolation guarantee
- `Leaderboard` trait — each method contract documented
- All three leaderboard structs — benchmark figures cited in docs
- `LatencySample` — each timestamp field's role in the pipeline
- `HistogramAggregator` — `record()` derivation, `emit_summary()` units

Result: `cargo doc --workspace --no-deps` — **zero warnings**.

### HONEST_LIMITATIONS.md
Six documented limitations with explanations and improvement paths:
1. Threaded pipeline HTTP trade-offs (blocking I/O vs non-blocking)
2. Zero-copy scope (parsing-stage only, not end-to-end)
3. Allocation audit methodology difference between pipelines
4. Scheduling drift as a simplified proxy
5. Tokio timer resolution and sub-millisecond deadlines
6. OS scheduling jitter as a hard floor on deadline achievability

### README.md
Complete rewrite with:
- Project overview and what it demonstrates
- Mermaid architecture diagram showing both pipelines and shared rts_core
- Prerequisites and build instructions
- Full command reference for all run modes
- Output reference table (12 files documented)
- Grading criterion map — 7 rows mapping each assessment criterion
  to the specific file or command that provides evidence

## Files Created/Modified
```
crates/rts_core/src/event.rs        (/// comments added)
crates/rts_core/src/parser.rs       (/// comments added)
crates/rts_core/src/allocator.rs    (/// comments added)
crates/rts_core/src/leaderboard.rs  (/// comments added)
crates/rts_core/src/metrics.rs      (/// comments added)
crates/rts_core/src/jitter_baseline.rs (/// comments added)
HONEST_LIMITATIONS.md               (complete rewrite, 6 sections)
README.md                           (complete rewrite)
```
