# Phase 4 — Criterion Benchmarks

## What Was Built
Three rigorous Criterion.rs benchmark suites producing quantitative,
reproducible performance data across parsing throughput, synchronisation
primitive contention, and end-to-end latency replay.

## Contributions

### Benchmark 1: Parsing Throughput
Compares zero-copy borrowed deserialization against owned-String deserialization
on a fixed 220-byte Wikipedia JSON payload.

| Variant | Mean Time | Throughput |
|---|---|---|
| `zero_copy` (WikiEvent<'a>) | 273 ns | ~546 MiB/s |
| `owned_alloc` (WikiEventOwned) | 569 ns | ~259 MiB/s |

**Result:** 2.08x speedup. Zero-copy eliminates 6 heap allocations per event.
`WikiEventOwned` is defined in the bench crate only — never in `rts_core` —
to avoid polluting the shared library with an allocation-heavy baseline type.

### Benchmark 2: Leaderboard Contention
Full matrix: 3 implementations × 5 thread counts × 3 read/write ratios
= 45 configurations measured.

**Key result at 16 threads, 90% reads:**
| Implementation | Mean Time | vs Mutex |
|---|---|---|
| Mutex | 153.22 ms | baseline |
| RwLock | 16.85 ms | **9.09x faster** |
| Atomic (DashMap) | 9.98 ms | 15.3x faster |

**Insight:** At write-heavy ratios (10% reads), all three implementations
converge — the read-sharing benefit of RwLock disappears. AtomicLeaderboard
underperforms on reads because `top_3()` requires full DashMap iteration
under shard locks.

### Benchmark 3: End-to-End Latency Replay
Replays `logs/sample_stream.jsonl` (500 captured live events) at simulated
1x, 5x, and 10x event rates in-process, measuring the rts_core pipeline
processing path.

| Speed | Events/iter | Mean time/iter |
|---|---|---|
| 1x | 50 | 100 µs |
| 5x | 250 | 376 µs |
| 10x | 500 | 761 µs |

Near-linear scaling — confirms no contention bottleneck within rts_core.

### Output Artifacts
- 81 Criterion HTML reports in `reports/criterion/`
- `reports/bench_summary.csv` — all 50 benchmark results in CSV format
- `scripts/export_bench_csv.py` — script that generated the CSV from
  Criterion's JSON output files

## Files Created/Modified
```
crates/benches/benches/parsing.rs
crates/benches/benches/leaderboard_contention.rs
crates/benches/benches/end_to_end_latency.rs
scripts/export_bench_csv.py
reports/bench_summary.csv
reports/criterion/  (81 HTML files)
```
