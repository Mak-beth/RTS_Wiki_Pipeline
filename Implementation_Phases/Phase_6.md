# Phase 6 — Analysis Scripts & Visualisations

## What Was Built
Six Python scripts that consume the pipeline JSONL logs and benchmark
CSV outputs to produce all figures used in the research report.
12 PNG plots generated at 300 DPI with colorblind-friendly palettes.

## Contributions

### Script 1: analyze_latency.py
Reads deadline_miss entries from JSONL logs, computes p50/p90/p99
split by human/bot priority, and renders a grouped bar chart with
a 2ms deadline reference line.

Outputs: `latency_by_priority_async.png`, `latency_by_priority_threaded.png`

### Script 2: plot_drift.py
Overlaid histogram of scheduling drift values for human vs bot events.
Vertical red line at 2ms deadline. Title includes deadline-miss counts
for each priority class.

Outputs: `drift_histogram_async.png`, `drift_histogram_threaded.png`

### Script 3: plot_overflow.py
Time-series bar chart of overflow events per second. Handles the case
where no overflows occurred (renders an annotated empty plot rather
than crashing) — important because a well-tuned system should have
few or zero overflows.

Outputs: `overflow_timeline_async.png`, `overflow_timeline_threaded.png`

### Script 4: plot_failsafe.py
Rolling p99 latency time series (100-sample window). Overlays shaded
red regions for degraded-mode periods and vertical lines at
ModeTransition events. Threshold lines at 2ms (deadline), 3ms
(recovery), and 5ms (degraded entry).

Outputs: `failsafe_timeline_async.png`, `failsafe_timeline_threaded.png`

### Script 5: compare_architectures.py
Side-by-side bar charts comparing async vs threaded for all six histogram
metrics (e2e latency and processing time, human and bot, p50/p90/p99).
Prints a summary table to stdout.

Output: `reports/figures/arch_comparison.png`

**Summary table produced:**
```
Metric                    Async p50  Async p99  Threaded p50  Threaded p99
E2E Latency (Human)           5547      29919          1013          5011
E2E Latency (Bot)             9367     113855           983         70527
Processing Time (Human)       2955      16135           533          1029
Processing Time (Bot)         4263      16167           534           999
```

### Script 6: export_bench_csv.py
Walks `target/criterion/` output tree, extracts mean_ns from each
`estimates.json`, and writes `reports/bench_summary.csv` with 50 rows.

## All 12 PNG Outputs
| File | Size |
|---|---|
| allocation_histogram_async.png | 89 KB |
| allocation_histogram_threaded.png | 129 KB |
| arch_comparison.png | 308 KB |
| drift_histogram_async.png | 104 KB |
| drift_histogram_threaded.png | 93 KB |
| failsafe_timeline_async.png | 152 KB |
| failsafe_timeline_threaded.png | 134 KB |
| latency_by_priority_async.png | 75 KB |
| latency_by_priority_threaded.png | 82 KB |
| overflow_timeline_async.png | 75 KB |
| overflow_timeline_threaded.png | 75 KB |

## Files Created
```
scripts/analyze_latency.py
scripts/plot_drift.py
scripts/plot_overflow.py
scripts/plot_failsafe.py
scripts/compare_architectures.py
scripts/export_bench_csv.py
reports/figures/  (12 PNG files)
requirements.txt
```
