"""
Analyse alloc_audit entries from a pipeline JSONL log.
Reports the percentage of parse calls with zero additional heap allocations
during the parsing stage — the quantitative proof of the zero-copy claim.

Usage:
    python scripts/allocation_audit.py --log <path> --out <path>

IMPORTANT: The claim being tested is
    "zero additional heap allocations during the parsing stage"
NOT "zero-copy end-to-end". The SSE library allocates a String per event.
That allocation is not measured here and is outside our control.

Measurement note (async_pipeline):
    The async pipeline audit is run with the Tokio current_thread runtime so
    that no other task can preempt the dispatcher between the two snapshots.
    This eliminates cross-thread allocator noise from concurrent ingestion and
    worker tasks and gives a clean per-call measurement of parse_event.

Measurement note (threaded_pipeline):
    The threaded pipeline uses dedicated OS threads. The dispatcher thread is
    isolated, but the global TrackingAllocator captures allocations from ALL
    threads. Worker threads call leaderboard.record() -> server_name.to_owned()
    on every event, creating concurrent allocation noise. The threaded result
    is therefore a conservative lower bound, not a clean isolation measurement.
"""

import argparse, json, pathlib, collections
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--fig", default="reports/figures/allocation_histogram.png",
                        help="Path for the output histogram PNG")
    args = parser.parse_args()

    alloc_counts = []
    alloc_bytes  = []

    with open(args.log, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("target") != "alloc_audit":
                continue
            fields = obj.get("fields", obj)
            alloc_counts.append(int(fields.get("allocs", fields.get("alloc_count", 0))))
            alloc_bytes.append(int(fields.get("bytes", fields.get("alloc_bytes", 0))))

    if not alloc_counts:
        print("ERROR: No alloc_audit entries found in log.")
        print("Ensure the pipeline was built with --features track-alloc")
        return

    total       = len(alloc_counts)
    zero_alloc  = sum(1 for c in alloc_counts if c == 0)
    zero_pct    = 100.0 * zero_alloc / total
    mean_allocs = sum(alloc_counts) / total
    max_allocs  = max(alloc_counts)
    mean_bytes  = sum(alloc_bytes) / total

    summary_lines = [
        "=== Allocation Audit Report ===",
        "",
        "Claim under test:",
        "  'Zero additional heap allocations during the parsing stage'",
        "  (The SSE library's initial String allocation is excluded — it is",
        "   outside the zero-copy boundary and is not counted here.)",
        "",
        f"Total parse calls measured : {total}",
        f"Calls with 0 allocs        : {zero_alloc}",
        f"Zero-allocation percentage : {zero_pct:.1f}%",
        f"Mean allocs per parse      : {mean_allocs:.3f}",
        f"Max allocs in one parse    : {max_allocs}",
        f"Mean bytes allocated       : {mean_bytes:.1f}",
        "",
    ]

    if zero_pct >= 95.0:
        summary_lines.append(f"RESULT: PASS ({zero_pct:.1f}% >= 95% threshold)")
        summary_lines.append("The zero-copy parsing claim is quantitatively supported.")
    else:
        summary_lines.append(f"RESULT: BELOW THRESHOLD ({zero_pct:.1f}% < 95%)")
        summary_lines.append("Investigate allocations in the hot path.")
        summary_lines.append(
            "For the threaded pipeline this is expected: worker threads allocate\n"
            "concurrently (server_name.to_owned() in leaderboard.record()), polluting\n"
            "the global AtomicUsize counters. See HONEST_LIMITATIONS.md."
        )

    summary_text = "\n".join(summary_lines)
    print(summary_text)

    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(summary_text, encoding="utf-8")

    # Histogram plot
    counter = collections.Counter(alloc_counts)
    # Cap display at allocs<=20 and lump the rest into a ">20" bucket
    MAX_SHOW = 20
    keys_raw  = sorted(counter.keys())
    gt_count  = sum(counter[k] for k in keys_raw if k > MAX_SHOW)
    keys      = [k for k in keys_raw if k <= MAX_SHOW]
    values    = [counter[k] for k in keys]
    if gt_count:
        keys.append(f">{MAX_SHOW}")
        values.append(gt_count)

    fig, ax = plt.subplots(figsize=(9, 5))
    colors = ["#2ecc71" if k == 0 else "#e74c3c" for k in (keys if gt_count == 0 else keys[:-1] + ["#e74c3c"])]
    # fix color list length
    colors = ["#2ecc71" if (isinstance(k, int) and k == 0) else "#e74c3c" for k in keys]
    bars = ax.bar([str(k) for k in keys], values, color=colors)
    ax.set_xlabel("Heap Allocations per Parse Call", fontsize=12)
    ax.set_ylabel("Count", fontsize=12)
    ax.set_title(
        f"Parsing-Stage Heap Allocations\n"
        f"{zero_pct:.1f}% zero-allocation  |  n={total} samples",
        fontsize=13,
    )
    ax.bar_label(bars, padding=3, fontsize=9)
    fig.tight_layout()
    fig_path = pathlib.Path(args.fig)
    fig_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(fig_path, dpi=300)
    plt.close(fig)
    print(f"Histogram saved to {fig_path}")


if __name__ == "__main__":
    main()
