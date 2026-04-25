"""
Side-by-side comparison of async vs threaded pipeline latency statistics.

Usage:
    python scripts/compare_architectures.py \
        --async-summary logs/async_summary.json \
        --threaded-summary logs/threaded_summary.json
"""

import argparse, json, pathlib
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def extract(summary, key):
    node = summary.get(key, {})
    return {
        "p50": node.get("p50", 0),
        "p90": node.get("p90", 0),
        "p99": node.get("p99", 0),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--async-summary",    required=True)
    ap.add_argument("--threaded-summary", required=True)
    args = ap.parse_args()

    async_s    = json.loads(pathlib.Path(args.async_summary).read_text())
    threaded_s = json.loads(pathlib.Path(args.threaded_summary).read_text())

    metrics = [
        ("e2e_latency_human",     "E2E Latency\n(Human)"),
        ("e2e_latency_bot",       "E2E Latency\n(Bot)"),
        ("drift_human",           "Scheduling Drift\n(Human)"),
        ("drift_bot",             "Scheduling Drift\n(Bot)"),
        ("processing_time_human", "Processing Time\n(Human)"),
        ("processing_time_bot",   "Processing Time\n(Bot)"),
    ]

    percentile_labels = ["p50", "p90", "p99"]
    x     = np.arange(len(percentile_labels))
    width = 0.35

    fig, axes = plt.subplots(2, 3, figsize=(15, 8))
    axes = axes.flatten()
    color_async    = "#2ecc71"
    color_threaded = "#3498db"

    for i, (key, label) in enumerate(metrics):
        ax = axes[i]
        a = extract(async_s, key)
        t = extract(threaded_s, key)
        a_vals = [a[p] for p in percentile_labels]
        t_vals = [t[p] for p in percentile_labels]
        bars_a = ax.bar(x - width / 2, a_vals, width,
                        label="Async",    color=color_async)
        bars_t = ax.bar(x + width / 2, t_vals, width,
                        label="Threaded", color=color_threaded)
        ax.set_xticks(x)
        ax.set_xticklabels(percentile_labels)
        ax.set_title(label, fontsize=11)
        ax.set_ylabel("µs")
        ax.legend(fontsize=8)
        ax.axhline(2000, color="red", linestyle="--", linewidth=0.8)
        # Annotate bars with values if non-zero
        for bar in list(bars_a) + list(bars_t):
            h = bar.get_height()
            if h > 0:
                ax.annotate(f"{h:.0f}",
                            xy=(bar.get_x() + bar.get_width() / 2, h),
                            xytext=(0, 2), textcoords="offset points",
                            ha="center", va="bottom", fontsize=7)

    fig.suptitle("Async vs Threaded Pipeline — Latency Comparison", fontsize=14)
    fig.tight_layout()
    out = pathlib.Path("reports/figures/arch_comparison.png")
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=300)
    plt.close(fig)
    print(f"Saved {out}")

    # Print summary table to stdout
    print(f"\n{'Metric':<30} {'Async p50':>10} {'Async p99':>10} "
          f"{'Threaded p50':>13} {'Threaded p99':>13}")
    print("-" * 80)
    for key, label in metrics:
        a = extract(async_s, key)
        t = extract(threaded_s, key)
        short = label.replace("\n", " ")
        print(f"  {short:<28} {a['p50']:>10.0f} {a['p99']:>10.0f} "
              f"{t['p50']:>13.0f} {t['p99']:>13.0f}")


if __name__ == "__main__":
    main()
