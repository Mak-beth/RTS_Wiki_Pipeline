"""
Read a pipeline JSONL log and compute p50/p90/p99 latency split by priority.
Saves a bar chart PNG and prints a summary table.

Usage: python scripts/analyze_latency.py --log <path> --out-dir <dir>

Data sources (in priority order):
  1. deadline_miss log events   — contain individual e2e_us measurements
  2. *_summary.json             — HDR histogram aggregate if no live events found
"""

import argparse, json, pathlib
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def percentiles(values, qs=(50, 90, 99)):
    if not values:
        return {q: 0 for q in qs}
    arr = np.array(values, dtype=float)
    return {q: float(np.percentile(arr, q)) for q in qs}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    ap.add_argument("--out-dir", default="reports/figures")
    args = ap.parse_args()

    human_e2e, bot_e2e = [], []

    with open(args.log, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            fields = obj.get("fields", {})
            if obj.get("target") == "deadline_miss":
                e2e = fields.get("e2e_us", 0)
                if fields.get("was_human", False):
                    human_e2e.append(e2e)
                else:
                    bot_e2e.append(e2e)

    # Fall back to summary JSON when no deadline_miss entries are present
    if not human_e2e and not bot_e2e:
        print("No deadline_miss entries — loading summary JSON for aggregate stats.")
        log_path = pathlib.Path(args.log)
        tag = "async" if "async" in log_path.name else "threaded"
        summary_path = log_path.parent / f"{tag}_summary.json"
        if summary_path.exists():
            s = json.loads(summary_path.read_text())
            h = s.get("e2e_latency_human", {})
            b = s.get("e2e_latency_bot",   {})
            print(f"\n{'Metric':<25} {'Human':>10} {'Bot':>10}")
            print("-" * 47)
            for q in ("p50", "p90", "p99"):
                print(f"  e2e {q} (µs)          {h.get(q, 0):>10.0f} {b.get(q, 0):>10.0f}")
            # Build synthetic lists for plotting from summary percentiles
            human_e2e = [h.get("p50", 0)] * 50 + [h.get("p90", 0)] * 9 + [h.get("p99", 0)]
            bot_e2e   = [b.get("p50", 0)] * 50 + [b.get("p90", 0)] * 9 + [b.get("p99", 0)]
        else:
            print(f"Summary file not found at {summary_path}")
            return

    h = percentiles(human_e2e)
    b = percentiles(bot_e2e)

    print(f"\n{'Metric':<25} {'Human':>10} {'Bot':>10}")
    print("-" * 47)
    for q in (50, 90, 99):
        print(f"  e2e p{q} (µs)          {h[q]:>10.1f} {b[q]:>10.1f}")

    fig, ax = plt.subplots(figsize=(8, 5))
    x = np.arange(3)
    width = 0.35
    labels = ["p50", "p90", "p99"]
    h_vals = [h[50], h[90], h[99]]
    b_vals = [b[50], b[90], b[99]]
    ax.bar(x - width / 2, h_vals, width, label="Human", color="#2ecc71")
    ax.bar(x + width / 2, b_vals, width, label="Bot",   color="#3498db")
    ax.axhline(2000, color="red", linestyle="--", label="2ms deadline")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("Latency (µs)")
    tag = "async" if "async" in pathlib.Path(args.log).name else "threaded"
    ax.set_title(f"End-to-End Latency by Priority ({tag})")
    ax.legend()
    fig.tight_layout()
    out = pathlib.Path(args.out_dir) / f"latency_by_priority_{tag}.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=300)
    plt.close(fig)
    print(f"Saved {out}")


if __name__ == "__main__":
    main()
