"""
Plot scheduling drift histogram (human vs bot), with 2ms deadline marker.
Drift is taken from deadline_miss log entries (e2e_us field).
If no deadline_miss events, falls back to summary JSON p-values.

Usage: python scripts/plot_drift.py --log <path>
"""

import argparse, json, pathlib
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    args = ap.parse_args()

    human_drift, bot_drift = [], []
    deadline_miss_human = deadline_miss_bot = 0

    with open(args.log, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("target") != "deadline_miss":
                continue
            fields = obj.get("fields", {})
            e2e = fields.get("e2e_us", 0)
            if fields.get("was_human", False):
                human_drift.append(e2e)
                if e2e > 2000:
                    deadline_miss_human += 1
            else:
                bot_drift.append(e2e)
                if e2e > 2000:
                    deadline_miss_bot += 1

    # Fall back to summary JSON when no deadline_miss entries are present
    if not human_drift and not bot_drift:
        print("No deadline_miss entries — loading summary JSON for drift data.")
        log_path = pathlib.Path(args.log)
        tag = "async" if "async" in log_path.name else "threaded"
        summary_path = log_path.parent / f"{tag}_summary.json"
        if summary_path.exists():
            s = json.loads(summary_path.read_text())
            # Reconstruct synthetic samples from e2e histogram percentiles
            def synthetic(node):
                p50, p90, p99 = node.get("p50",0), node.get("p90",0), node.get("p99",0)
                cnt = node.get("count", 0)
                if cnt == 0:
                    return []
                samples = (
                    [p50] * int(cnt * 0.50) +
                    [p90] * int(cnt * 0.40) +
                    [p99] * int(cnt * 0.09) +
                    [node.get("max", p99)] * max(1, int(cnt * 0.01))
                )
                return samples
            human_drift = synthetic(s.get("e2e_latency_human", {}))
            bot_drift   = synthetic(s.get("e2e_latency_bot",   {}))
            deadline_miss_human = sum(1 for v in human_drift if v > 2000)
            deadline_miss_bot   = sum(1 for v in bot_drift   if v > 2000)
            print(f"Loaded {len(human_drift)} human, {len(bot_drift)} bot synthetic samples.")
        else:
            print(f"No summary file at {summary_path}; cannot plot.")
            return

    tag = "async" if "async" in pathlib.Path(args.log).name else "threaded"
    all_vals = human_drift + bot_drift
    upper = max(max(all_vals, default=4000), 4000)
    bins = np.linspace(0, upper, 60)

    fig, ax = plt.subplots(figsize=(9, 5))
    if human_drift:
        ax.hist(human_drift, bins=bins, alpha=0.6, label="Human",
                color="#2ecc71", edgecolor="white")
    if bot_drift:
        ax.hist(bot_drift, bins=bins, alpha=0.6, label="Bot",
                color="#3498db", edgecolor="white")
    ax.axvline(2000, color="red", linestyle="--", linewidth=1.5,
               label="2ms deadline")
    ax.set_xlabel("End-to-End Latency / Drift (µs)")
    ax.set_ylabel("Count")
    ax.set_title(
        f"Scheduling Drift — Human vs Bot ({tag})\n"
        f"Deadline misses: human={deadline_miss_human}, bot={deadline_miss_bot}"
    )
    ax.legend()
    fig.tight_layout()
    out = pathlib.Path("reports/figures") / f"drift_histogram_{tag}.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=300)
    plt.close(fig)
    print(f"Saved {out}")


if __name__ == "__main__":
    main()
