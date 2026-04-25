"""
Plot overflow event count as a rolling 1-second time series.
An overflow occurs when the BoundedRing evicts the oldest item to make room
for a new one. Logs as target="overflow".

Usage: python scripts/plot_overflow.py --log <path>
"""

import argparse, json, pathlib, datetime
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    args = ap.parse_args()

    timestamps = []
    first_ts = None

    with open(args.log, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("target") != "overflow":
                continue
            # tracing-subscriber JSON puts timestamp at top level as "timestamp"
            ts_str = obj.get("timestamp", "")
            if ts_str:
                try:
                    ts = datetime.datetime.fromisoformat(
                        ts_str.replace("Z", "+00:00")
                    ).timestamp()
                    if first_ts is None:
                        first_ts = ts
                    timestamps.append(ts - first_ts)
                except Exception:
                    timestamps.append(float(len(timestamps)))
            else:
                timestamps.append(float(len(timestamps)))

    tag = "async" if "async" in pathlib.Path(args.log).name else "threaded"
    out = pathlib.Path("reports/figures") / f"overflow_timeline_{tag}.png"
    out.parent.mkdir(parents=True, exist_ok=True)

    if not timestamps:
        print("No overflow events found — channel capacity was never reached. "
              "This is a valid result at normal SSE rates.")
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.text(0.5, 0.5,
                "No overflow events recorded\n(channel capacity was never reached)",
                ha="center", va="center", fontsize=13, transform=ax.transAxes)
        ax.set_title(f"Overflow Events Over Time ({tag})")
        fig.tight_layout()
        fig.savefig(out, dpi=300)
        plt.close(fig)
        print(f"Saved empty plot to {out}")
        return

    max_t = max(timestamps)
    bucket_count = max(int(max_t) + 1, 1)
    buckets = [0] * bucket_count
    for t in timestamps:
        buckets[min(int(t), bucket_count - 1)] += 1

    fig, ax = plt.subplots(figsize=(9, 4))
    ax.bar(range(bucket_count), buckets, color="#e74c3c", edgecolor="white")
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Overflow Events")
    ax.set_title(f"Channel Overflow Events Over Time ({tag})  total={len(timestamps)}")
    fig.tight_layout()
    fig.savefig(out, dpi=300)
    plt.close(fig)
    print(f"Saved {out}")


if __name__ == "__main__":
    main()
