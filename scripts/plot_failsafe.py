"""
Plot rolling p99 latency with degraded-mode regions highlighted.
Data comes from deadline_miss log entries; mode_transition entries mark
degraded/normal transitions.
If no deadline_miss data, falls back to summary JSON and produces an
informational plot.

Usage: python scripts/plot_failsafe.py --log <path>
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

    e2e_series  = []
    transitions = []   # list of (event_index, "degraded"|"normal")

    with open(args.log, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            target = obj.get("target", "")
            fields = obj.get("fields", {})

            if target == "deadline_miss":
                e2e_series.append(fields.get("e2e_us", 0))
            elif target == "mode_transition":
                transitions.append((len(e2e_series), fields.get("to", "")))

    tag = "async" if "async" in pathlib.Path(args.log).name else "threaded"
    out = pathlib.Path("reports/figures") / f"failsafe_timeline_{tag}.png"
    out.parent.mkdir(parents=True, exist_ok=True)

    if not e2e_series:
        # Fall back: load summary and show bar chart of p50/p90/p99 with thresholds
        print("No deadline_miss entries — using summary JSON for fail-safe plot.")
        log_path = pathlib.Path(args.log)
        summary_path = log_path.parent / f"{tag}_summary.json"
        fig, ax = plt.subplots(figsize=(8, 4))
        if summary_path.exists():
            s = json.loads(summary_path.read_text())
            h = s.get("e2e_latency_human", {})
            b = s.get("e2e_latency_bot",   {})
            metrics = ["p50", "p90", "p99"]
            x = np.arange(len(metrics))
            w = 0.35
            ax.bar(x - w/2, [h.get(m, 0) for m in metrics], w,
                   label="Human", color="#2ecc71")
            ax.bar(x + w/2, [b.get(m, 0) for m in metrics], w,
                   label="Bot", color="#3498db")
            ax.axhline(5000, color="#e74c3c", linestyle="--", linewidth=1,
                       label="Degraded threshold (5ms)")
            ax.axhline(3000, color="#f39c12", linestyle="--", linewidth=1,
                       label="Recovery threshold (3ms)")
            ax.axhline(2000, color="#27ae60", linestyle="--", linewidth=1,
                       label="Deadline (2ms)")
            ax.set_xticks(x)
            ax.set_xticklabels(metrics)
            ax.set_ylabel("Latency (µs)")
            ax.legend(fontsize=9)
            ax.set_title(f"Fail-Safe Thresholds vs Measured Latency ({tag})\n"
                         f"(No mode transitions in this run — system stayed in normal mode)")
        else:
            ax.text(0.5, 0.5, "No latency data recorded",
                    ha="center", va="center", fontsize=13, transform=ax.transAxes)
            ax.set_title(f"Fail-Safe Mode Timeline ({tag})")
        fig.tight_layout()
        fig.savefig(out, dpi=300)
        plt.close(fig)
        print(f"Saved {out}")
        return

    # Rolling p99 over a 100-sample window
    window = 100
    rolling_p99 = []
    for i in range(len(e2e_series)):
        start = max(0, i - window + 1)
        chunk = sorted(e2e_series[start:i + 1])
        idx   = max(0, int(0.99 * len(chunk)) - 1)
        rolling_p99.append(chunk[idx])

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(rolling_p99, color="#2c3e50", linewidth=1, label="Rolling p99 (µs)")
    ax.axhline(5000, color="#e74c3c", linestyle="--", linewidth=1,
               label="Degraded threshold (5ms)")
    ax.axhline(3000, color="#f39c12", linestyle="--", linewidth=1,
               label="Recovery threshold (3ms)")
    ax.axhline(2000, color="#27ae60", linestyle="--", linewidth=1,
               label="Deadline (2ms)")

    # Shade degraded regions
    degraded_start = None
    first_degraded_shaded = False
    for idx, to in transitions:
        if to == "degraded":
            degraded_start = idx
        elif to == "normal" and degraded_start is not None:
            lbl = "Degraded mode" if not first_degraded_shaded else ""
            ax.axvspan(degraded_start, idx, alpha=0.15, color="red", label=lbl)
            first_degraded_shaded = True
            degraded_start = None

    # Mark transition lines
    for idx, to in transitions:
        color = "red" if to == "degraded" else "green"
        ax.axvline(idx, color=color, linewidth=1.2, alpha=0.7)

    ax.set_xlabel("Event Index")
    ax.set_ylabel("Rolling p99 Latency (µs)")
    ax.set_title(f"Fail-Safe Mode Timeline ({tag})")
    # Deduplicate legend
    handles, labels = ax.get_legend_handles_labels()
    seen = {}
    for h, l in zip(handles, labels):
        if l and l not in seen:
            seen[l] = h
    ax.legend(seen.values(), seen.keys(), fontsize=9)
    fig.tight_layout()
    fig.savefig(out, dpi=300)
    plt.close(fig)
    print(f"Saved {out}")


if __name__ == "__main__":
    main()
