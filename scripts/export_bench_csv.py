"""Export Criterion JSON results to CSV for report inclusion.

Criterion stores one estimates.json per (group, bench_id, variant) triplet.
Only the 'new' variant is collected to avoid duplicates.

Directory layout:
  target/criterion/<group>/<bench_id...>/new/estimates.json
"""
import json, csv, os, pathlib

CRITERION_DIR = pathlib.Path("target/criterion")
REPORTS_DIR   = pathlib.Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

def find_estimates(base: pathlib.Path):
    rows = []
    for estimates_file in base.rglob("estimates.json"):
        # Only collect the 'new' directory (not 'base') to avoid duplicates.
        if estimates_file.parent.name != "new":
            continue
        rel_parts = estimates_file.relative_to(base).parts
        # rel_parts: <group> / <bench...> / new / estimates.json
        group = rel_parts[0]
        bench = "/".join(rel_parts[1:-2])   # everything between group and new/
        data  = json.loads(estimates_file.read_text())
        mean_ns = data.get("mean", {}).get("point_estimate", 0)
        rows.append({"group": group, "bench": bench, "mean_ns": mean_ns})
    return rows

rows = find_estimates(CRITERION_DIR)
rows.sort(key=lambda r: (r["group"], r["bench"]))
out  = REPORTS_DIR / "bench_summary.csv"
with open(out, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["group", "bench", "mean_ns"])
    w.writeheader()
    w.writerows(rows)

print(f"Written {len(rows)} rows to {out}")
