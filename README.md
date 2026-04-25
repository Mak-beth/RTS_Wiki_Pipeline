# RTS_Wiki_Pipeline

Real-time Wikipedia Recent Changes pipeline — Rust concurrency & real-time scheduling assignment.

## Crates

| Crate | Role |
|---|---|
| `rts_core` | Shared types, zero-copy parser, tracking allocator, HDR metrics, leaderboard |
| `async_pipeline` | Tokio async ingestion + priority processing |
| `threaded_pipeline` | `std::thread` + `reqwest::blocking` ingestion (no Tokio) |
| `benches` | Criterion benchmarks: parsing, leaderboard contention, e2e latency |

## Build

```bash
cargo build --workspace
```

## Run

```bash
cargo run -p async_pipeline
cargo run -p threaded_pipeline
```

## Benchmarks

```bash
cargo bench --workspace
```

## Analysis

```bash
pip install pandas matplotlib
python scripts/analyze_latency.py logs/async_pipeline.jsonl
python scripts/compare_architectures.py
```
