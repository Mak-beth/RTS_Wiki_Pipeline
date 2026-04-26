# Phase 8 — Demo Script & Mock SSE Server

## Status: Not Yet Started

## What Will Be Built
A controlled 5-minute live demonstration script that showcases all system
behaviours without depending on external network conditions or OS-level
administrator privileges.

## Planned Contributions

### Mock SSE Server (crates/mock_sse_server/)
A small Tokio binary that:
- Serves a local SSE stream on `localhost:7777`
- Replays events from `logs/sample_stream.jsonl` in a loop
- Accepts `--fail-after <seconds>` flag — stops sending data to simulate
  network silence, then resumes after 15 seconds
- Allows controlled, reproducible network failure demonstration without
  touching OS network adapters

### Load Injector (crates/load_injector/)
A binary that pegs all CPU cores at 100% for a configurable duration.
Used to trigger the fail-safe degraded mode during the demo by artificially
inflating processing latency.

### Both Pipelines: --sse-url Flag
A `--sse-url` CLI flag (default: Wikipedia stream) allows both pipeline
binaries to be pointed at `localhost:7777` for demo and testing.

### Demo Script (scripts/demo.ps1)
PowerShell script automating the 5-minute demo timeline:
- 0:00–1:00 — Normal operation, live leaderboard updates, human/bot events
- 1:00–2:00 — Network failure via mock server, watchdog fires at ~10s
- 2:00–3:00 — CPU load injection, fail-safe triggers, ModeTransition logged
- 3:00–4:00 — Threaded pipeline, same scenarios repeated for comparison
- 4:00–5:00 — Criterion HTML reports walked through

### DEMO.md
Step-by-step presenter guide with exact PowerShell commands, timing markers,
and notes on what to point at for each segment.
