# Phase 9 — Research Report

## Status: Not Yet Started

## What Will Be Written
An academic-style research report of 3000–4000 words documenting the
design, implementation, and evaluation of the system.

## Structure
| Section | Weight | Key Content |
|---|---|---|
| Abstract | — | Key numbers: 2.08x parsing speedup, 9x RwLock advantage, 95.6% vs 7.5% deadline compliance |
| Introduction | 10% | Real-time problem context, OS jitter floor, project objectives |
| Related Work | 10% | Rust ownership model, RMS scheduling theory, async vs threaded literature |
| System Design | 35% | Architecture, zero-copy, backpressure, priority scheduling, synchronisation, fault tolerance |
| Results & Discussion | 35% | All benchmark numbers, latency comparison, deadline analysis, honest limitations |
| Conclusion | — | Three primary findings stated plainly |
| References | 10% | ~10 APA citations |

## Primary Findings to Report
1. Async runtimes are unsuitable for hard sub-millisecond deadlines.
   Tokio timer wheel resolution (~1ms) causes `sleep(100µs)` to take ~3ms,
   resulting in 92.5% deadline miss rate vs 4.4% for threaded.
2. RwLock outperforms Mutex by 9.09x at 16 threads under 90% read load.
   For read-dominant shared resources, RwLock is the correct primitive.
3. Zero-copy parsing eliminates 6 heap allocations per event and delivers
   2.08x throughput improvement over owned-String deserialization.

## Constraints
- Hard limit: 4000 words (brief penalises overruns)
- Must be written by the student — not AI-generated
- Report data is fully collected and ready in `reports/`
- Section-by-section review available — write one section, get feedback,
  then proceed to the next
