use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rts_core::{parse_event, HistogramAggregator, LatencySample, Leaderboard, MutexLeaderboard};
use std::sync::Arc;
use std::time::Instant;

// CARGO_MANIFEST_DIR is the absolute path to crates/benches at compile time.
// ../../logs/ navigates up to the workspace root where the sample lives.
const SAMPLE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../logs/sample_stream.jsonl"
);

fn load_sample() -> Vec<String> {
    let content = std::fs::read_to_string(SAMPLE_PATH).unwrap_or_else(|_| {
        panic!(
            "Sample file not found at {}. \
             Run: cargo run -p async_pipeline -- --duration 60 \
             --capture-sample logs/sample_stream.jsonl",
            SAMPLE_PATH
        )
    });
    content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.to_owned())
        .collect()
}

fn simulate_pipeline(
    events: &[String],
    event_count: usize,
    leaderboard: &Arc<MutexLeaderboard>,
) -> HistogramAggregator {
    let mut agg = HistogramAggregator::new();
    let slice = &events[..event_count.min(events.len())];

    for raw in slice {
        let ingest_time = Instant::now();
        let dequeue_time = Instant::now();
        let expected_start = dequeue_time;

        if let Ok(event) = parse_event(raw) {
            leaderboard.record(event.server_name);
            let was_human = !event.bot;
            let complete_time = Instant::now();

            agg.record(&LatencySample {
                ingest_time,
                dequeue_time,
                complete_time,
                expected_start,
                was_human,
            });
        }
    }
    agg
}

fn bench_e2e(c: &mut Criterion) {
    let events = load_sample();
    let mut group = c.benchmark_group("e2e_latency");

    let speeds: &[(&str, usize)] = &[("1x", 50), ("5x", 250), ("10x", 500)];

    for &(label, count) in speeds {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("pipeline_sim", label),
            &count,
            |b, &n| {
                let lb = Arc::new(MutexLeaderboard::new());
                b.iter(|| {
                    let agg = simulate_pipeline(&events, n, &lb);
                    std::hint::black_box(agg);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_e2e);
criterion_main!(benches);
