use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rts_core::{AtomicLeaderboard, Leaderboard, MutexLeaderboard, RwLockLeaderboard};
use std::sync::Arc;
use std::time::Duration;

const DOMAINS: &[&str] = &[
    "en.wikipedia.org",
    "de.wikipedia.org",
    "fr.wikipedia.org",
    "es.wikipedia.org",
    "ru.wikipedia.org",
];

fn run_contention<L: Leaderboard + 'static>(
    lb: Arc<L>,
    thread_count: usize,
    ops_per_thread: usize,
    read_pct: usize,
) {
    let mut handles = Vec::with_capacity(thread_count);
    for t in 0..thread_count {
        let lb = Arc::clone(&lb);
        handles.push(std::thread::spawn(move || {
            for i in 0..ops_per_thread {
                if (i + t) % 100 < read_pct {
                    let _ = std::hint::black_box(lb.top_3());
                } else {
                    lb.record(DOMAINS[i % DOMAINS.len()]);
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

fn bench_leaderboard(c: &mut Criterion) {
    let thread_counts = [1, 2, 4, 8, 16];
    let ratios: &[(&str, usize)] = &[
        ("reads_90", 90),
        ("reads_50", 50),
        ("reads_10", 10),
    ];
    let ops_per_thread = 5_000usize;

    for (ratio_name, read_pct) in ratios {
        let mut group = c.benchmark_group(format!("leaderboard/{ratio_name}"));
        group.measurement_time(Duration::from_secs(10));

        for &threads in &thread_counts {
            let total_ops = (threads * ops_per_thread) as u64;
            group.throughput(Throughput::Elements(total_ops));

            group.bench_with_input(
                BenchmarkId::new("Mutex", threads),
                &threads,
                |b, &t| {
                    b.iter(|| {
                        run_contention(
                            Arc::new(MutexLeaderboard::new()),
                            t,
                            ops_per_thread,
                            *read_pct,
                        )
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("RwLock", threads),
                &threads,
                |b, &t| {
                    b.iter(|| {
                        run_contention(
                            Arc::new(RwLockLeaderboard::new()),
                            t,
                            ops_per_thread,
                            *read_pct,
                        )
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("Atomic", threads),
                &threads,
                |b, &t| {
                    b.iter(|| {
                        run_contention(
                            Arc::new(AtomicLeaderboard::new()),
                            t,
                            ops_per_thread,
                            *read_pct,
                        )
                    })
                },
            );
        }
        group.finish();
    }
}

criterion_group!(benches, bench_leaderboard);
criterion_main!(benches);
