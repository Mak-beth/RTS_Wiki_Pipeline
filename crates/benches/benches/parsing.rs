use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use serde::Deserialize;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct WikiEventOwned {
    user: String,
    bot: bool,
    server_name: String,
    wiki: String,
    title: String,
    timestamp: Option<i64>,
    #[serde(rename = "type")]
    event_type: String,
}

const SAMPLE_JSON: &str = r#"{"user":"ExampleEditor","bot":false,"server_name":"en.wikipedia.org","wiki":"enwiki","title":"Rust (programming language)","timestamp":1700000000,"type":"edit"}"#;

fn bench_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("parsing");
    let input = SAMPLE_JSON.to_owned();
    let bytes = input.len() as u64;
    group.throughput(Throughput::Bytes(bytes));

    group.bench_function("zero_copy", |b| {
        b.iter(|| {
            let result = rts_core::parse_event(black_box(&input));
            black_box(result.unwrap());
        })
    });

    group.bench_function("owned_alloc", |b| {
        b.iter(|| {
            let result: WikiEventOwned = serde_json::from_str(black_box(&input)).unwrap();
            black_box(result);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_parsing);
criterion_main!(benches);
