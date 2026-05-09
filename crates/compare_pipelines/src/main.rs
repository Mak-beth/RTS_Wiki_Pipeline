use serde_json::Value;
use std::fs;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── 1. Async pipeline ─────────────────────────────────────────────────────
    println!("[1/2] Running async pipeline (mock, 30s)...");
    let status = Command::new("cargo")
        .args(["run", "--release", "-p", "async_pipeline",
               "--", "--mock", "--duration", "30"])
        .status()?;
    assert!(status.success(), "async_pipeline exited with non-zero status: {status}");

    // ── 2. Threaded pipeline ──────────────────────────────────────────────────
    println!("[2/2] Running threaded pipeline (mock, 30s)...");
    let status = Command::new("cargo")
        .args(["run", "--release", "-p", "threaded_pipeline",
               "--", "--mock", "--duration", "30"])
        .status()?;
    assert!(status.success(), "threaded_pipeline exited with non-zero status: {status}");

    // ── 3. Read both summaries ────────────────────────────────────────────────
    let async_json: Value =
        serde_json::from_str(&fs::read_to_string("logs/async_summary.json")?)?;
    let threaded_json: Value =
        serde_json::from_str(&fs::read_to_string("logs/threaded_summary.json")?)?;

    // ── 4. Print table ────────────────────────────────────────────────────────
    print_table(&async_json, &threaded_json);

    // ── 5. Write combined JSON ────────────────────────────────────────────────
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let combined = serde_json::json!({
        "async":        async_json,
        "threaded":     threaded_json,
        "generated_at": ts,
    });

    fs::create_dir_all("reports")?;
    fs::write("reports/comparison.json", serde_json::to_string_pretty(&combined)?)?;
    println!("\nWrote reports/comparison.json");

    Ok(())
}

/// Extract a u64 value at a two-level JSON path.
/// Returns "N/A" if any key is absent or the value is not a u64.
fn get(json: &Value, key1: &str, key2: &str) -> String {
    json.get(key1)
        .and_then(|v| v.get(key2))
        .and_then(|v| v.as_u64())
        .map(|n| n.to_string())
        .unwrap_or_else(|| "N/A".to_string())
}

fn print_table(a: &Value, t: &Value) {
    const W_M: usize = 34; // metric column width
    const W_V: usize = 12; // value column width

    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║              ASYNC vs THREADED COMPARISON                    ║");
    println!("║              30 second mock run, 2000 eps                    ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    // Header
    println!("{:<W_M$}│ {:<W_V$} │ {}", "Metric", "Async", "Threaded");

    // Separator: W_M dashes, ┼, (W_V+2) dashes, ┼, (W_V+1) dashes
    println!("{}┼{}┼{}",
             "─".repeat(W_M),
             "─".repeat(W_V + 2),
             "─".repeat(W_V + 1));

    // Data rows
    let rows: &[(&str, &str, &str)] = &[
        ("e2e latency human p50 (µs)",  "e2e_latency_human", "p50"),
        ("e2e latency human p90 (µs)",  "e2e_latency_human", "p90"),
        ("e2e latency human p99 (µs)",  "e2e_latency_human", "p99"),
        ("e2e latency bot p50    (µs)", "e2e_latency_bot",   "p50"),
        ("e2e latency bot p99    (µs)", "e2e_latency_bot",   "p99"),
        ("drift human p50        (µs)", "drift_human",       "p50"),
        ("drift human p99        (µs)", "drift_human",       "p99"),
        ("drift bot p99          (µs)", "drift_bot",         "p99"),
        ("events processed (human)",    "e2e_latency_human", "count"),
        ("events processed (bot)",      "e2e_latency_bot",   "count"),
    ];

    for (label, k1, k2) in rows {
        let av = get(a, k1, k2);
        let tv = get(t, k1, k2);
        println!("{:<W_M$}│ {:>W_V$} │ {:>W_V$}", label, av, tv);
    }

    // Winners
    println!();

    let human_p99_a = get(a, "e2e_latency_human", "p99").parse::<u64>().ok();
    let human_p99_t = get(t, "e2e_latency_human", "p99").parse::<u64>().ok();
    let bot_p99_a   = get(a, "e2e_latency_bot",   "p99").parse::<u64>().ok();
    let bot_p99_t   = get(t, "e2e_latency_bot",   "p99").parse::<u64>().ok();

    let human_winner = match (human_p99_a, human_p99_t) {
        (Some(a), Some(b)) if a < b => "Async",
        (Some(a), Some(b)) if b < a => "Threaded",
        (Some(_), Some(_))           => "Tie",
        _                            => "N/A",
    };

    let bot_winner = match (bot_p99_a, bot_p99_t) {
        (Some(a), Some(b)) if a < b => "Async",
        (Some(a), Some(b)) if b < a => "Threaded",
        (Some(_), Some(_))           => "Tie",
        _                            => "N/A",
    };

    println!("Winner (human p99 lower is better): {human_winner}");
    println!("Winner (bot p99 lower is better):   {bot_winner}");
}
