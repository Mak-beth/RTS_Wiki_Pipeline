use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::time::{SystemTime, UNIX_EPOCH};

const DOMAINS: [&str; 10] = [
    "en.wikipedia.org",
    "de.wikipedia.org",
    "fr.wikipedia.org",
    "es.wikipedia.org",
    "ru.wikipedia.org",
    "ja.wikipedia.org",
    "zh.wikipedia.org",
    "pt.wikipedia.org",
    "it.wikipedia.org",
    "nl.wikipedia.org",
];

const HUMAN_USERS: [&str; 4] = ["Alice", "Bob", "Carol", "Dave"];
const BOT_USERS: [&str; 3]   = ["ClueBot", "AntiVandal", "RefBot"];

/// Derive the short wiki database name from a `*.wikipedia.org` domain.
/// All ten domains in the fixed list have a 2-character language prefix.
fn wiki_from_domain(domain: &str) -> String {
    format!("{}wiki", &domain[..2])
}

/// Generate one synthetic Wikipedia SSE event as a JSON string.
///
/// The returned string is identical in shape to a real Wikipedia recent-changes
/// event: fields `user`, `bot`, `server_name`, `wiki`, `title`, `timestamp`,
/// and `type`.  All non-timestamp fields are driven by `rng`, so two calls with
/// the same RNG state produce identical output.
///
/// # Parameters
/// - `seq`  — monotonically increasing sequence number; used as the page title
///   suffix (`Page_<seq>`) to distinguish events.
/// - `rng`  — caller-owned `StdRng`.  Use `StdRng::seed_from_u64(42)` for a
///   reproducible sequence.
pub fn generate_event(seq: u64, rng: &mut StdRng) -> String {
    let is_bot       = rng.gen_bool(0.80);
    let domain_idx   = rng.gen_range(0..DOMAINS.len());
    let server_name  = DOMAINS[domain_idx];
    let wiki         = wiki_from_domain(server_name);

    let user = if is_bot {
        BOT_USERS[rng.gen_range(0..BOT_USERS.len())]
    } else {
        HUMAN_USERS[rng.gen_range(0..HUMAN_USERS.len())]
    };

    let timestamp: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    format!(
        r#"{{"user":"{user}","bot":{is_bot},"server_name":"{server_name}","wiki":"{wiki}","title":"Page_{seq}","timestamp":{timestamp},"type":"edit"}}"#
    )
}

/// A seeded, reproducible event generator.
///
/// Wraps [`generate_event`] with an internal `StdRng` seeded at `42` and an
/// auto-incrementing sequence counter.  Two `MockStream::new()` instances
/// produce bit-identical event sequences, enabling reproducible benchmarks.
pub struct MockStream {
    rng: StdRng,
    seq: u64,
}

impl MockStream {
    /// Create a new `MockStream` with seed `42`.
    pub fn new() -> Self {
        Self {
            rng: StdRng::seed_from_u64(42),
            seq: 0,
        }
    }

    /// Produce the next synthetic event JSON string.
    pub fn next_event(&mut self) -> String {
        let event = generate_event(self.seq, &mut self.rng);
        self.seq += 1;
        event
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_event_parses_via_parse_event() {
        let mut rng = StdRng::seed_from_u64(42);
        let json = generate_event(0, &mut rng);
        let result = crate::parse_event(&json);
        assert!(result.is_ok(), "parse_event failed on mock JSON: {json}");
        let event = result.unwrap();
        assert_eq!(event.event_type, "edit");
        assert!(!event.server_name.is_empty());
    }

    #[test]
    fn same_seed_same_output() {
        let ev1 = generate_event(0, &mut StdRng::seed_from_u64(42));
        let ev2 = generate_event(0, &mut StdRng::seed_from_u64(42));
        assert_eq!(ev1, ev2, "two RNGs with seed 42 produced different output");
    }

    #[test]
    fn mock_stream_deterministic_sequence() {
        let events1: Vec<String> = {
            let mut m = MockStream::new();
            (0..20).map(|_| m.next_event()).collect()
        };
        let events2: Vec<String> = {
            let mut m = MockStream::new();
            (0..20).map(|_| m.next_event()).collect()
        };
        // Timestamps are wall-clock seconds; strip them before comparing so the
        // test doesn't flap when a second boundary falls between the two loops.
        for (i, (a, b)) in events1.iter().zip(events2.iter()).enumerate() {
            let strip_ts = |s: &str| -> String {
                // Replace the numeric timestamp value with a placeholder.
                let re_start = s.find("\"timestamp\":").expect("no timestamp field");
                let after = &s[re_start + 12..];
                let re_end = after.find(',').expect("no comma after timestamp");
                format!("{}<TS>{}", &s[..re_start + 12], &after[re_end..])
            };
            assert_eq!(
                strip_ts(a),
                strip_ts(b),
                "event {i} differed between two MockStream instances"
            );
        }
    }

    #[test]
    fn bot_ratio_approximately_80_percent() {
        let mut m = MockStream::new();
        let n = 1000;
        let bots = (0..n)
            .filter(|_| {
                let j = m.next_event();
                j.contains("\"bot\":true")
            })
            .count();
        // Allow ±5 % around the 80 % target (750–850).
        assert!(bots >= 750 && bots <= 850,
            "expected ~80% bot, got {bots}/{n} = {}%", bots * 100 / n);
    }
}
