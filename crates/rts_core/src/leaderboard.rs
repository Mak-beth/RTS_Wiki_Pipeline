use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Shared interface for all leaderboard implementations.
pub trait Leaderboard: Send + Sync {
    /// Increment the edit count for `server_name` by 1.
    fn record(&self, server_name: &str);
    /// Return the top 3 domains sorted by descending edit count.
    fn top_3(&self) -> Vec<(String, u64)>;
}

// ─── Mutex implementation ────────────────────────────────────────────────────

pub struct MutexLeaderboard {
    counts: Mutex<HashMap<String, u64>>,
}

impl MutexLeaderboard {
    pub fn new() -> Self {
        Self { counts: Mutex::new(HashMap::new()) }
    }
}

impl Leaderboard for MutexLeaderboard {
    fn record(&self, server_name: &str) {
        *self.counts.lock().entry(server_name.to_owned()).or_insert(0) += 1;
    }

    fn top_3(&self) -> Vec<(String, u64)> {
        let map = self.counts.lock();
        top3_from_iter(map.iter().map(|(k, v)| (k.clone(), *v)))
    }
}

// ─── RwLock implementation ───────────────────────────────────────────────────

pub struct RwLockLeaderboard {
    counts: RwLock<HashMap<String, u64>>,
}

impl RwLockLeaderboard {
    pub fn new() -> Self {
        Self { counts: RwLock::new(HashMap::new()) }
    }
}

impl Leaderboard for RwLockLeaderboard {
    fn record(&self, server_name: &str) {
        *self.counts.write().entry(server_name.to_owned()).or_insert(0) += 1;
    }

    fn top_3(&self) -> Vec<(String, u64)> {
        let map = self.counts.read();
        top3_from_iter(map.iter().map(|(k, v)| (k.clone(), *v)))
    }
}

// ─── Atomic / DashMap implementation ────────────────────────────────────────

pub struct AtomicLeaderboard {
    counts: DashMap<String, AtomicU64>,
}

impl AtomicLeaderboard {
    pub fn new() -> Self {
        Self { counts: DashMap::new() }
    }
}

impl Leaderboard for AtomicLeaderboard {
    fn record(&self, server_name: &str) {
        // Fast path: entry already exists — no allocation needed.
        if let Some(v) = self.counts.get(server_name) {
            v.fetch_add(1, Ordering::Relaxed);
            return;
        }
        // Slow path: insert-or-find, then increment.
        self.counts
            .entry(server_name.to_owned())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    fn top_3(&self) -> Vec<(String, u64)> {
        top3_from_iter(
            self.counts.iter().map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed))),
        )
    }
}

// ─── Shared helper ───────────────────────────────────────────────────────────

fn top3_from_iter(iter: impl Iterator<Item = (String, u64)>) -> Vec<(String, u64)> {
    let mut v: Vec<(String, u64)> = iter.collect();
    v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    v.truncate(3);
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const DOMAINS: [&str; 5] = [
        "en.wikipedia.org",
        "de.wikipedia.org",
        "fr.wikipedia.org",
        "es.wikipedia.org",
        "ru.wikipedia.org",
    ];

    /// Run 8 concurrent threads, each recording 1 000 events with a skewed distribution:
    ///   en  40 % → 3 200 total
    ///   de  30 % → 2 400 total
    ///   fr  20 % → 1 600 total
    ///   es  10 % →   800 total
    ///   ru   0 % →     0 total
    fn run_8_threads<L: Leaderboard + 'static>(lb: Arc<L>) {
        let mut handles = Vec::with_capacity(8);
        for _ in 0..8 {
            let lb = Arc::clone(&lb);
            handles.push(std::thread::spawn(move || {
                for i in 0..1000usize {
                    let idx = match i % 10 {
                        0 | 1 | 2 | 3 => 0, // en — 40 %
                        4 | 5 | 6     => 1, // de — 30 %
                        7 | 8         => 2, // fr — 20 %
                        _             => 3, // es — 10 %
                    };
                    lb.record(DOMAINS[idx]);
                }
            }));
        }
        for h in handles {
            h.join().expect("thread panicked");
        }
    }

    fn assert_expected_top3(top: &[(String, u64)], label: &str) {
        assert_eq!(top.len(), 3, "{label}: expected 3 entries, got {}", top.len());
        assert_eq!(top[0], ("en.wikipedia.org".into(), 3200), "{label}: wrong rank-1");
        assert_eq!(top[1], ("de.wikipedia.org".into(), 2400), "{label}: wrong rank-2");
        assert_eq!(top[2], ("fr.wikipedia.org".into(), 1600), "{label}: wrong rank-3");
    }

    #[test]
    fn all_three_implementations_agree() {
        let mutex_lb  = Arc::new(MutexLeaderboard::new());
        let rwlock_lb = Arc::new(RwLockLeaderboard::new());
        let atomic_lb = Arc::new(AtomicLeaderboard::new());

        run_8_threads(Arc::clone(&mutex_lb));
        run_8_threads(Arc::clone(&rwlock_lb));
        run_8_threads(Arc::clone(&atomic_lb));

        let mt = mutex_lb.top_3();
        let rw = rwlock_lb.top_3();
        let at = atomic_lb.top_3();

        assert_expected_top3(&mt, "MutexLeaderboard");
        assert_expected_top3(&rw, "RwLockLeaderboard");
        assert_expected_top3(&at, "AtomicLeaderboard");

        assert_eq!(mt, rw, "Mutex and RwLock disagree");
        assert_eq!(mt, at, "Mutex and Atomic disagree");
    }
}
