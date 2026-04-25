use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Lock-free top-domain counter backed by DashMap + AtomicU64.
pub struct AtomicLeaderboard {
    counts: DashMap<String, AtomicU64>,
}

impl AtomicLeaderboard {
    pub fn new() -> Self {
        Self { counts: DashMap::new() }
    }

    pub fn increment(&self, domain: &str) {
        if let Some(entry) = self.counts.get(domain) {
            entry.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counts
                .entry(domain.to_owned())
                .or_insert_with(|| AtomicU64::new(0))
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn top3(&self) -> Vec<(String, u64)> {
        let mut v: Vec<(String, u64)> = self
            .counts
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        v.sort_by(|a, b| b.1.cmp(&a.1));
        v.truncate(3);
        v
    }
}

/// Mutex-based leaderboard for contention benchmarking.
pub struct MutexLeaderboard {
    counts: Mutex<HashMap<String, u64>>,
}

impl MutexLeaderboard {
    pub fn new() -> Self {
        Self { counts: Mutex::new(HashMap::new()) }
    }

    pub fn increment(&self, domain: &str) {
        let mut map = self.counts.lock();
        *map.entry(domain.to_owned()).or_insert(0) += 1;
    }

    pub fn top3(&self) -> Vec<(String, u64)> {
        let map = self.counts.lock();
        let mut v: Vec<(String, u64)> = map.iter().map(|(k, v)| (k.clone(), *v)).collect();
        v.sort_by(|a, b| b.1.cmp(&a.1));
        v.truncate(3);
        v
    }
}

/// RwLock-based leaderboard for contention benchmarking.
pub struct RwLockLeaderboard {
    counts: RwLock<HashMap<String, u64>>,
}

impl RwLockLeaderboard {
    pub fn new() -> Self {
        Self { counts: RwLock::new(HashMap::new()) }
    }

    pub fn increment(&self, domain: &str) {
        let mut map = self.counts.write();
        *map.entry(domain.to_owned()).or_insert(0) += 1;
    }

    pub fn top3(&self) -> Vec<(String, u64)> {
        let map = self.counts.read();
        let mut v: Vec<(String, u64)> = map.iter().map(|(k, v)| (k.clone(), *v)).collect();
        v.sort_by(|a, b| b.1.cmp(&a.1));
        v.truncate(3);
        v
    }
}
