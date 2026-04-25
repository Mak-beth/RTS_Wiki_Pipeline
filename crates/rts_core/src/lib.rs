pub mod event;
pub mod jitter_baseline;
pub mod leaderboard;
pub mod metrics;
pub mod parser;

#[cfg(feature = "track-alloc")]
pub mod allocator;

// Re-export the most commonly needed types at the crate root.
pub use event::{Priority, WikiEvent};
pub use jitter_baseline::{measure_os_jitter, OsJitterReport};
pub use leaderboard::{AtomicLeaderboard, Leaderboard, MutexLeaderboard, RwLockLeaderboard};
pub use metrics::{HistogramAggregator, LatencySample};
pub use parser::{parse_event, ParseError};
