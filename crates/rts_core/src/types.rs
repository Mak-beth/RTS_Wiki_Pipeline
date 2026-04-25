use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WikiEvent<'a> {
    #[serde(borrow)]
    pub wiki: &'a str,
    #[serde(borrow)]
    pub title: &'a str,
    #[serde(rename = "type", borrow)]
    pub event_type: &'a str,
    pub bot: bool,
    pub timestamp: i64,
    #[serde(borrow)]
    pub user: &'a str,
    pub length: Option<LengthInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LengthInfo {
    pub new: Option<u64>,
    pub old: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct TimedEvent {
    pub raw: String,
    pub ingested_at: Instant,
    pub priority: Priority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Bot   = 0,
    Human = 1,
}

impl Priority {
    pub fn from_bot_flag(bot: bool) -> Self {
        if bot { Priority::Bot } else { Priority::Human }
    }
}

#[derive(Debug, Serialize)]
pub struct DeadlineMissEvent {
    pub event_type: &'static str,
    pub elapsed_us: u64,
    pub deadline_us: u64,
    pub baseline_jitter_us: u64,
    pub priority: &'static str,
    pub timestamp_unix: u64,
}

#[derive(Debug, Serialize)]
pub struct OverflowEvent {
    pub event_type: &'static str,
    pub dropped_at_unix: u64,
    pub queue_capacity: usize,
}
