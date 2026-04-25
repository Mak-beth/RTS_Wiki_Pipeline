use serde::Deserialize;

/// A single Wikipedia Recent Changes SSE event.
/// Every string field borrows directly from the raw JSON buffer — no heap allocation
/// occurs during deserialization of string data.
#[derive(Debug, Clone, Deserialize)]
pub struct WikiEvent<'a> {
    #[serde(borrow)]
    pub user: &'a str,
    pub bot: bool,
    #[serde(borrow)]
    pub server_name: &'a str,
    #[serde(borrow)]
    pub wiki: &'a str,
    #[serde(borrow)]
    pub title: &'a str,
    pub timestamp: i64,
    #[serde(rename = "type", borrow)]
    pub event_type: &'a str,
}

/// Scheduling priority derived from the bot flag.
/// Human edits pre-empt bot edits in the priority queue.
#[derive(Debug, Clone, PartialEq)]
pub enum Priority {
    Human,
    Bot,
}

impl<'a> From<&WikiEvent<'a>> for Priority {
    fn from(event: &WikiEvent<'a>) -> Self {
        if event.bot {
            Priority::Bot
        } else {
            Priority::Human
        }
    }
}
