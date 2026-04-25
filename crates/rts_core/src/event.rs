use serde::Deserialize;

/// A single Wikipedia Recent Changes SSE event.
///
/// Every string field borrows directly from the raw JSON buffer — no heap
/// allocation occurs during deserialisation of string data (see [`crate::parse_event`]).
#[derive(Debug, Clone, Deserialize)]
pub struct WikiEvent<'a> {
    /// The username of the editor who made this change.
    #[serde(borrow)]
    pub user: &'a str,
    /// `true` when the edit was made by an automated bot account.
    pub bot: bool,
    /// The hostname of the wiki server (e.g. `"en.wikipedia.org"`).
    #[serde(borrow)]
    pub server_name: &'a str,
    /// The short wiki database name (e.g. `"enwiki"`).
    #[serde(borrow)]
    pub wiki: &'a str,
    /// The title of the page that was edited.
    #[serde(borrow)]
    pub title: &'a str,
    /// Unix timestamp of the edit in seconds.  `None` when the field is
    /// absent from the SSE payload (some meta-events omit it).
    pub timestamp: Option<i64>,
    /// The SSE event type string (e.g. `"edit"`, `"new"`, `"log"`).
    #[serde(rename = "type", borrow)]
    pub event_type: &'a str,
}

/// Scheduling priority derived from the `bot` flag of a [`WikiEvent`].
///
/// Human edits pre-empt bot edits in the dispatcher's priority queues.
/// When the pipeline enters *degraded mode* (rolling p99 > 5 ms), bot
/// events are dropped entirely to shed load.
#[derive(Debug, Clone, PartialEq)]
pub enum Priority {
    /// Edit made by a human editor — routed to the high-priority channel.
    Human,
    /// Edit made by a bot account — routed to the low-priority channel.
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
