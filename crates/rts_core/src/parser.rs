use crate::types::WikiEvent;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("empty SSE data field")]
    EmptyData,
    #[error("json parse failed: {0}")]
    Json(#[from] serde_json::Error),
}

/// Parse a WikiEvent borrowing all string fields from `src`.
/// Zero additional heap allocation: every &str points into the caller's buffer.
pub fn parse_event(src: &str) -> Result<WikiEvent<'_>, ParseError> {
    if src.is_empty() {
        return Err(ParseError::EmptyData);
    }
    let event: WikiEvent<'_> = serde_json::from_str(src)?;
    Ok(event)
}
