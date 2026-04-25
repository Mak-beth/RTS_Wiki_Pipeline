use crate::event::WikiEvent;
use thiserror::Error;

/// Errors that can occur when parsing a raw SSE payload.
#[derive(Debug, Error)]
pub enum ParseError {
    /// The JSON was syntactically invalid or a required field had the wrong type.
    #[error("json parse error: {0}")]
    JsonError(#[from] serde_json::Error),
    /// A structurally valid JSON object was missing a required field.
    #[error("missing required field")]
    MissingField,
}

/// Deserialise a [`WikiEvent`] from a borrowed JSON string.
///
/// All string fields in the returned event point directly into `buf` —
/// serde_json's borrowed deserialisation path produces `&str` slices that
/// alias the source bytes, so **zero additional heap allocation** occurs
/// during parsing of string data.
///
/// # Errors
///
/// Returns [`ParseError::JsonError`] if the input is not valid JSON or if a
/// required field is missing or has an incompatible type.
#[inline]
pub fn parse_event<'a>(buf: &'a str) -> Result<WikiEvent<'a>, ParseError> {
    let event: WikiEvent<'a> = serde_json::from_str(buf)?;
    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Priority;

    const SAMPLE_JSON: &str = r#"{"user":"Alice","bot":false,"server_name":"en.wikipedia.org","wiki":"enwiki","title":"Test Article","timestamp":1700000000,"type":"edit"}"#;

    #[test]
    fn parse_borrows_from_source() {
        let buf = SAMPLE_JSON.to_owned();
        let event = parse_event(&buf).expect("parse failed");

        let buf_start = buf.as_ptr() as usize;
        let buf_end   = buf_start + buf.len();

        let in_src = |s: &str, name: &str| {
            let s_start = s.as_ptr() as usize;
            let s_end   = s_start + s.len();
            assert!(
                s_start >= buf_start && s_end <= buf_end,
                "field `{}` (ptr={:#x}) does not point into source buffer [{:#x}..{:#x}]",
                name, s_start, buf_start, buf_end
            );
        };

        in_src(event.user,        "user");
        in_src(event.server_name, "server_name");
        in_src(event.wiki,        "wiki");
        in_src(event.title,       "title");
        in_src(event.event_type,  "event_type");
    }

    #[test]
    fn parse_correct_values() {
        let buf = SAMPLE_JSON.to_owned();
        let event = parse_event(&buf).unwrap();

        assert_eq!(event.user,        "Alice");
        assert_eq!(event.bot,         false);
        assert_eq!(event.server_name, "en.wikipedia.org");
        assert_eq!(event.wiki,        "enwiki");
        assert_eq!(event.title,       "Test Article");
        assert_eq!(event.timestamp,   Some(1_700_000_000));
        assert_eq!(event.event_type,  "edit");
    }

    #[test]
    fn priority_from_bot_false() {
        let buf = SAMPLE_JSON.to_owned();
        let event = parse_event(&buf).unwrap();
        assert_eq!(Priority::from(&event), Priority::Human);
    }

    #[test]
    fn priority_from_bot_true() {
        let src = r#"{"user":"SomeBot","bot":true,"server_name":"en.wikipedia.org","wiki":"enwiki","title":"Bot Page","timestamp":1700000001,"type":"edit"}"#.to_owned();
        let event = parse_event(&src).unwrap();
        assert_eq!(Priority::from(&event), Priority::Bot);
    }

    #[test]
    fn parse_error_on_invalid_json() {
        let result = parse_event("not json at all");
        assert!(result.is_err());
    }
}
