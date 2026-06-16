//! Pure parsing of traversal direction at the HTTP edge — kept socket-free so it is
//! unit-testable without axum. A multi-hop `path` element prefixed with `~` is an inverse
//! hop; a bare element is forward. The single-hop `?direction=` value is `forward`
//! (default) or `inverse`; anything else is rejected.

use crate::handler::{Direction, Hop};

/// The inverse-hop sigil on a `path` element. RFC-3986 unreserved, so no URL-encoding.
const INVERSE_SIGIL: char = '~';

/// Parse a comma-separated `path` query value into directed hops. Each element is trimmed;
/// a leading `~` (after trimming) marks the hop inverse and is stripped (then re-trimmed);
/// empty elements are dropped.
pub fn parse_path_hops(param: &str) -> Vec<Hop> {
    param
        .split(',')
        .filter_map(|raw| {
            let s = raw.trim();
            let (direction, name) = match s.strip_prefix(INVERSE_SIGIL) {
                Some(rest) => (Direction::Inverse, rest.trim()),
                None => (Direction::Forward, s),
            };
            if name.is_empty() {
                None
            } else {
                Some(Hop {
                    link: name.to_string(),
                    direction,
                })
            }
        })
        .collect()
}

/// An unrecognized single-hop `direction` value.
#[derive(Debug, thiserror::Error, PartialEq)]
#[error("invalid direction '{0}' (expected 'forward' or 'inverse')")]
pub struct InvalidDirection(pub String);

/// Parse the single-hop `?direction=` value. Absent or `forward` -> Forward; `inverse`
/// -> Inverse; anything else (including empty) -> error.
pub fn parse_direction(raw: Option<&str>) -> Result<Direction, InvalidDirection> {
    match raw {
        None | Some("forward") => Ok(Direction::Forward),
        Some("inverse") => Ok(Direction::Inverse),
        Some(other) => Err(InvalidDirection(other.to_string())),
    }
}
