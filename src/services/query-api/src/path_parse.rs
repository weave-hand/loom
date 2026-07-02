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

/// The recursion structure a `/objects/:type/graph` request selected.
#[derive(Debug)]
pub enum GraphMode {
    /// `?path=l1,..,lk`: an ordered cycle repeated to depth (tree view allowed).
    PathCycle(Vec<Hop>),
    /// `?links=l1,..`: a union of self-links.
    Union(Vec<String>),
    /// `?path=l0*,l1,..`: a recursive core + relational tail. `core_link` has the
    /// `*` stripped; each tail hop is re-emitted by name with the `~` sigil
    /// re-attached for inverse hops (a forward-only tail resolves `~x` as an
    /// absent forward link rather than silently dropping the sigil).
    CoreTail {
        core_link: String,
        tail_links: Vec<String>,
    },
}

/// Select the graph mode from the parsed `path`/`links` params and the `tree` flag,
/// enforcing the route's grammar in its historical precedence order. Every `Err`
/// string is served verbatim as the 400 body. A `*`-suffixed FORWARD segment marks
/// the recursive core; `~foo*` is NOT a core (it stays a path-cycle inverse hop
/// whose name ends in `*`, resolving to UnknownLink downstream).
pub fn parse_graph_mode(
    path: Vec<Hop>,
    links: Vec<String>,
    tree: bool,
) -> Result<GraphMode, String> {
    if !path.is_empty() && !links.is_empty() {
        return Err("specify either path or links, not both".to_string());
    }
    if !links.is_empty() {
        if tree {
            return Err("tree view is not supported with links (union)".to_string());
        }
        return Ok(GraphMode::Union(links));
    }
    if path.is_empty() {
        return Err("path or links requires at least one link".to_string());
    }
    let starred: Vec<usize> = path
        .iter()
        .enumerate()
        .filter(|(_, h)| h.direction == Direction::Forward && h.link.ends_with('*'))
        .map(|(i, _)| i)
        .collect();
    if starred.is_empty() {
        return Ok(GraphMode::PathCycle(path));
    }
    if tree {
        return Err("tree view is not supported for a recursive-core (*) path".to_string());
    }
    if starred.len() > 1 {
        return Err("at most one path segment may be marked recursive with `*`".to_string());
    }
    if starred.first().copied().unwrap_or(0) != 0 {
        return Err("the recursive `*` segment must be the first path segment".to_string());
    }
    let Some(first_hop) = path.first() else {
        return Err("empty path".to_string());
    };
    let core_link = first_hop.link.trim_end_matches('*').to_string();
    if core_link.is_empty() {
        return Err("recursive core link name must not be empty".to_string());
    }
    let tail_links: Vec<String> = path
        .get(1..)
        .unwrap_or_default()
        .iter()
        .map(|h| match h.direction {
            Direction::Forward => h.link.clone(),
            Direction::Inverse => format!("~{}", h.link),
        })
        .collect();
    Ok(GraphMode::CoreTail {
        core_link,
        tail_links,
    })
}
