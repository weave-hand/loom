//! Pure resolution of HTTP query-param filter keys into positioned chain filters.
//! A bare key (`col`) is a source filter (position 0); a `<linkname>.col` key targets
//! the type that link reaches in `path`. This is the relational `/links` surface: a link
//! that repeats in the path makes a per-hop filter on it ambiguous, which is the boundary
//! of the deferred graph (`/graph`) capability — such a filter is rejected here.

use crate::handler::ChainFilter;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum FilterResolveError {
    /// A `<prefix>.col` key whose `prefix` names no link in the path.
    #[error("unknown filter target: {0}")]
    UnknownTarget(String),
    /// A per-hop filter on a link that repeats in the path. Per-hop filtering across a
    /// repeated link is graph traversal (deferred to /graph), not relational traversal.
    #[error("ambiguous filter link '{0}': graph traversal (/graph) is not yet supported")]
    AmbiguousLink(String),
}

/// Resolve `(key, value)` params against `path` into positioned `ChainFilter`s. A key
/// containing a `.` is `<prefix>.<column>` (split on the FIRST dot); `prefix` must name a
/// link occurring exactly once in `path` (→ that link's position = index + 1). A key with
/// no `.` is a source filter (position 0).
pub fn resolve_chain_filters(
    path: &[String],
    params: Vec<(String, String)>,
) -> Result<Vec<ChainFilter>, FilterResolveError> {
    let mut counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for l in path {
        *counts.entry(l.as_str()).or_default() += 1;
    }
    let mut out = Vec::with_capacity(params.len());
    for (key, value) in params {
        match key.split_once('.') {
            None => out.push(ChainFilter {
                position: 0,
                column: key,
                raw: value,
            }),
            Some((prefix, column)) => match counts.get(prefix).copied().unwrap_or(0) {
                0 => return Err(FilterResolveError::UnknownTarget(prefix.to_string())),
                1 => {
                    let idx = path
                        .iter()
                        .position(|l| l == prefix)
                        .expect("count == 1 implies present");
                    out.push(ChainFilter {
                        position: idx + 1,
                        column: column.to_string(),
                        raw: value,
                    });
                }
                _ => return Err(FilterResolveError::AmbiguousLink(prefix.to_string())),
            },
        }
    }
    Ok(out)
}
