//! Pure reserved-query-param scraping at the HTTP edge — socket-free (like
//! `path_parse`) so per-route splits are unit-testable without axum. Each route names
//! ITS OWN reserved keys (a universal set would silently consume another route's
//! filter columns); everything unreserved stays a caller filter with order and
//! repeats preserved (a column may carry several predicates, e.g. a range).
//! Single-valued keys take the LAST occurrence — the loop-overwrite semantics the
//! routes previously hand-rolled; multi-valued keys (`_or`) read every occurrence.

use std::collections::HashMap;

/// The reserved values pulled out of one request's query params, keyed by reserved
/// key; a key's values accumulate in arrival order.
pub struct ReservedParams(HashMap<String, Vec<String>>);

impl ReservedParams {
    /// The last occurrence of a single-valued key (`?depth=1&depth=2` -> `"2"`).
    pub fn last(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.last()).map(String::as_str)
    }

    /// Every occurrence of a multi-valued key (`_or`), in arrival order.
    pub fn all(&self, key: &str) -> &[String] {
        self.0.get(key).map_or(&[], Vec::as_slice)
    }

    /// True when the key appeared at all (even with an empty value).
    pub fn present(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }
}

/// Split `params` into (reserved, filters): a pair whose key is in `keys` is
/// reserved; anything else is a caller filter (order/repeats preserved).
pub fn split_reserved(
    params: Vec<(String, String)>,
    keys: &[&str],
) -> (ReservedParams, Vec<(String, String)>) {
    let mut reserved: HashMap<String, Vec<String>> = HashMap::new();
    let mut filters = Vec::with_capacity(params.len());
    for (k, v) in params {
        if keys.contains(&k.as_str()) {
            reserved.entry(k).or_default().push(v);
        } else {
            filters.push((k, v));
        }
    }
    (ReservedParams(reserved), filters)
}

/// Split a comma-separated list value, dropping empty elements (`"a,,b,"` -> `[a, b]`).
pub fn comma_list(v: &str) -> Vec<String> {
    v.split(',')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

/// Parse the `_ids` object-set input: absent -> empty (no scoping); present with no
/// non-empty element -> the routes' shared 400 message.
pub fn parse_ids(raw: Option<&str>) -> Result<Vec<String>, &'static str> {
    match raw {
        None => Ok(Vec::new()),
        Some(v) => {
            let ids = comma_list(v);
            if ids.is_empty() {
                Err("_ids requires at least one value")
            } else {
                Ok(ids)
            }
        }
    }
}

/// Parse a `depth` value: absent -> `default`; present-but-unparsable -> the routes'
/// shared 400 message. Range policy stays with the caller (the graph routes cap at
/// `MAX_GRAPH_DEPTH`; lineage delegates to the capability's cap).
pub fn parse_depth(raw: Option<&str>, default: u32) -> Result<u32, &'static str> {
    match raw {
        None => Ok(default),
        // Named binding, not `|_|` — the enforced clippy::map_err_ignore lint
        // rejects a wildcard closure param (see engine_client.rs:59 precedent).
        Some(v) => v
            .parse::<u32>()
            .map_err(|_parse_err| "depth must be a positive integer"),
    }
}
