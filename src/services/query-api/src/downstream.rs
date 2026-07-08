//! Resolve action `downstream` [`JobTemplate`]s to concrete [`NewJob`]s at
//! action-invoke time.
//!
//! A `downstream` template's `payload` is a JSON tree whose string leaves
//! beginning with `@` are references into the action's primary-step resolved row:
//! `@self.<prop>` or the short form `@<prop>`. [`resolve_downstream`] walks each
//! template, substitutes every ref with the JSON form of the matching cell, leaves
//! literals (and any ref that doesn't resolve) untouched, and emits one
//! [`NewJob`] per template. It is pure logic — no I/O — and never panics: an
//! unresolved ref (define-time validation should have ruled it out, but the
//! resolver sees only `self_row`, which may differ) is left as the literal string
//! so the omission is observable rather than a hard crash.

use control_plane_core::{JobTemplate, NewJob};

use crate::render::natural;
use crate::serving::SqlValue;

/// Resolve `templates` against `self_row`, producing one [`NewJob`] per template
/// with `run_at: None` and `priority: 0`.
///
/// `self_row` is the primary step's resolved row as `(column_name, value)` pairs.
/// A ref matches the first pair whose column name equals the ref's name.
pub fn resolve_downstream(
    templates: &[JobTemplate],
    self_row: &[(String, SqlValue)],
) -> Vec<NewJob> {
    templates
        .iter()
        .map(|t| NewJob {
            kind: t.kind.clone(),
            payload: resolve_value(&t.payload, self_row),
            run_at: None,
            priority: 0,
        })
        .collect()
}

/// Recursively resolve refs in one JSON node. Literals pass through; an
/// unresolved ref is returned as-is (the original string leaf).
fn resolve_value(v: &serde_json::Value, self_row: &[(String, SqlValue)]) -> serde_json::Value {
    match v {
        serde_json::Value::String(s) if s.starts_with('@') => {
            // Strip the leading `@`, then an optional `self.` prefix. The match
            // guard guarantees the `@`, so `strip_prefix` returns `Some`.
            let after_at = s.strip_prefix('@').unwrap_or(s.as_str());
            let name = after_at.strip_prefix("self.").unwrap_or(after_at);
            match self_row.iter().find(|(col, _)| col == name) {
                Some((_, val)) => natural(val),
                None => v.clone(),
            }
        }
        serde_json::Value::Object(m) => serde_json::Value::Object(
            m.iter()
                .map(|(k, vv)| (k.clone(), resolve_value(vv, self_row)))
                .collect(),
        ),
        serde_json::Value::Array(a) => {
            serde_json::Value::Array(a.iter().map(|vv| resolve_value(vv, self_row)).collect())
        }
        other => other.clone(),
    }
}
