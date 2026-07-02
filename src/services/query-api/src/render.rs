//! Typed-object wire rendering: turn a governed `ObjectRows` into the JSON the query
//! front door serves, with each value shaped by its property's logical type (the core
//! vocabulary). Pure — no I/O. A `Null` cell is always JSON `null`; an unknown logical
//! type or a declared/value mismatch falls back to the cell's natural rendering rather
//! than failing a permitted read.

use control_plane_core::{Cursor, JsonRepr, json_repr_of};
use serde_json::{Value, json};

use crate::handler::{Associations, ObjectRows, ObjectTree};
use crate::serving::{SqlValue, iso_date, iso_timestamp};

/// `{ "objects": [ { property: typed_value, ... }, ... ], "next": <string>|null }`. Keys are
/// the projected column names in `columns` order; values are rendered per the aligned
/// logical type. `next` is the keyset cursor for the following page (the un-paginated read
/// path passes `None`, which always renders as JSON `null`).
pub fn objects_to_json(rows: &ObjectRows, next: Option<&Cursor>) -> Value {
    let objects: Vec<Value> = rows
        .rows
        .iter()
        .map(|row| {
            let mut obj = serde_json::Map::with_capacity(rows.columns.len());
            for (i, col) in rows.columns.iter().enumerate() {
                let logical_ty = rows.logical_types.get(i).map(String::as_str).unwrap_or("");
                // Bounds-safe: a short row (fewer cells than declared columns) renders
                // the missing cell as null rather than panicking — a permitted read must
                // never 500 on a serving-engine surprise.
                let cell = row.get(i).unwrap_or(&SqlValue::Null);
                obj.insert(col.clone(), render_cell(logical_ty, cell));
            }
            Value::Object(obj)
        })
        .collect();
    json!({ "objects": objects, "next": next.map(|c| c.0.clone()) })
}

/// `{ "associations": [ { "from": <typed id>, "to": <typed id> }, ... ] }`. Each id is
/// rendered by its identity property's logical type.
pub fn associations_to_json(a: &Associations) -> Value {
    let assocs: Vec<Value> = a
        .pairs
        .iter()
        .map(|(from, to)| {
            json!({
                "from": render_cell(&a.from_id_type, from),
                "to": render_cell(&a.to_id_type, to),
            })
        })
        .collect();
    json!({ "associations": assocs })
}

/// `{ "roots": [<id>, ...], "nodes": [ { "id", "depth", "parent", "object": {..} }, ... ] }`.
/// Roots are the parentless nodes (in node order, which is `(depth, id)`). `id`/`parent` are
/// rendered through the identity property's logical type (`parent` is JSON `null` for a root);
/// `object` is the governed typed projection, rendered exactly as `objects_to_json` does a row.
pub fn tree_to_json(tree: &ObjectTree) -> Value {
    let mut roots: Vec<Value> = Vec::new();
    let nodes: Vec<Value> = tree
        .nodes
        .iter()
        .map(|n| {
            let mut obj = serde_json::Map::with_capacity(tree.columns.len());
            for (i, col) in tree.columns.iter().enumerate() {
                let logical_ty = tree.logical_types.get(i).map(String::as_str).unwrap_or("");
                let cell = n.cells.get(i).unwrap_or(&SqlValue::Null);
                obj.insert(col.clone(), render_cell(logical_ty, cell));
            }
            let id_json = render_cell(&tree.identity_type, &n.id);
            let parent_json = render_cell(&tree.identity_type, &n.parent);
            if matches!(n.parent, SqlValue::Null) {
                roots.push(id_json.clone());
            }
            json!({
                "id": id_json,
                "depth": n.depth,
                "parent": parent_json,
                "object": Value::Object(obj),
            })
        })
        .collect();
    json!({ "roots": roots, "nodes": nodes })
}

/// Render one cell as JSON, driven by its declared logical type.
fn render_cell(logical_ty: &str, cell: &SqlValue) -> Value {
    if matches!(cell, SqlValue::Null) {
        return Value::Null;
    }
    match json_repr_of(logical_ty) {
        Ok(repr) => render_typed(repr, cell),
        Err(_) => natural(cell), // unknown logical type
    }
}

/// Render per the declared repr; on a repr/value mismatch, fall back to natural.
fn render_typed(repr: JsonRepr, cell: &SqlValue) -> Value {
    match (repr, cell) {
        (JsonRepr::Number, SqlValue::Int(i)) => json!(i),
        (JsonRepr::Number, SqlValue::Double(f)) => json!(f),
        (JsonRepr::NumericString, SqlValue::Int(i)) => json!(i.to_string()),
        (JsonRepr::Bool, SqlValue::Bool(b)) => json!(b),
        (JsonRepr::PlainString, SqlValue::Text(s)) => json!(s),
        (JsonRepr::IsoDate, SqlValue::Date(d)) => json!(iso_date(d)),
        (JsonRepr::IsoTimestamp, SqlValue::Timestamp(ts)) => json!(iso_timestamp(ts)),
        _ => natural(cell),
    }
}

/// Best-effort rendering by the cell's own variant — the shared fallback for unknown
/// types and declared/value mismatches.
fn natural(cell: &SqlValue) -> Value {
    match cell {
        SqlValue::Null => Value::Null,
        SqlValue::Int(i) => json!(i),
        SqlValue::Double(f) => json!(f),
        SqlValue::Bool(b) => json!(b),
        SqlValue::Text(s) => json!(s),
        SqlValue::Date(d) => json!(iso_date(d)),
        SqlValue::Timestamp(ts) => json!(iso_timestamp(ts)),
    }
}
