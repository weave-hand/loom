//! Typed-object wire rendering: turn a governed `ObjectRows` into the JSON the query
//! front door serves, with each value shaped by its property's logical type (the core
//! vocabulary). Pure — no I/O. A `Null` cell is always JSON `null`; an unknown logical
//! type or a declared/value mismatch falls back to the cell's natural rendering rather
//! than failing a permitted read.

use control_plane_core::{JsonRepr, json_repr_of};
use serde_json::{Value, json};

use crate::handler::{Associations, ObjectRows};
use crate::serving::{SqlValue, iso_date, iso_timestamp};

/// `{ "objects": [ { property: typed_value, ... }, ... ] }`. Keys are the projected
/// column names in `columns` order; values are rendered per the aligned logical type.
pub fn objects_to_json(rows: &ObjectRows) -> Value {
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
    json!({ "objects": objects })
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
