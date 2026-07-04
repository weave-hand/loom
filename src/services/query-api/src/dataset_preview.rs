//! Shape a served `Rows` (raw dataset sample) into the `/datasets/*/preview` wire body.
//! Display-only: every cell is rendered to a string, so the UI needs no type vocabulary.

use crate::serving::{SqlValue, iso_date, iso_timestamp};

/// Render one sampled cell to its display string. `Null` → `""`.
#[must_use]
pub fn cell_string(v: &SqlValue) -> String {
    match v {
        SqlValue::Null => String::new(),
        SqlValue::Text(s) => s.clone(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        SqlValue::Double(d) => d.to_string(),
        SqlValue::Date(d) => iso_date(d),
        SqlValue::Timestamp(t) => iso_timestamp(t),
    }
}

/// `{ "columns": [..], "rows": [[..]], "sampled": true }`.
#[must_use]
pub fn preview_body(rows: &crate::serving::Rows) -> serde_json::Value {
    let out_rows: Vec<serde_json::Value> = rows
        .rows
        .iter()
        .map(|r| {
            serde_json::Value::Array(
                r.iter()
                    .map(|c| serde_json::Value::String(cell_string(c)))
                    .collect(),
            )
        })
        .collect();
    serde_json::json!({
        "columns": rows.columns,
        "rows": out_rows,
        "sampled": true,
    })
}
