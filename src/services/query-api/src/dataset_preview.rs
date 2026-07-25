//! Compile the governed preview SELECT and shape a served `Rows` (raw dataset sample)
//! into the `/datasets/*/preview` wire body. Display-only: every cell is rendered to a
//! string, so the UI needs no type vocabulary.

use crate::governed::{GovernedType, Projection};
use crate::handler::QueryError;
use crate::serving::{SqlValue, iso_date, iso_timestamp};
use crate::sql::{SelectInputs, SqlDialect, compile_select_with};

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

/// Compile the governed preview SELECT for a type-governed dataset read: the type's
/// visible projection (declared properties minus denied; masked rendered as the mask
/// marker) with the subject's row filters ANDed, `LIMIT limit`. Fail-closed: zero
/// visible columns is `QueryError::Forbidden` (the caller maps it to the canonical
/// dataset 404), and a malformed persisted filter surfaces as the compile error.
pub fn governed_preview_sql(
    dialect: &dyn SqlDialect,
    g: &GovernedType,
    limit: u32,
) -> Result<(String, Vec<SqlValue>), QueryError> {
    let proj = Projection::visible(g)?;
    let (sql, params) = compile_select_with(
        dialect,
        &g.otype.table,
        &proj.columns,
        &proj.masked,
        &SelectInputs {
            row_filters: &g.row_filters,
            ..SelectInputs::default()
        },
        None,
        limit,
    )?;
    Ok((sql, params))
}
