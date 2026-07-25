//! The governed ad-hoc SQL console: a client-facing HTTP endpoint that runs a
//! bearer-authenticated subject's arbitrary read-only SQL under their
//! server-resolved [`control_plane_core::GovernedCatalog`], reusing the engine's
//! `execute_governed` substrate. Governance and read-only-ness are enforced in the
//! engine by construction (only governed read providers register; there is no
//! persist path); this module resolves the catalog server-side (never from the
//! wire), caps the result, and shapes the JSON body.

use crate::serving::{GovernedRows, Rows};

/// Truncate `rows` to `max_rows`, reporting whether truncation occurred. Pure.
#[must_use]
pub fn truncate_rows(mut rows: Rows, max_rows: usize) -> GovernedRows {
    let truncated = rows.rows.len() > max_rows;
    if truncated {
        rows.rows.truncate(max_rows);
    }
    GovernedRows { rows, truncated }
}

/// Shape a governed result into the console JSON body: `columns` (names), `rows`
/// (each cell a display string, `""` for NULL), and `truncated`. Mirrors
/// [`crate::dataset_preview::preview_body`], reusing `cell_string`.
#[must_use]
pub fn result_body(gr: &GovernedRows) -> serde_json::Value {
    let rows: Vec<Vec<String>> = gr
        .rows
        .rows
        .iter()
        .map(|r| r.iter().map(crate::dataset_preview::cell_string).collect())
        .collect();
    serde_json::json!({
        "columns": gr.rows.columns,
        "rows": rows,
        "truncated": gr.truncated,
    })
}
