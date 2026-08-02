//! The governed ad-hoc SQL console: a client-facing HTTP endpoint that runs a
//! bearer-authenticated subject's arbitrary read-only SQL under their
//! server-resolved [`control_plane_core::GovernedCatalog`], reusing the engine's
//! `execute_governed` substrate. Governance and read-only-ness are enforced in the
//! engine by construction (only governed read providers register; there is no
//! persist path); this module resolves the catalog server-side (never from the
//! wire), caps the result, and shapes the JSON body.

use axum::extract::{Json, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Deserialize;
use service_runtime::Subject;

use crate::governed::resolve_governed_catalog;
use crate::handler::QueryError;
use crate::http::{AppState, query_error_response};
use crate::openapi::SqlQueryResponse;
use crate::serving::{GovernedRows, Rows};

/// Default row cap when the caller supplies no `limit` — bounds a naive `SELECT *`
/// with no `LIMIT`.
const DEFAULT_SQL_ROWS: usize = 1_000;
/// Hard cap on the caller-supplied `limit` — bounds buffered result size.
const MAX_SQL_ROWS: usize = 10_000;

/// The `POST /sql` request body: arbitrary read-only SQL plus an optional row cap.
/// `deny_unknown_fields` so a typo'd/extraneous field is a 400, not silently ignored.
/// The governed catalog is NEVER accepted from the wire — it is resolved server-side
/// from the authenticated subject (see [`run_sql`]).
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SqlQueryRequest {
    pub sql: String,
    #[serde(default)]
    pub limit: Option<u32>,
}

/// `POST /sql` — run the authenticated subject's arbitrary read-only SQL under their
/// governed catalog, returning `{columns, rows, truncated}`. Governance and
/// read-only-ness are enforced in the engine by construction; this handler resolves
/// the per-subject `GovernedCatalog` server-side (never from the body), caps the
/// result, and shapes the JSON. A plan/validation fault (bad syntax, or a table the
/// subject cannot see — the engine's closed-world catalog makes an ungranted table
/// indistinguishable from a nonexistent one) is a legitimate client `400` carrying the
/// engine's plan message, which is safe to echo (it is the caller's own SQL vocabulary);
/// a backend fault is an opaque `500` with the detail logged server-side. A statement
/// that exceeds the engine's per-statement memory or wall-clock budget is a 429
/// carrying the budget name — valid SQL, too expensive.
#[utoipa::path(
    post, path = "/sql",
    request_body = SqlQueryRequest,
    responses(
        (status = 200, description = "Governed result rows", body = SqlQueryResponse),
        (status = 400, description = "Empty/malformed SQL, a rejected write (DDL/DML/COPY), or a table not visible to the subject"),
        (status = 429, description = "Statement exceeded its engine memory or wall-clock budget — narrow the query and retry"),
        (status = 500, description = "Serving error"),
    ),
    security(("bearer_auth" = [])),
    tag = "sql",
)]
pub async fn run_sql(
    State(st): State<AppState>,
    subject: Subject,
    Json(req): Json<SqlQueryRequest>,
) -> axum::response::Response {
    let sql = req.sql.trim().to_string();
    if sql.is_empty() {
        return (StatusCode::BAD_REQUEST, "empty sql").into_response();
    }
    let max_rows = req
        .limit
        .map_or(DEFAULT_SQL_ROWS, |n| (n as usize).clamp(1, MAX_SQL_ROWS));

    let catalog = match resolve_governed_catalog(st.cp.ontology(), st.cp.acl(), &subject.0).await {
        Ok(c) => c,
        Err(e) => return query_error_response(e, "sql console catalog"),
    };
    match st.serving.execute_governed(sql, catalog, max_rows).await {
        Ok(gr) => Json(result_body(&gr)).into_response(),
        Err(e) => query_error_response(QueryError::Serving(e), "sql console execute"),
    }
}

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
