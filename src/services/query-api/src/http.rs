//! Thin axum surface. All logic is in handler::read_object; this layer only maps
//! HTTP <-> the core and serializes Rows to JSON.

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json};
use axum::routing::get;
use control_plane_core::{Acl, Ontology, SubjectId};
use serde_json::json;

use crate::handler::{ObjectQuery, QueryDeps, QueryError, Subject, read_object};
use crate::serving::{Rows, ServingEngine, SqlValue};

/// Shared, owned dependencies (the 'static analog of handler::QueryDeps).
#[derive(Clone)]
pub struct AppState {
    pub ontology: Arc<dyn Ontology + Send + Sync>,
    pub acl: Arc<dyn Acl + Send + Sync>,
    pub serving: Arc<dyn ServingEngine>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/objects/:type_name", get(get_object))
        .with_state(state)
}

async fn get_object(
    State(st): State<AppState>,
    Path(type_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let subject = headers
        .get("X-Loom-Subject")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();
    // Slice limitation: every query-param filter binds as Text; typed (int/bool)
    // filters are a later spec. A text param against a typed column may simply match
    // no rows rather than error.
    let eq_filters = params
        .into_iter()
        .map(|(k, v)| (k, SqlValue::Text(v)))
        .collect();
    let deps = QueryDeps {
        ontology: st.ontology.as_ref(),
        acl: st.acl.as_ref(),
        serving: st.serving.as_ref(),
    };
    match read_object(
        &ObjectQuery {
            type_name,
            eq_filters,
        },
        &Subject(SubjectId(subject)),
        &deps,
    )
    .await
    {
        Ok(rows) => Json(rows_to_json(&rows)).into_response(),
        Err(QueryError::UnknownType(t)) => (StatusCode::NOT_FOUND, t).into_response(),
        Err(QueryError::Forbidden) => StatusCode::FORBIDDEN.into_response(),
        Err(QueryError::BadFilter(c)) => (StatusCode::BAD_REQUEST, c).into_response(),
        // Return an opaque body for backend/serving faults: a governance-fronted
        // service must not echo internal error detail (SQL fragments, table/column
        // names) to the client. TODO(serving-tier): log `e` server-side once a
        // tracing subscriber is wired in the binary.
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}

fn rows_to_json(rows: &Rows) -> serde_json::Value {
    let cells = |r: &Vec<SqlValue>| -> Vec<serde_json::Value> {
        r.iter()
            .map(|c| match c {
                SqlValue::Text(s) => json!(s),
                SqlValue::Int(i) => json!(i),
                SqlValue::Bool(b) => json!(b),
                SqlValue::Double(f) => json!(f),
                SqlValue::Date(d) => json!(crate::serving::iso_date(d)),
                SqlValue::Timestamp(ts) => json!(crate::serving::iso_timestamp(ts)),
                SqlValue::Null => serde_json::Value::Null,
            })
            .collect()
    };
    json!({
        "columns": rows.columns,
        "rows": rows.rows.iter().map(cells).collect::<Vec<_>>(),
    })
}
