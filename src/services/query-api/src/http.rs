//! Thin axum surface. All logic is in handler::read_object; this layer only maps
//! HTTP <-> the core and serializes Rows to JSON.

use std::sync::Arc;

use crate::handler::{
    Associations, ChainQuery, Hop, ObjectQuery, QueryDeps, QueryError, Subject, read_associations,
    read_linked_chain, read_object,
};
use crate::path_parse::{parse_direction, parse_path_hops};
use crate::serving::{ActionEngine, ServingEngine};
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};
use control_plane_core::{ControlPlane, SubjectId};

/// Shared, owned dependencies. Holds the control plane as one object-safe facade
/// (`Arc<dyn ControlPlane>`) and hands its narrow concern objects to the read path.
#[derive(Clone)]
pub struct AppState {
    pub cp: Arc<dyn ControlPlane>,
    pub serving: Arc<dyn ServingEngine>,
    pub action_engine: Arc<dyn ActionEngine>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/objects/:type_name", get(get_object))
        .route("/objects/:from_type/links/:link_name", get(get_linked))
        .route("/objects/:from_type/links", get(get_linked_chain))
        .route("/actions/:action_name", post(post_action))
        .with_state(state)
}

async fn get_object(
    State(st): State<AppState>,
    Path(type_name): Path<String>,
    Query(params): Query<Vec<(String, String)>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let subject = headers
        .get("X-Loom-Subject")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();
    // Repeated keys are preserved (a column may carry several predicates, e.g. a range);
    // the handler parses each value's operator and coerces it.
    let eq_filters = params;
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
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
        Ok(rows) => Json(crate::render::objects_to_json(&rows)).into_response(),
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

async fn get_linked(
    State(st): State<AppState>,
    Path((from_type, link_name)): Path<(String, String)>,
    Query(params): Query<Vec<(String, String)>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let subject = headers
        .get("X-Loom-Subject")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();
    // Pull `_direction` (single-hop knob) and `_shape` out of the params; the rest are filters.
    let mut direction_raw: Option<String> = None;
    let mut shape: Option<String> = None;
    let mut filter_params: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        match k.as_str() {
            "_direction" => direction_raw = Some(v),
            "_shape" => shape = Some(v),
            _ => filter_params.push((k, v)),
        }
    }
    let direction = match parse_direction(direction_raw.as_deref()) {
        Ok(d) => d,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    // Resolve filter keys against the single-link path: bare -> source (t_0), `<link>.col`
    // -> target (t_1). A bad prefix -> 400. (Filter keys use the bare link name.)
    let filters = match crate::chain_filter::resolve_chain_filters(
        std::slice::from_ref(&link_name),
        filter_params,
    ) {
        Ok(f) => f,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
    };
    let query = ChainQuery {
        from_type,
        path: vec![Hop {
            link: link_name,
            direction,
        }],
        filters,
    };
    let subj = Subject(SubjectId(subject));
    match shape.as_deref() {
        None | Some("objects") => respond_objects(read_linked_chain(&query, &subj, &deps).await),
        Some("association") => respond_associations(read_associations(&query, &subj, &deps).await),
        Some(other) => (StatusCode::BAD_REQUEST, format!("unknown shape: {other}")).into_response(),
    }
}

async fn get_linked_chain(
    State(st): State<AppState>,
    Path(from_type): Path<String>,
    Query(params): Query<Vec<(String, String)>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let subject = headers
        .get("X-Loom-Subject")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();
    // `_path` is the comma-separated ordered chain of (optionally `~`-inverse) link names;
    // every other pair is a filter. Repeated filter keys are preserved (e.g. a range).
    let mut hops: Vec<Hop> = Vec::new();
    let mut shape: Option<String> = None;
    let mut filter_params: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        match k.as_str() {
            "_path" => hops = parse_path_hops(&v),
            "_shape" => shape = Some(v),
            _ => filter_params.push((k, v)),
        }
    }
    // Filter keys reference bare link names; resolve against those (direction-independent).
    let names: Vec<String> = hops.iter().map(|h| h.link.clone()).collect();
    let filters = match crate::chain_filter::resolve_chain_filters(&names, filter_params) {
        Ok(f) => f,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
    };
    let query = ChainQuery {
        from_type,
        path: hops,
        filters,
    };
    let subj = Subject(SubjectId(subject));
    match shape.as_deref() {
        None | Some("objects") => respond_objects(read_linked_chain(&query, &subj, &deps).await),
        Some("association") => respond_associations(read_associations(&query, &subj, &deps).await),
        Some(other) => (StatusCode::BAD_REQUEST, format!("unknown shape: {other}")).into_response(),
    }
}

fn respond_objects(
    res: Result<crate::handler::ObjectRows, QueryError>,
) -> axum::response::Response {
    match res {
        Ok(rows) => Json(crate::render::objects_to_json(&rows)).into_response(),
        Err(e) => chain_error(e),
    }
}

fn respond_associations(res: Result<Associations, QueryError>) -> axum::response::Response {
    match res {
        Ok(a) => Json(crate::render::associations_to_json(&a)).into_response(),
        Err(e) => chain_error(e),
    }
}

/// Shared HTTP mapping for chain/association read errors.
fn chain_error(e: QueryError) -> axum::response::Response {
    match e {
        QueryError::UnknownType(t) => (StatusCode::NOT_FOUND, t).into_response(),
        QueryError::UnknownLink(l) => (StatusCode::NOT_FOUND, l).into_response(),
        QueryError::AmbiguousLink(l) => (StatusCode::BAD_REQUEST, l).into_response(),
        QueryError::BadChain(m) => (StatusCode::BAD_REQUEST, m).into_response(),
        QueryError::NoIdentity(t) => (StatusCode::BAD_REQUEST, t).into_response(),
        QueryError::Forbidden => StatusCode::FORBIDDEN.into_response(),
        QueryError::BadFilter(c) => (StatusCode::BAD_REQUEST, c).into_response(),
        // Opaque body for backend/serving faults (no internal detail leaked).
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}

async fn post_action(
    State(st): State<AppState>,
    Path(action_name): Path<String>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let subject = headers
        .get("X-Loom-Subject")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();
    let obj = match body.as_object() {
        Some(m) => m.clone(),
        None => return (StatusCode::BAD_REQUEST, "body must be a JSON object").into_response(),
    };
    let deps = crate::action::ActionDeps {
        cp: st.cp.as_ref(),
        action_engine: st.action_engine.as_ref(),
    };
    match crate::action::run_action(&action_name, &obj, &SubjectId(subject), &deps).await {
        Ok(rows) => {
            let body = crate::render::objects_to_json(&rows);
            // objects_to_json yields {"objects":[{...}]}; return the single created object.
            let one = body
                .get("objects")
                .and_then(|a| a.as_array())
                .and_then(|a| a.first())
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            (StatusCode::CREATED, Json(one)).into_response()
        }
        Err(crate::action::ActionError::UnknownAction(a)) => {
            (StatusCode::NOT_FOUND, a).into_response()
        }
        Err(crate::action::ActionError::Forbidden) => StatusCode::FORBIDDEN.into_response(),
        Err(crate::action::ActionError::BadParams(e)) => {
            (StatusCode::BAD_REQUEST, e.to_string()).into_response()
        }
        // Opaque body for backend/serving faults (no internal detail leaked).
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}
