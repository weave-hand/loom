//! Thin axum surface. All logic is in handler::read_object; this layer only maps
//! HTTP <-> the core and serializes Rows to JSON.

use std::sync::Arc;

use crate::handler::{
    Associations, ChainQuery, GraphQuery, GraphTailQuery, GraphUnionQuery, Hop, ObjectQuery,
    QueryDeps, QueryError, Subject, read_associations, read_graph_reach, read_graph_reach_union,
    read_graph_reach_with_tail, read_linked_chain, read_object,
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
        .route("/objects/:type_name/graph/:link_name", get(get_graph))
        .route("/objects/:type_name/graph", get(get_graph_path))
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
    // Pull the `_ids` object-set input out of the params; the rest are filters. Repeated
    // filter keys are preserved (a column may carry several predicates, e.g. a range); the
    // handler parses each value's operator and coerces it.
    let mut ids: Vec<String> = Vec::new();
    let mut eq_filters: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        if k == "_ids" {
            ids = v
                .split(',')
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
            if ids.is_empty() {
                return (StatusCode::BAD_REQUEST, "_ids requires at least one value")
                    .into_response();
            }
        } else {
            eq_filters.push((k, v));
        }
    }
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
    };
    match read_object(
        &ObjectQuery {
            type_name,
            eq_filters,
            ids,
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
        Err(QueryError::NoIdentity(t)) => (StatusCode::BAD_REQUEST, t).into_response(),
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
    // Pull `_direction` (single-hop knob), `_shape`, and `_ids` out of the params; the rest
    // are filters.
    let mut direction_raw: Option<String> = None;
    let mut shape: Option<String> = None;
    let mut ids: Vec<String> = Vec::new();
    let mut ids_present = false;
    let mut filter_params: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        match k.as_str() {
            "_direction" => direction_raw = Some(v),
            "_shape" => shape = Some(v),
            "_ids" => {
                ids_present = true;
                ids = v
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect();
            }
            _ => filter_params.push((k, v)),
        }
    }
    if ids_present && ids.is_empty() {
        return (StatusCode::BAD_REQUEST, "_ids requires at least one value").into_response();
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
        ids,
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
    let mut ids: Vec<String> = Vec::new();
    let mut ids_present = false;
    let mut filter_params: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        match k.as_str() {
            "_path" => hops = parse_path_hops(&v),
            "_shape" => shape = Some(v),
            "_ids" => {
                ids_present = true;
                ids = v
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect();
            }
            _ => filter_params.push((k, v)),
        }
    }
    if ids_present && ids.is_empty() {
        return (StatusCode::BAD_REQUEST, "_ids requires at least one value").into_response();
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
        ids,
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

const MAX_GRAPH_DEPTH: u32 = 10;
const DEFAULT_GRAPH_DEPTH: u32 = 5;

async fn get_graph(
    State(st): State<AppState>,
    Path((type_name, link_name)): Path<(String, String)>,
    Query(params): Query<Vec<(String, String)>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let subject = headers
        .get("X-Loom-Subject")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();
    // Pull `depth` and `_ids` out; the rest are seed filters.
    let mut depth = DEFAULT_GRAPH_DEPTH;
    let mut ids: Vec<String> = Vec::new();
    let mut ids_present = false;
    let mut filters: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        match k.as_str() {
            "depth" => match v.parse::<u32>() {
                Ok(d) => depth = d,
                Err(_) => {
                    return (StatusCode::BAD_REQUEST, "depth must be a positive integer")
                        .into_response();
                }
            },
            "_ids" => {
                ids_present = true;
                ids = v
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect();
            }
            _ => filters.push((k, v)),
        }
    }
    if ids_present && ids.is_empty() {
        return (StatusCode::BAD_REQUEST, "_ids requires at least one value").into_response();
    }
    if !(1..=MAX_GRAPH_DEPTH).contains(&depth) {
        return (
            StatusCode::BAD_REQUEST,
            format!("depth must be 1..={MAX_GRAPH_DEPTH}"),
        )
            .into_response();
    }
    graph_respond(
        &st,
        type_name,
        vec![link_name],
        depth,
        filters,
        ids,
        subject,
    )
    .await
}

/// Multi-link `?path=l1,l2` path-cycle route. Parses `?path=` (comma-split; empty/absent ->
/// 400) plus the same `depth`/`_ids`/filter handling as `get_graph`, then shares the
/// `read_graph_reach` call + error mapping via `graph_respond`.
async fn get_graph_path(
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
    let mut depth = DEFAULT_GRAPH_DEPTH;
    let mut ids: Vec<String> = Vec::new();
    let mut ids_present = false;
    let mut path: Vec<String> = Vec::new();
    let mut links: Vec<String> = Vec::new();
    let mut filters: Vec<(String, String)> = Vec::with_capacity(params.len());
    for (k, v) in params {
        match k.as_str() {
            "path" => {
                path = v
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect()
            }
            "links" => {
                links = v
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect()
            }
            "depth" => match v.parse::<u32>() {
                Ok(d) => depth = d,
                Err(_) => {
                    return (StatusCode::BAD_REQUEST, "depth must be a positive integer")
                        .into_response();
                }
            },
            "_ids" => {
                ids_present = true;
                ids = v
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect();
            }
            _ => filters.push((k, v)),
        }
    }
    if ids_present && ids.is_empty() {
        return (StatusCode::BAD_REQUEST, "_ids requires at least one value").into_response();
    }
    if !(1..=MAX_GRAPH_DEPTH).contains(&depth) {
        return (
            StatusCode::BAD_REQUEST,
            format!("depth must be 1..={MAX_GRAPH_DEPTH}"),
        )
            .into_response();
    }
    // Exactly one of `path` (ordered cycle / `*` recursive-core+tail) or `links` (self-link
    // union) selects the mode.
    if !path.is_empty() && !links.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "specify either path or links, not both",
        )
            .into_response();
    }
    if !links.is_empty() {
        return graph_union_respond(&st, type_name, links, depth, filters, ids, subject).await;
    }
    if path.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "path or links requires at least one link",
        )
            .into_response();
    }
    // Part B: a `*`-suffixed segment marks a recursive core followed by a relational tail. The
    // `*` MUST be on exactly one segment, and that segment MUST be the path prefix (index 0).
    let starred: Vec<usize> = path
        .iter()
        .enumerate()
        .filter(|(_, s)| s.ends_with('*'))
        .map(|(i, _)| i)
        .collect();
    if !starred.is_empty() {
        if starred.len() > 1 {
            return (
                StatusCode::BAD_REQUEST,
                "at most one path segment may be marked recursive with `*`",
            )
                .into_response();
        }
        if starred[0] != 0 {
            return (
                StatusCode::BAD_REQUEST,
                "the recursive `*` segment must be the first path segment",
            )
                .into_response();
        }
        let core_link = path[0].trim_end_matches('*').to_string();
        let tail_links: Vec<String> = path[1..].to_vec();
        return graph_tail_respond(
            &st, type_name, core_link, tail_links, depth, filters, ids, subject,
        )
        .await;
    }
    graph_respond(&st, type_name, path, depth, filters, ids, subject).await
}

/// Shared HTTP mapping for graph reachability read errors (path-cycle and union).
fn graph_error(e: QueryError) -> axum::response::Response {
    match e {
        QueryError::UnknownType(t) => (StatusCode::NOT_FOUND, t).into_response(),
        QueryError::UnknownLink(l) => (StatusCode::NOT_FOUND, l).into_response(),
        QueryError::NotCyclicPath(p) => (StatusCode::BAD_REQUEST, p).into_response(),
        QueryError::BadGraphPath(m) => (StatusCode::BAD_REQUEST, m).into_response(),
        QueryError::NoIdentity(t) => (StatusCode::BAD_REQUEST, t).into_response(),
        QueryError::BadFilter(c) => (StatusCode::BAD_REQUEST, c).into_response(),
        QueryError::Forbidden => StatusCode::FORBIDDEN.into_response(),
        // Opaque body for backend/serving faults (no internal detail leaked).
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}

/// Path-cycle (`?path=` / single `/graph/:link`) tail: build a `GraphQuery`, run
/// `read_graph_reach`, map via `graph_error`.
async fn graph_respond(
    st: &AppState,
    type_name: String,
    path: Vec<String>,
    depth: u32,
    filters: Vec<(String, String)>,
    ids: Vec<String>,
    subject: String,
) -> axum::response::Response {
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
    };
    match read_graph_reach(
        &GraphQuery {
            type_name,
            path,
            depth,
            filters,
            ids,
        },
        &Subject(SubjectId(subject)),
        &deps,
    )
    .await
    {
        Ok(rows) => Json(crate::render::objects_to_json(&rows)).into_response(),
        Err(e) => graph_error(e),
    }
}

/// Union (`?links=`) tail: build a `GraphUnionQuery`, run `read_graph_reach_union`, map via
/// `graph_error`.
async fn graph_union_respond(
    st: &AppState,
    type_name: String,
    links: Vec<String>,
    depth: u32,
    filters: Vec<(String, String)>,
    ids: Vec<String>,
    subject: String,
) -> axum::response::Response {
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
    };
    match read_graph_reach_union(
        &GraphUnionQuery {
            type_name,
            links,
            depth,
            filters,
            ids,
        },
        &Subject(SubjectId(subject)),
        &deps,
    )
    .await
    {
        Ok(rows) => Json(crate::render::objects_to_json(&rows)).into_response(),
        Err(e) => graph_error(e),
    }
}

/// Recursive-core + relational-tail (`?path=l0*,l1,…`) tail: build a `GraphTailQuery`, run
/// `read_graph_reach_with_tail`, map via `graph_error`.
#[allow(clippy::too_many_arguments)]
async fn graph_tail_respond(
    st: &AppState,
    type_name: String,
    core_link: String,
    tail_links: Vec<String>,
    depth: u32,
    filters: Vec<(String, String)>,
    ids: Vec<String>,
    subject: String,
) -> axum::response::Response {
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
    };
    match read_graph_reach_with_tail(
        &GraphTailQuery {
            type_name,
            core_link,
            tail_links,
            depth,
            filters,
            ids,
        },
        &Subject(SubjectId(subject)),
        &deps,
    )
    .await
    {
        Ok(rows) => Json(crate::render::objects_to_json(&rows)).into_response(),
        Err(e) => graph_error(e),
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
