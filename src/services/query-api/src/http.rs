//! Thin axum surface. All logic is in handler::read_object; this layer only maps
//! HTTP <-> the core and serializes Rows to JSON.

use std::fmt::Display;
use std::sync::Arc;

use crate::handler::{
    Associations, ChainQuery, GraphQuery, GraphTailQuery, GraphUnionQuery, Hop, ObjectQuery,
    QueryDeps, QueryError, read_associations, read_graph_reach, read_graph_reach_union,
    read_graph_reach_with_tail, read_linked_chain, read_object,
};
use crate::openapi::{JobAck, ObjectsResponse, VectorSearchResponse, WriteDeniedBody};
use crate::path_parse::{parse_direction, parse_path_hops};
use crate::serving::{ActionEngine, ServingEngine};
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};
use control_plane_core::{ControlPlane, GC_JOB_KIND, NewJob};
use service_runtime::Subject;

/// Upper bound on a single `/search` request's `k` — caps per-request work.
pub const K_MAX: usize = 1000;

/// The `POST /search/:type/:index_name` request body. `deny_unknown_fields` so a typo'd
/// or extraneous field is a 400, not silently ignored. `query` is the kNN probe vector.
#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct VectorSearchRequest {
    pub query: Vec<f32>,
    pub k: usize,
    #[serde(default)]
    pub nprobe: Option<u32>,
    #[serde(default)]
    pub ef_search: Option<u32>,
}

/// Returns `Err(message)` for an out-of-range `k` or empty `query`. The message is
/// safe to return verbatim in a 400 body (no internal detail).
pub fn validate_search_request(req: &VectorSearchRequest) -> Result<(), String> {
    if req.query.is_empty() {
        return Err("query must be a non-empty f32 array".to_string());
    }
    if req.k == 0 || req.k > K_MAX {
        return Err(format!("k must be in 1..={K_MAX}"));
    }
    Ok(())
}

/// Log a backend/serving fault server-side, then return the opaque 500 the client
/// sees. The detail (`error = %e`) is for operators only — the response body
/// carries no internal detail (SQL fragments, table/column names).
fn internal_error(context: &str, e: impl Display) -> axum::response::Response {
    tracing::error!(error = %e, "{context}");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}

/// Shared, owned dependencies. Holds the control plane as one object-safe facade
/// (`Arc<dyn ControlPlane>`) and hands its narrow concern objects to the read path.
#[derive(Clone)]
pub struct AppState {
    pub cp: Arc<dyn ControlPlane>,
    pub serving: Arc<dyn ServingEngine>,
    pub action_engine: Arc<dyn ActionEngine>,
    pub default_limit: u32,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/objects/:type_name", get(get_object))
        .route("/objects/:from_type/links/:link_name", get(get_linked))
        .route("/objects/:from_type/links", get(get_linked_chain))
        .route("/objects/:type_name/graph/:link_name", get(get_graph))
        .route("/objects/:type_name/graph", get(get_graph_path))
        .route("/actions/:action_name", post(post_action))
        .route("/search/:type_name/:index_name", post(post_search))
        .route("/maintenance/gc/:schema/:table", post(enqueue_gc))
        .with_state(state)
}

/// Operator-triggered physical GC: enqueue a `gc_table` job for `(schema, table)`.
/// A zero-pool worker drains it via the engine's `GcTable` RPC. Returns 202 with
/// the job id; the actual reclamation runs asynchronously.
#[utoipa::path(
    post, path = "/maintenance/gc/{schema}/{table}",
    params(
        ("schema" = String, Path, description = "Iceberg schema"),
        ("table" = String, Path, description = "Table name"),
    ),
    responses(
        (status = 202, description = "GC job enqueued", body = JobAck),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "maintenance",
)]
async fn enqueue_gc(
    State(st): State<AppState>,
    Path((schema, table)): Path<(String, String)>,
) -> axum::response::Response {
    let job = NewJob {
        kind: GC_JOB_KIND.to_string(),
        payload: serde_json::json!({ "schema": schema, "name": table }),
        run_at: None,
        priority: 0,
    };
    match st.cp.queue().enqueue(job).await {
        Ok(id) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "job_id": id.0.to_string() })),
        )
            .into_response(),
        Err(e) => internal_error("enqueue gc_table", e),
    }
}

#[utoipa::path(
    get, path = "/objects/{type_name}",
    params(("type_name" = String, Path, description = "Ontology object type")),
    responses(
        (status = 200, description = "Matching objects", body = ObjectsResponse),
        (status = 400, description = "Bad filter or _ids"),
        (status = 403, description = "Forbidden by ACL policy"),
        (status = 404, description = "Unknown type"),
    ),
    security(("bearer_auth" = [])),
    tag = "objects",
)]
async fn get_object(
    State(st): State<AppState>,
    Path(type_name): Path<String>,
    Query(params): Query<Vec<(String, String)>>,
    subject: Subject,
) -> impl IntoResponse {
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
        default_limit: st.default_limit,
    };
    match read_object(
        &ObjectQuery {
            type_name,
            eq_filters,
            ids,
        },
        &subject,
        &deps,
    )
    .await
    {
        Ok(rows) => Json(crate::render::objects_to_json(&rows)).into_response(),
        Err(QueryError::UnknownType(t)) => (StatusCode::NOT_FOUND, t).into_response(),
        Err(QueryError::Forbidden) => StatusCode::FORBIDDEN.into_response(),
        Err(QueryError::BadFilter(c)) => (StatusCode::BAD_REQUEST, c).into_response(),
        Err(QueryError::BadFilterValue(e)) => bad_filter_value_response(&e),
        Err(QueryError::NoIdentity(t)) => (StatusCode::BAD_REQUEST, t).into_response(),
        Err(e) => internal_error("object read serving fault", e),
    }
}

#[utoipa::path(
    get, path = "/objects/{from_type}/links/{link_name}",
    params(
        ("from_type" = String, Path, description = "Source object type"),
        ("link_name" = String, Path, description = "Link to traverse"),
    ),
    responses(
        (status = 200, description = "Linked objects or associations", body = ObjectsResponse),
        (status = 400, description = "Bad direction/shape/filter"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Unknown type or link"),
    ),
    security(("bearer_auth" = [])),
    tag = "links",
)]
async fn get_linked(
    State(st): State<AppState>,
    Path((from_type, link_name)): Path<(String, String)>,
    Query(params): Query<Vec<(String, String)>>,
    subject: Subject,
) -> impl IntoResponse {
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
        default_limit: st.default_limit,
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
    match shape.as_deref() {
        None | Some("objects") => respond_objects(read_linked_chain(&query, &subject, &deps).await),
        Some("association") => {
            respond_associations(read_associations(&query, &subject, &deps).await)
        }
        Some(other) => (StatusCode::BAD_REQUEST, format!("unknown shape: {other}")).into_response(),
    }
}

#[utoipa::path(
    get, path = "/objects/{from_type}/links",
    params(("from_type" = String, Path, description = "Source object type")),
    responses(
        (status = 200, description = "Chain-traversed objects or associations", body = ObjectsResponse),
        (status = 400, description = "Bad path/shape/filter"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Unknown type or link"),
    ),
    security(("bearer_auth" = [])),
    tag = "links",
)]
async fn get_linked_chain(
    State(st): State<AppState>,
    Path(from_type): Path<String>,
    Query(params): Query<Vec<(String, String)>>,
    subject: Subject,
) -> impl IntoResponse {
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
        default_limit: st.default_limit,
    };
    let query = ChainQuery {
        from_type,
        path: hops,
        filters,
        ids,
    };
    match shape.as_deref() {
        None | Some("objects") => respond_objects(read_linked_chain(&query, &subject, &deps).await),
        Some("association") => {
            respond_associations(read_associations(&query, &subject, &deps).await)
        }
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

/// Render an uncoercible filter value (`QueryError::BadFilterValue`) as a structured `400`
/// body. A coercion failure (`FilterError::Coerce`) echoes `{error, column, expected, value}`;
/// a grammar/arity failure (`FilterError::BadValue`) carries `{error, column}` only. A
/// *visibility* denial is a separate `QueryError::BadFilter` (bare column, no value echo) and
/// never reaches here.
fn bad_filter_value_response(e: &crate::filter::FilterError) -> axum::response::Response {
    use crate::filter::FilterError;
    let body = match e {
        FilterError::Coerce {
            column,
            expected,
            value,
            ..
        } => serde_json::json!({
            "error": "bad_filter_value",
            "column": column,
            "expected": expected,
            "value": value,
        }),
        FilterError::BadValue(column, _) => serde_json::json!({
            "error": "bad_filter_value",
            "column": column,
        }),
    };
    (StatusCode::BAD_REQUEST, Json(body)).into_response()
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
        QueryError::BadFilterValue(e) => bad_filter_value_response(&e),
        other => internal_error("chain/association read serving fault", other),
    }
}

// Deliberately `const`, not config: safety guardrails bounding traversal recursion /
// blast radius that an operator must not be able to lift per-deployment.
// `DEFAULT_GRAPH_DEPTH` stays const because it is coupled to the `MAX_GRAPH_DEPTH`
// guardrail, not a deployment concern. See road-config-seam-unification.
const MAX_GRAPH_DEPTH: u32 = 10;
const DEFAULT_GRAPH_DEPTH: u32 = 5;

#[utoipa::path(
    get, path = "/objects/{type_name}/graph/{link_name}",
    params(
        ("type_name" = String, Path, description = "Seed object type"),
        ("link_name" = String, Path, description = "Self-link to recurse"),
    ),
    responses(
        (status = 200, description = "Reachable objects", body = ObjectsResponse),
        (status = 400, description = "Bad depth/filter/path"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Unknown type or link"),
    ),
    security(("bearer_auth" = [])),
    tag = "graph",
)]
async fn get_graph(
    State(st): State<AppState>,
    Path((type_name, link_name)): Path<(String, String)>,
    Query(params): Query<Vec<(String, String)>>,
    subject: Subject,
) -> impl IntoResponse {
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
        &subject,
    )
    .await
}

/// Multi-link `?path=l1,l2` path-cycle route. Parses `?path=` (comma-split; empty/absent ->
/// 400) plus the same `depth`/`_ids`/filter handling as `get_graph`, then shares the
/// `read_graph_reach` call + error mapping via `graph_respond`.
#[utoipa::path(
    get, path = "/objects/{type_name}/graph",
    params(("type_name" = String, Path, description = "Seed object type")),
    responses(
        (status = 200, description = "Reachable objects via path/links", body = ObjectsResponse),
        (status = 400, description = "Bad path/links/depth/filter"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Unknown type or link"),
    ),
    security(("bearer_auth" = [])),
    tag = "graph",
)]
async fn get_graph_path(
    State(st): State<AppState>,
    Path(type_name): Path<String>,
    Query(params): Query<Vec<(String, String)>>,
    subject: Subject,
) -> impl IntoResponse {
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
        return graph_union_respond(&st, type_name, links, depth, filters, ids, &subject).await;
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
        if starred.first().copied().unwrap_or(0) != 0 {
            return (
                StatusCode::BAD_REQUEST,
                "the recursive `*` segment must be the first path segment",
            )
                .into_response();
        }
        let Some(first_path) = path.first() else {
            return (StatusCode::BAD_REQUEST, "empty path").into_response();
        };
        let core_link = first_path.trim_end_matches('*').to_string();
        if core_link.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                "recursive core link name must not be empty",
            )
                .into_response();
        }
        let tail_links: Vec<String> = path.get(1..).unwrap_or_default().to_vec();
        return graph_tail_respond(
            &st, type_name, core_link, tail_links, depth, filters, ids, &subject,
        )
        .await;
    }
    graph_respond(&st, type_name, path, depth, filters, ids, &subject).await
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
        QueryError::BadFilterValue(e) => bad_filter_value_response(&e),
        QueryError::Forbidden => StatusCode::FORBIDDEN.into_response(),
        other => internal_error("graph read serving fault", other),
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
    subject: &Subject,
) -> axum::response::Response {
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
        default_limit: st.default_limit,
    };
    match read_graph_reach(
        &GraphQuery {
            type_name,
            path,
            depth,
            filters,
            ids,
        },
        subject,
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
    subject: &Subject,
) -> axum::response::Response {
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
        default_limit: st.default_limit,
    };
    match read_graph_reach_union(
        &GraphUnionQuery {
            type_name,
            links,
            depth,
            filters,
            ids,
        },
        subject,
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
#[allow(
    clippy::too_many_arguments,
    reason = "HTTP handler requires all routing params"
)]
async fn graph_tail_respond(
    st: &AppState,
    type_name: String,
    core_link: String,
    tail_links: Vec<String>,
    depth: u32,
    filters: Vec<(String, String)>,
    ids: Vec<String>,
    subject: &Subject,
) -> axum::response::Response {
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
        default_limit: st.default_limit,
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
        subject,
        &deps,
    )
    .await
    {
        Ok(rows) => Json(crate::render::objects_to_json(&rows)).into_response(),
        Err(e) => graph_error(e),
    }
}

#[utoipa::path(
    post, path = "/actions/{action_name}",
    params(("action_name" = String, Path, description = "Ontology action id")),
    request_body = serde_json::Value,
    responses(
        (status = 201, description = "Action applied; created/affected object"),
        (status = 400, description = "Bad params"),
        (status = 403, description = "Write denied by ACL policy", body = WriteDeniedBody),
        (status = 404, description = "Unknown action"),
        (status = 422, description = "A value violates a property constraint, or an unsupported action shape", body = crate::openapi::ConstraintViolationsBody),
    ),
    security(("bearer_auth" = [])),
    tag = "actions",
)]
async fn post_action(
    State(st): State<AppState>,
    Path(action_name): Path<String>,
    subject: Subject,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let obj = match body.as_object() {
        Some(m) => m.clone(),
        None => return (StatusCode::BAD_REQUEST, "body must be a JSON object").into_response(),
    };
    let deps = crate::action::ActionDeps {
        cp: st.cp.as_ref(),
        action_engine: st.action_engine.as_ref(),
        serving: st.serving.as_ref(),
    };
    match crate::action::run_action(&action_name, &obj, &subject.0, &deps).await {
        Ok((rows, run_id)) => {
            let body = crate::render::objects_to_json(&rows);
            // objects_to_json yields {"objects":[{...}]}; return the single created object.
            let one = body
                .get("objects")
                .and_then(|a| a.as_array())
                .and_then(|a| a.first())
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            // Surface the action's run_id so a caller can locate its lineage via
            // Lineage::events_for. The body is unchanged (non-invasive).
            let mut resp = (StatusCode::CREATED, Json(one)).into_response();
            if let Ok(v) = axum::http::HeaderValue::from_str(&run_id.0.to_string()) {
                resp.headers_mut().insert("X-Loom-Run-Id", v);
            }
            resp
        }
        Err(crate::action::ActionError::UnknownAction(a)) => {
            (StatusCode::NOT_FOUND, a).into_response()
        }
        // Fine-grained Write-policy denial: a caller-scoped structured body (column
        // vs row_filter). The predicate / policy id / role stay server-side.
        Err(crate::action::ActionError::WriteDenied(reason)) => {
            (StatusCode::FORBIDDEN, Json(reason.to_body())).into_response()
        }
        // Per-value constraint violation: a structured 422 (malformed data), distinct from
        // the 403 ACL denial above. Body: { "violations": [ { "property", "rule" }, .. ] }.
        Err(crate::action::ActionError::ConstraintViolation(violations)) => {
            let items: Vec<serde_json::Value> = violations
                .iter()
                .map(|v| serde_json::json!({ "property": v.property, "rule": v.rule.as_str() }))
                .collect();
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({ "violations": items })),
            )
                .into_response()
        }
        // Coarse Write-gate denial (and other unit forbiddens): bodyless 403, unchanged.
        Err(crate::action::ActionError::Forbidden) => StatusCode::FORBIDDEN.into_response(),
        Err(crate::action::ActionError::BadParams(e)) => {
            (StatusCode::BAD_REQUEST, e.to_string()).into_response()
        }
        // A misconfigured action is a server-side config fault, surfaced with detail (distinct
        // from the opaque catch-all 500 below) so the operator can fix the ActionDef.
        Err(crate::action::ActionError::Misconfigured(m)) => {
            (StatusCode::INTERNAL_SERVER_ERROR, m).into_response()
        }
        Err(crate::action::ActionError::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(crate::action::ActionError::Unsupported(m)) => {
            (StatusCode::UNPROCESSABLE_ENTITY, m).into_response()
        }
        Err(e) => internal_error("action serving fault", e),
    }
}

/// Render a kNN hit's identity as JSON, following the cell's kind: `Int` -> number,
/// `Text` -> string, anything else -> null.
fn id_json(v: &crate::serving::SqlValue) -> serde_json::Value {
    match v {
        crate::serving::SqlValue::Int(i) => serde_json::json!(i),
        crate::serving::SqlValue::Text(s) => serde_json::json!(s),
        _ => serde_json::Value::Null,
    }
}

/// Governed kNN search: `POST /search/:type/:index_name`. Parses + validates the body
/// (manual deserialize -> 400 on any problem, BEFORE any engine call), then runs the
/// governed `vector_search` flow and maps its errors to status codes.
#[utoipa::path(
    post, path = "/search/{type_name}/{index_name}",
    params(
        ("type_name" = String, Path, description = "Object type"),
        ("index_name" = String, Path, description = "Vector index name"),
    ),
    request_body = VectorSearchRequest,
    responses(
        (status = 200, description = "kNN hits", body = VectorSearchResponse),
        (status = 400, description = "Bad request body or dimension mismatch"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "No such index"),
    ),
    security(("bearer_auth" = [])),
    tag = "search",
)]
async fn post_search(
    State(st): State<AppState>,
    Path((type_name, index_name)): Path<(String, String)>,
    subject: Subject,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let req: VectorSearchRequest = match serde_json::from_value(body) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    if let Err(msg) = validate_search_request(&req) {
        return (StatusCode::BAD_REQUEST, msg).into_response();
    }
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
        default_limit: st.default_limit,
    };
    let q = crate::handler::VectorSearchQuery {
        type_name,
        index_name,
        query: req.query,
        k: req.k,
        nprobe: req.nprobe,
        ef_search: req.ef_search,
    };
    match crate::handler::vector_search(&q, &subject, &deps).await {
        Ok(hits) => {
            let results: Vec<serde_json::Value> = hits
                .iter()
                .map(|h| serde_json::json!({ "id": id_json(&h.id), "distance": h.distance }))
                .collect();
            Json(serde_json::json!({ "results": results })).into_response()
        }
        Err(QueryError::Forbidden) => StatusCode::FORBIDDEN.into_response(),
        Err(QueryError::Serving(crate::serving::ServingError::NoIndex(m))) => {
            (StatusCode::NOT_FOUND, m).into_response()
        }
        Err(QueryError::Serving(crate::serving::ServingError::DimMismatch(m))) => {
            (StatusCode::BAD_REQUEST, m).into_response()
        }
        Err(QueryError::NoIdentity(t)) => (StatusCode::BAD_REQUEST, t).into_response(),
        Err(e) => internal_error("vector search serving fault", e),
    }
}
