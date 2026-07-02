//! Thin axum surface. All logic is in handler::read_object; this layer only maps
//! HTTP <-> the core and serializes Rows to JSON.

use std::fmt::Display;
use std::sync::Arc;

use crate::handler::{
    Associations, ChainQuery, Direction, GraphQuery, GraphReadKind, GraphReadQuery, Hop,
    ObjectQuery, QueryDeps, QueryError, read_associations, read_graph, read_graph_tree,
    read_linked_chain, read_object, read_object_page,
};
use crate::openapi::{
    JobAck, ObjectsResponse, OntologyTypesResponse, VectorSearchResponse, WriteDeniedBody,
};
use crate::path_parse::{parse_direction, parse_path_hops};
use crate::serving::{ActionEngine, ServingEngine};
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};
use control_plane_core::{
    ControlPlane, ControlPlaneError, Cursor, DatasetRef, GC_JOB_KIND, NewJob, PageReq, RunId,
};
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

/// Parse an `_ids` value: comma-split, drop empties; an empty result is the caller
/// fault every read endpoint 400s on.
#[expect(
    clippy::result_large_err,
    reason = "Err carries the exact axum Response the endpoint returns; boxing it would only \
              push the allocation to every call site for no benefit on this cold error path"
)]
fn parse_ids(v: &str) -> Result<Vec<String>, axum::response::Response> {
    let ids: Vec<String> = v
        .split(',')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    if ids.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "_ids requires at least one value").into_response());
    }
    Ok(ids)
}

/// Parse a `depth` value; a non-integer is the exact 400 the endpoints returned inline.
#[expect(
    clippy::result_large_err,
    reason = "Err carries the exact axum Response the endpoint returns; boxing it would only \
              push the allocation to every call site for no benefit on this cold error path"
)]
fn parse_depth(v: &str) -> Result<u32, axum::response::Response> {
    // A `match`, not `.map_err(|_| ..)`: the parse error carries no detail worth
    // preserving (the body is a fixed message), and this sidesteps map_err_ignore
    // without a suppression.
    match v.parse::<u32>() {
        Ok(d) => Ok(d),
        Err(_) => {
            Err((StatusCode::BAD_REQUEST, "depth must be a positive integer").into_response())
        }
    }
}

/// Parse a `tree` flag: `true`/`false` (case-insensitive); anything else is a 400.
#[expect(
    clippy::result_large_err,
    reason = "Err carries the exact axum Response the endpoint returns; boxing it would only \
              push the allocation to every call site for no benefit on this cold error path"
)]
fn parse_tree(v: &str) -> Result<bool, axum::response::Response> {
    match v.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err((StatusCode::BAD_REQUEST, "tree must be true or false").into_response()),
    }
}

/// Default + bound the graph `depth` knob. The range check is a safety guardrail
/// (bounds recursion), deliberately not config.
#[expect(
    clippy::result_large_err,
    reason = "Err carries the exact axum Response the endpoint returns; boxing it would only \
              push the allocation to every call site for no benefit on this cold error path"
)]
fn graph_depth(depth: Option<u32>) -> Result<u32, axum::response::Response> {
    let depth = depth.unwrap_or(DEFAULT_GRAPH_DEPTH);
    if !(1..=MAX_GRAPH_DEPTH).contains(&depth) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("depth must be 1..={MAX_GRAPH_DEPTH}"),
        )
            .into_response());
    }
    Ok(depth)
}

/// The reserved (non-filter) query params of a read endpoint, split from the caller
/// filter pairs in one pass. Each endpoint names exactly the keys it reserves; an
/// unreserved key stays a filter (so e.g. `_direction` on `/objects/{type}` is still an
/// unknown filter column -> 400, exactly as before this splitter existed).
#[derive(Default)]
struct ReservedParams {
    ids: Vec<String>,
    or_raw: Vec<String>,
    limit: Option<String>,
    cursor: Option<String>,
    direction: Option<String>,
    shape: Option<String>,
    path: Option<String>,
    links: Vec<String>,
    depth: Option<u32>,
    tree: bool,
    filters: Vec<(String, String)>,
}

impl ReservedParams {
    /// Split `params` on the endpoint's `reserved` key set. Parse failures return the
    /// exact 400s the endpoints previously produced inline.
    #[expect(
        clippy::result_large_err,
        reason = "Err carries the exact axum Response the endpoint returns; boxing it would \
                  only push the allocation to every call site for no benefit on this cold \
                  error path"
    )]
    fn split(
        params: Vec<(String, String)>,
        reserved: &[&str],
    ) -> Result<Self, axum::response::Response> {
        let mut out = Self::default();
        for (k, v) in params {
            if !reserved.contains(&k.as_str()) {
                out.filters.push((k, v));
                continue;
            }
            match k.as_str() {
                "_ids" => out.ids = parse_ids(&v)?,
                "_or" => out.or_raw.push(v),
                "limit" => out.limit = Some(v),
                "cursor" => out.cursor = Some(v),
                "_direction" => out.direction = Some(v),
                "_shape" => out.shape = Some(v),
                "path" | "_path" => out.path = Some(v),
                "links" => {
                    out.links = v
                        .split(',')
                        .filter(|s| !s.is_empty())
                        .map(String::from)
                        .collect();
                }
                "depth" => out.depth = Some(parse_depth(&v)?),
                "tree" => out.tree = parse_tree(&v)?,
                _ => out.filters.push((k, v)),
            }
        }
        Ok(out)
    }
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

impl AppState {
    /// The borrowed read-path dependency bundle — one construction point for the
    /// `QueryDeps` literal previously hand-built per endpoint.
    fn deps(&self) -> QueryDeps<'_> {
        QueryDeps {
            ontology: self.cp.ontology(),
            acl: self.cp.acl(),
            serving: self.serving.as_ref(),
            default_limit: self.default_limit,
        }
    }
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
        .route(
            "/lineage/datasets/:namespace/:name/upstream",
            get(get_lineage_upstream),
        )
        .route(
            "/lineage/datasets/:namespace/:name/downstream",
            get(get_lineage_downstream),
        )
        .route("/lineage/runs/:run_id/events", get(get_lineage_run_events))
        .route("/ontology/types", get(list_ontology_types))
        .with_state(state)
}

/// Ontology metadata: the defined object-type names, for the object-explorer UI's type
/// sidebar. Auth-required (via `Subject`) but deliberately NOT per-type ACL-gated —
/// this is ontology metadata (like `/openapi.json`), not object data; ACL governs the
/// latter via `/objects/{type}`.
#[utoipa::path(
    get, path = "/ontology/types",
    responses((status = 200, description = "Object-type names", body = OntologyTypesResponse)),
    security(("bearer_auth" = [])),
    tag = "ontology",
)]
async fn list_ontology_types(State(st): State<AppState>, _subject: Subject) -> impl IntoResponse {
    match st.cp.ontology().list_types(PageReq::unbounded()).await {
        Ok(page) => {
            let types: Vec<String> = page.items.into_iter().map(|t| t.name.0).collect();
            Json(serde_json::json!({ "types": types })).into_response()
        }
        Err(e) => internal_error("ontology list_types fault", e),
    }
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
    params(
        ("type_name" = String, Path, description = "Ontology object type"),
        ("limit" = Option<u32>, Query, description = "Page size, clamped to [1,200]; presence (with `cursor`) selects cursor pagination"),
        ("cursor" = Option<String>, Query, description = "Opaque keyset cursor from a previous page's `next`; presence (with `limit`) selects cursor pagination"),
    ),
    responses(
        (status = 200, description = "Matching objects", body = ObjectsResponse),
        (status = 400, description = "Bad filter, _ids, or pagination (no declared identity, denied/masked identity, _ids + pagination together, or a malformed cursor)"),
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
    // Pull the `_ids` object-set input, `_or` groups, and the `limit`/`cursor` pagination
    // knobs out of the params; the rest are filters. Repeated filter keys are preserved (a
    // column may carry several predicates, e.g. a range); the handler parses each value's
    // operator and coerces it. Presence of `limit` OR `cursor` selects the paginated read path.
    let p = match ReservedParams::split(params, &["_ids", "_or", "limit", "cursor"]) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let ReservedParams {
        ids,
        or_raw,
        filters,
        limit: raw_limit,
        cursor: raw_cursor,
        ..
    } = p;
    let deps = st.deps();
    let paginated = raw_limit.is_some() || raw_cursor.is_some();
    if paginated {
        let limit = match raw_limit {
            Some(s) => match s.parse::<u32>() {
                Ok(n) => n.clamp(1, crate::handler::MAX_PAGE),
                Err(_) => {
                    return (StatusCode::BAD_REQUEST, "limit must be a positive integer")
                        .into_response();
                }
            },
            None => deps.default_limit.clamp(1, crate::handler::MAX_PAGE),
        };
        let after = raw_cursor.map(Cursor);
        return match read_object_page(
            &ObjectQuery {
                type_name,
                filters,
                ids,
                or_raw,
            },
            &subject,
            &deps,
            limit,
            after,
        )
        .await
        {
            Ok((rows, next)) => {
                Json(crate::render::objects_to_json(&rows, next.as_ref())).into_response()
            }
            Err(e) => query_error_response(e, "object read serving fault"),
        };
    }
    match read_object(
        &ObjectQuery {
            type_name,
            filters,
            ids,
            or_raw,
        },
        &subject,
        &deps,
    )
    .await
    {
        Ok(rows) => Json(crate::render::objects_to_json(&rows, None)).into_response(),
        Err(e) => query_error_response(e, "object read serving fault"),
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
    let p = match ReservedParams::split(params, &["_direction", "_shape", "_ids"]) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let direction = match parse_direction(p.direction.as_deref()) {
        Ok(d) => d,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    respond_shaped(
        &st,
        from_type,
        vec![Hop {
            link: link_name,
            direction,
        }],
        p.shape,
        p.filters,
        p.ids,
        &subject,
    )
    .await
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
    let p = match ReservedParams::split(params, &["_path", "_shape", "_ids"]) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let hops = p.path.as_deref().map(parse_path_hops).unwrap_or_default();
    respond_shaped(&st, from_type, hops, p.shape, p.filters, p.ids, &subject).await
}

/// The shared tail of the two `/links` routes: resolve filter keys against the path's
/// bare link names, build the `ChainQuery`, and dispatch on `_shape`
/// (objects | association).
async fn respond_shaped(
    st: &AppState,
    from_type: String,
    path: Vec<Hop>,
    shape: Option<String>,
    filter_params: Vec<(String, String)>,
    ids: Vec<String>,
    subject: &Subject,
) -> axum::response::Response {
    // Filter keys reference bare link names; resolve against those (direction-independent).
    let names: Vec<String> = path.iter().map(|h| h.link.clone()).collect();
    let filters = match crate::chain_filter::resolve_chain_filters(&names, filter_params) {
        Ok(f) => f,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let deps = st.deps();
    let query = ChainQuery {
        from_type,
        path,
        filters,
        ids,
    };
    match shape.as_deref() {
        None | Some("objects") => respond_objects(read_linked_chain(&query, subject, &deps).await),
        Some("association") => {
            respond_associations(read_associations(&query, subject, &deps).await)
        }
        Some(other) => (StatusCode::BAD_REQUEST, format!("unknown shape: {other}")).into_response(),
    }
}

fn respond_objects(
    res: Result<crate::handler::ObjectRows, QueryError>,
) -> axum::response::Response {
    match res {
        Ok(rows) => Json(crate::render::objects_to_json(&rows, None)).into_response(),
        Err(e) => query_error_response(e, "chain/association read serving fault"),
    }
}

fn respond_associations(res: Result<Associations, QueryError>) -> axum::response::Response {
    match res {
        Ok(a) => Json(crate::render::associations_to_json(&a)).into_response(),
        Err(e) => query_error_response(e, "chain/association read serving fault"),
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

/// The single, TOTAL `QueryError` -> HTTP response mapping. Every variant is matched
/// deliberately — adding a `QueryError` variant is a compile error here, not a silent
/// 500. Governance denials are bodyless 403s; caller faults echo only the
/// caller-supplied name (never internal SQL/schema detail); backend faults log
/// server-side via `internal_error` with the given `context` and return an opaque 500.
fn query_error_response(e: QueryError, context: &'static str) -> axum::response::Response {
    use crate::serving::ServingError;
    match e {
        QueryError::UnknownType(t) => (StatusCode::NOT_FOUND, t).into_response(),
        QueryError::UnknownLink(l) => (StatusCode::NOT_FOUND, l).into_response(),
        QueryError::AmbiguousLink(l) => (StatusCode::BAD_REQUEST, l).into_response(),
        QueryError::Forbidden => StatusCode::FORBIDDEN.into_response(),
        QueryError::BadFilter(c) => (StatusCode::BAD_REQUEST, c).into_response(),
        QueryError::BadFilterValue(ref err) => bad_filter_value_response(err),
        QueryError::BadChain(m) | QueryError::BadGraphPath(m) | QueryError::BadPagination(m) => {
            (StatusCode::BAD_REQUEST, m).into_response()
        }
        QueryError::NoIdentity(t) => (StatusCode::BAD_REQUEST, t).into_response(),
        QueryError::NotCyclicPath(p) => (StatusCode::BAD_REQUEST, p).into_response(),
        QueryError::Serving(ServingError::NoIndex(m)) => (StatusCode::NOT_FOUND, m).into_response(),
        QueryError::Serving(ServingError::DimMismatch(m)) => {
            (StatusCode::BAD_REQUEST, m).into_response()
        }
        e @ (QueryError::Serving(ServingError::Engine(_))
        | QueryError::ControlPlane(_)
        | QueryError::Malformed(_)) => internal_error(context, e),
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
        ("tree" = Option<bool>, Query, description = "Return a shortest-path tree ({roots, nodes}; see ObjectTreeResponse) instead of the flat reachable set"),
    ),
    responses(
        (status = 200, description = "Reachable objects (or a shortest-path tree when ?tree=true; see ObjectTreeResponse)", body = ObjectsResponse),
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
    // Pull `depth`, `_ids`, and `tree` out; the rest are seed filters.
    let p = match ReservedParams::split(params, &["depth", "_ids", "tree"]) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let depth = match graph_depth(p.depth) {
        Ok(d) => d,
        Err(r) => return r,
    };
    if p.tree {
        graph_tree_respond(
            &st,
            type_name,
            vec![link_name.into()],
            depth,
            p.filters,
            p.ids,
            &subject,
        )
        .await
    } else {
        graph_read_respond(
            &st,
            type_name,
            GraphReadKind::PathCycle {
                path: vec![link_name.into()],
            },
            depth,
            p.filters,
            p.ids,
            &subject,
        )
        .await
    }
}

/// Multi-link `?path=l1,l2` path-cycle route. Parses `?path=` via `parse_path_hops`
/// (comma-split; empty/absent -> 400), where a `~`-prefixed element is followed backward
/// (an inverse hop, same grammar as the `/links` chain), plus the same `depth`/`_ids`/filter
/// handling as `get_graph`, then shares the `read_graph` call + error mapping via
/// `graph_read_respond`.
#[utoipa::path(
    get, path = "/objects/{type_name}/graph",
    params(
        ("type_name" = String, Path, description = "Seed object type"),
        ("tree" = Option<bool>, Query, description = "Return a shortest-path tree ({roots, nodes}; see ObjectTreeResponse) instead of the flat reachable set (path-cycle route only)"),
    ),
    responses(
        (status = 200, description = "Reachable objects via path/links (or a shortest-path tree when ?tree=true; see ObjectTreeResponse)", body = ObjectsResponse),
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
    let p = match ReservedParams::split(params, &["path", "links", "depth", "_ids", "tree"]) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let depth = match graph_depth(p.depth) {
        Ok(d) => d,
        Err(r) => return r,
    };
    let path = p.path.as_deref().map(parse_path_hops).unwrap_or_default();
    let links = p.links;
    let ids = p.ids;
    let filters = p.filters;
    let tree = p.tree;
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
        if tree {
            return (
                StatusCode::BAD_REQUEST,
                "tree view is not supported with links (union)",
            )
                .into_response();
        }
        return graph_read_respond(
            &st,
            type_name,
            GraphReadKind::UnionSelfLinks { links },
            depth,
            filters,
            ids,
            &subject,
        )
        .await;
    }
    if path.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "path or links requires at least one link",
        )
            .into_response();
    }
    // Part B: a `*`-suffixed FORWARD segment marks a recursive core + relational tail. `~foo*`
    // is NOT a tail — it stays a path-cycle inverse hop whose name ends in `*` (=> UnknownLink).
    let starred: Vec<usize> = path
        .iter()
        .enumerate()
        .filter(|(_, h)| h.direction == Direction::Forward && h.link.ends_with('*'))
        .map(|(i, _)| i)
        .collect();
    if !starred.is_empty() {
        if tree {
            return (
                StatusCode::BAD_REQUEST,
                "tree view is not supported for a recursive-core (*) path",
            )
                .into_response();
        }
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
        let Some(first_hop) = path.first() else {
            return (StatusCode::BAD_REQUEST, "empty path").into_response();
        };
        let core_link = first_hop.link.trim_end_matches('*').to_string();
        if core_link.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                "recursive core link name must not be empty",
            )
                .into_response();
        }
        // Tail is forward-only (part-B non-goal defers inverse tails); re-emit each remaining
        // hop's name, re-attaching `~` for any inverse hop so it resolves as an (absent)
        // forward link rather than silently dropping the sigil.
        let tail_links: Vec<String> = path
            .get(1..)
            .unwrap_or_default()
            .iter()
            .map(|h| match h.direction {
                Direction::Forward => h.link.clone(),
                Direction::Inverse => format!("~{}", h.link),
            })
            .collect();
        return graph_read_respond(
            &st,
            type_name,
            GraphReadKind::CoreTail {
                core_link,
                tail_links,
            },
            depth,
            filters,
            ids,
            &subject,
        )
        .await;
    }
    if tree {
        graph_tree_respond(&st, type_name, path, depth, filters, ids, &subject).await
    } else {
        graph_read_respond(
            &st,
            type_name,
            GraphReadKind::PathCycle { path },
            depth,
            filters,
            ids,
            &subject,
        )
        .await
    }
}

/// Shared `/graph` object-read tail: build a `GraphReadQuery` of the given kind, run
/// `read_graph`, render, map errors via `query_error_response`.
async fn graph_read_respond(
    st: &AppState,
    type_name: String,
    kind: GraphReadKind,
    depth: u32,
    filters: Vec<(String, String)>,
    ids: Vec<String>,
    subject: &Subject,
) -> axum::response::Response {
    let deps = st.deps();
    match read_graph(
        &GraphReadQuery {
            type_name,
            kind,
            depth,
            filters,
            ids,
        },
        subject,
        &deps,
    )
    .await
    {
        Ok(rows) => Json(crate::render::objects_to_json(&rows, None)).into_response(),
        Err(e) => query_error_response(e, "graph read serving fault"),
    }
}

/// Tree tail for `?tree=true` on the single `/graph/:link` and `?path=` path-cycle routes:
/// build a `GraphQuery`, run `read_graph_tree`, render `{roots, nodes}` via `tree_to_json`,
/// map errors via `query_error_response`.
async fn graph_tree_respond(
    st: &AppState,
    type_name: String,
    path: Vec<Hop>,
    depth: u32,
    filters: Vec<(String, String)>,
    ids: Vec<String>,
    subject: &Subject,
) -> axum::response::Response {
    let deps = st.deps();
    match read_graph_tree(
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
        Ok(tree) => Json(crate::render::tree_to_json(&tree)).into_response(),
        Err(e) => query_error_response(e, "graph read serving fault"),
    }
}

#[utoipa::path(
    post, path = "/actions/{action_name}",
    params(("action_name" = String, Path, description = "Ontology action id")),
    request_body = serde_json::Value,
    responses(
        (status = 201, description = "Action applied; created/affected object"),
        (status = 400, description = "Malformed or undecodable request body (not a JSON action envelope)"),
        (status = 403, description = "Write denied by ACL policy", body = WriteDeniedBody),
        (status = 404, description = "Unknown action"),
        (status = 422, description = "Semantic validation failure: bad or missing action params, a property-constraint violation, or an unsupported action shape", body = crate::openapi::ConstraintViolationsBody),
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
            let body = crate::render::objects_to_json(&rows, None);
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
        // A well-formed body whose params fail SEMANTIC validation (missing required param,
        // type mismatch, uncoercible value) is 422 — understood, but unprocessable. Malformed
        // / undecodable bodies never reach here: axum's `Json` extractor 400s invalid JSON,
        // and the non-object envelope guard above returns 400. Aligns with the
        // ConstraintViolation 422 on this same write path.
        Err(crate::action::ActionError::BadParams(e)) => {
            (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
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
    let deps = st.deps();
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
        Err(e) => query_error_response(e, "vector search serving fault"),
    }
}

/// Which direction of the provenance closure a request walks.
#[derive(Clone, Copy)]
enum LineageDir {
    Upstream,
    Downstream,
}

/// Map a lineage read error to a status. A `Validation` fault (over-cap/zero depth,
/// malformed cursor) is a caller error (400); anything else is an opaque 500 logged
/// server-side. An unknown dataset is NOT an error — the capability returns an empty
/// page, which serializes as `{ "datasets": [], "next_cursor": null }`.
fn lineage_error(e: ControlPlaneError) -> axum::response::Response {
    match e {
        ControlPlaneError::Validation(m) => (StatusCode::BAD_REQUEST, m).into_response(),
        other => internal_error("lineage read fault", other),
    }
}

/// Shared upstream/downstream handler: parse `depth` (default 1, forwarded to the
/// capability which caps it), `after`/`limit` (-> `PageReq`), then call the closure
/// read and serialize the page. `depth` beyond `LINEAGE_MAX_DEPTH` (or 0) is rejected
/// BELOW by the capability as `Validation` -> 400 — the wire cannot trigger an
/// unbounded walk.
async fn lineage_closure(
    st: &AppState,
    namespace: String,
    name: String,
    params: Vec<(String, String)>,
    dir: LineageDir,
) -> axum::response::Response {
    let mut depth: u32 = 1;
    let mut after: Option<String> = None;
    let mut limit: Option<String> = None;
    for (k, v) in params {
        match k.as_str() {
            "depth" => match v.parse::<u32>() {
                Ok(d) => depth = d,
                Err(_) => {
                    return (StatusCode::BAD_REQUEST, "depth must be a positive integer")
                        .into_response();
                }
            },
            "after" => after = Some(v),
            "limit" => limit = Some(v),
            _ => {} // ignore unknown query params
        }
    }
    let page = match crate::lineage_read::parse_lineage_page(after, limit) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let ds = DatasetRef { namespace, name };
    let lineage = st.cp.lineage();
    let res = match dir {
        LineageDir::Upstream => lineage.upstream(&ds, depth, page).await,
        LineageDir::Downstream => lineage.downstream(&ds, depth, page).await,
    };
    match res {
        Ok(page) => Json(crate::lineage_read::dataset_closure_body(page)).into_response(),
        Err(e) => lineage_error(e),
    }
}

#[utoipa::path(
    get, path = "/lineage/datasets/{namespace}/{name}/upstream",
    params(
        ("namespace" = String, Path, description = "Dataset namespace"),
        ("name" = String, Path, description = "Dataset name"),
        ("depth" = Option<u32>, Query, description = "Closure depth (default 1, capped)"),
        ("after" = Option<String>, Query, description = "Opaque next-page cursor"),
        ("limit" = Option<u32>, Query, description = "Max datasets per page"),
    ),
    responses(
        (status = 200, description = "Upstream dataset closure", body = crate::lineage_read::DatasetClosureResponse),
        (status = 400, description = "Bad depth/limit/cursor"),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "lineage",
)]
async fn get_lineage_upstream(
    State(st): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<Vec<(String, String)>>,
    _subject: Subject,
) -> axum::response::Response {
    lineage_closure(&st, namespace, name, params, LineageDir::Upstream).await
}

#[utoipa::path(
    get, path = "/lineage/datasets/{namespace}/{name}/downstream",
    params(
        ("namespace" = String, Path, description = "Dataset namespace"),
        ("name" = String, Path, description = "Dataset name"),
        ("depth" = Option<u32>, Query, description = "Closure depth (default 1, capped)"),
        ("after" = Option<String>, Query, description = "Opaque next-page cursor"),
        ("limit" = Option<u32>, Query, description = "Max datasets per page"),
    ),
    responses(
        (status = 200, description = "Downstream dataset closure", body = crate::lineage_read::DatasetClosureResponse),
        (status = 400, description = "Bad depth/limit/cursor"),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "lineage",
)]
async fn get_lineage_downstream(
    State(st): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<Vec<(String, String)>>,
    _subject: Subject,
) -> axum::response::Response {
    lineage_closure(&st, namespace, name, params, LineageDir::Downstream).await
}

#[utoipa::path(
    get, path = "/lineage/runs/{run_id}/events",
    params(
        ("run_id" = String, Path, description = "OpenLineage run id (UUID)"),
        ("after" = Option<String>, Query, description = "Opaque next-page cursor"),
        ("limit" = Option<u32>, Query, description = "Max events per page"),
    ),
    responses(
        (status = 200, description = "Events for the run", body = crate::lineage_read::RunEventsResponse),
        (status = 400, description = "Malformed run id or limit"),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "lineage",
)]
async fn get_lineage_run_events(
    State(st): State<AppState>,
    Path(run_id): Path<String>,
    Query(params): Query<Vec<(String, String)>>,
    _subject: Subject,
) -> axum::response::Response {
    let Ok(uuid) = uuid::Uuid::parse_str(&run_id) else {
        return (StatusCode::BAD_REQUEST, "run_id must be a UUID").into_response();
    };
    let mut after: Option<String> = None;
    let mut limit: Option<String> = None;
    for (k, v) in params {
        match k.as_str() {
            "after" => after = Some(v),
            "limit" => limit = Some(v),
            _ => {}
        }
    }
    let page = match crate::lineage_read::parse_lineage_page(after, limit) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    match st.cp.lineage().events_for(&RunId(uuid), page).await {
        Ok(page) => Json(crate::lineage_read::run_events_body(page)).into_response(),
        Err(e) => lineage_error(e),
    }
}
