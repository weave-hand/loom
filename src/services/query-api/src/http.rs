//! Thin axum surface. All logic is in handler::read_object; this layer only maps
//! HTTP <-> the core and serializes Rows to JSON.

use std::fmt::Display;
use std::sync::Arc;

use crate::handler::{
    Associations, ChainQuery, GraphQuery, GraphReadSpec, GraphTailQuery, GraphUnionQuery, Hop,
    ObjectQuery, QueryDeps, QueryError, read_associations, read_graph_reach_spec, read_graph_tree,
    read_linked_chain, read_object, read_object_page,
};
use crate::openapi::{
    DatasetDetailResponse, DatasetPreviewResponse, DatasetsResponse, JobAck, ObjectsResponse,
    OntologyTypesResponse, TypeDetailResponse, VectorSearchResponse, WriteDeniedBody,
};
use crate::path_parse::{parse_direction, parse_path_hops};
use crate::serving::{ActionEngine, ServingEngine};
use crate::sql::SqlDialect;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};
use control_plane_core::{
    ActionKind, ControlPlane, ControlPlaneError, Cursor, DatasetRef, DerivedPropertyDef,
    GC_JOB_KIND, LinkDef, NewJob, PageReq, RunId, SubjectId, TableRef, TypeName, VectorIndexDef,
};
use lineage_naming::LineageNaming;
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
    /// GC retention window (`LOOM_GC_RETENTION_SECS`). Time-travel reads use it
    /// to derive the retention horizon `H` (the same window `gc_table` reclaims
    /// under, so guard and reclaimer agree by construction). At or above `H` a
    /// selector is provably complete with no query. Below `H`, the guard asks
    /// `Catalog::snapshot_intact` the real question — has anything visible at the
    /// selector already been reclaimed, or is anything visible eligible to be —
    /// and 410s only then; a below-`H` read of a table whose visible rows were
    /// never end-capped (e.g. append-only) now serves.
    pub gc_retention: std::time::Duration,
    /// Deployment naming bridge: resolves a `DatasetRef` back to its governed
    /// `Table`/`Type` (or External) so the `/lineage` reads can ACL-filter per node.
    pub naming: Arc<LineageNaming>,
}

impl AppState {
    /// The per-request borrowed dependency bundle every read handler passes down —
    /// one construction point instead of a hand-built literal per route.
    pub fn deps(&self) -> QueryDeps<'_> {
        QueryDeps {
            ontology: self.cp.ontology(),
            acl: self.cp.acl(),
            serving: self.serving.as_ref(),
            catalog: self.cp.catalog(),
            default_limit: self.default_limit,
            gc_retention: self.gc_retention,
        }
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/objects/:type_name", get(get_object))
        .route("/objects/:type_name/changes", get(get_changes))
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
        .route(
            "/lineage/datasets/:namespace/:name/runs",
            get(get_lineage_dataset_runs),
        )
        .route("/ontology/types", get(list_ontology_types))
        .route("/ontology/types/:name", get(get_ontology_type))
        .route("/datasets", get(list_datasets))
        .route("/datasets/:schema/:table", get(get_dataset))
        .route("/datasets/:schema/:table/preview", get(dataset_preview))
        .with_state(state)
}

/// List the defined object-type names.
///
/// Ontology metadata for the type sidebar (like `/openapi.json`) — auth-required but
/// not per-type ACL-gated; object data itself is governed via `/objects/{type}`.
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

/// Map a control-plane metadata-read fault: `NotFound` is the caller's 404 (the message
/// carries the identifiers of the read — caller-supplied names, plus the resolved
/// snapshot id on the schema read); anything else is the opaque logged 500.
fn cp_read_error(context: &str, e: ControlPlaneError) -> axum::response::Response {
    match e {
        ControlPlaneError::NotFound(m) => (StatusCode::NOT_FOUND, m).into_response(),
        other => internal_error(context, other),
    }
}

/// The canonical "dataset not visible" 404 — one fixed body shared by the point
/// reads so an unreadable existing dataset is byte-identical to a nonexistent one
/// (no existence oracle; the point-read analog of the lineage seed gate's empty page).
fn dataset_not_found() -> axum::response::Response {
    (StatusCode::NOT_FOUND, "dataset not found").into_response()
}

/// Insert `description` into a hand-built `json!` object when present, so an absent
/// description is omitted entirely rather than emitted as `null`. `serde_json::Value`
/// has no top-level indexing guarantee for an arbitrary key, so this goes through
/// `as_object_mut` rather than `v["description"] = ..` (which clippy's
/// `indexing_slicing` restriction flags even though the map-insert form can't panic).
fn set_description(v: &mut serde_json::Value, description: Option<&String>) {
    if let Some(d) = description
        && let Some(map) = v.as_object_mut()
    {
        map.insert(
            "description".to_string(),
            serde_json::Value::String(d.clone()),
        );
    }
}

/// Render one `LinkDef` as the `LinkView` documentation shape (`cardinality` as its
/// persisted token). The physical `backing` stays server-side — this is ontology
/// metadata for callers, not storage detail.
fn link_view_json(l: &LinkDef) -> serde_json::Value {
    let mut v = serde_json::json!({
        "name": l.name,
        "from": l.from.0,
        "to": l.to.0,
        "cardinality": l.cardinality.as_str(),
    });
    set_description(&mut v, l.description.as_ref());
    v
}

/// Render one `DerivedPropertyDef` as its documentation shape:
/// `{ name, ty, link, agg, description? }`. `agg` is the externally-tagged serde form
/// (`"Count"` or `{"Sum":"col"}`); `description` is present only when set.
fn derived_view_json(d: &DerivedPropertyDef) -> serde_json::Value {
    let mut v = serde_json::json!({
        "name": d.name,
        "ty": d.ty,
        "link": d.link,
        "agg": d.agg,
    });
    set_description(&mut v, d.description.as_ref());
    v
}

/// Render one `VectorIndexDef` as its documentation shape:
/// `{ name, property, metric, spec, description? }`. `type_name` is omitted (redundant on
/// the type's own detail). `metric`/`spec` carry their serde forms (`"Cosine"`/`"L2"`;
/// `"Flat"`/`{"IvfFlat":..}`/`{"Hnsw":..}`); `description` is present only when set.
fn vector_index_view_json(idx: &VectorIndexDef) -> serde_json::Value {
    let mut v = serde_json::json!({
        "name": idx.name,
        "property": idx.property,
        "metric": idx.metric,
        "spec": idx.spec,
    });
    set_description(&mut v, idx.description.as_ref());
    v
}

/// Per-type ontology detail: properties, identity, backing table, and link adjacency.
///
/// Reports the declared properties (with required flags), identity, backing table, and
/// the type's outbound and inbound links. Like `/ontology/types`, auth-required but not
/// per-type ACL-gated (ontology metadata, not object data).
#[utoipa::path(
    get, path = "/ontology/types/{name}",
    params(("name" = String, Path, description = "Ontology object type")),
    responses(
        (status = 200, description = "Type detail: properties, identity, links", body = TypeDetailResponse),
        (status = 404, description = "Unknown type"),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "ontology",
)]
async fn get_ontology_type(
    State(st): State<AppState>,
    Path(name): Path<String>,
    _subject: Subject,
) -> axum::response::Response {
    let onto = st.cp.ontology();
    let type_name = TypeName(name);
    let ty = match onto.get_type(&type_name).await {
        Ok(t) => t,
        Err(e) => return cp_read_error("ontology get_type fault", e),
    };
    let links = match onto.links(&type_name, PageReq::unbounded()).await {
        Ok(page) => page.items,
        Err(e) => return cp_read_error("ontology links fault", e),
    };
    let links_to = match onto.links_to(&type_name, PageReq::unbounded()).await {
        Ok(page) => page.items,
        Err(e) => return cp_read_error("ontology links_to fault", e),
    };
    let mut indexes = match onto.vector_indexes_for(&type_name).await {
        Ok(v) => v,
        Err(e) => return cp_read_error("ontology vector_indexes_for fault", e),
    };
    // vector_indexes_for's order is unspecified; sort by name for a stable read surface.
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    let properties: Vec<serde_json::Value> = ty
        .properties
        .iter()
        .map(|p| {
            let mut v = serde_json::json!({ "name": p.name, "ty": p.ty, "required": p.required });
            set_description(&mut v, p.description.as_ref());
            v
        })
        .collect();
    let mut body = serde_json::json!({
        "name": ty.name.0,
        "table": { "schema": ty.table.schema, "name": ty.table.name },
        "identity": ty.identity,
        "properties": properties,
        "derived": ty.derived.iter().map(derived_view_json).collect::<Vec<_>>(),
        "vector_indexes": indexes.iter().map(vector_index_view_json).collect::<Vec<_>>(),
        "links": links.iter().map(link_view_json).collect::<Vec<_>>(),
        "links_to": links_to.iter().map(link_view_json).collect::<Vec<_>>(),
    });
    set_description(&mut body, ty.description.as_ref());
    Json(body).into_response()
}

/// List every table live in the Iceberg mirror, `(schema, name)`-ordered.
///
/// Catalog metadata reads are now per-dataset ACL-gated: a table is listed only if
/// `subject` can read it, under the same Table∨backing-Type predicate `/lineage`
/// enforces (`DatasetVisibility::is_table_readable`).
#[utoipa::path(
    get, path = "/datasets",
    params(
        ("sort" = Option<String>, Query, description = "Sort key: name | project | updated | rows. Absent preserves (schema,name) order."),
        ("dir" = Option<String>, Query, description = "Sort direction: asc (default) | desc. Only meaningful with sort."),
        ("project" = Option<String>, Query, description = "Filter to datasets whose project (schema) exactly matches."),
    ),
    responses(
        (status = 200, description = "Live mirror tables", body = DatasetsResponse),
        (status = 400, description = "Malformed sort/dir selector"),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "datasets",
)]
async fn list_datasets(
    State(st): State<AppState>,
    Query(params): Query<Vec<(String, String)>>,
    subject: Subject,
) -> axum::response::Response {
    let list_params = match crate::dataset_list::DatasetListParams::from_params(&params) {
        Ok(p) => p,
        Err(m) => return (StatusCode::BAD_REQUEST, m).into_response(),
    };
    let mut datasets = match collect_table_summaries(&st, &subject.0).await {
        Ok(d) => d,
        Err(r) => return r,
    };
    match collect_view_summaries(&st, &subject.0).await {
        Ok(mut v) => datasets.append(&mut v),
        Err(r) => return r,
    }
    let ordered = crate::dataset_list::apply(datasets, &list_params);
    let out: Vec<serde_json::Value> = ordered.iter().map(|d| d.to_json()).collect();
    Json(serde_json::json!({ "datasets": out })).into_response()
}

/// Collect the ACL-readable physical tables as `DatasetSummary`s (best-effort
/// `updated`/`rows`, exactly as the inline loop did). An ACL or catalog fault
/// short-circuits to the 500 response.
async fn collect_table_summaries(
    st: &AppState,
    subject: &SubjectId,
) -> Result<Vec<crate::dataset_list::DatasetSummary>, axum::response::Response> {
    let catalog = st.cp.catalog();
    let vis = crate::dataset_acl::DatasetVisibility::new(
        st.cp.acl(),
        st.cp.ontology(),
        st.naming.as_ref(),
    );
    let page = catalog
        .list_tables(PageReq::unbounded())
        .await
        .map_err(|e| internal_error("catalog list_tables fault", e))?;
    let mut out = Vec::with_capacity(page.items.len());
    for t in &page.items {
        match vis.is_table_readable(subject, t).await {
            Ok(true) => {}
            Ok(false) => continue,
            Err(e) => return Err(internal_error("catalog dataset acl fault", e)),
        }
        // Best-effort: a table with no readable snapshot renders "" / null rows.
        let (updated, rows) = match catalog.current_snapshot(t).await {
            Ok(s) => {
                let updated = s
                    .time
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_default();
                // Cheap exact count; a count hiccup must not 500 the list.
                let rows = catalog.row_count(t, s.id).await.ok();
                (updated, rows)
            }
            Err(_) => (String::new(), None),
        };
        out.push(crate::dataset_list::DatasetSummary {
            schema: t.schema.clone(),
            name: t.name.clone(),
            project: t.schema.clone(),
            updated,
            rows,
            kind: "table",
            base: None,
        });
    }
    Ok(out)
}

/// Collect the ACL-readable views as `DatasetSummary`s. A view gates identically to
/// a physical table (`is_table_readable` resolves a view ref); `updated` delegates
/// through `current_snapshot` to the base, `rows` is `null`, and `base` carries the
/// physical `{schema,name}`.
async fn collect_view_summaries(
    st: &AppState,
    subject: &SubjectId,
) -> Result<Vec<crate::dataset_list::DatasetSummary>, axum::response::Response> {
    let catalog = st.cp.catalog();
    let vis = crate::dataset_acl::DatasetVisibility::new(
        st.cp.acl(),
        st.cp.ontology(),
        st.naming.as_ref(),
    );
    let views = catalog
        .list_views(PageReq::unbounded())
        .await
        .map_err(|e| internal_error("catalog list_views fault", e))?;
    let mut out = Vec::with_capacity(views.items.len());
    for v in &views.items {
        match vis.is_table_readable(subject, &v.view).await {
            Ok(true) => {}
            Ok(false) => continue,
            Err(e) => return Err(internal_error("catalog dataset acl fault", e)),
        }
        let updated = match catalog.current_snapshot(&v.view).await {
            Ok(s) => s
                .time
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
            Err(_) => String::new(),
        };
        out.push(crate::dataset_list::DatasetSummary {
            schema: v.view.schema.clone(),
            name: v.view.name.clone(),
            project: v.view.schema.clone(),
            updated,
            rows: None,
            kind: "view",
            base: Some((v.base.schema.clone(), v.base.name.clone())),
        });
    }
    Ok(out)
}

/// Dataset detail: the table's snapshot (current, or the `?as_of`/`?as_of_snapshot`
/// selected one) and its column schema.
///
/// Composes the target snapshot (id + RFC3339 time) with the column schema at that snapshot.
#[utoipa::path(
    get, path = "/datasets/{schema}/{table}",
    params(
        ("schema" = String, Path, description = "Iceberg schema"),
        ("table" = String, Path, description = "Table name"),
        ("as_of" = Option<String>, Query, description = "Time-travel selector: RFC3339 timestamp -> the latest snapshot at/before it. Mutually exclusive with `as_of_snapshot`."),
        ("as_of_snapshot" = Option<i64>, Query, description = "Time-travel selector: an exact mirror snapshot id. Mutually exclusive with `as_of`."),
    ),
    responses(
        (status = 200, description = "Target snapshot + column schema", body = DatasetDetailResponse),
        (status = 400, description = "Malformed as_of/as_of_snapshot selector"),
        (status = 404, description = "Unknown table, or selector resolves to no live/prior snapshot, or a dataset the caller may not read (indistinguishable — no existence oracle)"),
        (status = 410, description = "The snapshot's data has been — or may at any moment be — reclaimed (below the retention horizon AND end-capped); a below-horizon selector over an append-only/intact table is not affected"),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "datasets",
)]
async fn get_dataset(
    State(st): State<AppState>,
    Path((schema, table)): Path<(String, String)>,
    Query(params): Query<Vec<(String, String)>>,
    subject: Subject,
) -> axum::response::Response {
    let catalog = st.cp.catalog();
    let table_ref = TableRef {
        schema,
        name: table,
    };
    let vis = crate::dataset_acl::DatasetVisibility::new(
        st.cp.acl(),
        st.cp.ontology(),
        st.naming.as_ref(),
    );
    match vis.is_table_readable(&subject.0, &table_ref).await {
        Ok(true) => {}
        Ok(false) => return dataset_not_found(),
        Err(e) => return cp_read_error("dataset detail acl fault", e),
    }
    let reserved = crate::query_params::split_reserved(params, &["as_of", "as_of_snapshot"]).0;
    let sel = match parse_as_of(reserved.last("as_of"), reserved.last("as_of_snapshot")) {
        Ok(s) => s,
        Err(m) => return (StatusCode::BAD_REQUEST, m).into_response(),
    };
    let snapshot = match resolve_dataset_snapshot(catalog, &table_ref, sel.as_ref()).await {
        Ok(s) => s,
        Err(e) => return cp_read_error("catalog snapshot resolution fault", e),
    };
    if sel.is_some()
        && let Err(e) = crate::handler::ensure_within_retention(
            catalog,
            st.gc_retention,
            &table_ref,
            snapshot.id,
        )
        .await
    {
        return query_error_response(e, "dataset as-of retention guard");
    }
    let table_schema = match catalog.schema(&table_ref, snapshot.id).await {
        Ok(s) => s,
        Err(e) => return cp_read_error("catalog schema fault", e),
    };
    let snapshot_time = snapshot
        .time
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    let columns: Vec<serde_json::Value> = table_schema
        .columns
        .iter()
        .map(|c| serde_json::json!({ "name": c.name, "ty": c.ty, "nullable": c.nullable }))
        .collect();
    // A view ref carries the same snapshot/columns (delegated to the base via the
    // catalog) plus its `kind`/`base` marker; a physical table is `kind: "table"`.
    let view = match catalog.get_view(&table_ref).await {
        Ok(v) => v,
        Err(e) => return cp_read_error("catalog get_view fault", e),
    };
    let mut body = serde_json::json!({
        "table": { "schema": table_ref.schema, "name": table_ref.name },
        "snapshot_id": snapshot.id.0,
        "snapshot_time": snapshot_time,
        "columns": columns,
        "kind": if view.is_some() { "view" } else { "table" },
    });
    if let Some(v) = &view
        && let Some(m) = body.as_object_mut()
    {
        m.insert(
            "base".into(),
            serde_json::json!({ "schema": v.base.schema, "name": v.base.name }),
        );
    }
    Json(body).into_response()
}

/// Resolve dataset-detail's target snapshot: the selector's snapshot when given,
/// else the current one. `NotFound` (bad id / pre-history timestamp / unknown table)
/// propagates to the 404 mapping.
async fn resolve_dataset_snapshot(
    catalog: &(dyn control_plane_core::Catalog + Send + Sync),
    table: &TableRef,
    sel: Option<&crate::handler::AsOfSelector>,
) -> Result<control_plane_core::Snapshot, ControlPlaneError> {
    use crate::handler::AsOfSelector;
    match sel {
        None => catalog.current_snapshot(table).await,
        Some(AsOfSelector::Snapshot(id)) => {
            let sid = control_plane_core::SnapshotId(*id);
            // Exact-history gate, shared with the object path (Catalog::snapshot):
            // replaces the previous O(history) snapshots().find(id) scan.
            catalog.snapshot(table, sid).await?.ok_or_else(|| {
                ControlPlaneError::NotFound(format!(
                    "{}.{} has no snapshot {}",
                    table.schema, table.name, id
                ))
            })
        }
        Some(AsOfSelector::Time(ts)) => {
            catalog.snapshot_as_of(table, *ts).await?.ok_or_else(|| {
                ControlPlaneError::NotFound(format!(
                    "{}.{} has no snapshot at or before {ts}",
                    table.schema, table.name
                ))
            })
        }
    }
}

/// Sample rows from a dataset: `SELECT * FROM "schema"."table" LIMIT n` over the engine.
///
/// Per-dataset ACL-gated (same predicate as `/datasets`). `limit` defaults to 20 and is
/// capped at 200; a malformed `limit` is a 400.
#[utoipa::path(
    get, path = "/datasets/{schema}/{table}/preview",
    params(
        ("schema" = String, Path, description = "Iceberg schema"),
        ("table" = String, Path, description = "Table name"),
        ("limit" = Option<u32>, Query, description = "Max sample rows (default 20, cap 200)"),
    ),
    responses(
        (status = 200, description = "Sampled rows", body = DatasetPreviewResponse),
        (status = 400, description = "Bad limit"),
        (status = 404, description = "Unknown or unreadable dataset"),
        (status = 500, description = "Serving error"),
    ),
    security(("bearer_auth" = [])),
    tag = "datasets",
)]
async fn dataset_preview(
    State(st): State<AppState>,
    Path((schema, table)): Path<(String, String)>,
    Query(params): Query<Vec<(String, String)>>,
    subject: Subject,
) -> axum::response::Response {
    const DEFAULT_LIMIT: u32 = 20;
    const MAX_LIMIT: u32 = 200;
    let limit = match params
        .iter()
        .find(|(k, _)| k == "limit")
        .map(|(_, v)| v.as_str())
    {
        None => DEFAULT_LIMIT,
        Some(raw) => match raw.parse::<u32>() {
            Ok(n) => n.clamp(1, MAX_LIMIT),
            Err(_) => {
                return (StatusCode::BAD_REQUEST, "limit must be a positive integer")
                    .into_response();
            }
        },
    };
    let table_ref = TableRef {
        schema: schema.clone(),
        name: table.clone(),
    };
    let vis = crate::dataset_acl::DatasetVisibility::new(
        st.cp.acl(),
        st.cp.ontology(),
        st.naming.as_ref(),
    );
    match vis.is_table_readable(&subject.0, &table_ref).await {
        Ok(true) => {}
        Ok(false) => return dataset_not_found(),
        Err(e) => return cp_read_error("dataset preview acl fault", e),
    }
    let dialect = crate::sql::DataFusionDialect;
    let sql = format!(
        "SELECT * FROM {}.{} LIMIT {limit}",
        dialect.quote_ident(&schema),
        dialect.quote_ident(&table),
    );
    match st.serving.fetch_rows(&sql, &[], None).await {
        Ok(rows) => Json(crate::dataset_preview::preview_body(&rows)).into_response(),
        Err(e) => internal_error("dataset preview serving fault", e),
    }
}

/// Enqueue physical GC for a table; returns 202 with the job id.
///
/// Operator-triggered. A worker drains the `gc_table` job asynchronously via the
/// engine's `GcTable` RPC — reclamation is not synchronous with this call.
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

/// List a type's objects, with optional filters and keyset pagination.
///
/// Each property is an optional filter (`?prop=op:value`); `_ids`, `_or`, `limit`, and
/// `cursor` are reserved. Governed per-type by ACL.
#[utoipa::path(
    get, path = "/objects/{type_name}",
    params(
        ("type_name" = String, Path, description = "Ontology object type"),
        ("limit" = Option<u32>, Query, description = "Page size, clamped to [1,200]; presence (with `cursor`) selects cursor pagination"),
        ("cursor" = Option<String>, Query, description = "Opaque keyset cursor from a previous page's `next`; presence (with `limit`) selects cursor pagination"),
        ("as_of" = Option<String>, Query, description = "Time-travel selector: RFC3339 timestamp -> the latest snapshot at/before it, for that type's backing table. Mutually exclusive with `as_of_snapshot`."),
        ("as_of_snapshot" = Option<i64>, Query, description = "Time-travel selector: an exact mirror snapshot id. Mutually exclusive with `as_of`."),
    ),
    responses(
        (status = 200, description = "Matching objects", body = ObjectsResponse),
        (status = 400, description = "Bad filter, _ids, or pagination (no declared identity, denied/masked identity, _ids + pagination together, a malformed cursor, or a malformed/mutually-exclusive as_of selector)"),
        (status = 403, description = "Forbidden by ACL policy"),
        (status = 404, description = "Unknown type, or the requested as-of snapshot/timestamp resolves to no live snapshot"),
        (status = 410, description = "The snapshot's data has been — or may at any moment be — reclaimed (below the retention horizon AND end-capped); a below-horizon selector over an append-only/intact table is not affected"),
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
    // Split the `_ids` object-set input, `_or` groups, and the `limit`/`cursor` pagination
    // knobs out of the params; the rest are filters. Repeated filter keys are preserved (a
    // column may carry several predicates, e.g. a range); the handler parses each value's
    // operator and coerces it. Presence of `limit` OR `cursor` selects the paginated read path.
    let (reserved, filters) = crate::query_params::split_reserved(
        params,
        &["_ids", "_or", "limit", "cursor", "as_of", "as_of_snapshot"],
    );
    let ids = match crate::query_params::parse_ids(reserved.last("_ids")) {
        Ok(ids) => ids,
        Err(m) => return (StatusCode::BAD_REQUEST, m).into_response(),
    };
    let or_raw: Vec<String> = reserved.all("_or").to_vec();
    let raw_limit = reserved.last("limit").map(String::from);
    let raw_cursor = reserved.last("cursor").map(String::from);
    let as_of = match parse_as_of(reserved.last("as_of"), reserved.last("as_of_snapshot")) {
        Ok(sel) => sel,
        Err(m) => return (StatusCode::BAD_REQUEST, m).into_response(),
    };
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
                as_of,
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
            as_of,
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

/// Subscribe to a CDC type's ordered change-event feed (chunked NDJSON).
///
/// Each line is one change event `{bucket, offset, change_kind, fields, cursor}`;
/// `cursor` is the opaque resume position AFTER that event. `?cursor=` accepts
/// `earliest` (default), `latest` (join the tail), or a previously returned
/// opaque cursor (type-bound). Ordering is per-bucket (gapless offsets); the
/// stream stays open (notify-driven, poll-fallback) unless `?max_events=` bounds
/// it. Policy is resolved at connect; a mid-stream policy change takes effect on
/// the next reconnect.
#[utoipa::path(
    get, path = "/objects/{type_name}/changes",
    params(
        ("type_name" = String, Path, description = "Ontology object type (must back a declared CDC table)"),
        ("cursor" = Option<String>, Query, description = "`earliest` (default) | `latest` | an opaque cursor from a previous event line"),
        ("fields" = Option<String>, Query, description = "Comma-separated projection over the governed columns"),
        ("max_events" = Option<u64>, Query, description = "Close the stream after N events (unset = endless tail)"),
    ),
    responses(
        (status = 200, description = "NDJSON change-event stream, one JSON object per line", body = String, content_type = "application/x-ndjson"),
        (status = 400, description = "Malformed/foreign cursor, unknown field, bad max_events, or the type's table is not a declared CDC table"),
        (status = 403, description = "Forbidden by ACL policy (checked before existence)"),
        (status = 404, description = "Unknown type"),
        (status = 501, description = "The serving engine does not implement the changelog feed"),
    ),
    security(("bearer_auth" = [])),
    tag = "objects",
)]
async fn get_changes(
    State(st): State<AppState>,
    Path(type_name): Path<String>,
    Query(q): Query<crate::subscribe::ChangesQuery>,
    subject: Subject,
) -> axum::response::Response {
    use crate::governed::{OnMissing, resolve_governed};
    use crate::subscribe::{CursorSpec, parse_cursor};

    // 1. Governance prologue: coarse Read gate before existence, then policy.
    let g = match resolve_governed(
        st.cp.ontology(),
        st.cp.acl(),
        &subject.0,
        &TypeName(type_name.clone()),
        OnMissing::NotFound,
    )
    .await
    {
        Ok(g) => g,
        Err(e) => return query_error_response(e, "changes read gate fault"),
    };
    let table = g.otype.table.clone();

    // 2. Cursor syntax + type binding (client faults before any engine call).
    let spec = match parse_cursor(q.cursor.as_deref()) {
        Ok(s) => s,
        Err(m) => return (StatusCode::BAD_REQUEST, m).into_response(),
    };
    if let CursorSpec::Resume(c) = &spec
        && c.t != type_name
    {
        return (
            StatusCode::BAD_REQUEST,
            "cursor was minted for another type",
        )
            .into_response();
    }

    // 3. ?fields= intersects the governed allowed set.
    let allowed = g.allowed();
    let fields = match q.fields.as_deref() {
        None => None,
        Some(raw) => {
            let want: Vec<String> = raw
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
            if let Some(bad) = want.iter().find(|f| !allowed.contains(f)) {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("unknown or denied field `{bad}`"),
                )
                    .into_response();
            }
            Some(want)
        }
    };

    // 4. Probe the engine: subscribable? (also yields the bucket set).
    let latest = match st.serving.changelog_latest(&table).await {
        Ok(Some(l)) => l,
        Ok(None) => {
            return (
                StatusCode::BAD_REQUEST,
                "type is not backed by a declared stream table",
            )
                .into_response();
        }
        Err(crate::serving::ServingError::Unsupported(_)) => {
            return (
                StatusCode::NOT_IMPLEMENTED,
                "changelog feed not available on this engine",
            )
                .into_response();
        }
        Err(e) => return internal_error("changelog latest fault", e),
    };

    // 5. Boot positions: earliest = 0 per bucket; latest = the high-water map;
    // resume = the cursor's map normalized onto the live bucket set (missing
    // buckets start at 0; unknown buckets dropped).
    let positions = match spec {
        CursorSpec::Earliest => latest.keys().map(|b| (*b, 0)).collect(),
        CursorSpec::Latest => latest,
        CursorSpec::Resume(c) => latest
            .keys()
            .map(|b| (*b, c.b.get(b).copied().unwrap_or(0)))
            .collect(),
    };

    let policy = crate::serving::ChangeFeedPolicy {
        row_filters: g.row_filters.clone(),
        denied: g.denied.iter().cloned().collect(),
        masked: g.masked.iter().cloned().collect(),
    };
    let stream = crate::subscribe::ndjson_feed_stream(crate::subscribe::FeedState {
        serving: st.serving.clone(),
        table,
        type_name,
        positions,
        policy,
        fields,
        remaining: q.max_events,
    });
    match axum::response::Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "application/x-ndjson")
        .body(axum::body::Body::from_stream(stream))
    {
        Ok(resp) => resp,
        Err(e) => internal_error("changes response build fault", e),
    }
}

/// Parse the mutually-exclusive `?as_of=` (RFC3339) / `?as_of_snapshot=` (i64) selectors.
/// Both present, a non-integer id, or an unparseable timestamp -> `Err(message)` (400).
fn parse_as_of(
    as_of: Option<&str>,
    as_of_snapshot: Option<&str>,
) -> Result<Option<crate::handler::AsOfSelector>, String> {
    match (as_of, as_of_snapshot) {
        (Some(_), Some(_)) => Err("as_of and as_of_snapshot are mutually exclusive".into()),
        (None, None) => Ok(None),
        (None, Some(id)) => id
            .parse::<i64>()
            .map(|n| Some(crate::handler::AsOfSelector::Snapshot(n)))
            .map_err(|e| format!("as_of_snapshot must be an integer snapshot id: {e}")),
        (Some(ts), None) => {
            time::OffsetDateTime::parse(ts, &time::format_description::well_known::Rfc3339)
                .map(|t| Some(crate::handler::AsOfSelector::Time(t)))
                .map_err(|e| format!("as_of must be an RFC3339 timestamp: {e}"))
        }
    }
}

/// Traverse a single link to its target objects (or association rows).
///
/// `_direction`, `_shape`, and `_ids` are reserved; other params filter the target.
/// Governed by ACL.
#[utoipa::path(
    get, path = "/objects/{from_type}/links/{link_name}",
    params(
        ("from_type" = String, Path, description = "Source object type"),
        ("link_name" = String, Path, description = "Link to traverse"),
        ("_direction" = Option<String>, Query, description = "Hop direction: `forward` (default) or `inverse`"),
        ("_shape" = Option<String>, Query, description = "Response shape: `objects` (default) or `association` (id pairs)"),
        ("_ids" = Option<String>, Query, description = "Comma-separated source identity set to restrict the traversal"),
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
    // Split `_direction` (single-hop knob), `_shape`, and `_ids` out of the params; the rest
    // are filters.
    let (reserved, filter_params) =
        crate::query_params::split_reserved(params, &["_direction", "_shape", "_ids"]);
    let ids = match crate::query_params::parse_ids(reserved.last("_ids")) {
        Ok(ids) => ids,
        Err(m) => return (StatusCode::BAD_REQUEST, m).into_response(),
    };
    let direction = match parse_direction(reserved.last("_direction")) {
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
    let query = ChainQuery {
        from_type,
        path: vec![Hop {
            link: link_name,
            direction,
        }],
        filters,
        ids,
    };
    respond_shaped(&st, query, reserved.last("_shape"), &subject).await
}

/// Traverse a `?path=` chain of links to the reached objects.
///
/// `?path=l1,l2` names the ordered link chain; `_shape` and `_ids` are reserved.
/// Governed by ACL.
#[utoipa::path(
    get, path = "/objects/{from_type}/links",
    params(
        ("from_type" = String, Path, description = "Source object type"),
        ("_path" = String, Query, description = "Comma-separated ordered link chain, e.g. `l1,l2`; a `~`-prefixed hop is inverse"),
        ("_shape" = Option<String>, Query, description = "Response shape: `objects` (default) or `association` (id pairs)"),
        ("_ids" = Option<String>, Query, description = "Comma-separated source identity set to restrict the traversal"),
    ),
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
    let (reserved, filter_params) =
        crate::query_params::split_reserved(params, &["_path", "_shape", "_ids"]);
    let ids = match crate::query_params::parse_ids(reserved.last("_ids")) {
        Ok(ids) => ids,
        Err(m) => return (StatusCode::BAD_REQUEST, m).into_response(),
    };
    let hops: Vec<Hop> = reserved
        .last("_path")
        .map(parse_path_hops)
        .unwrap_or_default();
    // Filter keys reference bare link names; resolve against those (direction-independent).
    let names: Vec<String> = hops.iter().map(|h| h.link.clone()).collect();
    let filters = match crate::chain_filter::resolve_chain_filters(&names, filter_params) {
        Ok(f) => f,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let query = ChainQuery {
        from_type,
        path: hops,
        filters,
        ids,
    };
    respond_shaped(&st, query, reserved.last("_shape"), &subject).await
}

/// The shared single-hop/chain response tail: dispatch `_shape` (objects default,
/// association pairs, unknown -> 400) over the governed chain read. The get_linked
/// and get_linked_chain tails were verbatim copies of this.
async fn respond_shaped(
    st: &AppState,
    query: ChainQuery,
    shape: Option<&str>,
    subject: &Subject,
) -> axum::response::Response {
    let deps = st.deps();
    match shape {
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

/// The single, TOTAL `QueryError` -> HTTP response mapping — the union of the four
/// partial per-route copies it replaced (object/chain/graph/search). No catch-all
/// over `QueryError`'s variants: adding a variant fails compilation here, forcing a
/// deliberate status. Client-fault variants echo only caller-supplied names (type/
/// column/link — no internal detail); backend faults go through `internal_error`
/// (logged server-side, opaque body). `Serving` splits: `NoIndex` is the /search
/// 404, `DimMismatch` its 400 — both constructed only on the vector-search path —
/// and everything else is an opaque 500. `Plan` is the planning-fault 400 —
/// constructed only off the engine wire/in-process planner.
pub fn query_error_response(e: QueryError, context: &'static str) -> axum::response::Response {
    match e {
        QueryError::UnknownType(t) => (StatusCode::NOT_FOUND, t).into_response(),
        QueryError::UnknownLink(l) => (StatusCode::NOT_FOUND, l).into_response(),
        QueryError::AmbiguousLink(l) => (StatusCode::BAD_REQUEST, l).into_response(),
        QueryError::Forbidden => StatusCode::FORBIDDEN.into_response(),
        QueryError::BadFilter(c) => (StatusCode::BAD_REQUEST, c).into_response(),
        QueryError::BadFilterValue(e) => bad_filter_value_response(&e),
        QueryError::BadChain(m) => (StatusCode::BAD_REQUEST, m).into_response(),
        QueryError::NoIdentity(t) => (StatusCode::BAD_REQUEST, t).into_response(),
        QueryError::NotCyclicPath(p) => (StatusCode::BAD_REQUEST, p).into_response(),
        QueryError::BadGraphPath(m) => (StatusCode::BAD_REQUEST, m).into_response(),
        QueryError::BadPagination(m) => (StatusCode::BAD_REQUEST, m).into_response(),
        QueryError::AsOfNotFound(m) => (StatusCode::NOT_FOUND, m).into_response(),
        QueryError::AsOfGone(m) => (StatusCode::GONE, m).into_response(),
        QueryError::Serving(crate::serving::ServingError::NoIndex(m)) => {
            (StatusCode::NOT_FOUND, m).into_response()
        }
        QueryError::Serving(crate::serving::ServingError::DimMismatch(m)) => {
            (StatusCode::BAD_REQUEST, m).into_response()
        }
        QueryError::Serving(crate::serving::ServingError::Plan(m)) => {
            (StatusCode::BAD_REQUEST, m).into_response()
        }
        e @ (QueryError::ControlPlane(_)
        | QueryError::Serving(_)
        | QueryError::Malformed(_)
        | QueryError::Internal { .. }) => internal_error(context, e),
    }
}

// Deliberately `const`, not config: safety guardrails bounding traversal recursion /
// blast radius that an operator must not be able to lift per-deployment.
// `DEFAULT_GRAPH_DEPTH` stays const because it is coupled to the `MAX_GRAPH_DEPTH`
// guardrail, not a deployment concern. See road-config-seam-unification.
const MAX_GRAPH_DEPTH: u32 = 10;
const DEFAULT_GRAPH_DEPTH: u32 = 5;

/// Parse a `?tree=` (or similar) boolean flag token. Accepts `true`/`false` (case-insensitive);
/// any other value is a 400. Absent -> `false` (the caller defaults before calling this).
fn parse_bool_flag(v: &str) -> Result<bool, ()> {
    match v.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(()),
    }
}

/// The shared `/graph` route knobs — `_ids`, `depth` (default + `MAX_GRAPH_DEPTH`
/// range guardrail), `tree` — pulled from the route's reserved params. Err carries
/// the ready 400 response (fixed check order: ids, depth parse, tree, depth range),
/// boxed to keep the `Result`'s Err variant small (`clippy::result_large_err`).
fn graph_knobs(
    reserved: &crate::query_params::ReservedParams,
) -> Result<(Vec<String>, u32, bool), Box<axum::response::Response>> {
    let ids = crate::query_params::parse_ids(reserved.last("_ids"))
        .map_err(|m| Box::new((StatusCode::BAD_REQUEST, m).into_response()))?;
    let depth = crate::query_params::parse_depth(reserved.last("depth"), DEFAULT_GRAPH_DEPTH)
        .map_err(|m| Box::new((StatusCode::BAD_REQUEST, m).into_response()))?;
    let tree = match reserved.last("tree") {
        None => false,
        Some(v) => parse_bool_flag(v).map_err(|()| {
            Box::new((StatusCode::BAD_REQUEST, "tree must be true or false").into_response())
        })?,
    };
    if !(1..=MAX_GRAPH_DEPTH).contains(&depth) {
        return Err(Box::new(
            (
                StatusCode::BAD_REQUEST,
                format!("depth must be 1..={MAX_GRAPH_DEPTH}"),
            )
                .into_response(),
        ));
    }
    Ok((ids, depth, tree))
}

/// Recurse a self-link from a seed type to all reachable objects.
///
/// `?depth` bounds the traversal (capped); `?tree=true` returns a shortest-path tree
/// instead of the flat reachable set. Governed by ACL.
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
    // Split `depth`, `_ids`, and `tree` out; the rest are seed filters.
    let (reserved, filters) =
        crate::query_params::split_reserved(params, &["depth", "_ids", "tree"]);
    let (ids, depth, tree) = match graph_knobs(&reserved) {
        Ok(t) => t,
        Err(resp) => return *resp,
    };
    let q = GraphQuery {
        type_name,
        path: vec![link_name.into()],
        depth,
        filters,
        ids,
    };
    if tree {
        graph_tree_respond(&st, q, &subject).await
    } else {
        graph_respond(&st, GraphReadSpec::PathCycle(&q), &subject).await
    }
}

/// Traverse a multi-link `?path=l1,l2` chain and return the reached objects.
///
/// `?path=` is a comma-separated hop chain (empty or absent is a 400); a `~`-prefixed
/// hop is followed backward (an inverse hop). Supports the same `depth`, `_ids`, and
/// filter parameters as the single-link graph route.
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
    let (reserved, filters) =
        crate::query_params::split_reserved(params, &["path", "links", "depth", "_ids", "tree"]);
    let (ids, depth, tree) = match graph_knobs(&reserved) {
        Ok(t) => t,
        Err(resp) => return *resp,
    };
    let path: Vec<Hop> = reserved
        .last("path")
        .map(parse_path_hops)
        .unwrap_or_default();
    let links: Vec<String> = reserved
        .last("links")
        .map(crate::query_params::comma_list)
        .unwrap_or_default();
    match crate::path_parse::parse_graph_mode(path, links, tree) {
        Err(m) => (StatusCode::BAD_REQUEST, m).into_response(),
        Ok(crate::path_parse::GraphMode::Union(links)) => {
            let q = GraphUnionQuery {
                type_name,
                links,
                depth,
                filters,
                ids,
            };
            graph_respond(&st, GraphReadSpec::UnionSelfLinks(&q), &subject).await
        }
        Ok(crate::path_parse::GraphMode::CoreTail {
            core_link,
            tail_links,
        }) => {
            let q = GraphTailQuery {
                type_name,
                core_link,
                tail_links,
                depth,
                filters,
                ids,
            };
            graph_respond(&st, GraphReadSpec::CoreTail(&q), &subject).await
        }
        Ok(crate::path_parse::GraphMode::PathCycle(path)) => {
            let q = GraphQuery {
                type_name,
                path,
                depth,
                filters,
                ids,
            };
            if tree {
                graph_tree_respond(&st, q, &subject).await
            } else {
                graph_respond(&st, GraphReadSpec::PathCycle(&q), &subject).await
            }
        }
    }
}

/// The one /graph reachability tail: run the spec'd read via the handler spine,
/// render, map errors.
async fn graph_respond(
    st: &AppState,
    spec: GraphReadSpec<'_>,
    subject: &Subject,
) -> axum::response::Response {
    let deps = st.deps();
    match read_graph_reach_spec(spec, subject, &deps).await {
        Ok(rows) => Json(crate::render::objects_to_json(&rows, None)).into_response(),
        Err(e) => query_error_response(e, "graph read serving fault"),
    }
}

/// Tree tail for `?tree=true` on the path-cycle routes: run `read_graph_tree`,
/// render `{roots, nodes}` via `tree_to_json`, map errors via `query_error_response`.
async fn graph_tree_respond(
    st: &AppState,
    q: GraphQuery,
    subject: &Subject,
) -> axum::response::Response {
    let deps = st.deps();
    match read_graph_tree(&q, subject, &deps).await {
        Ok(tree) => Json(crate::render::tree_to_json(&tree)).into_response(),
        Err(e) => query_error_response(e, "graph read serving fault"),
    }
}

/// Invoke an ontology action by name (typed governed write).
///
/// The body is the action's parameter envelope. Insert responds 201 Created;
/// Update/Delete respond 200 OK. The body carries the affected object.
/// Governed by ACL.
#[utoipa::path(
    post, path = "/actions/{action_name}",
    params(("action_name" = String, Path, description = "Ontology action id")),
    request_body = serde_json::Value,
    responses(
        (status = 201, description = "Insert action applied: the created object (single-step) or the `steps` envelope (multi-step)", body = crate::openapi::ActionStepsBody),
        (status = 200, description = "Update/Delete action applied: the affected object (Update) or pre-deletion values (Delete); the per-action `/docs` entry states each action's exact status", body = crate::openapi::ActionStepsBody),
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
        Ok((outcome, run_id, kind)) => {
            let body = match outcome {
                crate::action::ActionOutcome::Single(rows) => {
                    // Byte-compatible single-object response: the bare affected object.
                    crate::render::objects_to_json(&rows, None)
                        .get("objects")
                        .and_then(|a| a.as_array())
                        .and_then(|a| a.first())
                        .cloned()
                        .unwrap_or(serde_json::Value::Null)
                }
                crate::action::ActionOutcome::Multi(steps) => {
                    let steps_json: Vec<serde_json::Value> = steps
                        .iter()
                        .map(|s| {
                            let objects = crate::render::objects_to_json(&s.rows, None)
                                .get("objects")
                                .cloned()
                                .unwrap_or_else(|| serde_json::json!([]));
                            serde_json::json!({ "bind": s.bind, "target": s.target, "objects": objects })
                        })
                        .collect();
                    serde_json::json!({ "steps": steps_json })
                }
            };
            // Kind-true status: Insert mints a new row (201 Created); Update/Delete mutate or
            // remove an existing one (200 OK). Multi-step actions take their primary kind from
            // the first declared step (see `run_multi_step`).
            let status = match kind {
                ActionKind::Insert => StatusCode::CREATED,
                ActionKind::Update | ActionKind::Delete => StatusCode::OK,
            };
            // Surface the action's run_id so a caller can locate its lineage via
            // Lineage::events_for. The single-step body is unchanged (non-invasive).
            let mut resp = (status, Json(body)).into_response();
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

/// Governed k-nearest-neighbour vector search over a type's named index.
///
/// The request body is validated before any engine call — a malformed body is a 400.
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

/// Map a `LineageVisibility` fault to a status. Scan-cap over-run is a 422 (the
/// closure is ungovernably large; never a partial page). A `Validation` fault
/// (over-cap/zero depth, malformed cursor) is a caller 400; anything else is an
/// opaque 500 logged server-side. Unknown / unreadable seed is NOT an error — the
/// filter returns an empty page.
fn lineage_visibility_error(
    e: crate::lineage_filter::LineageVisibilityError,
) -> axum::response::Response {
    use crate::lineage_filter::LineageVisibilityError as E;
    match e {
        E::ScanCapExceeded => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "provenance closure too large to govern; reduce depth",
        )
            .into_response(),
        E::Cp(ControlPlaneError::Validation(m)) => (StatusCode::BAD_REQUEST, m).into_response(),
        E::Cp(other) => internal_error("lineage read fault", other),
    }
}

/// Shared upstream/downstream handler: parse `depth` (default 1, forwarded to the
/// capability which caps it), `after`/`limit` (-> `PageReq`), then call the governed
/// closure read and serialize the page. `depth` beyond `LINEAGE_MAX_DEPTH` (or 0) is
/// rejected BELOW by the filter as `Validation` -> 400 — the wire cannot trigger an
/// unbounded walk.
async fn lineage_closure(
    st: &AppState,
    namespace: String,
    name: String,
    params: Vec<(String, String)>,
    dir: crate::lineage_filter::LineageDir,
    subject: Subject,
) -> axum::response::Response {
    // Unknown params were ignored before the splitter; discarding the "filters" side keeps that.
    let (reserved, _) = crate::query_params::split_reserved(params, &["depth", "after", "limit"]);
    let depth = match crate::query_params::parse_depth(reserved.last("depth"), 1) {
        Ok(d) => d,
        Err(m) => return (StatusCode::BAD_REQUEST, m).into_response(),
    };
    let after = reserved.last("after").map(String::from);
    let limit = reserved.last("limit").map(String::from);
    let page = match crate::lineage_read::parse_lineage_page(after, limit) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let seed = DatasetRef { namespace, name };
    let vis = crate::lineage_filter::LineageVisibility::new(
        st.cp.acl(),
        st.cp.lineage(),
        st.cp.ontology(),
        st.naming.as_ref(),
    );
    let res = vis
        .visible_closure(&subject.0, &seed, depth, dir, &page)
        .await;
    match res {
        Ok(page) => Json(crate::lineage_read::dataset_closure_body(page)).into_response(),
        Err(e) => lineage_visibility_error(e),
    }
}

/// List a dataset's upstream lineage closure.
///
/// Walks producers to `?depth` (default 1, capped); paginated via `after`/`limit`.
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
    subject: Subject,
) -> axum::response::Response {
    lineage_closure(
        &st,
        namespace,
        name,
        params,
        crate::lineage_filter::LineageDir::Upstream,
        subject,
    )
    .await
}

/// List a dataset's downstream lineage closure.
///
/// Walks consumers to `?depth` (default 1, capped); paginated via `after`/`limit`.
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
    subject: Subject,
) -> axum::response::Response {
    lineage_closure(
        &st,
        namespace,
        name,
        params,
        crate::lineage_filter::LineageDir::Downstream,
        subject,
    )
    .await
}

/// List the lineage events emitted by a run.
///
/// Paginated via `after`/`limit`.
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
    subject: Subject,
) -> axum::response::Response {
    let Ok(uuid) = uuid::Uuid::parse_str(&run_id) else {
        return (StatusCode::BAD_REQUEST, "run_id must be a UUID").into_response();
    };
    let (reserved, _) = crate::query_params::split_reserved(params, &["after", "limit"]);
    let after = reserved.last("after").map(String::from);
    let limit = reserved.last("limit").map(String::from);
    let page = match crate::lineage_read::parse_lineage_page(after, limit) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    match st.cp.lineage().events_for(&RunId(uuid), page).await {
        Ok(page) => {
            let vis = crate::lineage_filter::LineageVisibility::new(
                st.cp.acl(),
                st.cp.lineage(),
                st.cp.ontology(),
                st.naming.as_ref(),
            );
            match vis.redact_events(&subject.0, page).await {
                Ok(red) => Json(crate::lineage_read::run_events_body(red)).into_response(),
                Err(e) => lineage_visibility_error(e),
            }
        }
        Err(e) => lineage_visibility_error(crate::lineage_filter::LineageVisibilityError::Cp(e)),
    }
}

/// List the lineage runs that touched a dataset, newest-first.
///
/// Per-dataset ACL-gated by seed readability (same predicate the `/lineage`
/// closures use). A denied or unknown dataset returns an empty page, never a 404 —
/// the endpoint is not a dataset-existence oracle. Paginated via `after`/`limit`.
#[utoipa::path(
    get, path = "/lineage/datasets/{namespace}/{name}/runs",
    params(
        ("namespace" = String, Path, description = "OpenLineage dataset namespace"),
        ("name" = String, Path, description = "OpenLineage dataset name"),
        ("after" = Option<String>, Query, description = "Opaque next-page cursor"),
        ("limit" = Option<u32>, Query, description = "Max runs per page"),
    ),
    responses(
        (status = 200, description = "Runs that touched the dataset (empty if unreadable/unknown)", body = crate::lineage_read::DatasetRunsResponse),
        (status = 400, description = "Malformed limit"),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "lineage",
)]
async fn get_lineage_dataset_runs(
    State(st): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<Vec<(String, String)>>,
    subject: Subject,
) -> axum::response::Response {
    let (reserved, _) = crate::query_params::split_reserved(params, &["after", "limit"]);
    let after = reserved.last("after").map(String::from);
    let limit = reserved.last("limit").map(String::from);
    let page = match crate::lineage_read::parse_lineage_page(after, limit) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let dataset = DatasetRef { namespace, name };
    let vis = crate::dataset_acl::DatasetVisibility::new(
        st.cp.acl(),
        st.cp.ontology(),
        st.naming.as_ref(),
    );
    match vis.is_readable(&subject.0, &dataset).await {
        Ok(true) => {}
        // Denied seed ≡ unknown seed: an empty page, never 404 (non-oracle),
        // matching the closure reads' seed gating.
        Ok(false) => {
            return Json(crate::lineage_read::dataset_runs_body(
                control_plane_core::Page::from_full(Vec::new()),
            ))
            .into_response();
        }
        Err(e) => return cp_read_error("dataset runs acl fault", e),
    }
    match st.cp.lineage().runs_for(&dataset, page).await {
        Ok(page) => Json(crate::lineage_read::dataset_runs_body(page)).into_response(),
        Err(e) => cp_read_error("dataset runs read fault", e),
    }
}
