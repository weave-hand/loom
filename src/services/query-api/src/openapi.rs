//! query-api's static OpenAPI document. `build_openapi()` returns it as a value so the
//! live document (`live_openapi`) can extend it with ontology-derived operations per
//! request. DTOs here are documentation shapes for the dynamic JSON the handlers
//! actually emit (`render::objects_to_json` produces open object maps).

use utoipa::{OpenApi, ToSchema};

/// Documentation shape for the `{ "objects": [ {..}, .. ] }` read response. Each object
/// is an open property map; utoipa maps `serde_json::Value` to a free-form object schema
/// (verified to derive `ToSchema` directly — no `#[schema(value_type)]` override needed).
#[derive(ToSchema)]
pub struct ObjectsResponse {
    /// The projected rows; each is an object-type's properties as a JSON map.
    pub objects: Vec<serde_json::Value>,
    /// Keyset cursor for the next page, or `null` on the last page (or when the read was
    /// not paginated — `?limit=`/`?cursor=` absent).
    pub next: Option<String>,
}

/// Documentation shape for the `{ "roots": [...], "nodes": [...] }` shortest-path-tree
/// response served when `?tree=true` is set on the graph routes. `roots` are identity values;
/// each node carries `id`, `depth`, `parent` (null for a root) and the governed typed `object`.
#[derive(ToSchema)]
pub struct ObjectTreeResponse {
    pub roots: Vec<serde_json::Value>,
    pub nodes: Vec<serde_json::Value>,
}

/// Documentation shape for a single kNN hit.
#[derive(ToSchema)]
pub struct VectorSearchHit {
    /// The object's identity (number or string, per the identity column kind).
    pub id: serde_json::Value,
    /// Distance from the probe vector.
    pub distance: f32,
}

/// Documentation shape for the `{ "results": [..] }` search response.
#[derive(ToSchema)]
pub struct VectorSearchResponse {
    pub results: Vec<VectorSearchHit>,
}

/// Documentation shape for a 202 job-enqueue acknowledgement.
#[derive(ToSchema)]
pub struct JobAck {
    /// The enqueued job's id (UUID string).
    pub job_id: String,
}

/// Documentation shape for a fine-grained Write-denial 403 body, mirroring
/// `action::WriteDenialReason::to_body`.
#[derive(ToSchema)]
pub struct WriteDeniedBody {
    /// Stable tag, always `"write_denied"`.
    pub error: String,
    /// What was denied: `"column"` or `"row_filter"`.
    pub reason: String,
    /// The denied column — present only for column denials.
    pub column: Option<String>,
}

/// One step's affected object in a multi-step action response.
#[derive(ToSchema)]
pub struct ActionStepResult {
    /// The step's declared bind name, when it declared one.
    pub bind: Option<String>,
    /// The step's target type name (always present).
    pub target: String,
    /// The step's affected object rows (same shape as an object read).
    pub objects: Vec<serde_json::Value>,
}

/// The 201 body of a MULTI-step action: one entry per declared step, in order.
/// (A single-step action returns the bare affected object instead.)
#[derive(ToSchema)]
pub struct ActionStepsBody {
    pub steps: Vec<ActionStepResult>,
}

/// Documentation shape for a 422 constraint-violation body on a typed-insert action.
#[derive(ToSchema)]
pub struct ConstraintViolationsBody {
    pub violations: Vec<ConstraintViolationItem>,
}

/// One constraint violation: the property and the rule it failed.
#[derive(ToSchema)]
pub struct ConstraintViolationItem {
    pub property: String,
    /// `range` | `length` | `pattern` | `one_of`.
    pub rule: String,
}

/// Documentation shape for the `{ "types": [..] }` ontology type-catalog response.
#[derive(ToSchema)]
pub struct OntologyTypesResponse {
    /// The defined object-type names.
    pub types: Vec<String>,
}

/// Documentation shape for a `{ "schema": .., "name": .. }` table reference.
#[derive(ToSchema)]
pub struct TableRefView {
    pub schema: String,
    pub name: String,
}

/// Documentation shape for one declared property in a type-detail response.
#[derive(ToSchema)]
pub struct PropertyView {
    pub name: String,
    /// The ontology's LOGICAL type name (e.g. `Long`), not the physical column type.
    pub ty: String,
    pub required: bool,
}

/// Documentation shape for one link in a type-detail response. The physical backing
/// (FK / join table) is deliberately not exposed.
#[derive(ToSchema)]
pub struct LinkView {
    pub name: String,
    /// Source object type.
    pub from: String,
    /// Target object type.
    pub to: String,
    /// `one` | `many`.
    pub cardinality: String,
}

/// Documentation shape for the `GET /ontology/types/{name}` type-detail response.
#[derive(ToSchema)]
pub struct TypeDetailResponse {
    pub name: String,
    /// The physical Iceberg table backing this type.
    pub table: TableRefView,
    /// The identity (primary-key) property, or `null` when none is declared.
    pub identity: Option<String>,
    /// The declared properties, in order.
    pub properties: Vec<PropertyView>,
    /// Outbound links (`from` = this type).
    pub links: Vec<LinkView>,
    /// Inbound links (`to` = this type).
    pub links_to: Vec<LinkView>,
}

/// Documentation shape for the `GET /datasets` response.
#[derive(ToSchema)]
pub struct DatasetsResponse {
    /// Every table currently live in the mirror, `(schema, name)`-ordered.
    pub datasets: Vec<TableRefView>,
}

/// Documentation shape for one column in a dataset-detail response.
#[derive(ToSchema)]
pub struct DatasetColumnView {
    pub name: String,
    /// The column's loom LOGICAL type name.
    pub ty: String,
    pub nullable: bool,
}

/// Documentation shape for the `GET /datasets/{schema}/{table}` response.
#[derive(ToSchema)]
pub struct DatasetDetailResponse {
    pub table: TableRefView,
    /// The table's current (latest live) snapshot id.
    /// The mirror's own sequence-allocated snapshot id (migration 0013) — safe as a
    /// JSON number because it is sequential and never approaches 2^53. Do NOT swap in
    /// the random Iceberg-native `iceberg_snapshot_id` without moving to the string
    /// encoding the wire uses for arbitrary int64s.
    pub snapshot_id: i64,
    /// RFC3339 timestamp of that snapshot.
    pub snapshot_time: String,
    /// The column schema at that snapshot, in column order.
    pub columns: Vec<DatasetColumnView>,
}

/// Documentation shape for the `GET /datasets/{schema}/{table}/preview` response.
#[derive(ToSchema)]
pub struct DatasetPreviewResponse {
    pub columns: Vec<String>,
    /// Every sampled cell rendered to a display string (`""` for `NULL`).
    pub rows: Vec<Vec<String>>,
    /// Always `true` — a marker that this is a LIMIT-bounded sample, not a full read.
    pub sampled: bool,
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "loom query-api",
        description = "Governed typed-object read + action API"
    ),
    paths(
        crate::http::get_object,
        crate::http::get_linked,
        crate::http::get_linked_chain,
        crate::http::get_graph,
        crate::http::get_graph_path,
        crate::http::post_action,
        crate::http::post_search,
        crate::http::enqueue_gc,
        crate::http::get_lineage_upstream,
        crate::http::get_lineage_downstream,
        crate::http::get_lineage_run_events,
        crate::http::list_ontology_types,
        crate::http::get_ontology_type,
        crate::http::list_datasets,
        crate::http::get_dataset,
        crate::http::dataset_preview,
    ),
    components(schemas(
        ObjectsResponse,
        ObjectTreeResponse,
        VectorSearchHit,
        VectorSearchResponse,
        JobAck,
        WriteDeniedBody,
        ActionStepsBody,
        ActionStepResult,
        ConstraintViolationsBody,
        ConstraintViolationItem,
        OntologyTypesResponse,
        TableRefView,
        PropertyView,
        LinkView,
        TypeDetailResponse,
        DatasetsResponse,
        DatasetColumnView,
        DatasetDetailResponse,
        DatasetPreviewResponse,
        crate::http::VectorSearchRequest,
        crate::lineage_read::DatasetNode,
        crate::lineage_read::DatasetClosureResponse,
        crate::lineage_read::LineageEventView,
        crate::lineage_read::RunEventsResponse,
    ))
)]
pub struct ApiDoc;

/// Build the static OpenAPI document. Returns a value (not a constant) so the live document
/// (`live_openapi`) can merge ontology-derived operations through this same seam. The
/// service's own paths are merged with the `service_runtime` fragments for the runtime
/// routes this service mounts (`serve.rs`): auth, service-account, and admin.
#[must_use]
pub fn build_openapi() -> utoipa::openapi::OpenApi {
    let mut doc = ApiDoc::openapi();
    doc.merge(service_runtime::auth_openapi());
    doc.merge(service_runtime::service_account_openapi());
    doc.merge(service_runtime::admin_openapi());
    doc
}

/// Bound on `list_types`/`list_actions` page draining — a defensive cap so a misbehaving
/// cursor can never spin forever. Adapters return one full page today; this tolerates future
/// keyset paging.
const MAX_TYPE_PAGES: usize = 10_000;

/// Build the OpenAPI document with per-request ontology-derived operations merged onto the
/// static base. Reads the live ontology through `cp` (`list_types` + per-type `links` +
/// `list_actions`), runs the pure generator, and extends the base document's paths +
/// component schemas. On a read error it logs and degrades — a docs endpoint must never fail
/// the whole document because an ontology read hiccupped.
pub async fn live_openapi(
    cp: std::sync::Arc<dyn control_plane_core::ControlPlane + Send + Sync>,
) -> utoipa::openapi::OpenApi {
    use control_plane_core::PageReq;

    let mut doc = build_openapi();
    let onto = cp.ontology();

    // Drain every defined type (one full page today; loop tolerates future keyset paging).
    let mut types = Vec::new();
    let mut after = None;
    for _ in 0..MAX_TYPE_PAGES {
        let page = match onto
            .list_types(PageReq {
                after: after.clone(),
                limit: None,
            })
            .await
        {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "openapi: list_types failed; serving static base");
                return doc;
            }
        };
        types.extend(page.items);
        match page.next {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }

    // Per-type outbound links (a failed read on one type is logged and skipped, not fatal).
    let mut links = Vec::new();
    for ty in &types {
        match onto.links(&ty.name, PageReq::unbounded()).await {
            Ok(page) => links.extend(page.items),
            Err(e) => {
                tracing::warn!(type_name = %ty.name.0, error = %e, "openapi: links read failed");
            }
        }
    }

    // Drain every defined action (same bounded loop as types). Unlike a `list_types`
    // failure, an action-read error degrades to an actionless document rather than the
    // static base — the types/links already read are still worth serving.
    let mut actions = Vec::new();
    let mut after = None;
    for _ in 0..MAX_TYPE_PAGES {
        let page = match onto
            .list_actions(PageReq {
                after: after.clone(),
                limit: None,
            })
            .await
        {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "openapi: list_actions failed; serving without actions");
                actions.clear();
                break;
            }
        };
        actions.extend(page.items);
        match page.next {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }

    let (paths, schemas) = crate::openapi_gen::ontology_openapi(&types, &links, &actions);
    doc.paths.paths.extend(paths.paths);
    if let Some(components) = doc.components.as_mut() {
        components.schemas.extend(schemas);
    }
    doc
}
