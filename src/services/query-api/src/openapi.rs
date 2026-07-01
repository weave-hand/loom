//! query-api's static OpenAPI document. `build_openapi()` returns it as a value (the
//! ontology hook: slice 2 will `.paths.extend(...)` ontology-derived operations into
//! the same document). DTOs here are documentation shapes for the dynamic JSON the
//! handlers actually emit (`render::objects_to_json` produces open object maps).

use utoipa::{OpenApi, ToSchema};

/// Documentation shape for the `{ "objects": [ {..}, .. ] }` read response. Each object
/// is an open property map; utoipa maps `serde_json::Value` to a free-form object schema
/// (verified to derive `ToSchema` directly — no `#[schema(value_type)]` override needed).
#[derive(ToSchema)]
pub struct ObjectsResponse {
    /// The projected rows; each is an object-type's properties as a JSON map.
    pub objects: Vec<serde_json::Value>,
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
    ),
    components(schemas(
        ObjectsResponse,
        VectorSearchHit,
        VectorSearchResponse,
        JobAck,
        WriteDeniedBody,
        crate::http::VectorSearchRequest,
    ))
)]
pub struct ApiDoc;

/// Build the static OpenAPI document. Returns a value (not a constant) so the live document
/// (`live_openapi`) can merge ontology-derived operations through this same seam.
#[must_use]
pub fn build_openapi() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

/// Bound on `list_types` page draining — a defensive cap so a misbehaving cursor can never
/// spin forever. Adapters return one full page today; this tolerates future keyset paging.
const MAX_TYPE_PAGES: usize = 10_000;

/// Build the OpenAPI document with per-request ontology-derived operations merged onto the
/// static base. Reads the live ontology through `cp` (`list_types` + per-type `links`), runs
/// the pure generator, and extends the base document's paths + component schemas. On a read
/// error it logs and returns the static base unchanged — a docs endpoint must never fail the
/// whole document because an ontology read hiccupped.
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

    let (paths, schemas) = crate::openapi_gen::ontology_openapi(&types, &links);
    doc.paths.paths.extend(paths.paths);
    if let Some(components) = doc.components.as_mut() {
        components.schemas.extend(schemas);
    }
    doc
}
