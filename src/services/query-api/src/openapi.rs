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

/// Build the static OpenAPI document. Returns a value (not a constant) so slice 2 can
/// merge ontology-derived operations through this same seam.
#[must_use]
pub fn build_openapi() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}
