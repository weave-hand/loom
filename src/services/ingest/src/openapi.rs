//! ingest's static OpenAPI document. `build_openapi()` returns it as a value (the
//! ontology hook for slice 2). DTOs are documentation shapes for the JSON the
//! handlers emit; the request body is binary Arrow IPC, documented as such.

use utoipa::{OpenApi, ToSchema};

/// Documentation shape for the land 200 acknowledgement.
#[derive(ToSchema)]
pub struct LandAck {
    /// The committed Iceberg snapshot id.
    pub snapshot_id: i64,
    /// `schema.table` of the landed dataset.
    pub dataset: String,
}

/// Documentation shape for the typed-model land 200 acknowledgement.
#[derive(ToSchema)]
pub struct ModelLandAck {
    /// The committed Iceberg snapshot id.
    pub snapshot_id: i64,
    /// The ontology type the rows were landed as.
    #[schema(rename = "type")]
    pub type_name: String,
}

/// Documentation shape for a 202 job-enqueue acknowledgement.
#[derive(ToSchema)]
pub struct JobAck {
    /// The enqueued job's id (UUID string).
    pub job_id: String,
}

/// Documentation shape for the 422 model-gate violations body.
#[derive(ToSchema)]
pub struct ViolationsBody {
    pub violations: Vec<Violation>,
}

/// One gate violation (a column and the reason it failed), mirroring
/// `http::violations_json`.
#[derive(ToSchema)]
pub struct Violation {
    pub column: String,
    /// `missing_required` | `type_mismatch` | `unsupported` | `constraint`.
    pub reason: String,
    /// The model-declared type — present only for `type_mismatch`.
    pub expected: Option<String>,
    /// The inferred Arrow type — present only for `type_mismatch`.
    pub found: Option<String>,
    /// The failed constraint rule (`range`|`length`|`pattern`|`one_of`) — present only
    /// for `constraint`.
    pub rule: Option<String>,
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "loom ingest",
        description = "Arrow-IPC landing + compaction API"
    ),
    paths(crate::http::land, crate::http::land_model, crate::http::compact),
    components(schemas(LandAck, ModelLandAck, JobAck, ViolationsBody, Violation))
)]
pub struct ApiDoc;

/// Build the static OpenAPI document (a value — the slice-2 ontology hook).
#[must_use]
pub fn build_openapi() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}
