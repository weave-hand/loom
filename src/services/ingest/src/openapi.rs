//! ingest's static OpenAPI document. `build_openapi()` returns it as a value (the
//! ontology hook for slice 2). DTOs are documentation shapes for the JSON the
//! handlers emit; the request body is binary Arrow IPC, documented as such.

use serde::Serialize;
use utoipa::{OpenApi, ToSchema};

use crate::gate::{Violation, ViolationReason};

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

/// The 422 model-gate violations body: `{ "violations": [ WireViolation, ... ] }`.
/// Serializes on the wire AND documents the response schema — one shape, no drift.
#[derive(Serialize, ToSchema)]
pub struct ViolationsBody {
    pub violations: Vec<WireViolation>,
}

/// One gate violation on the wire (a column, the reason token, and the
/// reason-specific fields). This single type is BOTH the serialized 422 body
/// element and the OpenAPI documentation schema, replacing the former split
/// between `http::violations_json` and a doc-only struct.
///
/// Fields are declared alphabetically and the reason-specific ones are
/// `skip_serializing_if` — loom's `serde_json` has no `preserve_order`, so the
/// `serde_json::Value` shape this replaced serialized its keys sorted; matching
/// that order + omitting absent keys keeps the bytes identical.
#[derive(Serialize, ToSchema)]
pub struct WireViolation {
    /// The offending column.
    pub column: String,
    /// The model-declared type — present only for `type_mismatch`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<String>,
    /// The inferred Arrow type — present only for `type_mismatch`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub found: Option<String>,
    /// `missing_required` | `type_mismatch` | `unsupported` | `constraint`.
    pub reason: String,
    /// The failed constraint rule (`range`|`length`|`pattern`|`one_of`) — present
    /// only for `constraint`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule: Option<String>,
}

impl From<&Violation> for WireViolation {
    fn from(v: &Violation) -> Self {
        match &v.reason {
            ViolationReason::MissingRequired => WireViolation {
                column: v.column.clone(),
                expected: None,
                found: None,
                reason: "missing_required".to_string(),
                rule: None,
            },
            ViolationReason::TypeMismatch { expected, found } => WireViolation {
                column: v.column.clone(),
                expected: Some(expected.clone()),
                found: Some(found.clone()),
                reason: "type_mismatch".to_string(),
                rule: None,
            },
            ViolationReason::Unsupported => WireViolation {
                column: v.column.clone(),
                expected: None,
                found: None,
                reason: "unsupported".to_string(),
                rule: None,
            },
            ViolationReason::Constraint { rule } => WireViolation {
                column: v.column.clone(),
                expected: None,
                found: None,
                reason: "constraint".to_string(),
                rule: Some(rule.clone()),
            },
        }
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "loom ingest",
        description = "Arrow-IPC landing + compaction API"
    ),
    paths(crate::http::land, crate::http::land_model, crate::http::compact),
    components(schemas(LandAck, ModelLandAck, JobAck, ViolationsBody, WireViolation))
)]
pub struct ApiDoc;

/// Build the static OpenAPI document (a value — the slice-2 ontology hook). The service's
/// own paths are merged with the `service_runtime` fragments for the runtime routes this
/// service mounts (`serve.rs`): auth and service-account — ingest mounts no admin router.
#[must_use]
pub fn build_openapi() -> utoipa::openapi::OpenApi {
    let mut doc = ApiDoc::openapi();
    doc.merge(service_runtime::auth_openapi());
    doc.merge(service_runtime::service_account_openapi());
    doc
}
