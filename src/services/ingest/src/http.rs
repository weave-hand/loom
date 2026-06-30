//! HTTP landing surface for ingest. Decodes an Arrow IPC request into what
//! `materialize` consumes (schema + batches + optional model gate + lineage),
//! drives the in-process land pipeline, and maps the result to HTTP. All landing
//! logic lives in `materialize`; this layer only does decode <-> HTTP mapping.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::post;
use control_plane_core::{
    Action, COMPACT_JOB_KIND, CompactJob, ControlPlane, ControlPlaneError, DatasetId, DatasetRef,
    Decision, EventType, LineageEvent, NewJob, PolicyTarget, RunId, TableRef, TypeName,
};
use serde::Deserialize;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::IngestError;
use crate::gate::{ColumnShape, ModelShape, Violation, ViolationReason};
use crate::landing::{LandRequest, LandingMaterializer};
use crate::materialize::resolve_columns;
use crate::model::model_shape_from_type;
use crate::openapi::{JobAck, LandAck, ModelLandAck, ViolationsBody};
use service_runtime::Subject;

/// Shared, owned dependencies: the configured landing backend (Iceberg),
/// chosen at boot. Gate + schema resolution + lineage are backend-agnostic
/// and happen in the handler before dispatch.
#[derive(Clone)]
pub struct AppState {
    pub materializer: Arc<dyn LandingMaterializer>,
    pub cp: Arc<dyn ControlPlane>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/datasets/:schema/:table", post(land))
        .route("/models/:type", post(land_model))
        .route("/tables/:schema/:table/compact", post(compact))
        .with_state(state)
}

/// Operator action: enqueue a compaction job for `{schema}.{table}`. Returns the
/// JobId; a zero-pool worker performs the compaction asynchronously.
#[utoipa::path(
    post, path = "/tables/{schema}/{table}/compact",
    params(
        ("schema" = String, Path, description = "Iceberg schema"),
        ("table" = String, Path, description = "Table name"),
    ),
    responses(
        (status = 202, description = "Compaction job enqueued", body = JobAck),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "tables",
)]
pub(crate) async fn compact(
    State(st): State<AppState>,
    Path((schema, table)): Path<(String, String)>,
) -> Response {
    let payload = match serde_json::to_value(CompactJob {
        schema,
        name: table,
    }) {
        Ok(v) => v,
        Err(_) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response();
        }
    };
    let job = NewJob {
        kind: COMPACT_JOB_KIND.to_string(),
        payload,
        run_at: None,
        priority: 0,
    };
    match st.cp.queue().enqueue(job).await {
        Ok(id) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "job_id": id.0.to_string() })),
        )
            .into_response(),
        // Opaque on backend faults (governance-fronted service).
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}

/// Decode an Arrow IPC stream into its schema and record batches.
fn decode_ipc(body: &[u8]) -> Result<(Arc<Schema>, Vec<RecordBatch>), arrow::error::ArrowError> {
    let reader = StreamReader::try_new(std::io::Cursor::new(body), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
}

/// Inbound model wire DTO. The gate types are serde-free domain types, so the
/// HTTP layer owns this representation and converts.
#[derive(Deserialize)]
struct LandModel {
    columns: Vec<LandColumn>,
}

#[derive(Deserialize)]
struct LandColumn {
    name: String,
    ty: String,
    required: bool,
}

impl From<LandModel> for ModelShape {
    fn from(m: LandModel) -> Self {
        ModelShape {
            columns: m
                .columns
                .into_iter()
                .map(|c| ColumnShape {
                    name: c.name,
                    ty: c.ty,
                    required: c.required,
                })
                .collect(),
        }
    }
}

/// Build the 422 body from gate violations (the domain enum is serde-free).
fn violations_json(violations: &[Violation]) -> serde_json::Value {
    let items: Vec<serde_json::Value> = violations
        .iter()
        .map(|v| match &v.reason {
            ViolationReason::MissingRequired => {
                serde_json::json!({ "column": v.column, "reason": "missing_required" })
            }
            ViolationReason::TypeMismatch { expected, found } => serde_json::json!({
                "column": v.column,
                "reason": "type_mismatch",
                "expected": expected,
                "found": found,
            }),
            ViolationReason::Unsupported => {
                serde_json::json!({ "column": v.column, "reason": "unsupported" })
            }
        })
        .collect();
    serde_json::json!({ "violations": items })
}

/// Governed model ingest: conform an Arrow batch to a pre-existing ontology type
/// and land it as typed objects into the type's table. Authorize before landing
/// (deny-by-default, no existence leak); a denied write never reaches the store.
#[utoipa::path(
    post, path = "/models/{type}",
    params(
        ("type" = String, Path, description = "Ontology type name"),
    ),
    request_body(
        content = Vec<u8>,
        description = "Arrow IPC stream (schema + record batches)",
        content_type = "application/vnd.apache.arrow.stream",
    ),
    responses(
        (status = 200, description = "Landed as typed objects; snapshot committed", body = ModelLandAck),
        (status = 400, description = "Invalid Arrow IPC / unsupported column type"),
        (status = 403, description = "Not authorized to write the type (also returned for an unknown type — no existence leak)"),
        (status = 422, description = "Data does not conform to the type", body = ViolationsBody),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "models",
)]
pub(crate) async fn land_model(
    State(st): State<AppState>,
    Path(type_name): Path<String>,
    subject: Subject,
    body: Bytes,
) -> Response {
    let type_name = TypeName(type_name);

    // 1. Coarse ACL gate BEFORE the type is resolved: an authenticated subject
    //    without a Write grant is 403, returned before we reveal whether the type
    //    exists. (`require_auth` already 401s an unauthenticated caller before this
    //    handler runs.) First Write use on the ingest plane.
    match st
        .cp
        .acl()
        .check(
            &subject.0,
            Action::Write,
            &PolicyTarget::Type(type_name.clone()),
        )
        .await
    {
        Ok(Decision::Allow) => {}
        Ok(Decision::Deny) => return StatusCode::FORBIDDEN.into_response(),
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }

    // 2. Resolve the model. A granted-but-nonexistent type is still 403 (no leak),
    //    distinct from a 404.
    let otype = match st.cp.ontology().get_type(&type_name).await {
        Ok(t) => t,
        Err(ControlPlaneError::NotFound(_)) => return StatusCode::FORBIDDEN.into_response(),
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    };

    // 3. Derive the conformance shape from the type (the gate seam).
    let shape = model_shape_from_type(&otype);

    // 4. Decode the Arrow IPC body.
    let (schema, batches) = match decode_ipc(&body) {
        Ok(sb) => sb,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid arrow ipc stream").into_response(),
    };

    // 5. Gate + resolve the physical schema (422 + violations on mismatch).
    let columns = match resolve_columns(&schema, Some(&shape)) {
        Ok(c) => c,
        Err(IngestError::DoesNotConform(violations)) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(violations_json(&violations)),
            )
                .into_response();
        }
        Err(IngestError::Infer(_)) => {
            return (StatusCode::BAD_REQUEST, "unsupported column type").into_response();
        }
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    };

    // 6. Land into the type's table with type-named lineage (rows trace to the model).
    let table = otype.table.clone();
    let type_label = type_name.0.clone();
    let file_prefix = Uuid::new_v4().to_string();
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(&type_name)],
        payload: serde_json::json!({ "source": "http-model", "type": type_label }),
    };
    let req = LandRequest {
        table: &table,
        schema: schema.clone(),
        columns: &columns,
        batches: &batches,
        ipc_body: body.as_ref(),
        file_prefix: &file_prefix,
        lineage,
    };

    match st.materializer.land(req).await {
        Ok(snap) => Json(serde_json::json!({
            "snapshot_id": snap.0,
            "type": type_label,
        }))
        .into_response(),
        Err(IngestError::DoesNotConform(violations)) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(violations_json(&violations)),
        )
            .into_response(),
        Err(IngestError::Infer(_)) => {
            (StatusCode::BAD_REQUEST, "unsupported column type").into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}

#[utoipa::path(
    post, path = "/datasets/{schema}/{table}",
    params(
        ("schema" = String, Path, description = "Iceberg schema"),
        ("table" = String, Path, description = "Dataset/table name"),
    ),
    request_body(
        content = Vec<u8>,
        description = "Arrow IPC stream (schema + record batches)",
        content_type = "application/vnd.apache.arrow.stream",
    ),
    responses(
        (status = 200, description = "Landed; snapshot committed", body = LandAck),
        (status = 400, description = "Invalid Arrow IPC / unsupported column type / bad header"),
        (status = 422, description = "Data does not conform to model gate", body = ViolationsBody),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "datasets",
)]
pub(crate) async fn land(
    State(st): State<AppState>,
    Path((schema_name, table_name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Optional model gate from X-Loom-Model (JSON).
    let gate: Option<ModelShape> = match headers.get("X-Loom-Model") {
        None => None,
        Some(v) => match v
            .to_str()
            .ok()
            .and_then(|s| serde_json::from_str::<LandModel>(s).ok())
        {
            Some(m) => Some(m.into()),
            None => return (StatusCode::BAD_REQUEST, "invalid X-Loom-Model").into_response(),
        },
    };

    // Optional run id from X-Loom-Run-Id.
    let run_id = match headers.get("X-Loom-Run-Id") {
        None => RunId(Uuid::new_v4()),
        Some(v) => match v.to_str().ok().and_then(|s| Uuid::parse_str(s).ok()) {
            Some(u) => RunId(u),
            None => return (StatusCode::BAD_REQUEST, "invalid X-Loom-Run-Id").into_response(),
        },
    };

    let (schema, batches) = match decode_ipc(&body) {
        Ok(sb) => sb,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid arrow ipc stream").into_response(),
    };

    let table = TableRef {
        schema: schema_name,
        name: table_name,
    };
    let file_prefix = Uuid::new_v4().to_string();
    let lineage = LineageEvent {
        run_id,
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(&table).dataset_ref()],
        payload: serde_json::json!({ "source": "http-land" }),
    };

    // Gate + schema resolution: backend-agnostic, run once before dispatch.
    let columns = match resolve_columns(&schema, gate.as_ref()) {
        Ok(c) => c,
        Err(IngestError::DoesNotConform(violations)) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(violations_json(&violations)),
            )
                .into_response();
        }
        Err(IngestError::Infer(_)) => {
            return (StatusCode::BAD_REQUEST, "unsupported column type").into_response();
        }
        Err(_) => return (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    };

    let req = LandRequest {
        table: &table,
        schema: schema.clone(),
        columns: &columns,
        batches: &batches,
        ipc_body: body.as_ref(),
        file_prefix: &file_prefix,
        lineage,
    };

    match st.materializer.land(req).await {
        Ok(snap) => Json(serde_json::json!({
            "snapshot_id": snap.0,
            "dataset": format!("{}.{}", table.schema, table.name),
        }))
        .into_response(),
        Err(IngestError::DoesNotConform(violations)) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(violations_json(&violations)),
        )
            .into_response(),
        Err(IngestError::Infer(_)) => {
            (StatusCode::BAD_REQUEST, "unsupported column type").into_response()
        }
        // Opaque for backend faults: a governance-fronted service must not echo
        // internal detail (SQL, paths) to the client.
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}
