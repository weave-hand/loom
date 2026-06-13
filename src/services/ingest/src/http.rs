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
use control_plane_core::{ControlPlane, DatasetId, EventType, LineageEvent, RunId, TableRef};
use object_store::ObjectStore;
use serde::Deserialize;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::IngestError;
use crate::gate::{ColumnShape, ModelShape, Violation, ViolationReason};
use crate::materialize::{MaterializeRequest, materialize};

/// Shared, owned dependencies: the control-plane facade + an object store.
#[derive(Clone)]
pub struct AppState {
    pub cp: Arc<dyn ControlPlane>,
    pub store: Arc<dyn ObjectStore>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/datasets/:schema/:table", post(land))
        .with_state(state)
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

async fn land(
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
    let file_name = format!("part-{}.parquet", Uuid::new_v4());
    let lineage = LineageEvent {
        run_id,
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(&table).dataset_ref()],
        payload: serde_json::json!({ "source": "http-land" }),
    };

    let req = MaterializeRequest {
        table: &table,
        schema,
        batches: &batches,
        file_name: &file_name,
        gate: gate.as_ref(),
        lineage,
    };

    match materialize(st.cp.as_ref(), st.store.as_ref(), req).await {
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
