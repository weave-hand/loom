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
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::post;
use control_plane_core::{ControlPlane, DatasetId, EventType, LineageEvent, RunId, TableRef};
use object_store::ObjectStore;
use time::OffsetDateTime;
use uuid::Uuid;

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

async fn land(
    State(st): State<AppState>,
    Path((schema_name, table_name)): Path<(String, String)>,
    body: Bytes,
) -> Response {
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
        run_id: RunId(Uuid::new_v4()),
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
        gate: None,
        lineage,
    };

    match materialize(st.cp.as_ref(), st.store.as_ref(), req).await {
        Ok(snap) => Json(serde_json::json!({
            "snapshot_id": snap.0,
            "dataset": format!("{}.{}", table.schema, table.name),
        }))
        .into_response(),
        // Refined into per-variant mapping in the next task.
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}
