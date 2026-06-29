//! query-api's `ActionEngine` as a thin wire client over the engine's
//! `EngineControl` UDS channel. ACL is enforced by the handler BEFORE this runs;
//! this sends a pre-authorized write. The typed Arrow batch is built and IPC-encoded
//! here (client-side); only the encoded bytes + JSON metadata cross the wire.

use arrow::array::RecordBatch;
use async_trait::async_trait;
use engine_wire::client::GrpcQueueClient;
use engine_wire::convert::LineageWire;

use crate::serving::{
    ActionEngine, ServingError, SqlValue, build_object_batch, build_object_batches,
};

fn to_serving<E: std::fmt::Display>(e: E) -> ServingError {
    ServingError::Engine(e.to_string())
}

/// Encode a single record batch as an Arrow IPC stream body. (Moved verbatim from
/// the old `serving_datafusion.rs`; stays client-side.)
pub fn encode_ipc_stream(batch: &RecordBatch) -> Result<Vec<u8>, ServingError> {
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(to_serving)?;
        w.write(batch).map_err(to_serving)?;
        w.finish().map_err(to_serving)?;
    }
    Ok(buf)
}

/// Wire-backed governed-write engine.
pub struct EngineActionClient {
    ctl: GrpcQueueClient,
}

impl EngineActionClient {
    /// Connect to the engine's `EngineControl` service at `socket` (a UDS path).
    pub async fn connect(socket: impl Into<String>) -> Result<Self, ServingError> {
        let ctl = GrpcQueueClient::connect(socket).await.map_err(to_serving)?;
        Ok(Self { ctl })
    }
}

#[async_trait]
impl ActionEngine for EngineActionClient {
    async fn write_object(
        &self,
        table: &control_plane_core::TableRef,
        columns: &[String],
        values: &[SqlValue],
        logical_types: &[String],
        event: control_plane_core::LineageEvent,
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        let (_schema, batch, specs) = build_object_batch(columns, values, logical_types)?;
        let ipc = encode_ipc_stream(&batch)?;
        let columns_json = serde_json::to_string(&specs).map_err(to_serving)?;
        let lineage_json = serde_json::to_string(&LineageWire::from(&event)).map_err(to_serving)?;
        let id = self
            .ctl
            .write_object(
                table.schema.clone(),
                table.name.clone(),
                ipc,
                columns_json,
                lineage_json,
            )
            .await
            .map_err(to_serving)?;
        Ok(control_plane_core::SnapshotId(id))
    }

    async fn overwrite_table(
        &self,
        table: &control_plane_core::TableRef,
        columns: &[String],
        rows: &[Vec<SqlValue>],
        logical_types: &[String],
        event: control_plane_core::LineageEvent,
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        let lineage_json = serde_json::to_string(&LineageWire::from(&event)).map_err(to_serving)?;
        let (ipc, columns_json) = if rows.is_empty() {
            // Delete-all: empty payload drives the truncate branch engine-side.
            let no_cols: &[control_plane_core::ColumnSpec] = &[];
            (
                Vec::new(),
                serde_json::to_string(no_cols).map_err(to_serving)?,
            )
        } else {
            let (_schema, batch, specs) = build_object_batches(columns, rows, logical_types)?;
            (
                encode_ipc_stream(&batch)?,
                serde_json::to_string(&specs).map_err(to_serving)?,
            )
        };
        let id = self
            .ctl
            .overwrite_table(
                table.schema.clone(),
                table.name.clone(),
                ipc,
                columns_json,
                lineage_json,
            )
            .await
            .map_err(to_serving)?;
        Ok(control_plane_core::SnapshotId(id))
    }
}
