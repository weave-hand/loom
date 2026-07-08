//! query-api's `ActionEngine` as a thin wire client over the engine's
//! `EngineControl` UDS channel. ACL is enforced by the handler BEFORE this runs;
//! this sends a pre-authorized write. The typed Arrow batch is built and IPC-encoded
//! here (client-side); only the encoded bytes + JSON metadata cross the wire.

use arrow::array::RecordBatch;
use async_trait::async_trait;
use engine_wire::client::GrpcQueueClient;
use engine_wire::convert::LineageWire;

use crate::serving::{
    ActionEngine, BeforeImage, ServingError, SqlValue, StepWrite, WriteMode, build_object_batch,
    build_object_batches,
};

fn to_serving<E: std::fmt::Display>(e: E) -> ServingError {
    ServingError::Engine(e.to_string())
}

/// Map a `write_delta` failure, preserving `ControlPlaneError::Conflict` (a CAS
/// loss on the inline-delta path, surfaced by `GrpcQueueClient::write_delta` via
/// `write_status`) as `ServingError::Conflict` rather than collapsing it into an
/// opaque `Engine` string, so the caller can distinguish a retryable conflict.
fn to_serving_write(e: control_plane_core::ControlPlaneError) -> ServingError {
    match e {
        control_plane_core::ControlPlaneError::Conflict(m) => ServingError::Conflict(m),
        other => ServingError::Engine(other.to_string()),
    }
}

/// Serialize the action's resolved downstream `NewJob`s into the wire `jobs_json`
/// string. Empty ⇒ empty string (the wire's "no jobs" sentinel), so the engine
/// side only deserializes when there is actually something to enqueue.
fn serialize_jobs(jobs: &[control_plane_core::NewJob]) -> Result<String, ServingError> {
    if jobs.is_empty() {
        Ok(String::new())
    } else {
        serde_json::to_string(jobs).map_err(|e| ServingError::Engine(format!("encode jobs: {e}")))
    }
}

/// Build the `ColumnSpec` list for a zero-row `Overwrite` (truncate) step, where
/// `build_object_batches` cannot run (it rejects empty rows) yet the engine still needs the
/// schema to re-project the emptied table. Every field is nullable — the same spec shape
/// `build_object_batches` produces from its `(columns, logical_types)`.
fn empty_specs(
    columns: &[String],
    logical_types: &[String],
) -> Result<Vec<control_plane_core::ColumnSpec>, ServingError> {
    columns
        .iter()
        .zip(logical_types)
        .map(|(name, logical)| {
            let base = control_plane_core::resolve_logical(logical)
                .ok_or_else(|| ServingError::Engine(format!("unknown logical type `{logical}`")))?;
            Ok(control_plane_core::ColumnSpec {
                name: name.clone(),
                ty: base.canonical_name(),
                nullable: true,
            })
        })
        .collect()
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
        jobs: &[control_plane_core::NewJob],
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        let jobs_json = serialize_jobs(jobs)?;
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
                &jobs_json,
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
        jobs: &[control_plane_core::NewJob],
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        let jobs_json = serialize_jobs(jobs)?;
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
                &jobs_json,
            )
            .await
            .map_err(to_serving)?;
        Ok(control_plane_core::SnapshotId(id))
    }

    async fn current_inline_version(
        &self,
        table: &control_plane_core::TableRef,
        id_column: &str,
        id_value: &SqlValue,
        id_logical: &str,
    ) -> Result<i64, ServingError> {
        let id_cols = vec![id_column.to_string()];
        let id_values = vec![id_value.clone()];
        let id_logicals = vec![id_logical.to_string()];
        let (_schema, batch, specs) = build_object_batch(&id_cols, &id_values, &id_logicals)?;
        let id_ipc = encode_ipc_stream(&batch)?;
        let columns_json = serde_json::to_string(&specs).map_err(to_serving)?;
        self.ctl
            .current_inline_version(
                table.schema.clone(),
                table.name.clone(),
                id_column.to_string(),
                id_ipc,
                columns_json,
            )
            .await
            .map_err(to_serving)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "mirrors the wire write_delta RPC shape one-for-one; a params struct would only obscure the call site"
    )]
    async fn write_delta(
        &self,
        table: &control_plane_core::TableRef,
        id_column: &str,
        tombstone: bool,
        columns: &[String],
        values: &[SqlValue],
        logical_types: &[String],
        before: Option<BeforeImage<'_>>,
        event: control_plane_core::LineageEvent,
        expected_version: i64,
        jobs: &[control_plane_core::NewJob],
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        let jobs_json = serialize_jobs(jobs)?;
        let lineage_json = serde_json::to_string(&LineageWire::from(&event)).map_err(to_serving)?;
        let (ipc, columns_json) = if tombstone {
            // A tombstone carries only the identity — locate it among the
            // caller's (columns, values, logical_types) by name and build a
            // one-cell id batch, same shape as `current_inline_version`.
            let idx = columns.iter().position(|c| c == id_column).ok_or_else(|| {
                ServingError::Engine(format!(
                    "write_delta: id column `{id_column}` not found in columns"
                ))
            })?;
            let id_value = values.get(idx).ok_or_else(|| {
                ServingError::Engine("write_delta: values shorter than columns".into())
            })?;
            let id_logical = logical_types.get(idx).ok_or_else(|| {
                ServingError::Engine("write_delta: logical_types shorter than columns".into())
            })?;
            let id_cols = vec![id_column.to_string()];
            let id_values = vec![id_value.clone()];
            let id_logicals = vec![id_logical.clone()];
            let (_schema, batch, specs) = build_object_batch(&id_cols, &id_values, &id_logicals)?;
            (
                encode_ipc_stream(&batch)?,
                serde_json::to_string(&specs).map_err(to_serving)?,
            )
        } else {
            let (_schema, batch, specs) = build_object_batch(columns, values, logical_types)?;
            (
                encode_ipc_stream(&batch)?,
                serde_json::to_string(&specs).map_err(to_serving)?,
            )
        };
        // The optional before-image (prior row) rides alongside the new row. It
        // carries its OWN ColumnSpecs (positionally aligned with its batch) so a
        // CDC consumer can build the −U/−D row directly. Absent ⇒ empty IPC.
        let (before_ipc, before_columns_json) = match before {
            Some(b) => {
                let (_schema, batch, specs) =
                    build_object_batch(b.columns, b.values, b.logical_types)?;
                (
                    encode_ipc_stream(&batch)?,
                    serde_json::to_string(&specs).map_err(to_serving)?,
                )
            }
            None => (Vec::new(), String::new()),
        };
        let id = self
            .ctl
            .write_delta(
                table.schema.clone(),
                table.name.clone(),
                id_column.to_string(),
                tombstone,
                ipc,
                columns_json,
                lineage_json,
                expected_version,
                before_ipc,
                before_columns_json,
                &jobs_json,
            )
            .await
            .map_err(to_serving_write)?;
        Ok(control_plane_core::SnapshotId(id))
    }

    async fn write_steps(
        &self,
        writes: &[StepWrite],
        event: control_plane_core::LineageEvent,
        jobs: &[control_plane_core::NewJob],
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        let jobs_json = serialize_jobs(jobs)?;
        // Build one wire `StepWrite` per target: its rows -> multi-row Arrow batch ->
        // IPC stream, plus the `ColumnSpec` JSON. The single lineage event (carrying
        // every target in its outputs) crosses once.
        let lineage_json = serde_json::to_string(&LineageWire::from(&event)).map_err(to_serving)?;
        let mut steps = Vec::with_capacity(writes.len());
        for w in writes {
            let overwrite = matches!(w.mode, WriteMode::Overwrite);
            // An empty-rows Overwrite (a multi-step Delete/Update that emptied the table) is a
            // truncate for that target: send an EMPTY IPC body (`build_object_batches` rejects
            // zero rows, so it must NOT be called) but the REAL column specs, so the engine
            // re-projects the table's schema at the shared snapshot and it reads as empty (not
            // as a missing table). Mirrors `overwrite_table`'s empty-body truncate, but keeps
            // the schema since the multi-step commit registers a whole snapshot.
            let (ipc, columns_json) = if overwrite && w.rows.is_empty() {
                (
                    Vec::new(),
                    serde_json::to_string(&empty_specs(&w.columns, &w.logical_types)?)
                        .map_err(to_serving)?,
                )
            } else {
                let (_schema, batch, specs) =
                    build_object_batches(&w.columns, &w.rows, &w.logical_types)?;
                (
                    encode_ipc_stream(&batch)?,
                    serde_json::to_string(&specs).map_err(to_serving)?,
                )
            };
            steps.push(engine_wire::pb::StepWrite {
                schema: w.table.schema.clone(),
                name: w.table.name.clone(),
                ipc,
                columns_json,
                overwrite,
            });
        }
        let id = self
            .ctl
            .write_steps(steps, lineage_json, &jobs_json)
            .await
            .map_err(to_serving_write)?;
        Ok(control_plane_core::SnapshotId(id))
    }
}
