//! The orchestrator: gate -> schema-selection -> write -> put -> one atomic Tx
//! (create_table + append_files + emit + commit). Ordering is put-then-commit;
//! a commit failure after put orphans the Parquet (documented; GC is deferred).

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use control_plane_core::{ColumnSpec, ControlPlane, DataFile, LineageEvent, SnapshotId, TableRef};
use object_store::ObjectStore;

use crate::IngestError;
use crate::gate::{ModelShape, validate};
use crate::infer::infer_columns;
use crate::store::put;
use crate::write::write_parquet;

/// One materialize call: land `batches` for `table`, optionally gated by a model.
pub struct MaterializeRequest<'a> {
    pub table: &'a TableRef,
    pub schema: Arc<Schema>,
    pub batches: &'a [RecordBatch],
    /// Caller-unique file name, e.g. "part-0.parquet".
    pub file_name: &'a str,
    pub gate: Option<&'a ModelShape>,
    /// The lineage event to emit in the same transaction (output dataset = the
    /// landed table). Built by the caller, which knows the datasource namespace.
    pub lineage: LineageEvent,
}

/// Land data as a registered DuckLake snapshot + lineage, atomically.
pub async fn materialize(
    cp: &dyn ControlPlane,
    object_store: &dyn ObjectStore,
    req: MaterializeRequest<'_>,
) -> Result<SnapshotId, IngestError> {
    // 1. Optional model gate — the front edge; reject before any write.
    if let Some(shape) = req.gate {
        validate(shape, &req.schema).map_err(IngestError::DoesNotConform)?;
    }

    // 2. Physical schema: the model wins when supplied; otherwise infer.
    let columns: Vec<ColumnSpec> = match req.gate {
        Some(shape) => shape
            .columns
            .iter()
            .map(|c| ColumnSpec {
                name: c.name.clone(),
                ty: c.ty.clone(),
                nullable: !c.required,
            })
            .collect(),
        None => infer_columns(&req.schema)?,
    };

    // 3. Write Parquet + extract stats.
    let written = write_parquet(req.schema.clone(), req.batches)?;

    // 4. Put to object storage (key = schema/table/file).
    let key = format!("{}/{}/{}", req.table.schema, req.table.name, req.file_name);
    let stored = put(object_store, &key, written.bytes).await?;

    // 5. One atomic transaction: create_table (idempotent) + append_files + emit.
    let mut tx = cp.begin().await?;
    tx.create_table(req.table, &columns).await?;
    tx.append_files(
        req.table,
        &[DataFile {
            path: stored.path,
            path_is_relative: stored.path_is_relative,
            record_count: written.record_count,
            file_size_bytes: written.file_size_bytes,
            footer_size: written.footer_size,
            column_stats: written.column_stats,
        }],
    )
    .await?;
    tx.emit(req.lineage).await?;
    tx.commit().await?.ok_or(IngestError::NoSnapshot)
}
