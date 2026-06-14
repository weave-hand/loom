//! The orchestrator: gate -> schema-selection -> datafusion write -> one atomic Tx
//! (create_table + append_files + emit + commit). Ordering is write-then-commit;
//! a commit failure after the write orphans the Parquet files (documented; GC deferred).

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use control_plane_core::{ColumnSpec, ControlPlane, DataFile, LineageEvent, SnapshotId, TableRef};
use object_store::ObjectStore;

use crate::IngestError;
use crate::gate::{ModelShape, validate};
use crate::infer::infer_columns;
use crate::write::{IngestWriteConfig, write_dataset};

/// One materialize call: land `batches` for `table`, optionally gated by a model.
pub struct MaterializeRequest<'a> {
    pub table: &'a TableRef,
    pub schema: Arc<Schema>,
    pub batches: &'a [RecordBatch],
    /// Caller-unique prefix (subdirectory) for this call's files, e.g. a run id.
    /// Files land at "<schema>/<table>/<file_prefix>/part-*.parquet".
    pub file_prefix: &'a str,
    pub gate: Option<&'a ModelShape>,
    /// The lineage event to emit in the same transaction (output dataset = the
    /// landed table). Built by the caller, which knows the datasource namespace.
    pub lineage: LineageEvent,
}

/// Land data as a registered DuckLake snapshot + lineage, atomically.
pub async fn materialize(
    cp: &dyn ControlPlane,
    object_store: Arc<dyn ObjectStore>,
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

    // 3. DataFusion write: N Snappy Parquet files straight to object storage.
    let dir_prefix = format!(
        "{}/{}/{}",
        req.table.schema, req.table.name, req.file_prefix
    );
    let files = write_dataset(
        object_store,
        &dir_prefix,
        req.schema.clone(),
        req.batches,
        &IngestWriteConfig::default(),
    )
    .await?;

    // 4. One atomic transaction: create_table (idempotent) + append_files + emit.
    let data_files: Vec<DataFile> = files
        .into_iter()
        .map(|f| DataFile {
            path: f.path,
            path_is_relative: true,
            record_count: f.record_count,
            file_size_bytes: f.file_size_bytes,
            footer_size: f.footer_size,
            column_stats: f.column_stats,
        })
        .collect();

    let mut tx = cp.begin().await?;
    tx.create_table(req.table, &columns).await?;
    tx.append_files(req.table, &data_files).await?;
    tx.emit(req.lineage).await?;
    tx.commit().await?.ok_or(IngestError::NoSnapshot)
}
