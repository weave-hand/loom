//! The orchestrator: gate -> schema-selection -> datafusion write -> one atomic Tx
//! (create_table + append_files + emit + commit). Ordering is write-then-commit;
//! a commit failure after the write orphans Parquet files (documented; GC deferred).

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use control_plane_core::{ColumnSpec, ControlPlane, DataFile, LineageEvent, SnapshotId, TableRef};
use object_store::ObjectStore;

use crate::IngestError;
use crate::gate::{ModelShape, validate};
use datafusion_io::{WriteConfig, infer_columns, write_dataset};

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

/// Validate the optional model gate and resolve the physical schema. Backend-
/// agnostic: the HTTP handler runs this once, before dispatching to whichever
/// landing backend is configured. The model wins when supplied; otherwise infer.
pub fn resolve_columns(
    schema: &Schema,
    gate: Option<&ModelShape>,
) -> Result<Vec<ColumnSpec>, IngestError> {
    if let Some(shape) = gate {
        validate(shape, schema).map_err(IngestError::DoesNotConform)?;
    }
    Ok(match gate {
        Some(shape) => shape
            .columns
            .iter()
            .map(|c| ColumnSpec {
                name: c.name.clone(),
                ty: c.ty.clone(),
                nullable: !c.required,
            })
            .collect(),
        None => infer_columns(schema)?,
    })
}

/// The write tail: DataFusion Parquet write + one atomic control-plane
/// transaction (create_table + append_files + emit + commit). `columns` is the
/// already-resolved physical schema (see [`resolve_columns`]).
#[allow(
    clippy::too_many_arguments,
    reason = "land requires cp, store, table, schema, columns, batches, file_prefix, and lineage — all structurally distinct args"
)]
pub async fn land(
    cp: &dyn ControlPlane,
    object_store: Arc<dyn ObjectStore>,
    table: &TableRef,
    schema: Arc<Schema>,
    columns: &[ColumnSpec],
    batches: &[RecordBatch],
    file_prefix: &str,
    lineage: LineageEvent,
) -> Result<SnapshotId, IngestError> {
    // DataFusion write: N Snappy Parquet files straight to object storage.
    let dir_prefix = format!("{}/{}/{}", table.schema, table.name, file_prefix);
    let files = write_dataset(
        object_store,
        &dir_prefix,
        schema,
        batches,
        &WriteConfig::default(),
    )
    .await?;

    // One atomic transaction: create_table (idempotent) + append_files + emit.
    let data_files: Vec<DataFile> = files
        .into_iter()
        .map(|f| DataFile {
            path: f.path,
            path_is_relative: true,
            file_format: control_plane_core::FileFormat::Parquet,
            record_count: f.record_count,
            file_size_bytes: f.file_size_bytes,
            column_stats: f.column_stats,
            parquet_footer_size: Some(f.footer_size),
        })
        .collect();

    let mut tx = cp.begin().await?;
    tx.create_table(table, columns).await?;
    tx.append_files(table, &data_files).await?;
    tx.emit(lineage).await?;
    tx.commit().await?.ok_or(IngestError::NoSnapshot)
}

/// Land data as a snapshot + lineage commit, atomically. Thin convenience over
/// [`resolve_columns`] + [`land`] for callers that hold a whole
/// [`MaterializeRequest`] (the integration tests).
pub async fn materialize(
    cp: &dyn ControlPlane,
    object_store: Arc<dyn ObjectStore>,
    req: MaterializeRequest<'_>,
) -> Result<SnapshotId, IngestError> {
    let columns = resolve_columns(&req.schema, req.gate)?;
    land(
        cp,
        object_store,
        req.table,
        req.schema.clone(),
        &columns,
        req.batches,
        req.file_prefix,
        req.lineage,
    )
    .await
}
