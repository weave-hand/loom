//! The transform primitive: resolve input DuckLake table(s), have DataFusion run a
//! SQL query over them, and commit the result as a new snapshot of the output table
//! plus lineage (inputs -> output), atomically. Append semantics.

use std::sync::Arc;

use control_plane_core::{ColumnSpec, ControlPlane, DataFile, LineageEvent, SnapshotId, TableRef};
use datafusion::execution::context::SessionContext;
use datafusion_io::{WriteConfig, infer_columns, scan_table, write_dataset};
use object_store::ObjectStore;

/// One transform: read `inputs`, run `sql`, write the result to `output`.
pub struct TransformRequest<'a> {
    pub inputs: &'a [TableRef],
    pub output: &'a TableRef,
    pub sql: &'a str,
    /// Built by the caller; inputs -> output. Emitted in the commit transaction.
    pub lineage: LineageEvent,
}

#[derive(Debug, thiserror::Error)]
pub enum TransformError {
    #[error("unknown input table {0}.{1}")]
    UnknownInput(String, String),
    #[error("ambiguous input table name {0}: two inputs would register under it")]
    AmbiguousInput(String),
    #[error("sql/datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    Scan(#[from] datafusion_io::ScanError),
    #[error(transparent)]
    Write(#[from] datafusion_io::WriteError),
    #[error(transparent)]
    Infer(#[from] datafusion_io::InferError),
    #[error(transparent)]
    ControlPlane(#[from] control_plane_core::ControlPlaneError),
    #[error("commit produced no snapshot id")]
    NoSnapshot,
}

/// Run one transform. `run_id` is a caller-unique output-file prefix (e.g. a UUID).
pub async fn run_transform(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    run_id: &str,
    req: TransformRequest<'_>,
) -> Result<SnapshotId, TransformError> {
    let ctx = SessionContext::new();

    // Inputs register under their unqualified `name`; DataFusion silently overwrites a
    // same-named table, so two inputs sharing a name (even across schemas) would shadow
    // and the SQL would compute against the wrong one. Reject that up front.
    let mut seen = std::collections::HashSet::new();
    for input in req.inputs {
        if !seen.insert(input.name.as_str()) {
            return Err(TransformError::AmbiguousInput(input.name.clone()));
        }
    }

    // 1. Resolve + register each input as a DataFusion table named by its table name.
    for input in req.inputs {
        let snapshot = cp
            .catalog()
            .current_snapshot(input)
            .await
            .map_err(|e| match e {
                control_plane_core::ControlPlaneError::NotFound(_) => {
                    TransformError::UnknownInput(input.schema.clone(), input.name.clone())
                }
                other => TransformError::ControlPlane(other),
            })?;
        let files = cp
            .catalog()
            .files(input, snapshot.id, control_plane_core::PageReq::unbounded())
            .await?;
        scan_table(&ctx, store.clone(), &input.name, input, &files.items).await?;
    }

    // 2. Run the SQL; collect the result + its Arrow schema.
    let df = ctx.sql(req.sql).await?;
    let schema: Arc<arrow::datatypes::Schema> = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await?;

    // 3. Output physical columns inferred from the result schema.
    let columns: Vec<ColumnSpec> = infer_columns(&schema)?;

    // 4. Write the result as N Snappy Parquet files under the output table dir.
    let dir_prefix = format!("{}/{}/{}", req.output.schema, req.output.name, run_id);
    let written = write_dataset(
        store,
        &dir_prefix,
        schema,
        &batches,
        &WriteConfig::default(),
    )
    .await?;
    let data_files: Vec<DataFile> = written
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

    // 5. One atomic Tx: create_table (idempotent) + append_files + emit lineage.
    let mut tx = cp.begin().await?;
    tx.create_table(req.output, &columns).await?;
    tx.append_files(req.output, &data_files).await?;
    tx.emit(req.lineage).await?;
    tx.commit().await?.ok_or(TransformError::NoSnapshot)
}
