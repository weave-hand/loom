//! The transform primitive: resolve input DuckLake table(s), have DataFusion run a SQL
//! query over them, and commit the result as a new snapshot of the output table plus
//! lineage (inputs -> output), atomically. Append semantics. Shared by the physical
//! path (inputs registered under their table name) and the typed path (registered under
//! the ontology type name, with a conformance contract).

use std::sync::Arc;

use control_plane_core::{
    ColumnSpec, ControlPlane, DataFile, LineageEvent, PropertyDef, SnapshotId, TableRef,
};
use datafusion::execution::context::SessionContext;
use datafusion_io::{WriteConfig, infer_columns, scan_table, write_dataset};
use object_store::ObjectStore;

use crate::conform::{Violation, check_conformance};

/// One input to a transform: a physical table plus the name it is registered under in
/// DataFusion (what the SQL references). The physical path registers tables under their
/// own name; the typed path registers them under the ontology type name.
pub struct TransformInput<'a> {
    pub table: &'a TableRef,
    pub register_as: &'a str,
}

/// One transform: read `inputs`, run `sql`, write the result to `output`.
pub struct TransformRequest<'a> {
    pub inputs: &'a [TransformInput<'a>],
    pub output: &'a TableRef,
    pub sql: &'a str,
    /// When `Some`, the result schema must EXACTLY conform to these properties (typed
    /// transforms); checked before any write. `None` skips the check (physical path).
    pub conform: Option<&'a [PropertyDef]>,
    /// Built by the caller; inputs -> output. Emitted in the commit transaction.
    pub lineage: LineageEvent,
}

#[derive(Debug, thiserror::Error)]
pub enum TransformError {
    #[error("unknown input table {0}.{1}")]
    UnknownInput(String, String),
    #[error("ambiguous input table name {0}: two inputs would register under it")]
    AmbiguousInput(String),
    #[error("output does not conform to the declared type ({} violation(s)): {0:?}", .0.len())]
    DoesNotConform(Vec<Violation>),
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

    // Inputs register under `register_as`; DataFusion silently overwrites a same-named
    // table, so two inputs sharing a registration name would shadow and the SQL would
    // compute against the wrong one. Reject that up front.
    let mut seen = std::collections::HashSet::new();
    for input in req.inputs {
        if !seen.insert(input.register_as) {
            return Err(TransformError::AmbiguousInput(
                input.register_as.to_string(),
            ));
        }
    }

    // 1. Resolve + register each input under its `register_as` name.
    for input in req.inputs {
        let snapshot = cp
            .catalog()
            .current_snapshot(input.table)
            .await
            .map_err(|e| match e {
                control_plane_core::ControlPlaneError::NotFound(_) => TransformError::UnknownInput(
                    input.table.schema.clone(),
                    input.table.name.clone(),
                ),
                other => TransformError::ControlPlane(other),
            })?;
        let files = cp
            .catalog()
            .files(
                input.table,
                snapshot.id,
                control_plane_core::PageReq::unbounded(),
            )
            .await?;
        scan_table(
            &ctx,
            store.clone(),
            input.register_as,
            input.table,
            &files.items,
        )
        .await?;
    }

    // 2. Run the SQL; resolve its result schema (no rows pulled yet).
    let df = ctx.sql(req.sql).await?;
    let schema: Arc<arrow::datatypes::Schema> = Arc::new(df.schema().as_arrow().clone());

    // 3. Output physical columns inferred from the result schema.
    let columns: Vec<ColumnSpec> = infer_columns(&schema)?;

    // 3a. Typed transforms: the result must EXACTLY conform to the declared type. Checked
    //     on the schema BEFORE collecting rows, so a non-conforming transform pulls no
    //     data and writes/commits nothing.
    if let Some(properties) = req.conform {
        check_conformance(&columns, properties).map_err(TransformError::DoesNotConform)?;
    }

    // 4. Collect the result rows now that the output is known to conform.
    let batches = df.collect().await?;

    // 5. Write the result as N Snappy Parquet files under the output table dir.
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
            file_format: control_plane_core::FileFormat::Parquet,
            record_count: f.record_count,
            file_size_bytes: f.file_size_bytes,
            column_stats: f.column_stats,
            parquet_footer_size: Some(f.footer_size),
        })
        .collect();

    // 6. One atomic Tx: create_table (idempotent) + append_files + emit lineage.
    let mut tx = cp.begin().await?;
    tx.create_table(req.output, &columns).await?;
    tx.append_files(req.output, &data_files).await?;
    tx.emit(req.lineage).await?;
    tx.commit().await?.ok_or(TransformError::NoSnapshot)
}
