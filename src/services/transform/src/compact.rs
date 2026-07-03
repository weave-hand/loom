//! Selective (size-threshold) compaction: coalesce a table's small Parquet files into
//! fewer size-targeted ones, leaving already-large files in place. Reads ONLY the small
//! files with DataFusion, rewrites them via `write_dataset`, and commits the swap through
//! the `compact_files` partial-supersede primitive. Physical reorganization only — the row
//! set and full time-travel history are preserved; no lineage edge is emitted.

use std::sync::Arc;

use control_plane_core::{ControlPlane, DataFile, FileRef, SnapshotId, TableRef, small_files};
use datafusion::execution::context::SessionContext;
use datafusion_io::{WriteConfig, absolute_data_files, scan_table, write_dataset};
use object_store::ObjectStore;

/// Tunables for a compaction run.
pub struct CompactConfig {
    /// Live files strictly smaller than this (in bytes) are compaction candidates.
    pub small_file_threshold_bytes: i64,
    /// Output sizing for the coalesced files.
    pub write: WriteConfig,
}

#[derive(Debug, thiserror::Error)]
pub enum CompactError {
    #[error("sql/datafusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    Scan(#[from] datafusion_io::ScanError),
    #[error(transparent)]
    Write(#[from] datafusion_io::WriteError),
    #[error(transparent)]
    ControlPlane(#[from] control_plane_core::ControlPlaneError),
    #[error("compact commit produced no snapshot id")]
    NoSnapshot,
}

/// Compact `table`'s small files. Returns the new snapshot id, or `Ok(None)` when there
/// are fewer than two small files (nothing worth coalescing — a no-op that also makes
/// re-running compaction converge). `run_id` is a caller-unique output-file prefix.
pub async fn compact_table(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    root_url: &str,
    run_id: &str,
    table: &TableRef,
    cfg: &CompactConfig,
) -> Result<Option<SnapshotId>, CompactError> {
    // 1. Current snapshot's live files.
    let snapshot = cp.catalog().current_snapshot(table).await?;
    let files = cp
        .catalog()
        .files(table, snapshot.id, control_plane_core::PageReq::unbounded())
        .await?;

    // 2. Select the sub-threshold files; bail out unless at least two can be coalesced.
    let small = small_files(&files.items, cfg.small_file_threshold_bytes);
    if small.len() < 2 {
        return Ok(None);
    }
    let small_refs: Vec<FileRef> = small.iter().map(|f| (*f).clone()).collect();
    let expire_paths: Vec<String> = small.iter().map(|f| f.path.clone()).collect();

    // 3. Read ONLY the small files. SELECT * + collect fully materializes their rows
    //    BEFORE the transaction opens, so reading and then expiring the same files in
    //    one commit is safe (no read-after-expire).
    let ctx = SessionContext::new();
    scan_table(&ctx, store.clone(), &table.name, table, &small_refs).await?;
    let df = ctx
        .sql(&format!("SELECT * FROM \"{}\"", table.name))
        .await?;
    let schema: Arc<arrow::datatypes::Schema> = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await?;

    // 4. Write the coalesced, size-targeted files.
    let dir_prefix = format!("{}/{}/{}", table.schema, table.name, run_id);
    let written = write_dataset(store, &dir_prefix, schema, &batches, &cfg.write).await?;
    // Store ABSOLUTE mirror paths so the serving engine (which resolves
    // `iceberg_mirror.data_file.path` as an absolute URL) can read the coalesced files.
    let new_files: Vec<DataFile> =
        absolute_data_files(written, root_url, &table.schema, &table.name);

    // 5. One Tx: swap the small files for the coalesced ones. No lineage (physical reorg).
    let mut tx = cp.begin().await?;
    tx.compact_files(table, &expire_paths, &new_files).await?;
    let snap = tx.commit().await?.ok_or(CompactError::NoSnapshot)?;
    Ok(Some(snap))
}
