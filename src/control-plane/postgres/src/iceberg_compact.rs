//! Iceberg subset-expire compaction commit — the engine-callable entry the
//! `EngineControl::CompactTable` RPC and `IcebergTx::compact_files` share. Mirrors
//! `iceberg_flush::flush_table`'s shape (read current snapshot, one Postgres tx,
//! mirror-only), but expires a *subset* of live files instead of end-capping all of
//! them, and registers the caller's already-written coalesced Parquet. No lineage
//! (physical reorganization). Time travel preserved: expired rows keep
//! `begin_snapshot < at`.

use control_plane_core::{Catalog, ControlPlaneError, DataFile, Result, SnapshotId, TableRef};
use sqlx::PgPool;

use crate::backend;
use crate::iceberg_catalog::IcebergCatalog;
use crate::iceberg_landing::{WriteMode, register_files};
use crate::iceberg_mirror::next_snapshot;

/// Compact `table`: at one new snapshot, expire the live files named by `expire`
/// (their absolute mirror paths) and register `write` (already written to object
/// store, absolute paths). Returns the new snapshot id, or `Ok(None)` if the table
/// was never written (no current snapshot). `Conflict` if any `expire` path is no
/// longer live (raced compaction).
pub async fn compact_table(
    pool: &PgPool,
    table: &TableRef,
    expire: &[String],
    write: &[DataFile],
) -> Result<Option<SnapshotId>> {
    let ice = IcebergCatalog::new(pool.clone());
    match ice.current_snapshot(table).await {
        Ok(_) => {}
        Err(ControlPlaneError::NotFound(_)) => return Ok(None),
        Err(e) => return Err(e),
    };

    let mut tx = pool.begin().await.map_err(backend)?;
    let at = next_snapshot(&mut tx, None).await?;
    // columns unused for Compact (schema-invariant) — pass &[].
    register_files(
        &mut tx,
        table,
        &[],
        write,
        WriteMode::Compact {
            expire_paths: expire.to_vec(),
        },
        at,
    )
    .await?;
    tx.commit().await.map_err(backend)?;
    Ok(Some(at))
}
