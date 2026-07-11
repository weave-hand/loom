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

/// Stateless configuration for the event-driven compaction auto-trigger
/// (`maybe_enqueue_compact`), carried on [`crate::iceberg_sql_catalog::SqlCatalog`]
/// as `Option<CompactTriggerCfg>` — `None` disables the trigger entirely,
/// preserving every existing commit path byte-identically.
#[derive(Debug, Clone, Copy)]
pub struct CompactTriggerCfg {
    /// Files strictly smaller than this count toward the trigger — the SAME
    /// cutoff the worker's `small_files` selection uses
    /// (`LOOM_COMPACT_THRESHOLD_BYTES`).
    pub small_file_bytes: i64,
    /// Live small-file count at/above which a `compact_table` job is enqueued
    /// (`LOOM_COMPACT_TRIGGER_FILES`). Should be validated `>= 2` at config parse.
    pub min_small_files: i64,
}

/// Evaluate the compaction auto-trigger for `table` inside the caller's commit
/// transaction, after the new files it just committed are already projected into
/// `iceberg_mirror.data_file`. Resolves the live `table_id` (a never-written
/// table is `Ok(None)`), skips declared stream tables, changelog tables, and
/// shadow-flagged tables (their own consolidation/flush triggers own that data —
/// see the design spec's guard rationale), then counts live files strictly
/// under `cfg.small_file_bytes`. At/above `cfg.min_small_files`, enqueues a
/// deduped `compact_table` job (`pg_insert_if_absent` — the same payload shape
/// the operator endpoint builds, so the two producers dedup against each
/// other's pending job). Pure Postgres: no object-store I/O, so this is safe to
/// call from inside a commit tx (`iss-iceberg-tx-objectstore`).
pub async fn maybe_enqueue_compact(
    conn: &mut sqlx::PgConnection,
    table: &TableRef,
    cfg: &CompactTriggerCfg,
) -> Result<Option<control_plane_core::JobId>> {
    let Some(tid) = crate::iceberg_mirror::live_table_id(conn, &table.schema, &table.name).await?
    else {
        return Ok(None);
    };
    // One round trip: eligibility (stream / changelog / shadow) + live small count.
    let row = sqlx::query!(
        "select \
           exists(select 1 from stream.stream_table s \
                  where s.table_id = $1 or s.changelog_table_id = $1) as \"is_stream!\", \
           exists(select 1 from iceberg_mirror.shadow_flag f \
                  where f.table_id = $1) as \"has_shadow!\", \
           (select count(*) from iceberg_mirror.data_file d \
             where d.table_id = $1 and d.end_snapshot is null \
               and d.file_size_bytes < $2) as \"small_count!\"",
        tid,
        cfg.small_file_bytes,
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(backend)?;
    if row.is_stream || row.has_shadow || row.small_count < cfg.min_small_files {
        return Ok(None);
    }
    let payload = serde_json::to_value(control_plane_core::CompactJob {
        schema: table.schema.clone(),
        name: table.name.clone(),
    })
    .map_err(|e| ControlPlaneError::Backend(format!("compact trigger payload: {e}").into()))?;
    crate::queue::pg_insert_if_absent(
        &mut *conn,
        &control_plane_core::NewJob {
            kind: control_plane_core::COMPACT_JOB_KIND.to_string(),
            payload,
            run_at: None,
            priority: 0,
        },
    )
    .await
}

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
