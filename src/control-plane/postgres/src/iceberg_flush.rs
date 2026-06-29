//! Inline flush/compaction: drain a table's live inline rows into a real Iceberg
//! Parquet snapshot and end-cap the inline rows, atomically. Library primitive —
//! no trigger policy (see the spec). Serialized per table by a session advisory
//! lock so two flushes can't both write Parquet for the same rows.

use control_plane_core::{
    BUILD_VECTOR_INDEX_JOB_KIND, BuildVectorIndexJob, Catalog, ColumnSpec, ControlPlaneError,
    DatasetId, EventType, LineageEvent, NewJob, Result, RunId, SnapshotId, TableRef,
};
use sqlx::PgPool;
use time::OffsetDateTime;

use crate::backend;
use crate::iceberg_catalog::IcebergCatalog;
use crate::iceberg_landing::append_parquet_snapshot;
use crate::iceberg_mirror::{live_table_id, reset_inline_trigger};
use crate::iceberg_sql_catalog::{InlineEndCap, SqlCatalog};

/// Flush `table`'s live inline rows into a real Iceberg Parquet snapshot, retiring
/// the inline rows at the same snapshot. Returns the new mirror snapshot id, or
/// `None` if there were no live inline rows. Serialized per table by a transaction-
/// scoped advisory lock held for the duration of this call.
///
/// The returned id is the table's current snapshot read back after commit, not
/// strictly the snapshot this flush allocated: under a concurrent committed writer
/// it may be newer. It is informational (it does not feed the end-cap), so treat it
/// as "at least this flush's data is live", not as an exact handle to the flush
/// snapshot.
pub async fn flush_table(
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    run_id: RunId,
) -> Result<Option<SnapshotId>> {
    // Transaction-scoped advisory lock on a stable key for this table, held for the
    // whole operation. A *xact* lock (not a session lock) auto-releases when this
    // transaction ends — including on panic, when the dropped transaction rolls back
    // — so the lock can never leak onto a pooled connection and silently defeat the
    // mutex on reuse (session advisory locks are re-entrant per connection). Two
    // concurrent flushes contend on `key`; the second blocks until the first's tx ends.
    let mut lock_tx = pool.begin().await.map_err(backend)?;
    let key = lock_key(&table.schema, &table.name);
    sqlx::query!("select pg_advisory_xact_lock($1)", key)
        .execute(&mut *lock_tx)
        .await
        .map_err(backend)?;

    let result = flush_locked(catalog, pool, table, run_id).await;

    // End the lock-holding transaction (nothing was written on it; rollback releases
    // the lock). A drop would do the same — this is explicit for clarity.
    drop(lock_tx.rollback().await);
    result
}

async fn flush_locked(
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    run_id: RunId,
) -> Result<Option<SnapshotId>> {
    let ice = IcebergCatalog::new(pool.clone());
    // If the table has never been written to (no snapshot in the mirror), there are
    // no inline rows to flush. Treat `NotFound` as the empty case, not an error.
    let current = match ice.current_snapshot(table).await {
        Ok(snap) => snap,
        Err(ControlPlaneError::NotFound(_)) => return Ok(None),
        Err(e) => return Err(e),
    };

    // Capture the live inline rows + their ids (+ the inline table id) at current.
    let Some((tid, row_ids, batch)) = ice.inline_live_batch(table, current.id).await? else {
        // Nothing live to flush — but a prior crash could have left the trigger
        // armed; disarm it so the table can re-trigger. No-op if no trigger row.
        let mut conn = pool.acquire().await.map_err(backend)?;
        if let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name).await? {
            reset_inline_trigger(&mut conn, tid).await?;
        }
        return Ok(None);
    };

    // Physical schema (model/inferred logical types) for create-if-absent.
    let columns: Vec<ColumnSpec> = ice
        .schema(table, current.id)
        .await?
        .columns
        .into_iter()
        .map(|c| ColumnSpec {
            name: c.name,
            ty: c.ty,
            nullable: c.nullable,
        })
        .collect();

    let lineage = compaction_event(table, run_id);
    let end_cap = InlineEndCap {
        table_id: tid,
        row_ids: &row_ids,
    };

    // Enqueue one rebuild job per declared vector index, atomically with the
    // snapshot commit. Deduped against pending (state='available') jobs so a
    // second flush while a build is already queued doesn't double-enqueue.
    let index_names = crate::vector_index::declared_vector_index_names(pool, table).await?;
    let rebuild_jobs: Vec<NewJob> = index_names
        .iter()
        .map(|index_name| {
            let payload = serde_json::to_value(BuildVectorIndexJob {
                schema: table.schema.clone(),
                name: table.name.clone(),
                index_name: index_name.clone(),
            })
            .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
            Ok(NewJob {
                kind: BUILD_VECTOR_INDEX_JOB_KIND.to_string(),
                payload,
                run_at: None,
                priority: 0,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let snap = append_parquet_snapshot(
        pool,
        catalog,
        table,
        &columns,
        vec![batch],
        Some(&lineage),
        Some(end_cap),
        false,
        &rebuild_jobs,
    )
    .await?;

    // Disarm the trigger now that the inline rows are file-backed. Standalone
    // (not in the cap tx): a crash between the cap and here self-heals because the
    // job is re-run and hits the None branch above. No-op if no trigger row.
    let mut conn = pool.acquire().await.map_err(backend)?;
    reset_inline_trigger(&mut conn, tid).await?;

    Ok(Some(snap))
}

/// A loom compaction lineage event: the table is both input and output.
fn compaction_event(table: &TableRef, run_id: RunId) -> LineageEvent {
    let dr = DatasetId::from(table).dataset_ref();
    LineageEvent {
        run_id,
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![dr.clone()],
        outputs: vec![dr],
        payload: serde_json::json!({ "source": "flush" }),
    }
}

/// 64-bit advisory-lock key from the table identity (stable per (schema, name)).
/// `pub(crate)` so GC (`iceberg_gc`) takes the *same* key and serializes against flush.
pub(crate) fn lock_key(schema: &str, name: &str) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    schema.hash(&mut h);
    "\u{1f}".hash(&mut h);
    name.hash(&mut h);
    h.finish() as i64
}
