//! Inline flush/compaction: drain a table's live inline rows into a real Iceberg
//! Parquet snapshot and end-cap the inline rows, atomically. Library primitive —
//! no trigger policy (see the spec). Serialized per table by a session advisory
//! lock so two flushes can't both write Parquet for the same rows.

/// The queue `kind` for an inline-flush job.
pub const FLUSH_JOB_KIND: &str = "flush_table";

/// The payload of a `flush_table` job: which table to flush. Produced by the
/// trigger (`inline_append`), consumed by the flush worker (Spec 2).
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct FlushJob {
    pub schema: String,
    pub name: String,
}

use control_plane_core::{
    Catalog, ColumnSpec, ControlPlaneError, DatasetId, EventType, LineageEvent, Result, RunId,
    SnapshotId, TableRef,
};
use sqlx::PgPool;
use time::OffsetDateTime;

use crate::backend;
use crate::iceberg_catalog::IcebergCatalog;
use crate::iceberg_landing::append_parquet_snapshot;
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
    sqlx::query("select pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut *lock_tx)
        .await
        .map_err(backend)?;

    let result = flush_locked(catalog, pool, table, run_id).await;

    // End the lock-holding transaction (nothing was written on it; rollback releases
    // the lock). A drop would do the same — this is explicit for clarity.
    let _ = lock_tx.rollback().await;
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

    let snap = append_parquet_snapshot(
        pool,
        catalog,
        table,
        &columns,
        vec![batch],
        Some(&lineage),
        Some(end_cap),
    )
    .await?;
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
fn lock_key(schema: &str, name: &str) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    schema.hash(&mut h);
    "\u{1f}".hash(&mut h);
    name.hash(&mut h);
    h.finish() as i64
}
