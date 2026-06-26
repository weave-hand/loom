//! Physical GC of end-capped Iceberg-mirror rows (slice 1).
//!
//! Every Iceberg retirement is an *end-cap*, not a delete: a row's `end_snapshot`
//! is set so older time-travel reads still see it, while the bytes (and, for
//! `data_file` rows, the Parquet object) live on. `gc_table` reclaims the
//! mirror-driven dead bytes — end-capped `data_file` rows + their Parquet, and
//! end-capped inline rows — under an age-based retention horizon, without ever
//! breaking an in-window time-travel read.
//!
//! ## Safety invariant
//! A row is reclaimable iff `end_snapshot IS NOT NULL AND end_snapshot <= H`,
//! where `H = max(snapshot_id) WHERE snapshot_time < now() - retention` (the
//! youngest snapshot fully aged out of the window). The MVCC read predicate is
//! `begin_snapshot <= at AND (end_snapshot IS NULL OR end_snapshot > at)`; an
//! end-capped row with `end_snapshot = E` is visible only for `at < E`. The
//! oldest `at` any in-window reader may supply is a snapshot `> H`, so if
//! `E <= H` the row is invisible to every guaranteed read. Live rows
//! (`end_snapshot IS NULL`) are never touched.
//!
//! ## Snapshot age
//! The age horizon uses the existing `iceberg_mirror.snapshot.snapshot_time`
//! column (`default now()`, inserted inside the committing transaction by
//! `next_snapshot`), so it is the snapshot's commit time — no extra column is
//! needed.
//!
//! ## Ordering: commit-then-delete
//! The mirror is the source of truth. The transaction deletes the rows first;
//! the Parquet objects are deleted *after* commit. A failure between the two
//! degrades a file to the already-deferred orphaned-Parquet class — never data
//! loss, never a dangling mirror→file reference.
//!
//! ## SQL strategy
//! This module uses *runtime* `sqlx::query`/`query_scalar` rather than the
//! compile-time `query!` macros the rest of the adapter prefers, because the
//! `.sqlx`-cache regen tool (`cargo sqlx prepare`) currently fails to compile the
//! crate in cargo-mode (`serde` is not a direct dependency — buck2 supplies it via
//! graph-wide feature unification, cargo does not). Static SQL literals need no
//! `AssertSqlSafe`; the dynamic `inline_<table_id>` delete uses it. Every query is
//! exercised against a real schema by the fixture tests in `tests/iceberg_gc.rs`.

use std::time::Duration;

use control_plane_core::{Result, TableRef};
use sqlx::{AssertSqlSafe, PgPool};
use time::OffsetDateTime;

use crate::backend;
use crate::iceberg_flush::lock_key;
use crate::iceberg_inline::inline_table_name;
use crate::iceberg_mirror::live_table_id;
use crate::iceberg_sql_catalog::SqlCatalog;

/// Counts of what a `gc_table` run reclaimed, for observability and tests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcSummary {
    pub data_file_rows: u64,
    pub inline_rows: u64,
    pub objects_deleted: u64,
}

/// Reclaim aged-out end-capped rows + their Parquet for one table.
///
/// Serializes per-table against flush/overwrite via the shared advisory lock
/// (`lock_key`). Returns an empty summary (a no-op, not an error) when the table
/// has no live mirror row or when no snapshot has aged out.
pub async fn gc_table(
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    retention: Duration,
) -> Result<GcSummary> {
    // Transaction-scoped advisory lock on the table's stable key — the SAME key
    // flush/overwrite take, so GC never races a concurrent flush on this table.
    let mut lock_tx = pool.begin().await.map_err(backend)?;
    let key = lock_key(&table.schema, &table.name);
    sqlx::query("select pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut *lock_tx)
        .await
        .map_err(backend)?;

    let result = gc_locked(catalog, pool, table, retention).await;

    // Rolling back the lock-holding tx releases the advisory lock.
    drop(lock_tx.rollback().await);
    result
}

async fn gc_locked(
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    retention: Duration,
) -> Result<GcSummary> {
    // 1. Resolve the live table id. A table with no live mirror row (never
    //    created, or dropped) is out of scope for slice 1 — a clean no-op.
    let mut conn = pool.acquire().await.map_err(backend)?;
    let tid = match live_table_id(&mut conn, &table.schema, &table.name).await? {
        Some(t) => t,
        None => return Ok(GcSummary::default()),
    };
    drop(conn);

    // 2. Resolve the horizon H = youngest snapshot fully aged out of the window.
    //    `now()` is taken in Rust; sub-second precision is irrelevant at GC scale.
    //    `max()` over zero matching rows yields NULL → None → a clean no-op.
    let cutoff = OffsetDateTime::now_utc() - time::Duration::seconds(retention.as_secs() as i64);
    let horizon: Option<i64> = sqlx::query_scalar(
        "select max(snapshot_id) from iceberg_mirror.snapshot where snapshot_time < $1",
    )
    .bind(cutoff)
    .fetch_one(pool)
    .await
    .map_err(backend)?;
    let h = match horizon {
        Some(h) => h,
        None => return Ok(GcSummary::default()),
    };

    // 3. Collect the Parquet paths of reclaimable data files (before deleting the
    //    rows that name them).
    let paths: Vec<String> = sqlx::query_scalar(
        "select path from iceberg_mirror.data_file \
         where table_id = $1 and end_snapshot is not null and end_snapshot <= $2",
    )
    .bind(tid)
    .bind(h)
    .fetch_all(pool)
    .await
    .map_err(backend)?;

    // 4. Delete mirror rows in one transaction: stats first (FK child), then the
    //    data_file rows, then end-capped inline rows.
    let mut tx = pool.begin().await.map_err(backend)?;
    sqlx::query(
        "delete from iceberg_mirror.data_file_column_stat \
         where data_file_id in ( \
             select data_file_id from iceberg_mirror.data_file \
             where table_id = $1 and end_snapshot is not null and end_snapshot <= $2)",
    )
    .bind(tid)
    .bind(h)
    .execute(&mut *tx)
    .await
    .map_err(backend)?;

    let data_file_rows = sqlx::query(
        "delete from iceberg_mirror.data_file \
         where table_id = $1 and end_snapshot is not null and end_snapshot <= $2",
    )
    .bind(tid)
    .bind(h)
    .execute(&mut *tx)
    .await
    .map_err(backend)?
    .rows_affected();

    // Inline storage is a per-table physical table that may not exist. Guard with
    // to_regclass; the dynamic table name forces a runtime AssertSqlSafe query.
    let inline = inline_table_name(tid);
    let exists: Option<String> = sqlx::query_scalar(AssertSqlSafe("select to_regclass($1)::text"))
        .bind(&inline)
        .fetch_one(&mut *tx)
        .await
        .map_err(backend)?;
    let inline_rows = if exists.is_some() {
        sqlx::query(AssertSqlSafe(format!(
            "delete from {inline} where end_snapshot is not null and end_snapshot <= $1"
        )))
        .bind(h)
        .execute(&mut *tx)
        .await
        .map_err(backend)?
        .rows_affected()
    } else {
        0
    };

    tx.commit().await.map_err(backend)?;

    // 5. Commit-then-delete: now reclaim the Parquet objects. A failed delete is
    //    logged and left as an orphan (never re-raised into a hard error).
    let mut objects_deleted = 0u64;
    for path in &paths {
        match catalog.delete_file(path).await {
            Ok(()) => objects_deleted += 1,
            Err(e) => tracing::warn!(
                error = %e,
                path = %path,
                "gc: failed to delete Parquet object; leaving as orphan"
            ),
        }
    }

    Ok(GcSummary {
        data_file_rows,
        inline_rows,
        objects_deleted,
    })
}
