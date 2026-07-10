//! Inline flush/compaction: drain a table's live inline rows into a real Iceberg
//! Parquet snapshot and end-cap the inline rows, atomically. Library primitive —
//! no trigger policy (see the spec). Serialized per table by a session advisory
//! lock so two flushes can't both write Parquet for the same rows.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, RecordBatch, StringArray};
use arrow_select::filter::filter_record_batch;
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlaneError, DatasetId, EventType, LineageEvent, Result, RunId,
    Snapshot, SnapshotId, StreamKind, TableRef,
};
use iceberg::{Catalog as IceCatalog, ErrorKind as IceErrorKind, NamespaceIdent, TableIdent};
use sqlx::PgPool;
use time::OffsetDateTime;

use crate::backend;
use crate::iceberg_catalog::IcebergCatalog;
use crate::iceberg_inline::has_shadow;
use crate::iceberg_landing::{
    append_parquet_snapshot, augment_with_framing, changelog_table_ref, coerce_batch_to_ice,
    ensure_iceberg_table,
};
use crate::iceberg_mirror::{live_table_id, reset_inline_trigger};
use crate::iceberg_sql_catalog::{CommitExtras, InlineEndCap, SqlCatalog};
use crate::iceberg_writer::{COMMIT_MAX_RETRIES, append_batches_on_tx, commit_backoff};
use crate::stream::pg_stream_meta;

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

    // A `kind='cdc'` table takes the dual-write branch (base +I/+U/-D subset AND
    // the full changelog, one tx) instead of the single-append path below — see
    // `flush_locked_cdc`. Read the kind once, keyed by the current table id (a
    // table with no inline storage yet has no tid, so it can never be CDC here).
    let mut conn = pool.acquire().await.map_err(backend)?;
    let is_cdc = match live_table_id(&mut conn, &table.schema, &table.name).await? {
        Some(tid) => pg_stream_meta(&mut *conn, tid)
            .await?
            .is_some_and(|m| m.kind == StreamKind::Cdc),
        None => false,
    };
    drop(conn);
    if is_cdc {
        return flush_locked_cdc(catalog, pool, table, &ice, current, run_id).await;
    }

    // Shadow guard (fast path; the authoritative guard is the read-set check below):
    // a table carrying inline shadow deltas must never be drained by the byte-trigger
    // flush — flushing a row-version/tombstone into Parquet would duplicate or
    // resurrect a file row. `inline_append` already refuses to enqueue this job for a
    // shadowed table, but a flush job can have been enqueued BEFORE the mutation
    // landed, so this is a cheap early-out. Mirrors the no-live-rows self-heal below:
    // reset the trigger and report the no-op.
    let mut conn = pool.acquire().await.map_err(backend)?;
    if let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name).await?
        && has_shadow(&mut conn, tid).await?
    {
        reset_inline_trigger(&mut conn, tid).await?;
        return Ok(None);
    }
    drop(conn);

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

    // Safety derives from the DATA, not the flag: a flush can only corrupt by
    // appending rows it READ, so checking the read set is race-free — a delta
    // committing after this read is not in `row_ids` and is not written. This
    // closes the conditional-clear write-skew (spec §3): if a shadow row is in
    // the set, the flag was cleared wrongly; restore it and no-op.
    let mut conn = pool.acquire().await.map_err(backend)?;
    if crate::iceberg_inline::shadow_rows_among(&mut conn, tid, &row_ids).await? {
        crate::iceberg_inline::set_has_shadow(&mut conn, tid).await?;
        reset_inline_trigger(&mut conn, tid).await?;
        return Ok(None);
    }
    drop(conn);

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
    // Job construction is shared with the overwrite/replace commit path via
    // `rebuild_jobs_for`, so the dedup keys always collide across paths.
    let rebuild_jobs = crate::vector_index::rebuild_jobs_for(pool, table).await?;

    // A stream/log table's reserved framing columns ride into the Iceberg physical
    // schema + mirror via `append_parquet_snapshot`'s `include_framing`; `columns`
    // above stays the plain logical (user) list either way — batch tables (`false`)
    // are byte-identical to before this parameter existed.
    let mut conn = pool.acquire().await.map_err(backend)?;
    let include_framing = crate::stream::pg_stream_bucket_count(&mut *conn, tid)
        .await?
        .is_some();
    drop(conn);

    let snap = append_parquet_snapshot(
        pool,
        catalog,
        table,
        &columns,
        vec![batch],
        CommitExtras {
            lineage: Some(&lineage),
            end_cap: Some(end_cap),
            jobs: &rebuild_jobs,
            ..CommitExtras::default()
        },
        include_framing,
    )
    .await?;

    // Disarm the trigger now that the inline rows are file-backed. Standalone
    // (not in the cap tx): a crash between the cap and here self-heals because the
    // job is re-run and hits the None branch above. No-op if no trigger row.
    let mut conn = pool.acquire().await.map_err(backend)?;
    reset_inline_trigger(&mut conn, tid).await?;

    Ok(Some(snap))
}

/// The CDC branch of [`flush_locked`]: drain a `kind='cdc'` table's live inline
/// rows into TWO Iceberg snapshots in ONE Postgres transaction — the base table
/// gains the `+I/+U/-D` subset (never `-U`), the changelog table
/// ([`changelog_table_ref`]) gains every row, `-U` included. Both tables' mirror
/// pointers advance together or not at all: `append_batches_on_tx` is a
/// SINGLE-ATTEMPT, caller-tx commit (`iceberg_writer.rs`), so both appends ride
/// the same `tx` and a lost CAS on EITHER rolls back both. The inline rows are
/// end-capped exactly once, on the base append's `CommitExtras`.
async fn flush_locked_cdc(
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    ice: &IcebergCatalog,
    current: Snapshot,
    run_id: RunId,
) -> Result<Option<SnapshotId>> {
    // The full change sequence (incl. `-U`) — the changelog set. `tid`/`row_ids`
    // are the same inline rows the base subset is filtered from, so one end-cap
    // (on the base append below) retires exactly what both batches were built from.
    let Some((tid, row_ids, all_batch)) = ice.inline_live_batch_full(table, current.id).await?
    else {
        // Nothing live to flush — disarm a possibly-armed trigger, same self-heal
        // as the non-CDC branch.
        let mut conn = pool.acquire().await.map_err(backend)?;
        if let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name).await? {
            reset_inline_trigger(&mut conn, tid).await?;
        }
        return Ok(None);
    };
    let base_batch = filter_out_minus_u(&all_batch)?;

    // Physical schema (user columns + framing — a CDC table is always a declared
    // stream table) shared by both tables' create-if-absent + field-id rewrap.
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
    let full_columns = augment_with_framing(&columns, true);

    let lineage = compaction_event(table, run_id);
    let end_cap = InlineEndCap {
        table_id: tid,
        row_ids: &row_ids,
    };
    let rebuild_jobs = crate::vector_index::rebuild_jobs_for(pool, table).await?;

    let clog_ref = changelog_table_ref(table);
    // Idempotent (create-if-absent): the changelog table's Iceberg metadata is
    // normally created at CDC declaration (`land_cdc`), but a table seeded only
    // via the inline write primitives (as in tests) never goes through that path
    // — so this is the backstop, mirroring `append_parquet_snapshot`'s own
    // `ensure_iceberg_table` call for the base table.
    ensure_iceberg_table(catalog, table, &columns, true).await?;
    ensure_iceberg_table(catalog, &clog_ref, &columns, true).await?;

    let base_ident = TableIdent::new(
        NamespaceIdent::new(table.schema.clone()),
        table.name.clone(),
    );
    let clog_ident = TableIdent::new(
        NamespaceIdent::new(clog_ref.schema.clone()),
        clog_ref.name.clone(),
    );
    let mut base_table = catalog.load_table(&base_ident).await.map_err(backend)?;
    let mut clog_table = catalog.load_table(&clog_ident).await.map_err(backend)?;

    // Retry loop (single-attempt `append_batches_on_tx`; a lost CAS rolls back the
    // whole tx so BOTH tables' pointers move together or not at all — mirrors
    // `iceberg_landing::land_parquet_stream`'s atomic direct-write retry).
    let mut attempt: u32 = 0;
    let new_snap = loop {
        let base_arrow = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(base_table.metadata().current_schema())
                .map_err(backend)?,
        );
        let clog_arrow = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(clog_table.metadata().current_schema())
                .map_err(backend)?,
        );
        let base_coerced = coerce_batch_to_ice(&base_batch, &base_arrow, &full_columns)?;
        let clog_coerced = coerce_batch_to_ice(&all_batch, &clog_arrow, &full_columns)?;

        let mut tx = pool.begin().await.map_err(backend)?;

        // Base: the +I/+U/-D subset, end-capping the inline rows here (exactly once).
        let base_res = append_batches_on_tx(
            catalog,
            &base_table,
            vec![base_coerced],
            CommitExtras {
                lineage: Some(&lineage),
                end_cap: Some(end_cap.clone()),
                jobs: &rebuild_jobs,
                ..CommitExtras::default()
            },
            &mut tx,
        )
        .await;

        // Changelog: every row incl. -U, append-only (no end-cap) — only attempted
        // if the base append staged cleanly on this tx.
        let clog_res = match base_res {
            Ok(_) => {
                append_batches_on_tx(
                    catalog,
                    &clog_table,
                    vec![clog_coerced],
                    CommitExtras {
                        lineage: Some(&lineage),
                        ..CommitExtras::default()
                    },
                    &mut tx,
                )
                .await
            }
            Err(e) => Err(e),
        };

        match clog_res {
            Ok(_) => {
                tx.commit().await.map_err(backend)?;
                break ice.current_snapshot(table).await?.id;
            }
            Err(e)
                if e.kind() == IceErrorKind::CatalogCommitConflicts
                    && attempt < COMMIT_MAX_RETRIES =>
            {
                // Roll back the WHOLE tx: whichever append staged (base, or neither)
                // is undone together, so a lost CAS never leaves one table's pointer
                // ahead of the other's.
                drop(tx.rollback().await);
                tokio::time::sleep(commit_backoff(&base_ident, attempt, None)).await;
                base_table = catalog.load_table(&base_ident).await.map_err(backend)?;
                clog_table = catalog.load_table(&clog_ident).await.map_err(backend)?;
                attempt += 1;
            }
            Err(e) => {
                drop(tx.rollback().await);
                return Err(backend(e));
            }
        }
    };

    // Disarm the trigger now that the inline rows are file-backed in both tables.
    let mut conn = pool.acquire().await.map_err(backend)?;
    reset_inline_trigger(&mut conn, tid).await?;

    Ok(Some(new_snap))
}

/// Filter `batch` (a stream-framed inline read, carrying `loom_change_kind`) to
/// the base subset: every row EXCEPT `-U` before-images. Used by the CDC flush
/// branch to derive the base append from the changelog's full read
/// (`inline_live_batch_full`), so both batches are built from the identical
/// live-row snapshot.
fn filter_out_minus_u(batch: &RecordBatch) -> Result<RecordBatch> {
    let idx = batch.schema().index_of("loom_change_kind").map_err(|e| {
        ControlPlaneError::Backend(
            format!("cdc flush: batch has no loom_change_kind column: {e}").into(),
        )
    })?;
    let kinds = batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            ControlPlaneError::Backend("cdc flush: loom_change_kind is not a string column".into())
        })?;
    let mask: BooleanArray = (0..kinds.len())
        .map(|i| Some(kinds.value(i) != "-U"))
        .collect();
    filter_record_batch(batch, &mask).map_err(backend)
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

/// A held per-table advisory lock (see [`lock_table`]). Dropping without calling
/// [`TableLock::release`] still releases the lock — it is a transaction-scoped
/// (`pg_advisory_xact_lock`) lock, so it is freed whenever the underlying
/// transaction ends, including via `Drop`; `release` just does it explicitly
/// and promptly rather than waiting on the async runtime to drop the future.
#[must_use]
pub struct TableLock {
    tx: sqlx::Transaction<'static, sqlx::Postgres>,
}

impl TableLock {
    /// End the lock-holding transaction (a rollback — nothing was ever written
    /// on it), releasing the advisory lock.
    pub async fn release(self) {
        drop(self.tx.rollback().await);
    }
}

/// Acquire the SAME per-table advisory lock `flush_table`/`gc_table` use (same
/// key derivation, `lock_key`), for the duration of the returned guard. This is
/// the shared mutex that serializes flush, GC, and (via this function)
/// consolidation against each other on one table: `pg_advisory_xact_lock`
/// BLOCKS until any concurrent holder's lock-transaction ends — no skip/retry —
/// so callers inherit exactly `flush_table`'s own contention behaviour.
pub async fn lock_table(pool: &PgPool, table: &TableRef) -> Result<TableLock> {
    let mut tx = pool.begin().await.map_err(backend)?;
    let key = lock_key(&table.schema, &table.name);
    sqlx::query!("select pg_advisory_xact_lock($1)", key)
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
    Ok(TableLock { tx })
}
