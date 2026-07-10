//! Engine-side `consolidate_table`: fold a shadow-bearing base down to one row
//! per identity and re-enable its suppressed flush. Dispatches per table kind —
//! the two arms are the SAME operation under two precedence orders:
//!
//! - **CDC arm** (`kind='cdc'`, [`Precedence::Offset`]): a CDC base carries the
//!   `+I/+U/-D` change subset (never `-U`) across however many flushes have run;
//!   the same identity can appear many times. [`consolidate_locked`] reads the
//!   base's PHYSICAL framed rows (Parquet files + any still-live inline tail),
//!   folds them so the greatest `loom_offset` per identity wins (dropping a `-D`
//!   winner — the identity was deleted), and rewrites the base as that folded set,
//!   preserving framing. When inline rows were folded in, the rewrite commits via
//!   the CONSUMING overwrite ([`overwrite_parquet_snapshot_consuming`]), which
//!   retires exactly those folded rows (an inline write that lands mid-fold
//!   survives, instead of being end-capped unfolded); with no live inline rows
//!   there is nothing to retire, so the plain [`overwrite_parquet_snapshot`]
//!   commits instead. The durable changelog table (every event, `-U` included) is
//!   never touched — it is the append-only log; its retention rides `gc_table`.
//!
//! - **COW arm** (non-CDC identity table with `has_shadow`, [`Precedence::Snapshot`]):
//!   a shadow-bearing non-CDC identity table accumulates an inline tier — plain
//!   appends, `+U` version rows, and `-D` tombstones — on top of its Parquet base.
//!   [`consolidate_cow_locked`] folds `files ∪ inline` so the greatest
//!   `begin_snapshot` per identity wins (dropping a tombstoned winner), materializes
//!   that merge-on-read view as a NEW base snapshot via the CONSUMING overwrite
//!   ([`overwrite_parquet_snapshot_consuming`]) — which retires exactly the folded
//!   inline rows (a mutation that lands mid-fold survives and keeps shadowing) —
//!   and re-enables the byte-trigger flush by clearing the quiescent `has_shadow`
//!   flag. The fold IS the read, materialized: reads before/after are identical
//!   by construction (see [`build_merge_view`](crate::serving), `Precedence::Snapshot`).
//!
//! Any other table (batch or log, an identity-less table, or one that has never
//! been written) is a no-op, reported as snapshot id `0`.

use control_plane_core::{
    Catalog, ColumnSpec, DatasetId, EventType, LineageEvent, RunId, StreamKind, StreamTables,
    TableRef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::lock_table;
use control_plane_postgres::iceberg_inline::{
    clear_has_shadow, clear_has_shadow_if_quiescent, has_shadow,
};
use control_plane_postgres::iceberg_landing::{
    overwrite_parquet_snapshot, overwrite_parquet_snapshot_consuming,
};
use control_plane_postgres::iceberg_mirror::{
    clear_consolidate_trigger, live_table_id, reset_inline_trigger,
};
use control_plane_postgres::iceberg_sql_catalog::{InlineEndCap, SqlCatalog};
use control_plane_postgres::ontology::identity_for_table;
use control_plane_postgres::read_files_as_batches;
use datafusion::execution::context::SessionContext;
use datafusion_io::register_batches;
use sqlx::PgPool;

use crate::serving::{EngineServingError, to_serving};

/// Quote a SQL identifier (double embedded `"`), matching `iceberg_inline.rs`/
/// `provider.rs` — injection-safe against a user-chosen column/identity name.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A loom consolidation lineage event: the table is both input and output,
/// mirroring `iceberg_flush.rs`'s `compaction_event`. `source` tags which arm
/// emitted it (`"consolidate_stream"` for the CDC fold, `"consolidate_cow"` for
/// the tombstone-aware COW fold).
fn consolidate_event(table: &TableRef, source: &str) -> LineageEvent {
    let dr = DatasetId::from(table).dataset_ref();
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![dr.clone()],
        outputs: vec![dr],
        payload: serde_json::json!({ "source": source }),
    }
}

/// Fold `table` down to one row per identity and re-enable its suppressed flush,
/// dispatching per table kind (see the module doc). Returns the new base snapshot
/// id, or `0` for a no-op: a table that is not live, not identity-bearing, or —
/// in the non-CDC case — not currently shadow-bearing.
pub async fn consolidate_table(
    cp: &PgControlPlane,
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
) -> Result<i64, EngineServingError> {
    let mut conn = pool.acquire().await.map_err(to_serving)?;
    let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .map_err(to_serving)?
    else {
        return Ok(0);
    };
    drop(conn);

    match cp.stream_meta(tid).await.map_err(to_serving)? {
        // CDC arm — the `Precedence::Offset` fold, unchanged from slice 1.
        Some(meta) if meta.kind == StreamKind::Cdc => {
            let identity = meta.bucket_key.ok_or_else(|| {
                EngineServingError::Engine(format!(
                    "cdc table {}.{} (tid {tid}) has no bucket_key",
                    table.schema, table.name
                ))
            })?;

            // Resolve the domain version column for a Versioned engine (None for the
            // offset-only engines). A declared versioned CDC table whose type has no
            // version column is a mis-configuration — loud error, never a silent fold.
            let version_col = if matches!(
                meta.merge_engine,
                control_plane_core::MergeEngine::Versioned
            ) {
                Some(
                    control_plane_postgres::ontology::version_for_table(pool, table)
                        .await
                        .map_err(to_serving)?
                        .ok_or_else(|| {
                            EngineServingError::Engine(format!(
                                "cdc table {}.{} (tid {tid}) is merge_engine=versioned but its type has no version column",
                                table.schema, table.name
                            ))
                        })?,
                )
            } else {
                None
            };

            // Serialize the read(files+inline)+fold+overwrite window below against a
            // concurrent `flush_table`/`gc_table` on the SAME table: same per-table
            // advisory-lock key (`iceberg_flush::lock_key`, via the shared `lock_table`
            // helper) flush/GC already take. Without this, a flush committing between
            // this function's file-read and its overwrite-commit could be end-capped by
            // the overwrite WITHOUT its rows ever entering the fold — silent data loss.
            // `pg_advisory_xact_lock` BLOCKS until acquired (no skip/retry), so a
            // concurrent flush simply makes this call wait rather than racing it —
            // exactly `flush_table`'s own contention behavior against a concurrent
            // flush/GC. The lock is released explicitly right after the overwrite
            // commits and the trigger flags below are cleared.
            let lock = lock_table(pool, table).await.map_err(to_serving)?;
            let result = consolidate_locked(
                pool,
                catalog,
                table,
                tid,
                &identity,
                meta.merge_engine,
                version_col.as_deref(),
            )
            .await;
            lock.release().await;
            result
        }
        // COW arm — a non-CDC table folds ONLY when it both bears an identity and
        // is currently shadow-bearing. An identity-less table (no dedup key) or a
        // quiescent one (nothing to fold) is a no-op, reported as snapshot id `0`.
        _ => {
            let Some(identity) = identity_for_table(pool, table).await.map_err(to_serving)? else {
                return Ok(0);
            };
            let mut conn = pool.acquire().await.map_err(to_serving)?;
            let shadowed = has_shadow(&mut conn, tid).await.map_err(to_serving)?;
            drop(conn);
            if !shadowed {
                return Ok(0);
            }
            // Same per-table advisory lock the CDC arm and flush/GC take — serialize
            // the read+fold+consuming-overwrite window against a concurrent flush.
            let lock = lock_table(pool, table).await.map_err(to_serving)?;
            let result = consolidate_cow_locked(pool, catalog, table, tid, &identity).await;
            lock.release().await;
            result
        }
    }
}

async fn consolidate_locked(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    tid: i64,
    identity: &str,
    engine: control_plane_core::MergeEngine,
    version_col: Option<&str>,
) -> Result<i64, EngineServingError> {
    let ice = IcebergCatalog::new(pool.clone());
    let current = match ice.current_snapshot(table).await {
        Ok(snap) => snap,
        // No snapshot yet — the CDC table was declared but never written to, so
        // there is nothing to fold.
        Err(control_plane_core::ControlPlaneError::NotFound(_)) => return Ok(0),
        Err(e) => return Err(to_serving(e)),
    };

    // Plain (framing-free) user columns — the logical schema. `overwrite_parquet_snapshot`
    // re-derives the physical (user + framing) column list itself from these.
    let user_cols: Vec<ColumnSpec> = ice
        .schema(table, current.id)
        .await
        .map_err(to_serving)?
        .columns
        .into_iter()
        .map(|c| ColumnSpec {
            name: c.name,
            ty: c.ty,
            nullable: c.nullable,
        })
        .collect();

    // The base's physical framed rows: live Parquet files ...
    let files = ice
        .files_with_stats(table, current.id)
        .await
        .map_err(to_serving)?;
    let paths: Vec<String> = files.into_iter().map(|f| f.path).collect();
    let (file_schema, file_batches) = read_files_as_batches(catalog, table, &paths)
        .await
        .map_err(to_serving)?;

    // ... UNION any still-live inline tail (un-flushed changes), so a consolidate
    // that runs without a preceding flush still folds correctly. `_full` keeps
    // `-U` before-images in the read, but they never win the fold (the adjacent
    // `+U` always carries a greater `loom_offset`) and are excluded from the
    // output projection like every other non-winning row.
    let inline = ice
        .inline_live_batch_full(table, current.id)
        .await
        .map_err(to_serving)?;

    let df_ctx = SessionContext::new();
    register_batches(&df_ctx, "base_files", file_schema, file_batches).map_err(to_serving)?;
    let has_inline = if let Some((_, _, inline_batch)) = &inline {
        register_batches(
            &df_ctx,
            "base_inline",
            inline_batch.schema(),
            vec![inline_batch.clone()],
        )
        .map_err(to_serving)?;
        true
    } else {
        false
    };

    let col_list = user_cols
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let id_quoted = quote_ident(identity);
    let union_sql = if has_inline {
        format!(
            "select {col_list}, loom_change_kind, loom_bucket, loom_offset from base_files \
             union all \
             select {col_list}, loom_change_kind, loom_bucket, loom_offset from base_inline"
        )
    } else {
        format!("select {col_list}, loom_change_kind, loom_bucket, loom_offset from base_files")
    };
    // Per-engine winner ordering (winner = ROW_NUMBER rank 1). LastRow is the
    // unchanged default (byte-identical). Versioned orders by the quoted domain
    // version column desc, tie-broken by loom_offset desc (highest version wins;
    // last-write-within-version wins). FirstRow takes the smallest offset.
    let order_clause = match engine {
        control_plane_core::MergeEngine::LastRow => "loom_offset desc".to_string(),
        control_plane_core::MergeEngine::FirstRow => "loom_offset asc".to_string(),
        control_plane_core::MergeEngine::Versioned => {
            // Unreachable: consolidate_stream resolves version_col for Versioned
            // before locking. Defense in depth — fall back to loom_offset if ever None.
            // `nulls last` matches `build_merge_view`'s `.sort(false, false)` (DESC
            // NULLS_LAST) so a nullable version column folds identically on read and
            // after consolidate (a NULL version ranks lowest, never wins).
            let vcol = version_col.unwrap_or("loom_offset");
            format!("{} desc nulls last, loom_offset desc", quote_ident(vcol))
        }
    };
    // Greatest-precedence per identity wins; a winner tombstoned by `-D` (a
    // delete) is dropped, so the identity does not resurrect in the folded base.
    // The base's physical framing carries no `loom_tombstone` column (only the
    // three reserved `loom_change_kind`/`loom_bucket`/`loom_offset` — see
    // `framing_column_specs`), but every delete is written with
    // `loom_change_kind = '-D'` (`write_cdc_row`), so this predicate is exactly
    // equivalent to also checking tombstone.
    let fold_sql = format!(
        "select {col_list}, loom_change_kind, loom_bucket, loom_offset from ( \
             select *, row_number() over ( \
                 partition by {id_quoted} order by {order_clause} \
             ) as _rn \
             from ({union_sql}) base_input \
         ) t where _rn = 1 and loom_change_kind <> '-D'"
    );

    let df = df_ctx
        .sql(&fold_sql)
        .await
        .map_err(EngineServingError::Plan)?;
    let folded = df.collect().await.map_err(to_serving)?;

    let lineage = consolidate_event(table, "consolidate_stream");
    // A CDC inline row committing mid-consolidation was previously blanket-capped
    // WITHOUT being folded or changelog-flushed — silent loss; the targeted cap
    // lets it survive to the next flush/consolidate. When `inline` is `None`
    // there was nothing live to consume, so the blanket cap is vacuous and the
    // plain overwrite is equivalent.
    let snap = match inline {
        Some((_, row_ids, _)) => overwrite_parquet_snapshot_consuming(
            pool,
            catalog,
            table,
            &user_cols,
            folded,
            Some(&lineage),
            InlineEndCap {
                table_id: tid,
                row_ids: &row_ids,
            },
        )
        .await
        .map_err(to_serving)?,
        None => overwrite_parquet_snapshot(
            pool,
            catalog,
            table,
            &user_cols,
            folded,
            Some(&lineage),
            &[],
        )
        .await
        .map_err(to_serving)?,
    };

    let mut conn = pool.acquire().await.map_err(to_serving)?;
    // Unconditional (unlike COW's `clear_has_shadow_if_quiescent`): `has_shadow` is
    // never consulted on the CDC path — the flush gate short-circuits on `is_cdc` first.
    clear_has_shadow(&mut conn, tid).await.map_err(to_serving)?;
    // Disarm the consolidate trigger too, so the next accrual of CDC deltas can
    // re-enqueue a `stream_consolidate` job (chosen over keying the re-arm off
    // `has_shadow` alone: this table's own row is authoritative and explicit,
    // and it stays correct even if a future change decouples the two flags).
    clear_consolidate_trigger(&mut conn, tid)
        .await
        .map_err(to_serving)?;

    Ok(snap.0)
}

/// The COW fold (`Precedence::Snapshot`): materialize a shadow-bearing non-CDC
/// identity table's merge-on-read view as a NEW base snapshot, retiring exactly
/// the consumed inline rows in the same commit. Mirrors [`consolidate_locked`]'s
/// shape but reads the MVCC (`begin_snapshot`/`loom_tombstone`) tiers instead of
/// the CDC (`loom_offset`) framing.
async fn consolidate_cow_locked(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    tid: i64,
    identity: &str,
) -> Result<i64, EngineServingError> {
    let ice = IcebergCatalog::new(pool.clone());
    let current = match ice.current_snapshot(table).await {
        Ok(snap) => snap,
        // No snapshot yet — declared but never written, so nothing to fold.
        Err(control_plane_core::ControlPlaneError::NotFound(_)) => return Ok(0),
        Err(e) => return Err(to_serving(e)),
    };

    // Plain (framing-free) user columns — the logical schema.
    // `overwrite_parquet_snapshot_consuming` re-derives the physical column list
    // from these (a non-CDC table has no framing to re-add).
    let user_cols: Vec<ColumnSpec> = ice
        .schema(table, current.id)
        .await
        .map_err(to_serving)?
        .columns
        .into_iter()
        .map(|c| ColumnSpec {
            name: c.name,
            ty: c.ty,
            nullable: c.nullable,
        })
        .collect();

    // The base's file tier: live Parquet paths at the fold snapshot.
    let files = ice
        .files_with_stats(table, current.id)
        .await
        .map_err(to_serving)?;
    let paths: Vec<String> = files.into_iter().map(|f| f.path).collect();
    let has_files = !paths.is_empty();

    // The inline shadow tier: EVERY live inline row (appends, `+U`, `-D`), with
    // its `begin_snapshot` precedence and `loom_tombstone`. `None` means the
    // `has_shadow` flag was stale (nothing live) — self-heal by clearing the flag
    // + triggers and no-op, the flush-style crash recovery.
    let Some((_shadow_tid, row_ids, inline_batch)) = ice
        .inline_live_batch_shadow(table, current.id)
        .await
        .map_err(to_serving)?
    else {
        let mut conn = pool.acquire().await.map_err(to_serving)?;
        clear_has_shadow_if_quiescent(&mut conn, tid)
            .await
            .map_err(to_serving)?;
        reset_inline_trigger(&mut conn, tid)
            .await
            .map_err(to_serving)?;
        clear_consolidate_trigger(&mut conn, tid)
            .await
            .map_err(to_serving)?;
        return Ok(0);
    };

    // Register the tiers. The inline tier is always present here (we early-returned
    // on `None`); a shadowed table CAN be inline-only, so `base_files` is registered
    // only when the file tier has rows.
    let df_ctx = SessionContext::new();
    if has_files {
        let (file_schema, file_batches) = read_files_as_batches(catalog, table, &paths)
            .await
            .map_err(to_serving)?;
        register_batches(&df_ctx, "base_files", file_schema, file_batches).map_err(to_serving)?;
    }
    register_batches(
        &df_ctx,
        "base_inline",
        inline_batch.schema(),
        vec![inline_batch],
    )
    .map_err(to_serving)?;

    let col_list = user_cols
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let id_quoted = quote_ident(identity);
    // File tier synthesizes precedence 0 + a false tombstone; the inline tier uses
    // `begin_snapshot` / `loom_tombstone` — exactly `Precedence::Snapshot`'s column
    // mapping in `build_merge_view` (serving.rs), so the fold and the live read can
    // never drift. An inline row's `begin_snapshot` is always > 0, so any shadow
    // delta outranks the file tier for its identity.
    let union_sql = if has_files {
        format!(
            "select {col_list}, 0 as _loom_prec, false as _loom_tomb from base_files \
             union all \
             select {col_list}, begin_snapshot as _loom_prec, loom_tombstone as _loom_tomb from base_inline"
        )
    } else {
        format!(
            "select {col_list}, begin_snapshot as _loom_prec, loom_tombstone as _loom_tomb from base_inline"
        )
    };
    // Greatest-`begin_snapshot` per identity wins; a winner that is a tombstone is
    // dropped, so the identity does not resurrect. The `Precedence::Snapshot` merge
    // materialized — reads before/after are identical by construction.
    let fold_sql = format!(
        "select {col_list} from ( \
             select {col_list}, _loom_tomb, row_number() over ( \
                 partition by {id_quoted} order by _loom_prec desc \
             ) as _rn \
             from ({union_sql}) base_input \
         ) t where _rn = 1 and _loom_tomb = false"
    );

    let df = df_ctx
        .sql(&fold_sql)
        .await
        .map_err(EngineServingError::Plan)?;
    let folded = df.collect().await.map_err(to_serving)?;

    // Consuming overwrite: write the folded survivors as the new base AND retire
    // exactly the inline rows we folded (`row_ids`) in one commit — every OTHER
    // live inline row (a mutation that landed mid-fold) survives and keeps shadowing.
    let lineage = consolidate_event(table, "consolidate_cow");
    let snap = overwrite_parquet_snapshot_consuming(
        pool,
        catalog,
        table,
        &user_cols,
        folded,
        Some(&lineage),
        InlineEndCap {
            table_id: tid,
            row_ids: &row_ids,
        },
    )
    .await
    .map_err(to_serving)?;

    // Post-commit self-heal, each idempotent and ordered AFTER the commit so a
    // crash before here leaves the flag set and the next run retries: clear
    // `has_shadow` ONLY if now quiescent (a mid-fold mutation must keep the flush
    // suppressed — spec §3), then reset the byte- and consolidate-triggers.
    let mut conn = pool.acquire().await.map_err(to_serving)?;
    clear_has_shadow_if_quiescent(&mut conn, tid)
        .await
        .map_err(to_serving)?;
    reset_inline_trigger(&mut conn, tid)
        .await
        .map_err(to_serving)?;
    clear_consolidate_trigger(&mut conn, tid)
        .await
        .map_err(to_serving)?;

    Ok(snap.0)
}
