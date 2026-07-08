//! Engine-side `consolidate_stream`: fold a `kind='cdc'` table's base by
//! LastRow-per-identity and clear its inline-shadow flag.
//!
//! A CDC base carries the `+I/+U/-D` change subset (never `-U`) across however
//! many flushes have run; the same identity can appear many times. This op reads
//! the base's PHYSICAL framed rows (Parquet files + any still-live inline tail),
//! folds them with DataFusion so the greatest `loom_offset` per identity wins
//! (dropping a `-D` winner — the identity was deleted), and rewrites the base as
//! that folded set via [`overwrite_parquet_snapshot`], which preserves framing
//! for a declared stream table. The durable changelog table (every event,
//! `-U` included) is never touched — it is the append-only log; its retention
//! rides `gc_table`. A non-CDC table (batch or log, or one that has never been
//! written) is a no-op, reported as snapshot id `0`.

use control_plane_core::{
    Catalog, ColumnSpec, DatasetId, EventType, LineageEvent, RunId, StreamKind, StreamTables,
    TableRef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::lock_table;
use control_plane_postgres::iceberg_inline::clear_has_shadow;
use control_plane_postgres::iceberg_landing::overwrite_parquet_snapshot;
use control_plane_postgres::iceberg_mirror::{clear_consolidate_trigger, live_table_id};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
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
/// mirroring `iceberg_flush.rs`'s `compaction_event`.
fn consolidate_event(table: &TableRef) -> LineageEvent {
    let dr = DatasetId::from(table).dataset_ref();
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![dr.clone()],
        outputs: vec![dr],
        payload: serde_json::json!({ "source": "consolidate_stream" }),
    }
}

/// Fold `table` (a CDC base) down to one row per identity and clear its
/// inline-shadow flag. Returns the new base snapshot id, or `0` if `table` is
/// not a live, declared `kind='cdc'` table (a no-op — consolidation only
/// applies to CDC bases).
pub async fn consolidate_stream(
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

    let Some(meta) = cp.stream_meta(tid).await.map_err(to_serving)? else {
        return Ok(0);
    };
    if meta.kind != StreamKind::Cdc {
        return Ok(0);
    }
    let identity = meta.bucket_key.ok_or_else(|| {
        EngineServingError::Engine(format!(
            "cdc table {}.{} (tid {tid}) has no bucket_key",
            table.schema, table.name
        ))
    })?;

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
    let result = consolidate_locked(pool, catalog, table, tid, &identity).await;
    lock.release().await;
    result
}

async fn consolidate_locked(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    tid: i64,
    identity: &str,
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
    // Greatest loom_offset per identity wins; a winner tombstoned by `-D` (a
    // delete) is dropped, so the identity does not resurrect in the folded base.
    // The base's physical framing carries no `loom_tombstone` column (only the
    // three reserved `loom_change_kind`/`loom_bucket`/`loom_offset` — see
    // `framing_column_specs`), but every delete is written with
    // `loom_change_kind = '-D'` (`write_cdc_row`), so this predicate is exactly
    // equivalent to also checking tombstone.
    let fold_sql = format!(
        "select {col_list}, loom_change_kind, loom_bucket, loom_offset from ( \
             select *, row_number() over ( \
                 partition by {id_quoted} order by loom_offset desc \
             ) as _rn \
             from ({union_sql}) base_input \
         ) t where _rn = 1 and loom_change_kind <> '-D'"
    );

    let df = df_ctx
        .sql(&fold_sql)
        .await
        .map_err(EngineServingError::Plan)?;
    let folded = df.collect().await.map_err(to_serving)?;

    let lineage = consolidate_event(table);
    let snap = overwrite_parquet_snapshot(pool, catalog, table, &user_cols, folded, Some(&lineage))
        .await
        .map_err(to_serving)?;

    let mut conn = pool.acquire().await.map_err(to_serving)?;
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
