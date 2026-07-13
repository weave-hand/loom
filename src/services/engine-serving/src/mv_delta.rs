//! The framed-delta read for a standing query (materialized view): every
//! `(loom_bucket, loom_offset)`-ordered event of a declared LOG stream table at
//! or beyond `mv`'s committed per-bucket watermark. Mirrors `consolidate_locked`'s
//! read shape (`consolidate.rs:180`) — same `lock_table` + `current_snapshot` +
//! `schema` -> `user_cols` + `files_with_stats` -> `read_files_as_batches` (only
//! when the mirror reports live files — an inline-only source has no Iceberg
//! catalog row to load) + `inline_live_batch_full` read — but folds NOTHING: the
//! output is every framed
//! row at-or-beyond the watermark, `(bucket, offset)`-ordered, framing columns
//! INCLUDED (the worker derives its watermark CAS bounds from them, then strips
//! them before running user SQL — see `MvDeltaTicket`'s doc comment).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{Field, Schema, SchemaRef};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlaneError, StreamKind, StreamMeta, StreamTables, TableRef,
    resolve_logical,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::lock_table;
use control_plane_postgres::iceberg_landing::framing_column_specs;
use control_plane_postgres::iceberg_mirror::live_table_id;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use control_plane_postgres::read_files_as_batches;
use control_plane_postgres::stream::pg_mv_watermarks;
use datafusion::execution::context::SessionContext;
use datafusion_io::register_batches;
use sqlx::PgPool;

use crate::serving::{EngineServingError, to_serving};

/// Quote a SQL identifier (double embedded `"`), matching `consolidate.rs`/
/// `iceberg_inline.rs` — injection-safe against a user-chosen column name.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The framed source delta of `table` for standing query `mv`: every event at or
/// beyond `mv`'s committed per-bucket watermark, files UNION inline,
/// `(loom_bucket, loom_offset)`-ordered. `table` must be a live, declared LOG
/// stream table — a CDC source is deferred (`Err(EngineServingError::Engine)`,
/// a deterministic message naming the table; the caller maps it to
/// `Status::failed_precondition`). Read under the per-table flush/GC/consolidate
/// advisory lock, exactly `consolidate_locked`'s read shape
/// (`engine-serving/src/consolidate.rs:180`) — a flush moving rows
/// files<->inline mid-read would otherwise double-read or drop them. A source
/// that has only ever been inline-appended (every fresh micro-batch MV output)
/// reads fine — the file tier is skipped, not loaded.
pub async fn mv_delta_scan(
    cp: &PgControlPlane,
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    mv: &str,
) -> Result<(SchemaRef, Vec<RecordBatch>), EngineServingError> {
    let mut conn = pool.acquire().await.map_err(to_serving)?;
    let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .map_err(to_serving)?
    else {
        return Err(EngineServingError::Engine(format!(
            "mv delta: unknown source table {}.{}",
            table.schema, table.name
        )));
    };
    drop(conn);

    let meta: StreamMeta = cp
        .stream_meta(tid)
        .await
        .map_err(to_serving)?
        .filter(|m| m.kind == StreamKind::Log)
        .ok_or_else(|| {
            EngineServingError::Engine(format!(
                "mv delta: {}.{} is not a declared log stream table (cdc sources are deferred)",
                table.schema, table.name
            ))
        })?;

    let wm = pg_mv_watermarks(pool, mv, tid).await.map_err(to_serving)?;

    // Same per-table advisory lock flush/GC/consolidate take (`consolidate_table`'s
    // comment explains the race this closes): serialize this read against a
    // concurrent flush so no row is double-read (still inline AND already
    // flushed) or dropped (flushed after the file-list snapshot below but before
    // the inline read).
    let lock = lock_table(pool, table).await.map_err(to_serving)?;
    let result = mv_delta_locked(catalog, pool, table, &meta, &wm).await;
    lock.release().await;
    result
}

async fn mv_delta_locked(
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    meta: &StreamMeta,
    wm: &BTreeMap<i32, i64>,
) -> Result<(SchemaRef, Vec<RecordBatch>), EngineServingError> {
    let ice = IcebergCatalog::new(pool.clone());
    let current = match ice.current_snapshot(table).await {
        Ok(snap) => snap,
        // Declared but never written: no snapshot means no schema was ever
        // resolved either, so there is nothing to read — an empty result.
        Err(ControlPlaneError::NotFound(_)) => {
            return Ok((Arc::new(Schema::empty()), vec![]));
        }
        Err(e) => return Err(to_serving(e)),
    };

    // Plain (framing-free) user columns — the logical schema.
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

    // The table's physical framed rows: live Parquet files ...
    let files = ice
        .files_with_stats(table, current.id)
        .await
        .map_err(to_serving)?;
    let paths: Vec<String> = files.into_iter().map(|f| f.path).collect();

    // ... UNION any still-live inline tail (un-flushed events), so a delta read
    // that runs without a preceding flush still sees the whole tail. `_full`
    // keeps every framed row (a log table never emits `-U`, so this is exactly
    // "every live inline row" — the `_full` variant is used only for parity with
    // `consolidate_locked`'s read shape).
    let inline = ice
        .inline_live_batch_full(table, current.id)
        .await
        .map_err(to_serving)?;

    // Neither tier: an empty delta over the MIRROR-derived framed schema. Returning
    // here (rather than falling through) is what keeps `read_files_as_batches` — and
    // therefore `catalog.load_table` — off the path for a table that has ONLY ever
    // been inline-appended: such a table has no vendored `iceberg_tables` row (only
    // the Parquet-write path's `ensure_iceberg_table` creates one), so `load_table`
    // would error even for an EMPTY file list. Same mirror-authoritative shape as
    // `build_serving_provider`'s zero-row provider (`serving.rs:211`, the #421
    // `iss-serving-empty-table-not-found` fix).
    if paths.is_empty() && inline.is_none() {
        return Ok((framed_schema(&user_cols)?, vec![]));
    }

    // Register ONLY the tiers that exist. Skipping the file tier when the mirror
    // reports no live files is the other half of the same fix — an MV output is
    // inline-only until its first flush (`commit_micro_batch` -> `inline_append_mv`),
    // and a downstream MV must be able to read it as a source immediately
    // (`iss-mv-delta-inline-source-unflushed`). A source WITH files takes the
    // unchanged path. Mirrors `consolidate_locked`'s shadow read
    // (`consolidate.rs:420`).
    let df_ctx = SessionContext::new();
    let mut tiers: Vec<&str> = Vec::new();
    if !paths.is_empty() {
        let (file_schema, file_batches) = read_files_as_batches(catalog, table, &paths)
            .await
            .map_err(to_serving)?;
        register_batches(&df_ctx, "mv_delta_files", file_schema, file_batches)
            .map_err(to_serving)?;
        tiers.push("mv_delta_files");
    }
    if let Some((_, _, inline_batch)) = &inline {
        register_batches(
            &df_ctx,
            "mv_delta_inline",
            inline_batch.schema(),
            vec![inline_batch.clone()],
        )
        .map_err(to_serving)?;
        tiers.push("mv_delta_inline");
    }

    let col_list = user_cols
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    // One `select ... from <tier>` per registered tier, UNION ALL'd. With both tiers
    // present this is byte-identical to the previous two-arm `has_inline` format!.
    let union_sql = tiers
        .iter()
        .map(|t| format!("select {col_list}, loom_change_kind, loom_bucket, loom_offset from {t}"))
        .collect::<Vec<_>>()
        .join(" union all ");

    // Per-bucket watermark predicate: every event at or beyond the mv's committed
    // next-offset for that bucket (a bucket absent from `wm` reads as 0 — the
    // whole bucket is undelivered — matching `MvWatermarks::mv_watermarks`'s
    // documented "buckets with no row are absent (read as 0)" contract).
    let preds: Vec<String> = (0..meta.bucket_count)
        .map(|b| {
            let from = wm.get(&b).copied().unwrap_or(0);
            format!("(loom_bucket = {b} and loom_offset >= {from})")
        })
        .collect();
    let sql = format!(
        "select {col_list}, loom_change_kind, loom_bucket, loom_offset \
         from ({union_sql}) d where {} order by loom_bucket, loom_offset",
        preds.join(" or ")
    );

    let df = df_ctx.sql(&sql).await.map_err(EngineServingError::Plan)?;
    let batches = df.collect().await.map_err(to_serving)?;

    let schema = match batches.first() {
        Some(b) => b.schema(),
        // Zero rows/batches: DataFusion leaves nothing to read a schema off, so
        // build it directly from the mirror column specs.
        None => framed_schema(&user_cols)?,
    };
    Ok((schema, batches))
}

/// Build the framed physical schema (`user_cols` + the three reserved
/// `loom_*` framing columns, [`framing_column_specs`]) directly from the mirror
/// column specs — the schema of an EMPTY delta result.
fn framed_schema(user_cols: &[ColumnSpec]) -> Result<SchemaRef, EngineServingError> {
    let framing = framing_column_specs();
    let mut fields: Vec<Field> = Vec::with_capacity(user_cols.len() + framing.len());
    for c in user_cols.iter().chain(framing.iter()) {
        let base = resolve_logical(&c.ty).ok_or_else(|| {
            EngineServingError::Engine(format!("mv delta: unknown logical type `{}`", c.ty))
        })?;
        fields.push(Field::new(&c.name, base.arrow_data_type(), c.nullable));
    }
    Ok(Arc::new(Schema::new(fields)))
}
