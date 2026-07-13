//! `mv_delta_scan` over a source table that has ONLY ever been inline-appended —
//! the shape of every fresh micro-batch MV output (`commit_micro_batch` ->
//! `inline_append_mv`), which never creates an Iceberg SQL-catalog row
//! (`ensure_iceberg_table` runs only on the Parquet-write path). Before the fix,
//! the file leg (`read_files_as_batches` -> `catalog.load_table`) errored even
//! with an EMPTY file list, so a downstream MV chained onto a fresh upstream MV
//! output could not run until the upstream flushed
//! (`iss-mv-delta-inline-source-unflushed`).
//!
//! Cases: (1) inline-only source -> the full framed delta; (2) flush, then more
//! inline rows -> the file ∪ inline union still correct (the transition case);
//! (3) inline-only source whose watermark has consumed everything -> an EMPTY
//! delta over the mirror-derived framed schema, not an error.
//!
//! loom_fixture_test (Postgres + a local `tempfile` warehouse), harness mirrored
//! from `serving_empty_table.rs` (PgFixture + `local_sql_catalog`) and
//! `postgres/tests/iceberg_flush.rs` (land/flush seeding).
//! Spec: docs/superpowers/specs/2026-07-12-mv-delta-inline-source-design.md

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use control_plane_core::{
    ColumnSpec, DatasetId, EventType, LineageEvent, MvWatermarks, RunId, TableRef, WatermarkAdvance,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_mirror::live_table_id;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use engine_serving::mv_delta_scan;
use loom_test_seed::local_sql_catalog;
use sqlx::PgPool;

/// The MV key every case reads under. A key with no `stream.mv_watermark` rows
/// reads every bucket at offset 0 (the documented "absent reads as 0" contract).
const MV: &str = "mv:test";

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

/// The `(id long, val long)` logical (framing-free) schema of the source.
fn columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "val".into(),
            ty: "long".into(),
            nullable: false,
        },
    ]
}

fn arrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]))
}

fn batch(ids: &[i64], vals: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(vals.to_vec())),
        ],
    )
    .expect("batch")
}

fn lineage(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "mv-delta-inline-source-test" }),
    }
}

/// Land `rows` INLINE ONLY into `table`, declaring it a 2-bucket LOG stream:
/// `inline_byte_limit: usize::MAX` forces `land`'s inline branch, so no Parquet
/// file and no Iceberg SQL-catalog row is ever created; `flush_byte_threshold:
/// i64::MAX` disarms the byte-trigger auto-flush.
async fn land_inline(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    ids: &[i64],
    vals: &[i64],
) {
    land(
        pool,
        catalog,
        table,
        &columns(),
        arrow_schema(),
        vec![batch(ids, vals)],
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        lineage(table),
        Some(2),
    )
    .await
    .expect("inline land (log stream declare)");
}

/// The `id` column (column 0 of the framed projection), sorted.
fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        for i in 0..b.num_rows() {
            out.push(arr.value(i));
        }
    }
    out.sort_unstable();
    out
}

/// The `(loom_bucket, loom_offset)` framing pairs, in the order the scan returned
/// them (the scan's `order by loom_bucket, loom_offset`).
fn framing(batches: &[RecordBatch]) -> Vec<(i32, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let bucket = b
            .column_by_name("loom_bucket")
            .expect("loom_bucket present")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("loom_bucket is Int32");
        let offset = b
            .column_by_name("loom_offset")
            .expect("loom_offset present")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("loom_offset is Int64");
        for i in 0..b.num_rows() {
            out.push((bucket.value(i), offset.value(i)));
        }
    }
    out
}

/// Field names of the framed delta schema.
fn names(schema: &SchemaRef) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

/// CAS-advance `MV`'s watermark past every `(bucket, offset)` in `batches` — the
/// same `advance_mv_watermark` a micro-batch commit issues. After this the MV has
/// "consumed" that delta.
async fn consume(cp: &PgControlPlane, tid: i64, batches: &[RecordBatch]) {
    let mut maxes: BTreeMap<i32, i64> = BTreeMap::new();
    for (bucket, offset) in framing(batches) {
        let e = maxes.entry(bucket).or_insert(offset);
        *e = (*e).max(offset);
    }
    let current = cp.mv_watermarks(MV, tid).await.expect("mv_watermarks");
    let advances: Vec<WatermarkAdvance> = maxes
        .iter()
        .map(|(&bucket, &max_offset)| WatermarkAdvance {
            bucket,
            from: current.get(&bucket).copied().unwrap_or(0),
            to: max_offset + 1,
        })
        .collect();
    cp.advance_mv_watermark(MV, tid, &advances)
        .await
        .expect("advance_mv_watermark");
}

/// Resolve the live mirror table id of `table`.
async fn tid_of(pool: &PgPool, table: &TableRef) -> i64 {
    let mut conn = pool.acquire().await.expect("acquire");
    live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .expect("live_table_id")
        .expect("table is live")
}

/// Case 1 (the defect): a source that has NEVER been flushed — no Parquet files,
/// no Iceberg catalog row — must still yield its full framed delta. Before the
/// fix this errored inside `read_files_as_batches`'s unconditional
/// `catalog.load_table`, even though the file list was empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_only_source_reads_full_delta_without_flush() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let table = tref("s", "mv_out");

    land_inline(&pool, &catalog, &table, &[1, 2, 3], &[10, 20, 30]).await;

    let (schema, batches) = mv_delta_scan(&cp, &catalog, &pool, &table, MV)
        .await
        .expect("an inline-only source must read as a delta without a flush");

    assert_eq!(
        ids(&batches),
        vec![1, 2, 3],
        "the whole inline tail is the delta"
    );
    assert_eq!(
        names(&schema),
        vec![
            "id",
            "val",
            "loom_change_kind",
            "loom_bucket",
            "loom_offset"
        ],
        "framed schema: user columns then the three reserved framing columns"
    );
    let pairs = framing(&batches);
    let mut sorted = pairs.clone();
    sorted.sort_unstable();
    assert_eq!(
        pairs, sorted,
        "rows come back (loom_bucket, loom_offset)-ordered"
    );
}

/// Case 2 (the transition): flush the inline-only source, then inline-append more
/// rows. The delta is now the UNION of the file tier and the still-live inline
/// tail — the pre-fix path for the file leg, unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flushed_then_inline_source_unions_both_tiers() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let table = tref("s", "mv_out");

    land_inline(&pool, &catalog, &table, &[1, 2], &[10, 20]).await;
    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush")
        .expect("the inline rows were flushed to Parquet");
    land_inline(&pool, &catalog, &table, &[3, 4], &[30, 40]).await;

    let (_schema, batches) = mv_delta_scan(&cp, &catalog, &pool, &table, MV)
        .await
        .expect("file ∪ inline delta");
    assert_eq!(
        ids(&batches),
        vec![1, 2, 3, 4],
        "flushed rows (file tier) UNION the un-flushed tail (inline tier)"
    );
}

/// Case 3 (the empty delta): an inline-only source whose watermark has already
/// consumed every event. No files, nothing live above the watermark — an EMPTY
/// delta over the mirror-derived framed schema, never an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_only_source_with_consumed_watermark_reads_empty_delta() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let table = tref("s", "mv_out");

    land_inline(&pool, &catalog, &table, &[1, 2, 3], &[10, 20, 30]).await;
    let tid = tid_of(&pool, &table).await;

    let (_schema, first) = mv_delta_scan(&cp, &catalog, &pool, &table, MV)
        .await
        .expect("first delta");
    consume(&cp, tid, &first).await;

    let (schema, batches) = mv_delta_scan(&cp, &catalog, &pool, &table, MV)
        .await
        .expect("a fully-consumed inline-only source reads EMPTY, not an error");
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        0,
        "nothing at or beyond the watermark"
    );
    assert_eq!(
        names(&schema),
        vec![
            "id",
            "val",
            "loom_change_kind",
            "loom_bucket",
            "loom_offset"
        ],
        "an empty delta still carries the mirror-derived framed schema"
    );
}
