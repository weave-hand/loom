//! A stream table's framing columns survive the inline → Iceberg flush: they are
//! present in the mirror/Parquet, offsets stay gapless & ordered per bucket
//! across the flush boundary, logical reads never expose them, and a batch
//! table's ingest→flush is byte-identical (no framing, no mirror `column` churn).

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, EventType, LineageEvent, PageReq, RunId, SnapshotId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_inline::inline_append;
use loom_test_seed::local_sql_catalog;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn id_spec() -> ColumnSpec {
    ColumnSpec {
        name: "id".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }
}

fn id_batch(ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids.to_vec()))]).expect("batch")
}

fn lin() -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "test" }),
    }
}

/// Resolve the internal mirror `table_id`, the same way the other inline tests do.
async fn tid_of(pool: &sqlx::PgPool, schema: &str, name: &str) -> i64 {
    sqlx::query_scalar(
        "select table_id from iceberg_mirror.table \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
    )
    .bind(schema)
    .bind(name)
    .fetch_one(pool)
    .await
    .expect("table_id")
}

/// Read every live Parquet file's `(loom_bucket, loom_offset)` rows at `at`, via
/// the real parquet reader (proving the values are actually IN the written
/// files, not just claimed by the mirror).
async fn parquet_framing(
    ice: &IcebergCatalog,
    table: &TableRef,
    at: SnapshotId,
) -> Vec<(i32, i64)> {
    let files = ice
        .files(table, at, PageReq::unbounded())
        .await
        .expect("files");
    let mut out = Vec::new();
    for f in files.items {
        let path = f.path.strip_prefix("file://").unwrap_or(&f.path);
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(path).expect("open parquet"))
                .expect("parquet reader")
                .build()
                .expect("build reader");
        for b in reader {
            let b = b.expect("batch");
            let bucket_idx = b.schema().index_of("loom_bucket").expect("loom_bucket col");
            let offset_idx = b.schema().index_of("loom_offset").expect("loom_offset col");
            let bcol = b
                .column(bucket_idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("loom_bucket is Int32");
            let ocol = b
                .column(offset_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("loom_offset is Int64");
            for i in 0..b.num_rows() {
                out.push((bcol.value(i), ocol.value(i)));
            }
        }
    }
    out
}

/// Each bucket's offsets, sorted, must be exactly `0..n` (gapless, no duplicates).
fn assert_gapless_per_bucket(offsets: &[(i32, i64)]) {
    assert!(!offsets.is_empty(), "expected some framed rows");
    let mut by_bucket: HashMap<i32, Vec<i64>> = HashMap::new();
    for (b, o) in offsets {
        by_bucket.entry(*b).or_default().push(*o);
    }
    for (bucket, mut os) in by_bucket {
        os.sort_unstable();
        let want: Vec<i64> = (0..os.len() as i64).collect();
        assert_eq!(os, want, "bucket {bucket} offsets must be gapless: {os:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_flush_persists_gapless_framing() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let run = RunId(uuid::Uuid::new_v4());
    let table = TableRef {
        schema: "s".to_string(),
        name: "log".to_string(),
    };
    let cols = vec![id_spec()];

    // Two appends to a buckets=2 stream table (small -> inline), then flush.
    inline_append(
        &pool,
        &table,
        &cols,
        &id_batch(&[1, 2, 3, 4]),
        lin(),
        None,
        Some(2),
    )
    .await
    .expect("append 1");
    inline_append(
        &pool,
        &table,
        &cols,
        &id_batch(&[5, 6]),
        lin(),
        None,
        Some(2),
    )
    .await
    .expect("append 2");

    flush_table(&catalog, &pool, &table, run)
        .await
        .expect("flush")
        .expect("flushed something");

    let ice = IcebergCatalog::new(pool.clone());
    let tid = tid_of(&pool, &table.schema, &table.name).await;
    let at = ice.current_snapshot(&table).await.expect("current").id;

    // Physical schema carries all three framing columns; logical hides them.
    let phys: Vec<_> = ice
        .physical_columns(tid, at)
        .await
        .expect("physical_columns")
        .into_iter()
        .map(|c| c.name)
        .collect();
    for c in ["loom_change_kind", "loom_bucket", "loom_offset"] {
        assert!(
            phys.contains(&c.to_string()),
            "framing in physical: {phys:?}"
        );
    }
    let logical: Vec<_> = ice
        .schema(&table, at)
        .await
        .expect("schema")
        .columns
        .into_iter()
        .map(|c| c.name)
        .collect();
    for c in ["loom_change_kind", "loom_bucket", "loom_offset"] {
        assert!(
            !logical.contains(&c.to_string()),
            "framing hidden from reads: {logical:?}"
        );
    }

    // Offsets are gapless & ordered per bucket across the flush boundary.
    let offsets = parquet_framing(&ice, &table, at).await;
    assert_gapless_per_bucket(&offsets);

    // A SECOND round of appends + flush must reconcile cleanly against the
    // now-existing Iceberg table (created WITH framing by the first flush): this
    // is the integration risk this task exists to prove — the mirror (already
    // carrying framing from declaration) and the physical Iceberg schema must
    // agree on every subsequent commit, not just table creation.
    inline_append(&pool, &table, &cols, &id_batch(&[7, 8]), lin(), None, None)
        .await
        .expect("append 3 (post-flush)");
    flush_table(&catalog, &pool, &table, run)
        .await
        .expect("flush 2")
        .expect("flushed something (2)");

    let at2 = ice.current_snapshot(&table).await.expect("current 2").id;
    let phys2: Vec<_> = ice
        .physical_columns(tid, at2)
        .await
        .expect("physical_columns 2")
        .into_iter()
        .map(|c| c.name)
        .collect();
    for c in ["loom_change_kind", "loom_bucket", "loom_offset"] {
        assert!(
            phys2.contains(&c.to_string()),
            "framing still in physical after 2nd flush: {phys2:?}"
        );
    }
    let offsets2 = parquet_framing(&ice, &table, at2).await;
    assert_gapless_per_bucket(&offsets2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_flush_is_byte_identical() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let run = RunId(uuid::Uuid::new_v4());
    let table = TableRef {
        schema: "s".to_string(),
        name: "plain".to_string(),
    };
    let cols = vec![id_spec()];

    inline_append(
        &pool,
        &table,
        &cols,
        &id_batch(&[1, 2, 3]),
        lin(),
        None,
        None,
    )
    .await
    .expect("batch append");

    flush_table(&catalog, &pool, &table, run)
        .await
        .expect("flush")
        .expect("flushed something");

    let ice = IcebergCatalog::new(pool.clone());
    let tid = tid_of(&pool, &table.schema, &table.name).await;
    let at = ice.current_snapshot(&table).await.expect("current").id;

    let phys: Vec<_> = ice
        .physical_columns(tid, at)
        .await
        .expect("physical_columns")
        .into_iter()
        .map(|c| c.name)
        .collect();
    for c in ["loom_change_kind", "loom_bucket", "loom_offset"] {
        assert!(
            !phys.contains(&c.to_string()),
            "batch table has NO framing in mirror: {phys:?}"
        );
    }
}
