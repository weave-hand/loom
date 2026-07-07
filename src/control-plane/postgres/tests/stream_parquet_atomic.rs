//! The direct large-write Parquet path reaches parity with the inline path for
//! stream tables: atomic Conflict/Validation reconcile + gapless per-bucket
//! offsets stamped into the written Parquet (offset allocation commits iff the
//! snapshot commits). A write above `inline_byte_limit` routes to `land_parquet`.

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlaneError, DatasetId, EventType, LineageEvent, PageReq, Result,
    RunId, SnapshotId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use loom_test_seed::local_sql_catalog;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqlx::PgPool;

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// A schema + single batch of `rows` rows with one `id: long` column.
fn body(rows: i64) -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .expect("batch");
    (schema, vec![batch])
}

fn lineage(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

/// `inline_byte_limit: 0` forces every write onto the direct Parquet path.
fn parquet_limits() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: 0,
        flush_byte_threshold: i64::MAX,
    }
}

/// Direct large write with an explicit stream mode; returns the landing result so
/// callers can assert on the error variant.
async fn try_land_large_stream(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    buckets: i32,
    rows: i64,
) -> Result<SnapshotId> {
    let (schema, batches) = body(rows);
    land(
        pool,
        catalog,
        table,
        &columns(),
        schema,
        batches,
        parquet_limits(),
        lineage(table),
        Some(buckets),
    )
    .await
}

async fn land_large_stream(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    buckets: i32,
    rows: i64,
) -> SnapshotId {
    try_land_large_stream(pool, catalog, table, buckets, rows)
        .await
        .expect("land large stream")
}

/// Direct large write with NO stream mode → a batch table.
async fn land_large_batch(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    rows: i64,
) -> SnapshotId {
    let (schema, batches) = body(rows);
    land(
        pool,
        catalog,
        table,
        &columns(),
        schema,
        batches,
        parquet_limits(),
        lineage(table),
        None,
    )
    .await
    .expect("land large batch")
}

/// Read every live Parquet file's `(loom_bucket, loom_offset)` rows at `at`, via
/// the real parquet reader — proving the framing is IN the written files. Also
/// asserts the `loom_change_kind` framing column is present and always `"+I"`.
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
            let kind_idx = b
                .schema()
                .index_of("loom_change_kind")
                .expect("loom_change_kind col");
            let bucket_idx = b.schema().index_of("loom_bucket").expect("loom_bucket col");
            let offset_idx = b.schema().index_of("loom_offset").expect("loom_offset col");
            let kcol = b
                .column(kind_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("loom_change_kind is Utf8");
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
                assert_eq!(kcol.value(i), "+I", "insert change kind");
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
async fn large_write_stamps_gapless_framing_in_parquet() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let table = TableRef {
        schema: "s".into(),
        name: "biglog".into(),
    };
    let snap = land_large_stream(&pool, &catalog, &table, 2, 5_000).await;

    let ice = IcebergCatalog::new(pool.clone());
    let cur = ice.current_snapshot(&table).await.expect("current");
    assert_eq!(cur.id, snap, "returned id is the mirror current snapshot");

    let framing = parquet_framing(&ice, &table, snap).await;
    assert_gapless_per_bucket(&framing);
    assert!(framing.iter().all(|(_, o)| *o >= 0), "offsets non-negative");
    assert_eq!(framing.len(), 5_000, "every row framed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_write_rejects_batch_to_stream_conversion() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let table = TableRef {
        schema: "s".into(),
        name: "wasbatch".into(),
    };
    land_large_batch(&pool, &catalog, &table, 5_000).await;
    let err = try_land_large_stream(&pool, &catalog, &table, 2, 5_000).await;
    assert!(
        matches!(err, Err(ControlPlaneError::Validation(_))),
        "convert rejected: {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_write_rejects_bucket_mismatch() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let table = TableRef {
        schema: "s".into(),
        name: "mism".into(),
    };
    land_large_stream(&pool, &catalog, &table, 2, 5_000).await;
    let err = try_land_large_stream(&pool, &catalog, &table, 3, 5_000).await;
    assert!(
        matches!(err, Err(ControlPlaneError::Conflict(_))),
        "mismatch rejected: {err:?}"
    );
}
