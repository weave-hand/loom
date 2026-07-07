//! `?mode=stream&buckets=N` on `POST /datasets/{schema}/{table}`: the first write
//! declares a log table (row in `stream.stream_table`, `bucket_count = N`); a later
//! bucket-count mismatch or a batch->stream conversion attempt is rejected with 400;
//! a plain (no `mode`) write stays byte-identical to today (batch table, no
//! `stream.stream_table` row). Hermetic Postgres fixture + a temp file warehouse;
//! tower oneshot, no socket. Mirrors `tests/http_land.rs`'s harness.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::ControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use ingest::http::{AppState, router};
use ingest::landing::IcebergMaterializer;
use sqlx::PgPool;
use tower::ServiceExt;

/// A 2-row batch: id: Int64 (required), name: Utf8 (nullable).
fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap()
}

/// Encode a batch as an Arrow IPC *stream* (schema + batch messages).
fn ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

/// Build an Iceberg-backed `AppState` over the fixture db + a temp file warehouse.
/// The `TempDir` is returned so the caller keeps the warehouse alive for the test.
async fn app_state(fx: &PgFixture, db: &str) -> (PgPool, tempfile::TempDir, AppState) {
    let pool = fx.pool_for(db).await;
    let wh = tempfile::tempdir().unwrap();
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), fx.pg_dsn(db));
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{}", wh.path().display()),
    );
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog");
    let cp: Arc<dyn ControlPlane> = Arc::new(service_runtime::control_plane(
        pool.clone(),
        Duration::from_millis(300),
    ));
    let state = AppState {
        materializer: Arc::new(IcebergMaterializer {
            catalog: Arc::new(catalog),
            pool: pool.clone(),
            inline_byte_limit: 16 * 1024 * 1024,
            flush_byte_threshold: 64 * 1024 * 1024,
        }),
        cp,
    };
    (pool, wh, state)
}

async fn post(state: AppState, uri: &str) -> axum::http::Response<Body> {
    router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .body(Body::from(ipc_bytes(&sample_batch())))
                .unwrap(),
        )
        .await
        .unwrap()
}

/// The declared bucket count for `schema.table`, or `None` if it has no
/// `stream.stream_table` row (i.e. it is a batch table, or does not exist).
async fn stream_bucket_count(pool: &PgPool, schema: &str, table: &str) -> Option<i32> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select st.bucket_count from stream.stream_table st \
         join iceberg_mirror.table t on t.table_id = st.table_id \
         where t.table_namespace = '{schema}' and t.table_name = '{table}' \
         and t.end_snapshot is null"
    )))
    .fetch_optional(pool)
    .await
    .expect("stream_table lookup")
}

#[tokio::test(flavor = "multi_thread")]
async fn first_write_declares_stream_with_bucket_count() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pool, _wh, state) = app_state(fx, &db).await;

    let res = post(state, "/datasets/main/events?mode=stream&buckets=2").await;
    assert_eq!(res.status(), StatusCode::OK);

    let bucket_count = stream_bucket_count(&pool, "main", "events").await;
    assert_eq!(
        bucket_count,
        Some(2),
        "the first write declares the log table with the requested bucket count"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn bucket_count_mismatch_on_second_write_is_400() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pool, _wh, state) = app_state(fx, &db).await;

    let res = post(state.clone(), "/datasets/main/events?mode=stream&buckets=2").await;
    assert_eq!(res.status(), StatusCode::OK);

    let res = post(state, "/datasets/main/events?mode=stream&buckets=3").await;
    assert_eq!(
        res.status(),
        StatusCode::BAD_REQUEST,
        "a conflicting bucket count on an already-declared stream table is rejected"
    );

    // The declared bucket count is unchanged by the rejected write.
    let bucket_count = stream_bucket_count(&pool, "main", "events").await;
    assert_eq!(bucket_count, Some(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_mode_on_existing_batch_table_is_400() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pool, _wh, state) = app_state(fx, &db).await;

    // Land as a plain batch table first.
    let res = post(state.clone(), "/datasets/main/customer").await;
    assert_eq!(res.status(), StatusCode::OK);

    let res = post(state, "/datasets/main/customer?mode=stream&buckets=2").await;
    assert_eq!(
        res.status(),
        StatusCode::BAD_REQUEST,
        "converting an existing batch table to a stream table is rejected"
    );

    let bucket_count = stream_bucket_count(&pool, "main", "customer").await;
    assert_eq!(
        bucket_count, None,
        "the rejected conversion attempt leaves the table as batch"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn plain_write_stays_batch_with_no_stream_table_row() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pool, _wh, state) = app_state(fx, &db).await;

    let res = post(state, "/datasets/main/customer").await;
    assert_eq!(res.status(), StatusCode::OK);

    let bucket_count = stream_bucket_count(&pool, "main", "customer").await;
    assert_eq!(
        bucket_count, None,
        "a plain write (no mode) never creates a stream.stream_table row"
    );
}
