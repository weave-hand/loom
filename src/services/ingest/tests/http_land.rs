//! Hermetic landing-endpoint smoke: POST an Arrow IPC stream, assert the land
//! happened end-to-end through the Iceberg materializer (fixture Postgres + a temp
//! file warehouse; tower oneshot, no socket). The model-gate / error paths reject
//! before materializing, so they exercise the same wiring without landing.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{Catalog, ControlPlane, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use http_body_util::BodyExt;
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
async fn app_state(
    fx: &PgFixture,
    db: &str,
) -> (Arc<dyn ControlPlane>, PgPool, tempfile::TempDir, AppState) {
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
    let cp: Arc<dyn ControlPlane> =
        Arc::new(service_runtime::control_plane(pool.clone(), Duration::from_millis(300)));
    let state = AppState {
        materializer: Arc::new(IcebergMaterializer {
            catalog: Arc::new(catalog),
            pool: pool.clone(),
            inline_byte_limit: 16 * 1024 * 1024,
            flush_byte_threshold: 64 * 1024 * 1024,
        }),
        cp: cp.clone(),
    };
    (cp, pool, wh, state)
}

#[tokio::test(flavor = "multi_thread")]
async fn unmodeled_land_succeeds_end_to_end() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (_cp, pool, _wh, state) = app_state(&fx, &db).await;
    let res = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/datasets/main/customer")
                .body(Body::from(ipc_bytes(&sample_batch())))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["dataset"], "main.customer");
    let snapshot_id = json["snapshot_id"]
        .as_i64()
        .expect("snapshot_id is an integer");

    // Prove the land actually happened: the mirror catalog now has a current
    // snapshot for the table at the returned id.
    let table = TableRef {
        schema: "main".into(),
        name: "customer".into(),
    };
    let snap = IcebergCatalog::new(pool.clone())
        .current_snapshot(&table)
        .await
        .expect("table has a current snapshot after landing");
    assert_eq!(
        snap.id.0, snapshot_id,
        "returned snapshot id matches the catalog"
    );
}

fn model_header(json: &str) -> (axum::http::HeaderName, axum::http::HeaderValue) {
    (
        axum::http::HeaderName::from_static("x-loom-model"),
        axum::http::HeaderValue::from_str(json).unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn modeled_land_succeeds() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (_cp, _pool, _wh, state) = app_state(&fx, &db).await;
    let model = r#"{"columns":[{"name":"id","ty":"long","required":true},{"name":"name","ty":"string","required":false}]}"#;
    let (hn, hv) = model_header(model);
    let res = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/datasets/main/customer")
                .header(hn, hv)
                .body(Body::from(ipc_bytes(&sample_batch())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn nonconforming_model_is_422_with_violations() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (_cp, pool, _wh, state) = app_state(&fx, &db).await;
    // Requires a column the batch does not have.
    let model = r#"{"columns":[{"name":"missing","ty":"long","required":true}]}"#;
    let (hn, hv) = model_header(model);
    let res = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/datasets/main/customer")
                .header(hn, hv)
                .body(Body::from(ipc_bytes(&sample_batch())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["violations"][0]["column"], "missing");
    assert_eq!(json["violations"][0]["reason"], "missing_required");

    // Nothing was written.
    let table = TableRef {
        schema: "main".into(),
        name: "customer".into(),
    };
    assert!(
        IcebergCatalog::new(pool.clone())
            .current_snapshot(&table)
            .await
            .is_err(),
        "a rejected land writes no catalog rows"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn garbage_body_is_400() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (_cp, _pool, _wh, state) = app_state(&fx, &db).await;
    let res = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/datasets/main/customer")
                .body(Body::from(b"not arrow ipc".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_model_header_is_400() {
    let fx = PgFixture::start();
    let (_seed, db) = fx.fresh_db().await;
    let (_cp, _pool, _wh, state) = app_state(&fx, &db).await;
    let (hn, hv) = model_header("not json");
    let res = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/datasets/main/customer")
                .header(hn, hv)
                .body(Body::from(ipc_bytes(&sample_batch())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}
