//! Real production-wiring land: build the ingest AppState via the service_runtime
//! helpers against a real (fixture) Postgres, POST an Arrow IPC stream through the
//! router into an `IcebergMaterializer`, and read the snapshot back from the
//! mirror-backed catalog. Exercises build_pool (over a unix socket) + control_plane.

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
use service_runtime::DbConfig;
use tower::ServiceExt;

fn ipc_bytes() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

#[tokio::test(flavor = "multi_thread")]
async fn lands_through_real_runtime_wiring() {
    let fixture = PgFixture::start();
    // Creates + migrates a fresh db; we rebuild our own pool through the runtime below.
    let (_seed, db) = fixture.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");

    // Build the real AppState via service_runtime — a unix-socket DbConfig (host = the
    // fixture's socket dir), so build_pool exercises the production connect path.
    let db_cfg = DbConfig {
        host: fixture.socket_path().to_string_lossy().into_owned(),
        port: 5432, // ignored for a socket host
        user: "postgres".into(),
        password: String::new(),
        dbname: db.clone(),
    };
    let pool = service_runtime::build_pool(&db_cfg)
        .await
        .expect("build pool");
    let cp: Arc<dyn ControlPlane> = Arc::new(service_runtime::control_plane(
        pool.clone(),
        Duration::from_millis(300),
    ));

    // The vendored Iceberg catalog over the same Postgres + a temp file warehouse.
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), fixture.pg_dsn(&db));
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{}", wh.path().display()),
    );
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog");

    let res = router(AppState {
        materializer: Arc::new(IcebergMaterializer {
            catalog: Arc::new(catalog),
            pool: pool.clone(),
            inline_byte_limit: 16 * 1024 * 1024,
            flush_byte_threshold: 64 * 1024 * 1024,
        }),
        cp: cp.clone(),
    })
    .oneshot(
        Request::builder()
            .method("POST")
            .uri("/datasets/main/customer")
            .body(Body::from(ipc_bytes()))
            .unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let snapshot_id = json["snapshot_id"].as_i64().expect("snapshot_id");

    let table = TableRef {
        schema: "main".into(),
        name: "customer".into(),
    };
    let snap = IcebergCatalog::new(pool.clone())
        .current_snapshot(&table)
        .await
        .expect("current snapshot after landing");
    assert_eq!(
        snap.id.0, snapshot_id,
        "returned id matches the real catalog"
    );
}
