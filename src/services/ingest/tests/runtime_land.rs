//! Real production-wiring land: build the ingest AppState via the service_runtime
//! helpers against a real (fixture) Postgres + bootstrapped DuckLake catalog, POST an
//! Arrow IPC stream through the router, and read the snapshot back from the catalog.
//! Exercises build_pool (over a unix socket), control_plane, and local_store.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{ControlPlane, TableRef};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use http_body_util::BodyExt;
use ingest::http::{AppState, router};
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
    let writer = DuckLakeWriter::new(fixture.socket_path(), &db);
    writer.bootstrap().await;

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
        pool,
        Duration::from_millis(300),
    ));
    let store = Arc::new(service_runtime::local_store(writer.data_path()).expect("store"));

    let res = router(AppState {
        cp: cp.clone(),
        store,
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
    let snap = cp
        .catalog()
        .current_snapshot(&table)
        .await
        .expect("current snapshot after landing");
    assert_eq!(
        snap.id.0, snapshot_id,
        "returned id matches the real catalog"
    );
}
