//! End-to-end Iceberg landing through the real HTTP router: POST an Arrow IPC
//! stream to an `IcebergMaterializer`-backed `AppState`, assert 200 + snapshot id,
//! and that the loom mirror + lineage reflect the landed rows. Proves the thin
//! forwarder wiring (raw IPC body + byte limit) end to end.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{Catalog, Lineage, PageReq, RunId, TableRef};
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
use tower::ServiceExt;

/// Encode a 3-row `id: Int64` batch as an Arrow IPC stream (arrow-58, the ingest
/// wire format).
fn ipc_bytes() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
    )
    .unwrap();
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_iceberg_land_inlines_through_http() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(&db).await;

    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), fx.pg_dsn(&db));
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{}", wh.path().display()),
    );
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog");

    let state = AppState {
        materializer: Arc::new(IcebergMaterializer {
            catalog: Arc::new(catalog),
            pool: pool.clone(),
            // Generous limit: a 3-row request stays inline.
            inline_byte_limit: 16 * 1024 * 1024,
            flush_byte_threshold: 1, // tiny: any inline landing crosses it
        }),
    };

    let run = uuid::Uuid::new_v4();
    let res = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/datasets/wh/customer")
                .header("X-Loom-Run-Id", run.to_string())
                .body(Body::from(ipc_bytes()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK, "land succeeds");
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let snap = json["snapshot_id"].as_i64().expect("snapshot_id");
    assert!(snap > 0);
    assert_eq!(json["dataset"], "wh.customer");

    // The inline rows are live in the mirror at the returned snapshot.
    let table = TableRef {
        schema: "wh".into(),
        name: "customer".into(),
    };
    let cur = IcebergCatalog::new(pool.clone())
        .current_snapshot(&table)
        .await
        .expect("current");
    assert_eq!(cur.id.0, snap, "returned id is the mirror current snapshot");

    // Lineage was emitted for the run, naming the landed dataset.
    let page = cp
        .events_for(&RunId(run), PageReq::unbounded())
        .await
        .expect("events");
    assert_eq!(page.items.len(), 1, "one lineage event");
    assert_eq!(page.items[0].outputs[0].name, "wh.customer");

    // The inline landing crossed the (tiny) flush threshold: exactly one job queued.
    let n: i64 = sqlx::query_scalar("select count(*) from queue.jobs where kind = 'flush_table'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        n, 1,
        "inline landing past the flush threshold enqueues one job"
    );
}
