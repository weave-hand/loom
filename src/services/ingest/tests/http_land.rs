//! Hermetic landing-endpoint smoke: POST an Arrow IPC stream, assert the land
//! happened end-to-end through the Iceberg materializer (fixture Postgres + a temp
//! file warehouse; tower oneshot, no socket). The model-gate / error paths reject
//! before materializing, so they exercise the same wiring without landing.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{Catalog, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{ingest_router, ipc_bytes, post_ipc, sample_batch};
use http_body_util::BodyExt;
use tower::ServiceExt;

#[tokio::test(flavor = "multi_thread")]
async fn unmodeled_land_succeeds_end_to_end() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (app, _pg, pool, _wh) = ingest_router(fx, &db).await;
    let res = post_ipc(app, "/datasets/main/customer", ipc_bytes(&sample_batch())).await;

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
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (app, _pg, _pool, _wh) = ingest_router(fx, &db).await;
    let model = r#"{"columns":[{"name":"id","ty":"long","required":true},{"name":"name","ty":"string","required":false}]}"#;
    let (hn, hv) = model_header(model);
    let res = app
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
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (app, _pg, pool, _wh) = ingest_router(fx, &db).await;
    // Requires a column the batch does not have.
    let model = r#"{"columns":[{"name":"missing","ty":"long","required":true}]}"#;
    let (hn, hv) = model_header(model);
    let res = app
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
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (app, _pg, _pool, _wh) = ingest_router(fx, &db).await;
    let res = post_ipc(app, "/datasets/main/customer", b"not arrow ipc".to_vec()).await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_model_header_is_400() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (app, _pg, _pool, _wh) = ingest_router(fx, &db).await;
    let (hn, hv) = model_header("not json");
    let res = app
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

#[tokio::test(flavor = "multi_thread")]
async fn bad_run_id_is_400() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (app, _pg, _pool, _wh) = ingest_router(fx, &db).await;
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/datasets/main/customer")
                .header("x-loom-run-id", "not-a-uuid")
                .body(Body::from(ipc_bytes(&sample_batch())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}
