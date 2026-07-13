//! POST /tables/{schema}/{table}/compact — the guarded operator surface
//! (iss-compact-endpoint-unguarded): admin-only, guarded (stream/changelog/
//! shadow refused), deduped through `maybe_enqueue_compact`, and honest about
//! its result (404 / 202 {job_id} / 200 {job_id: null}).
//! loom_fixture_test (Postgres).

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use axum::Router;
use axum::body::Body;
use axum::http::header::AUTHORIZATION;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    COMPACT_JOB_KIND, ColumnSpec, DatasetId, EventType, LineageEvent, RunId, TableRef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use e2e_support::{app_state, compact_app, session_token};
use http_body_util::BodyExt;
use ingest::http::AppState;
use loom_test_seed::local_sql_catalog;
use sqlx::PgPool;
use time::OffsetDateTime;
use tower::ServiceExt;

/// The fixture db + a temp warehouse: the `AppState` the router runs on, the pool,
/// the concrete control plane (auth/ACL/stream seeding), and a real `SqlCatalog`
/// to land small files through (`e2e_support::app_state` builds the state's own
/// materializer but does not hand back a catalog).
async fn harness(
    fx: &PgFixture,
    db: &str,
) -> (
    Arc<PgControlPlane>,
    PgPool,
    SqlCatalog,
    tempfile::TempDir,
    AppState,
) {
    let (pg, pool, wh, state) = app_state(fx, db).await;
    let catalog = local_sql_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    (pg, pool, catalog, wh, state)
}

/// POST the compact route with a bearer token; return (status, body JSON).
async fn post_compact(
    app: Router,
    schema: &str,
    table: &str,
    token: &str,
) -> (StatusCode, serde_json::Value) {
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/tables/{schema}/{table}/compact"))
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

fn lineage(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "compact-endpoint-test" }),
    }
}

/// Land `n` one-row Parquet files (`inline_byte_limit: 0` forces the Parquet
/// branch, so each call emits exactly one small file; `flush_byte_threshold:
/// i64::MAX` keeps a flush job out of the queue counts). Mirrors
/// `postgres/tests/compact_trigger.rs::land_n_small`.
async fn land_n_small(pool: &PgPool, catalog: &SqlCatalog, table: &TableRef, n: i64) {
    for id in 0..n {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![id]))])
                .expect("batch");
        land(
            pool,
            catalog,
            table,
            &columns(),
            schema,
            vec![batch],
            InlineLimits {
                inline_byte_limit: 0,
                flush_byte_threshold: i64::MAX,
            },
            lineage(table),
            None,
        )
        .await
        .expect("land");
    }
}

async fn available_jobs(pool: &PgPool) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "select count(*) from queue.jobs where kind = $1 and state = 'available'",
    )
    .bind(COMPACT_JOB_KIND)
    .fetch_one(pool)
    .await
    .unwrap()
}

fn table(name: &str) -> TableRef {
    TableRef {
        schema: "wh".into(),
        name: name.into(),
    }
}

/// The gate: an authenticated but non-admin subject cannot enqueue maintenance
/// work (the defect this issue closes — any authenticated caller could).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_admin_subject_is_forbidden() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, catalog, _wh, state) = harness(fx, &db).await;
    land_n_small(&pool, &catalog, &table("orders"), 2).await;

    let token = session_token(&pg, "alice").await; // authenticated, NOT admin
    let (status, _body) =
        post_compact(compact_app(state, pg.clone()), "wh", "orders", &token).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(
        available_jobs(&pool).await,
        0,
        "a denied POST enqueues nothing"
    );
}

/// Layer order: `require_auth` runs OUTSIDE `require_admin`, so a caller with no
/// bearer is 401 (not the admin gate's 403).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_bearer_is_unauthorized() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, _pool, _catalog, _wh, state) = harness(fx, &db).await;

    let res = compact_app(state, pg)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/wh/orders/compact")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}
