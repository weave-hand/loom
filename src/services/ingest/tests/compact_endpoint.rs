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
    COMPACT_JOB_KIND, ColumnSpec, ControlPlane, DatasetId, EventType, LineageEvent, RunId,
    StreamTables, TableRef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_compact::{CompactTriggerCfg, maybe_enqueue_compact};
use control_plane_postgres::iceberg_inline::set_has_shadow;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_mirror::{ensure_table, live_table_id, next_snapshot};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use e2e_support::{
    admin_session_token, app_state, compact_app, ipc_bytes, merged_app, post_dataset_q,
    sample_batch, session_token,
};
use http_body_util::BodyExt;
use ingest::http::AppState;
use loom_test_seed::local_sql_catalog;
use sqlx::PgPool;
use time::OffsetDateTime;
use tower::ServiceExt;

/// The small-file cutoff `e2e_support::app_state` puts on the `AppState` — the
/// fixture's Parquet files are a few hundred bytes, so all of them qualify.
/// Repeated here so the hand-driven auto-trigger call uses the same policy the
/// handler does.
const SMALL_FILE_BYTES: i64 = 1 << 20;

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

/// Land one small Parquet file against `table`, declaring it a log stream of
/// `buckets` buckets at GENESIS — `buckets` rides this, the table's FIRST land
/// call (see `land`'s `stream_buckets` doc: it declares on first write).
/// Declaring at genesis, before any data ever lands, is legal both before and
/// after the not-over-existing-data stream/CDC declare guard (#625). Mirrors
/// `postgres/tests/compact_trigger.rs::land_genesis_stream`.
async fn land_genesis_stream(pool: &PgPool, catalog: &SqlCatalog, table: &TableRef, buckets: i32) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![0i64]))])
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
        Some(buckets),
    )
    .await
    .expect("land");
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

/// Eager policy + the 202 contract: exactly 2 small files — below the
/// auto-trigger's default 8 — compacts on an explicit operator request, and the
/// enqueued job carries the same `{schema, name}` payload the auto-trigger builds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_post_enqueues_job_at_two_small_files() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, catalog, _wh, state) = harness(fx, &db).await;
    land_n_small(&pool, &catalog, &table("orders"), 2).await;

    let token = admin_session_token(&pg, "root").await;
    let (status, body) = post_compact(compact_app(state, pg.clone()), "wh", "orders", &token).await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body["job_id"].is_string(), "202 carries the job id: {body}");
    assert_eq!(available_jobs(&pool).await, 1);

    let job = pg
        .queue()
        .dequeue(&[COMPACT_JOB_KIND.to_string()], "test")
        .await
        .unwrap()
        .expect("a compact_table job was enqueued");
    assert_eq!(job.kind, COMPACT_JOB_KIND);
    assert_eq!(job.payload["schema"], "wh");
    assert_eq!(job.payload["name"], "orders");
}

/// Dedup: a second POST while the first job is still pending adds nothing and
/// says so honestly (200 `{job_id: null}`), instead of the old unconditional 202.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeat_post_dedups_to_one_job() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, catalog, _wh, state) = harness(fx, &db).await;
    land_n_small(&pool, &catalog, &table("orders"), 2).await;
    let token = admin_session_token(&pg, "root").await;

    let (s1, b1) = post_compact(
        compact_app(state.clone(), pg.clone()),
        "wh",
        "orders",
        &token,
    )
    .await;
    assert_eq!(s1, StatusCode::ACCEPTED);
    assert!(b1["job_id"].is_string());

    let (s2, b2) = post_compact(compact_app(state, pg.clone()), "wh", "orders", &token).await;
    assert_eq!(s2, StatusCode::OK, "suppressed: a job is already pending");
    assert!(b2["job_id"].is_null());

    assert_eq!(available_jobs(&pool).await, 1, "exactly one pending job");
}

/// Cross-producer dedup: an auto-trigger job already pending absorbs the
/// operator POST (same `(kind, payload)` → `pg_insert_if_absent`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_after_auto_trigger_adds_nothing() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, catalog, _wh, state) = harness(fx, &db).await;
    let t = table("orders");
    land_n_small(&pool, &catalog, &t, 2).await;

    // The auto-trigger's own entrypoint, invoked exactly as the catalog does.
    let mut conn = pool.acquire().await.expect("acquire");
    let auto = maybe_enqueue_compact(
        &mut conn,
        &t,
        &CompactTriggerCfg {
            small_file_bytes: SMALL_FILE_BYTES,
            min_small_files: 2,
        },
    )
    .await
    .expect("auto trigger");
    drop(conn);
    assert!(auto.is_some(), "auto-trigger enqueued");
    assert_eq!(available_jobs(&pool).await, 1);

    let token = admin_session_token(&pg, "root").await;
    let (status, body) = post_compact(compact_app(state, pg.clone()), "wh", "orders", &token).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["job_id"].is_null());
    assert_eq!(available_jobs(&pool).await, 1, "still exactly one");
}

/// Eligibility: a declared stream table is refused (its own consolidation owns
/// that data; compaction could resurrect tombstoned rows).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_stream_table_is_refused() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, catalog, _wh, state) = harness(fx, &db).await;
    let t = table("events");
    // Declare the table a stream at GENESIS — `buckets=4` rides the FIRST land,
    // before any data has landed — then keep landing to reach the same 2 files
    // the original test seeded.
    land_genesis_stream(&pool, &catalog, &t, 4).await;
    land_n_small(&pool, &catalog, &t, 1).await;

    let token = admin_session_token(&pg, "root").await;
    let (status, body) = post_compact(compact_app(state, pg.clone()), "wh", "events", &token).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["job_id"].is_null());
    assert_eq!(available_jobs(&pool).await, 0);
}

/// Eligibility: a CDC table's changelog table is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changelog_table_is_refused() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, catalog, _wh, state) = harness(fx, &db).await;
    let base = table("base");
    let changelog = table("base__changelog");

    // `base` must be declared CDC at GENESIS — before any data lands on it.
    // Mirrors `postgres/tests/compact_trigger.rs::changelog_table_skips`: an
    // explicit tx for `ensure_table` (its savepoint retry needs one) to get the
    // empty mirror row + tid, commit, THEN declare_cdc. `set_changelog_table_id`
    // UPDATEs an existing stream_table row, so this declare is required
    // regardless — `base` never needs any landed data of its own for this test.
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let base_tid = ensure_table(&mut tx, &base.schema, &base.name, at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    pg.declare_cdc(base_tid, 1, "id", control_plane_core::MergeEngine::LastRow)
        .await
        .expect("declare_cdc");

    // `changelog` accrues the small files; `base`'s stream_table row (declared
    // above) points its changelog_table_id at `changelog`'s tid.
    land_n_small(&pool, &catalog, &changelog, 2).await;
    let mut conn = pool.acquire().await.expect("acquire");
    let clog_tid = live_table_id(&mut conn, &changelog.schema, &changelog.name)
        .await
        .expect("live_table_id")
        .expect("changelog has a live row");
    drop(conn);
    pg.set_changelog_table_id(base_tid, clog_tid)
        .await
        .expect("set_changelog_table_id");

    let token = admin_session_token(&pg, "root").await;
    let (status, body) = post_compact(
        compact_app(state, pg.clone()),
        "wh",
        "base__changelog",
        &token,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["job_id"].is_null());
    assert_eq!(available_jobs(&pool).await, 0);
}

/// Eligibility: a shadow-flagged table is refused (COW merge-on-read takes the
/// highest `begin_snapshot`; re-projecting files could resurrect tombstones).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shadow_flagged_table_is_refused() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, catalog, _wh, state) = harness(fx, &db).await;
    let t = table("orders");
    land_n_small(&pool, &catalog, &t, 2).await;

    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, &t.schema, &t.name)
        .await
        .expect("live_table_id")
        .expect("table has a live row");
    set_has_shadow(&mut conn, tid)
        .await
        .expect("set_has_shadow");
    drop(conn);

    let token = admin_session_token(&pg, "root").await;
    let (status, body) = post_compact(compact_app(state, pg.clone()), "wh", "orders", &token).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["job_id"].is_null());
    assert_eq!(available_jobs(&pool).await, 0);
}

/// The composition pin: the app as `serve.rs` builds it — `protect(router(state),
/// auth)` MERGED with `compact_routes(state, auth)`. Driving the two sub-routers in
/// isolation cannot see the merge, so this is where an admin gate accidentally
/// bleeding onto the data plane (or falling off the operator route) shows up. One
/// authenticated NON-admin token: `POST /datasets/{schema}/{table}` lands (the data
/// plane is authn-only), the same token on `POST /tables/../compact` is 403, and an
/// admin token on that same merged app gets the 202.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merged_app_gates_only_the_operator_route() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, catalog, _wh, state) = harness(fx, &db).await;
    land_n_small(&pool, &catalog, &table("orders"), 2).await;

    let app = merged_app(state, pg.clone());
    let alice = session_token(&pg, "alice").await; // authenticated, NOT admin

    // The data plane is NOT admin-gated: a plain authenticated token lands.
    let (status, body) = post_dataset_q(
        app.clone(),
        "wh",
        "landed",
        "",
        &alice,
        ipc_bytes(&sample_batch()),
    )
    .await;
    assert!(
        status.is_success(),
        "the merged app must not admin-gate /datasets, got {status} ({body})"
    );

    // The operator route IS admin-gated — same app, same non-admin token.
    let (status, _body) = post_compact(app.clone(), "wh", "orders", &alice).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(available_jobs(&pool).await, 0, "no job from a denied POST");

    // ... and an admin still gets through the merged app.
    let root = admin_session_token(&pg, "root").await;
    let (status, body) = post_compact(app, "wh", "orders", &root).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body["job_id"].is_string(), "202 carries the job id: {body}");
    assert_eq!(available_jobs(&pool).await, 1);
}

/// 404: a never-written table. (The pre-fix endpoint happily enqueued here.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_table_is_404() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _catalog, _wh, state) = harness(fx, &db).await;

    let token = admin_session_token(&pg, "root").await;
    let (status, _body) = post_compact(compact_app(state, pg.clone()), "wh", "ghost", &token).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(available_jobs(&pool).await, 0);
}
