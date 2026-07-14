//! A micro-batch MV cannot source a declared CDC table: `mv_delta_scan` reads LOG
//! sources only (`engine-serving/src/mv_delta.rs` — "cdc sources are deferred"), so such
//! an MV could never run, never advance its watermark, and would pin its source's MV
//! floor at 0 forever — permanently declining that table's consolidate fold
//! (`#iss-end-cap-ignores-mv-floor`). Refuse the configuration instead.
//!
//! The guard is SYMMETRIC, and both halves are load-bearing:
//!
//! * registration — `define_transform` refuses a micro-batch body whose source is
//!   already a declared CDC table;
//! * declaration — `reconcile_stream_mode` refuses a CDC declaration on a table a
//!   micro-batch MV already sources.
//!
//! A registration-only guard is trivially defeated by ordering, through a path that must
//! stay legitimate: an MV may be registered over a source that does not exist yet (it
//! becomes a log stream on its first `?mode=stream` write — `tests/mv_floor.rs`'s
//! `registered_but_unrun_mv_floors_at_zero` depends on exactly that), and the source can
//! then be written `?mode=cdc`. Lift both when `fut-mv-cdc-source` lands.
//!
//! loom_fixture_test (Postgres).

use control_plane_core::{
    ControlPlane, ControlPlaneError, MergeEngine, SnapshotId, StreamTables, TableRef,
    TransformBody, TransformDef, TransformName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{CdcDecl, InlineLimits, land, land_cdc};
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use end_cap_seed::{batch, columns, lineage, tref};
use loom_test_seed::local_sql_catalog;

/// Register a micro-batch MV named `mv_a` over `source`.
async fn define_mv(cp: &PgControlPlane, source: &TableRef) -> Result<(), ControlPlaneError> {
    cp.transforms()
        .define_transform(TransformDef {
            name: TransformName("mv_a".into()),
            body: TransformBody::MicroBatch {
                source: source.clone(),
                output: tref("s", "out_a"),
                buckets: 1,
                sql: "select id from src".into(),
            },
            schedule: None,
            on_input_commit: false,
        })
        .await
}

/// Ensure `table` exists in the mirror and return its live table id.
async fn ensure(pool: &sqlx::PgPool, table: &TableRef) -> i64 {
    let mut tx = pool.begin().await.expect("tx");
    let at = next_snapshot(&mut tx, None).await.expect("snapshot");
    let tid = ensure_table(&mut tx, &table.schema, &table.name, at)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    tid
}

/// Declare `table` CDC through the PRODUCTION declaration path — `land_cdc` is the only
/// caller that reaches `reconcile_stream_mode` with a `StreamDecl::Cdc`. Deliberately NOT
/// the raw `StreamTables::declare_cdc` trait method: that takes a bare `table_id` (no
/// `TableRef` to resolve readers against), stays unguarded on purpose as the test/admin
/// escape hatch, and using it here would make the test vacuous.
async fn declare_cdc_via_land(
    pool: &sqlx::PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
) -> Result<SnapshotId, ControlPlaneError> {
    let (schema, batches) = batch(2);
    land_cdc(
        pool,
        catalog,
        table,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(table),
        None, // stream_buckets MUST be None — passing both is an internal-caller bug
        Some(CdcDecl {
            buckets: 1,
            bucket_key: "id".into(),
            merge_engine: MergeEngine::LastRow,
        }),
        &[],
    )
    .await
}

/// Declare `table` a LOG stream through the same production path (`land`).
async fn declare_log_via_land(
    pool: &sqlx::PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
) -> Result<SnapshotId, ControlPlaneError> {
    let (schema, batches) = batch(2);
    land(
        pool,
        catalog,
        table,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(table),
        Some(1), // a LOG declaration
    )
    .await
}

/// REGISTRATION SIDE: a micro-batch MV over an already-declared CDC source is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn micro_batch_over_a_cdc_source_is_refused() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let src = tref("s", "cdc_events");

    let tid = ensure(&pool, &src).await;
    cp.declare_cdc(tid, 1, "id", MergeEngine::LastRow)
        .await
        .expect("declare_cdc");

    let err = define_mv(&cp, &src)
        .await
        .expect_err("a micro-batch MV over a CDC source must be refused");
    match err {
        ControlPlaneError::Validation(m) => assert!(
            m.contains("cdc"),
            "the message must explain the CDC source: {m}"
        ),
        other => panic!("expected Validation, got {other:?}"),
    }
}

/// Non-regression: a LOG source registers fine — this is the supported configuration and
/// every MV test in the tree depends on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn micro_batch_over_a_log_source_is_allowed() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let src = tref("s", "log_events");

    let tid = ensure(&pool, &src).await;
    cp.declare_stream(tid, 1).await.expect("declare_stream");

    define_mv(&cp, &src).await.expect("a log source is allowed");
}

/// Non-regression: an UNDECLARED source registers fine (it becomes a log stream on its
/// first `?mode=stream` write) — `registered_but_unrun_mv_floors_at_zero` in
/// `tests/mv_floor.rs` depends on exactly this ordering.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn micro_batch_over_an_undeclared_source_is_allowed() {
    let fx = PgFixture::shared();
    let (cp, _db) = fx.fresh_db().await;
    define_mv(&cp, &tref("s", "not_yet"))
        .await
        .expect("an undeclared source is allowed");
}

/// THE SYMMETRIC HALF — the ordering that defeats a registration-only guard. Register the
/// MV over a source that does not exist yet (legitimate, and asserted above), THEN declare
/// that source CDC via the production path. The declaration must be refused; otherwise a
/// live micro-batch reader ends up sourcing a CDC table, its floor pins every bucket at 0
/// forever, and the table's consolidate fold declines on every attempt for good.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declaring_cdc_on_a_table_an_mv_sources_is_refused() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let src = tref("s", "events");

    // 1. Register the MV over a source that does not exist yet — allowed.
    define_mv(&cp, &src)
        .await
        .expect("undeclared source is allowed");

    // 2. Now write that source as CDC through the production declaration path. Refused.
    let err = declare_cdc_via_land(&pool, &catalog, &src)
        .await
        .expect_err("declaring CDC on a table a micro-batch MV sources must be refused");
    match err {
        ControlPlaneError::Validation(m) => assert!(
            m.contains("micro-batch"),
            "the message must name the reader: {m}"
        ),
        other => panic!("expected Validation, got {other:?}"),
    }
}

/// Non-regression: declaring a LOG stream on a table an MV sources is the SUPPORTED
/// configuration (it is what every MV in the tree does) and must still work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declaring_a_log_stream_on_a_table_an_mv_sources_is_allowed() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let src = tref("s", "events");

    define_mv(&cp, &src)
        .await
        .expect("undeclared source is allowed");
    declare_log_via_land(&pool, &catalog, &src)
        .await
        .expect("a LOG declaration over an MV source is the supported configuration");
}
