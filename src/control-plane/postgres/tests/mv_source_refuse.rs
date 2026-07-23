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
use sqlx::PgPool;

/// Barrier: block until some backend in this database is waiting on an ADVISORY
/// lock — i.e. a first-declaring `reconcile_stream_mode` (or a `define_transform`)
/// is blocked on `pg_advisory_xact_lock(lock_key(source))` held by another tx.
/// `wait_event_type='Lock' / wait_event='advisory'` is exactly what
/// `pg_advisory_xact_lock` waits on (verified against the pinned PG 17.9). Polls
/// `pg_stat_activity`; panics rather than hanging.
///
/// Load-bearing: only once the loser is observably blocked may the caller commit
/// the winner. Commit any earlier and the loser's guard read could see the
/// winner's row without ever contending on the lock — a green test for the wrong
/// reason (it would pass even if the lock were removed).
async fn await_lock_blocked(pool: &PgPool) {
    for _ in 0..600 {
        let blocked: i64 = sqlx::query_scalar(
            "select count(*) from pg_stat_activity \
             where datname = current_database() \
               and wait_event_type = 'Lock' \
               and wait_event = 'advisory' \
               and query like 'select pg_advisory_xact_lock%'",
        )
        .fetch_one(pool)
        .await
        .expect("pg_stat_activity advisory barrier probe");
        if blocked >= 1 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the losing operation never blocked on the per-table advisory lock");
}

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

/// THE RACE — order A: a micro-batch MV registration commits WHILE a first CDC
/// declaration is in flight. The registration wins the per-table lock; the CDC
/// declaration blocks on it, then — re-reading its `pg_micro_batch_readers` guard
/// UNDER the lock — sees the committed MV and is refused. Without the fix the CDC
/// declaration takes no lock, never blocks (the barrier would time out), and both
/// commit into the wedged state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cdc_declare_racing_a_committing_mv_registration_is_refused() {
    use control_plane_postgres::iceberg_flush::lock_key;

    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let src = tref("s", "events");

    // WINNER (simulated in-flight `define_transform`): hold `lock_key(src)` and
    // insert an UNCOMMITTED micro-batch transform row naming `src` as its source.
    // `pg_micro_batch_readers` reads `transforms.transform` directly and decodes
    // the `body` with `serde_json::from_value`, so a body serialized with
    // `serde_json::to_value` (exactly what `define_transform` stores) round-trips.
    let mut winner = pool.begin().await.expect("begin winner tx");
    sqlx::query("select pg_advisory_xact_lock($1)")
        .bind(lock_key(&src.schema, &src.name))
        .execute(&mut *winner)
        .await
        .expect("winner takes lock_key(src)");
    let body = serde_json::to_value(TransformBody::MicroBatch {
        source: src.clone(),
        output: tref("s", "out_a"),
        buckets: 1,
        sql: "select id from src".into(),
    })
    .expect("serialize micro-batch body");
    sqlx::query("insert into transforms.transform (name, body) values ($1, $2)")
        .bind("mv_a")
        .bind(body)
        .execute(&mut *winner)
        .await
        .expect("winner inserts the MV registration row");

    // LOSER: the real production CDC declaration path, on its own connection.
    let pool_b = pool.clone();
    let src_b = src.clone();
    let loser = tokio::spawn(async move { declare_cdc_via_land(&pool_b, &catalog, &src_b).await });

    // Only once the loser is observably blocked on the advisory lock do we commit
    // the winner — so the loser is guaranteed to contend on the lock, not race
    // past the guard.
    await_lock_blocked(&pool).await;
    winner
        .commit()
        .await
        .expect("commit winner MV registration");

    let err = loser
        .await
        .expect("join loser")
        .expect_err("a CDC declaration racing a committing MV registration must be refused");
    match err {
        ControlPlaneError::Validation(m) => assert!(
            m.contains("micro-batch"),
            "the refusal must name the reader: {m}"
        ),
        other => panic!("expected Validation, got {other:?}"),
    }

    // WEDGED STATE ABSENT (spec post-condition): the MV row committed, but its
    // source has NO stream_table row — the refused CDC declaration rolled back with
    // its landing tx, so it is never "both a CDC stream_table row and an MV body
    // naming that source".
    let cdc_rows: i64 = sqlx::query_scalar(
        "select count(*) from stream.stream_table st \
         join iceberg_mirror.\"table\" t on t.table_id = st.table_id \
         where t.table_namespace = $1 and t.table_name = $2 and st.kind = 'cdc'",
    )
    .bind(&src.schema)
    .bind(&src.name)
    .fetch_one(&pool)
    .await
    .expect("count cdc stream_table rows for src");
    assert_eq!(
        cdc_rows, 0,
        "the refused CDC declaration must have left no cdc row"
    );
    let mv_rows: i64 =
        sqlx::query_scalar("select count(*) from transforms.transform where name = 'mv_a'")
            .fetch_one(&pool)
            .await
            .expect("count the committed MV row");
    assert_eq!(mv_rows, 1, "the MV registration is the survivor");
}
