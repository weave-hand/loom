//! Regression + characterization tests for the no-transaction (auto-commit) arm of
//! `SqlCatalog::execute` — the path that begins a tx, runs one statement, then commits.
//! Fixes [[iss-sqlcatalog-execute-commit-swallow]]
//! (docs/superpowers/specs/2026-06-29-sqlcatalog-execute-commit-error-design.md).
//!
//! `loom_fixture_test`, not an inline module — loom forbids inline `#[test]`.

use loom_test_seed::local_sql_catalog;

use control_plane_postgres::fixture::PgFixture;

/// Behavior lock: a statement that fails *at execution time* (duplicate primary key)
/// surfaces as `Err`, and the auto-commit transaction rolls back so nothing extra
/// persists. Passes on both pre- and post-fix code (Postgres downgrades COMMIT on an
/// aborted tx to ROLLBACK), so this guards the statement-failure surface rather than
/// catching the swallowed-commit bug.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn statement_failure_rolls_back_and_errors() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    catalog
        .execute(
            "CREATE TABLE loom_exec_probe (id TEXT PRIMARY KEY)",
            vec![],
            None,
        )
        .await
        .expect("create probe table");

    catalog
        .execute(
            "INSERT INTO loom_exec_probe (id) VALUES (?)",
            vec![Some("a")],
            None,
        )
        .await
        .expect("first insert persists");

    // Duplicate primary key -> the statement fails at execution time.
    let dup = catalog
        .execute(
            "INSERT INTO loom_exec_probe (id) VALUES (?)",
            vec![Some("a")],
            None,
        )
        .await;
    assert!(dup.is_err(), "duplicate insert must surface an error");

    // The failed statement's auto-commit tx rolled back: exactly the first row remains.
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM loom_exec_probe")
        .fetch_one(&pool)
        .await
        .expect("count rows");
    assert_eq!(count, 1, "only the first insert persisted");
}

/// Regression test for the swallowed-commit bug. The duplicate rows pass the
/// `DEFERRABLE INITIALLY DEFERRED` unique check at statement time, so `execute`
/// succeeds at the statement and then fails at `COMMIT`. Pre-fix code dropped the
/// commit `Result` and returned the earlier `Ok`; the fix propagates the failure as
/// `Err`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_failure_propagates_as_error() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    catalog
        .execute(
            "CREATE TABLE loom_commit_probe (\
                 id TEXT, \
                 UNIQUE (id) DEFERRABLE INITIALLY DEFERRED\
             )",
            vec![],
            None,
        )
        .await
        .expect("create deferred-constraint table");

    // One statement inserts two equal rows; the deferred UNIQUE check fires at COMMIT.
    let result = catalog
        .execute(
            "INSERT INTO loom_commit_probe (id) VALUES (?), (?)",
            vec![Some("dup"), Some("dup")],
            None,
        )
        .await;
    assert!(
        result.is_err(),
        "a commit-time constraint violation must propagate as Err, not be swallowed"
    );
}
