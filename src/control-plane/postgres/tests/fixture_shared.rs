//! PgFixture::shared(): one cluster per test process, database-level isolation.
//! loom_fixture_test (Postgres).

use control_plane_postgres::fixture::PgFixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_returns_one_cluster_with_isolated_databases() {
    let a = PgFixture::shared();
    let b = PgFixture::shared();
    assert!(
        std::ptr::eq(a, b),
        "shared() must return the same fixture instance"
    );

    let (_cp1, db1) = a.fresh_db().await;
    let (_cp2, db2) = b.fresh_db().await;
    assert_ne!(
        db1, db2,
        "fresh_db stays per-call isolated on the shared cluster"
    );

    let pool = a.pool_for(&db1).await;
    let one: i64 = sqlx::query_scalar("select 1::bigint")
        .fetch_one(&pool)
        .await
        .expect("query on shared cluster");
    assert_eq!(one, 1);
}

/// Regression guard for the PDEATHSIG thread-semantics footgun: the cluster
/// must survive the death of earlier tests' tokio worker threads. (prctl's
/// PDEATHSIG fires on SPAWNING-THREAD death — which is why shared() boots from
/// a dedicated parked thread, and why this second, later-scheduled test
/// exists.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_survives_other_tests_runtimes() {
    // Force sequencing after the sibling test has likely completed at least
    // once: do our own full round-trip regardless of libtest scheduling.
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let one: i64 = sqlx::query_scalar("select 1::bigint")
        .fetch_one(&pool)
        .await
        .expect("shared cluster still alive");
    assert_eq!(one, 1);
    assert!(db.starts_with("loom_test_"));
}
