//! inline_trigger helpers: bump accrues + reports the effective threshold; arm
//! and reset flip the flag/counter. loom_fixture_test (hermetic Postgres).

use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_mirror::{
    arm_inline_trigger, bump_inline_trigger, reset_inline_trigger,
};

// Fixture API (verified against tests/iceberg_flush.rs): `PgFixture::start()` is
// NOT async; `fresh_db()` and `pool_for()` are.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bump_accrues_and_reports_effective_threshold() {
    let fx = PgFixture::start();
    let (_, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let mut conn = pool.acquire().await.unwrap();
    let tid = 4242_i64;

    // First bump creates the row; effective falls back to the global default.
    let s1 = bump_inline_trigger(&mut conn, tid, 100, 1_000)
        .await
        .unwrap();
    assert_eq!(s1.live_bytes, 100);
    assert_eq!(s1.effective, 1_000);
    assert!(!s1.enqueued);

    // Second bump accumulates.
    let s2 = bump_inline_trigger(&mut conn, tid, 250, 1_000)
        .await
        .unwrap();
    assert_eq!(s2.live_bytes, 350);

    // Arm sets the flag; the next bump reports it.
    arm_inline_trigger(&mut conn, tid).await.unwrap();
    let s3 = bump_inline_trigger(&mut conn, tid, 1, 1_000).await.unwrap();
    assert!(s3.enqueued);

    // Reset clears counter + flag.
    reset_inline_trigger(&mut conn, tid).await.unwrap();
    let s4 = bump_inline_trigger(&mut conn, tid, 5, 1_000).await.unwrap();
    assert_eq!(s4.live_bytes, 5);
    assert!(!s4.enqueued);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_table_threshold_overrides_global() {
    let fx = PgFixture::start();
    let (_, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let mut conn = pool.acquire().await.unwrap();
    let tid = 99_i64;
    bump_inline_trigger(&mut conn, tid, 1, 10_000)
        .await
        .unwrap();
    // Set a per-table override below the global.
    sqlx::query("update iceberg_mirror.inline_trigger set threshold = 50 where table_id = $1")
        .bind(tid)
        .execute(&mut *conn)
        .await
        .unwrap();
    let s = bump_inline_trigger(&mut conn, tid, 1, 10_000)
        .await
        .unwrap();
    assert_eq!(s.effective, 50, "per-table threshold must beat the global");
}
