//! Fixture tests for the end-cap seam (`iss-end-cap-ignores-mv-floor`): a `Removing`
//! end-cap of offsets an MV has not read is blocked; a `Reframing` one (flush,
//! compaction — same rows re-projected at the same offsets) is not; a `Destroying`
//! one (catalog drop) bypasses the floor on purpose.
//!
//! Seeds come from the shared `end_cap_seed` library — a declared LOG stream table of
//! one bucket with six events landed to Parquet FILES, one registered micro-batch MV,
//! watermark advanced to 3, so offsets 3..6 are unread.

use control_plane_core::{ControlPlaneError, mv_key};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::mv_floor::{EndCapIntent, guard_end_cap, removal_blocked};
use end_cap_seed::{advance, seed_source, tref};

/// The core block: files carrying offsets the MV has not read block a `Removing`
/// end-cap, and `guard_end_cap` turns that into a `Validation` naming the laggard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removal_is_blocked_above_the_floor() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // inline = false => straight to Parquet FILES (the file tier of the guard).
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        false,
        &[("mv_a", "out_a")],
    )
    .await;
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 3).await;

    let mut conn = s.pool.acquire().await.expect("conn");
    assert!(
        removal_blocked(&mut conn, &s.src, s.tid)
            .await
            .expect("removal_blocked")
            .is_some(),
        "offsets 3..6 are unread — a removal must be blocked"
    );

    let err = guard_end_cap(&mut conn, &s.src, s.tid, &EndCapIntent::Removing)
        .await
        .expect_err("a Removing end-cap above the floor must be refused");
    match err {
        ControlPlaneError::Validation(m) => {
            assert!(
                m.starts_with("mv-floor refuses end-cap:"),
                "unexpected message: {m}"
            );
            assert!(
                m.contains("out_a"),
                "the message must name the laggard MV: {m}"
            );
        }
        other => panic!("expected Validation, got {other:?}"),
    }
}

/// The over-refusal guard: the SAME state, declared `Reframing`, is NOT refused. This is
/// what proves the seam distinguishes reframing from removal — the test that catches the
/// naive "refuse any end-cap above the floor" implementation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reframing_is_never_refused() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        false,
        &[("mv_a", "out_a")],
    )
    .await;
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 3).await;

    let mut conn = s.pool.acquire().await.expect("conn");
    guard_end_cap(&mut conn, &s.src, s.tid, &EndCapIntent::Reframing)
        .await
        .expect("a reframing end-cap must never consult the floor");
}

/// The drop bypass: `Destroying` proceeds on purpose (the operator dropped the source, so
/// its MVs are dead by definition; wedging drop-GC on a dead MV forever is worse).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn destroying_bypasses_the_floor() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        false,
        &[("mv_a", "out_a")],
    )
    .await;
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 3).await;

    let mut conn = s.pool.acquire().await.expect("conn");
    guard_end_cap(
        &mut conn,
        &s.src,
        s.tid,
        &EndCapIntent::Destroying {
            reason: "test: catalog drop",
        },
    )
    .await
    .expect("a destroying end-cap bypasses the floor on purpose");
}

/// The fast path: a table no MV sources has no floor, so every intent is a no-op and a
/// `Removing` end-cap proceeds byte-identically to the pre-seam behavior. This is what
/// makes the seam free for every table in the tree that is not an MV source.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_table_no_mv_reads_is_never_refused() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // No MVs registered => no reader => no floor.
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        false,
        &[],
    )
    .await;

    let mut conn = s.pool.acquire().await.expect("conn");
    assert!(
        removal_blocked(&mut conn, &s.src, s.tid)
            .await
            .expect("removal_blocked")
            .is_none(),
        "no reader => no floor => nothing blocks"
    );
    guard_end_cap(&mut conn, &s.src, s.tid, &EndCapIntent::Removing)
        .await
        .expect("a table no MV reads is never refused");
}

/// A caught-up MV releases the tail: watermark at 6 of 6 blocks nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_caught_up_mv_does_not_block_removal() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        false,
        &[("mv_a", "out_a")],
    )
    .await;
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 6).await;

    let mut conn = s.pool.acquire().await.expect("conn");
    assert!(
        removal_blocked(&mut conn, &s.src, s.tid)
            .await
            .expect("removal_blocked")
            .is_none(),
        "a fully-consumed source blocks nothing"
    );
}
