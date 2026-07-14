//! Fixture tests for the end-cap seam (`iss-end-cap-ignores-mv-floor`): a `Removing`
//! end-cap of offsets an MV has not read is blocked; a `Reframing` one (flush,
//! compaction — same rows re-projected at the same offsets) is not; a `Destroying`
//! one (catalog drop) bypasses the floor on purpose.
//!
//! Seeds come from the shared `end_cap_seed` library — a declared LOG stream table of
//! one bucket with six events landed to Parquet FILES, one registered micro-batch MV,
//! watermark advanced to 3, so offsets 3..6 are unread.

use control_plane_core::{ControlPlaneError, RunId, mv_key};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_landing::overwrite_stream_base;
use control_plane_postgres::iceberg_mirror::{end_cap_live_data_files, next_snapshot};
use control_plane_postgres::mv_floor::{
    EndCapIntent, MV_FLOOR_REFUSAL_PREFIX, guard_end_cap, removal_blocked,
};
use end_cap_seed::{
    advance, cdc_specs, framed_cdc_batch, lineage, live_file_count, seed_floored_cdc, seed_source,
    tref,
};

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

/// The INLINE tier, genuinely exercised: `inline = true` keeps all 6 rows as LIVE
/// rows in `inline_<tid>` (no Parquet files at all, so the file tier finds nothing
/// live and falls through). The MV has read 0,1,2; offsets 3,4,5 are still live
/// inline rows above the floor, so `removal_blocked` must hit the `select
/// exists(...)` query against `inline_<tid>` — not the `inline_table_exists`
/// early-return — and find a blocking row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_tier_blocks_above_the_floor() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // inline = true => rows stay inline (never flushed to Parquet), so this test
    // exercises the INLINE tier of `removal_blocked`, not the file tier.
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        true,
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
        "offsets 3..6 are still live inline rows above the floor — a removal must be blocked"
    );

    let err = guard_end_cap(&mut conn, &s.src, s.tid, &EndCapIntent::Removing)
        .await
        .expect_err("a Removing end-cap above the floor must be refused (inline tier)");
    match err {
        ControlPlaneError::Validation(m) => {
            assert!(
                m.starts_with("mv-floor refuses end-cap:"),
                "unexpected message: {m}"
            );
        }
        other => panic!("expected Validation, got {other:?}"),
    }
}

/// The INLINE tier's other branch: watermark caught up to 6 of 6 means every live
/// inline row is strictly below its bucket's floor, so the `select exists(...)`
/// query against `inline_<tid>` must run and find NOTHING blocking.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_tier_does_not_block_at_the_floor() {
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
        true,
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
        "every live inline row is below the floor — nothing should block"
    );
    guard_end_cap(&mut conn, &s.src, s.tid, &EndCapIntent::Removing)
        .await
        .expect("a fully-consumed inline source is never refused");
}

// ---- the primitives themselves ---------------------------------------------
//
// The tests above drive `guard_end_cap` directly. These drive the guarded
// PRIMITIVE on a real transaction — what makes the guard structural (an argument
// of the signature) rather than a convention a new caller can forget.

/// The primitive itself refuses, and the live set is untouched (the tx rolls back).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removing_end_cap_primitive_is_refused() {
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
    let before = live_file_count(&s.pool, s.tid).await;
    assert!(before > 0, "seed must leave live files");

    let mut tx = s.pool.begin().await.expect("tx");
    let at = next_snapshot(&mut tx, None).await.expect("snapshot");
    end_cap_live_data_files(&mut tx, &s.src, s.tid, at, &EndCapIntent::Removing)
        .await
        .expect_err("the primitive itself must refuse");
    drop(tx); // rolls back

    assert_eq!(
        live_file_count(&s.pool, s.tid).await,
        before,
        "a refused end-cap must leave the live set untouched"
    );
}

/// The same primitive call, declared `Reframing`, commits — the over-refusal guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reframing_end_cap_primitive_commits() {
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

    let mut tx = s.pool.begin().await.expect("tx");
    let at = next_snapshot(&mut tx, None).await.expect("snapshot");
    end_cap_live_data_files(&mut tx, &s.src, s.tid, at, &EndCapIntent::Reframing)
        .await
        .expect("a reframing end-cap must commit");
    tx.commit().await.expect("commit");

    assert_eq!(live_file_count(&s.pool, s.tid).await, 0);
}

/// A floored CDC table MUST still flush, because a flush is REFRAMING.
///
/// This is the regression test for the seam's worst near-miss: flush has TWO commits that
/// end-cap inline rows (`iceberg_flush.rs`'s non-CDC append AND the CDC base append inside
/// `flush_locked_cdc`), and `EndCapIntent`'s default is `Removing`. Miss either and a
/// floored table can never flush again — and NO other test in the tree puts a floor on a
/// CDC table, so the whole suite would stay green while shipping it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_flush_with_a_floored_source_still_succeeds() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let s = seed_floored_cdc(fx, &cp, &db).await;

    flush_table(&s.catalog, &s.pool, &s.table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("a CDC flush is REFRAMING and must not be refused by the MV floor")
        .expect("flush produced a snapshot");
}

/// The refusal MESSAGE must survive the catalog commit wrap. `guard_end_cap` raises
/// `ControlPlaneError::Validation` inside `write_mirror`, but `commit_mirror_in_tx`
/// re-wraps it as `iceberg::Error{Unexpected}` and `append_parquet_snapshot` re-wraps
/// THAT with `backend()` — so the VARIANT is destroyed on the way out and only the
/// string survives. `consolidate_table`'s race arm (an MV floor appearing between its
/// pre-check and its commit) therefore matches on [`MV_FLOOR_REFUSAL_PREFIX`], not on
/// the variant. If this test ever fails, that arm is dead code and a raced CDC fold
/// retries every 60 seconds forever (`RetryPolicy::Retry` has no max-attempts).
///
/// `overwrite_stream_base` is the CDC fold's framed door, driven here directly: same
/// commit path, without needing the engine.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_floor_refusal_message_survives_the_commit_wrap() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let s = seed_floored_cdc(fx, &cp, &db).await;

    let err = overwrite_stream_base(
        &s.pool,
        &s.catalog,
        &s.table,
        &cdc_specs(),
        vec![framed_cdc_batch(1, 300, 0, 0)],
        Some(&lineage(&s.table)),
        None,
    )
    .await
    .expect_err("a floored stream base must refuse the framed overwrite");

    assert!(
        err.to_string().contains(MV_FLOOR_REFUSAL_PREFIX),
        "the refusal message must survive the catalog wrap; got: {err}"
    );
}
