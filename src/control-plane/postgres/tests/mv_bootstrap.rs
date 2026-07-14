//! The earliest-surviving-offset computation a newly registered micro-batch MV starts from
//! (iss-mv-register-below-reclaimed-floor).
//!
//! The invariant every test here defends: the computed start ROUNDS DOWN. It may name an
//! offset below the true earliest surviving row (harmless — the MV re-scans an empty range,
//! and the relaxed CAS still advances) but must NEVER name one above it (a silent data hole).

use std::time::Duration;

use control_plane_core::{MvWatermarks, RunId, SnapshotId, mv_key};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_gc::gc_table;
use control_plane_postgres::iceberg_mirror::end_cap_live_data_files;
use control_plane_postgres::mv_bootstrap::earliest_surviving_offsets;
use control_plane_postgres::mv_floor::{EndCapIntent, mv_floor};
use end_cap_seed::{
    advance, age_all_snapshots, current_snapshot_id, land_more, register_mv, register_mv_result,
    seed_source, tref,
};

const SEVEN_DAYS: Duration = Duration::from_secs(7 * 24 * 3600);

/// Physically destroy every offset currently live in `src`: flush (end-caps the inline copies
/// and writes a Parquet file), end-cap that file, age the snapshots past the horizon, then GC.
/// With no MV registered the floor is `None`, so GC is unguarded and really does take them.
async fn reclaim_everything(s: &end_cap_seed::Seeded) {
    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    let snap = current_snapshot_id(&s.pool, &s.src).await;
    let mut tx = s.pool.begin().await.expect("tx");
    end_cap_live_data_files(
        &mut tx,
        &s.src,
        s.tid,
        SnapshotId(snap),
        &EndCapIntent::Reframing,
    )
    .await
    .expect("end-cap the flushed file");
    tx.commit().await.expect("commit");
    age_all_snapshots(&s.pool).await;
    gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
}

/// A source nothing has ever reclaimed starts at 0 in every bucket — today's behavior,
/// preserved exactly. This is what keeps `registered_but_unrun_mv_floors_at_zero` green.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn untouched_source_starts_at_zero() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(2),
        true,
        &[],
    )
    .await;

    let mut conn = s.pool.acquire().await.expect("conn");
    let starts = earliest_surviving_offsets(&mut conn, s.tid, 2)
        .await
        .expect("starts");
    assert_eq!(
        starts,
        [(0, 0), (1, 0)].into_iter().collect(),
        "nothing was ever reclaimed: every bucket's earliest surviving offset is 0"
    );
}

/// The acceptance case: a prefix is PHYSICALLY GONE and a surviving range was landed above it.
/// The start must name the surviving range, not 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaimed_prefix_starts_above_the_hole() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // One bucket keeps the arithmetic exact: offsets 0..6 inline, NO MV (so GC is unguarded).
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        true,
        &[],
    )
    .await;

    reclaim_everything(&s).await;
    land_more(&s.pool, &s.catalog, &s.src, 6, Some(1), true).await; // offsets 6..12

    let mut conn = s.pool.acquire().await.expect("conn");
    let starts = earliest_surviving_offsets(&mut conn, s.tid, 1)
        .await
        .expect("starts");
    assert_eq!(
        starts.get(&0).copied(),
        Some(6),
        "offsets 0..6 are gone; the earliest SURVIVING offset is 6 — starting at 0 defines a \
         first delta over a range that no longer exists"
    );
}

/// Nothing live at all: the start is the allocator's high-water mark. Starting at 0 here would
/// floor GC at 0 forever with no row to justify it — the over-hold the bootstrap exists to kill.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fully_reclaimed_source_starts_at_the_high_water_mark() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        4,
        Some(1),
        true,
        &[],
    )
    .await;

    reclaim_everything(&s).await;

    let mut conn = s.pool.acquire().await.expect("conn");
    let starts = earliest_surviving_offsets(&mut conn, s.tid, 1)
        .await
        .expect("starts");
    assert_eq!(
        starts.get(&0).copied(),
        Some(4),
        "no live row survives; the only start that skips nothing is the allocator's next offset"
    );
}

/// A source landed straight to PARQUET (no inline tier): the live file's `loom_offset` min stat
/// supplies the bound. One bucket ⇒ the file is single-bucket ⇒ the bound is exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_tier_supplies_the_bound() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        5,
        Some(1),
        false,
        &[],
    )
    .await;

    let mut conn = s.pool.acquire().await.expect("conn");
    let starts = earliest_surviving_offsets(&mut conn, s.tid, 1)
        .await
        .expect("starts");
    assert_eq!(
        starts.get(&0).copied(),
        Some(0),
        "the live file's loom_offset min stat is 0"
    );
}

/// The file tier is the SOLE source of a NON-ZERO bound: a prefix is physically gone and the
/// survivors live only in a Parquet FILE — the inline tier holds nothing live, so the answer can
/// come from nowhere but the file's `loom_offset` min stat.
///
/// This is the test that actually pins the file query. `file_tier_supplies_the_bound` cannot:
/// its expected `0` is ALSO the value every file-tier failure mode fails safe to (a missing
/// `loom_offset` stat rounds DOWN to 0), so breaking the file read leaves it green. Here a broken
/// file read collapses the answer to 0 and the assertion catches it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_tier_alone_supplies_a_nonzero_bound() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // One bucket keeps the arithmetic exact: offsets 0..6 inline, no MV (so GC is unguarded).
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        true,
        &[],
    )
    .await;

    // Offsets 0..6 are now physically gone, and nothing is live in either tier.
    reclaim_everything(&s).await;
    // The survivors land STRAIGHT TO PARQUET (`inline: false`): offsets 6..12 in a live file,
    // nothing live inline.
    land_more(&s.pool, &s.catalog, &s.src, 6, Some(1), false).await;

    let mut conn = s.pool.acquire().await.expect("conn");
    let starts = earliest_surviving_offsets(&mut conn, s.tid, 1)
        .await
        .expect("starts");
    assert_eq!(
        starts.get(&0).copied(),
        Some(6),
        "the only live data is a Parquet file over offsets 6..12; its loom_offset min stat is the \
         only thing that can prove the start is 6 — a 0 here means the file read produced nothing \
         and the fail-safe rounded down, and a 12 means it skipped the file's live rows entirely"
    );
}

/// The item's acceptance test. Registering an MV against a source whose prefix is GONE
/// bootstraps its watermarks to the surviving range — and therefore does NOT reset the source's
/// GC floor to 0. Covers both the mis-read and the over-hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registering_against_a_reclaimed_source_bootstraps_the_watermark() {
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
        &[],
    )
    .await;

    reclaim_everything(&s).await;
    land_more(&s.pool, &s.catalog, &s.src, 6, Some(1), true).await;

    // NOW register. The MV can never read offsets 0..6 — they do not exist.
    register_mv(&cp, "mv_a", &s.src, &tref("s", "out_a")).await;
    let mv = mv_key(&tref("s", "out_a"));

    let wm = cp.mv_watermarks(&mv, s.tid).await.expect("watermarks");
    assert_eq!(
        wm.get(&0).copied(),
        Some(6),
        "the MV is bootstrapped to the surviving range, not to 0"
    );

    let start: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(
        "select start_offset from stream.mv_watermark where mv = $1 and bucket = 0",
    ))
    .bind(mv.as_str())
    .fetch_one(&s.pool)
    .await
    .expect("start_offset");
    assert_eq!(
        start, 6,
        "the bootstrap is RECORDED: 'this MV never saw offsets 0..6' is an auditable fact"
    );

    let mut conn = s.pool.acquire().await.expect("conn");
    let floor = mv_floor(&mut conn, &s.src, s.tid)
        .await
        .expect("floor")
        .expect("a registered MV reads this source");
    assert_eq!(
        floor.per_bucket.get(&0).copied(),
        Some(6),
        "registering no longer drops the source's GC floor to 0 — the over-hold is gone"
    );
}

/// Registering against a never-reclaimed source bootstraps to 0 — today's behavior, unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registering_against_an_untouched_source_starts_at_zero() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(2),
        true,
        &[("mv_a", "out_a")],
    )
    .await;

    let wm = cp
        .mv_watermarks(&mv_key(&tref("s", "out_a")), s.tid)
        .await
        .expect("watermarks");
    assert_eq!(
        wm,
        [(0, 0), (1, 0)].into_iter().collect(),
        "nothing was reclaimed: the MV starts at 0, exactly as before"
    );
}

/// Define-before-land stays legal: registering against a source that does not exist yet is not
/// an error, and bootstraps nothing (nothing can have been lost).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn define_before_land_is_still_legal() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        2,
        Some(1),
        true,
        &[],
    )
    .await;

    register_mv(
        &cp,
        "mv_future",
        &tref("s", "nonexistent"),
        &tref("s", "out_f"),
    )
    .await;

    let wm = cp
        .mv_watermarks(&mv_key(&tref("s", "out_f")), s.tid)
        .await
        .expect("watermarks");
    assert!(
        wm.is_empty(),
        "no source, nothing to bootstrap — and no error"
    );
}

/// A redefinition that keeps the same output RESUMES: the bootstrap must never clobber a
/// watermark a run has already advanced (`on conflict do nothing`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redefining_the_same_output_does_not_reset_progress() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        8,
        Some(1),
        true,
        &[("mv_a", "out_a")],
    )
    .await;
    let mv = mv_key(&tref("s", "out_a"));
    advance(&cp, &mv, s.tid, 0, 0, 5).await;

    register_mv(&cp, "mv_a", &s.src, &tref("s", "out_a")).await; // same name, source, output

    let wm = cp.mv_watermarks(&mv, s.tid).await.expect("watermarks");
    assert_eq!(
        wm.get(&0).copied(),
        Some(5),
        "the MV resumes where it left off — the bootstrap did not reset it"
    );
}

/// `define_transform` must serialize against the SOURCE's per-table advisory lock — the one GC
/// holds across BOTH its floor read and its reclaim. Without it, a registration commits inside
/// GC's window and GC reclaims below the brand-new MV's floor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn define_transform_blocks_on_the_sources_table_lock() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        4,
        Some(1),
        true,
        &[],
    )
    .await;

    // Hold `lock_key(s.events)` by hand — exactly what `gc_table` does for the whole of its
    // floor-read + reclaim.
    let key = control_plane_postgres::iceberg_flush::lock_key(&s.src.schema, &s.src.name);
    let mut holder = s.pool.begin().await.expect("tx");
    sqlx::query(sqlx::AssertSqlSafe("select pg_advisory_xact_lock($1)"))
        .bind(key)
        .execute(&mut *holder)
        .await
        .expect("hold the source's table lock");

    let blocked = tokio::time::timeout(
        Duration::from_millis(750),
        register_mv_result(&cp, "mv_a", &s.src, &tref("s", "out_a")),
    )
    .await;
    assert!(
        blocked.is_err(),
        "define_transform did NOT take lock_key(source) — it committed while GC held the table \
         lock, which is exactly the race this item closes"
    );

    holder.rollback().await.expect("release");
    tokio::time::timeout(
        Duration::from_secs(10),
        register_mv_result(&cp, "mv_a", &s.src, &tref("s", "out_a")),
    )
    .await
    .expect("register once the lock is free")
    .expect("register");
}

/// A registration and a concurrent commit on the SAME source must not deadlock: the commit path
/// takes lock_key(table) then row-locks transforms.transform, so define_transform must take them
/// in that same order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registration_and_commit_do_not_deadlock() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        4,
        Some(1),
        true,
        &[("mv_a", "out_a")],
    )
    .await;

    // `join!` holds its futures across the await, so the output `TableRef` needs a binding
    // that outlives the statement (a temporary inside the macro is dropped too early).
    let out = tref("s", "out_a");
    let (flush, define) = tokio::join!(
        flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4())),
        register_mv_result(&cp, "mv_a", &s.src, &out),
    );
    flush.expect("flush must not deadlock against a concurrent registration");
    define.expect("register must not deadlock against a concurrent flush");
}
