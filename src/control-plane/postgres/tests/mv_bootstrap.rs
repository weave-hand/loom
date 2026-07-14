//! The earliest-surviving-offset computation a newly registered micro-batch MV starts from
//! (iss-mv-register-below-reclaimed-floor).
//!
//! The invariant every test here defends: the computed start ROUNDS DOWN. It may name an
//! offset below the true earliest surviving row (harmless — the MV re-scans an empty range,
//! and the relaxed CAS still advances) but must NEVER name one above it (a silent data hole).

use std::time::Duration;

use control_plane_core::{RunId, SnapshotId};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_gc::gc_table;
use control_plane_postgres::iceberg_mirror::end_cap_live_data_files;
use control_plane_postgres::mv_bootstrap::earliest_surviving_offsets;
use control_plane_postgres::mv_floor::EndCapIntent;
use end_cap_seed::{age_all_snapshots, current_snapshot_id, land_more, seed_source};

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
