//! Fixture tests for the MV read-position floor (`mv_floor`) and the
//! watermark-aware GC guard it drives (road-mv-watermark-aware-gc).
//!
//! The floor is the per-bucket `min(next_offset)` across every micro-batch MV
//! reading a source table — where an MV with no watermark row for a bucket
//! (a registered-but-never-run MV, or one that has never touched that bucket)
//! floors it at 0, exactly as `mv_delta_scan` reads it.

use std::time::Duration;

use control_plane_core::{ControlPlane, RunId, SnapshotId, TableRef, TransformName, mv_key};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_gc::gc_table;
use control_plane_postgres::iceberg_mirror::end_cap_live_data_files;
use control_plane_postgres::mv_floor::{EndCapIntent, mv_floor};
use end_cap_seed::{
    advance, age_all_snapshots, current_snapshot_id, data_file_count, end_capped_inline_count,
    seed_source, tref,
};
use iceberg::{Catalog as _, NamespaceIdent, TableIdent};

// ---- floor semantics -------------------------------------------------------

/// A plain (non-stream) table has no floor at all: GC keeps its fast path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_table_has_no_floor() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        3,
        None,
        true,
        &[],
    )
    .await;

    let mut conn = s.pool.acquire().await.expect("conn");
    assert_eq!(
        mv_floor(&mut conn, &s.src, s.tid).await.expect("floor"),
        None,
        "a non-stream table has no MV floor"
    );
}

/// A declared stream table nothing reads: still no floor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_table_without_readers_has_no_floor() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        4,
        Some(2),
        true,
        &[],
    )
    .await;

    let mut conn = s.pool.acquire().await.expect("conn");
    assert_eq!(
        mv_floor(&mut conn, &s.src, s.tid).await.expect("floor"),
        None,
        "no MV reads this stream table -> no floor"
    );
}

/// A registered MV that has never run pins EVERY bucket at 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_but_unrun_mv_floors_at_zero() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        4,
        Some(2),
        true,
        &[("mv_a", "out_a")],
    )
    .await;

    let mut conn = s.pool.acquire().await.expect("conn");
    let floor = mv_floor(&mut conn, &s.src, s.tid)
        .await
        .expect("floor")
        .expect("a registered MV reads this source");
    assert_eq!(
        floor.per_bucket,
        [(0, 0), (1, 0)].into_iter().collect(),
        "an unrun MV floors every bucket at 0"
    );
    assert_eq!(floor.min_offset(), 0, "the file guard holds everything");
}

/// Two MVs at different progress: the SLOWER one sets each bucket's floor, and an
/// MV with no row for a bucket floors THAT bucket at 0 even while ahead elsewhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slowest_mv_sets_the_floor_per_bucket() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        8,
        Some(2),
        true,
        &[("mv_a", "out_a"), ("mv_b", "out_b")],
    )
    .await;
    let a = mv_key(&tref("s", "out_a"));
    let b = mv_key(&tref("s", "out_b"));

    // mv_a consumed bucket 0 to 5 and bucket 1 to 3; mv_b consumed only bucket 0,
    // to 2, and has never touched bucket 1.
    advance(&cp, &a, s.tid, 0, 0, 5).await;
    advance(&cp, &a, s.tid, 1, 0, 3).await;
    advance(&cp, &b, s.tid, 0, 0, 2).await;

    let mut conn = s.pool.acquire().await.expect("conn");
    let floor = mv_floor(&mut conn, &s.src, s.tid)
        .await
        .expect("floor")
        .expect("two MVs read this source");
    assert_eq!(
        floor.per_bucket,
        [(0, 2), (1, 0)].into_iter().collect(),
        "bucket 0: min(5, 2) = 2; bucket 1: mv_b has no row there -> 0"
    );
    assert_eq!(
        floor.slowest.get(&0).map(String::as_str),
        Some(b.as_str()),
        "bucket 0's laggard is mv_b"
    );
    assert_eq!(
        floor.min_offset(),
        0,
        "the file guard takes the cross-bucket minimum"
    );
}

/// A single caught-up MV floors at its watermark (the release case Task 2 leans on).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caught_up_mv_floors_at_its_watermark() {
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
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 4).await;

    let mut conn = s.pool.acquire().await.expect("conn");
    let floor = mv_floor(&mut conn, &s.src, s.tid)
        .await
        .expect("floor")
        .expect("registered MV");
    assert_eq!(floor.per_bucket, [(0, 4)].into_iter().collect());
    assert_eq!(floor.min_offset(), 4);
}

const SEVEN_DAYS: Duration = Duration::from_secs(7 * 24 * 3600);

/// End-cap every live data file of `tid` at `snap` through the REAL guarded primitive.
/// Declared `Reframing` — which is what plain-coalesce compaction is: the same rows are
/// re-projected at the same offsets, so the floor is (correctly) not consulted. Before
/// the end-cap seam existed this was raw SQL, because no guarded API did.
async fn end_cap_data_files(pool: &sqlx::PgPool, table: &TableRef, tid: i64, snap: i64) {
    let mut tx = pool.begin().await.expect("tx");
    end_cap_live_data_files(
        &mut tx,
        table,
        tid,
        SnapshotId(snap),
        &EndCapIntent::Reframing,
    )
    .await
    .expect("end-cap data files");
    tx.commit().await.expect("commit");
}

// ---- GC under the floor ----------------------------------------------------

/// A lagging MV holds its source's unread tail: end-capped inline rows AT OR ABOVE
/// the MV's watermark survive GC, the ones below it are reclaimed, and
/// `held_by_mv_floor` counts the difference.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lagging_mv_holds_the_unread_tail() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // 6 inline events (offsets 0..6) in one bucket, one MV registered.
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

    // The MV has consumed offsets 0,1,2 (next_offset = 3): 3,4,5 are unread.
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 3).await;

    // Flush: the 6 rows land in a live Parquet file and their inline copies are
    // end-capped — the age-eligible candidates GC would otherwise take.
    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    age_all_snapshots(&s.pool).await;
    assert_eq!(
        end_capped_inline_count(&s.pool, s.tid).await,
        6,
        "all 6 inline rows are end-capped candidates"
    );

    let summary = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    assert_eq!(
        summary.inline_rows, 3,
        "only offsets 0,1,2 — strictly below the floor — are reclaimed"
    );
    assert_eq!(
        summary.held_by_mv_floor, 3,
        "offsets 3,4,5 are held for the lagging MV"
    );
    assert_eq!(
        end_capped_inline_count(&s.pool, s.tid).await,
        3,
        "the unread tail is physically still there"
    );
}

/// Once the MV catches up, the next GC reclaims what was held: the floor releases.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caught_up_mv_releases_the_tail() {
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
    let mv = mv_key(&tref("s", "out_a"));
    advance(&cp, &mv, s.tid, 0, 0, 3).await;
    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    age_all_snapshots(&s.pool).await;
    let first = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("first gc");
    assert_eq!(first.held_by_mv_floor, 3, "the tail is held while lagging");

    // The MV consumes the rest (3 -> 6).
    advance(&cp, &mv, s.tid, 0, 3, 6).await;

    let second = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("second gc");
    assert_eq!(second.inline_rows, 3, "the held tail is now reclaimed");
    assert_eq!(second.held_by_mv_floor, 0, "nothing is held any more");
    assert_eq!(
        end_capped_inline_count(&s.pool, s.tid).await,
        0,
        "no end-capped inline rows remain"
    );
}

/// A registered-but-never-run MV pins EVERYTHING — including end-capped FILES and
/// their Parquet, which is the tier a future compaction/retention path would eat.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrun_mv_pins_every_end_capped_row_and_file() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // Land straight to FILES; register an MV and never run it.
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

    let files_before = data_file_count(&s.pool, s.tid).await;
    assert!(files_before > 0, "the source landed at least one file");
    let snap = current_snapshot_id(&s.pool, &s.src).await;
    end_cap_data_files(&s.pool, &s.src, s.tid, snap).await;
    age_all_snapshots(&s.pool).await;

    let summary = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    assert_eq!(
        summary.data_file_rows, 0,
        "every file carries offsets the unrun MV has not read (floor 0): none reclaimed"
    );
    assert_eq!(summary.objects_deleted, 0, "no Parquet object deleted");
    assert_eq!(
        summary.held_by_mv_floor,
        u64::try_from(files_before).expect("count fits u64"),
        "every end-capped file is held by the floor"
    );
    assert_eq!(
        data_file_count(&s.pool, s.tid).await,
        files_before,
        "the mirror rows survive"
    );
}

/// The FILE tier's release case: once the MV has consumed past every offset a file
/// carries, GC must reclaim BOTH the `data_file` mirror row AND its Parquet object.
///
/// This is the counterpart of `unrun_mv_pins_every_end_capped_row_and_file` (floor 0,
/// nothing reclaimable) and the regression test for the defect where the floor guard
/// was re-evaluated in the row-delete statement AFTER the stats it reads had already
/// been deleted: the guard then went NULL, no `data_file` row was deleted, and the
/// Parquet was destroyed anyway — a dangling mirror -> missing-file reference. The
/// victim set is now materialized once, inside the tx, and drives both the row deletes
/// and the post-commit object deletes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caught_up_mv_releases_end_capped_files() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // 6 events landed straight to FILES (offsets 0..6) in one bucket, one MV.
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

    // The MV has consumed everything: floor 6 > every offset in every file.
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 6).await;

    let files_before = data_file_count(&s.pool, s.tid).await;
    assert!(files_before > 0, "the source landed at least one file");
    let snap = current_snapshot_id(&s.pool, &s.src).await;
    end_cap_data_files(&s.pool, &s.src, s.tid, snap).await;
    age_all_snapshots(&s.pool).await;

    let summary = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    let expected = u64::try_from(files_before).expect("count fits u64");
    assert_eq!(
        summary.data_file_rows, expected,
        "every end-capped file is strictly below the floor -> its mirror row is deleted"
    );
    assert_eq!(
        summary.objects_deleted, expected,
        "and its Parquet object is deleted — rows and objects must never disagree"
    );
    assert_eq!(
        summary.held_by_mv_floor, 0,
        "the caught-up MV holds nothing"
    );
    assert_eq!(
        data_file_count(&s.pool, s.tid).await,
        0,
        "no data_file row survives naming a deleted object"
    );
}

/// Two MVs at different offsets: the SLOWER one bounds the reclaim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slowest_of_two_mvs_bounds_the_reclaim() {
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
        &[("mv_a", "out_a"), ("mv_b", "out_b")],
    )
    .await;
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 5).await;
    advance(&cp, &mv_key(&tref("s", "out_b")), s.tid, 0, 0, 2).await;

    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    age_all_snapshots(&s.pool).await;

    let summary = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    assert_eq!(
        summary.inline_rows, 2,
        "offsets 0,1 only — mv_b, at 2, is the laggard"
    );
    assert_eq!(summary.held_by_mv_floor, 4, "offsets 2..6 are held");
}

/// A table no MV reads GCs exactly as before the floor existed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_source_table_gcs_unchanged() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        None,
        true,
        &[],
    )
    .await;

    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    age_all_snapshots(&s.pool).await;

    let summary = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    assert_eq!(
        summary.inline_rows, 6,
        "every end-capped inline row reclaimed"
    );
    assert_eq!(summary.held_by_mv_floor, 0, "no floor, nothing held");
    assert_eq!(end_capped_inline_count(&s.pool, s.tid).await, 0);
}

/// Dropping the source BYPASSES the floor: the dropped incarnation is fully
/// reclaimed even with a lagging MV registered (drop-GC must converge; the run
/// logs a warning naming the stranded MVs).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_source_bypasses_the_floor() {
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
    // The MV is barely started: offset 1 of 6 — a floor that would hold everything
    // above it if this were a live table.
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 1).await;
    let files_before = data_file_count(&s.pool, s.tid).await;
    assert!(files_before > 0, "the source landed at least one file");

    // Drop the source through the iceberg Catalog trait — the same call the
    // dropped-incarnation tests in `tests/iceberg_gc.rs:535` make.
    let ident = TableIdent::new(
        NamespaceIdent::new(s.src.schema.clone()),
        s.src.name.clone(),
    );
    s.catalog.drop_table(&ident).await.expect("drop source");
    // Age the whole history so the drop snapshot itself is past the horizon (full
    // reclaim of the dropped incarnation).
    age_all_snapshots(&s.pool).await;

    let summary = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    assert!(
        summary.data_file_rows > 0,
        "the dropped incarnation's files are reclaimed — the floor is bypassed"
    );
    assert_eq!(
        summary.held_by_mv_floor, 0,
        "held counts the LIVE incarnation only; there is none"
    );
    assert_eq!(
        data_file_count(&s.pool, s.tid).await,
        0,
        "no data_file row of the dropped incarnation survives"
    );
}

/// Deleting the MV's registration releases the floor — the documented escape hatch
/// out of a hold by a dead MV. Its watermark rows go with the def, so the held tail
/// becomes reclaimable on the next GC.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_the_mv_registration_releases_the_floor() {
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
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 3).await;
    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    age_all_snapshots(&s.pool).await;
    let first = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("first gc");
    assert_eq!(first.held_by_mv_floor, 3, "held while the MV lags");

    cp.transforms()
        .delete_transform(&TransformName("mv_a".into()))
        .await
        .expect("delete the mv registration");

    let second = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("second gc");
    assert_eq!(second.inline_rows, 3, "the dead MV's hold is released");
    assert_eq!(second.held_by_mv_floor, 0, "no reader, no floor");
    assert_eq!(end_capped_inline_count(&s.pool, s.tid).await, 0);
}

/// The over-refusal guard, on the REAL path: a flush with an MV still at 3 of 6 must
/// SUCCEED. Flush end-caps inline rows above the floor by design and re-projects those
/// SAME rows into live Parquet at the SAME `(bucket, offset)` — the rows never leave the
/// live set, so no MV can miss one. A seam that refused any end-cap at or above the floor
/// would break flush entirely; this test is what fails when someone writes that seam.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_with_a_lagging_mv_still_succeeds() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // inline = true, so the flush has rows to move.
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

    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("a flush is REFRAMING: it must not be refused by the MV floor")
        .expect("flush produced a snapshot");

    assert!(
        data_file_count(&s.pool, s.tid).await > 0,
        "flush re-projected the inline rows into live files"
    );
}
