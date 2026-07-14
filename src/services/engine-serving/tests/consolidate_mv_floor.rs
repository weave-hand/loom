//! A CDC consolidate fold that would remove offsets an MV has not read SKIPS and
//! RE-ARMS — it never errors. The fold is a `Removing` end-cap (it retires every live
//! file and re-projects only the fold winners), so it is exactly the path the end-cap
//! seam refuses. Propagating that refusal would poison the queue: `RetryPolicy::Retry`
//! has no max-attempts anywhere, so a failing `stream_consolidate` job is a 60-second
//! failure drumbeat forever; `Abandon` is worse still — it leaves the shadow tier
//! permanently unfolded. So: warn, `clear_consolidate_trigger` (a write-proportional
//! re-arm, no timers), `Ok(0)`. The same posture `gc_locked` already takes against the
//! floor — hold, warn, succeed.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use control_plane_core::RunId;
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use end_cap_seed::{FlooredCdc, live_file_count, seed_floored_cdc};

/// Seed the floored CDC table AND flush it. `seed_floored_cdc` deliberately does NOT
/// flush (a sibling test needs an unflushed table to flush), but the CDC fold reads
/// framed Parquet from the base — without the flush there are no live data files and
/// every assertion below is vacuous. The flush is itself the `Reframing` case: it must
/// succeed despite the floor.
async fn seed_flushed(fx: &PgFixture) -> (PgControlPlane, FlooredCdc) {
    let (cp, db) = fx.fresh_db().await;
    let s = seed_floored_cdc(fx, &cp, &db).await;
    flush_table(&s.catalog, &s.pool, &s.table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("a CDC flush is REFRAMING and must not be refused by the MV floor")
        .expect("the flush must produce a snapshot");
    (cp, s)
}

/// `(delta_count, enqueued)` of the table's consolidate trigger. The row exists only
/// because `seed_floored_cdc` writes its delta with `consolidate_threshold: Some(1000)`
/// — with `None` `bump_consolidate_trigger` never runs and this is a `RowNotFound`.
async fn trigger(pool: &sqlx::PgPool, tid: i64) -> (i64, bool) {
    sqlx::query_as(
        "select delta_count, enqueued from iceberg_mirror.consolidate_trigger \
         where table_id = $1",
    )
    .bind(tid)
    .fetch_one(pool)
    .await
    .expect("trigger row")
}

/// A watermark row against the CDC source floors it: the fold returns `Ok(0)`, the live
/// files are untouched, and the trigger is cleared so a later write can re-enqueue.
/// It must NEVER error — the queue has no max-attempts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_floored_cdc_source_makes_the_fold_skip_and_rearm() {
    let fx = PgFixture::shared();
    let (cp, s) = seed_flushed(fx).await;

    let before = live_file_count(&s.pool, s.tid).await;
    assert!(before > 0, "the flush must have left live files");
    let (armed, _) = trigger(&s.pool, s.tid).await;
    assert!(
        armed > 0,
        "the seeded delta must have bumped the trigger, or the reset below proves nothing"
    );

    let folded = engine_serving::consolidate_table(&cp, &s.catalog, &s.pool, &s.table)
        .await
        .expect("a blocked fold must SUCCEED as a no-op — never error, never abandon");
    assert_eq!(folded, 0, "a blocked fold folds nothing");
    assert_eq!(
        live_file_count(&s.pool, s.tid).await,
        before,
        "a blocked fold must not end-cap a single file"
    );

    let (count, enqueued) = trigger(&s.pool, s.tid).await;
    assert_eq!(count, 0, "the trigger must be reset");
    assert!(
        !enqueued,
        "the trigger must be disarmed so a later write can re-enqueue"
    );
}

/// Non-regression: with no floor, the CDC fold runs exactly as it did before the seam.
/// Dropping the ghost watermark row leaves the source with no reader at all (no
/// registered micro-batch def names it either), so `mv_floor` is `None` and the guard
/// is a no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unfloored_cdc_source_folds_unchanged() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let s = seed_floored_cdc(fx, &cp, &db).await;
    sqlx::query("delete from stream.mv_watermark where source_table_id = $1")
        .bind(s.tid)
        .execute(&s.pool)
        .await
        .expect("drop the ghost watermark — the source's only reader");
    flush_table(&s.catalog, &s.pool, &s.table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush")
        .expect("the flush must produce a snapshot");

    let folded = engine_serving::consolidate_table(&cp, &s.catalog, &s.pool, &s.table)
        .await
        .expect("consolidate_table");
    assert!(folded > 0, "an unfloored source folds normally");

    let (count, enqueued) = trigger(&s.pool, s.tid).await;
    assert_eq!(count, 0, "a completed fold clears the trigger too");
    assert!(!enqueued);
}
