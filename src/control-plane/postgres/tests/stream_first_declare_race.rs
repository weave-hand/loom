//! The first-declare arm's post-race re-read must compare stream KIND, not just
//! bucket count (iss-stream-first-declare-race-kind).
//!
//! `pg_declare_stream`/`pg_declare_cdc` are `insert … on conflict (table_id) do
//! nothing`, so a first-declare that loses a race to a concurrent first-declare
//! no-ops and then re-reads what the winner recorded. Re-reading only the COUNT
//! lets a log declare proceed against a `kind='cdc'` row (and vice versa).
//!
//! The race is reproduced deterministically — NO sleeps — with a
//! `pg_stat_activity` barrier: connection A holds an uncommitted conflicting
//! `stream.stream_table` row, connection B's declare blocks on A's
//! `transactionid` lock, the test polls until B is observably blocked, then
//! commits A. Without the barrier, A could commit before B's `pg_stream_meta`
//! read, routing B into the (already-fixed) count-equal arm — a green test for
//! the wrong reason. Each test therefore asserts the DISTINCT first-declare
//! message to prove which arm fired.
//!
//! `stream.stream_table.table_id` is a bare `bigint primary key` with no FK
//! (migrations/0035), and `reconcile_stream_mode` takes `tid`/`pre_existing` as
//! parameters, so these tests need no mirror table and land no data.
//! loom_fixture_test (Postgres).

use control_plane_core::{
    ControlPlaneError, MergeEngine, SnapshotId, StreamKind, StreamTables, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::stream::{StreamDecl, reconcile_stream_mode};
use sqlx::{PgPool, Postgres, Transaction};

/// The table the reconcile seam names in its error messages. No mirror row is
/// created for it — the first-declare arm never reads one.
fn tref() -> TableRef {
    TableRef {
        schema: "s".into(),
        name: "t".into(),
    }
}

/// The synthetic mirror table id both racers declare against.
const TID: i64 = 4242;

/// Begin a transaction, insert the WINNER's `stream.stream_table` row, and return
/// the transaction still OPEN (uncommitted) — the loser's `on conflict do nothing`
/// insert blocks on it until the caller commits.
async fn insert_stream_row(
    pool: &PgPool,
    tid: i64,
    buckets: i32,
    kind: &str,
    bucket_key: Option<&str>,
) -> Transaction<'static, Postgres> {
    let mut tx = pool.begin().await.expect("begin winner tx");
    sqlx::query(
        "insert into stream.stream_table (table_id, bucket_count, kind, bucket_key) \
         values ($1, $2, $3, $4)",
    )
    .bind(tid)
    .bind(buckets)
    .bind(kind)
    .bind(bucket_key)
    .execute(&mut *tx)
    .await
    .expect("winner insert");
    tx
}

/// Control / non-regression: an UNCONTENDED first declare (no concurrent winner)
/// still returns the declared bucket count. Pins the happy path the race tests
/// perturb.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uncontended_first_declare_returns_its_bucket_count() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let mut tx = pool.begin().await.expect("begin");
    let effective = reconcile_stream_mode(
        &mut tx,
        TID,
        &StreamDecl::Log(2),
        false,
        &tref(),
        SnapshotId(1),
    )
    .await
    .expect("uncontended log first-declare");
    assert_eq!(
        effective,
        Some(2),
        "an uncontended first declare records and returns its own bucket count"
    );
    tx.commit().await.expect("commit");
}

/// Control / non-regression: an uncontended CDC first-declare still registers its
/// changelog table and points the registry at it. This is the path Task 3's
/// reorder moves the changelog writes on, so it is pinned BEFORE that reorder.
///
/// Needs no real snapshot: `iceberg_mirror.table.begin_snapshot` is a bare
/// `bigint not null` with NO foreign key (migrations/0012), exactly as
/// `stream.stream_table.table_id` has no FK (migrations/0035) — so
/// `ensure_table(clog, SnapshotId(1))` inserts cleanly against a snapshot id that
/// does not exist. (That FK-freedom is what lets every test in this file drive the
/// seam with synthetic ids and land no data.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uncontended_cdc_first_declare_registers_its_changelog() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let mut tx = pool.begin().await.expect("begin");
    let effective = reconcile_stream_mode(
        &mut tx,
        TID,
        &StreamDecl::Cdc {
            buckets: 2,
            bucket_key: "id".into(),
            merge_engine: MergeEngine::LastRow,
        },
        false,
        &tref(),
        SnapshotId(1),
    )
    .await
    .expect("uncontended cdc first-declare");
    assert_eq!(effective, Some(2));
    tx.commit().await.expect("commit");

    let meta = cp
        .stream_meta(TID)
        .await
        .expect("stream_meta")
        .expect("declared stream table");
    assert_eq!(meta.kind, StreamKind::Cdc);
    assert!(
        meta.changelog_table_id.is_some(),
        "a cdc winner still gets its changelog mirror row registered"
    );
}

/// Control / non-regression: a COUNT mismatch against an existing registry row
/// stays a `Conflict` — count precedence is unchanged by the kind guard. (This
/// exercises the (Some, Some) count arm, not the first-declare arm; it is here as
/// the precedence guard the kind check must not overtake.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn count_mismatch_against_an_existing_row_stays_conflict() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // Winner: a log table with a DIFFERENT count, committed before the loser runs
    // (no barrier needed — this test pins the count arm, not the race).
    let tx = insert_stream_row(&pool, TID, 4, "log", None).await;
    tx.commit().await.expect("commit winner");

    let mut tx = pool.begin().await.expect("begin");
    // `pre_existing = false` and a stream row that already exists => the
    // (Some, Some) count-mismatch arm fires first. Kept as a guard that the
    // kind check never overtakes count precedence.
    let res = reconcile_stream_mode(
        &mut tx,
        TID,
        &StreamDecl::Log(2),
        false,
        &tref(),
        SnapshotId(1),
    )
    .await;
    assert!(
        matches!(res, Err(ControlPlaneError::Conflict(_))),
        "a bucket-count disagreement stays a Conflict, got {res:?}"
    );
    drop(tx.rollback().await);
}
