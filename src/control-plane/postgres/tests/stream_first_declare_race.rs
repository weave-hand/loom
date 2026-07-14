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
/// insert blocks on it until the caller commits. `merge_engine` is `None` for the
/// pre-existing callers (the column then takes its `'last_row'` default per
/// migrations/0040); `Some(token)` lets a merge_engine-mismatch race pin the
/// winner to a SPECIFIC engine.
async fn insert_stream_row(
    pool: &PgPool,
    tid: i64,
    buckets: i32,
    kind: &str,
    bucket_key: Option<&str>,
    merge_engine: Option<&str>,
) -> Transaction<'static, Postgres> {
    let mut tx = pool.begin().await.expect("begin winner tx");
    // A single insert, `merge_engine` bound as `Option<&str>`: `coalesce($5,
    // 'last_row')` reproduces the column's own default (migrations/0040) when the
    // caller passes `None`, so there is exactly one INSERT statement rather than
    // two near-identical branches.
    sqlx::query(
        "insert into stream.stream_table \
         (table_id, bucket_count, kind, bucket_key, merge_engine) \
         values ($1, $2, $3, $4, coalesce($5, 'last_row'))",
    )
    .bind(tid)
    .bind(buckets)
    .bind(kind)
    .bind(bucket_key)
    .bind(merge_engine)
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
    let tx = insert_stream_row(&pool, TID, 4, "log", None, None).await;
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

/// Barrier: block until some backend in this database is waiting on another
/// transaction's lock inside a `stream.stream_table` insert — i.e. the loser's
/// `insert … on conflict do nothing` is blocked on the uncommitted winner.
/// `wait_event_type='Lock' / wait_event='transactionid'` is exactly what that
/// insert waits on against an uncommitted conflicting row (verified against the
/// pinned PG 17.9). Polls `pg_stat_activity`; panics rather than hanging.
///
/// Load-bearing: only once the loser is observably blocked may the caller commit
/// the winner. Commit it any earlier and the loser's `pg_stream_meta` read at the
/// top of `reconcile_stream_mode` sees the winner's row, routing it into the
/// (already-guarded) count-equal arm — a green test for the wrong reason.
///
/// Reads another backend's `query` column, which Postgres exposes only to a
/// superuser or a `pg_read_all_stats` member. The fixture connects as `postgres`
/// (superuser — `fixture.rs:402`), so this holds; if that ever changes, the poll
/// would spin and panic. The privilege-free equivalent, if it comes to that, is
/// `where cardinality(pg_blocking_pids(pid)) > 0` (in these fresh single-purpose
/// databases only the loser can be blocked, so it stays deterministic).
async fn await_declare_blocked(pool: &PgPool) {
    for _ in 0..600 {
        let blocked: i64 = sqlx::query_scalar(
            "select count(*) from pg_stat_activity \
             where datname = current_database() \
               and wait_event_type = 'Lock' \
               and wait_event = 'transactionid' \
               and query like 'insert into stream.stream_table%'",
        )
        .fetch_one(pool)
        .await
        .expect("pg_stat_activity barrier probe");
        if blocked >= 1 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the losing declare never blocked on the winner's transactionid lock");
}

/// THE DEFECT: a log first-declare that loses the `on conflict do nothing` race
/// to a CDC first-declare with the SAME bucket count must be rejected. Before the
/// fix the re-read compares only the count, so the log write proceeds against a
/// `kind='cdc'` row — log framing stamped into CDC storage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn log_first_declare_losing_to_cdc_winner_is_validation_error() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // A: the CDC winner, held UNCOMMITTED.
    let winner = insert_stream_row(&pool, TID, 2, "cdc", Some("id"), None).await;

    // B: the log loser. Its `pg_stream_meta` read returns None (A is uncommitted), so
    // it takes the (Some, None) first-declare arm, and its `pg_declare_stream` insert
    // then BLOCKS on A's transactionid lock.
    let pool_b = pool.clone();
    let loser = tokio::spawn(async move {
        let mut tx = pool_b.begin().await.expect("begin loser tx");
        let res = reconcile_stream_mode(
            &mut tx,
            TID,
            &StreamDecl::Log(2),
            false,
            &tref(),
            SnapshotId(1),
        )
        .await;
        drop(tx.rollback().await);
        res
    });

    await_declare_blocked(&pool).await;
    winner.commit().await.expect("commit winner");

    let res = loser.await.expect("join loser");
    assert!(
        matches!(&res, Err(ControlPlaneError::Validation(msg))
                 if msg.contains("declared concurrently with a different stream kind")),
        "a log first-declare losing to a cdc winner with the same count must be rejected \
         by the FIRST-DECLARE arm, got {res:?}"
    );
}

/// Symmetric: a CDC first-declare losing to a LOG winner with the same count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cdc_first_declare_losing_to_log_winner_is_validation_error() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let winner = insert_stream_row(&pool, TID, 2, "log", None, None).await;

    let pool_b = pool.clone();
    let loser = tokio::spawn(async move {
        let mut tx = pool_b.begin().await.expect("begin loser tx");
        let res = reconcile_stream_mode(
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
        .await;
        drop(tx.rollback().await);
        res
    });

    await_declare_blocked(&pool).await;
    winner.commit().await.expect("commit winner");

    let res = loser.await.expect("join loser");
    assert!(
        matches!(&res, Err(ControlPlaneError::Validation(msg))
                 if msg.contains("declared concurrently with a different stream kind")),
        "a cdc first-declare losing to a log winner with the same count must be rejected \
         by the FIRST-DECLARE arm, got {res:?}"
    );
}

/// THE DEFECT (finding 1): the re-read compared `bucket_count` and `kind` but
/// NOT `merge_engine` — so a `LastRow` CDC first-declare losing a race to a
/// `Versioned` CDC winner with the SAME bucket count and bucket_key still
/// slipped through: no-op insert, count matches, kind matches ('cdc' both
/// sides), and the loser then proceeds under the winner's fold semantics it
/// never requested. Before the fix this asserts `Ok(Some(2))` (accepted), not a
/// barrier timeout or a `Conflict` from the count-equal arm.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cdc_first_declare_losing_to_different_merge_engine_winner_is_conflict() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // A: the Versioned winner, held UNCOMMITTED. Same count and bucket_key as
    // the loser below, so only the merge_engine guard can distinguish them.
    let winner = insert_stream_row(&pool, TID, 2, "cdc", Some("id"), Some("versioned")).await;

    // B: the LastRow loser.
    let pool_b = pool.clone();
    let loser = tokio::spawn(async move {
        let mut tx = pool_b.begin().await.expect("begin loser tx");
        let res = reconcile_stream_mode(
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
        .await;
        drop(tx.rollback().await);
        res
    });

    await_declare_blocked(&pool).await;
    winner.commit().await.expect("commit winner");

    let res = loser.await.expect("join loser");
    assert!(
        matches!(&res, Err(ControlPlaneError::Conflict(msg)) if msg.contains("declared concurrently")),
        "a cdc first-declare losing to a winner with a DIFFERENT merge_engine and the \
         same count/bucket_key must be rejected by the FIRST-DECLARE arm, got {res:?}"
    );
}

/// THE DEFECT (finding 1): the re-read also never compared `bucket_key` — a
/// CDC first-declare losing a race to a same-count, same-engine winner keyed on
/// a DIFFERENT identity column still slipped through. Before the fix this
/// asserts `Ok(Some(2))`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cdc_first_declare_losing_to_different_bucket_key_winner_is_validation_error() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // A: the winner, keyed on "user_id", held UNCOMMITTED. Same count and
    // (default) merge_engine as the loser, so only the bucket_key guard can
    // distinguish them.
    let winner = insert_stream_row(&pool, TID, 2, "cdc", Some("user_id"), None).await;

    // B: the loser, keyed on "id".
    let pool_b = pool.clone();
    let loser = tokio::spawn(async move {
        let mut tx = pool_b.begin().await.expect("begin loser tx");
        let res = reconcile_stream_mode(
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
        .await;
        drop(tx.rollback().await);
        res
    });

    await_declare_blocked(&pool).await;
    winner.commit().await.expect("commit winner");

    let res = loser.await.expect("join loser");
    assert!(
        matches!(&res, Err(ControlPlaneError::Validation(msg)) if msg.contains("different bucket_key")),
        "a cdc first-declare losing to a winner with a DIFFERENT bucket_key and the \
         same count/merge_engine must be rejected by the FIRST-DECLARE arm, got {res:?}"
    );
}

/// THE DEFECT (follow-up finding 1): the COUNT-EQUAL `(Some, Some)` redeclare arm
/// checks `bucket_count`, `kind` and `merge_engine` but — asymmetrically with the
/// first-declare arm above — NOT `bucket_key`. A `Cdc` redeclare of an
/// already-declared CDC table with the SAME bucket count and merge_engine but a
/// DIFFERENT bucket_key is silently accepted, and the declared intent is then
/// ignored (`inline_append` re-reads the registry's bucket_key, so rows are
/// bucketed by the FIRST declarer's key, not the redeclare's). No concurrency
/// needed: a plain sequential declare-then-redeclare reaches the count-equal arm.
/// Before the fix this asserts `Ok(Some(2))` (accepted).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cdc_redeclare_with_different_bucket_key_same_count_and_engine_is_validation_error() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // First declare: cdc, bucket_key "id", default (last_row) engine, committed.
    let mut tx = pool.begin().await.expect("begin first declare");
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
    .expect("first cdc declare");
    assert_eq!(effective, Some(2));
    tx.commit().await.expect("commit first declare");

    // Redeclare: same count, same engine, but a DIFFERENT bucket_key. This is a
    // plain sequential call reaching the count-equal `(Some, Some)` arm (no
    // concurrency/barrier needed).
    let mut tx = pool.begin().await.expect("begin redeclare");
    let res = reconcile_stream_mode(
        &mut tx,
        TID,
        &StreamDecl::Cdc {
            buckets: 2,
            bucket_key: "other".into(),
            merge_engine: MergeEngine::LastRow,
        },
        true,
        &tref(),
        SnapshotId(2),
    )
    .await;
    drop(tx.rollback().await);
    assert!(
        matches!(&res, Err(ControlPlaneError::Validation(msg)) if msg.contains("different bucket_key")),
        "a cdc redeclare with the same count/engine but a DIFFERENT bucket_key must be \
         rejected by the COUNT-EQUAL arm, got {res:?}"
    );
}
