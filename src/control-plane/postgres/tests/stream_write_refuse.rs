//! The write refusals that dissolve `iss-end-cap-ignores-mv-floor`'s lossy end-cap paths
//! at the source rather than guarding them:
//!
//! * **E6** — `overwrite_parquet_snapshot` (and its empty-batch `overwrite_truncate`
//!   branch) over a DECLARED STREAM table. The only production caller that legitimately
//!   overwrites one is the CDC consolidate fold, now on `overwrite_stream_base`.
//!
//! loom_fixture_test (Postgres).

use control_plane_core::ControlPlaneError;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::overwrite_parquet_snapshot;
use end_cap_seed::{batch, columns, lineage, live_file_count, seed_source};

/// E6, the non-empty branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_of_a_declared_stream_table_is_refused() {
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
        &[],
    )
    .await;
    let before = live_file_count(&s.pool, s.tid).await;

    let (_, batches) = batch(2);
    let err = overwrite_parquet_snapshot(
        &s.pool,
        &s.catalog,
        &s.src,
        &columns(),
        batches,
        Some(&lineage(&s.src)),
        &[],
    )
    .await
    .expect_err("overwrite of a declared stream table must be refused");

    match err {
        ControlPlaneError::Validation(m) => assert!(
            m.starts_with("stream-table target refused:"),
            "unexpected message: {m}"
        ),
        other => panic!("expected Validation, got {other:?}"),
    }
    assert_eq!(
        live_file_count(&s.pool, s.tid).await,
        before,
        "a refused overwrite must leave the live set untouched"
    );
}

/// E6, the WORST case: the empty-batch `overwrite_truncate` branch, which before this
/// fix end-capped every live data file AND every live inline row with no stream check of
/// any kind — a delete-all that silently destroys the whole offset range.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncate_of_a_declared_stream_table_is_refused() {
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
        &[],
    )
    .await;
    let before = live_file_count(&s.pool, s.tid).await;
    assert!(before > 0, "seed must leave live files");

    let err = overwrite_parquet_snapshot(
        &s.pool,
        &s.catalog,
        &s.src,
        &columns(),
        vec![], // the truncate branch
        Some(&lineage(&s.src)),
        &[],
    )
    .await
    .expect_err("truncate of a declared stream table must be refused");

    match err {
        ControlPlaneError::Validation(m) => assert!(
            m.starts_with("stream-table target refused:"),
            "unexpected message: {m}"
        ),
        other => panic!("expected Validation, got {other:?}"),
    }
    assert_eq!(
        live_file_count(&s.pool, s.tid).await,
        before,
        "a refused truncate must leave every live file in place"
    );
}

/// Non-regression: a PLAIN (undeclared) table overwrites exactly as before — the check
/// that the refusal is scoped and did not just break every overwrite in the tree.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_of_a_plain_table_still_succeeds() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // buckets = None => a plain, undeclared batch table.
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        None,
        false,
        &[],
    )
    .await;

    let (_, batches) = batch(2);
    overwrite_parquet_snapshot(
        &s.pool,
        &s.catalog,
        &s.src,
        &columns(),
        batches,
        Some(&lineage(&s.src)),
        &[],
    )
    .await
    .expect("a plain table overwrites unchanged");
}
