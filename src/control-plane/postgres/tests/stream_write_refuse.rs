//! The write refusals that dissolve `iss-end-cap-ignores-mv-floor`'s lossy end-cap paths
//! at the source rather than guarding them:
//!
//! * **E6** — `overwrite_parquet_snapshot` (and its empty-batch `overwrite_truncate`
//!   branch) over a DECLARED STREAM table. The only production caller that legitimately
//!   overwrites one is the CDC consolidate fold, now on `overwrite_stream_base`.
//!
//! * **E7** — `write_inline_delta` (a typed UPDATE/DELETE) over a DECLARED LOG table.
//!   All four routes into that state converge on the write, so the write is where it
//!   is refused.
//!
//! loom_fixture_test (Postgres).

use control_plane_core::{ColumnSpec, ControlPlaneError, StreamTables};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_inline::{has_shadow, write_inline_delta};
use control_plane_postgres::iceberg_landing::overwrite_parquet_snapshot;
use end_cap_seed::{batch, columns, id_only_batch, lineage, live_file_count, seed_source};

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

/// The one-cell `(id)` spec a typed DELETE lowers to.
fn id_specs() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// Drive the real `write_inline_delta` with a one-cell `(id)` tombstone batch — exactly
/// what a typed DELETE lowers to — and assert it is refused with the stable prefix and
/// that `has_shadow` was NEVER set.
///
/// `has_shadow` is the load-bearing assertion: it is what would hand the table to
/// `consolidate_table`'s COW arm, AND it suppresses the non-CDC byte-trigger flush, so a
/// table wedged with the flag set would also grow its inline tier without bound.
async fn assert_typed_delete_refused(s: &end_cap_seed::Seeded) {
    let err = write_inline_delta(
        &s.pool,
        &s.src,
        &id_specs(),
        "id",
        true, // tombstone
        &id_only_batch(3),
        None, // no before-image (non-CDC)
        lineage(&s.src),
        0,    // CAS witness: no prior inline row for this id
        None, // no consolidate threshold
        &[],
    )
    .await
    .expect_err("a typed DELETE against a declared log table must be refused");

    match err {
        ControlPlaneError::Validation(m) => assert!(
            m.starts_with("stream-table target refused:"),
            "unexpected message: {m}"
        ),
        other => panic!("expected Validation, got {other:?}"),
    }

    // The refusal is IN-TX: nothing was written and `has_shadow` was never set.
    let mut conn = s.pool.acquire().await.expect("conn");
    assert!(
        !has_shadow(&mut conn, s.tid).await.expect("has_shadow"),
        "a refused mutation must not set has_shadow"
    );
}

/// E7, the route that actually reaches the COW fold: a table LANDED PLAIN and declared a
/// log stream afterwards (`define-then-declare`). Its mirror column set carries no framing,
/// so `ensure_inline_schema` is happy and — before this fix — the typed DELETE SUCCEEDED:
/// it wrote an unframed delta row (NULL loom_bucket/loom_offset), set `has_shadow`, and
/// handed the table to `consolidate_table`'s COW arm, which folds an offset-framed event
/// log by identity — end-capping every live file and re-projecting only the fold winners,
/// destroying offsets no MV has read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_mutation_of_a_table_declared_log_after_landing_is_refused() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // buckets = None => landed as a PLAIN table (no framing columns in the mirror)...
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
    // ...and only THEN declared a log stream.
    cp.declare_stream(s.tid, 1).await.expect("declare_stream");

    assert_typed_delete_refused(&s).await;
}

/// E7, the base-bound route: a table declared a log stream AT LAND time. This shape never
/// reached the COW fold — its mirror column set already carries the framing columns, so
/// `full_live_column_specs` handed `inline_ddl` a list holding `loom_change_kind` and the
/// create-table died with `column "loom_change_kind" specified more than once` (a Backend
/// 500). Refusing before `ensure_inline_schema` turns that into the same clean 422 as the
/// route above, rather than leaving one of the four routes surfacing as an engine fault.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_mutation_of_a_table_declared_log_at_land_is_refused() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // buckets = Some(1) => a DECLARED LOG stream table from the first write.
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

    assert_typed_delete_refused(&s).await;
}
