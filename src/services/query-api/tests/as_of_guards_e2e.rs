//! Retention-horizon guard e2e (`road-timetravel-selector-guards`, item A;
//! `iss-timetravel-quiet-table-overconservative`).
//!
//! `main.thing`: S1 lands ids {1,2}, S2 appends {3,4} (live set {1,2,3,4}) — both via
//! `seed_arrays`, which is an APPEND, so nothing is ever end-capped by `setup` alone. With
//! `gc_retention = ZERO` every committed snapshot has aged out, so the horizon H = S2 (the
//! catalog-global tip at guard-eval time) and:
//!   - objects `?as_of_snapshot={S1}` -> 200 ids {1,2}: S1 < H, but S1's row is
//!     APPEND-ONLY — nothing visible at S1 was ever end-capped, so the read is provably
//!     complete forever (`Catalog::snapshot_intact`). This is the exact case the OLD `at <
//!     H` proxy wrongly 410'd.
//!   - objects `?as_of={S1 time}`     -> 200, same reasoning (the guard covers both
//!     selector kinds identically).
//!   - objects `?as_of_snapshot={S2}` -> 200 ids {1,2,3,4}: S2 is thing's own
//!     live/current snapshot, so `at >= H` holds trivially (a resolved selector
//!     can never exceed the catalog-global tip, and `H <= current(table)`
//!     always) — the read is exactly live-equivalent, guard or no guard.
//!   - no selector -> 200 (the guard never runs on the live path).
//!   - datasets `?as_of_snapshot={S1}` -> 200 snapshot_id == S1; `{S2}` -> 200 snapshot_id == S2.
//!   - a GENUINELY end-capped read (assertion 7: overwrite `main.thing`, end-capping S1's
//!     and S2's files) still 410s — the guard's refusal path has real e2e coverage, not
//!     just the (now-corrected) false positive this file used to assert.
//!
//! With the default (7-day) retention the same S1 selectors stay 200 (in-window; unchanged
//! by the fix, since `at >= H` already holds).
//!
//! This file's prose and its assertions used to be factually wrong about its own fixture:
//! it claimed S1 was "a genuinely stale read of a table that has since been rewritten", but
//! `seed_arrays` -> `append_batches` is an APPEND, not a rewrite — nothing is ever
//! end-capped by `setup`. The guard rewire (`ensure_within_retention` now delegates to
//! `Catalog::snapshot_intact` below the horizon, instead of refusing everything below it)
//! corrects that: three assertions below flip from 410 to 200. Assertion 7 is new,
//! constructed with a REAL overwrite, so the refusal path keeps genuine e2e coverage.
//!
//! See `postgres/tests/timetravel_intact.rs` for the precise catalog-level pinning of
//! this predicate, including `truncate_is_not_quiet` — the reason a bare "quiet table"
//! (`no writes since S`) exemption is UNSOUND and was never the fix here.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use axum::http::StatusCode;
use control_plane_core::{ColumnSpec, ControlPlane, PageReq, TableRef};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_landing::overwrite_parquet_snapshot;
use e2e_support::{
    InProcessServingEngine, TEST_GC_RETENTION, get_with_retention, grant_read, ids_i64 as ids,
    subject_with_role, tref, two_batch_thing_setup,
};

/// Overwrite `main.thing` with a single replacement row `{id: 9, name: "z"}` — a REAL
/// rewrite that end-caps every existing row's files. `two_batch_thing_setup`'s own
/// seeding only ever APPENDS, so this is the one place in the file that actually
/// rewrites, backing assertion 7's genuinely end-capped read.
async fn overwrite_thing_with_single_row(
    pool: &sqlx::PgPool,
    writer: &IcebergWriter,
    thing: &TableRef,
) {
    let cols = [
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "name".into(),
            ty: "string".into(),
            nullable: true,
        },
    ];
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let replacement = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int64Array::from(vec![9_i64])),
            Arc::new(StringArray::from(vec!["z"])),
        ],
    )
    .expect("replacement batch");

    overwrite_parquet_snapshot(
        pool,
        &writer.sql_catalog().await,
        thing,
        &cols,
        vec![replacement],
        None,
        &[],
    )
    .await
    .expect("overwrite");
}

/// Drive a `GET /objects/Thing...` (or `/datasets/...`) read at `retention` and assert its
/// status. When `want_status` is 200, also assert the returned object-set id list; callers
/// checking a dataset-detail body instead read `body` themselves.
async fn assert_status_and_ids(
    cp: &Arc<PgControlPlane>,
    eng: &Arc<InProcessServingEngine>,
    uri: &str,
    retention: Duration,
    want_status: StatusCode,
    want_ids: &[i64],
    label: &str,
) {
    let (status, body) = get_with_retention(cp.clone(), eng.clone(), uri, "alice", retention).await;
    assert_eq!(status, want_status, "{label}: {body}");
    if want_status == StatusCode::OK {
        assert_eq!(ids(&body), want_ids, "{label}: {body}");
    }
}

/// Drive a `GET /datasets/main/thing...` read at `retention` and assert its status is 200
/// with the given `snapshot_id`.
async fn assert_dataset_snapshot(
    cp: &Arc<PgControlPlane>,
    eng: &Arc<InProcessServingEngine>,
    uri: &str,
    retention: Duration,
    want_snapshot: i64,
    label: &str,
) {
    let (status, body) = get_with_retention(cp.clone(), eng.clone(), uri, "alice", retention).await;
    assert_eq!(status, StatusCode::OK, "{label}: {body}");
    assert_eq!(
        body["snapshot_id"].as_i64(),
        Some(want_snapshot),
        "{label}: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn retention_horizon_guards_time_travel_selectors() {
    let fx = PgFixture::shared();
    let (cp, eng, writer, s1, s2, pool) = two_batch_thing_setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Thing").await;

    // Look up S1's wall-clock time via the catalog, for the `?as_of=` (RFC3339) case.
    let thing = tref("main", "thing");
    let page = cp
        .catalog()
        .snapshots(&thing, PageReq::unbounded())
        .await
        .expect("snapshots");
    let s1_snap = page
        .items
        .iter()
        .find(|s| s.id.0 == s1)
        .expect("S1 present in snapshot history");
    let s1_time = s1_snap
        .time
        .format(&time::format_description::well_known::Rfc3339)
        .expect("format S1 time as RFC3339");

    // -- Duration::ZERO retention: every committed snapshot has aged out --
    let zero = Duration::ZERO;

    // 1. `?as_of_snapshot={S1}` -> 200 ids {1,2}: S1 < H, but S1 is APPEND-ONLY --
    //    nothing visible at S1 was ever end-capped, so the read is provably complete
    //    forever. This is the case the OLD `at < H` proxy wrongly refused.
    assert_status_and_ids(
        &cp,
        &eng,
        &format!("/objects/Thing?as_of_snapshot={s1}"),
        zero,
        StatusCode::OK,
        &[1, 2],
        "as_of_snapshot=S1 (ZERO)",
    )
    .await;

    // 2. `?as_of={S1 time}` -> 200: the guard covers the Time selector arm identically.
    assert_status_and_ids(
        &cp,
        &eng,
        &format!("/objects/Thing?as_of={s1_time}"),
        zero,
        StatusCode::OK,
        &[1, 2],
        "as_of=S1 time (ZERO)",
    )
    .await;

    // 3. `?as_of_snapshot={S2}` -> 200 ids {1,2,3,4}: S2 is thing's own live/tip
    //    snapshot, so `at >= H` holds even under ZERO retention (a resolved
    //    selector can never exceed the tip, and H is bounded by it).
    assert_status_and_ids(
        &cp,
        &eng,
        &format!("/objects/Thing?as_of_snapshot={s2}"),
        zero,
        StatusCode::OK,
        &[1, 2, 3, 4],
        "as_of_snapshot=S2 (ZERO)",
    )
    .await;

    // 4. No selector -> 200: the live path never runs the horizon guard.
    assert_status_and_ids(
        &cp,
        &eng,
        "/objects/Thing",
        zero,
        StatusCode::OK,
        &[1, 2, 3, 4],
        "live read (ZERO)",
    )
    .await;

    // 5. Dataset detail mirrors the object-read guard: S1 and S2 are both append-only ->
    //    200, each reporting its own snapshot_id.
    assert_dataset_snapshot(
        &cp,
        &eng,
        &format!("/datasets/main/thing?as_of_snapshot={s1}"),
        zero,
        s1,
        "dataset as_of_snapshot=S1 (ZERO)",
    )
    .await;
    assert_dataset_snapshot(
        &cp,
        &eng,
        &format!("/datasets/main/thing?as_of_snapshot={s2}"),
        zero,
        s2,
        "dataset as_of_snapshot=S2 (ZERO)",
    )
    .await;

    // 6. Default (7-day) retention: the same S1 selector stays in-window -> 200.
    assert_status_and_ids(
        &cp,
        &eng,
        &format!("/objects/Thing?as_of_snapshot={s1}"),
        TEST_GC_RETENTION,
        StatusCode::OK,
        &[1, 2],
        "as_of_snapshot=S1 (default)",
    )
    .await;

    // 7. A genuinely end-capped read still 410s. Overwrite `main.thing` (this REPLACES the
    //    live set, end-capping S1's and S2's files at S3) and read at S1 under ZERO
    //    retention: S1's rows are now eligible for reclaim, so the read is refused. This is
    //    the case the file previously only CLAIMED to cover — `seed_arrays` appends, so
    //    assertion 1 above was never testing a rewritten table at all.
    overwrite_thing_with_single_row(&pool, &writer, &thing).await;

    assert_status_and_ids(
        &cp,
        &eng,
        &format!("/objects/Thing?as_of_snapshot={s1}"),
        zero,
        StatusCode::GONE,
        &[],
        "as_of_snapshot=S1 after a REAL rewrite (ZERO)",
    )
    .await;
}
