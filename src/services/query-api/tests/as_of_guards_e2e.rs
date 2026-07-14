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

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use axum::http::StatusCode;
use control_plane_core::{ColumnSpec, ControlPlane, ObjectType, Ontology, PageReq, TypeName};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::overwrite_parquet_snapshot;
use e2e_support::{
    InProcessServingEngine, get, get_with_retention, grant_read, ids_i64 as ids, prop,
    subject_with_role, tref,
};

/// Seed `Thing(id Long identity, name String)` in `main.thing` via two appends:
/// S1 lands ids {1,2}, S2 appends ids {3,4} (cumulative -> live set {1,2,3,4}).
/// Returns the wired control plane + serving engine + (S1, S2) snapshot ids; the
/// caller MUST keep the `IcebergWriter` alive (its TempDir holds the Parquet
/// read). Mirrors `as_of_objects_e2e::setup` exactly.
async fn setup(
    fx: &PgFixture,
) -> (
    PgControlPlane,
    InProcessServingEngine,
    IcebergWriter,
    i64,
    i64,
    sqlx::PgPool,
) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let thing = tref("main", "thing");
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), true),
    ];

    // S1: ids {1,2}.
    let s1 = writer
        .seed_arrays(
            "main",
            "thing",
            &cols,
            &[SeedCol::Long(vec![1, 2]), SeedCol::Str(vec!["a", "b"])],
        )
        .await;

    // S2: append ids {3,4} -> live set is now {1,2,3,4}.
    let s2 = writer
        .seed_arrays(
            "main",
            "thing",
            &cols,
            &[SeedCol::Long(vec![3, 4]), SeedCol::Str(vec!["c", "d"])],
        )
        .await;

    cp.define_type(ObjectType {
        name: TypeName("Thing".into()),
        properties: vec![prop("id", "Long", true), prop("name", "String", false)],
        derived: vec![],
        table: thing,
        identity: Some("id".into()),
        version: None,
    })
    .await
    .unwrap();

    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    (cp, eng, writer, s1, s2, pool)
}

#[tokio::test(flavor = "multi_thread")]
async fn retention_horizon_guards_time_travel_selectors() {
    let fx = PgFixture::shared();
    let (cp, eng, writer, s1, s2, pool) = setup(fx).await;
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

    // 1. `?as_of_snapshot={S1}` -> 200 ids {1,2}: S1 < H, but S1 is APPEND-ONLY --
    //    nothing visible at S1 was ever end-capped, so the read is provably complete
    //    forever. This is the case the OLD `at < H` proxy wrongly refused.
    let (status, body) = get_with_retention(
        cp.clone(),
        eng.clone(),
        &format!("/objects/Thing?as_of_snapshot={s1}"),
        "alice",
        std::time::Duration::ZERO,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "as_of_snapshot=S1 (ZERO): {body}");
    assert_eq!(ids(&body), vec![1, 2], "as_of_snapshot=S1 (ZERO): {body}");

    // 2. `?as_of={S1 time}` -> 200: the guard covers the Time selector arm identically.
    let (status, body) = get_with_retention(
        cp.clone(),
        eng.clone(),
        &format!("/objects/Thing?as_of={s1_time}"),
        "alice",
        std::time::Duration::ZERO,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "as_of=S1 time (ZERO): {body}");
    assert_eq!(ids(&body), vec![1, 2], "as_of=S1 time (ZERO): {body}");

    // 3. `?as_of_snapshot={S2}` -> 200 ids {1,2,3,4}: S2 is thing's own live/tip
    //    snapshot, so `at >= H` holds even under ZERO retention (a resolved
    //    selector can never exceed the tip, and H is bounded by it).
    let (status, body) = get_with_retention(
        cp.clone(),
        eng.clone(),
        &format!("/objects/Thing?as_of_snapshot={s2}"),
        "alice",
        std::time::Duration::ZERO,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "as_of_snapshot=S2 (ZERO): {body}");
    assert_eq!(
        ids(&body),
        vec![1, 2, 3, 4],
        "as_of_snapshot=S2 (ZERO): {body}"
    );

    // 4. No selector -> 200: the live path never runs the horizon guard.
    let (status, body) = get_with_retention(
        cp.clone(),
        eng.clone(),
        "/objects/Thing",
        "alice",
        std::time::Duration::ZERO,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "live read (ZERO): {body}");
    assert_eq!(ids(&body), vec![1, 2, 3, 4], "live read (ZERO): {body}");

    // 5. Dataset detail mirrors the object-read guard: S1 is append-only -> 200.
    let (status, body) = get_with_retention(
        cp.clone(),
        eng.clone(),
        &format!("/datasets/main/thing?as_of_snapshot={s1}"),
        "alice",
        std::time::Duration::ZERO,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "dataset as_of_snapshot=S1 (ZERO): {body}"
    );
    assert_eq!(
        body["snapshot_id"].as_i64(),
        Some(s1),
        "dataset as_of_snapshot=S1 (ZERO): {body}"
    );

    let (status, body) = get_with_retention(
        cp.clone(),
        eng.clone(),
        &format!("/datasets/main/thing?as_of_snapshot={s2}"),
        "alice",
        std::time::Duration::ZERO,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "dataset as_of_snapshot=S2 (ZERO): {body}"
    );
    assert_eq!(
        body["snapshot_id"].as_i64(),
        Some(s2),
        "dataset as_of_snapshot=S2 (ZERO): {body}"
    );

    // 6. Default (7-day) retention: the same S1 selector stays in-window -> 200.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        &format!("/objects/Thing?as_of_snapshot={s1}"),
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "as_of_snapshot=S1 (default): {body}"
    );
    assert_eq!(
        ids(&body),
        vec![1, 2],
        "as_of_snapshot=S1 (default): {body}"
    );

    // 7. A genuinely end-capped read still 410s. Overwrite `main.thing` (this REPLACES the
    //    live set, end-capping S1's and S2's files at S3) and read at S1 under ZERO
    //    retention: S1's rows are now eligible for reclaim, so the read is refused. This is
    //    the case the file previously only CLAIMED to cover — `seed_arrays` appends, so
    //    assertion 1 above was never testing a rewritten table at all.
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
        &pool,
        &writer.sql_catalog().await,
        &thing,
        &cols,
        vec![replacement],
        None,
        &[],
    )
    .await
    .expect("overwrite");

    let (status, body) = get_with_retention(
        cp.clone(),
        eng.clone(),
        &format!("/objects/Thing?as_of_snapshot={s1}"),
        "alice",
        std::time::Duration::ZERO,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::GONE,
        "as_of_snapshot=S1 after a REAL rewrite (ZERO): {body}"
    );
}
