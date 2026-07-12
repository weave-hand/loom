//! Retention-horizon guard e2e (`road-timetravel-selector-guards`, item A).
//!
//! `main.thing`: S1 lands ids {1,2}, S2 appends {3,4} (live set {1,2,3,4}). With
//! `gc_retention = ZERO` every committed snapshot has aged out, so the horizon
//! H = S2 (the catalog-global tip at guard-eval time) and:
//!   - objects `?as_of_snapshot={S1}` -> 410 (S1 < H and S1 < thing's current S2:
//!     no exemption applies to a genuinely stale read of a table that has since
//!     been rewritten).
//!   - objects `?as_of={S1 time}`     -> 410 (the guard covers both selector kinds).
//!   - objects `?as_of_snapshot={S2}` -> 200 ids {1,2,3,4}: S2 is thing's own
//!     live/current snapshot, so `at >= H` holds trivially (a resolved selector
//!     can never exceed the catalog-global tip, and `H <= current(table)`
//!     always) — the read is exactly live-equivalent, guard or no guard.
//!   - no selector -> 200 (the guard never runs on the live path).
//!   - datasets `?as_of_snapshot={S1}` -> 410; `{S2}` -> 200 with snapshot_id == S2.
//!
//! With the default (7-day) retention the same S1 selectors stay 200 (in-window).
//!
//! NOTE on the removed "quiet-table exemption" branch of
//! `ensure_within_retention`: the guard used to check
//! `at >= current_snapshot(table).id` after `at >= H` had already failed, on
//! the theory that a quiet table (unchanged since `at`) should be exempt even
//! when `H` had advanced past it. That branch was dead code and has been
//! removed -- the guard is now pure `at < H`. Reason: this repo's
//! `iceberg_mirror.snapshot` sequence (`iceberg_mirror.snapshot_seq`) is a
//! single counter shared by every table in the catalog, and
//! `Catalog::current_snapshot` resolves to the newest such id at which the
//! queried table is still live (not dropped) -- i.e. for any live table it is
//! always the catalog-global tip. `snapshot_horizon` is bounded by that same
//! tip, so `H <= current_snapshot(table).id` holds unconditionally, meaning
//! `at < H` already implies `at < current_snapshot(table).id` too -- the
//! second check could never independently flip a `Gone` verdict to `Ok`. A
//! precise per-table exemption is impossible under the global snapshot
//! sequence; it would need a distinct per-table "last write" catalog query.
//! Filed as a known limitation, not fixed here: see
//! `iss-timetravel-quiet-table-overconservative`. Every assertion below stays
//! green: the `S2 -> 200` case already holds via `at >= H` (S2 is the tip = H
//! under ZERO retention), independent of the removed branch.

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::{ControlPlane, ObjectType, Ontology, PageReq, TypeName};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
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
    (cp, eng, writer, s1, s2)
}

#[tokio::test(flavor = "multi_thread")]
async fn retention_horizon_guards_time_travel_selectors() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer, s1, s2) = setup(fx).await;
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

    // 1. `?as_of_snapshot={S1}` -> 410: S1 < H (== S2, the tip) and S1 < thing's
    //    current snapshot (S2) -- a genuinely stale read of a rewritten table.
    let (status, body) = get_with_retention(
        cp.clone(),
        eng.clone(),
        &format!("/objects/Thing?as_of_snapshot={s1}"),
        "alice",
        std::time::Duration::ZERO,
    )
    .await;
    assert_eq!(status, StatusCode::GONE, "as_of_snapshot=S1 (ZERO): {body}");

    // 2. `?as_of={S1 time}` -> 410: the guard covers the Time selector arm too.
    let (status, body) = get_with_retention(
        cp.clone(),
        eng.clone(),
        &format!("/objects/Thing?as_of={s1_time}"),
        "alice",
        std::time::Duration::ZERO,
    )
    .await;
    assert_eq!(status, StatusCode::GONE, "as_of=S1 time (ZERO): {body}");

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

    // 5. Dataset detail mirrors the object-read guard.
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
        StatusCode::GONE,
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
}
