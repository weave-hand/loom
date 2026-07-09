//! `GET /objects/{type}` and `GET /datasets/{schema}/{table}` time-travel selector
//! e2e: `?as_of_snapshot=` / `?as_of=`.
//!
//! Seeds a single `Thing(id Long, name String)` table via TWO appends (S1: ids
//! {1,2}, S2 appends {3,4} so the live/S2 set is {1,2,3,4}), then proves:
//!   - `?as_of_snapshot={S1}` -> only the first-write ids {1,2}.
//!   - no selector -> the full/live id set {1,2,3,4}.
//!   - `?as_of={rfc3339 of S1}` -> the first-write ids {1,2}.
//!   - `?as_of=x&as_of_snapshot=1` (both given) -> 400.
//!   - `?as_of_snapshot=0` (before the table's first snapshot -> never live) -> 404.
//!     (A snapshot id ABOVE the current max is deliberately not used here: `schema`'s
//!     liveness gate treats a currently-live table's version range as open-ended, so an
//!     id past the max still resolves as "live" — only an id before the table's own
//!     `begin_snapshot` is reliably never-live.)
//!   - `?as_of=not-a-date` -> 400.
//!
//! Dataset-detail (`GET /datasets/main/thing`) assertions reuse the same fixture:
//!   - `?as_of_snapshot={S1}` -> `snapshot_id == S1`.
//!   - no selector -> `snapshot_id == S2` (the current snapshot).
//!   - `?as_of_snapshot=999999` (non-existent id) -> 404 (dataset detail resolves via
//!     `snapshots().find`, unlike the object-read path's liveness-range check, so an
//!     id that never appears in the table's snapshot history 404s directly).
//!   - `?as_of=bad` -> 400.

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::{ControlPlane, ObjectType, Ontology, PageReq, TypeName};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{
    InProcessServingEngine, get, grant_read, ids_i64 as ids, prop, subject_with_role, tref,
};

/// Seed `Thing(id Long identity, name String)` in `main.thing` via two appends:
/// S1 lands ids {1,2}, S2 appends ids {3,4} (cumulative -> live set {1,2,3,4}).
/// Returns the wired control plane + serving engine + (S1, S2) snapshot ids; the
/// caller MUST keep the `IcebergWriter` alive (its TempDir holds the Parquet read).
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
async fn as_of_selectors_resolve_and_apply() {
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

    // `?as_of_snapshot={S1}` -> only the first-write ids.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        &format!("/objects/Thing?as_of_snapshot={s1}"),
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "as_of_snapshot=S1: {body}");
    assert_eq!(ids(&body), vec![1, 2], "as_of_snapshot=S1: {body}");

    // No selector -> the full/live id set.
    let (status, body) = get(cp.clone(), eng.clone(), "/objects/Thing", "alice").await;
    assert_eq!(status, StatusCode::OK, "live read: {body}");
    assert_eq!(ids(&body), vec![1, 2, 3, 4], "live read: {body}");

    // `?as_of={rfc3339 of S1}` -> the first-write ids.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        &format!("/objects/Thing?as_of={s1_time}"),
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "as_of=S1 time: {body}");
    assert_eq!(ids(&body), vec![1, 2], "as_of=S1 time: {body}");

    // Both given -> 400 (mutually exclusive).
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Thing?as_of=x&as_of_snapshot=1",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "as_of + as_of_snapshot both given"
    );

    // An id before the table's first snapshot (never live) -> 404.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Thing?as_of_snapshot=0",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "as_of_snapshot=0");

    // An unparseable timestamp -> 400.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Thing?as_of=not-a-date",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "as_of=not-a-date");

    // -- Dataset detail (`GET /datasets/main/thing`) selector assertions --

    // `?as_of_snapshot={S1}` -> snapshot_id == S1.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        &format!("/datasets/main/thing?as_of_snapshot={s1}"),
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "dataset as_of_snapshot=S1: {body}");
    assert_eq!(
        body["snapshot_id"].as_i64(),
        Some(s1),
        "dataset as_of_snapshot=S1: {body}"
    );

    // No selector -> the current snapshot (S2).
    let (status, body) = get(cp.clone(), eng.clone(), "/datasets/main/thing", "alice").await;
    assert_eq!(status, StatusCode::OK, "dataset live read: {body}");
    assert_eq!(
        body["snapshot_id"].as_i64(),
        Some(s2),
        "dataset live read: {body}"
    );

    // A snapshot id never present in the table's history -> 404 (dataset detail
    // resolves the explicit-id case via `snapshots().find`, not the liveness-range
    // check the object-read path uses).
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/datasets/main/thing?as_of_snapshot=999999",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "dataset as_of_snapshot=999999"
    );

    // An unparseable timestamp -> 400.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/datasets/main/thing?as_of=bad",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "dataset as_of=bad");
}
