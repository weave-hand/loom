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
//!   - `?as_of_snapshot=999999999` (above the newest snapshot) -> 404. Both surfaces
//!     resolve the explicit-id case via `Catalog::snapshot` (exists in history AND
//!     the table live at it), so an id outside the table's snapshot history 404s
//!     regardless of whether it falls below or above the live range.
//!   - `?as_of=not-a-date` -> 400.
//!
//! Dataset-detail (`GET /datasets/main/thing`) assertions reuse the same fixture:
//!   - `?as_of_snapshot={S1}` -> `snapshot_id == S1`.
//!   - no selector -> `snapshot_id == S2` (the current snapshot).
//!   - `?as_of_snapshot=999999` (non-existent id) -> 404, via the same
//!     `Catalog::snapshot` history lookup as the object-read path.
//!   - `?as_of=bad` -> 400.

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::{ControlPlane, PageReq};
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{
    get, grant_read, ids_i64 as ids, subject_with_role, tref, two_batch_thing_setup,
};

#[tokio::test(flavor = "multi_thread")]
async fn as_of_selectors_resolve_and_apply() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer, s1, s2, _pool) = two_batch_thing_setup(fx).await;
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

    // An id ABOVE the newest snapshot -> 404. Previously the object path's
    // open-ended liveness range resolved this as "live" and read current data;
    // history-backed validation (Catalog::snapshot) now rejects it, matching
    // the dataset path.
    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Thing?as_of_snapshot=999999999",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "as_of_snapshot above history"
    );

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
