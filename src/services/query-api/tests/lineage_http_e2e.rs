//! End-to-end tests for the governed lineage read endpoints over the real query-api
//! HTTP router + auth gate + Postgres lineage adapter. Seeds a known provenance
//! graph directly via `Lineage::emit` (no serving engine needed).

use std::sync::Arc;

use axum::http::StatusCode;
// `ControlPlane` is in scope for `cp.lineage()` (a trait method on the concrete
// `PgControlPlane`); `Lineage` is NOT imported — its methods are called on the
// `&dyn Lineage` the accessor returns, which needs no trait in scope (an unused
// `Lineage` import would fail the enforced clippy `unused_imports` on test targets).
use control_plane_core::{ControlPlane, DatasetRef, EventType, LineageEvent, RunId};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{NoServing, get, get_unauth};

fn ds(ns: &str, name: &str) -> DatasetRef {
    DatasetRef {
        namespace: ns.to_string(),
        name: name.to_string(),
    }
}

fn edge(inp: DatasetRef, out: DatasetRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
        inputs: vec![inp],
        outputs: vec![out],
        payload: serde_json::json!({}),
    }
}

/// Uppercase hex digit for a nibble (0..=15).
fn hex(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        _ => (b'A' + n - 10) as char,
    }
}

/// Percent-encode a string for use as a query-string value (opaque cursors contain
/// JSON metacharacters). Encodes everything outside the unreserved set.
fn pct(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(hex(b >> 4));
            out.push(hex(b & 0x0f));
        }
    }
    out
}

/// Sorted dataset names from a `{ datasets: [...] }` closure body.
fn names(body: &serde_json::Value) -> Vec<String> {
    let mut v: Vec<String> = body["datasets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["name"].as_str().unwrap().to_string())
        .collect();
    v.sort();
    v
}

async fn fresh(fx: &PgFixture) -> Arc<PgControlPlane> {
    let (cp, _db) = fx.fresh_db().await;
    Arc::new(cp)
}

#[tokio::test(flavor = "multi_thread")]
async fn upstream_downstream_traversal() {
    let fx = PgFixture::shared();
    let cp = fresh(fx).await;
    // A -> B -> C
    cp.lineage()
        .emit(edge(ds("w", "a"), ds("w", "b")))
        .await
        .unwrap();
    cp.lineage()
        .emit(edge(ds("w", "b"), ds("w", "c")))
        .await
        .unwrap();

    // upstream(C, depth=2) = {A, B}
    let (status, body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/w/c/upstream?depth=2",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        names(&body),
        vec!["a".to_string(), "b".to_string()],
        "{body}"
    );

    // downstream(A, depth=2) = {B, C}
    let (status, body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/w/a/downstream?depth=2",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        names(&body),
        vec!["b".to_string(), "c".to_string()],
        "{body}"
    );

    // depth=1 (default) = one hop
    let (status, body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/w/c/upstream",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        names(&body),
        vec!["b".to_string()],
        "default depth 1: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn depth_over_cap_is_400() {
    let fx = PgFixture::shared();
    let cp = fresh(fx).await;
    cp.lineage()
        .emit(edge(ds("w", "a"), ds("w", "b")))
        .await
        .unwrap();
    // LINEAGE_MAX_DEPTH is 32; 99 is over-cap -> the capability rejects (400), never
    // an unbounded walk.
    let (status, _body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/w/b/upstream?depth=99",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn pagination_pages_every_dataset_once() {
    let fx = PgFixture::shared();
    let cp = fresh(fx).await;
    // fan-out: 7 inputs each feeding Z
    for i in 0..7 {
        cp.lineage()
            .emit(edge(ds("w", &format!("in{i:02}")), ds("w", "z")))
            .await
            .unwrap();
    }

    let mut seen: Vec<String> = Vec::new();
    let mut after: Option<String> = None;
    for _ in 0..10 {
        let uri = match &after {
            Some(cur) => format!(
                "/lineage/datasets/w/z/upstream?depth=1&limit=3&after={}",
                pct(cur)
            ),
            None => "/lineage/datasets/w/z/upstream?depth=1&limit=3".to_string(),
        };
        let (status, body) = get(cp.clone(), Arc::new(NoServing), &uri, "alice").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let page = body["datasets"].as_array().unwrap();
        assert!(page.len() <= 3, "page never exceeds limit: {body}");
        for d in page {
            seen.push(d["name"].as_str().unwrap().to_string());
        }
        match body["next_cursor"].as_str() {
            Some(c) => after = Some(c.to_string()),
            None => break,
        }
    }
    seen.sort();
    let expected: Vec<String> = (0..7).map(|i| format!("in{i:02}")).collect();
    assert_eq!(
        seen, expected,
        "every dataset returned exactly once in stable order"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn run_events_returns_the_runs_events() {
    let fx = PgFixture::shared();
    let cp = fresh(fx).await;
    let run = RunId(uuid::Uuid::new_v4());
    for i in 0..3 {
        cp.lineage()
            .emit(LineageEvent {
                run_id: run,
                event_type: EventType::Running,
                event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000 + i).unwrap(),
                inputs: vec![],
                outputs: vec![ds("w", &format!("o{i}"))],
                payload: serde_json::json!({ "i": i }),
            })
            .await
            .unwrap();
    }
    let uri = format!("/lineage/runs/{}/events", run.0);
    let (status, body) = get(cp.clone(), Arc::new(NoServing), &uri, "alice").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["events"].as_array().unwrap().len(), 3, "{body}");
    assert_eq!(body["events"][0]["event_type"], "running");
}

#[tokio::test(flavor = "multi_thread")]
async fn unauthenticated_is_401_authenticated_is_200() {
    let fx = PgFixture::shared();
    let cp = fresh(fx).await;
    cp.lineage()
        .emit(edge(ds("w", "a"), ds("w", "b")))
        .await
        .unwrap();

    let status = get_unauth(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/w/b/upstream",
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "no token -> 401");

    let (status, _body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/w/b/upstream",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "any verified subject -> 200");
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_run_id_is_400_unknown_dataset_is_empty() {
    let fx = PgFixture::shared();
    let cp = fresh(fx).await;

    // malformed run id UUID -> 400
    let (status, _body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/runs/not-a-uuid/events",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // unknown dataset -> empty page, not an error
    let (status, body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/w/nope/upstream",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["datasets"].as_array().unwrap().len(), 0, "{body}");
    assert!(body["next_cursor"].is_null());
}
