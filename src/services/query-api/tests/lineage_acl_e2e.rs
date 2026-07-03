//! End-to-end proof that `/lineage` reads are least-disclosure: cut-not-skip,
//! per-subject flat-set diff, seed gating, pagination completeness, events
//! redaction, external default-allow, and downstream governed + cut. Seeds
//! `loom:type` provenance refs (each resolves to an ontology `Type`, gated by
//! `grant_read`) via `Lineage::emit`; drives the real query-api router + auth
//! gate + Postgres adapter.

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::{
    ControlPlane, DatasetRef, EventType, LineageEvent, ObjectType, RoleId, RunId,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{NoServing, get, grant_read, subject_with_role};

fn ty(name: &str) -> DatasetRef {
    DatasetRef {
        namespace: "loom:type".into(),
        name: name.into(),
    }
}

fn ext(ns: &str, name: &str) -> DatasetRef {
    DatasetRef {
        namespace: ns.into(),
        name: name.into(),
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

/// Define a minimal ontology type `name` so a `PolicyTarget::Type` grant on it
/// validates — BOTH adapters reject a Type grant on an unknown type
/// (`grant references unknown type`). Call **once** per type name (a second
/// `define_type` of the same name errors). `define_type` is a pure ontology write
/// (no physical table needed). Types that are only *denied* (never granted) need no
/// definition — `Acl::check` returns `Deny` for an unknown type without erroring.
async fn deftype(cp: &PgControlPlane, name: &str) {
    cp.ontology()
        .define_type(
            ObjectType::build(name, ("lin", name))
                .prop_req("id", "Long")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
}

/// Define each granted type (deduped) then grant `role` `Read` on all of them.
async fn grant_types(cp: &PgControlPlane, role: &RoleId, types: &[&str]) {
    for t in types {
        deftype(cp, t).await;
        grant_read(cp, role, t).await;
    }
}

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

/// Uppercase hex digit for a nibble (0..=15).
fn hex(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        _ => (b'A' + n - 10) as char,
    }
}

/// Percent-encode a query value (opaque cursors contain JSON metacharacters).
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

#[tokio::test(flavor = "multi_thread")]
async fn cut_not_skip_denied_intermediate_hides_ancestor() {
    let fx = PgFixture::shared();
    let cp = fresh(fx).await;
    // A -> N -> X -> S
    for (i, o) in [("A", "N"), ("N", "X"), ("X", "S")] {
        cp.lineage().emit(edge(ty(i), ty(o))).await.unwrap();
    }
    // U reads A, X, S but NOT N.
    let (_u, role) = subject_with_role(&cp, "u").await;
    grant_types(&cp, &role, &["A", "X", "S"]).await;
    let (status, body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/loom:type/S/upstream?depth=3",
        "u",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(names(&body), vec!["X".to_string()], "cut hides A: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn flat_set_differs_between_admin_and_restricted() {
    let fx = PgFixture::shared();
    let cp = fresh(fx).await;
    for (i, o) in [("A", "N"), ("N", "X"), ("X", "S")] {
        cp.lineage().emit(edge(ty(i), ty(o))).await.unwrap();
    }
    // Define the shared type set ONCE (a second define_type of a name errors), then
    // grant per subject. admin reads everything; restricted reads only X, S.
    for t in ["A", "N", "X", "S"] {
        deftype(&cp, t).await;
    }
    let (_a, admin_role) = subject_with_role(&cp, "admin").await;
    for t in ["A", "N", "X", "S"] {
        grant_read(&cp, &admin_role, t).await;
    }
    let (_r, r_role) = subject_with_role(&cp, "restricted").await;
    for t in ["X", "S"] {
        grant_read(&cp, &r_role, t).await;
    }
    let (_s, admin_body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/loom:type/S/upstream?depth=3",
        "admin",
    )
    .await;
    let (_s2, r_body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/loom:type/S/upstream?depth=3",
        "restricted",
    )
    .await;
    assert_eq!(
        names(&admin_body),
        vec!["A".to_string(), "N".to_string(), "X".to_string()]
    );
    assert_eq!(
        names(&r_body),
        vec!["X".to_string()],
        "restricted: cut at N"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn seed_gating_denied_seed_is_empty_like_unknown() {
    let fx = PgFixture::shared();
    let cp = fresh(fx).await;
    cp.lineage().emit(edge(ty("A"), ty("S"))).await.unwrap();
    // U reads A but not the seed S. (S and NOPE are never granted, so they need no
    // define_type — the check returns Deny for both, giving identical empty pages.)
    let (_u, role) = subject_with_role(&cp, "u").await;
    grant_types(&cp, &role, &["A"]).await;
    let (status, denied) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/loom:type/S/upstream?depth=2",
        "u",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        denied["datasets"].as_array().unwrap().len(),
        0,
        "denied seed → empty: {denied}"
    );
    // Unknown seed is identical (non-oracle).
    let (status2, unknown) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/loom:type/NOPE/upstream?depth=2",
        "u",
    )
    .await;
    assert_eq!(status2, StatusCode::OK);
    assert_eq!(unknown["datasets"], denied["datasets"], "denied ≡ unknown");
}

#[tokio::test(flavor = "multi_thread")]
async fn pagination_pages_every_visible_ref_once() {
    let fx = PgFixture::shared();
    let cp = fresh(fx).await;
    // 6 readable + 3 denied inputs feed Z.
    for i in 0..6 {
        cp.lineage()
            .emit(edge(ty(&format!("r{i}")), ty("Z")))
            .await
            .unwrap();
    }
    for i in 0..3 {
        cp.lineage()
            .emit(edge(ty(&format!("d{i}")), ty("Z")))
            .await
            .unwrap();
    }
    // Grant Z + the 6 readable inputs; the 3 `d{i}` are left undefined+ungranted (denied).
    let (_u, role) = subject_with_role(&cp, "u").await;
    deftype(&cp, "Z").await;
    grant_read(&cp, &role, "Z").await;
    for i in 0..6 {
        let r = format!("r{i}");
        deftype(&cp, &r).await;
        grant_read(&cp, &role, &r).await;
    }
    let mut seen: Vec<String> = Vec::new();
    let mut after: Option<String> = None;
    for _ in 0..12 {
        let uri = match &after {
            Some(c) => format!(
                "/lineage/datasets/loom:type/Z/upstream?depth=1&limit=2&after={}",
                pct(c)
            ),
            None => "/lineage/datasets/loom:type/Z/upstream?depth=1&limit=2".to_string(),
        };
        let (status, body) = get(cp.clone(), Arc::new(NoServing), &uri, "u").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let page = body["datasets"].as_array().unwrap();
        assert!(page.len() <= 2, "page ≤ limit: {body}");
        for d in page {
            seen.push(d["name"].as_str().unwrap().to_string());
        }
        match body["next_cursor"].as_str() {
            Some(c) => {
                assert_eq!(page.len(), 2, "non-final page is full: {body}");
                after = Some(c.to_string());
            }
            None => break,
        }
    }
    seen.sort();
    let expected: Vec<String> = (0..6).map(|i| format!("r{i}")).collect();
    assert_eq!(
        seen, expected,
        "every visible ref once, denied absent, no short-page drop"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn events_redaction_omits_denied_refs_keeps_envelope() {
    let fx = PgFixture::shared();
    let cp = fresh(fx).await;
    let run = RunId(uuid::Uuid::new_v4());
    cp.lineage()
        .emit(LineageEvent {
            run_id: run,
            event_type: EventType::Complete,
            event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
            inputs: vec![ty("A"), ty("SECRET")],
            outputs: vec![ty("OUT")],
            payload: serde_json::json!({ "k": 1 }),
        })
        .await
        .unwrap();
    // Grant A + OUT; SECRET is left undefined+ungranted (denied → redacted).
    let (_u, role) = subject_with_role(&cp, "u").await;
    grant_types(&cp, &role, &["A", "OUT"]).await;
    let uri = format!("/lineage/runs/{}/events", run.0);
    let (status, body) = get(cp.clone(), Arc::new(NoServing), &uri, "u").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ev = &body["events"][0];
    let inputs: Vec<String> = ev["inputs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        inputs,
        vec!["A".to_string()],
        "SECRET redacted from inputs: {body}"
    );
    assert_eq!(ev["outputs"].as_array().unwrap().len(), 1, "OUT kept");
    assert_eq!(ev["event_type"], "complete", "envelope intact");
}

#[tokio::test(flavor = "multi_thread")]
async fn external_source_is_default_allowed() {
    let fx = PgFixture::shared();
    let cp = fresh(fx).await;
    // s3 external source and a denied internal sibling both feed S.
    cp.lineage()
        .emit(edge(ext("s3://raw", "landing.csv"), ty("S")))
        .await
        .unwrap();
    cp.lineage()
        .emit(edge(ty("SECRET"), ty("S")))
        .await
        .unwrap();
    // reads S, not SECRET (SECRET left undefined+ungranted → denied).
    let (_u, role) = subject_with_role(&cp, "u").await;
    grant_types(&cp, &role, &["S"]).await;
    let (status, body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/loom:type/S/upstream?depth=2",
        "u",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        names(&body),
        vec!["landing.csv".to_string()],
        "external allowed, SECRET denied: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn downstream_closure_is_governed_and_cuts() {
    let fx = PgFixture::shared();
    let cp = fresh(fx).await;
    // A -> B -> C (input->output edges; downstream walks them forward).
    for (i, o) in [("A", "B"), ("B", "C")] {
        cp.lineage().emit(edge(ty(i), ty(o))).await.unwrap();
    }
    // Define A,B,C once; grant per subject (two subjects share the type set).
    for t in ["A", "B", "C"] {
        deftype(&cp, t).await;
    }
    // u reads everything: downstream(A, depth=2) = {B, C}.
    let (_u, urole) = subject_with_role(&cp, "u").await;
    for t in ["A", "B", "C"] {
        grant_read(&cp, &urole, t).await;
    }
    let (status, body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/loom:type/A/downstream?depth=2",
        "u",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        names(&body),
        vec!["B".to_string(), "C".to_string()],
        "downstream descendants: {body}"
    );

    // v cannot read B: cut → C is unreachable downstream from A.
    let (_v, vrole) = subject_with_role(&cp, "v").await;
    for t in ["A", "C"] {
        grant_read(&cp, &vrole, t).await;
    }
    let (status2, body2) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/loom:type/A/downstream?depth=2",
        "v",
    )
    .await;
    assert_eq!(status2, StatusCode::OK, "{body2}");
    assert_eq!(
        names(&body2),
        Vec::<String>::new(),
        "denied B cuts C from downstream: {body2}"
    );
}
