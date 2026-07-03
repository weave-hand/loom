//! Pure-logic unit tests for the `LineageVisibility` BFS: cut-not-skip, cycle
//! termination, depth bound, scan cap, external default-allow, fail-closed
//! unresolvable, and the sort+window keyset round-trip. Memory control plane
//! (lineage + ACL) + a real file-backed naming bridge; no Postgres, RE-eligible.

use std::time::Duration;

// `Acl` is imported because `subject_reading` calls `define_subject`/`define_role`/
// `assign_role`/`grant` on the concrete `MemoryControlPlane` (these are `Acl`-trait
// methods on a concrete receiver, so the trait must be in scope; cf. http_smoke.rs).
// `Ontology` is imported for the same reason: `subject_reading` also calls
// `define_type` on the concrete receiver, because `MemoryControlPlane::grant` now
// validates that a `PolicyTarget::Type` names a defined ontology type (Validation
// error otherwise) — a real behavior this crate's ACL adapter enforces that the
// original test-brief draft predated.
// `Lineage` is deliberately NOT imported — `cp.lineage().emit(..)` calls through the
// `&dyn Lineage` accessor return, which needs no trait in scope (an unused import
// would trip clippy's `unused_imports` on test targets).
use control_plane_core::{
    Acl, Action, ControlPlane, DatasetRef, Effect, EventType, LineageEvent, ObjectType, Ontology,
    PageReq, PolicyTarget, RoleId, RunId, SubjectId, TypeName, encode_dataset_cursor,
};
use control_plane_memory::MemoryControlPlane;
use lineage_naming::LineageNaming;
use query_api::lineage_filter::{LineageDir, LineageVisibility, LineageVisibilityError};
use store_config::{ObjectStoreBackend, ObjectStoreConfig};

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

fn naming() -> LineageNaming {
    LineageNaming::from_object_store(&ObjectStoreConfig {
        warehouse_uri: "file:///loom".into(),
        backend: ObjectStoreBackend::Local,
    })
}

fn cp() -> MemoryControlPlane {
    MemoryControlPlane::new(Duration::from_millis(300))
}

/// Grant the subject `Read` on each named `loom:type` type. Defines the ontology
/// type first (idempotent: same name+table is a no-op re-define) — `grant` rejects
/// a `PolicyTarget::Type` naming an undefined type with `Validation`, so a bare
/// grant call is not enough here.
async fn subject_reading(cp: &MemoryControlPlane, name: &str, types: &[&str]) -> SubjectId {
    let subj = SubjectId(name.into());
    let role = RoleId(format!("{name}-role"));
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    for t in types {
        cp.define_type(ObjectType::build(*t, ("ns", *t)).done())
            .await
            .unwrap();
        cp.grant(
            &role,
            Action::Read,
            PolicyTarget::Type(TypeName((*t).into())),
            Effect::Allow,
        )
        .await
        .unwrap();
    }
    subj
}

fn names(page: &control_plane_core::Page<DatasetRef>) -> Vec<String> {
    let mut v: Vec<String> = page.items.iter().map(|d| d.name.clone()).collect();
    v.sort();
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn cut_not_skip_denied_intermediate_hides_its_ancestors() {
    // upstream chain A -> N -> X -> S (edges output->input reversed by upstream).
    let cp = cp();
    for (i, o) in [("A", "N"), ("N", "X"), ("X", "S")] {
        cp.lineage().emit(edge(ty(i), ty(o))).await.unwrap();
    }
    let bridge = naming();
    // U reads A, X, S but NOT N.
    let subj = subject_reading(&cp, "u", &["A", "X", "S"]).await;
    let vis = LineageVisibility::new(cp.acl(), cp.lineage(), &bridge);
    let page = vis
        .visible_closure(
            &subj,
            &ty("S"),
            3,
            LineageDir::Upstream,
            &PageReq::unbounded(),
        )
        .await
        .unwrap();
    // Only X is reachable through readable nodes; N cut the branch so A is hidden.
    assert_eq!(names(&page), vec!["X".to_string()], "cut: N hides A");

    // A subject that can also read N sees {X, A, N}.
    let subj2 = subject_reading(&cp, "v", &["A", "N", "X", "S"]).await;
    let page2 = vis_for(&cp, &bridge)
        .visible_closure(
            &subj2,
            &ty("S"),
            3,
            LineageDir::Upstream,
            &PageReq::unbounded(),
        )
        .await
        .unwrap();
    assert_eq!(
        names(&page2),
        vec!["A".to_string(), "N".to_string(), "X".to_string()]
    );
}

fn vis_for<'a>(cp: &'a MemoryControlPlane, bridge: &'a LineageNaming) -> LineageVisibility<'a> {
    LineageVisibility::new(cp.acl(), cp.lineage(), bridge)
}

#[tokio::test(flavor = "multi_thread")]
async fn seed_unreadable_returns_empty_page() {
    let cp = cp();
    cp.lineage().emit(edge(ty("A"), ty("S"))).await.unwrap();
    let bridge = naming();
    // U reads A but not the seed S.
    let subj = subject_reading(&cp, "u", &["A"]).await;
    let page = vis_for(&cp, &bridge)
        .visible_closure(
            &subj,
            &ty("S"),
            3,
            LineageDir::Upstream,
            &PageReq::unbounded(),
        )
        .await
        .unwrap();
    assert!(
        page.items.is_empty(),
        "seed gating: unreadable seed → empty"
    );
    assert!(page.next.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn cycle_terminates_via_visited_guard() {
    let cp = cp();
    // X <-> Y cycle.
    cp.lineage().emit(edge(ty("X"), ty("Y"))).await.unwrap();
    cp.lineage().emit(edge(ty("Y"), ty("X"))).await.unwrap();
    let bridge = naming();
    let subj = subject_reading(&cp, "u", &["X", "Y"]).await;
    let page = vis_for(&cp, &bridge)
        .visible_closure(
            &subj,
            &ty("X"),
            10,
            LineageDir::Upstream,
            &PageReq::unbounded(),
        )
        .await
        .unwrap();
    // Seed excluded; the only other node is Y. Terminates (no hang).
    assert_eq!(names(&page), vec!["Y".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn depth_zero_and_over_cap_are_validation_errors() {
    let cp = cp();
    let bridge = naming();
    let subj = subject_reading(&cp, "u", &[]).await;
    for bad in [0u32, 33u32] {
        let err = vis_for(&cp, &bridge)
            .visible_closure(
                &subj,
                &ty("S"),
                bad,
                LineageDir::Upstream,
                &PageReq::unbounded(),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, LineageVisibilityError::Cp(_)),
            "depth {bad} → Cp(Validation)"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn external_refs_are_default_allowed_unresolvable_loom_is_denied() {
    let cp = cp();
    // seed S (readable) <- external s3 source and <- a malformed loom ref.
    cp.lineage()
        .emit(edge(ext("s3://raw", "bucket.csv"), ty("S")))
        .await
        .unwrap();
    cp.lineage()
        .emit(edge(ext("loom", "nodot"), ty("S")))
        .await
        .unwrap(); // malformed → fail-closed
    let bridge = naming();
    let subj = subject_reading(&cp, "u", &["S"]).await;
    let page = vis_for(&cp, &bridge)
        .visible_closure(
            &subj,
            &ty("S"),
            2,
            LineageDir::Upstream,
            &PageReq::unbounded(),
        )
        .await
        .unwrap();
    // External present; the malformed loom-namespace ref is fail-closed (absent).
    assert_eq!(names(&page), vec!["bucket.csv".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn windowing_pages_every_visible_ref_once_in_order() {
    let cp = cp();
    // Fan-out: 5 readable inputs feed Z; all under loom:type.
    for i in 0..5 {
        cp.lineage()
            .emit(edge(ty(&format!("in{i}")), ty("Z")))
            .await
            .unwrap();
    }
    let bridge = naming();
    let subj = subject_reading(&cp, "u", &["Z", "in0", "in1", "in2", "in3", "in4"]).await;
    let vis = vis_for(&cp, &bridge);
    let mut seen: Vec<String> = Vec::new();
    let mut after: Option<control_plane_core::Cursor> = None;
    for _ in 0..10 {
        let req = PageReq {
            after: after.clone(),
            limit: Some(2),
        };
        let page = vis
            .visible_closure(&subj, &ty("Z"), 1, LineageDir::Upstream, &req)
            .await
            .unwrap();
        assert!(page.items.len() <= 2);
        for d in &page.items {
            seen.push(d.name.clone());
        }
        match page.next {
            Some(c) => after = Some(c),
            None => break,
        }
    }
    seen.sort();
    assert_eq!(seen, vec!["in0", "in1", "in2", "in3", "in4"]);
    // Cursor is the encoding of the last emitted ref on a non-final page.
    let first = vis
        .visible_closure(
            &subj,
            &ty("Z"),
            1,
            LineageDir::Upstream,
            &PageReq {
                after: None,
                limit: Some(2),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        first.next,
        Some(encode_dataset_cursor(first.items.last().unwrap()))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn scan_cap_exceeded_is_its_own_error() {
    let cp = cp();
    // A small readable fan-out; a cap of 1 is exceeded on the second distinct node.
    for i in 0..4 {
        cp.lineage()
            .emit(edge(ty(&format!("in{i}")), ty("Z")))
            .await
            .unwrap();
    }
    let bridge = naming();
    let subj = subject_reading(&cp, "u", &["Z", "in0", "in1", "in2", "in3"]).await;
    let err = vis_for(&cp, &bridge)
        .with_scan_cap(1)
        .visible_closure(
            &subj,
            &ty("Z"),
            1,
            LineageDir::Upstream,
            &PageReq::unbounded(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, LineageVisibilityError::ScanCapExceeded),
        "over-cap → ScanCapExceeded"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn redact_events_nulls_payload_when_any_ref_denied() {
    let cp = cp();
    let bridge = naming();
    let subj = subject_reading(&cp, "u", &["A"]).await; // reads A, not B
    let run = RunId(uuid::Uuid::new_v4());
    let ev = LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
        inputs: vec![ty("A"), ty("B")],
        outputs: vec![ty("B")],
        payload: serde_json::json!({ "k": 1 }),
    };
    let page = control_plane_core::Page {
        items: vec![ev],
        next: None,
    };
    let red = vis_for(&cp, &bridge)
        .redact_events(&subj, page)
        .await
        .unwrap();
    let e = &red.items[0];
    assert_eq!(e.inputs, vec![ty("A")], "denied B removed from inputs");
    assert!(e.outputs.is_empty(), "denied B removed from outputs");
    assert_eq!(e.run_id, run, "envelope intact");
    assert!(
        e.payload.is_null(),
        "any redaction nulls the payload (least disclosure), got {:?}",
        e.payload
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn redact_events_keeps_payload_when_all_refs_readable() {
    let cp = cp();
    let bridge = naming();
    let subj = subject_reading(&cp, "u", &["A", "B"]).await; // reads every ref
    let ev = LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
        inputs: vec![ty("A")],
        outputs: vec![ty("B")],
        payload: serde_json::json!({ "k": 1 }),
    };
    let page = control_plane_core::Page {
        items: vec![ev],
        next: None,
    };
    let red = vis_for(&cp, &bridge)
        .redact_events(&subj, page)
        .await
        .unwrap();
    let e = &red.items[0];
    assert_eq!(e.inputs, vec![ty("A")], "nothing redacted");
    assert_eq!(e.outputs, vec![ty("B")], "nothing redacted");
    assert_eq!(
        e.payload,
        serde_json::json!({ "k": 1 }),
        "all refs readable → payload verbatim"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn redact_events_stored_null_payload_stays_null() {
    let cp = cp();
    let bridge = naming();
    let subj = subject_reading(&cp, "u", &["A", "B"]).await; // reads every ref → nothing redacted
    let ev = LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
        inputs: vec![ty("A")],
        outputs: vec![ty("B")],
        payload: serde_json::Value::Null,
    };
    let page = control_plane_core::Page {
        items: vec![ev],
        next: None,
    };
    let red = vis_for(&cp, &bridge)
        .redact_events(&subj, page)
        .await
        .unwrap();
    assert!(
        red.items[0].payload.is_null(),
        "a stored-null payload with no redaction stays null (no false 'redacted' signal)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_site_namespace_ref_is_denied_and_cut() {
    // The test bridge's site_namespace is "file:///loom" (see `naming`). A ref under
    // it whose name is not `schema.table` resolves to Unresolvable → fail closed:
    // denied AND cut, so a node reachable only through it is never discovered, and it
    // yields the empty page as a seed. A genuinely-external ref still passes.
    let cp = cp();
    // upstream(S): a malformed site-ns ref and a foreign s3 source both feed S.
    cp.lineage()
        .emit(edge(ext("file:///loom", "Customer"), ty("S")))
        .await
        .unwrap();
    cp.lineage()
        .emit(edge(ext("s3://raw", "landing.csv"), ty("S")))
        .await
        .unwrap();
    // G is upstream ONLY of the malformed (cut) node.
    cp.lineage()
        .emit(edge(ty("G"), ext("file:///loom", "Customer")))
        .await
        .unwrap();
    let bridge = naming();
    let subj = subject_reading(&cp, "u", &["S", "G"]).await;
    let vis = vis_for(&cp, &bridge);

    // As an intermediate: cut node absent, G (reachable only through it) hidden,
    // external source passes.
    let page = vis
        .visible_closure(
            &subj,
            &ty("S"),
            3,
            LineageDir::Upstream,
            &PageReq::unbounded(),
        )
        .await
        .unwrap();
    assert_eq!(
        names(&page),
        vec!["landing.csv".to_string()],
        "malformed site-ns ref is cut (G hidden); external still passes"
    );

    // As a seed: fails closed → empty page (like a denied/unknown seed).
    let seeded = vis
        .visible_closure(
            &subj,
            &ext("file:///loom", "Customer"),
            2,
            LineageDir::Upstream,
            &PageReq::unbounded(),
        )
        .await
        .unwrap();
    assert!(
        seeded.items.is_empty(),
        "malformed site-ns seed fails closed → empty page"
    );
}
