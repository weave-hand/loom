//! End-to-end proof that `GET /lineage/datasets/{ns}/{name}/runs` is governed by
//! seed readability: a readable dataset lists the runs that touched it (with the
//! matched role), and a denied or unknown seed returns an empty page — never a 404
//! (non-oracle), consistent with the `/lineage` closure reads. Seeds `loom:type`
//! provenance refs (each resolves to an ontology `Type`, gated by `grant_read`) via
//! `Lineage::emit`; drives the real query-api router + auth gate + Postgres adapter.

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::{ControlPlane, DatasetRef, EventType, LineageEvent, ObjectType, RunId};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{NoServing, get, grant_read, subject_with_role};

fn ty(name: &str) -> DatasetRef {
    DatasetRef {
        namespace: "loom:type".into(),
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
/// validates (both adapters reject a Type grant on an unknown type). Only *granted*
/// types need defining — a denied type's `Acl::check` returns `Deny` without erroring.
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

async fn fresh(fx: &PgFixture) -> Arc<PgControlPlane> {
    let (cp, _db) = fx.fresh_db().await;
    Arc::new(cp)
}

#[tokio::test(flavor = "multi_thread")]
async fn runs_lists_touching_run_for_readable_dataset() {
    let fx = PgFixture::shared();
    let cp = fresh(fx).await;
    // One run consumes A and produces S (S is our query target; the run touched it
    // as an output).
    let run = RunId(uuid::Uuid::new_v4());
    cp.lineage()
        .emit(LineageEvent {
            run_id: run,
            event_type: EventType::Complete,
            event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
            inputs: vec![ty("A")],
            outputs: vec![ty("S")],
            payload: serde_json::json!({}),
        })
        .await
        .unwrap();
    // u can read S.
    let (_u, role) = subject_with_role(&cp, "u").await;
    deftype(&cp, "S").await;
    grant_read(&cp, &role, "S").await;
    let (status, body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/loom:type/S/runs",
        "u",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let runs = body["runs"].as_array().unwrap();
    // `deftype(S)` emits a table→type binding edge that also touches S, so S's run
    // history legitimately includes that binding run too — assert OUR emitted run is
    // present with the matched role rather than an exact count.
    let mine = runs
        .iter()
        .find(|r| r["run_id"] == run.0.to_string())
        .unwrap_or_else(|| panic!("our run present in history: {body}"));
    assert_eq!(mine["role"], "output", "S is our run's output: {body}");
    assert_eq!(mine["latest_event_type"], "complete");
}

#[tokio::test(flavor = "multi_thread")]
async fn runs_denied_seed_is_empty_like_unknown() {
    let fx = PgFixture::shared();
    let cp = fresh(fx).await;
    cp.lineage().emit(edge(ty("A"), ty("S"))).await.unwrap();
    // u can read A but NOT the seed S (S never granted → denied). A run DID touch S,
    // but the denied seed must not disclose it.
    let (_u, role) = subject_with_role(&cp, "u").await;
    deftype(&cp, "A").await;
    grant_read(&cp, &role, "A").await;
    let (status, denied) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/loom:type/S/runs",
        "u",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{denied}");
    assert_eq!(
        denied["runs"].as_array().unwrap().len(),
        0,
        "denied seed → empty: {denied}"
    );
    // Unknown seed is identical (non-oracle).
    let (status2, unknown) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/loom:type/NOPE/runs",
        "u",
    )
    .await;
    assert_eq!(status2, StatusCode::OK);
    assert_eq!(unknown["runs"], denied["runs"], "denied ≡ unknown");
}
