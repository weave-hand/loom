//! Handler-level HTTP proof of the multi-step action response envelope.
//!
//! `POST /actions/{name}` for a multi-step action returns `{"steps":[...]}` listing
//! EVERY step's affected object (`bind`, `target`, `objects`), while a single-step
//! action's response stays the byte-compatible bare object. Drives the real axum
//! router (through the auth gate, via `post_action_raw`) over a hermetic Postgres +
//! LocalFsStorage warehouse with a real engine writer — `StubAction`'s default
//! `write_steps` errors, so a canned action engine cannot exercise this path.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, ControlPlane, Effect, ObjectType, ParamDef,
    PolicyTarget, RoleId, SubjectId, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{InProcessServingEngine, post_action_raw};
use query_api::serving::{ActionEngine, ServingEngine};
use serde_json::json;

fn tn(s: &str) -> TypeName {
    TypeName(s.into())
}

/// Grant Write on every named type to a fresh `writer` subject/role.
async fn writer_on(cp: &PgControlPlane, types: &[&str]) -> SubjectId {
    let subj = SubjectId("writer".into());
    let role = RoleId("writers".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    for t in types {
        cp.grant(
            &role,
            Action::Write,
            PolicyTarget::Type(TypeName((*t).into())),
            Effect::Allow,
        )
        .await
        .unwrap();
    }
    subj
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_step_envelope_and_single_step_back_compat() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // Order + LineItem: the multi-step createOrderWithLines action (mirrors
    // action_multi_object_e2e.rs's seed).
    cp.ontology()
        .define_type(
            ObjectType::build("Order", ("main", "order"))
                .prop_req("id", "Long")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_type(
            ObjectType::build("LineItem", ("main", "line_item"))
                .prop_req("id", "Long")
                .prop_req("orderId", "Long")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
    e2e_support::define_create_order_with_lines_action(&cp).await;

    // Gadget: a single-step bind-less Insert action for the back-compat check.
    cp.ontology()
        .define_type(
            ObjectType::build("Gadget", ("main", "gadget"))
                .prop_req("id", "Long")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("createGadget".into()),
            tn("Gadget"),
            ActionKind::Insert,
            vec![ParamDef::new("id", "Long").required()],
            vec![],
        ))
        .await
        .unwrap();

    let subj = writer_on(&cp, &["Order", "LineItem", "Gadget"]).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving: Arc<dyn ServingEngine> = Arc::new(InProcessServingEngine::new(
        IcebergCatalog::new(pool.clone()),
    ));
    let action_engine: Arc<dyn ActionEngine> = Arc::new(engine);
    let cp = Arc::new(cp);

    // Multi-step: 201 with the steps envelope, all three entries, run-id header present.
    let (status, headers, body) = post_action_raw(
        cp.clone(),
        serving.clone(),
        action_engine.clone(),
        "/actions/createOrderWithLines",
        &json!({ "oid": "500", "li1": "1", "li2": "2" }),
        subj.0.as_str(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");
    assert!(
        headers.get("X-Loom-Run-Id").is_some(),
        "run-id header emitted for multi"
    );
    let steps = body
        .get("steps")
        .and_then(|s| s.as_array())
        .expect("steps envelope");
    assert_eq!(steps.len(), 3);
    assert_eq!(steps[0]["bind"], json!("order"));
    assert_eq!(steps[0]["target"], json!("Order"));
    assert_eq!(
        steps[0]["objects"].as_array().unwrap()[0]["id"],
        json!("500")
    ); // rendered ids are JSON strings (render_cell for Long)
    assert_eq!(steps[1]["bind"], json!(null));
    assert_eq!(steps[1]["target"], json!("LineItem"));
    assert_eq!(
        steps[1]["objects"].as_array().unwrap()[0]["orderId"],
        json!("500")
    );

    // Single-step: 201 with the BARE object (no `steps` wrapper), run-id header present.
    let (status, headers, body) = post_action_raw(
        cp.clone(),
        serving.clone(),
        action_engine.clone(),
        "/actions/createGadget",
        &json!({ "id": "7" }),
        subj.0.as_str(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");
    assert!(headers.get("X-Loom-Run-Id").is_some());
    assert!(
        body.get("steps").is_none(),
        "single-step response is the bare object, unchanged"
    );
    assert!(
        body.get("id").is_some(),
        "bare affected object rendered at top level"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_status_is_kind_true() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // Widget: createWidget/updateWidget/deleteWidget (mirrors update_delete_e2e.rs's seed
    // via the shared e2e_support::define_widget/grant_writer_role helpers).
    let widget = e2e_support::define_widget(&cp).await;
    let (subj, role) = e2e_support::grant_writer_role(&cp, &widget).await;

    // Order + LineItem: the multi-step createOrderWithLines action, for the
    // Insert-first-wins primary-kind guard (mirrors the test above's seed).
    cp.ontology()
        .define_type(
            ObjectType::build("Order", ("main", "order"))
                .prop_req("id", "Long")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_type(
            ObjectType::build("LineItem", ("main", "line_item"))
                .prop_req("id", "Long")
                .prop_req("orderId", "Long")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
    e2e_support::define_create_order_with_lines_action(&cp).await;
    for t in ["Order", "LineItem"] {
        cp.grant(
            &role,
            Action::Write,
            PolicyTarget::Type(TypeName(t.into())),
            Effect::Allow,
        )
        .await
        .unwrap();
    }

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving: Arc<dyn ServingEngine> = Arc::new(InProcessServingEngine::new(
        IcebergCatalog::new(pool.clone()),
    ));
    let action_engine: Arc<dyn ActionEngine> = Arc::new(engine);
    let cp = Arc::new(cp);

    // Insert → 201 Created (mints the row Update/Delete then target).
    let (status, _headers, body) = post_action_raw(
        cp.clone(),
        serving.clone(),
        action_engine.clone(),
        "/actions/createWidget",
        &json!({ "id": "1", "name": "a", "qty": "1" }),
        subj.0.as_str(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "Insert is 201: {body}");

    // Update → 200 OK; run-id header still present.
    let (status, headers, body) = post_action_raw(
        cp.clone(),
        serving.clone(),
        action_engine.clone(),
        "/actions/updateWidget",
        &json!({ "id": "1", "qty": "9" }),
        subj.0.as_str(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "Update is kind-true 200: {body}");
    assert!(headers.get("X-Loom-Run-Id").is_some());

    // Delete → 200 OK.
    let (status, headers, body) = post_action_raw(
        cp.clone(),
        serving.clone(),
        action_engine.clone(),
        "/actions/deleteWidget",
        &json!({ "id": "1" }),
        subj.0.as_str(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "Delete is kind-true 200: {body}");
    assert!(headers.get("X-Loom-Run-Id").is_some());

    // Multi-step primary-kind guard: Insert-first → 201 (first step wins).
    let (status, _headers, body) = post_action_raw(
        cp.clone(),
        serving.clone(),
        action_engine.clone(),
        "/actions/createOrderWithLines",
        &json!({ "oid": "600", "li1": "1", "li2": "2" }),
        subj.0.as_str(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "multi-step Insert-first is 201: {body}"
    );
}
