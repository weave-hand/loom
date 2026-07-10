//! A multi-step action whose second step targets a declared stream table must fail
//! with HTTP 422 (the refusal), never 500, and write nothing.
//!
//! Path B wire counterpart to the transform (Path A) 422 coverage: the guard
//! originates in `iceberg_landing::write_steps` (`ControlPlaneError::Validation`,
//! `"stream-table target refused:"`), and this test drives it end-to-end through the
//! real axum router, the real engine writer over a UDS
//! (`GrpcQueueClient::write_steps`), down to the postgres guard — proving the whole
//! chain (`EngineServingError::Validation` -> `Status::invalid_argument` ->
//! `ControlPlaneError::Validation` -> `ServingError::Unsupported` ->
//! `ActionError::Unsupported`) renders as 422, not the catch-all 500.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::{
    Acl, Action, ControlPlane, Effect, ObjectType, PolicyTarget, RoleId, StreamTables, SubjectId,
    TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use e2e_support::InProcessServingEngine;
use query_api::serving::{ActionEngine, ServingEngine};
use serde_json::json;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

/// Create a fresh `writer` subject (role `writers`) granted Write + Read on every named
/// type; returns the subject and its role. Mirrors `action_multi_object_e2e.rs::writer_on`.
async fn writer_on(cp: &PgControlPlane, types: &[&str]) -> (SubjectId, RoleId) {
    let subj = SubjectId("writer".into());
    let role = RoleId("writers".into());
    cp.define_subject(&subj).await.expect("define subject");
    cp.define_role(&role).await.expect("define role");
    cp.assign_role(&subj, &role).await.expect("assign role");
    for t in types {
        for action in [Action::Write, Action::Read] {
            cp.grant(
                &role,
                action,
                PolicyTarget::Type(TypeName((*t).into())),
                Effect::Allow,
            )
            .await
            .expect("grant");
        }
    }
    (subj, role)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_step_action_targeting_stream_returns_422() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let warehouse = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(&db).await;
    let cp = Arc::new(cp);

    // 1. Define the two ontology types (backing tables main.order / main.line_item),
    //    mirroring action_multi_object_e2e.rs:124-160.
    cp.ontology()
        .define_type(
            ObjectType::build("Order", ("main", "order"))
                .prop_req("id", "Long")
                .prop("note", "String")
                .identity("id")
                .done(),
        )
        .await
        .expect("define Order");
    cp.ontology()
        .define_type(
            ObjectType::build("LineItem", ("main", "line_item"))
                .prop_req("id", "Long")
                .prop_req("orderId", "Long")
                .prop("sku", "String")
                .identity("id")
                .done(),
        )
        .await
        .expect("define LineItem");

    // 2. Define the 3-step create-order-with-lines action + grant Write (+Read).
    e2e_support::define_create_order_with_lines_action(&cp).await;
    let (subj, _role) = writer_on(&cp, &["Order", "LineItem"]).await;

    // 3. Engine writer over a real UDS (exercises GrpcQueueClient::write_steps) + serving.
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let action_engine: Arc<dyn ActionEngine> = Arc::new(engine);
    let serving: Arc<dyn ServingEngine> = Arc::new(InProcessServingEngine::new(
        IcebergCatalog::new(pool.clone()),
    ));

    // 4. Declare LineItem's backing table (main.line_item) a stream table.
    let lines_tbl = tref("main", "line_item");
    let mut tx = pool.begin().await.expect("begin");
    let at = next_snapshot(&mut tx, None).await.expect("snap");
    let tid = ensure_table(&mut tx, &lines_tbl.schema, &lines_tbl.name, at)
        .await
        .expect("ensure");
    tx.commit().await.expect("commit");
    cp.declare_stream(tid, 4).await.expect("declare_stream");

    // 5. POST the multi-step action over HTTP -> expect 422 with the refusal message,
    //    never 500, and confirm nothing committed. `ActionError::Unsupported` renders
    //    as a plain-text body (not JSON), so `post_action_text` is used (its non-JSON
    //    fallback preserves the raw message; `post_action_raw`'s falls back to Null).
    let (status, body) = e2e_support::post_action_text(
        cp.clone(),
        serving.clone(),
        action_engine.clone(),
        "/actions/createOrderWithLines",
        &json!({ "oid": "500", "li1": "1", "li2": "2" }),
        subj.0.as_str(),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "body: {body}");
    assert!(
        body.to_string().contains("stream-table target refused:"),
        "body: {body}"
    );
}
