//! Governance tests for UPDATE/DELETE actions: fine-grained Write policy enforcement
//! (deny-column and row-filter) on the COW mutate path, and the vector-guard that rejects
//! UPDATE/DELETE on types with a vector property.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, CompareOp, ControlPlane, Effect, ObjectType, Policy,
    PolicyTarget, RoleId, RowFilter, ScalarValue, SubjectId, TypeName,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{InProcessServingEngine, define_widget, grant_writer_role};
use query_api::action::{ActionDeps, ActionError, WriteDenialReason, run_action};
use serde_json::json;

// ---------------------------------------------------------------------------
// Test 1 — deny-column blocks an UPDATE that sets the denied column
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_column_denied() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let (subj, role) = grant_writer_role(&cp, &widget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, name:"a", qty:1} — no policy yet.
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "a", "qty": "1" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert");

    // Set policy: deny writes to the `qty` column.
    cp.set_policy(
        &role,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: None,
            deny_columns: vec!["qty".to_string()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // UPDATE {id:1, qty:9} must be denied because `qty` is a denied column.
    let err = run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            &err,
            ActionError::WriteDenied(WriteDenialReason::Column(c)) if c == "qty"
        ),
        "expected WriteDenied(Column(\"qty\")), got: {err:?}"
    );

    drop(warehouse);
}

// ---------------------------------------------------------------------------
// Test 2 — row-filter blocks an UPDATE whose *resulting* row fails the filter
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_row_filter_denied_resulting() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let (subj, role) = grant_writer_role(&cp, &widget).await;

    // Set policy first: row_filter `qty < 5`.
    cp.set_policy(
        &role,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: Some(RowFilter::Compare {
                property: "qty".into(),
                op: CompareOp::Lt,
                value: ScalarValue::Int(5),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, qty:1} — passes the filter (1 < 5).
    run_action(
        "createWidget",
        json!({ "id": "1", "qty": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert (qty=1 passes filter)");

    // UPDATE {id:1, qty:9}: resulting row (qty=9) fails `qty < 5` -> WriteDenied(RowFilter).
    let err = run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, ActionError::WriteDenied(WriteDenialReason::RowFilter)),
        "expected WriteDenied(RowFilter) for resulting row violation, got: {err:?}"
    );

    drop(warehouse);
}

// ---------------------------------------------------------------------------
// Test 3 — row-filter blocks an UPDATE whose *existing* row fails the filter
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_row_filter_denied_existing() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let (subj, role) = grant_writer_role(&cp, &widget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, qty:9} BEFORE setting the restrictive policy — no filter yet.
    run_action(
        "createWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert (no policy yet)");

    // Now set policy: row_filter `qty < 5`.
    cp.set_policy(
        &role,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: Some(RowFilter::Compare {
                property: "qty".into(),
                op: CompareOp::Lt,
                value: ScalarValue::Int(5),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // UPDATE {id:1, qty:1}: existing row (qty=9) fails `qty < 5` -> WriteDenied(RowFilter).
    let err = run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, ActionError::WriteDenied(WriteDenialReason::RowFilter)),
        "expected WriteDenied(RowFilter) for existing-row violation, got: {err:?}"
    );

    drop(warehouse);
}

// ---------------------------------------------------------------------------
// Test 4 — row-filter blocks a DELETE whose target row fails the filter
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_row_filter_denied() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let (subj, role) = grant_writer_role(&cp, &widget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, qty:9} and {id:2, qty:1} BEFORE setting the restrictive policy.
    run_action(
        "createWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert 1 (no policy yet)");
    run_action(
        "createWidget",
        json!({ "id": "2", "qty": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert 2 (no policy yet)");

    // Now set policy: row_filter `qty < 5`.
    cp.set_policy(
        &role,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: Some(RowFilter::Compare {
                property: "qty".into(),
                op: CompareOp::Lt,
                value: ScalarValue::Int(5),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // DELETE {id:1}: existing row (qty=9) fails `qty < 5` -> WriteDenied(RowFilter).
    let err = run_action(
        "deleteWidget",
        json!({ "id": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, ActionError::WriteDenied(WriteDenialReason::RowFilter)),
        "expected WriteDenied(RowFilter) for delete on row failing filter, got: {err:?}"
    );

    drop(warehouse);
}

// ---------------------------------------------------------------------------
// Test 5 — vector guard: UPDATE/DELETE on a type with a vector property is Unsupported
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vector_guard() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // Define VectorWidget(id Long identity, embedding vector(4)).
    let vwidget = TypeName("VectorWidget".into());
    cp.ontology()
        .define_type(
            ObjectType::build("VectorWidget", ("main", "vector_widget"))
                .prop_req("id", "Long")
                .prop("embedding", "vector(4)")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();

    // Define deleteVectorWidget (Delete, param id).
    cp.ontology()
        .define_action(
            ActionDef::build("deleteVectorWidget", "VectorWidget", ActionKind::Delete)
                .param_req("id", "Long")
                .done(),
        )
        .await
        .unwrap();

    // Grant Write + Read on VectorWidget.
    let subj = SubjectId("vwriter".into());
    let role = RoleId("vwriters".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Write,
        PolicyTarget::Type(vwidget.clone()),
        Effect::Allow,
    )
    .await
    .unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(vwidget.clone()),
        Effect::Allow,
    )
    .await
    .unwrap();

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // deleteVectorWidget fires ensure_cow_supported before any row lookup -> Unsupported.
    // No row needs to be seeded: the guard fires first.
    let err = run_action(
        "deleteVectorWidget",
        json!({ "id": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, ActionError::Unsupported(_)),
        "expected Unsupported for DELETE on vector type, got: {err:?}"
    );

    drop(warehouse);
}
