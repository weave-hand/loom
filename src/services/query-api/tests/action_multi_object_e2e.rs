//! Real-Iceberg end-to-end proof of MULTI-OBJECT / MULTI-STEP actions (spec tests 3-7).
//!
//! Each test spins up the REAL in-process engine writer (`spawn_engine_writer`) over a
//! hermetic Postgres + LocalFsStorage warehouse, invokes `run_action` against a
//! multi-step `ActionDef`, and reads the result back through the governed serving path.
//! These cover the paths `multi_step_run.rs` stubs out (that unit test records the
//! `write_steps` call against a fake engine); here the write actually lands (or rolls
//! back) on Iceberg.
//!
//!   * test 3 — order + N line items, cross-step `@order.id` FK wiring, one RunId;
//!   * test 4 — atomic rollback: a 2nd-step failure commits NOTHING (no data, no lineage);
//!   * test 5 — per-step governance: a denied column (403) / constraint violation (422)
//!     on step 2 each roll the whole action back, identical to the single-object gate;
//!   * test 6 — the single RunId's lineage `outputs` lists every step's target;
//!   * test 7 — mixed kinds (Insert + Update on a pre-existing object) commit atomically,
//!     and a vector-typed target still rejects a Delete step via `ensure_cow_supported`.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, ActionStep, Assignment, ControlPlane,
    DatasetRef, Effect, ObjectType, PageReq, ParamDef, Policy, PolicyTarget, PropertyConstraints,
    RangeConstraint, RoleId, SubjectId, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::InProcessServingEngine;
use query_api::action::{ActionDeps, ActionError, WriteDenialReason, run_action};
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::SqlValue;
use serde_json::json;

// ---- small construction helpers (mirroring multi_step_run.rs) --------------------------

fn tn(s: &str) -> TypeName {
    TypeName(s.into())
}

/// A required param renamed away from the property it writes (`binds`).
fn param_bound(name: &str, ty: &str, required: bool, binds: &str) -> ParamDef {
    ParamDef {
        name: name.into(),
        ty: ty.into(),
        required,
        binds: Some(binds.into()),
    }
}

/// A `PropertyConstraints` with only a numeric `max` (for the constraint-violation test).
fn max_constraint(max: f64) -> PropertyConstraints {
    PropertyConstraints {
        range: Some(RangeConstraint {
            min: None,
            max: Some(max),
        }),
        ..PropertyConstraints::default()
    }
}

/// Create a fresh `writer` subject (role `writers`) granted Write + Read on every named
/// type; returns the subject and its role (so callers can refine with a `set_policy`).
async fn writer_on(cp: &PgControlPlane, types: &[&str]) -> (SubjectId, RoleId) {
    let subj = SubjectId("writer".into());
    let role = RoleId("writers".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    for t in types {
        for action in [Action::Write, Action::Read] {
            cp.grant(
                &role,
                action,
                PolicyTarget::Type(TypeName((*t).into())),
                Effect::Allow,
            )
            .await
            .unwrap();
        }
    }
    (subj, role)
}

/// Read every live object of `type_name` (as `subj`) through the governed read path and
/// return the `objects` JSON array. The serving engine is a fresh in-process twin over the
/// same mirror `pool` the engine writer commits to.
async fn read_objects(
    cp: &PgControlPlane,
    pool: &sqlx::PgPool,
    type_name: &str,
    subj: &SubjectId,
) -> Vec<serde_json::Value> {
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let qdeps = QueryDeps {
        ontology: cp.ontology(),
        acl: cp.acl(),
        catalog: cp.catalog(),
        serving: &serving,
        default_limit: 1000,
    };
    let rows = read_object(
        &ObjectQuery {
            type_name: type_name.into(),
            filters: vec![],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
        },
        &Subject(subj.clone()),
        &qdeps,
    )
    .await
    .unwrap();
    objects_to_json(&rows, None)["objects"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

// ---- type + action definitions ---------------------------------------------------------

/// `Order(id Long required identity, note String)`.
async fn define_order(cp: &PgControlPlane) {
    cp.ontology()
        .define_type(
            ObjectType::build("Order", ("main", "order"))
                .prop_req("id", "Long")
                .prop("note", "String")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
}

// ========================================================================================
// Test 3 (+ Test 6 lineage): order + two line items, cross-step FK, one RunId.
// ========================================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn order_with_lines_round_trips_with_cross_step_fk_and_one_run() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    define_order(&cp).await;
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
        .unwrap();

    // createOrderWithLines: step0 inserts the Order (bind `order`); step1 & step2 each
    // insert a LineItem whose `orderId` is `@order.id` (a StepRef into the parent's
    // just-resolved identity). Two line items ⇒ two LineItem steps.
    e2e_support::define_create_order_with_lines_action(&cp).await;

    let (subj, _role) = writer_on(&cp, &["Order", "LineItem"]).await;
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    let body = json!({ "oid": "500", "li1": "1", "li2": "2" });
    let (outcome, run_id, _kind) = run_action(
        "createOrderWithLines",
        body.as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("multi-step action runs");

    let steps = match outcome {
        query_api::action::ActionOutcome::Multi(s) => s,
        query_api::action::ActionOutcome::Single(_) => panic!("multi-step action must yield Multi"),
    };
    assert_eq!(steps.len(), 3, "one result per declared step, in order");
    // Step 0: the bound Order (id 500).
    assert_eq!(steps[0].bind.as_deref(), Some("order"));
    assert_eq!(steps[0].target, "Order");
    assert_eq!(steps[0].rows.rows.len(), 1);
    let oidc = steps[0]
        .rows
        .columns
        .iter()
        .position(|c| c == "id")
        .expect("Order has id");
    assert_eq!(steps[0].rows.rows[0][oidc], SqlValue::Int(500));
    // Steps 1 & 2: the two unbound LineItems, carrying the parent's resolved id.
    for (i, want_id) in [(1usize, 1i64), (2usize, 2i64)] {
        assert_eq!(steps[i].bind, None, "LineItem steps are unbound");
        assert_eq!(steps[i].target, "LineItem");
        let idc = steps[i]
            .rows
            .columns
            .iter()
            .position(|c| c == "id")
            .expect("LineItem has id");
        let fkc = steps[i]
            .rows
            .columns
            .iter()
            .position(|c| c == "orderId")
            .expect("LineItem has orderId");
        assert_eq!(steps[i].rows.rows[0][idc], SqlValue::Int(want_id));
        assert_eq!(
            steps[i].rows.rows[0][fkc],
            SqlValue::Int(500),
            "cross-step FK wired"
        );
    }

    // (a) The Order round-trips through the Iceberg serving engine.
    let orders = read_objects(&cp, &pool, "Order", &subj).await;
    assert_eq!(orders.len(), 1, "one Order created");
    assert_eq!(orders[0]["id"], json!("500"), "Order id round-trips");

    // (b) Both LineItems round-trip AND each carries the parent Order's identity as its
    //     cross-step FK (`orderId == Order.id == 500`).
    let mut lines = read_objects(&cp, &pool, "LineItem", &subj).await;
    lines.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    assert_eq!(lines.len(), 2, "two LineItems created");
    assert_eq!(lines[0]["id"], json!("1"));
    assert_eq!(lines[1]["id"], json!("2"));
    for li in &lines {
        assert_eq!(
            li["orderId"],
            json!("500"),
            "LineItem.orderId is the parent Order's resolved identity"
        );
    }

    // (c) Test 6: one lineage event under the returned RunId, whose `outputs` lists EVERY
    //     step's target dataset (Order + one per LineItem step).
    let events = cp
        .lineage()
        .events_for(&run_id, PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(
        events.items.len(),
        1,
        "one lineage event for the action run"
    );
    assert_eq!(
        events.items[0].outputs,
        vec![
            DatasetRef::from(&tn("Order")),
            DatasetRef::from(&tn("LineItem")),
            DatasetRef::from(&tn("LineItem")),
        ],
        "lineage outputs list every step's target dataset"
    );

    drop(warehouse);
}

// ========================================================================================
// Test 4 (+ Test 5, 422 class): a constraint violation on step 2 rolls the whole action
// back — no object of EITHER type, no lineage.
// ========================================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn constraint_violation_on_step2_rolls_back_everything() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    define_order(&cp).await;
    // LineItem carries a constrained `qty` (max 100); step 2 supplies 999 → 422.
    cp.ontology()
        .define_type(
            ObjectType::build("LineItem", ("main", "line_item"))
                .prop_req("id", "Long")
                .prop_req("orderId", "Long")
                .prop_with("qty", "Long", false, max_constraint(100.0))
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("createOrderWithLine".into()),
            steps: vec![
                ActionStep {
                    target: tn("Order"),
                    kind: ActionKind::Insert,
                    parameters: vec![param_bound("oid", "Long", true, "id")],
                    assignments: vec![],
                    bind: Some("order".into()),
                },
                ActionStep {
                    target: tn("LineItem"),
                    kind: ActionKind::Insert,
                    parameters: vec![
                        param_bound("li1", "Long", true, "id"),
                        param_bound("qty", "Long", false, "qty"),
                    ],
                    assignments: vec![Assignment::step_ref("orderId", "order", "id")],
                    bind: None,
                },
            ],
            downstream: Vec::new(),
        })
        .await
        .unwrap();

    let (subj, _role) = writer_on(&cp, &["Order", "LineItem"]).await;
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    let body = json!({ "oid": "500", "li1": "1", "qty": "999" });
    let err = run_action(
        "createOrderWithLine",
        body.as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect_err("step-2 constraint violation must fail the action");
    assert!(
        matches!(err, ActionError::ConstraintViolation(ref v) if !v.is_empty()),
        "step-2 constraint violation surfaces as the 422 class, got {err:?}"
    );

    // Nothing committed for EITHER type: the multi-step orchestration governs every step
    // before the single `write_steps`, so a step-2 failure means NO mirror table was ever
    // created — including the step-1 Order. A read has nothing to return; the live-table
    // set is the definitive "no rows anywhere" oracle. Lineage commits atomically WITH the
    // snapshot, so no snapshot ⇒ no lineage either.
    let live = IcebergCatalog::new(pool.clone())
        .live_tables()
        .await
        .unwrap();
    assert!(
        live.is_empty(),
        "rolled-back action created no mirror table for any step, got {live:?}"
    );

    drop(warehouse);
}

// ========================================================================================
// Test 5 (403 class): a denied column on step 2 rolls the whole action back.
// ========================================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn denied_column_on_step2_rolls_back_everything() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    define_order(&cp).await;
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
        .unwrap();
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("createOrderWithLine".into()),
            steps: vec![
                ActionStep {
                    target: tn("Order"),
                    kind: ActionKind::Insert,
                    parameters: vec![param_bound("oid", "Long", true, "id")],
                    assignments: vec![],
                    bind: Some("order".into()),
                },
                ActionStep {
                    target: tn("LineItem"),
                    kind: ActionKind::Insert,
                    parameters: vec![
                        param_bound("li1", "Long", true, "id"),
                        param_bound("sku", "String", false, "sku"),
                    ],
                    assignments: vec![Assignment::step_ref("orderId", "order", "id")],
                    bind: None,
                },
            ],
            downstream: Vec::new(),
        })
        .await
        .unwrap();

    let (subj, role) = writer_on(&cp, &["Order", "LineItem"]).await;
    // Coarse Write is allowed; a fine-grained Write policy denies the `sku` column on
    // LineItem — step 2 sets `sku`, so the fine gate rejects it with a 403.
    cp.set_policy(
        &role,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(tn("LineItem")),
            row_filter: None,
            deny_columns: vec!["sku".into()],
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

    let body = json!({ "oid": "500", "li1": "1", "sku": "WIDGET" });
    let err = run_action(
        "createOrderWithLine",
        body.as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect_err("step-2 denied column must fail the action");
    assert!(
        matches!(err, ActionError::WriteDenied(WriteDenialReason::Column(ref c)) if c == "sku"),
        "step-2 denied column surfaces as the 403 class naming `sku`, got {err:?}"
    );

    // Same all-or-nothing oracle as the 422 case: no mirror table for any step.
    let live = IcebergCatalog::new(pool.clone())
        .live_tables()
        .await
        .unwrap();
    assert!(
        live.is_empty(),
        "denied-column action created no mirror table for any step, got {live:?}"
    );

    drop(warehouse);
}

// ========================================================================================
// Test 7a: mixed kinds — an Insert step + an Update step on a PRE-EXISTING object commit
// atomically under one RunId.
// ========================================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_insert_and_update_commit_atomically() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // Order(id identity, status) + LineItem(id identity, orderId).
    cp.ontology()
        .define_type(
            ObjectType::build("Order", ("main", "order"))
                .prop_req("id", "Long")
                .prop("status", "String")
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

    // A single-object seed action to create the PRE-EXISTING Order the multi-step action
    // will later UPDATE.
    cp.ontology()
        .define_action(
            ActionDef::build("createOrder", "Order", ActionKind::Insert)
                .param_req("id", "Long")
                .param("status", "String")
                .done(),
        )
        .await
        .unwrap();
    // fulfillOrder: step0 inserts a new LineItem; step1 UPDATEs the pre-existing Order.
    // Distinct kinds (Insert=Append, Update=Overwrite) and distinct tables in one action.
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("fulfillOrder".into()),
            steps: vec![
                ActionStep {
                    target: tn("LineItem"),
                    kind: ActionKind::Insert,
                    parameters: vec![
                        param_bound("liId", "Long", true, "id"),
                        param_bound("liOrderId", "Long", true, "orderId"),
                    ],
                    assignments: vec![],
                    bind: None,
                },
                ActionStep {
                    target: tn("Order"),
                    kind: ActionKind::Update,
                    parameters: vec![
                        param_bound("ordId", "Long", true, "id"),
                        param_bound("status", "String", false, "status"),
                    ],
                    assignments: vec![],
                    bind: None,
                },
            ],
            downstream: Vec::new(),
        })
        .await
        .unwrap();

    let (subj, _role) = writer_on(&cp, &["Order", "LineItem"]).await;
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed the pre-existing Order (id=1, status="pending").
    run_action(
        "createOrder",
        json!({ "id": "1", "status": "pending" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed order");
    let seeded = read_objects(&cp, &pool, "Order", &subj).await;
    assert_eq!(seeded.len(), 1, "seed committed one Order");
    assert_eq!(seeded[0]["status"], json!("pending"), "seed status");

    // Run the mixed-kind action: insert LineItem 7, update Order 1 → status "shipped".
    let (_rows, run_id, _kind) = run_action(
        "fulfillOrder",
        json!({ "liId": "7", "liOrderId": "1", "ordId": "1", "status": "shipped" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("mixed insert+update action runs");

    // (a) The Insert landed: LineItem 7 exists, FK'd to Order 1.
    let lines = read_objects(&cp, &pool, "LineItem", &subj).await;
    assert_eq!(lines.len(), 1, "one LineItem inserted");
    assert_eq!(lines[0]["id"], json!("7"));
    assert_eq!(lines[0]["orderId"], json!("1"));

    // (b) The Update took effect: still exactly ONE Order (id=1), now "shipped" — the
    //     Overwrite superseded the inline seed row (both tiers end-capped), no duplicate.
    let orders = read_objects(&cp, &pool, "Order", &subj).await;
    assert_eq!(orders.len(), 1, "still exactly one Order after the update");
    assert_eq!(orders[0]["id"], json!("1"));
    assert_eq!(
        orders[0]["status"],
        json!("shipped"),
        "the Update step took effect on the pre-existing Order"
    );

    // (c) One RunId for the whole mixed action; its lineage lists BOTH step targets.
    let events = cp
        .lineage()
        .events_for(&run_id, PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(
        events.items.len(),
        1,
        "one lineage event for the mixed action"
    );
    assert_eq!(
        events.items[0].outputs,
        vec![
            DatasetRef::from(&tn("LineItem")),
            DatasetRef::from(&tn("Order")),
        ],
        "lineage outputs list both the Insert and Update targets"
    );

    drop(warehouse);
}

// ========================================================================================
// Test 8 (I-1 regression): a multi-step Delete that empties a table (its ONLY row removed)
// commits atomically alongside an Insert step — the empty-rows Overwrite end-caps that table
// at the shared snapshot instead of erroring. Before the fix this returned a 500 (the empty
// batch was rejected by `build_object_batches`).
// ========================================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_step_delete_emptying_table_commits_atomically() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    define_order(&cp).await;
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

    // A single-object seed action to create the ONE Order the multi-step action later deletes.
    cp.ontology()
        .define_action(
            ActionDef::build("createOrder", "Order", ActionKind::Insert)
                .param_req("id", "Long")
                .param("note", "String")
                .done(),
        )
        .await
        .unwrap();
    // closeOrder: step0 inserts a LineItem; step1 DELETEs the Order — which is the table's only
    // row, so the staged Overwrite carries EMPTY rows (the single-row → empty end-cap case).
    // Distinct tables, so the same-table multi-mutation guard does not fire.
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("closeOrder".into()),
            steps: vec![
                ActionStep {
                    target: tn("LineItem"),
                    kind: ActionKind::Insert,
                    parameters: vec![
                        param_bound("liId", "Long", true, "id"),
                        param_bound("liOrderId", "Long", true, "orderId"),
                    ],
                    assignments: vec![],
                    bind: None,
                },
                ActionStep {
                    target: tn("Order"),
                    kind: ActionKind::Delete,
                    parameters: vec![param_bound("ordId", "Long", true, "id")],
                    assignments: vec![],
                    bind: None,
                },
            ],
            downstream: Vec::new(),
        })
        .await
        .unwrap();

    let (subj, _role) = writer_on(&cp, &["Order", "LineItem"]).await;
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed the one Order (id=1).
    run_action(
        "createOrder",
        json!({ "id": "1", "note": "n" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed order");
    assert_eq!(
        read_objects(&cp, &pool, "Order", &subj).await.len(),
        1,
        "seed committed one Order"
    );

    // Insert LineItem 9 + delete the only Order in one atomic action.
    let (_rows, run_id, _kind) = run_action(
        "closeOrder",
        json!({ "liId": "9", "liOrderId": "1", "ordId": "1" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("multi-step delete-to-empty + insert commits");

    // (a) The Insert landed.
    let lines = read_objects(&cp, &pool, "LineItem", &subj).await;
    assert_eq!(lines.len(), 1, "one LineItem inserted");
    assert_eq!(lines[0]["id"], json!("9"));

    // (b) The Delete emptied the Order table — the empty-rows Overwrite end-capped both tiers.
    //     A fully-emptied-but-live table still registers in the serving engine as a zero-row
    //     relation over the mirror schema (`iss-serving-empty-table-not-found`), so the governed
    //     read-back SUCCEEDS with zero rows rather than erroring — that empty result IS the
    //     "no live Order remains" signal (and proves the truncate committed, not a 500).
    let serving2 = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let qdeps = QueryDeps {
        ontology: cp.ontology(),
        acl: cp.acl(),
        catalog: cp.catalog(),
        serving: &serving2,
        default_limit: 1000,
    };
    let order_read = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            filters: vec![],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
        },
        &Subject(subj.clone()),
        &qdeps,
    )
    .await
    .expect("emptied-but-live Order table still registers; read succeeds with zero rows");
    assert!(
        order_read.rows.is_empty(),
        "the only Order was deleted, so the read returns zero rows, got {order_read:?}"
    );

    // (c) One RunId whose lineage lists both step targets — the whole action is one snapshot.
    let events = cp
        .lineage()
        .events_for(&run_id, PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(events.items.len(), 1, "one lineage event for the action");
    assert_eq!(
        events.items[0].outputs,
        vec![
            DatasetRef::from(&tn("LineItem")),
            DatasetRef::from(&tn("Order")),
        ],
        "lineage outputs list both the Insert and the Delete targets"
    );

    drop(warehouse);
}

// ========================================================================================
// Test 7b: a vector-typed target still rejects a Delete step via `ensure_cow_supported`.
// ========================================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vector_target_rejects_delete_step() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // A vector-bearing type: the scalar copy-on-write path cannot rewrite list columns,
    // so any Update/Delete step must be rejected before it can drop other rows' vectors.
    cp.ontology()
        .define_type(
            ObjectType::build("VecThing", ("main", "vec_thing"))
                .prop_req("id", "Long")
                .prop_req("embedding", "vector(4)")
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
    // A single BOUND Delete step routes through the multi-step orchestration
    // (`run_multi_step` → `govern_and_build_mutate`), exercising `ensure_cow_supported`
    // on the multi-object write path (not the single-object `run_mutate` path).
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("deleteVec".into()),
            steps: vec![ActionStep {
                target: tn("VecThing"),
                kind: ActionKind::Delete,
                parameters: vec![param_bound("vid", "Long", true, "id")],
                assignments: vec![],
                bind: Some("v".into()),
            }],
            downstream: Vec::new(),
        })
        .await
        .unwrap();

    let (subj, _role) = writer_on(&cp, &["VecThing"]).await;
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    let err = run_action(
        "deleteVec",
        json!({ "vid": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect_err("a Delete step on a vector-typed target must be rejected");
    assert!(
        matches!(err, ActionError::Unsupported(ref m) if m.contains("vector")),
        "vector-typed Delete step is rejected by ensure_cow_supported, got {err:?}"
    );

    // Rejected before any write: no mirror table exists.
    let live = IcebergCatalog::new(pool.clone())
        .live_tables()
        .await
        .unwrap();
    assert!(live.is_empty(), "rejected Delete created no mirror table");

    drop(warehouse);
}
