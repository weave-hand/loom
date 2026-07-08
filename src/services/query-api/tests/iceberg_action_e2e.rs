//! Iceberg ActionEngine e2e: a governed typed-insert on the Iceberg serving backend
//! lands the row + its lineage atomically (one PG tx), reads back through the
//! loom-native DataFusion serving engine, and is governed by write-enforcement.
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

// `Acl` is needed in scope to call `define_subject`/`define_role`/`assign_role`/
// `grant` on the concrete `PgControlPlane`. `IcebergCatalog::live_tables` is an
// inherent method, so the `Catalog` trait is intentionally NOT imported (importing
// it unused fails the clippy/lint gate).
use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, ControlPlane, DatasetRef, Effect, ObjectType,
    PageReq, ParamDef, PolicyTarget, PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::InProcessServingEngine;
use query_api::action::{ActionDeps, ActionError, run_action};
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::ActionEngine;
use serde_json::json;

/// Define `Widget(id Long required, name String)` + a `createWidget` insert action.
async fn define_widget(cp: &PgControlPlane) -> TypeName {
    let widget = TypeName("Widget".into());
    cp.ontology()
        .define_type(ObjectType {
            name: widget.clone(),
            table: TableRef {
                schema: "main".into(),
                name: "widget".into(),
            },
            properties: vec![
                PropertyDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
                PropertyDef {
                    name: "name".into(),
                    ty: "String".into(),
                    required: false,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
            ],
            derived: vec![],
            identity: None,
        })
        .await
        .unwrap();
    cp.ontology()
        .define_action(ActionDef::single_step(
            ActionName("createWidget".into()),
            widget.clone(),
            ActionKind::Insert,
            vec![
                ParamDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                    binds: None,
                },
                ParamDef {
                    name: "name".into(),
                    ty: "String".into(),
                    required: false,
                    binds: None,
                },
            ],
            vec![],
        ))
        .await
        .unwrap();
    widget
}

/// Grant Write + Read on `widget` to a fresh `writer` subject.
async fn grant_writer(cp: &PgControlPlane, widget: &TypeName) -> SubjectId {
    let subj = SubjectId("writer".into());
    let role = RoleId("writers".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Write,
        PolicyTarget::Type(widget.clone()),
        Effect::Allow,
    )
    .await
    .unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(widget.clone()),
        Effect::Allow,
    )
    .await
    .unwrap();
    subj
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn action_inserts_typed_object_readable_with_atomic_lineage() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    // Large flush threshold so the single inline row never enqueues a flush job.
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };
    let body = json!({ "id": "42", "name": "gadget" });
    let (outcome, run_id, _kind) =
        run_action("createWidget", body.as_object().unwrap(), &subj, &deps)
            .await
            .expect("action runs");
    let created = match outcome {
        query_api::action::ActionOutcome::Single(rows) => rows,
        query_api::action::ActionOutcome::Multi(_) => panic!("single-step action yields Single"),
    };
    assert_eq!(
        objects_to_json(&created, None)["objects"][0],
        json!({ "id": "42", "name": "gadget" })
    );

    // Read back through the in-process serving engine (inline+file union).
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
            type_name: "Widget".into(),
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
    assert_eq!(
        objects_to_json(&rows, None)["objects"][0],
        json!({ "id": "42", "name": "gadget" }),
        "round-trips through the Iceberg serving engine"
    );

    // Lineage committed atomically with the row, findable by run_id.
    let events = cp
        .lineage()
        .events_for(&run_id, PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(
        events.items.len(),
        1,
        "one lineage event for the action's run"
    );
    assert_eq!(
        events.items[0].outputs,
        vec![DatasetRef::from(&TypeName("Widget".into()))]
    );

    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ungranted_subject_is_forbidden_and_writes_nothing() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let _widget = define_widget(&cp).await; // type + action defined, NO grant.
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    let subj = SubjectId("nobody".into());
    let err = run_action(
        "createWidget",
        json!({ "id": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, ActionError::Forbidden),
        "ungranted -> Forbidden"
    );

    // Nothing written: enforcement short-circuits before the engine, so no mirror
    // table was ever created.
    let live = IcebergCatalog::new(pool.clone())
        .live_tables()
        .await
        .unwrap();
    assert!(live.is_empty(), "forbidden action created no mirror table");

    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_write_commits_neither_row_nor_lineage() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let table = TableRef {
        schema: "main".into(),
        name: "widget".into(),
    };
    let run_id = control_plane_core::RunId(uuid::Uuid::new_v4());
    let event = control_plane_core::LineageEvent {
        run_id,
        event_type: control_plane_core::EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(&TypeName("Widget".into()))],
        payload: json!({}),
    };

    // A value whose variant does not match its declared type fails batch-building
    // BEFORE land() — the engine's write is all-or-nothing.
    let err = engine
        .write_object(
            &table,
            &["id".to_string()],
            &[query_api::serving::SqlValue::Text("not-a-long".into())],
            &["Long".to_string()],
            event,
        )
        .await
        .expect_err("type mismatch must fail");
    match err {
        query_api::serving::ServingError::Engine(_) => {}
        other => panic!("unexpected error variant: {other}"),
    }

    // Neither a mirror table nor a lineage event was committed.
    let live = IcebergCatalog::new(pool.clone())
        .live_tables()
        .await
        .unwrap();
    assert!(live.is_empty(), "failed write created no mirror table");
    let events = cp
        .lineage()
        .events_for(&run_id, PageReq::unbounded())
        .await
        .unwrap();
    assert!(events.items.is_empty(), "failed write emitted no lineage");

    drop(warehouse);
}
