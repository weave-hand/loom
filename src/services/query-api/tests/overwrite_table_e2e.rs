//! `ActionEngine::overwrite_table` e2e: land a 2-row table via the action insert path,
//! then call `overwrite_table` with a 1-row replacement set, assert the governed read
//! returns exactly that 1 new row, and the lineage event is findable by run_id.
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, ControlPlane, DatasetRef, Effect, LineageEvent,
    ObjectType, PageReq, ParamDef, PolicyTarget, PropertyDef, RoleId, RunId, SubjectId, TableRef,
    TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::InProcessServingEngine;
use query_api::action::{ActionDeps, run_action};
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::{ActionEngine, SqlValue};
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
async fn overwrite_table_replaces_all_rows_with_atomic_lineage() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    // Large flush threshold so inline rows never enqueue a flush job.
    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Land two rows via the action insert path.
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "alpha" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("insert row 1");
    run_action(
        "createWidget",
        json!({ "id": "2", "name": "beta" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("insert row 2");

    // Verify we have two rows before the overwrite.
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let qdeps = QueryDeps {
        ontology: cp.ontology(),
        acl: cp.acl(),
        catalog: cp.catalog(),
        serving: &serving,
        default_limit: 1000,
    };
    let rows_before = read_object(
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
        objects_to_json(&rows_before, None)["objects"]
            .as_array()
            .unwrap()
            .len(),
        2,
        "two rows before overwrite"
    );

    // Overwrite: replace all rows with a single new row.
    let table = TableRef {
        schema: "main".into(),
        name: "widget".into(),
    };
    let run_id = RunId(uuid::Uuid::new_v4());
    let event = LineageEvent {
        run_id,
        event_type: control_plane_core::EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(&TypeName("Widget".into()))],
        payload: json!({}),
    };

    let _snap = engine
        .overwrite_table(
            &table,
            &["id".to_string(), "name".to_string()],
            &[vec![SqlValue::Int(99), SqlValue::Text("gamma".to_string())]],
            &["Long".to_string(), "String".to_string()],
            event,
        )
        .await
        .expect("overwrite_table succeeds");

    // Read back: must see exactly the 1 new row.
    let serving2 = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let qdeps2 = QueryDeps {
        ontology: cp.ontology(),
        acl: cp.acl(),
        catalog: cp.catalog(),
        serving: &serving2,
        default_limit: 1000,
    };
    let rows_after = read_object(
        &ObjectQuery {
            type_name: "Widget".into(),
            filters: vec![],
            ids: vec![],
            or_raw: Vec::new(),
            as_of: None,
        },
        &Subject(subj.clone()),
        &qdeps2,
    )
    .await
    .unwrap();
    let objs = objects_to_json(&rows_after, None);
    let arr = objs["objects"].as_array().unwrap();
    assert_eq!(arr.len(), 1, "exactly 1 row after overwrite");
    assert_eq!(
        arr[0],
        json!({ "id": "99", "name": "gamma" }),
        "overwritten row has the new values"
    );

    // Lineage committed atomically with the overwrite, findable by run_id.
    let events = cp
        .lineage()
        .events_for(&run_id, PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(
        events.items.len(),
        1,
        "one lineage event for the overwrite run"
    );
    assert_eq!(
        events.items[0].outputs,
        vec![DatasetRef::from(&TypeName("Widget".into()))]
    );

    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_table_empty_rows_truncates() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Land one row.
    run_action(
        "createWidget",
        json!({ "id": "7", "name": "seed" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("insert seed row");

    // Truncate by passing empty rows.
    let table = TableRef {
        schema: "main".into(),
        name: "widget".into(),
    };
    let run_id = RunId(uuid::Uuid::new_v4());
    let event = LineageEvent {
        run_id,
        event_type: control_plane_core::EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(&TypeName("Widget".into()))],
        payload: json!({}),
    };
    let snap = engine
        .overwrite_table(&table, &[], &[], &[], event)
        .await
        .expect("overwrite_table(empty) truncates");
    assert!(snap.0 > 0, "truncate advances the snapshot id");

    // Verify via the mirror catalog: no live data files at the new snapshot.
    // (An empty table is not registered in the DataFusion serving engine, so
    // we check the mirror state directly rather than going through read_object.)
    let ice = IcebergCatalog::new(pool.clone());
    let live_files = ice
        .files_with_stats(&table, snap)
        .await
        .expect("files_with_stats");
    assert!(
        live_files.is_empty(),
        "truncate leaves no live data files at the new snapshot"
    );

    // Lineage event for truncate is committed atomically.
    let events = cp
        .lineage()
        .events_for(&run_id, PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(events.items.len(), 1, "lineage event for truncate run");
    assert_eq!(
        events.items[0].outputs,
        vec![DatasetRef::from(&TypeName("Widget".into()))]
    );

    drop(warehouse);
}
