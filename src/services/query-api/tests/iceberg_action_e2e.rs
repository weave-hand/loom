//! Iceberg ActionEngine e2e: a governed typed-insert on the Iceberg serving backend
//! lands the row + its lineage atomically (one PG tx), reads back through the
//! loom-native DataFusion serving engine, and is governed by write-enforcement.
//! loom_fixture_test (Postgres + LocalFsStorage warehouse; no DuckDB).

use std::collections::HashMap;
use std::sync::Arc;

// `Acl` is needed in scope to call `define_subject`/`define_role`/`assign_role`/
// `grant` on the concrete `PgControlPlane`. `IcebergCatalog::live_tables` is an
// inherent method, so the `Catalog` trait is intentionally NOT imported (importing
// it unused fails the clippy/lint gate).
use control_plane_core::{
    Acl, Action, ActionDef, ActionName, ControlPlane, DatasetRef, Effect, ObjectType, PageReq,
    ParamDef, PolicyTarget, PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use query_api::action::{ActionDeps, ActionError, run_action};
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::ActionEngine;
use query_api::serving_datafusion::{DataFusionServingEngine, IcebergActionWriter};
use serde_json::json;

/// Build a vendored SqlCatalog over `dsn` + a `file://warehouse` (the action writer
/// needs one even though a single inline row never touches it — `land` only uses the
/// catalog on the Parquet branch).
async fn build_catalog(dsn: &str, warehouse: &std::path::Path) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn.to_string());
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{}", warehouse.display()),
    );
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("build SqlCatalog")
}

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
                },
                PropertyDef {
                    name: "name".into(),
                    ty: "String".into(),
                    required: false,
                },
            ],
            derived: vec![],
            identity: None,
        })
        .await
        .unwrap();
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("createWidget".into()),
            target: widget.clone(),
            parameters: vec![
                ParamDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                },
                ParamDef {
                    name: "name".into(),
                    ty: "String".into(),
                    required: false,
                },
            ],
        })
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
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = Arc::new(build_catalog(&dsn, warehouse.path()).await);

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    // Large flush threshold so the single inline row never enqueues a flush job.
    let engine = IcebergActionWriter::new(catalog, pool.clone(), 16 * 1024 * 1024, i64::MAX);
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
    };
    let body = json!({ "id": "42", "name": "gadget" });
    let (created, run_id) = run_action("createWidget", body.as_object().unwrap(), &subj, &deps)
        .await
        .expect("action runs");
    assert_eq!(
        objects_to_json(&created)["objects"][0],
        json!({ "id": "42", "name": "gadget" })
    );

    // Read back through the Iceberg DataFusion serving engine (inline+file union).
    let serving = DataFusionServingEngine::new(IcebergCatalog::new(pool.clone()), None);
    let qdeps = QueryDeps {
        ontology: cp.ontology(),
        acl: cp.acl(),
        serving: &serving,
    };
    let rows = read_object(
        &ObjectQuery {
            type_name: "Widget".into(),
            eq_filters: vec![],
            ids: vec![],
        },
        &Subject(subj.clone()),
        &qdeps,
    )
    .await
    .unwrap();
    assert_eq!(
        objects_to_json(&rows)["objects"][0],
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
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = Arc::new(build_catalog(&dsn, warehouse.path()).await);

    let _widget = define_widget(&cp).await; // type + action defined, NO grant.
    let engine = IcebergActionWriter::new(catalog, pool.clone(), 16 * 1024 * 1024, i64::MAX);
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
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
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = Arc::new(build_catalog(&dsn, warehouse.path()).await);

    let engine = IcebergActionWriter::new(catalog, pool.clone(), 16 * 1024 * 1024, i64::MAX);
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
