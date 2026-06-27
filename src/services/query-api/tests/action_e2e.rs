//! Actions e2e: define a type + a named insert action, grant Write, invoke the action
//! (which commits the row and its lineage event atomically), and read the new object
//! back through the governed read path. Also: an ungranted subject is forbidden.
//! Real Postgres + Iceberg (no DuckDB).

use std::collections::HashMap;
use std::sync::Arc;

use control_plane_core::{
    Acl, Action, ActionDef, ActionName, CompareOp, ControlPlane, DatasetRef, Effect, ObjectType,
    PageReq, Policy, PolicyTarget, PropertyDef, RoleId, RowFilter, ScalarValue, SubjectId,
    TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use e2e_support::InProcessServingEngine;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use query_api::action::{ActionDeps, ActionError, run_action};
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving_datafusion::IcebergActionWriter;
use serde_json::json;

/// Build a vendored SqlCatalog over `dsn` + a `file://warehouse`.
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

/// A booted fixture with a fully-granted writer over a two-column `main.widget`.
struct WidgetWriter {
    cp: PgControlPlane,
    pool: sqlx::PgPool,
    widget: TypeName,
    subj: SubjectId,
    role: RoleId,
    engine: IcebergActionWriter,
    warehouse: tempfile::TempDir,
}

/// Boot a fixture, define the `Widget` type + a `createWidget` insert action,
/// grant Write+Read to a `writer` subject, and attach an Iceberg action writer.
async fn setup_widget_writer(fx: &PgFixture) -> WidgetWriter {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = Arc::new(build_catalog(&dsn, warehouse.path()).await);

    // Define the Widget type + a createWidget insert action.
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
                control_plane_core::ParamDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                },
                control_plane_core::ParamDef {
                    name: "name".into(),
                    ty: "String".into(),
                    required: false,
                },
            ],
        })
        .await
        .unwrap();

    // Grant Write on Widget to a subject (and Read, to verify the round-trip).
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

    let engine = IcebergActionWriter::new(catalog, pool.clone(), 16 * 1024 * 1024, i64::MAX);
    WidgetWriter {
        cp,
        pool,
        widget,
        subj,
        role,
        engine,
        warehouse,
    }
}

/// Count Widget rows visible to `subj` via the governed read path. The
/// `IcebergActionWriter` creates the mirror table lazily on first write, so before any
/// row lands there is no `main.widget` table at all; treat that as zero rows (the serving
/// engine only registers tables from `live_tables()`, and querying a missing one errors).
async fn widget_count(cp: &PgControlPlane, pool: &sqlx::PgPool, subj: &SubjectId) -> usize {
    let catalog = IcebergCatalog::new(pool.clone());
    let exists = catalog
        .live_tables()
        .await
        .unwrap()
        .iter()
        .any(|t| t.schema == "main" && t.name == "widget");
    if !exists {
        return 0;
    }
    let eng = InProcessServingEngine::new(catalog);
    let deps = QueryDeps {
        ontology: cp.ontology(),
        acl: cp.acl(),
        serving: &eng,
        default_limit: 1000,
    };
    let rows = read_object(
        &ObjectQuery {
            type_name: "Widget".into(),
            eq_filters: vec![],
            ids: vec![],
        },
        &Subject(subj.clone()),
        &deps,
    )
    .await
    .unwrap();
    rows.rows.len()
}

#[tokio::test(flavor = "multi_thread")]
async fn action_inserts_a_typed_object_that_reads_back_with_atomic_lineage() {
    let fx = PgFixture::start();
    let WidgetWriter {
        cp,
        pool,
        subj,
        engine,
        widget: _,
        role: _,
        warehouse: _,
    } = setup_widget_writer(&fx).await;
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
    };
    let body = json!({ "id": "42", "name": "gadget" });
    let (created, run_id) = run_action("createWidget", body.as_object().unwrap(), &subj, &deps)
        .await
        .expect("action runs");

    // The created object is returned with typed values (Long id as string).
    let created_json = objects_to_json(&created);
    assert_eq!(
        created_json["objects"][0],
        json!({ "id": "42", "name": "gadget" }),
    );

    // It reads back through the governed read path.
    let catalog = IcebergCatalog::new(pool.clone());
    let reader = InProcessServingEngine::new(catalog);
    let qdeps = QueryDeps {
        ontology: cp.ontology(),
        acl: cp.acl(),
        serving: &reader,
        default_limit: 1000,
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
        "round-trips"
    );

    // Lineage is now committed ATOMICALLY with the row and is findable by run_id —
    // exactly the assertion part-1 could not make (it skipped lineage as the dangling
    // slice). The event's outputs name the Widget dataset.
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
}

#[tokio::test(flavor = "multi_thread")]
async fn ungranted_subject_is_forbidden() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");
    let catalog = Arc::new(build_catalog(&dsn, warehouse.path()).await);

    let widget = TypeName("Widget".into());
    cp.ontology()
        .define_type(ObjectType {
            name: widget.clone(),
            table: TableRef {
                schema: "main".into(),
                name: "widget".into(),
            },
            properties: vec![PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            }],
            derived: vec![],
            identity: None,
        })
        .await
        .unwrap();
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("createWidget".into()),
            target: widget.clone(),
            parameters: vec![control_plane_core::ParamDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            }],
        })
        .await
        .unwrap();

    let engine = IcebergActionWriter::new(catalog, pool.clone(), 16 * 1024 * 1024, i64::MAX);
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
    };
    // No grant for this subject -> Forbidden, and nothing written.
    let subj = SubjectId("nobody".into());
    let err = run_action(
        "createWidget",
        json!({ "id": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, ActionError::Forbidden));

    // Nothing written: enforcement short-circuits before the engine, so no mirror
    // table was ever created.
    let live = IcebergCatalog::new(pool.clone())
        .live_tables()
        .await
        .unwrap();
    assert!(live.is_empty(), "forbidden action created no mirror table");

    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_policy_enforces_row_filter_and_deny_column() {
    let fx = PgFixture::start();
    let WidgetWriter {
        cp,
        pool,
        widget,
        subj,
        role,
        engine,
        // Keep the `file://` warehouse TempDir alive for the whole test: the
        // IcebergActionWriter writes Parquet into it and the read-back resolves those files.
        warehouse,
    } = setup_widget_writer(&fx).await;
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
    };

    // --- Phase 1: a Write policy with a row filter `name = "gadget"`.
    cp.set_policy(
        &role,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: Some(RowFilter::Compare {
                property: "name".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("gadget".into()),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // Row that fails the filter -> Forbidden, nothing written.
    let err = run_action(
        "createWidget",
        json!({ "id": "1", "name": "widget" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            err,
            ActionError::WriteDenied(query_api::action::WriteDenialReason::RowFilter)
        ),
        "row filter denies non-gadget: {err:?}"
    );
    assert_eq!(
        widget_count(&cp, &pool, &subj).await,
        0,
        "denied write wrote nothing"
    );

    // Row that satisfies the filter -> allowed.
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "gadget" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("conforming write allowed");
    assert_eq!(
        widget_count(&cp, &pool, &subj).await,
        1,
        "conforming write landed"
    );

    // --- Phase 2: replace the policy with a deny-column on `name` (upsert).
    cp.set_policy(
        &role,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: None,
            deny_columns: vec!["name".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // Setting the denied column -> Forbidden, nothing new written.
    let err = run_action(
        "createWidget",
        json!({ "id": "2", "name": "gadget" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            &err,
            ActionError::WriteDenied(query_api::action::WriteDenialReason::Column(c)) if c == "name"
        ),
        "deny-column blocks setting name: {err:?}"
    );
    assert_eq!(
        widget_count(&cp, &pool, &subj).await,
        1,
        "deny-column write wrote nothing"
    );

    // Not setting the denied column (name is optional) -> allowed.
    run_action(
        "createWidget",
        json!({ "id": "2" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("write that omits the denied column is allowed");
    assert_eq!(
        widget_count(&cp, &pool, &subj).await,
        2,
        "write omitting denied column landed"
    );

    // --- Phase 3: a restrictive *Read* policy must NOT gate writes (independence).
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: Some(RowFilter::Compare {
                property: "id".into(),
                op: CompareOp::Lt,
                value: ScalarValue::Int(0),
            }),
            deny_columns: vec!["name".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    // The Write policy is still the deny-column-`name` one from Phase 2; a write that
    // omits name still succeeds — the Read policy is not consulted on the write path.
    run_action(
        "createWidget",
        json!({ "id": "3" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("a restrictive Read policy does not gate the write");
    // Read policy row_filter (id < 0) excludes all 3 rows -> read returns 0.
    // We can't count via read_object here (the read policy hides them), so we verify
    // the write succeeded by checking that it didn't error.
    // The action itself completed without error, which is the meaningful assertion.
    drop(warehouse);
}
