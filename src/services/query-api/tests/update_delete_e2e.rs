//! UPDATE/DELETE actions e2e: define a `Widget(id Long identity, name String, qty Long)`
//! type with an `updateWidget` (PATCH) and a `deleteWidget` action, grant Write+Read, and
//! exercise the whole-table copy-on-write mutate path through `run_action`:
//!   1. UPDATE merges only the named columns (PATCH) — unset columns are retained.
//!   2. DELETE removes the located row.
//!   3. A mutate on an absent identity returns `ActionError::NotFound`.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use control_plane_core::{
    Acl, Action, ActionDef, ActionKind, ActionName, ControlPlane, Effect, ObjectType, ParamDef,
    PolicyTarget, PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::InProcessServingEngine;
use query_api::action::{ActionDeps, ActionError, run_action};
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use serde_json::json;

/// Define `Widget(id Long identity, name String, qty Long)` + `updateWidget` (id+qty)
/// and `deleteWidget` (id) actions.
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
                PropertyDef {
                    name: "qty".into(),
                    ty: "Long".into(),
                    required: false,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .unwrap();
    // Insert action covering all three columns (seed rows).
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("createWidget".into()),
            target: widget.clone(),
            parameters: vec![
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
                ParamDef {
                    name: "qty".into(),
                    ty: "Long".into(),
                    required: false,
                    binds: None,
                },
            ],
            kind: ActionKind::Insert,
            assignments: vec![],
        })
        .await
        .unwrap();
    // UPDATE action: id (identity) + qty (PATCH — name is left out, must be retained).
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("updateWidget".into()),
            target: widget.clone(),
            parameters: vec![
                ParamDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                    binds: None,
                },
                ParamDef {
                    name: "qty".into(),
                    ty: "Long".into(),
                    required: true,
                    binds: None,
                },
            ],
            kind: ActionKind::Update,
            assignments: vec![],
        })
        .await
        .unwrap();
    // DELETE action: id only.
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("deleteWidget".into()),
            target: widget.clone(),
            parameters: vec![ParamDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                binds: None,
            }],
            kind: ActionKind::Delete,
            assignments: vec![],
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

/// The single Widget object visible to `subj` for `id`, as JSON (or `None`).
async fn read_widget(
    cp: &PgControlPlane,
    pool: &sqlx::PgPool,
    subj: &SubjectId,
    id: i64,
) -> Option<serde_json::Value> {
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let qdeps = QueryDeps {
        ontology: cp.ontology(),
        acl: cp.acl(),
        serving: &serving,
        default_limit: 1000,
    };
    let rows = read_object(
        &ObjectQuery {
            type_name: "Widget".into(),
            filters: vec![],
            ids: vec![],
            or_raw: Vec::new(),
        },
        &Subject(subj.clone()),
        &qdeps,
    )
    .await
    .unwrap();
    objects_to_json(&rows)["objects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["id"] == json!(id.to_string()))
        .cloned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_merges_named_columns() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(&fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, name:"a", qty:1}.
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

    // UPDATE {id:1, qty:9} — name is NOT a param, so it must be retained (PATCH).
    let (affected, _run) = run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("update runs");
    // The affected object returns the new version.
    let affected_json = objects_to_json(&affected);
    assert_eq!(affected_json["objects"][0]["qty"], json!("9"));
    assert_eq!(affected_json["objects"][0]["name"], json!("a"));

    // Read back: qty updated to 9, name retained as "a".
    let row = read_widget(&cp, &pool, &subj, 1)
        .await
        .expect("row present");
    assert_eq!(row["qty"], json!("9"), "qty updated");
    assert_eq!(row["name"], json!("a"), "name retained (PATCH)");

    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_removes_row() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(&fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed two rows so the table stays non-empty after deleting one (an empty table is
    // unregistered in the serving engine, which would make the read-back error rather than
    // report zero rows — the truncate-to-empty case is covered by overwrite_table_e2e).
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "a", "qty": "1" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert 1");
    run_action(
        "createWidget",
        json!({ "id": "2", "name": "b", "qty": "2" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert 2");
    assert!(
        read_widget(&cp, &pool, &subj, 1).await.is_some(),
        "row 1 present before delete"
    );

    // DELETE {id:1}.
    run_action(
        "deleteWidget",
        json!({ "id": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("delete runs");

    // Read back: no row with id=1, but row 2 is untouched.
    assert!(
        read_widget(&cp, &pool, &subj, 1).await.is_none(),
        "row 1 removed after delete"
    );
    assert!(
        read_widget(&cp, &pool, &subj, 2).await.is_some(),
        "row 2 retained after deleting row 1"
    );

    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn not_found() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(&fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed one unrelated row so the table exists and the read returns rows.
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

    // DELETE an absent identity -> NotFound.
    let err = run_action(
        "deleteWidget",
        json!({ "id": "999" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, ActionError::NotFound),
        "mutate on absent identity is NotFound: {err:?}"
    );

    drop(warehouse);
}
