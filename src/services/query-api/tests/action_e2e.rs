//! Actions e2e: define a type + a named insert action, grant Write, invoke the action
//! (which commits the row and its lineage event atomically), and read the new object
//! back through the governed read path. Also: an ungranted subject is forbidden.
//! Real Postgres + DuckDB.

use std::sync::Arc;

use control_plane_core::{
    Acl, Action, ActionDef, ActionName, CompareOp, ControlPlane, DatasetRef, Effect, ObjectType,
    PageReq, Policy, PolicyTarget, PropertyDef, RoleId, RowFilter, ScalarValue, SubjectId,
    TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::action::{ActionDeps, ActionError, run_action};
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::{DuckLakeActionWriter, EmbeddedDuckDb};
use serde_json::json;

fn parquet_count(dir: &std::path::Path) -> usize {
    fn walk(dir: &std::path::Path, n: &mut usize) {
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, n);
                } else if p.extension().is_some_and(|x| x == "parquet") {
                    *n += 1;
                }
            }
        }
    }
    let mut n = 0;
    walk(dir, &mut n);
    n
}

/// A booted fixture with a fully-granted writer over a two-column `main.widget`.
///
/// The caller owns `fx` (which keeps Postgres alive); `writer_fx` must be kept
/// alive for the duration of the test — its `TempDir` holds the Parquet files
/// the engines read.
struct WidgetWriter {
    cp: PgControlPlane,
    writer_fx: DuckLakeWriter,
    data_path: std::path::PathBuf,
    pg_conn: String,
    widget: TypeName,
    subj: SubjectId,
    role: RoleId,
    engine: DuckLakeActionWriter,
}

/// Boot a fixture, seed `main.widget(id BIGINT, name VARCHAR)`, define the
/// `Widget` type + a `createWidget` insert action, grant Write+Read to a
/// `writer` subject, and attach a DuckLake writer engine.
///
/// Shared by the two action tests that exercise a granted writer;
/// `ungranted_subject_is_forbidden` deliberately builds its own (ungranted,
/// id-only) setup and is left alone.
async fn setup_widget_writer(fx: &PgFixture) -> WidgetWriter {
    let (cp, db) = fx.fresh_db().await;
    let writer_fx = DuckLakeWriter::new(fx.socket_path(), &db);
    writer_fx.bootstrap().await;
    writer_fx
        .seed(
            "main",
            "widget",
            &[
                ("id".into(), "BIGINT".into(), false),
                ("name".into(), "VARCHAR".into(), true),
            ],
            &[],
        )
        .await;
    let data_path = writer_fx.data_path().to_path_buf();
    let pg_conn = format!(
        "dbname={db} host={} user=postgres",
        fx.socket_path().display()
    );

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

    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&data_path).unwrap());
    let engine = DuckLakeActionWriter::new(Arc::new(cp.clone()), store);
    WidgetWriter {
        cp,
        writer_fx,
        data_path,
        pg_conn,
        widget,
        subj,
        role,
        engine,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn action_inserts_a_typed_object_that_reads_back_with_atomic_lineage() {
    let fx = PgFixture::start();
    let WidgetWriter {
        cp,
        data_path,
        pg_conn,
        subj,
        engine,
        writer_fx: _writer,
        widget: _,
        role: _,
    } = setup_widget_writer(&fx).await;
    let deps = ActionDeps { cp: &cp, action_engine: &engine };
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

    // The loom-owned write produced a Parquet data file (the part-1 inline property
    // is retired in favor of atomicity).
    assert!(parquet_count(&data_path) > 0, "action write produced a Parquet file");

    // It reads back through the governed read path.
    let reader = EmbeddedDuckDb::attach(&pg_conn, &data_path).await.unwrap();
    let qdeps = QueryDeps { ontology: cp.ontology(), acl: cp.acl(), serving: &reader };
    let rows = read_object(
        &ObjectQuery { type_name: "Widget".into(), eq_filters: vec![], ids: vec![] },
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
    let events = cp.lineage().events_for(&run_id, PageReq::unbounded()).await.unwrap();
    assert_eq!(events.items.len(), 1, "one lineage event for the action's run");
    assert_eq!(events.items[0].outputs, vec![DatasetRef::from(&TypeName("Widget".into()))]);
}

#[tokio::test(flavor = "multi_thread")]
async fn ungranted_subject_is_forbidden() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer_fx = DuckLakeWriter::new(fx.socket_path(), &db);
    writer_fx.bootstrap().await;
    writer_fx
        .seed(
            "main",
            "widget",
            &[("id".into(), "BIGINT".into(), false)],
            &[],
        )
        .await;
    let pg_conn = format!(
        "dbname={db} host={} user=postgres",
        fx.socket_path().display()
    );

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

    let data_path = writer_fx.data_path().to_path_buf();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&data_path).unwrap());
    let engine = DuckLakeActionWriter::new(Arc::new(cp.clone()), store);
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
    // The table has no rows (forbidden action wrote nothing).
    let count = writer_fx
        .query_scalar("SELECT count(*) FROM lake.main.widget")
        .await;
    assert_eq!(count, "0", "forbidden action wrote nothing");

    // pg_conn referenced to avoid dead-code warnings
    let _ = pg_conn;
}

#[tokio::test(flavor = "multi_thread")]
async fn write_policy_enforces_row_filter_and_deny_column() {
    let fx = PgFixture::start();
    let WidgetWriter {
        cp,
        writer_fx,
        widget,
        subj,
        role,
        engine,
        data_path: _,
        pg_conn: _,
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
        matches!(err, ActionError::Forbidden),
        "row filter denies non-gadget"
    );
    assert_eq!(
        writer_fx
            .query_scalar("SELECT count(*) FROM lake.main.widget")
            .await,
        "0",
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
        writer_fx
            .query_scalar("SELECT count(*) FROM lake.main.widget")
            .await,
        "1",
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
        matches!(err, ActionError::Forbidden),
        "deny-column blocks setting name"
    );
    assert_eq!(
        writer_fx
            .query_scalar("SELECT count(*) FROM lake.main.widget")
            .await,
        "1",
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
        writer_fx
            .query_scalar("SELECT count(*) FROM lake.main.widget")
            .await,
        "2",
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
    assert_eq!(
        writer_fx
            .query_scalar("SELECT count(*) FROM lake.main.widget")
            .await,
        "3",
        "Read policy did not block the write"
    );
}
