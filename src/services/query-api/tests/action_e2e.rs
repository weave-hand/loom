//! Actions e2e: define a type + a named insert action, grant Write, invoke the action,
//! and read the new object back through the governed read path. Also: an ungranted
//! subject is forbidden. Real Postgres + DuckDB.

use control_plane_core::{
    Acl, Action, ActionDef, ActionName, CompareOp, ControlPlane, Effect, ObjectType, ParamDef,
    Policy, PolicyTarget, PropertyDef, RoleId, RowFilter, ScalarValue, SubjectId, TableRef,
    TypeName,
};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use query_api::action::{ActionDeps, ActionError, run_action};
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::{EmbeddedDuckDb, EmbeddedDuckDbWriter};
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

#[tokio::test(flavor = "multi_thread")]
async fn action_inserts_a_typed_object_that_reads_back() {
    let fx = PgFixture::start();
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

    let engine = EmbeddedDuckDbWriter::attach(&pg_conn, &data_path)
        .await
        .unwrap();
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
    };
    let body = json!({ "id": "42", "name": "gadget" });
    let created = run_action("createWidget", body.as_object().unwrap(), &subj, &deps)
        .await
        .expect("action runs");
    // The created object is returned with typed values.
    let created_json = objects_to_json(&created);
    assert_eq!(
        created_json["objects"][0],
        json!({ "id": "42", "name": "gadget" }),
        "created object echoed as typed JSON (Long id as string)"
    );

    // It inlined — no Parquet data file was written.
    assert_eq!(
        parquet_count(&data_path),
        0,
        "action write inlined, no Parquet"
    );

    // It reads back through the governed read path.
    let reader = EmbeddedDuckDb::attach(&pg_conn, &data_path).await.unwrap();
    let qdeps = QueryDeps {
        ontology: cp.ontology(),
        acl: cp.acl(),
        serving: &reader,
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
    let read_json = objects_to_json(&rows);
    assert_eq!(
        read_json["objects"][0],
        json!({ "id": "42", "name": "gadget" }),
        "round-trips"
    );

    // Lineage: run_action emits a best-effort, type-named LineageEvent for the write
    // (inputs=[], outputs=[Widget]). It is intentionally NOT asserted here — the event has
    // no inputs and run_action doesn't surface its run_id, so upstream/downstream/events_for
    // can't locate it from the test. Emission is exercised by run_action's code path;
    // strict, queryable action lineage is a follow-on (the dangling-slice note, Task 9).
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
            parameters: vec![ParamDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            }],
        })
        .await
        .unwrap();

    let engine = EmbeddedDuckDbWriter::attach(&pg_conn, writer_fx.data_path())
        .await
        .unwrap();
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
}

#[tokio::test(flavor = "multi_thread")]
async fn write_policy_enforces_row_filter_and_deny_column() {
    let fx = PgFixture::start();
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

    let engine = EmbeddedDuckDbWriter::attach(&pg_conn, &data_path)
        .await
        .unwrap();
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
