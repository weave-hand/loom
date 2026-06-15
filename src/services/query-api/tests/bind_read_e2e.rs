//! Connective-tissue e2e: a dataset landed by the ingest materializer, bound to an
//! ontology type by `bind`, is retrievable through the governed read path. Proves
//! loom's two layers (landing + model) meet.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Acl, Action, DatasetRef, Effect, EventType, LineageEvent, ObjectType, PolicyTarget,
    PropertyDef, RoleId, RunId, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use ingest::{MaterializeRequest, bind, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::EmbeddedDuckDb;
use serde_json::json;
use time::OffsetDateTime;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread")]
async fn landed_then_bound_dataset_is_queryable() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;

    let table = TableRef {
        schema: "main".into(),
        name: "customer".into(),
    };

    // 1. LAND: materialize a dataset (id int64, email varchar, amount float64).
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a@x"), Some("b@x")])),
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5)])),
        ],
    )
    .unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(&table)],
        payload: serde_json::json!({}),
    };
    materialize(
        &cp,
        store.clone(),
        MaterializeRequest {
            table: &table,
            schema,
            batches: &[batch],
            file_prefix: "run-1",
            gate: None,
            lineage,
        },
    )
    .await
    .unwrap();

    // 2. BIND: a Customer type over the landed table (validated against physical schema).
    bind(
        &cp,
        &cp,
        ObjectType {
            name: TypeName("Customer".into()),
            properties: vec![
                PropertyDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                },
                PropertyDef {
                    name: "email".into(),
                    ty: "EmailAddress".into(),
                    required: false,
                },
                PropertyDef {
                    name: "amount".into(),
                    ty: "Double".into(),
                    required: false,
                },
            ],
            derived: vec![],
            table: table.clone(),
        },
    )
    .await
    .unwrap();

    // 3. GRANT a Read ACL.
    let subj = SubjectId("analyst".into());
    let role = RoleId("analysts".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName("Customer".into())),
        Effect::Allow,
    )
    .await
    .unwrap();

    // 4. READ through the governed front door.
    let eng = EmbeddedDuckDb::attach(
        &format!(
            "dbname={} host={} user=postgres",
            db,
            fx.socket_path().display()
        ),
        writer.data_path(),
    )
    .await
    .unwrap();
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let rows = read_object(
        &ObjectQuery {
            type_name: "Customer".into(),
            eq_filters: vec![],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap();

    assert_eq!(
        rows.columns,
        vec!["id".to_string(), "email".to_string(), "amount".to_string()]
    );
    assert_eq!(rows.rows.len(), 2, "both landed rows are retrievable");

    // The typed wire contract end-to-end: id (Long) renders as a STRING, amount
    // (Double) as a number, through the real materialize -> bind -> read path.
    let body = objects_to_json(&rows);
    let mut objs: Vec<serde_json::Value> = body["objects"].as_array().unwrap().clone();
    objs.sort_by_key(|o| o["id"].as_str().unwrap().to_string());
    assert_eq!(
        objs,
        vec![
            json!({ "id": "1", "email": "a@x", "amount": 1.5 }),
            json!({ "id": "2", "email": "b@x", "amount": 2.5 }),
        ],
        "Long id serializes as a string; Double amount as a number"
    );
}
