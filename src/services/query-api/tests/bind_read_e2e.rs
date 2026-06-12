//! Connective-tissue e2e: a dataset landed by the ingest materializer, bound to an
//! ontology type by `bind`, is retrievable through the governed read path. Proves
//! loom's two layers (landing + model) meet.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Acl, Action, DatasetRef, Effect, EventType, LineageEvent, ObjectType, PolicyTarget,
    PropertyDef, RoleId, RunId, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use ingest::{MaterializeRequest, bind, materialize};
use object_store::local::LocalFileSystem;
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::serving::{EmbeddedDuckDb, SqlValue};
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

    // 1. LAND: materialize a dataset (id int64, email varchar).
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a@x"), Some("b@x")])),
        ],
    )
    .unwrap();
    let store = LocalFileSystem::new_with_prefix(writer.data_path()).unwrap();
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef {
            namespace: "loom-ingest".into(),
            name: "main.customer".into(),
        }],
        payload: serde_json::json!({}),
    };
    materialize(
        &cp,
        &store,
        MaterializeRequest {
            table: &table,
            schema,
            batches: &[batch],
            file_name: "loom.parquet",
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
            ],
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
    let eng = EmbeddedDuckDb::attach(fx.socket_path(), &db, writer.data_path())
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

    assert_eq!(rows.columns, vec!["id".to_string(), "email".to_string()]);
    assert_eq!(rows.rows.len(), 2, "both landed rows are retrievable");
    let ids: Vec<&SqlValue> = rows.rows.iter().map(|r| &r[0]).collect();
    assert!(
        ids.contains(&&SqlValue::Int(1)) && ids.contains(&&SqlValue::Int(2)),
        "both row ids (1 and 2) must be present"
    );
}
