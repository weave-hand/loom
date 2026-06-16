//! Typed input filters e2e: filter a Double and a Boolean column through read_object.
//! A Text bind would match nothing; coercion to the column's logical type makes it work.
//! An uncoercible value is a 400 (BadFilter).

use std::sync::Arc;

use arrow::array::{BooleanArray, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Acl, Action, DatasetRef, Effect, EventType, LineageEvent, ObjectType, Ontology, PolicyTarget,
    PropertyDef, RoleId, RunId, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::handler::{ObjectQuery, QueryDeps, QueryError, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::EmbeddedDuckDb;
use time::OffsetDateTime;
use uuid::Uuid;

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

async fn land(
    cp: &PgControlPlane,
    store: &Arc<dyn ObjectStore>,
    table: &TableRef,
    schema: Arc<Schema>,
    batch: RecordBatch,
) {
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(table)],
        payload: serde_json::json!({}),
    };
    materialize(
        cp,
        store.clone(),
        MaterializeRequest {
            table,
            schema,
            batches: &[batch],
            file_prefix: "run-1",
            gate: None,
            lineage,
        },
    )
    .await
    .unwrap();
}

/// Seed an Order table with NON-TEXT columns: id Long, amount Double, active Boolean.
/// Rows: (1, 10.5, true), (2, 20.0, false), (3, 10.5, true). The caller MUST keep
/// the returned `DuckLakeWriter` alive (its TempDir holds the Parquet files).
async fn setup(fx: &PgFixture) -> (PgControlPlane, EmbeddedDuckDb, DuckLakeWriter, SubjectId) {
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    let ord = tref("main", "orders");
    let ord_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
    ]));
    let ord_batch = RecordBatch::try_new(
        ord_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![Some(10.5), Some(20.0), Some(10.5)])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
            ])),
        ],
    )
    .unwrap();
    land(&cp, &store, &ord, ord_schema, ord_batch).await;

    cp.define_type(ObjectType {
        name: TypeName("Order".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "amount".into(),
                ty: "Double".into(),
                required: false,
            },
            PropertyDef {
                name: "active".into(),
                ty: "Boolean".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: ord.clone(),
    })
    .await
    .unwrap();

    // Subject `a` (alice) with a role granted Read on Order.
    let subj = SubjectId("alice".into());
    let role = RoleId("alice-role".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName("Order".into())),
        Effect::Allow,
    )
    .await
    .unwrap();

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
    (cp, eng, writer, subj)
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_filters_match_and_reject() {
    let fx = PgFixture::start();
    let (cp, eng, _writer, a) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let ids = |rows: &query_api::handler::ObjectRows| {
        let body = objects_to_json(rows);
        let mut v: Vec<String> = body["objects"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| o["id"].as_str().unwrap().to_string())
            .collect();
        v.sort();
        v
    };

    // Double filter: amount = 10.5 -> rows 1, 3.
    let r = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![("amount".into(), "10.5".into())],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&r), vec!["1".to_string(), "3".to_string()]);

    // Boolean filter: active = true -> rows 1, 3 ; active = false -> row 2.
    let r_true = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![("active".into(), "true".into())],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&r_true), vec!["1".to_string(), "3".to_string()]);
    let r_false = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![("active".into(), "false".into())],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&r_false), vec!["2".to_string()]);

    // Uncoercible value -> BadFilter (400).
    let err = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![("amount".into(), "abc".into())],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, QueryError::BadFilter(_)),
        "uncoercible filter value -> BadFilter"
    );
}
