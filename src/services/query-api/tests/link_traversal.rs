//! Governed link traversal e2e: land Customer + Order tables, define an FK link and a
//! many-to-many link, and exercise the both-ends governance matrix against real DuckDB.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Acl, Action, Cardinality, CompareOp, DatasetRef, Effect, EventType, LineageEvent, LinkBacking,
    LinkDef, ObjectType, Ontology, Policy, PolicyTarget, PropertyDef, RoleId, RowFilter, RunId,
    ScalarValue, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::handler::{LinkQuery, QueryDeps, QueryError, Subject, read_linked_objects};
use query_api::serving::{EmbeddedDuckDb, SqlValue};
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

/// Seed two types (Customer, Order) + an FK link Customer-(orders)->Order, and land
/// rows. Returns (cp, attached serving engine, the live writer that owns the data dir).
/// The caller MUST keep the returned `DuckLakeWriter` alive: its `TempDir` holds the
/// Parquet files the engine reads, and dropping it deletes them out from under DuckDB.
async fn setup(fx: &PgFixture) -> (PgControlPlane, EmbeddedDuckDb, DuckLakeWriter) {
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    let cust = tref("main", "customer");
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    let cust_batch = RecordBatch::try_new(
        cust_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("CA"), Some("NY"), Some("CA")])),
        ],
    )
    .unwrap();
    land(&cp, &store, &cust, cust_schema, cust_batch).await;

    let ord = tref("main", "orders");
    let ord_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, true),
        Field::new("secret", DataType::Utf8, true),
    ]));
    let ord_batch = RecordBatch::try_new(
        ord_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 11, 12, 13])),
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(Float64Array::from(vec![
                Some(50.0),
                Some(200.0),
                Some(70.0),
                Some(300.0),
            ])),
            Arc::new(StringArray::from(vec![
                Some("x"),
                Some("y"),
                Some("z"),
                Some("w"),
            ])),
        ],
    )
    .unwrap();
    land(&cp, &store, &ord, ord_schema, ord_batch).await;

    cp.define_type(ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "region".into(),
                ty: "String".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: cust.clone(),
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("Order".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "customer_id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "amount".into(),
                ty: "Double".into(),
                required: false,
            },
            PropertyDef {
                name: "secret".into(),
                ty: "String".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: ord.clone(),
    })
    .await
    .unwrap();

    cp.define_link(LinkDef {
        name: "orders".into(),
        from: TypeName("Customer".into()),
        to: TypeName("Order".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
    })
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
    (cp, eng, writer)
}

async fn grant_read(cp: &PgControlPlane, role: &RoleId, type_name: &str) {
    cp.grant(
        role,
        Action::Read,
        PolicyTarget::Type(TypeName(type_name.into())),
        Effect::Allow,
    )
    .await
    .unwrap();
}

async fn analyst(cp: &PgControlPlane) -> (SubjectId, RoleId) {
    let subj = SubjectId("analyst".into());
    let role = RoleId("analysts".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    (subj, role)
}

fn ids_of(rows: &query_api::handler::ObjectRows) -> Vec<i64> {
    let mut ids: Vec<i64> = rows
        .rows
        .iter()
        .map(|r| match &r[0] {
            SqlValue::Int(i) => *i,
            other => panic!("id not int: {other:?}"),
        })
        .collect();
    ids.sort();
    ids
}

#[tokio::test(flavor = "multi_thread")]
async fn fk_traversal_returns_linked_targets() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let (subj, role) = analyst(&cp).await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;

    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let rows = read_linked_objects(
        &LinkQuery {
            from_type: "Customer".into(),
            link: "orders".into(),
            source_filters: vec![("region".into(), "CA".into())],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(rows.columns, vec!["id", "customer_id", "amount", "secret"]);
    assert_eq!(ids_of(&rows), vec![10, 11, 13]);
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_read_on_source_is_forbidden() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let (subj, role) = analyst(&cp).await;
    grant_read(&cp, &role, "Order").await;

    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let err = read_linked_objects(
        &LinkQuery {
            from_type: "Customer".into(),
            link: "orders".into(),
            source_filters: vec![],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QueryError::Forbidden));
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_read_on_target_is_forbidden() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let (subj, role) = analyst(&cp).await;
    grant_read(&cp, &role, "Customer").await;

    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let err = read_linked_objects(
        &LinkQuery {
            from_type: "Customer".into(),
            link: "orders".into(),
            source_filters: vec![],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QueryError::Forbidden));
}

#[tokio::test(flavor = "multi_thread")]
async fn source_row_filter_closes_the_leak() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let (subj, role) = analyst(&cp).await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    cp.set_policy(
        &role,
        Policy {
            target: PolicyTarget::Type(TypeName("Customer".into())),
            row_filter: Some(RowFilter::Compare {
                property: "region".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("CA".into()),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let rows = read_linked_objects(
        &LinkQuery {
            from_type: "Customer".into(),
            link: "orders".into(),
            source_filters: vec![],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(
        ids_of(&rows),
        vec![10, 11, 13],
        "order 12 (NY customer) excluded via source policy"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn target_row_filter_and_projection_apply() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let (subj, role) = analyst(&cp).await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    cp.set_policy(
        &role,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: Some(RowFilter::Compare {
                property: "amount".into(),
                op: CompareOp::Gt,
                value: ScalarValue::Int(100),
            }),
            deny_columns: vec!["secret".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let rows = read_linked_objects(
        &LinkQuery {
            from_type: "Customer".into(),
            link: "orders".into(),
            source_filters: vec![],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap();
    assert!(
        !rows.columns.contains(&"secret".to_string()),
        "secret column denied"
    );
    assert_eq!(ids_of(&rows), vec![11, 13]);
}

#[tokio::test(flavor = "multi_thread")]
async fn source_filter_on_denied_column_is_bad_filter() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let (subj, role) = analyst(&cp).await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    cp.set_policy(
        &role,
        Policy {
            target: PolicyTarget::Type(TypeName("Customer".into())),
            row_filter: None,
            deny_columns: vec!["region".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let err = read_linked_objects(
        &LinkQuery {
            from_type: "Customer".into(),
            link: "orders".into(),
            source_filters: vec![("region".into(), "CA".into())],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QueryError::BadFilter(c) if c == "region"));
}

#[tokio::test(flavor = "multi_thread")]
async fn many_to_many_dedups_shared_targets() {
    let fx = PgFixture::start();
    let (cp, eng, writer) = setup(&fx).await;
    let (subj, role) = analyst(&cp).await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;

    // Land the mapping table into the SAME data dir the engine reads (writer.data_path()),
    // not a fresh writer (whose TempDir would be a different, unread directory).
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());
    let map = tref("main", "customer_order");
    let map_schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("order_id", DataType::Int64, false),
    ]));
    let map_batch = RecordBatch::try_new(
        map_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 3])),
            Arc::new(Int64Array::from(vec![11, 11])),
        ],
    )
    .unwrap();
    land(&cp, &store, &map, map_schema, map_batch).await;

    cp.define_link(LinkDef {
        name: "shared".into(),
        from: TypeName("Customer".into()),
        to: TypeName("Order".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::JoinTable {
            table: map.clone(),
            from_key: "id".into(),
            from_column: "customer_id".into(),
            to_column: "order_id".into(),
            to_key: "id".into(),
        },
    })
    .await
    .unwrap();

    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let rows = read_linked_objects(
        &LinkQuery {
            from_type: "Customer".into(),
            link: "shared".into(),
            source_filters: vec![("region".into(), "CA".into())],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids_of(&rows), vec![11], "shared target deduped to one row");
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_link_is_reported() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let (subj, role) = analyst(&cp).await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;

    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let err = read_linked_objects(
        &LinkQuery {
            from_type: "Customer".into(),
            link: "nope".into(),
            source_filters: vec![],
        },
        &Subject(subj),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QueryError::UnknownLink(l) if l == "nope"));
}
