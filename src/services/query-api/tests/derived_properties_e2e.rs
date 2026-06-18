//! Derived properties e2e: Customer.orderCount (COUNT) + totalSpend (SUM) over the
//! Customer->Order FK link, served through read_object. Both-ends governance: without
//! Read on Order the derived props are omitted; an Order row-filter narrows the aggregate.

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Acl, Action, Aggregation, Cardinality, CompareOp, DatasetRef, DerivedPropertyDef, Effect,
    EventType, LineageEvent, LinkBacking, LinkDef, ObjectType, Ontology, Policy, PolicyTarget,
    PropertyDef, RoleId, RowFilter, RunId, ScalarValue, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object};
use query_api::render::objects_to_json;
use query_api::serving::EmbeddedDuckDb;
use serde_json::json;
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

/// Seed Customer + Order with an FK link Customer-(orders)->Order and the two derived
/// properties orderCount (COUNT) + totalSpend (SUM(amount)). Customer 1 has orders
/// (10, 5.0, 'shipped') and (11, 7.0, 'pending'); customer 2 has none.
/// The caller MUST keep the returned `DuckLakeWriter` alive (its TempDir holds the
/// Parquet files the engine reads).
async fn setup(fx: &PgFixture) -> (PgControlPlane, EmbeddedDuckDb, DuckLakeWriter) {
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    let cust = tref("main", "customer");
    let cust_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let cust_batch = RecordBatch::try_new(
        cust_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    land(&cp, &store, &cust, cust_schema, cust_batch).await;

    let ord = tref("main", "orders");
    let ord_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, true),
        Field::new("status", DataType::Utf8, true),
    ]));
    let ord_batch = RecordBatch::try_new(
        ord_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 11])),
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(Float64Array::from(vec![Some(5.0), Some(7.0)])),
            Arc::new(StringArray::from(vec![Some("shipped"), Some("pending")])),
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
                name: "status".into(),
                ty: "String".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: ord.clone(),
        identity: None,
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![PropertyDef {
            name: "id".into(),
            ty: "Long".into(),
            required: true,
        }],
        derived: vec![
            DerivedPropertyDef {
                name: "orderCount".into(),
                ty: "Long".into(),
                link: "orders".into(),
                agg: Aggregation::Count,
            },
            DerivedPropertyDef {
                name: "totalSpend".into(),
                ty: "Double".into(),
                link: "orders".into(),
                agg: Aggregation::Sum("amount".into()),
            },
        ],
        table: cust.clone(),
        identity: None,
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

async fn subject_with_role(cp: &PgControlPlane, name: &str) -> (SubjectId, RoleId) {
    let subj = SubjectId(name.into());
    let role = RoleId(format!("{name}-role"));
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    (subj, role)
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

fn sorted_objects(rows: &query_api::handler::ObjectRows) -> Vec<serde_json::Value> {
    let body = objects_to_json(rows);
    let mut objs: Vec<serde_json::Value> = body["objects"].as_array().unwrap().clone();
    objs.sort_by_key(|o| o["id"].as_str().unwrap().to_string());
    objs
}

#[tokio::test(flavor = "multi_thread")]
async fn derived_aggregates_are_served_and_governed() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };

    // ---- subject A: Read on Customer AND Order -> sees derived ----
    let (a, a_role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &a_role, "Customer").await;
    grant_read(&cp, &a_role, "Order").await;
    let rows = read_object(
        &ObjectQuery {
            type_name: "Customer".into(),
            eq_filters: vec![],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    let objs = sorted_objects(&rows);
    // Customer 1: 2 orders, total 12.0 ; Customer 2: 0 orders, total 0.0.
    // Long renders as a numeric string; Double as a number (see render.rs / bind_read_e2e).
    assert_eq!(
        objs[0],
        json!({ "id": "1", "orderCount": "2", "totalSpend": 12.0 })
    );
    assert_eq!(
        objs[1],
        json!({ "id": "2", "orderCount": "0", "totalSpend": 0.0 })
    );

    // ---- subject B: Read on Customer ONLY -> derived OMITTED (both-ends governance) ----
    let (b, b_role) = subject_with_role(&cp, "bob").await;
    grant_read(&cp, &b_role, "Customer").await;
    let rows_b = read_object(
        &ObjectQuery {
            type_name: "Customer".into(),
            eq_filters: vec![],
        },
        &Subject(b),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(
        rows_b.columns,
        vec!["id".to_string()],
        "no Read on Order -> both derived props omitted"
    );

    // ---- subject C: Read on both + Order row-filter status='shipped' -> narrowed ----
    let (c, c_role) = subject_with_role(&cp, "carol").await;
    grant_read(&cp, &c_role, "Customer").await;
    grant_read(&cp, &c_role, "Order").await;
    cp.set_policy(
        &c_role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: Some(RowFilter::Compare {
                property: "status".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("shipped".into()),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    let rows_c = read_object(
        &ObjectQuery {
            type_name: "Customer".into(),
            eq_filters: vec![],
        },
        &Subject(c),
        &deps,
    )
    .await
    .unwrap();
    let objs_c = sorted_objects(&rows_c);
    // Only the 'shipped' order (amount 5.0) counts for customer 1; customer 2 still empty.
    assert_eq!(
        objs_c[0],
        json!({ "id": "1", "orderCount": "1", "totalSpend": 5.0 })
    );
    assert_eq!(
        objs_c[1],
        json!({ "id": "2", "orderCount": "0", "totalSpend": 0.0 })
    );
}
