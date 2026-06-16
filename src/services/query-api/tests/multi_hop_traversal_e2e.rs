//! Multi-hop traversal e2e: Customer -> Order -> LineItem, governed at every hop.
//! Served + correct; an intermediate Order row-filter narrows the reachable LineItems;
//! denying Read on the intermediate Order type forbids the whole traversal.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
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
use query_api::handler::{
    ChainFilter, ChainQuery, QueryDeps, QueryError, Subject, read_linked_chain,
};
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

fn srcf(col: &str, val: &str) -> ChainFilter {
    ChainFilter {
        position: 0,
        column: col.into(),
        raw: val.into(),
    }
}

fn hopf(position: usize, col: &str, val: &str) -> ChainFilter {
    ChainFilter {
        position,
        column: col.into(),
        raw: val.into(),
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

/// Seed three tables forming the FK chain customer -> orders -> line_items, define the
/// three ontology types and the two FK links, attach the serving engine. The caller MUST
/// keep the returned `DuckLakeWriter` alive (its TempDir holds the Parquet files the engine
/// reads; dropping it deletes them out from under DuckDB).
async fn setup(fx: &PgFixture) -> (PgControlPlane, EmbeddedDuckDb, DuckLakeWriter) {
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // customer(id, region): (1,'CA'), (2,'NY')
    let cust = tref("main", "customer");
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    let cust_batch = RecordBatch::try_new(
        cust_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("CA"), Some("NY")])),
        ],
    )
    .unwrap();
    land(&cp, &store, &cust, cust_schema, cust_batch).await;

    // orders(id, customer_id, status): (10,1,'shipped'),(11,1,'pending'),(20,2,'shipped')
    let ord = tref("main", "orders");
    let ord_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
    ]));
    let ord_batch = RecordBatch::try_new(
        ord_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 11, 20])),
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(StringArray::from(vec![
                Some("shipped"),
                Some("pending"),
                Some("shipped"),
            ])),
        ],
    )
    .unwrap();
    land(&cp, &store, &ord, ord_schema, ord_batch).await;

    // line_items(id, order_id, sku): (100,10,'A'),(101,10,'B'),(102,11,'C'),(200,20,'D')
    let li = tref("main", "line_items");
    let li_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("order_id", DataType::Int64, false),
        Field::new("sku", DataType::Utf8, true),
    ]));
    let li_batch = RecordBatch::try_new(
        li_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![100, 101, 102, 200])),
            Arc::new(Int64Array::from(vec![10, 10, 11, 20])),
            Arc::new(StringArray::from(vec![
                Some("A"),
                Some("B"),
                Some("C"),
                Some("D"),
            ])),
        ],
    )
    .unwrap();
    land(&cp, &store, &li, li_schema, li_batch).await;

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
                name: "status".into(),
                ty: "String".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: ord.clone(),
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("LineItem".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "order_id".into(),
                ty: "Long".into(),
                required: true,
            },
            PropertyDef {
                name: "sku".into(),
                ty: "String".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: li.clone(),
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
    cp.define_link(LinkDef {
        name: "lineItems".into(),
        from: TypeName("Order".into()),
        to: TypeName("LineItem".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "order_id".into(),
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

#[tokio::test(flavor = "multi_thread")]
async fn multi_hop_served_and_governed() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };

    // ---- subject A: Read on all three -> sees the reachable LineItems ----
    let (a, a_role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &a_role, "Customer").await;
    grant_read(&cp, &a_role, "Order").await;
    grant_read(&cp, &a_role, "LineItem").await;
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![srcf("region", "CA")],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap();
    let body = objects_to_json(&rows);
    let mut ids: Vec<String> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    // Customer 1 (CA) -> orders 10,11 -> line_items 100,101 (order 10) + 102 (order 11).
    assert_eq!(
        ids,
        vec!["100".to_string(), "101".to_string(), "102".to_string()]
    );

    // ---- subject C: Read on all three + Order row-filter status='shipped' ----
    let (c, c_role) = subject_with_role(&cp, "carol").await;
    grant_read(&cp, &c_role, "Customer").await;
    grant_read(&cp, &c_role, "Order").await;
    grant_read(&cp, &c_role, "LineItem").await;
    cp.set_policy(
        &c_role,
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
    let rows_c = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![srcf("region", "CA")],
        },
        &Subject(c.clone()),
        &deps,
    )
    .await
    .unwrap();
    let body_c = objects_to_json(&rows_c);
    let mut ids_c: Vec<String> = body_c["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().to_string())
        .collect();
    ids_c.sort();
    // Only shipped order 10 is traversable -> line_items 100,101 (102 via pending order 11 dropped).
    assert_eq!(ids_c, vec!["100".to_string(), "101".to_string()]);

    // ---- subject B: Read on Customer + LineItem but NOT Order -> 403 ----
    let (b, b_role) = subject_with_role(&cp, "bob").await;
    grant_read(&cp, &b_role, "Customer").await;
    grant_read(&cp, &b_role, "LineItem").await;
    let err = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![],
        },
        &Subject(b.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, QueryError::Forbidden),
        "no Read on intermediate Order -> Forbidden"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn target_filter_narrows_final_set() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let (a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;

    // Customer 1 (CA) reaches line_items 100,101,102; a final-target sku filter narrows.
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![srcf("region", "CA"), hopf(2, "sku", "A")],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    let body = objects_to_json(&rows);
    let ids: Vec<String> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, vec!["100".to_string()], "only line_item 100 has sku=A");
}

#[tokio::test(flavor = "multi_thread")]
async fn intermediate_typed_filter_coerces_and_narrows() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let (a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;

    // Intermediate Order.id is Long: filtering id=10 coerces to Int(10) and binds at the
    // intermediate position t_1 (proving typed coercion flows through a non-source hop, not
    // just the source). Order 10 matches -> line_items 100,101 (102 hangs off order 11,
    // excluded). (The text-vs-typed counterfactual for non-castable types is covered in
    // typed_filter_e2e.rs with Double/Boolean columns.)
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![srcf("region", "CA"), hopf(1, "id", "10")],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    let body = objects_to_json(&rows);
    let mut ids: Vec<String> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["100".to_string(), "101".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn source_and_intermediate_filters_combine() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let (a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;

    // Source region=CA AND intermediate Order.status=pending -> only order 11 -> line_item 102.
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![srcf("region", "CA"), hopf(1, "status", "pending")],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    let body = objects_to_json(&rows);
    let ids: Vec<String> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, vec!["102".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_positioned_filters_are_rejected() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let (a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;
    // Deny the final-target sku column for this subject.
    cp.set_policy(
        &role,
        Policy {
            target: PolicyTarget::Type(TypeName("LineItem".into())),
            row_filter: None,
            deny_columns: vec!["sku".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // A filter on a denied target column -> BadFilter (visibility before coercion).
    let denied = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![hopf(2, "sku", "A")],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(denied, QueryError::BadFilter(ref c) if c == "sku"),
        "denied target column filter -> BadFilter; got {denied:?}"
    );

    // An operator filter on the same denied column is rejected too (visibility precedes parse).
    let denied_op = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![hopf(2, "sku", "ne:A")],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(denied_op, QueryError::BadFilter(ref c) if c == "sku"),
        "operator filter on denied column -> BadFilter; got {denied_op:?}"
    );

    // A position past the end of the chain -> BadFilter (guarded, never panics).
    let oob = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![hopf(5, "id", "1")],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(oob, QueryError::BadFilter(ref c) if c == "id"),
        "out-of-range position -> BadFilter; got {oob:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn intermediate_comparison_operator_narrows() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let (a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;

    // Intermediate Order.id is Long: id > 10 keeps order 11 (drops order 10) for Customer 1,
    // so only line_item 102 (which hangs off order 11) is reachable.
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![srcf("region", "CA"), hopf(1, "id", "gt:10")],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    let body = objects_to_json(&rows);
    let ids: Vec<String> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids, vec!["102".to_string()]);
}
