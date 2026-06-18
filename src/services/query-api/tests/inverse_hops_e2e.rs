//! Inverse-direction traversal e2e: a forward FK chain Customer -> Order -> LineItem is
//! seeded, then traversed *backwards*. Inverse single-hop (Order ~orders-> Customer) and
//! inverse two-hop (LineItem ~lineItems,~orders-> Customer) both serve the correct origin
//! objects; governance still gates every reached type; an ambiguous inbound link is a
//! deterministic error and an unknown inbound link is UnknownLink.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Acl, Action, Cardinality, DatasetRef, Effect, EventType, LineageEvent, LinkBacking, LinkDef,
    ObjectType, Ontology, PolicyTarget, PropertyDef, RoleId, RunId, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use ingest::{MaterializeRequest, materialize};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::handler::{
    ChainQuery, Direction, Hop, QueryDeps, QueryError, Subject, read_linked_chain,
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

fn inv(link: &str) -> Hop {
    Hop {
        link: link.into(),
        direction: Direction::Inverse,
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

/// Seed customer -> orders -> line_items (the same shape as the forward multi-hop e2e),
/// define the three types and the two forward FK links `orders` and `lineItems`, attach
/// the engine. Keep the returned `DuckLakeWriter` alive (its TempDir holds the Parquet).
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
        identity: None,
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
        identity: None,
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

/// Sorted `id` values from a served object set. Note: loom renders a `Long` property as a
/// JSON *string* (not a number), so `id` is read via `as_str()` — matching the existing
/// multi-hop traversal e2e.
fn ids(rows: &query_api::handler::ObjectRows) -> Vec<String> {
    let body = objects_to_json(rows);
    let mut out: Vec<String> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_str().unwrap().to_string())
        .collect();
    out.sort_unstable();
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn inverse_single_hop_reaches_origin_customer() {
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

    // From Order, follow `orders` (Customer -> Order) INVERSE -> the Customers that own
    // an order. Orders 10,11 belong to customer 1; order 20 to customer 2 -> {1,2}.
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Order".into(),
            path: vec![inv("orders")],
            filters: vec![],
            ids: vec![],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&rows), vec!["1".to_string(), "2".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn inverse_two_hop_chain_reaches_origin_customer() {
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

    // From LineItem, INVERSE `lineItems` (Order -> LineItem) -> Order, then INVERSE
    // `orders` (Customer -> Order) -> Customer. All four line items trace back to
    // customers {1,2}.
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "LineItem".into(),
            path: vec![inv("lineItems"), inv("orders")],
            filters: vec![],
            ids: vec![],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&rows), vec!["1".to_string(), "2".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn inverse_hop_is_governed_on_the_reached_type() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    // Grant LineItem (source) and Customer (final) but NOT Order (the intermediate type
    // the inverse `lineItems` hop reaches) -> the whole traversal is Forbidden.
    let (a, role) = subject_with_role(&cp, "carol").await;
    grant_read(&cp, &role, "LineItem").await;
    grant_read(&cp, &role, "Customer").await;

    let err = read_linked_chain(
        &ChainQuery {
            from_type: "LineItem".into(),
            path: vec![inv("lineItems"), inv("orders")],
            filters: vec![],
            ids: vec![],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QueryError::Forbidden), "got {err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_inbound_link_is_unknown_link() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    let (a, role) = subject_with_role(&cp, "dan").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;

    // No link named `ghost` points at Order -> UnknownLink.
    let err = read_linked_chain(
        &ChainQuery {
            from_type: "Order".into(),
            path: vec![inv("ghost")],
            filters: vec![],
            ids: vec![],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, QueryError::UnknownLink(ref l) if l == "ghost"),
        "got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ambiguous_inbound_link_is_rejected() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &eng,
    };
    // Define a SECOND link also named `lineItems` but from Customer -> LineItem, so two
    // links named `lineItems` are inbound to LineItem (from Order and from Customer).
    // (Keying is (name, from), so both persist.) An inverse hop over `lineItems` from
    // LineItem can't pick one deterministically -> AmbiguousLink.
    cp.define_link(LinkDef {
        name: "lineItems".into(),
        from: TypeName("Customer".into()),
        to: TypeName("LineItem".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "order_id".into(),
        },
    })
    .await
    .unwrap();

    let (a, role) = subject_with_role(&cp, "erin").await;
    grant_read(&cp, &role, "LineItem").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "Customer").await;

    let err = read_linked_chain(
        &ChainQuery {
            from_type: "LineItem".into(),
            path: vec![inv("lineItems")],
            filters: vec![],
            ids: vec![],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, QueryError::AmbiguousLink(ref l) if l == "lineItems"),
        "got {err:?}"
    );
}
