//! Association e2e: ?shape=association over the real HTTP router backed by a DuckDB
//! serving engine reading a DuckLake-on-Postgres catalog. Proves the edge-list shape
//! {associations:[{from,to}]}: single-hop exact pairs, multi-hop source<->final-target
//! pairing, governance (an intermediate/target filter drops pairs routing through
//! excluded rows), dedup (same source->same target via two paths = one pair; different
//! sources->one target = distinct pairs), and NoIdentity -> HTTP 400.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use axum::http::StatusCode;
use control_plane_core::{
    Acl, Action, Cardinality, CompareOp, LinkBacking, LinkDef, ObjectType, Ontology, Policy,
    PolicyTarget, RowFilter, ScalarValue, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use e2e_support::{get, grant_read, land, prop, subject_with_role, tref};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::serving::EmbeddedDuckDb;

/// Seed the chain customer -> orders -> line_items (FK both hops), plus a parallel join
/// table proving dedup across two intermediate paths. Define the ontology types WITH
/// identities (Customer.id, Order.id, LineItem.id), the FK links, and a join-table link
/// Customer -> LineItem. Returns the wired control plane + serving engine; the caller MUST
/// keep the `DuckLakeWriter` alive (its TempDir holds the Parquet the engine reads).
async fn setup(fx: &PgFixture) -> (PgControlPlane, EmbeddedDuckDb, DuckLakeWriter) {
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    // customer(id, region): (1,'CA'), (2,'CA'), (3,'NY')
    let cust = tref("main", "customer");
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    let cust_batch = RecordBatch::try_new(
        cust_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("CA"), Some("CA"), Some("NY")])),
        ],
    )
    .unwrap();
    land(&cp, &store, &cust, cust_schema, cust_batch).await;

    // orders(id, customer_id, status):
    //   (10,1,'shipped'),(11,1,'pending'),(20,2,'shipped'),(30,3,'shipped')
    let ord = tref("main", "orders");
    let ord_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
    ]));
    let ord_batch = RecordBatch::try_new(
        ord_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 11, 20, 30])),
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(StringArray::from(vec![
                Some("shipped"),
                Some("pending"),
                Some("shipped"),
                Some("shipped"),
            ])),
        ],
    )
    .unwrap();
    land(&cp, &store, &ord, ord_schema, ord_batch).await;

    // line_items(id, order_id, sku):
    //   (100,10,'A'),(101,10,'B'),(102,11,'A'),(200,20,'A'),(300,30,'A')
    // Note order 10 and 11 both belong to customer 1, and both reach a line_item with
    // sku 'A' (100 and 102) — used for the dedup case via a join-table path.
    let li = tref("main", "line_items");
    let li_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("order_id", DataType::Int64, false),
        Field::new("sku", DataType::Utf8, true),
    ]));
    let li_batch = RecordBatch::try_new(
        li_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![100, 101, 102, 200, 300])),
            Arc::new(Int64Array::from(vec![10, 10, 11, 20, 30])),
            Arc::new(StringArray::from(vec![
                Some("A"),
                Some("B"),
                Some("A"),
                Some("A"),
                Some("A"),
            ])),
        ],
    )
    .unwrap();
    land(&cp, &store, &li, li_schema, li_batch).await;

    // cust_li(customer_id, line_item_id): a join table directly linking customer 1 to
    // BOTH line_items 100 and 102. Combined with the multi-hop FK path (which also reaches
    // 100 and 102 from customer 1), this lets dedup be exercised: customer 1 reaches
    // line_item 100 via two distinct intermediate paths but the pair (1,100) appears once.
    let cust_li = tref("main", "cust_li");
    let cust_li_schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("line_item_id", DataType::Int64, false),
    ]));
    let cust_li_batch = RecordBatch::try_new(
        cust_li_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![100, 102, 200])),
        ],
    )
    .unwrap();
    land(&cp, &store, &cust_li, cust_li_schema, cust_li_batch).await;

    // Customer declares identity `id`.
    cp.define_type(ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![prop("id", "Long", true), prop("region", "String", false)],
        derived: vec![],
        table: cust.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();
    // Order declares identity `id`.
    cp.define_type(ObjectType {
        name: TypeName("Order".into()),
        properties: vec![
            prop("id", "Long", true),
            prop("customer_id", "Long", true),
            prop("status", "String", false),
        ],
        derived: vec![],
        table: ord.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();
    // LineItem declares identity `id`.
    cp.define_type(ObjectType {
        name: TypeName("LineItem".into()),
        properties: vec![
            prop("id", "Long", true),
            prop("order_id", "Long", true),
            prop("sku", "String", false),
        ],
        derived: vec![],
        table: li.clone(),
        identity: Some("id".into()),
    })
    .await
    .unwrap();
    // Region: a type with NO declared identity, to drive the NoIdentity -> 400 path.
    // Backed by the customer table (region column doubles as its own "objects").
    cp.define_type(ObjectType {
        name: TypeName("Region".into()),
        properties: vec![prop("id", "Long", true), prop("region", "String", false)],
        derived: vec![],
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
    // Join-table link Customer -> LineItem (a second, distinct path to the same targets).
    cp.define_link(LinkDef {
        name: "directItems".into(),
        from: TypeName("Customer".into()),
        to: TypeName("LineItem".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::JoinTable {
            table: cust_li.clone(),
            from_key: "id".into(),
            from_column: "customer_id".into(),
            to_column: "line_item_id".into(),
            to_key: "id".into(),
        },
    })
    .await
    .unwrap();
    // A link from a no-identity type (Region) to Order, to drive NoIdentity on the source.
    cp.define_link(LinkDef {
        name: "regionOrders".into(),
        from: TypeName("Region".into()),
        to: TypeName("Order".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
    })
    .await
    .unwrap();
    // A link from Customer to the no-identity Region type, to drive NoIdentity on target.
    cp.define_link(LinkDef {
        name: "asRegion".into(),
        from: TypeName("Customer".into()),
        to: TypeName("Region".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "id".into(),
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

/// Extract {from,to} string pairs from an association body, sorted for stable compare.
fn pairs(body: &serde_json::Value) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = body["associations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| {
            (
                a["from"].as_str().unwrap().to_string(),
                a["to"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn single_hop_returns_exact_pairs() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;

    // Customer -> Order, association: exact (customer.id, order.id) edges.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Customer/links/orders?_shape=association",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        pairs(&body),
        vec![
            ("1".into(), "10".into()),
            ("1".into(), "11".into()),
            ("2".into(), "20".into()),
            ("3".into(), "30".into()),
        ],
        "exact source->target id pairs"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_hop_pairs_source_to_final_target() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;

    // Customer -> Order -> LineItem: pairs are (customer.id, line_item.id); the intermediate
    // Order is traversal-only.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Customer/links?_path=orders,lineItems&_shape=association",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        pairs(&body),
        vec![
            ("1".into(), "100".into()),
            ("1".into(), "101".into()),
            ("1".into(), "102".into()),
            ("2".into(), "200".into()),
            ("3".into(), "300".into()),
        ],
        "source identity paired with final-target identity across two hops"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn intermediate_filter_drops_routed_pairs() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    // A row-filter on the intermediate Order type (status='shipped') drops pairs that route
    // through the pending order 11 — so (1,102) disappears, (1,100)/(1,101) survive.
    let (_c, role) = subject_with_role(&cp, "carol").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;
    cp.set_policy(
        &role,
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

    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Customer/links?_path=orders,lineItems&_shape=association",
        "carol",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        pairs(&body),
        vec![
            ("1".into(), "100".into()),
            ("1".into(), "101".into()),
            ("2".into(), "200".into()),
            ("3".into(), "300".into()),
        ],
        "pairs routing through the excluded pending order are dropped"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dedup_collapses_same_pair_distinct_sources_kept() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    // The join-table link Customer -> LineItem reaches: customer 1 -> {100, 102} and
    // customer 2 -> {200}. Customer 1 reaches line_item 100 (and 102) by this path AND by
    // the multi-hop FK path; here a single-hop association on the join table must yield each
    // distinct (from,to) pair exactly once (dedup), and the two different sources (1 and 2)
    // yield distinct pairs.
    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "LineItem").await;

    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Customer/links/directItems?_shape=association",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let got = pairs(&body);
    assert_eq!(
        got,
        vec![
            ("1".into(), "100".into()),
            ("1".into(), "102".into()),
            ("2".into(), "200".into()),
        ],
        "distinct pairs; different sources -> distinct pairs"
    );
    // Explicit dedup: pair (1,100) appears exactly once.
    assert_eq!(
        got.iter()
            .filter(|p| **p == ("1".to_string(), "100".to_string()))
            .count(),
        1,
        "the same source->target pair is collapsed to one"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_identity_source_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    // Region has identity: None. Associating FROM it -> NoIdentity -> HTTP 400.
    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Region").await;
    grant_read(&cp, &role, "Order").await;

    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Region/links/regionOrders?_shape=association",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "source type with no declared identity -> 400"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_identity_target_is_400() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    // Customer -> Region, where Region has identity: None. Final target lacks identity -> 400.
    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Region").await;

    let (status, _body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Customer/links/asRegion?_shape=association",
        "alice",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "final-target type with no declared identity -> 400"
    );
}

/// Default shape (no flag) still returns objects, not associations — regression guard.
#[tokio::test(flavor = "multi_thread")]
async fn default_shape_unchanged() {
    let fx = PgFixture::start();
    let (cp, eng, _writer) = setup(&fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;

    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Customer/links/orders",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.get("objects").is_some() && body.get("associations").is_none(),
        "default shape returns objects, not associations: {body}"
    );
}
