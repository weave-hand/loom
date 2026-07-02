//! Object-set input (`?_ids=`) e2e over the real HTTP router backed by an Iceberg/DataFusion
//! serving engine. Proves `?_ids=1,2` scopes a plain read to those source objects by declared
//! identity; a traversal `…/links/orders?_ids=1` scopes the source before the hop; `?_ids=` +
//! `?_shape=association` scopes the association's source; `?_ids=` on a no-identity type ->
//! HTTP 400; and a present-but-empty `?_ids=` -> HTTP 400.

use std::sync::Arc;

use axum::http::StatusCode;
use control_plane_core::{Cardinality, LinkBacking, LinkDef, ObjectType, Ontology, TypeName};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture, SeedCol};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{InProcessServingEngine, get, grant_read, prop, subject_with_role, tref};

/// Seed customer(id, region) ids {1,2,3} and orders(order_id, customer_id, status) FK-linked.
/// Define Customer (identity `id`), Order (identity `order_id`), the FK link `orders`, and a
/// `Plain` type backed by the same table but with NO declared identity (to drive the
/// no-identity -> 400 path). Caller MUST keep the `IcebergWriter` alive.
async fn setup(fx: &PgFixture) -> (PgControlPlane, InProcessServingEngine, IcebergWriter) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);

    // customer(id, region): (1,'CA'), (2,'CA'), (3,'NY')
    let cust = tref("main", "customer");
    let cust_cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("region".to_string(), "string".to_string(), true),
    ];
    writer
        .seed_arrays(
            "main",
            "customer",
            &cust_cols,
            &[
                SeedCol::Long(vec![1, 2, 3]),
                SeedCol::Str(vec!["CA", "CA", "NY"]),
            ],
        )
        .await;

    // orders(order_id, customer_id, status):
    //   (10,1,'shipped'),(11,1,'pending'),(20,2,'shipped'),(30,3,'shipped')
    let ord = tref("main", "orders");
    let ord_cols = vec![
        ("order_id".to_string(), "long".to_string(), false),
        ("customer_id".to_string(), "long".to_string(), false),
        ("status".to_string(), "string".to_string(), true),
    ];
    writer
        .seed_arrays(
            "main",
            "orders",
            &ord_cols,
            &[
                SeedCol::Long(vec![10, 11, 20, 30]),
                SeedCol::Long(vec![1, 1, 2, 3]),
                SeedCol::Str(vec!["shipped", "pending", "shipped", "shipped"]),
            ],
        )
        .await;

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
    // Order declares identity `order_id`.
    cp.define_type(ObjectType {
        name: TypeName("Order".into()),
        properties: vec![
            prop("order_id", "Long", true),
            prop("customer_id", "Long", true),
            prop("status", "String", false),
        ],
        derived: vec![],
        table: ord.clone(),
        identity: Some("order_id".into()),
    })
    .await
    .unwrap();
    // Plain: a type with NO declared identity, backed by the customer table.
    cp.define_type(ObjectType {
        name: TypeName("Plain".into()),
        properties: vec![prop("id", "Long", true), prop("region", "String", false)],
        derived: vec![],
        table: cust.clone(),
        identity: None,
    })
    .await
    .unwrap();

    // Customer -> Order over the FK (customer.id = orders.customer_id).
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

    let catalog = IcebergCatalog::new(pool.clone());
    let eng = InProcessServingEngine::new(catalog);
    (cp, eng, writer)
}

/// Sorted values of the given identity key from an objects body. `Long` identities render
/// as JSON strings (NumericString repr), so read each as a string and parse to i64.
fn id_values(body: &serde_json::Value, key: &str) -> Vec<i64> {
    let mut out: Vec<i64> = body["objects"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o[key].as_str().unwrap().parse::<i64>().unwrap())
        .collect();
    out.sort();
    out
}

/// Sorted {from,to} string pairs from an association body.
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
async fn ids_scopes_a_plain_read() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;

    // ?_ids=1,2 returns exactly customers 1 and 2 (id 3 excluded).
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Customer?_ids=1,2",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        id_values(&body, "id"),
        vec![1, 2],
        "scoped to the given identities"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ids_scopes_a_traversal_source() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;

    // ?_ids=1 scopes the SOURCE customer to id 1 before the hop -> only customer 1's orders.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Customer/links/orders?_ids=1",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        id_values(&body, "order_id"),
        vec![10, 11],
        "only orders of the scoped source customer 1"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ids_scopes_an_association_source() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;

    // ?_ids=1 + association: pairs only from source customer 1.
    let (status, body) = get(
        cp.clone(),
        eng.clone(),
        "/objects/Customer/links/orders?_ids=1&_shape=association",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        pairs(&body),
        vec![("1".into(), "10".into()), ("1".into(), "11".into())],
        "association pairs scoped to source customer 1"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ids_on_a_no_identity_type_is_400() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Plain").await;

    // Plain has identity: None -> ?_ids= -> NoIdentity -> HTTP 400.
    let (status, _body) = get(cp.clone(), eng.clone(), "/objects/Plain?_ids=1", "alice").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "?_ids= on a type with no declared identity -> 400"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn present_but_empty_ids_is_400() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup(fx).await;
    let cp = Arc::new(cp);
    let eng = Arc::new(eng);

    let (_a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;

    // ?_ids= present but empty (zero non-empty values) -> HTTP 400.
    let (status, _body) = get(cp.clone(), eng.clone(), "/objects/Customer?_ids=", "alice").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "present-but-empty ?_ids= -> 400"
    );
}
