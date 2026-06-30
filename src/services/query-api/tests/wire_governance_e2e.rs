//! Each EngineControl governance RPC round-trips its core payload engine<->client,
//! identically to a direct PgControlPlane read, and a missing type surfaces NotFound
//! over the wire just as it does direct.

use control_plane_core::{
    Action, Cardinality, ControlPlane, ControlPlaneError, LinkBacking, LinkDef, ObjectType,
    PageReq, PolicyTarget, PropertyDef, SubjectId, TableRef, TypeName,
};
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{connect_gov_client, spawn_engine};

/// Boot a fresh db + warehouse and define a `customer` type, an `order` type, and an
/// `orders` link customer->order, so get_type/resolve/links all resolve.
async fn seed(
    fx: &PgFixture,
) -> (
    control_plane_postgres::PgControlPlane,
    String,
    tempfile::TempDir,
) {
    let (cp, db) = fx.fresh_db().await;
    let warehouse = tempfile::tempdir().expect("warehouse");
    let customer = TypeName("customer".into());
    let order = TypeName("order".into());
    for (name, table) in [(&customer, "customer"), (&order, "order")] {
        cp.ontology()
            .define_type(ObjectType {
                name: name.clone(),
                properties: vec![PropertyDef {
                    name: "id".into(),
                    ty: "long".into(),
                    required: true,
                }],
                derived: vec![],
                table: TableRef {
                    schema: "main".into(),
                    name: table.into(),
                },
                identity: Some("id".into()),
            })
            .await
            .expect("define_type");
    }
    cp.ontology()
        .define_link(LinkDef {
            name: "orders".into(),
            from: customer.clone(),
            to: order.clone(),
            cardinality: Cardinality::Many,
            backing: LinkBacking::ForeignKey {
                from_column: "id".into(),
                to_column: "customer_id".into(),
            },
        })
        .await
        .expect("define_link");
    (cp, db, warehouse)
}

#[tokio::test]
async fn rpc_roundtrips_match_direct_reads() {
    let fx = PgFixture::start();
    let (cp, db, warehouse) = seed(&fx).await;
    let (sock, _guard) = spawn_engine(&fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let client = connect_gov_client(&sock).await;

    // get_type parity
    let name = TypeName("customer".into());
    let direct = cp
        .ontology()
        .get_type(&name)
        .await
        .expect("direct get_type");
    let wire = client.gov_get_type(&name).await.expect("wire get_type");
    assert_eq!(direct, wire);

    // resolve parity
    assert_eq!(
        cp.ontology().resolve(&name).await.expect("direct resolve"),
        client.gov_resolve(&name).await.expect("wire resolve"),
    );

    // links parity
    let dl = cp
        .ontology()
        .links(&name, PageReq::unbounded())
        .await
        .expect("direct links");
    let wl = client
        .gov_links(&name, &PageReq::unbounded())
        .await
        .expect("wire links");
    assert_eq!(dl.items, wl.items);

    // check parity (deny-by-default: unknown subject -> Deny on both)
    let subject = SubjectId("nobody".into());
    let target = PolicyTarget::Type(name.clone());
    assert_eq!(
        cp.acl()
            .check(&subject, Action::Read, &target)
            .await
            .expect("direct check"),
        client
            .gov_check(&subject, Action::Read, &target)
            .await
            .expect("wire check"),
    );

    // NotFound parity
    let missing = TypeName("does_not_exist".into());
    let direct_err = cp
        .ontology()
        .get_type(&missing)
        .await
        .expect_err("direct missing");
    let wire_err = client
        .gov_get_type(&missing)
        .await
        .expect_err("wire missing");
    assert!(matches!(direct_err, ControlPlaneError::NotFound(_)));
    assert!(matches!(wire_err, ControlPlaneError::NotFound(_)));
}
