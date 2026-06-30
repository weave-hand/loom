//! Each EngineControl governance RPC round-trips its core payload engine<->client,
//! identically to a direct PgControlPlane read, and a missing type surfaces NotFound
//! over the wire just as it does direct.

use std::sync::Arc;

use control_plane_core::{
    Action, ActionDef, ActionKind, ActionName, Cardinality, ControlPlane, ControlPlaneError,
    LinkBacking, LinkDef, ObjectType, PageReq, ParamDef, PolicyTarget, PropertyDef, RoleId,
    SubjectId, TableRef, TypeName,
};
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{connect_gov_client, spawn_engine};
use query_api::wire_control_plane::WireControlPlane;

/// Boot a fresh db + warehouse and define:
/// - `customer`, `order`, `group` types
/// - `orders` FK link customer->order
/// - `memberships` JoinTable link customer<->group (many-to-many)
/// - `createCustomer` action with a non-empty `parameters` list
///
/// This exercises `get_type`/`resolve`/`links`/`links_to`/`get_action` and covers the
/// `LinkBacking::JoinTable` + `ParamDef` wire payloads.
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
    let group = TypeName("group".into());
    for (name, table) in [
        (&customer, "customer"),
        (&order, "order"),
        (&group, "group"),
    ] {
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
    // FK link: customer -> order (one-to-many).
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
        .expect("define_link fk");
    // JoinTable link: customer <-> group (many-to-many via membership table).
    cp.ontology()
        .define_link(LinkDef {
            name: "memberships".into(),
            from: customer.clone(),
            to: group.clone(),
            cardinality: Cardinality::Many,
            backing: LinkBacking::JoinTable {
                table: TableRef {
                    schema: "main".into(),
                    name: "customer_group".into(),
                },
                from_key: "id".into(),
                from_column: "customer_id".into(),
                to_column: "group_id".into(),
                to_key: "id".into(),
            },
        })
        .await
        .expect("define_link join_table");
    // Action with parameters (covers ParamDef wire payload + get_action RPC).
    cp.ontology()
        .define_action(ActionDef {
            name: ActionName("createCustomer".into()),
            target: customer.clone(),
            parameters: vec![
                ParamDef {
                    name: "name".into(),
                    ty: "String".into(),
                    required: true,
                },
                ParamDef {
                    name: "email".into(),
                    ty: "String".into(),
                    required: false,
                },
            ],
            kind: ActionKind::Insert,
        })
        .await
        .expect("define_action");
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

    // get_action parity — exercises ActionDef + ParamDef over the wire.
    let action_name = ActionName("createCustomer".into());
    let direct_action = cp
        .ontology()
        .get_action(&action_name)
        .await
        .expect("direct get_action");
    let wire_action = client
        .gov_get_action(&action_name)
        .await
        .expect("wire get_action");
    assert_eq!(direct_action, wire_action, "get_action parity");
    // The parameters vec must be non-empty so ParamDef serde is actually exercised.
    assert!(
        !wire_action.parameters.is_empty(),
        "action must have parameters to exercise ParamDef serde"
    );

    // links_to parity — exercises the inbound-link RPC (inbound to `order` via orders FK).
    let order = TypeName("order".into());
    let direct_links_to = cp
        .ontology()
        .links_to(&order, PageReq::unbounded())
        .await
        .expect("direct links_to");
    let wire_links_to = client
        .gov_links_to(&order, &PageReq::unbounded())
        .await
        .expect("wire links_to");
    assert_eq!(
        direct_links_to.items, wire_links_to.items,
        "links_to parity for order"
    );

    // links parity for the JoinTable link — exercises LinkBacking::JoinTable serde.
    let group = TypeName("group".into());
    let direct_group_links = cp
        .ontology()
        .links_to(&group, PageReq::unbounded())
        .await
        .expect("direct links_to group");
    let wire_group_links = client
        .gov_links_to(&group, &PageReq::unbounded())
        .await
        .expect("wire links_to group");
    assert_eq!(
        direct_group_links.items, wire_group_links.items,
        "links_to parity for group (JoinTable backing)"
    );
    // Verify the JoinTable backing actually made it through the wire.
    let membership = wire_group_links
        .items
        .iter()
        .find(|l| l.name == "memberships")
        .expect("memberships link present in wire response");
    assert!(
        matches!(membership.backing, LinkBacking::JoinTable { .. }),
        "JoinTable backing preserved through wire serde"
    );
}

#[tokio::test]
async fn wire_acl_is_read_only() {
    let fx = PgFixture::start();
    let (cp, db, warehouse) = seed(&fx).await;
    let (sock, _guard) = spawn_engine(&fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let client = connect_gov_client(&sock).await;
    let cp = Arc::new(cp);
    let wire = WireControlPlane::new(client, cp.clone() as Arc<dyn ControlPlane>);

    // Read methods work over the wire (acl()/ontology() resolve).
    assert!(
        wire.ontology()
            .get_type(&TypeName("customer".into()))
            .await
            .is_ok()
    );

    // Write/define methods fail loudly rather than silently no-op.
    let err = wire
        .acl()
        .define_role(&RoleId("x".into()))
        .await
        .expect_err("define_role");
    assert!(matches!(err, ControlPlaneError::Backend(_)));
    let customer = cp
        .ontology()
        .get_type(&TypeName("customer".into()))
        .await
        .unwrap();
    let err = wire
        .ontology()
        .define_type(customer)
        .await
        .expect_err("define_type");
    assert!(matches!(err, ControlPlaneError::Backend(_)));

    // queue() delegates to the direct plane (still usable for GC enqueue).
    let _ = wire.queue(); // does not panic
}

#[tokio::test]
#[should_panic(expected = "read-only")]
async fn wire_catalog_is_guarded() {
    let fx = PgFixture::start();
    let (cp, db, warehouse) = seed(&fx).await;
    let (sock, _guard) = spawn_engine(&fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let client = connect_gov_client(&sock).await;
    let wire = WireControlPlane::new(client, Arc::new(cp) as Arc<dyn ControlPlane>);
    let _ = wire.catalog(); // must panic: query-api never reads catalog over this plane
}
