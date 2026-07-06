//! Inverse-direction traversal e2e: a forward FK chain Customer -> Order -> LineItem is
//! seeded, then traversed *backwards*. Inverse single-hop (Order ~orders-> Customer) and
//! inverse two-hop (LineItem ~lineItems,~orders-> Customer) both serve the correct origin
//! objects; governance still gates every reached type; an ambiguous inbound link is a
//! deterministic error and an unknown inbound link is UnknownLink.

use control_plane_core::{
    Acl, Action, Cardinality, ControlPlane, Effect, LinkBacking, LinkDef, Ontology, PolicyTarget,
    RoleId, SubjectId, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{ids, setup_iceberg};
use query_api::handler::{
    ChainQuery, Direction, Hop, QueryDeps, QueryError, Subject, read_linked_chain,
};

fn inv(link: &str) -> Hop {
    Hop {
        link: link.into(),
        direction: Direction::Inverse,
    }
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
async fn inverse_single_hop_reaches_origin_customer() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup_iceberg(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &*eng,
        default_limit: 1000,
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
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup_iceberg(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &*eng,
        default_limit: 1000,
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
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup_iceberg(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &*eng,
        default_limit: 1000,
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
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup_iceberg(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &*eng,
        default_limit: 1000,
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
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup_iceberg(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: cp.catalog(),
        serving: &*eng,
        default_limit: 1000,
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
