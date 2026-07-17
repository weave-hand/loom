//! resolve_hop is the one copy of the forward/inverse link match shared by the chain
//! and graph resolvers: forward follows `links(current)` by name; inverse follows
//! `links_to(current)` with an ambiguity check and role-swapped (reversed) backing.

use std::time::Duration;

use control_plane_core::{Cardinality, LinkBacking, LinkDef, ObjectType, Ontology, TypeName};
use control_plane_memory::MemoryControlPlane;
use query_api::governed::resolve_hop;
use query_api::handler::{Direction, Hop, QueryError};

fn simple_type(name: &str, table: &str) -> ObjectType {
    ObjectType::build(name, ("main", table))
        .prop_req("id", "Long")
        .identity("id")
        .done()
}

/// Person -employer-> Company; Team -staff-> Company and Guild -staff-> Company (an
/// ambiguous inbound name at Company).
async fn seeded() -> MemoryControlPlane {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(simple_type("Person", "person"))
        .await
        .unwrap();
    cp.define_type(simple_type("Company", "company"))
        .await
        .unwrap();
    cp.define_type(simple_type("Team", "team")).await.unwrap();
    cp.define_type(simple_type("Guild", "guild")).await.unwrap();
    cp.define_link(LinkDef::fk(
        "employer",
        "Person",
        "Company",
        Cardinality::One,
        "employer_id",
        "id",
    ))
    .await
    .unwrap();
    cp.define_link(LinkDef::fk(
        "staff",
        "Team",
        "Company",
        Cardinality::One,
        "company_id",
        "id",
    ))
    .await
    .unwrap();
    cp.define_link(LinkDef::fk(
        "staff",
        "Guild",
        "Company",
        Cardinality::One,
        "company_id",
        "id",
    ))
    .await
    .unwrap();
    cp
}

#[tokio::test]
async fn forward_hop_lands_on_the_link_target() {
    let cp = seeded().await;
    let (landed, backing) = resolve_hop(&cp, &TypeName("Person".into()), &Hop::from("employer"))
        .await
        .unwrap();
    assert_eq!(landed.0, "Company");
    assert!(matches!(
        backing,
        LinkBacking::ForeignKey { ref from_column, .. } if from_column == "employer_id"
    ));
}

#[tokio::test]
async fn forward_unknown_link_is_unknown_link() {
    let cp = seeded().await;
    let err = resolve_hop(&cp, &TypeName("Person".into()), &Hop::from("nope"))
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::UnknownLink(l) if l == "nope"));
}

#[tokio::test]
async fn inverse_hop_lands_on_the_link_origin_with_reversed_backing() {
    let cp = seeded().await;
    let hop = Hop {
        link: "employer".into(),
        direction: Direction::Inverse,
    };
    let (landed, backing) = resolve_hop(&cp, &TypeName("Company".into()), &hop)
        .await
        .unwrap();
    assert_eq!(landed.0, "Person");
    // Reversed: the join now reads Company(id) -> Person(employer_id).
    assert!(matches!(
        backing,
        LinkBacking::ForeignKey { ref from_column, ref to_column }
            if from_column == "id" && to_column == "employer_id"
    ));
}

#[tokio::test]
async fn ambiguous_inbound_link_is_ambiguous_link() {
    let cp = seeded().await;
    let hop = Hop {
        link: "staff".into(),
        direction: Direction::Inverse,
    };
    let err = resolve_hop(&cp, &TypeName("Company".into()), &hop)
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::AmbiguousLink(l) if l == "staff"));
}
