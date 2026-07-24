//! Unit tests for the shared `DatasetVisibility` readable predicate:
//! Table grant → readable; a backing-Type grant → readable via the fallback;
//! no grant → not readable. Memory control plane (ACL + ontology) + a real
//! file-backed naming bridge; no Postgres, RE-eligible.

use control_plane_core::{
    Acl, Action, ControlPlane, Effect, ObjectType, Ontology, PolicyTarget, RoleId, SubjectId,
    TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::dataset_acl::{DatasetVisibility, TableReadGrant};
use query_api::lineage_filter::local_naming;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

/// A subject in a fresh role; returns (subject, role). The role starts with no grants.
async fn subject_in_role(cp: &MemoryControlPlane, name: &str) -> (SubjectId, RoleId) {
    let subj = SubjectId(name.into());
    let role = RoleId(format!("{name}-role"));
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    (subj, role)
}

#[tokio::test(flavor = "multi_thread")]
async fn table_grant_makes_table_readable() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    let (subj, role) = subject_in_role(&cp, "reader").await;
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Table(tref("main", "events")),
        Effect::Allow,
    )
    .await
    .unwrap();

    let bridge = local_naming();
    let vis = DatasetVisibility::new(cp.acl(), cp.ontology(), bridge.as_ref());
    assert!(
        vis.is_table_readable(&subj, &tref("main", "events"))
            .await
            .unwrap()
    );
    // A different table the subject has no grant on stays unreadable.
    assert!(
        !vis.is_table_readable(&subj, &tref("main", "other"))
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn backing_type_grant_makes_table_readable_via_fallback() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    // Define a type backed by main.events; grant Read on the TYPE only. Exact
    // `ObjectType` field set (verified against src/control-plane/core/src/ontology.rs):
    // name, properties, derived, table, identity: Option<String>, version: Option<String>.
    // The fallback only reads `ty.table`/`ty.name`, so empty props / no identity suffice.
    cp.ontology()
        .define_type(ObjectType::build("Event", ("main", "events")).done())
        .await
        .unwrap();
    let (subj, role) = subject_in_role(&cp, "typed").await;
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName("Event".into())),
        Effect::Allow,
    )
    .await
    .unwrap();

    let bridge = local_naming();
    let vis = DatasetVisibility::new(cp.acl(), cp.ontology(), bridge.as_ref());
    // No Table grant, but the backing-Type grant makes the table readable.
    assert!(
        vis.is_table_readable(&subj, &tref("main", "events"))
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_grant_is_not_readable() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    let (subj, _role) = subject_in_role(&cp, "nobody").await;
    let bridge = local_naming();
    let vis = DatasetVisibility::new(cp.acl(), cp.ontology(), bridge.as_ref());
    assert!(
        !vis.is_table_readable(&subj, &tref("main", "events"))
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_grant_resolves_to_raw_mode() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    let (subj, role) = subject_in_role(&cp, "reader").await;
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Table(tref("main", "events")),
        Effect::Allow,
    )
    .await
    .unwrap();

    let bridge = local_naming();
    let vis = DatasetVisibility::new(cp.acl(), cp.ontology(), bridge.as_ref());
    assert_eq!(
        vis.table_read_grant(&subj, &tref("main", "events"))
            .await
            .unwrap(),
        Some(TableReadGrant::Table)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn type_only_grant_resolves_to_governed_mode() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    let (subj, role) = subject_in_role(&cp, "reader").await;
    // Type "Event" backed by main.events; grant Read on the TYPE only.
    cp.define_type(
        ObjectType::build("Event", ("main", "events"))
            .prop_req("id", "Long")
            .prop("note", "String")
            .done(),
    )
    .await
    .unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName("Event".into())),
        Effect::Allow,
    )
    .await
    .unwrap();

    let bridge = local_naming();
    let vis = DatasetVisibility::new(cp.acl(), cp.ontology(), bridge.as_ref());
    assert_eq!(
        vis.table_read_grant(&subj, &tref("main", "events"))
            .await
            .unwrap(),
        Some(TableReadGrant::Type(TypeName("Event".into())))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_grant_wins_over_type_grant() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    let (subj, role) = subject_in_role(&cp, "reader").await;
    cp.define_type(
        ObjectType::build("Event", ("main", "events"))
            .prop_req("id", "Long")
            .done(),
    )
    .await
    .unwrap();
    for target in [
        PolicyTarget::Table(tref("main", "events")),
        PolicyTarget::Type(TypeName("Event".into())),
    ] {
        cp.grant(&role, Action::Read, target, Effect::Allow)
            .await
            .unwrap();
    }

    let bridge = local_naming();
    let vis = DatasetVisibility::new(cp.acl(), cp.ontology(), bridge.as_ref());
    assert_eq!(
        vis.table_read_grant(&subj, &tref("main", "events"))
            .await
            .unwrap(),
        Some(TableReadGrant::Table)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_grant_resolves_to_none() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    let (subj, _role) = subject_in_role(&cp, "reader").await;

    let bridge = local_naming();
    let vis = DatasetVisibility::new(cp.acl(), cp.ontology(), bridge.as_ref());
    assert_eq!(
        vis.table_read_grant(&subj, &tref("main", "events"))
            .await
            .unwrap(),
        None
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn governed_mode_picks_first_readable_backing_type() {
    // Two types back the same table; the subject may read only the second —
    // that one governs. (When both are readable the first in list order wins,
    // mirroring resolve_governed_catalog's first-wins precedent.)
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    let (subj, role) = subject_in_role(&cp, "reader").await;
    for name in ["EventA", "EventB"] {
        cp.define_type(
            ObjectType::build(name, ("main", "events"))
                .prop_req("id", "Long")
                .done(),
        )
        .await
        .unwrap();
    }
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName("EventB".into())),
        Effect::Allow,
    )
    .await
    .unwrap();

    let bridge = local_naming();
    let vis = DatasetVisibility::new(cp.acl(), cp.ontology(), bridge.as_ref());
    assert_eq!(
        vis.table_read_grant(&subj, &tref("main", "events"))
            .await
            .unwrap(),
        Some(TableReadGrant::Type(TypeName("EventB".into())))
    );
}
