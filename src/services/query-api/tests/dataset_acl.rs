//! Unit tests for the shared `DatasetVisibility` readable predicate:
//! Table grant → readable; a backing-Type grant → readable via the fallback;
//! no grant → not readable. Memory control plane (ACL + ontology) + a real
//! file-backed naming bridge; no Postgres, RE-eligible.

use control_plane_core::{
    Acl, Action, ControlPlane, Effect, ObjectType, PolicyTarget, RoleId, SubjectId, TableRef,
    TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::dataset_acl::DatasetVisibility;
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
        .define_type(ObjectType {
            name: TypeName("Event".into()),
            properties: vec![],
            derived: vec![],
            table: tref("main", "events"),
            identity: None,
            version: None,
        })
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
