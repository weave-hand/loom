//! read_associations on an in-memory control plane + a stub serving engine. The stub
//! returns canned id pairs; the test asserts the returned `pairs` and
//! the per-end identity logical types, plus the `NoIdentity` governance path. Real-data
//! pairs are covered by the association e2e (Task 4).

use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, Cardinality, Effect, LinkDef, ObjectType, Ontology, Policy, PolicyTarget, RoleId,
    SubjectId, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::handler::{
    Associations, ChainQuery, Hop, QueryDeps, QueryError, Subject, read_associations,
};
use query_api::serving::{Rows, ServingEngine, ServingError, SqlValue};

/// A serving stub that returns canned (source_id, target_id) pairs in the order
/// compile_chain_pairs projects them.
struct PairServing {
    rows: Vec<Vec<SqlValue>>,
}

#[async_trait]
impl ServingEngine for PairServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
        _at: Option<control_plane_core::SnapshotId>,
    ) -> Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec!["id".into(), "order_id".into()],
            rows: self.rows.clone(),
        })
    }
}

fn customer_type(identity: Option<String>) -> ObjectType {
    let b = ObjectType::build("Customer", ("main", "customer"))
        .prop_req("id", "Long")
        .prop("region", "Text");
    match identity {
        Some(id) => b.identity(id).done(),
        None => b.done(),
    }
}

fn order_type(identity: Option<String>) -> ObjectType {
    let b = ObjectType::build("Order", ("main", "order"))
        .prop_req("order_id", "Long")
        .prop("customer_id", "Long");
    match identity {
        Some(id) => b.identity(id).done(),
        None => b.done(),
    }
}

/// Seed a control plane: Customer --orders--> Order (FK), an analyst granted Read on both.
async fn seeded(customer: ObjectType, order: ObjectType) -> (MemoryControlPlane, SubjectId) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(customer).await.unwrap();
    cp.define_type(order).await.unwrap();
    cp.define_link(LinkDef::fk(
        "orders",
        "Customer",
        "Order",
        Cardinality::Many,
        "id",
        "customer_id",
    ))
    .await
    .unwrap();

    let analyst = SubjectId("analyst".into());
    let reader = RoleId("reader".into());
    cp.define_subject(&analyst).await.unwrap();
    cp.define_role(&reader).await.unwrap();
    cp.assign_role(&analyst, &reader).await.unwrap();
    for t in ["Customer", "Order"] {
        cp.grant(
            &reader,
            Action::Read,
            PolicyTarget::Type(TypeName(t.into())),
            Effect::Allow,
        )
        .await
        .unwrap();
    }
    (cp, analyst)
}

fn assoc_query() -> ChainQuery {
    ChainQuery {
        from_type: "Customer".into(),
        path: vec![Hop::from("orders")],
        filters: vec![],
        ids: vec![],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn read_associations_returns_id_pairs() {
    let (cp, subj) = seeded(
        customer_type(Some("id".into())),
        order_type(Some("order_id".into())),
    )
    .await;
    let serving = PairServing {
        rows: vec![
            vec![SqlValue::Int(5), SqlValue::Int(12)],
            vec![SqlValue::Int(5), SqlValue::Int(13)],
            vec![SqlValue::Int(6), SqlValue::Int(14)],
        ],
    };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: &cp,
        serving: &serving,
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
    };
    let Associations {
        from_id_type,
        to_id_type,
        pairs,
    } = read_associations(&assoc_query(), &Subject(subj), &deps)
        .await
        .unwrap();
    assert_eq!(from_id_type, "Long");
    assert_eq!(to_id_type, "Long");
    assert_eq!(
        pairs,
        vec![
            (SqlValue::Int(5), SqlValue::Int(12)),
            (SqlValue::Int(5), SqlValue::Int(13)),
            (SqlValue::Int(6), SqlValue::Int(14)),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn read_associations_rejects_source_without_identity() {
    let (cp, subj) = seeded(customer_type(None), order_type(Some("order_id".into()))).await;
    let serving = PairServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: &cp,
        serving: &serving,
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
    };
    let err = read_associations(&assoc_query(), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(err, QueryError::NoIdentity(t) if t == "Customer"),
        "expected NoIdentity(Customer)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn read_associations_rejects_target_without_identity() {
    let (cp, subj) = seeded(customer_type(Some("id".into())), order_type(None)).await;
    let serving = PairServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: &cp,
        serving: &serving,
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
    };
    let err = read_associations(&assoc_query(), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(err, QueryError::NoIdentity(t) if t == "Order"),
        "expected NoIdentity(Order)"
    );
}

// The identity-visibility gate (leak prevention): an association projects the source and
// target identity values, so a caller who cannot READ an identity column must not obtain
// it through a pair. Denied (source) and masked (target) identity each -> Forbidden.

#[tokio::test(flavor = "multi_thread")]
async fn read_associations_forbids_a_denied_source_identity() {
    let (cp, subj) = seeded(
        customer_type(Some("id".into())),
        order_type(Some("order_id".into())),
    )
    .await;
    // Deny the source identity column for the reader role the analyst holds.
    cp.set_policy(
        &RoleId("reader".into()),
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Customer".into())),
            row_filter: None,
            deny_columns: vec!["id".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    let serving = PairServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: &cp,
        serving: &serving,
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
    };
    let err = read_associations(&assoc_query(), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(err, QueryError::Forbidden),
        "a denied source identity -> Forbidden, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn read_associations_forbids_a_masked_target_identity() {
    let (cp, subj) = seeded(
        customer_type(Some("id".into())),
        order_type(Some("order_id".into())),
    )
    .await;
    // Mask the target identity column: you cannot name a target you can only see masked.
    cp.set_policy(
        &RoleId("reader".into()),
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: None,
            deny_columns: vec![],
            mask_columns: vec!["order_id".into()],
        },
    )
    .await
    .unwrap();
    let serving = PairServing { rows: vec![] };
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        catalog: &cp,
        serving: &serving,
        default_limit: 1000,
        gc_retention: std::time::Duration::from_secs(7 * 24 * 3600),
    };
    let err = read_associations(&assoc_query(), &Subject(subj), &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(err, QueryError::Forbidden),
        "a masked target identity -> Forbidden, got {err:?}"
    );
}
