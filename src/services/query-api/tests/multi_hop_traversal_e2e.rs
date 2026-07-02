//! Multi-hop traversal e2e: Customer -> Order -> LineItem, governed at every hop.
//! Served + correct; an intermediate Order row-filter narrows the reachable LineItems;
//! denying Read on the intermediate Order type forbids the whole traversal.

use control_plane_core::{
    Acl, Action, CompareOp, Effect, Policy, PolicyTarget, RoleId, RowFilter, ScalarValue,
    SubjectId, TypeName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{ids, setup_iceberg};
use query_api::handler::{
    ChainFilter, ChainQuery, QueryDeps, QueryError, Subject, read_linked_chain,
};

fn srcf(col: &str, val: &str) -> ChainFilter {
    ChainFilter {
        position: 0,
        column: col.into(),
        raw: val.into(),
    }
}

fn hopf(position: usize, col: &str, val: &str) -> ChainFilter {
    ChainFilter {
        position,
        column: col.into(),
        raw: val.into(),
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
async fn multi_hop_served_and_governed() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup_iceberg(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &*eng,
        default_limit: 1000,
    };

    // ---- subject A: Read on all three -> sees the reachable LineItems ----
    let (a, a_role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &a_role, "Customer").await;
    grant_read(&cp, &a_role, "Order").await;
    grant_read(&cp, &a_role, "LineItem").await;
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![srcf("region", "CA")],
            ids: vec![],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap();
    // Customer 1 (CA) -> orders 10,11 -> line_items 100,101 (order 10) + 102 (order 11).
    assert_eq!(
        ids(&rows),
        vec!["100".to_string(), "101".to_string(), "102".to_string()]
    );

    // ---- subject C: Read on all three + Order row-filter status='shipped' ----
    let (c, c_role) = subject_with_role(&cp, "carol").await;
    grant_read(&cp, &c_role, "Customer").await;
    grant_read(&cp, &c_role, "Order").await;
    grant_read(&cp, &c_role, "LineItem").await;
    cp.set_policy(
        &c_role,
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
    let rows_c = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![srcf("region", "CA")],
            ids: vec![],
        },
        &Subject(c.clone()),
        &deps,
    )
    .await
    .unwrap();
    // Only shipped order 10 is traversable -> line_items 100,101 (102 via pending order 11 dropped).
    assert_eq!(ids(&rows_c), vec!["100".to_string(), "101".to_string()]);

    // ---- subject B: Read on Customer + LineItem but NOT Order -> 403 ----
    let (b, b_role) = subject_with_role(&cp, "bob").await;
    grant_read(&cp, &b_role, "Customer").await;
    grant_read(&cp, &b_role, "LineItem").await;
    let err = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![],
            ids: vec![],
        },
        &Subject(b.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, QueryError::Forbidden),
        "no Read on intermediate Order -> Forbidden"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn target_filter_narrows_final_set() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup_iceberg(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &*eng,
        default_limit: 1000,
    };
    let (a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;

    // Customer 1 (CA) reaches line_items 100,101,102; a final-target sku filter narrows.
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![srcf("region", "CA"), hopf(2, "sku", "A")],
            ids: vec![],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(
        ids(&rows),
        vec!["100".to_string()],
        "only line_item 100 has sku=A"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn intermediate_typed_filter_coerces_and_narrows() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup_iceberg(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &*eng,
        default_limit: 1000,
    };
    let (a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;

    // Intermediate Order.id is Long: filtering id=10 coerces to Int(10) and binds at the
    // intermediate position t_1 (proving typed coercion flows through a non-source hop, not
    // just the source). Order 10 matches -> line_items 100,101 (102 hangs off order 11,
    // excluded). (The text-vs-typed counterfactual for non-castable types is covered in
    // typed_filter_e2e.rs with Double/Boolean columns.)
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![srcf("region", "CA"), hopf(1, "id", "10")],
            ids: vec![],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&rows), vec!["100".to_string(), "101".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn source_and_intermediate_filters_combine() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup_iceberg(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &*eng,
        default_limit: 1000,
    };
    let (a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;

    // Source region=CA AND intermediate Order.status=pending -> only order 11 -> line_item 102.
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![srcf("region", "CA"), hopf(1, "status", "pending")],
            ids: vec![],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&rows), vec!["102".to_string()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_positioned_filters_are_rejected() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup_iceberg(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &*eng,
        default_limit: 1000,
    };
    let (a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;
    // Deny the final-target sku column for this subject.
    cp.set_policy(
        &role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("LineItem".into())),
            row_filter: None,
            deny_columns: vec!["sku".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // A filter on a denied target column -> BadFilter (visibility before coercion).
    let denied = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![hopf(2, "sku", "A")],
            ids: vec![],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(denied, QueryError::BadFilter(ref c) if c == "sku"),
        "denied target column filter -> BadFilter; got {denied:?}"
    );

    // An operator filter on the same denied column is rejected too (visibility precedes parse).
    let denied_op = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![hopf(2, "sku", "ne:A")],
            ids: vec![],
        },
        &Subject(a.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(denied_op, QueryError::BadFilter(ref c) if c == "sku"),
        "operator filter on denied column -> BadFilter; got {denied_op:?}"
    );

    // A position past the end of the chain -> BadFilter (guarded, never panics).
    let oob = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![hopf(5, "id", "1")],
            ids: vec![],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(oob, QueryError::BadFilter(ref c) if c == "id"),
        "out-of-range position -> BadFilter; got {oob:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn intermediate_comparison_operator_narrows() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup_iceberg(fx).await;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &cp,
        serving: &*eng,
        default_limit: 1000,
    };
    let (a, role) = subject_with_role(&cp, "alice").await;
    grant_read(&cp, &role, "Customer").await;
    grant_read(&cp, &role, "Order").await;
    grant_read(&cp, &role, "LineItem").await;

    // Intermediate Order.id is Long: id > 10 keeps order 11 (drops order 10) for Customer 1,
    // so only line_item 102 (which hangs off order 11) is reachable.
    let rows = read_linked_chain(
        &ChainQuery {
            from_type: "Customer".into(),
            path: vec!["orders".into(), "lineItems".into()],
            filters: vec![srcf("region", "CA"), hopf(1, "id", "gt:10")],
            ids: vec![],
        },
        &Subject(a),
        &deps,
    )
    .await
    .unwrap();
    assert_eq!(ids(&rows), vec!["102".to_string()]);
}
