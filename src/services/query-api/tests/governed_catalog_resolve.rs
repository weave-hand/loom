//! `resolve_governed_catalog` is the edge half of the external SQL wire: enumerate
//! ontology types, coarse Read-gate each (deny-by-default — an ungranted type is
//! OMITTED), and fold `load_policy` into one `GovernedTable` per visible type.

use std::time::Duration;

use control_plane_core::{
    Acl, Action, CompareOp, Effect, GovernedCatalog, ObjectType, Ontology, Policy, PolicyTarget,
    PropertyConstraints, PropertyDef, RoleId, RowFilter, ScalarValue, SubjectId, TableRef,
    TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::governed::resolve_governed_catalog;

fn prop(name: &str, ty: &str) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required: false,
        constraints: PropertyConstraints::default(),
    }
}

fn object_type(name: &str, table: TableRef) -> ObjectType {
    ObjectType {
        name: TypeName(name.into()),
        properties: vec![
            prop("id", "Long"),
            prop("status", "String"),
            prop("secret", "String"),
        ],
        derived: vec![],
        table,
        identity: Some("id".into()),
        version: None,
    }
}

fn orders_table() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "orders".into(),
    }
}

fn customers_table() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "customers".into(),
    }
}

fn secrets_table() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "secrets".into(),
    }
}

fn orders_policy() -> Policy {
    Policy {
        target: PolicyTarget::Type(TypeName("Orders".into())),
        row_filter: Some(RowFilter::Compare {
            property: "status".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Text("open".into()),
        }),
        deny_columns: vec!["secret".into()],
        mask_columns: vec!["status".into()],
    }
}

/// Seed: `Orders` (row-filter + mask policy for role R), `Customers` (plain Read
/// grant), `Secrets` (no grant at all), subject S in role R, plus `OrdersAlias` — a
/// fourth granted type bound to `Orders`' own `TableRef` (the duplicate-table case).
///
/// `MemoryControlPlane::list_types` iterates a `HashMap`, so which of `Orders` /
/// `OrdersAlias` the resolver visits first is not something a test may pin down.
/// `OrdersAlias` is therefore seeded with the IDENTICAL row-filter/deny/mask policy
/// as `Orders` (just under its own `TypeName`, since policy is keyed by target) —
/// whichever of the two "wins" the dedup, the resulting entry's policy content is
/// the same. What this test actually exercises is that exactly one entry survives
/// for the shared `TableRef` (first-wins-by-list-order, `tables.len()` for that
/// table == 1), not which of the two type names supplied it.
async fn seeded() -> (MemoryControlPlane, SubjectId) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));

    cp.define_type(object_type("Orders", orders_table()))
        .await
        .unwrap();
    cp.define_type(object_type("Customers", customers_table()))
        .await
        .unwrap();
    cp.define_type(object_type("Secrets", secrets_table()))
        .await
        .unwrap();
    // Duplicate table binding: a fourth granted type sharing Orders' TableRef.
    cp.define_type(object_type("OrdersAlias", orders_table()))
        .await
        .unwrap();

    let subject = SubjectId("s".into());
    let role_r = RoleId("r".into());
    cp.define_subject(&subject).await.unwrap();
    cp.define_role(&role_r).await.unwrap();
    cp.assign_role(&subject, &role_r).await.unwrap();

    cp.grant(
        &role_r,
        Action::Read,
        PolicyTarget::Type(TypeName("Orders".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    cp.set_policy(&role_r, Action::Read, orders_policy())
        .await
        .unwrap();

    cp.grant(
        &role_r,
        Action::Read,
        PolicyTarget::Type(TypeName("Customers".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    // Customers: plain grant, no row-filter/mask policy — empty policy vectors.

    // Secrets: deliberately never granted.

    cp.grant(
        &role_r,
        Action::Read,
        PolicyTarget::Type(TypeName("OrdersAlias".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    let mut alias_policy = orders_policy();
    alias_policy.target = PolicyTarget::Type(TypeName("OrdersAlias".into()));
    cp.set_policy(&role_r, Action::Read, alias_policy)
        .await
        .unwrap();

    (cp, subject)
}

#[tokio::test]
async fn omits_ungranted_types_deny_by_default() {
    let (cp, subject) = seeded().await;
    let catalog: GovernedCatalog = resolve_governed_catalog(&cp, &cp, &subject).await.unwrap();

    let tables: std::collections::HashSet<TableRef> =
        catalog.tables.iter().map(|gt| gt.table.clone()).collect();
    let expected: std::collections::HashSet<TableRef> =
        [orders_table(), customers_table()].into_iter().collect();
    assert_eq!(tables, expected, "Secrets must be omitted — no grant");
}

#[tokio::test]
async fn folds_row_filter_denied_and_masked_into_the_orders_entry() {
    let (cp, subject) = seeded().await;
    let catalog = resolve_governed_catalog(&cp, &cp, &subject).await.unwrap();

    let orders_entry = catalog
        .table_for(&orders_table())
        .expect("Orders' table must be present");
    assert_eq!(orders_entry.row_filters.len(), 1);
    assert_eq!(orders_entry.denied, vec!["secret".to_string()]);
    assert_eq!(orders_entry.masked, vec!["status".to_string()]);
}

#[tokio::test]
async fn customers_entry_has_empty_policy_vectors() {
    let (cp, subject) = seeded().await;
    let catalog = resolve_governed_catalog(&cp, &cp, &subject).await.unwrap();

    let customers_entry = catalog
        .table_for(&customers_table())
        .expect("Customers' table must be present");
    assert!(customers_entry.row_filters.is_empty());
    assert!(customers_entry.denied.is_empty());
    assert!(customers_entry.masked.is_empty());
}

#[tokio::test]
async fn duplicate_table_binding_yields_one_entry_first_wins() {
    let (cp, subject) = seeded().await;
    let catalog = resolve_governed_catalog(&cp, &cp, &subject).await.unwrap();

    let matches: Vec<_> = catalog
        .tables
        .iter()
        .filter(|gt| gt.table == orders_table())
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "Orders and OrdersAlias share a TableRef; only one entry may survive"
    );
}
