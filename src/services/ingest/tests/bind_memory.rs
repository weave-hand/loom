//! Define-time validation against the in-memory fakes: derived-property and
//! bind_link validation collect all violations and persist nothing on rejection.
//! `MemoryControlPlane` implements both `Catalog` and `Ontology`, so the whole seam
//! is testable in-memory. Column `ty` strings here are loom LOGICAL type names
//! (what the catalog returns), not physical DuckLake types.

use std::time::Duration;

use control_plane_core::{
    Aggregation, Cardinality, DerivedPropertyDef, LinkBacking, LinkDef, ObjectType, Ontology,
    PageReq, PropertyDef, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use ingest::{BindError, BindViolationReason, bind, bind_link};

fn cp() -> MemoryControlPlane {
    MemoryControlPlane::new(Duration::from_millis(300))
}
fn tref(name: &str) -> TableRef {
    TableRef {
        schema: "main".into(),
        name: name.into(),
    }
}
fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
    }
}
fn derived(name: &str, ty: &str, link: &str, agg: Aggregation) -> DerivedPropertyDef {
    DerivedPropertyDef {
        name: name.into(),
        ty: ty.into(),
        link: link.into(),
        agg,
    }
}

// ---------------------------------------------------------------------------
// Derived-property validation (extends `bind`)
// ---------------------------------------------------------------------------

/// Seed an Order (table `order`, FK `customer_id`) source type and a Customer
/// (table `customer`, cols id/amount/name/flag) target type, linked Order->Customer
/// via FK `customer_id = id`. Returns the cp ready to `bind` an Order WITH derived.
async fn seed_order_customer() -> MemoryControlPlane {
    let cp = cp();
    cp.seed_catalog(
        &tref("order"),
        &[
            ("id".into(), "long".into(), false),
            ("customer_id".into(), "long".into(), true),
        ],
        &[1],
    );
    cp.seed_catalog(
        &tref("customer"),
        &[
            ("id".into(), "long".into(), false),
            ("amount".into(), "double".into(), true),
            ("name".into(), "string".into(), true),
            ("flag".into(), "boolean".into(), true),
        ],
        &[1],
    );
    cp.define_type(ObjectType {
        name: TypeName("Order".into()),
        properties: vec![prop("id", "Long", true), prop("customer_id", "Long", false)],
        derived: vec![],
        table: tref("order"),
        identity: None,
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![],
        table: tref("customer"),
        identity: None,
    })
    .await
    .unwrap();
    cp.define_link(LinkDef {
        name: "customer".into(),
        from: TypeName("Order".into()),
        to: TypeName("Customer".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: "customer_id".into(),
            to_column: "id".into(),
        },
    })
    .await
    .unwrap();
    cp
}

/// Re-bind Order with one derived property; return the rejection error.
async fn bind_order_derived(cp: &MemoryControlPlane, d: DerivedPropertyDef) -> BindError {
    let ty = ObjectType {
        name: TypeName("Order".into()),
        properties: vec![prop("id", "Long", true), prop("customer_id", "Long", false)],
        derived: vec![d],
        table: tref("order"),
        identity: None,
    };
    bind(cp, cp, ty).await.unwrap_err()
}

#[tokio::test]
async fn valid_derived_properties_round_trip() {
    let cp = seed_order_customer().await;
    let ty = ObjectType {
        name: TypeName("Order".into()),
        properties: vec![prop("id", "Long", true), prop("customer_id", "Long", false)],
        derived: vec![
            derived("ct", "Long", "customer", Aggregation::Count),
            derived(
                "total",
                "Double",
                "customer",
                Aggregation::Sum("amount".into()),
            ),
            derived("hi", "String", "customer", Aggregation::Max("name".into())),
        ],
        table: tref("order"),
        identity: None,
    };
    bind(&cp, &cp, ty.clone()).await.unwrap();
    assert_eq!(cp.get_type(&TypeName("Order".into())).await.unwrap(), ty);
}

#[tokio::test]
async fn unknown_derived_link_is_a_violation() {
    let cp = seed_order_customer().await;
    let err = bind_order_derived(&cp, derived("x", "Long", "nope", Aggregation::Count)).await;
    let BindError::DoesNotConform(v) = err else {
        panic!("{err:?}")
    };
    assert!(v.iter().any(|x| x.property == "x"
        && matches!(&x.reason, BindViolationReason::UnknownDerivedLink(l) if l == "nope")));
}

#[tokio::test]
async fn missing_agg_column_is_a_violation() {
    let cp = seed_order_customer().await;
    let d = derived("s", "Double", "customer", Aggregation::Sum("ghost".into()));
    let err = bind_order_derived(&cp, d).await;
    let BindError::DoesNotConform(v) = err else {
        panic!("{err:?}")
    };
    assert!(v.iter().any(
        |x| x.property == "s" && matches!(x.reason, BindViolationReason::MissingAggColumn)
    ));
}

#[tokio::test]
async fn bad_agg_type_is_a_violation() {
    let cp = seed_order_customer().await;
    // Sum over a string column is not applicable.
    let d = derived("s", "Double", "customer", Aggregation::Sum("name".into()));
    let err = bind_order_derived(&cp, d).await;
    let BindError::DoesNotConform(v) = err else {
        panic!("{err:?}")
    };
    assert!(
        v.iter().any(
            |x| x.property == "s" && matches!(x.reason, BindViolationReason::BadAggType { .. })
        )
    );
}

#[tokio::test]
async fn bad_derived_result_type_is_a_violation() {
    let cp = seed_order_customer().await;
    // Count must be an integer-category result; declaring String is inconsistent.
    let err = bind_order_derived(&cp, derived("c", "String", "customer", Aggregation::Count)).await;
    let BindError::DoesNotConform(v) = err else {
        panic!("{err:?}")
    };
    assert!(v.iter().any(|x| x.property == "c"
        && matches!(x.reason, BindViolationReason::BadDerivedResultType { .. })));
}

#[tokio::test]
async fn min_over_unordered_boolean_is_bad_agg_type() {
    let cp = seed_order_customer().await;
    let d = derived("m", "Boolean", "customer", Aggregation::Min("flag".into()));
    let err = bind_order_derived(&cp, d).await;
    let BindError::DoesNotConform(v) = err else {
        panic!("{err:?}")
    };
    assert!(
        v.iter().any(
            |x| x.property == "m" && matches!(x.reason, BindViolationReason::BadAggType { .. })
        )
    );
}

#[tokio::test]
async fn collects_all_derived_violations_and_persists_nothing() {
    let cp = seed_order_customer().await;
    let ty = ObjectType {
        name: TypeName("Order".into()),
        properties: vec![prop("id", "Long", true), prop("customer_id", "Long", false)],
        derived: vec![
            derived("a", "Long", "nope", Aggregation::Count),
            derived("b", "Double", "customer", Aggregation::Sum("ghost".into())),
            derived("c", "String", "customer", Aggregation::Count),
        ],
        table: tref("order"),
        identity: None,
    };
    let err = bind(&cp, &cp, ty).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("{err:?}")
    };
    assert!(v.iter().any(
        |x| x.property == "a" && matches!(x.reason, BindViolationReason::UnknownDerivedLink(_))
    ));
    assert!(v.iter().any(
        |x| x.property == "b" && matches!(x.reason, BindViolationReason::MissingAggColumn)
    ));
    assert!(v.iter().any(|x| x.property == "c"
        && matches!(x.reason, BindViolationReason::BadDerivedResultType { .. })));
    // Re-binding Order failed, so the original (derived-free) Order is unchanged.
    let order = cp.get_type(&TypeName("Order".into())).await.unwrap();
    assert!(order.derived.is_empty());
}

#[tokio::test]
async fn derived_link_on_undefined_type_is_unknown_link() {
    // The type being bound does not exist yet: ontology.links(NotFound) -> treated as
    // empty -> every derived link is unknown (enforces authoring order).
    let cp = cp();
    cp.seed_catalog(&tref("ghost"), &[("id".into(), "long".into(), false)], &[1]);
    let ty = ObjectType {
        name: TypeName("Ghost".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![derived("d", "Long", "any", Aggregation::Count)],
        table: tref("ghost"),
        identity: None,
    };
    let err = bind(&cp, &cp, ty).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("{err:?}")
    };
    assert!(v.iter().any(
        |x| x.property == "d" && matches!(x.reason, BindViolationReason::UnknownDerivedLink(_))
    ));
}

// ---------------------------------------------------------------------------
// Link validation (`bind_link`)
// ---------------------------------------------------------------------------

/// From-type Person(table person, cols id/employer_id/team_id) and to-type
/// Company(table company, col id). No link yet.
async fn two_types() -> MemoryControlPlane {
    let cp = cp();
    cp.seed_catalog(
        &tref("person"),
        &[
            ("id".into(), "long".into(), false),
            ("employer_id".into(), "long".into(), true),
            ("team_id".into(), "long".into(), true),
        ],
        &[1],
    );
    cp.seed_catalog(
        &tref("company"),
        &[("id".into(), "long".into(), false)],
        &[1],
    );
    cp.define_type(ObjectType {
        name: TypeName("Person".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![],
        table: tref("person"),
        identity: None,
    })
    .await
    .unwrap();
    cp.define_type(ObjectType {
        name: TypeName("Company".into()),
        properties: vec![prop("id", "Long", true)],
        derived: vec![],
        table: tref("company"),
        identity: None,
    })
    .await
    .unwrap();
    cp
}

fn fk_link(from_column: &str, to_column: &str) -> LinkDef {
    LinkDef {
        name: "employer".into(),
        from: TypeName("Person".into()),
        to: TypeName("Company".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: from_column.into(),
            to_column: to_column.into(),
        },
    }
}

#[tokio::test]
async fn bind_link_fk_good_round_trips() {
    let cp = two_types().await;
    bind_link(&cp, &cp, fk_link("employer_id", "id"))
        .await
        .unwrap();
    let links = cp
        .links(&TypeName("Person".into()), PageReq::unbounded())
        .await
        .unwrap();
    assert!(links.items.iter().any(|l| l.name == "employer"));
}

#[tokio::test]
async fn bind_link_fk_bad_from_column_is_missing_column() {
    let cp = two_types().await;
    let err = bind_link(&cp, &cp, fk_link("nope", "id"))
        .await
        .unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("{err:?}")
    };
    assert!(v.iter().any(
        |x| x.property == "nope" && matches!(x.reason, BindViolationReason::MissingColumn)
    ));
    // Nothing persisted.
    let links = cp
        .links(&TypeName("Person".into()), PageReq::unbounded())
        .await
        .unwrap();
    assert!(links.items.is_empty());
}

#[tokio::test]
async fn bind_link_fk_bad_to_column_is_missing_column() {
    let cp = two_types().await;
    let err = bind_link(&cp, &cp, fk_link("employer_id", "nope"))
        .await
        .unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("{err:?}")
    };
    assert!(v.iter().any(
        |x| x.property == "nope" && matches!(x.reason, BindViolationReason::MissingColumn)
    ));
}

/// JoinTable: membership(from_col person_id, to_col company_id); person.id, company.id.
fn jt_link(
    table: &str,
    from_key: &str,
    from_column: &str,
    to_column: &str,
    to_key: &str,
) -> LinkDef {
    LinkDef {
        name: "member".into(),
        from: TypeName("Person".into()),
        to: TypeName("Company".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::JoinTable {
            table: tref(table),
            from_key: from_key.into(),
            from_column: from_column.into(),
            to_column: to_column.into(),
            to_key: to_key.into(),
        },
    }
}

async fn two_types_with_join() -> MemoryControlPlane {
    let cp = two_types().await;
    cp.seed_catalog(
        &tref("membership"),
        &[
            ("person_id".into(), "long".into(), false),
            ("company_id".into(), "long".into(), false),
        ],
        &[1],
    );
    cp
}

#[tokio::test]
async fn bind_link_join_table_good_round_trips() {
    let cp = two_types_with_join().await;
    // from_key=person.id, from_column=membership.person_id,
    // to_column=membership.company_id, to_key=company.id  (codebase semantics)
    let link = jt_link("membership", "id", "person_id", "company_id", "id");
    bind_link(&cp, &cp, link).await.unwrap();
    let links = cp
        .links(&TypeName("Person".into()), PageReq::unbounded())
        .await
        .unwrap();
    assert!(links.items.iter().any(|l| l.name == "member"));
}

#[tokio::test]
async fn bind_link_join_table_bad_from_key_on_from_table() {
    let cp = two_types_with_join().await;
    let link = jt_link("membership", "nope", "person_id", "company_id", "id");
    let err = bind_link(&cp, &cp, link).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("{err:?}")
    };
    assert!(v.iter().any(
        |x| x.property == "nope" && matches!(x.reason, BindViolationReason::MissingColumn)
    ));
}

#[tokio::test]
async fn bind_link_join_table_bad_join_column() {
    let cp = two_types_with_join().await;
    // from_column must exist on the JOIN table, not the from-type's table.
    let link = jt_link("membership", "id", "nope", "company_id", "id");
    let err = bind_link(&cp, &cp, link).await.unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("{err:?}")
    };
    assert!(v.iter().any(
        |x| x.property == "nope" && matches!(x.reason, BindViolationReason::MissingColumn)
    ));
}

#[tokio::test]
async fn bind_link_join_table_absent_is_table_not_found() {
    let cp = two_types().await; // no membership table seeded
    let link = jt_link("membership", "id", "person_id", "company_id", "id");
    let err = bind_link(&cp, &cp, link).await.unwrap_err();
    assert!(
        matches!(&err, BindError::TableNotFound(t) if t.name == "membership"),
        "{err:?}"
    );
}
