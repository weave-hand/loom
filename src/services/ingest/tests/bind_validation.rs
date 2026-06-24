//! Define-time ontology validation, exercised fully in-memory (memory `Catalog` +
//! memory `Ontology`). Covers `bind`'s derived-property pass and the sibling
//! `bind_link` backing-column validator. The exhaustive violation matrix lives
//! here; `bind.rs` carries the postgres-parity smoke against the real DuckLake
//! catalog.

use std::time::Duration;

use control_plane_core::{
    Aggregation, Cardinality, DerivedPropertyDef, LinkBacking, LinkDef, ObjectType, Ontology,
    PageReq, PropertyDef, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use ingest::{BindError, BindViolationReason, bind, bind_link};

fn cp() -> MemoryControlPlane {
    MemoryControlPlane::new(Duration::from_secs(5))
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

fn object_type(
    name: &str,
    table: &str,
    properties: Vec<PropertyDef>,
    derived: Vec<DerivedPropertyDef>,
) -> ObjectType {
    ObjectType {
        name: TypeName(name.into()),
        properties,
        derived,
        table: tref(table),
        identity: None,
    }
}

/// Seed the customer/order graph: `main.customer(id long, name string)`,
/// `main.order(id long, customer_id long, amount double, status string,
/// active boolean)`, the base `Customer`/`Order` types (no derived), and a
/// `Customer.orders -> Order` FK link (`customer.id = order.customer_id`).
async fn seed_graph(cp: &MemoryControlPlane) {
    cp.seed_catalog(
        &tref("customer"),
        &[
            ("id".into(), "long".into(), false),
            ("name".into(), "string".into(), true),
        ],
        &[1],
    );
    cp.seed_catalog(
        &tref("order"),
        &[
            ("id".into(), "long".into(), false),
            ("customer_id".into(), "long".into(), true),
            ("amount".into(), "double".into(), true),
            ("status".into(), "string".into(), true),
            ("active".into(), "boolean".into(), true),
        ],
        &[1],
    );
    cp.define_type(object_type(
        "Customer",
        "customer",
        vec![prop("id", "Long", true)],
        vec![],
    ))
    .await
    .unwrap();
    cp.define_type(object_type(
        "Order",
        "order",
        vec![prop("id", "Long", true)],
        vec![],
    ))
    .await
    .unwrap();
    cp.define_link(LinkDef {
        name: "orders".into(),
        from: TypeName("Customer".into()),
        to: TypeName("Order".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
    })
    .await
    .unwrap();
}

/// Re-`bind` Customer with the given derived properties (the base type already
/// exists from `seed_graph`).
fn customer_with(derived: Vec<DerivedPropertyDef>) -> ObjectType {
    object_type(
        "Customer",
        "customer",
        vec![prop("id", "Long", true)],
        derived,
    )
}

async fn customer_derived(cp: &MemoryControlPlane) -> Vec<DerivedPropertyDef> {
    cp.get_type(&TypeName("Customer".into()))
        .await
        .unwrap()
        .derived
}

// ---- derived-property validation ----

#[tokio::test]
async fn bind_accepts_valid_derived_properties_and_persists_them() {
    let cp = cp();
    seed_graph(&cp).await;

    let derived = vec![
        derived("orderCount", "Long", "orders", Aggregation::Count),
        derived(
            "totalSpend",
            "Double",
            "orders",
            Aggregation::Sum("amount".into()),
        ),
        derived(
            "biggestOrder",
            "Double",
            "orders",
            Aggregation::Max("amount".into()),
        ),
    ];
    bind(&cp, &cp, customer_with(derived.clone()))
        .await
        .unwrap();

    assert_eq!(customer_derived(&cp).await, derived);
}

#[tokio::test]
async fn bind_rejects_derived_naming_an_undefined_link() {
    let cp = cp();
    seed_graph(&cp).await;

    let err = bind(
        &cp,
        &cp,
        customer_with(vec![derived(
            "nope",
            "Long",
            "ghostLink",
            Aggregation::Count,
        )]),
    )
    .await
    .unwrap_err();

    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(v.iter().any(|x| x.property == "nope"
        && matches!(&x.reason, BindViolationReason::UnknownDerivedLink(l) if l == "ghostLink")));
    // Nothing persisted: the base Customer still has no derived property.
    assert!(customer_derived(&cp).await.is_empty());
}

#[tokio::test]
async fn bind_rejects_derived_agg_column_absent_from_target() {
    let cp = cp();
    seed_graph(&cp).await;

    let err = bind(
        &cp,
        &cp,
        customer_with(vec![derived(
            "spend",
            "Double",
            "orders",
            Aggregation::Sum("nope".into()),
        )]),
    )
    .await
    .unwrap_err();

    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(v.iter().any(
        |x| x.property == "spend" && matches!(x.reason, BindViolationReason::MissingAggColumn)
    ));
    assert!(customer_derived(&cp).await.is_empty());
}

#[tokio::test]
async fn bind_rejects_sum_over_a_non_numeric_column() {
    let cp = cp();
    seed_graph(&cp).await;

    // status is `string` -> Sum is not applicable. Declared Double is numeric, so
    // the ONLY violation is BadAggType (isolates the applicability check).
    let err = bind(
        &cp,
        &cp,
        customer_with(vec![derived(
            "spend",
            "Double",
            "orders",
            Aggregation::Sum("status".into()),
        )]),
    )
    .await
    .unwrap_err();

    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(v.iter().any(|x| x.property == "spend"
        && matches!(&x.reason, BindViolationReason::BadAggType { agg, column }
            if agg == "Sum" && column == "status")));
}

#[tokio::test]
async fn bind_rejects_max_over_a_non_ordered_column() {
    let cp = cp();
    seed_graph(&cp).await;

    // active is `boolean` -> Min/Max not applicable. Declared Boolean matches the
    // column type, so the only violation is BadAggType.
    let err = bind(
        &cp,
        &cp,
        customer_with(vec![derived(
            "latest",
            "Boolean",
            "orders",
            Aggregation::Max("active".into()),
        )]),
    )
    .await
    .unwrap_err();

    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(v.iter().any(|x| x.property == "latest"
        && matches!(&x.reason, BindViolationReason::BadAggType { agg, .. } if agg == "Max")));
}

#[tokio::test]
async fn bind_rejects_count_declared_as_a_non_integer_result() {
    let cp = cp();
    seed_graph(&cp).await;

    let err = bind(
        &cp,
        &cp,
        customer_with(vec![derived(
            "howMany",
            "String",
            "orders",
            Aggregation::Count,
        )]),
    )
    .await
    .unwrap_err();

    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(v.iter().any(|x| x.property == "howMany"
        && matches!(&x.reason, BindViolationReason::BadDerivedResultType { declared, .. }
            if declared == "String")));
}

#[tokio::test]
async fn bind_rejects_min_max_result_type_not_matching_the_column() {
    let cp = cp();
    seed_graph(&cp).await;

    // amount is `double`; Min returns the column's type (Double), so a declared
    // Long is inconsistent. amount IS numeric/ordered, so no BadAggType — the only
    // violation is the result-type mismatch.
    let err = bind(
        &cp,
        &cp,
        customer_with(vec![derived(
            "smallest",
            "Long",
            "orders",
            Aggregation::Min("amount".into()),
        )]),
    )
    .await
    .unwrap_err();

    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(v.iter().any(|x| x.property == "smallest"
        && matches!(&x.reason, BindViolationReason::BadDerivedResultType { declared, expected }
            if declared == "Long" && expected == "double")));
}

#[tokio::test]
async fn bind_collects_all_derived_violations() {
    let cp = cp();
    seed_graph(&cp).await;

    let err = bind(
        &cp,
        &cp,
        customer_with(vec![
            derived("a", "Long", "ghostLink", Aggregation::Count), // UnknownDerivedLink
            derived("b", "Double", "orders", Aggregation::Sum("nope".into())), // MissingAggColumn
            derived("c", "String", "orders", Aggregation::Count),  // BadDerivedResultType
        ]),
    )
    .await
    .unwrap_err();

    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(v.iter().any(
        |x| x.property == "a" && matches!(x.reason, BindViolationReason::UnknownDerivedLink(_))
    ));
    assert!(
        v.iter()
            .any(|x| x.property == "b"
                && matches!(x.reason, BindViolationReason::MissingAggColumn))
    );
    assert!(v.iter().any(|x| x.property == "c"
        && matches!(x.reason, BindViolationReason::BadDerivedResultType { .. })));
    assert!(customer_derived(&cp).await.is_empty());
}

#[tokio::test]
async fn bind_reports_unknown_link_when_the_type_is_brand_new() {
    // A type that fails its FIRST bind (never define_type'd) has no links, so a
    // derived link is unknown — and nothing is persisted at all.
    let cp = cp();
    cp.seed_catalog(
        &tref("customer"),
        &[("id".into(), "long".into(), false)],
        &[1],
    );

    let err = bind(
        &cp,
        &cp,
        customer_with(vec![derived("n", "Long", "orders", Aggregation::Count)]),
    )
    .await
    .unwrap_err();

    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(
        v.iter()
            .any(|x| matches!(x.reason, BindViolationReason::UnknownDerivedLink(_)))
    );
    assert!(matches!(
        cp.get_type(&TypeName("Customer".into())).await,
        Err(control_plane_core::ControlPlaneError::NotFound(_))
    ));
}

// ---- bind_link backing-column validation ----

/// Seed `customer`/`order`/`cust_ord` tables + the `Customer`/`Order` types so
/// `bind_link` can resolve endpoint tables and the join table.
async fn seed_link_tables(cp: &MemoryControlPlane) {
    cp.seed_catalog(
        &tref("customer"),
        &[
            ("id".into(), "long".into(), false),
            ("name".into(), "string".into(), true),
        ],
        &[1],
    );
    cp.seed_catalog(
        &tref("order"),
        &[
            ("id".into(), "long".into(), false),
            ("customer_id".into(), "long".into(), true),
        ],
        &[1],
    );
    cp.seed_catalog(
        &tref("cust_ord"),
        &[
            ("cust_id".into(), "long".into(), false),
            ("ord_id".into(), "long".into(), false),
        ],
        &[1],
    );
    cp.define_type(object_type(
        "Customer",
        "customer",
        vec![prop("id", "Long", true)],
        vec![],
    ))
    .await
    .unwrap();
    cp.define_type(object_type(
        "Order",
        "order",
        vec![prop("id", "Long", true)],
        vec![],
    ))
    .await
    .unwrap();
}

fn fk_link(from_column: &str, to_column: &str) -> LinkDef {
    LinkDef {
        name: "orders".into(),
        from: TypeName("Customer".into()),
        to: TypeName("Order".into()),
        cardinality: Cardinality::Many,
        backing: LinkBacking::ForeignKey {
            from_column: from_column.into(),
            to_column: to_column.into(),
        },
    }
}

fn jt_link(from_key: &str, from_column: &str, to_column: &str, to_key: &str) -> LinkDef {
    jt_link_on("cust_ord", from_key, from_column, to_column, to_key)
}

fn jt_link_on(
    table: &str,
    from_key: &str,
    from_column: &str,
    to_column: &str,
    to_key: &str,
) -> LinkDef {
    LinkDef {
        name: "viaOrders".into(),
        from: TypeName("Customer".into()),
        to: TypeName("Order".into()),
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

async fn link_names(cp: &MemoryControlPlane) -> Vec<String> {
    cp.links(&TypeName("Customer".into()), PageReq::unbounded())
        .await
        .unwrap()
        .items
        .into_iter()
        .map(|l| l.name)
        .collect()
}

#[tokio::test]
async fn bind_link_accepts_a_valid_foreign_key() {
    let cp = cp();
    seed_link_tables(&cp).await;

    bind_link(&cp, &cp, fk_link("id", "customer_id"))
        .await
        .unwrap();
    assert!(link_names(&cp).await.contains(&"orders".to_string()));
}

#[tokio::test]
async fn bind_link_rejects_a_missing_fk_from_column() {
    let cp = cp();
    seed_link_tables(&cp).await;

    let err = bind_link(&cp, &cp, fk_link("nope", "customer_id"))
        .await
        .unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(
        v.iter()
            .any(|x| x.property == "nope"
                && matches!(x.reason, BindViolationReason::MissingColumn))
    );
    assert!(link_names(&cp).await.is_empty());
}

#[tokio::test]
async fn bind_link_rejects_a_missing_fk_to_column() {
    let cp = cp();
    seed_link_tables(&cp).await;

    let err = bind_link(&cp, &cp, fk_link("id", "nope"))
        .await
        .unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(
        v.iter()
            .any(|x| x.property == "nope"
                && matches!(x.reason, BindViolationReason::MissingColumn))
    );
}

#[tokio::test]
async fn bind_link_collects_all_fk_violations() {
    let cp = cp();
    seed_link_tables(&cp).await;

    let err = bind_link(&cp, &cp, fk_link("bad_from", "bad_to"))
        .await
        .unwrap_err();
    let BindError::DoesNotConform(v) = err else {
        panic!("expected DoesNotConform, got {err:?}");
    };
    assert!(v.iter().any(|x| x.property == "bad_from"));
    assert!(v.iter().any(|x| x.property == "bad_to"));
}

#[tokio::test]
async fn bind_link_accepts_a_valid_join_table() {
    let cp = cp();
    seed_link_tables(&cp).await;

    // from_key on customer (id), from_column/to_column on the join table
    // (cust_id/ord_id), to_key on order (id) — the read-time mapping.
    bind_link(&cp, &cp, jt_link("id", "cust_id", "ord_id", "id"))
        .await
        .unwrap();
    assert!(link_names(&cp).await.contains(&"viaOrders".to_string()));
}

#[tokio::test]
async fn bind_link_rejects_each_bad_join_table_column_position() {
    // from_key (from-type table), from_column + to_column (join table),
    // to_key (to-type table). One bad column per position -> MissingColumn.
    let cases = [
        ("from_key", jt_link("nope", "cust_id", "ord_id", "id")),
        ("from_column", jt_link("id", "nope", "ord_id", "id")),
        ("to_column", jt_link("id", "cust_id", "nope", "id")),
        ("to_key", jt_link("id", "cust_id", "ord_id", "nope")),
    ];
    for (label, link) in cases {
        let cp = cp();
        seed_link_tables(&cp).await;
        let err = bind_link(&cp, &cp, link).await.unwrap_err();
        let BindError::DoesNotConform(v) = err else {
            panic!("{label}: expected DoesNotConform, got {err:?}");
        };
        assert!(
            v.iter()
                .any(|x| x.property == "nope"
                    && matches!(x.reason, BindViolationReason::MissingColumn)),
            "{label}: expected a MissingColumn for 'nope', got {v:?}"
        );
        assert!(
            link_names(&cp).await.is_empty(),
            "{label}: must not persist"
        );
    }
}

#[tokio::test]
async fn bind_link_rejects_a_join_table_absent_from_the_catalog() {
    let cp = cp();
    seed_link_tables(&cp).await;

    let err = bind_link(
        &cp,
        &cp,
        jt_link_on("ghost_jt", "id", "cust_id", "ord_id", "id"),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, BindError::TableNotFound(t) if t.name == "ghost_jt"),
        "expected TableNotFound(ghost_jt)"
    );
}
