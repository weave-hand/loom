//! Define-time validation of a derived property's aggregate column against its
//! link's target type (`validate_derived_columns`) — best-effort: only a
//! *resolvable* link+target is checked; an unresolvable link/target defers to
//! the read path.

use std::collections::HashMap;

use control_plane_core::{
    Aggregation, ControlPlaneError, DerivedPropertyDef, ObjectType, PropertyConstraints,
    PropertyDef, TableRef, TypeName, validate_derived_columns,
};

fn prop(name: &str, ty: &str) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required: false,
        constraints: PropertyConstraints::default(),
    }
}

fn target_type(name: &str, props: Vec<PropertyDef>) -> ObjectType {
    ObjectType {
        name: TypeName(name.into()),
        properties: props,
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: name.to_ascii_lowercase(),
        },
        identity: None,
        version: None,
    }
}

fn derived(link: &str, agg: Aggregation) -> DerivedPropertyDef {
    DerivedPropertyDef {
        name: "x".into(),
        ty: "Double".into(),
        link: link.into(),
        agg,
    }
}

#[test]
fn count_skips_with_no_column() {
    let targets: HashMap<String, ObjectType> = HashMap::new();
    let d = vec![derived("transactions", Aggregation::Count)];
    // Even with an unresolvable link, Count never looks at a column.
    assert!(validate_derived_columns(&d, |ln| targets.get(ln)).is_ok());
}

#[test]
fn undeclared_column_is_ok_skipped() {
    let mut targets: HashMap<String, ObjectType> = HashMap::new();
    targets.insert(
        "transactions".into(),
        target_type(
            "Transaction",
            vec![prop("id", "Long"), prop("amount", "Double")],
        ),
    );
    // `nope` is not a declared property — it may be a valid catalog-only column,
    // so validation SKIPS it (best-effort; the ingest `bind` seam is the catalog-aware
    // backstop), rather than rejecting it.
    let d = vec![derived("transactions", Aggregation::Sum("nope".into()))];
    assert!(validate_derived_columns(&d, |ln| targets.get(ln)).is_ok());
}

#[test]
fn sum_over_non_numeric_is_validation_error() {
    let mut targets: HashMap<String, ObjectType> = HashMap::new();
    targets.insert(
        "transactions".into(),
        target_type(
            "Transaction",
            vec![prop("id", "Long"), prop("note", "String")],
        ),
    );
    let d = vec![derived("transactions", Aggregation::Sum("note".into()))];
    let result = validate_derived_columns(&d, |ln| targets.get(ln));
    assert!(
        matches!(result, Err(ControlPlaneError::Validation(_))),
        "Sum over a non-numeric column should be a Validation error: {result:?}"
    );
}

#[test]
fn min_over_ordered_column_is_ok() {
    let mut targets: HashMap<String, ObjectType> = HashMap::new();
    targets.insert(
        "transactions".into(),
        target_type(
            "Transaction",
            vec![prop("id", "Long"), prop("note", "String")],
        ),
    );
    let d = vec![derived("transactions", Aggregation::Min("note".into()))];
    assert!(validate_derived_columns(&d, |ln| targets.get(ln)).is_ok());
}

#[test]
fn unresolvable_link_is_ok_deferred() {
    let targets: HashMap<String, ObjectType> = HashMap::new();
    let d = vec![derived("ghostlink", Aggregation::Sum("whatever".into()))];
    assert!(validate_derived_columns(&d, |ln| targets.get(ln)).is_ok());
}

#[test]
fn valid_sum_over_numeric_column_is_ok() {
    let mut targets: HashMap<String, ObjectType> = HashMap::new();
    targets.insert(
        "transactions".into(),
        target_type(
            "Transaction",
            vec![prop("id", "Long"), prop("amount", "Double")],
        ),
    );
    let d = vec![derived("transactions", Aggregation::Sum("amount".into()))];
    assert!(validate_derived_columns(&d, |ln| targets.get(ln)).is_ok());
}
