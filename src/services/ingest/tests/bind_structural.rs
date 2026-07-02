//! Pure, fake-free unit tests for `bind`'s structural violation collectors
//! (property type/nullability, identity, reserved names). No catalog/ontology —
//! just an `ObjectType` and a `TableSchema`.

use control_plane_core::{
    Aggregation, ColumnDef, DerivedPropertyDef, ObjectType, PropertyDef, TableRef, TableSchema,
    TypeName,
};
use ingest::bind::{
    BindViolationReason, identity_violation, property_violations, reserved_name_violations,
    structural_violations,
};

fn col(name: &str, ty: &str, nullable: bool) -> ColumnDef {
    ColumnDef {
        order: 0,
        name: name.into(),
        ty: ty.into(),
        nullable,
    }
}

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
        constraints: control_plane_core::PropertyConstraints::default(),
    }
}

fn otype(properties: Vec<PropertyDef>, identity: Option<&str>) -> ObjectType {
    ObjectType {
        name: TypeName("T".into()),
        properties,
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "t".into(),
        },
        identity: identity.map(str::to_string),
    }
}

#[test]
fn property_missing_column_is_reported() {
    let ty = otype(vec![prop("id", "Long", true)], None);
    let schema = TableSchema { columns: vec![] };
    let v = property_violations(&ty, &schema);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].property, "id");
    assert_eq!(v[0].reason, BindViolationReason::MissingColumn);
}

#[test]
fn required_but_nullable_column_is_a_violation() {
    let ty = otype(vec![prop("id", "Long", true)], None);
    let schema = TableSchema {
        columns: vec![col("id", "long", true)],
    };
    let v = property_violations(&ty, &schema);
    assert!(
        v.iter()
            .any(|x| x.reason == BindViolationReason::NullabilityViolation)
    );
}

#[test]
fn conforming_property_yields_no_violation() {
    let ty = otype(vec![prop("id", "Long", true)], None);
    let schema = TableSchema {
        columns: vec![col("id", "long", false)],
    };
    assert!(property_violations(&ty, &schema).is_empty());
}

#[test]
fn property_type_mismatch_is_reported() {
    // Property declared `String` but the physical column is `long` -> TypeMismatch
    // (both are known logical types, so not UnknownLogicalType).
    let ty = otype(vec![prop("id", "String", true)], None);
    let schema = TableSchema {
        columns: vec![col("id", "long", false)],
    };
    let v = property_violations(&ty, &schema);
    assert!(
        v.iter()
            .any(|x| matches!(x.reason, BindViolationReason::TypeMismatch { .. }))
    );
}

#[test]
fn identity_naming_no_property_is_reported() {
    let ty = otype(vec![prop("id", "Long", true)], Some("missing"));
    let v = identity_violation(&ty).expect("violation");
    assert_eq!(v.property, "missing");
    assert!(matches!(v.reason, BindViolationReason::BadIdentity(_)));
}

#[test]
fn identity_naming_non_required_property_is_reported() {
    let ty = otype(vec![prop("id", "Long", false)], Some("id"));
    let v = identity_violation(&ty).expect("violation");
    assert!(matches!(v.reason, BindViolationReason::BadIdentity(_)));
}

#[test]
fn identity_naming_required_property_is_ok() {
    let ty = otype(vec![prop("id", "Long", true)], Some("id"));
    assert!(identity_violation(&ty).is_none());
}

#[test]
fn underscore_property_and_derived_names_are_reserved() {
    let ty = otype(vec![prop("_secret", "Long", true)], None);
    let v = reserved_name_violations(&ty);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].property, "_secret");
    assert_eq!(v[0].reason, BindViolationReason::ReservedName);
}

#[test]
fn underscore_derived_name_is_reserved() {
    let mut ty = otype(vec![prop("id", "Long", true)], None);
    ty.derived = vec![DerivedPropertyDef {
        name: "_hidden".into(),
        ty: "Long".into(),
        link: "orders".into(),
        agg: Aggregation::Count,
    }];
    let v = reserved_name_violations(&ty);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].property, "_hidden");
    assert_eq!(v[0].reason, BindViolationReason::ReservedName);
}

#[test]
fn structural_violations_composes_all_three_passes() {
    // A reserved property name AND a bad identity in one type.
    let ty = otype(vec![prop("_x", "Long", true)], Some("missing"));
    let schema = TableSchema {
        columns: vec![col("_x", "long", false)],
    };
    let v = structural_violations(&ty, &schema);
    assert!(
        v.iter()
            .any(|x| x.reason == BindViolationReason::ReservedName)
    );
    assert!(
        v.iter()
            .any(|x| matches!(x.reason, BindViolationReason::BadIdentity(_)))
    );
}
