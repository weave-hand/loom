//! Unit tests for the exact-match conformance check. Pure logic — no fixtures.

use control_plane_core::{ColumnSpec, PropertyDef};
use transform::conform::{Violation, check_conformance};

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
    }
}

fn col(name: &str, ty: &str, nullable: bool) -> ColumnSpec {
    ColumnSpec {
        name: name.into(),
        ty: ty.into(),
        nullable,
    }
}

#[test]
fn exact_match_conforms() {
    let props = vec![prop("id", "Long", true), prop("region", "String", false)];
    let cols = vec![col("id", "long", false), col("region", "string", true)];
    assert_eq!(check_conformance(&cols, &props), Ok(()));
}

#[test]
fn missing_column_is_a_violation() {
    let props = vec![prop("id", "Long", true), prop("region", "String", false)];
    let cols = vec![col("id", "long", false)];
    assert_eq!(
        check_conformance(&cols, &props),
        Err(vec![Violation::MissingColumn {
            property: "region".into(),
            logical: "String".into(),
        }])
    );
}

#[test]
fn type_mismatch_is_a_violation() {
    let props = vec![prop("id", "Long", true)];
    let cols = vec![col("id", "integer", false)];
    assert_eq!(
        check_conformance(&cols, &props),
        Err(vec![Violation::TypeMismatch {
            property: "id".into(),
            logical: "Long".into(),
            physical: "integer".into(),
        }])
    );
}

#[test]
fn unknown_logical_type_is_a_violation() {
    let props = vec![prop("id", "Wibble", true)];
    let cols = vec![col("id", "long", false)];
    assert_eq!(
        check_conformance(&cols, &props),
        Err(vec![Violation::UnknownLogicalType {
            property: "id".into(),
            logical: "Wibble".into(),
        }])
    );
}

#[test]
fn required_property_over_nullable_column_is_a_violation() {
    let props = vec![prop("id", "Long", true)];
    let cols = vec![col("id", "long", true)];
    assert_eq!(
        check_conformance(&cols, &props),
        Err(vec![Violation::NullabilityViolation {
            property: "id".into(),
        }])
    );
}

#[test]
fn extra_result_column_is_a_violation() {
    let props = vec![prop("id", "Long", true)];
    let cols = vec![col("id", "long", false), col("extra", "string", true)];
    assert_eq!(
        check_conformance(&cols, &props),
        Err(vec![Violation::UnexpectedColumn {
            column: "extra".into(),
        }])
    );
}

#[test]
fn all_violations_are_collected() {
    // `id` mismatched (integer vs Long), `region` missing, `extra` unexpected — all three.
    let props = vec![prop("id", "Long", true), prop("region", "String", false)];
    let cols = vec![col("id", "integer", false), col("extra", "boolean", true)];
    let err = check_conformance(&cols, &props).unwrap_err();
    assert!(err.contains(&Violation::TypeMismatch {
        property: "id".into(),
        logical: "Long".into(),
        physical: "integer".into(),
    }));
    assert!(err.contains(&Violation::MissingColumn {
        property: "region".into(),
        logical: "String".into(),
    }));
    assert!(err.contains(&Violation::UnexpectedColumn {
        column: "extra".into(),
    }));
    assert_eq!(
        err.len(),
        3,
        "exactly three violations, none short-circuited"
    );
}

#[test]
fn empty_inputs_conform() {
    // Degenerate case: no properties, no columns -> trivially conforms.
    assert_eq!(check_conformance(&[], &[]), Ok(()));
}

#[test]
fn unknown_logical_type_and_nullability_co_occur() {
    // A required property with an unknown logical type backed by a nullable column
    // yields BOTH violations — the nullability check is independent of the type check
    // (mirrors ingest::bind's collect-everything behavior).
    let props = vec![prop("id", "Wibble", true)];
    let cols = vec![col("id", "long", true)];
    let err = check_conformance(&cols, &props).unwrap_err();
    assert!(err.contains(&Violation::UnknownLogicalType {
        property: "id".into(),
        logical: "Wibble".into(),
    }));
    assert!(err.contains(&Violation::NullabilityViolation {
        property: "id".into(),
    }));
    assert_eq!(
        err.len(),
        2,
        "both the unknown-type and nullability violations are reported"
    );
}
