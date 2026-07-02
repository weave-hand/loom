//! Pure unit tests for the insert write-path phases extracted from
//! `action.rs::run_insert`: `value_constraint_violations` (per-value declared-
//! constraint check over resolved write pairs) and `expand_to_full_row`
//! (full-property NULL expansion), plus the shared `affected_object` response
//! epilogue. No fixture; the e2e twins live in constraints_action_http /
//! action_e2e.

use control_plane_core::{ConstraintRule, ObjectType, PropertyConstraints, RangeConstraint};
use query_api::action::{expand_to_full_row, value_constraint_violations};
use query_api::serving::SqlValue;

/// Gauge(id Long required, qty Long [0..=100], note String) — one constrained
/// property, one unconstrained, one required-uncovered-by-constraints.
fn gauge() -> ObjectType {
    ObjectType::build("Gauge", ("main", "gauge"))
        .prop_req("id", "Long")
        .prop_with(
            "qty",
            "Long",
            false,
            PropertyConstraints {
                range: Some(RangeConstraint {
                    min: Some(0.0),
                    max: Some(100.0),
                }),
                ..PropertyConstraints::default()
            },
        )
        .prop("note", "String")
        .identity("id")
        .done()
}

#[test]
fn in_range_values_have_no_violations() {
    let pairs = vec![
        ("id".to_string(), SqlValue::Int(1)),
        ("qty".to_string(), SqlValue::Int(50)),
    ];
    assert!(
        value_constraint_violations(&gauge(), &pairs)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn out_of_range_value_is_collected() {
    let pairs = vec![("qty".to_string(), SqlValue::Int(999))];
    let v = value_constraint_violations(&gauge(), &pairs).unwrap();
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].property, "qty");
    assert_eq!(v[0].rule, ConstraintRule::Range);
}

#[test]
fn empty_null_and_unconstrained_cells_are_skipped() {
    // Empty pair set: trivially clean (the DELETE path's shape after Task 7).
    assert!(value_constraint_violations(&gauge(), &[]).unwrap().is_empty());
    let pairs = vec![
        // Omitted optional (NULL): no value to check.
        ("qty".to_string(), SqlValue::Null),
        // No constraints declared on `note`.
        ("note".to_string(), SqlValue::Text("x".into())),
        // Not a property: skipped (conformance rejects the shape upstream).
        ("ghost".to_string(), SqlValue::Int(1)),
    ];
    assert!(
        value_constraint_violations(&gauge(), &pairs)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn expand_fills_unset_properties_with_null_in_declared_order() {
    let pairs = vec![("qty".to_string(), SqlValue::Int(7))];
    let (cols, vals, logical) = expand_to_full_row(&gauge(), &pairs);
    assert_eq!(cols, vec!["id", "qty", "note"]);
    assert_eq!(
        vals,
        vec![SqlValue::Null, SqlValue::Int(7), SqlValue::Null]
    );
    assert_eq!(logical, vec!["Long", "Long", "String"]);
}
