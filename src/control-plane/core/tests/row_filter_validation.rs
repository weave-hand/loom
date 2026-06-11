//! Tests for `validate_row_filter` — the shared definition of RowFilter
//! well-formedness (structural CompareOp<->ScalarValue invariant + optional
//! property-existence). Lives as an integration test because buck2 only runs
//! `rust_test` targets, not a library's inline `#[cfg(test)]` module.

use std::collections::HashSet;

use control_plane_core::{CompareOp, RowFilter, ScalarValue, validate_row_filter};

fn props(names: &[&str]) -> HashSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}

#[test]
fn validate_structural_ok_and_err() {
    // In/NotIn require a list; scalar ops require a non-list; IsNull ignores value.
    let in_list = RowFilter::Compare {
        property: "r".into(),
        op: CompareOp::In,
        value: ScalarValue::List(vec![ScalarValue::Text("EU".into())]),
    };
    assert!(validate_row_filter(&in_list, None).is_ok());

    let in_scalar = RowFilter::Compare {
        property: "r".into(),
        op: CompareOp::In,
        value: ScalarValue::Text("EU".into()),
    };
    assert!(validate_row_filter(&in_scalar, None).is_err());

    let eq_list = RowFilter::Compare {
        property: "r".into(),
        op: CompareOp::Eq,
        value: ScalarValue::List(vec![]),
    };
    assert!(validate_row_filter(&eq_list, None).is_err());

    let eq_scalar = RowFilter::Compare {
        property: "r".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Int(1),
    };
    assert!(validate_row_filter(&eq_scalar, None).is_ok());

    let is_null = RowFilter::Compare {
        property: "r".into(),
        op: CompareOp::IsNull,
        value: ScalarValue::List(vec![]), // ignored for IsNull
    };
    assert!(validate_row_filter(&is_null, None).is_ok());

    // NotIn mirrors In (list required); IsNotNull mirrors IsNull (value ignored).
    let not_in_scalar = RowFilter::Compare {
        property: "r".into(),
        op: CompareOp::NotIn,
        value: ScalarValue::Int(1),
    };
    assert!(validate_row_filter(&not_in_scalar, None).is_err());
    let is_not_null = RowFilter::Compare {
        property: "r".into(),
        op: CompareOp::IsNotNull,
        value: ScalarValue::List(vec![]),
    };
    assert!(validate_row_filter(&is_not_null, None).is_ok());
}

#[test]
fn error_messages_name_the_problem() {
    // The reason string is part of the contract (surfaced via Validation/CompileError).
    let in_scalar = RowFilter::Compare {
        property: "r".into(),
        op: CompareOp::In,
        value: ScalarValue::Text("x".into()),
    };
    assert!(
        validate_row_filter(&in_scalar, None)
            .unwrap_err()
            .contains("list")
    );
    let f = RowFilter::Compare {
        property: "ghost".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Int(1),
    };
    assert!(
        validate_row_filter(&f, Some(&props(&["real"])))
            .unwrap_err()
            .contains("ghost")
    );
}

#[test]
fn validate_property_existence() {
    let f = RowFilter::Compare {
        property: "known".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Int(1),
    };
    assert!(validate_row_filter(&f, Some(&props(&["known", "other"]))).is_ok());
    assert!(validate_row_filter(&f, Some(&props(&["other"]))).is_err());
    // None = structural only, property not checked.
    assert!(validate_row_filter(&f, None).is_ok());
}

#[test]
fn validate_recurses_into_and_or_not() {
    // A malformed leaf deep in the tree is caught.
    let bad = RowFilter::And(vec![
        RowFilter::Compare {
            property: "a".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Int(1),
        },
        RowFilter::Or(vec![RowFilter::Not(Box::new(RowFilter::Compare {
            property: "b".into(),
            op: CompareOp::In,
            value: ScalarValue::Int(2), // In with non-list -> err
        }))]),
    ]);
    assert!(validate_row_filter(&bad, None).is_err());
    assert!(validate_row_filter(&bad, Some(&props(&["a", "b"]))).is_err());
}
