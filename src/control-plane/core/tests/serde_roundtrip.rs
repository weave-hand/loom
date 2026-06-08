//! Property-based JSON round-trips for the ACL filter types. Complements the
//! hand-built example in `acl.rs`'s inline tests with deep nesting, empty vecs, and
//! unicode property names.

use control_plane_core::{CompareOp, RowFilter, ScalarValue};
use proptest::prelude::*;

fn compare_op() -> impl Strategy<Value = CompareOp> {
    prop_oneof![
        Just(CompareOp::Eq),
        Just(CompareOp::Ne),
        Just(CompareOp::Lt),
        Just(CompareOp::Le),
        Just(CompareOp::Gt),
        Just(CompareOp::Ge),
        Just(CompareOp::In),
        Just(CompareOp::NotIn),
        Just(CompareOp::IsNull),
        Just(CompareOp::IsNotNull),
    ]
}

fn scalar_value() -> impl Strategy<Value = ScalarValue> {
    let leaf = prop_oneof![
        any::<String>().prop_map(ScalarValue::Text),
        any::<i64>().prop_map(ScalarValue::Int),
        any::<bool>().prop_map(ScalarValue::Bool),
    ];
    // Bounded recursion: lists up to depth 3, up to 5 elements (incl. empty).
    leaf.prop_recursive(3, 16, 5, |inner| {
        prop::collection::vec(inner, 0..5).prop_map(ScalarValue::List)
    })
}

fn row_filter() -> impl Strategy<Value = RowFilter> {
    let leaf =
        (".*", compare_op(), scalar_value()).prop_map(|(property, op, value)| RowFilter::Compare {
            property,
            op,
            value,
        });
    // Bounded recursion: And/Or/Not trees up to depth 4, up to ~32 nodes.
    leaf.prop_recursive(4, 32, 5, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..5).prop_map(RowFilter::And),
            prop::collection::vec(inner.clone(), 0..5).prop_map(RowFilter::Or),
            inner.prop_map(|f| RowFilter::Not(Box::new(f))),
        ]
    })
}

proptest! {
    #[test]
    fn row_filter_json_round_trips(f in row_filter()) {
        let json = serde_json::to_string(&f).unwrap();
        let back: RowFilter = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(f, back);
    }

    #[test]
    fn scalar_value_json_round_trips(v in scalar_value()) {
        let json = serde_json::to_string(&v).unwrap();
        let back: ScalarValue = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(v, back);
    }
}
