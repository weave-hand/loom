//! Pure tests for the write-policy evaluator: the typed leaf comparator
//! (`compare_cell`), the three-valued tree walk (`eval`), and the gate
//! (`check_write_policy`). No fixture.

use control_plane_core::{CompareOp, Policy, PolicyTarget, RowFilter, ScalarValue, TypeName};
use query_api::serving::SqlValue;
use query_api::write_filter::{compare_cell, eval};
use std::collections::BTreeMap;

fn date(y: i32, m: u8, d: u8) -> SqlValue {
    SqlValue::Date(time::Date::from_calendar_date(y, time::Month::try_from(m).unwrap(), d).unwrap())
}

#[test]
fn compare_cell_text_eq_and_ne() {
    let cell = SqlValue::Text("open".into());
    let op = ScalarValue::Text("open".into());
    assert_eq!(compare_cell(&cell, CompareOp::Eq, &op), Some(true));
    assert_eq!(compare_cell(&cell, CompareOp::Ne, &op), Some(false));
    let other = ScalarValue::Text("closed".into());
    assert_eq!(compare_cell(&cell, CompareOp::Eq, &other), Some(false));
}

#[test]
fn compare_cell_int_ordering() {
    let cell = SqlValue::Int(5);
    assert_eq!(
        compare_cell(&cell, CompareOp::Ge, &ScalarValue::Int(3)),
        Some(true)
    );
    assert_eq!(
        compare_cell(&cell, CompareOp::Lt, &ScalarValue::Int(3)),
        Some(false)
    );
    assert_eq!(
        compare_cell(&cell, CompareOp::Le, &ScalarValue::Int(5)),
        Some(true)
    );
}

#[test]
fn compare_cell_double_vs_int_coerces_numerically() {
    let cell = SqlValue::Double(19.99);
    assert_eq!(
        compare_cell(&cell, CompareOp::Ge, &ScalarValue::Int(10)),
        Some(true)
    );
    assert_eq!(
        compare_cell(&cell, CompareOp::Lt, &ScalarValue::Int(10)),
        Some(false)
    );
    let whole = SqlValue::Double(10.0);
    assert_eq!(
        compare_cell(&whole, CompareOp::Eq, &ScalarValue::Int(10)),
        Some(true)
    );
}

#[test]
fn compare_cell_date_vs_iso_text() {
    let cell = date(2026, 6, 16);
    assert_eq!(
        compare_cell(
            &cell,
            CompareOp::Lt,
            &ScalarValue::Text("2026-07-01".into())
        ),
        Some(true)
    );
    assert_eq!(
        compare_cell(
            &cell,
            CompareOp::Eq,
            &ScalarValue::Text("2026-06-16".into())
        ),
        Some(true)
    );
    // Unparseable operand -> UNKNOWN.
    assert_eq!(
        compare_cell(
            &cell,
            CompareOp::Eq,
            &ScalarValue::Text("not-a-date".into())
        ),
        None
    );
}

#[test]
fn compare_cell_bool_ordering_is_unknown_but_eq_works() {
    let cell = SqlValue::Bool(true);
    assert_eq!(
        compare_cell(&cell, CompareOp::Eq, &ScalarValue::Bool(true)),
        Some(true)
    );
    assert_eq!(
        compare_cell(&cell, CompareOp::Ne, &ScalarValue::Bool(true)),
        Some(false)
    );
    // Ordering on bool is deliberately undefined.
    assert_eq!(
        compare_cell(&cell, CompareOp::Lt, &ScalarValue::Bool(false)),
        None
    );
}

#[test]
fn compare_cell_type_mismatch_is_unknown() {
    let cell = SqlValue::Int(5);
    assert_eq!(
        compare_cell(&cell, CompareOp::Eq, &ScalarValue::Text("5".into())),
        None
    );
}

#[test]
fn compare_cell_null_handling() {
    let null = SqlValue::Null;
    assert_eq!(
        compare_cell(&null, CompareOp::IsNull, &ScalarValue::Int(0)),
        Some(true)
    );
    assert_eq!(
        compare_cell(&null, CompareOp::IsNotNull, &ScalarValue::Int(0)),
        Some(false)
    );
    // Any value op on a NULL cell is UNKNOWN.
    assert_eq!(
        compare_cell(&null, CompareOp::Eq, &ScalarValue::Int(0)),
        None
    );
    // IsNull on a non-null cell is false.
    let cell = SqlValue::Int(1);
    assert_eq!(
        compare_cell(&cell, CompareOp::IsNull, &ScalarValue::Int(0)),
        Some(false)
    );
    assert_eq!(
        compare_cell(&cell, CompareOp::IsNotNull, &ScalarValue::Int(0)),
        Some(true)
    );
}

#[test]
fn compare_cell_in_and_not_in() {
    let cell = SqlValue::Int(2);
    let list = ScalarValue::List(vec![ScalarValue::Int(1), ScalarValue::Int(2)]);
    assert_eq!(compare_cell(&cell, CompareOp::In, &list), Some(true));
    assert_eq!(compare_cell(&cell, CompareOp::NotIn, &list), Some(false));
    let miss = SqlValue::Int(9);
    assert_eq!(compare_cell(&miss, CompareOp::In, &list), Some(false));
    assert_eq!(compare_cell(&miss, CompareOp::NotIn, &list), Some(true));
    // A non-list operand under In is UNKNOWN.
    assert_eq!(
        compare_cell(&cell, CompareOp::In, &ScalarValue::Int(2)),
        None
    );
}

#[test]
fn compare_cell_in_with_unknown_element_propagates() {
    // A list mixing a non-matching value with a type-mismatched element: no match
    // found, but one element is UNKNOWN -> the whole IN is UNKNOWN (not false).
    let cell = SqlValue::Int(9);
    let list = ScalarValue::List(vec![ScalarValue::Int(1), ScalarValue::Text("x".into())]);
    assert_eq!(compare_cell(&cell, CompareOp::In, &list), None);
    assert_eq!(compare_cell(&cell, CompareOp::NotIn, &list), None);
    // But a present match short-circuits to known, even with an UNKNOWN element.
    let hit = SqlValue::Int(1);
    assert_eq!(compare_cell(&hit, CompareOp::In, &list), Some(true));
    assert_eq!(compare_cell(&hit, CompareOp::NotIn, &list), Some(false));
}

#[test]
fn compare_cell_double_int_coercion_direction_and_magnitude() {
    // Pins the Int-operand -> f64 coercion direction at a large magnitude.
    let big = SqlValue::Double(1_000_000.5);
    assert_eq!(
        compare_cell(&big, CompareOp::Gt, &ScalarValue::Int(1_000_000)),
        Some(true)
    );
    assert_eq!(
        compare_cell(&big, CompareOp::Lt, &ScalarValue::Int(1_000_001)),
        Some(true)
    );
    // Exact-equality only when the double is whole and equal to the int.
    let whole = SqlValue::Double(1_000_000.0);
    assert_eq!(
        compare_cell(&whole, CompareOp::Eq, &ScalarValue::Int(1_000_000)),
        Some(true)
    );
    assert_eq!(
        compare_cell(&big, CompareOp::Eq, &ScalarValue::Int(1_000_000)),
        Some(false)
    );
}

fn row<'a>(pairs: &[(&'a str, &'a SqlValue)]) -> BTreeMap<&'a str, &'a SqlValue> {
    pairs.iter().copied().collect()
}

#[test]
fn eval_compare_leaf_uses_row_cell() {
    let name = SqlValue::Text("gadget".into());
    let r = row(&[("name", &name)]);
    let f = RowFilter::Compare {
        property: "name".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("gadget".into()),
    };
    assert_eq!(eval(&f, &r), Some(true));
}

#[test]
fn eval_absent_property_reads_as_null() {
    let r: BTreeMap<&str, &SqlValue> = BTreeMap::new();
    // name is unset -> NULL -> Eq is UNKNOWN.
    let f = RowFilter::Compare {
        property: "name".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("gadget".into()),
    };
    assert_eq!(eval(&f, &r), None);
    // IsNull on the unset cell is true.
    let g = RowFilter::Compare {
        property: "name".into(),
        op: CompareOp::IsNull,
        value: ScalarValue::Text("x".into()),
    };
    assert_eq!(eval(&g, &r), Some(true));
}

#[test]
fn eval_and_or_not_three_valued() {
    let n = SqlValue::Int(5);
    let r = row(&[("n", &n)]);
    let t = RowFilter::Compare {
        property: "n".into(),
        op: CompareOp::Ge,
        value: ScalarValue::Int(1),
    }; // true
    let f = RowFilter::Compare {
        property: "n".into(),
        op: CompareOp::Lt,
        value: ScalarValue::Int(1),
    }; // false
    let u = RowFilter::Compare {
        property: "missing".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Int(1),
    }; // unknown

    // AND: false beats unknown.
    assert_eq!(
        eval(&RowFilter::And(vec![f.clone(), u.clone()]), &r),
        Some(false)
    );
    // AND: unknown beats true.
    assert_eq!(eval(&RowFilter::And(vec![t.clone(), u.clone()]), &r), None);
    // AND of all-true.
    assert_eq!(
        eval(&RowFilter::And(vec![t.clone(), t.clone()]), &r),
        Some(true)
    );
    // OR: true beats unknown.
    assert_eq!(
        eval(&RowFilter::Or(vec![t.clone(), u.clone()]), &r),
        Some(true)
    );
    // OR: unknown beats false.
    assert_eq!(eval(&RowFilter::Or(vec![f.clone(), u.clone()]), &r), None);
    // NOT(unknown) = unknown; NOT(true) = false.
    assert_eq!(eval(&RowFilter::Not(Box::new(u.clone())), &r), None);
    assert_eq!(eval(&RowFilter::Not(Box::new(t.clone())), &r), Some(false));
}

#[test]
fn eval_empty_and_or_identities() {
    let r: BTreeMap<&str, &SqlValue> = BTreeMap::new();
    assert_eq!(eval(&RowFilter::And(vec![]), &r), Some(true));
    assert_eq!(eval(&RowFilter::Or(vec![]), &r), Some(false));
}

// Silence unused-import warnings for symbols used by Task 3's tests.
#[allow(dead_code)]
fn _later_task_imports(_: Policy, _: PolicyTarget, _: TypeName) {}
