//! Pure tests for the write-policy evaluator: the typed leaf comparator
//! (`compare_cell`), the three-valued tree walk (`eval`), and the gate
//! (`check_write_policy`). No fixture.

use control_plane_core::{CompareOp, Policy, PolicyTarget, RowFilter, ScalarValue, TypeName};
use query_api::serving::SqlValue;
use query_api::write_filter::compare_cell;

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

// Silence unused-import warnings for symbols used by later tasks' tests.
#[allow(dead_code)]
fn _later_task_imports(_: Policy, _: PolicyTarget, _: RowFilter, _: TypeName) {}
