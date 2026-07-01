//! Unit tests for `row_filter_to_expr`: the RowFilter → DataFusion Expr translation
//! that governs the external-SQL path. Pure logic, no Postgres — a `rust_test`.

use control_plane_core::{CompareOp, RowFilter, ScalarValue};
use datafusion::prelude::{Expr, col, lit};
use engine_serving::governed::row_filter_to_expr;

fn cmp(property: &str, op: CompareOp, value: ScalarValue) -> RowFilter {
    RowFilter::Compare {
        property: property.to_string(),
        op,
        value,
    }
}

#[test]
fn compare_ops_map_to_binary_exprs() {
    let cases: Vec<(RowFilter, Expr)> = vec![
        (
            cmp("a", CompareOp::Eq, ScalarValue::Int(1)),
            col("a").eq(lit(1_i64)),
        ),
        (
            cmp("a", CompareOp::Ne, ScalarValue::Int(1)),
            col("a").not_eq(lit(1_i64)),
        ),
        (
            cmp("a", CompareOp::Lt, ScalarValue::Int(1)),
            col("a").lt(lit(1_i64)),
        ),
        (
            cmp("a", CompareOp::Le, ScalarValue::Int(1)),
            col("a").lt_eq(lit(1_i64)),
        ),
        (
            cmp("a", CompareOp::Gt, ScalarValue::Int(1)),
            col("a").gt(lit(1_i64)),
        ),
        (
            cmp("a", CompareOp::Ge, ScalarValue::Int(1)),
            col("a").gt_eq(lit(1_i64)),
        ),
        (
            cmp("s", CompareOp::Eq, ScalarValue::Text("x".into())),
            col("s").eq(lit("x")),
        ),
        (
            cmp("b", CompareOp::Eq, ScalarValue::Bool(true)),
            col("b").eq(lit(true)),
        ),
    ];
    for (f, expected) in cases {
        assert_eq!(row_filter_to_expr(&f).expect("translate"), expected);
    }
}

#[test]
fn null_ops_ignore_value() {
    assert_eq!(
        row_filter_to_expr(&cmp("a", CompareOp::IsNull, ScalarValue::Int(0))).expect("null"),
        col("a").is_null()
    );
    assert_eq!(
        row_filter_to_expr(&cmp("a", CompareOp::IsNotNull, ScalarValue::Int(0))).expect("notnull"),
        col("a").is_not_null()
    );
}

#[test]
fn in_and_not_in_use_list() {
    let items = ScalarValue::List(vec![ScalarValue::Int(1), ScalarValue::Int(2)]);
    assert_eq!(
        row_filter_to_expr(&cmp("a", CompareOp::In, items.clone())).expect("in"),
        col("a").in_list(vec![lit(1_i64), lit(2_i64)], false)
    );
    assert_eq!(
        row_filter_to_expr(&cmp("a", CompareOp::NotIn, items)).expect("not in"),
        col("a").in_list(vec![lit(1_i64), lit(2_i64)], true)
    );
}

#[test]
fn boolean_tree_nests_and_or_not() {
    let f = RowFilter::And(vec![
        cmp("a", CompareOp::Eq, ScalarValue::Int(1)),
        RowFilter::Or(vec![
            cmp("b", CompareOp::Eq, ScalarValue::Int(2)),
            RowFilter::Not(Box::new(cmp("c", CompareOp::Eq, ScalarValue::Int(3)))),
        ]),
    ]);
    let expected = col("a")
        .eq(lit(1_i64))
        .and(col("b").eq(lit(2_i64)).or(!col("c").eq(lit(3_i64))));
    assert_eq!(row_filter_to_expr(&f).expect("tree"), expected);
}

#[test]
fn invariant_violation_fails_closed() {
    // In with a non-list value is malformed; must Err, not panic.
    assert!(row_filter_to_expr(&cmp("a", CompareOp::In, ScalarValue::Int(1))).is_err());
    // Scalar op with a list value is malformed.
    assert!(
        row_filter_to_expr(&cmp(
            "a",
            CompareOp::Eq,
            ScalarValue::List(vec![ScalarValue::Int(1)])
        ))
        .is_err()
    );
}
