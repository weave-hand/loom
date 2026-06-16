//! Unit tests for query-param filter coercion. Pure logic.

use control_plane_core::CompareOp;
use query_api::filter::coerce_predicate;
use query_api::filter::{FilterError, coerce_filter};
use query_api::serving::SqlValue;

#[test]
fn integer_and_double_coerce_via_number_repr() {
    assert_eq!(
        coerce_filter("x", "Integer", "100").unwrap(),
        SqlValue::Int(100)
    );
    assert_eq!(
        coerce_filter("x", "Double", "100").unwrap(),
        SqlValue::Int(100)
    );
    assert_eq!(
        coerce_filter("x", "Double", "1.5").unwrap(),
        SqlValue::Double(1.5)
    );
    assert_eq!(
        coerce_filter("x", "Integer", "-7").unwrap(),
        SqlValue::Int(-7)
    );
}

#[test]
fn long_coerces_to_int() {
    assert_eq!(
        coerce_filter("id", "Long", "100").unwrap(),
        SqlValue::Int(100)
    );
    assert!(matches!(
        coerce_filter("id", "Long", "1.5"),
        Err(FilterError::BadValue(_, _))
    ));
}

#[test]
fn boolean_coerces_strictly() {
    assert_eq!(
        coerce_filter("a", "Boolean", "true").unwrap(),
        SqlValue::Bool(true)
    );
    assert_eq!(
        coerce_filter("a", "Boolean", "false").unwrap(),
        SqlValue::Bool(false)
    );
    assert!(matches!(
        coerce_filter("a", "Boolean", "maybe"),
        Err(FilterError::BadValue(_, _))
    ));
    assert!(matches!(
        coerce_filter("a", "Boolean", "1"),
        Err(FilterError::BadValue(_, _))
    ));
}

#[test]
fn string_passes_through() {
    assert_eq!(
        coerce_filter("s", "String", "hi").unwrap(),
        SqlValue::Text("hi".into())
    );
    assert_eq!(
        coerce_filter("s", "String", "100").unwrap(),
        SqlValue::Text("100".into())
    );
}

#[test]
fn date_and_timestamp_coerce_from_iso() {
    let d = coerce_filter("d", "Date", "2026-06-16").unwrap();
    assert!(matches!(d, SqlValue::Date(_)));
    let ts = coerce_filter("t", "Timestamp", "2026-06-16T12:00:00").unwrap();
    assert!(matches!(ts, SqlValue::Timestamp(_)));
    assert!(matches!(
        coerce_filter("d", "Date", "nope"),
        Err(FilterError::BadValue(_, _))
    ));
    assert!(matches!(
        coerce_filter("t", "Timestamp", "2026-06-16"),
        Err(FilterError::BadValue(_, _))
    ));
}

#[test]
fn uncoercible_number_and_unknown_type_error() {
    assert!(matches!(
        coerce_filter("x", "Double", "abc"),
        Err(FilterError::BadValue(_, _))
    ));
    assert!(matches!(
        coerce_filter("x", "Integer", "abc"),
        Err(FilterError::BadValue(_, _))
    ));
    assert!(matches!(
        coerce_filter("x", "Nonsense", "1"),
        Err(FilterError::BadValue(_, _))
    ));
}

#[test]
fn bare_value_is_eq() {
    let p = coerce_predicate("amount", "Double", "100").unwrap();
    assert_eq!(p.op, CompareOp::Eq);
    assert_eq!(p.values, vec![SqlValue::Int(100)]);
}

#[test]
fn scalar_ops_parse_and_coerce() {
    let p = coerce_predicate("amount", "Double", "gt:100").unwrap();
    assert_eq!(p.op, CompareOp::Gt);
    assert_eq!(p.values, vec![SqlValue::Int(100)]);
    let p2 = coerce_predicate("amount", "Double", "le:1.5").unwrap();
    assert_eq!(p2.op, CompareOp::Le);
    assert_eq!(p2.values, vec![SqlValue::Double(1.5)]);
    let p3 = coerce_predicate("status", "String", "ne:cancelled").unwrap();
    assert_eq!(p3.op, CompareOp::Ne);
    assert_eq!(p3.values, vec![SqlValue::Text("cancelled".into())]);
}

#[test]
fn in_and_nin_coerce_each_operand() {
    let p = coerce_predicate("id", "Long", "in:1,2,3").unwrap();
    assert_eq!(p.op, CompareOp::In);
    assert_eq!(
        p.values,
        vec![SqlValue::Int(1), SqlValue::Int(2), SqlValue::Int(3)]
    );
    let p2 = coerce_predicate("region", "String", "nin:NY,TX").unwrap();
    assert_eq!(p2.op, CompareOp::NotIn);
    assert_eq!(
        p2.values,
        vec![SqlValue::Text("NY".into()), SqlValue::Text("TX".into())]
    );
}

#[test]
fn null_ops_take_no_operand() {
    let n = coerce_predicate("c", "Timestamp", "isnull").unwrap();
    assert_eq!(n.op, CompareOp::IsNull);
    assert!(n.values.is_empty());
    let nn = coerce_predicate("c", "Timestamp", "isnotnull").unwrap();
    assert_eq!(nn.op, CompareOp::IsNotNull);
    assert!(nn.values.is_empty());
}

#[test]
fn eq_escape_forces_literal() {
    // `rest` is everything after the FIRST colon, so the literal `gt:foo` passes through.
    let p = coerce_predicate("name", "String", "eq:gt:foo").unwrap();
    assert_eq!(p.op, CompareOp::Eq);
    assert_eq!(p.values, vec![SqlValue::Text("gt:foo".into())]);
}

#[test]
fn arity_errors() {
    assert!(coerce_predicate("amount", "Double", "gt").is_err()); // scalar, no operand
    assert!(coerce_predicate("status", "String", "in:").is_err()); // set, empty
    assert!(coerce_predicate("c", "Timestamp", "isnull:x").is_err()); // null op given an operand
}

#[test]
fn bad_operand_is_error() {
    assert!(coerce_predicate("amount", "Double", "gt:abc").is_err());
    assert!(coerce_predicate("id", "Long", "in:1,x,3").is_err());
}
