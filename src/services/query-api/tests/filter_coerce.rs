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

#[test]
fn set_operands_escape_commas_and_backslashes() {
    // Headline: an escaped comma keeps "a,b" as one operand.
    let p = coerce_predicate("tag", "String", r"in:a\,b,c").unwrap();
    assert_eq!(p.op, CompareOp::In);
    assert_eq!(
        p.values,
        vec![SqlValue::Text("a,b".into()), SqlValue::Text("c".into())]
    );

    // Escaped backslash -> a single literal backslash operand.
    let p2 = coerce_predicate("tag", "String", r"in:a\\b").unwrap();
    assert_eq!(p2.values, vec![SqlValue::Text(r"a\b".into())]);

    // Lone escaped comma is one non-empty operand "," (NOT an empty-operand error).
    let p3 = coerce_predicate("tag", "String", r"in:\,").unwrap();
    assert_eq!(p3.values, vec![SqlValue::Text(",".into())]);

    // Escaped comma at the END of an operand: `\,` consumes the trailing comma,
    // so "a," is one non-empty operand (it must NOT be read as a dangling split).
    let p5 = coerce_predicate("tag", "String", r"in:a\,").unwrap();
    assert_eq!(p5.values, vec![SqlValue::Text("a,".into())]);

    // Lone escaped backslash is one operand "\".
    let p6 = coerce_predicate("tag", "String", r"in:\\").unwrap();
    assert_eq!(p6.values, vec![SqlValue::Text(r"\".into())]);

    // nin parity (same arm handles both).
    let p4 = coerce_predicate("tag", "String", r"nin:x\,y,z").unwrap();
    assert_eq!(p4.op, CompareOp::NotIn);
    assert_eq!(
        p4.values,
        vec![SqlValue::Text("x,y".into()), SqlValue::Text("z".into())]
    );
}

#[test]
fn set_operand_escape_errors_and_unescaped_empties() {
    // Unescaped empty segments still error (unchanged contract).
    assert!(coerce_predicate("tag", "String", "in:a,").is_err());
    assert!(coerce_predicate("tag", "String", "in:,a").is_err());
    assert!(coerce_predicate("tag", "String", "in:a,,b").is_err());

    // Unknown escape and dangling escape are hard errors.
    assert!(coerce_predicate("tag", "String", r"in:a\b").is_err());
    assert!(coerce_predicate("tag", "String", r"in:a\").is_err());
}

#[test]
fn scalar_op_does_not_unescape() {
    // Regression guard: escaping must NOT leak into scalar parsing.
    // `eq:a\,b` stays the literal operand `a\,b` (rest taken whole, no split/unescape).
    let p = coerce_predicate("name", "String", r"eq:a\,b").unwrap();
    assert_eq!(p.op, CompareOp::Eq);
    assert_eq!(p.values, vec![SqlValue::Text(r"a\,b".into())]);
}

#[test]
fn predicate_edge_cases_are_pinned() {
    // Single-operand `in` stays a set op (In with a 1-element Vec), not collapsed to Eq.
    let one = coerce_predicate("id", "Long", "in:5").unwrap();
    assert_eq!(one.op, CompareOp::In);
    assert_eq!(one.values, vec![SqlValue::Int(5)]);

    // Negative number operand round-trips through coerce_filter.
    let neg = coerce_predicate("amount", "Double", "gt:-5").unwrap();
    assert_eq!(neg.op, CompareOp::Gt);
    assert_eq!(neg.values, vec![SqlValue::Int(-5)]);

    // Empty middle operand in a set is an error (exercises the per-part empty check).
    assert!(coerce_predicate("id", "Long", "in:1,,3").is_err());

    // A null op with a trailing colon but no operand is accepted (empty rest = no operand).
    let n = coerce_predicate("c", "Timestamp", "isnull:").unwrap();
    assert_eq!(n.op, CompareOp::IsNull);
    assert!(n.values.is_empty());
}
