//! Unit tests for query-param filter coercion. Pure logic.

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
