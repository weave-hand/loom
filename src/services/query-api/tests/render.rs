//! The logical-type -> JSON rendering matrix (pure). Proves Long renders as a STRING
//! (the >2^53 precision rule), temporal types as ISO-8601, the natural fallback for
//! unknown/mismatched types, and the { "objects": [...] } envelope shape.

use query_api::handler::ObjectRows;
use query_api::render::objects_to_json;
use query_api::serving::SqlValue;
use serde_json::json;

fn one(logical_ty: &str, cell: SqlValue) -> serde_json::Value {
    let rows = ObjectRows {
        columns: vec!["c".into()],
        logical_types: vec![logical_ty.into()],
        rows: vec![vec![cell]],
    };
    objects_to_json(&rows, None)["objects"][0]["c"].clone()
}

#[test]
fn long_renders_as_string_preserving_precision_past_2_53() {
    assert_eq!(
        one("Long", SqlValue::Int(9_007_199_254_740_993)),
        json!("9007199254740993")
    );
}

#[test]
fn scalar_types_render_per_vocabulary() {
    assert_eq!(one("Integer", SqlValue::Int(42)), json!(42));
    assert_eq!(one("Double", SqlValue::Double(3.5)), json!(3.5));
    assert_eq!(one("Boolean", SqlValue::Bool(true)), json!(true));
    assert_eq!(one("String", SqlValue::Text("hi".into())), json!("hi"));
    assert_eq!(
        one("EmailAddress", SqlValue::Text("a@b.com".into())),
        json!("a@b.com")
    );
}

#[test]
fn temporal_types_render_iso_8601() {
    assert_eq!(
        one("Date", SqlValue::Date(time::macros::date!(2026 - 06 - 12))),
        json!("2026-06-12")
    );
    assert_eq!(
        one(
            "Timestamp",
            SqlValue::Timestamp(time::macros::datetime!(2026 - 06 - 12 14:09:42))
        ),
        json!("2026-06-12T14:09:42")
    );
}

#[test]
fn null_is_json_null_regardless_of_type() {
    assert_eq!(one("Long", SqlValue::Null), serde_json::Value::Null);
    assert_eq!(one("Date", SqlValue::Null), serde_json::Value::Null);
}

#[test]
fn unknown_type_falls_back_to_natural_rendering() {
    assert_eq!(one("Money", SqlValue::Double(1.25)), json!(1.25));
    assert_eq!(one("Money", SqlValue::Text("x".into())), json!("x"));
}

#[test]
fn declared_value_mismatch_falls_back_to_natural_rendering() {
    assert_eq!(one("Date", SqlValue::Int(7)), json!(7));
}

#[test]
fn objects_to_json_builds_keyed_objects_in_column_order() {
    let rows = ObjectRows {
        columns: vec!["id".into(), "email".into()],
        logical_types: vec!["Long".into(), "EmailAddress".into()],
        rows: vec![
            vec![SqlValue::Int(1), SqlValue::Text("a@x".into())],
            vec![SqlValue::Int(2), SqlValue::Text("b@x".into())],
        ],
    };
    assert_eq!(
        objects_to_json(&rows, None),
        json!({ "objects": [
            { "id": "1", "email": "a@x" },
            { "id": "2", "email": "b@x" },
        ], "next": null })
    );
}
