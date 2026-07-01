use control_plane_core::ParamDef;
use query_api::params::{ParamError, parse_params};
use query_api::serving::SqlValue;
use serde_json::json;

fn p(name: &str, ty: &str, required: bool) -> ParamDef {
    ParamDef {
        name: name.into(),
        ty: ty.into(),
        required,
        binds: None,
    }
}
fn body(v: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    v.as_object().unwrap().clone()
}

#[test]
fn parses_typed_params_in_order() {
    let params = vec![
        p("id", "Long", true),
        p("score", "Double", false),
        p("name", "String", false),
    ];
    let got = parse_params(
        &params,
        &body(json!({ "id": "10", "score": 1.5, "name": "x" })),
    )
    .unwrap();
    assert_eq!(
        got,
        vec![
            ("id".into(), SqlValue::Int(10)),
            ("score".into(), SqlValue::Double(1.5)),
            ("name".into(), SqlValue::Text("x".into())),
        ]
    );
}

#[test]
fn missing_required_is_an_error() {
    let params = vec![p("id", "Long", true)];
    assert_eq!(
        parse_params(&params, &body(json!({}))),
        Err(ParamError::Missing("id".into()))
    );
}

#[test]
fn unknown_param_is_an_error() {
    let params = vec![p("id", "Long", true)];
    assert_eq!(
        parse_params(&params, &body(json!({ "id": "1", "extra": 2 }))),
        Err(ParamError::Unknown("extra".into()))
    );
}

#[test]
fn mistyped_value_is_an_error() {
    let params = vec![p("id", "Long", true)];
    assert!(matches!(
        parse_params(&params, &body(json!({ "id": 10 }))), // Long must be a string
        Err(ParamError::BadValue(_, _))
    ));
}

#[test]
fn optional_absent_becomes_null() {
    let params = vec![p("id", "Long", true), p("name", "String", false)];
    let got = parse_params(&params, &body(json!({ "id": "1" }))).unwrap();
    assert_eq!(got[1], ("name".into(), SqlValue::Null));
}

#[test]
fn parses_bool_date_timestamp_and_integer() {
    use time::macros::{date, datetime};
    let params = vec![
        p("flag", "Boolean", true),
        p("when", "Date", true),
        p("at", "Timestamp", true),
        p("count", "Integer", true),
    ];
    let got = parse_params(
        &params,
        &body(json!({
            "flag": true,
            "when": "2026-06-15",
            "at": "2026-06-15T12:30:00",
            "count": 5
        })),
    )
    .unwrap();
    assert_eq!(
        got,
        vec![
            ("flag".into(), SqlValue::Bool(true)),
            ("when".into(), SqlValue::Date(date!(2026 - 06 - 15))),
            (
                "at".into(),
                SqlValue::Timestamp(datetime!(2026 - 06 - 15 12:30:00)),
            ),
            ("count".into(), SqlValue::Int(5)),
        ]
    );
}

#[test]
fn invalid_iso_date_is_an_error() {
    let params = vec![p("when", "Date", true)];
    assert!(matches!(
        parse_params(&params, &body(json!({ "when": "not-a-date" }))),
        Err(ParamError::BadValue(_, _))
    ));
}
