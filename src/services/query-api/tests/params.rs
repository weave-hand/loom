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

// --- resolve_action_row: param->property mapping + constant assignments (slice 1) ---

use control_plane_core::{
    ActionDef, ActionKind, ActionName, Assignment, ObjectType, PropertyDef, TableRef, TypeName,
};
use query_api::params::resolve_action_row;

fn gadget() -> ObjectType {
    let prop = |name: &str, ty: &str, required: bool| PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
        constraints: control_plane_core::PropertyConstraints::default(),
    };
    ObjectType {
        name: TypeName("Gadget".into()),
        properties: vec![
            prop("id", "Long", true),
            prop("name", "String", false),
            prop("status", "String", false),
        ],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "gadget".into(),
        },
        identity: None,
    }
}

fn pb(name: &str, ty: &str, required: bool, binds: Option<&str>) -> ParamDef {
    ParamDef {
        name: name.into(),
        ty: ty.into(),
        required,
        binds: binds.map(str::to_string),
    }
}

fn insert(params: Vec<ParamDef>, assignments: Vec<Assignment>) -> ActionDef {
    ActionDef {
        name: ActionName("a".into()),
        target: TypeName("Gadget".into()),
        parameters: params,
        kind: ActionKind::Insert,
        assignments,
    }
}

#[test]
fn resolve_maps_binds_to_property() {
    let action = insert(
        vec![
            pb("id", "Long", true, None),
            pb("displayName", "String", false, Some("name")),
        ],
        vec![],
    );
    let pairs = resolve_action_row(
        &action,
        &gadget(),
        &body(json!({ "id": "7", "displayName": "Widget A" })),
    )
    .unwrap();
    // keyed by PROPERTY, not param name:
    assert!(
        pairs
            .iter()
            .any(|(c, v)| c == "name" && *v == SqlValue::Text("Widget A".into()))
    );
    assert!(pairs.iter().all(|(c, _)| c != "displayName"));
}

#[test]
fn resolve_appends_constants() {
    let action = insert(
        vec![pb("id", "Long", true, None)],
        vec![Assignment::constant("status", json!("active"))],
    );
    let pairs = resolve_action_row(&action, &gadget(), &body(json!({ "id": "7" }))).unwrap();
    assert!(
        pairs
            .iter()
            .any(|(c, v)| c == "status" && *v == SqlValue::Text("active".into()))
    );
}

#[test]
fn resolve_back_compat_no_binds_no_constants() {
    let action = insert(
        vec![
            pb("id", "Long", true, None),
            pb("name", "String", false, None),
        ],
        vec![],
    );
    let pairs = resolve_action_row(&action, &gadget(), &body(json!({ "id": "7" }))).unwrap();
    assert_eq!(
        pairs
            .iter()
            .find(|(c, _)| c == "id")
            .map(|(_, v)| v.clone()),
        Some(SqlValue::Int(7))
    );
    // omitted optional `name` ⇒ Null pair (matches parse_params behavior).
    assert!(
        pairs
            .iter()
            .any(|(c, v)| c == "name" && *v == SqlValue::Null)
    );
}

#[test]
fn resolve_rejects_unknown_body_key() {
    let action = insert(vec![pb("id", "Long", true, None)], vec![]);
    assert!(matches!(
        resolve_action_row(&action, &gadget(), &body(json!({ "id": "7", "nope": "x" }))),
        Err(ParamError::Unknown(_))
    ));
}
