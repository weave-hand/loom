use control_plane_core::ParamDef;
use query_api::params::{ParamError, parse_params};
use query_api::serving::SqlValue;
use serde_json::json;

fn p(name: &str, ty: &str, required: bool) -> ParamDef {
    let pd = ParamDef::new(name, ty);
    if required { pd.required() } else { pd }
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

use control_plane_core::{ActionKind, ActionStep, Assignment, ObjectType, PropertyDef, TypeName};
use query_api::params::{StepEnv, resolve_action_row};

fn gadget() -> ObjectType {
    let prop = |name: &str, ty: &str, required: bool| {
        let p = PropertyDef::new(name, ty);
        if required { p.required() } else { p }
    };
    ObjectType::build("Gadget", ("main", "gadget"))
        .add_prop(prop("id", "Long", true))
        .add_prop(prop("name", "String", false))
        .add_prop(prop("status", "String", false))
        .done()
}

fn pb(name: &str, ty: &str, required: bool, binds: Option<&str>) -> ParamDef {
    let mut p = ParamDef::new(name, ty);
    if required {
        p = p.required();
    }
    if let Some(b) = binds {
        p = p.binds(b);
    }
    p
}

// The single step under test. `resolve_action_row` now takes one `ActionStep` (the sole step of
// a single-step action, or one step of a multi-step action); these tests exercise that step.
fn insert(params: Vec<ParamDef>, assignments: Vec<Assignment>) -> ActionStep {
    ActionStep {
        target: TypeName("Gadget".into()),
        kind: ActionKind::Insert,
        parameters: params,
        assignments,
        bind: None,
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
        now(),
        &StepEnv::new(),
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
    let pairs = resolve_action_row(
        &action,
        &gadget(),
        &body(json!({ "id": "7" })),
        now(),
        &StepEnv::new(),
    )
    .unwrap();
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
    let pairs = resolve_action_row(
        &action,
        &gadget(),
        &body(json!({ "id": "7" })),
        now(),
        &StepEnv::new(),
    )
    .unwrap();
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
        resolve_action_row(
            &action,
            &gadget(),
            &body(json!({ "id": "7", "nope": "x" })),
            now(),
            &StepEnv::new(),
        ),
        Err(ParamError::Unknown(_))
    ));
}

// --- resolve_action_row: computed `Expr` assignments (slice 2) ---

fn now() -> time::PrimitiveDateTime {
    time::PrimitiveDateTime::new(
        time::Date::from_calendar_date(2026, time::Month::July, 3).unwrap(),
        time::Time::from_hms(9, 0, 0).unwrap(),
    )
}

/// `gadget()` + a `total: Double` property, for computed-assignment tests.
fn gadget_with_total() -> ObjectType {
    let mut g = gadget();
    g.properties.push(PropertyDef::new("total", "Double"));
    g
}

#[test]
fn computed_expression_writes_value() {
    // total = id + 1 : Long(8), assignable to the Double `total` property.
    let action = insert(
        vec![pb("id", "Long", true, None)],
        vec![Assignment::expr("total", "id + 1")],
    );
    let pairs = resolve_action_row(
        &action,
        &gadget_with_total(),
        &body(json!({ "id": "7" })),
        now(),
        &StepEnv::new(),
    )
    .unwrap();
    let total = pairs
        .iter()
        .find(|(c, _)| c == "total")
        .map(|(_, v)| v.clone());
    assert_eq!(total, Some(SqlValue::Int(8)));
}

#[test]
fn computed_now_uses_injected_clock() {
    // createdAt = now() lands exactly the injected clock (deterministic).
    let mut g = gadget();
    g.properties
        .push(PropertyDef::new("createdAt", "Timestamp"));
    let action = insert(
        vec![pb("id", "Long", true, None)],
        vec![Assignment::expr("createdAt", "now()")],
    );
    let pairs = resolve_action_row(
        &action,
        &g,
        &body(json!({ "id": "1" })),
        now(),
        &StepEnv::new(),
    )
    .unwrap();
    let created = pairs
        .iter()
        .find(|(c, _)| c == "createdAt")
        .map(|(_, v)| v.clone());
    assert_eq!(created, Some(SqlValue::Timestamp(now())));
}

#[test]
fn computed_runtime_fault_is_bad_value() {
    let action = insert(
        vec![pb("id", "Long", true, None)],
        vec![Assignment::expr("total", "id / 0")],
    );
    let err = resolve_action_row(
        &action,
        &gadget_with_total(),
        &body(json!({ "id": "7" })),
        now(),
        &StepEnv::new(),
    )
    .unwrap_err();
    assert!(matches!(err, ParamError::BadValue(_, _)));
}
