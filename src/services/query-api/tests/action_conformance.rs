//! check_conformance validates that an ActionDef's parameters mirror its target ObjectType's
//! properties: every param names a real property of a compatible logical type (same BaseType),
//! and every required property is covered by a required param. Pure; collects ALL violations
//! into one ActionError::Misconfigured message.

use control_plane_core::{
    ActionDef, ActionKind, ActionName, ObjectType, ParamDef, PropertyDef, TableRef, TypeName,
};
use query_api::action::{ActionError, check_conformance};

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
        constraints: control_plane_core::PropertyConstraints::default(),
    }
}

fn param(name: &str, ty: &str, required: bool) -> ParamDef {
    ParamDef {
        name: name.into(),
        ty: ty.into(),
        required,
        binds: None,
    }
}

/// Widget: id (Long, required), name (String, optional).
fn widget(props: Vec<PropertyDef>) -> ObjectType {
    ObjectType {
        name: TypeName("Widget".into()),
        properties: props,
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "widget".into(),
        },
        identity: Some("id".into()),
    }
}

fn action(params: Vec<ParamDef>) -> ActionDef {
    ActionDef {
        name: ActionName("createWidget".into()),
        target: TypeName("Widget".into()),
        parameters: params,
        kind: ActionKind::Insert,
        assignments: vec![],
    }
}

fn msg(err: &ActionError) -> String {
    match err {
        ActionError::Misconfigured(m) => m.clone(),
        other => panic!("expected Misconfigured, got {other:?}"),
    }
}

#[test]
fn exact_mirror_conforms() {
    let target = widget(vec![
        prop("id", "Long", true),
        prop("name", "String", false),
    ]);
    let act = action(vec![
        param("id", "Long", true),
        param("name", "String", false),
    ]);
    assert!(check_conformance(&act, &target).is_ok());
}

#[test]
fn param_matching_no_property_is_rejected() {
    let target = widget(vec![prop("id", "Long", true)]);
    let act = action(vec![
        param("id", "Long", true),
        param("naem", "String", false),
    ]);
    let m = msg(&check_conformance(&act, &target).unwrap_err());
    assert!(
        m.contains("parameter `naem` matches no property of type `Widget`"),
        "got: {m}"
    );
}

#[test]
fn type_mismatch_is_rejected() {
    // Integer and Long are distinct base types (no widening).
    let target = widget(vec![prop("id", "Long", true)]);
    let act = action(vec![param("id", "Integer", true)]);
    let m = msg(&check_conformance(&act, &target).unwrap_err());
    assert!(
        m.contains("parameter `id` type `Integer` is incompatible with property `id` type `Long`"),
        "got: {m}"
    );
}

#[test]
fn unknown_param_type_is_rejected() {
    let target = widget(vec![prop("id", "Long", true)]);
    let act = action(vec![param("id", "Lng", true)]);
    let m = msg(&check_conformance(&act, &target).unwrap_err());
    assert!(
        m.contains("parameter `id` has unknown logical type `Lng`"),
        "got: {m}"
    );
}

#[test]
fn unknown_property_type_is_rejected() {
    let target = widget(vec![prop("id", "Lng", true)]);
    let act = action(vec![param("id", "Long", true)]);
    let m = msg(&check_conformance(&act, &target).unwrap_err());
    assert!(
        m.contains("property `id` of type `Widget` has unknown logical type `Lng`"),
        "got: {m}"
    );
}

#[test]
fn uncovered_required_property_is_rejected() {
    // name is required but no param covers it.
    let target = widget(vec![prop("id", "Long", true), prop("name", "String", true)]);
    let act = action(vec![param("id", "Long", true)]);
    let m = msg(&check_conformance(&act, &target).unwrap_err());
    assert!(
        m.contains("required property `name` of type `Widget` is not covered by any parameter"),
        "got: {m}"
    );
}

#[test]
fn required_property_covered_by_optional_param_is_rejected() {
    let target = widget(vec![prop("id", "Long", true)]);
    let act = action(vec![param("id", "Long", false)]);
    let m = msg(&check_conformance(&act, &target).unwrap_err());
    assert!(
        m.contains("required property `id` is covered by optional parameter `id`"),
        "got: {m}"
    );
}

#[test]
fn all_violations_are_collected() {
    // naem matches nothing AND required id is uncovered: both appear in one message.
    let target = widget(vec![prop("id", "Long", true)]);
    let act = action(vec![param("naem", "String", true)]);
    let m = msg(&check_conformance(&act, &target).unwrap_err());
    assert!(
        m.contains("parameter `naem` matches no property"),
        "got: {m}"
    );
    assert!(
        m.contains("required property `id` of type `Widget` is not covered"),
        "got: {m}"
    );
}

// --- Param->property mapping: binds + constant assignments (slice 1) ---

use control_plane_core::Assignment;

/// Gadget: id (Long, required), name (String, optional), status (String, optional).
fn gadget() -> ObjectType {
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

fn insert_action(params: Vec<ParamDef>, assignments: Vec<Assignment>) -> ActionDef {
    ActionDef {
        name: ActionName("a".into()),
        target: TypeName("Gadget".into()),
        parameters: params,
        kind: ActionKind::Insert,
        assignments,
    }
}

fn is_misconfigured(r: Result<(), ActionError>) -> bool {
    matches!(r, Err(ActionError::Misconfigured(_)))
}

#[test]
fn rename_and_constant_conform() {
    // `displayName` binds `name`; `id` covered by a required param; `status` filled by a constant.
    let a = insert_action(
        vec![
            pb("id", "Long", true, None),
            pb("displayName", "String", false, Some("name")),
        ],
        vec![Assignment::constant("status", serde_json::json!("active"))],
    );
    check_conformance(&a, &gadget()).expect("conforms");
}

#[test]
fn required_property_covered_by_constant_conforms() {
    // A REQUIRED property (`id`) covered ONLY by a constant is valid coverage.
    let a = insert_action(
        vec![],
        vec![Assignment::constant("id", serde_json::json!("7"))],
    );
    check_conformance(&a, &gadget()).expect("constant covers required");
}

#[test]
fn binds_unknown_property_rejected() {
    let a = insert_action(
        vec![
            pb("id", "Long", true, None),
            pb("x", "String", false, Some("nope")),
        ],
        vec![],
    );
    assert!(is_misconfigured(check_conformance(&a, &gadget())));
}

#[test]
fn constant_unknown_property_rejected() {
    let a = insert_action(
        vec![pb("id", "Long", true, None)],
        vec![Assignment::constant("nope", serde_json::json!("x"))],
    );
    assert!(is_misconfigured(check_conformance(&a, &gadget())));
}

#[test]
fn constant_type_mismatch_rejected() {
    // `status` is String; a bool constant is incompatible.
    let a = insert_action(
        vec![pb("id", "Long", true, None)],
        vec![Assignment::constant("status", serde_json::json!(true))],
    );
    assert!(is_misconfigured(check_conformance(&a, &gadget())));
}

#[test]
fn double_bind_param_and_constant_rejected() {
    // `name` bound by both a param and a constant.
    let a = insert_action(
        vec![
            pb("id", "Long", true, None),
            pb("name", "String", false, None),
        ],
        vec![Assignment::constant("name", serde_json::json!("x"))],
    );
    assert!(is_misconfigured(check_conformance(&a, &gadget())));
}

#[test]
fn double_bind_two_params_rejected() {
    let a = insert_action(
        vec![
            pb("id", "Long", true, None),
            pb("a", "String", false, Some("name")),
            pb("b", "String", false, Some("name")),
        ],
        vec![],
    );
    assert!(is_misconfigured(check_conformance(&a, &gadget())));
}

#[test]
fn uncovered_required_property_rejected() {
    // `id` (required) covered by neither a param nor a constant.
    let a = insert_action(
        vec![pb("displayName", "String", false, Some("name"))],
        vec![],
    );
    assert!(is_misconfigured(check_conformance(&a, &gadget())));
}
