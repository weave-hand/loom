//! check_conformance UPDATE/DELETE rules:
//! – both require a declared identity on the target type;
//! – both require a required parameter naming the identity property;
//! – DELETE takes ONLY the identity parameter (no extras);
//! – UPDATE relaxes required-property coverage (PATCH semantics).

use control_plane_core::{
    ActionDef, ActionKind, ActionName, ObjectType, ParamDef, PropertyDef, TypeName,
};
use query_api::action::check_conformance;

fn widget(identity: Option<&str>) -> ObjectType {
    let b = ObjectType::build("Widget", ("s", "widget"))
        .add_prop(PropertyDef::new("sku", "String").required())
        .add_prop(PropertyDef::new("qty", "Long"));
    match identity {
        Some(id) => b.identity(id).done(),
        None => b.done(),
    }
}

#[test]
fn delete_requires_identity_param_only() {
    let action = ActionDef::single_step(
        ActionName("del".into()),
        TypeName("Widget".into()),
        ActionKind::Delete,
        vec![ParamDef::new("sku", "String").required()],
        vec![],
    );
    assert!(check_conformance(&action, &widget(Some("sku"))).is_ok());
}

#[test]
fn delete_rejects_extra_params() {
    let action = ActionDef::single_step(
        ActionName("del".into()),
        TypeName("Widget".into()),
        ActionKind::Delete,
        vec![
            ParamDef::new("sku", "String").required(),
            ParamDef::new("qty", "Long"),
        ],
        vec![],
    );
    assert!(check_conformance(&action, &widget(Some("sku"))).is_err());
}

#[test]
fn mutate_requires_declared_identity() {
    let action = ActionDef::single_step(
        ActionName("del".into()),
        TypeName("Widget".into()),
        ActionKind::Delete,
        vec![ParamDef::new("sku", "String").required()],
        vec![],
    );
    assert!(check_conformance(&action, &widget(None)).is_err());
}

#[test]
fn update_allows_partial_columns() {
    // identity "sku" + one mutable column "qty"; required prop coverage relaxed for PATCH.
    let action = ActionDef::single_step(
        ActionName("up".into()),
        TypeName("Widget".into()),
        ActionKind::Update,
        vec![
            ParamDef::new("sku", "String").required(),
            ParamDef::new("qty", "Long"),
        ],
        vec![],
    );
    assert!(check_conformance(&action, &widget(Some("sku"))).is_ok());
}

// --- UPDATE/DELETE param->property mapping (binds + assignments) ---

use control_plane_core::Assignment;
use query_api::action::ActionError;

#[test]
fn update_identity_via_binds_conforms() {
    // Identity `sku` is bound by a required param renamed to `key`; `quantity` renames `qty`.
    let action = ActionDef::single_step(
        ActionName("upd".into()),
        TypeName("Widget".into()),
        ActionKind::Update,
        vec![
            ParamDef::new("key", "String").required().binds("sku"),
            ParamDef::new("quantity", "Long").binds("qty"),
        ],
        vec![],
    );
    check_conformance(&action, &widget(Some("sku"))).expect("update conforms via binds");
}

#[test]
fn delete_identity_via_binds_conforms() {
    // Delete's sole param renames the identity.
    let action = ActionDef::single_step(
        ActionName("del".into()),
        TypeName("Widget".into()),
        ActionKind::Delete,
        vec![ParamDef::new("key", "String").required().binds("sku")],
        vec![],
    );
    check_conformance(&action, &widget(Some("sku"))).expect("delete conforms via binds");
}

#[test]
fn delete_with_assignment_rejected() {
    // Delete takes only the identity param; a constant assignment is a misconfiguration.
    let action = ActionDef::single_step(
        ActionName("del".into()),
        TypeName("Widget".into()),
        ActionKind::Delete,
        vec![ParamDef::new("key", "String").required().binds("sku")],
        vec![Assignment::constant("qty", serde_json::json!(1))],
    );
    assert!(matches!(
        check_conformance(&action, &widget(Some("sku"))),
        Err(ActionError::Misconfigured(_))
    ));
}
