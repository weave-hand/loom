//! check_conformance UPDATE/DELETE rules:
//! – both require a declared identity on the target type;
//! – both require a required parameter naming the identity property;
//! – DELETE takes ONLY the identity parameter (no extras);
//! – UPDATE relaxes required-property coverage (PATCH semantics).

use control_plane_core::{
    ActionDef, ActionKind, ActionName, ObjectType, ParamDef, PropertyDef, TableRef, TypeName,
};
use query_api::action::check_conformance;

fn widget(identity: Option<&str>) -> ObjectType {
    ObjectType {
        name: TypeName("Widget".into()),
        properties: vec![
            PropertyDef {
                name: "sku".into(),
                ty: "String".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "qty".into(),
                ty: "Long".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        table: TableRef {
            schema: "s".into(),
            name: "widget".into(),
        },
        identity: identity.map(|s| s.to_string()),
        version: None,
    }
}

#[test]
fn delete_requires_identity_param_only() {
    let action = ActionDef::single_step(
        ActionName("del".into()),
        TypeName("Widget".into()),
        ActionKind::Delete,
        vec![ParamDef {
            name: "sku".into(),
            ty: "String".into(),
            required: true,
            binds: None,
        }],
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
            ParamDef {
                name: "sku".into(),
                ty: "String".into(),
                required: true,
                binds: None,
            },
            ParamDef {
                name: "qty".into(),
                ty: "Long".into(),
                required: false,
                binds: None,
            },
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
        vec![ParamDef {
            name: "sku".into(),
            ty: "String".into(),
            required: true,
            binds: None,
        }],
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
            ParamDef {
                name: "sku".into(),
                ty: "String".into(),
                required: true,
                binds: None,
            },
            ParamDef {
                name: "qty".into(),
                ty: "Long".into(),
                required: false,
                binds: None,
            },
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
            ParamDef {
                name: "key".into(),
                ty: "String".into(),
                required: true,
                binds: Some("sku".into()),
            },
            ParamDef {
                name: "quantity".into(),
                ty: "Long".into(),
                required: false,
                binds: Some("qty".into()),
            },
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
        vec![ParamDef {
            name: "key".into(),
            ty: "String".into(),
            required: true,
            binds: Some("sku".into()),
        }],
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
        vec![ParamDef {
            name: "key".into(),
            ty: "String".into(),
            required: true,
            binds: Some("sku".into()),
        }],
        vec![Assignment::constant("qty", serde_json::json!(1))],
    );
    assert!(matches!(
        check_conformance(&action, &widget(Some("sku"))),
        Err(ActionError::Misconfigured(_))
    ));
}
