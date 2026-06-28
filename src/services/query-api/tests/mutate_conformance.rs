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
            },
            PropertyDef {
                name: "qty".into(),
                ty: "Long".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: TableRef {
            schema: "s".into(),
            name: "widget".into(),
        },
        identity: identity.map(|s| s.to_string()),
    }
}

#[test]
fn delete_requires_identity_param_only() {
    let action = ActionDef {
        name: ActionName("del".into()),
        target: TypeName("Widget".into()),
        parameters: vec![ParamDef {
            name: "sku".into(),
            ty: "String".into(),
            required: true,
        }],
        kind: ActionKind::Delete,
    };
    assert!(check_conformance(&action, &widget(Some("sku"))).is_ok());
}

#[test]
fn delete_rejects_extra_params() {
    let action = ActionDef {
        name: ActionName("del".into()),
        target: TypeName("Widget".into()),
        parameters: vec![
            ParamDef {
                name: "sku".into(),
                ty: "String".into(),
                required: true,
            },
            ParamDef {
                name: "qty".into(),
                ty: "Long".into(),
                required: false,
            },
        ],
        kind: ActionKind::Delete,
    };
    assert!(check_conformance(&action, &widget(Some("sku"))).is_err());
}

#[test]
fn mutate_requires_declared_identity() {
    let action = ActionDef {
        name: ActionName("del".into()),
        target: TypeName("Widget".into()),
        parameters: vec![ParamDef {
            name: "sku".into(),
            ty: "String".into(),
            required: true,
        }],
        kind: ActionKind::Delete,
    };
    assert!(check_conformance(&action, &widget(None)).is_err());
}

#[test]
fn update_allows_partial_columns() {
    // identity "sku" + one mutable column "qty"; required prop coverage relaxed for PATCH.
    let action = ActionDef {
        name: ActionName("up".into()),
        target: TypeName("Widget".into()),
        parameters: vec![
            ParamDef {
                name: "sku".into(),
                ty: "String".into(),
                required: true,
            },
            ParamDef {
                name: "qty".into(),
                ty: "Long".into(),
                required: false,
            },
        ],
        kind: ActionKind::Update,
    };
    assert!(check_conformance(&action, &widget(Some("sku"))).is_ok());
}
