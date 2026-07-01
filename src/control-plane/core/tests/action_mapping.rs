use control_plane_core::{ActionDef, ActionKind, ActionName, ConstAssignment, ParamDef, TypeName};

fn param(name: &str, ty: &str, binds: Option<&str>) -> ParamDef {
    ParamDef {
        name: name.into(),
        ty: ty.into(),
        required: true,
        binds: binds.map(str::to_string),
    }
}

#[test]
fn param_binds_property_defaults_to_name() {
    assert_eq!(
        param("displayName", "String", None).binds_property(),
        "displayName"
    );
    assert_eq!(
        param("displayName", "String", Some("name")).binds_property(),
        "name"
    );
}

#[test]
fn action_def_carries_binds_and_assignments_through_serde() {
    let a = ActionDef {
        name: ActionName("createGadget".into()),
        target: TypeName("Gadget".into()),
        parameters: vec![param("displayName", "String", Some("name"))],
        kind: ActionKind::Insert,
        assignments: vec![ConstAssignment {
            property: "status".into(),
            value: serde_json::json!("active"),
        }],
    };
    let json = serde_json::to_string(&a).expect("serialize");
    let back: ActionDef = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, a, "ActionDef round-trips with binds + assignments");
}

#[test]
fn back_compat_action_has_empty_mapping() {
    // An action whose params are named for their properties and carries no constants:
    // binds is None and assignments is empty — the byte-for-byte-unchanged shape.
    let a = ActionDef {
        name: ActionName("createWidget".into()),
        target: TypeName("Widget".into()),
        parameters: vec![param("id", "Long", None)],
        kind: ActionKind::Insert,
        assignments: vec![],
    };
    assert!(a.assignments.is_empty());
    assert_eq!(a.parameters[0].binds, None);
    assert_eq!(a.parameters[0].binds_property(), "id");
}
