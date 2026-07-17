use control_plane_core::{ActionDef, ActionKind, ActionName, Assignment, ParamDef, TypeName};

fn param(name: &str, ty: &str, binds: Option<&str>) -> ParamDef {
    let p = ParamDef::new(name, ty).required();
    match binds {
        Some(b) => p.binds(b),
        None => p,
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
    let a = ActionDef::single_step(
        ActionName("createGadget".into()),
        TypeName("Gadget".into()),
        ActionKind::Insert,
        vec![param("displayName", "String", Some("name"))],
        vec![Assignment::constant("status", serde_json::json!("active"))],
    );
    let json = serde_json::to_string(&a).expect("serialize");
    let back: ActionDef = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, a, "ActionDef round-trips with binds + assignments");
}

#[test]
fn back_compat_action_has_empty_mapping() {
    // An action whose params are named for their properties and carries no constants:
    // binds is None and assignments is empty — the byte-for-byte-unchanged shape.
    let a = ActionDef::single_step(
        ActionName("createWidget".into()),
        TypeName("Widget".into()),
        ActionKind::Insert,
        vec![param("id", "Long", None)],
        vec![],
    );
    assert!(a.steps[0].assignments.is_empty());
    assert_eq!(a.steps[0].parameters[0].binds, None);
    assert_eq!(a.steps[0].parameters[0].binds_property(), "id");
}
