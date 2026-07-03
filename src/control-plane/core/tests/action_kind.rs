use control_plane_core::{ActionDef, ActionKind, ActionName, TypeName};

#[test]
fn action_kind_defaults_to_insert() {
    assert_eq!(ActionKind::default(), ActionKind::Insert);
}

#[test]
fn action_def_carries_kind() {
    let a = ActionDef::single_step(
        ActionName("a".into()),
        TypeName("T".into()),
        ActionKind::Delete,
        vec![],
        vec![],
    );
    assert_eq!(a.steps[0].kind, ActionKind::Delete);
}
