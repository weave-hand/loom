use control_plane_core::{ActionDef, ActionKind, ActionName, TypeName};

#[test]
fn action_kind_defaults_to_insert() {
    assert_eq!(ActionKind::default(), ActionKind::Insert);
}

#[test]
fn action_def_carries_kind() {
    let a = ActionDef {
        name: ActionName("a".into()),
        target: TypeName("T".into()),
        parameters: vec![],
        kind: ActionKind::Delete,
        assignments: vec![],
    };
    assert_eq!(a.kind, ActionKind::Delete);
}
