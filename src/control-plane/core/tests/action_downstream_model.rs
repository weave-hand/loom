use control_plane_core::{ActionDef, ActionKind, ActionName, JobTemplate};
use serde_json::json;

fn tn(s: &str) -> control_plane_core::TypeName {
    control_plane_core::TypeName(s.into())
}

#[test]
fn downstream_round_trips_through_serde() {
    let action = ActionDef::single_step(
        ActionName("createOrder".into()),
        tn("Order"),
        ActionKind::Insert,
        vec![],
        vec![],
    );
    // builder path
    let action = action.downstream(vec![JobTemplate {
        kind: "transform".into(),
        payload: json!({ "orderId": "@self.id" }),
    }]);
    let json = serde_json::to_string(&action).unwrap();
    let back: ActionDef = serde_json::from_str(&json).unwrap();
    assert_eq!(back.downstream.len(), 1);
    assert_eq!(back.downstream[0].kind, "transform");
    assert_eq!(back.downstream[0].payload, json!({ "orderId": "@self.id" }));
}

#[test]
fn legacy_action_without_downstream_deserializes_to_empty() {
    // A pre-slice-4 serialized action has no `downstream` key.
    let legacy =
        r#"{"name":"oldAct","target":"Widget","kind":"insert","parameters":[],"assignments":[]}"#;
    let back: ActionDef = serde_json::from_str(legacy).unwrap();
    assert!(
        back.downstream.is_empty(),
        "legacy action gets empty downstream"
    );
}
