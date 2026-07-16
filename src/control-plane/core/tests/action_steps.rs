use control_plane_core::{
    ActionDef, ActionKind, ActionName, ActionStep, Assignment, ParamDef, TypeName,
};

fn tn(s: &str) -> TypeName {
    TypeName(s.into())
}

// A legacy single-step action serializes to the flat, pre-steps JSON (byte-compat).
#[test]
fn single_step_serializes_flat() {
    let a = ActionDef::single_step(
        ActionName("createWidget".into()),
        tn("Widget"),
        ActionKind::Insert,
        vec![ParamDef::new("id", "Long").required()],
        vec![Assignment::constant("status", serde_json::json!("active"))],
    );
    let v = serde_json::to_value(&a).expect("serialize");
    assert!(
        v.get("target").is_some(),
        "flat form carries top-level target"
    );
    assert!(v.get("steps").is_none(), "flat form has no steps key");
    // Round-trips through the flat form.
    let back: ActionDef = serde_json::from_value(v).expect("deserialize flat");
    assert_eq!(back, a);
}

// A multi-step action serializes to the stepped form and round-trips.
#[test]
fn multi_step_round_trips() {
    let a = ActionDef {
        name: ActionName("createOrderWithLines".into()),
        steps: vec![
            ActionStep {
                target: tn("Order"),
                kind: ActionKind::Insert,
                parameters: vec![ParamDef {
                    name: "orderId".into(),
                    ty: "Long".into(),
                    required: true,
                    binds: Some("id".into()),
                }],
                assignments: vec![],
                bind: Some("order".into()),
            },
            ActionStep {
                target: tn("LineItem"),
                kind: ActionKind::Insert,
                parameters: vec![ParamDef {
                    name: "sku".into(),
                    ty: "String".into(),
                    required: true,
                    binds: None,
                }],
                // A cross-step reference: LineItem.orderId = @order.id.
                assignments: vec![Assignment::step_ref("orderId", "order", "id")],
                bind: None,
            },
        ],
        downstream: Vec::new(),
    };
    let v = serde_json::to_value(&a).expect("serialize");
    assert!(v.get("steps").is_some(), "stepped form carries steps");
    let back: ActionDef = serde_json::from_value(v).expect("deserialize stepped");
    assert_eq!(back, a);
}

// The fluent builder produces the same multi-step action (with a cross-step ref)
// as the struct-literal form — the ergonomic define path for docs/callers.
#[test]
fn builder_defines_multi_step_with_cross_step_ref() {
    let built = ActionDef::build("createOrderWithLines", "Order", ActionKind::Insert)
        .param_bound("orderId", "Long", true, "id")
        .bind("order")
        .step("LineItem", ActionKind::Insert)
        .param_req("sku", "String")
        .assign_step_ref("orderId", "order", "id")
        .done();

    let expected = ActionDef {
        name: ActionName("createOrderWithLines".into()),
        steps: vec![
            ActionStep {
                target: tn("Order"),
                kind: ActionKind::Insert,
                parameters: vec![ParamDef {
                    name: "orderId".into(),
                    ty: "Long".into(),
                    required: true,
                    binds: Some("id".into()),
                }],
                assignments: vec![],
                bind: Some("order".into()),
            },
            ActionStep {
                target: tn("LineItem"),
                kind: ActionKind::Insert,
                parameters: vec![ParamDef {
                    name: "sku".into(),
                    ty: "String".into(),
                    required: true,
                    binds: None,
                }],
                assignments: vec![Assignment::step_ref("orderId", "order", "id")],
                bind: None,
            },
        ],
        downstream: Vec::new(),
    };
    assert_eq!(built, expected);
}

// Legacy flat JSON (no `steps` key) still deserializes into one implicit step.
#[test]
fn legacy_flat_json_lifts_to_one_step() {
    let json = serde_json::json!({
        "name": "createWidget",
        "target": "Widget",
        "kind": "insert",
        "parameters": [{ "name": "id", "ty": "Long", "required": true }],
        "assignments": []
    });
    let a: ActionDef = serde_json::from_value(json).expect("deserialize legacy flat");
    assert_eq!(a.steps.len(), 1);
    assert_eq!(a.steps[0].target, tn("Widget"));
    assert_eq!(a.steps[0].bind, None);
}
