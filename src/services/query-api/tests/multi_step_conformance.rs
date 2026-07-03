//! Multi-step define-time conformance + cross-step reference (`StepRef`) validation.
//!
//! `check_conformance_steps` iterates an action's steps in order, running the single-step
//! checks against each step's own target and threading a growing set of `bind -> that step's
//! target property-set`. A `StepRef { bind, prop }` assignment is valid only when `bind` names a
//! *strictly earlier* step's bind and `prop` is a real property of that step's target. This
//! test pins spec test-2's five rejection cases + the valid backward reference.

use control_plane_core::{
    ActionDef, ActionKind, ActionName, ActionStep, Assignment, ObjectType, ParamDef, TypeName,
};
use query_api::action::{ActionError, check_conformance_steps};

fn tn(s: &str) -> TypeName {
    TypeName(s.into())
}

fn param(name: &str, ty: &str, required: bool) -> ParamDef {
    ParamDef {
        name: name.into(),
        ty: ty.into(),
        required,
        binds: None,
    }
}

fn param_bound(name: &str, ty: &str, required: bool, binds: &str) -> ParamDef {
    ParamDef {
        name: name.into(),
        ty: ty.into(),
        required,
        binds: Some(binds.into()),
    }
}

/// Order: id (Long, required, identity), note (String, optional).
fn order() -> ObjectType {
    ObjectType::build("Order", ("main", "order"))
        .prop_req("id", "Long")
        .prop("note", "String")
        .identity("id")
        .done()
}

/// LineItem: orderId (Long, required), sku (String, required).
fn line_item() -> ObjectType {
    ObjectType::build("LineItem", ("main", "line_item"))
        .prop_req("orderId", "Long")
        .prop_req("sku", "String")
        .done()
}

/// A two-step action: step0 inserts an Order (bound `order`), step1 inserts a LineItem whose
/// non-`sku` assignments are supplied by the caller. Both steps otherwise conform, so any
/// rejection isolates the assignment(s) under test.
fn action(step0_assignments: Vec<Assignment>, li_assignments: Vec<Assignment>) -> ActionDef {
    ActionDef {
        name: ActionName("createOrderWithLine".into()),
        steps: vec![
            ActionStep {
                target: tn("Order"),
                kind: ActionKind::Insert,
                parameters: vec![param_bound("orderId", "Long", true, "id")],
                assignments: step0_assignments,
                bind: Some("order".into()),
            },
            ActionStep {
                target: tn("LineItem"),
                kind: ActionKind::Insert,
                parameters: vec![param("sku", "String", true)],
                assignments: li_assignments,
                bind: Some("li".into()),
            },
        ],
    }
}

fn targets() -> Vec<ObjectType> {
    vec![order(), line_item()]
}

fn is_misconfigured(r: Result<(), ActionError>) -> bool {
    matches!(r, Err(ActionError::Misconfigured(_)))
}

fn err_msg(r: Result<(), ActionError>) -> String {
    match r {
        Err(ActionError::Misconfigured(m)) => m,
        other => panic!("expected Misconfigured, got {other:?}"),
    }
}

// The valid case: LineItem.orderId = @order.id — a backward reference to a strictly earlier,
// bound step whose target really has property `id`.
#[test]
fn valid_backward_ref_conforms() {
    let a = action(vec![], vec![Assignment::step_ref("orderId", "order", "id")]);
    assert!(
        check_conformance_steps(&a, &targets()).is_ok(),
        "backward @order.id ref should conform"
    );
}

// Case 1: a StepRef naming a LATER step's bind (`li` is step1's bind, referenced from step0).
#[test]
fn forward_ref_is_rejected() {
    let a = action(
        vec![Assignment::step_ref("note", "li", "sku")],
        vec![Assignment::step_ref("orderId", "order", "id")],
    );
    let m = err_msg(check_conformance_steps(&a, &targets()));
    assert!(
        m.contains("li"),
        "forward ref to a later step's bind should be rejected, got: {m}"
    );
}

// Case 2: a StepRef naming the step's OWN bind (self-reference; `li` is step1's own bind).
#[test]
fn self_ref_is_rejected() {
    let a = action(
        vec![],
        vec![Assignment::step_ref("orderId", "li", "orderId")],
    );
    assert!(
        is_misconfigured(check_conformance_steps(&a, &targets())),
        "self-reference should be rejected"
    );
}

// Case 3: a StepRef naming an UNBOUND step (no step declares bind `ghost`).
#[test]
fn unbound_ref_is_rejected() {
    let a = action(vec![], vec![Assignment::step_ref("orderId", "ghost", "id")]);
    assert!(
        is_misconfigured(check_conformance_steps(&a, &targets())),
        "reference to an unbound step should be rejected"
    );
}

// Case 4: a StepRef whose `prop` is NOT a property of the bound step's target
// (Order has no `nonesuch`).
#[test]
fn ref_to_unknown_property_is_rejected() {
    let a = action(
        vec![],
        vec![Assignment::step_ref("orderId", "order", "nonesuch")],
    );
    let m = err_msg(check_conformance_steps(&a, &targets()));
    assert!(
        m.contains("nonesuch"),
        "reference to a non-property of the bound step should be rejected, got: {m}"
    );
}

// I-2 guard: an Update/Delete step whose target table is ALSO written by another step is
// rejected at define time. A multi-step Overwrite reads the table's pre-action committed state,
// so a sibling write to the same table would be a lost update / spurious NotFound. Here step0
// inserts an Order and step1 DELETEs the same table.
#[test]
fn same_table_update_delete_alongside_another_step_is_rejected() {
    let a = ActionDef {
        name: ActionName("badSameTable".into()),
        steps: vec![
            ActionStep {
                target: tn("Order"),
                kind: ActionKind::Insert,
                parameters: vec![param_bound("newId", "Long", true, "id")],
                assignments: vec![],
                bind: Some("order".into()),
            },
            ActionStep {
                target: tn("Order"),
                kind: ActionKind::Delete,
                parameters: vec![param_bound("delId", "Long", true, "id")],
                assignments: vec![],
                bind: None,
            },
        ],
    };
    let m = err_msg(check_conformance_steps(&a, &[order(), order()]));
    assert!(
        m.contains("Update/Delete"),
        "an Update/Delete step sharing a table with another step should be rejected, got: {m}"
    );
}

// The allowed counterpart: two Insert steps to the SAME table coalesce as appends — the guard
// must NOT fire for Insert+Insert.
#[test]
fn same_table_two_inserts_is_allowed() {
    let a = ActionDef {
        name: ActionName("twoInserts".into()),
        steps: vec![
            ActionStep {
                target: tn("Order"),
                kind: ActionKind::Insert,
                parameters: vec![param_bound("id1", "Long", true, "id")],
                assignments: vec![],
                bind: None,
            },
            ActionStep {
                target: tn("Order"),
                kind: ActionKind::Insert,
                parameters: vec![param_bound("id2", "Long", true, "id")],
                assignments: vec![],
                bind: None,
            },
        ],
    };
    assert!(
        check_conformance_steps(&a, &[order(), order()]).is_ok(),
        "two Insert steps to one table should be allowed (they coalesce as appends)"
    );
}

// Case 5: a property double-bound within ONE step (a param and a constant both write `id`).
// The existing intra-step "no double-write" rule must fire per step.
#[test]
fn intra_step_double_write_is_rejected() {
    // step0 already has a param binding `id`; add a constant also writing `id`.
    let a = action(
        vec![Assignment::constant("id", serde_json::json!("7"))],
        vec![Assignment::step_ref("orderId", "order", "id")],
    );
    let m = err_msg(check_conformance_steps(&a, &targets()));
    assert!(
        m.contains("more than one"),
        "property written by both a param and a constant should be rejected, got: {m}"
    );
}
