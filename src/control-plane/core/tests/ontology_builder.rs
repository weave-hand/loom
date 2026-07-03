//! The seed-builder DSL assembles structs identical to handwritten literals —
//! builder output == literal, field for field, including ordering.

use control_plane_core::{
    ActionDef, ActionKind, ActionName, Aggregation, Assignment, DerivedPropertyDef,
    LengthConstraint, ObjectType, ParamDef, PropertyConstraints, PropertyDef, TableRef, TypeName,
};

#[test]
fn object_type_builder_matches_literal() {
    let built = ObjectType::build("Widget", ("main", "widget"))
        .prop_req("id", "Long")
        .prop("name", "String")
        .prop("qty", "Long")
        .identity("id")
        .done();
    let literal = ObjectType {
        name: TypeName("Widget".into()),
        table: TableRef {
            schema: "main".into(),
            name: "widget".into(),
        },
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                constraints: PropertyConstraints::default(),
            },
            PropertyDef {
                name: "name".into(),
                ty: "String".into(),
                required: false,
                constraints: PropertyConstraints::default(),
            },
            PropertyDef {
                name: "qty".into(),
                ty: "Long".into(),
                required: false,
                constraints: PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        identity: Some("id".into()),
    };
    assert_eq!(built, literal);
}

#[test]
fn object_type_builder_defaults_are_empty() {
    let t = ObjectType::build("Bare", ("wh", "bare")).done();
    assert_eq!(t.properties, vec![]);
    assert_eq!(t.derived, vec![]);
    assert_eq!(t.identity, None, "no declared identity by default");
}

#[test]
fn object_type_builder_constraints_and_derived_hooks() {
    let constraints = PropertyConstraints {
        length: Some(LengthConstraint {
            min: Some(1),
            max: Some(8),
        }),
        ..PropertyConstraints::default()
    };
    let agg = DerivedPropertyDef {
        name: "order_count".into(),
        ty: "Long".into(),
        link: "orders".into(),
        agg: Aggregation::Count,
    };
    let t = ObjectType::build("Account", ("main", "account"))
        .prop_with("code", "String", true, constraints.clone())
        .derived(agg.clone())
        .done();
    assert_eq!(t.properties[0].constraints, constraints);
    assert!(t.properties[0].required);
    assert_eq!(t.derived, vec![agg]);
}

#[test]
fn action_def_builder_matches_literal() {
    let built = ActionDef::build("updateWidget", "Widget", ActionKind::Update)
        .param_req("id", "Long")
        .param_req("qty", "Long")
        .done();
    let literal = ActionDef::single_step(
        ActionName("updateWidget".into()),
        TypeName("Widget".into()),
        ActionKind::Update,
        vec![
            ParamDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                binds: None,
            },
            ParamDef {
                name: "qty".into(),
                ty: "Long".into(),
                required: true,
                binds: None,
            },
        ],
        vec![],
    );
    assert_eq!(built, literal);
}

#[test]
fn action_def_builder_binds_and_assignment_hooks() {
    let a = ActionDef::build("renameCustomer", "Customer", ActionKind::Update)
        .param_req("customerId", "Long")
        .param_bound("newName", "String", false, "name")
        .assign("status", serde_json::json!("active"))
        .done();
    assert_eq!(a.steps[0].parameters[0].binds, None);
    assert_eq!(a.steps[0].parameters[1].binds.as_deref(), Some("name"));
    assert!(!a.steps[0].parameters[1].required);
    assert_eq!(
        a.steps[0].assignments,
        vec![Assignment::constant("status", serde_json::json!("active"))]
    );
    assert_eq!(a.steps[0].kind, ActionKind::Update);
}
