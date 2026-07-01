//! Unit tests for the per-value constraint validator and the define-time declaration check.

use control_plane_core::{
    ConstraintRule, LengthConstraint, ObjectType, PropertyConstraints, PropertyDef,
    PropertyValidator, RangeConstraint, TableRef, TypeName, validate_constraints,
};

fn sprop(name: &str, ty: &str, c: PropertyConstraints) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required: false,
        constraints: c,
    }
}

fn vstr(c: &PropertyConstraints, v: &str) -> Vec<ConstraintRule> {
    let validator = PropertyValidator::from_parts("p", c).expect("compiles");
    let mut out = Vec::new();
    validator.check_str(v, &mut out);
    out.into_iter().map(|x| x.rule).collect()
}

fn vnum(c: &PropertyConstraints, v: f64) -> Vec<ConstraintRule> {
    let validator = PropertyValidator::from_parts("p", c).expect("compiles");
    let mut out = Vec::new();
    validator.check_num(v, &mut out);
    out.into_iter().map(|x| x.rule).collect()
}

#[test]
fn range_below_and_above_fail_inside_passes() {
    let c = PropertyConstraints {
        range: Some(RangeConstraint {
            min: Some(1.0),
            max: Some(10.0),
        }),
        ..PropertyConstraints::default()
    };
    assert_eq!(vnum(&c, 0.5), vec![ConstraintRule::Range]);
    assert_eq!(vnum(&c, 10.5), vec![ConstraintRule::Range]);
    assert!(vnum(&c, 5.0).is_empty());
    assert!(vnum(&c, 1.0).is_empty(), "min is inclusive");
    assert!(vnum(&c, 10.0).is_empty(), "max is inclusive");
}

#[test]
fn length_under_and_over_fail_inside_passes() {
    let c = PropertyConstraints {
        length: Some(LengthConstraint {
            min: Some(2),
            max: Some(4),
        }),
        ..PropertyConstraints::default()
    };
    assert_eq!(vstr(&c, "a"), vec![ConstraintRule::Length]);
    assert_eq!(vstr(&c, "abcde"), vec![ConstraintRule::Length]);
    assert!(vstr(&c, "abc").is_empty());
    assert!(vstr(&c, "abcd").is_empty());
}

#[test]
fn pattern_match_and_no_match() {
    let c = PropertyConstraints {
        pattern: Some(r"^\d{3}$".into()),
        ..PropertyConstraints::default()
    };
    assert!(vstr(&c, "123").is_empty());
    assert_eq!(vstr(&c, "12a"), vec![ConstraintRule::Pattern]);
}

#[test]
fn one_of_in_and_out_of_set() {
    let c = PropertyConstraints {
        one_of: Some(vec!["a".into(), "b".into()]),
        ..PropertyConstraints::default()
    };
    assert!(vstr(&c, "a").is_empty());
    assert_eq!(vstr(&c, "z"), vec![ConstraintRule::OneOf]);
}

#[test]
fn multiple_violations_on_one_value_aggregate() {
    let c = PropertyConstraints {
        length: Some(LengthConstraint {
            min: Some(5),
            max: None,
        }),
        pattern: Some(r"^\d+$".into()),
        ..PropertyConstraints::default()
    };
    // "ab" is too short AND not all-digits → both rules fire.
    let rules = vstr(&c, "ab");
    assert!(rules.contains(&ConstraintRule::Length));
    assert!(rules.contains(&ConstraintRule::Pattern));
    assert_eq!(rules.len(), 2);
}

#[test]
fn empty_constraints_is_noop() {
    let c = PropertyConstraints::default();
    assert!(c.is_empty());
    let validator = PropertyValidator::from_parts("p", &c).expect("compiles");
    assert!(validator.is_noop());
    assert!(vstr(&c, "anything").is_empty());
    assert!(vnum(&c, 999.0).is_empty());
}

#[test]
fn declaration_rejects_range_on_string() {
    let props = vec![sprop(
        "name",
        "String",
        PropertyConstraints {
            range: Some(RangeConstraint {
                min: Some(0.0),
                max: None,
            }),
            ..PropertyConstraints::default()
        },
    )];
    let err = validate_constraints(&props).expect_err("range on string rejected");
    assert!(
        err.to_string().contains("range"),
        "message names the rule: {err}"
    );
}

#[test]
fn declaration_rejects_length_on_numeric() {
    let props = vec![sprop(
        "age",
        "Long",
        PropertyConstraints {
            length: Some(LengthConstraint {
                min: Some(1),
                max: None,
            }),
            ..PropertyConstraints::default()
        },
    )];
    validate_constraints(&props).expect_err("length on numeric rejected");
}

#[test]
fn declaration_rejects_invalid_regex() {
    let props = vec![sprop(
        "name",
        "String",
        PropertyConstraints {
            pattern: Some("(".into()),
            ..PropertyConstraints::default()
        },
    )];
    let err = validate_constraints(&props).expect_err("invalid regex rejected");
    assert!(
        err.to_string().contains("regex"),
        "message names regex: {err}"
    );
}

#[test]
fn declaration_accepts_valid_and_empty() {
    let props = vec![
        sprop(
            "name",
            "String",
            PropertyConstraints {
                pattern: Some(r"^\w+$".into()),
                one_of: None,
                length: Some(LengthConstraint {
                    min: Some(1),
                    max: Some(99),
                }),
                range: None,
            },
        ),
        sprop(
            "age",
            "Long",
            PropertyConstraints {
                range: Some(RangeConstraint {
                    min: Some(0.0),
                    max: Some(150.0),
                }),
                ..PropertyConstraints::default()
            },
        ),
        sprop("plain", "String", PropertyConstraints::default()),
    ];
    validate_constraints(&props).expect("valid declarations accepted");
}

#[test]
fn object_type_without_eq_still_partial_eq() {
    // Regression guard: ObjectType keeps PartialEq after the Eq drop.
    let t = ObjectType {
        name: TypeName("T".into()),
        properties: vec![],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "t".into(),
        },
        identity: None,
    };
    assert_eq!(t.clone(), t);
}
