//! seed_predicates is the shared visibility-gate + coerce + `_ids` loop: caller filters
//! against the allowed/masked projection (denied or masked -> BadFilter, no type-info
//! leak), then the object-set identity In predicate.

use std::collections::HashSet;

use control_plane_core::{CompareOp, ObjectType, PropertyDef};
use query_api::governed::{GovernedType, seed_predicates};
use query_api::handler::QueryError;
use query_api::serving::SqlValue;

fn prop(name: &str, ty: &str) -> PropertyDef {
    PropertyDef::new(name, ty)
}

fn governed(masked: &[&str]) -> GovernedType {
    GovernedType {
        otype: ObjectType::build("Person", ("main", "person"))
            .add_prop(prop("id", "Long"))
            .add_prop(prop("name", "String"))
            .add_prop(prop("ssn", "String"))
            .identity("id")
            .done(),
        row_filters: vec![],
        denied: HashSet::new(),
        masked: masked
            .iter()
            .map(|s| (*s).to_string())
            .collect::<HashSet<_>>(),
    }
}

fn allowed() -> Vec<String> {
    vec!["id".into(), "name".into(), "ssn".into()]
}

#[test]
fn coerces_filters_then_appends_the_ids_in_predicate() {
    let g = governed(&[]);
    let preds = seed_predicates(
        &g,
        &allowed(),
        &[("name".to_string(), "alice".to_string())],
        &["1".to_string(), "2".to_string()],
    )
    .unwrap();
    assert_eq!(preds.len(), 2);
    assert_eq!(preds[0].column, "name");
    assert_eq!(preds[1].column, "id");
    assert_eq!(preds[1].op, CompareOp::In);
    assert_eq!(preds[1].values, vec![SqlValue::Int(1), SqlValue::Int(2)]);
}

#[test]
fn filter_on_a_column_outside_allowed_is_bad_filter() {
    let g = governed(&[]);
    let err = seed_predicates(
        &g,
        &["id".to_string(), "name".to_string()], // ssn not visible
        &[("ssn".to_string(), "x".to_string())],
        &[],
    )
    .unwrap_err();
    assert!(matches!(err, QueryError::BadFilter(c) if c == "ssn"));
}

#[test]
fn filter_on_a_masked_column_is_bad_filter() {
    let g = governed(&["name"]);
    let err = seed_predicates(
        &g,
        &allowed(),
        &[("name".to_string(), "alice".to_string())],
        &[],
    )
    .unwrap_err();
    assert!(matches!(err, QueryError::BadFilter(c) if c == "name"));
}

#[test]
fn empty_inputs_yield_no_predicates() {
    let g = governed(&[]);
    let preds = seed_predicates(&g, &allowed(), &[], &[]).unwrap();
    assert!(preds.is_empty());
}
