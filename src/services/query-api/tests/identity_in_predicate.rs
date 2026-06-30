//! identity_in_predicate lowers a set of object ids to an In predicate on the type's
//! declared identity column, governed like any caller filter.

use std::collections::HashSet;

use control_plane_core::{CompareOp, ObjectType, PropertyDef, TableRef, TypeName};
use query_api::filter::FilterError;
use query_api::handler::{QueryError, identity_in_predicate, identity_is_governed};
use query_api::serving::SqlValue;

fn customer(identity: Option<String>) -> ObjectType {
    ObjectType {
        name: TypeName("Customer".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "long".into(),
                required: true,
            },
            PropertyDef {
                name: "name".into(),
                ty: "string".into(),
                required: false,
            },
        ],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "customer".into(),
        },
        identity,
    }
}

fn empty() -> HashSet<String> {
    HashSet::new()
}

#[test]
fn lowers_ids_to_an_in_predicate_on_identity() {
    let pred = identity_in_predicate(
        &customer(Some("id".into())),
        &empty(),
        &empty(),
        &["1".to_string(), "2".to_string()],
    )
    .unwrap()
    .expect("a predicate for a non-empty id set");
    assert_eq!(pred.column, "id");
    assert_eq!(pred.op, CompareOp::In);
    assert_eq!(pred.values, vec![SqlValue::Int(1), SqlValue::Int(2)]);
}

#[test]
fn empty_ids_yields_no_predicate() {
    let pred =
        identity_in_predicate(&customer(Some("id".into())), &empty(), &empty(), &[]).unwrap();
    assert!(pred.is_none());
}

#[test]
fn no_declared_identity_is_no_identity_error() {
    let err =
        identity_in_predicate(&customer(None), &empty(), &empty(), &["1".to_string()]).unwrap_err();
    assert!(matches!(err, QueryError::NoIdentity(t) if t == "Customer"));
}

#[test]
fn denied_identity_is_bad_filter() {
    let denied: HashSet<String> = ["id".to_string()].into_iter().collect();
    let err = identity_in_predicate(
        &customer(Some("id".into())),
        &denied,
        &empty(),
        &["1".to_string()],
    )
    .unwrap_err();
    assert!(matches!(err, QueryError::BadFilter(c) if c == "id"));
}

#[test]
fn masked_identity_is_bad_filter() {
    let masked: HashSet<String> = ["id".to_string()].into_iter().collect();
    let err = identity_in_predicate(
        &customer(Some("id".into())),
        &empty(),
        &masked,
        &["1".to_string()],
    )
    .unwrap_err();
    assert!(matches!(err, QueryError::BadFilter(c) if c == "id"));
}

#[test]
fn identity_is_governed_flags_denied_and_masked() {
    let denied: HashSet<String> = ["id".to_string()].into_iter().collect();
    let masked: HashSet<String> = ["id".to_string()].into_iter().collect();
    // Denied identity → governed.
    assert!(identity_is_governed(
        &customer(Some("id".into())),
        &denied,
        &empty()
    ));
    // Masked identity → governed.
    assert!(identity_is_governed(
        &customer(Some("id".into())),
        &empty(),
        &masked
    ));
    // Ungoverned identity → not governed.
    assert!(!identity_is_governed(
        &customer(Some("id".into())),
        &empty(),
        &empty()
    ));
    // Identity-less type → never governed, even if "id" is denied/masked.
    assert!(!identity_is_governed(&customer(None), &denied, &masked));
}

#[test]
fn uncoercible_value_is_bad_filter_value() {
    let err = identity_in_predicate(
        &customer(Some("id".into())),
        &empty(),
        &empty(),
        &["notanumber".to_string()],
    )
    .unwrap_err();
    // A value that does not coerce is BadFilterValue (a parse fault carrying the source
    // FilterError), distinct from BadFilter (a column-permission denial).
    assert!(
        matches!(&err, QueryError::BadFilterValue(FilterError::BadValue(c, _)) if c == "id"),
        "got {err:?}",
    );
}
