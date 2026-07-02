//! Projection owns the governed output-column set: visible physical columns in property
//! order (fail-closed on empty), the masked subset, the positional logical-type zip, and
//! the served-rows -> ObjectRows conversion.

use std::collections::HashSet;

use control_plane_core::{ObjectType, PropertyDef, TableRef, TypeName};
use query_api::governed::{GovernedType, Projection};
use query_api::handler::QueryError;
use query_api::serving::{Rows, SqlValue};

fn prop(name: &str, ty: &str) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required: false,
        constraints: control_plane_core::PropertyConstraints::default(),
    }
}

fn governed(denied: &[&str], masked: &[&str]) -> GovernedType {
    GovernedType {
        otype: ObjectType {
            name: TypeName("Order".into()),
            properties: vec![
                prop("id", "Long"),
                prop("status", "String"),
                prop("secret", "String"),
            ],
            derived: vec![],
            table: TableRef {
                schema: "main".into(),
                name: "orders".into(),
            },
            identity: Some("id".into()),
        },
        row_filters: vec![],
        denied: denied
            .iter()
            .map(|s| (*s).to_string())
            .collect::<HashSet<_>>(),
        masked: masked
            .iter()
            .map(|s| (*s).to_string())
            .collect::<HashSet<_>>(),
    }
}

#[test]
fn visible_projects_allowed_in_property_order_with_types_and_mask() {
    let p = Projection::visible(&governed(&["secret"], &["status"])).unwrap();
    assert_eq!(p.columns, vec!["id".to_string(), "status".to_string()]);
    assert_eq!(
        p.logical_types,
        vec!["Long".to_string(), "String".to_string()]
    );
    assert_eq!(p.masked, vec!["status".to_string()]);
}

#[test]
fn empty_visible_set_is_forbidden() {
    let err = Projection::visible(&governed(&["id", "status", "secret"], &[])).unwrap_err();
    assert!(matches!(err, QueryError::Forbidden));
}

#[test]
fn push_appends_derived_columns_and_masked_membership() {
    let mut p = Projection::visible(&governed(&["secret"], &[])).unwrap();
    p.push("orderCount".into(), "Long".into(), false);
    p.push("hiddenAgg".into(), "Long".into(), true);
    assert_eq!(
        p.columns,
        vec![
            "id".to_string(),
            "status".to_string(),
            "orderCount".to_string(),
            "hiddenAgg".to_string()
        ]
    );
    assert_eq!(p.logical_types.len(), 4);
    assert_eq!(p.masked, vec!["hiddenAgg".to_string()]);
}

#[test]
fn into_object_rows_zips_columns_types_and_rows() {
    let p = Projection::visible(&governed(&["secret"], &[])).unwrap();
    let rows = p.into_object_rows(Rows {
        columns: vec!["id".into(), "status".into()],
        rows: vec![vec![SqlValue::Int(1), SqlValue::Text("open".into())]],
    });
    assert_eq!(rows.columns, vec!["id".to_string(), "status".to_string()]);
    assert_eq!(
        rows.logical_types,
        vec!["Long".to_string(), "String".to_string()]
    );
    assert_eq!(rows.rows.len(), 1);
}
