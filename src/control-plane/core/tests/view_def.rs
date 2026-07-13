//! `ViewDef` shape validation: predicate/projection must resolve against the
//! base schema; the RowFilter structural invariants apply.

use control_plane_core::{
    ColumnDef, CompareOp, RowFilter, ScalarValue, TableRef, TableSchema, ViewDef,
    validate_view_shape,
};

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

fn base_schema() -> TableSchema {
    TableSchema {
        columns: vec![
            ColumnDef {
                order: 1,
                name: "id".into(),
                ty: "long".into(),
                nullable: false,
            },
            ColumnDef {
                order: 2,
                name: "region".into(),
                ty: "string".into(),
                nullable: true,
            },
            ColumnDef {
                order: 3,
                name: "amount".into(),
                ty: "long".into(),
                nullable: true,
            },
        ],
    }
}

fn eu_predicate() -> RowFilter {
    RowFilter::Compare {
        property: "region".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("EU".into()),
    }
}

fn vdef(predicate: Option<RowFilter>, columns: Option<Vec<String>>) -> ViewDef {
    ViewDef {
        view: tref("gov", "customers_eu"),
        base: tref("raw", "customers"),
        predicate,
        columns,
    }
}

#[test]
fn accepts_predicate_and_projection_over_base_columns() {
    let v = vdef(
        eu_predicate().into(),
        Some(vec!["id".into(), "region".into()]),
    );
    assert_eq!(validate_view_shape(&v, &base_schema()), Ok(()));
}

#[test]
fn accepts_bare_view_no_predicate_no_projection() {
    assert_eq!(
        validate_view_shape(&vdef(None, None), &base_schema()),
        Ok(())
    );
}

#[test]
fn rejects_predicate_on_unknown_column() {
    let v = vdef(
        Some(RowFilter::Compare {
            property: "nope".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Text("x".into()),
        }),
        None,
    );
    let err = validate_view_shape(&v, &base_schema()).unwrap_err();
    assert!(err.contains("nope"), "error names the bad column: {err}");
}

#[test]
fn rejects_projection_with_unknown_column() {
    let v = vdef(None, Some(vec!["id".into(), "ghost".into()]));
    let err = validate_view_shape(&v, &base_schema()).unwrap_err();
    assert!(err.contains("ghost"), "error names the bad column: {err}");
}

#[test]
fn rejects_empty_projection() {
    let v = vdef(None, Some(vec![]));
    assert!(validate_view_shape(&v, &base_schema()).is_err());
}

#[test]
fn rejects_duplicate_projection_column() {
    let v = vdef(None, Some(vec!["id".into(), "id".into()]));
    assert!(validate_view_shape(&v, &base_schema()).is_err());
}

#[test]
fn rejects_caller_only_compare_ops_in_predicate() {
    // Contains is caller-predicate-only; validate_row_filter rejects it and
    // validate_view_shape must surface that.
    let v = vdef(
        Some(RowFilter::Compare {
            property: "region".into(),
            op: CompareOp::Contains,
            value: ScalarValue::Text("E".into()),
        }),
        None,
    );
    assert!(validate_view_shape(&v, &base_schema()).is_err());
}

#[test]
fn rejects_self_referential_view() {
    let mut v = vdef(None, None);
    v.base = v.view.clone();
    assert!(validate_view_shape(&v, &base_schema()).is_err());
}
