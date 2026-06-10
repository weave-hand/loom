use control_plane_core::{CompareOp, RowFilter, ScalarValue, TableRef};
use query_api::serving::SqlValue;
use query_api::sql::compile_select;

fn t() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "orders".into(),
    }
}

#[test]
fn projects_allowed_columns_and_quotes_identifiers() {
    let (sql, params) = compile_select(&t(), &["id".into(), "status".into()], &[], &[], 100);
    assert_eq!(
        sql,
        r#"SELECT "id", "status" FROM "main"."orders" LIMIT 100"#
    );
    assert!(params.is_empty());
}

#[test]
fn compiles_acl_compare_leaf_as_bound_param() {
    let f = RowFilter::Compare {
        property: "status".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("open".into()),
    };
    let (sql, params) = compile_select(&t(), &["id".into()], std::slice::from_ref(&f), &[], 100);
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("status" = ?) LIMIT 100"#
    );
    assert_eq!(params, vec![SqlValue::Text("open".into())]);
}

#[test]
fn compiles_and_or_not_tree() {
    let f = RowFilter::And(vec![
        RowFilter::Compare {
            property: "a".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Int(1),
        },
        RowFilter::Or(vec![
            RowFilter::Compare {
                property: "b".into(),
                op: CompareOp::Gt,
                value: ScalarValue::Int(2),
            },
            RowFilter::Not(Box::new(RowFilter::Compare {
                property: "c".into(),
                op: CompareOp::IsNull,
                value: ScalarValue::Bool(true),
            })),
        ]),
    ]);
    let (sql, params) = compile_select(&t(), &["a".into()], std::slice::from_ref(&f), &[], 10);
    assert_eq!(
        sql,
        r#"SELECT "a" FROM "main"."orders" WHERE (("a" = ?) AND (("b" > ?) OR (NOT ("c" IS NULL)))) LIMIT 10"#
    );
    assert_eq!(params, vec![SqlValue::Int(1), SqlValue::Int(2)]);
}

#[test]
fn expands_in_list_into_placeholders() {
    let f = RowFilter::Compare {
        property: "region".into(),
        op: CompareOp::In,
        value: ScalarValue::List(vec![
            ScalarValue::Text("EU".into()),
            ScalarValue::Text("UK".into()),
        ]),
    };
    let (sql, params) = compile_select(&t(), &["id".into()], std::slice::from_ref(&f), &[], 10);
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("region" IN (?, ?)) LIMIT 10"#
    );
    assert_eq!(
        params,
        vec![SqlValue::Text("EU".into()), SqlValue::Text("UK".into())]
    );
}

#[test]
fn ands_acl_filter_with_request_equality_filter() {
    let acl = RowFilter::Compare {
        property: "tenant".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("acme".into()),
    };
    let eq = vec![("status".to_string(), SqlValue::Text("open".into()))];
    let (sql, params) = compile_select(&t(), &["id".into()], std::slice::from_ref(&acl), &eq, 10);
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("tenant" = ?) AND ("status" = ?) LIMIT 10"#
    );
    assert_eq!(
        params,
        vec![SqlValue::Text("acme".into()), SqlValue::Text("open".into())]
    );
}
