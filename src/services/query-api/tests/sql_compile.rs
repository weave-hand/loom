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
    let (sql, params) =
        compile_select(&t(), &["id".into(), "status".into()], &[], &[], &[], 100).unwrap();
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
    let (sql, params) = compile_select(
        &t(),
        &["id".into()],
        &[],
        std::slice::from_ref(&f),
        &[],
        100,
    )
    .unwrap();
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
    let (sql, params) =
        compile_select(&t(), &["a".into()], &[], std::slice::from_ref(&f), &[], 10).unwrap();
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
    let (sql, params) =
        compile_select(&t(), &["id".into()], &[], std::slice::from_ref(&f), &[], 10).unwrap();
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
    let (sql, params) = compile_select(
        &t(),
        &["id".into()],
        &[],
        std::slice::from_ref(&acl),
        &eq,
        10,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("tenant" = ?) AND ("status" = ?) LIMIT 10"#
    );
    assert_eq!(
        params,
        vec![SqlValue::Text("acme".into()), SqlValue::Text("open".into())]
    );
}

#[test]
fn expands_not_in_list_into_placeholders() {
    let f = RowFilter::Compare {
        property: "region".into(),
        op: CompareOp::NotIn,
        value: ScalarValue::List(vec![
            ScalarValue::Text("EU".into()),
            ScalarValue::Text("UK".into()),
        ]),
    };
    let (sql, params) =
        compile_select(&t(), &["id".into()], &[], std::slice::from_ref(&f), &[], 10).unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("region" NOT IN (?, ?)) LIMIT 10"#
    );
    assert_eq!(
        params,
        vec![SqlValue::Text("EU".into()), SqlValue::Text("UK".into())]
    );
}

#[test]
fn compiles_is_not_null_without_a_param() {
    // value is ignored for IS [NOT] NULL ops.
    let f = RowFilter::Compare {
        property: "closed_at".into(),
        op: CompareOp::IsNotNull,
        value: ScalarValue::Bool(true),
    };
    let (sql, params) =
        compile_select(&t(), &["id".into()], &[], std::slice::from_ref(&f), &[], 10).unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("closed_at" IS NOT NULL) LIMIT 10"#
    );
    assert!(params.is_empty());
}

#[test]
fn eq_filters_only_form_the_where_clause() {
    // No ACL row filter, only a request equality filter: the WHERE prefix and
    // conjunct-joining must still be correct (no leading/trailing AND).
    let eq = vec![("status".to_string(), SqlValue::Text("open".into()))];
    let (sql, params) = compile_select(&t(), &["id".into()], &[], &[], &eq, 10).unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("status" = ?) LIMIT 10"#
    );
    assert_eq!(params, vec![SqlValue::Text("open".into())]);
}

#[test]
fn masks_a_column_with_marker() {
    let (sql, params) = compile_select(
        &t(),
        &["id".into(), "secret".into()],
        &["secret".into()],
        &[],
        &[],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "id", '***' AS "secret" FROM "main"."orders" LIMIT 100"#
    );
    assert!(
        params.is_empty(),
        "the marker is a constant, not a bound param"
    );
}

#[test]
fn masking_preserves_projection_order_and_other_columns() {
    let (sql, _params) = compile_select(
        &t(),
        &["a".into(), "b".into(), "c".into()],
        &["b".into()],
        &[],
        &[],
        10,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT "a", '***' AS "b", "c" FROM "main"."orders" LIMIT 10"#
    );
}

#[test]
fn malformed_filter_is_an_error_not_a_panic() {
    // `In` with a non-list value is malformed; compile_select must return Err, not panic.
    let bad = RowFilter::Compare {
        property: "region".into(),
        op: CompareOp::In,
        value: ScalarValue::Text("EU".into()),
    };
    let res = compile_select(
        &t(),
        &["id".into()],
        &[],
        std::slice::from_ref(&bad),
        &[],
        10,
    );
    assert!(
        matches!(res, Err(query_api::sql::CompileError::MalformedFilter(_))),
        "malformed filter -> Err(MalformedFilter), no panic; got {res:?}"
    );
}
