use control_plane_core::{CompareOp, LinkBacking, RowFilter, ScalarValue, TableRef};
use query_api::serving::SqlValue;
use query_api::sql::compile_traversal;

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

#[test]
fn foreign_key_traversal_joins_and_distincts() {
    let (sql, params) = compile_traversal(
        &tref("main", "customer"),
        &tref("main", "orders"),
        &LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
        &["id".into(), "amount".into()],
        &[],
        &[],
        &[],
        &[("region".into(), SqlValue::Text("CA".into()))],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT DISTINCT t."id", t."amount" FROM "main"."orders" t JOIN "main"."customer" f ON f."id" = t."customer_id" WHERE (f."region" = ?) LIMIT 100"#
    );
    assert_eq!(params, vec![SqlValue::Text("CA".into())]);
}

#[test]
fn join_table_traversal_chains_two_joins() {
    let (sql, _params) = compile_traversal(
        &tref("main", "customer"),
        &tref("main", "orders"),
        &LinkBacking::JoinTable {
            table: tref("main", "customer_order"),
            from_key: "id".into(),
            from_column: "customer_id".into(),
            to_column: "order_id".into(),
            to_key: "id".into(),
        },
        &["id".into()],
        &[],
        &[],
        &[],
        &[],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT DISTINCT t."id" FROM "main"."orders" t JOIN "main"."customer_order" j ON j."order_id" = t."id" JOIN "main"."customer" f ON f."id" = j."customer_id" LIMIT 100"#
    );
}

#[test]
fn masks_target_column_and_ands_both_row_filters() {
    let (sql, params) = compile_traversal(
        &tref("main", "customer"),
        &tref("main", "orders"),
        &LinkBacking::ForeignKey {
            from_column: "id".into(),
            to_column: "customer_id".into(),
        },
        &["id".into(), "secret".into()],
        &["secret".into()],
        &[RowFilter::Compare {
            property: "region".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Text("CA".into()),
        }],
        &[RowFilter::Compare {
            property: "amount".into(),
            op: CompareOp::Gt,
            value: ScalarValue::Int(100),
        }],
        &[],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        r#"SELECT DISTINCT t."id", '***' AS "secret" FROM "main"."orders" t JOIN "main"."customer" f ON f."id" = t."customer_id" WHERE (f."region" = ?) AND (t."amount" > ?) LIMIT 100"#
    );
    assert_eq!(
        params,
        vec![SqlValue::Text("CA".into()), SqlValue::Int(100)]
    );
}
