use control_plane_core::{Aggregation, CompareOp, LinkBacking, RowFilter, ScalarValue, TableRef};
use query_api::filter::CallerPredicate;
use query_api::serving::SqlValue;
use query_api::sql::{
    DerivedAggregate, DerivedSelect, DuckDbDialect, SqlDialect, compile_select, compile_select_with,
};

struct BacktickDialect;
impl SqlDialect for BacktickDialect {
    fn quote_ident(&self, id: &str) -> String {
        format!("`{id}`")
    }
    fn placeholder(&self, one_based: usize) -> String {
        format!("${one_based}")
    }
    fn limit_clause(&self, limit: u32) -> String {
        format!("LIMIT {limit}")
    }
}

fn t() -> TableRef {
    TableRef {
        schema: "main".into(),
        name: "orders".into(),
    }
}

#[test]
fn default_compile_select_equals_explicit_duckdb() {
    let f = RowFilter::Compare {
        property: "status".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("open".into()),
    };
    let a = compile_select(
        &t(),
        &["id".into()],
        &[],
        std::slice::from_ref(&f),
        &[],
        &[],
        100,
    )
    .unwrap();
    let b = compile_select_with(
        &DuckDbDialect,
        &t(),
        &["id".into()],
        &[],
        std::slice::from_ref(&f),
        &[],
        &[],
        100,
    )
    .unwrap();
    assert_eq!(
        a, b,
        "the default wrapper must equal explicit DuckDbDialect"
    );
}

#[test]
fn dialect_controls_quoting_and_placeholders() {
    let f = RowFilter::Compare {
        property: "status".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("open".into()),
    };
    let (sql, params) = compile_select_with(
        &BacktickDialect,
        &t(),
        &["id".into()],
        &[],
        std::slice::from_ref(&f),
        &[],
        &[],
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT `id` FROM `main`.`orders` WHERE (`status` = $1) LIMIT 100"
    );
    assert_eq!(params, vec![SqlValue::Text("open".into())]);
}

#[test]
fn positional_indices_span_select_derived_then_where() {
    let derived = vec![DerivedSelect::Aggregate(Box::new(DerivedAggregate {
        name: "totalSpend".into(),
        agg: Aggregation::Sum("amount".into()),
        backing: LinkBacking::JoinTable {
            table: TableRef {
                schema: "main".into(),
                name: "customer_order".into(),
            },
            from_key: "id".into(),
            from_column: "customer_id".into(),
            to_column: "order_id".into(),
            to_key: "id".into(),
        },
        target_table: TableRef {
            schema: "main".into(),
            name: "orders".into(),
        },
        target_filters: vec![RowFilter::Compare {
            property: "status".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Text("shipped".into()),
        }],
    }))];
    let predicates = vec![CallerPredicate {
        column: "region".into(),
        op: CompareOp::Eq,
        values: vec![SqlValue::Text("CA".into())],
    }];
    let (sql, params) = compile_select_with(
        &BacktickDialect,
        &TableRef {
            schema: "main".into(),
            name: "customer".into(),
        },
        &["id".into()],
        &[],
        &[],
        &predicates,
        &derived,
        100,
    )
    .unwrap();
    assert_eq!(
        sql,
        "SELECT `id`, (SELECT COALESCE(SUM(sub.`amount`), 0) FROM `main`.`orders` sub \
         JOIN `main`.`customer_order` j ON j.`order_id` = sub.`id` \
         WHERE j.`customer_id` = o.`id` AND (sub.`status` = $1)) AS `totalSpend` \
         FROM `main`.`customer` o WHERE (`region` = $2) LIMIT 100"
    );
    assert_eq!(
        params,
        vec![
            SqlValue::Text("shipped".into()),
            SqlValue::Text("CA".into())
        ]
    );
}
