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
fn quote_ident_escapes_embedded_double_quote() {
    // A trusted-but-unvalidated identifier containing a `"` must not panic the
    // request thread; it is escaped per SQL identifier rules (`"` -> `""`).
    // See iss-quote-ident-panic.
    let d = DuckDbDialect;
    assert_eq!(d.quote_ident("we\"ird"), "\"we\"\"ird\"");
    // An ordinary identifier is unchanged apart from the surrounding quotes.
    assert_eq!(d.quote_ident("plain"), "\"plain\"");
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
fn duckdb_dialect_requests_order_barrier() {
    use query_api::sql::{DuckDbDialect, SqlDialect};
    assert!(DuckDbDialect.limit_needs_order_barrier());
}

#[test]
fn datafusion_dialect_keeps_bare_limit_and_renders_like_duckdb() {
    use query_api::sql::{DataFusionDialect, DuckDbDialect, SqlDialect};
    let df = DataFusionDialect;
    let duck = DuckDbDialect;
    // No barrier on the DataFusion path (it has no multi-file LIMIT bug).
    assert!(!df.limit_needs_order_barrier());
    // Identical rendering to DuckDB: the compiled SQL is valid for both engines.
    assert_eq!(df.quote_ident("a\"b"), duck.quote_ident("a\"b"));
    assert_eq!(df.placeholder(3), duck.placeholder(3));
    assert_eq!(df.limit_clause(1000), duck.limit_clause(1000));
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
