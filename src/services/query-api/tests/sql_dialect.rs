use control_plane_core::{Aggregation, CompareOp, LinkBacking, RowFilter, ScalarValue, TableRef};
use query_api::filter::CallerPredicate;
use query_api::serving::SqlValue;
use query_api::sql::{
    DataFusionDialect, DerivedAggregate, DerivedSelect, SqlDialect, compile_select,
    compile_select_with,
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
    let d = DataFusionDialect;
    assert_eq!(d.quote_ident("we\"ird"), "\"we\"\"ird\"");
    // An ordinary identifier is unchanged apart from the surrounding quotes.
    assert_eq!(d.quote_ident("plain"), "\"plain\"");
}

#[test]
fn default_compile_select_equals_explicit_datafusion() {
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
        &[],
        100,
    )
    .unwrap();
    let b = compile_select_with(
        &DataFusionDialect,
        &t(),
        &["id".into()],
        &[],
        std::slice::from_ref(&f),
        &[],
        &[],
        &[],
        None,
        100,
    )
    .unwrap();
    assert_eq!(
        a, b,
        "the default wrapper must equal explicit DataFusionDialect"
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
        &[],
        None,
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
fn datafusion_dialect_emits_bare_limit() {
    // The sole serving dialect emits a bare `LIMIT` (no ORDER BY barrier): DataFusion
    // has no multi-file `LIMIT` corruption bug (iss-multi-file-limit-misread).
    let df = DataFusionDialect;
    assert_eq!(df.limit_clause(1000), "LIMIT 1000");
    let (sql, _params) =
        compile_select(&t(), &["id".into()], &[], &[], &[], &[], &[], 1000).unwrap();
    assert_eq!(
        sql, r#"SELECT "id" FROM "main"."orders" LIMIT 1000"#,
        "bare LIMIT, no ORDER BY barrier: {sql}"
    );
}

#[test]
fn compile_select_emits_order_by_when_requested() {
    let (sql, _params) = compile_select_with(
        &DataFusionDialect,
        &t(),
        &["id".into(), "name".into()],
        &[],
        &[],
        &[],
        &[],
        &[],
        Some("id"),
        10,
    )
    .unwrap();
    assert!(sql.contains("ORDER BY"), "expected ORDER BY, got: {sql}");
    // ORDER BY must precede LIMIT.
    assert!(
        sql.find("ORDER BY").unwrap() < sql.find("LIMIT").unwrap(),
        "got: {sql}"
    );
    assert!(
        sql.contains(r#"ORDER BY "id" ASC"#),
        "orders by identity: {sql}"
    );
}

#[test]
fn compile_select_no_order_by_by_default() {
    let (sql, _params) = compile_select_with(
        &DataFusionDialect,
        &t(),
        &["id".into()],
        &[],
        &[],
        &[],
        &[],
        &[],
        None,
        10,
    )
    .unwrap();
    assert!(
        !sql.contains("ORDER BY"),
        "default read stays unordered: {sql}"
    );
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
        &[],
        &derived,
        None,
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
