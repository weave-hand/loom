//! e2e over `execute_governed_sql_stream`: row filter holds under arbitrary SQL,
//! denied columns absent, masked columns redacted (incl. through GROUP BY), and an
//! empty policy = full visibility. Governance is applied regardless of client SQL.

use arrow::array::{Array, Int64Array, StringArray};
use control_plane_core::{CompareOp, GovernedCatalog, GovernedTable, RowFilter, ScalarValue, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use engine_serving::governed::execute_governed_sql_stream;
use futures::TryStreamExt;

fn gt(schema: &str, name: &str) -> TableRef { TableRef { schema: schema.into(), name: name.into() } }

async fn run(catalog: &IcebergCatalog, sql: &str, cat: &GovernedCatalog) -> Vec<arrow::record_batch::RecordBatch> {
    let stream = execute_governed_sql_stream(catalog, sql, cat, None).await.expect("governed stream");
    stream.try_collect().await.expect("collect")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_filter_holds_under_join() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    writer.seed("s", "orders", &cols, &[5]).await; // ids 0..4
    let catalog = IcebergCatalog::new(pool);

    // Policy: only rows with id >= 2 are visible on `orders`.
    let cat = GovernedCatalog {
        tables: vec![GovernedTable {
            table: gt("s", "orders"),
            row_filters: vec![RowFilter::Compare {
                property: "id".into(), op: CompareOp::Ge, value: ScalarValue::Int(2),
            }],
            denied: vec![], masked: vec![],
        }],
    };
    // Client SQL that *tries* to see everything (self-join, WHERE true).
    let batches = run(&catalog, "SELECT o.\"id\" FROM \"s\".\"orders\" o WHERE o.\"id\" >= 0 ORDER BY o.\"id\"", &cat).await;
    let ids: Vec<i64> = collect_i64(&batches, 0);
    assert_eq!(ids, vec![2, 3, 4], "row filter applied regardless of client predicate");
}

fn collect_i64(batches: &[arrow::record_batch::RecordBatch], col: usize) -> Vec<i64> {
    batches.iter().flat_map(|b| {
        let a = b.column(col).as_any().downcast_ref::<Int64Array>().expect("i64");
        (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
    }).collect()
}

fn collect_str(batches: &[arrow::record_batch::RecordBatch], col: usize) -> Vec<String> {
    batches.iter().flat_map(|b| {
        let a = b.column(col).as_any().downcast_ref::<StringArray>().expect("utf8");
        (0..a.len()).map(|i| a.value(i).to_string()).collect::<Vec<_>>()
    }).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn denied_column_is_absent() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("secret".to_string(), "string".to_string(), false),
    ];
    writer.seed("s", "t", &cols, &[3]).await;
    let catalog = IcebergCatalog::new(pool);
    let cat = GovernedCatalog { tables: vec![GovernedTable {
        table: gt("s", "t"), row_filters: vec![], denied: vec!["secret".into()], masked: vec![],
    }]};
    // Naming the denied column => planning error.
    let err = execute_governed_sql_stream(&catalog, "SELECT \"secret\" FROM \"s\".\"t\"", &cat, None).await;
    assert!(err.is_err(), "denied column must not resolve");
    // SELECT * must not include it.
    let batches = run(&catalog, "SELECT * FROM \"s\".\"t\"", &cat).await;
    assert!(batches[0].schema().field_with_name("secret").is_err(), "denied col absent from schema");
    assert!(batches[0].schema().field_with_name("id").is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn masked_column_redacted_through_group_by() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("email".to_string(), "string".to_string(), false),
    ];
    writer.seed("s", "u", &cols, &[3]).await;
    let catalog = IcebergCatalog::new(pool);
    let cat = GovernedCatalog { tables: vec![GovernedTable {
        table: gt("s", "u"), row_filters: vec![], denied: vec![], masked: vec!["email".into()],
    }]};
    // Direct select: every email is '***'
    let batches = run(&catalog, "SELECT \"email\" FROM \"s\".\"u\"", &cat).await;
    let emails = collect_str(&batches, 0);
    assert!(emails.iter().all(|e| e == "***"), "masked values redacted");
    // Through a GROUP BY: groups on '***'
    let grouped = run(&catalog, "SELECT \"email\", count(*) AS c FROM \"s\".\"u\" GROUP BY \"email\"", &cat).await;
    assert_eq!(collect_str(&grouped, 0), vec!["***".to_string()], "single masked group");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_policy_is_full_visibility() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![("id".to_string(), "long".to_string(), false)];
    writer.seed("s", "t", &cols, &[3]).await;
    let catalog = IcebergCatalog::new(pool);
    // No GovernedTable entry at all => fully visible.
    let cat = GovernedCatalog::default();
    let batches = run(&catalog, "SELECT \"id\" FROM \"s\".\"t\" ORDER BY \"id\"", &cat).await;
    assert_eq!(collect_i64(&batches, 0), vec![0, 1, 2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parity_with_compiled_governed_sql() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("email".to_string(), "string".to_string(), false),
    ];
    writer.seed("s", "u", &cols, &[6]).await; // ids 0..5
    let catalog = IcebergCatalog::new(pool);

    // Governed path: id >= 2, email masked.
    let cat = GovernedCatalog { tables: vec![GovernedTable {
        table: gt("s", "u"),
        row_filters: vec![RowFilter::Compare { property: "id".into(), op: CompareOp::Ge, value: ScalarValue::Int(2) }],
        denied: vec![], masked: vec!["email".into()],
    }]};
    let g = run(&catalog, "SELECT \"id\", \"email\" FROM \"s\".\"u\" ORDER BY \"id\"", &cat).await;

    // Equivalent hand-compiled governed SQL (what compile_select_with emits for this
    // subject policy): masked email -> '***' literal, row filter as WHERE, over the
    // ungoverned execute_query. Row-for-row identical to the governed provider.
    let expected = engine_serving::execute_query(
        &catalog,
        "SELECT \"id\", '***' AS \"email\" FROM \"s\".\"u\" WHERE (\"id\" >= 2) ORDER BY \"id\"",
        None,
    ).await.expect("compiled");

    assert_eq!(collect_i64(&g, 0), collect_i64(&expected, 0), "ids match");
    assert_eq!(collect_str(&g, 1), collect_str(&expected, 1), "masked emails match");
}
