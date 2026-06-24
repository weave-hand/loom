//! End-to-end execution test for `engine_serving::execute_query`: seeds a table
//! with file rows + live inline rows via `IcebergWriter`, then asserts on the
//! returned `RecordBatch`es directly (no `Rows`/`SqlValue` dep on query-api).

use arrow::array::Int64Array;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execute_query_unions_file_and_inline() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    // Seed 3 file rows: ids 0, 1, 2
    writer.seed("sales", "orders", &cols, &[3]).await;
    // Inline 1 row: id 100
    writer
        .inline(
            "sales",
            "orders",
            &cols,
            &[(100, "row100")],
            uuid::Uuid::new_v4(),
        )
        .await;

    let catalog = IcebergCatalog::new(pool);
    let batches = engine_serving::execute_query(
        &catalog,
        "SELECT \"id\" FROM \"sales\".\"orders\" ORDER BY \"id\"",
    )
    .await
    .expect("execute_query");

    let ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column is Int64");
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();

    // 3 file rows (0, 1, 2) + 1 inline row (100), ordered
    assert_eq!(ids, vec![0, 1, 2, 100], "file rows + inline row unioned");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execute_query_inline_only() {
    // A table that only ever had inline writes (no Parquet files) still reads back.
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    writer
        .inline(
            "events",
            "audit",
            &cols,
            &[(7, "seven")],
            uuid::Uuid::new_v4(),
        )
        .await;

    let catalog = IcebergCatalog::new(pool);
    let batches =
        engine_serving::execute_query(&catalog, "SELECT \"id\" FROM \"events\".\"audit\"")
            .await
            .expect("execute_query");

    let ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column is Int64");
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();

    assert_eq!(ids, vec![7], "inline-only table reads back");
}
