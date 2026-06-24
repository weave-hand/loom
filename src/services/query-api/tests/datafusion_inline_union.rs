//! fetch_rows unions a table's Parquet files with its inline rows. loom_fixture_test
//! (Postgres + LocalFsStorage; no DuckDB).

use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use query_api::serving::SqlValue;
use query_api::serving_datafusion::batches_to_rows;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_rows_unions_file_and_inline() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    writer.seed("sales", "orders", &cols, &[3]).await; // file rows id 0,1,2
    writer
        .inline(
            "sales",
            "orders",
            &cols,
            &[(100, "row100")],
            uuid::Uuid::new_v4(),
        )
        .await; // inline row id 100

    // was: DataFusionServingEngine::new(IcebergCatalog::new(pool)).fetch_rows(sql, &[])
    let catalog = IcebergCatalog::new(pool);
    let sql = "SELECT \"id\", \"name\" FROM \"sales\".\"orders\" ORDER BY \"id\"";
    let inlined = query_api::serving::inline_params(sql, &[]);
    let rows = batches_to_rows(
        engine_serving::execute_query(&catalog, &inlined, None)
            .await
            .expect("execute_query"),
    );

    // 3 file rows (0,1,2) + 1 inline row (100), unioned.
    let ids: Vec<i64> = rows
        .rows
        .iter()
        .map(|r| match &r[0] {
            SqlValue::Int(n) => *n,
            other => panic!("id not int: {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec![0, 1, 2, 100], "file rows + inline row unioned");
    assert_eq!(
        rows.rows.last().unwrap()[1],
        SqlValue::Text("row100".into()),
        "the inline row's name reads back"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_rows_inline_only_table() {
    // A table that only ever had inline writes (no Parquet files) still reads back —
    // exercises the "no file:// URLs, only memory://" registration path.
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

    // was: DataFusionServingEngine::new(IcebergCatalog::new(pool)).fetch_rows(sql, &params)
    let catalog = IcebergCatalog::new(pool);
    let sql = "SELECT \"id\", \"name\" FROM \"events\".\"audit\" WHERE (\"id\" = ?)";
    let params = &[SqlValue::Int(7)];
    let inlined = query_api::serving::inline_params(sql, params);
    let rows = batches_to_rows(
        engine_serving::execute_query(&catalog, &inlined, None)
            .await
            .expect("execute_query"),
    );
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Int(7), SqlValue::Text("seven".into())]],
        "inline-only table reads back, with a filter spanning the (single) source"
    );
}
