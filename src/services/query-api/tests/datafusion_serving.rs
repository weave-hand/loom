//! engine_serving::execute_query over a seeded Iceberg table returns correct
//! Rows for the handler's compiled-SQL shape. loom_fixture_test (Postgres +
//! LocalFsStorage; no DuckDB).

use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use query_api::serving::SqlValue;
use query_api::serving_datafusion::batches_to_rows;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_rows_over_iceberg() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    writer.seed("sales", "orders", &cols, &[3]).await; // ids 0,1,2 ; names row0,row1,row2

    // was: DataFusionServingEngine::new(IcebergCatalog::new(pool)).fetch_rows(sql, &params)
    let catalog = IcebergCatalog::new(pool);
    let sql = "SELECT \"id\", \"name\" FROM \"sales\".\"orders\" WHERE (\"id\" = ?) LIMIT 100";
    let params = &[SqlValue::Int(1)];
    let inlined = query_api::serving::inline_params(sql, params);
    let rows = batches_to_rows(
        engine_serving::execute_query(&catalog, &inlined)
            .await
            .expect("execute_query"),
    );

    assert_eq!(rows.columns, vec!["id", "name"]);
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Int(1), SqlValue::Text("row1".into())]]
    );

    drop(writer);
}

/// A JOIN across two schema-qualified tables proves the engine registers more than
/// one table in a single SessionContext (and that `register_object_store` stays
/// idempotent across the registration loop) — the multi-table path the single-table
/// test above can't exercise.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_rows_joins_two_tables() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    // Two tables in one warehouse, both keyed id 0,1,2 ; names row0,row1,row2.
    writer.seed("sales", "orders", &cols, &[3]).await;
    writer.seed("sales", "customers", &cols, &[3]).await;

    // was: DataFusionServingEngine::new(IcebergCatalog::new(pool)).fetch_rows(sql, &params)
    let catalog = IcebergCatalog::new(pool);
    let sql = "SELECT o.\"id\", c.\"name\" FROM \"sales\".\"orders\" o \
               JOIN \"sales\".\"customers\" c ON o.\"id\" = c.\"id\" \
               WHERE (o.\"id\" = ?) LIMIT 100";
    let params = &[SqlValue::Int(2)];
    let inlined = query_api::serving::inline_params(sql, params);
    let rows = batches_to_rows(
        engine_serving::execute_query(&catalog, &inlined)
            .await
            .expect("execute_query"),
    );

    assert_eq!(rows.columns, vec!["id", "name"]);
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Int(2), SqlValue::Text("row2".into())]]
    );

    drop(writer);
}
