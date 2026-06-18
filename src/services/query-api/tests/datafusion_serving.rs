//! DataFusionServingEngine::fetch_rows over a seeded Iceberg table returns correct
//! Rows for the handler's compiled-SQL shape. loom_fixture_test (Postgres +
//! LocalFsStorage; no DuckDB).

use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use query_api::serving::{ServingEngine, SqlValue};
use query_api::serving_datafusion::DataFusionServingEngine;

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

    let engine = DataFusionServingEngine::new(IcebergCatalog::new(pool));

    // The compiled-read shape: quoted idents, a bound `?`, LIMIT.
    let sql = "SELECT \"id\", \"name\" FROM \"sales\".\"orders\" WHERE (\"id\" = ?) LIMIT 100";
    let rows = engine
        .fetch_rows(sql, &[SqlValue::Int(1)])
        .await
        .expect("fetch_rows");

    assert_eq!(rows.columns, vec!["id", "name"]);
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Int(1), SqlValue::Text("row1".into())]]
    );

    drop(writer);
}
