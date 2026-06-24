//! register_iceberg_table registers a seeded Iceberg table's absolute-path Parquet
//! files so a raw DataFusion SELECT reads them back. loom_fixture_test (Postgres +
//! LocalFsStorage; no DuckDB).

use control_plane_core::TableRef;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use datafusion::prelude::SessionContext;
use engine_serving::register_iceberg_table;
use query_api::serving::SqlValue;
use query_api::serving_datafusion::batches_to_rows;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registers_and_selects_back() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    // Two appends -> two Parquet files of 3 + 2 rows = 5 rows total.
    writer.seed("sales", "orders", &cols, &[3, 2]).await;

    let catalog = IcebergCatalog::new(pool);
    let table = TableRef {
        schema: "sales".into(),
        name: "orders".into(),
    };

    let ctx = SessionContext::new();
    register_iceberg_table(&ctx, &catalog, &table)
        .await
        .expect("register");

    // Dogfood batches_to_rows to avoid fragile raw-Arrow casts.
    let df = ctx
        .sql("SELECT count(*) AS n FROM \"sales\".\"orders\"")
        .await
        .expect("sql");
    let rows = batches_to_rows(df.collect().await.expect("collect"));
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Int(5)]],
        "both appended files are registered and scanned"
    );

    // Keep the writer (its warehouse TempDir holds the files) alive until here.
    drop(writer);
}
