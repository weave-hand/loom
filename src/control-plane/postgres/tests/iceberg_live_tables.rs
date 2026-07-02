//! IcebergCatalog::live_tables returns exactly the tables live in the mirror.
//! `rust_test` integration target (loom_fixture_test), not an inline module.

use control_plane_core::TableRef;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_tables_lists_only_live() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![("id".to_string(), "long".to_string(), false)];
    writer.seed("sales", "orders", &cols, &[3]).await;
    writer.seed("sales", "shipments", &cols, &[2]).await;
    writer.drop_table("sales", "shipments").await;

    let catalog = IcebergCatalog::new(pool);
    let mut live = catalog.live_tables().await.expect("live_tables");
    live.sort_by(|a, b| {
        (a.schema.as_str(), a.name.as_str()).cmp(&(b.schema.as_str(), b.name.as_str()))
    });

    assert_eq!(
        live,
        vec![TableRef {
            schema: "sales".into(),
            name: "orders".into()
        }],
        "only the un-dropped table is live"
    );
}
