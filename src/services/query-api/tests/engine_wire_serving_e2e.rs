//! Cross-wire e2e: boots a real engine `FlightDataService` over a UDS, then connects
//! an `EngineServingClient` (now an internal **Flight SQL** client) and asserts:
//!   1. a small read unions file + inline rows in order;
//!   2. a result far larger than the old ~4 MB unary gRPC message cap streams back
//!      intact (the payoff of streaming);
//!   3. a malformed query surfaces as a `ServingError`.

use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_server::FlightServiceServer;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use engine::flight::FlightDataService;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use query_api::engine_client::EngineServingClient;
use query_api::serving::{ServingEngine, SqlValue};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = std::collections::HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{warehouse}"),
    );
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

async fn spawn_flight(fx: &PgFixture, db: &str, warehouse: &str) -> (tempfile::TempDir, String) {
    let sock_dir = tempfile::tempdir().expect("socket dir");
    let sock_path = sock_dir.path().join("engine.sock");
    let sock_str = sock_path.to_string_lossy().to_string();
    let pool = fx.pool_for(db).await;
    let svc = FlightDataService {
        catalog: make_catalog(fx.pg_dsn(db), warehouse).await,
        serving_catalog: IcebergCatalog::new(pool.clone()),
        serving_store: None,
        pool,
    };
    let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind uds");
    let incoming = UnixListenerStream::new(listener);
    tokio::spawn(async move {
        let _serve_result = Server::builder()
            .add_service(FlightServiceServer::new(svc))
            .serve_with_incoming(incoming)
            .await;
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    (sock_dir, sock_str)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_wire_unions_file_and_inline() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let wh = tempfile::tempdir().expect("warehouse");

    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    let writer = IcebergWriter::new(pool.clone(), dsn);
    writer.seed("sales", "orders", &cols, &[3]).await;
    writer
        .inline(
            "sales",
            "orders",
            &cols,
            &[(100, "row100")],
            uuid::Uuid::new_v4(),
        )
        .await;

    let (_sock_dir, sock) = spawn_flight(&fx, &db, &wh.path().display().to_string()).await;
    let client = EngineServingClient::connect(&sock).await.expect("connect");

    let rows = client
        .fetch_rows(r#"SELECT "id" FROM "sales"."orders" ORDER BY "id""#, &[])
        .await
        .expect("fetch_rows over flight-sql");
    let ids: Vec<i64> = rows
        .rows
        .iter()
        .map(|r| match &r[0] {
            SqlValue::Int(n) => *n,
            other => panic!("expected Int, got {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec![0, 1, 2, 100]);

    // Malformed SQL → ServingError (engine maps it to internal; client surfaces Err).
    assert!(
        client.fetch_rows("SELECT FROM nope", &[]).await.is_err(),
        "malformed SQL must error"
    );
}

/// A result whose collected Arrow IPC would exceed the ~4 MB unary gRPC message cap
/// streams back intact over Flight SQL. 600_000 `i64` rows ≈ 4.8 MB for the id column
/// alone — the old unary `ExecuteQueryResponse{ipc}` would have exceeded tonic's
/// default 4 MB decode limit and failed; per-batch Flight messages do not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_result_streams_past_unary_cap() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let wh = tempfile::tempdir().expect("warehouse");

    let cols = vec![("id".to_string(), "long".to_string(), false)];
    let writer = IcebergWriter::new(pool.clone(), dsn);
    writer.seed("big", "rows", &cols, &[600_000]).await;

    let (_sock_dir, sock) = spawn_flight(&fx, &db, &wh.path().display().to_string()).await;
    let client = EngineServingClient::connect(&sock).await.expect("connect");

    let rows = client
        .fetch_rows(r#"SELECT "id" FROM "big"."rows""#, &[])
        .await
        .expect("large result must stream back, not hit the 4 MB cap");
    assert_eq!(
        rows.rows.len(),
        600_000,
        "all rows must arrive over the stream"
    );
}
