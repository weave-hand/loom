//! Engine-level Flight SQL wire test: boot a `FlightDataService` over a UDS, seed a
//! table (file rows + one inline row), issue a `CommandStatementQuery` via the
//! `FlightSqlClient`, and assert the streamed batches reassemble to the unioned
//! result. Also asserts a malformed query surfaces an error (mapped from the
//! engine's `do_get`).

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array};
use arrow_flight::flight_service_server::FlightServiceServer;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use engine::flight::FlightDataService;
use engine_wire::flight::FlightSqlClient;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
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
    let file_catalog = make_catalog(fx.pg_dsn(db), warehouse).await;
    let svc = FlightDataService {
        catalog: file_catalog,
        serving_catalog: IcebergCatalog::new(pool.clone()),
        serving_store: None,
        pool,
    };

    let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind uds");
    let incoming = UnixListenerStream::new(listener);
    tokio::spawn(async move {
        drop(
            Server::builder()
                .add_service(FlightServiceServer::new(svc))
                .serve_with_incoming(incoming)
                .await,
        );
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    (sock_dir, sock_str)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flight_sql_streams_unioned_result() {
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
    let client = FlightSqlClient::connect(&sock).await.expect("connect");

    let batches = client
        .execute(r#"SELECT "id" FROM "sales"."orders" ORDER BY "id""#.to_string())
        .await
        .expect("flight-sql execute");
    let mut ids = Vec::new();
    for b in &batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64");
        for i in 0..col.len() {
            ids.push(col.value(i));
        }
    }
    assert_eq!(
        ids,
        vec![0, 1, 2, 100],
        "file rows + inline row, streamed in id order"
    );

    // Malformed SQL must surface as an error (engine maps it from do_get).
    let err = client.execute("SELECT FROM nope".to_string()).await;
    assert!(err.is_err(), "malformed SQL must error");
}
