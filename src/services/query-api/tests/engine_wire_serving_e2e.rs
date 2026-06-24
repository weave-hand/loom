//! Cross-wire e2e: boots a real `EngineQueryService` tonic server over a UDS,
//! seeds a table with both Parquet file rows and a live inline row, then
//! connects an `EngineServingClient` to that UDS and asserts the unioned result.
//!
//! This is the ONLY e2e coverage of the `EngineServingClient` production wire
//! path — arrow-58 IPC round-trip, param inlining, and the DataFusion union of
//! file + inline rows all exercised together.

use std::time::Duration;

use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use engine::query::EngineQueryService;
use engine_wire::pb::engine_query_server::EngineQueryServer;
use query_api::engine_client::EngineServingClient;
use query_api::serving::{ServingEngine, SqlValue};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

/// Spawn an `EngineQueryService` on a temp UDS. Returns the socket path (alive
/// while the returned `tempfile::TempDir` is held).
async fn spawn_query_server(pool: sqlx::PgPool) -> (tempfile::TempDir, String) {
    let sock_dir = tempfile::tempdir().expect("socket dir");
    let sock_path = sock_dir.path().join("engine_query.sock");
    let sock_str = sock_path.to_string_lossy().to_string();

    let catalog = IcebergCatalog::new(pool);
    let svc = EngineQueryService { catalog };

    let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind uds");
    let incoming = UnixListenerStream::new(listener);

    tokio::spawn(async move {
        Server::builder()
            .add_service(EngineQueryServer::new(svc))
            .serve_with_incoming(incoming)
            .await
            .ok();
    });

    // Small pause so the server is ready to accept.
    tokio::time::sleep(Duration::from_millis(20)).await;

    (sock_dir, sock_str)
}

/// Seed the `sales.orders` table with file-backed rows (ids 0,1,2) and one live
/// inline row (id 100, name "row100"), then query over the engine wire and assert
/// the exact unioned result.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_wire_unions_file_and_inline() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    // Seed: 3 file rows (ids 0,1,2) then 1 live inline row (id 100).
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

    // Boot the engine query service over a UDS.
    let (_sock_dir, sock) = spawn_query_server(pool).await;

    // Connect an `EngineServingClient` to the UDS.
    let client = EngineServingClient::connect(&sock)
        .await
        .expect("connect EngineServingClient");

    // Fetch rows: SELECT id FROM sales.orders ORDER BY id
    let sql = r#"SELECT "id" FROM "sales"."orders" ORDER BY "id""#;
    let rows = client
        .fetch_rows(sql, &[])
        .await
        .expect("fetch_rows over engine wire");

    // Expect [0, 1, 2, 100] — 3 file rows + 1 inline row, unioned.
    let ids: Vec<i64> = rows
        .rows
        .iter()
        .map(|r| match &r[0] {
            SqlValue::Int(n) => *n,
            other => panic!("expected SqlValue::Int, got {other:?}"),
        })
        .collect();
    assert_eq!(
        ids,
        vec![0, 1, 2, 100],
        "engine wire: file rows + inline row must be unioned in id order"
    );
}
