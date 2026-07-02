//! Engine-level Flight file-ticket membership guard: boot a `FlightDataService`
//! over a UDS, land two tables A and B, and drive `do_get` with `FlightTicket`s:
//!   * positive — A's ticket naming A's live files streams A's rows;
//!   * negative — A's ticket naming B's file path is rejected (no bytes), the
//!     case that fails on `main` today.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array};
use arrow_flight::flight_service_server::FlightServiceServer;
use control_plane_core::{Catalog, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use engine::flight::FlightDataService;
use engine_wire::flight::{FlightTableClient, FlightTicket};
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

/// Capture a table's live-snapshot data-file paths from the mirror.
async fn live_files(pool: &sqlx::PgPool, table: &TableRef) -> Vec<String> {
    let cat = IcebergCatalog::new(pool.clone());
    let snap = cat.current_snapshot(table).await.expect("current_snapshot");
    cat.files_with_stats(table, snap.id)
        .await
        .expect("files_with_stats")
        .into_iter()
        .map(|f| f.path)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_ticket_naming_files_outside_live_snapshot() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let wh = tempfile::tempdir().expect("warehouse");

    let cols = vec![("id".to_string(), "long".to_string(), false)];
    let writer = IcebergWriter::new(pool.clone(), dsn);
    // Table A: one Parquet file with 3 rows (ids 0,1,2).
    writer.seed("main", "a", &cols, &[3]).await;
    // Table B: one Parquet file with 2 rows.
    writer.seed("main", "b", &cols, &[2]).await;

    let table_a = TableRef {
        schema: "main".into(),
        name: "a".into(),
    };
    let table_b = TableRef {
        schema: "main".into(),
        name: "b".into(),
    };
    let a_files = live_files(&pool, &table_a).await;
    let b_files = live_files(&pool, &table_b).await;
    assert!(
        !a_files.is_empty(),
        "table A must have at least one live file"
    );
    assert!(
        !b_files.is_empty(),
        "table B must have at least one live file"
    );

    let (_sock_dir, sock) = spawn_flight(fx, &db, &wh.path().display().to_string()).await;
    let client = FlightTableClient::connect(&sock).await.expect("connect");

    // Positive: A's ticket naming A's own live files streams A's rows.
    let ok = client
        .fetch(FlightTicket {
            schema: "main".into(),
            name: "a".into(),
            files: a_files.clone(),
        })
        .await
        .expect("positive fetch must succeed");
    let mut ids = Vec::new();
    for b in &ok {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64");
        for i in 0..col.len() {
            ids.push(col.value(i));
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1, 2], "positive ticket streams A's rows");

    // Negative: A's ticket naming B's file path is rejected (the case that fails on main).
    let err = client
        .fetch(FlightTicket {
            schema: "main".into(),
            name: "a".into(),
            files: b_files.clone(),
        })
        .await;
    let msg = format!("{}", err.expect_err("negative ticket must be rejected"));
    assert!(
        msg.contains("outside the table's live snapshot"),
        "rejection must come from the membership guard, got: {msg}"
    );

    // Negative: a bogus path is likewise rejected.
    let err2 = client
        .fetch(FlightTicket {
            schema: "main".into(),
            name: "a".into(),
            files: vec!["file:///nope/0.parquet".to_string()],
        })
        .await;
    let msg2 = format!("{}", err2.expect_err("bogus ticket must be rejected"));
    assert!(
        msg2.contains("outside the table's live snapshot"),
        "bogus ticket must hit the membership guard, got: {msg2}"
    );
}
