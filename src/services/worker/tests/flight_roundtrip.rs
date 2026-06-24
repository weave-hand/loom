//! Flight round-trip e2e: the engine serves Arrow Flight alongside the
//! control-plane service; a zero-pool `FlightTableClient` streams a known file
//! set and reconstructs the exact rows and schema.
//!
//! The test harness has Postgres (it's a fixture test). Postgres is used ONLY
//! to land files and resolve file paths; the streaming path goes through
//! `FlightTableClient` with no Postgres connection on the client side.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, DatasetId, EventType, LineageEvent, RunId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use engine::flight::FlightDataService;
use engine::service::EngineControlService;
use engine_wire::flight::{FlightTableClient, FlightTicket};
use engine_wire::pb::engine_control_server::EngineControlServer;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use tonic::transport::Server;

// ---- helpers ---------------------------------------------------------------

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// Encode `ids` as an Arrow-57 IPC stream body (single `id: Int64` column).
fn ipc_body(ids: &[i64]) -> Vec<u8> {
    use arrow_ipc::writer::StreamWriter;
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .expect("batch");
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

fn lineage(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "flight-roundtrip-test" }),
    }
}

async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = HashMap::new();
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

/// Spawn an engine on a tmpdir UDS serving BOTH EngineControl and Arrow Flight.
/// Returns (sock_dir, sock_path_string) — caller must hold `sock_dir` alive.
async fn spawn_server(fx: &PgFixture, db: &str) -> (tempfile::TempDir, String) {
    let wh = tempfile::tempdir().expect("warehouse dir");
    let sock_dir = tempfile::tempdir().expect("socket dir");
    let sock_path = sock_dir.path().join("engine.sock");
    let sock_str = sock_path.to_string_lossy().to_string();

    let pool = fx.pool_for(db).await;
    let cp = control_plane_postgres::PgControlPlane::new(pool.clone(), Duration::from_millis(5000));
    let wh_str = wh.path().display().to_string();
    let control_catalog = make_catalog(fx.pg_dsn(db), &wh_str).await;
    let flight_catalog = make_catalog(fx.pg_dsn(db), &wh_str).await;

    let svc = EngineControlService {
        cp,
        catalog: control_catalog,
        pool: pool.clone(),
    };
    let flight_svc = FlightDataService {
        catalog: flight_catalog,
        pool,
    };

    let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind uds");
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);

    tokio::spawn(async move {
        let _wh = wh; // keep warehouse tempdir alive for the task lifetime
        Server::builder()
            .add_service(EngineControlServer::new(svc))
            .add_service(FlightServiceServer::new(flight_svc))
            .serve_with_incoming(incoming)
            .await
            .ok();
    });

    // Small pause so the server is ready to accept.
    tokio::time::sleep(Duration::from_millis(20)).await;

    (sock_dir, sock_str)
}

// ---- test ------------------------------------------------------------------

/// Zero-pool Flight round-trip: the test harness has Postgres (fixture), uses
/// it only to land files and resolve paths; the actual streaming goes over
/// Arrow Flight from `FlightTableClient` with no Postgres on the client side.
///
/// Two `land` calls produce (at least) one Parquet file each. The client
/// fetches the whole file set in one `do_get` and reconstructs exactly 5 rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_streams_a_file_set_and_reconstructs_exact_rows() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // A separate catalog instance for the test-side `land` calls. Its warehouse
    // tempdir differs from the server's (spawn_server builds its own pair), and
    // that is safe: all three catalogs share the same Postgres DSN, where the
    // iceberg mirror stores each data file as an absolute `file://` path. The
    // server's `flight_catalog` resolves those absolute paths via `LocalFsStorage`
    // (which ignores the configured warehouse on read), so it reads exactly the
    // Parquet files this catalog wrote under `wh`. The warehouse only matters for
    // *where new files are written*, never for reads.
    let wh = tempfile::tempdir().expect("warehouse dir");
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let (_sock_dir, sock) = spawn_server(&fx, &db).await;

    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };

    // Land ids [1, 2, 3] into the first Parquet file (inline_byte_limit = 0
    // forces a real Parquet write rather than an inline-only write).
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(&[1, 2, 3]),
        0,        // inline_byte_limit = 0 → always write real Parquet
        i64::MAX, // flush_byte_threshold → no auto-enqueue
        lineage(RunId(uuid::Uuid::new_v4()), &table),
    )
    .await
    .expect("land batch 1");

    // Land ids [4, 5] into a second Parquet file.
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(&[4, 5]),
        0,
        i64::MAX,
        lineage(RunId(uuid::Uuid::new_v4()), &table),
    )
    .await
    .expect("land batch 2");

    // Resolve the live file paths at the current snapshot via the IcebergCatalog mirror.
    // MUST use control_plane_core::Catalog trait for `current_snapshot`.
    let ice = IcebergCatalog::new(pool.clone());
    let snap = ice.current_snapshot(&table).await.expect("snapshot");
    let files: Vec<String> = ice
        .files_with_stats(&table, snap.id)
        .await
        .expect("files")
        .into_iter()
        .map(|f| f.path)
        .collect();
    assert!(
        !files.is_empty(),
        "at least one Parquet file must have been written"
    );

    // Zero-pool Flight client: no Postgres connection on the client path.
    let client = FlightTableClient::connect(&sock).await.expect("connect");
    let batches = client
        .fetch(FlightTicket {
            schema: table.schema.clone(),
            name: table.name.clone(),
            files,
        })
        .await
        .expect("flight fetch");

    // Exact row count: 3 + 2 = 5.
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 5, "expected 5 rows total, got {total}");

    // Schema fidelity: the stream carries the table's Arrow schema with an `id` field.
    assert!(
        batches[0]
            .schema()
            .fields()
            .iter()
            .any(|f| f.name() == "id"),
        "reconstructed batches must carry an 'id' field"
    );

    // Optionally assert the exact ids in sorted order.
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column must be Int64Array")
                .values()
                .iter()
                .copied()
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 4, 5], "exact ids must round-trip");
}
