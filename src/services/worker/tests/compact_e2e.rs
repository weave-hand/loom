//! Engine-wire compaction e2e (Iceberg): land several small files, run the worker's
//! compact handler over the wire, assert the small files coalesce, the row set is
//! preserved, and a prior snapshot still time-travels. Plus a no-op (<2 small files).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    COMPACT_JOB_KIND, Catalog, ColumnSpec, CompactJob, DatasetId, EventType, Job, JobId,
    LineageEvent, RunId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use engine::flight::FlightDataService;
use engine::service::EngineControlService;
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::{FlightTableClient, FlightTicket};
use engine_wire::pb::engine_control_server::EngineControlServer;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use store_config::{ObjectStoreConfig, build_write_store};
use tonic::transport::Server;
use worker::compact::{CompactCtx, handle_compact};

// ---- helpers ---------------------------------------------------------------

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// Encode `ids` as an Arrow IPC stream body (single `id: Int64` column).
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
        payload: serde_json::json!({ "source": "compact-e2e-test" }),
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

/// Spawn an engine on the given warehouse path serving BOTH EngineControl and Arrow Flight.
/// Returns (sock_dir, sock_path_string) — caller must hold `sock_dir` alive.
async fn spawn_server(fx: &PgFixture, db: &str, wh_path: &str) -> (tempfile::TempDir, String) {
    let sock_dir = tempfile::tempdir().expect("socket dir");
    let sock_path = sock_dir.path().join("engine.sock");
    let sock_str = sock_path.to_string_lossy().to_string();

    let pool = fx.pool_for(db).await;
    let cp = control_plane_postgres::PgControlPlane::new(pool.clone(), Duration::from_millis(5000));
    let control_catalog = make_catalog(fx.pg_dsn(db), wh_path).await;
    let flight_catalog = make_catalog(fx.pg_dsn(db), wh_path).await;

    let svc = EngineControlService {
        cp,
        catalog: control_catalog,
        pool: pool.clone(),
    };
    let flight_svc = FlightDataService {
        catalog: flight_catalog,
        serving_catalog: IcebergCatalog::new(pool.clone()),
        serving_store: None,
        pool,
    };

    let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind uds");
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);

    tokio::spawn(async move {
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

fn make_compact_job(schema: &str, name: &str) -> Job {
    Job {
        id: JobId(uuid::Uuid::new_v4()),
        kind: COMPACT_JOB_KIND.to_string(),
        payload: serde_json::to_value(CompactJob {
            schema: schema.into(),
            name: name.into(),
        })
        .unwrap(),
        attempts: 0,
        run_at: time::OffsetDateTime::now_utc(),
    }
}

// ---- test ------------------------------------------------------------------

/// Worker compact e2e: land three small Iceberg files, run handle_compact over the
/// wire (ListFiles -> Flight read -> write -> CompactTable), assert the small files
/// coalesce into one, the row set is preserved, the coalesced file is Flight-readable
/// (proves the registered absolute path is resolvable), time travel works, and a
/// second run is a no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_compacts_small_files_over_the_wire() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // Shared warehouse dir — used by the server engine AND the worker write store.
    // Both must agree on the same physical root so the absolute file:// paths registered
    // by the worker are resolvable by the engine's Iceberg FileIO on Flight reads.
    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();

    // Catalog for seeding (land calls).
    let catalog = make_catalog(fx.pg_dsn(&db), &wh_str).await;

    let (_sock_dir, sock) = spawn_server(&fx, &db, &wh_str).await;

    let acc = TableRef {
        schema: "main".into(),
        name: "acc".into(),
    };

    // Land 3 small files (1 row each). inline_byte_limit=0 forces real Parquet writes.
    for id in [1_i64, 2, 3] {
        land(
            &pool,
            &catalog,
            &acc,
            &columns(),
            &ipc_body(&[id]),
            0,        // inline_byte_limit = 0 -> always write real Parquet
            i64::MAX, // flush_byte_threshold -> no auto-enqueue
            lineage(RunId(uuid::Uuid::new_v4()), &acc),
        )
        .await
        .expect("land");
    }

    // Verify 3 files were seeded.
    let ice = IcebergCatalog::new(pool.clone());
    let before = ice.current_snapshot(&acc).await.expect("snapshot before");
    let files_before = ice
        .files_with_stats(&acc, before.id)
        .await
        .expect("files before");
    assert_eq!(files_before.len(), 3, "three small files before compaction");

    // Build CompactCtx pointing at the same warehouse so the worker writes there.
    let mut env_map = HashMap::new();
    env_map.insert("LOOM_WAREHOUSE_URI".to_string(), format!("file://{wh_str}"));
    let store_cfg = ObjectStoreConfig::parse_from_env(&env_map).expect("store config");
    let write = Arc::new(build_write_store(&store_cfg).expect("write store"));

    let control = GrpcQueueClient::connect(&sock)
        .await
        .expect("connect control");
    let flight = FlightTableClient::connect(&sock)
        .await
        .expect("connect flight");

    let ctx = CompactCtx {
        control,
        flight,
        write,
        threshold_bytes: 10 * 1024 * 1024, // 10 MiB — all 1-row files qualify
    };

    // Run the compaction handler.
    handle_compact(&ctx, make_compact_job("main", "acc"))
        .await
        .expect("compact");

    // Assert: coalesced to 1 file, row count preserved.
    let head = ice.current_snapshot(&acc).await.expect("snapshot after");
    let after = ice
        .files_with_stats(&acc, head.id)
        .await
        .expect("files after");
    assert_eq!(after.len(), 1, "three small files coalesced into one");

    let total: i64 = after.iter().map(|f| f.record_count).sum();
    assert_eq!(total, 3, "row set preserved");

    // 4b. CRITICAL: read the coalesced file's rows back over Flight.
    // This proves the registered absolute path ({root_url}/{schema}/{name}/{rel}) is
    // resolvable by the engine's Iceberg FileIO — a path-scheme or layout mismatch
    // would cause this to error, not just fail the count assert.
    let coalesced_path = after[0].path.clone();
    let flight2 = FlightTableClient::connect(&sock)
        .await
        .expect("connect flight 2");
    let batches = flight2
        .fetch(FlightTicket {
            schema: acc.schema.clone(),
            name: acc.name.clone(),
            files: vec![coalesced_path],
        })
        .await
        .expect("coalesced file is Flight-readable");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "coalesced file streams back the full row set");

    // Time travel: prior snapshot still lists the three originals.
    let files_then = ice
        .files_with_stats(&acc, before.id)
        .await
        .expect("files at prior snapshot");
    assert_eq!(
        files_then.len(),
        3,
        "prior snapshot retains the original three files (time travel)"
    );

    // Second run: only 1 file remains -> no-op (< 2 small files).
    handle_compact(&ctx, make_compact_job("main", "acc"))
        .await
        .expect("second compact no-op");
    let head2 = ice
        .current_snapshot(&acc)
        .await
        .expect("snapshot after no-op");
    assert_eq!(head2.id, head.id, "no-op created no new snapshot");
}
