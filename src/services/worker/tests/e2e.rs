//! End-to-end test: inline writes past the threshold → Spec-1 producer enqueues
//! a `flush_table` job → worker drains it over the engine-wire → table becomes
//! file-backed.
//!
//! Drive form: bare dequeue→handle_flush→complete cycle through `GrpcQueueClient`
//! (avoids the awkwardness of cancelling `Worker::run` after exactly one job),
//! which still exercises the full wire path end-to-end.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, DatasetId, EventType, LineageEvent, PageReq, Queue, RunId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_inline::inline_append;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use engine::service::EngineControlService;
use engine_wire::client::GrpcQueueClient;
use engine_wire::pb::engine_control_server::EngineControlServer;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use tonic::transport::Server;
use worker::handler::handle_flush;

// ---- helpers ---------------------------------------------------------------

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

fn inline_batch(ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids.to_vec()))]).expect("batch")
}

fn inline_lineage(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "e2e-worker-test" }),
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

/// Spawn an `EngineControlService` on a tmpdir UDS. Returns the sock_dir (held
/// alive by the caller) and the socket path string.
async fn spawn_server(fx: &PgFixture, db: &str) -> (tempfile::TempDir, String) {
    let wh = tempfile::tempdir().expect("warehouse dir");
    let sock_dir = tempfile::tempdir().expect("socket dir");
    let sock_path = sock_dir.path().join("engine.sock");
    let sock_str = sock_path.to_string_lossy().to_string();

    let pool = fx.pool_for(db).await;
    let cp = control_plane_postgres::PgControlPlane::new(pool.clone(), Duration::from_millis(5000));
    let catalog = make_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;

    let svc = EngineControlService { cp, catalog, pool };
    let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind uds");
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);

    tokio::spawn(async move {
        let _wh = wh; // keep warehouse tempdir alive for the task lifetime
        Server::builder()
            .add_service(EngineControlServer::new(svc))
            .serve_with_incoming(incoming)
            .await
            .ok();
    });

    // Small pause so the server is ready to accept.
    tokio::time::sleep(Duration::from_millis(20)).await;

    (sock_dir, sock_str)
}

// ---- test ------------------------------------------------------------------

/// Full e2e: inline_append with a byte threshold → job enqueued by Spec-1
/// producer → GrpcQueueClient dequeues → handle_flush flushes over the wire →
/// client completes → table is file-backed and queue is empty.
///
/// Drive form: bare `dequeue → handle_flush → complete` cycle (not `Worker::run`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_threshold_enqueues_and_worker_flushes() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // Stand up the engine server (owns the second pool + catalog instance).
    let (_sock_dir, sock) = spawn_server(&fx, &db).await;

    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };
    let run = RunId(uuid::Uuid::new_v4());

    // inline_append with threshold=1 so any inline bytes cross it →
    // atomically enqueues exactly ONE flush_table job.
    inline_append(
        &pool,
        &table,
        &columns(),
        &inline_batch(&[1, 2, 3]),
        inline_lineage(run, &table),
        Some(1), // 1-byte threshold — any inline write crosses this
    )
    .await
    .expect("inline_append with threshold");

    // Connect a GrpcQueueClient to the engine.
    let client = GrpcQueueClient::connect(&sock).await.expect("connect");

    // Dequeue the flush_table job that Spec-1 enqueued.
    let job = client
        .dequeue(
            &[control_plane_core::FLUSH_JOB_KIND.to_string()],
            "e2e-worker",
        )
        .await
        .expect("dequeue")
        .expect("a flush_table job must be present after inline_append with threshold");

    assert_eq!(
        job.kind,
        control_plane_core::FLUSH_JOB_KIND,
        "dequeued job kind must be flush_table"
    );

    let job_id = job.id;

    // Run the handler — calls flush_table over the wire, draining inline rows
    // into a Parquet file.
    handle_flush(client.clone(), job)
        .await
        .expect("handle_flush must succeed");

    // Complete the job. (In the full Worker::run loop this is done automatically
    // when the handler returns Ok; in this bare-cycle drive we call it explicitly.)
    client.complete(job_id).await.expect("complete job");

    // Assert: table is now file-backed.
    let ice = IcebergCatalog::new(pool.clone());
    let cur = ice
        .current_snapshot(&table)
        .await
        .expect("current snapshot");
    let files = ice
        .files(&table, cur.id, PageReq::unbounded())
        .await
        .expect("files");
    assert!(
        !files.items.is_empty(),
        "table must have at least one Parquet file after worker flush"
    );

    // No live inline rows remain.
    let inline = ice
        .inline_parquet(&table, cur.id)
        .await
        .expect("inline_parquet");
    assert!(
        inline.is_none(),
        "inline rows must be retired after worker flush"
    );

    // Assert: queue is empty after complete.
    let dequeue_again = client
        .dequeue(
            &[control_plane_core::FLUSH_JOB_KIND.to_string()],
            "e2e-worker",
        )
        .await
        .expect("dequeue after complete");
    assert!(
        dequeue_again.is_none(),
        "queue must be empty after job completed"
    );
}
