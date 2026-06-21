//! Integration test: queue operations + flush_table round-trip over a real UDS.
//!
//! Boots an ephemeral Postgres, constructs an `EngineControlService` server
//! (bound to a tempdir unix socket), and drives it via `GrpcQueueClient`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, DatasetId, EventType, LineageEvent, NewJob, PageReq, Queue, RetryPolicy,
    RunId, TableRef,
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

// ---- helpers copied from postgres/tests/iceberg_flush.rs ------------------

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
        payload: serde_json::json!({ "source": "test" }),
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

// ---- server fixture -------------------------------------------------------

/// Spawn an `EngineControlService` on a tmpdir UDS. Returns the socket path
/// (kept alive while the returned tempdir is held).
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
        // _wh keeps the warehouse tempdir alive for the lifetime of the task.
        let _wh = wh;
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

// ---- test cases -----------------------------------------------------------

/// Case 1: enqueue a job via the fixture's PgControlPlane, dequeue + complete
/// it over the wire, then a second dequeue returns None.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dequeue_and_complete() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let (_sock_dir, sock) = spawn_server(&fx, &db).await;
    let client = GrpcQueueClient::connect(&sock).await.expect("connect");

    // Seed a job directly via the fixture's control plane.
    cp.enqueue(NewJob {
        kind: "flush_table".into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    })
    .await
    .expect("enqueue");

    // Dequeue over the wire.
    let job = client
        .dequeue(&["flush_table".to_string()], "worker-1")
        .await
        .expect("dequeue")
        .expect("should have a job");

    assert_eq!(job.kind, "flush_table");

    // Complete it.
    client.complete(job.id).await.expect("complete");

    // Second dequeue returns None.
    let none = client
        .dequeue(&["flush_table".to_string()], "worker-1")
        .await
        .expect("dequeue 2");
    assert!(none.is_none(), "queue should be empty after complete");
}

/// Case 2: fail with Retry then Abandon — no panic, Abandon makes the job terminal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fail_retry_then_abandon() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let (_sock_dir, sock) = spawn_server(&fx, &db).await;
    let client = GrpcQueueClient::connect(&sock).await.expect("connect");

    cp.enqueue(NewJob {
        kind: "flush_table".into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    })
    .await
    .expect("enqueue");

    // Dequeue + fail with Retry (very short delay so it's immediately re-available).
    let job = client
        .dequeue(&["flush_table".to_string()], "w")
        .await
        .expect("dequeue")
        .expect("job");

    client
        .fail(
            job.id,
            "transient error",
            RetryPolicy::Retry {
                delay: Duration::from_millis(0),
            },
        )
        .await
        .expect("fail retry");

    // Dequeue again (retry made it available).
    let job2 = client
        .dequeue(&["flush_table".to_string()], "w")
        .await
        .expect("dequeue after retry")
        .expect("job 2");

    // Abandon — job goes terminal.
    client
        .fail(job2.id, "permanent error", RetryPolicy::Abandon)
        .await
        .expect("fail abandon");

    // Job is now terminal; dequeue returns None.
    let none = client
        .dequeue(&["flush_table".to_string()], "w")
        .await
        .expect("dequeue after abandon");
    assert!(none.is_none(), "abandoned job must not reappear");
}

/// Case 3: await_jobs wakes when a job is enqueued.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn await_jobs_wakes_on_notify() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let (_sock_dir, sock) = spawn_server(&fx, &db).await;
    let client = GrpcQueueClient::connect(&sock).await.expect("connect");

    // Start await_jobs (5s timeout) in a background task.
    let client2 = client.clone();
    let wait_task = tokio::spawn(async move {
        client2
            .await_jobs(&["flush_table".to_string()], Duration::from_secs(5))
            .await
            .expect("await_jobs");
    });

    // After ~100ms enqueue a job to wake the listener.
    tokio::time::sleep(Duration::from_millis(100)).await;
    cp.enqueue(NewJob {
        kind: "flush_table".into(),
        payload: serde_json::json!({}),
        run_at: None,
        priority: 0,
    })
    .await
    .expect("enqueue");

    // await_jobs must return well before the 5s timeout.
    let result = tokio::time::timeout(Duration::from_secs(4), wait_task).await;
    assert!(
        result.is_ok(),
        "await_jobs should have returned before the 5s timeout"
    );
    result.unwrap().expect("task join");
}

/// Case 4: flush_table over the wire drains inline rows into a Parquet file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_over_wire() {
    let fx = PgFixture::start();
    let (_, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(&db).await;

    // Build a second catalog for the direct inline_append call.
    let direct_catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let (_sock_dir, sock) = {
        // Build the server using the same db/warehouse.
        let sock_dir = tempfile::tempdir().expect("socket dir");
        let sock_path = sock_dir.path().join("engine.sock");
        let sock_str = sock_path.to_string_lossy().to_string();

        let pool2 = fx.pool_for(&db).await;
        let cp2 =
            control_plane_postgres::PgControlPlane::new(pool2.clone(), Duration::from_millis(5000));
        let catalog2 = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

        let svc = EngineControlService {
            cp: cp2,
            catalog: catalog2,
            pool: pool2,
        };
        let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind");
        let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
        tokio::spawn(async move {
            Server::builder()
                .add_service(EngineControlServer::new(svc))
                .serve_with_incoming(incoming)
                .await
                .ok();
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        (sock_dir, sock_str)
    };

    let client = GrpcQueueClient::connect(&sock).await.expect("connect");

    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };
    let run = RunId(uuid::Uuid::new_v4());

    // Append rows as inline directly via the postgres layer.
    inline_append(
        &pool,
        &table,
        &columns(),
        &inline_batch(&[10, 20, 30]),
        inline_lineage(run, &table),
        None,
    )
    .await
    .expect("inline_append");

    // Flush over the wire.
    let snap_id = client
        .flush_table("wh".into(), "t".into())
        .await
        .expect("flush_table")
        .expect("should produce a snapshot");

    // Verify the table now has file-backed rows.
    let ice = IcebergCatalog::new(pool.clone());
    let snap = control_plane_core::SnapshotId(snap_id);
    let files = ice
        .files(&table, snap, PageReq::unbounded())
        .await
        .expect("files");
    assert!(
        !files.items.is_empty(),
        "flush_over_wire: at least one Parquet file must exist"
    );
    let flushed_rows: i64 = files.items.iter().map(|f| f.record_count).sum();
    assert_eq!(flushed_rows, 3, "flush_over_wire: exactly 3 rows flushed");

    // And no live inline rows remain.
    let inline = ice
        .inline_parquet(&table, snap)
        .await
        .expect("inline_parquet");
    assert!(
        inline.is_none(),
        "flush_over_wire: inline rows must be retired after flush"
    );

    // Suppress unused variable warning for direct_catalog
    drop(direct_catalog);
}
