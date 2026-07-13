//! End-to-end test: inline writes past the threshold → Spec-1 producer enqueues
//! a `flush_table` job → worker drains it over the engine-wire → table becomes
//! file-backed.
//!
//! Drive form: bare dequeue→handle_flush→complete cycle through `GrpcQueueClient`
//! (avoids the awkwardness of cancelling `Worker::run` after exactly one job),
//! which still exercises the full wire path end-to-end.

use loom_test_seed::local_sql_catalog;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlane, DatasetId, EventType, GC_JOB_KIND, LineageEvent, NewJob,
    PageReq, Queue, RunId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_inline::inline_append;
use control_plane_postgres::iceberg_landing::{InlineLimits, land, overwrite_parquet_snapshot};
use engine::flight::FlightDataService;
use engine::service::EngineControlService;
use engine_serving::IcebergActionWriter;
use engine_wire::client::GrpcQueueClient;
use engine_wire::pb::engine_control_server::EngineControlServer;
use loom_test_flight::test_write_store;
use tonic::transport::Server;
use worker::handler::{handle_flush, handle_gc};

/// A schema + batch of `rows` rows (`id: long` = `0..rows`), for `land`. `land`
/// now takes pre-decoded batches, so build these directly rather than
/// round-tripping through an Arrow IPC encode/decode.
fn ipc_body(rows: i64) -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let b = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .expect("batch");
    (schema, vec![b])
}

/// Strip a `file://` URL to a local filesystem path.
fn local_path(file_url: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(file_url.strip_prefix("file://").unwrap_or(file_url))
}

/// Backdate snapshot `snap_id`'s `snapshot_time` so it ages out of any window.
async fn age_snapshot(pool: &sqlx::PgPool, snap_id: i64) {
    let old = time::OffsetDateTime::now_utc() - time::Duration::days(365);
    sqlx::query("update iceberg_mirror.snapshot set snapshot_time = $1 where snapshot_id = $2")
        .bind(old)
        .bind(snap_id)
        .execute(pool)
        .await
        .expect("age snapshot");
}

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

/// Spawn an `EngineControlService` on a tmpdir UDS. Returns the sock_dir (held
/// alive by the caller) and the socket path string.
async fn spawn_server(fx: &PgFixture, db: &str) -> (tempfile::TempDir, String) {
    let wh = tempfile::tempdir().expect("warehouse dir");
    let sock_dir = tempfile::tempdir().expect("socket dir");
    let sock_path = sock_dir.path().join("engine.sock");
    let sock_str = sock_path.to_string_lossy().to_string();

    let pool = fx.pool_for(db).await;
    let cp = control_plane_postgres::PgControlPlane::new(pool.clone(), Duration::from_millis(5000));
    let wh_str = wh.path().display().to_string();
    let control_catalog = Arc::new(local_sql_catalog(fx.pg_dsn(db), &wh_str).await);
    let flight_catalog = Arc::new(local_sql_catalog(fx.pg_dsn(db), &wh_str).await);
    let writer_catalog = local_sql_catalog(fx.pg_dsn(db), &wh_str).await;
    let writer = IcebergActionWriter::new(
        Arc::new(writer_catalog),
        pool.clone(),
        16 * 1024 * 1024,
        i64::MAX,
    );
    let write_store = test_write_store(&wh_str);

    let svc = EngineControlService {
        cp: cp.clone(),
        catalog: control_catalog,
        pool: pool.clone(),
        retention: Duration::from_secs(7 * 24 * 3600),
        writer,
        flush_byte_threshold: i64::MAX,
        write_store,
        orphan_sweep_grace: Duration::from_secs(24 * 3600),
        serving_store: None,
    };
    let flight_svc = FlightDataService {
        catalog: flight_catalog,
        serving_catalog: control_plane_postgres::iceberg_catalog::IcebergCatalog::new(pool.clone()),
        serving_store: None,
        pool,
        cp,
    };
    let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind uds");
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);

    tokio::spawn(async move {
        let _wh = wh; // keep warehouse tempdir alive for the task lifetime
        drop(
            Server::builder()
                .add_service(EngineControlServer::new(svc))
                .add_service(FlightServiceServer::new(flight_svc))
                .serve_with_incoming(incoming)
                .await,
        );
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
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // Stand up the engine server (owns the second pool + catalog instance).
    let (_sock_dir, sock) = spawn_server(fx, &db).await;

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
        None,
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
    handle_flush(client.clone(), loom_config::WorkerTuning::default(), job)
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
        .inline_live_batch(&table, cur.id)
        .await
        .expect("inline_live_batch");
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

/// At-least-once redelivery: two workers dispatch the SAME flush_table job
/// concurrently. The per-table advisory lock makes the duplicate a safe no-op —
/// the rows are written exactly once. Holds under any interleaving, so non-flaky.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_dispatch_flush_is_idempotent_over_the_wire() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let (_sock_dir, sock) = spawn_server(fx, &db).await;

    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };
    let run = RunId(uuid::Uuid::new_v4());

    // Land three inline rows directly (threshold None — we drive the duplicate
    // dispatch ourselves rather than through the queue; two flush RPCs of the same
    // table *is* the same job delivered twice).
    inline_append(
        &pool,
        &table,
        &columns(),
        &inline_batch(&[1, 2, 3]),
        inline_lineage(run, &table),
        None,
        None,
    )
    .await
    .expect("inline_append");

    // Two concurrent flush_table RPCs for the same table (one cloned client,
    // tonic multiplexes; the engine runs each as its own handler task).
    let c1 = GrpcQueueClient::connect(&sock).await.expect("connect");
    let c2 = c1.clone();
    let (a, b) = tokio::join!(
        c1.flush_table("wh".into(), "t".into()),
        c2.flush_table("wh".into(), "t".into()),
    );
    let a = a.expect("rpc a ok");
    let b = b.expect("rpc b ok");

    // (1) Exactly one dispatch did the work; the other was a no-op.
    assert!(
        a.is_some() ^ b.is_some(),
        "exactly one flush wrote a snapshot (got {a:?}, {b:?})"
    );
    assert!(
        a.is_none() || b.is_none(),
        "the duplicate dispatch is a no-op"
    );

    // (2) Durable state == a single flush: one fileset holding exactly the 3 rows
    //     (not 6), and the inline rows retired.
    let ice = IcebergCatalog::new(pool.clone());
    let cur = ice.current_snapshot(&table).await.expect("current");
    let files = ice
        .files(&table, cur.id, PageReq::unbounded())
        .await
        .expect("files");
    let rows: i64 = files.items.iter().map(|f| f.record_count).sum();
    assert_eq!(
        rows, 3,
        "rows written exactly once across both dispatches (no double-write)"
    );
    let inline = ice
        .inline_live_batch(&table, cur.id)
        .await
        .expect("inline_live_batch");
    assert!(inline.is_none(), "inline rows retired exactly once");
}

/// Full e2e: a `gc_table` job flows through the worker's GC handler over the wire
/// (dequeue → handle_gc → engine `GcTable` RPC → reclaim primitive → complete),
/// physically deleting an aged-out end-capped Parquet object.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_job_flows_through_worker_and_reclaims_object() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let (_sock_dir, sock) = spawn_server(fx, &db).await;

    // Seed with a test-local catalog: land A (10 rows), then overwrite with B
    // (4 rows; end-caps A). The mirror records absolute file:// paths, which the
    // engine's GC deletes by path regardless of which warehouse wrote them.
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let table = TableRef {
        schema: "wh".into(),
        name: "t".into(),
    };
    let ice = IcebergCatalog::new(pool.clone());

    let (schema, batches) = ipc_body(10);
    let s1 = land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        inline_lineage(RunId(uuid::Uuid::new_v4()), &table),
        None,
    )
    .await
    .expect("land");
    let a_path = local_path(&ice.files_with_stats(&table, s1).await.expect("files@s1")[0].path);

    let s2 = overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &table,
        &columns(),
        vec![inline_batch(&[0, 1, 2, 3])],
        None,
        &[],
    )
    .await
    .expect("overwrite");
    age_snapshot(&pool, s2.0).await; // engine retention=7d → A (end=s2) reclaimable
    assert!(a_path.exists(), "object A present before gc");

    // Enqueue a gc_table job into the engine's queue.
    let cp = control_plane_postgres::PgControlPlane::new(pool.clone(), Duration::from_millis(5000));
    cp.queue()
        .enqueue(NewJob {
            kind: GC_JOB_KIND.to_string(),
            payload: serde_json::json!({ "schema": "wh", "name": "t" }),
            run_at: None,
            priority: 0,
        })
        .await
        .expect("enqueue gc job");

    // Drive it through the worker GC handler over the wire.
    let client = GrpcQueueClient::connect(&sock).await.expect("connect");
    let job = client
        .dequeue(&[GC_JOB_KIND.to_string()], "e2e-gc")
        .await
        .expect("dequeue")
        .expect("a gc_table job must be present");
    assert_eq!(job.kind, GC_JOB_KIND, "dequeued job kind must be gc_table");
    let job_id = job.id;
    handle_gc(client.clone(), loom_config::WorkerTuning::default(), job)
        .await
        .expect("handle_gc");
    client.complete(job_id).await.expect("complete");

    // Aged object A reclaimed over the wire; current data intact; queue drained.
    assert!(
        !a_path.exists(),
        "aged object A deleted by gc over the wire"
    );
    let cur = ice.current_snapshot(&table).await.expect("current");
    assert_eq!(cur.id, s2);
    let files = ice.files_with_stats(&table, s2).await.expect("files@s2");
    assert_eq!(
        files.iter().map(|f| f.record_count).sum::<i64>(),
        4,
        "live data intact after gc"
    );
    let again = client
        .dequeue(&[GC_JOB_KIND.to_string()], "e2e-gc")
        .await
        .expect("dequeue after complete");
    assert!(again.is_none(), "queue empty after gc job completed");
}
