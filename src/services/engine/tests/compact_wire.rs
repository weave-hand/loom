//! Engine ListFiles + CompactTable over the wire: list returns live files; compact
//! expires a subset and commits a new snapshot.
//!
//! Seeds ≥2 Iceberg files via `land`, then drives `list_files` + `compact_table`
//! through `GrpcQueueClient` against a real UDS-bound EngineControlService.

use loom_test_seed::local_sql_catalog;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, DatasetId, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use engine::service::EngineControlService;
use engine_serving::IcebergActionWriter;
use engine_wire::client::GrpcQueueClient;
use engine_wire::pb::engine_control_server::EngineControlServer;
use tonic::transport::Server;

// ---- helpers ---------------------------------------------------------------

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// A schema + batch (single `id: Int64` column) of `ids`. `land` now takes
/// pre-decoded batches, so build these directly rather than round-tripping
/// through an Arrow IPC encode/decode.
fn ipc_body(ids: &[i64]) -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .expect("batch");
    (schema, vec![batch])
}

fn lineage(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "compact-wire-test" }),
    }
}

/// Spawn an `EngineControlService` on a tmpdir UDS. Returns (sock_dir, sock_path).
async fn spawn_server(fx: &PgFixture, db: &str) -> (tempfile::TempDir, String) {
    let wh = tempfile::tempdir().expect("warehouse dir");
    let sock_dir = tempfile::tempdir().expect("socket dir");
    let sock_path = sock_dir.path().join("engine.sock");
    let sock_str = sock_path.to_string_lossy().to_string();

    let pool = fx.pool_for(db).await;
    let cp = control_plane_postgres::PgControlPlane::new(pool.clone(), Duration::from_millis(5000));
    let wh_str = wh.path().display().to_string();
    let catalog = Arc::new(local_sql_catalog(fx.pg_dsn(db), &wh_str).await);
    let writer_catalog = local_sql_catalog(fx.pg_dsn(db), &wh_str).await;
    let writer = IcebergActionWriter::new(
        Arc::new(writer_catalog),
        pool.clone(),
        16 * 1024 * 1024,
        i64::MAX,
    );

    let svc = EngineControlService {
        cp,
        catalog,
        pool,
        retention: Duration::from_secs(7 * 24 * 3600),
        writer,
    };
    let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind uds");
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);

    tokio::spawn(async move {
        let _wh = wh;
        drop(
            Server::builder()
                .add_service(EngineControlServer::new(svc))
                .serve_with_incoming(incoming)
                .await,
        );
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    (sock_dir, sock_str)
}

// ---- test ------------------------------------------------------------------

/// List then compact over the wire:
/// 1. Land two small Parquet files (a:3, b:2 rows).
/// 2. `list_files` returns 2 entries.
/// 3. `compact_table` expires both + registers one synthetic coalesced file.
/// 4. `list_files` after compact returns exactly the coalesced file with record_count=5.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_files_then_compact_over_the_wire() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // Separate catalog for the seeding land() calls (same Postgres DSN).
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;

    let (_sock_dir, sock) = spawn_server(fx, &db).await;

    let table = TableRef {
        schema: "main".into(),
        name: "t".into(),
    };

    // Land batch a: 3 rows → forces a real Parquet file (inline_byte_limit = 0).
    let (schema_a, batches_a) = ipc_body(&[1, 2, 3]);
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema_a,
        batches_a,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), &table),
    )
    .await
    .expect("land a");

    // Land batch b: 2 rows → second Parquet file.
    let (schema_b, batches_b) = ipc_body(&[4, 5]);
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema_b,
        batches_b,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), &table),
    )
    .await
    .expect("land b");

    let client = GrpcQueueClient::connect(&sock).await.expect("connect");

    // Step 3: list_files → 2 entries.
    let files = client
        .list_files("main".into(), "t".into())
        .await
        .expect("list_files");
    assert_eq!(files.len(), 2, "expected 2 files after landing two batches");

    let total_rows: i64 = files.iter().map(|f| f.record_count).sum();
    assert_eq!(total_rows, 5, "total rows should be 5");

    // Step 4: compact — expire both live files, register one coalesced synthetic file.
    let expire: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
    let wh_abs = wh.path().display().to_string();
    let coalesced = vec![control_plane_core::DataFile {
        path: format!("{wh_abs}/main/t/c/part-0.parquet"),
        path_is_relative: false,
        file_format: control_plane_core::FileFormat::Parquet,
        record_count: 5,
        file_size_bytes: 200,
        column_stats: vec![],
        parquet_footer_size: None,
    }];

    let snap = client
        .compact_table("main".into(), "t".into(), expire, &coalesced)
        .await
        .expect("compact_table");
    assert!(snap.is_some(), "compaction must return a snapshot id");

    // Step 5: list again → exactly the coalesced file.
    let after = client
        .list_files("main".into(), "t".into())
        .await
        .expect("list_files after compact");
    assert_eq!(after.len(), 1, "expected exactly 1 file after compaction");
    assert_eq!(after[0].record_count, 5, "coalesced file must have 5 rows");
}
