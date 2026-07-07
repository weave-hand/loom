//! Engine-wire compaction e2e (Iceberg): land several small files, run the worker's
//! compact handler over the wire, assert the small files coalesce, the row set is
//! preserved, and a prior snapshot still time-travels. Plus a no-op (<2 small files).

use loom_test_flight::{EngineOpts, spawn_engine_uds};
use loom_test_seed::local_sql_catalog;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    COMPACT_JOB_KIND, Catalog, ColumnSpec, CompactJob, DatasetId, EventType, Job, JobId,
    LineageEvent, RunId, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::{FlightTableClient, FlightTicket};
use store_config::{ObjectStoreConfig, build_write_store};
use worker::compact::{CompactCtx, handle_compact};

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
        payload: serde_json::json!({ "source": "compact-e2e-test" }),
    }
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
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // Shared warehouse dir — used by the server engine AND the worker write store.
    // Both must agree on the same physical root so the absolute file:// paths registered
    // by the worker are resolvable by the engine's Iceberg FileIO on Flight reads.
    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();

    // Catalog for seeding (land calls).
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh_str).await;

    let eng = spawn_engine_uds(
        fx,
        &db,
        &wh_str,
        EngineOpts {
            control: true,
            ..EngineOpts::default()
        },
    )
    .await;

    let acc = TableRef {
        schema: "main".into(),
        name: "acc".into(),
    };

    // Land 3 small files (1 row each). inline_byte_limit=0 forces real Parquet writes.
    for id in [1_i64, 2, 3] {
        let (schema, batches) = ipc_body(&[id]);
        land(
            &pool,
            &catalog,
            &acc,
            &columns(),
            schema,
            batches,
            InlineLimits {
                inline_byte_limit: 0,           // always write real Parquet
                flush_byte_threshold: i64::MAX, // no auto-enqueue
            },
            lineage(RunId(uuid::Uuid::new_v4()), &acc),
            None,
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

    let control = GrpcQueueClient::connect(&eng.sock)
        .await
        .expect("connect control");
    let flight = FlightTableClient::connect(&eng.sock)
        .await
        .expect("connect flight");

    let ctx = CompactCtx {
        control,
        flight,
        write,
        threshold_bytes: 10 * 1024 * 1024, // 10 MiB — all 1-row files qualify
        write_cfg: datafusion_io::WriteConfig::default(),
        worker_tuning: loom_config::WorkerTuning::default(),
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
    let flight2 = FlightTableClient::connect(&eng.sock)
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

/// Worker compact e2e (mixed sizes): land three small files plus one large file, pin
/// the threshold to the large file's exact size (`file_size_bytes < threshold` is false
/// at equality, so it is excluded), and assert the small files coalesce into one while
/// the large file stays live with its path unchanged and the full row set survives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_leaves_large_files_untouched() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();

    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh_str).await;

    let eng = spawn_engine_uds(
        fx,
        &db,
        &wh_str,
        EngineOpts {
            control: true,
            ..EngineOpts::default()
        },
    )
    .await;

    let mixed = TableRef {
        schema: "main".into(),
        name: "mixed".into(),
    };

    // Three 1-row (small) files.
    for id in [1_i64, 2, 3] {
        let (schema, batches) = ipc_body(&[id]);
        land(
            &pool,
            &catalog,
            &mixed,
            &columns(),
            schema,
            batches,
            InlineLimits {
                inline_byte_limit: 0,
                flush_byte_threshold: i64::MAX,
            },
            lineage(RunId(uuid::Uuid::new_v4()), &mixed),
            None,
        )
        .await
        .expect("land small");
    }

    // One 200-row (large) file.
    let big_ids: Vec<i64> = (0..200).collect();
    let (schema, batches) = ipc_body(&big_ids);
    land(
        &pool,
        &catalog,
        &mixed,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), &mixed),
        None,
    )
    .await
    .expect("land large");

    // Verify 4 files were seeded; the largest pins the compaction threshold.
    let ice = IcebergCatalog::new(pool.clone());
    let before = ice.current_snapshot(&mixed).await.expect("snapshot before");
    let files_before = ice
        .files_with_stats(&mixed, before.id)
        .await
        .expect("files before");
    assert_eq!(files_before.len(), 4, "four files before compaction");
    let large = files_before
        .iter()
        .max_by_key(|f| f.file_size_bytes)
        .expect("a largest file")
        .clone();

    // Build CompactCtx pointing at the same warehouse, threshold pinned to the
    // large file's exact size so only the three small files qualify.
    let mut env_map = HashMap::new();
    env_map.insert("LOOM_WAREHOUSE_URI".to_string(), format!("file://{wh_str}"));
    let store_cfg = ObjectStoreConfig::parse_from_env(&env_map).expect("store config");
    let write = Arc::new(build_write_store(&store_cfg).expect("write store"));

    let control = GrpcQueueClient::connect(&eng.sock)
        .await
        .expect("connect control");
    let flight = FlightTableClient::connect(&eng.sock)
        .await
        .expect("connect flight");

    let ctx = CompactCtx {
        control,
        flight,
        write,
        threshold_bytes: large.file_size_bytes,
        write_cfg: datafusion_io::WriteConfig::default(),
        worker_tuning: loom_config::WorkerTuning::default(),
    };

    handle_compact(&ctx, make_compact_job("main", "mixed"))
        .await
        .expect("compact");

    // After: the large file is still live (path unchanged) plus one coalesced file.
    let head = ice.current_snapshot(&mixed).await.expect("snapshot after");
    let after = ice
        .files_with_stats(&mixed, head.id)
        .await
        .expect("files after");
    assert_eq!(after.len(), 2, "large file untouched + one coalesced file");
    assert!(
        after.iter().any(|f| f.path == large.path),
        "the large file is left live with its path unchanged"
    );

    // Row set preserved: 3 small + 200 large = 203.
    let total: i64 = after.iter().map(|f| f.record_count).sum();
    assert_eq!(total, 203, "compaction over a mixed set preserves all rows");
}
