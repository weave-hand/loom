//! Acceptance e2e for the compaction auto-trigger: landing small files past a
//! configured cutoff auto-enqueues a `compact_table` job with NO operator POST,
//! the worker drains it over the engine wire and converges the files,
//! compaction's own commit does NOT re-trigger, and a fresh batch of writes
//! re-arms the trigger. Mirrors `compact_e2e.rs`'s harness (engine over UDS,
//! `local_sql_catalog`, `CompactCtx` + `handle_compact`) and
//! `control-plane/postgres/tests/compact_trigger.rs`'s `available_jobs` helper,
//! with two changes: the catalog is built `with_compact_trigger(..)`, and no
//! job is ever enqueued by hand.

use loom_test_flight::{EngineOpts, spawn_engine_uds};
use loom_test_seed::local_sql_catalog;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    COMPACT_JOB_KIND, Catalog, ColumnSpec, DatasetId, EventType, LineageEvent, Queue, RunId,
    TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_compact::CompactTriggerCfg;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::FlightTableClient;
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

/// A schema + batch (single `id: Int64` column) of `ids`. `land` takes
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
        payload: serde_json::json!({ "source": "compact-auto-e2e-test" }),
    }
}

fn small_limits() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: 0,           // always write real Parquet
        flush_byte_threshold: i64::MAX, // no flush auto-enqueue
    }
}

/// Land one 1-row Parquet file per id in `ids` against `table` through `catalog`.
/// No manual job enqueue anywhere in this helper — any `compact_table` job that
/// shows up afterward came from the auto-trigger.
async fn land_smalls(
    pool: &sqlx::PgPool,
    catalog: &control_plane_postgres::iceberg_sql_catalog::SqlCatalog,
    table: &TableRef,
    ids: &[i64],
) {
    for id in ids {
        let (schema, batches) = ipc_body(&[*id]);
        land(
            pool,
            catalog,
            table,
            &columns(),
            schema,
            batches,
            small_limits(),
            lineage(RunId(uuid::Uuid::new_v4()), table),
            None,
        )
        .await
        .expect("land");
    }
}

async fn available_jobs(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "select count(*) from queue.jobs where kind = $1 and state = 'available'",
    )
    .bind(COMPACT_JOB_KIND)
    .fetch_one(pool)
    .await
    .unwrap()
}

// ---- test ------------------------------------------------------------------

/// Full acceptance flow: auto-enqueue on crossing, drain over the wire, no
/// re-trigger from compaction's own commit, and re-arm on a fresh batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_trigger_drains_and_rearms_over_the_wire() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    // Shared warehouse dir — used by the server engine AND the worker write store.
    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();

    // Cutoff matches `ctx.threshold_bytes` below, so the same files that
    // qualify for compaction also qualify as "small" for the trigger.
    let threshold_bytes: i64 = 10 * 1024 * 1024; // 10 MiB — all 1-row files qualify

    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh_str)
        .await
        .with_compact_trigger(CompactTriggerCfg {
            small_file_bytes: threshold_bytes,
            min_small_files: 3,
        });

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

    // ---- Step 1: land 3 small files with NO manual enqueue anywhere. ------
    land_smalls(&pool, &catalog, &acc, &[1, 2, 3]).await;

    assert_eq!(
        available_jobs(&pool).await,
        1,
        "landing 3 small files auto-enqueues exactly one compact_table job, \
         with no operator POST"
    );

    // ---- Step 2: dequeue the auto-enqueued job over the engine wire and run
    // handle_compact. -------------------------------------------------------
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

    let job = control
        .dequeue(&[COMPACT_JOB_KIND.to_string()], "auto-e2e-worker")
        .await
        .expect("dequeue")
        .expect("the auto-enqueued compact_table job must be present");
    assert_eq!(
        job.kind, COMPACT_JOB_KIND,
        "dequeued job kind must be compact_table"
    );
    let job_id = job.id;

    let ctx = CompactCtx {
        control: control.clone(),
        flight,
        write,
        threshold_bytes,
        write_cfg: datafusion_io::WriteConfig::default(),
        worker_tuning: loom_config::WorkerTuning::default(),
    };

    handle_compact(&ctx, job).await.expect("compact");
    control
        .complete(job_id)
        .await
        .expect("complete compact job");

    // ---- Step 3: post-compaction assertions. ------------------------------
    let ice = IcebergCatalog::new(pool.clone());
    let head = ice.current_snapshot(&acc).await.expect("snapshot after");
    let after = ice
        .files_with_stats(&acc, head.id)
        .await
        .expect("files after");
    assert_eq!(after.len(), 1, "three small files coalesced into one");

    let total: i64 = after.iter().map(|f| f.record_count).sum();
    assert_eq!(total, 3, "row set preserved across compaction");

    assert!(
        after[0].file_size_bytes < threshold_bytes,
        "the coalesced file is itself still under the cutoff, so fewer than \
         min_small_files small files remain live — the re-arm check below is \
         meaningful rather than trivially satisfied by a still-crossed count"
    );

    assert_eq!(
        available_jobs(&pool).await,
        0,
        "compaction's own commit did not re-trigger the auto-enqueue"
    );

    // ---- Step 4: land 3 MORE small files -> the trigger re-arms. ----------
    land_smalls(&pool, &catalog, &acc, &[4, 5, 6]).await;

    assert_eq!(
        available_jobs(&pool).await,
        1,
        "a fresh batch of small-file writes re-arms the trigger"
    );
}
