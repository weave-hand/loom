//! End-to-end proof of the scheduled-maintenance surface (`road-scheduled-maintenance-jobs`):
//! a `queue.schedule` cron row fires (`Queue::fire_due_job_schedules`) and the
//! zero-pool worker drains the resulting job over the engine wire, for both
//! schedulable job kinds — `gc_table` and `compact_table`.
//!
//! Drive form: bare `dequeue -> handle -> complete` through `GrpcQueueClient`
//! (the `e2e.rs`/`compact_auto_e2e.rs` template), avoiding `Worker::run`'s
//! cancellation machinery. The engine-wire spawn uses the shared
//! `loom_test_flight::spawn_engine_uds` harness.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    COMPACT_JOB_KIND, Catalog, ColumnSpec, DatasetId, EventType, GC_JOB_KIND, JobSchedule,
    LineageEvent, ORPHAN_SWEEP_JOB_KIND, Queue, RunId, TableRef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::FlightTableClient;
use loom_test_flight::{EngineOpts, spawn_engine_uds};
use loom_test_seed::local_sql_catalog;
use store_config::{ObjectStoreConfig, build_write_store};
use worker::compact::{CompactCtx, handle_compact};
use worker::handler::{handle_gc, handle_sweep_orphans};

// ---- helpers ---------------------------------------------------------------

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// A schema + single-batch `RecordBatch` of `ids` (`id: long`), for `land`.
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
        payload: serde_json::json!({ "source": "scheduled-maintenance-e2e-test" }),
    }
}

/// Real-Parquet landing limits (no inline rows, no auto-flush) — every `land`
/// call produces one physical file, matching `compact_auto_e2e.rs`'s harness.
fn small_limits() -> InlineLimits {
    InlineLimits {
        inline_byte_limit: 0,
        flush_byte_threshold: i64::MAX,
    }
}

// ---- gc_table leg -----------------------------------------------------------

/// A `gc_table` schedule fires exactly once at its due probe, is not due
/// before then, does not re-fire once advanced, and the enqueued job drains
/// end-to-end through `handle_gc` over the engine wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_schedule_fires_and_worker_drains_over_the_wire() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_millis(5000));

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

    let table = TableRef {
        schema: "main".into(),
        name: "gc_target".into(),
    };
    let (schema, batches) = ipc_body(&[1, 2, 3]);
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        small_limits(),
        lineage(RunId(uuid::Uuid::new_v4()), &table),
        None,
    )
    .await
    .expect("land gc_target");

    let gc_payload = serde_json::json!({"schema": "main", "name": "gc_target"});
    let now = time::OffsetDateTime::now_utc();
    cp.define_job_schedule(JobSchedule {
        name: "nightly-gc-e2e".into(),
        kind: GC_JOB_KIND.into(),
        payload: gc_payload.clone(),
        cron: "0 3 * * *".into(),
    })
    .await
    .expect("define gc schedule");

    // Not due at define-time.
    assert!(
        cp.fire_due_job_schedules(now, 32)
            .await
            .expect("fire_due_job_schedules (not due)")
            .is_empty(),
        "a freshly defined daily schedule is not due at its own define-time"
    );

    // Due at a probe two days out.
    let probe = now + time::Duration::days(2);
    let fired = cp
        .fire_due_job_schedules(probe, 32)
        .await
        .expect("fire_due_job_schedules (due)");
    assert_eq!(fired.len(), 1, "exactly one schedule fires at the probe");
    assert_eq!(fired[0].name, "nightly-gc-e2e");
    let job_id = fired[0].job.expect("due fire enqueues a job");

    // Re-firing at the same probe is empty — the advance was atomic.
    assert!(
        cp.fire_due_job_schedules(probe, 32)
            .await
            .expect("re-fire at same probe")
            .is_empty(),
        "re-firing at the same probe fires nothing (next_run_at already advanced)"
    );

    // Dequeue and drain the fired job over the engine wire.
    let client = GrpcQueueClient::connect(&eng.sock).await.expect("connect");
    let job = client
        .dequeue(&[GC_JOB_KIND.to_string()], "sched-e2e")
        .await
        .expect("dequeue")
        .expect("the fired gc_table job must be present");
    assert_eq!(
        job.id, job_id,
        "dequeued job matches the fired schedule's job id"
    );
    assert_eq!(job.kind, GC_JOB_KIND, "dequeued job kind must be gc_table");
    assert_eq!(
        job.payload, gc_payload,
        "dequeued job carries the schedule's payload"
    );

    handle_gc(client.clone(), loom_config::WorkerTuning::default(), job)
        .await
        .expect("handle_gc must succeed over the wire");
    client.complete(job_id).await.expect("complete gc job");

    // Queue drained after completion.
    let again = client
        .dequeue(&[GC_JOB_KIND.to_string()], "sched-e2e")
        .await
        .expect("dequeue after complete");
    assert!(
        again.is_none(),
        "queue must be empty after the scheduled gc job completed"
    );
}

// ---- compact_table leg -------------------------------------------------------

/// A `compact_table` schedule fires exactly once at its due probe, is not due
/// before then, does not re-fire once advanced, and the enqueued job drains
/// end-to-end through `handle_compact` over the engine wire, physically
/// coalescing the table's small files.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_schedule_fires_and_worker_drains_over_the_wire() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_millis(5000));

    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();
    // No `.with_compact_trigger(..)` here: the catalog's auto-trigger stays
    // off, so the only `compact_table` job that can appear is the one this
    // test's schedule fires — an auto-trigger firing would otherwise dedup-
    // suppress the schedule's own fire and falsify the `job: Some(_)` assert.
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

    let table = TableRef {
        schema: "main".into(),
        name: "compact_target".into(),
    };
    // Three separate small files so there is something to coalesce.
    for id in [1i64, 2, 3] {
        let (schema, batches) = ipc_body(&[id]);
        land(
            &pool,
            &catalog,
            &table,
            &columns(),
            schema,
            batches,
            small_limits(),
            lineage(RunId(uuid::Uuid::new_v4()), &table),
            None,
        )
        .await
        .expect("land compact_target file");
    }

    let compact_payload = serde_json::json!({"schema": "main", "name": "compact_target"});
    let now = time::OffsetDateTime::now_utc();
    cp.define_job_schedule(JobSchedule {
        name: "nightly-compact-e2e".into(),
        kind: COMPACT_JOB_KIND.into(),
        payload: compact_payload.clone(),
        cron: "0 3 * * *".into(),
    })
    .await
    .expect("define compact schedule");

    // Not due at define-time.
    assert!(
        cp.fire_due_job_schedules(now, 32)
            .await
            .expect("fire_due_job_schedules (not due)")
            .is_empty(),
        "a freshly defined daily schedule is not due at its own define-time"
    );

    // Due at a probe two days out.
    let probe = now + time::Duration::days(2);
    let fired = cp
        .fire_due_job_schedules(probe, 32)
        .await
        .expect("fire_due_job_schedules (due)");
    assert_eq!(fired.len(), 1, "exactly one schedule fires at the probe");
    assert_eq!(fired[0].name, "nightly-compact-e2e");
    let job_id = fired[0].job.expect("due fire enqueues a job");

    // Re-firing at the same probe is empty — the advance was atomic.
    assert!(
        cp.fire_due_job_schedules(probe, 32)
            .await
            .expect("re-fire at same probe")
            .is_empty(),
        "re-firing at the same probe fires nothing (next_run_at already advanced)"
    );

    let control = GrpcQueueClient::connect(&eng.sock)
        .await
        .expect("connect control");
    let flight = FlightTableClient::connect(&eng.sock)
        .await
        .expect("connect flight");

    let job = control
        .dequeue(&[COMPACT_JOB_KIND.to_string()], "sched-e2e")
        .await
        .expect("dequeue")
        .expect("the fired compact_table job must be present");
    assert_eq!(
        job.id, job_id,
        "dequeued job matches the fired schedule's job id"
    );
    assert_eq!(
        job.kind, COMPACT_JOB_KIND,
        "dequeued job kind must be compact_table"
    );
    assert_eq!(
        job.payload, compact_payload,
        "dequeued job carries the schedule's payload"
    );

    let mut env_map = HashMap::new();
    env_map.insert("LOOM_WAREHOUSE_URI".to_string(), format!("file://{wh_str}"));
    let store_cfg = ObjectStoreConfig::parse_from_env(&env_map).expect("store config");
    let write = Arc::new(build_write_store(&store_cfg).expect("write store"));

    let ctx = CompactCtx {
        control: control.clone(),
        flight,
        write,
        threshold_bytes: 10 * 1024 * 1024, // 10 MiB — all 1-row files qualify as small
        write_cfg: datafusion_io::WriteConfig::default(),
        worker_tuning: loom_config::WorkerTuning::default(),
    };

    handle_compact(&ctx, job)
        .await
        .expect("handle_compact must succeed over the wire");
    control
        .complete(job_id)
        .await
        .expect("complete compact job");

    // Physical coalescing: 3 small files -> 1, rows preserved.
    let ice = IcebergCatalog::new(pool.clone());
    let cur = ice
        .current_snapshot(&table)
        .await
        .expect("current snapshot after compact");
    let files = ice
        .files_with_stats(&table, cur.id)
        .await
        .expect("files after compact");
    assert_eq!(
        files.len(),
        1,
        "three small files coalesced into one by the scheduled compact job"
    );
    let total: i64 = files.iter().map(|f| f.record_count).sum();
    assert_eq!(
        total, 3,
        "row set preserved across the scheduled compaction"
    );

    // Queue drained after completion.
    let again = control
        .dequeue(&[COMPACT_JOB_KIND.to_string()], "sched-e2e")
        .await
        .expect("dequeue after complete");
    assert!(
        again.is_none(),
        "queue must be empty after the scheduled compact job completed"
    );
}

// ---- sweep_orphans leg -------------------------------------------------------

/// A `sweep_orphans` schedule fires exactly once at its due probe and the
/// enqueued job drains end-to-end through `handle_sweep_orphans` over the engine
/// wire: a planted orphan `.parquet` is deleted while a referenced (landed) file
/// survives. Grace is 0 (via `EngineOpts`) so the freshly-planted orphan is
/// immediately reclaimable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweep_orphans_schedule_fires_and_worker_drains_over_the_wire() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_millis(5000));

    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh_str).await;

    let eng = spawn_engine_uds(
        fx,
        &db,
        &wh_str,
        EngineOpts {
            control: true,
            orphan_sweep_grace: Duration::ZERO,
            ..EngineOpts::default()
        },
    )
    .await;

    // A referenced (landed) file that MUST survive the sweep.
    let table = TableRef {
        schema: "main".into(),
        name: "kept".into(),
    };
    let (schema, batches) = ipc_body(&[1, 2, 3]);
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        schema,
        batches,
        small_limits(),
        lineage(RunId(uuid::Uuid::new_v4()), &table),
        None,
    )
    .await
    .expect("land kept");
    let ice = IcebergCatalog::new(pool.clone());
    let cur = ice.current_snapshot(&table).await.expect("snapshot");
    let kept = std::path::PathBuf::from(
        ice.files_with_stats(&table, cur.id).await.expect("files")[0]
            .path
            .strip_prefix("file://")
            .expect("file:// path"),
    );
    assert!(kept.exists(), "referenced file present before sweep");

    // A planted orphan under the warehouse, referenced by NO mirror row.
    let orphan = wh.path().join("orphan-xyz.parquet");
    std::fs::write(&orphan, b"orphan-bytes").expect("write orphan");

    let now = time::OffsetDateTime::now_utc();
    cp.define_job_schedule(JobSchedule {
        name: "nightly-sweep-e2e".into(),
        kind: ORPHAN_SWEEP_JOB_KIND.into(),
        payload: serde_json::json!({}),
        cron: "0 4 * * *".into(),
    })
    .await
    .expect("define sweep schedule");

    assert!(
        cp.fire_due_job_schedules(now, 32)
            .await
            .expect("fire (not due)")
            .is_empty(),
        "a freshly defined daily schedule is not due at define-time"
    );

    let probe = now + time::Duration::days(2);
    let fired = cp
        .fire_due_job_schedules(probe, 32)
        .await
        .expect("fire (due)");
    assert_eq!(fired.len(), 1, "exactly one schedule fires at the probe");
    assert_eq!(fired[0].name, "nightly-sweep-e2e");
    let job_id = fired[0].job.expect("due fire enqueues a job");

    let client = GrpcQueueClient::connect(&eng.sock).await.expect("connect");
    let job = client
        .dequeue(&[ORPHAN_SWEEP_JOB_KIND.to_string()], "sched-e2e")
        .await
        .expect("dequeue")
        .expect("the fired sweep_orphans job must be present");
    assert_eq!(
        job.id, job_id,
        "dequeued job matches the fired schedule's job id"
    );
    assert_eq!(job.kind, ORPHAN_SWEEP_JOB_KIND);
    assert_eq!(
        job.payload,
        serde_json::json!({}),
        "empty warehouse-scoped payload"
    );

    handle_sweep_orphans(client.clone(), loom_config::WorkerTuning::default(), job)
        .await
        .expect("handle_sweep_orphans must succeed over the wire");
    client.complete(job_id).await.expect("complete sweep job");

    assert!(
        !orphan.exists(),
        "planted orphan deleted by the scheduled sweep"
    );
    assert!(kept.exists(), "referenced file survived the sweep");

    let again = client
        .dequeue(&[ORPHAN_SWEEP_JOB_KIND.to_string()], "sched-e2e")
        .await
        .expect("dequeue after complete");
    assert!(
        again.is_none(),
        "queue empty after the scheduled sweep completed"
    );
}
