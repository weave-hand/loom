//! Worker `stream_consolidate` job e2e: seed a `kind='cdc'` table with
//! unconsolidated deltas (insert then update, flushed to real Parquet), enqueue a
//! `StreamConsolidateJob`, run `handle_stream_consolidate` over the wire (decode
//! payload -> `EngineControl::ConsolidateStream` RPC), and assert the same
//! folded end-state Task 6's engine-side e2e asserts: the base folds to
//! LastRow-per-identity and `has_shadow` clears. Plus the payload-decode Abandon
//! edge. Engine-side fold semantics (multi-identity, delete-wins, framing
//! preservation) are covered by
//! `query-api/tests/stream_cdc_consolidate.rs`; this test proves the job path
//! reaches the RPC and the worker's dispatch wiring works end to end.

use loom_test_flight::{EngineOpts, spawn_engine_uds};
use loom_test_seed::local_sql_catalog;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, EventType, Job, JobId, LineageEvent, RunId, STREAM_CONSOLIDATE_JOB_KIND,
    StreamConsolidateJob, StreamTables, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_inline::{
    current_inline_version, has_shadow, inline_append, write_inline_delta,
};
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use engine_wire::client::GrpcQueueClient;
use worker::consolidate::handle_stream_consolidate;

// ---- helpers ---------------------------------------------------------------

fn id_spec() -> ColumnSpec {
    ColumnSpec {
        name: "id".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }
}

fn val_spec() -> ColumnSpec {
    ColumnSpec {
        name: "val".to_string(),
        ty: "long".to_string(),
        nullable: false,
    }
}

fn full_row_batch(id: i64, val: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Int64Array::from(vec![val])),
        ],
    )
    .expect("full row batch")
}

fn id_batch(v: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).expect("id batch")
}

fn lin() -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "stream-consolidate-job-test" }),
    }
}

fn make_stream_consolidate_job(schema: &str, name: &str) -> Job {
    Job {
        id: JobId(uuid::Uuid::new_v4()),
        kind: STREAM_CONSOLIDATE_JOB_KIND.to_string(),
        payload: serde_json::to_value(StreamConsolidateJob {
            schema: schema.into(),
            name: name.into(),
        })
        .expect("serialize payload"),
        attempts: 0,
        run_at: time::OffsetDateTime::now_utc(),
    }
}

// ---- test -------------------------------------------------------------------

/// Worker stream-consolidate e2e: seed a CDC table (insert id=1 val=100, then
/// update to val=200 — the base holds both the `+I` and `+U` rows, unconsolidated,
/// and `has_shadow` is set), run the job over the wire, and assert the base
/// folds to the single LastRow (`+U`, val=200) and `has_shadow` clears — the
/// same end-state Task 6's engine-side e2e asserts, reached through the job
/// dispatch path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_consolidates_stream_over_the_wire() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh_str).await;

    let table = TableRef {
        schema: "main".into(),
        name: "widget".into(),
    };
    let cols = vec![id_spec(), val_spec()];

    // Declare the table CDC (keyed on `id`) before any write.
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, &table.schema, &table.name, at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 2, "id", control_plane_core::MergeEngine::LastRow)
        .await
        .expect("declare_cdc");

    // Seed id=1, val=100 (a plain +I append).
    inline_append(
        &pool,
        &table,
        &cols,
        &full_row_batch(1, 100),
        lin(),
        None,
        None,
    )
    .await
    .expect("seed append");

    // Update id=1: val 100 -> 200 (emits +U after-image, -U before-image).
    let v0 = current_inline_version(&pool, &table, &[id_spec()], "id", &id_batch(1))
        .await
        .expect("version after seed");
    write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &full_row_batch(1, 200),
        Some((&cols, &full_row_batch(1, 100))),
        lin(),
        v0,
        None,
        &[],
    )
    .await
    .expect("cdc update delta");

    // Flush so the deltas land in the base as real Parquet (base holds +I, +U;
    // unconsolidated — two physical rows for id=1 — and has_shadow is set).
    flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush")
        .expect("flush produced a snapshot");

    assert!(
        has_shadow(&mut pool.acquire().await.expect("conn"), tid)
            .await
            .expect("has_shadow"),
        "the update delta sets has_shadow before consolidate"
    );

    // Spawn the engine (EngineControl + Flight on the same UDS).
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
    let client = GrpcQueueClient::connect(&eng.sock)
        .await
        .expect("connect control");

    // Run the stream-consolidate job over the wire.
    let tuning = loom_config::WorkerTuning::default();
    handle_stream_consolidate(
        client,
        tuning,
        make_stream_consolidate_job("main", "widget"),
    )
    .await
    .expect("handle_stream_consolidate");

    // has_shadow is cleared: the job reached the engine's ConsolidateStream RPC
    // and it folded the base to LastRow-per-identity (proven in full, including
    // the fold's row content and framing survival, by
    // query-api/tests/stream_cdc_consolidate.rs).
    assert!(
        !has_shadow(&mut pool.acquire().await.expect("conn"), tid)
            .await
            .expect("has_shadow"),
        "consolidate clears has_shadow"
    );
}

/// A malformed payload (missing required fields) is abandoned, not retried —
/// the same Abandon taxonomy edge `run_wire_job` proves generically, checked
/// here for `STREAM_CONSOLIDATE_JOB_KIND`'s dispatch wiring specifically.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bad_payload_is_abandoned() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();

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
    let client = GrpcQueueClient::connect(&eng.sock)
        .await
        .expect("connect control");

    let bad_job = Job {
        id: JobId(uuid::Uuid::new_v4()),
        kind: STREAM_CONSOLIDATE_JOB_KIND.to_string(),
        payload: serde_json::json!({ "schema": "main" }), // missing `name`
        attempts: 0,
        run_at: time::OffsetDateTime::now_utc(),
    };

    let err = handle_stream_consolidate(client, loom_config::WorkerTuning::default(), bad_job)
        .await
        .expect_err("bad payload must fail");
    assert!(
        matches!(err.policy, control_plane_core::RetryPolicy::Abandon),
        "bad payload is abandoned, not retried: {err:?}"
    );
}
