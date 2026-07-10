//! Task 6: registration → trigger wiring for `MicroBatch` (materialized-view)
//! defs. Proves the SAME data-trigger machinery slice 3 built for `Physical`
//! defs (`tests/data_triggers.rs`) covers the new body kind with no
//! special-casing: fire + frozen payload, debounce, composability (an MV
//! output commit is itself a first-class data-trigger seam), MV↔MV cycle
//! rejection at define time, and the define-time refuse-guard EXEMPTION that
//! lets a running MV be redefined (contrasted against the still-refused
//! Physical/Typed re-target case, #416).

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use loom_test_seed::local_sql_catalog;

use control_plane_core::{
    ColumnSpec, ControlPlaneError, DatasetId, EventType, LineageEvent, OutputMode, PageReq, Queue,
    RunId, RunState, RunTrigger, STREAM_MV_JOB_KIND, StreamMvJob, TableRef, TransformBody,
    TransformDef, TransformName, Transforms, WatermarkAdvance, mv_key,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_inline::{MvCommit, inline_append_mv};
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_mirror::live_table_id;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// A schema + single batch of `rows` rows, one `id: long` column (ids `0..rows`),
/// for `land` seeding.
fn ipc_body(rows: i64) -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .expect("batch");
    (schema, vec![batch])
}

/// A single `id: long` batch, for `inline_append_mv` (which takes one batch,
/// not a `Vec`).
fn one_batch(rows: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .expect("batch")
}

fn lineage(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

fn mv_def(name: &str, source: &TableRef, output: &TableRef, sql: &str) -> TransformDef {
    TransformDef {
        name: TransformName(name.into()),
        body: TransformBody::MicroBatch {
            source: source.clone(),
            output: output.clone(),
            buckets: 1,
            sql: sql.into(),
        },
        schedule: None,
        on_input_commit: true,
    }
}

fn physical_def(name: &str, input: &TableRef, output: &TableRef) -> TransformDef {
    TransformDef {
        name: TransformName(name.into()),
        body: TransformBody::Physical {
            inputs: vec![input.clone()],
            output: output.clone(),
            sql: "select * from src".into(),
            output_mode: OutputMode::Append,
        },
        schedule: None,
        on_input_commit: false,
    }
}

/// Leg 1 + 2: a `MicroBatch` def with `on_input_commit: true` fires on a
/// source `land`, the dequeued job's payload decodes to the def's exact
/// `StreamMvJob` (with a fresh `run_id`), `list_runs` shows one `Queued` run
/// with `trigger: DataTrigger` — and landing again WITHOUT draining the run
/// stays at exactly one `Queued` run (at-most-one-pending debounce).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn microbatch_def_fires_with_frozen_payload_and_debounces() {
    let fx = PgFixture::shared();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let events = tref("s", "events");
    let mv1_out = tref("s", "mv1");
    let def = mv_def("mv1", &events, &mv1_out, "select id from events");
    pg.define_transform(def.clone()).await.expect("define mv1");

    let (schema, batches) = ipc_body(3);
    land(
        &pool,
        &catalog,
        &events,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), &events),
        Some(2),
    )
    .await
    .expect("land events");

    let job = pg
        .dequeue(&[STREAM_MV_JOB_KIND.to_string()], "w")
        .await
        .expect("dequeue")
        .expect("a queued stream_mv job");
    assert_eq!(job.kind, STREAM_MV_JOB_KIND, "kind survives the queue");
    let payload: StreamMvJob =
        serde_json::from_value(job.payload.clone()).expect("decode StreamMvJob");
    assert_eq!(payload.source, events, "frozen source");
    assert_eq!(payload.output, mv1_out, "frozen output");
    assert_eq!(payload.buckets, 1, "frozen buckets");
    assert_eq!(payload.sql, "select id from events", "frozen sql");

    let runs = pg
        .list_runs(Some(&TransformName("mv1".into())), PageReq::default())
        .await
        .expect("list_runs");
    assert_eq!(runs.items.len(), 1, "exactly one fired run");
    let r = &runs.items[0];
    assert_eq!(r.state, RunState::Queued);
    assert_eq!(r.trigger, RunTrigger::DataTrigger);
    assert_eq!(r.transform, Some(TransformName("mv1".into())));
    assert_eq!(r.body, def.body, "the run freezes the def's exact body");
    assert_eq!(
        payload.run_id,
        Some(r.run_id),
        "the job payload's run_id is the fresh run's id"
    );

    // Leg 2: land again WITHOUT draining — the run row is still Queued, so the
    // commit-seam matcher debounces (at-most-one-pending).
    let (schema2, batches2) = ipc_body(2);
    land(
        &pool,
        &catalog,
        &events,
        &columns(),
        schema2,
        batches2,
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), &events),
        Some(2),
    )
    .await
    .expect("land events again");

    let runs_after = pg
        .list_runs(Some(&TransformName("mv1".into())), PageReq::default())
        .await
        .expect("list_runs after second land");
    assert_eq!(
        runs_after.items.len(),
        1,
        "still exactly one Queued run — debounced"
    );
    assert_eq!(
        runs_after.items[0].run_id, r.run_id,
        "the SAME run, not a new one"
    );
}

/// Leg 3: an MV's own output commit (via `inline_append_mv`) is itself a
/// first-class data-trigger seam — it enqueues a downstream MV's job in the
/// SAME transaction as the commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mv_output_commit_enqueues_downstream_mv_job() {
    let fx = PgFixture::shared();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let events = tref("s", "events");
    let mv1 = tref("s", "mv1");
    let mv2 = tref("s", "mv2");

    pg.define_transform(mv_def("mv1", &events, &mv1, "select id from events"))
        .await
        .expect("define mv1");
    pg.define_transform(mv_def("mv2", &mv1, &mv2, "select id from mv1"))
        .await
        .expect("define mv2");

    // Seed s.events (declares it a 2-bucket log stream) — this also fires
    // mv1's own def; drain that job so the next dequeue below unambiguously
    // observes mv2's job.
    let (schema, batches) = ipc_body(3);
    land(
        &pool,
        &catalog,
        &events,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), &events),
        Some(2),
    )
    .await
    .expect("land events");
    let mv1_job = pg
        .dequeue(&[STREAM_MV_JOB_KIND.to_string()], "w")
        .await
        .expect("dequeue")
        .expect("mv1's own fired job");
    assert_eq!(mv1_job.kind, STREAM_MV_JOB_KIND);

    let mut conn = pool.acquire().await.expect("conn");
    let events_tid = live_table_id(&mut conn, &events.schema, &events.name)
        .await
        .expect("live_table_id")
        .expect("events is declared");
    drop(conn);

    let commit = MvCommit {
        mv: mv_key(&mv1),
        source_table_id: events_tid,
        advances: vec![WatermarkAdvance {
            bucket: 0,
            from: 0,
            to: 1,
        }],
        run_id: None,
    };
    inline_append_mv(
        &pool,
        &mv1,
        &columns(),
        &one_batch(1),
        lineage(RunId(uuid::Uuid::new_v4()), &mv1),
        None,
        1,
        &commit,
    )
    .await
    .expect("commit mv1's micro-batch output");

    let job = pg
        .dequeue(&[STREAM_MV_JOB_KIND.to_string()], "w")
        .await
        .expect("dequeue")
        .expect("mv2's job, enqueued by the SAME transaction as mv1's commit");
    assert_eq!(job.kind, STREAM_MV_JOB_KIND);
    let payload: StreamMvJob = serde_json::from_value(job.payload.clone()).expect("decode");
    assert_eq!(payload.source, mv1);
    assert_eq!(payload.output, mv2);

    let runs2 = pg
        .list_runs(Some(&TransformName("mv2".into())), PageReq::default())
        .await
        .expect("list_runs mv2");
    assert_eq!(runs2.items.len(), 1, "mv2 fired exactly once");
    assert_eq!(runs2.items[0].trigger, RunTrigger::DataTrigger);
}

/// Leg 4: an MV→MV trigger cycle (`mv_a: s.t1 → s.t2`, `mv_b: s.t2 → s.t1`,
/// both `on_input_commit`) is a `Validation` error at define time, naming
/// both defs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mv_mv_cycle_rejected_at_define_time() {
    let fx = PgFixture::shared();
    let (pg, _db) = fx.fresh_db().await;

    let t1 = tref("c", "t1");
    let t2 = tref("c", "t2");
    pg.define_transform(mv_def("mv_a", &t1, &t2, "select 1 from t1"))
        .await
        .expect("define mv_a");

    let err = pg
        .define_transform(mv_def("mv_b", &t2, &t1, "select 1 from t2"))
        .await
        .expect_err("mv_a <-> mv_b forms a data-trigger cycle");
    assert!(
        matches!(err, ControlPlaneError::Validation(_)),
        "got {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("mv_a") && msg.contains("mv_b"),
        "error names both defs: {msg}"
    );
}

/// Leg 5 (the refuse-guard exemption regression): defining `mv1: s.events →
/// s.mv1`, committing a micro-batch to `s.mv1` (declaring it a log stream),
/// then RE-defining `mv1` with edited `sql` (same output) must SUCCEED — the
/// `MicroBatch { .. } => None` exemption at the define-time
/// `pg_refuse_stream_target` site. CONTRAST: a `Physical` transform
/// re-targeting `s.mv1` as its output is still refused (#416, unmodified).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redefine_after_first_commit_is_exempt_but_physical_retarget_still_refused() {
    let fx = PgFixture::shared();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let events = tref("s", "events");
    let mv1 = tref("s", "mv1");
    pg.define_transform(mv_def("mv1", &events, &mv1, "select id from events"))
        .await
        .expect("define mv1");

    let (schema, batches) = ipc_body(2);
    land(
        &pool,
        &catalog,
        &events,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), &events),
        Some(2),
    )
    .await
    .expect("land events");

    let mut conn = pool.acquire().await.expect("conn");
    let events_tid = live_table_id(&mut conn, &events.schema, &events.name)
        .await
        .expect("live_table_id")
        .expect("events is declared");
    drop(conn);

    let commit = MvCommit {
        mv: mv_key(&mv1),
        source_table_id: events_tid,
        advances: vec![WatermarkAdvance {
            bucket: 0,
            from: 0,
            to: 1,
        }],
        run_id: None,
    };
    inline_append_mv(
        &pool,
        &mv1,
        &columns(),
        &one_batch(1),
        lineage(RunId(uuid::Uuid::new_v4()), &mv1),
        None,
        1,
        &commit,
    )
    .await
    .expect("commit mv1's first micro-batch — s.mv1 is now a declared log stream");

    // Re-define mv1 (edited sql, same output): must succeed, NOT a Validation refusal.
    pg.define_transform(mv_def(
        "mv1",
        &events,
        &mv1,
        "select id, id as id2 from events",
    ))
    .await
    .expect(
        "redefining a running MV after its first commit must succeed — \
         MicroBatch is exempt from the legacy-write refuse guard",
    );

    // CONTRAST: a Physical transform re-targeting s.mv1 (now a declared
    // stream) is still refused — do not regress #416.
    let err = pg
        .define_transform(physical_def("phys_retarget", &events, &mv1))
        .await
        .expect_err("a Physical def re-targeting a declared stream must still be refused");
    assert!(
        matches!(err, ControlPlaneError::Validation(_)),
        "got {err:?}"
    );
    assert!(
        err.to_string().contains("stream-table target refused:"),
        "msg: {err}"
    );
}
