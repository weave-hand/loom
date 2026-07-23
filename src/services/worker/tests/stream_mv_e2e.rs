//! Micro-batch MV convergence e2e: an MV over a 2-bucket source log stream,
//! driven across multiple micro-batches (with a flush between them), converges
//! to the batch-equivalent result; its output is a declared log stream table
//! with gapless +I framing (structurally subscribable); a re-run on an
//! unchanged source is a no-op.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlane, DatasetId, EventType, Job, JobFailure, JobId, LineageEvent,
    MvWatermarks, Queue, RetryPolicy, RunId, RunState, RunTrigger, STREAM_MV_JOB_KIND, StreamKind,
    StreamMvJob, StreamTables, TableRef, TransformBody, TransformDef, TransformName, TransformRun,
    WatermarkAdvance, mv_key,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_mirror::live_table_id;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use engine_wire::client::GcCounts;
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::{FlightSqlClient, FlightTableClient};
use loom_test_flight::{EngineGuard, EngineOpts, spawn_engine_uds};
use loom_test_seed::local_sql_catalog;
use worker::stream_mv::{StreamMvCtx, handle_stream_mv};

// ---- helpers ---------------------------------------------------------------

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

fn events_columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "val".into(),
            ty: "long".into(),
            nullable: false,
        },
    ]
}

/// A `(id: Long, val: Long)` schema + batch, for `land` seeding.
fn events_batch(ids: &[i64], vals: &[i64]) -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(vals.to_vec())),
        ],
    )
    .expect("batch");
    (schema, vec![batch])
}

fn seed_lineage(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "stream-mv-e2e-test" }),
    }
}

/// Land one `(id, val)` batch to `src` straight to a Parquet FILE (`inline_byte_limit: 0`),
/// bucketed into `buckets` log-stream buckets. The shared land step for the
/// register-after-land tests; `buckets` is the only thing that varies between them.
async fn land_events(
    pool: &sqlx::PgPool,
    catalog: &SqlCatalog,
    src: &TableRef,
    ids: &[i64],
    vals: &[i64],
    buckets: i32,
) {
    let (schema, batches) = events_batch(ids, vals);
    land(
        pool,
        catalog,
        src,
        &events_columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        seed_lineage(src),
        Some(buckets),
    )
    .await
    .expect("land events");
}

async fn build_ctx(sock: &str) -> StreamMvCtx {
    let control = GrpcQueueClient::connect(sock)
        .await
        .expect("connect control");
    let table = FlightTableClient::connect(sock)
        .await
        .expect("connect flight table");
    StreamMvCtx {
        control,
        table,
        worker_tuning: loom_config::WorkerTuning::default(),
    }
}

/// Handles shared by the register-after-land e2e tests below. The keep-alive fields (fixture,
/// db name, engine guard, warehouse tempdir) must outlive the test body — dropping any of them
/// tears down the DB / engine / warehouse — so tests bind them to `_`-prefixed locals.
struct MvEnv {
    cp: PgControlPlane,
    pool: sqlx::PgPool,
    catalog: SqlCatalog,
    ctx: StreamMvCtx,
    engine: GrpcQueueClient,
    sql_client: FlightSqlClient,
    fx: &'static PgFixture,
    db: String,
    eng: EngineGuard,
    wh: tempfile::TempDir,
}

/// Boot a fresh fixture DB + a local SQL catalog over a temp warehouse, spawn a control+flight
/// engine over a UDS, and wire the stream-MV ctx and the control/SQL clients. The common
/// preamble for the register-after-land tests.
async fn setup_mv_env() -> MvEnv {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
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
            flight: true,
            ..EngineOpts::default()
        },
    )
    .await;
    let ctx = build_ctx(&eng.sock).await;
    let engine = GrpcQueueClient::connect(&eng.sock)
        .await
        .expect("connect control");
    let sql_client = FlightSqlClient::connect(&eng.sock)
        .await
        .expect("connect sql");
    MvEnv {
        cp,
        pool,
        catalog,
        ctx,
        engine,
        sql_client,
        fx,
        db,
        eng,
        wh,
    }
}

fn make_stream_mv_job(
    source: &TableRef,
    output: &TableRef,
    buckets: i32,
    sql: &str,
    run_id: Option<uuid::Uuid>,
) -> Job {
    Job {
        id: JobId(uuid::Uuid::new_v4()),
        kind: STREAM_MV_JOB_KIND.to_string(),
        payload: serde_json::to_value(StreamMvJob {
            source: source.clone(),
            output: output.clone(),
            buckets,
            sql: sql.to_string(),
            run_id,
            enrich: None,
            on: None,
        })
        .expect("payload"),
        attempts: 0,
        run_at: time::OffsetDateTime::now_utc(),
    }
}

/// Register a micro-batch MV def naming `output` over `source`, with NO data trigger
/// (`on_input_commit: false`) so landing never auto-fires a run — these tests drive their
/// runs by hand. Since #627 the watermark CAS a micro-batch commit issues carries a
/// def-existence guard (`mv_key(output)` must be named by a live micro-batch def), so a
/// `run_micro_batch` happy path must register its def first; the register-then-run tests
/// below (`gc_holds_…`, `mv_registered_…`) already do this inline.
async fn register_mv_def(cp: &PgControlPlane, source: &TableRef, output: &TableRef, sql: &str) {
    cp.transforms()
        .define_transform(TransformDef {
            name: TransformName(format!("mv_{}_{}", output.schema, output.name)),
            body: TransformBody::MicroBatch {
                source: source.clone(),
                output: output.clone(),
                buckets: 1,
                sql: sql.to_string(),
            },
            schedule: None,
            on_input_commit: false,
        })
        .await
        .expect("register mv def");
}

/// Submit + dequeue + run one micro-batch over the REAL queue, mirroring how
/// `transform_e2e` drives its jobs: a `TransformRun` carrying a frozen
/// `MicroBatch` body is submitted (record + queued job, atomically), then
/// dequeued via the wire `GrpcQueueClient` (pinning `STREAM_MV_JOB_KIND`
/// end-to-end) and executed. Returns the run id (for state assertions) and the
/// handler's result.
async fn run_micro_batch(
    cp: &PgControlPlane,
    ctx: &StreamMvCtx,
    source: &TableRef,
    output: &TableRef,
    buckets: i32,
    sql: &str,
) -> (uuid::Uuid, std::result::Result<(), JobFailure>) {
    let rid = uuid::Uuid::new_v4();
    let body = TransformBody::MicroBatch {
        source: source.clone(),
        output: output.clone(),
        buckets,
        sql: sql.to_string(),
    };
    let run = TransformRun {
        run_id: rid,
        transform: None,
        trigger: RunTrigger::AdHoc,
        state: RunState::Queued,
        body: body.clone(),
        queued_at: time::OffsetDateTime::now_utc(),
        started_at: None,
        finished_at: None,
        snapshot_id: None,
        error: None,
    };
    cp.transforms()
        .submit_run(run, body.to_job(rid))
        .await
        .expect("submit micro-batch run");

    let job = ctx
        .control
        .dequeue(&[STREAM_MV_JOB_KIND.to_string()], "e2e-worker")
        .await
        .expect("dequeue")
        .expect("a queued stream_mv job");
    assert_eq!(job.kind, STREAM_MV_JOB_KIND, "kind survives the queue");

    let result = handle_stream_mv(ctx, job).await;
    (rid, result)
}

/// Read `s.doubled`'s `(id, dbl)` rows over the engine's SQL serving path
/// (Flight SQL), as an order-independent set.
async fn doubled_rows(sql: &FlightSqlClient) -> HashSet<(i64, i64)> {
    let batches = sql
        .execute("select id, dbl from \"s\".\"doubled\" order by id".to_string())
        .await
        .expect("query doubled");
    let mut out = HashSet::new();
    for b in &batches {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let dbls = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("dbl column");
        for i in 0..b.num_rows() {
            out.insert((ids.value(i), dbls.value(i)));
        }
    }
    out
}

/// Assert every row of a framed batch carries `loom_change_kind = "+I"` and
/// `loom_bucket = 0`, and that the observed `loom_offset`s are exactly
/// `0..batch.num_rows()` (gapless from zero).
fn assert_gapless_append_framing(batch: &RecordBatch) {
    let kinds = batch
        .column_by_name("loom_change_kind")
        .expect("loom_change_kind column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("loom_change_kind is Utf8");
    let buckets = batch
        .column_by_name("loom_bucket")
        .expect("loom_bucket column")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("loom_bucket is Int32");
    let offsets = batch
        .column_by_name("loom_offset")
        .expect("loom_offset column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("loom_offset is Int64");

    let mut seen: Vec<i64> = Vec::new();
    for i in 0..batch.num_rows() {
        assert_eq!(kinds.value(i), "+I", "every output row is a plain append");
        assert_eq!(
            buckets.value(i),
            0,
            "single-bucket output: every row lands in bucket 0"
        );
        seen.push(offsets.value(i));
    }
    seen.sort_unstable();
    let n = i64::try_from(batch.num_rows()).expect("row count fits i64");
    let expected: Vec<i64> = (0..n).collect();
    assert_eq!(seen, expected, "offsets are gapless from zero");
}

// ---- tests -----------------------------------------------------------------

/// Legs 1-6: two-bucket source, two micro-batches (a flush in between),
/// convergence to the batch-equivalent result, structural subscribability of
/// the output, and an idempotent re-run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mv_converges_across_micro_batches_and_flush() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
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
            flight: true,
            ..EngineOpts::default()
        },
    )
    .await;

    let ctx = build_ctx(&eng.sock).await;
    let sql_client = FlightSqlClient::connect(&eng.sock)
        .await
        .expect("connect sql");

    // 1. Seed s.events: (1,10),(2,20),(3,30), declared a 2-bucket log stream.
    let src = tref("s", "events");
    // #627: register the MV def up front so the micro-batch commits' watermark CAS passes the
    // def-existence guard (register-before-run, like the tests further down).
    register_mv_def(
        &cp,
        &src,
        &tref("s", "doubled"),
        "select id, val * 2 as dbl from events",
    )
    .await;
    let (schema, batches) = events_batch(&[1, 2, 3], &[10, 20, 30]);
    land(
        &pool,
        &catalog,
        &src,
        &events_columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        seed_lineage(&src),
        Some(2),
    )
    .await
    .expect("land initial events");

    // 2/3. First micro-batch: doubles val. Convergence #1.
    let dst = tref("s", "doubled");
    let sql = "select id, val * 2 as dbl from events";
    let (rid1, result1) = run_micro_batch(&cp, &ctx, &src, &dst, 1, sql).await;
    result1.expect("first micro-batch");
    let run1 = cp.transforms().get_run(rid1).await.expect("run1");
    assert_eq!(run1.state, RunState::Succeeded, "run1 succeeded");

    assert_eq!(
        doubled_rows(&sql_client).await,
        HashSet::from([(1, 20), (2, 40), (3, 60)]),
        "convergence #1: batch-equivalent doubled rows"
    );

    // 4. Flush the source, land two more rows, run a second micro-batch.
    flush_table(&catalog, &pool, &src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush source");
    let (schema2, batches2) = events_batch(&[4, 5], &[40, 50]);
    land(
        &pool,
        &catalog,
        &src,
        &events_columns(),
        schema2,
        batches2,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        seed_lineage(&src),
        Some(2),
    )
    .await
    .expect("land more events");

    let (rid2, result2) = run_micro_batch(&cp, &ctx, &src, &dst, 1, sql).await;
    result2.expect("second micro-batch");
    let run2 = cp.transforms().get_run(rid2).await.expect("run2");
    assert_eq!(run2.state, RunState::Succeeded, "run2 succeeded");

    let rows2 = doubled_rows(&sql_client).await;
    assert_eq!(
        rows2,
        HashSet::from([(1, 20), (2, 40), (3, 60), (4, 80), (5, 100)]),
        "convergence #2: the delta scan crossed the flush boundary, no duplicates"
    );

    // 5. Structural subscribability: gapless +I framing on the output.
    let ice = IcebergCatalog::new(pool.clone());
    let snap = ice.current_snapshot(&dst).await.expect("output snapshot");
    let (out_tid, _row_ids, out_batch) = ice
        .inline_live_batch_full(&dst, snap.id)
        .await
        .expect("read output framing")
        .expect("output has live rows");
    assert_eq!(
        out_batch.num_rows(),
        5,
        "all 5 doubled rows are live inline"
    );
    assert_gapless_append_framing(&out_batch);

    let meta = cp
        .stream_meta(out_tid)
        .await
        .expect("stream meta")
        .expect("output is a declared stream table");
    assert_eq!(
        meta.kind,
        StreamKind::Log,
        "the output is declared a LOG stream table"
    );

    // 6. Idempotent re-run: no new source rows -> empty-delta no-op.
    let (rid3, result3) = run_micro_batch(&cp, &ctx, &src, &dst, 1, sql).await;
    result3.expect("idempotent re-run");
    let run3 = cp.transforms().get_run(rid3).await.expect("run3");
    assert_eq!(
        run3.state,
        RunState::Succeeded,
        "run3 succeeded (empty-delta no-op)"
    );
    assert_eq!(
        doubled_rows(&sql_client).await,
        rows2,
        "re-running with no new source rows changed nothing"
    );
}

/// Leg 7: a filtering micro-batch (non-empty delta, SQL output zero rows)
/// still advances the watermark past the consumed delta and lands nothing;
/// re-running with no new source rows stays a no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filtering_micro_batch_advances_watermark_with_empty_output() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
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
            flight: true,
            ..EngineOpts::default()
        },
    )
    .await;
    let ctx = build_ctx(&eng.sock).await;

    // All rows fail the predicate below (val <= 1000).
    let src = tref("f", "events");
    // #627: register the MV def up front so the micro-batch commit's watermark CAS passes the
    // def-existence guard.
    register_mv_def(
        &cp,
        &src,
        &tref("f", "filtered"),
        "select id, val from events where val > 1000",
    )
    .await;
    let (schema, batches) = events_batch(&[1, 2, 3], &[10, 20, 30]);
    land(
        &pool,
        &catalog,
        &src,
        &events_columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        seed_lineage(&src),
        Some(2),
    )
    .await
    .expect("land events");

    let dst = tref("f", "filtered");
    let sql = "select id, val from events where val > 1000";

    let mut conn = pool.acquire().await.expect("conn");
    let src_tid = live_table_id(&mut conn, &src.schema, &src.name)
        .await
        .expect("live_table_id")
        .expect("source is declared");
    drop(conn);
    let mv = mv_key(&dst);

    let before = cp
        .mv_watermarks(&mv, src_tid)
        .await
        .expect("watermarks before");
    assert!(before.is_empty(), "no watermark recorded before any run");

    let (rid1, result1) = run_micro_batch(&cp, &ctx, &src, &dst, 1, sql).await;
    result1.expect("filtering micro-batch");
    let run1 = cp.transforms().get_run(rid1).await.expect("run1");
    assert_eq!(
        run1.state,
        RunState::Succeeded,
        "a filtering micro-batch still succeeds"
    );

    // The output table gains NO rows: the empty-ipc commit branch never lands
    // or declares anything for this mv's first (filtering) run.
    let listed = ctx
        .control
        .list_files(dst.schema.clone(), dst.name.clone())
        .await
        .expect("list output");
    assert!(
        listed.columns.is_none(),
        "the filtering run's empty output never declared the output table"
    );

    let after = cp
        .mv_watermarks(&mv, src_tid)
        .await
        .expect("watermarks after");
    assert_ne!(
        after, before,
        "the watermark advanced past the consumed (filtered) delta"
    );
    assert!(!after.is_empty(), "at least one bucket's watermark moved");

    // Re-running with no new source rows is a genuine no-op: the empty-delta
    // path never reprocesses the already-consumed (filtered) delta.
    let (rid2, result2) = run_micro_batch(&cp, &ctx, &src, &dst, 1, sql).await;
    result2.expect("idempotent re-run of the filtering mv");
    let run2 = cp.transforms().get_run(rid2).await.expect("run2");
    assert_eq!(run2.state, RunState::Succeeded);

    let after2 = cp
        .mv_watermarks(&mv, src_tid)
        .await
        .expect("watermarks after re-run");
    assert_eq!(
        after2, after,
        "the empty-delta re-run did not reprocess the filtered delta"
    );
}

/// Leg 8: a job whose source is a plain batch table (never declared a stream)
/// deterministically abandons — the engine's `mv_delta_scan` refusal maps to
/// `failed_precondition` on the wire, and the worker keys off it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_batch_source_abandons() {
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
            flight: true,
            ..EngineOpts::default()
        },
    )
    .await;
    let ctx = build_ctx(&eng.sock).await;

    // No `stream_buckets` -> a plain batch table.
    let src = tref("b", "plain");
    let (schema, batches) = events_batch(&[1, 2], &[10, 20]);
    land(
        &pool,
        &catalog,
        &src,
        &events_columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        seed_lineage(&src),
        None,
    )
    .await
    .expect("land plain batch table");

    let dst = tref("b", "out");
    let job = make_stream_mv_job(&src, &dst, 1, "select id, val from events", None);
    let err = handle_stream_mv(&ctx, job)
        .await
        .expect_err("a non-stream source must fail");
    assert!(
        matches!(err.policy, RetryPolicy::Abandon),
        "a non-stream source is deterministic (Abandon), got {:?}",
        err.policy
    );
    assert!(
        err.error.contains("mv delta:") && err.error.contains("not a declared log stream table"),
        "error names the refusal, got: {}",
        err.error
    );
}

// ---- MV watermark floor over the wire (road-mv-watermark-aware-gc) ----------

/// GC `src` over the wire, panicking on transport error (`what` labels which GC
/// call failed) — returns the reclaim + hold counts (`data_file_rows`,
/// `inline_rows`, `objects_deleted`, `held_by_mv_floor`) the scenarios below
/// assert against.
async fn gc_over_wire(engine: &GrpcQueueClient, src: &TableRef, what: &str) -> GcCounts {
    engine
        .gc_table(src.schema.clone(), src.name.clone())
        .await
        .expect(what)
}

/// Backdate EVERY snapshot so the whole history is aged out of the engine's GC
/// window (7 days in the test harness — `loom_test_flight::spawn_engine_uds`).
async fn age_all_snapshots(pool: &sqlx::PgPool) {
    let old = time::OffsetDateTime::now_utc() - time::Duration::days(365);
    sqlx::query("update iceberg_mirror.snapshot set snapshot_time = $1")
        .bind(old)
        .execute(pool)
        .await
        .expect("age all snapshots");
}

/// End-cap every live data file of `tid` at snapshot `snap` — the mirror state any
/// file-retiring path (compaction, a future small-file merge, stream retention)
/// leaves behind, and the only state GC ever reclaims. Done in SQL because the real
/// writers of it live above this crate (they must write replacement Parquet first).
async fn end_cap_data_files(pool: &sqlx::PgPool, tid: i64, snap: i64) {
    sqlx::query(
        "update iceberg_mirror.data_file set end_snapshot = $1 \
         where table_id = $2 and end_snapshot is null",
    )
    .bind(snap)
    .bind(tid)
    .execute(pool)
    .await
    .expect("end-cap data files");
}

/// Every `iceberg_mirror.data_file` row of `tid` (any `end_snapshot`) as
/// `(local path, max loom_offset)`, ordered by that offset: the Parquet object plus
/// the exact bound GC's file guard compares against the MV floor.
async fn data_files(pool: &sqlx::PgPool, tid: i64) -> Vec<(std::path::PathBuf, i64)> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "select df.path, cs.max_value::bigint from iceberg_mirror.data_file df \
         join iceberg_mirror.data_file_column_stat cs on cs.data_file_id = df.data_file_id \
         where df.table_id = $1 and cs.column_name = 'loom_offset' \
         order by cs.max_value::bigint",
    )
    .bind(tid)
    .fetch_all(pool)
    .await
    .expect("list data files with their loom_offset bound");
    rows.into_iter()
        .map(|(url, max_offset)| {
            (
                std::path::PathBuf::from(url.strip_prefix("file://").unwrap_or(&url)),
                max_offset,
            )
        })
        .collect()
}

/// GC's MV watermark floor, end to end over the engine wire
/// (road-mv-watermark-aware-gc): a source whose micro-batch MV is behind keeps the
/// BYTES of its unread tail — the end-capped FILE tier a compaction/retention path
/// leaves for GC — until the MV catches up, at which point GC converges and takes
/// both the mirror row and the Parquet object.
///
/// Scope, stated honestly: this proves BYTE RETENTION, not hole prevention. GC only
/// ever reclaims end-capped rows, which an MV delta (a current-snapshot read via
/// `mv_delta_scan`) cannot see in the first place — so no GC-tier guard can keep an
/// MV's delta complete. The hole is created at END-CAP time, and closing that is the
/// follow-up item this branch files. What the guard does deliver, and what is pinned
/// here, is that the bytes the lagging MV still needs are not destroyed underneath it.
///
/// The lag is produced through the real API: the events land in TWO batches and the
/// micro-batch runs only over the first, so the MV's watermark genuinely sits
/// mid-stream. Catch-up uses `advance_mv_watermark` (the same CAS
/// `pg_advance_mv_watermark` a micro-batch commit issues) — re-running the MV cannot
/// serve as catch-up here, because by then the source's files are end-capped and a
/// delta scan reads LIVE rows only.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_holds_a_lagging_mvs_end_capped_files_and_converges_on_catch_up() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
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
            flight: true,
            ..EngineOpts::default()
        },
    )
    .await;
    let ctx = build_ctx(&eng.sock).await;
    let engine = GrpcQueueClient::connect(&eng.sock)
        .await
        .expect("connect control");

    let src = tref("s", "events");
    let out = tref("s", "doubled");
    let sql = "select id, val * 2 as dbl from events";

    // 1. REGISTER the MV (no data trigger: this test drives its runs by hand, so the
    //    floor comes from the registration plus the watermarks those runs commit).
    cp.transforms()
        .define_transform(TransformDef {
            name: TransformName("mv_doubled".into()),
            body: TransformBody::MicroBatch {
                source: src.clone(),
                output: out.clone(),
                buckets: 1,
                sql: sql.to_string(),
            },
            schedule: None,
            on_input_commit: false,
        })
        .await
        .expect("register mv");

    // 2. Batch #1: 3 events into a 1-bucket log stream, landed straight to PARQUET
    //    (inline_byte_limit: 0) — the FILE tier is what this test is about. These are
    //    loom_offsets 0,1,2.
    let land_events = async |ids: &[i64], vals: &[i64]| {
        let (schema, batches) = events_batch(ids, vals);
        land(
            &pool,
            &catalog,
            &src,
            &events_columns(),
            schema,
            batches,
            InlineLimits {
                inline_byte_limit: 0,
                flush_byte_threshold: i64::MAX,
            },
            seed_lineage(&src),
            Some(1),
        )
        .await
        .expect("land events");
    };
    land_events(&[1, 2, 3], &[10, 20, 30]).await;

    // 3. Micro-batch #1 consumes exactly that batch: the watermark lands at 3.
    let (_, r1) = run_micro_batch(&cp, &ctx, &src, &out, 1, sql).await;
    r1.expect("first micro-batch");

    // 4. Batch #2 lands offsets 3,4,5 into a SECOND file — and the MV is not run
    //    again. It is now genuinely lagging at 3, through the real API only.
    land_events(&[4, 5, 6], &[40, 50, 60]).await;

    let mut conn = pool.acquire().await.expect("conn");
    let tid = live_table_id(&mut conn, &src.schema, &src.name)
        .await
        .expect("tid")
        .expect("live tid");
    drop(conn);
    let mv = mv_key(&out);
    assert_eq!(
        cp.mv_watermarks(&mv, tid).await.expect("watermarks"),
        [(0, 3)].into_iter().collect(),
        "the MV consumed batch #1 only: bucket 0 sits at offset 3 of 6"
    );

    // 5. A file-retiring path runs (compaction / retention): both live Parquet files
    //    are end-capped, and the history ages out. Now GC may take them.
    let before = data_files(&pool, tid).await;
    assert_eq!(
        before.len(),
        2,
        "each land wrote one Parquet file, got: {before:?}"
    );
    assert_eq!(
        before.iter().map(|f| f.1).collect::<Vec<_>>(),
        vec![2, 5],
        "file A carries offsets 0..2, file B carries 3..5"
    );
    let (file_a, file_b) = (before[0].0.clone(), before[1].0.clone());

    let snap = IcebergCatalog::new(pool.clone())
        .current_snapshot(&src)
        .await
        .expect("current snapshot")
        .id
        .0;
    end_cap_data_files(&pool, tid, snap).await;
    age_all_snapshots(&pool).await;

    // 6. GC over the wire. The floor (next_offset = 3) is above every offset in file A
    //    (max 2) but not file B (max 5): A is reclaimed, B is HELD — mirror row and
    //    Parquet object alike. Without the floor, BOTH would go.
    let counts = gc_over_wire(&engine, &src, "gc over the wire").await;
    assert_eq!(
        counts.data_file_rows, 1,
        "only file A — wholly below the floor — is taken"
    );
    assert_eq!(
        counts.objects_deleted, 1,
        "and exactly one Parquet object is deleted"
    );
    assert_eq!(
        counts.held_by_mv_floor, 1,
        "file B (max offset 5) sits above the floor (next_offset 3): its one data_file row is held by the MV floor"
    );

    assert_eq!(
        data_files(&pool, tid).await,
        vec![(file_b.clone(), 5)],
        "the lagging MV's unread offsets keep file B's mirror row alive"
    );
    assert!(
        file_b.exists(),
        "and its Parquet bytes are still on disk: {}",
        file_b.display()
    );
    assert!(
        !file_a.exists(),
        "the consumed file's Parquet is gone — the guard holds bytes, it does not stall GC"
    );

    // 7. The MV catches up (the CAS a micro-batch commit issues), and the floor
    //    releases: the next GC converges, taking file B's row AND its object.
    cp.advance_mv_watermark(
        &mv,
        tid,
        &[WatermarkAdvance {
            bucket: 0,
            from: 3,
            to: 6,
        }],
    )
    .await
    .expect("the mv catches up past the tail");

    let counts = gc_over_wire(&engine, &src, "second gc").await;
    assert_eq!(counts.data_file_rows, 1, "the held file is now reclaimed");
    assert_eq!(counts.objects_deleted, 1, "and its Parquet object with it");
    assert_eq!(
        counts.held_by_mv_floor, 0,
        "with the MV caught up, the floor holds nothing back"
    );
    assert!(
        data_files(&pool, tid).await.is_empty(),
        "with the MV caught up, GC converges: no data_file row survives"
    );
    assert!(
        !file_b.exists(),
        "the held Parquet object is finally reclaimed: {}",
        file_b.display()
    );
}

/// The linchpin (iss-mv-register-below-reclaimed-floor): an MV registered AFTER a
/// source's prefix was physically GC'd reads EXACTLY the surviving range and its first
/// run COMMITS — it is neither wedged (the CAS Conflict a rounded-down bootstrap once
/// triggered) nor short (a first delta that skips the surviving tail).
///
/// The ORDERING is the whole point, and it is deliberately the INVERSE of
/// `gc_holds_a_lagging_mvs_end_capped_files_and_converges_on_catch_up` above (which
/// registers the MV BEFORE the first land, so its floor keeps offset 0 alive and it
/// never exercises the truncated-source path):
///
///   1. Land offsets 0,1,2 — with NO MV registered, so the MV floor is `None` and GC is
///      unguarded.
///   2. End-cap + age + `gc_table`: offsets 0,1,2 are PHYSICALLY GONE.
///   3. Land offsets 3,4,5 — the surviving range.
///   4. NOW `define_transform` the MV. Its bootstrap seeds the watermark to the source's
///      earliest SURVIVING offset (3), not 0.
///   5. Run one micro-batch and prove all three: the run SUCCEEDS, the watermark advanced
///      to {0: 6} (the CAS moved it off the bootstrapped 3), and the output holds exactly
///      the 3 surviving rows — not 6 (it did not re-read the reclaimed prefix) and not 0
///      (the delta is not short).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mv_registered_over_a_gcd_source_reads_the_surviving_range_and_commits() {
    let MvEnv {
        cp,
        pool,
        catalog,
        ctx,
        engine,
        sql_client,
        fx: _fx,
        db: _db,
        eng: _eng,
        wh: _wh,
    } = setup_mv_env().await;

    let src = tref("s", "events");
    let out = tref("s", "doubled");
    let sql = "select id, val * 2 as dbl from events";

    // 1. Land offsets 0,1,2 straight to a Parquet FILE. NO MV is registered, so the MV
    //    floor is `None` and GC is unguarded.
    land_events(&pool, &catalog, &src, &[1, 2, 3], &[10, 20, 30], 1).await;

    let mut conn = pool.acquire().await.expect("conn");
    let tid = live_table_id(&mut conn, &src.schema, &src.name)
        .await
        .expect("tid")
        .expect("live tid");
    drop(conn);

    let before = data_files(&pool, tid).await;
    assert_eq!(
        before.iter().map(|f| f.1).collect::<Vec<_>>(),
        vec![2],
        "the first land wrote one file carrying offsets 0..2"
    );
    let file_a = before[0].0.clone();

    // 2. End-cap that file, age the history, and GC it over the wire. With no MV floor,
    //    GC really reclaims: offsets 0,1,2 are physically gone.
    let snap = IcebergCatalog::new(pool.clone())
        .current_snapshot(&src)
        .await
        .expect("current snapshot")
        .id
        .0;
    end_cap_data_files(&pool, tid, snap).await;
    age_all_snapshots(&pool).await;

    let counts = gc_over_wire(&engine, &src, "gc over the wire").await;
    assert_eq!(
        counts.data_file_rows, 1,
        "the unguarded prefix file is reclaimed"
    );
    assert_eq!(counts.objects_deleted, 1, "and its Parquet object with it");
    assert!(
        data_files(&pool, tid).await.is_empty(),
        "no live data_file survives: the prefix is physically gone"
    );
    assert!(
        !file_a.exists(),
        "the prefix's Parquet bytes are gone: {}",
        file_a.display()
    );

    // 3. Land the surviving range — offsets 3,4,5 — into a fresh file. The per-bucket
    //    offset allocator is persisted independently of GC, so these do NOT restart at 0.
    land_events(&pool, &catalog, &src, &[4, 5, 6], &[40, 50, 60], 1).await;
    let survivors = data_files(&pool, tid).await;
    assert_eq!(
        survivors.iter().map(|f| f.1).collect::<Vec<_>>(),
        vec![5],
        "the survivor file carries offsets 3..5"
    );

    // 4. NOW register the MV. Its bootstrap seeds the watermark to the source's earliest
    //    SURVIVING offset — 3, not 0.
    cp.transforms()
        .define_transform(TransformDef {
            name: TransformName("mv_doubled".into()),
            body: TransformBody::MicroBatch {
                source: src.clone(),
                output: out.clone(),
                buckets: 1,
                sql: sql.to_string(),
            },
            schedule: None,
            on_input_commit: false,
        })
        .await
        .expect("register mv over the truncated source");

    let mv = mv_key(&out);
    assert_eq!(
        cp.mv_watermarks(&mv, tid)
            .await
            .expect("bootstrap watermark"),
        [(0, 3)].into_iter().collect(),
        "the bootstrap seeded the watermark to the earliest surviving offset (3), not 0"
    );

    // 5. Run one micro-batch and prove the three acceptance criteria.
    let (rid, result) = run_micro_batch(&cp, &ctx, &src, &out, 1, sql).await;
    result.expect("first micro-batch over the truncated source");
    let run = cp.transforms().get_run(rid).await.expect("run");
    assert_eq!(
        run.state,
        RunState::Succeeded,
        "the run COMMITTED — not wedged: the CAS accepted the advance off the bootstrapped 3"
    );

    assert_eq!(
        cp.mv_watermarks(&mv, tid)
            .await
            .expect("watermark after run"),
        [(0, 6)].into_iter().collect(),
        "the delta covered offsets 3..6 and the CAS advanced the watermark from 3 to 6"
    );

    assert_eq!(
        doubled_rows(&sql_client).await,
        HashSet::from([(4, 80), (5, 100), (6, 120)]),
        "the output holds EXACTLY the 3 surviving rows — the delta is neither short (0 rows) \
         nor re-reading the reclaimed prefix (6 rows)"
    );
}

/// The production scenario, and the ONLY e2e that exercises the CAS relaxation
/// (iss-mv-register-below-reclaimed-floor): a MULTI-bucket source whose survivor file
/// spans buckets, so the bootstrap ROUNDS DOWN below one bucket's true surviving offset.
///
/// Why single-bucket (the test above) is not enough: a single-bucket survivor file gives
/// `earliest_surviving_offsets` an EXACT per-bucket bound (`loom_bucket` min == max), so the
/// bootstrap lands exactly at the delta's observed minimum and the strict `next_offset = from`
/// CAS still matches — the `<=` relaxation is never exercised. In PRODUCTION a flush is not
/// bucket-partitioned, so real survivor files ARE cross-bucket: the file carries only a
/// CROSS-bucket `loom_offset` min, which bounds EVERY bucket down to the lowest bucket's offset
/// (`mv_bootstrap.rs`). A bucket whose true surviving min sits above that cross bound is
/// bootstrapped BELOW its first surviving offset — and the first micro-batch's advance can only
/// commit because the CAS accepts `next_offset <= from`. This is the scenario the whole branch
/// exists for; the Task-3 revert reds THIS test (proven in the branch report).
///
/// Construction (fully deterministic — log-stream bucketing is `row_index % buckets`, offsets
/// from a persisted per-bucket cursor):
///   - Advance bucket 0's cursor to 2 with two single-row lands (each 1 row -> bucket 0 only),
///     then GC them: bucket 0 has NO live rows but its cursor stays at 2.
///   - Land a 4-row survivor batch: rows 0,2 -> bucket 0 (offsets 2,3), rows 1,3 -> bucket 1
///     (offsets 0,1), in ONE cross-bucket file whose `loom_offset` min is 0 (from bucket 1).
///   - Register the MV: the cross bound (0) bootstraps BOTH buckets to 0 — undershooting
///     bucket 0, whose first surviving offset is 2.
///   - Run once: bucket 0's delta observes min 2, so its advance is `{from: 2, to: 4}` against a
///     watermark row at 0 — the `next_offset <= from` case. The run must SUCCEED, the watermark
///     must reach {0: 4, 1: 2}, and the output must hold exactly the 4 survivors.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mv_over_a_gcd_cross_bucket_source_commits_from_an_undershooting_bootstrap() {
    let MvEnv {
        cp,
        pool,
        catalog,
        ctx,
        engine,
        sql_client,
        fx: _fx,
        db: _db,
        eng: _eng,
        wh: _wh,
    } = setup_mv_env().await;

    let src = tref("s", "events");
    let out = tref("s", "doubled");
    let sql = "select id, val * 2 as dbl from events";

    // 1. Two SINGLE-row lands into a 2-bucket log stream, straight to Parquet FILEs: each row 0
    //    maps to bucket 0 (row_index % 2 == 0), so bucket 0's cursor advances to 2 while bucket 1
    //    is never touched (cursor stays 0). No MV yet.
    land_events(&pool, &catalog, &src, &[1], &[10], 2).await;
    land_events(&pool, &catalog, &src, &[2], &[20], 2).await;

    let mut conn = pool.acquire().await.expect("conn");
    let tid = live_table_id(&mut conn, &src.schema, &src.name)
        .await
        .expect("tid")
        .expect("live tid");
    drop(conn);

    let prefix = data_files(&pool, tid).await;
    assert_eq!(
        prefix.len(),
        2,
        "two single-row lands wrote two bucket-0 files, got: {prefix:?}"
    );

    // 2. End-cap + age + GC the whole prefix. With no MV floor, GC reclaims both files: bucket 0
    //    has no live rows, but its persisted offset cursor stays at 2.
    let snap = IcebergCatalog::new(pool.clone())
        .current_snapshot(&src)
        .await
        .expect("current snapshot")
        .id
        .0;
    end_cap_data_files(&pool, tid, snap).await;
    age_all_snapshots(&pool).await;

    let counts = gc_over_wire(&engine, &src, "gc over the wire").await;
    assert_eq!(counts.data_file_rows, 2, "both prefix files are reclaimed");
    assert_eq!(
        counts.objects_deleted, 2,
        "and both Parquet objects with them"
    );
    assert!(
        data_files(&pool, tid).await.is_empty(),
        "no live data_file survives the prefix GC"
    );

    // 3. Land the 4-row survivor batch into ONE cross-bucket file:
    //      row 0 -> bucket 0 offset 2, row 1 -> bucket 1 offset 0,
    //      row 2 -> bucket 0 offset 3, row 3 -> bucket 1 offset 1.
    //    The file's `loom_offset` min is 0 (bucket 1); bucket 0's surviving min is 2.
    land_events(
        &pool,
        &catalog,
        &src,
        &[10, 11, 12, 13],
        &[100, 110, 120, 130],
        2,
    )
    .await;
    let survivors = data_files(&pool, tid).await;
    assert_eq!(
        survivors.len(),
        1,
        "the survivor land wrote a single cross-bucket file, got: {survivors:?}"
    );
    assert_eq!(
        survivors[0].1, 3,
        "its max loom_offset is 3 (bucket 0's top)"
    );

    // 4. Register the MV. The cross-bucket file bounds EVERY bucket to the file's min offset (0),
    //    so the bootstrap plants bucket 0 at 0 — BELOW its true surviving min of 2 (the undershoot
    //    the `<=` CAS exists to accept).
    cp.transforms()
        .define_transform(TransformDef {
            name: TransformName("mv_doubled".into()),
            body: TransformBody::MicroBatch {
                source: src.clone(),
                output: out.clone(),
                buckets: 2,
                sql: sql.to_string(),
            },
            schedule: None,
            on_input_commit: false,
        })
        .await
        .expect("register mv over the truncated cross-bucket source");

    let mv = mv_key(&out);
    assert_eq!(
        cp.mv_watermarks(&mv, tid)
            .await
            .expect("bootstrap watermark"),
        [(0, 0), (1, 0)].into_iter().collect(),
        "the cross-bucket file rounds both buckets down to 0 — bucket 0 UNDERSHOOTS its true min (2)"
    );

    // 5. Run one micro-batch. Bucket 0's delta observes min offset 2, so its advance is
    //    {from: 2, to: 4} against a watermark row sitting at 0 — the `next_offset <= from` case
    //    the CAS relaxation makes commit. Prove the three acceptance criteria.
    let (rid, result) = run_micro_batch(&cp, &ctx, &src, &out, 2, sql).await;
    result.expect("first micro-batch over the truncated cross-bucket source");
    let run = cp.transforms().get_run(rid).await.expect("run");
    assert_eq!(
        run.state,
        RunState::Succeeded,
        "the run COMMITTED — the CAS accepted bucket 0's advance from a watermark BELOW its delta min"
    );

    assert_eq!(
        cp.mv_watermarks(&mv, tid)
            .await
            .expect("watermark after run"),
        [(0, 4), (1, 2)].into_iter().collect(),
        "both buckets advanced past their surviving tail (bucket 0: 0 -> 4, bucket 1: 0 -> 2)"
    );

    assert_eq!(
        doubled_rows(&sql_client).await,
        HashSet::from([(10, 200), (11, 220), (12, 240), (13, 260)]),
        "the output holds EXACTLY the 4 surviving rows across both buckets — not short, not \
         re-reading the reclaimed prefix"
    );
}

/// The big symptom (#627): a queued MV run that OUTLIVES its def must re-create nothing.
/// `delete_transform` cancels no queue job, so a run enqueued-but-not-yet-executed before the
/// delete runs after it. With an empty `mv_watermarks` it would scan from offset 0, every
/// advance would be `from == 0` (the INSERT branch), and it would RESURRECT the output table
/// with a full re-materialization while re-pinning the source's MV floor at a defless key. The
/// def-existence guard turns this into a clean abandon with NO side effects: the CAS runs inside
/// the output-commit tx, so its `Validation` rolls the whole commit back — output uncreated,
/// watermark untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_mv_run_outliving_its_def_recreates_nothing() {
    let MvEnv {
        cp,
        pool,
        catalog,
        ctx,
        engine: _engine,
        sql_client: _sql_client,
        fx: _fx,
        db: _db,
        eng: _eng,
        wh: _wh,
    } = setup_mv_env().await;

    let src = tref("s", "events");
    let out = tref("s", "doubled");
    let sql = "select id, val * 2 as dbl from events";

    // 1. Seed the source log stream (3 rows, offsets 0,1,2) and register the MV over it.
    land_events(&pool, &catalog, &src, &[1, 2, 3], &[10, 20, 30], 1).await;
    cp.transforms()
        .define_transform(TransformDef {
            name: TransformName("mv_doubled".into()),
            body: TransformBody::MicroBatch {
                source: src.clone(),
                output: out.clone(),
                buckets: 1,
                sql: sql.to_string(),
            },
            schedule: None,
            on_input_commit: false,
        })
        .await
        .expect("register mv");

    // 2. Enqueue ONE MV run — the exact submit the happy path does — but do NOT run it yet.
    let rid = uuid::Uuid::new_v4();
    let body = TransformBody::MicroBatch {
        source: src.clone(),
        output: out.clone(),
        buckets: 1,
        sql: sql.to_string(),
    };
    let run = TransformRun {
        run_id: rid,
        transform: None,
        trigger: RunTrigger::AdHoc,
        state: RunState::Queued,
        body: body.clone(),
        queued_at: time::OffsetDateTime::now_utc(),
        started_at: None,
        finished_at: None,
        snapshot_id: None,
        error: None,
    };
    cp.transforms()
        .submit_run(run, body.to_job(rid))
        .await
        .expect("enqueue the mv run");

    // 3. Delete the def while its run sits queued: the run now outlives its def.
    cp.transforms()
        .delete_transform(&TransformName("mv_doubled".into()))
        .await
        .expect("delete the mv def");

    // 4. Run the worker job to completion: dequeue the still-queued job and handle it.
    let job = ctx
        .control
        .dequeue(&[STREAM_MV_JOB_KIND.to_string()], "e2e-worker")
        .await
        .expect("dequeue")
        .expect("the queued stream_mv job survives the def delete");
    let err = handle_stream_mv(&ctx, job)
        .await
        .expect_err("a run whose def is gone must not commit");

    // (b) Abandoned/Failed, and the message names the guard's refusal.
    assert!(
        matches!(err.policy, RetryPolicy::Abandon),
        "a deleted def is deterministic (Abandon), got {:?}",
        err.policy
    );
    assert!(
        err.error.contains("no micro-batch def names"),
        "the failure names the def-existence guard, got: {}",
        err.error
    );

    // (a) The output table was never resurrected: no columns were ever declared for it.
    let listed = ctx
        .control
        .list_files(out.schema.clone(), out.name.clone())
        .await
        .expect("list output");
    assert!(
        listed.columns.is_none(),
        "the refused commit rolled back: the output table does not exist"
    );

    // (c) No ghost watermark row was planted under the defless key.
    let mut conn = pool.acquire().await.expect("conn");
    let src_tid = live_table_id(&mut conn, &src.schema, &src.name)
        .await
        .expect("live_table_id")
        .expect("source is declared");
    drop(conn);
    assert!(
        cp.mv_watermarks(&mv_key(&out), src_tid)
            .await
            .expect("watermarks")
            .is_empty(),
        "the guard rolled back the CAS: no watermark row survives at the ghost key"
    );
}
