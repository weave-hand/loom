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
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::{FlightSqlClient, FlightTableClient};
use loom_test_flight::{EngineOpts, spawn_engine_uds};
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
    let (file_rows, _inline_rows, objects) = engine
        .gc_table(src.schema.clone(), src.name.clone())
        .await
        .expect("gc over the wire");
    assert_eq!(
        file_rows, 1,
        "only file A — wholly below the floor — is taken"
    );
    assert_eq!(objects, 1, "and exactly one Parquet object is deleted");

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

    let (file_rows2, _, objects2) = engine
        .gc_table(src.schema.clone(), src.name.clone())
        .await
        .expect("second gc");
    assert_eq!(file_rows2, 1, "the held file is now reclaimed");
    assert_eq!(objects2, 1, "and its Parquet object with it");
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
