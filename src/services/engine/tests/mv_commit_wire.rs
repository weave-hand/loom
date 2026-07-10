//! Engine wire test for `CommitMicroBatch`: the atomicity heart of the
//! stream-continuous slice. In ONE Postgres transaction it inline-lands a
//! micro-batch result declared a log stream table, CAS-advances the source's
//! per-bucket watermark, and marks the driving run succeeded. Legs: the atomic
//! happy path, a stale CAS aborting the whole commit, an empty micro-batch
//! that just closes its run, and an output-collision guard (existing batch
//! table) that writes nothing.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlane, ControlPlaneError, DatasetRef, EventType, LineageEvent,
    MvWatermarks, RunId, RunState, RunTrigger, StreamKind, StreamTables, TableRef, TransformBody,
    TransformRun,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_inline::inline_append;
use engine_wire::client::GrpcQueueClient;
use engine_wire::pb;
use loom_test_flight::{EngineOpts, spawn_engine_uds};

// ---- helpers ---------------------------------------------------------------

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

fn id_spec() -> ColumnSpec {
    ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }
}

fn id_batch(ids: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids.to_vec()))]).expect("batch")
}

/// Encode `ids` as an Arrow IPC stream — exactly what a worker hands
/// `commit_micro_batch` as the micro-batch's result rows.
fn id_ipc(ids: &[i64]) -> Vec<u8> {
    let batch = id_batch(ids);
    let mut buf = Vec::new();
    {
        let mut w =
            arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

fn lin(inputs: &[TableRef], outputs: &[TableRef]) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: inputs.iter().map(DatasetRef::from).collect(),
        outputs: outputs.iter().map(DatasetRef::from).collect(),
        payload: serde_json::json!({ "source": "mv-commit-wire-test" }),
    }
}

/// Resolve the internal mirror `table_id`, the same way the other inline tests do
/// (`mv_delta.rs`'s `tid_of`).
async fn tid_of(pool: &sqlx::PgPool, schema: &str, name: &str) -> i64 {
    sqlx::query_scalar(
        "select table_id from iceberg_mirror.table \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
    )
    .bind(schema)
    .bind(name)
    .fetch_one(pool)
    .await
    .expect("table_id")
}

async fn table_exists(pool: &sqlx::PgPool, schema: &str, name: &str) -> bool {
    let n: i64 = sqlx::query_scalar(
        "select count(*) from iceberg_mirror.table \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
    )
    .bind(schema)
    .bind(name)
    .fetch_one(pool)
    .await
    .expect("count table");
    n > 0
}

fn adv(bucket: i32, from: i64, to: i64) -> pb::MvAdvance {
    pb::MvAdvance { bucket, from, to }
}

/// Submit a tracked `TransformRun` carrying a `MicroBatch` body (ad-hoc trigger,
/// no named def), mirroring the `submit_run` pattern `transform_e2e.rs` uses to
/// seed a run before driving its commit RPC. Returns the run id.
async fn submit_microbatch_run(
    cp: &PgControlPlane,
    source: &TableRef,
    output: &TableRef,
    buckets: i32,
) -> uuid::Uuid {
    let rid = uuid::Uuid::new_v4();
    let body = TransformBody::MicroBatch {
        source: source.clone(),
        output: output.clone(),
        buckets,
        sql: "select * from mv_delta".into(),
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
        .expect("submit run");
    rid
}

/// Every live inline row of `table`, in `(id, loom_change_kind, loom_bucket,
/// loom_offset)` form. Empty if the table has no live rows (or doesn't exist).
async fn framed_rows(pool: &sqlx::PgPool, table: &TableRef) -> Vec<(i64, String, i32, i64)> {
    let ice = IcebergCatalog::new(pool.clone());
    let Ok(cur) = ice.current_snapshot(table).await else {
        return Vec::new();
    };
    let Some((_, _, batch)) = ice
        .inline_live_batch_full(table, cur.id)
        .await
        .expect("inline read")
    else {
        return Vec::new();
    };
    let id_idx = batch.schema().index_of("id").expect("id col");
    let kind_idx = batch
        .schema()
        .index_of("loom_change_kind")
        .expect("loom_change_kind col");
    let bucket_idx = batch
        .schema()
        .index_of("loom_bucket")
        .expect("loom_bucket col");
    let offset_idx = batch
        .schema()
        .index_of("loom_offset")
        .expect("loom_offset col");
    let ids = batch
        .column(id_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id is Int64");
    let kinds = batch
        .column(kind_idx)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("loom_change_kind is Utf8");
    let buckets = batch
        .column(bucket_idx)
        .as_any()
        .downcast_ref::<arrow::array::Int32Array>()
        .expect("loom_bucket is Int32");
    let offsets = batch
        .column(offset_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("loom_offset is Int64");
    (0..batch.num_rows())
        .map(|i| {
            (
                ids.value(i),
                kinds.value(i).to_string(),
                buckets.value(i),
                offsets.value(i),
            )
        })
        .collect()
}

// ---- tests -----------------------------------------------------------------

/// Legs 1-2: the atomic happy path (rows land, watermark advances, run
/// succeeds, all together) and a stale CAS aborting the WHOLE commit (no rows,
/// no watermark move, no run success) — proving the transaction boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_micro_batch_atomic_happy_path_then_stale_cas_aborts_everything() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let wh_str = wh.path().display().to_string();

    let src = tref("s", "events");
    let out = tref("s", "out");
    let cols = vec![id_spec()];

    // Seed the source: a 2-bucket log stream, 4 rows (2 per bucket, offsets 0..2).
    inline_append(
        &pool,
        &src,
        &cols,
        &id_batch(&[1, 2, 3, 4]),
        lin(&[], std::slice::from_ref(&src)),
        None,
        Some(2),
    )
    .await
    .expect("seed source");
    let src_tid = tid_of(&pool, "s", "events").await;

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
    let client = GrpcQueueClient::connect(&eng.sock).await.expect("connect");

    // ---- Leg 1: atomic happy path ----
    let rid1 = submit_microbatch_run(&cp, &src, &out, 1).await;
    let advances1 = vec![adv(0, 0, 2), adv(1, 0, 2)];
    let snap1 = client
        .commit_micro_batch(
            "s.out".into(),
            "s".into(),
            "events".into(),
            "s".into(),
            "out".into(),
            1,
            &cols,
            id_ipc(&[100, 200, 300, 400]),
            &lin(std::slice::from_ref(&src), std::slice::from_ref(&out)),
            advances1,
            Some(rid1),
        )
        .await
        .expect("commit_micro_batch")
        .expect("snapshot id present");

    // Output declared a 1-bucket log stream table.
    let out_tid = tid_of(&pool, "s", "out").await;
    let meta = cp
        .stream_meta(out_tid)
        .await
        .expect("stream_meta")
        .expect("declared stream table");
    assert_eq!(meta.kind, StreamKind::Log, "output declared a log table");
    assert_eq!(meta.bucket_count, 1, "output declared with 1 bucket");

    // Rows carry +I framing with gapless bucket-0 offsets 0..4.
    let rows = framed_rows(&pool, &out).await;
    assert_eq!(rows.len(), 4, "all 4 result rows landed");
    for (_, kind, bucket, _) in &rows {
        assert_eq!(kind, "+I", "log-table rows are +I");
        assert_eq!(*bucket, 0, "buckets=1 puts every row in bucket 0");
    }
    let mut offsets: Vec<i64> = rows.iter().map(|(_, _, _, o)| *o).collect();
    offsets.sort_unstable();
    assert_eq!(offsets, vec![0, 1, 2, 3], "gapless offsets 0..4");

    // The source watermark advanced to exactly the advances' `to`s.
    let wm = cp
        .mv_watermarks("s.out", src_tid)
        .await
        .expect("mv_watermarks");
    assert_eq!(wm.get(&0), Some(&2), "bucket 0 watermark advanced to 2");
    assert_eq!(wm.get(&1), Some(&2), "bucket 1 watermark advanced to 2");

    // The run is Succeeded with the committed snapshot id.
    let run1 = cp.transforms().get_run(rid1).await.expect("get_run");
    assert_eq!(run1.state, RunState::Succeeded);
    assert_eq!(run1.snapshot_id, Some(snap1));

    // ---- Leg 2: stale CAS aborts the WHOLE commit ----
    let rid2 = submit_microbatch_run(&cp, &src, &out, 1).await;
    // Same `from = 0` as leg 1 — but the watermark already moved to 2, so this
    // is now stale for BOTH buckets.
    let advances2 = vec![adv(0, 0, 2), adv(1, 0, 2)];
    let err = client
        .commit_micro_batch(
            "s.out".into(),
            "s".into(),
            "events".into(),
            "s".into(),
            "out".into(),
            1,
            &cols,
            id_ipc(&[500, 600, 700, 800]),
            &lin(std::slice::from_ref(&src), std::slice::from_ref(&out)),
            advances2,
            Some(rid2),
        )
        .await
        .expect_err("stale CAS must fail");
    assert!(
        matches!(err, ControlPlaneError::Conflict(_)),
        "stale CAS maps to Conflict, got: {err:?}"
    );

    // Nothing partial: row count unchanged, watermark unchanged, run not succeeded.
    let rows_after = framed_rows(&pool, &out).await;
    assert_eq!(
        rows_after.len(),
        4,
        "no rows from the aborted commit landed"
    );
    let wm_after = cp
        .mv_watermarks("s.out", src_tid)
        .await
        .expect("mv_watermarks");
    assert_eq!(wm_after.get(&0), Some(&2), "watermark bucket 0 unchanged");
    assert_eq!(wm_after.get(&1), Some(&2), "watermark bucket 1 unchanged");
    let run2 = cp.transforms().get_run(rid2).await.expect("get_run");
    assert_eq!(
        run2.state,
        RunState::Queued,
        "the aborted commit's run was never marked succeeded"
    );
}

/// Leg 3: an empty micro-batch (no rows, no advances) just closes its run —
/// `Succeeded { snapshot_id: 0 }`, no snapshot id in the response, and no
/// output table is created.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_micro_batch_empty_closes_run_without_writing() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let wh_str = wh.path().display().to_string();

    let src = tref("s", "events");
    let out = tref("s", "empty_out");
    let cols = vec![id_spec()];

    inline_append(
        &pool,
        &src,
        &cols,
        &id_batch(&[1, 2]),
        lin(&[], std::slice::from_ref(&src)),
        None,
        Some(1),
    )
    .await
    .expect("seed source");

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
    let client = GrpcQueueClient::connect(&eng.sock).await.expect("connect");

    let rid = submit_microbatch_run(&cp, &src, &out, 1).await;
    let resp = client
        .commit_micro_batch(
            "s.empty_out".into(),
            "s".into(),
            "events".into(),
            "s".into(),
            "empty_out".into(),
            1,
            &cols,
            Vec::new(),
            &lin(std::slice::from_ref(&src), std::slice::from_ref(&out)),
            Vec::new(),
            Some(rid),
        )
        .await
        .expect("empty commit_micro_batch");
    assert_eq!(resp, None, "empty micro-batch reports no snapshot id");

    let run = cp.transforms().get_run(rid).await.expect("get_run");
    assert_eq!(run.state, RunState::Succeeded);
    assert_eq!(
        run.snapshot_id,
        Some(0),
        "empty micro-batch closes its run at snapshot_id 0"
    );

    assert!(
        !table_exists(&pool, "s", "empty_out").await,
        "an empty micro-batch creates no output table"
    );
}

/// Leg 4: committing against a PRE-EXISTING batch table as output is rejected
/// (the `reconcile_stream_mode` batch->stream guard) — `Validation`, nothing
/// written, the driving run left untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_micro_batch_output_collision_with_batch_table_is_rejected() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let wh_str = wh.path().display().to_string();

    let src = tref("s", "events");
    let collide = tref("s", "collide");
    let cols = vec![id_spec()];

    inline_append(
        &pool,
        &src,
        &cols,
        &id_batch(&[1, 2]),
        lin(&[], std::slice::from_ref(&src)),
        None,
        Some(1),
    )
    .await
    .expect("seed source");
    // A pre-existing BATCH table (no stream declaration) at the intended output name.
    inline_append(
        &pool,
        &collide,
        &cols,
        &id_batch(&[9]),
        lin(&[], std::slice::from_ref(&collide)),
        None,
        None,
    )
    .await
    .expect("seed batch collide table");

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
    let client = GrpcQueueClient::connect(&eng.sock).await.expect("connect");

    let rid = submit_microbatch_run(&cp, &src, &collide, 1).await;
    let err = client
        .commit_micro_batch(
            "s.collide".into(),
            "s".into(),
            "events".into(),
            "s".into(),
            "collide".into(),
            1,
            &cols,
            id_ipc(&[42]),
            &lin(std::slice::from_ref(&src), std::slice::from_ref(&collide)),
            vec![adv(0, 1, 2)],
            Some(rid),
        )
        .await
        .expect_err("batch->stream collision must be rejected");
    assert!(
        matches!(err, ControlPlaneError::Validation(_)),
        "batch->stream conversion maps to Validation, got: {err:?}"
    );

    // Nothing written: the collide table (still a plain batch table, never
    // declared a stream) carries only its original row. Read via raw SQL —
    // `inline_live_batch_full` projects the mirror-registered physical schema,
    // which for a batch table has no `loom_bucket`/`loom_offset` framing to
    // assert on, so a direct row count against its inline storage is the
    // simplest untouched-by-the-rejected-commit check.
    let collide_tid = tid_of(&pool, "s", "collide").await;
    // `AssertSqlSafe`: the table name is our own test-built identifier (a mirror
    // `table_id`, never user input), not injectable.
    let row_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select count(*) from iceberg_mirror.inline_{collide_tid}"
    )))
    .fetch_one(&pool)
    .await
    .expect("collide row count");
    assert_eq!(row_count, 1, "collide's original row is untouched");

    let run = cp.transforms().get_run(rid).await.expect("get_run");
    assert_eq!(
        run.state,
        RunState::Queued,
        "the rejected commit's run was never marked succeeded"
    );
}

/// Leg 5: a FILTERING micro-batch — it consumed a non-empty source delta
/// (`advances` non-empty) but its SQL dropped every row (`ipc` empty). The
/// watermark must still advance (else the consumed delta reprocesses
/// forever) and the run must succeed, but NO output table is declared and
/// NOTHING is landed. A stale second attempt over the same (now-advanced)
/// range is rejected via CAS, leaving the watermark untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_micro_batch_filtering_advances_watermark_without_output() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("wh");
    let wh_str = wh.path().display().to_string();

    let src = tref("s", "events");
    let out = tref("s", "filtered_out");
    let cols = vec![id_spec()];

    // Seed the source: a 2-bucket log stream, 4 rows (2 per bucket, offsets 0..2).
    inline_append(
        &pool,
        &src,
        &cols,
        &id_batch(&[1, 2, 3, 4]),
        lin(&[], std::slice::from_ref(&src)),
        None,
        Some(2),
    )
    .await
    .expect("seed source");
    let src_tid = tid_of(&pool, "s", "events").await;

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
    let client = GrpcQueueClient::connect(&eng.sock).await.expect("connect");

    // ---- The filtering micro-batch: consumed the delta, produced nothing ----
    let rid = submit_microbatch_run(&cp, &src, &out, 1).await;
    let advances = vec![adv(0, 0, 2), adv(1, 0, 2)];
    let resp = client
        .commit_micro_batch(
            "s.filtered_out".into(),
            "s".into(),
            "events".into(),
            "s".into(),
            "filtered_out".into(),
            1,
            &cols,
            Vec::new(), // empty ipc: the SQL filtered out every row
            &lin(std::slice::from_ref(&src), std::slice::from_ref(&out)),
            advances,
            Some(rid),
        )
        .await
        .expect("filtering commit_micro_batch");
    assert_eq!(
        resp, None,
        "filtering micro-batch with no output reports no snapshot id"
    );

    // The watermark still advanced to the advances' `to`s.
    let wm = cp
        .mv_watermarks("s.filtered_out", src_tid)
        .await
        .expect("mv_watermarks");
    assert_eq!(
        wm.get(&0),
        Some(&2),
        "bucket 0 watermark advanced despite empty output"
    );
    assert_eq!(
        wm.get(&1),
        Some(&2),
        "bucket 1 watermark advanced despite empty output"
    );

    // The run succeeded (closed at snapshot_id 0, same convention as the
    // both-empty case).
    let run = cp.transforms().get_run(rid).await.expect("get_run");
    assert_eq!(run.state, RunState::Succeeded);
    assert_eq!(
        run.snapshot_id,
        Some(0),
        "filtering micro-batch closes its run at snapshot_id 0"
    );

    // No output table was declared or created.
    assert!(
        !table_exists(&pool, "s", "filtered_out").await,
        "a filtering micro-batch with no output creates no output table"
    );

    // ---- Bonus: a stale second attempt over the same range is rejected ----
    let rid2 = submit_microbatch_run(&cp, &src, &out, 1).await;
    let stale_advances = vec![adv(0, 0, 2), adv(1, 0, 2)];
    let err = client
        .commit_micro_batch(
            "s.filtered_out".into(),
            "s".into(),
            "events".into(),
            "s".into(),
            "filtered_out".into(),
            1,
            &cols,
            Vec::new(),
            &lin(std::slice::from_ref(&src), std::slice::from_ref(&out)),
            stale_advances,
            Some(rid2),
        )
        .await
        .expect_err("stale CAS on a filtering commit must fail");
    assert!(
        matches!(err, ControlPlaneError::Conflict(_)),
        "stale CAS maps to Conflict, got: {err:?}"
    );
    let wm_after = cp
        .mv_watermarks("s.filtered_out", src_tid)
        .await
        .expect("mv_watermarks");
    assert_eq!(wm_after.get(&0), Some(&2), "watermark bucket 0 unchanged");
    assert_eq!(wm_after.get(&1), Some(&2), "watermark bucket 1 unchanged");
    let run2 = cp.transforms().get_run(rid2).await.expect("get_run");
    assert_eq!(
        run2.state,
        RunState::Queued,
        "the aborted stale commit's run was never marked succeeded"
    );
}
