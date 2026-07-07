//! Data triggers fire inside the landing/commit transactions (slice 3).
//! Each leg seeds a physical data-triggered def whose input is the table
//! being written, performs one commit path, and asserts on the run rows.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use loom_test_seed::local_sql_catalog;

use control_plane_core::{
    ColumnSpec, ColumnStat, ControlPlane, DataFile, DatasetId, EventType, FileFormat, LineageEvent,
    OutputMode, PageReq, RunId, RunState, RunTrigger, StatValue, TableControlPlane, TableRef,
    TransformBody, TransformDef, TransformName, TransformRun, Transforms,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_landing::{
    InlineLimits, StepLand, land, overwrite_parquet_snapshot, write_steps,
};

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

fn dt_def(name: &str, input: &TableRef, output: (&str, &str)) -> TransformDef {
    TransformDef {
        name: TransformName(name.into()),
        body: TransformBody::Physical {
            inputs: vec![input.clone()],
            output: tref(output.0, output.1),
            sql: "select 1".into(),
            output_mode: OutputMode::Append,
        },
        schedule: None,
        on_input_commit: true,
    }
}

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// A schema + single batch of `rows` rows, one `id: long` column (ids `0..rows`).
fn ipc_body(rows: i64) -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
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
        payload: serde_json::json!({ "source": "test" }),
    }
}

fn data_file(path: &str, rows: i64) -> DataFile {
    DataFile {
        path: path.into(),
        path_is_relative: true,
        file_format: FileFormat::Parquet,
        record_count: rows,
        file_size_bytes: rows * 16,
        column_stats: vec![ColumnStat {
            column_name: "id".into(),
            null_count: 0,
            column_size_bytes: rows * 8,
            min: Some(StatValue::I64(0)),
            max: Some(StatValue::I64(rows - 1)),
        }],
        parquet_footer_size: Some(120),
    }
}

async fn iceberg_cp(fx: &PgFixture) -> (IcebergControlPlane, tempfile::TempDir) {
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    (IcebergControlPlane::new(pg, catalog), wh)
}

/// Leg 1: an inline land (small batch) fires the matching data-triggered def.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_land_fires_a_data_trigger_run() {
    let fx = PgFixture::shared();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let input = tref("main", "input");
    let def = dt_def("dep", &input, ("main", "out"));
    pg.define_transform(def.clone()).await.expect("define");

    let (schema, batches) = ipc_body(3);
    land(
        &pool,
        &catalog,
        &input,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), &input),
        None,
    )
    .await
    .expect("inline land");

    let runs = pg
        .list_runs(Some(&TransformName("dep".into())), PageReq::default())
        .await
        .expect("list_runs");
    assert_eq!(runs.items.len(), 1, "exactly one fired run");
    let r = &runs.items[0];
    assert_eq!(r.state, RunState::Queued);
    assert_eq!(r.trigger, RunTrigger::DataTrigger);
    assert_eq!(r.transform, Some(TransformName("dep".into())));
    assert_eq!(r.body, def.body);
}

/// Leg 2: a Parquet land fires once; a second land while the run is still
/// Queued debounces (no second run); once the run is Running, a third land
/// fires again (Running does not suppress).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parquet_land_fires_and_debounces() {
    let fx = PgFixture::shared();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let input = tref("main", "input");
    let def = dt_def("dep", &input, ("main", "out"));
    pg.define_transform(def).await.expect("define");

    let name = TransformName("dep".into());
    let limits = InlineLimits {
        inline_byte_limit: 0, // forces the Parquet branch
        flush_byte_threshold: i64::MAX,
    };

    // First land: fires one run.
    let (schema, batches) = ipc_body(5);
    land(
        &pool,
        &catalog,
        &input,
        &columns(),
        schema,
        batches,
        limits,
        lineage(RunId(uuid::Uuid::new_v4()), &input),
        None,
    )
    .await
    .expect("land 1");
    let runs = pg
        .list_runs(Some(&name), PageReq::default())
        .await
        .expect("list_runs 1");
    assert_eq!(runs.items.len(), 1, "one run after first land");
    let run_id = runs.items[0].run_id;

    // Second land while the run is still Queued: debounced, still one run.
    let (schema, batches) = ipc_body(5);
    land(
        &pool,
        &catalog,
        &input,
        &columns(),
        schema,
        batches,
        limits,
        lineage(RunId(uuid::Uuid::new_v4()), &input),
        None,
    )
    .await
    .expect("land 2");
    let runs = pg
        .list_runs(Some(&name), PageReq::default())
        .await
        .expect("list_runs 2");
    assert_eq!(runs.items.len(), 1, "still one run — debounced");

    // Mark the run Running: a third land now fires again.
    pg.mark_run_running(run_id).await.expect("mark running");
    let (schema, batches) = ipc_body(5);
    land(
        &pool,
        &catalog,
        &input,
        &columns(),
        schema,
        batches,
        limits,
        lineage(RunId(uuid::Uuid::new_v4()), &input),
        None,
    )
    .await
    .expect("land 3");
    let runs = pg
        .list_runs(Some(&name), PageReq::default())
        .await
        .expect("list_runs 3");
    assert_eq!(runs.items.len(), 2, "Running does not suppress refiring");
}

/// Leg 3: a single `write_steps` call writing two distinct tables fires each
/// matched def once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_steps_fires_per_matched_def() {
    let fx = PgFixture::shared();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let in1 = tref("main", "in1");
    let in2 = tref("main", "in2");
    let def_a = dt_def("depA", &in1, ("main", "outA"));
    let def_b = dt_def("depB", &in2, ("main", "outB"));
    pg.define_transform(def_a).await.expect("define A");
    pg.define_transform(def_b).await.expect("define B");

    let (_, batches1) = ipc_body(3);
    let (_, batches2) = ipc_body(4);
    let steps = vec![
        StepLand {
            table: in1.clone(),
            columns: columns(),
            batches: batches1,
            overwrite: false,
        },
        StepLand {
            table: in2.clone(),
            columns: columns(),
            batches: batches2,
            overwrite: false,
        },
    ];
    write_steps(
        &pool,
        &catalog,
        steps,
        lineage(RunId(uuid::Uuid::new_v4()), &in1),
    )
    .await
    .expect("write_steps");

    let runs_a = pg
        .list_runs(Some(&TransformName("depA".into())), PageReq::default())
        .await
        .expect("list_runs A");
    assert_eq!(runs_a.items.len(), 1, "depA fired once");
    let runs_b = pg
        .list_runs(Some(&TransformName("depB".into())), PageReq::default())
        .await
        .expect("list_runs B");
    assert_eq!(runs_b.items.len(), 1, "depB fired once");
}

/// Leg 4: a def on a table that is not written never fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unmatched_table_does_not_fire() {
    let fx = PgFixture::shared();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let other = tref("main", "other");
    let input = tref("main", "input");
    let def = dt_def("dep-other", &other, ("main", "out"));
    pg.define_transform(def).await.expect("define");

    let (schema, batches) = ipc_body(3);
    land(
        &pool,
        &catalog,
        &input,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), &input),
        None,
    )
    .await
    .expect("land into unrelated table");

    let runs = pg
        .list_runs(Some(&TransformName("dep-other".into())), PageReq::default())
        .await
        .expect("list_runs");
    assert!(runs.items.is_empty(), "unmatched def never fires");
}

/// Leg 5: both the non-empty overwrite path and the zero-row truncate path
/// fire their matching data-triggered defs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_and_truncate_fire() {
    let fx = PgFixture::shared();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let input = tref("main", "input");

    // Seed the table with a real Parquet append first (overwrite needs an
    // existing table).
    let (schema, batches) = ipc_body(5);
    land(
        &pool,
        &catalog,
        &input,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), &input),
        None,
    )
    .await
    .expect("seed append");

    let def1 = dt_def("dep-ov", &input, ("main", "o1"));
    pg.define_transform(def1).await.expect("define dep-ov");

    // Non-empty overwrite fires dep-ov.
    overwrite_parquet_snapshot(
        &pool,
        &catalog,
        &input,
        &columns(),
        vec![ipc_body(2).1.remove(0)],
        Some(&lineage(RunId(uuid::Uuid::new_v4()), &input)),
    )
    .await
    .expect("overwrite");
    let runs1 = pg
        .list_runs(Some(&TransformName("dep-ov".into())), PageReq::default())
        .await
        .expect("list_runs dep-ov");
    assert_eq!(runs1.items.len(), 1, "overwrite fires dep-ov");

    // A fresh def for the truncate leg (dep-ov's run is still Queued and
    // would otherwise debounce a second firing).
    let def2 = dt_def("dep-trunc", &input, ("main", "o2"));
    pg.define_transform(def2).await.expect("define dep-trunc");

    // Zero-row overwrite -> the truncate branch.
    overwrite_parquet_snapshot(&pool, &catalog, &input, &columns(), vec![], None)
        .await
        .expect("truncate");
    let runs2 = pg
        .list_runs(Some(&TransformName("dep-trunc".into())), PageReq::default())
        .await
        .expect("list_runs dep-trunc");
    assert_eq!(runs2.items.len(), 1, "truncate fires dep-trunc");
}

/// Leg 6: `IcebergTx::commit` fires a downstream def and threads
/// `committing_run` through to the hook (self-skip plumbing).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn icebergtx_fires_downstream_and_marks_committing_run() {
    let fx = PgFixture::shared();
    let (cp, _wh) = iceberg_cp(fx).await;

    let src = tref("main", "src");
    let dst = tref("main", "dst");
    let def_a = dt_def("A", &src, ("main", "dst"));
    let def_b = dt_def("B", &dst, ("main", "sink"));
    cp.transforms()
        .define_transform(def_a.clone())
        .await
        .expect("define A");
    cp.transforms()
        .define_transform(def_b)
        .await
        .expect("define B");

    let rid = uuid::Uuid::new_v4();
    let run = TransformRun {
        run_id: rid,
        transform: Some(TransformName("A".into())),
        trigger: RunTrigger::Manual,
        state: RunState::Queued,
        body: def_a.body.clone(),
        queued_at: time::OffsetDateTime::now_utc(),
        started_at: None,
        finished_at: None,
        snapshot_id: None,
        error: None,
    };
    let job = def_a.body.to_job(rid);
    cp.transforms()
        .submit_run(run, job)
        .await
        .expect("submit R");
    cp.transforms()
        .mark_run_running(rid)
        .await
        .expect("mark running");

    let mut tx = cp.begin_table().await.expect("begin_table");
    tx.create_table(&dst, &columns())
        .await
        .expect("create_table");
    tx.append_files(&dst, &[data_file("a.parquet", 3)])
        .await
        .expect("append_files");
    tx.mark_run_succeeded(rid)
        .await
        .expect("mark_run_succeeded");
    tx.commit().await.expect("commit").expect("snapshot");

    let runs_b = cp
        .transforms()
        .list_runs(Some(&TransformName("B".into())), PageReq::default())
        .await
        .expect("list_runs B");
    assert_eq!(runs_b.items.len(), 1, "B fired exactly one run");
    assert_eq!(runs_b.items[0].state, RunState::Queued);
    assert_eq!(runs_b.items[0].trigger, RunTrigger::DataTrigger);

    let got_r = cp.transforms().get_run(rid).await.expect("get_run R");
    assert_eq!(got_r.state, RunState::Succeeded);

    let runs_a = cp
        .transforms()
        .list_runs(Some(&TransformName("A".into())), PageReq::default())
        .await
        .expect("list_runs A");
    assert_eq!(runs_a.items.len(), 1, "A still has only its own run R");
    assert_eq!(runs_a.items[0].run_id, rid);
}

/// Leg 7: flushing an inline-written table (a data-preserving rewrite) does
/// NOT refire the def that already fired on the original inline land.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_does_not_refire() {
    let fx = PgFixture::shared();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let input = tref("main", "input");
    let def = dt_def("dep", &input, ("main", "out"));
    pg.define_transform(def).await.expect("define");
    let name = TransformName("dep".into());

    // Land inline with a low flush threshold so live bytes cross it and a
    // flush job is enqueued.
    let (schema, batches) = ipc_body(3);
    land(
        &pool,
        &catalog,
        &input,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: 1,
        },
        lineage(RunId(uuid::Uuid::new_v4()), &input),
        None,
    )
    .await
    .expect("inline land 1");

    let runs = pg
        .list_runs(Some(&name), PageReq::default())
        .await
        .expect("list_runs after land");
    assert_eq!(runs.items.len(), 1, "one run after the inline land");

    // Drive the flush directly (mirrors tests/iceberg_flush.rs).
    let flush_run = RunId(uuid::Uuid::new_v4());
    flush_table(&catalog, &pool, &input, flush_run)
        .await
        .expect("flush")
        .expect("flushed something");

    let runs_after = pg
        .list_runs(Some(&name), PageReq::default())
        .await
        .expect("list_runs after flush");
    assert_eq!(
        runs_after.items.len(),
        1,
        "flush must not refire the already-fired def"
    );
}

/// Leg 8: an undecodable def body is skipped (not fatal) — a second, healthy
/// def matching the same commit still fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broken_def_body_is_skipped() {
    let fx = PgFixture::shared();
    let (pg, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;

    let input = tref("main", "input1");
    let def1 = dt_def("dep1", &input, ("main", "out1"));
    let def2 = dt_def("dep2", &input, ("main", "out2"));
    pg.define_transform(def1).await.expect("define dep1");
    pg.define_transform(def2).await.expect("define dep2");

    // Corrupt dep1's body directly — raw runtime SQL, fine in tests.
    sqlx::query("update transforms.transform set body = '\"nonsense\"'::jsonb where name = $1")
        .bind("dep1")
        .execute(&pool)
        .await
        .expect("corrupt dep1 body");

    let (schema, batches) = ipc_body(3);
    land(
        &pool,
        &catalog,
        &input,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: usize::MAX,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), &input),
        None,
    )
    .await
    .expect("land commits despite a poisoned def");

    let runs2 = pg
        .list_runs(Some(&TransformName("dep2".into())), PageReq::default())
        .await
        .expect("list_runs dep2");
    assert_eq!(runs2.items.len(), 1, "the healthy def still fires");

    let runs1 = pg
        .list_runs(Some(&TransformName("dep1".into())), PageReq::default())
        .await
        .expect("list_runs dep1");
    assert!(runs1.items.is_empty(), "the poisoned def never fires");
}

/// An undecodable EXISTING on_input_commit def must not 500 a subsequent define:
/// the trigger-cycle scan skips it and validates the decodable subset.
#[tokio::test]
async fn define_survives_poison_existing_def() {
    let fx = PgFixture::shared();
    let (pg, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let input = tref("main", "input1");
    pg.define_transform(dt_def("existing", &input, ("main", "out_existing")))
        .await
        .expect("define existing");

    // Corrupt the existing def's body directly (bypasses define_transform).
    sqlx::query("update transforms.transform set body = '\"nonsense\"'::jsonb where name = $1")
        .bind("existing")
        .execute(&pool)
        .await
        .expect("corrupt existing body");

    // A fresh on_input_commit define runs the trigger-cycle scan over every other
    // def — the poison one must be skipped, not fatal.
    pg.define_transform(dt_def("fresh", &input, ("main", "out_fresh")))
        .await
        .expect("define fresh succeeds despite a poison existing def");
}
