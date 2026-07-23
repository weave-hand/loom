//! Slice-5 headline e2e: a lookup-join MV emits the enriched stream.
//! Multi-micro-batch (flush between), processing-time enrichment semantics,
//! keyed ≡ unkeyed equivalence, exactly-once rerun. loom_fixture_test.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlane, EventType, Job, JobId, LineageEvent, LookupOn, MergeEngine,
    MvWatermarks, ObjectType, Ontology, Queue, RetryPolicy, RunId, RunState, RunTrigger,
    STREAM_MV_JOB_KIND, StreamMvJob, StreamTables, TableRef, TransformBody, TransformDef,
    TransformName, TransformRun, Transforms, mv_key,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_inline::{
    current_inline_version, inline_append, write_inline_delta,
};
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_mirror::{ensure_table, live_table_id, next_snapshot};
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::{FlightSqlClient, FlightTableClient};
use loom_test_flight::{EngineOpts, spawn_engine_uds};
use loom_test_seed::local_sql_catalog;
use sqlx::PgPool;
use worker::stream_mv::{StreamMvCtx, handle_stream_mv};

// ---- helpers ---------------------------------------------------------------

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

fn orders_columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "customer_id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "amount".into(),
            ty: "long".into(),
            nullable: false,
        },
    ]
}

fn orders_batch(
    ids: &[i64],
    customer_ids: &[i64],
    amounts: &[i64],
) -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(customer_ids.to_vec())),
            Arc::new(Int64Array::from(amounts.to_vec())),
        ],
    )
    .expect("orders batch");
    (schema, vec![batch])
}

fn customer_columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "name".into(),
            ty: "string".into(),
            nullable: false,
        },
    ]
}

fn customer_row(id: i64, name: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![name.to_string()])),
        ],
    )
    .expect("customer row")
}

/// A zero-row batch matching the customers schema — appended so `s.customers`
/// is LIVE in the mirror with a known schema but no rows (the live-but-empty
/// enrich case).
fn empty_customers_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::new_empty(schema)
}

fn id_only_batch(id: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![id]))]).expect("id batch")
}

fn lin() -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "stream-mv-join-e2e-test" }),
    }
}

/// Declare `s.customers` a CDC table (identity `id`, `MergeEngine::LastRow`)
/// and an ontology type with that identity — the combination
/// `build_serving_provider` requires to route through the CDC-aware fold (see
/// `engine-serving/src/serving.rs`). Returns the mirror table id.
async fn declare_customers(cp: &PgControlPlane, pool: &PgPool, table: &TableRef) -> i64 {
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, &table.schema, &table.name, at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 2, "id", MergeEngine::LastRow)
        .await
        .expect("declare_cdc");
    cp.define_type(
        ObjectType::build(
            format!("Type_{}_{}", table.schema, table.name),
            (table.schema.clone(), table.name.clone()),
        )
        .prop_req("id", "Long")
        .prop_req("name", "String")
        .identity("id")
        .done(),
    )
    .await
    .expect("define_type");
    tid
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

/// Submit + dequeue + run one join micro-batch over the REAL queue, mirroring
/// `stream_mv_e2e.rs`'s `run_micro_batch` but driving `TransformBody::MicroBatchJoin`.
#[expect(
    clippy::too_many_arguments,
    reason = "test helper mirroring stream_mv_e2e.rs's run_micro_batch, +1 param (enrich table) for the join variant"
)]
async fn run_micro_batch_join(
    cp: &PgControlPlane,
    ctx: &StreamMvCtx,
    source: &TableRef,
    enrich: &TableRef,
    on: Option<LookupOn>,
    output: &TableRef,
    buckets: i32,
    sql: &str,
) -> (
    uuid::Uuid,
    std::result::Result<(), control_plane_core::JobFailure>,
) {
    let rid = uuid::Uuid::new_v4();
    let body = TransformBody::MicroBatchJoin {
        source: source.clone(),
        enrich: enrich.clone(),
        on,
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
        .expect("submit micro-batch join run");

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

fn make_stream_mv_join_job(
    source: &TableRef,
    enrich: &TableRef,
    on: Option<LookupOn>,
    output: &TableRef,
    buckets: i32,
    sql: &str,
) -> Job {
    Job {
        id: JobId(uuid::Uuid::new_v4()),
        kind: STREAM_MV_JOB_KIND.to_string(),
        payload: serde_json::to_value(StreamMvJob {
            source: source.clone(),
            output: output.clone(),
            buckets,
            sql: sql.to_string(),
            run_id: None,
            enrich: Some(enrich.clone()),
            on,
        })
        .expect("payload"),
        attempts: 0,
        run_at: time::OffsetDateTime::now_utc(),
    }
}

/// Register a `MicroBatchJoin` def whose `output` names `mv_key(output)`,
/// satisfying the def-existence guard `advance_mv_watermark` enforces (#627):
/// a watermark CAS is refused unless a live `microbatch`/`microbatch_join`
/// def names the key it advances. `on_input_commit: false` — every run in
/// this file is submitted ad hoc via `run_micro_batch_join`, so the def only
/// needs to EXIST for the guard; it must not itself become a data-trigger
/// seam and fire a run of its own.
fn mv_join_def(
    name: &str,
    source: &TableRef,
    enrich: &TableRef,
    on: Option<LookupOn>,
    output: &TableRef,
    buckets: i32,
    sql: &str,
) -> TransformDef {
    TransformDef {
        name: TransformName(name.into()),
        body: TransformBody::MicroBatchJoin {
            source: source.clone(),
            enrich: enrich.clone(),
            on,
            output: output.clone(),
            buckets,
            sql: sql.to_string(),
        },
        schedule: None,
        on_input_commit: false,
    }
}

/// Read `(id, customer_id, name, amount)` rows of `table` over the engine's SQL
/// serving path, as an order-independent set.
async fn enriched_rows(
    sql: &FlightSqlClient,
    schema: &str,
    table: &str,
) -> HashSet<(i64, i64, String, i64)> {
    let batches = sql
        .execute(format!(
            "select id, customer_id, name, amount from \"{schema}\".\"{table}\" order by id"
        ))
        .await
        .expect("query enriched");
    let mut out = HashSet::new();
    for b in &batches {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let cids = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("customer_id column");
        let names = b
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        let amounts = b
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("amount column");
        for i in 0..b.num_rows() {
            out.insert((
                ids.value(i),
                cids.value(i),
                names.value(i).to_string(),
                amounts.value(i),
            ));
        }
    }
    out
}

/// Assert every row of a framed batch carries `loom_change_kind = "+I"` and,
/// per bucket, gapless `loom_offset`s from zero.
fn assert_gapless_append_framing_per_bucket(batch: &RecordBatch) {
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

    let mut per_bucket: BTreeMap<i32, Vec<i64>> = BTreeMap::new();
    for i in 0..batch.num_rows() {
        assert_eq!(kinds.value(i), "+I", "every output row is a plain append");
        per_bucket
            .entry(buckets.value(i))
            .or_default()
            .push(offsets.value(i));
    }
    for (bucket, offs) in &mut per_bucket {
        offs.sort_unstable();
        let n = i64::try_from(offs.len()).expect("row count fits i64");
        let expected: Vec<i64> = (0..n).collect();
        assert_eq!(
            *offs, expected,
            "bucket {bucket} offsets are gapless from zero"
        );
    }
}

// ---- tests -----------------------------------------------------------------

/// Cases 1/2/3/5: the lookup-join emits the enriched stream, converges across
/// a flush boundary, enriches strictly with processing-time semantics (an
/// enrich-side update is visible to SUBSEQUENT batches only, never retro-
/// applied to already-emitted rows), and a no-op rerun changes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_join_emits_enriched_stream_and_converges() {
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

    let src = tref("s", "orders");
    let enrich = tref("s", "customers");
    let dst = tref("s", "enriched_orders");
    let on = LookupOn {
        source_col: "customer_id".to_string(),
        enrich_col: "id".to_string(),
    };
    let sql = "select o.id, o.customer_id, c.name, o.amount from orders o join customers c on o.customer_id = c.id";

    // 1. Seed customers (CDC, identity id): {1:"ada", 2:"bob"}.
    declare_customers(&cp, &pool, &enrich).await;
    inline_append(
        &pool,
        &enrich,
        &customer_columns(),
        &customer_row(1, "ada"),
        lin(),
        None,
        None,
    )
    .await
    .expect("seed customer 1");
    inline_append(
        &pool,
        &enrich,
        &customer_columns(),
        &customer_row(2, "bob"),
        lin(),
        None,
        None,
    )
    .await
    .expect("seed customer 2");

    // 2. Seed orders batch 1: (1,1,100), (2,2,200), declared a 2-bucket log stream.
    let (schema1, batches1) = orders_batch(&[1, 2], &[1, 2], &[100, 200]);
    land(
        &pool,
        &catalog,
        &src,
        &orders_columns(),
        schema1,
        batches1,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lin(),
        Some(2),
    )
    .await
    .expect("land orders batch 1");

    // Register the join MV's def BEFORE any run advances its watermark — the
    // def-existence guard (#627) refuses the CAS otherwise.
    cp.define_transform(mv_join_def(
        "enriched_orders_mv",
        &src,
        &enrich,
        Some(on.clone()),
        &dst,
        1,
        sql,
    ))
    .await
    .expect("register enriched_orders join def (guard #627)");

    // Case 1: the lookup-join emits the enriched stream.
    let (rid1, result1) =
        run_micro_batch_join(&cp, &ctx, &src, &enrich, Some(on.clone()), &dst, 1, sql).await;
    result1.expect("first join micro-batch");
    let run1 = cp.transforms().get_run(rid1).await.expect("run1");
    assert_eq!(run1.state, RunState::Succeeded, "run1 succeeded");

    let after1 = enriched_rows(&sql_client, "s", "enriched_orders").await;
    assert_eq!(
        after1,
        HashSet::from([
            (1, 1, "ada".to_string(), 100),
            (2, 2, "bob".to_string(), 200),
        ]),
        "batch 1: the enriched (joined) rows land"
    );

    // Structural framing: +I, gapless per bucket from 0.
    let ice = IcebergCatalog::new(pool.clone());
    let snap = ice.current_snapshot(&dst).await.expect("output snapshot");
    let (_out_tid, _row_ids, out_batch) = ice
        .inline_live_batch_full(&dst, snap.id)
        .await
        .expect("read output framing")
        .expect("output has live rows");
    assert_gapless_append_framing_per_bucket(&out_batch);

    // Case 2: flush the source, land a second batch, converge across the flush.
    flush_table(&catalog, &pool, &src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush source");
    let (schema2, batches2) = orders_batch(&[3], &[1], &[50]);
    land(
        &pool,
        &catalog,
        &src,
        &orders_columns(),
        schema2,
        batches2,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lin(),
        Some(2),
    )
    .await
    .expect("land orders batch 2");

    let (rid2, result2) =
        run_micro_batch_join(&cp, &ctx, &src, &enrich, Some(on.clone()), &dst, 1, sql).await;
    result2.expect("second join micro-batch");
    let run2 = cp.transforms().get_run(rid2).await.expect("run2");
    assert_eq!(run2.state, RunState::Succeeded, "run2 succeeded");

    let after2 = enriched_rows(&sql_client, "s", "enriched_orders").await;
    assert_eq!(
        after2,
        HashSet::from([
            (1, 1, "ada".to_string(), 100),
            (2, 2, "bob".to_string(), 200),
            (3, 1, "ada".to_string(), 50),
        ]),
        "batch 2: convergence across the flush boundary, no duplicates"
    );

    let snap2 = ice.current_snapshot(&dst).await.expect("output snapshot 2");
    let (_out_tid2, _row_ids2, out_batch2) = ice
        .inline_live_batch_full(&dst, snap2.id)
        .await
        .expect("read output framing 2")
        .expect("output has live rows");
    assert_gapless_append_framing_per_bucket(&out_batch2);

    // Case 3: processing-time semantics. Update customer 1's name, land a
    // third batch, run again: batch-3 rows carry the NEW name; batch-1/2 rows
    // are untouched (no retroactive re-emission).
    let v0 = current_inline_version(
        &pool,
        &enrich,
        &[customer_columns()[0].clone()],
        "id",
        &id_only_batch(1),
    )
    .await
    .expect("version before update");
    write_inline_delta(
        &pool,
        &enrich,
        &customer_columns(),
        "id",
        false,
        &customer_row(1, "ada2"),
        Some((&customer_columns(), &customer_row(1, "ada"))),
        lin(),
        v0,
        None,
        &[],
    )
    .await
    .expect("update customer 1's name");

    let (schema3, batches3) = orders_batch(&[4, 5], &[1, 2], &[75, 80]);
    land(
        &pool,
        &catalog,
        &src,
        &orders_columns(),
        schema3,
        batches3,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lin(),
        Some(2),
    )
    .await
    .expect("land orders batch 3");

    let (rid3, result3) =
        run_micro_batch_join(&cp, &ctx, &src, &enrich, Some(on.clone()), &dst, 1, sql).await;
    result3.expect("third join micro-batch");
    let run3 = cp.transforms().get_run(rid3).await.expect("run3");
    assert_eq!(run3.state, RunState::Succeeded, "run3 succeeded");

    let after3 = enriched_rows(&sql_client, "s", "enriched_orders").await;
    assert_eq!(
        after3,
        HashSet::from([
            (1, 1, "ada".to_string(), 100),
            (2, 2, "bob".to_string(), 200),
            (3, 1, "ada".to_string(), 50),
            (4, 1, "ada2".to_string(), 75),
            (5, 2, "bob".to_string(), 80),
        ]),
        "batch 3: new rows carry the NEW name; earlier rows are untouched"
    );

    // Case 5: exactly-once rerun. No new source rows -> empty-delta no-op.
    let (rid4, result4) =
        run_micro_batch_join(&cp, &ctx, &src, &enrich, Some(on), &dst, 1, sql).await;
    result4.expect("idempotent re-run");
    let run4 = cp.transforms().get_run(rid4).await.expect("run4");
    assert_eq!(
        run4.state,
        RunState::Succeeded,
        "run4 succeeded (empty-delta no-op)"
    );
    assert_eq!(
        enriched_rows(&sql_client, "s", "enriched_orders").await,
        after3,
        "re-running with no new source rows changed nothing"
    );
}

/// Case 4: a keyed lookup-join (`on: Some`) and an unkeyed state-join
/// (`on: None`) over the same source/enrich topology produce identical
/// enriched row-sets.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyed_and_unkeyed_join_produce_identical_rows() {
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

    let src = tref("s", "orders");
    let enrich = tref("s", "customers");
    let dst_keyed = tref("s", "enriched_keyed");
    let dst_unkeyed = tref("s", "enriched_unkeyed");
    let sql = "select o.id, o.customer_id, c.name, o.amount from orders o join customers c on o.customer_id = c.id";

    declare_customers(&cp, &pool, &enrich).await;
    inline_append(
        &pool,
        &enrich,
        &customer_columns(),
        &customer_row(1, "ada"),
        lin(),
        None,
        None,
    )
    .await
    .expect("seed customer 1");
    inline_append(
        &pool,
        &enrich,
        &customer_columns(),
        &customer_row(2, "bob"),
        lin(),
        None,
        None,
    )
    .await
    .expect("seed customer 2");

    let (schema, batches) = orders_batch(&[1, 2, 3], &[1, 2, 1], &[10, 20, 30]);
    land(
        &pool,
        &catalog,
        &src,
        &orders_columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lin(),
        Some(2),
    )
    .await
    .expect("land orders");

    let on = LookupOn {
        source_col: "customer_id".to_string(),
        enrich_col: "id".to_string(),
    };

    // Register both join MVs' defs BEFORE either run advances its watermark —
    // the def-existence guard (#627) refuses the CAS otherwise.
    cp.define_transform(mv_join_def(
        "enriched_keyed_mv",
        &src,
        &enrich,
        Some(on.clone()),
        &dst_keyed,
        1,
        sql,
    ))
    .await
    .expect("register enriched_keyed join def (guard #627)");
    cp.define_transform(mv_join_def(
        "enriched_unkeyed_mv",
        &src,
        &enrich,
        None,
        &dst_unkeyed,
        1,
        sql,
    ))
    .await
    .expect("register enriched_unkeyed join def (guard #627)");

    let (rid_keyed, result_keyed) =
        run_micro_batch_join(&cp, &ctx, &src, &enrich, Some(on), &dst_keyed, 1, sql).await;
    result_keyed.expect("keyed join run");
    assert_eq!(
        cp.transforms().get_run(rid_keyed).await.expect("run").state,
        RunState::Succeeded
    );

    let (rid_unkeyed, result_unkeyed) =
        run_micro_batch_join(&cp, &ctx, &src, &enrich, None, &dst_unkeyed, 1, sql).await;
    result_unkeyed.expect("unkeyed (state-join) run");
    assert_eq!(
        cp.transforms()
            .get_run(rid_unkeyed)
            .await
            .expect("run")
            .state,
        RunState::Succeeded
    );

    let keyed_rows = enriched_rows(&sql_client, "s", "enriched_keyed").await;
    let unkeyed_rows = enriched_rows(&sql_client, "s", "enriched_unkeyed").await;
    assert_eq!(
        keyed_rows,
        HashSet::from([
            (1, 1, "ada".to_string(), 10),
            (2, 2, "bob".to_string(), 20),
            (3, 1, "ada".to_string(), 30),
        ]),
        "keyed lookup-join produces the joined rows"
    );
    assert_eq!(
        keyed_rows, unkeyed_rows,
        "keyed lookup-join and unkeyed state-join produce identical row-sets"
    );
}

/// Case 6: deterministic abandons. An unknown enrich table, and a
/// `LookupOn.source_col` missing from the delta, both fail the run terminally
/// (never retried) — matching the taxonomy `plain_batch_source_abandons`
/// proves for the source side.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deterministic_abandons() {
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

    let src = tref("s", "orders");
    let enrich = tref("s", "customers");
    declare_customers(&cp, &pool, &enrich).await;
    inline_append(
        &pool,
        &enrich,
        &customer_columns(),
        &customer_row(1, "ada"),
        lin(),
        None,
        None,
    )
    .await
    .expect("seed customer 1");

    let (schema, batches) = orders_batch(&[1], &[1], &[100]);
    land(
        &pool,
        &catalog,
        &src,
        &orders_columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lin(),
        Some(2),
    )
    .await
    .expect("land orders");

    let sql = "select o.id, o.customer_id, c.name, o.amount from orders o join customers c on o.customer_id = c.id";

    // 6a. Unknown enrich table.
    let unknown_enrich = tref("s", "nope");
    let on = LookupOn {
        source_col: "customer_id".to_string(),
        enrich_col: "id".to_string(),
    };
    let dst1 = tref("s", "enriched_bad1");
    let job1 = make_stream_mv_join_job(&src, &unknown_enrich, Some(on.clone()), &dst1, 1, sql);
    let err1 = handle_stream_mv(&ctx, job1)
        .await
        .expect_err("an unknown enrich table must fail");
    assert!(
        matches!(err1.policy, RetryPolicy::Abandon),
        "unknown enrich table is deterministic (Abandon), got {:?}",
        err1.policy
    );
    assert!(
        err1.error.contains("mv enrich:"),
        "error names the mv-enrich refusal, got: {}",
        err1.error
    );

    // 6b. `LookupOn.source_col` missing from the delta.
    let bad_on = LookupOn {
        source_col: "nonexistent_col".to_string(),
        enrich_col: "id".to_string(),
    };
    let dst2 = tref("s", "enriched_bad2");
    let job2 = make_stream_mv_join_job(&src, &enrich, Some(bad_on), &dst2, 1, sql);
    let err2 = handle_stream_mv(&ctx, job2)
        .await
        .expect_err("a missing source_col must fail");
    assert!(
        matches!(err2.policy, RetryPolicy::Abandon),
        "a missing source_col is deterministic (Abandon), got {:?}",
        err2.policy
    );
    assert!(
        err2.error.contains("lookup key"),
        "error names the lookup-key refusal, got: {}",
        err2.error
    );
}

/// Case 7: a join against a LIVE-BUT-EMPTY enrich table succeeds by joining
/// against an empty table. `s.customers` is declared (live in the mirror, known
/// schema) but carries ZERO rows; two `orders` rows are landed and the join MV
/// runs. The engine sends the enrich schema unconditionally (`with_schema`) even
/// though the enrich response is zero batches, so the worker registers an EMPTY
/// enrich table from that schema: the INNER JOIN yields zero rows, the run
/// SUCCEEDS (not Abandon), and the SOURCE watermark ADVANCES past the consumed
/// delta — regression coverage for the slice-5 Task 5 review gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_but_empty_enrich_joins_as_empty_table_and_succeeds() {
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

    let src = tref("s", "orders");
    let enrich = tref("s", "customers");
    let dst = tref("s", "enriched_empty");
    let on = LookupOn {
        source_col: "customer_id".to_string(),
        enrich_col: "id".to_string(),
    };
    let sql = "select o.id, o.customer_id, c.name, o.amount from orders o join customers c on o.customer_id = c.id";

    // Declare customers LIVE (CDC, identity id) but append ZERO rows, so it is
    // live-but-empty: a known schema in the mirror, no data.
    declare_customers(&cp, &pool, &enrich).await;
    inline_append(
        &pool,
        &enrich,
        &customer_columns(),
        &empty_customers_batch(),
        lin(),
        None,
        None,
    )
    .await
    .expect("seed empty customers");

    // Land two orders rows, a 2-bucket log stream.
    let (schema, batches) = orders_batch(&[1, 2], &[1, 2], &[100, 200]);
    land(
        &pool,
        &catalog,
        &src,
        &orders_columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lin(),
        Some(2),
    )
    .await
    .expect("land orders");

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

    // Register the join MV's def BEFORE the run advances its watermark — the
    // def-existence guard (#627) refuses the CAS otherwise.
    cp.define_transform(mv_join_def(
        "enriched_empty_mv",
        &src,
        &enrich,
        Some(on.clone()),
        &dst,
        1,
        sql,
    ))
    .await
    .expect("register enriched_empty join def (guard #627)");

    let (rid, result) =
        run_micro_batch_join(&cp, &ctx, &src, &enrich, Some(on), &dst, 1, sql).await;
    result.expect("join against a live-but-empty enrich succeeds");
    let run = cp.transforms().get_run(rid).await.expect("run");
    assert_eq!(
        run.state,
        RunState::Succeeded,
        "the run succeeds joining against an empty enrich table (not Abandon)"
    );

    // The INNER JOIN against an empty enrich yields zero rows: the empty-ipc
    // commit branch never declares the output table.
    let listed = ctx
        .control
        .list_files(dst.schema.clone(), dst.name.clone())
        .await
        .expect("list output");
    assert!(
        listed.columns.is_none(),
        "the empty-join output never declared the output table"
    );

    // The source watermark advances past the consumed delta.
    let after = cp
        .mv_watermarks(&mv, src_tid)
        .await
        .expect("watermarks after");
    assert_ne!(
        after, before,
        "the source watermark advanced past the consumed delta"
    );
    assert!(!after.is_empty(), "at least one bucket's watermark moved");
}
