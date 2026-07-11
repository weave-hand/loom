//! Task 6: the enrich-edge trigger/cycle/composability e2e. Most of this
//! behavior falls out of Task 1's `TriggerNode::resolve` arm, which already
//! lists BOTH `source` and `enrich` as a `MicroBatchJoin` def's data-trigger
//! inputs (`src/control-plane/core/src/transforms.rs`) — this test PINS it at
//! the worker level (real queue, real engine, `handle_stream_mv`):
//!
//! 1. an enrich-only commit wakes the join MV (debounced), and the triggered
//!    run processes an EMPTY source delta as a no-op (no NEW output rows,
//!    source watermark unchanged, run Succeeded);
//! 2. a source commit produces the join MV's enriched output;
//! 3. the join MV's own output commit is itself a data-trigger seam that
//!    fires a plain slice-4 `MicroBatch` MV defined over its output
//!    (composability); and
//! 4. an enrich-edge cycle (`MV1.enrich == MV2.output`, `MV2.source ==
//!    MV1.output`) is rejected at define time.
//!
//! Cases 1-3 share one scenario: an AD HOC join micro-batch (submitted
//! directly, mirroring `stream_mv_join_e2e.rs`'s `run_micro_batch_join`, no
//! `on_input_commit` def involved yet) seeds `s.enriched_orders` with one row
//! and advances the source watermark — establishing a known non-empty
//! baseline using ONLY the proven `land`/`handle_stream_mv` path from Task 5,
//! rather than an untested "land a zero-row batch" combination. The
//! `on_input_commit` defs are registered only AFTER that baseline, so Case 1's
//! enrich-only commit is guaranteed to see a truly empty (already-consumed)
//! source delta.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, ControlPlaneError, EventType, LineageEvent, LookupOn, MergeEngine,
    MvWatermarks, ObjectType, Ontology, PropertyConstraints, PropertyDef, Queue, RunState,
    RunTrigger, STREAM_MV_JOB_KIND, StreamMvJob, StreamTables, TableRef, TransformBody,
    TransformDef, TransformName, Transforms, TypeName, mv_key,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_inline::inline_append;
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

fn orders_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

fn orders_batch(ids: &[i64], customer_ids: &[i64], amounts: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        orders_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Int64Array::from(customer_ids.to_vec())),
            Arc::new(Int64Array::from(amounts.to_vec())),
        ],
    )
    .expect("orders batch")
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

fn lin() -> LineageEvent {
    LineageEvent {
        run_id: control_plane_core::RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "stream-mv-join-triggers-test" }),
    }
}

/// Declare `s.customers` a CDC table (identity `id`, `MergeEngine::LastRow`)
/// and an ontology type with that identity — mirrors
/// `stream_mv_join_e2e.rs`'s `declare_customers`, needed for the engine's
/// CDC-aware enrich fold.
async fn declare_customers(cp: &PgControlPlane, pool: &PgPool, table: &TableRef) {
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, &table.schema, &table.name, at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 2, "id", MergeEngine::LastRow)
        .await
        .expect("declare_cdc");
    cp.define_type(ObjectType {
        name: TypeName(format!("Type_{}_{}", table.schema, table.name)),
        table: table.clone(),
        identity: Some("id".to_string()),
        version: None,
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                constraints: PropertyConstraints::default(),
            },
            PropertyDef {
                name: "name".into(),
                ty: "String".into(),
                required: true,
                constraints: PropertyConstraints::default(),
            },
        ],
        derived: vec![],
    })
    .await
    .expect("define_type");
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

fn join_mv_def(
    name: &str,
    source: &TableRef,
    enrich: &TableRef,
    on: Option<LookupOn>,
    output: &TableRef,
    sql: &str,
) -> TransformDef {
    TransformDef {
        name: TransformName(name.into()),
        body: TransformBody::MicroBatchJoin {
            source: source.clone(),
            enrich: enrich.clone(),
            on,
            output: output.clone(),
            buckets: 1,
            sql: sql.into(),
        },
        schedule: None,
        on_input_commit: true,
    }
}

fn plain_mv_def(name: &str, source: &TableRef, output: &TableRef, sql: &str) -> TransformDef {
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

/// Dequeue one `stream_mv` job over the real queue, decode its `StreamMvJob`
/// payload, and return both — mirrors the dequeue half of
/// `stream_mv_join_e2e.rs`'s `run_micro_batch_join`, but WITHOUT submitting a
/// run: every run in this test is fired by the data-trigger seam itself
/// (`pg_fire_data_triggers`), not an ad-hoc `submit_run`.
async fn dequeue_stream_mv_job(ctx: &StreamMvCtx) -> (control_plane_core::Job, StreamMvJob) {
    let job = ctx
        .control
        .dequeue(&[STREAM_MV_JOB_KIND.to_string()], "e2e-worker")
        .await
        .expect("dequeue")
        .expect("a queued stream_mv job, fired by the data-trigger seam");
    assert_eq!(job.kind, STREAM_MV_JOB_KIND, "kind survives the queue");
    let payload: StreamMvJob =
        serde_json::from_value(job.payload.clone()).expect("decode StreamMvJob");
    (job, payload)
}

/// Submit + dequeue + run ONE ad hoc join micro-batch over the REAL queue —
/// mirrors `stream_mv_join_e2e.rs`'s `run_micro_batch_join`, used here only to
/// establish a known non-empty baseline BEFORE the `on_input_commit` defs
/// exist (so it cannot itself be data-triggered).
#[expect(
    clippy::too_many_arguments,
    reason = "test helper mirroring stream_mv_join_e2e.rs's run_micro_batch_join"
)]
async fn run_micro_batch_join_adhoc(
    cp: &PgControlPlane,
    ctx: &StreamMvCtx,
    source: &TableRef,
    enrich: &TableRef,
    on: Option<LookupOn>,
    output: &TableRef,
    buckets: i32,
    sql: &str,
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
    let run = control_plane_core::TransformRun {
        run_id: rid,
        transform: None,
        trigger: control_plane_core::RunTrigger::AdHoc,
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
        .expect("submit ad hoc join micro-batch run");

    let (job, _payload) = dequeue_stream_mv_job(ctx).await;
    handle_stream_mv(ctx, job)
        .await
        .expect("ad hoc join micro-batch seeds the baseline");
    let run = cp.transforms().get_run(rid).await.expect("ad hoc run");
    assert_eq!(run.state, RunState::Succeeded, "ad hoc seed run succeeds");
}

/// Read `(id, customer_id, name, amount)` rows of `table` over the engine's
/// SQL serving path, as an order-independent set — mirrors
/// `stream_mv_join_e2e.rs`'s `enriched_rows`.
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

// ---- tests -----------------------------------------------------------------

/// Cases 1-3: an enrich-only commit wakes the join MV and no-ops on an empty
/// source delta; a source commit produces the join MV's enriched output; and
/// that output commit is itself a data-trigger seam that fires a downstream
/// plain `MicroBatch` MV defined over it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enrich_and_source_triggers_compose_downstream() {
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
    let downstream_out = tref("s", "enriched_summary");
    let on = LookupOn {
        source_col: "customer_id".to_string(),
        enrich_col: "id".to_string(),
    };
    let join_sql = "select o.id, o.customer_id, c.name, o.amount from orders o \
                     join customers c on o.customer_id = c.id";
    let downstream_sql = "select id, customer_id, name, amount from enriched_orders";

    // ---- Baseline: an AD HOC join micro-batch seeds enriched_orders, using
    // ONLY the proven Task-5 land/handle_stream_mv path (no on_input_commit
    // def exists yet, so nothing here is data-triggered). -------------------

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

    land(
        &pool,
        &catalog,
        &src,
        &orders_columns(),
        orders_schema(),
        vec![orders_batch(&[1], &[1], &[999])],
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lin(),
        Some(2),
    )
    .await
    .expect("land the seed orders row");

    run_micro_batch_join_adhoc(
        &cp,
        &ctx,
        &src,
        &enrich,
        Some(on.clone()),
        &dst,
        1,
        join_sql,
    )
    .await;

    let baseline = enriched_rows(&sql_client, "s", "enriched_orders").await;
    assert_eq!(
        baseline,
        HashSet::from([(1, 1, "ada".to_string(), 999)]),
        "the ad hoc seed run lands the one baseline row"
    );

    // ---- Now register the on_input_commit defs. Defining fires nothing: no
    // commit has happened to either def's resolved inputs since. ------------

    cp.define_transform(join_mv_def(
        "join_mv",
        &src,
        &enrich,
        Some(on.clone()),
        &dst,
        join_sql,
    ))
    .await
    .expect("define join_mv");
    cp.define_transform(plain_mv_def(
        "downstream_mv",
        &dst,
        &downstream_out,
        downstream_sql,
    ))
    .await
    .expect("define downstream_mv");

    let mv = mv_key(&dst);
    let mut conn = pool.acquire().await.expect("conn");
    let src_tid = live_table_id(&mut conn, &src.schema, &src.name)
        .await
        .expect("live_table_id")
        .expect("orders is declared");
    drop(conn);

    let watermark_before_case1 = cp
        .mv_watermarks(&mv, src_tid)
        .await
        .expect("watermarks before Case 1");
    assert!(
        !watermark_before_case1.is_empty(),
        "the ad hoc seed run already advanced the source watermark"
    );

    // ---- Case 1: an enrich-only commit wakes the join MV, no-ops. ---------

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
    .expect("seed customer 2 -- fires join_mv's data trigger");

    let (job1, payload1) = dequeue_stream_mv_job(&ctx).await;
    assert_eq!(payload1.source, src, "frozen source");
    assert_eq!(payload1.enrich, Some(enrich.clone()), "frozen enrich");
    assert_eq!(payload1.output, dst, "frozen output");
    let run_id1 = payload1
        .run_id
        .expect("data-triggered job carries a run_id");

    handle_stream_mv(&ctx, job1)
        .await
        .expect("Case 1: an empty source delta is a clean no-op");
    let run1 = cp.transforms().get_run(run_id1).await.expect("run1");
    assert_eq!(
        run1.state,
        RunState::Succeeded,
        "Case 1: the no-op run still succeeds"
    );
    assert_eq!(
        run1.trigger,
        RunTrigger::DataTrigger,
        "Case 1: fired by the enrich commit, not ad hoc"
    );

    let after_case1 = enriched_rows(&sql_client, "s", "enriched_orders").await;
    assert_eq!(
        after_case1, baseline,
        "Case 1: no NEW output rows land from an empty source delta"
    );

    let watermark_after_case1 = cp
        .mv_watermarks(&mv, src_tid)
        .await
        .expect("watermarks after Case 1");
    assert_eq!(
        watermark_after_case1, watermark_before_case1,
        "Case 1: the source watermark is UNCHANGED — an empty source delta \
         carries zero advances"
    );

    // ---- Case 2: a source commit produces the join MV's enriched output. -

    land(
        &pool,
        &catalog,
        &src,
        &orders_columns(),
        orders_schema(),
        vec![orders_batch(&[10, 11], &[1, 1], &[100, 200])],
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lin(),
        Some(2),
    )
    .await
    .expect("land 2 orders rows -- fires join_mv's data trigger again");

    let (job2, payload2) = dequeue_stream_mv_job(&ctx).await;
    assert_eq!(payload2.source, src);
    assert_eq!(payload2.output, dst);
    let run_id2 = payload2
        .run_id
        .expect("data-triggered job carries a run_id");

    handle_stream_mv(&ctx, job2)
        .await
        .expect("Case 2: the source commit's delta joins and lands");
    let run2 = cp.transforms().get_run(run_id2).await.expect("run2");
    assert_eq!(
        run2.state,
        RunState::Succeeded,
        "Case 2: the triggered run succeeds"
    );
    assert_eq!(
        run2.trigger,
        RunTrigger::DataTrigger,
        "Case 2: fired by the source commit, not ad hoc"
    );

    let after_case2 = enriched_rows(&sql_client, "s", "enriched_orders").await;
    assert_eq!(
        after_case2,
        HashSet::from([
            (1, 1, "ada".to_string(), 999),
            (10, 1, "ada".to_string(), 100),
            (11, 1, "ada".to_string(), 200),
        ]),
        "Case 2: the triggered run's 2 new enriched rows land ALONGSIDE the \
         baseline row"
    );

    // ---- Case 3: the join MV's output commit fires the downstream MV. -----

    // Flush enriched_orders to Parquet before it is read as the downstream
    // MV's SOURCE: `mv_delta_scan`'s file read resolves the table through the
    // Iceberg catalog (`read_files_as_batches` -> `catalog.load_table`), which
    // only ever gets a row via the Parquet-write path (`ensure_iceberg_table`)
    // — an MV output that has ONLY ever been inline-appended (as every
    // `commit_micro_batch` write is) has no such row yet. Every other worker
    // fixture test sidesteps this by landing its SOURCE with
    // `inline_byte_limit: 0` (forcing a Parquet write); an MV's OUTPUT has no
    // such landing call, so this explicit flush is the composability
    // equivalent — mirrors `stream_mv_e2e.rs`'s convergence-across-flush use
    // of the same helper.
    flush_table(
        &catalog,
        &pool,
        &dst,
        control_plane_core::RunId(uuid::Uuid::new_v4()),
    )
    .await
    .expect("flush enriched_orders before it is read as downstream_mv's source");

    let (job3, payload3) = dequeue_stream_mv_job(&ctx).await;
    assert_eq!(
        payload3.source, dst,
        "Case 3: the downstream MV's job sources the join MV's OUTPUT"
    );
    assert_eq!(payload3.output, downstream_out);
    assert_eq!(
        payload3.enrich, None,
        "Case 3: the downstream MV is a plain (non-join) MicroBatch"
    );
    let run_id3 = payload3
        .run_id
        .expect("data-triggered job carries a run_id");

    handle_stream_mv(&ctx, job3)
        .await
        .expect("Case 3: the downstream MV's micro-batch runs");
    let run3 = cp.transforms().get_run(run_id3).await.expect("run3");
    assert_eq!(
        run3.state,
        RunState::Succeeded,
        "Case 3: the downstream MV's triggered run succeeds"
    );
    assert_eq!(
        run3.trigger,
        RunTrigger::DataTrigger,
        "Case 3: fired by the join MV's own output commit, not ad hoc"
    );

    let downstream_after = enriched_rows(&sql_client, "s", "enriched_summary").await;
    assert_eq!(
        downstream_after, after_case2,
        "Case 3: the downstream MV composes off the join MV's output commit \
         and reproduces the same rows"
    );
}

/// Case 4: an enrich-edge trigger cycle — `mv1: source=s.a, enrich=s.out2,
/// output=s.out1` and `mv2: source=s.out1, enrich=s.b, output=s.out2` — is
/// rejected at define time, because `TriggerNode::resolve` lists `enrich`
/// (not just `source`) among a `MicroBatchJoin` def's data-trigger inputs, so
/// `validate_no_trigger_cycle` sees the `mv2 -> mv1` edge through the enrich
/// side, closing the cycle `mv1 -> mv2 -> mv1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enrich_edge_cycle_rejected_at_define_time() {
    let fx = PgFixture::shared();
    let (pg, _db) = fx.fresh_db().await;

    let a = tref("c", "a");
    let b = tref("c", "b");
    let out1 = tref("c", "out1");
    let out2 = tref("c", "out2");

    pg.define_transform(join_mv_def(
        "mv1",
        &a,
        &out2,
        None,
        &out1,
        "select 1 from a join out2 on true",
    ))
    .await
    .expect("define mv1 -- no cycle yet, mv2 does not exist");

    let err = pg
        .define_transform(join_mv_def(
            "mv2",
            &out1,
            &b,
            None,
            &out2,
            "select 1 from out1 join b on true",
        ))
        .await
        .expect_err(
            "mv1 -> mv2 (via out1) and mv2 -> mv1 (via the out2 ENRICH edge) \
             forms a data-trigger cycle",
        );
    assert!(
        matches!(err, ControlPlaneError::Validation(_)),
        "got {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("mv1") && msg.contains("mv2"),
        "error names both defs: {msg}"
    );
}
