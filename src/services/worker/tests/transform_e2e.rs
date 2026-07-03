//! Engine-wire physical transform e2e: seed inputs as real Iceberg Parquet, run the
//! worker's `handle_transform` over the wire (ListFiles -> Flight read -> DataFusion
//! SQL -> write -> CommitTransform), and assert rows, lineage (byte-identical
//! `{"sql": ...}` payload), snapshots, overwrite time travel, and the Abandon
//! taxonomy edges (unknown input, ambiguous registration).

use loom_test_flight::{EngineOpts, spawn_engine_uds};
use loom_test_seed::local_sql_catalog;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlane, DatasetId, EventType, Job, JobId, LineageEvent, NewJob,
    OutputMode, Queue, RetryPolicy, RunId, SnapshotId, TRANSFORM_JOB_KIND, TableControlPlane,
    TableRef, TransformJob,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_control_plane::IcebergControlPlane;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::{FlightTableClient, FlightTicket};
use store_config::{ObjectStoreConfig, build_write_store};
use worker::transform::{TransformCtx, handle_transform};

// ---- helpers ---------------------------------------------------------------

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

/// A schema + batch (single `id: Int64` column) of `ids`, for `land` seeding.
fn ipc_body(ids: &[i64]) -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
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
        payload: serde_json::json!({ "source": "transform-e2e-test" }),
    }
}

fn make_transform_job(
    inputs: &[TableRef],
    output: &TableRef,
    sql: &str,
    output_mode: OutputMode,
) -> Job {
    Job {
        id: JobId(uuid::Uuid::new_v4()),
        kind: TRANSFORM_JOB_KIND.to_string(),
        payload: serde_json::to_value(TransformJob {
            inputs: inputs.to_vec(),
            output: output.clone(),
            sql: sql.to_string(),
            output_mode,
        })
        .unwrap(),
        attempts: 0,
        run_at: time::OffsetDateTime::now_utc(),
    }
}

/// Build a `TransformCtx` against the spawned engine's UDS, writing into the
/// shared warehouse (same physical root the engine serves).
async fn build_ctx(sock: &str, wh_str: &str) -> TransformCtx {
    let mut env_map = HashMap::new();
    env_map.insert("LOOM_WAREHOUSE_URI".to_string(), format!("file://{wh_str}"));
    let store_cfg = ObjectStoreConfig::parse_from_env(&env_map).expect("store config");
    let write = Arc::new(build_write_store(&store_cfg).expect("write store"));
    let control = GrpcQueueClient::connect(sock)
        .await
        .expect("connect control");
    let flight = FlightTableClient::connect(sock)
        .await
        .expect("connect flight");
    TransformCtx {
        control,
        flight,
        write,
        write_cfg: datafusion_io::WriteConfig::default(),
        worker_tuning: loom_config::WorkerTuning::default(),
    }
}

/// Read the single-Int64-column values of `table` at `snap` back over Flight.
async fn read_i64s(
    flight: &FlightTableClient,
    ice: &IcebergCatalog,
    table: &TableRef,
    snap: SnapshotId,
) -> HashSet<i64> {
    let files = ice.files_with_stats(table, snap).await.expect("files");
    let batches = flight
        .fetch(FlightTicket {
            schema: table.schema.clone(),
            name: table.name.clone(),
            files: files.iter().map(|f| f.path.clone()).collect(),
        })
        .await
        .expect("flight read-back");
    let mut vals = HashSet::new();
    for b in &batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        for i in 0..col.len() {
            vals.insert(col.value(i));
        }
    }
    vals
}

/// Create an input table with a schema but NO data files (current_snapshot exists,
/// `files` is empty, `schema` resolves) — the empty-input fixture the edge-2 cases need.
async fn create_empty_table(cp: &IcebergControlPlane, table: &TableRef, columns: &[ColumnSpec]) {
    let mut tx = cp.begin_table().await.unwrap();
    tx.create_table(table, columns).await.unwrap();
    // Register the table in the mirror with an EMPTY file list: the table/columns/schema
    // are projected at the snapshot (so `current_snapshot`/`schema` resolve), but no data
    // files exist — the "live input with zero files" fixture. A create-only commit would
    // allocate a snapshot without a mirror `table` row, so the input would read as NotFound.
    tx.append_files(table, &[]).await.unwrap();
    tx.commit().await.unwrap();
}

// ---- tests -----------------------------------------------------------------

/// Happy path over the REAL queue: enqueue a `"transform"` job through the fixture's
/// Postgres queue, dequeue it via the engine-wire `GrpcQueueClient` (pins the kind
/// string end-to-end), run `handle_transform`, and assert the output rows, the
/// committed snapshot, and the byte-identical lineage payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transform_runs_over_the_wire() {
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

    // Seed main.src with ids [1,2,3] as one real Parquet file.
    let src = tref("main", "src");
    let (schema, batches) = ipc_body(&[1, 2, 3]);
    land(
        &pool,
        &catalog,
        &src,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        seed_lineage(&src),
    )
    .await
    .expect("land");

    // Enqueue through the fixture's Postgres queue...
    let dst = tref("main", "dst");
    let sql = "SELECT id FROM src WHERE id >= 2";
    cp.queue()
        .enqueue(NewJob {
            kind: TRANSFORM_JOB_KIND.to_string(),
            payload: serde_json::to_value(TransformJob {
                inputs: vec![src.clone()],
                output: dst.clone(),
                sql: sql.to_string(),
                output_mode: OutputMode::Append,
            })
            .expect("payload"),
            run_at: None,
            priority: 0,
        })
        .await
        .expect("enqueue");

    // ...and dequeue it over the wire, like the worker binary does.
    let ctx = build_ctx(&eng.sock, &wh_str).await;
    let job = ctx
        .control
        .dequeue(&[TRANSFORM_JOB_KIND.to_string()], "e2e-worker")
        .await
        .expect("dequeue")
        .expect("a queued transform job");
    assert_eq!(job.kind, TRANSFORM_JOB_KIND, "kind survives the queue");

    handle_transform(&ctx, job).await.expect("transform");

    // Output rows: ids {2,3} land in main.dst, readable over Flight.
    let ice = IcebergCatalog::new(pool.clone());
    let snap = ice.current_snapshot(&dst).await.expect("output snapshot");
    let ids = read_i64s(&ctx.flight, &ice, &dst, snap.id).await;
    assert_eq!(
        ids,
        HashSet::from([2, 3]),
        "the transform's predicate filtered the rows"
    );

    // Lineage: the emitted payload is byte-identical to `{"sql": <job sql>}` and the
    // dataset edges name the physical tables.
    let payload: serde_json::Value = sqlx::query_scalar(
        "select e.payload from lineage.event e \
         join lineage.event_dataset d on d.event_id = e.event_id and d.direction = 'output' \
         where d.name = $1",
    )
    .bind("main.dst")
    .fetch_one(&pool)
    .await
    .expect("lineage payload");
    assert_eq!(
        payload,
        serde_json::json!({ "sql": sql }),
        "lineage payload is byte-identical"
    );

    let inputs: Vec<String> = sqlx::query_scalar(
        "select i.name from lineage.event_dataset i \
         join lineage.event_dataset o on o.event_id = i.event_id and o.direction = 'output' \
         where o.name = $1 and i.direction = 'input'",
    )
    .bind("main.dst")
    .fetch_all(&pool)
    .await
    .expect("lineage inputs");
    assert_eq!(
        inputs,
        vec!["main.src".to_string()],
        "the input table is the upstream dataset"
    );
}

/// An input table the engine does not know (`columns_json` absent on ListFiles) is
/// deterministically bad -> Abandon naming the table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_input_abandons() {
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
            flight: true,
            ..EngineOpts::default()
        },
    )
    .await;
    let ctx = build_ctx(&eng.sock, &wh_str).await;

    let job = make_transform_job(
        &[tref("main", "nope")],
        &tref("main", "out"),
        "SELECT * FROM nope",
        OutputMode::Append,
    );
    let err = handle_transform(&ctx, job)
        .await
        .expect_err("unknown input must fail");
    assert!(
        matches!(err.policy, RetryPolicy::Abandon),
        "unknown input is deterministic (Abandon), got {:?}",
        err.policy
    );
    assert!(
        err.error.contains("unknown input table"),
        "error names the class, got: {}",
        err.error
    );
}

/// Two inputs that would register under the same DataFusion name shadow each other;
/// rejected up front (Abandon) — BEFORE any RPC, which the message pins: had list_files
/// run first, these unknown tables would have failed as `unknown input table` instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ambiguous_register_as_abandons() {
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
            flight: true,
            ..EngineOpts::default()
        },
    )
    .await;
    let ctx = build_ctx(&eng.sock, &wh_str).await;

    let job = make_transform_job(
        &[tref("a", "dup"), tref("b", "dup")],
        &tref("main", "out"),
        "SELECT * FROM dup",
        OutputMode::Append,
    );
    let err = handle_transform(&ctx, job)
        .await
        .expect_err("ambiguous registration must fail");
    assert!(
        matches!(err.policy, RetryPolicy::Abandon),
        "ambiguity is deterministic (Abandon), got {:?}",
        err.policy
    );
    assert!(
        err.error
            .contains("ambiguous input table name dup: two inputs would register under it"),
        "error names the ambiguity (and proves it was checked before any RPC), got: {}",
        err.error
    );
}

/// A live input with zero files registers as an empty relation with the DECLARED
/// schema, so `SELECT count(*)` runs over it and commits a single row of `0` —
/// not an unknown-input Abandon and not a scan error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_input_counts_zero() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();
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

    // Zero-file fixture via a directly-constructed Iceberg control plane.
    let icp = IcebergControlPlane::new(cp, local_sql_catalog(fx.pg_dsn(&db), &wh_str).await);
    let input = tref("main", "empty_in");
    create_empty_table(&icp, &input, &columns()).await;

    let ctx = build_ctx(&eng.sock, &wh_str).await;
    let out = tref("main", "empty_count");
    let job = make_transform_job(
        std::slice::from_ref(&input),
        &out,
        "SELECT count(*) AS n FROM empty_in",
        OutputMode::Append,
    );
    handle_transform(&ctx, job)
        .await
        .expect("empty input is an empty relation, not an error");

    let ice = IcebergCatalog::new(pool);
    let snap = ice.current_snapshot(&out).await.expect("output snapshot");
    let vals = read_i64s(&ctx.flight, &ice, &out, snap.id).await;
    assert_eq!(
        vals,
        HashSet::from([0]),
        "count(*) over the empty input committed a single 0 row"
    );
}

/// `output_mode: overwrite` replaces the output's live contents (the live set serves
/// only the new result) while the pre-overwrite snapshot's file list is unchanged
/// (time travel).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_replaces_live_set_and_time_travels() {
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

    let src = tref("main", "ow_src");
    let (schema, batches) = ipc_body(&[1, 2, 3]);
    land(
        &pool,
        &catalog,
        &src,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        seed_lineage(&src),
    )
    .await
    .expect("land");

    let ctx = build_ctx(&eng.sock, &wh_str).await;
    let out = tref("main", "ow_out");

    // First: append ids <= 2.
    handle_transform(
        &ctx,
        make_transform_job(
            std::slice::from_ref(&src),
            &out,
            "SELECT id FROM ow_src WHERE id <= 2",
            OutputMode::Append,
        ),
    )
    .await
    .expect("append transform");

    let ice = IcebergCatalog::new(pool);
    let snap1 = ice.current_snapshot(&out).await.expect("snapshot 1");
    let files1: HashSet<String> = ice
        .files_with_stats(&out, snap1.id)
        .await
        .expect("files 1")
        .iter()
        .map(|f| f.path.clone())
        .collect();
    let ids1 = read_i64s(&ctx.flight, &ice, &out, snap1.id).await;
    assert_eq!(ids1, HashSet::from([1, 2]), "first run appended {{1,2}}");

    // Second: OVERWRITE with a different predicate.
    handle_transform(
        &ctx,
        make_transform_job(
            std::slice::from_ref(&src),
            &out,
            "SELECT id FROM ow_src WHERE id >= 3",
            OutputMode::Overwrite,
        ),
    )
    .await
    .expect("overwrite transform");

    let snap2 = ice.current_snapshot(&out).await.expect("snapshot 2");
    assert_ne!(snap2.id, snap1.id, "overwrite allocated a new snapshot");
    let ids2 = read_i64s(&ctx.flight, &ice, &out, snap2.id).await;
    assert_eq!(
        ids2,
        HashSet::from([3]),
        "the live set serves ONLY the overwrite result"
    );

    // Time travel: the pre-overwrite snapshot's file list is unchanged.
    let files_then: HashSet<String> = ice
        .files_with_stats(&out, snap1.id)
        .await
        .expect("files at prior snapshot")
        .iter()
        .map(|f| f.path.clone())
        .collect();
    assert_eq!(
        files_then, files1,
        "the prior snapshot still time-travels to the original files"
    );
}
