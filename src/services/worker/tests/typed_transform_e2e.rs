//! Typed transform e2e over the engine wire: resolve input/output ontology TYPES
//! via governance RPCs, run type-term SQL through the worker's
//! `handle_typed_transform` (ListFiles -> Flight read -> DataFusion SQL ->
//! conformance gate -> write -> CommitTransform), and assert the rows land in the
//! output type's backing table with TYPE-named lineage (`loom:type` dataset refs +
//! the byte-identical `{"sql", "input_tables", "output_table"}` payload), plus the
//! Abandon taxonomy edges (non-conforming result commits nothing, unknown type).

use loom_test_flight::{EngineGuard, EngineOpts, spawn_engine_uds};
use loom_test_seed::local_sql_catalog;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    Catalog, ColumnSpec, ControlPlane, ControlPlaneError, DatasetId, EventType, Job, JobId,
    LineageEvent, NewJob, ObjectType, OutputMode, PropertyDef, Queue, RetryPolicy, RunId,
    SnapshotId, TYPED_TRANSFORM_JOB_KIND, TableRef, TypeName, TypedTransformJob,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::{FlightSqlClient, FlightTableClient, FlightTicket};
use store_config::{ObjectStoreConfig, build_write_store};
use worker::transform::{TransformCtx, handle_typed_transform};

// ---- helpers ---------------------------------------------------------------

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
        constraints: control_plane_core::PropertyConstraints::default(),
    }
}

fn customer_columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "region".into(),
            ty: "string".into(),
            nullable: true,
        },
    ]
}

/// A schema + batch of two customers (id [1,2], region [CA,NY]), for `land` seeding.
fn customer_body() -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("CA"), Some("NY")])),
        ],
    )
    .expect("batch");
    (schema, vec![batch])
}

/// Like [`customer_columns`] but BOTH columns are REQUIRED (`nullable: false`) —
/// the shape the merge-view nullability blast radius needs (a required
/// non-identity column, as `Docs.embedding` is).
fn required_customer_columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "region".into(),
            ty: "string".into(),
            nullable: false,
        },
    ]
}

/// The arrow schema of [`required_customer_columns`] — both fields non-nullable.
fn required_customer_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ]))
}

/// A full `{id, region}` row batch — a `land` payload, and (single-row) the
/// non-CDC inline VERSION an UPDATE shadow writes.
fn required_customer_rows(rows: &[(i64, &str)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let regions: Vec<&str> = rows.iter().map(|(_, r)| *r).collect();
    RecordBatch::try_new(
        required_customer_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(regions)),
        ],
    )
    .expect("required_customer_rows")
}

fn seed_lineage(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "typed-transform-e2e-test" }),
    }
}

fn make_typed_job(inputs: &[&str], output: &str, sql: &str, output_mode: OutputMode) -> Job {
    Job {
        id: JobId(uuid::Uuid::new_v4()),
        kind: TYPED_TRANSFORM_JOB_KIND.to_string(),
        payload: serde_json::to_value(TypedTransformJob {
            inputs: inputs.iter().map(|s| (*s).to_string()).collect(),
            output: output.to_string(),
            sql: sql.to_string(),
            output_mode,
            run_id: None,
        })
        .unwrap(),
        attempts: 0,
        run_at: time::OffsetDateTime::now_utc(),
    }
}

/// Everything a typed-transform test runs against: a fresh database on the shared
/// fixture, a temp warehouse, and an engine serving control + Flight over a UDS.
/// Held as ONE struct so the warehouse `TempDir` guard outlives the test body (it is
/// dropped last — fields drop in declaration order, after the engine).
struct Harness {
    pg: PgControlPlane,
    pool: sqlx::PgPool,
    catalog: SqlCatalog,
    eng: EngineGuard,
    wh_str: String,
    _wh: tempfile::TempDir,
}

/// Boot a [`Harness`]. The engine carries BOTH services because the worker needs the
/// queue (control) and the SQL/table wire (Flight).
async fn boot(fx: &PgFixture) -> Harness {
    let (pg, db) = fx.fresh_db().await;
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

    Harness {
        pg,
        pool,
        catalog,
        eng,
        wh_str,
        _wh: wh,
    }
}

/// Seed the input type's backing table `main.customers` with two rows (real Iceberg
/// Parquet, forced cold) and define the `Customer` type over it, so `resolve()` finds
/// its table. The type is identity-less with an OPTIONAL `region` — the source seed
/// the happy-path and non-conformance tests share. (The inline-shadow test seeds a
/// deliberately different topology — identity + REQUIRED `region` — and stays local.)
async fn seed_customer_source(h: &Harness) -> TableRef {
    let customers = tref("main", "customers");
    let (schema, batches) = customer_body();
    land(
        &h.pool,
        &h.catalog,
        &customers,
        &customer_columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        seed_lineage(&customers),
        None,
    )
    .await
    .expect("land");

    h.pg.ontology()
        .define_type(ObjectType {
            name: TypeName("Customer".into()),
            properties: vec![prop("id", "Long", true), prop("region", "String", false)],
            derived: vec![],
            table: customers.clone(),
            identity: None,
            version: None,
        })
        .await
        .unwrap();
    customers
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
    let sql = FlightSqlClient::connect(sock).await.expect("connect sql");
    TransformCtx {
        control,
        sql,
        write,
        write_cfg: datafusion_io::WriteConfig::default(),
        worker_tuning: loom_config::WorkerTuning::default(),
    }
}

/// Read the single-Int64-column values of `table` at `snap` back over Flight.
async fn read_i64s(
    sock: &str,
    ice: &IcebergCatalog,
    table: &TableRef,
    snap: SnapshotId,
) -> HashSet<i64> {
    let flight = FlightTableClient::connect(sock)
        .await
        .expect("connect flight");
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

// ---- tests -----------------------------------------------------------------

/// Happy path over the REAL queue: define input + output types, enqueue a
/// `"typed-transform"` job through the fixture's Postgres queue, dequeue it via the
/// engine-wire `GrpcQueueClient` (pins the kind string end-to-end), run
/// `handle_typed_transform`, and assert the rows in the output type's backing table
/// plus the TYPE-named lineage (`loom:type` dataset refs, physical tables in the
/// byte-identical payload).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_transform_commits_with_type_named_lineage() {
    let h = boot(PgFixture::shared()).await;
    seed_customer_source(&h).await;

    // Define the OUTPUT type (its backing table main.customer_slim does NOT exist yet).
    let slim = tref("main", "customer_slim");
    h.pg.ontology()
        .define_type(ObjectType {
            name: TypeName("CustomerSlim".into()),
            properties: vec![prop("id", "Long", false)],
            derived: vec![],
            table: slim.clone(),
            identity: None,
            version: None,
        })
        .await
        .unwrap();

    // Enqueue through the fixture's Postgres queue (SQL is written in TYPE terms;
    // quoted so DataFusion does not lowercase the registered type name)...
    let sql = "SELECT \"Customer\".id AS id FROM \"Customer\" WHERE \"Customer\".id >= 2";
    h.pg.queue()
        .enqueue(NewJob {
            kind: TYPED_TRANSFORM_JOB_KIND.to_string(),
            payload: serde_json::to_value(TypedTransformJob {
                inputs: vec!["Customer".to_string()],
                output: "CustomerSlim".to_string(),
                sql: sql.to_string(),
                output_mode: OutputMode::Append,
                run_id: None,
            })
            .expect("payload"),
            run_at: None,
            priority: 0,
        })
        .await
        .expect("enqueue");

    // ...and dequeue it over the wire, like the worker binary does.
    let ctx = build_ctx(&h.eng.sock, &h.wh_str).await;
    let job = ctx
        .control
        .dequeue(&[TYPED_TRANSFORM_JOB_KIND.to_string()], "e2e-worker")
        .await
        .expect("dequeue")
        .expect("a queued typed-transform job");
    assert_eq!(
        job.kind, TYPED_TRANSFORM_JOB_KIND,
        "kind survives the queue"
    );

    handle_typed_transform(&ctx, job)
        .await
        .expect("typed transform");

    // Rows land in the OUTPUT TYPE's backing table, readable over Flight.
    let ice = IcebergCatalog::new(h.pool.clone());
    let snap = ice.current_snapshot(&slim).await.expect("output snapshot");
    let ids = read_i64s(&h.eng.sock, &ice, &slim, snap.id).await;
    assert_eq!(
        ids,
        HashSet::from([2]),
        "the typed transform's predicate filtered the rows into the backing table"
    );

    // Lineage: the transform event's payload is byte-identical to
    // `{"sql", "input_tables", "output_table"}` naming the PHYSICAL tables. The
    // `payload->>'sql'` filter excludes define_type's type-table binding edge,
    // which also names the type as an output.
    let payload: serde_json::Value = sqlx::query_scalar(
        "select e.payload from lineage.event e \
         join lineage.event_dataset d on d.event_id = e.event_id and d.direction = 'output' \
         where d.name = $1 and d.namespace = 'loom:type' and e.payload->>'sql' is not null",
    )
    .bind("CustomerSlim")
    .fetch_one(&h.pool)
    .await
    .expect("typed lineage payload");
    assert_eq!(
        payload,
        serde_json::json!({
            "sql": sql,
            "input_tables": ["main.customers"],
            "output_table": "main.customer_slim",
        }),
        "typed lineage payload is byte-identical"
    );

    // ...and the event's dataset edges are TYPE refs (`loom:type` namespace).
    let inputs: Vec<(String, String)> = sqlx::query_as(
        "select i.namespace, i.name from lineage.event_dataset i \
         join lineage.event e on e.event_id = i.event_id \
         join lineage.event_dataset o on o.event_id = i.event_id and o.direction = 'output' \
         where o.name = $1 and o.namespace = 'loom:type' and i.direction = 'input' \
           and e.payload->>'sql' is not null",
    )
    .bind("CustomerSlim")
    .fetch_all(&h.pool)
    .await
    .expect("typed lineage inputs");
    assert_eq!(
        inputs,
        vec![("loom:type".to_string(), "Customer".to_string())],
        "the input dataset is the TYPE ref, not the physical table"
    );
}

/// A result with a column the output type does not declare violates the exact-match
/// conformance contract: Abandon BEFORE anything is written — the output table has
/// no snapshot and no transform lineage event names the output type.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nonconforming_result_abandons_without_commit() {
    let h = boot(PgFixture::shared()).await;
    seed_customer_source(&h).await;

    // The output type declares ONLY `id`; the SQL below also yields `region`.
    let bad = tref("main", "customer_bad");
    h.pg.ontology()
        .define_type(ObjectType {
            name: TypeName("CustomerBad".into()),
            properties: vec![prop("id", "Long", false)],
            derived: vec![],
            table: bad.clone(),
            identity: None,
            version: None,
        })
        .await
        .unwrap();

    let ctx = build_ctx(&h.eng.sock, &h.wh_str).await;
    let job = make_typed_job(
        &["Customer"],
        "CustomerBad",
        "SELECT \"Customer\".id AS id, \"Customer\".region AS region FROM \"Customer\"",
        OutputMode::Append,
    );
    let err = handle_typed_transform(&ctx, job)
        .await
        .expect_err("non-conforming result must fail");
    assert!(
        matches!(err.policy, RetryPolicy::Abandon),
        "non-conformance is deterministic (Abandon), got {:?}",
        err.policy
    );
    assert!(
        err.error.contains("violation"),
        "error names the conformance violation, got: {}",
        err.error
    );

    // NOTHING was committed: the output table has no snapshot...
    let ice = IcebergCatalog::new(h.pool.clone());
    assert!(
        matches!(
            ice.current_snapshot(&bad).await,
            Err(ControlPlaneError::NotFound(_))
        ),
        "non-conforming transform committed no snapshot for the output table"
    );

    // ...and no transform lineage event names the output type (the `payload->>'sql'`
    // filter excludes define_type's binding edge, which legitimately exists).
    let events: i64 = sqlx::query_scalar(
        "select count(*) from lineage.event e \
         join lineage.event_dataset d on d.event_id = e.event_id and d.direction = 'output' \
         where d.name = $1 and e.payload->>'sql' is not null",
    )
    .bind("CustomerBad")
    .fetch_one(&h.pool)
    .await
    .expect("lineage count");
    assert_eq!(
        events, 0,
        "non-conforming transform emitted no lineage for the output"
    );
}

/// Blast-radius non-regression (iss-search-vector-merge-view-nullable): a typed
/// transform whose SOURCE table has a LIVE INLINE ROW (a benign UPDATE shadow — no
/// tombstone) over an IDENTITY type with a REQUIRED non-identity column must still
/// conform and commit. The source's rows come from the engine's SQL serving path,
/// i.e. `build_merge_view`; the worker infers the output columns from THAT served
/// Arrow schema (`infer_columns`, which copies `is_nullable()`), and
/// `check_conformance` rejects a nullable column for a REQUIRED property. Widening
/// the merged view's served nullability — the naive form of the Defect-B fix —
/// would abort this transform with a `NullabilityViolation`. The real fix restores
/// the mirror's nullability in the merge view's final projection (`loom_not_null`),
/// so this stays green — and the merge fold still wins (id=1 serves the shadow).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_transform_conforms_over_a_source_with_a_live_inline_row() {
    let h = boot(PgFixture::shared()).await;

    // Cold tier: two rows, every column present. (A deliberately different seed from
    // `seed_customer_source` — identity + a REQUIRED `region` — so it stays local.)
    let customers = tref("main", "customers");
    let cols = required_customer_columns();
    land(
        &h.pool,
        &h.catalog,
        &customers,
        &cols,
        required_customer_schema(),
        vec![required_customer_rows(&[(1, "CA"), (2, "NY")])],
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        seed_lineage(&customers),
        None,
    )
    .await
    .expect("land");

    // The input type has an IDENTITY — that is what routes the source through
    // `build_merge_view` at all — and a REQUIRED non-identity property.
    h.pg.ontology()
        .define_type(ObjectType {
            name: TypeName("Customer".into()),
            properties: vec![prop("id", "Long", true), prop("region", "String", true)],
            derived: vec![],
            table: customers.clone(),
            identity: Some("id".into()),
            version: None,
        })
        .await
        .unwrap();
    // The OUTPUT type also declares `region` REQUIRED: a nullable `region` in the
    // result schema is exactly what `check_conformance` refuses.
    let slim = tref("main", "customer_regions");
    h.pg.ontology()
        .define_type(ObjectType {
            name: TypeName("CustomerRegion".into()),
            properties: vec![prop("id", "Long", true), prop("region", "String", true)],
            derived: vec![],
            table: slim.clone(),
            identity: None,
            version: None,
        })
        .await
        .unwrap();

    // A LIVE INLINE UPDATE SHADOW on id=1 (a full row — no tombstone anywhere).
    let id_only = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1_i64]))],
    )
    .expect("id batch");
    let v = control_plane_postgres::iceberg_inline::current_inline_version(
        &h.pool, &customers, &cols, "id", &id_only,
    )
    .await
    .expect("current inline version");
    control_plane_postgres::iceberg_inline::write_inline_delta(
        &h.pool,
        &customers,
        &cols,
        "id",
        false, // row-version, NOT a tombstone
        &required_customer_rows(&[(1, "WA")]),
        None,
        seed_lineage(&customers),
        v,
        None,
        &[], // jobs
    )
    .await
    .expect("inline update shadow");

    // The same typed transform as the happy path, over the now-shadowed source.
    let ctx = build_ctx(&h.eng.sock, &h.wh_str).await;
    let job = make_typed_job(
        &["Customer"],
        "CustomerRegion",
        "SELECT \"Customer\".id AS id, \"Customer\".region AS region FROM \"Customer\"",
        OutputMode::Append,
    );
    handle_typed_transform(&ctx, job)
        .await
        .expect("typed transform over a source with a live inline row must conform and commit");

    // Committed — and the merge fold picked the shadow for id=1.
    let ice = IcebergCatalog::new(h.pool.clone());
    let snap = ice.current_snapshot(&slim).await.expect("output snapshot");
    let flight = FlightTableClient::connect(&h.eng.sock)
        .await
        .expect("connect flight");
    let files = ice.files_with_stats(&slim, snap.id).await.expect("files");
    let batches = flight
        .fetch(FlightTicket {
            schema: slim.schema.clone(),
            name: slim.name.clone(),
            files: files.iter().map(|f| f.path.clone()).collect(),
        })
        .await
        .expect("flight read-back");
    let mut got: Vec<(i64, String)> = Vec::new();
    for b in &batches {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 id");
        let regions = b
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 region");
        for i in 0..ids.len() {
            got.push((ids.value(i), regions.value(i).to_string()));
        }
    }
    got.sort_unstable();
    assert_eq!(
        got,
        vec![(1, "WA".to_string()), (2, "NY".to_string())],
        "the transform committed, and the inline shadow won for id=1"
    );
}

/// An input type the ontology does not know is deterministic (`gov_resolve` returns
/// NotFound over the wire) -> Abandon naming the type.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_type_abandons() {
    let fx = PgFixture::shared();
    let (_pg, db) = fx.fresh_db().await;

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

    let job = make_typed_job(
        &["Ghost"],
        "AlsoGhost",
        "SELECT * FROM \"Ghost\"",
        OutputMode::Append,
    );
    let err = handle_typed_transform(&ctx, job)
        .await
        .expect_err("unknown type must fail");
    assert!(
        matches!(err.policy, RetryPolicy::Abandon),
        "unknown type is deterministic (Abandon), got {:?}",
        err.policy
    );
    assert!(
        err.error.contains("unknown ontology type"),
        "error names the class, got: {}",
        err.error
    );
}
