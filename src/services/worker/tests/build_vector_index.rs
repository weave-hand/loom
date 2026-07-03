//! Worker build-vector-index e2e: land vector rows, run `handle_build_vector_index`
//! over the wire (→ engine RPC → postgres build primitive), assert the
//! `vector_index` mirror row matches a direct primitive call.
//!
//! (CI-only fixture test — boots Postgres; cannot run under a bare
//! `buck2 test //src/...` from a fresh environment without Postgres binaries.)

use loom_test_flight::{EngineOpts, spawn_engine_uds};
use loom_test_seed::{local_sql_catalog, vec4_columns, vec4_ipc};
use std::time::Duration;

use control_plane_core::{
    BUILD_VECTOR_INDEX_JOB_KIND, BuildVectorIndexJob, Catalog, ControlPlane, DatasetId, EventType,
    IndexSpec, Job, JobId, LineageEvent, Metric, ObjectType, PropertyDef, RunId, TableRef,
    TypeName, VectorIndexDef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::vector_index::{BuiltIndex, build_vector_index, lookup_vector_index};
use engine_wire::client::GrpcQueueClient;
use worker::handler::handle_build_vector_index;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn lineage(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "build-vector-index-e2e-test" }),
    }
}

fn make_build_vector_index_job(schema: &str, name: &str, index_name: &str) -> Job {
    Job {
        id: JobId(uuid::Uuid::new_v4()),
        kind: BUILD_VECTOR_INDEX_JOB_KIND.to_string(),
        payload: serde_json::to_value(BuildVectorIndexJob {
            schema: schema.into(),
            name: name.into(),
            index_name: index_name.into(),
        })
        .expect("serialize payload"),
        attempts: 0,
        run_at: time::OffsetDateTime::now_utc(),
    }
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

/// Worker build-vector-index e2e: land vector rows, run `handle_build_vector_index`
/// over the wire, assert the produced `vector_index` mirror row matches a direct
/// `build_vector_index` primitive call (same covered_snapshot and row_count).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_builds_vector_index_over_the_wire() {
    use control_plane_postgres::PgControlPlane;

    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_millis(5000));

    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();

    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh_str).await;

    let table = TableRef {
        schema: "main".into(),
        name: "vectors".into(),
    };

    // Register object type so build_vector_index can resolve the identity column.
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Vectors".into()),
            table: table.clone(),
            properties: vec![
                PropertyDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
                PropertyDef {
                    name: "embedding".into(),
                    ty: "vector(4)".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .expect("define_type");

    // Declare two named indexes on the same property: a flat and an hnsw.
    cp.ontology()
        .define_vector_index(VectorIndexDef {
            name: "by_flat".into(),
            type_name: TypeName("Vectors".into()),
            property: "embedding".into(),
            metric: Metric::Cosine,
            spec: IndexSpec::Flat,
        })
        .await
        .expect("define by_flat");
    cp.ontology()
        .define_vector_index(VectorIndexDef {
            name: "by_hnsw".into(),
            type_name: TypeName("Vectors".into()),
            property: "embedding".into(),
            metric: Metric::Cosine,
            spec: IndexSpec::Hnsw {
                m: None,
                ef_construction: None,
            },
        })
        .await
        .expect("define by_hnsw");

    // Land rows (forced to Parquet: inline_byte_limit = 0).
    let rows: &[(i64, [f32; 4])] = &[
        (1, [1.0, 0.0, 0.0, 0.0]),
        (2, [0.0, 1.0, 0.0, 0.0]),
        (3, [0.0, 0.0, 1.0, 0.0]),
    ];
    land(
        &pool,
        &catalog,
        &table,
        &vec4_columns(),
        &vec4_ipc(rows),
        InlineLimits {
            inline_byte_limit: 0,           // always write real Parquet
            flush_byte_threshold: i64::MAX, // no auto-enqueue
        },
        lineage(RunId(uuid::Uuid::new_v4()), &table),
    )
    .await
    .expect("land");

    // Spawn engine (EngineControl + Flight on the same UDS).
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

    // Connect the GrpcQueueClient (the handler's engine-wire client).
    let client = GrpcQueueClient::connect(&eng.sock)
        .await
        .expect("connect control");

    // Run the handler over the wire.
    let tuning = loom_config::WorkerTuning::default();
    handle_build_vector_index(
        client.clone(),
        tuning,
        make_build_vector_index_job("main", "vectors", "by_flat"),
    )
    .await
    .expect("handle_build_vector_index");

    // Fetch the current snapshot to look up the mirror row.
    let ice = IcebergCatalog::new(pool.clone());
    let snap = ice
        .current_snapshot(&table)
        .await
        .expect("current_snapshot");

    // Look up the vector_index mirror row via the same table_id.
    let table_id: i64 = sqlx::query_scalar(
        "select table_id from iceberg_mirror.\"table\" \
         where table_namespace = $1 and table_name = $2 and end_snapshot is null",
    )
    .bind(&table.schema)
    .bind(&table.name)
    .fetch_one(&pool)
    .await
    .expect("fetch table_id");

    let mirror_row = lookup_vector_index(&pool, table_id, "by_flat", snap.id.0)
        .await
        .expect("lookup_vector_index")
        .expect("mirror row must exist after handler ran");

    // Verify the mirror row is coherent.
    assert_eq!(
        mirror_row.covered_snapshot, snap.id.0,
        "covered_snapshot matches current snapshot"
    );
    assert_eq!(mirror_row.row_count, 3, "all three rows indexed");
    assert!(
        !mirror_row.puffin_path.is_empty(),
        "puffin_path is non-empty"
    );
    assert_eq!(mirror_row.column, "embedding");

    // Build the `by_hnsw` index over the wire; the mirror records index_kind = "hnsw".
    handle_build_vector_index(
        client.clone(),
        tuning,
        make_build_vector_index_job("main", "vectors", "by_hnsw"),
    )
    .await
    .expect("handle_build_vector_index hnsw");

    let hnsw_row = lookup_vector_index(&pool, table_id, "by_hnsw", snap.id.0)
        .await
        .expect("lookup_vector_index hnsw")
        .expect("Some");
    assert_eq!(hnsw_row.index_kind, "hnsw");

    // Cross-check: run the direct primitive and compare covered_snapshot + row_count.
    // A second build sees the same snapshot (idempotent — same data, new puffin written).
    let direct: BuiltIndex = build_vector_index(
        &local_sql_catalog(fx.pg_dsn(&db), &wh_str).await,
        &pool,
        &table,
        "by_flat",
        RunId(uuid::Uuid::new_v4()),
    )
    .await
    .expect("direct build_vector_index");

    assert_eq!(
        direct.covered_snapshot, mirror_row.covered_snapshot,
        "direct primitive targets the same snapshot"
    );
    assert_eq!(
        direct.row_count, mirror_row.row_count,
        "same row count via both paths"
    );
}

/// Negative path: a `build_vector_index` job naming an `index_name` with NO
/// ontology declaration must fail the build (the primitive returns `NotFound`,
/// surfaced over the wire and mapped to a `JobFailure`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn build_with_unknown_index_name_fails() {
    use control_plane_postgres::PgControlPlane;

    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_millis(5000));

    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh_str).await;

    let table = TableRef {
        schema: "main".into(),
        name: "vectors".into(),
    };

    // Register the type but declare NO vector index.
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Vectors".into()),
            table: table.clone(),
            properties: vec![
                PropertyDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
                PropertyDef {
                    name: "embedding".into(),
                    ty: "vector(4)".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .expect("define_type");

    // Land a row so the mirror table exists.
    let rows: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &vec4_columns(),
        &vec4_ipc(rows),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), &table),
    )
    .await
    .expect("land");

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

    // Drive a build for an index_name that was never declared → must fail.
    let tuning = loom_config::WorkerTuning::default();
    let result = handle_build_vector_index(
        client,
        tuning,
        make_build_vector_index_job("main", "vectors", "ghost"),
    )
    .await;

    assert!(
        result.is_err(),
        "build for an undeclared index_name must fail"
    );
}
