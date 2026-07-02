//! Worker build-vector-index e2e: land vector rows, run `handle_build_vector_index`
//! over the wire (→ engine RPC → postgres build primitive), assert the
//! `vector_index` mirror row matches a direct primitive call.
//!
//! (CI-only fixture test — boots Postgres; cannot run under a bare
//! `buck2 test //src/...` from a fresh environment without Postgres binaries.)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Int64Array, RecordBatch};
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    BUILD_VECTOR_INDEX_JOB_KIND, BuildVectorIndexJob, Catalog, ColumnSpec, ControlPlane, DatasetId,
    EventType, IndexSpec, Job, JobId, LineageEvent, Metric, ObjectType, PropertyDef, RunId,
    TableRef, TypeName, VectorIndexDef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use control_plane_postgres::vector_index::{BuiltIndex, build_vector_index, lookup_vector_index};
use engine::flight::FlightDataService;
use engine::service::EngineControlService;
use engine_serving::IcebergActionWriter;
use engine_wire::client::GrpcQueueClient;
use engine_wire::pb::engine_control_server::EngineControlServer;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use tonic::transport::Server;
use worker::handler::handle_build_vector_index;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "embedding".into(),
            ty: "vector(4)".into(),
            nullable: false,
        },
    ]
}

fn ipc_body(rows: &[(i64, [f32; 4])]) -> Vec<u8> {
    let element = Arc::new(Field::new("item", DataType::Float32, false));
    let mut lb = ListBuilder::new(Float32Builder::new()).with_field(element.clone());
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    for (_, emb) in rows {
        lb.values().append_slice(emb);
        lb.append(true);
    }
    let id_array = Int64Array::from(ids);
    let emb_array = lb.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("embedding", DataType::List(element), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(emb_array)],
    )
    .expect("batch");
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &schema).expect("writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    buf
}

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

async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{warehouse}"),
    );
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

/// Spawn an engine on the given warehouse path serving BOTH EngineControl and Arrow Flight.
/// Returns (sock_dir, sock_path_string) — caller must hold `sock_dir` alive.
async fn spawn_server(fx: &PgFixture, db: &str, wh_path: &str) -> (tempfile::TempDir, String) {
    let sock_dir = tempfile::tempdir().expect("socket dir");
    let sock_path = sock_dir.path().join("engine.sock");
    let sock_str = sock_path.to_string_lossy().to_string();

    let pool = fx.pool_for(db).await;
    let cp = control_plane_postgres::PgControlPlane::new(pool.clone(), Duration::from_millis(5000));
    let control_catalog = make_catalog(fx.pg_dsn(db), wh_path).await;
    let flight_catalog = make_catalog(fx.pg_dsn(db), wh_path).await;
    let writer_catalog = make_catalog(fx.pg_dsn(db), wh_path).await;
    let writer = IcebergActionWriter::new(
        Arc::new(writer_catalog),
        pool.clone(),
        16 * 1024 * 1024,
        i64::MAX,
    );

    let svc = EngineControlService {
        cp,
        catalog: control_catalog,
        pool: pool.clone(),
        retention: Duration::from_secs(7 * 24 * 3600),
        writer,
    };
    let flight_svc = FlightDataService {
        catalog: flight_catalog,
        serving_catalog: IcebergCatalog::new(pool.clone()),
        serving_store: None,
        pool,
    };

    let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind uds");
    let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);

    tokio::spawn(async move {
        drop(
            Server::builder()
                .add_service(EngineControlServer::new(svc))
                .add_service(FlightServiceServer::new(flight_svc))
                .serve_with_incoming(incoming)
                .await,
        );
    });

    // Small pause so the server is ready to accept.
    tokio::time::sleep(Duration::from_millis(20)).await;

    (sock_dir, sock_str)
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

    let catalog = make_catalog(fx.pg_dsn(&db), &wh_str).await;

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
        &columns(),
        &ipc_body(rows),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: // inline_byte_limit = 0 -> always write real Parquet
        i64::MAX,
        },
        // flush_byte_threshold -> no auto-enqueue
        lineage(RunId(uuid::Uuid::new_v4()), &table),
    )
    .await
    .expect("land");

    // Spawn engine (EngineControl + Flight on the same UDS).
    let (_sock_dir, sock) = spawn_server(fx, &db, &wh_str).await;

    // Connect the GrpcQueueClient (the handler's engine-wire client).
    let client = GrpcQueueClient::connect(&sock)
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
        &make_catalog(fx.pg_dsn(&db), &wh_str).await,
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
    let catalog = make_catalog(fx.pg_dsn(&db), &wh_str).await;

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
        &columns(),
        &ipc_body(rows),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(RunId(uuid::Uuid::new_v4()), &table),
    )
    .await
    .expect("land");

    let (_sock_dir, sock) = spawn_server(fx, &db, &wh_str).await;
    let client = GrpcQueueClient::connect(&sock)
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
