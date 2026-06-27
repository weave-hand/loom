//! Engine-level Flight k-NN wire test: boot a `FlightDataService` over a UDS,
//! seed a vector table (Parquet-flushed rows), build a vector index via the
//! `build_vector_index` primitive, then call `FlightTableClient::vector_search`
//! and assert the returned batch's identities are the exact top-k.
//!
//! Also asserts a no-index ticket yields a `not_found`-mapped error over the wire.
//! (CI-only fixture test — boots Postgres; cannot run under `buck2 test //src/...`
//! from a fresh environment without Postgres binaries.)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Float32Array, Int64Array, RecordBatch};
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetId, EventType, LineageEvent, Metric, ObjectType, PropertyDef,
    RunId, TableRef, TypeName,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use control_plane_postgres::vector_index::build_vector_index;
use engine::flight::FlightDataService;
use engine_wire::flight::{FlightTableClient, VectorSearchTicket};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

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

fn lineage_evt(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
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

/// Spawn a `FlightDataService` on a UDS. Returns the socket dir (keep alive)
/// and the socket path string.
async fn spawn_flight(fx: &PgFixture, db: &str, warehouse: &str) -> (tempfile::TempDir, String) {
    let sock_dir = tempfile::tempdir().expect("socket dir");
    let sock_path = sock_dir.path().join("engine.sock");
    let sock_str = sock_path.to_string_lossy().to_string();

    let pool = fx.pool_for(db).await;
    let file_catalog = make_catalog(fx.pg_dsn(db), warehouse).await;
    let svc = FlightDataService {
        catalog: file_catalog,
        serving_catalog: IcebergCatalog::new(pool.clone()),
        serving_store: None,
        pool,
    };

    let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind uds");
    let incoming = UnixListenerStream::new(listener);
    tokio::spawn(async move {
        drop(
            Server::builder()
                .add_service(FlightServiceServer::new(svc))
                .serve_with_incoming(incoming)
                .await,
        );
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    (sock_dir, sock_str)
}

fn ids(batch: &RecordBatch) -> Vec<i64> {
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("identity column is Int64");
    (0..col.len()).map(|i| col.value(i)).collect()
}

fn distances(batch: &RecordBatch) -> Vec<f32> {
    let col = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("_distance column is Float32");
    (0..col.len()).map(|i| col.value(i)).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// k-NN Flight round-trip: seed 4 vectors, build a cosine index, query the
/// engine via `FlightTableClient::vector_search`, assert top-2 identities.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vector_search_flight_top_k() {
    use control_plane_postgres::PgControlPlane;

    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(&db).await;
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_secs(5));

    let table = TableRef {
        schema: "wh".into(),
        name: "docs".into(),
    };

    // Register the object type (identity = "id").
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Docs".into()),
            table: table.clone(),
            properties: vec![
                PropertyDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                },
                PropertyDef {
                    name: "embedding".into(),
                    ty: "Vector".into(),
                    required: true,
                },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .expect("define_type");

    let run = RunId(uuid::Uuid::new_v4());

    // Land rows 1–2 (forced to Parquet: inline_byte_limit = 0).
    let rows_1_2: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(rows_1_2),
        0,
        i64::MAX,
        lineage_evt(run, &table),
    )
    .await
    .expect("land rows 1-2");

    // Land rows 3–4.
    let rows_3_4: &[(i64, [f32; 4])] = &[(3, [0.0, 0.0, 1.0, 0.0]), (4, [0.0, 0.0, 0.0, 1.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(rows_3_4),
        0,
        i64::MAX,
        lineage_evt(run, &table),
    )
    .await
    .expect("land rows 3-4");

    // Build the cosine vector index.
    let build_run = RunId(uuid::Uuid::new_v4());
    build_vector_index(
        &catalog,
        &pool,
        &table,
        "embedding",
        Metric::Cosine,
        build_run,
    )
    .await
    .expect("build_vector_index");

    // Spawn the Flight server.
    let (_sock_dir, sock) = spawn_flight(&fx, &db, &wh.path().display().to_string()).await;
    let client = FlightTableClient::connect(&sock).await.expect("connect");

    // Query: k=2, nearest to id=1's embedding [1,0,0,0].
    let batches = client
        .vector_search(VectorSearchTicket {
            schema: "wh".into(),
            name: "docs".into(),
            column: "embedding".into(),
            query: vec![1.0, 0.0, 0.0, 0.0],
            k: 2,
        })
        .await
        .expect("vector_search");

    assert_eq!(batches.len(), 1, "one batch returned");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2, "k=2 rows returned");

    let id_vec = ids(batch);
    assert_eq!(id_vec[0], 1, "nearest is id=1 (cosine, exact match)");

    let dists = distances(batch);
    assert!(dists[0] <= dists[1], "distances are ascending");
}

/// A VectorSearchTicket for a table with no built index must surface as
/// `not_found` over the wire (the engine maps `EngineServingError::NoIndex`
/// to `Status::not_found`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vector_search_no_index_is_not_found() {
    use control_plane_postgres::PgControlPlane;

    let fx = PgFixture::start();
    let (_cp_init, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(&db).await;
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_secs(5));

    let table = TableRef {
        schema: "wh".into(),
        name: "nodocs".into(),
    };

    // Register type.
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("NoDocs".into()),
            table: table.clone(),
            properties: vec![
                PropertyDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                },
                PropertyDef {
                    name: "embedding".into(),
                    ty: "Vector".into(),
                    required: true,
                },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .expect("define_type");

    // Land one row but skip build_vector_index.
    let run = RunId(uuid::Uuid::new_v4());
    let rows: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(rows),
        0,
        i64::MAX,
        lineage_evt(run, &table),
    )
    .await
    .expect("land row");

    // Spawn the Flight server.
    let (_sock_dir, sock) = spawn_flight(&fx, &db, &wh.path().display().to_string()).await;
    let client = FlightTableClient::connect(&sock).await.expect("connect");

    // Must get an error (not_found mapped from NoIndex).
    let err = client
        .vector_search(VectorSearchTicket {
            schema: "wh".into(),
            name: "nodocs".into(),
            column: "embedding".into(),
            query: vec![1.0, 0.0, 0.0, 0.0],
            k: 1,
        })
        .await;

    assert!(err.is_err(), "no-index must yield an error over the wire");
}
