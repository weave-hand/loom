//! Over-the-wire e2e smoke: boot the ingest + query-api routers on real
//! ephemeral TCP ports (via `service_runtime::serve`) and drive the land→read
//! vertical with a real `reqwest` client, over BOTH storage backends.
//!
//! This complements the in-process `oneshot` suite (behavioral breadth) with a
//! smoke of the network path: `serve()` + routers + body/JSON + status codes.
//! Backend assembly is cribbed faithfully from each binary's `main.rs`.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{ControlPlane, ObjectType, Ontology, TypeName};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use e2e_support::{grant_read, ids_i64, prop, spawn_http, subject_with_role, tref};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use ingest::landing::{DuckLakeMaterializer, IcebergMaterializer};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::serving::{DuckLakeActionWriter, EmbeddedDuckDb};
use query_api::serving_datafusion::{DataFusionServingEngine, IcebergActionWriter};

const INLINE_BYTE_LIMIT: usize = 16 * 1024 * 1024;

/// The two assembled routers for one backend, plus the control plane (for
/// not-under-test seeding). The backend's tempdirs/writers are returned
/// separately as a keep-alive (see `ducklake_backend`/`iceberg_backend`).
struct WireBackend {
    ingest: axum::Router,
    query: axum::Router,
    cp: Arc<PgControlPlane>,
}

/// Arrow IPC stream for `customer(id Int64 non-null, region Utf8 nullable)`
/// rows `(1,'CA'),(2,'NY')`.
fn customer_ipc() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(StringArray::from(vec![Some("CA"), Some("NY")])),
        ],
    )
    .unwrap();
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

/// Assemble the DuckLake-backed ingest + query routers over one fresh db +
/// bootstrapped DuckLake warehouse (mirrors the `main.rs` DuckLake arms).
/// Returns the backend plus a keep-alive (the `DuckLakeWriter` owns the tempdir
/// holding the Parquet files the engine reads) the caller must hold.
async fn ducklake_backend(fx: &PgFixture) -> (WireBackend, Box<dyn Any + Send>) {
    let (cp, db) = fx.fresh_db().await;
    let cp = Arc::new(cp);
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    let ingest = ingest::http::router(ingest::http::AppState {
        materializer: Arc::new(DuckLakeMaterializer {
            cp: cp.clone() as Arc<dyn ControlPlane>,
            store: store.clone(),
        }),
    });

    let eng = EmbeddedDuckDb::attach(
        &format!(
            "dbname={} host={} user=postgres",
            db,
            fx.socket_path().display()
        ),
        writer.data_path(),
    )
    .await
    .unwrap();

    let query = query_api::http::router(query_api::http::AppState {
        cp: cp.clone() as Arc<dyn ControlPlane>,
        serving: Arc::new(eng),
        action_engine: Arc::new(DuckLakeActionWriter::new(
            cp.clone() as Arc<dyn ControlPlane>,
            store,
        )),
    });

    (WireBackend { ingest, query, cp }, Box::new(writer))
}

/// Assemble the Iceberg-backed ingest + query routers over one fresh db + a
/// `file://` warehouse (mirrors the `main.rs` Iceberg arms). `flush_byte_threshold
/// = i64::MAX` keeps the small land inline (no async flush job / worker needed);
/// the DataFusion serving engine reads the inline rows directly. Returns the
/// backend plus a keep-alive (the warehouse tempdir) the caller must hold.
async fn iceberg_backend(fx: &PgFixture) -> (WireBackend, Box<dyn Any + Send>) {
    let (cp, db) = fx.fresh_db().await;
    let cp = Arc::new(cp);
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");

    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{}", warehouse.path().display()),
    );
    let catalog = Arc::new(
        SqlCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load("loom", props)
            .await
            .expect("build SqlCatalog"),
    );

    let ingest = ingest::http::router(ingest::http::AppState {
        materializer: Arc::new(IcebergMaterializer {
            catalog: catalog.clone(),
            pool: pool.clone(),
            inline_byte_limit: INLINE_BYTE_LIMIT,
            flush_byte_threshold: i64::MAX,
        }),
    });

    let query = query_api::http::router(query_api::http::AppState {
        cp: cp.clone() as Arc<dyn ControlPlane>,
        serving: Arc::new(DataFusionServingEngine::new(IcebergCatalog::new(
            pool.clone(),
        ))),
        action_engine: Arc::new(IcebergActionWriter::new(
            catalog.clone(),
            pool,
            INLINE_BYTE_LIMIT,
            i64::MAX,
        )),
    });

    (WireBackend { ingest, query, cp }, Box::new(warehouse))
}

/// Seed the not-under-test scaffolding (ontology type + ACL grant), spawn both
/// routers, and run the happy / deny / malformed assertions over `reqwest`.
async fn run_wire_vertical(backend: WireBackend) {
    // Ontology: bind type `Customer` -> table `main.customer`. (define_type does
    // not require the table to pre-exist; the land below creates it.)
    backend
        .cp
        .define_type(ObjectType {
            name: TypeName("Customer".into()),
            properties: vec![prop("id", "Long", true), prop("region", "String", false)],
            derived: vec![],
            table: tref("main", "customer"),
            identity: None,
        })
        .await
        .unwrap();
    // ACL: `reader` may read Customer; `intruder` is a real subject with a role
    // but no grant (a clean Forbidden, matching the existing deny-test pattern).
    let (_subj, role) = subject_with_role(&backend.cp, "reader").await;
    grant_read(&backend.cp, &role, "Customer").await;
    let (_intruder, _intruder_role) = subject_with_role(&backend.cp, "intruder").await;

    let (ingest_url, _ig) = spawn_http(backend.ingest).await;
    let (query_url, _qg) = spawn_http(backend.query).await;
    let client = reqwest::Client::new();

    // Happy path 1/2 — land over the wire.
    let resp = client
        .post(format!("{ingest_url}/datasets/main/customer"))
        .body(customer_ipc())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "land status");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["snapshot_id"].is_number(), "land body: {body}");

    // Happy path 2/2 — authorized typed read returns the landed rows.
    let resp = client
        .get(format!("{query_url}/objects/Customer"))
        .header("X-Loom-Subject", "reader")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "read status");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(ids_i64(&body), vec![1i64, 2], "read body: {body}");

    // Deny — ungranted subject -> 403.
    let resp = client
        .get(format!("{query_url}/objects/Customer"))
        .header("X-Loom-Subject", "intruder")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403, "deny status");

    // Malformed 1/2 — garbage Arrow body to ingest -> 400.
    let resp = client
        .post(format!("{ingest_url}/datasets/main/customer"))
        .body(vec![0u8, 1, 2, 3])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "garbage-arrow status");

    // Malformed 2/2 — unknown type to query-api -> 404.
    let resp = client
        .get(format!("{query_url}/objects/Nope"))
        .header("X-Loom-Subject", "reader")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "unknown-type status");
}

#[tokio::test(flavor = "multi_thread")]
async fn ducklake_wire_vertical() {
    let fx = PgFixture::start();
    let (backend, _keep) = ducklake_backend(&fx).await;
    run_wire_vertical(backend).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn iceberg_wire_vertical() {
    let fx = PgFixture::start();
    let (backend, _keep) = iceberg_backend(&fx).await;
    run_wire_vertical(backend).await;
}
