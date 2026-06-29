//! engine binary: builds the EngineControlService from the environment and serves
//! it over a unix-domain socket.

use std::collections::HashMap;

use arrow_flight::flight_service_server::FlightServiceServer;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use engine::flight::FlightDataService;
use engine::service::EngineControlService;
use engine_wire::pb::engine_control_server::EngineControlServer;
use iceberg::CatalogBuilder;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;
    let cp = service_runtime::control_plane(pool.clone(), cfg.lock_timeout);

    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), cfg.db.pg_url());
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        cfg.object_store.warehouse_uri.clone(),
    );

    // Build three SqlCatalog instances from the same props — SqlCatalog is not Clone.
    let props_for_writer = props.clone();
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)
        .load("loom", props.clone())
        .await?;
    let flight_catalog = SqlCatalogBuilder::default()
        .with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)
        .load("loom", props)
        .await?;

    // Governed-write executor config (defaults mirror ingest's RoutingTuning).
    let inline_byte_limit: usize = std::env::var("LOOM_INLINE_BYTE_LIMIT")
        .ok()
        .map(|s| s.parse())
        .transpose()
        .map_err(|e: std::num::ParseIntError| -> Box<dyn std::error::Error> {
            format!("LOOM_INLINE_BYTE_LIMIT: {e}").into()
        })?
        .unwrap_or(16 * 1024 * 1024);
    let flush_byte_threshold: i64 = std::env::var("LOOM_FLUSH_BYTE_THRESHOLD")
        .ok()
        .map(|s| s.parse())
        .transpose()
        .map_err(|e: std::num::ParseIntError| -> Box<dyn std::error::Error> {
            format!("LOOM_FLUSH_BYTE_THRESHOLD: {e}").into()
        })?
        .unwrap_or(64 * 1024 * 1024);

    // A third SqlCatalog for the writer (SqlCatalog is not Clone; the engine already
    // builds two for control + flight).
    let writer_catalog = SqlCatalogBuilder::default()
        .with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)
        .load("loom", props_for_writer)
        .await?;
    let writer = engine_serving::IcebergActionWriter::new(
        std::sync::Arc::new(writer_catalog),
        pool.clone(),
        inline_byte_limit,
        flush_byte_threshold,
    );

    let socket_path = std::env::var("LOOM_ENGINE_SOCKET")?;

    // Remove stale socket file if present (error is expected when no socket exists).
    drop(std::fs::remove_file(&socket_path));

    let listener = UnixListener::bind(&socket_path)?;
    let incoming = UnixListenerStream::new(listener);

    let control = EngineControlService {
        cp,
        catalog,
        pool: pool.clone(),
        retention: cfg.gc_retention,
        writer,
    };
    let flight = FlightDataService {
        catalog: flight_catalog,
        serving_catalog: IcebergCatalog::new(pool.clone()),
        serving_store: service_runtime::build_serving_object_store(&cfg.object_store)?,
        pool,
    };

    Server::builder()
        .add_service(EngineControlServer::new(control))
        .add_service(FlightServiceServer::new(flight))
        .serve_with_incoming_shutdown(incoming, async {
            drop(tokio::signal::ctrl_c().await);
        })
        .await?;

    Ok(())
}
