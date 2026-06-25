//! engine binary: builds the EngineControlService from the environment and serves
//! it over a unix-domain socket.

use std::collections::HashMap;

use arrow_flight::flight_service_server::FlightServiceServer;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use engine::flight::FlightDataService;
use engine::query::EngineQueryService;
use engine::service::EngineControlService;
use engine_wire::pb::engine_control_server::EngineControlServer;
use engine_wire::pb::engine_query_server::EngineQueryServer;
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

    // Build two SqlCatalog instances from the same props — SqlCatalog is not Clone.
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)
        .load("loom", props.clone())
        .await?;
    let flight_catalog = SqlCatalogBuilder::default()
        .with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)
        .load("loom", props)
        .await?;

    let socket_path = std::env::var("LOOM_ENGINE_SOCKET").expect("LOOM_ENGINE_SOCKET must be set");

    // Remove stale socket file if present.
    let _ = std::fs::remove_file(&socket_path);

    let listener = UnixListener::bind(&socket_path)?;
    let incoming = UnixListenerStream::new(listener);

    let control = EngineControlService {
        cp,
        catalog,
        pool: pool.clone(),
    };
    let query = EngineQueryService {
        catalog: IcebergCatalog::new(pool.clone()),
        serving_store: service_runtime::build_serving_object_store(&cfg.object_store)?,
    };
    let flight = FlightDataService {
        catalog: flight_catalog,
        serving_catalog: IcebergCatalog::new(pool.clone()),
        serving_store: service_runtime::build_serving_object_store(&cfg.object_store)?,
        pool,
    };

    Server::builder()
        .add_service(EngineControlServer::new(control))
        .add_service(EngineQueryServer::new(query))
        .add_service(FlightServiceServer::new(flight))
        .serve_with_incoming_shutdown(incoming, async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;

    Ok(())
}
