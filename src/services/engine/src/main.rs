//! engine binary: builds the EngineControlService from the environment and serves
//! it over a unix-domain socket.

use std::collections::HashMap;
use std::sync::Arc;

use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use engine::service::EngineControlService;
use engine_wire::pb::engine_control_server::EngineControlServer;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
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
        format!("file://{}", cfg.data_path.display()),
    );
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await?;

    let socket_path = std::env::var("LOOM_ENGINE_SOCKET").expect("LOOM_ENGINE_SOCKET must be set");

    // Remove stale socket file if present.
    let _ = std::fs::remove_file(&socket_path);

    let listener = UnixListener::bind(&socket_path)?;
    let incoming = UnixListenerStream::new(listener);

    let svc = EngineControlService { cp, catalog, pool };

    Server::builder()
        .add_service(EngineControlServer::new(svc))
        .serve_with_incoming_shutdown(incoming, async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;

    Ok(())
}
