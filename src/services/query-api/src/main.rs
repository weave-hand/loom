//! query-api binary: build the read AppState from env config via service_runtime —
//! a Postgres control plane plus a serving engine selected by LOOM_SERVING_BACKEND
//! (DuckLake-on-DuckDB by default, or the loom-native DataFusion engine over the
//! Iceberg mirror) — and serve the HTTP API.

use std::sync::Arc;

use control_plane_core::ControlPlane;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, EmbeddedDuckDb, EmbeddedDuckDbWriter, ServingEngine};
use query_api::serving_datafusion::{
    DataFusionServingEngine, ServingBackend, UnsupportedActionEngine, parse_serving_backend,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;
    let backend = parse_serving_backend(std::env::var("LOOM_SERVING_BACKEND").ok().as_deref())
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    // cp (ontology + ACL) is format-agnostic and identical for both backends.
    let cp: Arc<dyn ControlPlane> = Arc::new(service_runtime::control_plane(
        pool.clone(),
        cfg.lock_timeout,
    ));

    let (serving, action_engine): (Arc<dyn ServingEngine>, Arc<dyn ActionEngine>) = match backend {
        ServingBackend::DuckLake => (
            Arc::new(EmbeddedDuckDb::attach(&cfg.db.ducklake_libpq(), &cfg.data_path).await?),
            Arc::new(EmbeddedDuckDbWriter::attach(&cfg.db.ducklake_libpq(), &cfg.data_path).await?),
        ),
        ServingBackend::Iceberg => (
            Arc::new(DataFusionServingEngine::new(IcebergCatalog::new(pool))),
            Arc::new(UnsupportedActionEngine),
        ),
    };

    let app = router(AppState {
        cp,
        serving,
        action_engine,
    });
    service_runtime::serve(cfg.bind_addr, app).await?;
    Ok(())
}
