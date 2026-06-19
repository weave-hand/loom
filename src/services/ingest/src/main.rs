//! ingest binary: build the landing AppState from the environment via service_runtime
//! and serve the HTTP API.

use std::sync::Arc;

use ingest::http::{AppState, router};
use ingest::landing::{DuckLakeMaterializer, LandingMaterializer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;
    let cp = Arc::new(service_runtime::control_plane(pool, cfg.lock_timeout));
    let store = Arc::new(service_runtime::local_store(&cfg.data_path)?);
    let materializer: Arc<dyn LandingMaterializer> = Arc::new(DuckLakeMaterializer { cp, store });
    let app = router(AppState { materializer });
    service_runtime::serve(cfg.bind_addr, app).await?;
    Ok(())
}
