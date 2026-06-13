//! ingest binary: build the landing AppState from the environment via service_runtime
//! and serve the HTTP API.

use std::sync::Arc;

use ingest::http::{AppState, router};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;
    let cp = Arc::new(service_runtime::control_plane(pool, cfg.lock_timeout));
    let store = Arc::new(service_runtime::local_store(&cfg.data_path)?);
    let app = router(AppState { cp, store });
    service_runtime::serve(cfg.bind_addr, app).await?;
    Ok(())
}
