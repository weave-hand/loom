//! query-api binary: build the read AppState from env config via service_runtime —
//! a Postgres control plane + an embedded DuckDB serving engine attached to the same
//! DuckLake catalog — and serve the HTTP API.

use std::sync::Arc;

use control_plane_core::ControlPlane;
use query_api::http::{AppState, router};
use query_api::serving::{EmbeddedDuckDb, EmbeddedDuckDbWriter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;
    let cp: Arc<dyn ControlPlane> =
        Arc::new(service_runtime::control_plane(pool, cfg.lock_timeout));
    let serving = Arc::new(EmbeddedDuckDb::attach(&cfg.db.ducklake_libpq(), &cfg.data_path).await?);
    let action_engine =
        Arc::new(EmbeddedDuckDbWriter::attach(&cfg.db.ducklake_libpq(), &cfg.data_path).await?);
    let app = router(AppState {
        cp,
        serving,
        action_engine,
    });
    service_runtime::serve(cfg.bind_addr, app).await?;
    Ok(())
}
