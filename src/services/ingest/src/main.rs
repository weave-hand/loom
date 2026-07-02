//! ingest binary: shared setup, bind the HTTP listener, serve via `ingest::serve`.
use std::sync::Arc;

use control_plane_core::ControlPlane;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    service_runtime::init_tracing();
    let cfg = service_runtime::Config::from_env()?;
    if service_runtime::migrate_requested() {
        service_runtime::run_migrations(&cfg.db).await?;
        return Ok(());
    }
    let (pool, _pg) = service_runtime::build_pool_managed(&cfg).await?;

    let pg = Arc::new(service_runtime::control_plane(
        pool.clone(),
        cfg.lock_timeout,
    ));
    let cp: Arc<dyn ControlPlane> = pg.clone();
    let auth = service_runtime::AuthState {
        auth: pg.clone(),
        session_ttl: service_runtime::session_ttl_from_env(),
    };
    let admin_subject = std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME")
        .ok()
        .map(control_plane_core::SubjectId);
    let max_ttl = service_runtime::service_token_max_ttl_from_env();

    let listener = tokio::net::TcpListener::bind(cfg.bind_addr).await?;
    ingest::serve(
        &cfg,
        pool,
        cp,
        auth,
        admin_subject,
        max_ttl,
        listener,
        std::future::pending(),
    )
    .await
}
