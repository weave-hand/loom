//! ingest binary: shared bootstrap, bind the HTTP listener, serve via `ingest::serve`.
use std::sync::Arc;

use control_plane_core::ControlPlane;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    service_runtime::init_tracing();
    let env = service_runtime::env_map();
    let ctx = match service_runtime::bootstrap(&env).await? {
        service_runtime::Boot::Migrated => return Ok(()),
        service_runtime::Boot::Ready(ctx) => ctx,
    };
    let cp: Arc<dyn ControlPlane> = ctx.pg.clone();

    // SIGTERM (what container runtimes send) and SIGINT both drain the server. The
    // shutdown source here used to be a future that never resolves, so the
    // graceful-shutdown plumbing behind it could never fire; `run_bounded` then
    // stops a wedged request from holding the process past the grace period.
    let shutdown = service_runtime::Shutdown::install(service_runtime::shutdown_timeout(&env)?);

    let listener = tokio::net::TcpListener::bind(ctx.cfg.bind_addr).await?;
    service_runtime::run_bounded(
        &shutdown,
        Box::pin(ingest::serve(
            &ctx.cfg,
            ctx.pool.clone(),
            cp,
            ctx.auth.clone(),
            ctx.max_ttl,
            listener,
            shutdown.signalled(),
        )),
    )
    .await
}
