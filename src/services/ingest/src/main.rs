//! ingest binary: shared bootstrap, bind the HTTP listener, serve via `ingest::serve`.
use std::sync::Arc;

use control_plane_core::ControlPlane;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    service_runtime::init_tracing();
    let env = service_runtime::env_map();

    // Registered immediately after the env snapshot, before `bootstrap` (pool
    // connect + migrate gate) does any slow work — a SIGTERM arriving during
    // that window is caught here instead of hitting the kernel default and
    // killing the process outright. SIGTERM is what container runtimes send;
    // SIGINT covers an interactive `^C`. `run_bounded` (below) later stops a
    // wedged request from holding the process past the grace period.
    let shutdown = service_runtime::Shutdown::install(service_runtime::shutdown_timeout(&env)?);

    let ctx = match service_runtime::bootstrap(&env).await? {
        service_runtime::Boot::Migrated => return Ok(()),
        service_runtime::Boot::Ready(ctx) => ctx,
    };
    let cp: Arc<dyn ControlPlane> = ctx.pg.clone();

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
