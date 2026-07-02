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

    let listener = tokio::net::TcpListener::bind(ctx.cfg.bind_addr).await?;
    ingest::serve(
        &ctx.cfg,
        ctx.pool.clone(),
        cp,
        ctx.auth.clone(),
        ctx.max_ttl,
        listener,
        std::future::pending(),
    )
    .await
}
