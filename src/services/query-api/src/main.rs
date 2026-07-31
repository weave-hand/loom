//! query-api binary: shared bootstrap, bind the HTTP listener, serve via `query_api::serve`.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    service_runtime::init_tracing();
    let env = service_runtime::env_map();

    // Registered immediately after the env snapshot, before `bootstrap` (config
    // → migrate-and-exit gate → pool → control plane → auth) does any slow
    // work — a SIGTERM arriving during that window is caught here instead of
    // hitting the kernel default and killing the process outright. SIGTERM is
    // what container runtimes send; SIGINT covers an interactive `^C`.
    // `run_bounded` (below) later stops a wedged request from holding the
    // process past the grace period.
    let shutdown = service_runtime::Shutdown::install(service_runtime::shutdown_timeout(&env)?);

    // `ctx` owns the embedded-PG handle (None here — query-api deploys
    // external), so the keep-alive is structural for the process lifetime.
    let ctx = match service_runtime::bootstrap(&env).await? {
        service_runtime::Boot::Migrated => return Ok(()),
        service_runtime::Boot::Ready(ctx) => ctx,
    };

    let engine_socket = service_runtime::req_var(&env, "LOOM_ENGINE_SOCKET")?;

    let listener = tokio::net::TcpListener::bind(ctx.cfg.bind_addr).await?;
    service_runtime::run_bounded(
        &shutdown,
        Box::pin(query_api::serve(
            &ctx.cfg,
            ctx.pg.clone(),
            ctx.auth.clone(),
            engine_socket,
            listener,
            shutdown.signalled(),
        )),
    )
    .await
}
