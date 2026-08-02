//! engine binary: shared bootstrap, bind the UDS from the env snapshot, serve
//! via `engine::run`.
use tokio::net::UnixListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Without a subscriber every tracing event this process emits — including the
    // drain-timeout ERROR below — is silently discarded.
    service_runtime::init_tracing();
    let env = service_runtime::env_map();

    // Registered immediately after the env snapshot, before `bootstrap` and the
    // `UnixListener::bind` below do any slow/blocking work — a SIGTERM arriving
    // during that window is caught here instead of hitting the kernel default
    // and killing the process outright. SIGTERM is what container runtimes
    // send; SIGINT covers an interactive `^C`. `run_bounded` (below) later
    // stops a wedged RPC from holding the process past the termination grace
    // period.
    let shutdown = service_runtime::Shutdown::install(service_runtime::shutdown_timeout(&env)?);

    let ctx = match service_runtime::bootstrap(&env).await? {
        service_runtime::Boot::Migrated => return Ok(()),
        service_runtime::Boot::Ready(ctx) => ctx,
    };

    let socket_path = service_runtime::req_var(&env, "LOOM_ENGINE_SOCKET")?;
    drop(std::fs::remove_file(&socket_path)); // remove stale socket (missing is fine)
    let listener = UnixListener::bind(&socket_path)?;
    let tuning = engine::EngineTuning::from_map(&env)?;

    let (ready_tx, _ready_rx) = tokio::sync::oneshot::channel();
    service_runtime::run_bounded(
        &shutdown,
        Box::pin(engine::run(
            listener,
            &ctx.cfg,
            ctx.pool.clone(),
            tuning,
            ready_tx,
            shutdown.signalled(),
        )),
    )
    .await
}
