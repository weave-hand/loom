//! engine binary: shared bootstrap, bind the UDS from the env snapshot, serve
//! via `engine::run`.
use tokio::net::UnixListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let env = service_runtime::env_map();
    let ctx = match service_runtime::bootstrap(&env).await? {
        service_runtime::Boot::Migrated => return Ok(()),
        service_runtime::Boot::Ready(ctx) => ctx,
    };

    let socket_path = service_runtime::req_var(&env, "LOOM_ENGINE_SOCKET")?;
    drop(std::fs::remove_file(&socket_path)); // remove stale socket (missing is fine)
    let listener = UnixListener::bind(&socket_path)?;
    let tuning = engine::EngineTuning::from_map(&env)?;

    let (ready_tx, _ready_rx) = tokio::sync::oneshot::channel();
    engine::run(
        listener,
        &ctx.cfg,
        ctx.pool.clone(),
        tuning,
        ready_tx,
        async {
            drop(tokio::signal::ctrl_c().await);
        },
    )
    .await
}
