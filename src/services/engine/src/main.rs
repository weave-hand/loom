//! engine binary: bind the UDS from the environment and serve via `engine::run`.
use tokio::net::UnixListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg = service_runtime::Config::from_env()?;
    if service_runtime::migrate_requested() {
        service_runtime::run_migrations(&cfg.db).await?;
        return Ok(());
    }
    let (pool, _pg) = service_runtime::build_pool_managed(&cfg).await?;

    let env = service_runtime::env_map();
    let socket_path = service_runtime::req_var(&env, "LOOM_ENGINE_SOCKET")?;
    drop(std::fs::remove_file(&socket_path)); // remove stale socket (missing is fine)
    let listener = UnixListener::bind(&socket_path)?;
    let tuning = engine::EngineTuning::from_map(&env)?;

    let (ready_tx, _ready_rx) = tokio::sync::oneshot::channel();
    engine::run(listener, &cfg, pool, tuning, ready_tx, async {
        drop(tokio::signal::ctrl_c().await);
    })
    .await
}
