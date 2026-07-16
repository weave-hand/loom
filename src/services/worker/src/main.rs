//! Zero-pool flush/GC/compact worker binary.
//!
//! Reads `LOOM_ENGINE_SOCKET` (required), `LOOM_WORKER_ID` (default: random uuid),
//! `LOOM_LOCK_TIMEOUT_MS` (default: 5000), and `LOOM_WAREHOUSE_URI` (required —
//! this binary is postgres-free, so it has no `Config`/data_path to fall back on).
//! Resolves those into a [`worker::runtime::WorkerRuntime`] and hands off to
//! `run_worker`, which connects to the engine over a UDS and drains `flush_table`,
//! `gc_table`, `compact_table`, `sweep_orphans`, `transform`, `typed-transform`,
//! `stream_consolidate`, `stream_mv`, and `build_vector_index` jobs. No Postgres in
//! the dep closure — the engine owns PG.
//!
//! The composition itself lives in the library so this binary and the standalone
//! composite (`//src/services/standalone`) cannot drift.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // One env snapshot drives both the bootstrap reads and the composed config.
    let env = loom_config::env_map();
    let socket = env
        .get("LOOM_ENGINE_SOCKET")
        .ok_or("LOOM_ENGINE_SOCKET must be set")?
        .clone();
    let worker_id = env
        .get("LOOM_WORKER_ID")
        .cloned()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    // Strict parse (matches `service_runtime::Config`): a malformed value fails startup
    // rather than silently falling back — the same no-lossy-`.ok()` rule as the tuning seam.
    let mut lease_ms: u64 = 5000;
    loom_config::overlay_opt(&env, "LOOM_LOCK_TIMEOUT_MS", &mut lease_ms)?;
    let lease = Duration::from_millis(lease_ms);

    // Compose worker config as defaults < file < env (see `JobConfig`'s `LayeredConfig`).
    let wcfg: datafusion_io::JobConfig = loom_config::load(&env)?;

    let store_cfg = store_config::ObjectStoreConfig::parse_from_env(&env)?;
    let write = Arc::new(store_config::build_write_store(&store_cfg)?);
    let mut threshold_bytes: i64 = 128 * 1024 * 1024;
    loom_config::overlay_opt(&env, "LOOM_COMPACT_THRESHOLD_BYTES", &mut threshold_bytes)?;

    let shutdown = CancellationToken::new();
    let sig = shutdown.clone();
    tokio::spawn(async move {
        drop(tokio::signal::ctrl_c().await);
        sig.cancel();
    });

    worker::runtime::run_worker(
        worker::runtime::WorkerRuntime {
            socket,
            worker_id,
            lease,
            write,
            jobs: wcfg,
            compact_threshold_bytes: threshold_bytes,
        },
        shutdown,
    )
    .await?;
    Ok(())
}
