//! Zero-pool flush/GC/compact worker binary.
//!
//! Reads `LOOM_ENGINE_SOCKET` (required), `LOOM_WORKER_ID` (default: random uuid),
//! `LOOM_LOCK_TIMEOUT_MS` (default: 5000), and `LOOM_WAREHOUSE_URI` (required for
//! compaction). Connects to the engine over a UDS and runs the generic
//! `control_plane_worker::Worker<GrpcQueueClient>` loop, draining `flush_table`,
//! `gc_table`, and `compact_table` jobs (dispatched by kind). No Postgres in the
//! dep closure — the engine owns PG.

use std::sync::Arc;
use std::time::Duration;

use control_plane_core::{COMPACT_JOB_KIND, FLUSH_JOB_KIND, GC_JOB_KIND, JobFailure, RetryPolicy};
use control_plane_worker::Worker;
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::FlightTableClient;
use tokio_util::sync::CancellationToken;
use worker::compact::{CompactCtx, handle_compact};

/// Thin composed config struct for the worker binary.
/// Defaults < file (`LOOM_CONFIG_FILE`) < env overlays.
#[derive(Default, serde::Deserialize)]
#[serde(default)]
struct WorkerConfig {
    worker: loom_config::WorkerTuning,
    write: datafusion_io::WriteConfig,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = std::env::var("LOOM_ENGINE_SOCKET")?;
    let worker_id =
        std::env::var("LOOM_WORKER_ID").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());
    let lease = Duration::from_millis(
        std::env::var("LOOM_LOCK_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5000),
    );

    // Build the composed config: defaults < file < env.
    let env = loom_config::env_map();
    let mut wcfg = WorkerConfig::default();
    if let Some(path) = env.get("LOOM_CONFIG_FILE") {
        let doc = std::fs::read_to_string(path)
            .map_err(|e| loom_config::invalid("LOOM_CONFIG_FILE", e))?;
        wcfg = loom_config::parse_config_doc(&doc)?;
    }
    wcfg.worker.overlay_env(&env)?;
    wcfg.write.overlay_env(&env)?;
    wcfg.worker.validate()?;
    wcfg.write.validate()?;

    let store_cfg = store_config::ObjectStoreConfig::parse_from_env(&env)?;
    let write = Arc::new(store_config::build_write_store(&store_cfg)?);
    let flight = FlightTableClient::connect(&socket).await?;
    let threshold_bytes = env
        .get("LOOM_COMPACT_THRESHOLD_BYTES")
        .and_then(|v| v.parse().ok())
        .unwrap_or(128 * 1024 * 1024_i64);

    let client = GrpcQueueClient::connect(&socket).await?;
    let flush = client.clone();
    let worker_tuning = wcfg.worker;
    let cctx = CompactCtx {
        control: client.clone(),
        flight,
        write,
        threshold_bytes,
        write_cfg: wcfg.write.clone(),
        worker_tuning,
    };
    let worker = Worker::new(client, worker_id, lease).with_poll_interval(wcfg.worker.poll_interval());

    let shutdown = CancellationToken::new();
    let sig = shutdown.clone();
    tokio::spawn(async move {
        drop(tokio::signal::ctrl_c().await);
        sig.cancel();
    });

    worker
        .run(
            &[
                FLUSH_JOB_KIND.to_string(),
                GC_JOB_KIND.to_string(),
                COMPACT_JOB_KIND.to_string(),
            ],
            shutdown,
            move |job| {
                let flush = flush.clone();
                let cctx = cctx.clone();
                async move {
                    match job.kind.as_str() {
                        k if k == FLUSH_JOB_KIND => {
                            worker::handler::handle_flush(flush, worker_tuning, job).await
                        }
                        k if k == GC_JOB_KIND => {
                            worker::handler::handle_gc(flush, worker_tuning, job).await
                        }
                        k if k == COMPACT_JOB_KIND => handle_compact(&cctx, job).await,
                        other => Err(JobFailure {
                            error: format!("unknown job kind: {other}"),
                            policy: RetryPolicy::Abandon,
                        }),
                    }
                }
            },
        )
        .await?;
    Ok(())
}
