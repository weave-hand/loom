//! Zero-pool flush worker binary.
//!
//! Reads `LOOM_ENGINE_SOCKET` (required), `LOOM_WORKER_ID` (default: random uuid),
//! and `LOOM_LOCK_TIMEOUT_MS` (default: 5000). Connects to the engine over a UDS
//! and runs the generic `control_plane_worker::Worker<GrpcQueueClient>` loop,
//! draining `flush_table` jobs. No Postgres in the dep closure — the engine owns PG.

use std::time::Duration;

use control_plane_core::FLUSH_JOB_KIND;
use control_plane_worker::Worker;
use engine_wire::client::GrpcQueueClient;
use tokio_util::sync::CancellationToken;

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

    let client = GrpcQueueClient::connect(socket).await?;
    let flush = client.clone();
    let worker = Worker::new(client, worker_id, lease);

    let shutdown = CancellationToken::new();
    let sig = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        sig.cancel();
    });

    worker
        .run(&[FLUSH_JOB_KIND.to_string()], shutdown, move |job| {
            let flush = flush.clone();
            async move { worker::handler::handle_flush(flush, job).await }
        })
        .await?;
    Ok(())
}
