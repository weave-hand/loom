//! Zero-pool flush/GC/compact worker binary.
//!
//! Reads `LOOM_ENGINE_SOCKET` (required), `LOOM_WORKER_ID` (default: random uuid),
//! `LOOM_LOCK_TIMEOUT_MS` (default: 5000), and `LOOM_WAREHOUSE_URI` (required for
//! compaction). Connects to the engine over a UDS and runs the generic
//! `control_plane_worker::Worker<GrpcQueueClient>` loop, draining `flush_table`,
//! `gc_table`, `compact_table`, `sweep_orphans`, `transform`, `typed-transform`,
//! `stream_consolidate`, and `stream_mv` jobs (dispatched by kind). No Postgres in
//! the dep closure — the engine owns PG.

use std::sync::Arc;
use std::time::Duration;

use control_plane_core::{
    BUILD_VECTOR_INDEX_JOB_KIND, COMPACT_JOB_KIND, FLUSH_JOB_KIND, GC_JOB_KIND, JobFailure,
    ORPHAN_SWEEP_JOB_KIND, STREAM_CONSOLIDATE_JOB_KIND, STREAM_MV_JOB_KIND, TRANSFORM_JOB_KIND,
    TYPED_TRANSFORM_JOB_KIND,
};
use control_plane_worker::Worker;
use engine_wire::client::GrpcQueueClient;
use engine_wire::flight::{FlightSqlClient, FlightTableClient};
use tokio_util::sync::CancellationToken;
use worker::compact::{CompactCtx, handle_compact};
use worker::consolidate::handle_stream_consolidate;
use worker::stream_mv::{StreamMvCtx, handle_stream_mv};
use worker::transform::{TransformCtx, handle_transform, handle_typed_transform};

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
    let flight = FlightTableClient::connect(&socket).await?;
    let sql = FlightSqlClient::connect(&socket).await?;
    let mut threshold_bytes: i64 = 128 * 1024 * 1024;
    loom_config::overlay_opt(&env, "LOOM_COMPACT_THRESHOLD_BYTES", &mut threshold_bytes)?;

    let client = GrpcQueueClient::connect(&socket).await?;
    let flush = client.clone();
    let worker_tuning = wcfg.worker;
    let mv_table = flight.clone();
    let cctx = CompactCtx {
        control: client.clone(),
        flight,
        write: write.clone(),
        threshold_bytes,
        write_cfg: wcfg.write.clone(),
        worker_tuning,
    };
    let tctx = TransformCtx {
        control: client.clone(),
        sql,
        write,
        write_cfg: wcfg.write.clone(),
        worker_tuning,
    };
    let mctx = StreamMvCtx {
        control: client.clone(),
        table: mv_table,
        worker_tuning,
    };
    let worker =
        Worker::new(client, worker_id, lease).with_poll_interval(wcfg.worker.poll_interval());

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
                BUILD_VECTOR_INDEX_JOB_KIND.to_string(),
                TRANSFORM_JOB_KIND.to_string(),
                TYPED_TRANSFORM_JOB_KIND.to_string(),
                STREAM_CONSOLIDATE_JOB_KIND.to_string(),
                STREAM_MV_JOB_KIND.to_string(),
                ORPHAN_SWEEP_JOB_KIND.to_string(),
            ],
            shutdown,
            move |job| {
                let flush = flush.clone();
                let cctx = cctx.clone();
                let tctx = tctx.clone();
                let mctx = mctx.clone();
                async move {
                    match job.kind.as_str() {
                        k if k == FLUSH_JOB_KIND => {
                            worker::handler::handle_flush(flush, worker_tuning, job).await
                        }
                        k if k == GC_JOB_KIND => {
                            worker::handler::handle_gc(flush, worker_tuning, job).await
                        }
                        k if k == ORPHAN_SWEEP_JOB_KIND => {
                            worker::handler::handle_sweep_orphans(flush, worker_tuning, job).await
                        }
                        k if k == COMPACT_JOB_KIND => handle_compact(&cctx, job).await,
                        k if k == TRANSFORM_JOB_KIND => handle_transform(&tctx, job).await,
                        k if k == TYPED_TRANSFORM_JOB_KIND => {
                            handle_typed_transform(&tctx, job).await
                        }
                        k if k == BUILD_VECTOR_INDEX_JOB_KIND => {
                            worker::handler::handle_build_vector_index(flush, worker_tuning, job)
                                .await
                        }
                        k if k == STREAM_CONSOLIDATE_JOB_KIND => {
                            handle_stream_consolidate(flush, worker_tuning, job).await
                        }
                        k if k == STREAM_MV_JOB_KIND => handle_stream_mv(&mctx, job).await,
                        other => Err(JobFailure::abandon(format!("unknown job kind: {other}"))),
                    }
                }
            },
        )
        .await?;
    Ok(())
}
