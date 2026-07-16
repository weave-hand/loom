//! The worker's process composition: connect to an engine over a UDS, build the
//! job contexts, and run the dispatch loop. Shared by the `worker-bin` binary and
//! the standalone composite so the two cannot drift — the composition is the
//! contract ("which kinds does a loom worker drain?"), not a per-binary detail.

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
use store_config::WriteStore;
use tokio_util::sync::CancellationToken;

use crate::compact::{CompactCtx, handle_compact};
use crate::consolidate::handle_stream_consolidate;
use crate::stream_mv::{StreamMvCtx, handle_stream_mv};
use crate::transform::{TransformCtx, handle_transform, handle_typed_transform};

/// Everything the worker loop needs, independent of how the host process
/// obtained it. `worker-bin` reads these from the environment; the standalone
/// composite takes them from its already-resolved `Config`/`StandaloneTuning`.
pub struct WorkerRuntime {
    /// Path of the engine's unix-domain socket to dial.
    pub socket: String,
    /// Queue-lease identity. Must be unique per running worker.
    pub worker_id: String,
    /// Queue lock lease (`LOOM_LOCK_TIMEOUT_MS` in both hosts).
    pub lease: Duration,
    pub write: Arc<WriteStore>,
    pub jobs: datafusion_io::JobConfig,
    pub compact_threshold_bytes: i64,
}

/// Every job kind a loom worker drains. One list, so a kind added here reaches
/// both the binary and the composite.
fn job_kinds() -> Vec<String> {
    vec![
        FLUSH_JOB_KIND.to_string(),
        GC_JOB_KIND.to_string(),
        COMPACT_JOB_KIND.to_string(),
        BUILD_VECTOR_INDEX_JOB_KIND.to_string(),
        TRANSFORM_JOB_KIND.to_string(),
        TYPED_TRANSFORM_JOB_KIND.to_string(),
        STREAM_CONSOLIDATE_JOB_KIND.to_string(),
        STREAM_MV_JOB_KIND.to_string(),
        ORPHAN_SWEEP_JOB_KIND.to_string(),
    ]
}

/// Connect to the engine, build the contexts, and run the dispatch loop until
/// `shutdown` is cancelled.
pub async fn run_worker(
    rt: WorkerRuntime,
    shutdown: CancellationToken,
) -> control_plane_core::Result<()> {
    let control = GrpcQueueClient::connect(&rt.socket).await?;
    let flight = FlightTableClient::connect(&rt.socket).await?;
    let sql = FlightSqlClient::connect(&rt.socket).await?;

    let worker_tuning = rt.jobs.worker;
    let cctx = CompactCtx {
        control: control.clone(),
        flight: flight.clone(),
        write: rt.write.clone(),
        threshold_bytes: rt.compact_threshold_bytes,
        write_cfg: rt.jobs.write.clone(),
        worker_tuning,
    };
    let tctx = TransformCtx {
        control: control.clone(),
        sql,
        write: rt.write,
        write_cfg: rt.jobs.write.clone(),
        worker_tuning,
    };
    let mctx = StreamMvCtx {
        control: control.clone(),
        table: flight,
        worker_tuning,
    };
    let flush = control.clone();
    let worker = Worker::new(control, rt.worker_id, rt.lease)
        .with_poll_interval(worker_tuning.poll_interval());

    worker
        .run(&job_kinds(), shutdown, move |job| {
            let flush = flush.clone();
            let cctx = cctx.clone();
            let tctx = tctx.clone();
            let mctx = mctx.clone();
            async move {
                match job.kind.as_str() {
                    k if k == FLUSH_JOB_KIND => {
                        crate::handler::handle_flush(flush, worker_tuning, job).await
                    }
                    k if k == GC_JOB_KIND => {
                        crate::handler::handle_gc(flush, worker_tuning, job).await
                    }
                    k if k == ORPHAN_SWEEP_JOB_KIND => {
                        crate::handler::handle_sweep_orphans(flush, worker_tuning, job).await
                    }
                    k if k == COMPACT_JOB_KIND => handle_compact(&cctx, job).await,
                    k if k == TRANSFORM_JOB_KIND => handle_transform(&tctx, job).await,
                    k if k == TYPED_TRANSFORM_JOB_KIND => handle_typed_transform(&tctx, job).await,
                    k if k == BUILD_VECTOR_INDEX_JOB_KIND => {
                        crate::handler::handle_build_vector_index(flush, worker_tuning, job).await
                    }
                    k if k == STREAM_CONSOLIDATE_JOB_KIND => {
                        handle_stream_consolidate(flush, worker_tuning, job).await
                    }
                    k if k == STREAM_MV_JOB_KIND => handle_stream_mv(&mctx, job).await,
                    other => Err(JobFailure::abandon(format!("unknown job kind: {other}"))),
                }
            }
        })
        .await
}
