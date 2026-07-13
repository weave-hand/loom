//! The worker's job handlers: parse a flush_table / gc_table / build_vector_index
//! / sweep_orphans job and run it over the wire. All of them are one shape —
//! deserialize the typed payload (parse error => Abandon), run one RPC (RPC error
//! => Retry with backoff) — captured by `run_wire_job`.
use std::future::Future;

use control_plane_core::{BuildVectorIndexJob, FlushJob, GcJob, Job, JobFailure, OrphanSweepJob};
use engine_wire::client::GrpcQueueClient;
use loom_config::WorkerTuning;

/// Run a single-RPC wire job: parse `job.payload` as `P`, then call `rpc`.
/// A parse failure is terminal (`Abandon`); an RPC failure is retried with the
/// tuning's backoff. `what` names the job kind for the parse-error message.
pub async fn run_wire_job<P, F, Fut, E>(
    job: Job,
    tuning: WorkerTuning,
    what: &str,
    rpc: F,
) -> std::result::Result<(), JobFailure>
where
    P: serde::de::DeserializeOwned,
    F: FnOnce(P) -> Fut,
    Fut: Future<Output = std::result::Result<(), E>>,
    E: std::fmt::Display,
{
    let payload: P = serde_json::from_value(job.payload)
        .map_err(|e| JobFailure::abandon(format!("bad {what} payload: {e}")))?;
    rpc(payload)
        .await
        .map_err(|e| JobFailure::retry(tuning.backoff(job.attempts), e.to_string()))?;
    Ok(())
}

pub async fn handle_flush(
    flush: GrpcQueueClient,
    tuning: WorkerTuning,
    job: Job,
) -> std::result::Result<(), JobFailure> {
    run_wire_job(job, tuning, "flush", |FlushJob { schema, name }| async move {
        flush.flush_table(schema, name).await.map(|_| ())
    })
    .await
}

pub async fn handle_gc(
    engine: GrpcQueueClient,
    tuning: WorkerTuning,
    job: Job,
) -> std::result::Result<(), JobFailure> {
    run_wire_job(job, tuning, "gc", |GcJob { schema, name }| async move {
        engine.gc_table(schema, name).await.map(|_| ())
    })
    .await
}

pub async fn handle_sweep_orphans(
    engine: GrpcQueueClient,
    tuning: WorkerTuning,
    job: Job,
) -> std::result::Result<(), JobFailure> {
    run_wire_job(
        job,
        tuning,
        "sweep_orphans",
        |OrphanSweepJob {}| async move { engine.sweep_orphans().await.map(|_| ()) },
    )
    .await
}

pub async fn handle_build_vector_index(
    client: GrpcQueueClient,
    tuning: WorkerTuning,
    job: Job,
) -> std::result::Result<(), JobFailure> {
    run_wire_job(
        job,
        tuning,
        "build_vector_index",
        |BuildVectorIndexJob {
             schema,
             name,
             index_name,
         }| async move {
            client
                .build_vector_index(schema, name, index_name)
                .await
                .map(|_| ())
        },
    )
    .await
}
