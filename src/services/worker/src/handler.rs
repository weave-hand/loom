//! The worker's job handlers: parse a flush_table / gc_table / build_vector_index
//! job and run it over the wire.
use control_plane_core::{BuildVectorIndexJob, FlushJob, GcJob, Job, JobFailure, RetryPolicy};
use engine_wire::client::GrpcQueueClient;
use loom_config::WorkerTuning;

pub async fn handle_flush(
    flush: GrpcQueueClient,
    tuning: WorkerTuning,
    job: Job,
) -> std::result::Result<(), JobFailure> {
    let FlushJob { schema, name } =
        serde_json::from_value(job.payload).map_err(|e| JobFailure {
            error: format!("bad flush payload: {e}"),
            policy: RetryPolicy::Abandon,
        })?;
    flush
        .flush_table(schema, name)
        .await
        .map_err(|e| JobFailure {
            error: e.to_string(),
            policy: RetryPolicy::Retry {
                delay: tuning.backoff(job.attempts),
            },
        })?;
    Ok(())
}

pub async fn handle_gc(
    engine: GrpcQueueClient,
    tuning: WorkerTuning,
    job: Job,
) -> std::result::Result<(), JobFailure> {
    let GcJob { schema, name } = serde_json::from_value(job.payload).map_err(|e| JobFailure {
        error: format!("bad gc payload: {e}"),
        policy: RetryPolicy::Abandon,
    })?;
    engine
        .gc_table(schema, name)
        .await
        .map_err(|e| JobFailure {
            error: e.to_string(),
            policy: RetryPolicy::Retry {
                delay: tuning.backoff(job.attempts),
            },
        })?;
    Ok(())
}

pub async fn handle_build_vector_index(
    client: GrpcQueueClient,
    tuning: WorkerTuning,
    job: Job,
) -> std::result::Result<(), JobFailure> {
    let BuildVectorIndexJob {
        schema,
        name,
        column,
        index_kind,
        nlist,
    } = serde_json::from_value(job.payload).map_err(|e| JobFailure {
        error: format!("bad build_vector_index payload: {e}"),
        policy: RetryPolicy::Abandon,
    })?;
    client
        .build_vector_index(schema, name, column, index_kind, nlist)
        .await
        .map_err(|e| JobFailure {
            error: e.to_string(),
            policy: RetryPolicy::Retry {
                delay: tuning.backoff(job.attempts),
            },
        })?;
    Ok(())
}
