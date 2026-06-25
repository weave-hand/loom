//! The worker's job handlers: parse a flush_table / gc_table job and run it over
//! the wire.
use control_plane_core::{FlushJob, GcJob, Job, JobFailure, RetryPolicy};
use engine_wire::client::GrpcQueueClient;
use std::time::Duration;

pub async fn handle_flush(flush: GrpcQueueClient, job: Job) -> std::result::Result<(), JobFailure> {
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
                delay: backoff(job.attempts),
            },
        })?;
    Ok(())
}

pub async fn handle_gc(engine: GrpcQueueClient, job: Job) -> std::result::Result<(), JobFailure> {
    let GcJob { schema, name } = serde_json::from_value(job.payload).map_err(|e| JobFailure {
        error: format!("bad gc payload: {e}"),
        policy: RetryPolicy::Abandon,
    })?;
    engine.gc_table(schema, name).await.map_err(|e| JobFailure {
        error: e.to_string(),
        policy: RetryPolicy::Retry {
            delay: backoff(job.attempts),
        },
    })?;
    Ok(())
}

fn backoff(attempts: i32) -> Duration {
    // simple capped exponential: 1s, 2s, 4s, … max 60s
    let secs = 1u64
        .checked_shl(attempts.clamp(0, 6) as u32)
        .unwrap_or(64)
        .min(60);
    Duration::from_secs(secs)
}
