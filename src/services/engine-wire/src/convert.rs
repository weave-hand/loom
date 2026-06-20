//! core <-> pb translation. Pure, no I/O. Payload crosses as a JSON string;
//! JobId as a Uuid string; run_at as unix microseconds; RetryPolicy as a oneof.
use crate::pb;
use control_plane_core::{Job, JobId, RetryPolicy};

#[derive(Debug)]
pub struct ConvertError(pub String);

pub fn job_to_pb(j: &Job) -> pb::Job {
    pb::Job {
        id: j.id.0.to_string(),
        kind: j.kind.clone(),
        payload: j.payload.to_string(),
        attempts: j.attempts,
        run_at: (j.run_at.unix_timestamp_nanos() / 1_000) as i64, // micros
    }
}

pub fn job_from_pb(p: pb::Job) -> Result<Job, ConvertError> {
    Ok(Job {
        id: JobId(
            p.id.parse()
                .map_err(|e| ConvertError(format!("job id: {e}")))?,
        ),
        kind: p.kind,
        payload: serde_json::from_str(&p.payload)
            .map_err(|e| ConvertError(format!("payload: {e}")))?,
        attempts: p.attempts,
        run_at: time::OffsetDateTime::from_unix_timestamp_nanos((p.run_at as i128) * 1_000)
            .map_err(|e| ConvertError(format!("run_at: {e}")))?,
    })
}

pub fn retry_policy_to_pb(r: &RetryPolicy) -> pb::RetryPolicy {
    use pb::retry_policy::Kind;
    pb::RetryPolicy {
        kind: Some(match r {
            RetryPolicy::Retry { delay } => Kind::RetryDelayMs(delay.as_millis() as i64),
            RetryPolicy::Abandon => Kind::Abandon(pb::Abandon {}),
        }),
    }
}

pub fn retry_policy_from_pb(p: pb::RetryPolicy) -> Result<RetryPolicy, ConvertError> {
    use pb::retry_policy::Kind;
    match p.kind {
        Some(Kind::RetryDelayMs(ms)) => Ok(RetryPolicy::Retry {
            delay: std::time::Duration::from_millis(ms.max(0) as u64),
        }),
        Some(Kind::Abandon(_)) => Ok(RetryPolicy::Abandon),
        None => Err(ConvertError("retry policy: empty oneof".into())),
    }
}
