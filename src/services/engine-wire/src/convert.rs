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

use control_plane_core::{DatasetRef, EventType, LineageEvent, RunId};

/// Serde-native mirror of `DatasetRef` (`{namespace, name}`).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DatasetRefWire {
    pub namespace: String,
    pub name: String,
}

/// Serde-native mirror of `LineageEvent` for the engine write RPCs. The two fields
/// `time`/`uuid` cannot serialize without crate-feature changes (see the plan's
/// Global Constraints) are carried as primitives: `run_id` as the Uuid string,
/// `event_time_micros` as unix microseconds.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LineageWire {
    pub run_id: String,
    pub event_type: String,
    pub event_time_micros: i64,
    pub inputs: Vec<DatasetRefWire>,
    pub outputs: Vec<DatasetRefWire>,
    pub payload: serde_json::Value,
}

fn event_type_str(t: EventType) -> &'static str {
    match t {
        EventType::Start => "Start",
        EventType::Running => "Running",
        EventType::Complete => "Complete",
        EventType::Abort => "Abort",
        EventType::Fail => "Fail",
    }
}

fn event_type_from(s: &str) -> Result<EventType, String> {
    match s {
        "Start" => Ok(EventType::Start),
        "Running" => Ok(EventType::Running),
        "Complete" => Ok(EventType::Complete),
        "Abort" => Ok(EventType::Abort),
        "Fail" => Ok(EventType::Fail),
        other => Err(format!("unknown EventType `{other}`")),
    }
}

impl From<&DatasetRef> for DatasetRefWire {
    fn from(d: &DatasetRef) -> Self {
        Self {
            namespace: d.namespace.clone(),
            name: d.name.clone(),
        }
    }
}

impl From<DatasetRefWire> for DatasetRef {
    fn from(d: DatasetRefWire) -> Self {
        Self {
            namespace: d.namespace,
            name: d.name,
        }
    }
}

impl From<&LineageEvent> for LineageWire {
    fn from(e: &LineageEvent) -> Self {
        // i128 nanos → i64 micros. Lineage timestamps are well within i64-micros range.
        let micros = (e.event_time.unix_timestamp_nanos() / 1_000) as i64;
        Self {
            run_id: e.run_id.0.to_string(),
            event_type: event_type_str(e.event_type).to_string(),
            event_time_micros: micros,
            inputs: e.inputs.iter().map(DatasetRefWire::from).collect(),
            outputs: e.outputs.iter().map(DatasetRefWire::from).collect(),
            payload: e.payload.clone(),
        }
    }
}

impl TryFrom<LineageWire> for LineageEvent {
    type Error = String;

    fn try_from(w: LineageWire) -> Result<Self, Self::Error> {
        let run_id = RunId(uuid::Uuid::parse_str(&w.run_id).map_err(|e| e.to_string())?);
        let event_type = event_type_from(&w.event_type)?;
        let event_time = time::OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(w.event_time_micros) * 1_000,
        )
        .map_err(|e| e.to_string())?;
        Ok(LineageEvent {
            run_id,
            event_type,
            event_time,
            inputs: w.inputs.into_iter().map(DatasetRef::from).collect(),
            outputs: w.outputs.into_iter().map(DatasetRef::from).collect(),
            payload: w.payload,
        })
    }
}
