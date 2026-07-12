//! The scheduled-maintenance-job contract, shared by the producer (operator
//! HTTP endpoint) and the consumer (the worker/scheduler). Lives in core so a
//! zero-pool worker can read it without depending on the postgres adapter.
//! A `JobSchedule` binds a cron expression to a recurring `gc_table` or
//! `compact_table` job; other queue kinds are not (yet) schedulable.

use crate::compact_job::{COMPACT_JOB_KIND, CompactJob};
use crate::error::{ControlPlaneError, Result};
use crate::gc::{GC_JOB_KIND, GcJob};
use crate::validate_cron;

/// The job `kind`s that may be scheduled. Membership here, not in
/// [`crate::KNOWN_JOB_KINDS`], gates [`validate_job_schedule`]: a kind can be a
/// known queue job (e.g. `transform`) without being schedulable.
pub const SCHEDULABLE_JOB_KINDS: &[&str] = &[GC_JOB_KIND, COMPACT_JOB_KIND];

/// A named cron schedule for a recurring maintenance job. `kind` selects the
/// job type (one of [`SCHEDULABLE_JOB_KINDS`]); `payload` is that kind's typed
/// job body, still JSON here since schedules cross the same serde boundary as
/// the queue; `cron` is a 5-field UTC cron expression (see
/// [`crate::validate_cron`]).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct JobSchedule {
    pub name: String,
    pub kind: String,
    pub payload: serde_json::Value,
    pub cron: String,
}

/// Validate a schedule: non-empty name, valid cron expression, a schedulable
/// kind, and a payload that decodes as that kind's typed job body. Each
/// failure is a `Validation` error naming the offending field.
pub fn validate_job_schedule(s: &JobSchedule) -> Result<()> {
    if s.name.is_empty() {
        return Err(ControlPlaneError::Validation(
            "job schedule name must not be empty".into(),
        ));
    }
    validate_cron(&s.cron)?;
    if !SCHEDULABLE_JOB_KINDS.contains(&s.kind.as_str()) {
        return Err(ControlPlaneError::Validation(format!(
            "job schedule kind '{}' is not schedulable",
            s.kind
        )));
    }
    match s.kind.as_str() {
        GC_JOB_KIND => {
            serde_json::from_value::<GcJob>(s.payload.clone()).map_err(|e| {
                ControlPlaneError::Validation(format!("invalid gc_table payload: {e}"))
            })?;
        }
        COMPACT_JOB_KIND => {
            serde_json::from_value::<CompactJob>(s.payload.clone()).map_err(|e| {
                ControlPlaneError::Validation(format!("invalid compact_table payload: {e}"))
            })?;
        }
        other => {
            return Err(ControlPlaneError::Validation(format!(
                "job schedule kind '{other}' is not schedulable"
            )));
        }
    }
    Ok(())
}
