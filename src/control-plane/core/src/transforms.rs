//! The transforms concern: named transform definitions and their runs.
//!
//! A [`TransformDef`] is the authored artifact — SQL over physical tables
//! (`Physical`) or ontology types (`Typed`), mirroring the two queue job
//! payloads. A [`TransformRun`] is the durable record of one execution; its
//! `run_id` doubles as the lineage `run_id`, so a run's lineage events are
//! queryable with no extra linkage. Runs freeze the body they executed:
//! redefinition never rewrites history.
//!
//! Slice 1 delivers definitions + runs only: [`validate_transform_def`]
//! rejects `schedule`/`on_input_commit` until slices 2/3 make them live.

use std::str::FromStr;

use async_trait::async_trait;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::TableRef;
use crate::error::{ControlPlaneError, Result};
use crate::page::{Page, PageReq};
use crate::queue::NewJob;
use crate::transform_job::{
    OutputMode, TRANSFORM_JOB_KIND, TYPED_TRANSFORM_JOB_KIND, TransformJob, TypedTransformJob,
};

/// A transform's name (unique key of the definition).
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TransformName(pub String);

/// The executable body of a transform — mirrors the queue job payloads.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum TransformBody {
    /// SQL over physical tables.
    Physical {
        inputs: Vec<TableRef>,
        output: TableRef,
        sql: String,
        #[serde(default)]
        output_mode: OutputMode,
    },
    /// SQL in ontology-type vocabulary.
    Typed {
        inputs: Vec<String>,
        output: String,
        sql: String,
        #[serde(default)]
        output_mode: OutputMode,
    },
}

impl TransformBody {
    /// Build the queue job for one run of this body, threading `run_id` into
    /// the payload. Serialization of the payload structs cannot fail (plain
    /// data), so this is infallible.
    #[must_use]
    pub fn to_job(&self, run_id: Uuid) -> NewJob {
        let (kind, payload) = match self {
            Self::Physical {
                inputs,
                output,
                sql,
                output_mode,
            } => (
                TRANSFORM_JOB_KIND,
                serde_json::to_value(TransformJob {
                    inputs: inputs.clone(),
                    output: output.clone(),
                    sql: sql.clone(),
                    output_mode: *output_mode,
                    run_id: Some(run_id),
                }),
            ),
            Self::Typed {
                inputs,
                output,
                sql,
                output_mode,
            } => (
                TYPED_TRANSFORM_JOB_KIND,
                serde_json::to_value(TypedTransformJob {
                    inputs: inputs.clone(),
                    output: output.clone(),
                    sql: sql.clone(),
                    output_mode: *output_mode,
                    run_id: Some(run_id),
                }),
            ),
        };
        NewJob {
            kind: kind.to_string(),
            payload: payload.unwrap_or(serde_json::Value::Null),
            run_at: None,
            priority: 0,
        }
    }
}

/// A named transform definition. `schedule` (slice 2) and `on_input_commit`
/// (slice 3) are carried in the shape but rejected by
/// [`validate_transform_def`] until their slices land.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TransformDef {
    pub name: TransformName,
    pub body: TransformBody,
    #[serde(default)]
    pub schedule: Option<String>,
    #[serde(default)]
    pub on_input_commit: bool,
}

/// What started a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunTrigger {
    Manual,
    Schedule,
    DataTrigger,
    AdHoc,
}

/// A run's lifecycle state. `Queued → Running → Succeeded | Failed`, with
/// `Running → Queued` on a retryable failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunState {
    Queued,
    Running,
    Succeeded,
    Failed,
}

impl RunTrigger {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Schedule => "schedule",
            Self::DataTrigger => "data-trigger",
            Self::AdHoc => "ad-hoc",
        }
    }
}

impl FromStr for RunTrigger {
    type Err = ControlPlaneError;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "manual" => Ok(Self::Manual),
            "schedule" => Ok(Self::Schedule),
            "data-trigger" => Ok(Self::DataTrigger),
            "ad-hoc" => Ok(Self::AdHoc),
            other => Err(ControlPlaneError::Validation(format!(
                "unknown run trigger: {other}"
            ))),
        }
    }
}

impl RunState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

impl FromStr for RunState {
    type Err = ControlPlaneError;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            other => Err(ControlPlaneError::Validation(format!(
                "unknown run state: {other}"
            ))),
        }
    }
}

/// The durable record of one transform execution. `body` is frozen at submit
/// time. Deliberately NOT serde: postgres stores typed columns and HTTP maps
/// to explicit DTOs, so this never crosses a serde boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct TransformRun {
    /// Doubles as the lineage `run_id`.
    pub run_id: Uuid,
    /// `None` for ad-hoc runs.
    pub transform: Option<TransformName>,
    pub trigger: RunTrigger,
    pub state: RunState,
    pub body: TransformBody,
    pub queued_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub snapshot_id: Option<i64>,
    pub error: Option<String>,
}

/// Terminal-or-retry outcome reported for a run.
#[derive(Clone, Debug, PartialEq)]
pub enum RunOutcome {
    /// The commit produced this snapshot. (A no-snapshot commit is the
    /// worker's deterministic-abandon path and ends `Failed`.)
    Succeeded { snapshot_id: i64 },
    /// Retryable failure: state back to `Queued`, error retained.
    RetryQueued { error: String },
    /// Terminal failure (abandon).
    Failed { error: String },
}

/// Slice-1 definition validation, shared by both adapters: `schedule` and
/// `on_input_commit` are carried in the shape but not yet live.
pub fn validate_transform_def(def: &TransformDef) -> Result<()> {
    if def.schedule.is_some() {
        return Err(ControlPlaneError::Validation(
            "transform schedules are not supported yet (slice 2)".into(),
        ));
    }
    if def.on_input_commit {
        return Err(ControlPlaneError::Validation(
            "data-triggered transforms are not supported yet (slice 3)".into(),
        ));
    }
    Ok(())
}

/// The transforms concern: named definitions and their runs.
#[async_trait]
pub trait Transforms {
    /// Define or redefine (upsert) a transform. Typed bodies validate that
    /// input/output type names exist in the ontology (`Validation` otherwise).
    async fn define_transform(&self, def: TransformDef) -> Result<()>;
    /// `NotFound` if undefined.
    async fn get_transform(&self, name: &TransformName) -> Result<TransformDef>;
    /// All definitions, name-ordered, single full page (`page` accepted for
    /// future use like the other list reads).
    async fn list_transforms(&self, page: PageReq) -> Result<Page<TransformDef>>;
    /// Idempotent: deleting an unknown name is `Ok(())`. Runs keep their
    /// frozen body and name (plain text, no FK) — history survives deletion.
    async fn delete_transform(&self, name: &TransformName) -> Result<()>;

    /// Record `run` (must be `Queued`) and enqueue `job` atomically: the job
    /// is visible to a worker iff the run row exists.
    async fn submit_run(&self, run: TransformRun, job: NewJob) -> Result<crate::JobId>;
    /// `Queued → Running`, stamping `started_at`. `NotFound` if unknown.
    async fn mark_run_running(&self, run_id: Uuid) -> Result<()>;
    /// Apply a [`RunOutcome`]. `NotFound` if unknown.
    async fn finish_run(&self, run_id: Uuid, outcome: RunOutcome) -> Result<()>;
    /// `NotFound` if unknown.
    async fn get_run(&self, run_id: Uuid) -> Result<TransformRun>;
    /// Runs (optionally of one transform), newest first (`queued_at` desc,
    /// `run_id` desc tiebreak), single full page.
    async fn list_runs(
        &self,
        transform: Option<&TransformName>,
        page: PageReq,
    ) -> Result<Page<TransformRun>>;
}
