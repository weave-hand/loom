//! The transforms concern: named transform definitions and their runs.
//!
//! A [`TransformDef`] is the authored artifact — SQL over physical tables
//! (`Physical`) or ontology types (`Typed`), mirroring the two queue job
//! payloads. A [`TransformRun`] is the durable record of one execution; its
//! `run_id` doubles as the lineage `run_id`, so a run's lineage events are
//! queryable with no extra linkage. Runs freeze the body they executed:
//! redefinition never rewrites history.
//!
//! Slice 1 delivered definitions + runs only, rejecting both `schedule` and
//! `on_input_commit`. Slice 2 makes `schedule` live: [`validate_cron`] and
//! [`next_cron_occurrence`] back real cron validation/scheduling in
//! [`validate_transform_def`]. Slice 3 makes `on_input_commit` live:
//! [`TriggerNode`] resolves a def's physical io and
//! [`validate_no_trigger_cycle`] rejects a firing cycle among the
//! data-triggered set; adapters run that check at define-time (with the
//! existing def set + ontology), not this module.

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

/// A named transform definition. `schedule`, when set, is a cron expression
/// validated by [`validate_transform_def`] (slice 2, live). `on_input_commit`
/// (slice 3, live) marks the def as firing on a commit to one of its inputs;
/// adapter-side define-time validation rejects trigger cycles.
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

/// Validate a cron expression. Standard 5-field (minute hour day-of-month
/// month day-of-week), evaluated in UTC. Croner's default configuration
/// (used here) does NOT tolerate an optional seconds field — a 6-field
/// expression is rejected unless the caller opts in via croner's
/// `with_seconds_optional`/`with_seconds_required` builders, which this
/// helper does not use. Observed against the vendored croner 2.2.0 source.
pub fn validate_cron(expr: &str) -> Result<()> {
    croner::Cron::new(expr)
        .parse()
        .map(|_| ())
        .map_err(|e| ControlPlaneError::Validation(format!("invalid cron expression: {e}")))
}

/// The next UTC occurrence of `expr` strictly after `after`. Evaluation is
/// pinned to UTC by constructing a `chrono::DateTime<Utc>` from `after` and
/// passing it to croner's generic `find_next_occurrence` — croner takes the
/// timezone from the `DateTime` argument, so there is no `Local`-based entry
/// point in this path.
pub fn next_cron_occurrence(expr: &str, after: OffsetDateTime) -> Result<OffsetDateTime> {
    let cron = croner::Cron::new(expr)
        .parse()
        .map_err(|e| ControlPlaneError::Validation(format!("invalid cron expression: {e}")))?;
    let after_c =
        chrono::DateTime::<chrono::Utc>::from_timestamp(after.unix_timestamp(), after.nanosecond())
            .ok_or_else(|| ControlPlaneError::Validation("timestamp out of range".into()))?;
    let next = cron
        .find_next_occurrence(&after_c, false)
        .map_err(|e| ControlPlaneError::Validation(format!("no next cron occurrence: {e}")))?;
    OffsetDateTime::from_unix_timestamp(next.timestamp())
        .map_err(|e| ControlPlaneError::Validation(format!("cron occurrence out of range: {e}")))
}

/// Definition validation, shared by both adapters. `schedule` (slice 2) is
/// live: a valid cron expression is accepted, an invalid one rejected.
/// `on_input_commit` (slice 3) is live too; cycle validation among the
/// data-triggered set is adapter-side (it needs the existing def set and the
/// ontology to resolve typed bodies), not here.
pub fn validate_transform_def(def: &TransformDef) -> Result<()> {
    if def.name.0.is_empty() {
        return Err(ControlPlaneError::Validation(
            "transform name must not be empty".into(),
        ));
    }
    if def.name.0 == "run" {
        return Err(ControlPlaneError::Validation(
            "transform name 'run' is reserved (collides with the ad-hoc run route)".into(),
        ));
    }
    if let Some(expr) = &def.schedule {
        validate_cron(expr)?;
    }
    Ok(())
}

/// A data-triggered def's physical io, resolved for cycle validation and
/// commit-seam matching.
#[derive(Clone, Debug, PartialEq)]
pub struct TriggerNode {
    pub name: String,
    pub inputs: Vec<TableRef>,
    /// `None` when the def's output does not resolve (e.g. a deleted
    /// ontology type): an unresolvable output can never match a commit, so
    /// it contributes no edge.
    pub output: Option<TableRef>,
}

impl TriggerNode {
    /// Resolve `body` to physical io given `types` (ontology type name →
    /// backing table). Typed names absent from `types` contribute nothing:
    /// a vanished binding cannot match a commit, so it forms no edge.
    #[must_use]
    pub fn resolve(
        name: &TransformName,
        body: &TransformBody,
        types: &std::collections::HashMap<String, TableRef>,
    ) -> Self {
        let (inputs, output) = match body {
            TransformBody::Physical { inputs, output, .. } => {
                (inputs.clone(), Some(output.clone()))
            }
            TransformBody::Typed { inputs, output, .. } => (
                inputs
                    .iter()
                    .filter_map(|t| types.get(t).cloned())
                    .collect(),
                types.get(output).cloned(),
            ),
        };
        Self {
            name: name.0.clone(),
            inputs,
            output,
        }
    }
}

/// Reject a firing cycle among data-triggered defs. Edge X → Y iff Y reads
/// X's resolved output (a def reading its own output is a self-cycle).
/// `nodes` is the complete data-triggered set INCLUDING the candidate being
/// defined. Kahn's algorithm: repeatedly remove zero-in-degree nodes; any
/// remainder is cyclic and is named in the error.
pub fn validate_no_trigger_cycle(nodes: &[TriggerNode]) -> Result<()> {
    use std::collections::HashMap;
    let mut indegree: HashMap<&str, usize> = nodes.iter().map(|n| (n.name.as_str(), 0)).collect();
    let mut succs: HashMap<&str, Vec<&str>> = HashMap::new();
    for from in nodes {
        let Some(out) = &from.output else { continue };
        for to in nodes.iter().filter(|to| to.inputs.contains(out)) {
            succs
                .entry(from.name.as_str())
                .or_default()
                .push(to.name.as_str());
            if let Some(d) = indegree.get_mut(to.name.as_str()) {
                *d += 1;
            }
        }
    }
    let mut ready: Vec<&str> = indegree
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(n, _)| *n)
        .collect();
    let mut removed = 0usize;
    while let Some(n) = ready.pop() {
        removed += 1;
        for s in succs.get(n).map(Vec::as_slice).unwrap_or_default() {
            if let Some(d) = indegree.get_mut(s) {
                *d -= 1;
                if *d == 0 {
                    ready.push(s);
                }
            }
        }
    }
    if removed == nodes.len() {
        return Ok(());
    }
    let mut cyclic: Vec<&str> = indegree
        .iter()
        .filter(|(_, d)| **d > 0)
        .map(|(n, _)| *n)
        .collect();
    cyclic.sort_unstable();
    Err(ControlPlaneError::Validation(format!(
        "data-trigger cycle among transforms: {}",
        cyclic.join(", ")
    )))
}

/// The transforms concern: named definitions and their runs.
///
/// Lifecycle methods deliberately carry no state-transition guards: the queue
/// is at-least-once, so a retried job that already committed may legitimately
/// re-mark a terminal run — the record follows execution, it does not gate it.
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
    /// Sweep runs stuck `Running` whose queue job is no longer live (neither
    /// `available` nor in-flight `running`) and whose `started_at` predates
    /// `running_since_before`: mark each `Failed("reporting lost")`. Returns the
    /// swept run ids. Runs with a live job are left untouched — they self-heal on
    /// retry or crashed-worker reclaim.
    async fn reconcile_stranded_runs(
        &self,
        running_since_before: OffsetDateTime,
    ) -> Result<Vec<Uuid>>;
    /// `NotFound` if unknown.
    async fn get_run(&self, run_id: Uuid) -> Result<TransformRun>;
    /// Runs (optionally of one transform), newest first (`queued_at` desc,
    /// `run_id` desc tiebreak), single full page.
    async fn list_runs(
        &self,
        transform: Option<&TransformName>,
        page: PageReq,
    ) -> Result<Page<TransformRun>>;

    /// Atomically claim schedule-due definitions: `schedule` set and
    /// `next_run_at <= now`, at most `limit`, advancing each claimed def's
    /// `next_run_at` to the next occurrence after `now`. Concurrent claimers
    /// never both receive the same due def. A claimed occurrence that the
    /// caller fails to submit is SKIPPED, not retried (at-most-once).
    async fn claim_due_schedules(
        &self,
        now: OffsetDateTime,
        limit: u32,
    ) -> Result<Vec<TransformDef>>;
    /// Derived schedule state: when the def would next fire (`None` when
    /// unscheduled). `NotFound` for an unknown transform.
    async fn next_run_at(&self, name: &TransformName) -> Result<Option<OffsetDateTime>>;

    /// Every definition with `on_input_commit` set, name-ordered — the
    /// commit-seam matcher's candidate set.
    async fn data_triggered_defs(&self) -> Result<Vec<TransformDef>>;
}
