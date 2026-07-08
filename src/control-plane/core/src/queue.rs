//! The job-queue concern: typed job envelope + the `Queue` trait. Payloads are
//! opaque JSON; retry is caller-driven (`fail` takes a `RetryPolicy`).

use std::time::Duration;

use async_trait::async_trait;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::error::Result;

/// Identifier for an enqueued job.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct JobId(pub Uuid);

/// A job to enqueue.
pub struct NewJob {
    pub kind: String,
    pub payload: serde_json::Value,
    /// When the job becomes eligible. `None` => now.
    pub run_at: Option<OffsetDateTime>,
    /// Higher dequeued first.
    pub priority: i32,
}

/// Every job kind a worker dispatches — the define-time allowlist for action
/// `downstream` [`crate::JobTemplate`]s. Mirrors the worker's dispatch match: a kind an
/// action enqueues but no worker handles would deadlock the queue. The consts are
/// re-exported at the crate root, so `crate::<KIND>` resolves here.
pub const KNOWN_JOB_KINDS: &[&str] = &[
    crate::FLUSH_JOB_KIND,
    crate::GC_JOB_KIND,
    crate::COMPACT_JOB_KIND,
    crate::BUILD_VECTOR_INDEX_JOB_KIND,
    crate::STREAM_CONSOLIDATE_JOB_KIND,
    crate::TRANSFORM_JOB_KIND,
    crate::TYPED_TRANSFORM_JOB_KIND,
];

/// A claimed job handed to a worker.
#[derive(Debug)]
pub struct Job {
    pub id: JobId,
    pub kind: String,
    pub payload: serde_json::Value,
    /// Number of times this job has been dequeued (this claim included). Callers
    /// use it to compute their own backoff.
    pub attempts: i32,
    pub run_at: OffsetDateTime,
}

/// What to do with a job after a failure. Caller-driven.
#[derive(Debug)]
pub enum RetryPolicy {
    /// Make the job available again after `delay`.
    Retry { delay: Duration },
    /// Move the job to a terminal `failed` state, retained for inspection.
    Abandon,
}

/// What a worker's job handler returns on failure: a message plus the
/// caller-driven [`RetryPolicy`] to apply.
#[derive(Debug)]
pub struct JobFailure {
    pub error: String,
    pub policy: RetryPolicy,
}

impl JobFailure {
    /// A terminal failure: move the job to `failed`, retained for inspection.
    pub fn abandon(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            policy: RetryPolicy::Abandon,
        }
    }

    /// A retryable failure: make the job available again after `delay`.
    pub fn retry(delay: std::time::Duration, error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            policy: RetryPolicy::Retry { delay },
        }
    }
}

#[async_trait]
pub trait Queue {
    /// Enqueue a job (autocommit). For transactional enqueue, use [`crate::Tx::enqueue`].
    async fn enqueue(&self, job: NewJob) -> Result<JobId>;
    /// Claim the next eligible job for one of `kinds`, marking it running under
    /// `worker`. Jobs are eligible when `available`, or `running` with an expired
    /// lock (crashed-worker reclaim).
    async fn dequeue(&self, kinds: &[String], worker: &str) -> Result<Option<Job>>;
    /// Remove a finished job.
    async fn complete(&self, id: JobId) -> Result<()>;
    /// Record a failure and apply `policy`.
    async fn fail(&self, id: JobId, error: &str, policy: RetryPolicy) -> Result<()>;
    /// Refresh the lock so a long-running job isn't reclaimed.
    async fn heartbeat(&self, id: JobId) -> Result<()>;
    /// Block until a job of one of `kinds` may have become available, or until
    /// `timeout` elapses — whichever comes first. A best-effort wakeup hint for
    /// workers: spurious early returns are allowed (the caller re-checks via
    /// `dequeue`), and the `timeout` is the polling fallback that bounds latency
    /// when a notification is missed.
    async fn await_jobs(&self, kinds: &[String], timeout: Duration) -> Result<()>;
}
