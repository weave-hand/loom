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
}
