//! A generic job-queue worker: a `dequeue → handle → complete/fail` loop with
//! `await_jobs` wakeups (LISTEN/NOTIFY for the pg adapter, a `Notify` for the
//! fake), a polling fallback, and graceful shutdown via a `CancellationToken`.
//!
//! Generic over any [`Queue`]; owns the queue by value (adapters are `Clone`).
//! `core` stays runtime-free — the worker carries the tokio dependency.

use std::future::Future;
use std::time::Duration;

use control_plane_core::{Job, JobFailure, Queue, Result};
use tokio_util::sync::CancellationToken;

/// Default polling fallback interval. NOTIFY drives normal latency; this bounds
/// the wait when a notification is missed or a lock expires for reclaim.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// A worker that processes jobs of the given kinds from a [`Queue`].
pub struct Worker<Q> {
    queue: Q,
    worker_id: String,
    poll_interval: Duration,
    heartbeat_interval: Duration,
}

impl<Q: Queue + Send + Sync> Worker<Q> {
    /// Create a worker. `worker_id` stamps the lock (`locked_by`) so reclaim and
    /// observability can attribute in-flight jobs. `lease` MUST match the queue's
    /// configured `lock_timeout` — it is how long a claimed job stays locked
    /// without a heartbeat. The worker heartbeats the in-flight job every
    /// `lease / 3` (floored at 1ms) so a handler running longer than the lease is
    /// not reclaimed and double-executed.
    pub fn new(queue: Q, worker_id: impl Into<String>, lease: Duration) -> Self {
        Self {
            queue,
            worker_id: worker_id.into(),
            poll_interval: DEFAULT_POLL_INTERVAL,
            heartbeat_interval: (lease / 3).max(Duration::from_millis(1)),
        }
    }

    /// Override the polling fallback interval (mainly for tests).
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Run the loop until `shutdown` is cancelled. Per iteration: claim a job and
    /// run `handler` (`Ok` → complete, `Err(JobFailure)` → fail with its policy);
    /// if none is available, wait for a wakeup or the poll interval. An in-flight
    /// job is always finished before shutdown returns; cancellation is only
    /// observed between jobs and while idle.
    pub async fn run<F, Fut>(
        &self,
        kinds: &[String],
        shutdown: CancellationToken,
        handler: F,
    ) -> Result<()>
    where
        F: Fn(Job) -> Fut + Send,
        Fut: Future<Output = std::result::Result<(), JobFailure>> + Send,
    {
        loop {
            if shutdown.is_cancelled() {
                break;
            }
            match self.queue.dequeue(kinds, &self.worker_id).await? {
                Some(job) => {
                    let id = job.id;
                    // Run the handler while heartbeating the lease on a timer, so a
                    // handler that outlives `lock_timeout` isn't reclaimed and
                    // double-executed. The heartbeat is best-effort: a missed tick is
                    // recoverable (the next tick retries; worst case the lease lapses
                    // and reclaim does its job) — deliberately asymmetric with the
                    // `?`-propagating dequeue/complete/fail below.
                    let fut = handler(job);
                    tokio::pin!(fut);
                    let mut hb = tokio::time::interval(self.heartbeat_interval);
                    let outcome = loop {
                        tokio::select! {
                            res = &mut fut => break res,
                            _ = hb.tick() => {
                                let _ = self.queue.heartbeat(id).await;
                            }
                        }
                    };
                    match outcome {
                        Ok(()) => self.queue.complete(id).await?,
                        Err(JobFailure { error, policy }) => {
                            self.queue.fail(id, &error, policy).await?
                        }
                    }
                }
                None => {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        r = self.queue.await_jobs(kinds, self.poll_interval) => { r?; }
                    }
                }
            }
        }
        Ok(())
    }
}
