use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{Job, JobId, NewJob, Queue, Result, RetryPolicy};
use time::OffsetDateTime;

use crate::MemoryControlPlane;

#[async_trait]
impl Queue for MemoryControlPlane {
    #[tracing::instrument(skip(self, job), level = "debug")]
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        let id = Self::insert(&mut self.rows.lock(), job);
        self.notify.notify_waiters();
        Ok(JobId(id))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn dequeue(&self, kinds: &[String], _worker: &str) -> Result<Option<Job>> {
        let now = OffsetDateTime::now_utc();
        let cutoff = now - self.lock_timeout;
        let mut rows = self.rows.lock();
        let mut idxs: Vec<usize> = (0..rows.len())
            .filter(|&i| {
                rows.get(i).is_some_and(|r| {
                    kinds.contains(&r.kind)
                        && r.run_at <= now
                        && (r.state == "available"
                            || (r.state == "running" && r.locked_at.is_none_or(|t| t < cutoff)))
                })
            })
            .collect();
        idxs.sort_by(|&a, &b| {
            let pa = rows.get(a).map_or(0, |r| r.priority);
            let pb = rows.get(b).map_or(0, |r| r.priority);
            let ra = rows.get(a).map(|r| r.run_at);
            let rb = rows.get(b).map(|r| r.run_at);
            pb.cmp(&pa).then(ra.cmp(&rb))
        });
        let Some(&i) = idxs.first() else {
            return Ok(None);
        };
        let row = rows
            .get_mut(i)
            .ok_or_else(|| control_plane_core::ControlPlaneError::NotFound("queue row".into()))?;
        row.state = "running";
        row.locked_at = Some(now);
        row.attempts += 1;
        Ok(Some(Job {
            id: JobId(row.id),
            kind: row.kind.clone(),
            payload: row.payload.clone(),
            attempts: row.attempts,
            run_at: row.run_at,
        }))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn complete(&self, id: JobId) -> Result<()> {
        self.rows.lock().retain(|r| r.id != id.0);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn fail(&self, id: JobId, _error: &str, policy: RetryPolicy) -> Result<()> {
        let mut rows = self.rows.lock();
        if let Some(r) = rows.iter_mut().find(|r| r.id == id.0) {
            match policy {
                RetryPolicy::Retry { delay } => {
                    r.state = "available";
                    r.run_at = OffsetDateTime::now_utc() + delay;
                    r.locked_at = None;
                }
                RetryPolicy::Abandon => {
                    r.state = "failed";
                    r.locked_at = None;
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn heartbeat(&self, id: JobId) -> Result<()> {
        let mut rows = self.rows.lock();
        if let Some(r) = rows.iter_mut().find(|r| r.id == id.0) {
            r.locked_at = Some(OffsetDateTime::now_utc());
        }
        Ok(())
    }

    async fn await_jobs(&self, _kinds: &[String], timeout: Duration) -> Result<()> {
        // notify_waiters only wakes already-registered waiters; a notification
        // racing ahead of `notified()` is intentionally lost — the `timeout`
        // polling fallback bounds the resulting latency (same contract as pg).
        // Intentionally ignore whether the timeout elapsed or a notification
        // arrived — the caller polls again either way.
        drop(tokio::time::timeout(timeout, self.notify.notified()).await);
        Ok(())
    }
}
