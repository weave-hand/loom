use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{Job, JobId, NewJob, Queue, Result, RetryPolicy};
use time::OffsetDateTime;

use crate::MemoryControlPlane;

#[async_trait]
impl Queue for MemoryControlPlane {
    #[tracing::instrument(skip(self, job), level = "debug")]
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        let id = Self::insert(&mut self.rows.lock().unwrap(), job);
        self.notify.notify_waiters();
        Ok(JobId(id))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn dequeue(&self, kinds: &[String], _worker: &str) -> Result<Option<Job>> {
        let now = OffsetDateTime::now_utc();
        let cutoff = now - self.lock_timeout;
        let mut rows = self.rows.lock().unwrap();
        let mut idxs: Vec<usize> = (0..rows.len())
            .filter(|&i| {
                let r = &rows[i];
                kinds.contains(&r.kind)
                    && r.run_at <= now
                    && (r.state == "available"
                        || (r.state == "running" && r.locked_at.is_none_or(|t| t < cutoff)))
            })
            .collect();
        idxs.sort_by(|&a, &b| {
            rows[b]
                .priority
                .cmp(&rows[a].priority)
                .then(rows[a].run_at.cmp(&rows[b].run_at))
        });
        let Some(&i) = idxs.first() else {
            return Ok(None);
        };
        rows[i].state = "running";
        rows[i].locked_at = Some(now);
        rows[i].attempts += 1;
        let r = &rows[i];
        Ok(Some(Job {
            id: JobId(r.id),
            kind: r.kind.clone(),
            payload: r.payload.clone(),
            attempts: r.attempts,
            run_at: r.run_at,
        }))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn complete(&self, id: JobId) -> Result<()> {
        self.rows.lock().unwrap().retain(|r| r.id != id.0);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn fail(&self, id: JobId, _error: &str, policy: RetryPolicy) -> Result<()> {
        let mut rows = self.rows.lock().unwrap();
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
        let mut rows = self.rows.lock().unwrap();
        if let Some(r) = rows.iter_mut().find(|r| r.id == id.0) {
            r.locked_at = Some(OffsetDateTime::now_utc());
        }
        Ok(())
    }

    async fn await_jobs(&self, _kinds: &[String], timeout: Duration) -> Result<()> {
        // notify_waiters only wakes already-registered waiters; a notification
        // racing ahead of `notified()` is intentionally lost — the `timeout`
        // polling fallback bounds the resulting latency (same contract as pg).
        let _ = tokio::time::timeout(timeout, self.notify.notified()).await;
        Ok(())
    }
}
