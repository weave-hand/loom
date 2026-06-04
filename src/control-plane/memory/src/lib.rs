//! In-memory fake adapter for the control-plane traits — fast, hermetic tests
//! and local dev. NOT for production use. Jobs live in a `Vec` behind a `Mutex`;
//! a `Tx` stages writes and applies them on commit (read-committed semantics).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{ControlPlane, Job, JobId, NewJob, Queue, Result, RetryPolicy, Tx};
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Clone)]
struct Row {
    id: Uuid,
    kind: String,
    payload: serde_json::Value,
    state: &'static str, // "available" | "running" | "failed"
    run_at: OffsetDateTime,
    priority: i32,
    attempts: i32,
    locked_at: Option<OffsetDateTime>,
}

#[derive(Clone)]
pub struct MemoryControlPlane {
    rows: Arc<Mutex<Vec<Row>>>,
    lock_timeout: Duration,
}

impl MemoryControlPlane {
    pub fn new(lock_timeout: Duration) -> Self {
        Self {
            rows: Arc::new(Mutex::new(Vec::new())),
            lock_timeout,
        }
    }

    fn insert(rows: &mut Vec<Row>, job: NewJob) -> Uuid {
        let id = Uuid::new_v4();
        Self::insert_with_id(rows, id, job);
        id
    }

    fn insert_with_id(rows: &mut Vec<Row>, id: Uuid, job: NewJob) {
        rows.push(Row {
            id,
            kind: job.kind,
            payload: job.payload,
            state: "available",
            run_at: job.run_at.unwrap_or_else(OffsetDateTime::now_utc),
            priority: job.priority,
            attempts: 0,
            locked_at: None,
        });
    }
}

#[async_trait]
impl Queue for MemoryControlPlane {
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        Ok(JobId(Self::insert(&mut self.rows.lock().unwrap(), job)))
    }

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

    async fn complete(&self, id: JobId) -> Result<()> {
        self.rows.lock().unwrap().retain(|r| r.id != id.0);
        Ok(())
    }

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

    async fn heartbeat(&self, id: JobId) -> Result<()> {
        let mut rows = self.rows.lock().unwrap();
        if let Some(r) = rows.iter_mut().find(|r| r.id == id.0) {
            r.locked_at = Some(OffsetDateTime::now_utc());
        }
        Ok(())
    }
}

#[async_trait]
impl ControlPlane for MemoryControlPlane {
    async fn begin(&self) -> Result<Box<dyn Tx + Send>> {
        Ok(Box::new(MemoryTx {
            rows: self.rows.clone(),
            staged: Vec::new(),
        }))
    }
}

struct MemoryTx {
    rows: Arc<Mutex<Vec<Row>>>,
    staged: Vec<(Uuid, NewJob)>,
}

#[async_trait]
impl Tx for MemoryTx {
    async fn commit(self: Box<Self>) -> Result<()> {
        let mut rows = self.rows.lock().unwrap();
        for (id, job) in self.staged {
            MemoryControlPlane::insert_with_id(&mut rows, id, job);
        }
        Ok(())
    }
    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
    }
    async fn enqueue(&mut self, job: NewJob) -> Result<JobId> {
        let id = Uuid::new_v4();
        self.staged.push((id, job));
        Ok(JobId(id))
    }
}
