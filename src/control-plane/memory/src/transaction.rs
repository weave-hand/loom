use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use control_plane_core::{JobId, LineageEvent, NewJob, Result, Tx};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::lineage::LineageState;
use crate::{MemoryControlPlane, Row};

pub(crate) struct MemoryTx {
    pub(crate) rows: Arc<Mutex<Vec<Row>>>,
    pub(crate) notify: Arc<Notify>,
    pub(crate) lineage: Arc<Mutex<LineageState>>,
    pub(crate) staged: Vec<(Uuid, NewJob)>,
    pub(crate) staged_events: Vec<LineageEvent>,
}

#[async_trait]
impl Tx for MemoryTx {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn commit(self: Box<Self>) -> Result<()> {
        let staged_any = !self.staged.is_empty();
        {
            // Hold BOTH locks across the whole apply so commit is atomic w.r.t. any
            // single-lock reader (dequeue locks `rows`; events_for locks `lineage`):
            // no partial commit is observable. Lock order rows-then-lineage must be
            // consistent everywhere to stay deadlock-free (readers take only one
            // lock; no reader takes both).
            let mut rows = self.rows.lock().unwrap();
            let mut lin = self.lineage.lock().unwrap();
            for (id, job) in self.staged {
                MemoryControlPlane::insert_with_id(&mut rows, id, job);
            }
            lin.events.extend(self.staged_events);
        }
        if staged_any {
            self.notify.notify_waiters();
        }
        Ok(())
    }
    #[tracing::instrument(skip(self), level = "debug")]
    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
    }
    #[tracing::instrument(skip(self, job), level = "debug")]
    async fn enqueue(&mut self, job: NewJob) -> Result<JobId> {
        let id = Uuid::new_v4();
        self.staged.push((id, job));
        Ok(JobId(id))
    }
    #[tracing::instrument(skip(self, event), fields(run_id = ?event.run_id, event_type = ?event.event_type), level = "debug")]
    async fn emit(&mut self, event: LineageEvent) -> Result<()> {
        self.staged_events.push(event);
        Ok(())
    }
}
