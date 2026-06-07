use async_trait::async_trait;
use control_plane_core::{JobId, LineageEvent, NewJob, Result, Tx};
use sqlx::Postgres;

use crate::backend;
use crate::lineage::pg_emit;
use crate::queue::pg_insert;

pub(crate) struct PgTx {
    pub(crate) tx: sqlx::Transaction<'static, Postgres>,
}

#[async_trait]
impl Tx for PgTx {
    async fn commit(self: Box<Self>) -> Result<()> {
        self.tx.commit().await.map_err(backend)
    }
    async fn rollback(self: Box<Self>) -> Result<()> {
        self.tx.rollback().await.map_err(backend)
    }
    async fn enqueue(&mut self, job: NewJob) -> Result<JobId> {
        pg_insert(&mut *self.tx, &job).await
    }
    async fn emit(&mut self, event: LineageEvent) -> Result<()> {
        pg_emit(&mut *self.tx, &event).await
    }
}
