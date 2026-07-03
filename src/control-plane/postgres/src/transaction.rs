//! The Postgres `Tx` implementation for the **non-table-format** transactional
//! concerns: transactional queue `enqueue` and lineage `emit`, committed/rolled
//! back as a single real Postgres transaction.
//!
//! `PgTx` implements only the narrow `Tx` — it carries no table-format staging
//! surface (`TableTx`); the Iceberg write path lives in
//! `IcebergControlPlane`/`IcebergMaterializer`, not here.

use async_trait::async_trait;
use control_plane_core::{JobId, LineageEvent, NewJob, Result, SnapshotId, Tx};
use sqlx::Postgres;

use crate::backend;
use crate::lineage::pg_emit;
use crate::queue::pg_insert;

pub(crate) struct PgTx {
    pub(crate) tx: sqlx::Transaction<'static, Postgres>,
}

#[async_trait]
impl Tx for PgTx {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn commit(self: Box<Self>) -> Result<Option<SnapshotId>> {
        self.tx.commit().await.map_err(backend)?;
        Ok(None)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn rollback(self: Box<Self>) -> Result<()> {
        self.tx.rollback().await.map_err(backend)
    }

    #[tracing::instrument(skip(self, job), level = "debug")]
    async fn enqueue(&mut self, job: NewJob) -> Result<JobId> {
        pg_insert(&mut *self.tx, &job).await
    }

    #[tracing::instrument(skip(self, event), fields(run_id = ?event.run_id, event_type = ?event.event_type), level = "debug")]
    async fn emit(&mut self, event: LineageEvent) -> Result<()> {
        pg_emit(&mut *self.tx, &event).await
    }
}
