use async_trait::async_trait;
use control_plane_core::{
    ColumnSpec, DataFile, JobId, LineageEvent, NewJob, Result, SnapshotId, TableRef, Tx,
};
use sqlx::Postgres;

use crate::backend;
use crate::lineage::pg_emit;
use crate::queue::pg_insert;

pub(crate) struct PgTx {
    pub(crate) tx: sqlx::Transaction<'static, Postgres>,
    pub(crate) staged_tables: Vec<(TableRef, Vec<ColumnSpec>)>,
    pub(crate) staged_files: Vec<(TableRef, Vec<DataFile>)>,
    pub(crate) staged_replacements: Vec<(TableRef, Vec<DataFile>)>,
}

#[async_trait]
impl Tx for PgTx {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn commit(mut self: Box<Self>) -> Result<Option<SnapshotId>> {
        if !self.staged_tables.is_empty()
            || !self.staged_files.is_empty()
            || !self.staged_replacements.is_empty()
        {
            let id = crate::snapshot::commit_snapshot(
                &mut self.tx,
                &self.staged_tables,
                &self.staged_files,
                &self.staged_replacements,
            )
            .await?;
            self.tx.commit().await.map_err(backend)?;
            return Ok(Some(id));
        }
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

    async fn create_table(&mut self, table: &TableRef, columns: &[ColumnSpec]) -> Result<()> {
        self.staged_tables.push((table.clone(), columns.to_vec()));
        Ok(())
    }

    async fn append_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()> {
        self.staged_files.push((table.clone(), files.to_vec()));
        Ok(())
    }

    async fn replace_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()> {
        self.staged_replacements
            .push((table.clone(), files.to_vec()));
        Ok(())
    }
}
