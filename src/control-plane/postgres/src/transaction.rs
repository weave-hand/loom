//! The Postgres `Tx` implementation for the **non-table-format** transactional
//! concerns: transactional queue `enqueue` and lineage `emit`, committed/rolled
//! back as a single real Postgres transaction.
//!
//! The DuckLake table format that this transaction used to also drive (staged
//! `create_table`/`append_files`/`replace_files`/`compact_files` flushed by
//! `snapshot::commit_snapshot` on commit) has been removed; Iceberg is the table
//! format. Those table-write methods now return an explicit error — the Iceberg
//! write path lives in `IcebergControlPlane`/`IcebergMaterializer`, not here.

use async_trait::async_trait;
use control_plane_core::{
    ColumnSpec, ControlPlaneError, DataFile, JobId, LineageEvent, NewJob, Result, SnapshotId,
    TableRef, Tx,
};
use sqlx::Postgres;

use crate::backend;
use crate::lineage::pg_emit;
use crate::queue::pg_insert;

pub(crate) struct PgTx {
    pub(crate) tx: sqlx::Transaction<'static, Postgres>,
}

/// The error returned by the table-format write methods, which DuckLake used to
/// back. Iceberg owns the table-format write path now.
fn no_table_format() -> ControlPlaneError {
    ControlPlaneError::Validation(
        "PgControlPlane transactions carry no table-format writer; use IcebergControlPlane".into(),
    )
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

    async fn create_table(&mut self, _table: &TableRef, _columns: &[ColumnSpec]) -> Result<()> {
        Err(no_table_format())
    }

    async fn append_files(&mut self, _table: &TableRef, _files: &[DataFile]) -> Result<()> {
        Err(no_table_format())
    }

    async fn replace_files(&mut self, _table: &TableRef, _files: &[DataFile]) -> Result<()> {
        Err(no_table_format())
    }

    async fn compact_files(
        &mut self,
        _table: &TableRef,
        _expire: &[String],
        _write: &[DataFile],
    ) -> Result<()> {
        Err(no_table_format())
    }
}
