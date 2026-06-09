//! The cross-concern transaction seam. `ControlPlane::begin` opens a unit of work;
//! operations issued on the returned `Tx` commit together or roll back together.

use async_trait::async_trait;

use crate::catalog::{SnapshotId, TableRef};
use crate::error::Result;
use crate::lineage::LineageEvent;
use crate::queue::{JobId, NewJob};
use crate::snapshot::{ColumnSpec, DataFile};

#[async_trait]
pub trait ControlPlane: Send + Sync {
    /// Open a unit of work. Issue operations on the returned `Tx`, then `commit`
    /// or `rollback`.
    async fn begin(&self) -> Result<Box<dyn Tx + Send>>;
}

#[async_trait]
pub trait Tx: Send {
    /// Commit the unit of work. Returns the new `SnapshotId` if a catalog op
    /// (create_table/append_files) was staged, else `None`.
    async fn commit(self: Box<Self>) -> Result<Option<SnapshotId>>;
    async fn rollback(self: Box<Self>) -> Result<()>;
    /// Enqueue a job within this unit of work: visible to workers only if the
    /// transaction commits. Makes "commit a change AND enqueue downstream work"
    /// atomic.
    async fn enqueue(&mut self, job: NewJob) -> Result<JobId>;
    /// Emit a lineage event within this unit of work: persisted only if the
    /// transaction commits. Makes "record lineage AND enqueue downstream work"
    /// atomic.
    async fn emit(&mut self, event: LineageEvent) -> Result<()>;
    /// Create a physical DuckLake table. Staged; applied at commit. Idempotent: a
    /// no-op if the table already exists live.
    async fn create_table(&mut self, table: &TableRef, columns: &[ColumnSpec]) -> Result<()>;
    /// Register already-written Parquet data files as part of the snapshot. Staged.
    async fn append_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()>;
}
