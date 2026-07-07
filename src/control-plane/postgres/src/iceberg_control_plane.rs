//! An Iceberg-backed `TableControlPlane`/`TableTx`. Its `begin_table()` is the
//! table-format staging seam: the same transform write code (`create_table →
//! append_files | replace_files → emit → commit`) commits its already-written
//! Parquet to Iceberg, selected at boot.
//!
//! Reads resolve through the mirror-backed [`IcebergCatalog`]; the other concerns
//! (ontology/acl/lineage/queue) are backend-neutral and delegate to the inner
//! `PgControlPlane`. `IcebergTx::commit` registers the staged files mirror-only (see
//! [`crate::iceberg_landing::register_files`]) at a freshly allocated snapshot, in one
//! Postgres transaction with the staged lineage/enqueue — the same atomicity `PgTx`
//! gives.

use std::sync::Arc;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Auth, Catalog, ColumnSpec, ControlPlane, ControlPlaneError, DataFile, JobId, Lineage,
    LineageEvent, NewJob, Ontology, Queue, Result, SnapshotId, TableControlPlane, TableRef,
    TableTx, Transforms, Tx,
};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::iceberg_catalog::IcebergCatalog;
use crate::iceberg_landing::{WriteMode, ensure_iceberg_table, register_files};
use crate::iceberg_mirror::next_snapshot;
use crate::iceberg_sql_catalog::SqlCatalog;
use crate::lineage::pg_emit;
use crate::queue::pg_insert;
use crate::{PgControlPlane, backend};

/// A `ControlPlane` whose `begin()` writes to Iceberg. `catalog()` is the mirror-backed
/// read surface; the other concerns delegate to the inner `PgControlPlane`
/// (lineage/acl/ontology/queue share the same Postgres tables regardless of table format).
pub struct IcebergControlPlane {
    pg: PgControlPlane,
    ice: IcebergCatalog,
    catalog: Arc<SqlCatalog>,
    pool: PgPool,
}

impl IcebergControlPlane {
    /// Build over an existing `PgControlPlane` (its pool is reused for reads + writes)
    /// and the vendored Iceberg `SqlCatalog` (for table creation at commit) —
    /// owned or already-shared (`Arc`), e.g. the engine's shared catalog.
    pub fn new(pg: PgControlPlane, catalog: impl Into<Arc<SqlCatalog>>) -> Self {
        let pool = pg.pool().clone();
        let ice = IcebergCatalog::new(pool.clone());
        Self {
            pg,
            ice,
            catalog: catalog.into(),
            pool,
        }
    }
}

#[async_trait]
impl ControlPlane for IcebergControlPlane {
    fn catalog(&self) -> &(dyn Catalog + Send + Sync) {
        &self.ice
    }
    fn ontology(&self) -> &(dyn Ontology + Send + Sync) {
        self.pg.ontology()
    }
    fn acl(&self) -> &(dyn Acl + Send + Sync) {
        self.pg.acl()
    }
    fn lineage(&self) -> &(dyn Lineage + Send + Sync) {
        self.pg.lineage()
    }
    fn queue(&self) -> &(dyn Queue + Send + Sync) {
        self.pg.queue()
    }
    fn transforms(&self) -> &(dyn Transforms + Send + Sync) {
        self.pg.transforms()
    }
    fn auth(&self) -> &(dyn Auth + Send + Sync) {
        self.pg.auth()
    }
    async fn begin(&self) -> Result<Box<dyn Tx + Send>> {
        Ok(self.begin_table().await?)
    }
}

#[async_trait]
impl TableControlPlane for IcebergControlPlane {
    async fn begin_table(&self) -> Result<Box<dyn TableTx + Send>> {
        let tx = self.pool.begin().await.map_err(backend)?;
        Ok(Box::new(IcebergTx {
            tx,
            catalog: self.catalog.clone(),
            staged_creates: Vec::new(),
            staged_files: Vec::new(),
            staged_compacts: Vec::new(),
            staged_run_success: None,
        }))
    }
}

/// The Iceberg unit of work. `enqueue`/`emit` apply immediately on the held tx (as
/// `PgTx` does); `create_table`/`append_files`/`replace_files` are staged and applied
/// at `commit`, which allocates one snapshot and registers the files mirror-only.
pub struct IcebergTx {
    tx: Transaction<'static, Postgres>,
    catalog: Arc<SqlCatalog>,
    staged_creates: Vec<(TableRef, Vec<ColumnSpec>)>,
    staged_files: Vec<(TableRef, Vec<DataFile>, WriteMode)>,
    staged_compacts: Vec<(TableRef, Vec<String>, Vec<DataFile>)>,
    staged_run_success: Option<Uuid>,
}

#[async_trait]
impl Tx for IcebergTx {
    async fn commit(self: Box<Self>) -> Result<Option<SnapshotId>> {
        // Destructure into owned locals so the staged-create lookup (`&staged_creates`)
        // and the `&mut tx` register borrow touch disjoint values, not overlapping
        // borrows of `self`.
        let IcebergTx {
            mut tx,
            catalog,
            staged_creates,
            staged_files,
            staged_compacts,
            staged_run_success,
        } = *self;

        if staged_creates.is_empty() && staged_files.is_empty() && staged_compacts.is_empty() {
            if staged_run_success.is_some() {
                return Err(ControlPlaneError::Validation(
                    "mark_run_succeeded staged without a snapshot-producing write".into(),
                ));
            }
            // Only lineage/enqueue (already applied on the held tx) — commit them.
            tx.commit().await.map_err(backend)?;
            return Ok(None);
        }
        // 1. Ensure the real Iceberg table(s) exist — idempotent, on the catalog's own
        //    connection (the same pre-tx create pattern `append_parquet_snapshot` uses;
        //    a bare empty table is the only artifact if the held tx later rolls back).
        for (table, cols) in &staged_creates {
            // Transform-committed tables don't carry stream framing (out of this
            // slice's scope — Plan 1b Task 5 is the direct-write parity task).
            ensure_iceberg_table(&catalog, table, cols, false).await?;
        }
        // 2. One snapshot for this unit of work, allocated in the held tx.
        let at = next_snapshot(&mut tx, None).await?;
        // 3. Register staged files at `at`, in the held tx (mirror-only). `run.rs`
        //    always `create_table`s the output before registering its files, so the
        //    column lookup is present; error loudly otherwise (never hit by `run.rs`).
        for (table, files, mode) in &staged_files {
            let cols = staged_creates
                .iter()
                .find(|(t, _)| t == table)
                .map(|(_, c)| c.as_slice())
                .ok_or_else(|| {
                    ControlPlaneError::Backend(
                        format!(
                            "IcebergTx: files staged for {table:?} without a preceding create_table"
                        )
                        .into(),
                    )
                })?;
            register_files(&mut tx, table, cols, files, mode.clone(), at).await?;
        }
        // 4. Register staged compactions — columns unused by the Compact arm, so &[].
        for (table, expire, write) in &staged_compacts {
            register_files(
                &mut tx,
                table,
                &[],
                write,
                WriteMode::Compact {
                    expire_paths: expire.clone(),
                },
                at,
            )
            .await?;
        }
        if let Some(rid) = staged_run_success {
            crate::transforms::pg_mark_run_succeeded(&mut *tx, rid, at.0).await?;
        }
        // Data triggers (slice 3): fire for the tables this commit wrote new
        // data into. Compaction-only commits rewrite existing data and fire
        // nothing. `staged_run_success` is the committing run — its own
        // transform is suppressed inside the hook.
        let mut written: Vec<TableRef> = staged_files.iter().map(|(t, _, _)| t.clone()).collect();
        written.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
        written.dedup();
        crate::transforms::pg_fire_data_triggers(&mut tx, &written, staged_run_success).await?;
        tx.commit().await.map_err(backend)?;
        Ok(Some(at))
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

#[async_trait]
impl TableTx for IcebergTx {
    async fn create_table(&mut self, table: &TableRef, columns: &[ColumnSpec]) -> Result<()> {
        self.staged_creates.push((table.clone(), columns.to_vec()));
        Ok(())
    }

    async fn append_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()> {
        self.staged_files
            .push((table.clone(), files.to_vec(), WriteMode::Append));
        Ok(())
    }

    async fn replace_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()> {
        self.staged_files
            .push((table.clone(), files.to_vec(), WriteMode::Overwrite));
        Ok(())
    }

    async fn compact_files(
        &mut self,
        table: &TableRef,
        expire: &[String],
        write: &[DataFile],
    ) -> Result<()> {
        self.staged_compacts
            .push((table.clone(), expire.to_vec(), write.to_vec()));
        Ok(())
    }

    async fn mark_run_succeeded(&mut self, run_id: Uuid) -> Result<()> {
        if self.staged_run_success.is_some() {
            return Err(ControlPlaneError::Validation(
                "a run success mark is already staged on this transaction".into(),
            ));
        }
        self.staged_run_success = Some(run_id);
        Ok(())
    }
}
