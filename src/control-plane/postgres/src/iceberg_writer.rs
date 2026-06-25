//! Real Iceberg write path: drives Arrow record batches through the `iceberg`
//! writer chain to real Parquet, then commits them as a `fast_append`. The
//! commit funnels through the vendored catalog's `update_table`, which is where
//! loom projects the `iceberg_mirror.*` rows atomically (see `iceberg_sql_catalog`).
//! This module is pure write — it holds no mirror logic.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{
    Catalog, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent,
};
use parquet::file::properties::WriterProperties;

use control_plane_core::LineageEvent;

use crate::iceberg_sql_catalog::{CommitExtras, InlineEndCap, SqlCatalog};

/// Bound on commit retries after a lost pointer CAS. A conflict at the cap
/// propagates the original (still retryable-flagged) error so a higher layer
/// (e.g. the queue worker's RetryPolicy) can re-drive it.
const COMMIT_MAX_RETRIES: u32 = 5;
/// Base delay for exponential backoff between commit attempts.
const COMMIT_BACKOFF_BASE: Duration = Duration::from_millis(5);
/// Cap on the exponential backoff delay.
const COMMIT_BACKOFF_CAP: Duration = Duration::from_millis(200);

/// A neutral summary of one committed Parquet data file, returned to callers
/// (tests, the seeder) that want to assert on the write without depending on
/// the `iceberg::spec::DataFile` shape.
pub struct WrittenFile {
    /// On-storage path of the written Parquet file (may be a `file://` URL).
    pub path: String,
    /// Rows written to the file.
    pub record_count: i64,
    /// On-disk size in bytes.
    pub file_size_bytes: i64,
}

/// Write `batches` to real Parquet under `table`'s location and commit them as a
/// single `fast_append`. Returns one [`WrittenFile`] per produced data file. The
/// caller must have created `table` in `catalog` already. The mirror projection
/// happens inside `catalog.update_table` during `commit` — not here.
pub async fn append_batches(
    catalog: &dyn Catalog,
    table: &Table,
    batches: Vec<RecordBatch>,
) -> Result<Vec<WrittenFile>> {
    let data_files = write_parquet(table, batches).await?;
    let summaries: Vec<WrittenFile> = data_files
        .iter()
        .map(|df| WrittenFile {
            path: df.file_path().to_string(),
            record_count: df.record_count() as i64,
            file_size_bytes: df.file_size_in_bytes() as i64,
        })
        .collect();

    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(summaries)
}

/// Commit a `fast_append` of `data_files`, retrying on a lost pointer CAS.
///
/// A conflict means this writer's staged metadata was built against a now-stale
/// `metadata_location`, so a bare CAS replay would re-conflict forever. On
/// `CatalogCommitConflicts` we instead reload the table (picking up the winning
/// writer's new parent snapshot), re-stage the **same** already-written
/// `data_files` against it, and re-commit, with bounded exponential backoff plus
/// a per-writer jitter term so colliding writers de-synchronize. The data files
/// are written once (UUID-prefixed paths) and reused across attempts, so
/// re-adding them is correct and collision-free. Attempt 0 reuses the
/// already-loaded `table`, so the reload round-trip is paid only on the retry
/// path. Any non-conflict error, or a conflict past `COMMIT_MAX_RETRIES`,
/// propagates unchanged.
async fn commit_append_with_retry(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    mut table: Table,
    data_files: Vec<DataFile>,
) -> Result<()> {
    let mut attempt: u32 = 0;
    loop {
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(data_files.clone());
        let tx = action.apply(tx)?;
        match tx.commit(catalog).await {
            Ok(_) => return Ok(()),
            Err(e)
                if e.kind() == ErrorKind::CatalogCommitConflicts
                    && attempt < COMMIT_MAX_RETRIES =>
            {
                tokio::time::sleep(commit_backoff(ident, attempt)).await;
                table = catalog.load_table(ident).await?;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Backoff delay for a given attempt: `min(CAP, BASE * 2^attempt)` plus a small
/// jitter derived from the table identity hashed with the attempt, so one
/// writer's attempt N and N+1 do not land on the same delay. No `rand` crate —
/// the jitter is a deterministic hash, which is enough to break ties. Note the
/// jitter is the *same* across writers contending on one table (they share the
/// `TableIdent`), so de-synchronization across writers comes mainly from the
/// exponential growth and the natural spread in Postgres commit latency rather
/// than from the jitter; if high-N contention ever proves under-spread, mix a
/// per-writer entropy source into the hash here (a tracked deferred refinement,
/// not needed at the calibrated N=8).
fn commit_backoff(ident: &TableIdent, attempt: u32) -> Duration {
    let exp = COMMIT_BACKOFF_BASE
        .saturating_mul(1u32 << attempt.min(16))
        .min(COMMIT_BACKOFF_CAP);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    ident.hash(&mut hasher);
    attempt.hash(&mut hasher);
    // Jitter in [0, BASE): keep it bounded so it never dominates the delay.
    let jitter_ms = hasher.finish() % (COMMIT_BACKOFF_BASE.as_millis() as u64).max(1);
    exp + Duration::from_millis(jitter_ms)
}

/// A per-call `Catalog` decorator that carries `CommitExtras` (lineage and/or an
/// inline end-cap) into the one `update_table` the iceberg commit performs, so both
/// land in the same Postgres tx as the pointer CAS + mirror projection. Every other
/// method delegates to the inner `SqlCatalog`. Constructed fresh per append (holds
/// borrows), so there is no shared mutable state across concurrent commits.
struct CommitExtrasCatalog<'a> {
    inner: &'a SqlCatalog,
    lineage: Option<&'a LineageEvent>,
    end_cap: Option<InlineEndCap<'a>>,
    overwrite: bool,
}

impl std::fmt::Debug for CommitExtrasCatalog<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitExtrasCatalog")
            .field("lineage", &self.lineage.is_some())
            .field("end_cap", &self.end_cap.is_some())
            .field("overwrite", &self.overwrite)
            .finish()
    }
}

#[async_trait]
impl Catalog for CommitExtrasCatalog<'_> {
    /// The one method that differs: route the commit through `do_update_table`
    /// with the extras so they commit/roll back atomically with the snapshot.
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.inner
            .do_update_table(
                commit,
                CommitExtras {
                    lineage: self.lineage,
                    end_cap: self.end_cap.as_ref().map(|c| InlineEndCap {
                        table_id: c.table_id,
                        row_ids: c.row_ids,
                    }),
                    overwrite: self.overwrite,
                },
            )
            .await
    }

    // --- pure delegation below ---
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        self.inner.namespace_exists(namespace).await
    }
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        self.inner.drop_namespace(namespace).await
    }
    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.inner.create_table(namespace, creation).await
    }
    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        self.inner.load_table(table).await
    }
    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.drop_table(table).await
    }
    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        self.inner.table_exists(table).await
    }
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.inner.rename_table(src, dest).await
    }
    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }
    async fn purge_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.purge_table(table).await
    }
}

/// Append `batches` as real Parquet and commit, running `extras` (lineage and/or
/// inline end-cap) inside the one commit tx. Generalizes
/// [`append_batches_with_lineage`]. Takes a concrete `&SqlCatalog` because the
/// [`CommitExtrasCatalog`] decorator needs the inherent `do_update_table`.
pub async fn append_batches_with_extras(
    catalog: &SqlCatalog,
    table: &Table,
    batches: Vec<RecordBatch>,
    lineage: Option<&LineageEvent>,
    end_cap: Option<InlineEndCap<'_>>,
    overwrite: bool,
) -> Result<Vec<WrittenFile>> {
    let data_files = write_parquet(table, batches).await?;
    let summaries: Vec<WrittenFile> = data_files
        .iter()
        .map(|df| WrittenFile {
            path: df.file_path().to_string(),
            record_count: df.record_count() as i64,
            file_size_bytes: df.file_size_in_bytes() as i64,
        })
        .collect();

    let wrapper = CommitExtrasCatalog {
        inner: catalog,
        lineage,
        end_cap,
        overwrite,
    };
    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx)?;
    tx.commit(&wrapper).await?;
    Ok(summaries)
}

/// Like [`append_batches`], but emits `lineage` atomically with the commit: the
/// pointer CAS + mirror projection + lineage row share one Postgres transaction
/// via the [`CommitExtrasCatalog`] decorator. The lineage event is re-presented on
/// each commit-retry attempt and only persists on the winning, committed attempt (a
/// lost CAS rolls the lineage row back with the snapshot). Thin wrapper around
/// [`append_batches_with_extras`].
pub async fn append_batches_with_lineage(
    catalog: &SqlCatalog,
    table: &Table,
    batches: Vec<RecordBatch>,
    lineage: &LineageEvent,
) -> Result<Vec<WrittenFile>> {
    append_batches_with_extras(catalog, table, batches, Some(lineage), None, false).await
}

async fn write_parquet(table: &Table, batches: Vec<RecordBatch>) -> Result<Vec<DataFile>> {
    write_parquet_with_schema(table, table.metadata().current_schema().clone(), batches).await
}

/// Like [`write_parquet`], but stamps `schema` (rather than the table's current
/// schema) into the Parquet `ParquetWriterBuilder`. The additive landing path passes
/// the SUPERSET schema (incl. the newly-added columns) so the written Parquet carries
/// every column's field id, even though the real Iceberg metadata schema is not evolved.
/// `location`/`file_io` still come from the table.
pub async fn write_parquet_with_schema(
    table: &Table,
    schema: iceberg::spec::SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<Vec<DataFile>> {
    let location_generator = DefaultLocationGenerator::new(table.metadata())?;
    // Unique per-append prefix: DefaultFileNameGenerator restarts its counter at 0
    // each call, so a fixed prefix would emit the same `<prefix>-00000.parquet` path
    // for every append — and iceberg's fast_append rejects re-adding an already-
    // referenced path (whether from a prior sequential append or a concurrent writer).
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("loom-{}", uuid::Uuid::new_v4()),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_builder = ParquetWriterBuilder::new(WriterProperties::default(), schema);
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
    for batch in batches {
        writer.write(batch).await?;
    }
    writer.close().await
}
