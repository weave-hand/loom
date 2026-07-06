//! The catalog concern: a read-only view over the active table-format catalog. The
//! table-format adapter (Iceberg) populates it; loom reads it. Snapshots are
//! catalog-global and identified by a monotonic id; tables/files/columns are
//! versioned by `begin`/`end` snapshot ranges (MVCC), so reads are "this table
//! *at* that snapshot".

use async_trait::async_trait;
use time::OffsetDateTime;

use crate::error::Result;
use crate::page::{Page, PageReq};

/// A catalog-global snapshot/version id (monotonic). Portable across table formats
/// (Iceberg, Delta all key versions by i64).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SnapshotId(pub i64);

/// A `schema.table` reference within the table-format catalog.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TableRef {
    pub schema: String,
    pub name: String,
}

/// A point-in-time version of the catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub id: SnapshotId,
    pub time: OffsetDateTime,
    pub schema_version: i64,
}

/// A Parquet file backing a table at a snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRef {
    pub path: String,
    pub record_count: i64,
    pub file_size_bytes: i64,
}

/// The subset of `files` smaller than `threshold_bytes` — the compaction candidate set.
/// Pure — no I/O — so it is unit-testable without a catalog.
pub fn small_files(files: &[FileRef], threshold_bytes: i64) -> Vec<&FileRef> {
    files
        .iter()
        .filter(|f| f.file_size_bytes < threshold_bytes)
        .collect()
}

/// One column of a table's schema at a snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    pub order: i64,
    pub name: String,
    /// The column's loom LOGICAL type name (the adapter maps from its physical
    /// catalog type on read). Typing is an ontology concern.
    pub ty: String,
    pub nullable: bool,
}

/// A table's column schema at a snapshot, in column order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchema {
    pub columns: Vec<ColumnDef>,
}

#[async_trait]
pub trait Catalog {
    /// The latest snapshot at which `table` is live. `NotFound` if the table does
    /// not exist at the catalog's current snapshot.
    async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot>;
    /// The latest snapshot at or before `ts` at which `table` is live, or `None`
    /// if the table has no live snapshot at/before that instant (created later, or
    /// never existed). Time-travel resolution for a wall-clock `as_of` read.
    async fn snapshot_as_of(
        &self,
        table: &TableRef,
        ts: OffsetDateTime,
    ) -> Result<Option<Snapshot>>;
    /// All snapshots at which `table` is live, oldest first. `NotFound` if the
    /// table never existed. The `page` request is accepted but not yet enforced;
    /// results are a single full page.
    async fn snapshots(&self, table: &TableRef, page: PageReq) -> Result<Page<Snapshot>>;
    /// The Parquet files live for `table` at snapshot `at`. `NotFound` if the
    /// table is not live at `at`. The `page` request is accepted but not yet enforced;
    /// results are a single full page.
    async fn files(&self, table: &TableRef, at: SnapshotId, page: PageReq)
    -> Result<Page<FileRef>>;
    /// `table`'s column schema at snapshot `at`, in column order. `NotFound` if
    /// the table is not live at `at`.
    async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema>;
    /// Every table currently live in the mirror (no end-cap), `(schema, name)`-ordered.
    /// The `page` request is accepted but not yet enforced; results are a single
    /// full page.
    async fn list_tables(&self, page: PageReq) -> Result<Page<TableRef>>;
}
