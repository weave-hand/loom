//! The catalog concern: a read-only view over DuckLake's catalog (`ducklake.*`).
//! loom reads the catalog; DuckLake (the DuckDB client) writes it. Snapshots are
//! catalog-global and identified by a monotonic id; tables/files/columns are
//! versioned by `begin`/`end` snapshot ranges (MVCC), so reads are "this table
//! *at* that snapshot".

use async_trait::async_trait;
use time::OffsetDateTime;

use crate::error::Result;

/// A DuckLake catalog-global snapshot id (monotonic).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SnapshotId(pub i64);

/// A `schema.table` reference within the DuckLake catalog.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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

/// One column of a table's schema at a snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    pub order: i64,
    pub name: String,
    /// DuckLake's column type string, kept opaque (typing is an ontology concern).
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
    /// All snapshots at which `table` is live, oldest first. `NotFound` if the
    /// table never existed.
    async fn snapshots(&self, table: &TableRef) -> Result<Vec<Snapshot>>;
    /// The Parquet files live for `table` at snapshot `at`. `NotFound` if the
    /// table is not live at `at`.
    async fn files(&self, table: &TableRef, at: SnapshotId) -> Result<Vec<FileRef>>;
    /// `table`'s column schema at snapshot `at`, in column order. `NotFound` if
    /// the table is not live at `at`.
    async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema>;
}
