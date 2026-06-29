//! The engine-side governed-write executor (relocated from query-api). It receives
//! a pre-authorized write: the caller (query-api) has already enforced ACL, built
//! the typed Arrow batch, and IPC-encoded it. This executor lands or overwrites it,
//! committing the row(s) and lineage atomically via `iceberg_landing`. It is
//! governance-free — exactly mirroring the read path.

use std::sync::Arc;

use arrow::array::RecordBatch;
use control_plane_core::{ColumnSpec, LineageEvent, SnapshotId, TableRef};
use control_plane_postgres::iceberg_landing;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use sqlx::PgPool;

use crate::serving::EngineServingError;

/// Decode an Arrow IPC stream body into its record batches. An empty body yields
/// an empty vector (the truncate / delete-all signal for overwrite).
fn decode_ipc(ipc: &[u8]) -> Result<Vec<RecordBatch>, EngineServingError> {
    if ipc.is_empty() {
        return Ok(Vec::new());
    }
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc), None)
        .map_err(|e| EngineServingError::Engine(e.to_string()))?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| EngineServingError::Engine(e.to_string()))
}

/// The relocated `ActionEngine` executor. Holds the same dependencies the old
/// query-api writer held: an Iceberg `SqlCatalog`, a `PgPool`, and the inline/flush
/// byte routing knobs.
pub struct IcebergActionWriter {
    catalog: Arc<SqlCatalog>,
    pool: PgPool,
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
}

impl IcebergActionWriter {
    #[must_use]
    pub fn new(
        catalog: Arc<SqlCatalog>,
        pool: PgPool,
        inline_byte_limit: usize,
        flush_byte_threshold: i64,
    ) -> Self {
        Self {
            catalog,
            pool,
            inline_byte_limit,
            flush_byte_threshold,
        }
    }

    /// Governed typed-insert: land one IPC-encoded row + its lineage atomically.
    pub async fn write_object(
        &self,
        table: &TableRef,
        columns: &[ColumnSpec],
        ipc: &[u8],
        event: LineageEvent,
    ) -> Result<SnapshotId, EngineServingError> {
        iceberg_landing::land(
            &self.pool,
            &self.catalog,
            table,
            columns,
            ipc,
            self.inline_byte_limit,
            self.flush_byte_threshold,
            event,
        )
        .await
        .map_err(|e| EngineServingError::Engine(e.to_string()))
    }

    /// Copy-on-write overwrite (UPDATE/DELETE): replace the table's entire live
    /// contents with the decoded batch(es), committing `event` atomically. An empty
    /// `ipc` truncates the table (delete-all).
    pub async fn overwrite_table(
        &self,
        table: &TableRef,
        columns: &[ColumnSpec],
        ipc: &[u8],
        event: LineageEvent,
    ) -> Result<SnapshotId, EngineServingError> {
        let batches = decode_ipc(ipc)?;
        iceberg_landing::overwrite_parquet_snapshot(
            &self.pool,
            &self.catalog,
            table,
            columns,
            batches,
            Some(&event),
        )
        .await
        .map_err(|e| EngineServingError::Engine(e.to_string()))
    }
}
