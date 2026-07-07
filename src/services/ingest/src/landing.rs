//! The `LandingMaterializer` port. `IcebergMaterializer` forwards to the
//! postgres-crate `iceberg_landing` entrypoint — the one landing backend.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use control_plane_core::{ColumnSpec, LineageEvent, SnapshotId, TableRef};

use control_plane_postgres::iceberg_landing::{InlineLimits, land as iceberg_land};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use sqlx::PgPool;

use crate::IngestError;

/// One landing request: the model gate has already passed, `columns` is the
/// resolved physical schema, and `lineage` is built. The caller has already
/// decoded the Arrow IPC body (via `datafusion_io::decode_ipc`); `schema`/
/// `batches` are forwarded to the postgres-crate landing entrypoint as-is.
pub struct LandRequest<'a> {
    pub table: &'a TableRef,
    /// arrow schema of the decoded batches.
    pub schema: Arc<Schema>,
    /// Resolved physical schema (model-supplied or inferred).
    pub columns: &'a [ColumnSpec],
    /// arrow batches.
    pub batches: &'a [RecordBatch],
    /// Caller-unique prefix (subdirectory) for this call's files, e.g. a run id.
    pub file_prefix: &'a str,
    pub lineage: LineageEvent,
}

/// The landing port: land one request and return the new snapshot id. One impl
/// per table format, chosen at boot and injected into the HTTP `AppState`.
#[async_trait]
pub trait LandingMaterializer: Send + Sync {
    async fn land(&self, req: LandRequest<'_>) -> Result<SnapshotId, IngestError>;
}

/// Lands to Iceberg via the loom-native landing path. A thin forwarder: it passes
/// the pre-decoded schema/batches, the resolved columns, the byte limit, and the
/// lineage event. Small requests inline (mirror-only rows); large requests write
/// real Parquet — both emit lineage atomically and return the loom mirror
/// snapshot id.
pub struct IcebergMaterializer {
    pub catalog: Arc<SqlCatalog>,
    pub pool: PgPool,
    pub inline_byte_limit: usize,
    /// Live-inline-byte total at/above which a flush_table job is enqueued.
    pub flush_byte_threshold: i64,
}

#[async_trait]
impl LandingMaterializer for IcebergMaterializer {
    async fn land(&self, req: LandRequest<'_>) -> Result<SnapshotId, IngestError> {
        iceberg_land(
            &self.pool,
            &self.catalog,
            req.table,
            req.columns,
            req.schema.clone(),
            req.batches.to_vec(),
            InlineLimits {
                inline_byte_limit: self.inline_byte_limit,
                flush_byte_threshold: self.flush_byte_threshold,
            },
            req.lineage,
            // `LandRequest` gains a `stream_buckets` field in Task 4; for now this
            // path never declares stream intent.
            None,
        )
        .await
        .map_err(IngestError::from)
    }
}
