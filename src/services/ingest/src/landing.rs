//! Landing backend selection + the `LandingMaterializer` port. `DuckLakeMaterializer`
//! preserves today's path; `IcebergMaterializer` forwards to the postgres-crate
//! `iceberg_landing` entrypoint. Selected in `main` by `LOOM_LANDING_BACKEND`,
//! mirroring query-api's `LOOM_SERVING_BACKEND`.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use control_plane_core::{ColumnSpec, ControlPlane, LineageEvent, SnapshotId, TableRef};
use object_store::ObjectStore;

use control_plane_postgres::iceberg_landing::land as iceberg_land;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use sqlx::PgPool;

use crate::IngestError;
use crate::materialize::land_ducklake;

/// Which table format a running ingest service lands to. Chosen once at boot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LandingBackend {
    /// DuckLake via the DataFusion Parquet write path (default; today's behaviour).
    DuckLake,
    /// Iceberg via the loom-native landing path (inline rows + real Parquet).
    Iceberg,
}

/// Parse `LOOM_LANDING_BACKEND`. Unset/empty -> DuckLake. Case-insensitive.
pub fn parse_landing_backend(v: Option<&str>) -> Result<LandingBackend, String> {
    match v.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") | Some("ducklake") => Ok(LandingBackend::DuckLake),
        Some("iceberg") => Ok(LandingBackend::Iceberg),
        Some(other) => Err(format!(
            "LOOM_LANDING_BACKEND must be 'ducklake' or 'iceberg', got {other:?}"
        )),
    }
}

/// One landing request: the model gate has already passed, `columns` is the
/// resolved physical schema, and `lineage` is built. Backends consume the fields
/// they need — DuckLake uses `schema`/`batches`; Iceberg uses `ipc_body` (the
/// postgres crate, which owns the Iceberg writer chain, re-decodes it there).
pub struct LandRequest<'a> {
    pub table: &'a TableRef,
    /// arrow schema of the decoded batches (DuckLake path).
    pub schema: Arc<Schema>,
    /// Resolved physical schema (model-supplied or inferred).
    pub columns: &'a [ColumnSpec],
    /// arrow batches (DuckLake path).
    pub batches: &'a [RecordBatch],
    /// Raw Arrow IPC body (Iceberg path; re-decoded in the postgres crate).
    pub ipc_body: &'a [u8],
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

/// Lands to DuckLake via the DataFusion Parquet write path — today's behaviour,
/// now behind the port. Uses the request's arrow-58 `schema`/`batches`.
pub struct DuckLakeMaterializer {
    pub cp: Arc<dyn ControlPlane>,
    pub store: Arc<dyn ObjectStore>,
}

#[async_trait]
impl LandingMaterializer for DuckLakeMaterializer {
    async fn land(&self, req: LandRequest<'_>) -> Result<SnapshotId, IngestError> {
        land_ducklake(
            self.cp.as_ref(),
            self.store.clone(),
            req.table,
            req.schema.clone(),
            req.columns,
            req.batches,
            req.file_prefix,
            req.lineage,
        )
        .await
    }
}

/// Lands to Iceberg via the loom-native landing path. A thin forwarder: it passes
/// the raw IPC body (re-decoded inside the postgres crate, which owns the Iceberg
/// writer chain), the resolved columns, the byte limit, and the lineage event. Small
/// requests inline (mirror-only rows); large requests write real Parquet — both
/// emit lineage atomically and return the loom mirror snapshot id.
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
            req.ipc_body,
            self.inline_byte_limit,
            self.flush_byte_threshold,
            req.lineage,
        )
        .await
        .map_err(IngestError::from)
    }
}
