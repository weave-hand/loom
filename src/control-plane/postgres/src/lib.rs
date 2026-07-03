//! Postgres adapter for the control-plane traits, backed by sqlx.
//!
//! `PgControlPlane` wraps a sqlx `PgPool`; `begin()` opens a real Postgres
//! transaction. The [`fixture`] module boots an ephemeral, hermetic Postgres for
//! tests.
//!
//! The `queue` concern uses compile-time `query!` macros validated against the
//! committed `.sqlx/` offline cache (regenerate with `tools/sqlx-prepare.sh`);
//! the remaining concerns still use sqlx's runtime query API.

use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Catalog, ControlPlane, ControlPlaneError, Lineage, Ontology, Queue, Result, Tx,
};
use sqlx::PgPool;

pub mod fixture;

mod acl;
mod auth;
pub mod iceberg_catalog;
pub mod iceberg_compact;
pub mod iceberg_control_plane;
pub mod iceberg_flush;
pub mod iceberg_gc;
pub mod iceberg_inline;
pub mod iceberg_landing;
pub mod iceberg_mirror;
pub mod iceberg_read;
pub mod iceberg_schema_evolution;
pub mod iceberg_sql_catalog;
pub mod iceberg_stats;
pub mod iceberg_type;
pub mod iceberg_writer;
pub mod puffin;
pub mod vector_index;
pub use iceberg_read::read_files_as_batches;
mod lineage;
mod ontology;
mod queue;
mod transaction;

use iceberg_catalog::IcebergCatalog;
use transaction::PgTx;

/// Postgres-backed control plane over a sqlx connection pool.
#[derive(Clone)]
pub struct PgControlPlane {
    pool: PgPool,
    lock_timeout: Duration,
    /// Read adapter over the `iceberg_mirror.*` projection, returned by
    /// [`ControlPlane::catalog`].
    iceberg_catalog: IcebergCatalog,
}

impl PgControlPlane {
    /// Wrap an existing connection pool. Callers own pool setup (the test fixture
    /// builds one per fresh database; services will build one at startup).
    pub fn new(pool: PgPool, lock_timeout: Duration) -> Self {
        let iceberg_catalog = IcebergCatalog::new(pool.clone());
        Self {
            pool,
            lock_timeout,
            iceberg_catalog,
        }
    }

    /// The underlying connection pool. Used by the Iceberg read adapter and test
    /// seeder, which share the control plane's database.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// The control-plane migrations, baked into the binary at compile time via
/// `sqlx::migrate!` (no migrations-on-disk). The `./` prefix is required: sqlx
/// rejects a bare single-component path. The `migrations/*.sql` files are
/// declared into the compile sandbox by the `mapped_srcs` on this crate's
/// buck target, so the macro reads them hermetically (incl. on RE).
pub fn embedded_migrator() -> sqlx::migrate::Migrator {
    sqlx::migrate!("./migrations")
}

/// Apply the embedded migrations (tracked in `_sqlx_migrations`; idempotent —
/// re-runs as a no-op).
pub async fn run_embedded_migrations(pool: &PgPool) -> Result<()> {
    embedded_migrator().run(pool).await.map_err(backend)?;
    Ok(())
}

#[async_trait]
impl ControlPlane for PgControlPlane {
    fn catalog(&self) -> &(dyn Catalog + Send + Sync) {
        &self.iceberg_catalog
    }
    fn ontology(&self) -> &(dyn Ontology + Send + Sync) {
        self
    }
    fn acl(&self) -> &(dyn Acl + Send + Sync) {
        self
    }
    fn lineage(&self) -> &(dyn Lineage + Send + Sync) {
        self
    }
    fn queue(&self) -> &(dyn Queue + Send + Sync) {
        self
    }
    async fn begin(&self) -> Result<Box<dyn Tx + Send>> {
        // A plain Postgres transaction backing the transactional queue/lineage
        // concerns (`enqueue`/`emit`). The table-write methods on this `Tx` error;
        // Iceberg owns the table format.
        let tx = self.pool.begin().await.map_err(backend)?;
        Ok(Box::new(PgTx { tx }))
    }
}

/// Box a concrete error as `ControlPlaneError::Backend`, carrying the source.
/// THE single boxing helper — call sites use `map_err(backend)` (or a
/// domain-mapping helper like `auth::conflict_or_backend`); never flatten via
/// `.to_string()`, which severs the source chain. `Backend` is
/// `#[error(transparent)]`, so the Display text is the source's own.
fn backend<E: std::error::Error + Send + Sync + 'static>(e: E) -> ControlPlaneError {
    ControlPlaneError::Backend(Box::new(e))
}
