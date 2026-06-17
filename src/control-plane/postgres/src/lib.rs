//! Postgres adapter for the control-plane traits, backed by sqlx.
//!
//! `PgControlPlane` wraps a sqlx `PgPool`; `begin()` opens a real Postgres
//! transaction. The [`fixture`] module boots an ephemeral, hermetic Postgres for
//! tests.
//!
//! The `queue` concern uses compile-time `query!` macros validated against the
//! committed `.sqlx/` offline cache (regenerate with `tools/sqlx-prepare.sh`);
//! the remaining concerns still use sqlx's runtime query API.

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, Cardinality, Catalog, ControlPlane, ControlPlaneError, Effect, EventType, Lineage,
    Ontology, PolicyTarget, Queue, Result, Tx,
};
use sqlx::PgPool;

pub mod fixture;

mod acl;
mod catalog;
pub mod ducklake_type;
pub mod iceberg_sql_catalog;
pub mod iceberg_type;
mod lineage;
mod ontology;
mod queue;
mod snapshot;
mod transaction;

use transaction::PgTx;

/// Postgres-backed control plane over a sqlx connection pool.
#[derive(Clone)]
pub struct PgControlPlane {
    pool: PgPool,
    lock_timeout: Duration,
}

impl PgControlPlane {
    /// Wrap an existing connection pool. Callers own pool setup (the test fixture
    /// builds one per fresh database; services will build one at startup).
    pub fn new(pool: PgPool, lock_timeout: Duration) -> Self {
        Self { pool, lock_timeout }
    }
}

/// Apply pending migrations from `migrations_dir` (tracked in `_sqlx_migrations`).
pub async fn run_migrations(pool: &PgPool, migrations_dir: &Path) -> Result<()> {
    let migrator = sqlx::migrate::Migrator::new(migrations_dir)
        .await
        .map_err(|e| ControlPlaneError::Backend(Box::new(e)))?;
    migrator
        .run(pool)
        .await
        .map_err(|e| ControlPlaneError::Backend(Box::new(e)))?;
    Ok(())
}

#[async_trait]
impl ControlPlane for PgControlPlane {
    fn catalog(&self) -> &(dyn Catalog + Send + Sync) {
        self
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
        let tx = self.pool.begin().await.map_err(backend)?;
        Ok(Box::new(PgTx {
            tx,
            staged_tables: Vec::new(),
            staged_files: Vec::new(),
        }))
    }
}

fn backend(e: sqlx::Error) -> ControlPlaneError {
    ControlPlaneError::Backend(Box::new(e))
}

fn cardinality_to_str(c: Cardinality) -> &'static str {
    match c {
        Cardinality::One => "one",
        Cardinality::Many => "many",
    }
}

fn cardinality_from_str(s: &str) -> Cardinality {
    match s {
        "many" => Cardinality::Many,
        _ => Cardinality::One,
    }
}

fn event_type_to_str(t: EventType) -> &'static str {
    match t {
        EventType::Start => "start",
        EventType::Running => "running",
        EventType::Complete => "complete",
        EventType::Abort => "abort",
        EventType::Fail => "fail",
    }
}

fn event_type_from_str(s: &str) -> EventType {
    match s {
        "running" => EventType::Running,
        "complete" => EventType::Complete,
        "abort" => EventType::Abort,
        "fail" => EventType::Fail,
        _ => EventType::Start,
    }
}

fn action_to_str(a: Action) -> &'static str {
    match a {
        Action::Read => "read",
        Action::Write => "write",
    }
}

fn effect_to_str(effect: Effect) -> &'static str {
    match effect {
        Effect::Allow => "allow",
        Effect::Deny => "deny",
    }
}

/// `(kind, a, b)` column encoding of a target.
fn target_cols(t: &PolicyTarget) -> (&'static str, String, String) {
    match t {
        PolicyTarget::Type(n) => ("type", n.0.clone(), String::new()),
        PolicyTarget::Table(r) => ("table", r.schema.clone(), r.name.clone()),
    }
}
