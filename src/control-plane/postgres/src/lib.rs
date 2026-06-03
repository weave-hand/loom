//! Postgres adapter for the control-plane traits, backed by sqlx.
//!
//! `PgControlPlane` wraps a sqlx `PgPool`; `begin()` opens a real Postgres
//! transaction, so the same `tx_contract` suite that the in-memory fake passes is
//! validated against actual Postgres. The [`fixture`] module boots an ephemeral,
//! hermetic Postgres for tests.
//!
//! SQL is issued through sqlx's runtime query API (no compile-time `query!`
//! macros / `.sqlx` offline metadata yet — those arrive in Phase 1 with a real
//! schema). The `_probe` table the `probe_*` ops touch is created by the test
//! fixture and, like the probe ops themselves, is removed once Phase 1 puts real
//! operations on `Tx`.

use async_trait::async_trait;
use control_plane_core::{ControlPlane, ControlPlaneError, Result, Tx};
use sqlx::{PgPool, Postgres};

pub mod fixture;

/// Postgres-backed control plane over a sqlx connection pool.
#[derive(Clone)]
pub struct PgControlPlane {
    pool: PgPool,
}

impl PgControlPlane {
    /// Wrap an existing connection pool. Callers own pool setup (the test fixture
    /// builds one per fresh database; services will build one at startup).
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ControlPlane for PgControlPlane {
    async fn begin(&self) -> Result<Box<dyn Tx + Send>> {
        let tx = self.pool.begin().await.map_err(backend)?;
        Ok(Box::new(PgTx { tx }))
    }
}

struct PgTx {
    tx: sqlx::Transaction<'static, Postgres>,
}

#[async_trait]
impl Tx for PgTx {
    async fn commit(self: Box<Self>) -> Result<()> {
        self.tx.commit().await.map_err(backend)
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        self.tx.rollback().await.map_err(backend)
    }

    async fn probe_put(&mut self, key: &str, val: i64) -> Result<()> {
        sqlx::query(
            "insert into _probe (k, v) values ($1, $2) \
             on conflict (k) do update set v = excluded.v",
        )
        .bind(key)
        .bind(val)
        .execute(&mut *self.tx)
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn probe_get(&mut self, key: &str) -> Result<Option<i64>> {
        let row: Option<(i64,)> = sqlx::query_as("select v from _probe where k = $1")
            .bind(key)
            .fetch_optional(&mut *self.tx)
            .await
            .map_err(backend)?;
        Ok(row.map(|(v,)| v))
    }
}

fn backend(e: sqlx::Error) -> ControlPlaneError {
    ControlPlaneError::Backend(Box::new(e))
}
