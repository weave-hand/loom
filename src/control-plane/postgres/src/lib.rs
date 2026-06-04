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

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    ControlPlane, ControlPlaneError, Job, JobId, NewJob, Queue, Result, RetryPolicy, Tx,
};
use sqlx::{PgPool, Postgres, Row as _};
use time::OffsetDateTime;
use uuid::Uuid;

pub mod fixture;

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

async fn pg_insert<'e, E: sqlx::PgExecutor<'e>>(ex: E, job: &NewJob) -> Result<JobId> {
    let id = Uuid::new_v4();
    sqlx::query(
        "insert into queue.jobs (id, kind, payload, state, run_at, priority) \
         values ($1, $2, $3, 'available', coalesce($4, now()), $5)",
    )
    .bind(id)
    .bind(&job.kind)
    .bind(&job.payload)
    .bind(job.run_at)
    .bind(job.priority)
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(JobId(id))
}

fn row_to_job(row: &sqlx::postgres::PgRow) -> Job {
    Job {
        id: JobId(row.get("id")),
        kind: row.get("kind"),
        payload: row.get("payload"),
        attempts: row.get("attempts"),
        run_at: row.get("run_at"),
    }
}

#[async_trait]
impl Queue for PgControlPlane {
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        pg_insert(&self.pool, &job).await
    }

    async fn dequeue(&self, kinds: &[String], worker: &str) -> Result<Option<Job>> {
        let cutoff = OffsetDateTime::now_utc() - self.lock_timeout;
        let row = sqlx::query(
            "update queue.jobs set state='running', locked_at=now(), locked_by=$1, \
                 attempts=attempts+1, updated_at=now() \
             where id = ( \
                 select id from queue.jobs \
                 where kind = any($2) and run_at <= now() \
                   and (state='available' or (state='running' and locked_at < $3)) \
                 order by priority desc, run_at asc \
                 for update skip locked limit 1) \
             returning id, kind, payload, attempts, run_at",
        )
        .bind(worker)
        .bind(kinds)
        .bind(cutoff)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        Ok(row.as_ref().map(row_to_job))
    }

    async fn complete(&self, id: JobId) -> Result<()> {
        sqlx::query("delete from queue.jobs where id = $1")
            .bind(id.0)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn fail(&self, id: JobId, error: &str, policy: RetryPolicy) -> Result<()> {
        match policy {
            RetryPolicy::Retry { delay } => {
                let run_at = OffsetDateTime::now_utc() + delay;
                sqlx::query(
                    "update queue.jobs set state='available', run_at=$2, locked_at=null, \
                         locked_by=null, last_error=$3, updated_at=now() where id=$1",
                )
                .bind(id.0)
                .bind(run_at)
                .bind(error)
                .execute(&self.pool)
                .await
                .map_err(backend)?;
            }
            RetryPolicy::Abandon => {
                sqlx::query(
                    "update queue.jobs set state='failed', locked_at=null, locked_by=null, \
                         last_error=$2, updated_at=now() where id=$1",
                )
                .bind(id.0)
                .bind(error)
                .execute(&self.pool)
                .await
                .map_err(backend)?;
            }
        }
        Ok(())
    }

    async fn heartbeat(&self, id: JobId) -> Result<()> {
        sqlx::query("update queue.jobs set locked_at=now() where id=$1")
            .bind(id.0)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
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
