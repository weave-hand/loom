use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{Job, JobId, NewJob, Queue, Result, RetryPolicy};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{PgControlPlane, backend};

pub(crate) async fn pg_insert<'e, E: sqlx::PgExecutor<'e>>(ex: E, job: &NewJob) -> Result<JobId> {
    let id = Uuid::new_v4();
    // Insert and fire the wakeup in a single statement: pg_notify inside a
    // transaction is buffered until commit, so a rolled-back enqueue is silent.
    // Channel is per-kind so workers only wake for kinds they handle.
    sqlx::query!(
        "with ins as ( \
             insert into queue.jobs (id, kind, payload, state, run_at, priority) \
             values ($1, $2, $3, 'available', coalesce($4, now()), $5) \
             returning kind) \
         select pg_notify('loom_queue:' || kind, '') from ins",
        id,
        &job.kind,
        &job.payload,
        job.run_at,
        job.priority,
    )
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(JobId(id))
}

#[async_trait]
impl Queue for PgControlPlane {
    #[tracing::instrument(skip(self, job), level = "debug")]
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        pg_insert(&self.pool, &job).await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn dequeue(&self, kinds: &[String], worker: &str) -> Result<Option<Job>> {
        let cutoff = OffsetDateTime::now_utc() - self.lock_timeout;
        let row = sqlx::query!(
            "update queue.jobs set state='running', locked_at=now(), locked_by=$1, \
                 attempts=attempts+1, updated_at=now() \
             where id = ( \
                 select id from queue.jobs \
                 where kind = any($2) and run_at <= now() \
                   and (state='available' or (state='running' and locked_at < $3)) \
                 order by priority desc, run_at asc \
                 for update skip locked limit 1) \
             returning id, kind, payload, attempts, run_at",
            worker,
            kinds,
            cutoff,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;
        Ok(row.map(|r| Job {
            id: JobId(r.id),
            kind: r.kind,
            payload: r.payload,
            attempts: r.attempts,
            run_at: r.run_at,
        }))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn complete(&self, id: JobId) -> Result<()> {
        sqlx::query!("delete from queue.jobs where id = $1", id.0)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn fail(&self, id: JobId, error: &str, policy: RetryPolicy) -> Result<()> {
        match policy {
            RetryPolicy::Retry { delay } => {
                let run_at = OffsetDateTime::now_utc() + delay;
                sqlx::query!(
                    "update queue.jobs set state='available', run_at=$2, locked_at=null, \
                         locked_by=null, last_error=$3, updated_at=now() where id=$1",
                    id.0,
                    run_at,
                    error,
                )
                .execute(&self.pool)
                .await
                .map_err(backend)?;
            }
            RetryPolicy::Abandon => {
                sqlx::query!(
                    "update queue.jobs set state='failed', locked_at=null, locked_by=null, \
                         last_error=$2, updated_at=now() where id=$1",
                    id.0,
                    error,
                )
                .execute(&self.pool)
                .await
                .map_err(backend)?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn heartbeat(&self, id: JobId) -> Result<()> {
        sqlx::query!("update queue.jobs set locked_at=now() where id=$1", id.0)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        Ok(())
    }

    async fn await_jobs(&self, kinds: &[String], timeout: Duration) -> Result<()> {
        let mut listener = sqlx::postgres::PgListener::connect_with(&self.pool)
            .await
            .map_err(backend)?;
        let channels: Vec<String> = kinds.iter().map(|k| format!("loom_queue:{k}")).collect();
        listener
            .listen_all(channels.iter().map(String::as_str))
            .await
            .map_err(backend)?;
        // A notification, or the polling-fallback timeout — whichever first.
        drop(tokio::time::timeout(timeout, listener.recv()).await);
        Ok(())
    }
}
