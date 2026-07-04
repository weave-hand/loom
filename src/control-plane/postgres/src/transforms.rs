//! Postgres transforms concern over the `transforms` schema (migration 0031).

use async_trait::async_trait;
use control_plane_core::{
    ControlPlaneError, JobId, NewJob, Page, PageReq, Result, RunOutcome, RunState, RunTrigger,
    TransformBody, TransformDef, TransformName, TransformRun, Transforms, validate_transform_def,
};
use uuid::Uuid;

use crate::queue::pg_insert;
use crate::{PgControlPlane, backend};

fn ser(v: &impl serde::Serialize) -> Result<serde_json::Value> {
    serde_json::to_value(v).map_err(|e| ControlPlaneError::Serialization(e.to_string()))
}

fn de_body(v: serde_json::Value) -> Result<TransformBody> {
    serde_json::from_value(v).map_err(|e| ControlPlaneError::Serialization(e.to_string()))
}

/// Mark `run_id` succeeded at `snapshot_id` on any executor — callable from
/// inside `IcebergTx::commit`'s held transaction (Task 6).
pub(crate) async fn pg_mark_run_succeeded<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    run_id: Uuid,
    snapshot_id: i64,
) -> Result<()> {
    let res = sqlx::query!(
        "update transforms.run \
         set state = 'succeeded', snapshot_id = $2, finished_at = now() \
         where run_id = $1",
        run_id,
        snapshot_id,
    )
    .execute(ex)
    .await
    .map_err(backend)?;
    if res.rows_affected() == 0 {
        return Err(ControlPlaneError::NotFound(format!("run {run_id}")));
    }
    Ok(())
}

/// Fields shared by `get_run`/`list_runs` — one row of `transforms.run` —
/// decoded into a [`TransformRun`]. Kept as a plain struct (rather than
/// duplicating the decode inline) since each caller's `query!` row is an
/// anonymous per-macro type; constructing this from either row's fields is
/// the cheapest way to share the decode logic.
struct RunRow {
    run_id: Uuid,
    transform: Option<String>,
    trigger: String,
    state: String,
    body: serde_json::Value,
    queued_at: time::OffsetDateTime,
    started_at: Option<time::OffsetDateTime>,
    finished_at: Option<time::OffsetDateTime>,
    snapshot_id: Option<i64>,
    error: Option<String>,
}

fn decode_run(row: RunRow) -> Result<TransformRun> {
    Ok(TransformRun {
        run_id: row.run_id,
        transform: row.transform.map(TransformName),
        trigger: row.trigger.parse::<RunTrigger>()?,
        state: row.state.parse::<RunState>()?,
        body: de_body(row.body)?,
        queued_at: row.queued_at,
        started_at: row.started_at,
        finished_at: row.finished_at,
        snapshot_id: row.snapshot_id,
        error: row.error,
    })
}

#[async_trait]
impl Transforms for PgControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_transform(&self, def: TransformDef) -> Result<()> {
        validate_transform_def(&def)?;
        if let TransformBody::Typed { inputs, output, .. } = &def.body {
            let mut names: Vec<String> = inputs.clone();
            names.push(output.clone());
            names.sort();
            names.dedup();
            let known = sqlx::query_scalar!(
                "select count(*) from ontology.object_type where name = any($1)",
                &names,
            )
            .fetch_one(self.pool())
            .await
            .map_err(backend)?
            .unwrap_or(0);
            if known != i64::try_from(names.len()).unwrap_or(i64::MAX) {
                return Err(ControlPlaneError::Validation(
                    "typed transform references unknown ontology types".into(),
                ));
            }
        }
        let body = ser(&def.body)?;
        sqlx::query!(
            "insert into transforms.transform (name, body, schedule, on_input_commit) \
             values ($1, $2, $3, $4) \
             on conflict (name) do update set \
                 body = excluded.body, schedule = excluded.schedule, \
                 on_input_commit = excluded.on_input_commit",
            def.name.0,
            body,
            def.schedule,
            def.on_input_commit,
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_transform(&self, name: &TransformName) -> Result<TransformDef> {
        let row = sqlx::query!(
            "select name, body, schedule, on_input_commit \
             from transforms.transform where name = $1",
            name.0,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(format!("transform {}", name.0)))?;
        Ok(TransformDef {
            name: TransformName(row.name),
            body: de_body(row.body)?,
            schedule: row.schedule,
            on_input_commit: row.on_input_commit,
        })
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_transforms(&self, _page: PageReq) -> Result<Page<TransformDef>> {
        let rows = sqlx::query!(
            "select name, body, schedule, on_input_commit \
             from transforms.transform order by name",
        )
        .fetch_all(self.pool())
        .await
        .map_err(backend)?;
        let items = rows
            .into_iter()
            .map(|r| {
                Ok(TransformDef {
                    name: TransformName(r.name),
                    body: de_body(r.body)?,
                    schedule: r.schedule,
                    on_input_commit: r.on_input_commit,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Page::from_full(items))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn delete_transform(&self, name: &TransformName) -> Result<()> {
        sqlx::query!("delete from transforms.transform where name = $1", name.0)
            .execute(self.pool())
            .await
            .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, run, job), level = "debug")]
    async fn submit_run(&self, run: TransformRun, job: NewJob) -> Result<JobId> {
        let body = ser(&run.body)?;
        let mut tx = self.pool().begin().await.map_err(backend)?;
        sqlx::query!(
            "insert into transforms.run \
                 (run_id, transform, trigger, state, body, queued_at) \
             values ($1, $2, $3, $4, $5, $6)",
            run.run_id,
            run.transform.as_ref().map(|t| t.0.clone()),
            run.trigger.as_str(),
            run.state.as_str(),
            body,
            run.queued_at,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        let id = pg_insert(&mut *tx, &job).await?;
        tx.commit().await.map_err(backend)?;
        Ok(id)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn mark_run_running(&self, run_id: Uuid) -> Result<()> {
        let res = sqlx::query!(
            "update transforms.run set state = 'running', started_at = now() \
             where run_id = $1",
            run_id,
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        if res.rows_affected() == 0 {
            return Err(ControlPlaneError::NotFound(format!("run {run_id}")));
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn finish_run(&self, run_id: Uuid, outcome: RunOutcome) -> Result<()> {
        let res = match outcome {
            RunOutcome::Succeeded { snapshot_id } => {
                return pg_mark_run_succeeded(self.pool(), run_id, snapshot_id).await;
            }
            RunOutcome::RetryQueued { error } => sqlx::query!(
                "update transforms.run set state = 'queued', error = $2 where run_id = $1",
                run_id,
                error,
            )
            .execute(self.pool())
            .await
            .map_err(backend)?,
            RunOutcome::Failed { error } => sqlx::query!(
                "update transforms.run \
                 set state = 'failed', error = $2, finished_at = now() where run_id = $1",
                run_id,
                error,
            )
            .execute(self.pool())
            .await
            .map_err(backend)?,
        };
        if res.rows_affected() == 0 {
            return Err(ControlPlaneError::NotFound(format!("run {run_id}")));
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_run(&self, run_id: Uuid) -> Result<TransformRun> {
        let r = sqlx::query!(
            "select run_id, transform, trigger, state, body, queued_at, \
                    started_at, finished_at, snapshot_id, error \
             from transforms.run where run_id = $1",
            run_id,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(format!("run {run_id}")))?;
        decode_run(RunRow {
            run_id: r.run_id,
            transform: r.transform,
            trigger: r.trigger,
            state: r.state,
            body: r.body,
            queued_at: r.queued_at,
            started_at: r.started_at,
            finished_at: r.finished_at,
            snapshot_id: r.snapshot_id,
            error: r.error,
        })
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_runs(
        &self,
        transform: Option<&TransformName>,
        _page: PageReq,
    ) -> Result<Page<TransformRun>> {
        let rows = sqlx::query!(
            "select run_id, transform, trigger, state, body, queued_at, \
                    started_at, finished_at, snapshot_id, error \
             from transforms.run \
             where ($1::text is null or transform = $1) \
             order by queued_at desc, run_id desc",
            transform.map(|t| t.0.clone()),
        )
        .fetch_all(self.pool())
        .await
        .map_err(backend)?;
        let items = rows
            .into_iter()
            .map(|r| {
                decode_run(RunRow {
                    run_id: r.run_id,
                    transform: r.transform,
                    trigger: r.trigger,
                    state: r.state,
                    body: r.body,
                    queued_at: r.queued_at,
                    started_at: r.started_at,
                    finished_at: r.finished_at,
                    snapshot_id: r.snapshot_id,
                    error: r.error,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Page::from_full(items))
    }
}
