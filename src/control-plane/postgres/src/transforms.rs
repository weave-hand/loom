//! Postgres transforms concern over the `transforms` schema (migration 0031).

use async_trait::async_trait;
use control_plane_core::{
    ControlPlaneError, JobId, NewJob, Page, PageReq, Result, RunOutcome, RunState, RunTrigger,
    TableRef, TransformBody, TransformDef, TransformName, TransformRun, Transforms, TriggerNode,
    next_cron_occurrence, validate_no_trigger_cycle, validate_transform_def,
};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::queue::pg_insert;
use crate::{PgControlPlane, backend};

/// Serializes trigger-DAG validation against concurrent defines: two racing
/// `define_transform`s could each see the other absent and jointly commit a
/// cycle. Arbitrary constant, unique within loom's advisory-lock usage.
const TRANSFORM_DEFINE_LOCK: i64 = 0x6c6f_6f6d_7472; // "loomtr"

fn ser(v: &impl serde::Serialize) -> Result<serde_json::Value> {
    serde_json::to_value(v).map_err(|e| ControlPlaneError::Serialization(e.to_string()))
}

fn de_body(v: serde_json::Value) -> Result<TransformBody> {
    serde_json::from_value(v).map_err(|e| ControlPlaneError::Serialization(e.to_string()))
}

/// Resolve every typed name appearing in `bodies` to its backing table, in
/// one query. Missing names are simply absent from the map (an unresolvable
/// type cannot match a commit and forms no edge).
async fn pg_type_tables<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    bodies: &[(TransformName, TransformBody)],
) -> Result<std::collections::HashMap<String, TableRef>> {
    let mut names: Vec<String> = bodies
        .iter()
        .flat_map(|(_, b)| match b {
            TransformBody::Typed { inputs, output, .. } => inputs
                .iter()
                .chain(std::iter::once(output))
                .cloned()
                .collect::<Vec<_>>(),
            TransformBody::Physical { .. } => Vec::new(),
        })
        .collect();
    names.sort_unstable();
    names.dedup();
    if names.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let rows = sqlx::query!(
        "select name, table_schema, table_name from ontology.object_type \
         where name = any($1)",
        &names,
    )
    .fetch_all(ex)
    .await
    .map_err(backend)?;
    Ok(rows
        .into_iter()
        .map(|r| {
            (
                r.name,
                TableRef {
                    schema: r.table_schema,
                    name: r.table_name,
                },
            )
        })
        .collect())
}

/// Insert a run row on any executor — callable from inside a commit
/// transaction (the data-trigger seam) as well as `submit_run`'s own tx.
pub(crate) async fn pg_insert_run<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    run: &TransformRun,
) -> Result<()> {
    let body = ser(&run.body)?;
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
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(())
}

/// Fire data-triggered transforms for `committed` tables, inside the
/// caller's open commit transaction — the slice-3 seam. For each def with
/// `on_input_commit` whose resolved inputs intersect `committed`:
/// lock the def row (name order; serializes the debounce against concurrent
/// commits without deadlock), skip if a `Queued` run already exists
/// (at-most-one-pending; `Running` does not suppress), then insert a
/// `DataTrigger` run and its queue job atomically with the commit.
///
/// `committing_run` is the run performing this commit (self-trigger
/// suppression): its transform, if any, never re-fires from its own write.
/// Undecodable def bodies are skipped with a warning — a poisoned admin
/// artifact must not fail unrelated ingest commits. Typed names that no
/// longer resolve contribute no match.
pub(crate) async fn pg_fire_data_triggers(
    conn: &mut sqlx::PgConnection,
    committed: &[TableRef],
    committing_run: Option<Uuid>,
) -> Result<usize> {
    if committed.is_empty() {
        return Ok(0);
    }
    let rows = sqlx::query!(
        "select name, body from transforms.transform where on_input_commit order by name",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(backend)?;
    if rows.is_empty() {
        return Ok(0);
    }
    let mut bodies: Vec<(TransformName, TransformBody)> = Vec::with_capacity(rows.len());
    for r in rows {
        match de_body(r.body) {
            Ok(b) => bodies.push((TransformName(r.name), b)),
            Err(e) => {
                tracing::warn!(transform = %r.name, error = %e,
                    "data trigger: undecodable body skipped");
            }
        }
    }
    let skip: Option<String> = match committing_run {
        Some(rid) => sqlx::query_scalar!(
            "select transform from transforms.run where run_id = $1",
            rid,
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(backend)?
        .flatten(),
        None => None,
    };
    let types = pg_type_tables(&mut *conn, &bodies).await?;
    let mut fired = 0usize;
    for (name, candidate_body) in bodies {
        if skip.as_deref() == Some(name.0.as_str()) {
            continue;
        }
        let node = TriggerNode::resolve(&name, &candidate_body, &types);
        if !node.inputs.iter().any(|t| committed.contains(t)) {
            continue;
        }
        // Re-read the body under the row lock: a redefine committing between
        // the candidate query and here must not enqueue a stale body ("the
        // run freezes the def's body at enqueue" means the body live at the
        // locked instant).
        let live = sqlx::query!(
            "select body from transforms.transform where name = $1 for update",
            name.0,
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(backend)?;
        let Some(row) = live else {
            continue; // deleted since the candidate query
        };
        let body = match de_body(row.body) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(transform = %name.0, error = %e,
                    "data trigger: undecodable body skipped");
                continue;
            }
        };
        let pending = sqlx::query_scalar!(
            r#"select exists(
                   select 1 from transforms.run where transform = $1 and state = 'queued'
               ) as "pending!""#,
            name.0,
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
        if pending {
            continue;
        }
        let run_id = Uuid::new_v4();
        let run = TransformRun {
            run_id,
            transform: Some(name),
            trigger: RunTrigger::DataTrigger,
            state: RunState::Queued,
            body: body.clone(),
            queued_at: OffsetDateTime::now_utc(),
            started_at: None,
            finished_at: None,
            snapshot_id: None,
            error: None,
        };
        pg_insert_run(&mut *conn, &run).await?;
        crate::queue::pg_insert(&mut *conn, &body.to_job(run_id)).await?;
        fired += 1;
    }
    Ok(fired)
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
        let body = ser(&def.body)?;
        let next_run_at = def
            .schedule
            .as_deref()
            .map(|e| next_cron_occurrence(e, OffsetDateTime::now_utc()))
            .transpose()?;
        let mut tx = self.pool().begin().await.map_err(backend)?;
        sqlx::query!("select pg_advisory_xact_lock($1)", TRANSFORM_DEFINE_LOCK)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        if let TransformBody::Typed { inputs, output, .. } = &def.body {
            let mut names: Vec<String> = inputs.clone();
            names.push(output.clone());
            names.sort();
            names.dedup();
            let known = sqlx::query_scalar!(
                "select count(*) from ontology.object_type where name = any($1)",
                &names,
            )
            .fetch_one(&mut *tx)
            .await
            .map_err(backend)?
            .unwrap_or(0);
            if known != i64::try_from(names.len()).unwrap_or(i64::MAX) {
                return Err(ControlPlaneError::Validation(
                    "typed transform references unknown ontology types".into(),
                ));
            }
        }
        if def.on_input_commit {
            let existing = sqlx::query!(
                "select name, body from transforms.transform \
                 where on_input_commit and name <> $1",
                def.name.0,
            )
            .fetch_all(&mut *tx)
            .await
            .map_err(backend)?;
            let mut bodies: Vec<(TransformName, TransformBody)> =
                Vec::with_capacity(existing.len() + 1);
            for r in existing {
                match de_body(r.body) {
                    Ok(b) => bodies.push((TransformName(r.name), b)),
                    Err(e) => {
                        tracing::warn!(transform = %r.name, error = %e,
                            "trigger-cycle scan: undecodable body skipped");
                    }
                }
            }
            // The candidate being defined is always in-memory and decodable, so
            // the def under construction is always validated against the cycle set.
            bodies.push((def.name.clone(), def.body.clone()));
            let types = pg_type_tables(&mut *tx, &bodies).await?;
            let nodes: Vec<TriggerNode> = bodies
                .iter()
                .map(|(n, b)| TriggerNode::resolve(n, b, &types))
                .collect();
            validate_no_trigger_cycle(&nodes)?;
        }
        sqlx::query!(
            "insert into transforms.transform (name, body, schedule, on_input_commit, next_run_at) \
             values ($1, $2, $3, $4, $5) \
             on conflict (name) do update set \
                 body = excluded.body, schedule = excluded.schedule, \
                 on_input_commit = excluded.on_input_commit, \
                 next_run_at = excluded.next_run_at",
            def.name.0,
            body,
            def.schedule,
            def.on_input_commit,
            next_run_at,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
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
        let mut tx = self.pool().begin().await.map_err(backend)?;
        pg_insert_run(&mut *tx, &run).await?;
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

    #[tracing::instrument(skip(self), level = "debug")]
    async fn claim_due_schedules(
        &self,
        now: OffsetDateTime,
        limit: u32,
    ) -> Result<Vec<TransformDef>> {
        let mut tx = self.pool().begin().await.map_err(backend)?;
        let rows = sqlx::query!(
            "select name, body, schedule, on_input_commit from transforms.transform \
             where schedule is not null and next_run_at is not null and next_run_at <= $1 \
             order by next_run_at, name \
             limit $2 \
             for update skip locked",
            now,
            i64::from(limit),
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(backend)?;
        let mut claimed = Vec::with_capacity(rows.len());
        for r in rows {
            let def = TransformDef {
                name: TransformName(r.name),
                body: de_body(r.body)?,
                schedule: r.schedule,
                on_input_commit: r.on_input_commit,
            };
            let Some(expr) = def.schedule.as_deref() else {
                continue;
            };
            let next = next_cron_occurrence(expr, now)?;
            sqlx::query!(
                "update transforms.transform set next_run_at = $2 where name = $1",
                def.name.0,
                next,
            )
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
            claimed.push(def);
        }
        tx.commit().await.map_err(backend)?;
        Ok(claimed)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn next_run_at(&self, name: &TransformName) -> Result<Option<OffsetDateTime>> {
        let row = sqlx::query!(
            "select next_run_at from transforms.transform where name = $1",
            name.0,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(format!("transform {}", name.0)))?;
        Ok(row.next_run_at)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn data_triggered_defs(&self) -> Result<Vec<TransformDef>> {
        let rows = sqlx::query!(
            "select name, body, schedule, on_input_commit from transforms.transform \
             where on_input_commit order by name",
        )
        .fetch_all(self.pool())
        .await
        .map_err(backend)?;
        rows.into_iter()
            .map(|r| {
                Ok(TransformDef {
                    name: TransformName(r.name),
                    body: de_body(r.body)?,
                    schedule: r.schedule,
                    on_input_commit: r.on_input_commit,
                })
            })
            .collect()
    }
}
