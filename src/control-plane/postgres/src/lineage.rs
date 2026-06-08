use async_trait::async_trait;
use control_plane_core::{DatasetRef, Lineage, LineageEvent, Result, RunId};
use sqlx::{AssertSqlSafe, Row as _};

use crate::{PgControlPlane, backend, event_type_from_str, event_type_to_str};

pub(crate) async fn pg_emit<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    event: &LineageEvent,
) -> Result<()> {
    // One round-trip: insert the event, then its input/output rows via unnest.
    // Ordinals come from WITH ORDINALITY (1-based; the read path orders by
    // `ordinal`, so the absolute base is irrelevant).
    sqlx::query(AssertSqlSafe(
        "with e as ( \
             insert into lineage.event (run_id, event_type, event_time, payload) \
             values ($1, $2, $3, $4) returning event_id) \
         insert into lineage.event_dataset (event_id, direction, ordinal, namespace, name) \
         select e.event_id, d.direction, d.ordinal, d.namespace, d.name \
         from e, ( \
             select 'input' as direction, ord as ordinal, ns as namespace, nm as name \
             from unnest($5::text[], $6::text[]) with ordinality as t(ns, nm, ord) \
             union all \
             select 'output', ord, ns, nm \
             from unnest($7::text[], $8::text[]) with ordinality as t(ns, nm, ord)) d",
    ))
    .bind(event.run_id.0)
    .bind(event_type_to_str(event.event_type))
    .bind(event.event_time)
    .bind(&event.payload)
    .bind(
        event
            .inputs
            .iter()
            .map(|d| d.namespace.clone())
            .collect::<Vec<_>>(),
    )
    .bind(
        event
            .inputs
            .iter()
            .map(|d| d.name.clone())
            .collect::<Vec<_>>(),
    )
    .bind(
        event
            .outputs
            .iter()
            .map(|d| d.namespace.clone())
            .collect::<Vec<_>>(),
    )
    .bind(
        event
            .outputs
            .iter()
            .map(|d| d.name.clone())
            .collect::<Vec<_>>(),
    )
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(())
}

#[async_trait]
impl Lineage for PgControlPlane {
    #[tracing::instrument(skip(self, event), fields(run_id = ?event.run_id, event_type = ?event.event_type), level = "debug")]
    async fn emit(&self, event: LineageEvent) -> Result<()> {
        pg_emit(&self.pool, &event).await
    }

    async fn events_for(&self, run: &RunId) -> Result<Vec<LineageEvent>> {
        let rows = sqlx::query(AssertSqlSafe(
            "select event_id, event_type, event_time, payload from lineage.event \
             where run_id = $1 order by event_id",
        ))
        .bind(run.0)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let event_id: i64 = r.get("event_id");
            out.push(LineageEvent {
                run_id: *run,
                event_type: event_type_from_str(r.get::<String, _>("event_type").as_str()),
                event_time: r.get("event_time"),
                inputs: self.event_datasets(event_id, "input").await?,
                outputs: self.event_datasets(event_id, "output").await?,
                payload: r.get("payload"),
            });
        }
        Ok(out)
    }

    async fn upstream(&self, dataset: &DatasetRef) -> Result<Vec<DatasetRef>> {
        self.graph_step(dataset, "output", "input").await
    }

    async fn downstream(&self, dataset: &DatasetRef) -> Result<Vec<DatasetRef>> {
        self.graph_step(dataset, "input", "output").await
    }
}

impl PgControlPlane {
    /// The datasets of one event in one direction, ordered by ordinal.
    async fn event_datasets(&self, event_id: i64, direction: &str) -> Result<Vec<DatasetRef>> {
        let rows = sqlx::query(AssertSqlSafe(
            "select namespace, name from lineage.event_dataset \
             where event_id = $1 and direction = $2 order by ordinal",
        ))
        .bind(event_id)
        .bind(direction)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| DatasetRef {
                namespace: r.get("namespace"),
                name: r.get("name"),
            })
            .collect())
    }

    /// One-hop graph: distinct datasets on `to_dir` of any event that has
    /// `dataset` on `from_dir`. `upstream` = (output -> input); `downstream` =
    /// (input -> output).
    async fn graph_step(
        &self,
        dataset: &DatasetRef,
        from_dir: &str,
        to_dir: &str,
    ) -> Result<Vec<DatasetRef>> {
        let rows = sqlx::query(AssertSqlSafe(
            "select distinct b.namespace, b.name \
             from lineage.event_dataset a \
             join lineage.event_dataset b on b.event_id = a.event_id and b.direction = $4 \
             where a.direction = $3 and a.namespace = $1 and a.name = $2",
        ))
        .bind(&dataset.namespace)
        .bind(&dataset.name)
        .bind(from_dir)
        .bind(to_dir)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| DatasetRef {
                namespace: r.get("namespace"),
                name: r.get("name"),
            })
            .collect())
    }
}
