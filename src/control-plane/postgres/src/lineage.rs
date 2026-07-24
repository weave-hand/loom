use async_trait::async_trait;
use control_plane_core::{
    DatasetRef, Lineage, LineageEvent, Page, PageReq, Result, RunId, RunRole, RunSummary,
    check_depth, decode_dataset_cursor, decode_event_cursor, decode_run_cursor,
    encode_dataset_cursor, encode_event_cursor, encode_run_cursor,
};

use crate::{PgControlPlane, backend};

pub(crate) async fn pg_emit<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    event: &LineageEvent,
) -> Result<()> {
    // One round-trip: insert the event, then its input/output rows via unnest.
    // Ordinals come from WITH ORDINALITY (1-based; the read path orders by
    // `ordinal`, so the absolute base is irrelevant).
    sqlx::query!(
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
        event.run_id.0,
        event.event_type.as_str(),
        event.event_time,
        &event.payload,
        &event
            .inputs
            .iter()
            .map(|d| d.namespace.clone())
            .collect::<Vec<_>>(),
        &event
            .inputs
            .iter()
            .map(|d| d.name.clone())
            .collect::<Vec<_>>(),
        &event
            .outputs
            .iter()
            .map(|d| d.namespace.clone())
            .collect::<Vec<_>>(),
        &event
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

    #[tracing::instrument(skip(self), level = "debug")]
    async fn events_for(&self, run: &RunId, page: PageReq) -> Result<Page<LineageEvent>> {
        let after = page.after.as_ref().map(decode_event_cursor).transpose()?;
        let fetch = page.fetch_limit_i64();
        let rows = sqlx::query!(
            "select event_id, event_type, event_time, payload from lineage.event \
             where run_id = $1 and ($2::bigint is null or event_id > $2) \
             order by event_id limit $3",
            run.0,
            after,
            fetch,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let ids: Vec<i64> = rows.iter().map(|r| r.event_id).collect();
        // ONE query hydrates every page event's datasets (was 2 queries per
        // event). `order by event_id, ordinal` preserves each direction's
        // ordinal order after the per-event split below.
        let ds_rows = sqlx::query!(
            "select event_id, direction, namespace, name from lineage.event_dataset \
             where event_id = any($1) order by event_id, ordinal",
            &ids,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut inputs: std::collections::HashMap<i64, Vec<DatasetRef>> =
            std::collections::HashMap::new();
        let mut outputs: std::collections::HashMap<i64, Vec<DatasetRef>> =
            std::collections::HashMap::new();
        for d in ds_rows {
            // Unknown tokens are a loud error (a corrupt row), never silently
            // dropped or misfiled — mirrors the core enum codecs.
            let bucket = match d.direction.as_str() {
                "input" => &mut inputs,
                "output" => &mut outputs,
                other => {
                    return Err(control_plane_core::ControlPlaneError::Validation(format!(
                        "unknown lineage direction '{other}'"
                    )));
                }
            };
            bucket.entry(d.event_id).or_default().push(DatasetRef {
                namespace: d.namespace,
                name: d.name,
            });
        }
        let mut keyed: Vec<(i64, LineageEvent)> = Vec::with_capacity(rows.len());
        for r in rows {
            let event_id = r.event_id;
            keyed.push((
                event_id,
                LineageEvent {
                    run_id: *run,
                    event_type: r.event_type.parse()?,
                    event_time: r.event_time,
                    inputs: inputs.remove(&event_id).unwrap_or_default(),
                    outputs: outputs.remove(&event_id).unwrap_or_default(),
                    payload: r.payload,
                },
            ));
        }
        let paged = Page::from_keyset(keyed, page.limit, |(id, _)| encode_event_cursor(*id));
        Ok(Page {
            items: paged.items.into_iter().map(|(_, e)| e).collect(),
            next: paged.next,
        })
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn upstream(
        &self,
        dataset: &DatasetRef,
        depth: u32,
        page: PageReq,
    ) -> Result<Page<DatasetRef>> {
        // upstream = walk output→input edges (ancestry).
        self.graph_closure(dataset, "output", "input", depth, page)
            .await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn downstream(
        &self,
        dataset: &DatasetRef,
        depth: u32,
        page: PageReq,
    ) -> Result<Page<DatasetRef>> {
        // downstream = walk input→output edges (descendancy).
        self.graph_closure(dataset, "input", "output", depth, page)
            .await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn runs_for(&self, dataset: &DatasetRef, page: PageReq) -> Result<Page<RunSummary>> {
        let after = page.after.as_ref().map(decode_run_cursor).transpose()?;
        let fetch = page.fetch_limit_i64();
        // Per-run collapse: for each run that touches (ns,name), the run's newest
        // event (max event_id) supplies time+type; `bool_or(direction='output')`
        // gives the role (output wins). Keyset by that max event_id, newest-first
        // (bigserial is monotonic with emit order, so `max_eid < cursor` walks back).
        let rows = sqlx::query!(
            "select r.run_id as \"run_id!\", \
                    r.max_eid as \"max_eid!\", \
                    e.event_type as \"event_type!\", \
                    e.event_time as \"event_time!\", \
                    r.has_output as \"has_output!\" \
             from ( \
                 select ev.run_id, \
                        max(ed.event_id) as max_eid, \
                        bool_or(ed.direction = 'output') as has_output \
                 from lineage.event_dataset ed \
                 join lineage.event ev on ev.event_id = ed.event_id \
                 where ed.namespace = $1 and ed.name = $2 \
                 group by ev.run_id) r \
             join lineage.event e on e.event_id = r.max_eid \
             where ($3::bigint is null or r.max_eid < $3) \
             order by r.max_eid desc \
             limit $4",
            &dataset.namespace,
            &dataset.name,
            after,
            fetch,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut keyed: Vec<(i64, RunSummary)> = Vec::with_capacity(rows.len());
        for r in rows {
            let role = if r.has_output {
                RunRole::Output
            } else {
                RunRole::Input
            };
            keyed.push((
                r.max_eid,
                RunSummary {
                    run_id: RunId(r.run_id),
                    latest_event_time: r.event_time,
                    latest_event_type: r.event_type.parse()?,
                    role,
                },
            ));
        }
        let paged = Page::from_keyset(keyed, page.limit, |(id, _)| encode_run_cursor(*id));
        Ok(Page {
            items: paged.items.into_iter().map(|(_, s)| s).collect(),
            next: paged.next,
        })
    }
}

impl PgControlPlane {
    /// Depth-bounded transitive closure over `lineage.event_dataset`. The seed sits
    /// on `from_dir`; neighbors are taken from `to_dir` of co-member events. A
    /// `WITH RECURSIVE` CTE (mirroring query-api's `/graph` reachability) bounded by
    /// `depth`; `UNION` + the final `DISTINCT` give set semantics and the depth cap
    /// guarantees termination even on cyclic re-run graphs. Keyset-paginated by
    /// `(namespace, name)`.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn graph_closure(
        &self,
        dataset: &DatasetRef,
        from_dir: &str,
        to_dir: &str,
        depth: u32,
        page: PageReq,
    ) -> Result<Page<DatasetRef>> {
        check_depth(depth)?;
        let after = page.after.as_ref().map(decode_dataset_cursor).transpose()?;
        let (after_ns, after_name) = match &after {
            Some(d) => (Some(d.namespace.as_str()), Some(d.name.as_str())),
            None => (None, None),
        };
        let max_depth = i32::try_from(depth).unwrap_or(i32::MAX);
        let fetch = page.fetch_limit_i64();
        let rows = sqlx::query!(
            "with recursive closure(namespace, name, depth) as ( \
                 select $1::text, $2::text, 0 \
               union \
                 select b.namespace, b.name, closure.depth + 1 \
                 from closure \
                 join lineage.event_dataset a \
                   on a.namespace = closure.namespace and a.name = closure.name \
                   and a.direction = $3 \
                 join lineage.event_dataset b \
                   on b.event_id = a.event_id and b.direction = $4 \
                 where closure.depth < $5) \
             select distinct namespace as \"namespace!\", name as \"name!\" \
             from closure \
             where not (namespace = $1 and name = $2) \
               and ($6::text is null or (namespace, name) > ($6, $7)) \
             order by namespace, name \
             limit $8",
            &dataset.namespace,
            &dataset.name,
            from_dir,
            to_dir,
            max_depth,
            after_ns,
            after_name,
            fetch,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let items: Vec<DatasetRef> = rows
            .into_iter()
            .map(|r| DatasetRef {
                namespace: r.namespace,
                name: r.name,
            })
            .collect();
        Ok(Page::from_keyset(items, page.limit, encode_dataset_cursor))
    }
}
