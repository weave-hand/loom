use async_trait::async_trait;
use control_plane_core::{
    DatasetRef, Lineage, LineageEvent, Page, PageReq, Result, RunId, check_depth,
    decode_dataset_cursor, decode_event_cursor, encode_dataset_cursor, encode_event_cursor,
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
        let mut keyed: Vec<(i64, LineageEvent)> = Vec::with_capacity(rows.len());
        for r in rows {
            let event_id = r.event_id;
            keyed.push((
                event_id,
                LineageEvent {
                    run_id: *run,
                    event_type: r.event_type.parse()?,
                    event_time: r.event_time,
                    inputs: self.event_datasets(event_id, "input").await?,
                    outputs: self.event_datasets(event_id, "output").await?,
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
}

impl PgControlPlane {
    /// The datasets of one event in one direction, ordered by ordinal.
    #[tracing::instrument(skip(self), level = "debug")]
    async fn event_datasets(&self, event_id: i64, direction: &str) -> Result<Vec<DatasetRef>> {
        let rows = sqlx::query!(
            "select namespace, name from lineage.event_dataset \
             where event_id = $1 and direction = $2 order by ordinal",
            event_id,
            direction,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows
            .into_iter()
            .map(|r| DatasetRef {
                namespace: r.namespace,
                name: r.name,
            })
            .collect())
    }

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
