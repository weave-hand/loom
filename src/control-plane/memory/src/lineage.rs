use std::collections::HashSet;

use async_trait::async_trait;
use control_plane_core::{
    DatasetRef, Lineage, LineageEvent, Page, PageReq, Result, RunId, check_depth,
    decode_dataset_cursor, decode_event_cursor, encode_dataset_cursor, encode_event_cursor,
};

use crate::MemoryControlPlane;

#[derive(Default)]
pub(crate) struct LineageState {
    pub(crate) events: Vec<LineageEvent>,
}

/// Which way to walk the per-event input/output co-membership graph.
#[derive(Clone, Copy)]
enum Dir {
    /// output→input: ancestry (upstream).
    Upstream,
    /// input→output: descendancy (downstream).
    Downstream,
}

/// One-hop neighbors of `node` in `dir`, scanning `events`. Upstream: for every
/// event that *produces* `node` (node ∈ outputs), its inputs. Downstream: for every
/// event that *consumes* `node` (node ∈ inputs), its outputs.
fn neighbors(events: &[LineageEvent], node: &DatasetRef, dir: Dir) -> Vec<DatasetRef> {
    let mut out = Vec::new();
    for e in events {
        let (probe, yield_) = match dir {
            Dir::Upstream => (&e.outputs, &e.inputs),
            Dir::Downstream => (&e.inputs, &e.outputs),
        };
        if probe.contains(node) {
            out.extend(yield_.iter().cloned());
        }
    }
    out
}

/// Depth-bounded BFS closure with a visited set (the cycle guard). Returns the
/// reachable set excluding the seed, sorted by `(namespace, name)`.
fn closure(events: &[LineageEvent], start: &DatasetRef, depth: u32, dir: Dir) -> Vec<DatasetRef> {
    let mut visited: HashSet<DatasetRef> = HashSet::new();
    visited.insert(start.clone());
    let mut frontier = vec![start.clone()];
    let mut result: Vec<DatasetRef> = Vec::new();
    for _ in 0..depth {
        let mut next_frontier = Vec::new();
        for node in &frontier {
            for nbr in neighbors(events, node, dir) {
                if visited.insert(nbr.clone()) {
                    result.push(nbr.clone());
                    next_frontier.push(nbr);
                }
            }
        }
        if next_frontier.is_empty() {
            break;
        }
        frontier = next_frontier;
    }
    result.sort();
    result
}

/// Apply keyset pagination to an already-sorted dataset set.
fn paginate_datasets(sorted: Vec<DatasetRef>, page: &PageReq) -> Result<Page<DatasetRef>> {
    let after = page.after.as_ref().map(decode_dataset_cursor).transpose()?;
    let filtered: Vec<DatasetRef> = match after {
        Some(a) => sorted.into_iter().filter(|d| *d > a).collect(),
        None => sorted,
    };
    let limited: Vec<DatasetRef> = filtered.into_iter().take(page.fetch_take()).collect();
    Ok(Page::from_keyset(
        limited,
        page.limit,
        encode_dataset_cursor,
    ))
}

#[async_trait]
impl Lineage for MemoryControlPlane {
    #[tracing::instrument(skip(self, event), fields(run_id = ?event.run_id, event_type = ?event.event_type), level = "debug")]
    async fn emit(&self, event: LineageEvent) -> Result<()> {
        self.lineage.lock().events.push(event);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn events_for(&self, run: &RunId, page: PageReq) -> Result<Page<LineageEvent>> {
        let after = page.after.as_ref().map(decode_event_cursor).transpose()?;
        let lin = self.lineage.lock();
        // Stable key = the event's insert index (append-only Vec, emit order).
        let mut keyed: Vec<(i64, LineageEvent)> = lin
            .events
            .iter()
            .enumerate()
            .filter(|(_, e)| e.run_id == *run)
            .map(|(i, e)| (i64::try_from(i).unwrap_or(i64::MAX), e.clone()))
            .collect();
        if let Some(a) = after {
            keyed.retain(|(i, _)| *i > a);
        }
        let limited: Vec<(i64, LineageEvent)> = keyed.into_iter().take(page.fetch_take()).collect();
        let paged = Page::from_keyset(limited, page.limit, |(i, _)| encode_event_cursor(*i));
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
        check_depth(depth)?;
        let set = closure(&self.lineage.lock().events, dataset, depth, Dir::Upstream);
        paginate_datasets(set, &page)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn downstream(
        &self,
        dataset: &DatasetRef,
        depth: u32,
        page: PageReq,
    ) -> Result<Page<DatasetRef>> {
        check_depth(depth)?;
        let set = closure(&self.lineage.lock().events, dataset, depth, Dir::Downstream);
        paginate_datasets(set, &page)
    }
}
