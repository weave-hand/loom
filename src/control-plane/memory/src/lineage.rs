use std::collections::HashSet;

use async_trait::async_trait;
use control_plane_core::{DatasetRef, Lineage, LineageEvent, Page, PageReq, Result, RunId};

use crate::MemoryControlPlane;

#[derive(Default)]
pub(crate) struct LineageState {
    pub(crate) events: Vec<LineageEvent>,
}

#[async_trait]
impl Lineage for MemoryControlPlane {
    #[tracing::instrument(skip(self, event), fields(run_id = ?event.run_id, event_type = ?event.event_type), level = "debug")]
    async fn emit(&self, event: LineageEvent) -> Result<()> {
        self.lineage.lock().events.push(event);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn events_for(&self, run: &RunId, _page: PageReq) -> Result<Page<LineageEvent>> {
        Ok(Page::from_full(
            self.lineage
                .lock()
                .events
                .iter()
                .filter(|e| e.run_id == *run)
                .cloned()
                .collect(),
        ))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn upstream(&self, dataset: &DatasetRef, _page: PageReq) -> Result<Page<DatasetRef>> {
        let lin = self.lineage.lock();
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for e in lin.events.iter().filter(|e| e.outputs.contains(dataset)) {
            for d in &e.inputs {
                if seen.insert(d.clone()) {
                    out.push(d.clone());
                }
            }
        }
        Ok(Page::from_full(out))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn downstream(&self, dataset: &DatasetRef, _page: PageReq) -> Result<Page<DatasetRef>> {
        let lin = self.lineage.lock();
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for e in lin.events.iter().filter(|e| e.inputs.contains(dataset)) {
            for d in &e.outputs {
                if seen.insert(d.clone()) {
                    out.push(d.clone());
                }
            }
        }
        Ok(Page::from_full(out))
    }
}
