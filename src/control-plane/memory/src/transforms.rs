//! Memory transforms concern: definitions and runs in two maps.

use std::collections::HashMap;

use async_trait::async_trait;
use control_plane_core::{
    ControlPlaneError, JobId, NewJob, Page, PageReq, Result, RunOutcome, RunState, TransformBody,
    TransformDef, TransformName, TransformRun, Transforms, validate_transform_def,
};
use uuid::Uuid;

use crate::MemoryControlPlane;

#[derive(Default)]
pub(crate) struct TransformsState {
    pub(crate) defs: HashMap<String, TransformDef>,
    pub(crate) runs: HashMap<Uuid, TransformRun>,
}

#[async_trait]
impl Transforms for MemoryControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_transform(&self, def: TransformDef) -> Result<()> {
        validate_transform_def(&def)?;
        if let TransformBody::Typed { inputs, output, .. } = &def.body {
            let ont = self.ontology.lock();
            for ty in inputs.iter().chain(std::iter::once(output)) {
                if !ont.types.contains_key(ty) {
                    return Err(ControlPlaneError::Validation(format!(
                        "unknown ontology type in typed transform: {ty}"
                    )));
                }
            }
        }
        self.transforms.lock().defs.insert(def.name.0.clone(), def);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_transform(&self, name: &TransformName) -> Result<TransformDef> {
        self.transforms
            .lock()
            .defs
            .get(&name.0)
            .cloned()
            .ok_or_else(|| ControlPlaneError::NotFound(format!("transform {}", name.0)))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_transforms(&self, _page: PageReq) -> Result<Page<TransformDef>> {
        let mut items: Vec<_> = self.transforms.lock().defs.values().cloned().collect();
        items.sort_by(|a, b| a.name.0.cmp(&b.name.0));
        Ok(Page::from_full(items))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn delete_transform(&self, name: &TransformName) -> Result<()> {
        self.transforms.lock().defs.remove(&name.0);
        Ok(())
    }

    #[tracing::instrument(skip(self, run, job), level = "debug")]
    async fn submit_run(&self, run: TransformRun, job: NewJob) -> Result<JobId> {
        // GLOBAL LOCK ORDER: `rows` BEFORE `transforms` — Task 6's
        // `MemoryTx::commit` extends the documented rows->lineage->catalog
        // order with transforms LAST, and this method must agree or the two
        // paths ABBA-deadlock. Holding both across the insert pair keeps the
        // job invisible until its run exists (mirrors the postgres tx).
        let id;
        {
            let mut rows = self.rows.lock();
            let mut st = self.transforms.lock();
            id = MemoryControlPlane::insert(&mut rows, job);
            st.runs.insert(run.run_id, run);
        }
        self.notify.notify_waiters();
        Ok(JobId(id))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn mark_run_running(&self, run_id: Uuid) -> Result<()> {
        let mut st = self.transforms.lock();
        let run = st
            .runs
            .get_mut(&run_id)
            .ok_or_else(|| ControlPlaneError::NotFound(format!("run {run_id}")))?;
        run.state = RunState::Running;
        run.started_at = Some(time::OffsetDateTime::now_utc());
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn finish_run(&self, run_id: Uuid, outcome: RunOutcome) -> Result<()> {
        let mut st = self.transforms.lock();
        let run = st
            .runs
            .get_mut(&run_id)
            .ok_or_else(|| ControlPlaneError::NotFound(format!("run {run_id}")))?;
        apply_outcome(run, outcome);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_run(&self, run_id: Uuid) -> Result<TransformRun> {
        self.transforms
            .lock()
            .runs
            .get(&run_id)
            .cloned()
            .ok_or_else(|| ControlPlaneError::NotFound(format!("run {run_id}")))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_runs(
        &self,
        transform: Option<&TransformName>,
        _page: PageReq,
    ) -> Result<Page<TransformRun>> {
        let mut items: Vec<_> = self
            .transforms
            .lock()
            .runs
            .values()
            .filter(|r| transform.is_none_or(|t| r.transform.as_ref() == Some(t)))
            .cloned()
            .collect();
        items.sort_by(|a, b| b.queued_at.cmp(&a.queued_at).then(b.run_id.cmp(&a.run_id)));
        Ok(Page::from_full(items))
    }
}

/// Shared state transition for [`RunOutcome`] — also used by the memory
/// `TableTx` success path (Task 6).
pub(crate) fn apply_outcome(run: &mut TransformRun, outcome: RunOutcome) {
    match outcome {
        RunOutcome::Succeeded { snapshot_id } => {
            run.state = RunState::Succeeded;
            run.snapshot_id = Some(snapshot_id);
            run.finished_at = Some(time::OffsetDateTime::now_utc());
        }
        RunOutcome::RetryQueued { error } => {
            run.state = RunState::Queued;
            run.error = Some(error);
        }
        RunOutcome::Failed { error } => {
            run.state = RunState::Failed;
            run.error = Some(error);
            run.finished_at = Some(time::OffsetDateTime::now_utc());
        }
    }
}
