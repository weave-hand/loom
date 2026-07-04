use std::sync::Arc;

use parking_lot::Mutex;

use async_trait::async_trait;
use control_plane_core::{
    ColumnSpec, ControlPlaneError, DataFile, JobId, LineageEvent, NewJob, Result, RunOutcome,
    RunState, RunTrigger, SnapshotId, TableRef, TableTx, TransformName, TransformRun, TriggerNode,
    Tx,
};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::Versioned;
use crate::catalog::CatalogState;
use crate::lineage::LineageState;
use crate::ontology::OntologyState;
use crate::transforms::TransformsState;
use crate::{MemoryControlPlane, Row};

/// One staged catalog file write, in STAGING ORDER. Mirrors the postgres
/// `IcebergTx`'s `WriteMode`-tagged log: append and replace stay one ordered
/// sequence, so a replace end-caps only what is live before it in the log.
pub(crate) enum StagedWrite {
    Append(TableRef, Vec<DataFile>),
    Replace(TableRef, Vec<DataFile>),
}

pub(crate) struct MemoryTx {
    pub(crate) rows: Arc<Mutex<Vec<Row>>>,
    pub(crate) notify: Arc<Notify>,
    pub(crate) lineage: Arc<Mutex<LineageState>>,
    pub(crate) catalog: Arc<Mutex<CatalogState>>,
    pub(crate) transforms: Arc<Mutex<TransformsState>>,
    pub(crate) ontology: Arc<Mutex<OntologyState>>,
    pub(crate) staged: Vec<(Uuid, NewJob)>,
    pub(crate) staged_events: Vec<LineageEvent>,
    pub(crate) staged_tables: Vec<(TableRef, Vec<ColumnSpec>)>,
    pub(crate) staged_writes: Vec<StagedWrite>,
    pub(crate) staged_compactions: Vec<(TableRef, Vec<String>, Vec<DataFile>)>,
    pub(crate) staged_run_success: Option<Uuid>,
}

#[async_trait]
impl Tx for MemoryTx {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn commit(self: Box<Self>) -> Result<Option<SnapshotId>> {
        use control_plane_core::{ColumnDef, FileRef};

        // Validate-before-mutate, cheap and lock-free: a staged success mark with NO
        // staged table-format write at all can never yield a snapshot, so reject it up
        // front (mirrors the postgres `IcebergTx`'s early-return-branch check). This is
        // the common misuse case; the residual case — writes staged but a `create_table`
        // that turns out to be a no-op (table already live) — is caught structurally
        // below and is accepted as programmer error (see the comment at the
        // `last_snapshot` check).
        if self.staged_run_success.is_some()
            && self.staged_tables.is_empty()
            && self.staged_writes.is_empty()
            && self.staged_compactions.is_empty()
        {
            return Err(ControlPlaneError::Validation(
                "mark_run_succeeded staged without a snapshot-producing write".into(),
            ));
        }

        // Ontology snapshot for data-trigger resolution, taken and DROPPED
        // before the commit locks. Invariant (deadlock-freedom): the
        // ontology lock is never acquired while any of rows/lineage/catalog/
        // transforms is held — `define_type` holds ontology THEN lineage,
        // i.e. ontology is only ever taken FIRST; no path takes any of the
        // four then ontology. This snapshot keeps it that way. Staleness
        // across this instant is immaterial (postgres reads per-statement
        // inside its tx).
        let type_tables: std::collections::HashMap<String, TableRef> = self
            .ontology
            .lock()
            .types
            .iter()
            .map(|(n, t)| (n.clone(), t.table.clone()))
            .collect();
        // The committed new-data table set (appends + replaces; compactions
        // and bare creates fire nothing), captured before the apply loop
        // consumes `staged_writes`.
        let written: Vec<TableRef> = self
            .staged_writes
            .iter()
            .map(|w| match w {
                StagedWrite::Append(t, _) | StagedWrite::Replace(t, _) => t.clone(),
            })
            .collect();
        let mut fired = false;

        let staged_any = !self.staged.is_empty();
        let mut last_snapshot: Option<i64> = None;
        {
            // Hold ALL FOUR locks across the whole apply so the commit is atomic
            // w.r.t. any single-lock reader (dequeue locks `rows`; events_for locks
            // `lineage`; the catalog reads lock `catalog`): no partial commit — across
            // queue, lineage, catalog, AND transforms — is ever observable, faithfully
            // modelling the postgres single-`sqlx::Transaction` guarantee. Lock order is
            // rows->lineage->catalog->transforms (transforms LAST — Task 6's success
            // mark, and slice 3's data-trigger enqueue below, both need it last;
            // `submit_run` takes rows then transforms, agreeing with this ordering);
            // readers each take only ONE of these locks (no reader takes two), so
            // holding all four here cannot deadlock. The ontology snapshot above is
            // taken and dropped BEFORE this block, so ontology is never held
            // alongside any of the four — see the invariant note there.
            let mut rows = self.rows.lock();
            let mut lin = self.lineage.lock();
            let mut cat = self.catalog.lock();
            let mut transforms = self.transforms.lock();

            // Verify a staged run success mark's run exists BEFORE any mutation below —
            // an unknown run must abort the whole commit with nothing applied.
            if let Some(rid) = self.staged_run_success
                && !transforms.runs.contains_key(&rid)
            {
                return Err(ControlPlaneError::NotFound(format!("run {rid}")));
            }

            // Validate every fallible precondition BEFORE mutating any state, so a
            // rejected op aborts the whole commit with nothing applied (the guards
            // drop on the early return — no partial commit). A named compaction expire
            // target that is not live -> Conflict (a concurrent compaction superseded
            // it), matching the postgres guard.
            for (table, expire, _write) in &self.staged_compactions {
                let expire_set: std::collections::HashSet<&str> =
                    expire.iter().map(|p| p.as_str()).collect();
                let live_matched = cat
                    .files
                    .get(table)
                    .map(|fs| {
                        fs.iter()
                            .filter(|f| f.end.is_none() && expire_set.contains(f.val.path.as_str()))
                            .count()
                    })
                    .unwrap_or(0);
                if live_matched != expire.len() {
                    return Err(control_plane_core::ControlPlaneError::Conflict(format!(
                        "compact_files: {} of {} expire targets live for {}.{}",
                        live_matched,
                        expire.len(),
                        table.schema,
                        table.name
                    )));
                }
            }

            // --- queue + lineage ---
            for (id, job) in self.staged {
                MemoryControlPlane::insert_with_id(&mut rows, id, job);
            }
            lin.events.extend(self.staged_events);

            // --- catalog ---
            // Apply staged table creations (idempotent: skip if already live).
            for (table, columns) in self.staged_tables {
                let key = table.clone();
                if cat.tables.contains_key(&key) {
                    continue;
                }
                let s = cat.new_snapshot();
                last_snapshot = Some(s);
                cat.tables.insert(
                    key.clone(),
                    Versioned {
                        begin: s,
                        end: None,
                        val: (),
                    },
                );
                let cols: Vec<Versioned<ColumnDef>> = columns
                    .iter()
                    .enumerate()
                    .map(|(i, spec)| Versioned {
                        begin: s,
                        end: None,
                        val: ColumnDef {
                            order: i as i64,
                            name: spec.name.clone(),
                            ty: spec.ty.clone(),
                            nullable: spec.nullable,
                        },
                    })
                    .collect();
                cat.columns.insert(key, cols);
            }

            // --- catalog file writes, replayed in STAGING order (matches the
            // postgres IcebergTx: a replace end-caps only files live before
            // it in the log; a later append stays live) ---
            for write in self.staged_writes {
                match write {
                    StagedWrite::Append(table, files) => {
                        let s = cat.new_snapshot();
                        last_snapshot = Some(s);
                        for file in files {
                            cat.files.entry(table.clone()).or_default().push(Versioned {
                                begin: s,
                                end: None,
                                val: FileRef {
                                    path: file.path,
                                    record_count: file.record_count,
                                    file_size_bytes: file.file_size_bytes,
                                },
                            });
                        }
                    }
                    StagedWrite::Replace(table, files) => {
                        let s = cat.new_snapshot();
                        last_snapshot = Some(s);
                        if let Some(existing) = cat.files.get_mut(&table) {
                            for f in existing.iter_mut() {
                                if f.end.is_none() {
                                    f.end = Some(s);
                                }
                            }
                        }
                        for file in files {
                            cat.files.entry(table.clone()).or_default().push(Versioned {
                                begin: s,
                                end: None,
                                val: FileRef {
                                    path: file.path,
                                    record_count: file.record_count,
                                    file_size_bytes: file.file_size_bytes,
                                },
                            });
                        }
                    }
                }
            }

            // Apply staged file compactions (validated above): expire the NAMED live
            // files at a new snapshot and add the coalesced replacements. Unlike a
            // replacement, the table's other live files are untouched.
            for (table, expire, files) in self.staged_compactions {
                let key = table.clone();
                let expire_set: std::collections::HashSet<&str> =
                    expire.iter().map(|p| p.as_str()).collect();
                let s = cat.new_snapshot();
                last_snapshot = Some(s);
                if let Some(existing) = cat.files.get_mut(&key) {
                    for f in existing.iter_mut() {
                        if f.end.is_none() && expire_set.contains(f.val.path.as_str()) {
                            f.end = Some(s);
                        }
                    }
                }
                for file in files {
                    cat.files.entry(key.clone()).or_default().push(Versioned {
                        begin: s,
                        end: None,
                        val: FileRef {
                            path: file.path,
                            record_count: file.record_count,
                            file_size_bytes: file.file_size_bytes,
                        },
                    });
                }
            }

            // Apply the staged run success mark LAST — it needs `last_snapshot`,
            // which only the writes above allocate. The run's existence was already
            // verified at the top of this block, before any mutation; a `None`
            // `last_snapshot` here means writes were staged but every `create_table`
            // turned out to be a no-op (table already live) — an accepted, rare
            // programmer-error edge case (see the up-front check above). Unlike the
            // up-front check, this abort happens after the queue/lineage/catalog
            // sections above already applied (in-memory state has no transactional
            // rollback the way the postgres `IcebergTx`'s held `sqlx::Transaction`
            // does); the up-front check exists precisely to make this residual path
            // unreachable in practice.
            if let Some(rid) = self.staged_run_success {
                let Some(s) = last_snapshot else {
                    return Err(ControlPlaneError::Validation(
                        "mark_run_succeeded staged without a snapshot-producing write".into(),
                    ));
                };
                let run = transforms
                    .runs
                    .get_mut(&rid)
                    .ok_or_else(|| ControlPlaneError::NotFound(format!("run {rid}")))?;
                crate::transforms::apply_outcome(run, RunOutcome::Succeeded { snapshot_id: s });
            }

            // --- data triggers (slice 3): mirror pg_fire_data_triggers ---
            // For each `on_input_commit` def (name-ordered, matching the
            // postgres lock-order-by-name intent even though the memory
            // adapter has no per-row lock to take) whose ontology-resolved
            // inputs intersect `written`: skip the committing run's own
            // transform (self-skip), skip if a `Queued` run of that def
            // already exists (debounce; `Running` does not suppress), then
            // enqueue a fresh `DataTrigger` run with the def's CURRENT body
            // (re-read from `transforms.defs`, so a redefine racing this
            // commit is reflected, mirroring postgres's `for update` re-read).
            if !written.is_empty() {
                let skip: Option<String> = self
                    .staged_run_success
                    .and_then(|rid| transforms.runs.get(&rid))
                    .and_then(|r| r.transform.as_ref().map(|t| t.0.clone()));
                let mut names: Vec<String> = transforms
                    .defs
                    .iter()
                    .filter(|(_, d)| d.on_input_commit)
                    .map(|(n, _)| n.clone())
                    .collect();
                names.sort_unstable();
                for name in names {
                    if skip.as_deref() == Some(name.as_str()) {
                        continue;
                    }
                    let Some((body, inputs)) = transforms.defs.get(&name).map(|def| {
                        let node = TriggerNode::resolve(&def.name, &def.body, &type_tables);
                        (def.body.clone(), node.inputs)
                    }) else {
                        continue; // deleted since the candidate scan
                    };
                    if !inputs.iter().any(|t| written.contains(t)) {
                        continue;
                    }
                    let pending = transforms.runs.values().any(|r| {
                        r.state == RunState::Queued
                            && r.transform.as_ref().is_some_and(|t| t.0 == name)
                    });
                    if pending {
                        continue;
                    }
                    let run_id = Uuid::new_v4();
                    MemoryControlPlane::insert(&mut rows, body.to_job(run_id));
                    transforms.runs.insert(
                        run_id,
                        TransformRun {
                            run_id,
                            transform: Some(TransformName(name)),
                            trigger: RunTrigger::DataTrigger,
                            state: RunState::Queued,
                            body,
                            queued_at: time::OffsetDateTime::now_utc(),
                            started_at: None,
                            finished_at: None,
                            snapshot_id: None,
                            error: None,
                        },
                    );
                    fired = true;
                }
            }
        }

        if staged_any || fired {
            self.notify.notify_waiters();
        }
        Ok(last_snapshot.map(SnapshotId))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip(self, job), level = "debug")]
    async fn enqueue(&mut self, job: NewJob) -> Result<JobId> {
        let id = Uuid::new_v4();
        self.staged.push((id, job));
        Ok(JobId(id))
    }

    #[tracing::instrument(skip(self, event), fields(run_id = ?event.run_id, event_type = ?event.event_type), level = "debug")]
    async fn emit(&mut self, event: LineageEvent) -> Result<()> {
        self.staged_events.push(event);
        Ok(())
    }
}

#[async_trait]
impl TableTx for MemoryTx {
    async fn create_table(&mut self, table: &TableRef, columns: &[ColumnSpec]) -> Result<()> {
        self.staged_tables.push((table.clone(), columns.to_vec()));
        Ok(())
    }

    async fn append_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()> {
        self.staged_writes
            .push(StagedWrite::Append(table.clone(), files.to_vec()));
        Ok(())
    }

    async fn replace_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()> {
        self.staged_writes
            .push(StagedWrite::Replace(table.clone(), files.to_vec()));
        Ok(())
    }

    async fn compact_files(
        &mut self,
        table: &TableRef,
        expire: &[String],
        write: &[DataFile],
    ) -> Result<()> {
        self.staged_compactions
            .push((table.clone(), expire.to_vec(), write.to_vec()));
        Ok(())
    }

    async fn mark_run_succeeded(&mut self, run_id: Uuid) -> Result<()> {
        if self.staged_run_success.is_some() {
            return Err(ControlPlaneError::Validation(
                "a run success mark is already staged on this transaction".into(),
            ));
        }
        self.staged_run_success = Some(run_id);
        Ok(())
    }
}
