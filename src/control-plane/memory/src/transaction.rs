use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use control_plane_core::{
    ColumnSpec, DataFile, JobId, LineageEvent, NewJob, Result, SnapshotId, TableRef, Tx,
};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::Versioned;
use crate::catalog::CatalogState;
use crate::lineage::LineageState;
use crate::{MemoryControlPlane, Row};

pub(crate) struct MemoryTx {
    pub(crate) rows: Arc<Mutex<Vec<Row>>>,
    pub(crate) notify: Arc<Notify>,
    pub(crate) lineage: Arc<Mutex<LineageState>>,
    pub(crate) catalog: Arc<Mutex<CatalogState>>,
    pub(crate) staged: Vec<(Uuid, NewJob)>,
    pub(crate) staged_events: Vec<LineageEvent>,
    pub(crate) staged_tables: Vec<(TableRef, Vec<ColumnSpec>)>,
    pub(crate) staged_files: Vec<(TableRef, Vec<DataFile>)>,
    pub(crate) staged_replacements: Vec<(TableRef, Vec<DataFile>)>,
    pub(crate) staged_compactions: Vec<(TableRef, Vec<String>, Vec<DataFile>)>,
}

#[async_trait]
impl Tx for MemoryTx {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn commit(self: Box<Self>) -> Result<Option<SnapshotId>> {
        let staged_any = !self.staged.is_empty();
        {
            // Hold BOTH locks across the whole apply so commit is atomic w.r.t. any
            // single-lock reader (dequeue locks `rows`; events_for locks `lineage`):
            // no partial commit is observable. Lock order rows-then-lineage must be
            // consistent everywhere to stay deadlock-free (readers take only one
            // lock; no reader takes both).
            let mut rows = self.rows.lock().unwrap();
            let mut lin = self.lineage.lock().unwrap();
            for (id, job) in self.staged {
                MemoryControlPlane::insert_with_id(&mut rows, id, job);
            }
            lin.events.extend(self.staged_events);
        }
        if staged_any {
            self.notify.notify_waiters();
        }

        let has_catalog_ops = !self.staged_tables.is_empty()
            || !self.staged_files.is_empty()
            || !self.staged_replacements.is_empty()
            || !self.staged_compactions.is_empty();
        if !has_catalog_ops {
            return Ok(None);
        }

        let mut last_snapshot: Option<i64> = None;
        {
            use control_plane_core::ColumnDef;

            let mut cat = self.catalog.lock().unwrap();

            // Apply staged table creations (idempotent: skip if already live).
            for (table, columns) in self.staged_tables {
                let key = (table.schema.clone(), table.name.clone());
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

            // Apply staged file appends.
            for (table, files) in self.staged_files {
                use control_plane_core::FileRef;
                let key = (table.schema.clone(), table.name.clone());
                let s = cat.new_snapshot();
                last_snapshot = Some(s);
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

            // Apply staged file replacements: expire the table's live files at a new
            // snapshot, then add the replacements live at that snapshot.
            for (table, files) in self.staged_replacements {
                use control_plane_core::FileRef;
                let key = (table.schema.clone(), table.name.clone());
                let s = cat.new_snapshot();
                last_snapshot = Some(s);
                if let Some(existing) = cat.files.get_mut(&key) {
                    for f in existing.iter_mut() {
                        if f.end.is_none() {
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

            // Apply staged file compactions: expire the NAMED live files at a new
            // snapshot and add the coalesced replacements. Unlike a replacement, the
            // table's other live files are untouched. Validate BEFORE mutating (the
            // memory commit applies directly to shared state, so a mid-apply error must
            // not leave partial changes): a named path that is not live -> Conflict (a
            // concurrent compaction superseded it), matching the postgres guard.
            for (table, expire, files) in self.staged_compactions {
                use control_plane_core::FileRef;
                let key = (table.schema.clone(), table.name.clone());
                let expire_set: std::collections::HashSet<&str> =
                    expire.iter().map(|p| p.as_str()).collect();
                let live_matched = cat
                    .files
                    .get(&key)
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

    async fn create_table(&mut self, table: &TableRef, columns: &[ColumnSpec]) -> Result<()> {
        self.staged_tables.push((table.clone(), columns.to_vec()));
        Ok(())
    }

    async fn append_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()> {
        self.staged_files.push((table.clone(), files.to_vec()));
        Ok(())
    }

    async fn replace_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()> {
        self.staged_replacements
            .push((table.clone(), files.to_vec()));
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
}
