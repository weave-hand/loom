//! In-memory fake adapter for the control-plane traits — fast, hermetic tests
//! and local dev. NOT for production use. Jobs live in a `Vec` behind a `Mutex`;
//! a `Tx` stages writes and applies them on commit (read-committed semantics).

mod acl;
mod auth;
mod catalog;
mod lineage;
mod ontology;
mod queue;
mod transaction;

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Auth, Catalog, ColumnDef, ControlPlane, FileRef, Lineage, NewJob, Ontology, Queue, Result,
    SnapshotId, TableControlPlane, TableRef, TableTx, Tx,
};
use time::OffsetDateTime;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::acl::AclState;
use crate::auth::AuthState;
use crate::catalog::CatalogState;
use crate::lineage::LineageState;
use crate::ontology::OntologyState;
use crate::transaction::MemoryTx;

#[derive(Clone)]
pub(crate) struct Row {
    pub(crate) id: Uuid,
    pub(crate) kind: String,
    pub(crate) payload: serde_json::Value,
    pub(crate) state: &'static str, // "available" | "running" | "failed"
    pub(crate) run_at: OffsetDateTime,
    pub(crate) priority: i32,
    pub(crate) attempts: i32,
    pub(crate) locked_at: Option<OffsetDateTime>,
}

/// An MVCC-versioned catalog row: live at snapshot `s` when `begin <= s` and
/// (`end` is None or `end > s`).
#[derive(Clone)]
pub(crate) struct Versioned<T> {
    pub(crate) begin: i64,
    pub(crate) end: Option<i64>,
    pub(crate) val: T,
}

impl<T> Versioned<T> {
    pub(crate) fn live_at(&self, s: i64) -> bool {
        self.begin <= s && self.end.is_none_or(|e| e > s)
    }
}

#[derive(Clone)]
pub struct MemoryControlPlane {
    rows: Arc<Mutex<Vec<Row>>>,
    notify: Arc<Notify>,
    catalog: Arc<Mutex<CatalogState>>,
    ontology: Arc<Mutex<OntologyState>>,
    acl: Arc<Mutex<AclState>>,
    auth: Arc<Mutex<AuthState>>,
    lineage: Arc<Mutex<LineageState>>,
    lock_timeout: Duration,
}

impl MemoryControlPlane {
    pub fn new(lock_timeout: Duration) -> Self {
        Self {
            rows: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(Notify::new()),
            catalog: Arc::new(Mutex::new(CatalogState::default())),
            ontology: Arc::new(Mutex::new(OntologyState::default())),
            acl: Arc::new(Mutex::new(AclState::default())),
            auth: Arc::new(Mutex::new(AuthState::default())),
            lineage: Arc::new(Mutex::new(LineageState::default())),
            lock_timeout,
        }
    }

    fn insert(rows: &mut Vec<Row>, job: NewJob) -> Uuid {
        let id = Uuid::new_v4();
        Self::insert_with_id(rows, id, job);
        id
    }

    fn insert_with_id(rows: &mut Vec<Row>, id: Uuid, job: NewJob) {
        rows.push(Row {
            id,
            kind: job.kind,
            payload: job.payload,
            state: "available",
            run_at: job.run_at.unwrap_or_else(OffsetDateTime::now_utc),
            priority: job.priority,
            attempts: 0,
            locked_at: None,
        });
    }

    /// Test-support: create `table` (if absent) with `columns` as
    /// `(name, type, nullable)`, then apply each entry of `batches` as its own
    /// snapshot adding one data file of that many rows. Returns the per-batch
    /// snapshot ids, in order. Append-only.
    pub fn seed_catalog(
        &self,
        table: &TableRef,
        columns: &[(String, String, bool)],
        batches: &[usize],
    ) -> Vec<SnapshotId> {
        let mut cat = self.catalog.lock();
        let key = table.clone();

        if !cat.tables.contains_key(&key) {
            let s = cat.new_snapshot();
            cat.tables.insert(
                key.clone(),
                Versioned {
                    begin: s,
                    end: None,
                    val: (),
                },
            );
            let cols = columns
                .iter()
                .enumerate()
                .map(|(i, (name, ty, nullable))| Versioned {
                    begin: s,
                    end: None,
                    val: ColumnDef {
                        order: i as i64,
                        name: name.clone(),
                        ty: ty.clone(),
                        nullable: *nullable,
                    },
                })
                .collect();
            cat.columns.insert(key.clone(), cols);
        }

        let mut out = Vec::new();
        for (i, n) in batches.iter().enumerate() {
            let s = cat.new_snapshot();
            let file = FileRef {
                path: format!("data/{}_{}.parquet", table.name, i),
                record_count: *n as i64,
                file_size_bytes: (*n as i64) * 16,
            };
            cat.files.entry(key.clone()).or_default().push(Versioned {
                begin: s,
                end: None,
                val: file,
            });
            out.push(SnapshotId(s));
        }
        out
    }

    /// Test-support: drop `table` at a fresh snapshot, setting `end` on the table and
    /// its still-open files/columns so the MVCC `end`-bound is exercised at the
    /// file/column level (not just short-circuited by the table-liveness gate).
    pub fn drop_table_catalog(&self, table: &TableRef) -> SnapshotId {
        let mut cat = self.catalog.lock();
        let key = table.clone();
        let d = cat.new_snapshot();
        if let Some(t) = cat.tables.get_mut(&key) {
            t.end = Some(d);
        }
        for f in cat.files.get_mut(&key).into_iter().flatten() {
            if f.end.is_none() {
                f.end = Some(d);
            }
        }
        for c in cat.columns.get_mut(&key).into_iter().flatten() {
            if c.end.is_none() {
                c.end = Some(d);
            }
        }
        SnapshotId(d)
    }
}

#[async_trait]
impl ControlPlane for MemoryControlPlane {
    fn catalog(&self) -> &(dyn Catalog + Send + Sync) {
        self
    }
    fn ontology(&self) -> &(dyn Ontology + Send + Sync) {
        self
    }
    fn acl(&self) -> &(dyn Acl + Send + Sync) {
        self
    }
    fn lineage(&self) -> &(dyn Lineage + Send + Sync) {
        self
    }
    fn queue(&self) -> &(dyn Queue + Send + Sync) {
        self
    }
    fn auth(&self) -> &(dyn Auth + Send + Sync) {
        self
    }
    async fn begin(&self) -> Result<Box<dyn Tx + Send>> {
        Ok(self.begin_table().await?)
    }
}

#[async_trait]
impl TableControlPlane for MemoryControlPlane {
    async fn begin_table(&self) -> Result<Box<dyn TableTx + Send>> {
        Ok(Box::new(MemoryTx {
            rows: self.rows.clone(),
            notify: self.notify.clone(),
            lineage: self.lineage.clone(),
            catalog: self.catalog.clone(),
            staged: Vec::new(),
            staged_events: Vec::new(),
            staged_tables: Vec::new(),
            staged_writes: Vec::new(),
            staged_compactions: Vec::new(),
        }))
    }
}
