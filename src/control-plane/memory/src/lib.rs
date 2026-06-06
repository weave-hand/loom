//! In-memory fake adapter for the control-plane traits — fast, hermetic tests
//! and local dev. NOT for production use. Jobs live in a `Vec` behind a `Mutex`;
//! a `Tx` stages writes and applies them on commit (read-committed semantics).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, Catalog, ColumnDef, ControlPlane, ControlPlaneError, DatasetRef, Decision,
    FileRef, Job, JobId, Lineage, LineageEvent, LinkDef, NewJob, ObjectType, Ontology, Policy,
    PolicyTarget, Queue, Result, RetryPolicy, RoleId, RunId, Snapshot, SnapshotId, SubjectId,
    TableRef, TableSchema, Tx, TypeName,
};
use time::OffsetDateTime;
use tokio::sync::Notify;
use uuid::Uuid;

#[derive(Clone)]
struct Row {
    id: Uuid,
    kind: String,
    payload: serde_json::Value,
    state: &'static str, // "available" | "running" | "failed"
    run_at: OffsetDateTime,
    priority: i32,
    attempts: i32,
    locked_at: Option<OffsetDateTime>,
}

/// An MVCC-versioned catalog row: live at snapshot `s` when `begin <= s` and
/// (`end` is None or `end > s`).
#[derive(Clone)]
struct Versioned<T> {
    begin: i64,
    end: Option<i64>,
    val: T,
}

impl<T> Versioned<T> {
    fn live_at(&self, s: i64) -> bool {
        self.begin <= s && self.end.is_none_or(|e| e > s)
    }
}

#[derive(Default)]
struct CatalogState {
    next_snapshot: i64,
    snapshots: Vec<Snapshot>,
    tables: HashMap<(String, String), Versioned<()>>,
    columns: HashMap<(String, String), Vec<Versioned<ColumnDef>>>,
    files: HashMap<(String, String), Vec<Versioned<FileRef>>>,
}

#[derive(Default)]
struct OntologyState {
    types: HashMap<String, ObjectType>,
    links: Vec<LinkDef>,
}

/// Encoded `PolicyTarget` used as a map/set key: `(kind, a, b)`.
type TargetKey = (String, String, String);

fn target_key(t: &PolicyTarget) -> TargetKey {
    match t {
        PolicyTarget::Type(n) => ("type".into(), n.0.clone(), String::new()),
        PolicyTarget::Table(r) => ("table".into(), r.schema.clone(), r.name.clone()),
    }
}

#[derive(Default)]
struct AclState {
    subjects: HashSet<String>,
    roles: HashSet<String>,
    members: HashSet<(String, String)>, // (subject, role)
    grants: HashSet<(String, Action, TargetKey)>, // (role, action, target)
    policies: HashMap<(String, TargetKey), Policy>, // (role, target) -> policy
}

#[derive(Default)]
struct LineageState {
    events: Vec<LineageEvent>,
}

#[derive(Clone)]
pub struct MemoryControlPlane {
    rows: Arc<Mutex<Vec<Row>>>,
    notify: Arc<Notify>,
    catalog: Arc<Mutex<CatalogState>>,
    ontology: Arc<Mutex<OntologyState>>,
    acl: Arc<Mutex<AclState>>,
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
        let mut cat = self.catalog.lock().unwrap();
        let key = (table.schema.clone(), table.name.clone());

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
}

impl CatalogState {
    fn new_snapshot(&mut self) -> i64 {
        let id = self.next_snapshot;
        self.next_snapshot += 1;
        self.snapshots.push(Snapshot {
            id: SnapshotId(id),
            time: OffsetDateTime::now_utc(),
            schema_version: 0,
        });
        id
    }

    fn latest_live(&self, key: &(String, String)) -> Option<i64> {
        let t = self.tables.get(key)?;
        self.snapshots
            .iter()
            .rev()
            .map(|s| s.id.0)
            .find(|&s| t.live_at(s))
    }
}

#[async_trait]
impl Catalog for MemoryControlPlane {
    async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot> {
        let cat = self.catalog.lock().unwrap();
        let key = (table.schema.clone(), table.name.clone());
        let s = cat.latest_live(&key).ok_or_else(|| {
            ControlPlaneError::NotFound(format!("{}.{}", table.schema, table.name))
        })?;
        Ok(cat
            .snapshots
            .iter()
            .find(|sn| sn.id.0 == s)
            .cloned()
            .unwrap())
    }

    async fn snapshots(&self, table: &TableRef) -> Result<Vec<Snapshot>> {
        let cat = self.catalog.lock().unwrap();
        let key = (table.schema.clone(), table.name.clone());
        let t = cat.tables.get(&key).ok_or_else(|| {
            ControlPlaneError::NotFound(format!("{}.{}", table.schema, table.name))
        })?;
        Ok(cat
            .snapshots
            .iter()
            .filter(|sn| t.live_at(sn.id.0))
            .cloned()
            .collect())
    }

    async fn files(&self, table: &TableRef, at: SnapshotId) -> Result<Vec<FileRef>> {
        let cat = self.catalog.lock().unwrap();
        let key = (table.schema.clone(), table.name.clone());
        let live = cat.tables.get(&key).is_some_and(|t| t.live_at(at.0));
        if !live {
            return Err(ControlPlaneError::NotFound(format!(
                "{}.{} @ {}",
                table.schema, table.name, at.0
            )));
        }
        Ok(cat
            .files
            .get(&key)
            .into_iter()
            .flatten()
            .filter(|f| f.live_at(at.0))
            .map(|f| f.val.clone())
            .collect())
    }

    async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema> {
        let cat = self.catalog.lock().unwrap();
        let key = (table.schema.clone(), table.name.clone());
        let live = cat.tables.get(&key).is_some_and(|t| t.live_at(at.0));
        if !live {
            return Err(ControlPlaneError::NotFound(format!(
                "{}.{} @ {}",
                table.schema, table.name, at.0
            )));
        }
        let mut cols: Vec<ColumnDef> = cat
            .columns
            .get(&key)
            .into_iter()
            .flatten()
            .filter(|c| c.live_at(at.0))
            .map(|c| c.val.clone())
            .collect();
        cols.sort_by_key(|c| c.order);
        Ok(TableSchema { columns: cols })
    }
}

#[async_trait]
impl Ontology for MemoryControlPlane {
    async fn define_type(&self, ty: ObjectType) -> Result<()> {
        self.ontology
            .lock()
            .unwrap()
            .types
            .insert(ty.name.0.clone(), ty);
        Ok(())
    }

    async fn define_link(&self, link: LinkDef) -> Result<()> {
        let mut ont = self.ontology.lock().unwrap();
        for endpoint in [&link.from, &link.to] {
            if !ont.types.contains_key(&endpoint.0) {
                return Err(ControlPlaneError::NotFound(format!("type {}", endpoint.0)));
            }
        }
        ont.links
            .retain(|l| !(l.name == link.name && l.from == link.from));
        ont.links.push(link);
        Ok(())
    }

    async fn get_type(&self, name: &TypeName) -> Result<ObjectType> {
        self.ontology
            .lock()
            .unwrap()
            .types
            .get(&name.0)
            .cloned()
            .ok_or_else(|| ControlPlaneError::NotFound(name.0.clone()))
    }

    async fn list_types(&self) -> Result<Vec<ObjectType>> {
        Ok(self
            .ontology
            .lock()
            .unwrap()
            .types
            .values()
            .cloned()
            .collect())
    }

    async fn links(&self, name: &TypeName) -> Result<Vec<LinkDef>> {
        let ont = self.ontology.lock().unwrap();
        if !ont.types.contains_key(&name.0) {
            return Err(ControlPlaneError::NotFound(name.0.clone()));
        }
        Ok(ont
            .links
            .iter()
            .filter(|l| l.from == *name)
            .cloned()
            .collect())
    }

    async fn resolve(&self, name: &TypeName) -> Result<TableRef> {
        Ok(self.get_type(name).await?.table)
    }
}

#[async_trait]
impl Acl for MemoryControlPlane {
    async fn define_subject(&self, id: &SubjectId) -> Result<()> {
        self.acl.lock().unwrap().subjects.insert(id.0.clone());
        Ok(())
    }

    async fn define_role(&self, id: &RoleId) -> Result<()> {
        self.acl.lock().unwrap().roles.insert(id.0.clone());
        Ok(())
    }

    async fn assign_role(&self, subject: &SubjectId, role: &RoleId) -> Result<()> {
        let mut acl = self.acl.lock().unwrap();
        if !acl.subjects.contains(&subject.0) {
            return Err(ControlPlaneError::NotFound(format!(
                "subject {}",
                subject.0
            )));
        }
        if !acl.roles.contains(&role.0) {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        acl.members.insert((subject.0.clone(), role.0.clone()));
        Ok(())
    }

    async fn unassign_role(&self, subject: &SubjectId, role: &RoleId) -> Result<()> {
        self.acl
            .lock()
            .unwrap()
            .members
            .remove(&(subject.0.clone(), role.0.clone()));
        Ok(())
    }

    async fn grant(&self, role: &RoleId, action: Action, target: PolicyTarget) -> Result<()> {
        let mut acl = self.acl.lock().unwrap();
        if !acl.roles.contains(&role.0) {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        acl.grants
            .insert((role.0.clone(), action, target_key(&target)));
        Ok(())
    }

    async fn revoke(&self, role: &RoleId, action: Action, target: &PolicyTarget) -> Result<()> {
        self.acl
            .lock()
            .unwrap()
            .grants
            .remove(&(role.0.clone(), action, target_key(target)));
        Ok(())
    }

    async fn set_policy(&self, role: &RoleId, policy: Policy) -> Result<()> {
        let mut acl = self.acl.lock().unwrap();
        if !acl.roles.contains(&role.0) {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        let key = (role.0.clone(), target_key(&policy.target));
        acl.policies.insert(key, policy);
        Ok(())
    }

    async fn clear_policy(&self, role: &RoleId, target: &PolicyTarget) -> Result<()> {
        self.acl
            .lock()
            .unwrap()
            .policies
            .remove(&(role.0.clone(), target_key(target)));
        Ok(())
    }

    async fn check(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
    ) -> Result<Decision> {
        let acl = self.acl.lock().unwrap();
        let tk = target_key(target);
        let allow = acl
            .members
            .iter()
            .filter(|(s, _)| s == &subject.0)
            .any(|(_, role)| acl.grants.contains(&(role.clone(), action, tk.clone())));
        Ok(if allow {
            Decision::Allow
        } else {
            Decision::Deny
        })
    }

    async fn policies_for(
        &self,
        subject: &SubjectId,
        target: &PolicyTarget,
    ) -> Result<Vec<Policy>> {
        let acl = self.acl.lock().unwrap();
        let tk = target_key(target);
        Ok(acl
            .members
            .iter()
            .filter(|(s, _)| s == &subject.0)
            .filter_map(|(_, role)| acl.policies.get(&(role.clone(), tk.clone())).cloned())
            .collect())
    }
}

#[async_trait]
impl Lineage for MemoryControlPlane {
    async fn emit(&self, event: LineageEvent) -> Result<()> {
        self.lineage.lock().unwrap().events.push(event);
        Ok(())
    }

    async fn events_for(&self, run: &RunId) -> Result<Vec<LineageEvent>> {
        Ok(self
            .lineage
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|e| e.run_id == *run)
            .cloned()
            .collect())
    }

    async fn upstream(&self, dataset: &DatasetRef) -> Result<Vec<DatasetRef>> {
        let lin = self.lineage.lock().unwrap();
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for e in lin.events.iter().filter(|e| e.outputs.contains(dataset)) {
            for d in &e.inputs {
                if seen.insert(d.clone()) {
                    out.push(d.clone());
                }
            }
        }
        Ok(out)
    }

    async fn downstream(&self, dataset: &DatasetRef) -> Result<Vec<DatasetRef>> {
        let lin = self.lineage.lock().unwrap();
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for e in lin.events.iter().filter(|e| e.inputs.contains(dataset)) {
            for d in &e.outputs {
                if seen.insert(d.clone()) {
                    out.push(d.clone());
                }
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl Queue for MemoryControlPlane {
    async fn enqueue(&self, job: NewJob) -> Result<JobId> {
        let id = Self::insert(&mut self.rows.lock().unwrap(), job);
        self.notify.notify_waiters();
        Ok(JobId(id))
    }

    async fn dequeue(&self, kinds: &[String], _worker: &str) -> Result<Option<Job>> {
        let now = OffsetDateTime::now_utc();
        let cutoff = now - self.lock_timeout;
        let mut rows = self.rows.lock().unwrap();
        let mut idxs: Vec<usize> = (0..rows.len())
            .filter(|&i| {
                let r = &rows[i];
                kinds.contains(&r.kind)
                    && r.run_at <= now
                    && (r.state == "available"
                        || (r.state == "running" && r.locked_at.is_none_or(|t| t < cutoff)))
            })
            .collect();
        idxs.sort_by(|&a, &b| {
            rows[b]
                .priority
                .cmp(&rows[a].priority)
                .then(rows[a].run_at.cmp(&rows[b].run_at))
        });
        let Some(&i) = idxs.first() else {
            return Ok(None);
        };
        rows[i].state = "running";
        rows[i].locked_at = Some(now);
        rows[i].attempts += 1;
        let r = &rows[i];
        Ok(Some(Job {
            id: JobId(r.id),
            kind: r.kind.clone(),
            payload: r.payload.clone(),
            attempts: r.attempts,
            run_at: r.run_at,
        }))
    }

    async fn complete(&self, id: JobId) -> Result<()> {
        self.rows.lock().unwrap().retain(|r| r.id != id.0);
        Ok(())
    }

    async fn fail(&self, id: JobId, _error: &str, policy: RetryPolicy) -> Result<()> {
        let mut rows = self.rows.lock().unwrap();
        if let Some(r) = rows.iter_mut().find(|r| r.id == id.0) {
            match policy {
                RetryPolicy::Retry { delay } => {
                    r.state = "available";
                    r.run_at = OffsetDateTime::now_utc() + delay;
                    r.locked_at = None;
                }
                RetryPolicy::Abandon => {
                    r.state = "failed";
                    r.locked_at = None;
                }
            }
        }
        Ok(())
    }

    async fn heartbeat(&self, id: JobId) -> Result<()> {
        let mut rows = self.rows.lock().unwrap();
        if let Some(r) = rows.iter_mut().find(|r| r.id == id.0) {
            r.locked_at = Some(OffsetDateTime::now_utc());
        }
        Ok(())
    }

    async fn await_jobs(&self, _kinds: &[String], timeout: Duration) -> Result<()> {
        // notify_waiters only wakes already-registered waiters; a notification
        // racing ahead of `notified()` is intentionally lost — the `timeout`
        // polling fallback bounds the resulting latency (same contract as pg).
        let _ = tokio::time::timeout(timeout, self.notify.notified()).await;
        Ok(())
    }
}

#[async_trait]
impl ControlPlane for MemoryControlPlane {
    async fn begin(&self) -> Result<Box<dyn Tx + Send>> {
        Ok(Box::new(MemoryTx {
            rows: self.rows.clone(),
            notify: self.notify.clone(),
            lineage: self.lineage.clone(),
            staged: Vec::new(),
            staged_events: Vec::new(),
        }))
    }
}

struct MemoryTx {
    rows: Arc<Mutex<Vec<Row>>>,
    notify: Arc<Notify>,
    lineage: Arc<Mutex<LineageState>>,
    staged: Vec<(Uuid, NewJob)>,
    staged_events: Vec<LineageEvent>,
}

#[async_trait]
impl Tx for MemoryTx {
    async fn commit(self: Box<Self>) -> Result<()> {
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
        Ok(())
    }
    async fn rollback(self: Box<Self>) -> Result<()> {
        Ok(())
    }
    async fn enqueue(&mut self, job: NewJob) -> Result<JobId> {
        let id = Uuid::new_v4();
        self.staged.push((id, job));
        Ok(JobId(id))
    }
    async fn emit(&mut self, event: LineageEvent) -> Result<()> {
        self.staged_events.push(event);
        Ok(())
    }
}
