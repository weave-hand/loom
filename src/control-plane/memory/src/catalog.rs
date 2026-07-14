use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use control_plane_core::{
    Catalog, ColumnDef, ControlPlaneError, FileRef, Page, PageReq, Result, Snapshot, SnapshotId,
    TableRef, TableSchema, ViewDef, validate_view_shape, view_definition_event,
};
use time::OffsetDateTime;

use crate::{MemoryControlPlane, Versioned};

#[derive(Default)]
pub(crate) struct CatalogState {
    pub(crate) next_snapshot: i64,
    pub(crate) snapshots: Vec<Snapshot>,
    pub(crate) tables: HashMap<TableRef, Versioned<()>>,
    pub(crate) columns: HashMap<TableRef, Vec<Versioned<ColumnDef>>>,
    pub(crate) files: HashMap<TableRef, Vec<Versioned<FileRef>>>,
    /// Virtual datasets, keyed by `(schema, name)` (`TableRef` has no `Ord`) so
    /// iteration is `(schema, name)`-ordered for free.
    pub(crate) views: BTreeMap<(String, String), ViewDef>,
}

impl CatalogState {
    pub(crate) fn new_snapshot(&mut self) -> i64 {
        let id = self.next_snapshot;
        self.next_snapshot += 1;
        self.snapshots.push(Snapshot {
            id: SnapshotId(id),
            time: OffsetDateTime::now_utc(),
            schema_version: 0,
        });
        id
    }

    fn latest_live(&self, key: &TableRef) -> Option<i64> {
        let t = self.tables.get(key)?;
        self.snapshots
            .iter()
            .rev()
            .map(|s| s.id.0)
            .find(|&s| t.live_at(s))
    }

    /// Resolve `table` through a view definition to its physical base, carrying
    /// along the view's column projection (only `schema()` applies it). A ref
    /// that is not a view resolves to itself with no projection.
    fn resolve_view(&self, table: &TableRef) -> (TableRef, Option<Vec<String>>) {
        match self.views.get(&(table.schema.clone(), table.name.clone())) {
            Some(v) => (v.base.clone(), v.columns.clone()),
            None => (table.clone(), None),
        }
    }

    /// Whether `key` is a live physical table (present in `tables`, no end-cap).
    fn is_live_table(&self, key: &TableRef) -> bool {
        self.tables.get(key).is_some_and(|t| t.end.is_none())
    }

    /// Collect `table`'s live column schema, base-order (mirrors `Catalog::schema`'s
    /// column collection, minus the liveness gate the caller already checked).
    fn live_columns(&self, table: &TableRef, at: i64) -> Vec<ColumnDef> {
        let mut cols: Vec<ColumnDef> = self
            .columns
            .get(table)
            .into_iter()
            .flatten()
            .filter(|c| c.live_at(at))
            .map(|c| c.val.clone())
            .collect();
        cols.sort_by_key(|c| c.order);
        cols
    }
}

#[async_trait]
impl Catalog for MemoryControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot> {
        let cat = self.catalog.lock();
        let (table, _) = cat.resolve_view(table);
        let table = &table;
        let s = cat.latest_live(table).ok_or_else(|| {
            ControlPlaneError::NotFound(format!("{}.{}", table.schema, table.name))
        })?;
        cat.snapshots
            .iter()
            .find(|sn| sn.id.0 == s)
            .cloned()
            .ok_or_else(|| ControlPlaneError::NotFound(format!("{}.{}", table.schema, table.name)))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn snapshot_as_of(
        &self,
        table: &TableRef,
        ts: OffsetDateTime,
    ) -> Result<Option<Snapshot>> {
        let cat = self.catalog.lock();
        let (table, _) = cat.resolve_view(table);
        let table = &table;
        let Some(t) = cat.tables.get(table) else {
            return Ok(None);
        };
        // Latest (highest id) snapshot with time <= ts at which the table is live.
        Ok(cat
            .snapshots
            .iter()
            .rev()
            .find(|sn| sn.time <= ts && t.live_at(sn.id.0))
            .cloned())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn snapshot(&self, table: &TableRef, id: SnapshotId) -> Result<Option<Snapshot>> {
        let cat = self.catalog.lock();
        let (table, _) = cat.resolve_view(table);
        let table = &table;
        let Some(t) = cat.tables.get(table) else {
            return Ok(None);
        };
        Ok(cat
            .snapshots
            .iter()
            .find(|sn| sn.id == id && t.live_at(sn.id.0))
            .cloned())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn snapshot_horizon(&self, cutoff: OffsetDateTime) -> Result<Option<SnapshotId>> {
        let cat = self.catalog.lock();
        Ok(cat
            .snapshots
            .iter()
            .filter(|sn| sn.time < cutoff)
            .map(|sn| sn.id)
            .max())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn snapshot_intact(
        &self,
        table: &TableRef,
        at: SnapshotId,
        horizon: SnapshotId,
    ) -> Result<bool> {
        let cat = self.catalog.lock();
        let (table, _) = cat.resolve_view(table);
        let table = &table;
        let live = cat.tables.get(table).is_some_and(|t| t.live_at(at.0));
        if !live {
            return Err(ControlPlaneError::NotFound(format!(
                "{}.{} @ {}",
                table.schema, table.name, at.0
            )));
        }
        // Clause 1 is vacuous in the fake: it models no GC, so nothing is ever physically
        // reclaimed and its watermark is permanently 0 (`at >= 0` always holds). That is
        // faithful, not a stub — there is nothing to be blind to.
        //
        // Clause 2 is the whole verdict: no file visible at `at` may be eligible for
        // reclaim (`begin <= at < end <= horizon`). The fake has no inline tier, so data
        // files are the only tier there is.
        let eligible = cat
            .files
            .get(table)
            .into_iter()
            .flatten()
            .any(|f| f.begin <= at.0 && f.end.is_some_and(|e| e > at.0 && e <= horizon.0));
        Ok(!eligible)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn snapshots(&self, table: &TableRef, _page: PageReq) -> Result<Page<Snapshot>> {
        let cat = self.catalog.lock();
        let (table, _) = cat.resolve_view(table);
        let table = &table;
        let t = cat.tables.get(table).ok_or_else(|| {
            ControlPlaneError::NotFound(format!("{}.{}", table.schema, table.name))
        })?;
        Ok(Page::from_full(
            cat.snapshots
                .iter()
                .filter(|sn| t.live_at(sn.id.0))
                .cloned()
                .collect(),
        ))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn files(
        &self,
        table: &TableRef,
        at: SnapshotId,
        _page: PageReq,
    ) -> Result<Page<FileRef>> {
        let cat = self.catalog.lock();
        let (table, _) = cat.resolve_view(table);
        let table = &table;
        let live = cat.tables.get(table).is_some_and(|t| t.live_at(at.0));
        if !live {
            return Err(ControlPlaneError::NotFound(format!(
                "{}.{} @ {}",
                table.schema, table.name, at.0
            )));
        }
        Ok(Page::from_full(
            cat.files
                .get(table)
                .into_iter()
                .flatten()
                .filter(|f| f.live_at(at.0))
                .map(|f| f.val.clone())
                .collect(),
        ))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema> {
        let cat = self.catalog.lock();
        let (table, projection) = cat.resolve_view(table);
        let table = &table;
        let live = cat.tables.get(table).is_some_and(|t| t.live_at(at.0));
        if !live {
            return Err(ControlPlaneError::NotFound(format!(
                "{}.{} @ {}",
                table.schema, table.name, at.0
            )));
        }
        let mut cols = cat.live_columns(table, at.0);
        if let Some(proj) = projection {
            cols.retain(|c| proj.contains(&c.name));
        }
        Ok(TableSchema { columns: cols })
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_tables(&self, _page: PageReq) -> Result<Page<TableRef>> {
        let cat = self.catalog.lock();
        // Live = no end-cap, mirroring the pg adapter's `end_snapshot is null`.
        let mut live: Vec<TableRef> = cat
            .tables
            .iter()
            .filter(|(_, v)| v.end.is_none())
            .map(|(t, _)| t.clone())
            .collect();
        live.sort_by(|a, b| (&a.schema, &a.name).cmp(&(&b.schema, &b.name)));
        Ok(Page::from_full(live))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_view(&self, view: ViewDef) -> Result<()> {
        // Validate under the lock, then emit lineage after (memory lineage
        // takes its own lock — mirror how define_type emits its binding event).
        {
            let mut cat = self.catalog.lock();
            let vkey = (view.view.schema.clone(), view.view.name.clone());
            if cat.views.contains_key(&vkey) {
                return Err(ControlPlaneError::Conflict(format!(
                    "view {}.{} already exists",
                    view.view.schema, view.view.name
                )));
            }
            if cat.is_live_table(&view.view) {
                return Err(ControlPlaneError::Conflict(format!(
                    "{}.{} is a physical table",
                    view.view.schema, view.view.name
                )));
            }
            let bkey = (view.base.schema.clone(), view.base.name.clone());
            if cat.views.contains_key(&bkey) {
                return Err(ControlPlaneError::Validation(
                    "view-over-view is not supported".into(),
                ));
            }
            if !cat.is_live_table(&view.base) {
                return Err(ControlPlaneError::NotFound(format!(
                    "{}.{}",
                    view.base.schema, view.base.name
                )));
            }
            let base_at = cat.latest_live(&view.base).ok_or_else(|| {
                ControlPlaneError::NotFound(format!("{}.{}", view.base.schema, view.base.name))
            })?;
            let base_schema = TableSchema {
                columns: cat.live_columns(&view.base, base_at),
            };
            validate_view_shape(&view, &base_schema).map_err(ControlPlaneError::Validation)?;
            cat.views.insert(vkey, view.clone());
        }
        // Best-effort lineage emission, same pattern as `define_type`'s binding
        // event: appended outside the catalog lock, under lineage's own lock.
        self.lineage
            .lock()
            .events
            .push(view_definition_event(&view));
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn drop_view(&self, view: &TableRef) -> Result<()> {
        let mut cat = self.catalog.lock();
        let vkey = (view.schema.clone(), view.name.clone());
        if cat.views.remove(&vkey).is_none() {
            return Err(ControlPlaneError::NotFound(format!(
                "{}.{}",
                view.schema, view.name
            )));
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn get_view(&self, view: &TableRef) -> Result<Option<ViewDef>> {
        let cat = self.catalog.lock();
        Ok(cat
            .views
            .get(&(view.schema.clone(), view.name.clone()))
            .cloned())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_views(&self, _page: PageReq) -> Result<Page<ViewDef>> {
        let cat = self.catalog.lock();
        // BTreeMap iteration is already (schema, name)-ordered.
        Ok(Page::from_full(cat.views.values().cloned().collect()))
    }
}
