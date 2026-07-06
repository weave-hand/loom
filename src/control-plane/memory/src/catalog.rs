use std::collections::HashMap;

use async_trait::async_trait;
use control_plane_core::{
    Catalog, ColumnDef, ControlPlaneError, FileRef, Page, PageReq, Result, Snapshot, SnapshotId,
    TableRef, TableSchema,
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
}

#[async_trait]
impl Catalog for MemoryControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot> {
        let cat = self.catalog.lock();
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
    async fn snapshots(&self, table: &TableRef, _page: PageReq) -> Result<Page<Snapshot>> {
        let cat = self.catalog.lock();
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
        let live = cat.tables.get(table).is_some_and(|t| t.live_at(at.0));
        if !live {
            return Err(ControlPlaneError::NotFound(format!(
                "{}.{} @ {}",
                table.schema, table.name, at.0
            )));
        }
        let mut cols: Vec<ColumnDef> = cat
            .columns
            .get(table)
            .into_iter()
            .flatten()
            .filter(|c| c.live_at(at.0))
            .map(|c| c.val.clone())
            .collect();
        cols.sort_by_key(|c| c.order);
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
}
