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
    pub(crate) tables: HashMap<(String, String), Versioned<()>>,
    pub(crate) columns: HashMap<(String, String), Vec<Versioned<ColumnDef>>>,
    pub(crate) files: HashMap<(String, String), Vec<Versioned<FileRef>>>,
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

    async fn snapshots(&self, table: &TableRef, _page: PageReq) -> Result<Page<Snapshot>> {
        let cat = self.catalog.lock().unwrap();
        let key = (table.schema.clone(), table.name.clone());
        let t = cat.tables.get(&key).ok_or_else(|| {
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

    async fn files(
        &self,
        table: &TableRef,
        at: SnapshotId,
        _page: PageReq,
    ) -> Result<Page<FileRef>> {
        let cat = self.catalog.lock().unwrap();
        let key = (table.schema.clone(), table.name.clone());
        let live = cat.tables.get(&key).is_some_and(|t| t.live_at(at.0));
        if !live {
            return Err(ControlPlaneError::NotFound(format!(
                "{}.{} @ {}",
                table.schema, table.name, at.0
            )));
        }
        Ok(Page::from_full(
            cat.files
                .get(&key)
                .into_iter()
                .flatten()
                .filter(|f| f.live_at(at.0))
                .map(|f| f.val.clone())
                .collect(),
        ))
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
