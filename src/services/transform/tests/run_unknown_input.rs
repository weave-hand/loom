//! Edge 1: a missing input that surfaces at `Catalog::files` (a drop-race between
//! current_snapshot and files) must classify as `UnknownInput` (-> Abandon), not
//! `ControlPlane` (-> Retry forever). Uses a hand-rolled control-plane double so the
//! NotFound lands at `files`, which a real fixture cannot deterministically produce.

use std::sync::Arc;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Auth, Catalog, ControlPlane, ControlPlaneError, EventType, FileRef, Lineage, LineageEvent,
    Ontology, Page, PageReq, Queue, RunId, Snapshot, SnapshotId, TableControlPlane, TableRef,
    TableSchema, TableTx, Tx,
};
use datafusion_io::WriteConfig;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use transform::{OutputMode, TransformError, TransformInput, TransformRequest, run_transform};

/// A catalog where the table is live at `current_snapshot` but vanishes by the time
/// `files` is read — the dropped-between-reads race. `schema`/`snapshots` are never
/// reached (the `files` error precedes them).
struct FilesNotFoundCatalog;

#[async_trait]
impl Catalog for FilesNotFoundCatalog {
    async fn current_snapshot(&self, _t: &TableRef) -> control_plane_core::Result<Snapshot> {
        Ok(Snapshot {
            id: SnapshotId(1),
            time: time::OffsetDateTime::UNIX_EPOCH,
            schema_version: 0,
        })
    }
    async fn snapshots(
        &self,
        _t: &TableRef,
        _p: PageReq,
    ) -> control_plane_core::Result<Page<Snapshot>> {
        unreachable!("snapshots not read by run_transform")
    }
    async fn files(
        &self,
        _t: &TableRef,
        _at: SnapshotId,
        _p: PageReq,
    ) -> control_plane_core::Result<Page<FileRef>> {
        Err(ControlPlaneError::NotFound("dropped between reads".into()))
    }
    async fn schema(
        &self,
        _t: &TableRef,
        _at: SnapshotId,
    ) -> control_plane_core::Result<TableSchema> {
        unreachable!("schema not reached — files errors first")
    }
}

struct StubCp {
    catalog: FilesNotFoundCatalog,
}

#[async_trait]
impl ControlPlane for StubCp {
    fn catalog(&self) -> &(dyn Catalog + Send + Sync) {
        &self.catalog
    }
    fn ontology(&self) -> &(dyn Ontology + Send + Sync) {
        unreachable!("ontology not used by run_transform input resolution")
    }
    fn acl(&self) -> &(dyn Acl + Send + Sync) {
        unreachable!("acl not used by run_transform input resolution")
    }
    fn lineage(&self) -> &(dyn Lineage + Send + Sync) {
        unreachable!("lineage not used by run_transform input resolution")
    }
    fn queue(&self) -> &(dyn Queue + Send + Sync) {
        unreachable!("queue not used by run_transform input resolution")
    }
    fn auth(&self) -> &(dyn Auth + Send + Sync) {
        unreachable!("auth not used by run_transform input resolution")
    }
    async fn begin(&self) -> control_plane_core::Result<Box<dyn Tx + Send>> {
        unreachable!("begin not reached — input resolution fails first")
    }
}

#[async_trait]
impl TableControlPlane for StubCp {
    async fn begin_table(&self) -> control_plane_core::Result<Box<dyn TableTx + Send>> {
        unreachable!("begin not reached — input resolution fails first")
    }
}

#[tokio::test]
async fn missing_input_at_files_is_unknown_input() {
    let cp = StubCp {
        catalog: FilesNotFoundCatalog,
    };
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let table = TableRef {
        schema: "main".into(),
        name: "gone".into(),
    };
    let input = TransformInput {
        table: &table,
        register_as: "gone",
    };
    let output = TableRef {
        schema: "main".into(),
        name: "out".into(),
    };
    let lineage = LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::UNIX_EPOCH,
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({}),
    };
    let res = run_transform(
        &cp,
        store,
        "file:///tmp/loom-test-wh",
        &WriteConfig::default(),
        "run-x",
        TransformRequest {
            inputs: &[input],
            output: &output,
            sql: "SELECT 1",
            conform: None,
            output_mode: OutputMode::Append,
            lineage,
        },
    )
    .await;
    match res {
        Err(TransformError::UnknownInput(s, n)) => {
            assert_eq!(s, "main");
            assert_eq!(n, "gone");
        }
        other => panic!("expected UnknownInput, got {other:?}"),
    }
}
