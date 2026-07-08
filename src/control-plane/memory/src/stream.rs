use async_trait::async_trait;
use control_plane_core::{
    BucketOffsets, ControlPlaneError, MergeEngine, Result, StreamKind, StreamMeta, StreamTables,
};

use crate::MemoryControlPlane;

#[async_trait]
impl BucketOffsets for MemoryControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn allocate_offset(&self, table_id: i64, bucket: i32, count: i64) -> Result<i64> {
        if count <= 0 {
            return Err(ControlPlaneError::Validation(format!(
                "allocate_offset requires count > 0, got {count}"
            )));
        }
        let mut map = self.offsets.lock();
        let next = map.entry((table_id, bucket)).or_insert(0);
        let first = *next;
        *next += count;
        Ok(first)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn peek_offset(&self, table_id: i64, bucket: i32) -> Result<i64> {
        Ok(self
            .offsets
            .lock()
            .get(&(table_id, bucket))
            .copied()
            .unwrap_or(0))
    }
}

#[async_trait]
impl StreamTables for MemoryControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn declare_stream(&self, table_id: i64, bucket_count: i32) -> Result<()> {
        // Idempotent, first-wins: only insert if absent.
        self.stream_tables
            .lock()
            .entry(table_id)
            .or_insert(StreamMeta {
                bucket_count,
                kind: StreamKind::Log,
                bucket_key: None,
                changelog_table_id: None,
                merge_engine: MergeEngine::LastRow,
            });
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn declare_cdc(
        &self,
        table_id: i64,
        bucket_count: i32,
        bucket_key: &str,
        merge_engine: MergeEngine,
    ) -> Result<()> {
        self.stream_tables
            .lock()
            .entry(table_id)
            .or_insert(StreamMeta {
                bucket_count,
                kind: StreamKind::Cdc,
                bucket_key: Some(bucket_key.to_string()),
                changelog_table_id: None,
                merge_engine,
            });
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn stream_bucket_count(&self, table_id: i64) -> Result<Option<i32>> {
        Ok(self
            .stream_tables
            .lock()
            .get(&table_id)
            .map(|m| m.bucket_count))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn stream_meta(&self, table_id: i64) -> Result<Option<StreamMeta>> {
        Ok(self.stream_tables.lock().get(&table_id).cloned())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn set_changelog_table_id(&self, table_id: i64, changelog_table_id: i64) -> Result<()> {
        if let Some(m) = self.stream_tables.lock().get_mut(&table_id) {
            m.changelog_table_id = Some(changelog_table_id);
        }
        Ok(())
    }
}
