use async_trait::async_trait;
use control_plane_core::{BucketOffsets, ControlPlaneError, Result, StreamTables};

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
            .or_insert(bucket_count);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn stream_bucket_count(&self, table_id: i64) -> Result<Option<i32>> {
        Ok(self.stream_tables.lock().get(&table_id).copied())
    }
}
