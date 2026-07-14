use async_trait::async_trait;
use control_plane_core::{
    BucketOffsets, ControlPlaneError, MergeEngine, MvWatermarks, Result, StreamKind, StreamMeta,
    StreamTables, WatermarkAdvance,
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

#[async_trait]
impl MvWatermarks for MemoryControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn mv_watermarks(
        &self,
        mv: &str,
        source_table_id: i64,
    ) -> Result<std::collections::BTreeMap<i32, i64>> {
        Ok(self
            .mv_watermarks
            .lock()
            .iter()
            .filter(|((m, sid, _), _)| m == mv && *sid == source_table_id)
            .map(|((_, _, bucket), offset)| (*bucket, *offset))
            .collect())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn advance_mv_watermark(
        &self,
        mv: &str,
        source_table_id: i64,
        advances: &[WatermarkAdvance],
    ) -> Result<()> {
        let mut map = self.mv_watermarks.lock();
        for adv in advances {
            let key = (mv.to_string(), source_table_id, adv.bucket);
            match map.get(&key).copied() {
                // Mirrors the postgres CAS predicate in `pg_advance_mv_watermark` — read its
                // comment for the full argument. In short: `current <= adv.from` (not `==`)
                // because a rounded-down bootstrap can leave the row BELOW the delta's observed
                // minimum offset; `current < adv.to` because that relaxation alone would let a
                // non-monotone advance REWIND the watermark (`{from: 900, to: 5}` against a row
                // at 100), re-appending the offsets in between on every later run. Together they
                // make monotonicity structural rather than a caller contract, and still refuse a
                // replay (the winner leaves `current = to`, failing both conjuncts).
                Some(current) if current <= adv.from && current < adv.to => {
                    map.insert(key, adv.to);
                }
                None if adv.from == 0 => {
                    map.insert(key, adv.to);
                }
                current => {
                    return Err(ControlPlaneError::Conflict(format!(
                        "mv watermark refused advance {}..{} : {mv} source {source_table_id} \
                         bucket {} — the watermark is either ABOVE `from` (a concurrent run \
                         already covered this delta) or at/above `to` (the advance is not \
                         monotone); found {:?}",
                        adv.from, adv.to, adv.bucket, current
                    )));
                }
            }
        }
        Ok(())
    }
}
