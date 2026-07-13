//! The MV read-position floor: how far GC may reclaim a micro-batch MV's SOURCE
//! table without eating offsets the MV has not consumed yet
//! (road-mv-watermark-aware-gc).
//!
//! `stream.mv_watermark(mv, source_table_id, bucket, next_offset)` records the
//! next unprocessed `loom_offset` per MV per bucket. A bucket with NO row for an
//! MV has been consumed not at all by it — `mv_delta_scan` reads such a bucket
//! from 0 (`engine-serving/src/mv_delta.rs`), so the floor must too. A bucket's
//! floor is therefore the MINIMUM, across every MV reading the source, of (that
//! MV's watermark for the bucket, or 0), and GC may not reclaim a row or file
//! carrying an offset at or above it.
//!
//! ## Who reads a source
//! The union of (a) every registered `MicroBatch`/`MicroBatchJoin` transform def
//! whose `source` is the table (keyed by `mv_key(output)`) — this is what makes a
//! registered-but-never-run MV pin its source at 0 — and (b) every `mv` holding a
//! watermark row against the source (an ad-hoc run, or one mid-deletion).
//! Deleting an MV's transform def deletes its watermark rows in the same
//! transaction (`Transforms::delete_transform`), so dropping the registration is a
//! real escape hatch out of a floor held by a dead MV.

use std::collections::{BTreeMap, BTreeSet};

use control_plane_core::{Result, TableRef};
use sqlx::PgPool;

use crate::backend;
use crate::stream::pg_stream_bucket_count;
use crate::transforms::pg_micro_batch_readers;

/// One source table's reclaim floor: the per-bucket next-offset below which GC
/// may reclaim, plus the MV that set each bucket's floor (the laggard — the
/// operator's lead to a wedged MV).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvFloor {
    /// `bucket -> floor`. Every bucket of the source is present (`0..bucket_count`).
    pub per_bucket: BTreeMap<i32, i64>,
    /// `bucket -> the mv key whose watermark set that bucket's floor`.
    pub slowest: BTreeMap<i32, String>,
}

impl MvFloor {
    /// The single bound FILE-granular reclaim may use. Per-file column stats are
    /// not per-bucket, so a file is reclaimable only strictly below the SMALLEST
    /// floor across every bucket — conservative by construction: it can only ever
    /// hold a file longer, never reclaim one it should not. An empty map yields 0,
    /// i.e. "hold everything" — the fail-safe direction.
    #[must_use]
    pub fn min_offset(&self) -> i64 {
        self.per_bucket.values().copied().min().unwrap_or(0)
    }
}

/// The reclaim floor for the live incarnation `tid` of `table`, or `None` — the
/// fast path — when `table` is not a declared stream table or no MV reads it. A
/// `None` floor leaves every GC predicate byte-identical to the pre-floor
/// behavior.
pub async fn mv_floor(pool: &PgPool, table: &TableRef, tid: i64) -> Result<Option<MvFloor>> {
    let Some(bucket_count) = pg_stream_bucket_count(pool, tid).await? else {
        return Ok(None);
    };
    let readers = mv_readers(pool, table, tid).await?;
    if readers.is_empty() {
        return Ok(None);
    }

    let rows = sqlx::query!(
        "select mv, bucket, next_offset from stream.mv_watermark where source_table_id = $1",
        tid,
    )
    .fetch_all(pool)
    .await
    .map_err(backend)?;
    let mut wm: BTreeMap<String, BTreeMap<i32, i64>> = BTreeMap::new();
    for r in rows {
        wm.entry(r.mv).or_default().insert(r.bucket, r.next_offset);
    }

    let mut per_bucket = BTreeMap::new();
    let mut slowest = BTreeMap::new();
    for bucket in 0..bucket_count {
        // `readers` is non-empty and sorted (BTreeSet), so this always sets both
        // values, and ties resolve to the first mv key in sort order — the log
        // names the same laggard run to run.
        let mut floor = 0i64;
        let mut who = String::new();
        let mut first = true;
        for mv in &readers {
            let next = wm
                .get(mv)
                .and_then(|buckets| buckets.get(&bucket))
                .copied()
                .unwrap_or(0);
            if first || next < floor {
                floor = next;
                who.clone_from(mv);
                first = false;
            }
        }
        per_bucket.insert(bucket, floor);
        slowest.insert(bucket, who);
    }

    Ok(Some(MvFloor {
        per_bucket,
        slowest,
    }))
}

/// The MVs a full reclaim of dropped incarnations `tids` of `table` would strand:
/// any MV still holding watermarks against a dropped incarnation, plus — only when
/// `include_registered` — the registered readers of the `(schema, name)`. Drives the
/// dropped-source warning; the reclaim itself deliberately proceeds (the operator
/// dropped the source, so its MVs are dead by definition; wedging drop-GC on a dead
/// MV forever is strictly worse).
///
/// `include_registered` exists because registration keys on `(schema, name)` and
/// cannot tell incarnations apart: after a DROP-and-RECREATE, an MV happily reading
/// the NEW table would otherwise be named "stranded" on every GC run while the old
/// incarnations drain. The caller passes `live.is_none()` — no live incarnation, so
/// a registered reader really is reading nothing.
pub async fn stranded_mv_readers(
    pool: &PgPool,
    table: &TableRef,
    tids: &[i64],
    include_registered: bool,
) -> Result<BTreeSet<String>> {
    let mut out = if include_registered {
        pg_micro_batch_readers(pool, table).await?
    } else {
        BTreeSet::new()
    };
    for tid in tids {
        out.extend(watermark_mvs(pool, *tid).await?);
    }
    Ok(out)
}

/// Registered MV readers of `table` ∪ MVs with watermark rows against `tid`.
async fn mv_readers(pool: &PgPool, table: &TableRef, tid: i64) -> Result<BTreeSet<String>> {
    let mut out = pg_micro_batch_readers(pool, table).await?;
    out.extend(watermark_mvs(pool, tid).await?);
    Ok(out)
}

/// Distinct `mv` keys holding a watermark row against source `tid`.
async fn watermark_mvs(pool: &PgPool, tid: i64) -> Result<Vec<String>> {
    sqlx::query_scalar!(
        "select distinct mv from stream.mv_watermark where source_table_id = $1",
        tid,
    )
    .fetch_all(pool)
    .await
    .map_err(backend)
}
