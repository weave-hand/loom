//! Where a newly registered micro-batch MV starts reading its source
//! (iss-mv-register-below-reclaimed-floor).
//!
//! A brand-new MV has no `stream.mv_watermark` rows, so `mv_floor` defaults it to 0 in every
//! bucket and `mv_delta_scan` defines its first delta as "the source from offset 0". Both are
//! wrong for a source whose prefix is already gone: the floor pins the source's GC at 0 until
//! the MV first runs (holding every end-capped byte for a reader that can never want it), and
//! the run itself cannot even commit — the delta's observed minimum offset is above 0, and the
//! watermark CAS has no row to advance (`crate::stream::pg_advance_mv_watermark`), so the run
//! Conflicts and rolls back, forever.
//!
//! This module computes where the MV should actually start — the source's EARLIEST SURVIVING
//! offset per bucket, over data LIVE AT THE CURRENT TIP (`end_snapshot is null`), which is
//! exactly the set `mv_delta_scan` can read — and `define_transform` writes it into
//! `stream.mv_watermark` as the MV's recorded starting position (`start_offset`).
//!
//! ## The rounding rule: DOWN, NEVER UP
//! An offset ABOVE the true earliest surviving row would make the MV skip live rows — a silent
//! data hole. An offset BELOW it only makes the first delta scan a range where nothing survives.
//! So every uncertainty resolves DOWNWARD, to 0:
//!
//! - a live file with no `loom_offset` min stat: we cannot prove where it starts ⇒ 0;
//! - a live inline row with a NULL `loom_bucket`/`loom_offset` (written before the table was
//!   declared a stream — see `#iss-mv-floor-holds-pre-declaration-files`) ⇒ 0.
//!
//! This fail-safe direction is the INVERSE of [`crate::mv_floor`]'s, which guards a `max` and so
//! resolves a missing stat UPWARD (hold the file). Same principle — never let a missing stat
//! cause data loss — opposite direction, because one bounds a reclaim and the other bounds a read.
//!
//! ## Precision, and why the CAS had to relax
//! Exact per-bucket for the inline tier, and for a single-bucket Parquet file (`loom_bucket` min
//! == max). Flush does not partition by bucket (`crate::iceberg_flush`), so a flushed file
//! generally SPANS buckets and its `loom_offset` min is a cross-bucket bound: it lowers EVERY
//! bucket's start, below that bucket's true min. A watermark strictly below the delta's first
//! surviving offset is therefore NORMAL here — which is precisely why the CAS accepts
//! `next_offset <= from` rather than demanding equality (`crate::stream`).

use std::collections::BTreeMap;

use control_plane_core::Result;
use sqlx::PgConnection;

use crate::backend;
use crate::iceberg_inline::{inline_table_exists, inline_table_name};
use crate::stream::pg_peek_offset;

/// Lower `slot` to `v` (or seed it) — the only way a cross-bucket candidate is recorded, so the
/// bound can only ever move DOWN.
fn lower(slot: &mut Option<i64>, v: i64) {
    *slot = Some(slot.map_or(v, |cur| cur.min(v)));
}

/// The same, for a per-bucket candidate.
fn lower_exact(map: &mut BTreeMap<i32, i64>, bucket: i32, v: i64) {
    map.entry(bucket)
        .and_modify(|cur| *cur = (*cur).min(v))
        .or_insert(v);
}

/// The earliest offset still LIVE in each bucket of stream table `tid`, over both storage tiers,
/// rounded down (see the module docs). Every bucket in `0..bucket_count` is present. A bucket
/// with no live data at all takes the allocator's high-water mark
/// (`stream.bucket_offset.next`): nothing survives to be read, so the only start that skips
/// nothing is the end.
///
/// # Errors
/// Propagates any backend error from the mirror/inline/stream reads.
pub async fn earliest_surviving_offsets(
    conn: &mut PgConnection,
    tid: i64,
    bucket_count: i32,
) -> Result<BTreeMap<i32, i64>> {
    // Candidates that apply to ONE bucket (exact), and candidates that apply to EVERY bucket
    // (a cross-bucket file's min, or an unprovable stat's 0).
    let mut exact: BTreeMap<i32, i64> = BTreeMap::new();
    let mut cross: Option<i64> = None;

    // ---- file tier ---------------------------------------------------------
    // Live files only: an end-capped file is already invisible to `mv_delta_scan`, which reads at
    // the current snapshot. Bounds are stored as text and re-typed here — the cast sits OUTSIDE
    // the scalar subquery for the same reason `iceberg_gc.rs` documents.
    let files = sqlx::query!(
        "select \
           (select cs.min_value from iceberg_mirror.data_file_column_stat cs \
              where cs.data_file_id = df.data_file_id \
                and cs.column_name = 'loom_offset')::bigint as \"off_min?\", \
           (select cs.min_value from iceberg_mirror.data_file_column_stat cs \
              where cs.data_file_id = df.data_file_id \
                and cs.column_name = 'loom_bucket')::int as \"bkt_min?\", \
           (select cs.max_value from iceberg_mirror.data_file_column_stat cs \
              where cs.data_file_id = df.data_file_id \
                and cs.column_name = 'loom_bucket')::int as \"bkt_max?\" \
         from iceberg_mirror.data_file df \
         where df.table_id = $1 and df.end_snapshot is null",
        tid,
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(backend)?;

    for f in files {
        let Some(off) = f.off_min else {
            // No `loom_offset` stat: we cannot prove where this file starts. Round down.
            lower(&mut cross, 0);
            continue;
        };
        match (f.bkt_min, f.bkt_max) {
            // A single-bucket file: its offset min is that bucket's, exactly.
            (Some(lo), Some(hi)) if lo == hi => lower_exact(&mut exact, lo, off),
            // Spans buckets (or carries no bucket stat): the min is a cross-bucket bound.
            _ => lower(&mut cross, off),
        }
    }

    // ---- inline tier -------------------------------------------------------
    // Exact per bucket. The `inline_<tid>` identifier is dynamic (hence `AssertSqlSafe`); `tid`
    // comes from our own mirror, never user input — the same pattern as `mv_floor::removal_blocked`.
    if inline_table_exists(&mut *conn, tid).await? {
        let inline = inline_table_name(tid);
        let rows: Vec<(i32, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "select loom_bucket, min(loom_offset) from {inline} \
             where end_snapshot is null \
               and loom_bucket is not null and loom_offset is not null \
             group by loom_bucket"
        )))
        .fetch_all(&mut *conn)
        .await
        .map_err(backend)?;
        for (bucket, off) in rows {
            lower_exact(&mut exact, bucket, off);
        }

        // An UNFRAMED live row — written before this table was declared a stream, so it carries
        // no bucket/offset at all. We cannot place it, so we cannot prove any bucket starts above
        // 0. Round down. (The read-side twin of `#iss-mv-floor-holds-pre-declaration-files`.)
        let unframed: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "select exists(select 1 from {inline} \
             where end_snapshot is null \
               and (loom_bucket is null or loom_offset is null))"
        )))
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
        if unframed {
            lower(&mut cross, 0);
        }
    }

    // ---- fold --------------------------------------------------------------
    let mut out = BTreeMap::new();
    for bucket in 0..bucket_count {
        let candidate = match (exact.get(&bucket).copied(), cross) {
            (Some(a), Some(c)) => Some(a.min(c)),
            (Some(a), None) => Some(a),
            (None, Some(c)) => Some(c),
            (None, None) => None,
        };
        let start = match candidate {
            Some(v) => v,
            None => pg_peek_offset(&mut *conn, tid, bucket).await?,
        };
        out.insert(bucket, start);
    }
    Ok(out)
}
