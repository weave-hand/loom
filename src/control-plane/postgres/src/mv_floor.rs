//! The MV read-position floor: the per-bucket offset below which GC may reclaim a
//! micro-batch MV's SOURCE table (road-mv-watermark-aware-gc).
//!
//! `stream.mv_watermark(mv, source_table_id, bucket, next_offset)` records the
//! next unprocessed `loom_offset` per MV per bucket. A bucket with NO row for an
//! MV has been consumed not at all by it — `mv_delta_scan` reads such a bucket
//! from 0 (`engine-serving/src/mv_delta.rs`), so the floor must too. A bucket's
//! floor is therefore the MINIMUM, across every MV reading the source, of (that
//! MV's watermark for the bucket, or 0), and a guarded caller may not reclaim (or
//! end-cap) a row or file carrying an offset at or above it.
//!
//! ## What this delivers — and what it does not
//! On GC (`crate::iceberg_gc`, its only caller today) the floor is **byte-retention
//! defense**: a lagging MV's end-capped bytes are not physically destroyed while it
//! is behind, and the hold is counted (`GcSummary.held_by_mv_floor`) and logged with
//! the laggard's name. It does NOT stand between an MV and a data hole, and must not
//! be described as one — GC only ever reclaims END-CAPPED rows, which an MV's
//! current-snapshot delta can no longer read anyway. This module's other half is
//! being the **seam** the END-CAP-issuing paths call before they end-cap an offset
//! an MV has not consumed (that is where the hole is actually created) — see
//! [`EndCapIntent`] / [`guard_end_cap`] below.
//!
//! ## Who reads a source
//! The union of (a) every registered `MicroBatch`/`MicroBatchJoin` transform def
//! whose `source` is the table (keyed by `mv_key(output)`) — this is what makes a
//! registered-but-never-run MV pin its source at 0 — and (b) every `mv` holding a
//! watermark row against the source (an ad-hoc run, or one mid-deletion).
//! Un-registering an MV releases its floor: both `Transforms::delete_transform` and
//! a `define_transform` that redefines the MV off its output delete that output's
//! watermark rows in the same transaction, so no key can outlive every def that
//! names it (a ghost key would floor the source forever — nothing could reach it).

use std::collections::{BTreeMap, BTreeSet};

use control_plane_core::{ControlPlaneError, Result, TableRef};
use sqlx::PgConnection;

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

    /// The bare OR-chain predicate selecting inline rows STRICTLY BELOW this floor,
    /// per bucket — `(loom_bucket = B and loom_offset < F)` for each bucket whose
    /// floor is above 0 (a bucket at floor 0 contributes no clause: nothing in it is
    /// reclaimable), joined with `or`, parenthesized; or the bare string `"false"`
    /// when every bucket's floor is 0.
    ///
    /// This is the SHARED derivation between GC's `delete_end_capped_inline_rows`
    /// (`iceberg_gc.rs`) and the end-cap guard's `removal_blocked` below — the two
    /// MUST stay in lockstep, since GC reclaims exactly what the guard calls
    /// below-floor; if they ever diverged, GC could reclaim rows the guard is still
    /// protecting. Callers splice the returned string into their own dynamic SQL
    /// (each wraps it differently — see the two call sites); the bucket/offset
    /// literals come from our own mirror, never from user input.
    #[must_use]
    pub fn below_floor_pred(&self) -> String {
        let clauses: Vec<String> = self
            .per_bucket
            .iter()
            .filter(|(_, offset)| **offset > 0)
            .map(|(bucket, offset)| format!("(loom_bucket = {bucket} and loom_offset < {offset})"))
            .collect();
        if clauses.is_empty() {
            "false".to_owned()
        } else {
            format!("({})", clauses.join(" or "))
        }
    }
}

/// The reclaim floor for the live incarnation `tid` of `table`, or `None` — the
/// fast path — when `table` is not a declared stream table or no MV reads it. A
/// `None` floor leaves every GC predicate byte-identical to the pre-floor
/// behavior.
pub async fn mv_floor(
    conn: &mut PgConnection,
    table: &TableRef,
    tid: i64,
) -> Result<Option<MvFloor>> {
    let Some(bucket_count) = pg_stream_bucket_count(&mut *conn, tid).await? else {
        return Ok(None);
    };
    let readers = mv_readers(&mut *conn, table, tid).await?;
    if readers.is_empty() {
        return Ok(None);
    }

    let rows = sqlx::query!(
        "select mv, bucket, next_offset from stream.mv_watermark where source_table_id = $1",
        tid,
    )
    .fetch_all(&mut *conn)
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
    conn: &mut PgConnection,
    table: &TableRef,
    tids: &[i64],
    include_registered: bool,
) -> Result<BTreeSet<String>> {
    let mut out = if include_registered {
        pg_micro_batch_readers(&mut *conn, table).await?
    } else {
        BTreeSet::new()
    };
    for tid in tids {
        out.extend(watermark_mvs(&mut *conn, *tid).await?);
    }
    Ok(out)
}

/// Registered MV readers of `table` ∪ MVs with watermark rows against `tid`.
async fn mv_readers(
    conn: &mut PgConnection,
    table: &TableRef,
    tid: i64,
) -> Result<BTreeSet<String>> {
    let mut out = pg_micro_batch_readers(&mut *conn, table).await?;
    out.extend(watermark_mvs(&mut *conn, tid).await?);
    Ok(out)
}

/// Distinct `mv` keys holding a watermark row against source `tid`.
async fn watermark_mvs(conn: &mut PgConnection, tid: i64) -> Result<Vec<String>> {
    sqlx::query_scalar!(
        "select distinct mv from stream.mv_watermark where source_table_id = $1",
        tid,
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(backend)
}

/// The stable prefix of the seam's refusal message (`ControlPlaneError::Validation`,
/// see [`guard_end_cap`]). All five end-cap primitives now require an [`EndCapIntent`]
/// and call `guard_end_cap` before writing, across every end-cap-issuing call site in
/// the tree, so a `Removing` end-cap that would take offsets a micro-batch MV has not
/// read is refused in production today.
///
/// The prefix — not the error VARIANT — is the contract, because the variant does not
/// survive the catalog boundary (`ControlPlaneError::Validation` arrives as `Backend`; see
/// `engine-serving`'s `consolidate::is_mv_floor_refusal`, and the same idiom in
/// `worker/src/stream_mv.rs`'s `classify_*` over gRPC). `consolidate_table` matches on it to
/// turn the refusal into a clean skip-and-re-arm rather than a queue-poisoning error.
pub const MV_FLOOR_REFUSAL_PREFIX: &str = "mv-floor refuses end-cap:";

/// Why a caller is end-capping, for the [`guard_end_cap`]/[`removal_blocked`] seam
/// below. Intent CANNOT be inferred from the SQL: flush and compaction end-cap offsets
/// ABOVE the floor and re-project those same rows at the same `(bucket, offset)`, so a
/// blanket "refuse any end-cap at or above the floor" would break them — the type
/// forces a caller to say which case it is. Threaded through every end-cap-issuing path
/// in the tree: all five end-cap primitives take an `&EndCapIntent` and call
/// `guard_end_cap` with it before writing, so every caller must construct and pass one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EndCapIntent<'a> {
    /// **No row an MV can read leaves the live set.** No floor consult.
    ///
    /// The usual shape is re-projection: the same rows land back in the new live set at
    /// the SAME `(bucket, offset)` — flush (inline rows → live Parquet), plain-coalesce
    /// compaction (small files → one big file).
    ///
    /// **Read the invariant literally: it is about what an MV can READ, not about
    /// re-projecting every row.** The CDC base flush is `Reframing` while dropping the
    /// `-U` before-images from the base (`iceberg_flush::flush_locked_cdc` re-projects
    /// only the `+I/+U/-D` subset), and that is sound for a reason specific to CDC: the
    /// durable changelog retains every `-U` in the SAME transaction, and `mv_delta_scan`
    /// reads LOG sources only (`engine-serving/src/mv_delta.rs` — it hard-refuses a CDC
    /// source), so no MV can ever scan a CDC base.
    ///
    /// So do NOT reason "flush is `Reframing`, therefore a subset re-projection is always
    /// fine". On a LOG stream table — the one an MV actually replays — dropping any live
    /// row IS a removal, and the intent is [`Self::Removing`]. If you are adding a
    /// base-rewriting path, the question to answer is not "do I re-project?" but "can any
    /// MV still read every offset I am retiring?".
    Reframing,
    /// The offsets LEAVE the live set. Must clear the MV floor. The DEFAULT, so a new
    /// caller that starts end-capping without thinking gets the guard.
    #[default]
    Removing,
    /// Deliberate destruction; the floor is bypassed ON PURPOSE and the reason logged.
    /// The catalog drop: the operator dropped the source, so its MVs are dead by
    /// definition, and wedging drop-GC on a dead MV forever is strictly worse than
    /// stranding it (which [`stranded_mv_readers`] warns about).
    Destroying {
        /// Why the destruction is legitimate; logged with the bypass.
        reason: &'a str,
    },
}

/// `Some(floor)` iff REMOVING the live offsets of `tid` would take rows at or above some
/// MV's read position — i.e. iff a `Removing` end-cap must be refused. `None` means
/// nothing blocks: not a declared stream table, no MV reads it, or every live offset is
/// already below the floor.
///
/// Bounds are GC's, reused exactly:
/// - **Files** — [`MvFloor::min_offset`] (the cross-bucket minimum; per-file column stats
///   are not per-bucket) vs the file's `loom_offset` MAX stat. A file with NO stat is HELD
///   (fail-safe). A file straddling the floor cannot be partially end-capped without a
///   rewrite, so it is REFUSED, not filtered.
/// - **Inline rows** — per-bucket precise (`loom_bucket = b and loom_offset < floor_b`).
///
/// The `::bigint` cast lives OUTSIDE the scalar subquery for the same reason it does in
/// `victim_data_files`: `max_value` is `text` holding EVERY column's bound (including
/// string columns), and Postgres may reorder quals inside one `WHERE`.
pub async fn removal_blocked(
    conn: &mut PgConnection,
    table: &TableRef,
    tid: i64,
) -> Result<Option<MvFloor>> {
    let Some(floor) = mv_floor(&mut *conn, table, tid).await? else {
        return Ok(None);
    };

    // File tier: any LIVE file NOT provably below the floor blocks. `coalesce(_, false)`
    // makes a missing `loom_offset` stat block too — the fail-safe direction.
    let blocking_file: Option<i64> = sqlx::query_scalar!(
        "select df.data_file_id from iceberg_mirror.data_file df \
         where df.table_id = $1 and df.end_snapshot is null \
           and not coalesce(( \
                 select cs.max_value from iceberg_mirror.data_file_column_stat cs \
                 where cs.data_file_id = df.data_file_id \
                   and cs.column_name = 'loom_offset')::bigint < $2::bigint, false) \
         limit 1",
        tid,
        floor.min_offset(),
    )
    .fetch_optional(&mut *conn)
    .await
    .map_err(backend)?;
    if blocking_file.is_some() {
        return Ok(Some(floor));
    }

    // Inline tier: per-bucket precise. A live row blocks unless it is strictly below ITS
    // bucket's floor; an unframed row (NULL bucket/offset) blocks — fail-safe. The
    // `inline_<tid>` identifier is dynamic, so this is `AssertSqlSafe`; every literal
    // comes from our own mirror, never user input (same as `delete_end_capped_inline_rows`).
    //
    // `select exists(...)` -> bool: a bare `select 1` yields int4 and would fail to decode.
    if !crate::iceberg_inline::inline_table_exists(&mut *conn, tid).await? {
        return Ok(None);
    }
    let below = floor.below_floor_pred();
    let inline = crate::iceberg_inline::inline_table_name(tid);
    let blocking_row: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select exists(select 1 from {inline} \
         where end_snapshot is null and not coalesce({below}, false))"
    )))
    .fetch_one(&mut *conn)
    .await
    .map_err(backend)?;
    if blocking_row {
        return Ok(Some(floor));
    }
    Ok(None)
}

/// Refuse a `Removing` end-cap that would take offsets at or above any MV's read
/// position. Runs on the CALLER'S connection — pass the transaction the write commits
/// on, so a refusal and the write it guards are one unit: the caller's rollback undoes
/// the write. (The read takes no row locks and loom runs READ COMMITTED, so this is NOT
/// atomic against a concurrent `define_transform` registering a new reader; it is a
/// floor observed at read time, not a lock on the reader set.)
///
/// `Reframing` short-circuits (no query at all). `Destroying` logs and proceeds.
pub async fn guard_end_cap(
    conn: &mut PgConnection,
    table: &TableRef,
    tid: i64,
    intent: &EndCapIntent<'_>,
) -> Result<()> {
    match *intent {
        EndCapIntent::Reframing => return Ok(()),
        EndCapIntent::Destroying { reason } => {
            tracing::info!(
                schema = %table.schema, name = %table.name, tid, reason,
                "end-cap bypasses the MV floor",
            );
            return Ok(());
        }
        EndCapIntent::Removing => {}
    }
    let Some(floor) = removal_blocked(&mut *conn, table, tid).await? else {
        return Ok(());
    };
    Err(ControlPlaneError::Validation(format!(
        "{MV_FLOOR_REFUSAL_PREFIX} {}.{} carries offsets a micro-batch MV has not read \
         (floor {}, slowest {:?}); removing them would leave a hole in its delta",
        table.schema,
        table.name,
        floor.min_offset(),
        floor.slowest,
    )))
}
