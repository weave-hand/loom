# MV registration against a truncated source — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Registering a micro-batch MV against a stream source **bootstraps** its
`stream.mv_watermark` rows to the source's earliest *surviving* offsets (rounded down, never
up) — recorded in a new `start_offset` column — and takes the source's per-table advisory
lock, so a registration can neither silently under-read a reclaimed prefix nor straddle GC's
floor-read/reclaim window.

**Architecture:** All the work lands in `src/control-plane/postgres`. A new module
`mv_bootstrap.rs` computes the per-bucket earliest-surviving offset over **live-at-the-current-tip**
data (exact for the inline tier and for single-bucket Parquet files; rounded down to the
cross-bucket bound for multi-bucket files; **0** whenever a stat is missing) and writes the
bootstrap rows. `define_transform` (`transforms.rs`) calls it inside its existing transaction,
after taking `lock_key(source)` — the same per-table advisory lock GC/flush/consolidate
serialize under — at the **top** of the transaction, before any `transforms.transform` row lock.

**Tech Stack:** Rust, sqlx compile-time `query!` (offline `.sqlx` cache), Postgres advisory
locks, buck2 `loom_fixture_test`.

## Decisions already made (do not relitigate)

The spec (`docs/superpowers/specs/2026-07-14-mv-register-source-floor-design.md`) left four
calls to a human. They were made:

1. **Start policy = earliest-surviving**, rounded down. (Not `latest`.) A never-reclaimed
   source therefore bootstraps to **0**, and every existing floor test stays green.
2. **No `start_at: Earliest | Latest | RequireComplete` knob** on `TransformBody` in this PR.
   Fixed policy. The knob is filed as a FUTURE item in Task 6.
3. **A bootstrapped start is durably recorded**: migration **0045** adds `start_offset` to
   `stream.mv_watermark`.
4. **Coarse precision, always rounding down.** Cross-bucket file bounds are acceptable; a
   missing stat resolves to 0. No Parquet reads, no new low-watermark table.

## What the spec gets WRONG (verified against this branch's tree — trust this plan, not the spec)

PR #443 landed after the spec was written and moved almost everything it cites.

- **Every `file:line` in the spec is stale.** Re-derived line numbers are in each task below.
- **Spec correction 7 is already done.** `mv_floor` is *already* `mv_floor(conn: &mut PgConnection, table: &TableRef, tid: i64)` (`mv_floor.rs:97`). There is no sequencing dependency left on `#iss-end-cap-ignores-mv-floor`.
- **Spec correction 3's premise is dead.** GC's floor read is *already inside* the GC transaction (`iceberg_gc.rs:162`, inside the `pool.begin()` at `:155`, all under `lock_key` taken at `:112`). Its **conclusion still stands and is the fix**: `define_transform` takes only the global `TRANSFORM_DEFINE_LOCK` (`transforms.rs:399`) and never `lock_key(source)`. `iceberg_gc.rs:131-137` says so in a comment naming this very item.
- **"`define_transform` never touches `stream.*`" is FALSE.** It already calls `pg_refuse_mv_over_cdc_source` (`transforms.rs:460-462`) and already deletes `stream.mv_watermark` rows (`:489`). The bootstrap write has an obvious home in the same transaction.
- **Spec correction 6 understates what is available.** `loom_bucket` min/max column stats **are** recorded per data file (both framing columns are stat'd — `iceberg_landing.rs:276`, `:496`). A **single-bucket file is therefore exactly identifiable** (`loom_bucket` min == max) and can contribute an exact per-bucket bound. Only genuinely multi-bucket files are coarse. Flush is indeed not bucket-partitioned (`iceberg_flush.rs:170`), so multi-bucket files are the common case.
- **The new migration is `0045`**, not `0043` (latest on disk is `0044_dataset_view.sql`).
- **The bootstrap CANNOT be a testkit contract**, contrary to the spec's non-regression note. The memory backend has **no `TableRef` → `table_id` mapping at all** (`grep -rl 'resolve_table' src/control-plane/memory/src/` → nothing; `stream_tables` is keyed by a raw `i64` the caller supplies). Memory's `define_transform` cannot resolve an MV's source to a stream table id, and memory never reclaims anything, so there is nothing to certify cross-backend. **Both halves of this item are postgres-only.** Do not add a testkit contract; do not touch `src/control-plane/memory`.

## Global Constraints

- **Tests are `rust_test`/`loom_fixture_test` targets only** — never inline `#[cfg(test)] mod tests`. The `no-inline-tests` prek hook enforces this.
- New fixture tests **must** use `loom_fixture_test` (from `//src/control-plane/postgres/defs.bzl`), not a bare `rust_test`, or the postgres fixture env is missing and the test cannot boot.
- Strict clippy (whole `pedantic` + `restriction` groups on lib code). No `unwrap`/`expect`/`panic`/`indexing_slicing` in `src/`. Silence locally with `#[expect(lint, reason = "...")]` — a bare `#[allow]` without a `reason` fails `allow_attributes_without_reason`.
- **Any new/changed SQL ⇒ run `./tools/sqlx-prepare.sh` and commit the `.sqlx` change.** The `//src/control-plane/postgres:sqlx-cache-check` test fails otherwise.
- `buck2 run //tools:prek -- run --all-files` before **every** commit (rustfmt is a separate hook from clippy — clippy-clean is not lint-clean). `git add` new files *before* the gating prek run, or they are skipped as untracked.
- Build/test with `--console none`: `buck2 build -v0 --console none //src/control-plane/postgres:...`, `buck2 test --console none //src/control-plane/postgres:...`.
- Dynamic SQL (the `inline_<tid>` table name) uses `sqlx::AssertSqlSafe`, with every literal sourced from our own mirror — never from user input. This is the established pattern (`mv_floor.rs:310-321`).

---

### Task 1: The earliest-surviving-offset query

A read-only, per-bucket computation over the source's **live** data. No migration, no writes.

**Files:**
- Create: `src/control-plane/postgres/src/mv_bootstrap.rs`
- Modify: `src/control-plane/postgres/src/lib.rs` (add `pub mod mv_bootstrap;`)
- Modify: `src/control-plane/postgres/src/iceberg_flush.rs:416` (`pub(crate) fn lock_key` → `pub fn lock_key`, needed by Task 3's test; do it now so the module surface settles once)
- Modify: `src/control-plane/postgres/src/stream.rs:376` (`pg_peek_offset`: ensure it is at least `pub(crate)`; widen if it is private)
- Create: `src/control-plane/postgres/tests/mv_bootstrap.rs`
- Modify: `src/control-plane/postgres/tests/end_cap_seed.rs` (add the `land_more` helper)
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test` target `mv-bootstrap`)

**Interfaces:**
- Consumes: `crate::iceberg_mirror::live_table_id(conn: &mut PgConnection, ns: &str, name: &str) -> Result<Option<i64>>` (`iceberg_mirror.rs:307`); `crate::stream::pg_stream_bucket_count(conn, tid) -> Result<Option<i32>>`; `crate::stream::pg_peek_offset` (`stream.rs:376`); `crate::iceberg_inline::{inline_table_name, inline_table_exists}` (`iceberg_inline.rs:42`); `crate::backend` (the sqlx→`ControlPlaneError` mapper).
- Produces: `pub(crate) async fn earliest_surviving_offsets(conn: &mut PgConnection, tid: i64, bucket_count: i32) -> Result<BTreeMap<i32, i64>>` — every bucket in `0..bucket_count` is present. Task 2 consumes it.

**The rule that governs every branch below: ROUND DOWN, NEVER UP.** An overshoot silently
skips live rows the MV can still read — a data hole. An undershoot only makes the MV re-scan
an offset range where nothing survives, which yields no rows and is harmless. So a stat we
cannot read resolves to **0**, not to "skip it".

- [ ] **Step 1: Add the `land_more` helper to the shared seed library**

The acceptance test must land rows *after* a GC. `seed_source` lands once and `land` is not
re-exported, so extend the shared library (this is what `end_cap_seed` is for — do NOT
copy-paste a landing call into the test file).

Read `src/control-plane/postgres/tests/end_cap_seed.rs:163-210` first and mirror `seed_source`'s
own call into `land` exactly (same `InlineLimits`, same `columns()`, same `lineage(...)`,
same `buckets` argument). Append to `end_cap_seed.rs`:

```rust
/// Land `rows` MORE events into an already-seeded source — the "a surviving range
/// exists above the reclaimed prefix" half of the bootstrap tests. Offsets continue
/// from the stream allocator's high-water mark, so these rows sit strictly above
/// anything GC has taken. `inline` picks the tier exactly as `seed_source` does.
pub async fn land_more(
    pool: &sqlx::PgPool,
    catalog: &SqlCatalog,
    src: &TableRef,
    rows: i64,
    buckets: Option<i32>,
    inline: bool,
) {
    let (schema, batches) = batch(rows);
    land(
        pool,
        catalog,
        src,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: if inline { 1 << 20 } else { 0 },
            flush_byte_threshold: i64::MAX,
        },
        lineage(src),
        buckets,
    )
    .await
    .expect("land more");
}
```

If `seed_source`'s `land(...)` call differs from the above in any argument, **match
`seed_source`, not this snippet** — it is the ground truth in the tree.

- [ ] **Step 2: Write the failing tests**

Create `src/control-plane/postgres/tests/mv_bootstrap.rs`. These test `earliest_surviving_offsets`
directly. Note it is `pub(crate)` — for the integration test to reach it, re-export it from the
module as `pub` (see Step 4's module header); the module is a normal `pub mod`.

```rust
//! The earliest-surviving-offset computation and the registration bootstrap it feeds
//! (iss-mv-register-below-reclaimed-floor).
//!
//! The invariant every test here defends: the computed start ROUNDS DOWN. It may name an
//! offset below the true earliest surviving row (harmless — the MV re-scans an empty
//! range) but must NEVER name one above it (a silent data hole).

use std::time::Duration;

use control_plane_core::{ControlPlane, TableRef, mv_key};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_gc::gc_table;
use control_plane_postgres::iceberg_mirror::end_cap_live_data_files;
use control_plane_postgres::mv_bootstrap::earliest_surviving_offsets;
use control_plane_postgres::mv_floor::EndCapIntent;
use control_plane_core::{RunId, SnapshotId};
use end_cap_seed::{
    age_all_snapshots, current_snapshot_id, land_more, register_mv, seed_source, tref,
};

const SEVEN_DAYS: Duration = Duration::from_secs(7 * 24 * 3600);

/// A source nothing has ever reclaimed starts at 0 in every bucket — today's behavior,
/// preserved exactly. (This is what keeps `registered_but_unrun_mv_floors_at_zero` green.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn untouched_source_starts_at_zero() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(2),
        true,
        &[],
    )
    .await;

    let mut conn = s.pool.acquire().await.expect("conn");
    let starts = earliest_surviving_offsets(&mut conn, s.tid, 2)
        .await
        .expect("starts");
    assert_eq!(
        starts,
        [(0, 0), (1, 0)].into_iter().collect(),
        "nothing was ever reclaimed: every bucket's earliest surviving offset is 0"
    );
}

/// The acceptance case: a prefix is PHYSICALLY GONE (end-capped, aged, GC'd) and a
/// surviving range was landed above it. The start must name the surviving range, not 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaimed_prefix_starts_above_the_hole() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // One bucket keeps the arithmetic exact: offsets 0..6 inline, no MV registered, so
    // the floor is None and GC is unguarded — it really does take the prefix.
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        true,
        &[],
    )
    .await;

    // Physically destroy offsets 0..6: flush end-caps the inline copies, aging makes them
    // reclaimable, and an unguarded GC deletes them. (Mirror `mv_floor.rs`'s
    // `lagging_mv_holds_the_unread_tail` — same three moves, minus the MV.)
    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    let snap = current_snapshot_id(&s.pool, &s.src).await;
    let mut tx = s.pool.begin().await.expect("tx");
    end_cap_live_data_files(&mut tx, &s.src, s.tid, SnapshotId(snap), &EndCapIntent::Reframing)
        .await
        .expect("end-cap the flushed file");
    tx.commit().await.expect("commit");
    age_all_snapshots(&s.pool).await;
    gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");

    // A surviving range above the hole: offsets 6..12.
    land_more(&s.pool, &s.catalog, &s.src, 6, Some(1), true).await;

    let mut conn = s.pool.acquire().await.expect("conn");
    let starts = earliest_surviving_offsets(&mut conn, s.tid, 1)
        .await
        .expect("starts");
    assert_eq!(
        starts.get(&0).copied(),
        Some(6),
        "offsets 0..6 are gone; the earliest SURVIVING offset is 6 — starting at 0 would \
         define a first delta over a range that no longer exists"
    );
}

/// Nothing live at all (every row reclaimed, nothing landed since): the start is the
/// allocator's high-water mark. Starting at 0 here would floor GC at 0 forever with no
/// row to justify it — the over-hold the bootstrap exists to kill.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fully_reclaimed_source_starts_at_the_high_water_mark() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        4,
        Some(1),
        true,
        &[],
    )
    .await;
    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    let snap = current_snapshot_id(&s.pool, &s.src).await;
    let mut tx = s.pool.begin().await.expect("tx");
    end_cap_live_data_files(&mut tx, &s.src, s.tid, SnapshotId(snap), &EndCapIntent::Reframing)
        .await
        .expect("end-cap");
    tx.commit().await.expect("commit");
    age_all_snapshots(&s.pool).await;
    gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");

    let mut conn = s.pool.acquire().await.expect("conn");
    let starts = earliest_surviving_offsets(&mut conn, s.tid, 1)
        .await
        .expect("starts");
    assert_eq!(
        starts.get(&0).copied(),
        Some(4),
        "no live row survives; the only sane start is the allocator's next offset (4)"
    );
}

/// A source landed straight to PARQUET (no inline tier): the file's `loom_offset` min
/// stat supplies the bound. With one bucket the file is single-bucket, so the bound is
/// exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_tier_supplies_the_bound() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        5,
        Some(1),
        false, // straight to Parquet
        &[],
    )
    .await;

    let mut conn = s.pool.acquire().await.expect("conn");
    let starts = earliest_surviving_offsets(&mut conn, s.tid, 1)
        .await
        .expect("starts");
    assert_eq!(
        starts.get(&0).copied(),
        Some(0),
        "the live file's loom_offset min stat is 0"
    );
}
```

- [ ] **Step 3: Run the tests to verify they fail**

First add the BUCK target (the test cannot even build without it). Append to
`src/control-plane/postgres/BUCK`, mirroring the `mv-floor` target at `BUCK:686-702`:

```python
loom_fixture_test(
    name = "mv-bootstrap",
    crate = "mv_bootstrap",
    srcs = ["tests/mv_bootstrap.rs"],
    crate_root = "tests/mv_bootstrap.rs",
    deps = [
        ":end-cap-seed",
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

Run: `buck2 test --console none //src/control-plane/postgres:mv-bootstrap`
Expected: **BUILD FAILED** — `unresolved import control_plane_postgres::mv_bootstrap` /
`cannot find function earliest_surviving_offsets`. That is the correct first failure.

- [ ] **Step 4: Implement `earliest_surviving_offsets`**

Create `src/control-plane/postgres/src/mv_bootstrap.rs`:

```rust
//! Where a newly registered micro-batch MV starts reading its source
//! (iss-mv-register-below-reclaimed-floor).
//!
//! A brand-new MV has no `stream.mv_watermark` rows, so `mv_floor` defaults it to 0 in
//! every bucket and `mv_delta_scan` defines its first delta as "the source from offset 0".
//! Both are wrong for a source whose prefix is already gone: the delta silently under-reads
//! (the rows below the surviving range no longer exist), and the floor pins the source's GC
//! at 0 in every bucket until the MV first runs — holding every end-capped byte for a
//! reader that can never want it.
//!
//! This module computes where the MV should actually start — the source's EARLIEST
//! SURVIVING offset per bucket, over data LIVE AT THE CURRENT TIP (`end_snapshot is null`),
//! which is exactly the set `mv_delta_scan` can read — and `define_transform` writes it into
//! `stream.mv_watermark` as the MV's recorded starting position (`start_offset`).
//!
//! ## The rounding rule: DOWN, NEVER UP
//! An offset ABOVE the true earliest surviving row would make the MV skip live rows — a
//! silent data hole, the exact failure this module exists to prevent. An offset BELOW it
//! only makes the MV's first delta scan a range where nothing survives, which returns no
//! rows. So every uncertainty resolves DOWNWARD, to 0:
//!
//! - a live file with no `loom_offset` min stat: we cannot prove where it starts ⇒ 0;
//! - a live inline row with a NULL `loom_bucket`/`loom_offset` (written before the table was
//!   declared a stream — see `#iss-mv-floor-holds-pre-declaration-files`) ⇒ 0.
//!
//! Note this fail-safe direction is the INVERSE of the one in [`crate::mv_floor`], which
//! guards a `max` and therefore resolves a missing stat UPWARD (hold the file). Same
//! principle — never let a missing stat cause data loss — opposite direction, because one
//! bounds a reclaim and the other bounds a read.
//!
//! ## Precision
//! Exact per-bucket for the inline tier, and for a single-bucket Parquet file (its
//! `loom_bucket` min == max). Flush does not partition by bucket
//! (`crate::iceberg_flush`), so a flushed file generally SPANS buckets and its `loom_offset`
//! min is a cross-bucket bound: it lowers EVERY bucket's start. Coarse, and deliberately so
//! — coarse in the safe direction.

use std::collections::BTreeMap;

use control_plane_core::{Result, TableRef};
use sqlx::PgConnection;

use crate::backend;
use crate::iceberg_inline::{inline_table_exists, inline_table_name};
use crate::stream::{pg_peek_offset, pg_stream_bucket_count};

/// Lower `slot` to `v` (or seed it) — the only way a candidate is ever recorded, so a
/// bound can only ever move DOWN.
fn lower(slot: &mut Option<i64>, v: i64) {
    *slot = Some(slot.map_or(v, |cur| cur.min(v)));
}

/// The earliest offset still LIVE in each bucket of stream table `tid`, over both storage
/// tiers, rounded down (see the module docs). Every bucket in `0..bucket_count` is present
/// in the result. A bucket with no live data at all takes the allocator's high-water mark
/// (`stream.bucket_offset.next`): nothing survives to be read, so the only start that skips
/// nothing is the end.
pub async fn earliest_surviving_offsets(
    conn: &mut PgConnection,
    tid: i64,
    bucket_count: i32,
) -> Result<BTreeMap<i32, i64>> {
    // Candidates that apply to ONE bucket (exact), and candidates that apply to EVERY
    // bucket (a cross-bucket file's min, or an unprovable stat's 0).
    let mut exact: BTreeMap<i32, i64> = BTreeMap::new();
    let mut cross: Option<i64> = None;

    // ---- file tier ---------------------------------------------------------
    // Live files only (`end_snapshot is null`) — an end-capped file is already invisible
    // to `mv_delta_scan`, which reads at the current snapshot. The bounds are stored as
    // text and re-typed here, exactly as `mv_floor::removal_blocked` does.
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
            (Some(lo), Some(hi)) if lo == hi => lower(exact.entry(lo).or_insert(off).into(), off),
            // Spans buckets (or has no bucket stat): the min is a cross-bucket bound and
            // lowers every bucket.
            _ => lower(&mut cross, off),
        }
    }

    // ---- inline tier -------------------------------------------------------
    // Exact per bucket. The `inline_<tid>` identifier is dynamic (hence `AssertSqlSafe`);
    // `tid` comes from our own mirror, never from user input — the same pattern as
    // `mv_floor::removal_blocked` and `iceberg_gc::delete_end_capped_inline_rows`.
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
            lower(exact.entry(bucket).or_insert(off).into(), off);
        }

        // An UNFRAMED live row — written before this table was declared a stream, so it
        // carries no bucket/offset at all. We cannot place it, so we cannot prove any
        // bucket starts above 0. Round down. (This is the read-side twin of
        // `#iss-mv-floor-holds-pre-declaration-files`.)
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
            // Nothing live in this bucket: start at the end. Exact, per-bucket, already
            // stored — and it skips nothing, because there is nothing to skip.
            None => pg_peek_offset(&mut *conn, tid, bucket).await?,
        };
        out.insert(bucket, start);
    }
    Ok(out)
}
```

**Note on `lower(exact.entry(lo).or_insert(off).into(), off)`:** that is wrong Rust —
`&mut i64` is not `&mut Option<i64>`. Write the exact-map merge as a plain min instead:

```rust
fn lower_exact(map: &mut BTreeMap<i32, i64>, bucket: i32, v: i64) {
    map.entry(bucket)
        .and_modify(|cur| *cur = (*cur).min(v))
        .or_insert(v);
}
```

and call `lower_exact(&mut exact, lo, off)` / `lower_exact(&mut exact, bucket, off)` at the
two sites above. Keep `lower` for the `Option<i64>` cross bound.

Wire the module in `src/control-plane/postgres/src/lib.rs` next to the other `pub mod`
declarations (alphabetical if the file is ordered that way):

```rust
pub mod mv_bootstrap;
```

Widen `lock_key` in `src/control-plane/postgres/src/iceberg_flush.rs:416` from
`pub(crate) fn lock_key` to `pub fn lock_key` (Task 3's lock-race test needs to compute the
key from an integration test). Widen `pg_peek_offset` (`stream.rs:376`) to `pub(crate)` if it
is currently private.

- [ ] **Step 5: Regenerate the sqlx cache**

The new `query!` needs its offline entry.

Run: `./tools/sqlx-prepare.sh`
Then: `git add src/control-plane/postgres/.sqlx`

- [ ] **Step 6: Run the tests to verify they pass**

Run: `buck2 test --console none //src/control-plane/postgres:mv-bootstrap`
Expected: `Tests finished: Pass 4. Fail 0.`

If `reclaimed_prefix_starts_above_the_hole` reports `Some(0)` instead of `Some(6)`, the GC
did not actually reclaim — check that `seed_source` was called with **no** MVs (an MV makes
the floor non-`None` and GC holds everything) and that `age_all_snapshots` ran before
`gc_table`.

- [ ] **Step 7: Lint and commit**

```bash
git add src/control-plane/postgres/src/mv_bootstrap.rs \
        src/control-plane/postgres/src/lib.rs \
        src/control-plane/postgres/src/iceberg_flush.rs \
        src/control-plane/postgres/src/stream.rs \
        src/control-plane/postgres/tests/mv_bootstrap.rs \
        src/control-plane/postgres/tests/end_cap_seed.rs \
        src/control-plane/postgres/BUCK \
        src/control-plane/postgres/.sqlx
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "feat(stream): compute a stream source's earliest surviving offset per bucket"
```

---

### Task 2: Record the start — migration 0045 + the bootstrap write in `define_transform`

**Files:**
- Create: `src/control-plane/postgres/migrations/0045_mv_watermark_start_offset.sql`
- Modify: `src/control-plane/postgres/src/mv_bootstrap.rs` (add the write)
- Modify: `src/control-plane/postgres/src/transforms.rs` (call it from `define_transform`)
- Modify: `src/control-plane/postgres/tests/mv_bootstrap.rs` (registration tests)

**Interfaces:**
- Consumes: `earliest_surviving_offsets` (Task 1); `crate::iceberg_mirror::live_table_id`; `crate::transforms::mv_source(&TransformBody) -> Option<&TableRef>` (`transforms.rs:117`, already exists) and `mv_output` (`:128`); `control_plane_core::mv_key(&TableRef) -> String`.
- Produces: `pub async fn bootstrap_mv_watermarks(conn: &mut PgConnection, mv: &str, source: &TableRef) -> Result<()>` — idempotent, `on conflict do nothing`. Task 4 consumes the same module.

- [ ] **Step 1: Write the migration**

Create `src/control-plane/postgres/migrations/0045_mv_watermark_start_offset.sql`:

```sql
-- Where an MV was told to BEGIN reading its source, per bucket
-- (iss-mv-register-below-reclaimed-floor).
--
-- `next_offset` alone cannot answer "did this MV ever see offsets 0..N?": a row
-- bootstrapped at N by `define_transform` (because the source's prefix was already
-- reclaimed) is byte-identical to one a run advanced to N. `start_offset` records the
-- difference, so the gap is an auditable fact rather than a lost log line.
--
-- The default is the correct backfill for every pre-existing row AND for every row the
-- watermark CAS creates from 0: such an MV genuinely started at offset 0.
alter table stream.mv_watermark
    add column start_offset bigint not null default 0 check (start_offset >= 0);
```

- [ ] **Step 2: Write the failing tests**

Append to `src/control-plane/postgres/tests/mv_bootstrap.rs`. These drive the REAL
registration path (`register_mv` → `define_transform`), which is the whole point.

```rust
/// Registering an MV against a source whose prefix is GONE bootstraps its watermarks to
/// the surviving range — and therefore does NOT reset the source's GC floor to 0. This is
/// the item's acceptance test: it covers both the under-read and the over-hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registering_against_a_reclaimed_source_bootstraps_the_watermark() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        true,
        &[], // no MV yet — GC must be unguarded so it really takes the prefix
    )
    .await;

    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    let snap = current_snapshot_id(&s.pool, &s.src).await;
    let mut tx = s.pool.begin().await.expect("tx");
    end_cap_live_data_files(&mut tx, &s.src, s.tid, SnapshotId(snap), &EndCapIntent::Reframing)
        .await
        .expect("end-cap");
    tx.commit().await.expect("commit");
    age_all_snapshots(&s.pool).await;
    gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    land_more(&s.pool, &s.catalog, &s.src, 6, Some(1), true).await;

    // NOW register. The MV can never read offsets 0..6 — they do not exist.
    register_mv(&cp, "mv_a", &s.src, &tref("s", "out_a")).await;
    let mv = mv_key(&tref("s", "out_a"));

    let wm = cp.mv_watermarks(&mv, s.tid).await.expect("watermarks");
    assert_eq!(
        wm.get(&0).copied(),
        Some(6),
        "the MV is bootstrapped to the surviving range, not to 0"
    );

    let start: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(
        "select start_offset from stream.mv_watermark where mv = $1 and bucket = 0",
    ))
    .bind(&mv)
    .fetch_one(&s.pool)
    .await
    .expect("start_offset");
    assert_eq!(
        start, 6,
        "the bootstrap is RECORDED: 'this MV never saw offsets 0..6' is an auditable fact"
    );

    let mut conn = s.pool.acquire().await.expect("conn");
    let floor = control_plane_postgres::mv_floor::mv_floor(&mut conn, &s.src, s.tid)
        .await
        .expect("floor")
        .expect("a registered MV reads this source");
    assert_eq!(
        floor.per_bucket.get(&0).copied(),
        Some(6),
        "registering no longer drops the source's GC floor to 0 (the over-hold is gone)"
    );
}

/// Registering against a never-reclaimed source bootstraps to 0 — today's behavior,
/// unchanged. `start_offset` records the honest 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registering_against_an_untouched_source_starts_at_zero() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(2),
        true,
        &[("mv_a", "out_a")],
    )
    .await;

    let wm = cp
        .mv_watermarks(&mv_key(&tref("s", "out_a")), s.tid)
        .await
        .expect("watermarks");
    assert_eq!(
        wm,
        [(0, 0), (1, 0)].into_iter().collect(),
        "nothing was reclaimed: the MV starts at 0, exactly as before"
    );
}

/// Define-before-land stays legal: registering against a source that does not exist yet
/// is not an error, and bootstraps nothing (nothing can have been lost).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn define_before_land_is_still_legal() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        2,
        Some(1),
        true,
        &[],
    )
    .await;

    // `s.nonexistent` has never been landed, let alone declared a stream.
    register_mv(&cp, "mv_future", &tref("s", "nonexistent"), &tref("s", "out_f")).await;

    let wm = cp
        .mv_watermarks(&mv_key(&tref("s", "out_f")), s.tid)
        .await
        .expect("watermarks");
    assert!(
        wm.is_empty(),
        "no source, nothing to bootstrap — and no error"
    );
}

/// A redefinition that keeps the same output RESUMES: the bootstrap must never clobber a
/// watermark a run has already advanced (`on conflict do nothing`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redefining_the_same_output_does_not_reset_progress() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        8,
        Some(1),
        true,
        &[("mv_a", "out_a")],
    )
    .await;
    let mv = mv_key(&tref("s", "out_a"));
    end_cap_seed::advance(&cp, &mv, s.tid, 0, 0, 5).await;

    // Redefine the SAME MV (same name, same source, same output).
    register_mv(&cp, "mv_a", &s.src, &tref("s", "out_a")).await;

    let wm = cp.mv_watermarks(&mv, s.tid).await.expect("watermarks");
    assert_eq!(
        wm.get(&0).copied(),
        Some(5),
        "the MV resumes where it left off — the bootstrap did not reset it to 0"
    );
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `buck2 test --console none //src/control-plane/postgres:mv-bootstrap`
Expected: FAIL — `registering_against_a_reclaimed_source_bootstraps_the_watermark` gets an
empty watermark map (`None`, not `Some(6)`), because nothing bootstraps yet. (The
`start_offset` scalar query will also fail: the column does not exist until the migration is
applied — which the fixture does on boot, so after Step 1 it will exist and the assertion
will simply report no row.)

- [ ] **Step 4: Implement the bootstrap write**

Append to `src/control-plane/postgres/src/mv_bootstrap.rs`:

```rust
/// Seed `mv`'s `stream.mv_watermark` rows for `source` at the source's earliest surviving
/// offsets, recording each as the MV's `start_offset`. Called by
/// [`crate::transforms`]`::define_transform` inside its transaction, so the registration and
/// the start it implies commit as one unit.
///
/// A source that is not (yet) a declared stream table — or does not exist at all — is a
/// no-op: define-before-land is a legal, common flow, and a source with no offsets can have
/// lost none.
///
/// `on conflict do nothing` is load-bearing: a redefinition that keeps the same output keeps
/// its watermarks (the MV resumes where it left off), and the bootstrap must never drag a
/// live MV's position backwards.
pub async fn bootstrap_mv_watermarks(
    conn: &mut PgConnection,
    mv: &str,
    source: &TableRef,
) -> Result<()> {
    let Some(tid) =
        crate::iceberg_mirror::live_table_id(&mut *conn, &source.schema, &source.name).await?
    else {
        return Ok(());
    };
    let Some(bucket_count) = pg_stream_bucket_count(&mut *conn, tid).await? else {
        return Ok(());
    };
    let starts = earliest_surviving_offsets(&mut *conn, tid, bucket_count).await?;

    for (bucket, start) in &starts {
        sqlx::query!(
            "insert into stream.mv_watermark \
                 (mv, source_table_id, bucket, next_offset, start_offset) \
             values ($1, $2, $3, $4, $4) \
             on conflict (mv, source_table_id, bucket) do nothing",
            mv,
            tid,
            bucket,
            start,
        )
        .execute(&mut *conn)
        .await
        .map_err(backend)?;
    }

    if starts.values().any(|s| *s > 0) {
        tracing::info!(
            mv,
            source = %format!("{}.{}", source.schema, source.name),
            ?starts,
            "MV registered against a source whose prefix is already gone: bootstrapped its \
             watermarks to the earliest surviving offsets. It will never see the offsets below."
        );
    }
    Ok(())
}
```

Now call it from `define_transform` in `src/control-plane/postgres/src/transforms.rs`.
Insert **immediately after** the `pg_refuse_mv_over_cdc_source` block (currently `:460-462`)
and **before** the prior-def `for update` (currently `:474`):

```rust
        // A brand-new MV has no watermark rows, so `mv_floor` would default it to 0 in every
        // bucket and `mv_delta_scan` would define its first delta as "the source from 0" —
        // under-reading a prefix that may already be gone, and pinning the source's GC floor
        // at 0 until the MV first runs. Seed its start explicitly instead, in THIS
        // transaction: the registration and the position it implies commit together, under
        // the source's table lock taken at the top of this tx.
        if let Some(src) = mv_source(&def.body) {
            if let Some(out) = mv_output(&def.body) {
                crate::mv_bootstrap::bootstrap_mv_watermarks(&mut tx, &mv_key(out), src).await?;
            }
        }
```

`mv_key` is already imported in `transforms.rs` (it is in the `control_plane_core::{...}` use
list at the top). `mv_source`/`mv_output` are the existing private helpers at `:117`/`:128`.

**Ordering note (do not move this):** the bootstrap must run *before* the `for update` on
`transforms.transform`, so that this transaction's lock acquisition order matches the commit
path's (table lock, then transform row locks — see Task 3). It must also run *after*
`pg_refuse_mv_over_cdc_source`, so a refused registration never writes watermark rows.

- [ ] **Step 5: Regenerate the sqlx cache**

The migration changes the schema AND there is a new `query!`.

Run: `./tools/sqlx-prepare.sh`
Then: `git add src/control-plane/postgres/.sqlx`

- [ ] **Step 6: Run the tests to verify they pass**

Run: `buck2 test --console none //src/control-plane/postgres:mv-bootstrap`
Expected: `Tests finished: Pass 8. Fail 0.`

Then the neighbours that must not regress:

Run: `buck2 test --console none //src/control-plane/postgres:mv-floor //src/control-plane/postgres:mv-watermarks //src/control-plane/postgres:transforms //src/control-plane/postgres:sqlx-cache-check`
Expected: all pass. In particular `registered_but_unrun_mv_floors_at_zero` (`mv_floor.rs:79`)
must stay green — an untouched source bootstraps to 0, so the floor is still 0.

- [ ] **Step 7: Lint and commit**

```bash
git add -A
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "fix(stream): bootstrap a new MV's watermark to its source's surviving offsets"
```

---

### Task 3: Close the registration/GC race — take `lock_key(source)`

Bootstrapping alone still races: GC can read the MV floor and then reclaim, with a
registration committing in between. loom sets no isolation level, so `pool.begin()` is READ
COMMITTED and each statement takes a fresh snapshot; the floor read takes no row locks. The
only fix is to make the registration take the **same per-table advisory lock** GC serializes
under — `lock_key(source)` — which `define_transform` today does not (`iceberg_gc.rs:131-137`
says exactly this, naming this item).

**Files:**
- Modify: `src/control-plane/postgres/src/transforms.rs` (`define_transform`, right after the `TRANSFORM_DEFINE_LOCK` at `:399-402`)
- Modify: `src/control-plane/postgres/tests/mv_bootstrap.rs` (the race + deadlock tests)

**Interfaces:**
- Consumes: `crate::iceberg_flush::lock_key(schema: &str, name: &str) -> i64` (made `pub` in Task 1).
- Produces: nothing new. This is a locking change only.

**⚠ The deadlock hazard — get the ORDER right.** The commit path takes
`lock_table(table)` (i.e. `lock_key`) and *then* row-locks `transforms.transform`
(`pg_fire_data_triggers`, `transforms.rs:281-284`, `for update`) — see `iceberg_flush.rs:54`
→ `append_parquet_snapshot` → `pg_fire_data_triggers`. `define_transform` row-locks its own
def at `:474`. Acquiring `lock_key(source)` **after** that `for update` is a classic
inversion and will deadlock under load. **Global order:
`TRANSFORM_DEFINE_LOCK` → `lock_key(source)` → `transforms.transform` row locks.** Take it at
the very top of the transaction.

(Deadlock-freedom of the outer pair: `TRANSFORM_DEFINE_LOCK` is taken by `define_transform`
and nothing else in the tree — `grep -rn 'TRANSFORM_DEFINE_LOCK' src/` — so no path can hold
`lock_key(t)` and then wait for it. Taking the global lock first is therefore safe.)

- [ ] **Step 1: Write the failing tests**

Append to `src/control-plane/postgres/tests/mv_bootstrap.rs`:

```rust
/// `define_transform` must serialize against the SOURCE's per-table advisory lock — the one
/// GC holds across its floor read AND its reclaim. Without it, a registration commits inside
/// GC's window and GC reclaims below the brand-new MV's floor.
///
/// Hold the lock by hand, then assert the registration BLOCKS on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn define_transform_blocks_on_the_sources_table_lock() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        4,
        Some(1),
        true,
        &[],
    )
    .await;

    // Hold `lock_key(s.events)` in a transaction we control — exactly what `gc_table` does
    // for the whole of its floor-read + reclaim.
    let key = control_plane_postgres::iceberg_flush::lock_key(&s.src.schema, &s.src.name);
    let mut holder = s.pool.begin().await.expect("tx");
    sqlx::query(sqlx::AssertSqlSafe("select pg_advisory_xact_lock($1)"))
        .bind(key)
        .execute(&mut *holder)
        .await
        .expect("hold the source's table lock");

    // The registration must not be able to slip past it.
    let blocked = tokio::time::timeout(
        Duration::from_millis(750),
        register_mv_result(&cp, "mv_a", &s.src, &tref("s", "out_a")),
    )
    .await;
    assert!(
        blocked.is_err(),
        "define_transform did NOT take lock_key(source) — it committed while GC held the \
         table lock, which is exactly the race this item closes"
    );

    // Release, and it goes through.
    holder.rollback().await.expect("release");
    tokio::time::timeout(
        Duration::from_secs(10),
        register_mv_result(&cp, "mv_a", &s.src, &tref("s", "out_a")),
    )
    .await
    .expect("register once the lock is free")
    .expect("register");
}

/// A registration and a concurrent commit on the SAME source must not deadlock: the commit
/// path takes lock_key(table) then row-locks transforms.transform, so define_transform must
/// take them in that same order (never the reverse).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registration_and_commit_do_not_deadlock() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        4,
        Some(1),
        true,
        &[("mv_a", "out_a")],
    )
    .await;

    // A flush (lock_key -> pg_fire_data_triggers row locks) racing a redefinition of the MV
    // that reads the same source (TRANSFORM_DEFINE_LOCK -> lock_key -> row locks).
    let (flush, define) = tokio::join!(
        flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4())),
        register_mv_result(&cp, "mv_a", &s.src, &tref("s", "out_a")),
    );
    flush.expect("flush must not deadlock against a concurrent registration");
    define.expect("register must not deadlock against a concurrent flush");
}
```

`register_mv` (in `end_cap_seed`) `expect`s internally, so it cannot express "did this
block / did this error". Add a `Result`-returning sibling to
`src/control-plane/postgres/tests/end_cap_seed.rs` — and rewrite `register_mv` to call it, so
there is one definition of the MV def, not two:

```rust
/// `register_mv`, but surfacing the error — for the tests that assert on BLOCKING or on a
/// refusal rather than on success.
pub async fn register_mv_result(
    cp: &PgControlPlane,
    name: &str,
    source: &TableRef,
    output: &TableRef,
) -> control_plane_core::Result<()> {
    cp.transforms()
        .define_transform(TransformDef {
            name: TransformName(name.into()),
            body: TransformBody::MicroBatch {
                source: source.clone(),
                output: output.clone(),
                buckets: 1,
                sql: "select id from events".into(),
            },
            schedule: None,
            on_input_commit: false,
        })
        .await
}

pub async fn register_mv(cp: &PgControlPlane, name: &str, source: &TableRef, output: &TableRef) {
    register_mv_result(cp, name, source, output)
        .await
        .expect("register mv");
}
```

(Replace the existing `register_mv` body at `end_cap_seed.rs:113-128` with the pair above,
keeping its doc comment.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 test --console none //src/control-plane/postgres:mv-bootstrap`
Expected: `define_transform_blocks_on_the_sources_table_lock` FAILS with
"define_transform did NOT take lock_key(source)" — today the registration commits straight
through the held lock. (`registration_and_commit_do_not_deadlock` will pass already; it is
the regression net for the fix, not a driver of it.)

- [ ] **Step 3: Take the lock**

In `src/control-plane/postgres/src/transforms.rs`, immediately after the
`TRANSFORM_DEFINE_LOCK` acquisition (`:399-402`) and before **everything** else:

```rust
        // Serialize this registration against its SOURCE table's flush / consolidate / GC.
        // GC reads the MV floor and reclaims under `lock_key(source)` (`crate::iceberg_gc`),
        // and READ COMMITTED gives that floor read no protection from a registration that
        // commits between it and the reclaim: the new MV's floor would simply not exist yet,
        // and GC would reclaim below it. Holding the same key for the whole of this
        // transaction makes the two mutually exclusive.
        //
        // ORDER IS LOAD-BEARING. The commit path takes `lock_key(table)` and THEN row-locks
        // `transforms.transform` (`pg_fire_data_triggers`). Taking this lock here — at the
        // top, before the `for update` below — puts us in that same order. Acquiring it after
        // the row lock would invert the pair and deadlock.
        if let Some(src) = mv_source(&def.body) {
            let key = crate::iceberg_flush::lock_key(&src.schema, &src.name);
            sqlx::query!("select pg_advisory_xact_lock($1)", key)
                .execute(&mut *tx)
                .await
                .map_err(backend)?;
        }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `buck2 test --console none //src/control-plane/postgres:mv-bootstrap`
Expected: `Tests finished: Pass 10. Fail 0.`

- [ ] **Step 5: Regenerate the sqlx cache and commit**

Run: `./tools/sqlx-prepare.sh` (the new `query!` needs an entry), then:

```bash
git add -A
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "fix(stream): serialize MV registration against its source's table lock"
```

---

### Task 4: Fix the stale-source watermark leak (a defect found while building this)

**Found while reading `define_transform`'s watermark reconciliation — verified reachable, not
speculative.** `define_transform` releases the watermarks of a **prior OUTPUT** when a
redefinition moves the MV off it (`transforms.rs:479-502`). It does **not** release the
watermarks of a prior **SOURCE**. Redefine an MV to read a *different source* while keeping
the same output (`mv_key` unchanged) and the old rows — keyed
`(mv, old_source_table_id, bucket)` — survive with no def naming that source. `mv_floor`'s
reader set counts "every mv holding a watermark row against the source"
(`mv_floor.rs:24-30`), so the **old source is floored at those offsets forever, by an MV that
no longer reads it, with no def to reach it**. That is the same permanent "GC never
converges" shape as `#iss-mv-watermark-ghost-rows`, and its fix is one statement in the
transaction we are already editing.

`MicroBatchJoin` has exactly one stream `source` (plus a static `enrich` — `core/src/transforms.rs:75-83`),
so an mv key legitimately holds watermark rows against **exactly one** source table. "Delete
this mv's rows for every source that is not the current one" is therefore total and safe.

**Files:**
- Modify: `src/control-plane/postgres/src/transforms.rs` (`define_transform`, the watermark-reconciliation block at `:479-502`)
- Modify: `src/control-plane/postgres/tests/mv_bootstrap.rs`

- [ ] **Step 1: Write the failing test**

Append to `src/control-plane/postgres/tests/mv_bootstrap.rs`:

```rust
/// Redefining an MV onto a DIFFERENT SOURCE (same output, so the same `mv_key`) must
/// release the old source's watermarks. Otherwise the old source is floored forever by an MV
/// that no longer reads it — and no def names those rows, so nothing can ever reach them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redefining_onto_a_new_source_releases_the_old_sources_floor() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        8,
        Some(1),
        true,
        &[("mv_a", "out_a")],
    )
    .await;
    let mv = mv_key(&tref("s", "out_a"));
    end_cap_seed::advance(&cp, &mv, s.tid, 0, 0, 3).await;

    // A second stream source, and the MV is redefined to read IT instead — same output.
    let other = tref("s", "events2");
    land_more_to(&s.pool, &s.catalog, &other, 4, Some(1), true).await;
    register_mv(&cp, "mv_a", &other, &tref("s", "out_a")).await;

    // The OLD source must no longer be floored by this MV.
    let wm_old = cp.mv_watermarks(&mv, s.tid).await.expect("watermarks");
    assert!(
        wm_old.is_empty(),
        "the MV no longer reads s.events — its watermarks there must be released, or \
         s.events is floored at offset 3 forever with no def to point an operator at"
    );

    let mut conn = s.pool.acquire().await.expect("conn");
    let floor = control_plane_postgres::mv_floor::mv_floor(&mut conn, &s.src, s.tid)
        .await
        .expect("floor");
    assert_eq!(
        floor, None,
        "no MV reads s.events any more: its GC floor is gone entirely"
    );
}
```

This needs a `land_more_to` that lands into an arbitrary (not-yet-existing) table. Extend
`end_cap_seed.rs` — generalize rather than duplicate: make `land_more` take the `TableRef`
it lands into (it already does in the Task 1 snippet — it takes `src: &TableRef`). **So
`land_more_to` is not a new function: call `land_more(&s.pool, &s.catalog, &other, 4,
Some(1), true)` directly** and delete the `land_more_to` name from the test above. Confirm
`land` creates the table and declares the stream on first land (it does — that is how
`seed_source` creates `s.events`); if it does not, declare `other` with
`cp.declare_stream(...)` first, mirroring `seed_source`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:mv-bootstrap`
Expected: FAIL — `wm_old` still holds `{0: 3}`, and `mv_floor(s.events)` is
`Some(per_bucket: {0: 3})`, pinning the old source forever.

- [ ] **Step 3: Release the stale source's rows**

In `src/control-plane/postgres/src/transforms.rs`, inside the existing watermark-reconciliation
block (right after the prior-output delete at `:489`), add the source half. Extend the block's
comment to say what it now covers:

```rust
        // ... existing prior-OUTPUT release stays exactly as it is ...

        // The SOURCE half of the same reconciliation. Watermarks are keyed
        // `(mv, source_table_id, bucket)`, so a redefinition that keeps the OUTPUT (same
        // `mv_key`) but moves to a DIFFERENT SOURCE leaves the old source's rows behind —
        // referenced by no def, unreachable by `delete_transform` (which keys on the def's
        // current source), and counted by `mv_floor`'s reader set, which would floor that
        // source at the dead MV's offsets forever. An mv key reads exactly one source
        // (`MicroBatchJoin`'s second input is a static `enrich`, not a stream), so "every
        // source that is not the current one" is precisely the stale set.
        if let Some(mv) = &new_mv {
            let src_tid = match mv_source(&def.body) {
                Some(src) => {
                    crate::iceberg_mirror::live_table_id(&mut tx, &src.schema, &src.name).await?
                }
                None => None,
            };
            match src_tid {
                // Keep the current source's rows (that is the resume case); drop the rest.
                Some(tid) => {
                    sqlx::query!(
                        "delete from stream.mv_watermark \
                         where mv = $1 and source_table_id <> $2",
                        mv,
                        tid,
                    )
                    .execute(&mut *tx)
                    .await
                    .map_err(backend)?;
                }
                // The source does not exist (define-before-land): this MV reads nothing, so
                // any watermark row it holds is against a source it has left behind.
                None => {
                    sqlx::query!("delete from stream.mv_watermark where mv = $1", mv)
                        .execute(&mut *tx)
                        .await
                        .map_err(backend)?;
                }
            }
        }
```

**Placement:** this must run **before** the bootstrap call added in Task 2 would be a problem
— it is not, because they touch disjoint rows (this deletes rows for *other* sources; the
bootstrap inserts rows for the *current* one). But the bootstrap currently sits earlier in the
function (right after `pg_refuse_mv_over_cdc_source`, before the `for update`), and this block
sits later. **Move the bootstrap call to here, immediately after this delete**, so the whole
watermark reconciliation for the MV — release the old output, release the stale source, seed
the new source — reads as one block in one place. The **lock** (Task 3) stays at the top of
the transaction; only the bootstrap *write* moves. Re-verify the ordering note in Task 2 Step
4 still holds: the write happens after the `for update`, which is fine — the lock ordering
constraint is about the *advisory lock*, not the bootstrap write.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `buck2 test --console none //src/control-plane/postgres:mv-bootstrap //src/control-plane/postgres:mv-floor //src/control-plane/postgres:mv-watermarks //src/control-plane/postgres:transforms`
Expected: all pass, 11 in `mv-bootstrap`.

- [ ] **Step 5: Regenerate the sqlx cache, lint, commit**

```bash
./tools/sqlx-prepare.sh
git add -A
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "fix(stream): release an MV's watermarks on its PRIOR source, not just its prior output"
```

---

### Task 5: Full-suite verification and the metric gate

**Files:** none (verification), plus whatever the gate says to fix.

- [ ] **Step 1: Build and test the whole first-party tree**

Run: `buck2 build -v0 --console none //src/...`
Expected: silent, exit 0.

Run: `buck2 test --console none //src/... -j 8`
Expected: `Fail 0`. (The `-j 8` cap is mandatory locally: the full suite starves the 8
postgres fixture boot slots otherwise and throws non-deterministic 120s timeouts that look
like real failures.)

Pay attention to the **worker** and **engine-serving** suites: `mv_delta` seeds its lower
bound from the watermark (`mv_delta.rs:205`), so a bootstrapped row changes what an MV's
first delta reads. `stream_mv_e2e.rs` is the one to watch.

- [ ] **Step 2: Run the metric gate — and FIX what it finds**

```bash
# from the loom-complexity / loom-duplication skills, `diff` mode
```

Invoke the `loom-complexity` skill with `diff` and the `loom-duplication` skill with `diff`.

**Compare against the MERGE-BASE, not the committed register:** measure the touched functions
at `git merge-base HEAD main` and again at `HEAD`. The delta is the finding.

**If this branch worsened any hotspot on any axis (cc, cognitive, MI, SLOC), or introduced any
cross-file duplication pair ≥ 20 lines: FIX IT IN THIS PR.** `define_transform` is already a
long function and this plan adds three blocks to it — if the gate reddens it, the extraction
is obvious and should have existed already: pull the whole MV-watermark reconciliation
(release prior output → release stale source → bootstrap current source) into a single
`crate::mv_bootstrap::reconcile_mv_watermarks(&mut tx, &def, prior_body)` and have
`define_transform` call that one function. Do not defer this to `loom-complexity-fix`.

The likeliest duplication finding is the seed/GC preamble repeated across the new tests
(flush → end_cap → age → gc). If duplo flags it, hoist it into `end_cap_seed` as
`pub async fn reclaim_prefix(pool, catalog, src, tid)` and call it from each — that is what
the shared library is for.

Put the before/after numbers in the PR body.

---

### Task 6: Documentation registers

**Files:**
- Modify: `docs/ISSUES.md` (remove `#iss-mv-register-below-reclaimed-floor`)
- Modify: `docs/FUTURE.md` (add the `start_at` knob)
- Modify: `docs/system-capabilities/` (record the landed capability)
- Modify: `docs/ISSUES.md` — update `#iss-mv-cdc-declare-register-race`'s prose (see below)

Use the **`loom-docs-update`** skill; it knows the grammar. The substance:

- [ ] **Step 1: Close the item**

Remove the `#iss-mv-register-below-reclaimed-floor` entry from `docs/ISSUES.md` (registers
carry open work only) and record the landed capability under `docs/system-capabilities/` —
the stream/transform page: MV registration bootstraps its watermark to the source's earliest
surviving offset (recorded in `stream.mv_watermark.start_offset`) and serializes against the
source's per-table lock.

- [ ] **Step 2: File the deferred knob** (this is the ONE thing this PR is allowed to file —
it is a design decision a human declined to make now, which is the register's stated bar)

Add to `docs/FUTURE.md` under `## transform`:

```markdown
- [ ] **Per-def MV start policy (`start_at: Earliest | Latest | RequireComplete`)** `{#fut-mv-start-at-policy area:transform status:deferred from:2026-07-14-mv-register-source-floor-design pr:-  spec:-}`
  `define_transform` bootstraps a new micro-batch MV to its source's **earliest surviving**
  offsets — the policy is fixed in code (`crate::mv_bootstrap`). Two other policies are
  coherent and were deliberately not built: **latest** (`stream.bucket_offset.next` — exact,
  per-bucket, already stored, needs no scan; Kafka's `auto.offset.reset=latest`, and arguably
  what an operator adding an MV to a long-running stream actually wants) and
  **require-complete** (refuse the registration outright if the source has been truncated past
  0 — an explicit opt-in rather than the time-bomb default, since under any real retention
  policy every stream table eventually loses its prefix). Both fit a
  `#[serde(default)] start_at` field on `TransformBody::MicroBatch`/`MicroBatchJoin`
  (backward-compatible: bodies are JSONB). Deferred because it is a product decision, not a
  correctness one — the correctness half (never silently under-read, never over-hold) is
  closed by `earliest`.
```

- [ ] **Step 3: Correct a neighbouring entry that this branch falsifies**

`#iss-mv-cdc-declare-register-race`'s prose says the fix "means making the ingest declaration
path take `TRANSFORM_DEFINE_LOCK` (the global advisory lock `define_transform` already holds)
— or moving both onto a shared per-table key". **`define_transform` now takes the per-table
key** (Task 3), so half of that sentence is already true and the remaining work is smaller
than the entry claims: the ingest declaration path needs `lock_key(table)`, which it can take
without touching `TRANSFORM_DEFINE_LOCK` at all. Update the entry's fix-shape prose to say
so. Do **not** close it — the ingest-throughput decision it names is still a human's to make,
and this branch does not make it.

- [ ] **Step 4: Commit**

```bash
git add docs/
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "docs: close iss-mv-register-below-reclaimed-floor, defer the MV start-policy knob"
```

---

## Self-review notes (done at authoring time)

- **Spec coverage.** Acceptance 1 (bootstrap, recorded) → Tasks 1+2. Acceptance 2 (over-hold
  gone) → Task 2's `registering_against_a_reclaimed_source_bootstraps_the_watermark` floor
  assertion. Acceptance 3 (`lock_key(source)`, no inversion) → Task 3. Acceptance 4
  (define-before-land legal) → Task 2's `define_before_land_is_still_legal`. Acceptance 5
  (suites green) → Task 5.
- **Spec items deliberately NOT built:** the testkit contract (impossible — memory has no
  `TableRef` → `table_id`; justified at the top of this plan) and the `start_at` field
  (human decision: filed, Task 6).
- **Type consistency:** `earliest_surviving_offsets(&mut PgConnection, i64, i32) -> Result<BTreeMap<i32, i64>>`
  and `bootstrap_mv_watermarks(&mut PgConnection, &str, &TableRef) -> Result<()>` are used with
  those exact signatures at every call site in Tasks 2 and 4.
- **Known rough edge for the implementer:** the `lower(exact.entry(..).or_insert(..).into(), ..)`
  line in Task 1 Step 4 is deliberately called out as wrong Rust, with the `lower_exact`
  replacement given. Do not paste the broken form.
