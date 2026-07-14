# MV registration against a truncated source — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Registering a micro-batch MV against a stream source **bootstraps** its
`stream.mv_watermark` rows to the source's earliest *surviving* offsets (rounded down, never
up) — recorded in a new `start_offset` column — takes the source's per-table advisory lock,
and relaxes the watermark CAS so a bootstrapped start actually advances. Together these stop a
new MV from wedging (or silently mis-reading) against a source whose prefix is already gone,
and stop a registration from straddling GC's floor-read/reclaim window.

**Architecture:** The work lands almost entirely in `src/control-plane/postgres`. A new module
`mv_bootstrap.rs` computes the per-bucket earliest-surviving offset over
**live-at-the-current-tip** data (exact for the inline tier and for single-bucket Parquet
files; rounded down to the cross-bucket bound for multi-bucket files; **0** whenever a stat is
missing) and writes the bootstrap rows. `define_transform` (`transforms.rs`) calls it inside
its existing transaction, after taking `lock_key(source)` at the **top** of that transaction.
One cross-backend change rides along: the watermark CAS's `from > 0` branch relaxes from
`next_offset = from` to `next_offset <= from` (postgres + memory + a testkit contract case).

**Tech Stack:** Rust, sqlx compile-time `query!` (offline `.sqlx` cache), Postgres advisory
locks, buck2 `loom_fixture_test`.

## Decisions already made (do not relitigate)

1. **Start policy = earliest-surviving**, rounded down. (Not `latest`.) A never-reclaimed
   source bootstraps to **0**, so every existing floor test stays green.
2. **No `start_at: Earliest | Latest | RequireComplete` knob** on `TransformBody` in this PR.
   Fixed policy. The knob is filed as a FUTURE item in Task 8.
3. **A bootstrapped start is durably recorded**: migration **0045** adds `start_offset` to
   `stream.mv_watermark`.
4. **Coarse precision, always rounding down.** Cross-bucket file bounds are acceptable; a
   missing stat resolves to 0. No Parquet reads, no new low-watermark table.
5. **The watermark CAS relaxes to `next_offset <= from`** (Task 3). This is REQUIRED by
   decision 4 — see the next section. Exactly-once is preserved.

## What the spec gets WRONG (verified against this tree — trust this plan, not the spec)

PR #443 landed after the spec was written and moved almost everything it cites. Worse, two of
the spec's load-bearing *claims* are false.

**❶ The spec's "no CAS change needed" is FALSE, and it is the whole ballgame.**

The spec asserts (spec:93-95) that a bootstrapped row at `next_offset = N > 0` "is advanced by
the `from > 0` UPDATE branch (which requires the row to exist at exactly N — it does)". It does
not. The worker does **not** send the watermark as `from`. `framing_bounds`
(`src/services/worker/src/stream_mv.rs:277-317`) sets

```rust
.map(|(bucket, (min, max))| WatermarkAdvance { bucket, from: min, to: max + 1 })
```

— `from` is the **minimum `loom_offset` actually observed in the delta batches**, on the stated
assumption (`:270-276`) that "the delta is gapless per bucket from the committed watermark, so
the observed minimum offset for a bucket IS the watermark's `from` bound." **That assumption is
exactly what a truncated source violates.** And the CAS (`stream.rs:794-796`) is

```sql
update stream.mv_watermark set next_offset = $4
 where mv=$1 and source_table_id=$2 and bucket=$3 and next_offset = $5   -- $5 = from
```

0 rows ⇒ `ControlPlaneError::Conflict` (`:807-813`) ⇒ the whole MV commit rolls back.

So a bootstrap rounded *down* (bucket's row at 3, delta's first surviving offset 5) sends
`from = 5`, the CAS looks for `next_offset = 5`, finds 3, and **Conflicts on every retry,
forever**. Decision 4 (coarse) makes this the common case, not a corner: flush is not
bucket-partitioned (`iceberg_flush.rs:170`), so a flushed file spans buckets and its
`loom_offset` min is a cross-bucket bound that lowers *every* bucket's start below its true
min.

**❷ The register entry's symptom is wrong, too.** `docs/ISSUES.md` says the MV "silently
under-reads a short prefix rather than failing loud." Trace it: today a new MV over a truncated
source has **no** watermark row, reads from 0, observes min 6, sends `from = 6` → the `from > 0`
branch UPDATEs a row **that does not exist** → 0 rows → `Conflict`. It does not under-read. It
**wedges, loudly and permanently, and never runs at all.** The bootstrap is what creates the row
the CAS needs; the `<=` relaxation is what lets a rounded-down row satisfy it. Say this in the PR
body — the entry's premise is part of what this branch corrects.

**❸ Stale citations and already-done work:**

- **Every `file:line` in the spec is stale.** Re-derived numbers are in each task below.
- **Spec correction 7 is already done.** `mv_floor` already takes `&mut PgConnection`
  (`mv_floor.rs:97`). No sequencing dependency on `#iss-end-cap-ignores-mv-floor` remains.
- **Spec correction 3's premise is dead.** GC's floor read is *already inside* the GC
  transaction (`iceberg_gc.rs:162`, inside the `pool.begin()` at `:155`, under `lock_key` taken
  at `:112`). Its **conclusion still stands and is the fix**: `define_transform` takes only the
  global `TRANSFORM_DEFINE_LOCK` (`transforms.rs:399`) and never `lock_key(source)`.
  `iceberg_gc.rs:131-137` says exactly this, naming this item.
- **"`define_transform` never touches `stream.*`" is FALSE.** It already calls
  `pg_refuse_mv_over_cdc_source` (`transforms.rs:460-462`) and already deletes
  `stream.mv_watermark` rows (`:489`).
- **Spec correction 6 understates what is available.** `loom_bucket` min/max column stats **are**
  recorded per data file (`iceberg_mirror.rs:591` builds the stat list from the full Iceberg
  schema, framing columns included). A **single-bucket file is exactly identifiable**
  (`loom_bucket` min == max). Only genuinely multi-bucket files are coarse.
- **The new migration is `0045`** (latest on disk is `0044_dataset_view.sql`).
- **The BOOTSTRAP cannot be a testkit contract**, contrary to the spec's non-regression note.
  The memory backend has **no `TableRef` → `table_id` mapping at all** (`stream_tables` is keyed
  by a raw `i64` the caller supplies), so memory's `define_transform` cannot resolve an MV's
  source to a stream table id — and memory never reclaims anything. **The bootstrap and the lock
  are postgres-only.** The **CAS relaxation (Task 3) is the one part that IS cross-backend** and
  does get a testkit contract case.

## Global Constraints

- **Tests are `rust_test`/`loom_fixture_test` targets only** — never inline `#[cfg(test)] mod tests`. The `no-inline-tests` prek hook enforces this.
- New fixture tests **must** use `loom_fixture_test` (`//src/control-plane/postgres/defs.bzl`), not a bare `rust_test`, or the postgres fixture env is missing and the test cannot boot.
- Strict clippy (whole `pedantic` + `restriction` groups on lib code). No `unwrap`/`expect`/`panic`/`indexing_slicing` in `src/`. Silence locally with `#[expect(lint, reason = "...")]` — a bare `#[allow]` without a `reason` fails `allow_attributes_without_reason`. **Unused imports in a test file also redden the gate** (`tools/clippy-all.sh` treats a non-empty `[clippy.txt]` as failure).
- **Any new/changed SQL ⇒ run `./tools/sqlx-prepare.sh` and commit the `.sqlx` change.** The `//src/control-plane/postgres:sqlx-cache-check` test fails otherwise.
- `buck2 run //tools:prek -- run --all-files` before **every** commit (rustfmt is a separate hook from clippy). `git add` new files *before* the gating prek run, or they are skipped as untracked.
- Build/test with `--console none`: `buck2 build -v0 --console none //src/...`, `buck2 test --console none //src/...`.
- Dynamic SQL (the `inline_<tid>` table name) uses `sqlx::AssertSqlSafe`, with every literal sourced from our own mirror — never from user input. Established pattern: `mv_floor.rs:310-321`.

---

### Task 1: The earliest-surviving-offset query

Read-only, per-bucket, over the source's **live** data. No migration, no writes.

**Files:**
- Create: `src/control-plane/postgres/src/mv_bootstrap.rs`
- Modify: `src/control-plane/postgres/src/lib.rs` (add `pub mod mv_bootstrap;` — it MUST be `pub mod`; `mod transforms;` at `:48` is private and would hide it from the test)
- Modify: `src/control-plane/postgres/src/iceberg_flush.rs:416` (`pub(crate) fn lock_key` → `pub fn lock_key`; Task 4's test computes the key from an integration test)
- Create: `src/control-plane/postgres/tests/mv_bootstrap.rs`
- Modify: `src/control-plane/postgres/tests/end_cap_seed.rs` (add `land_more`, add `register_mv_result`)
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test` target `mv-bootstrap`)

**Do NOT touch `pg_peek_offset`** — it is already `pub(crate) async fn pg_peek_offset<'e, E: sqlx::PgExecutor<'e>>(ex, table_id, bucket) -> Result<i64>` (`stream.rs:377`), and `&mut PgConnection` satisfies `PgExecutor`. No widening needed.

**Interfaces:**
- Consumes: `crate::iceberg_mirror::live_table_id(&mut PgConnection, &str, &str) -> Result<Option<i64>>` (`iceberg_mirror.rs:307`); `crate::stream::{pg_stream_bucket_count, pg_peek_offset}`; `crate::iceberg_inline::{inline_table_name, inline_table_exists}` (`iceberg_inline.rs:42`, `:64`); `crate::backend`.
- Produces: **`pub async fn earliest_surviving_offsets(conn: &mut PgConnection, tid: i64, bucket_count: i32) -> Result<BTreeMap<i32, i64>>`** — every bucket in `0..bucket_count` present. `pub`, not `pub(crate)`: the integration test reaches it as `control_plane_postgres::mv_bootstrap::earliest_surviving_offsets`.

**The rule that governs every branch: ROUND DOWN, NEVER UP.** An overshoot skips live rows the
MV can still read — a data hole. An undershoot only makes the MV re-scan a range where nothing
survives, which yields no rows (and, after Task 3, advances cleanly). So any stat we cannot read
resolves to **0**.

- [ ] **Step 1: Extend the shared seed library**

The tests need to land rows *after* a GC, and to observe a `define_transform` that blocks or
errors. Both belong in `end_cap_seed` (the shared support library) — do **not** copy-paste
landing or registration code into the test file.

Read `end_cap_seed.rs:163-210` (`seed_source`) first and mirror its `land(...)` call argument
for argument. Append:

```rust
/// Land `rows` MORE events into a source — the "a surviving range exists above the reclaimed
/// prefix" half of the bootstrap tests. Offsets continue from the stream allocator's
/// high-water mark, so these rows sit strictly above anything GC has taken. Also used to
/// CREATE a second source: `land` declares the stream on first land (that is how
/// `seed_source` creates `s.events`), so landing into a fresh `TableRef` is enough.
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

And replace the existing `register_mv` (`end_cap_seed.rs:113-128`) with a `Result`-returning
sibling plus a thin `expect`ing wrapper, so there is **one** definition of the MV def:

```rust
/// `register_mv`, but surfacing the error — for tests that assert on BLOCKING or on a refusal
/// rather than on success.
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

(Keep `register_mv`'s existing doc comment on the pair.)

- [ ] **Step 2: Write the failing tests**

Create `src/control-plane/postgres/tests/mv_bootstrap.rs`.

**Import care (a reviewer caught this):** `mv_watermarks` is a method of
`control_plane_core::MvWatermarks`, **not** of `ControlPlane` — the trait must be in scope or
every `cp.mv_watermarks(..)` fails to compile. See `end_cap_seed.rs:24-28` for the precedent.
Import only what you use; an unused import reddens clippy.

```rust
//! The earliest-surviving-offset computation and the registration bootstrap it feeds
//! (iss-mv-register-below-reclaimed-floor).
//!
//! The invariant every test here defends: the computed start ROUNDS DOWN. It may name an
//! offset below the true earliest surviving row (harmless — the MV re-scans an empty range,
//! and the relaxed CAS still advances) but must NEVER name one above it (a silent data hole).

use std::time::Duration;

use control_plane_core::{MvWatermarks, RunId, SnapshotId, mv_key};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_gc::gc_table;
use control_plane_postgres::iceberg_mirror::end_cap_live_data_files;
use control_plane_postgres::mv_bootstrap::earliest_surviving_offsets;
use control_plane_postgres::mv_floor::{EndCapIntent, mv_floor};
use end_cap_seed::{
    advance, age_all_snapshots, current_snapshot_id, land_more, register_mv, register_mv_result,
    seed_source, tref,
};

const SEVEN_DAYS: Duration = Duration::from_secs(7 * 24 * 3600);

/// Physically destroy every offset currently live in `src`: flush (end-caps the inline copies
/// and writes a Parquet file), end-cap that file, age the snapshots past the horizon, then GC.
/// With no MV registered the floor is `None`, so GC is unguarded and really does take them.
async fn reclaim_everything(s: &end_cap_seed::Seeded) {
    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    let snap = current_snapshot_id(&s.pool, &s.src).await;
    let mut tx = s.pool.begin().await.expect("tx");
    end_cap_live_data_files(
        &mut tx,
        &s.src,
        s.tid,
        SnapshotId(snap),
        &EndCapIntent::Reframing,
    )
    .await
    .expect("end-cap the flushed file");
    tx.commit().await.expect("commit");
    age_all_snapshots(&s.pool).await;
    gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
}

/// A source nothing has ever reclaimed starts at 0 in every bucket — today's behavior,
/// preserved exactly. This is what keeps `registered_but_unrun_mv_floors_at_zero` green.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn untouched_source_starts_at_zero() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(fx, &cp, &db, &wh.path().display().to_string(), 6, Some(2), true, &[]).await;

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

/// The acceptance case: a prefix is PHYSICALLY GONE and a surviving range was landed above it.
/// The start must name the surviving range, not 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reclaimed_prefix_starts_above_the_hole() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // One bucket keeps the arithmetic exact: offsets 0..6 inline, NO MV (so GC is unguarded).
    let s = seed_source(fx, &cp, &db, &wh.path().display().to_string(), 6, Some(1), true, &[]).await;

    reclaim_everything(&s).await;
    land_more(&s.pool, &s.catalog, &s.src, 6, Some(1), true).await; // offsets 6..12

    let mut conn = s.pool.acquire().await.expect("conn");
    let starts = earliest_surviving_offsets(&mut conn, s.tid, 1)
        .await
        .expect("starts");
    assert_eq!(
        starts.get(&0).copied(),
        Some(6),
        "offsets 0..6 are gone; the earliest SURVIVING offset is 6 — starting at 0 defines a \
         first delta over a range that no longer exists"
    );
}

/// Nothing live at all: the start is the allocator's high-water mark. Starting at 0 here would
/// floor GC at 0 forever with no row to justify it — the over-hold the bootstrap exists to kill.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fully_reclaimed_source_starts_at_the_high_water_mark() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(fx, &cp, &db, &wh.path().display().to_string(), 4, Some(1), true, &[]).await;

    reclaim_everything(&s).await;

    let mut conn = s.pool.acquire().await.expect("conn");
    let starts = earliest_surviving_offsets(&mut conn, s.tid, 1)
        .await
        .expect("starts");
    assert_eq!(
        starts.get(&0).copied(),
        Some(4),
        "no live row survives; the only start that skips nothing is the allocator's next offset"
    );
}

/// A source landed straight to PARQUET (no inline tier): the live file's `loom_offset` min stat
/// supplies the bound. One bucket ⇒ the file is single-bucket ⇒ the bound is exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_tier_supplies_the_bound() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s =
        seed_source(fx, &cp, &db, &wh.path().display().to_string(), 5, Some(1), false, &[]).await;

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

- [ ] **Step 3: Add the BUCK target and run the tests to verify they fail**

Append to `src/control-plane/postgres/BUCK`, mirroring `mv-floor` (`BUCK:686-702`):

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
Expected: **BUILD FAILED** — `unresolved import control_plane_postgres::mv_bootstrap`. That is
the correct first failure.

- [ ] **Step 4: Implement `earliest_surviving_offsets`**

Create `src/control-plane/postgres/src/mv_bootstrap.rs`:

```rust
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

use control_plane_core::{Result, TableRef};
use sqlx::PgConnection;

use crate::backend;
use crate::iceberg_inline::{inline_table_exists, inline_table_name};
use crate::stream::{pg_peek_offset, pg_stream_bucket_count};

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
    // the scalar subquery for the same reason `iceberg_gc.rs:383-388` documents.
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
```

Add `pub mod mv_bootstrap;` to `src/control-plane/postgres/src/lib.rs` (next to `pub mod mv_floor;` at `:38`), and widen `lock_key` (`iceberg_flush.rs:416`) to `pub fn`.

- [ ] **Step 5: Regenerate the sqlx cache**

Run: `./tools/sqlx-prepare.sh`
Then: `git add src/control-plane/postgres/.sqlx`

- [ ] **Step 6: Run the tests to verify they pass**

Run: `buck2 test --console none //src/control-plane/postgres:mv-bootstrap`
Expected: `Tests finished: Pass 4. Fail 0.`

If `reclaimed_prefix_starts_above_the_hole` reports `Some(0)`: the GC did not reclaim — check
`seed_source` was called with **no** MVs (an MV makes the floor non-`None` and GC holds
everything) and that `age_all_snapshots` ran before `gc_table`.

- [ ] **Step 7: Lint and commit**

```bash
git add src/control-plane/postgres/src/mv_bootstrap.rs \
        src/control-plane/postgres/src/lib.rs \
        src/control-plane/postgres/src/iceberg_flush.rs \
        src/control-plane/postgres/tests/mv_bootstrap.rs \
        src/control-plane/postgres/tests/end_cap_seed.rs \
        src/control-plane/postgres/BUCK \
        src/control-plane/postgres/.sqlx
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "feat(stream): compute a stream source's earliest surviving offset per bucket"
```

---

### Task 2: Record the start — migration 0045 + the bootstrap write

**Files:**
- Create: `src/control-plane/postgres/migrations/0045_mv_watermark_start_offset.sql`
- Modify: `src/control-plane/postgres/src/mv_bootstrap.rs` (the write)
- Modify: `src/control-plane/postgres/src/transforms.rs` (`define_transform`)
- Modify: `src/control-plane/postgres/tests/mv_bootstrap.rs`

**Interfaces:**
- Consumes: `earliest_surviving_offsets` (Task 1); `crate::iceberg_mirror::live_table_id`; the existing private helpers `mv_source` (`transforms.rs:117`) / `mv_output` (`:128`); `control_plane_core::mv_key`.
- Produces: `pub async fn bootstrap_mv_watermarks(conn: &mut PgConnection, mv: &str, source: &TableRef) -> Result<()>` — idempotent (`on conflict do nothing`).

- [ ] **Step 1: Write the migration**

Create `src/control-plane/postgres/migrations/0045_mv_watermark_start_offset.sql`:

```sql
-- Where an MV was told to BEGIN reading its source, per bucket
-- (iss-mv-register-below-reclaimed-floor).
--
-- `next_offset` alone cannot answer "did this MV ever see offsets 0..N?": a row bootstrapped at
-- N by `define_transform` (because the source's prefix was already reclaimed) is byte-identical
-- to one a run advanced to N. `start_offset` records the difference, so the gap is an auditable
-- fact rather than a lost log line.
--
-- The default is the correct backfill for every pre-existing row AND for every row the watermark
-- CAS creates from 0: such an MV genuinely started at offset 0.
alter table stream.mv_watermark
    add column start_offset bigint not null default 0 check (start_offset >= 0);
```

- [ ] **Step 2: Write the failing tests**

Append to `tests/mv_bootstrap.rs`:

```rust
/// The item's acceptance test. Registering an MV against a source whose prefix is GONE
/// bootstraps its watermarks to the surviving range — and therefore does NOT reset the source's
/// GC floor to 0. Covers both the mis-read and the over-hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registering_against_a_reclaimed_source_bootstraps_the_watermark() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(fx, &cp, &db, &wh.path().display().to_string(), 6, Some(1), true, &[]).await;

    reclaim_everything(&s).await;
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
    .bind(mv.as_str())
    .fetch_one(&s.pool)
    .await
    .expect("start_offset");
    assert_eq!(
        start, 6,
        "the bootstrap is RECORDED: 'this MV never saw offsets 0..6' is an auditable fact"
    );

    let mut conn = s.pool.acquire().await.expect("conn");
    let floor = mv_floor(&mut conn, &s.src, s.tid)
        .await
        .expect("floor")
        .expect("a registered MV reads this source");
    assert_eq!(
        floor.per_bucket.get(&0).copied(),
        Some(6),
        "registering no longer drops the source's GC floor to 0 — the over-hold is gone"
    );
}

/// Registering against a never-reclaimed source bootstraps to 0 — today's behavior, unchanged.
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

/// Define-before-land stays legal: registering against a source that does not exist yet is not
/// an error, and bootstraps nothing (nothing can have been lost).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn define_before_land_is_still_legal() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(fx, &cp, &db, &wh.path().display().to_string(), 2, Some(1), true, &[]).await;

    register_mv(&cp, "mv_future", &tref("s", "nonexistent"), &tref("s", "out_f")).await;

    let wm = cp
        .mv_watermarks(&mv_key(&tref("s", "out_f")), s.tid)
        .await
        .expect("watermarks");
    assert!(wm.is_empty(), "no source, nothing to bootstrap — and no error");
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
    advance(&cp, &mv, s.tid, 0, 0, 5).await;

    register_mv(&cp, "mv_a", &s.src, &tref("s", "out_a")).await; // same name, source, output

    let wm = cp.mv_watermarks(&mv, s.tid).await.expect("watermarks");
    assert_eq!(
        wm.get(&0).copied(),
        Some(5),
        "the MV resumes where it left off — the bootstrap did not reset it"
    );
}
```

- [ ] **Step 3: Run to verify they fail**

Run: `buck2 test --console none //src/control-plane/postgres:mv-bootstrap`
Expected: `registering_against_a_reclaimed_source_bootstraps_the_watermark` FAILS — the
watermark map is empty (`None`, not `Some(6)`), because nothing bootstraps yet.

- [ ] **Step 4: Implement the bootstrap write**

Append to `src/control-plane/postgres/src/mv_bootstrap.rs`:

```rust
/// Seed `mv`'s `stream.mv_watermark` rows for `source` at the source's earliest surviving
/// offsets, recording each as the MV's `start_offset`. Called by `define_transform` inside its
/// transaction, so the registration and the start it implies commit as one unit.
///
/// A source that is not (yet) a declared stream table — or does not exist at all — is a no-op:
/// define-before-land is a legal, common flow, and a source with no offsets can have lost none.
///
/// `on conflict do nothing` is load-bearing: a redefinition that keeps the same output keeps its
/// watermarks (the MV resumes where it left off), and the bootstrap must never drag a live MV's
/// position backwards.
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
        // Deref: `query!` already takes its args by reference, so passing `&i32` would make `&&i32`,
        // which does not implement `Encode`.
        sqlx::query!(
            "insert into stream.mv_watermark \
                 (mv, source_table_id, bucket, next_offset, start_offset) \
             values ($1, $2, $3, $4, $4) \
             on conflict (mv, source_table_id, bucket) do nothing",
            mv,
            tid,
            *bucket,
            *start,
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

Call it from `define_transform` (`transforms.rs`), **immediately after** the
`pg_refuse_mv_over_cdc_source` block (`:460-462`) and **before** the prior-def `for update`
(`:474`):

```rust
        // A brand-new MV has no watermark rows, so `mv_floor` would default it to 0 in every
        // bucket and `mv_delta_scan` would define its first delta as "the source from 0" —
        // pinning the source's GC floor at 0 until the MV first runs, and (worse) leaving the
        // watermark CAS no row to advance, so the first run Conflicts and rolls back forever.
        // Seed its start explicitly instead, in THIS transaction: the registration and the
        // position it implies commit together, under the source's table lock taken at the top.
        if let Some(src) = mv_source(&def.body) {
            if let Some(out) = mv_output(&def.body) {
                crate::mv_bootstrap::bootstrap_mv_watermarks(&mut tx, &mv_key(out), src).await?;
            }
        }
```

`mv_key` is already imported in `transforms.rs`. **Placement is deliberate:** after the CDC
refusal (so a refused registration writes no rows) and before the `for update` (so this
transaction's lock order matches the commit path's — see Task 4).

- [ ] **Step 5: Regenerate the sqlx cache**

Run: `./tools/sqlx-prepare.sh` (the migration changes the schema AND there is a new `query!`).

- [ ] **Step 6: Run the tests**

Run: `buck2 test --console none //src/control-plane/postgres:mv-bootstrap`
Expected: `Pass 8. Fail 0.`

Run: `buck2 test --console none //src/control-plane/postgres:mv-floor //src/control-plane/postgres:mv-watermarks //src/control-plane/postgres:transforms //src/control-plane/postgres:sqlx-cache-check`
Expected: all pass. `registered_but_unrun_mv_floors_at_zero` (`mv_floor.rs:79`) must stay green.

- [ ] **Step 7: Lint and commit**

```bash
git add -A && buck2 run //tools:prek -- run --all-files && git add -A
git commit -m "fix(stream): bootstrap a new MV's watermark to its source's surviving offsets"
```

---

### Task 3: Relax the watermark CAS to `next_offset <= from`

**Without this, Task 2 is worse than useless** — see "What the spec gets WRONG ❶". The CAS
demands `next_offset = from`, where `from` is the delta's *observed* minimum offset, not the
watermark. A bootstrapped row rounded below that minimum (the common case, because flush is not
bucket-partitioned) never matches, so the MV Conflicts on every run, forever.

**This is the one cross-backend change in the branch:** postgres, memory, and a testkit contract
case.

**Why exactly-once survives.** `to` is `observed_max + 1 > from >= next_offset`, so an accepted
advance is strictly monotone. A duplicate or stale run is still rejected: once the winner has set
`next_offset = to`, the loser's predicate `to <= from` is false (its `from` was below `to`). And
a run can never *skip* live rows, because `mv_delta_scan` reads `loom_offset >= next_offset`
(`mv_delta.rs:203-206`) — an observed minimum above the watermark proves the offsets between
them do not exist.

**Files:**
- Modify: `src/control-plane/postgres/src/stream.rs:789-806` (`pg_advance_mv_watermark`)
- Modify: `src/control-plane/memory/src/stream.rs:116-130` (`advance_mv_watermark`)
- Modify: `src/control-plane/core/src/stream.rs:169-171` (the `WatermarkAdvance` doc — it currently says `from` is "the offset the delta was read at", which the worker does not honor)
- Modify: `src/control-plane/testkit/src/lib.rs` (`mv_watermarks_contract`, `:6482+`)

- [ ] **Step 1: Write the failing contract case**

In `src/control-plane/testkit/src/lib.rs`, inside `mv_watermarks_contract` (`:6482`), append —
this runs against **both** backends:

```rust
    // --- a watermark BELOW the delta's observed minimum still advances
    // (iss-mv-register-below-reclaimed-floor). `define_transform` bootstraps a new MV to its
    // source's earliest surviving offset ROUNDED DOWN — a file spanning buckets yields only a
    // cross-bucket bound — so a bucket's row can legitimately sit below the first offset that
    // actually survives in it. The CAS must accept that (`next_offset <= from`) or the MV can
    // never make its first commit. Exactly-once is unaffected: the advance is still monotone,
    // and a stale re-advance is still refused (asserted below). ---
    let cas_mv = "main.cas_relax_out";
    let cas_tid = 9100;
    cp.advance_mv_watermark(
        cas_mv,
        cas_tid,
        &[WatermarkAdvance { bucket: 0, from: 0, to: 1 }],
    )
    .await
    .unwrap(); // creates the row at next_offset = 1

    // The delta's observed minimum is 7 — offsets 1..7 do not survive. The row sits at 1.
    cp.advance_mv_watermark(
        cas_mv,
        cas_tid,
        &[WatermarkAdvance { bucket: 0, from: 7, to: 12 }],
    )
    .await
    .expect("a watermark BELOW the delta's observed minimum must still advance");
    assert_eq!(
        cp.mv_watermarks(cas_mv, cas_tid).await.unwrap().get(&0),
        Some(&12),
        "the undershooting watermark advanced to the delta's upper bound"
    );

    // Exactly-once still holds: replaying the SAME advance is refused, because 12 <= 7 is false.
    assert!(
        matches!(
            cp.advance_mv_watermark(
                cas_mv,
                cas_tid,
                &[WatermarkAdvance { bucket: 0, from: 7, to: 12 }],
            )
            .await,
            Err(ControlPlaneError::Conflict(_))
        ),
        "a replayed advance is still a Conflict — relaxing to `<=` did not weaken exactly-once"
    );
```

(`ControlPlaneError` and `WatermarkAdvance` are already imported in the testkit.)

- [ ] **Step 2: Run to verify it fails on BOTH backends**

Run: `buck2 test --console none //src/control-plane/postgres:mv-watermarks //src/control-plane/memory:mv-watermarks`

(If the memory-side contract target has a different name, find it: `grep -rn 'mv_watermarks_contract' src/control-plane/*/tests/`.)

Expected: FAIL on both — "a watermark BELOW the delta's observed minimum must still advance"
gets `Conflict`.

- [ ] **Step 3: Relax both backends**

`src/control-plane/postgres/src/stream.rs`, in `pg_advance_mv_watermark`, change **only** the
`from != 0` branch's predicate:

```rust
    let sql = if adv.from == 0 {
        "insert into stream.mv_watermark (mv, source_table_id, bucket, next_offset) \
         values ($1, $2, $3, $4) \
         on conflict (mv, source_table_id, bucket) do update set next_offset = $4 \
         where stream.mv_watermark.next_offset = 0"
    } else {
        // `<=`, not `=`: a bootstrapped row (`crate::mv_bootstrap`) may sit BELOW the delta's
        // observed minimum offset, because the bootstrap rounds down (a cross-bucket file stat
        // bounds every bucket). Demanding equality would Conflict on that MV's every run,
        // forever. Exactly-once is preserved: `to` = observed max + 1 > `from` >= `next_offset`,
        // so an accepted advance is strictly monotone, and a replayed advance finds
        // `next_offset = to`, for which `to <= from` is false.
        "update stream.mv_watermark set next_offset = $4 \
         where mv = $1 and source_table_id = $2 and bucket = $3 and next_offset <= $5"
    };
```

`src/control-plane/memory/src/stream.rs`, in `advance_mv_watermark`, the matching arm:

```rust
            match map.get(&key).copied() {
                // `<=`, not `==` — see the postgres adapter's `pg_advance_mv_watermark`.
                Some(current) if current <= adv.from => {
                    map.insert(key, adv.to);
                }
                None if adv.from == 0 => {
                    map.insert(key, adv.to);
                }
                current => {
                    return Err(ControlPlaneError::Conflict(format!(
                        "mv watermark advanced concurrently: {mv} source {source_table_id} \
                         bucket {} expected {} found {:?}",
                        adv.bucket, adv.from, current
                    )));
                }
            }
```

And correct the `WatermarkAdvance` doc in `src/control-plane/core/src/stream.rs:169-171`, which
is currently misleading:

```rust
/// One bucket's watermark CAS: advance `bucket` from `from` to `to`. `from` is the LOWEST
/// offset the delta actually carried for this bucket (`framing_bounds` in the worker), which
/// is at or above the committed watermark — equal to it when the delta is gapless from it, and
/// strictly above it when the offsets in between no longer survive (a GC'd prefix, or a
/// rounded-down bootstrap — `mv_bootstrap` in the postgres adapter). The CAS therefore accepts
/// `next_offset <= from` and refuses anything above it: a watermark ahead of the delta means a
/// concurrent run already covered it.
```

- [ ] **Step 4: Run to verify they pass**

Run: `buck2 test --console none //src/control-plane/postgres:mv-watermarks //src/control-plane/memory:mv-watermarks //src/control-plane/postgres:mv-bootstrap`
Expected: all pass.

- [ ] **Step 5: sqlx, lint, commit**

The changed SQL is `AssertSqlSafe` (runtime), not `query!`, so no `.sqlx` entry changes — but run
`./tools/sqlx-prepare.sh` anyway to be certain, then:

```bash
git add -A && buck2 run //tools:prek -- run --all-files && git add -A
git commit -m "fix(stream): let the watermark CAS accept an undershooting start (next_offset <= from)"
```

---

### Task 4: Close the registration/GC race — take `lock_key(source)`

Bootstrapping alone still races: GC can read the MV floor and *then* reclaim, with a
registration committing in between. loom sets no isolation level, so `pool.begin()` is READ
COMMITTED and each statement takes a fresh snapshot; the floor read takes no row locks. The fix
is to make the registration take the **same per-table advisory lock** GC serializes under —
`lock_key(source)` — which `define_transform` today does not (`iceberg_gc.rs:131-137` says so,
naming this item).

**Files:**
- Modify: `src/control-plane/postgres/src/transforms.rs` (`define_transform`, right after `TRANSFORM_DEFINE_LOCK` at `:399-402`)
- Modify: `src/control-plane/postgres/tests/mv_bootstrap.rs`

**⚠ ORDER IS LOAD-BEARING.** The commit path takes `lock_key(table)` (`iceberg_flush.rs:48-56`)
and *then* row-locks `transforms.transform` (`pg_fire_data_triggers`, `transforms.rs:281-284`,
`for update`). `define_transform` row-locks its own def at `:474`. Acquiring `lock_key(source)`
**after** that `for update` inverts the pair and deadlocks. **Take it at the very top:
`TRANSFORM_DEFINE_LOCK` → `lock_key(source)` → `transforms.transform` row locks.**
(`TRANSFORM_DEFINE_LOCK` is taken by nothing else in the tree — `transforms.rs:19` and `:399` are
its only mentions — so no path can hold `lock_key(t)` and then wait on it. `delete_transform`
takes no locks at all.)

- [ ] **Step 1: Write the failing tests**

Append to `tests/mv_bootstrap.rs`:

```rust
/// `define_transform` must serialize against the SOURCE's per-table advisory lock — the one GC
/// holds across BOTH its floor read and its reclaim. Without it, a registration commits inside
/// GC's window and GC reclaims below the brand-new MV's floor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn define_transform_blocks_on_the_sources_table_lock() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(fx, &cp, &db, &wh.path().display().to_string(), 4, Some(1), true, &[]).await;

    // Hold `lock_key(s.events)` by hand — exactly what `gc_table` does for the whole of its
    // floor-read + reclaim.
    let key = control_plane_postgres::iceberg_flush::lock_key(&s.src.schema, &s.src.name);
    let mut holder = s.pool.begin().await.expect("tx");
    sqlx::query(sqlx::AssertSqlSafe("select pg_advisory_xact_lock($1)"))
        .bind(key)
        .execute(&mut *holder)
        .await
        .expect("hold the source's table lock");

    let blocked = tokio::time::timeout(
        Duration::from_millis(750),
        register_mv_result(&cp, "mv_a", &s.src, &tref("s", "out_a")),
    )
    .await;
    assert!(
        blocked.is_err(),
        "define_transform did NOT take lock_key(source) — it committed while GC held the table \
         lock, which is exactly the race this item closes"
    );

    holder.rollback().await.expect("release");
    tokio::time::timeout(
        Duration::from_secs(10),
        register_mv_result(&cp, "mv_a", &s.src, &tref("s", "out_a")),
    )
    .await
    .expect("register once the lock is free")
    .expect("register");
}

/// A registration and a concurrent commit on the SAME source must not deadlock: the commit path
/// takes lock_key(table) then row-locks transforms.transform, so define_transform must take them
/// in that same order.
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

    let (flush, define) = tokio::join!(
        flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4())),
        register_mv_result(&cp, "mv_a", &s.src, &tref("s", "out_a")),
    );
    flush.expect("flush must not deadlock against a concurrent registration");
    define.expect("register must not deadlock against a concurrent flush");
}
```

- [ ] **Step 2: Run to verify the lock test fails**

Run: `buck2 test --console none //src/control-plane/postgres:mv-bootstrap`
Expected: `define_transform_blocks_on_the_sources_table_lock` FAILS — today the registration
commits straight through the held lock. (`registration_and_commit_do_not_deadlock` passes
already; it is the regression net, not a driver.)

- [ ] **Step 3: Take the lock**

In `define_transform`, immediately after the `TRANSFORM_DEFINE_LOCK` acquisition (`:399-402`),
before everything else:

```rust
        // Serialize this registration against its SOURCE table's flush / consolidate / GC. GC
        // reads the MV floor and reclaims under `lock_key(source)` (`crate::iceberg_gc`), and
        // READ COMMITTED gives that floor read no protection from a registration committing
        // between it and the reclaim: the new MV's floor would simply not exist yet, and GC
        // would reclaim below it. Holding the same key for this whole transaction makes the two
        // mutually exclusive.
        //
        // ORDER IS LOAD-BEARING. The commit path takes `lock_key(table)` and THEN row-locks
        // `transforms.transform` (`pg_fire_data_triggers`). Taking this here — at the top, before
        // the `for update` below — puts us in that same order. After it would deadlock.
        if let Some(src) = mv_source(&def.body) {
            let key = crate::iceberg_flush::lock_key(&src.schema, &src.name);
            sqlx::query!("select pg_advisory_xact_lock($1)", key)
                .execute(&mut *tx)
                .await
                .map_err(backend)?;
        }
```

- [ ] **Step 4: Run to verify they pass**

Run: `buck2 test --console none //src/control-plane/postgres:mv-bootstrap`
Expected: `Pass 10. Fail 0.`

- [ ] **Step 5: sqlx, lint, commit**

```bash
./tools/sqlx-prepare.sh
git add -A && buck2 run //tools:prek -- run --all-files && git add -A
git commit -m "fix(stream): serialize MV registration against its source's table lock"
```

---

### Task 5: Release an MV's watermarks on its PRIOR SOURCE (a defect found while planning)

**Found while reading `define_transform`'s watermark reconciliation — verified reachable, not
speculative.** The code releases the watermarks of a **prior OUTPUT** when a redefinition moves
the MV off it (`transforms.rs:479-502`). It does **not** release the watermarks of a prior
**SOURCE**. Redefine an MV to read a different source while keeping the same output (`mv_key`
unchanged) and the old rows — keyed `(mv, old_source_table_id, bucket)` — survive with no def
naming that source. `mv_floor`'s reader set counts "every mv holding a watermark row against the
source" (`mv_floor.rs:24-30`), so the **old source is floored at those offsets forever, by an MV
that no longer reads it, with no def to reach it.** Same permanent "GC never converges" shape as
`#iss-mv-watermark-ghost-rows`; one statement in a transaction we are already editing.

`MicroBatchJoin` has exactly one stream `source` (its second input is a static `enrich` —
`core/src/transforms.rs:75-83`), so an mv key holds watermark rows against exactly one source.

**⚠ Do NOT write the obvious version of this fix.** The tempting rule is "delete every row for
this mv whose `source_table_id` is not the current source's, and if the current source cannot be
resolved, delete them all." **That last clause breaks the testkit `transforms_contract`, which
runs against postgres.** The contract (`testkit/src/lib.rs:5804-5871`) defines an MV over
`main.mv_redef_src` — a table that does **not** exist in the mirror — advances its watermark
against a **synthetic** tid (9001), then redefines with the same output and asserts the watermark
survives ("resume, not reset", `:5866-5870`). An unresolvable source must be a **no-op**.

Key off the **prior body's source** instead: it is already in hand, it names exactly the stale
rows, and it touches nothing when the source did not move.

**Files:**
- Modify: `src/control-plane/postgres/src/transforms.rs` (the existing `match de_body(row.body)` block at `:479-502`)
- Modify: `src/control-plane/postgres/tests/mv_bootstrap.rs`

- [ ] **Step 1: Write the failing test**

```rust
/// Redefining an MV onto a DIFFERENT SOURCE (same output, so the same `mv_key`) must release the
/// old source's watermarks — else the old source is floored forever by an MV that no longer reads
/// it, and no def names those rows, so nothing can ever reach them.
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
    advance(&cp, &mv, s.tid, 0, 0, 3).await;

    // A second stream source (first land creates + declares it), and the MV is redefined onto it
    // — same output, so the same mv key.
    let other = tref("s", "events2");
    land_more(&s.pool, &s.catalog, &other, 4, Some(1), true).await;
    register_mv(&cp, "mv_a", &other, &tref("s", "out_a")).await;

    let wm_old = cp.mv_watermarks(&mv, s.tid).await.expect("watermarks");
    assert!(
        wm_old.is_empty(),
        "the MV no longer reads s.events — its watermarks there must be released, or s.events is \
         floored at offset 3 forever with no def to point an operator at"
    );

    let mut conn = s.pool.acquire().await.expect("conn");
    assert_eq!(
        mv_floor(&mut conn, &s.src, s.tid).await.expect("floor"),
        None,
        "no MV reads s.events any more: its GC floor is gone entirely"
    );
}
```

- [ ] **Step 2: Run to verify it fails**

Expected: `wm_old` still holds `{0: 3}` and `mv_floor(s.events)` is `Some(per_bucket: {0: 3})`.

- [ ] **Step 3: Release the stale source's rows**

Extend the existing arm — it currently binds only `output`; bind `source` too:

```rust
        if let Some(row) = prior {
            match de_body(row.body) {
                Ok(
                    TransformBody::MicroBatch { output, source, .. }
                    | TransformBody::MicroBatchJoin { output, source, .. },
                ) => {
                    let old_mv = mv_key(&output);
                    if new_mv.as_ref() != Some(&old_mv) {
                        // (existing) the OUTPUT moved: the old key's rows are orphaned.
                        sqlx::query!("delete from stream.mv_watermark where mv = $1", old_mv)
                            .execute(&mut *tx)
                            .await
                            .map_err(backend)?;
                    } else if mv_source(&def.body) != Some(&source) {
                        // The SOURCE moved while the OUTPUT stayed: the `mv_key` is unchanged, so
                        // the branch above did not fire, but the rows keyed
                        // `(mv, OLD source_table_id, bucket)` are now referenced by no def.
                        // `delete_transform` keys on the def's CURRENT source and can never reach
                        // them, while `crate::mv_floor`'s reader set counts every mv holding a
                        // watermark row against a source — so the old source would stay floored at
                        // the departed MV's offsets forever.
                        //
                        // Keyed on the PRIOR source (not "everything that is not the current
                        // source"): it names exactly the stale rows, and it is a no-op when the
                        // source did not move — which is what keeps the resume case, and the
                        // testkit contract's synthetic-tid redefinition, intact.
                        if let Some(old_tid) = crate::iceberg_mirror::live_table_id(
                            &mut tx,
                            &source.schema,
                            &source.name,
                        )
                        .await?
                        {
                            sqlx::query!(
                                "delete from stream.mv_watermark \
                                 where mv = $1 and source_table_id = $2",
                                old_mv,
                                old_tid,
                            )
                            .execute(&mut *tx)
                            .await
                            .map_err(backend)?;
                        }
                    }
                }
                // ... the Physical/Typed and Err arms stay exactly as they are ...
            }
        }
```

**Leave the Task 2 bootstrap call where it is** (before the `for update`). It and this delete
touch disjoint rows in every case, so their order does not matter:

| redefinition | bootstrap inserts | this delete removes |
|---|---|---|
| output moved (v1 → v2) | rows for `(mv_key(v2), current source)` | rows for `mv_key(v1)` — the *existing* arm |
| source moved, output kept | rows for `(mv, NEW source tid)` | rows for `(mv, OLD source tid)` |
| body flips to `Physical` | nothing (`mv_output` is `None`) | rows for the old `mv_key` — the *existing* arm |

**Residual gap, accepted:** if the *prior* source's mirror row is itself gone (the table was
dropped), `live_table_id` returns `None` and the stale rows survive — but so does nothing else:
the source no longer exists, so there is nothing left to floor.

- [ ] **Step 4: Run the tests**

Run: `buck2 test --console none //src/control-plane/postgres:mv-bootstrap //src/control-plane/postgres:mv-floor //src/control-plane/postgres:transforms`
Expected: all pass (11 in `mv-bootstrap`). **`//src/control-plane/postgres:transforms` carries
the `transforms_contract` — if it reddens, you wrote the "delete them all" version.**

- [ ] **Step 5: sqlx, lint, commit**

```bash
./tools/sqlx-prepare.sh
git add -A && buck2 run //tools:prek -- run --all-files && git add -A
git commit -m "fix(stream): release an MV's watermarks on its prior SOURCE, not just its prior output"
```

---

### Task 6: The worker e2e — prove the first delta is neither short nor wedged

The spec asked for this (spec:144-145) and it is the only test that exercises the whole chain:
GC a prefix → register → run → the delta covers exactly the surviving range and the CAS commits.
Tasks 1-5 are all unit-level; **without this, the branch could land green with the MV still
wedged.**

**Files:**
- Modify: `src/services/worker/tests/stream_mv_e2e.rs`

- [ ] **Step 1: Write the failing test**

Model it on `gc_holds_a_lagging_mvs_end_capped_files_and_converges_on_catch_up`
(`stream_mv_e2e.rs:625+`) — it already has the land/GC/`run_micro_batch` machinery. The shape:

1. Land events into `s.events` (1 bucket) — offsets 0..3.
2. **Do not register the MV yet.** Flush, end-cap the file, age, `gc_table` → offsets 0..3 are
   physically gone.
3. Land more events — offsets 3..6 survive.
4. **Now** `define_transform` the MV over `s.events` (this is the ordering that matters — the
   existing tests at `:656` register *before* the first land, which is why they never hit this
   path; do **not** "tidy" them into this order).
5. `run_micro_batch(...)`, then assert:
   - the run is `Succeeded` (**not** `Conflict` — this is the wedge check),
   - `cp.mv_watermarks(&mv, tid)` is `{0: 6}` — the delta covered the surviving range and the CAS
     advanced from the bootstrapped 3,
   - the output table's rows are exactly the 3 surviving events (**not** 6, and **not** 0).

Assert on the output row count explicitly: "the delta is not short" is the acceptance criterion
and a watermark assertion alone does not prove it.

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/services/worker:stream-mv-e2e`
(Find the exact target: `grep -n 'stream_mv_e2e' src/services/worker/BUCK`.)

Expected: with Tasks 1-5 in place it should **pass**. If you are running this task in isolation
before them, it fails with the run in `Failed`/`Conflict`. Either way, **temporarily revert Task
3's `<=`** and confirm this test goes red — that is the proof the CAS relaxation is what makes
the first delta commit, and it is the single most important verification in this branch. Restore
Task 3 afterwards.

- [ ] **Step 3: Lint and commit**

```bash
git add -A && buck2 run //tools:prek -- run --all-files && git add -A
git commit -m "test(worker): a new MV over a GC'd source reads the surviving range and commits"
```

---

### Task 7: Full-suite verification and the metric gate

- [ ] **Step 1: Build and test the whole first-party tree**

Run: `buck2 build -v0 --console none //src/...` → silent, exit 0.
Run: `buck2 test --console none //src/... -j 8` → `Fail 0`.

**The `-j 8` cap is mandatory locally**: the full suite otherwise starves the 8 postgres fixture
boot slots and throws non-deterministic 120s timeouts that look like real failures.

Watch specifically (they read watermarks and the plan changed what registration writes):
`//src/control-plane/postgres:{mv-bootstrap,mv-floor,mv-watermarks,transforms,stream-mv-triggers,define-transform-refuse,mv-source-refuse,data-triggers}`,
`//src/control-plane/memory:*`, `//src/services/worker:{stream-mv-e2e,stream-mv-join-triggers,transform-e2e}`,
`//src/services/engine:{mv-commit-wire,transform-wire,scheduler}`, `//src/services/runtime:admin-management`.

Two known-fragile assertions that survive only by accident — **do not "tidy" them**:
- `stream_mv_e2e.rs:438-439` `assert!(before.is_empty(), "no watermark recorded before any run")`
  — survives only because that test drives `run_micro_batch` directly and never calls
  `define_transform`.
- `stream_mv_e2e.rs:709-712` asserts the whole map equals `[(0,3)]` — survives only because its
  `define_transform` (`:656`) runs **before** the source is landed (`:687`), so the bootstrap
  no-ops.

- [ ] **Step 2: Run the metric gate — and FIX what it finds**

Invoke `loom-complexity` with `diff` and `loom-duplication` with `diff`. **Compare against the
MERGE-BASE, not the committed register:** measure the touched functions at
`git merge-base HEAD main` and again at `HEAD`.

**If this branch worsened any hotspot on any axis (cc, cognitive, MI, SLOC), or introduced any
cross-file duplication pair ≥ 20 lines: FIX IT IN THIS PR.**

`define_transform` is already long and this branch adds three blocks to it. If the gate reddens
it, the extraction is obvious and should have existed already: pull the whole MV-watermark
reconciliation (release prior output → release stale source → bootstrap current source) into one
`crate::mv_bootstrap::reconcile_mv_watermarks(&mut tx, &def, prior_body)` and have
`define_transform` call that. Do not defer to `loom-complexity-fix`.

The likeliest duplication finding is the seed/GC preamble across the new tests — which Task 1
already hoists into `reclaim_everything`. If duplo still flags it, move that helper into
`end_cap_seed` and share it.

Put the before/after numbers in the PR body.

---

### Task 8: Documentation registers

Use the **`loom-docs-update`** skill (it knows the grammar).

- [ ] **Step 1: Close the item**

Remove `#iss-mv-register-below-reclaimed-floor` from `docs/ISSUES.md` (registers carry open work
only) and record the landed capability under `docs/system-capabilities/` (the stream/transform
page): MV registration bootstraps its watermark to the source's earliest surviving offset
(recorded in `stream.mv_watermark.start_offset`), serializes against the source's per-table lock,
and the watermark CAS accepts an undershooting start.

- [ ] **Step 2: File the deferred knob** (the ONE thing this PR may file — a design decision a
human declined to make now, which is the register's stated bar)

Add to `docs/FUTURE.md` under `## transform`:

```markdown
- [ ] **Per-def MV start policy (`start_at: Earliest | Latest | RequireComplete`)** `{#fut-mv-start-at-policy area:transform status:deferred from:2026-07-14-mv-register-source-floor-design pr:- spec:-}`
  `define_transform` bootstraps a new micro-batch MV to its source's **earliest surviving**
  offsets — the policy is fixed in code (`crate::mv_bootstrap`). Two other policies are coherent
  and were deliberately not built: **latest** (`stream.bucket_offset.next` — exact, per-bucket,
  already stored, needs no scan; Kafka's `auto.offset.reset=latest`, and arguably what an operator
  adding an MV to a long-running stream actually wants) and **require-complete** (refuse the
  registration outright if the source has been truncated past 0 — an explicit opt-in rather than
  the time-bomb default, since under any real retention policy every stream table eventually loses
  its prefix). Both fit a `#[serde(default)] start_at` field on
  `TransformBody::MicroBatch`/`MicroBatchJoin` (backward-compatible: bodies are JSONB). Deferred
  because it is a product decision, not a correctness one — the correctness half (never mis-read,
  never wedge, never over-hold) is closed by `earliest`.
```

- [ ] **Step 3: Correct the prose this branch falsifies**

Two places now contain statements that are false once this lands. Fix both:

1. `src/control-plane/postgres/src/iceberg_gc.rs:131-137` — says closing the registration race
   "needs the registration to take the same per-table lock GC serializes under — that is
   `#iss-mv-register-below-reclaimed-floor`, which remains OPEN." It no longer remains open;
   `define_transform` now takes `lock_key(source)`. Rewrite to state the invariant that now holds.
2. `docs/ISSUES.md` → `#iss-mv-cdc-declare-register-race` — its fix-shape prose says the fix
   "means making the ingest declaration path take `TRANSFORM_DEFINE_LOCK` … or moving both onto a
   shared per-table key." **`define_transform` now takes the per-table key**, so the remaining
   work is smaller than the entry claims: only the ingest declaration path needs
   `lock_key(table)`. Update the prose. **Do not close it** — the ingest-throughput decision it
   names is still a human's to make, and this branch does not make it.

- [ ] **Step 4: Commit**

```bash
git add -A && buck2 run //tools:prek -- run --all-files && git add -A
git commit -m "docs: close iss-mv-register-below-reclaimed-floor, defer the MV start-policy knob"
```

---

## For the PR body (do not lose these)

- **The register entry's symptom was wrong.** It said the MV "silently under-reads a short
  prefix." It does not — it *wedges*: the CAS has no row to advance, Conflicts, and the MV never
  runs. State the corrected symptom.
- **The spec's "no CAS change needed" was wrong**, and that is why Task 3 exists.
- **New user-visible property:** `define_transform` on a micro-batch MV now blocks behind any
  in-flight flush / consolidate / GC **of its source**, with no timeout (`pg_advisory_xact_lock`
  blocks). An admin `POST /transforms` can therefore stall for the duration of a GC of a large
  table. This is the intended trade (structural exclusion over throughput) but it is new, and it
  should be called out rather than discovered.
- The metric-gate before/after numbers.
