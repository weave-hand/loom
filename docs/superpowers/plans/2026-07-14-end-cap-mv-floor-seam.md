# End-cap seam — the MV floor at the point of harm Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close `iss-end-cap-ignores-mv-floor` — make it structurally impossible for an end-capping path to remove offsets a micro-batch MV has not read, by refusing the three illegitimate configurations outright and making every end-cap primitive declare its intent.

**Architecture:** Three layers, in order of how much they buy. (1) **Refuse** — the two unguarded lossy paths (`overwrite_table` over a declared stream table; the COW consolidate fold over a declared *log* table) and the one unserviceable registration (a micro-batch MV over a *CDC* source) are not legitimate operations, so they are refused rather than guarded, which dissolves them. (2) **The seam** — `EndCapIntent` becomes a required argument of all five end-cap primitives, so every present and future retention path must say whether its end-cap *removes* offsets from the live set (consult the MV floor) or merely *reframes* them (flush, compaction — same rows, same offsets, no consult). (3) **Skip-and-re-arm** — the CDC consolidate fold converts a floor refusal into a clean decline instead of erroring, because the job queue has no max-attempts.

**Tech Stack:** Rust, sqlx (compile-time `query!` + `AssertSqlSafe` for dynamic identifiers), Postgres, buck2, DataFusion (engine-serving), Arrow.

## Global Constraints

- **Tests are `rust_test` / `loom_fixture_test` targets only** — never inline `#[cfg(test)] mod tests`. New fixture tests MUST use `loom_fixture_test` (`src/control-plane/postgres/defs.bzl`), or they run without the fixture env and fail to boot Postgres.
- **Strict clippy** (whole `pedantic` + `restriction` groups on production lib/bin code). No `unwrap`/`expect`/`panic`/`indexing_slicing` in production code. Silence locally with `#[expect(lint, reason = "...")]`. **An unused import fails the gate** (`tools/clippy-all.sh` treats a non-empty `[clippy.txt]` as failure) — import exactly what you name.
- **`buck2 run //tools:prek -- run --all-files` before every commit.** rustfmt is a separate hook; clippy-clean ≠ lint-clean. `git add` new files *before* the gating prek run — prek skips untracked files.
- **New or changed compile-time SQL ⇒ `bash tools/sqlx-prepare.sh`, and commit the `.sqlx` change.** This is a **hard requirement, not a fallback** — the `//src/control-plane/postgres:sqlx-cache-check` test enforces freshness. (It boots the hermetic Postgres; `initdb` refuses to run as root, so run it on a non-root host.)
- **Build/test invocation:** `buck2 build -v0 --console none //src/...`, `buck2 test --console none //src/...`. Run the full suite with `-j 8` (the Postgres fixture has 8 boot slots; unthrottled runs produce non-deterministic 120s timeouts that look like real failures).
- **Conventional Commits** enforced by a `commit-msg` prek hook.
- GC's commit-then-delete ordering and advisory-lock discipline are unchanged — this seam guards **end-cap**, not reclaim.

## Verified-idiom notes (do not improvise these — they were got wrong once already)

- Fixture: `let fx = PgFixture::shared();` then `let (cp, db) = fx.fresh_db().await;` and `let wh = tempfile::tempdir().expect("warehouse");`. **`PgFixture::boot()` and `PgControlPlane::connect()` DO NOT EXIST.** `fresh_db` generates the db name — never hard-code one.
- `flush_table(catalog: &SqlCatalog, pool: &PgPool, table: &TableRef, run_id: RunId) -> Result<Option<SnapshotId>>` — **catalog first, and a 4th `RunId`**.
- `consolidate_table(&cp, &sql_catalog, &pool, &table) -> Result<i64, EngineServingError>` — **control plane first, four args.**
- `LineageEvent { run_id, event_type, event_time, inputs, outputs, payload }` — there is no `job_name`. Outputs are `DatasetRef`, built with `DatasetId::from(table).dataset_ref()`.
- Postgres `select 1` yields **int4**; binding it to `i64` is a *runtime* decode error. The house idiom for an existence probe is `select exists(select 1 …)` → `bool` (`iceberg_inline.rs:141-147`).

## Decisions taken (the spec's open questions, resolved before planning)

The spec (`docs/superpowers/specs/2026-07-14-end-cap-mv-floor-seam-design.md`) ends with four questions it says a human must decide. They were researched against the tree and decided. **These override the spec where they differ:**

1. **Is overwriting a declared stream table ever legitimate? — No, except the CDC consolidate fold.** `ActionEngine::overwrite_table` has **zero production callers** (single-step mutate routes through `write_delta`; multi-step through `write_steps`, which already refuses stream targets at `iceberg_landing.rs:588-593`). ⇒ **Refuse** declared stream targets on the public overwrite entrypoints; give the CDC fold a private framed entrypoint. Dissolves E6.
2. **What does a correct COW fold over a log stream table mean? — Nothing; refuse the mutation.** The spec proposes refusing at `bind`, which **does not work**: `bind()` is not on any production route (`/admin/models` and infer-and-create call `define_type` directly), and four distinct routes reach the state. They all converge at `write_inline_delta`, which already loads `pg_stream_meta` three statements before it unconditionally sets `has_shadow`. ⇒ **Refuse there.** Dissolves E7.
3. **Refuse vs defer for the consolidate job? — Neither: skip and re-arm.** `RetryPolicy::Retry` has **no max-attempts** (`postgres/src/queue.rs:116-144`), so an `Err` is a 60-second failure drumbeat forever; `Abandon` leaves the table's shadow tier permanently unfolded. ⇒ **Warn naming the laggard, `clear_consolidate_trigger`, return `Ok(0)`** — what flush's shadow guard, consolidate's own stale-flag branch (`consolidate.rs:404-414`), and `gc_locked` already do.
4. **Should `compact_table` carry the guard itself? — Yes, structurally, via the intent argument.** Compaction is `Reframing`, so the guard short-circuits and behavior is byte-identical — but it now *declares* that rather than relying on its enqueue producers to skip stream tables.
5. **[Added after plan review] A micro-batch MV over a CDC source is refused — in BOTH directions.** `define_transform` happily registers one while `mv_delta_scan` accepts **log sources only** (`mv_delta.rs:72-82`), so its watermark can never advance, its floor is pinned at 0 forever, and the CDC fold would decline on every attempt — "skip and re-arm" would re-arm into the identical skip and **permanently disable that table's consolidation**. ⇒ Task 5 refuses it at **registration** (source already declared CDC) *and* at **declaration** (`reconcile_stream_mode`/`declare_cdc` on a table a micro-batch MV already sources). **Both are required.** A registration-only guard is trivially defeated by ordering — register the MV over a not-yet-existing source (which is legitimate and must stay allowed: an MV's source becomes a log stream on its first `?mode=stream` write), then write it with `?mode=cdc`. That reaches the identical wedge through a path the registration guard never sees.

**Also decided:** `iss-mv-register-below-reclaimed-floor` stays **open**. Task 1's `&PgPool → &mut PgConnection` refactor closes that item's *race* half for free; its other half (bootstrapping a watermark at registration) is out of scope. Say so in the PR body; do not close it.

## What this actually buys — state it honestly

**After Tasks 3, 4 and 5 there is no production path by which a `Removing` end-cap can touch a declared LOG stream table.** Every producer was enumerated: `register_files(Overwrite)` (both callers already refuse stream targets), `overwrite_with_cap`/`overwrite_truncate` (Task 3 refuses), `write_mirror(overwrite)` (only reachable through `overwrite_with_cap`), `apply_commit_extras`'s `end_cap` (flush ⇒ `Reframing`; a consuming overwrite ⇒ refused), `write_inline_delta` (Task 4 refuses), `mark_dropped` (`Destroying` by design).

So: **the refusals are the fix.** The seam (Task 2) is a *type-level constraint on future retention paths* — its runtime guard is load-bearing on zero paths today. That is a deliberate, accepted trade (the whole reason this defect existed is that guarding by convention let E6 and E7 appear unnoticed), but the plan must not pretend the guard is proven end-to-end on a live lossy path, because after this work there is no such path. Do not overclaim it in the PR body either.

## Corrections to carry into `docs/ISSUES.md` at close (Task 8)

The register entry is wrong in five places. The closing PR must fix the record, not just delete the entry:

1. The COW/shadow fold is **not** part of `iceberg_compact::compact_table` — it is `consolidate_table`'s COW arm (`consolidate.rs:356-510`).
2. "All current paths are benign" is **false** — E6 and E7 were live, unguarded, lossy end-caps over exactly the MV source class.
3. The CDC consolidate fold (E8) is **already** a built lossy end-cap, held back only by `mv_delta_scan`'s log-only source filter.
4. **Flush is missing from the entry**, and it is the constraint that makes the naive "refuse any end-cap above the floor" seam wrong. It has **two** commit paths, not one.
5. E7 is worse than recorded: the fold projects user columns only while `include_framing` resolves `true`, so `coerce_batch_to_ice` indexes a 3-column-short batch positionally and **panics**; and an all-tombstone fold produces an empty batch, which short-circuits into `overwrite_truncate` and silently destroys the entire offset range.

---

## File Structure

**Modified — control plane (`src/control-plane/postgres/`):**
- `src/mv_floor.rs` — the seam. `mv_floor` → `&mut PgConnection`; gains `EndCapIntent`, `removal_blocked`, `guard_end_cap`, `MV_FLOOR_REFUSAL_PREFIX`.
- `src/iceberg_mirror.rs` — `end_cap_files_by_path`, `end_cap_live_data_files`, `mark_dropped` take `&TableRef` + `&EndCapIntent`.
- `src/iceberg_inline.rs` — `end_cap_live_inline_rows`, `end_cap_inline_rows_by_id` take `&TableRef` + `&EndCapIntent`; `write_inline_delta` refuses a declared log table.
- `src/iceberg_gc.rs` — reads the floor inside the GC transaction.
- `src/iceberg_landing.rs` — `register_files` maps `WriteMode` → intent; `overwrite_with_cap`/`overwrite_truncate` refuse declared stream targets; new `overwrite_stream_base`.
- `src/iceberg_flush.rs` — **both** end-capping commits declare `Reframing` (`:180` non-CDC, `:295` CDC base).
- `src/iceberg_sql_catalog/commit_mirror.rs` — `CommitExtras` carries the intent; `write_mirror` gains an intent param; `apply_commit_extras` gains a `&TableRef`.
- `src/iceberg_sql_catalog/catalog.rs` — `mark_dropped` call site passes `Destroying`.
- `src/transforms.rs` — `define_transform` refuses a micro-batch MV over a CDC source.

**Modified — engine-serving (`src/services/engine-serving/`):**
- `src/consolidate.rs` — CDC arm pre-checks the floor and skips-and-re-arms; uses the framed entrypoint; defensive `Log` arm.
- `src/action_writer.rs` — `write_delta` and `overwrite_table` map `ControlPlaneError::Validation` → `EngineServingError::Validation` (without this the new refusals surface as **HTTP 500, not 422**).

**Tests — new:**
- `postgres/tests/end_cap_seed.rs` — a **`rust_library`** (`//src/control-plane/postgres:end-cap-seed`), NOT a test target. The shared seed/assert helpers every new fixture test in this plan uses, and which `tests/mv_floor.rs` migrates onto in Task 7. Mirrors the established `//src/services/query-api:e2e-support` pattern (`query-api/BUCK:338-356`). **Operator decision: the seed trio is shared, not copied per file** — do not re-copy `tref`/`columns`/`batch`/`lineage` into any test file.
- `postgres/tests/end_cap_intent.rs`, `postgres/tests/stream_write_refuse.rs`, `postgres/tests/mv_source_refuse.rs`, `engine-serving/tests/consolidate_mv_floor.rs` (+ four `loom_fixture_test` targets, each depending on `end-cap-seed`).

**Tests — modified:** `postgres/tests/mv_floor.rs` (synthetic raw-SQL helper **deleted**; flush over-refusal tests added), `postgres/tests/stream_overwrite_framing.rs` (re-pointed at the framed entrypoint), `postgres/tests/iceberg_overwrite.rs:331` + `postgres/tests/dataset_view.rs:100` (signature updates).

## Acceptance mapping

| Spec acceptance | Where it is met |
| --- | --- |
| 1. A `Removing` end-cap at/above an MV's read position is refused, atomically with the caller's write | Task 2a (`guard_end_cap` on the caller's tx) + Task 2b (`removing_end_cap_primitive_is_refused`, a real `end_cap_live_data_files` call in a real tx). **NOT** on the `overwrite_table` path the spec names — Task 3 refuses that path outright, which is strictly stronger. See *What this actually buys*. |
| 2. Flush and compaction of unconsumed offsets still succeed (`Reframing`) — the over-refusal test | Task 7 (`flush_with_a_lagging_mv_still_succeeds`, the real `flush_table`) + Task 2b (`cdc_flush_with_a_floored_source_still_succeeds`, the CDC flush — and `reframing_end_cap_primitive_commits`) |
| 3. The drop bypass still converges and still names stranded MVs | Task 2a (`destroying_bypasses_the_floor`); existing `dropped_source_bypasses_the_floor` stays green |
| 4. Every one of the five end-cap primitives requires an explicit intent | Task 2b |
| 5. Existing suites green | Task 8 |

**Do not** weaken Task 3's refusal in order to demonstrate the floor guard on the overwrite path. The refusal is the fix; the guard is the structural backstop behind it.

---

### Task 1: `mv_floor` on a connection, read inside the GC transaction

`mv_floor` takes `&PgPool` today, which makes a transaction-atomic guard impossible: a guard on a *different* connection than the caller's write can straddle a concurrent `define_transform`. Its three dependencies (`pg_stream_bucket_count`, `pg_micro_batch_readers`, `pg_mv_watermarks`) are already `E: sqlx::PgExecutor`-generic — only `mv_floor`'s own `fetch_all(pool)` is pool-bound.

**No new test.** This is a pure signature change whose gate is the existing 13-test `//src/control-plane/postgres:mv-floor` suite staying green; a new test here could only assert old behavior under a new name.

**Files:**
- Modify: `src/control-plane/postgres/src/mv_floor.rs:69` (`mv_floor`), `:133` (`stranded_mv_readers`), `:151` (`mv_readers`), `:158` (`watermark_mvs`)
- Modify: `src/control-plane/postgres/src/iceberg_gc.rs:135-157` (the floor read in `gc_locked`), `:326` (`stranded_readers`)
- Modify: `src/control-plane/postgres/tests/mv_floor.rs` (5 `mv_floor(&s.pool, …)` call sites)

**Interfaces:**
- Produces: `pub async fn mv_floor(conn: &mut PgConnection, table: &TableRef, tid: i64) -> Result<Option<MvFloor>>`; `pub async fn stranded_mv_readers(conn: &mut PgConnection, table: &TableRef, tids: &[i64], include_registered: bool) -> Result<BTreeSet<String>>`. Task 2a builds `guard_end_cap` on the first.

- [ ] **Step 1: Change the four signatures in `mv_floor.rs`**

Replace `use sqlx::PgPool;` with `use sqlx::PgConnection;`, then take a connection and reborrow at each use:

```rust
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
    // ... body from `let mut wm` onward is UNCHANGED ...
}

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

async fn mv_readers(
    conn: &mut PgConnection,
    table: &TableRef,
    tid: i64,
) -> Result<BTreeSet<String>> {
    let mut out = pg_micro_batch_readers(&mut *conn, table).await?;
    out.extend(watermark_mvs(&mut *conn, tid).await?);
    Ok(out)
}

async fn watermark_mvs(conn: &mut PgConnection, tid: i64) -> Result<Vec<String>> {
    sqlx::query_scalar!(
        "select distinct mv from stream.mv_watermark where source_table_id = $1",
        tid,
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(backend)
}
```

Update the module doc (`mv_floor.rs:19-21`), which currently says wiring the floor into the end-cap paths "is `#iss-end-cap-ignores-mv-floor`":

```rust
//! being the **seam** the END-CAP-issuing paths call before they end-cap an offset
//! an MV has not consumed (that is where the hole is actually created) — see
//! [`EndCapIntent`] / [`guard_end_cap`] below.
```

- [ ] **Step 2: Read the floor inside the GC transaction**

In `gc_locked`, replace the block from `// 2. Resolve the (maybe) live incarnation` through the counter declarations (`iceberg_gc.rs:135-157`) with:

```rust
    // 2. One transaction for the whole run. The floor is read INSIDE it (2b), so the
    //    floor read and the reclaim cannot straddle a concurrent `define_transform` — a
    //    brand-new MV either commits before this tx's snapshot (and floors us) or after
    //    it (and finds its source intact). Reading it on the pool, as this once did, left
    //    exactly that window open (`#iss-mv-register-below-reclaimed-floor`).
    let mut tx = pool.begin().await.map_err(backend)?;
    let live = live_table_id(&mut tx, &table.schema, &table.name).await?;
    let dropped = dropped_table_ids(&mut tx, &table.schema, &table.name).await?;

    // 2b. The MV read-position floor of the LIVE incarnation (`None` for a table no
    //     micro-batch MV reads — the guard then goes NULL, a pre-floor no-op).
    let floor = match live {
        Some(tid) => mv_floor(&mut tx, table, tid).await?,
        None => None,
    };
    let file_guard: Option<i64> = floor.as_ref().map(MvFloor::min_offset);
    let stranded = stranded_readers(&mut tx, table, live, &dropped).await?;

    // 3. Reclaim the live incarnation (floor-guarded), then every dropped incarnation
    //    (unguarded), collecting every object path to delete post-commit.
    let mut paths: Vec<String> = Vec::new();
    let mut data_file_rows = 0u64;
    let mut inline_rows = 0u64;
    let mut held_by_mv_floor = 0u64;
```

The `if let Some(tid) = live { … }` / `reclaim_dropped` / `tx.commit()` block below is unchanged.

- [ ] **Step 3: `stranded_readers` takes the transaction**

`iceberg_gc.rs:326` currently takes `pool: &PgPool`. Change to `conn: &mut sqlx::PgConnection` and forward: `stranded_mv_readers(&mut *conn, table, &tids, live.is_none())`. Body otherwise unchanged.

- [ ] **Step 4: Update the five test call sites**

In `tests/mv_floor.rs` (`plain_table_has_no_floor`, `stream_table_without_readers_has_no_floor`, `registered_but_unrun_mv_floors_at_zero`, `slowest_mv_sets_the_floor_per_bucket`, `caught_up_mv_floors_at_its_watermark`), each `mv_floor(&s.pool, …)` becomes:

```rust
    let mut conn = s.pool.acquire().await.expect("conn");
    let floor = mv_floor(&mut conn, &s.src, s.tid).await.expect("floor");
```

Keep each test's existing assertions; only the first argument changes.

- [ ] **Step 5: Verify the refactor is behavior-preserving**

Run: `buck2 test --console none //src/control-plane/postgres:mv-floor //src/control-plane/postgres:iceberg-gc`
Expected: `mv-floor` → `Pass 13. Fail 0`; all `iceberg-gc` tests pass. Any failure is a real regression — this task changes no behavior.

- [ ] **Step 6: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres/src/mv_floor.rs src/control-plane/postgres/src/iceberg_gc.rs src/control-plane/postgres/tests/mv_floor.rs
git commit -m "refactor(mv-floor): read the floor on a connection, inside GC's transaction"
```

---

### Task 2s: the shared postgres test-seed library

**Operator decision.** Every fixture test in this plan needs the same seed shapes. They are **shared through a library, not copied per file** — the `//src/services/query-api:e2e-support` pattern (`query-api/BUCK:338-356`), which CLAUDE.md names as the fix for exactly this copy-paste. Tasks 2a/2b/3/5/6 consume it; Task 7 migrates `tests/mv_floor.rs` onto it.

**Files:**
- Create: `src/control-plane/postgres/tests/end_cap_seed.rs`
- Modify: `src/control-plane/postgres/BUCK` (a `rust_library` — **not** a `loom_fixture_test`)

**Interfaces — Produces** (every later task imports from `end_cap_seed`):
- `tref(schema, name) -> TableRef`
- `columns() -> Vec<ColumnSpec>` — `(id: long NOT NULL, label: string NULLABLE)`. **The string column is load-bearing**: it puts a non-numeric `max_value` into `data_file_column_stat`, so the floor's file guard really runs against TEXT bounds and its `::bigint` cast must stay OUTSIDE the scalar subquery. `label` is **nullable** because Task 4's tombstone writes every non-id column NULL.
- `batch(rows: i64) -> (SchemaRef, Vec<RecordBatch>)` — ids `0..rows`, labels `e0..`
- `lineage(&TableRef) -> LineageEvent`
- `Seeded { pool, catalog, src, tid }` and `seed_source(fx, cp, db, wh, rows, buckets: Option<i32>, inline: bool, mvs: &[(&str,&str)]) -> Seeded` — **lifted verbatim from `tests/mv_floor.rs:122-186`**, which is its current home. Keep its `#[expect(clippy::too_many_arguments, reason = ...)]`.
- `register_mv(cp, name, source, output)` and `advance(cp, mv, source_tid, bucket, from, to)` — also lifted from `tests/mv_floor.rs:96-119`.
- CDC shapes (Tasks 2b/6): `cdc_specs() -> Vec<ColumnSpec>` `(id: long, val: long)`; `row_batch(id, val)`; `id_only_batch(id)`; `framed_cdc_batch(id, val, bucket, offset)` (user cols then `loom_change_kind: Utf8`, `loom_bucket: Int32`, `loom_offset: Int64` — the order `augment_with_framing` builds); `FlooredCdc { pool, catalog, table, tid, _wh }` and `seed_floored_cdc(fx, cp, db) -> FlooredCdc`.
- Assertions: `live_file_count(pool, tid) -> i64`, `data_file_count(pool, tid) -> i64`, `end_capped_inline_count(pool, tid) -> i64`, `current_snapshot_id(pool, table) -> i64`, `age_all_snapshots(pool)` — the last four lifted from `tests/mv_floor.rs:348-411`.

**It is a `rust_library`, so it does NOT get `LOOM_TEST_LINT_ALLOWS`.** Give it the crate-level allow block the sibling support libraries carry (copy from `query-api/tests/e2e_support.rs:1-11`):

```rust
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::let_underscore_must_use,
    clippy::unused_result_ok,
    clippy::map_err_ignore,
    clippy::unreachable,
    reason = "test/fixture harness code, not a production path"
)]
//! Shared seed/assert helpers for the end-cap-seam fixture tests
//! (`iss-end-cap-ignores-mv-floor`) and for `tests/mv_floor.rs`.
//!
//! Every helper here was either lifted from `tests/mv_floor.rs` (its previous, and
//! only, home) or is a seed shape more than one of the new tests needs. Per-test
//! topologies stay LOCAL to their test file — this library carries what is genuinely
//! shared, not everything.
```

Everything each helper needs is public (`land`, `land_cdc`, `flush_table`, `inline_append`, `current_inline_version`, `write_inline_delta`, `live_table_id`, `ensure_table`, `next_snapshot`, `mv_floor`, `local_sql_catalog`).

BUCK — a `rust_library`, deps as the `mv-floor` target's (`BUCK:596-615`) plus `//third-party:iceberg` is **not** needed here:

```python
rust_library(
    name = "end-cap-seed",
    crate = "end_cap_seed",
    srcs = ["tests/end_cap_seed.rs"],
    crate_root = "tests/end_cap_seed.rs",
    deps = [
        "//src/testing:seed",
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 1: Write the library**

Lift the helpers named above out of `tests/mv_floor.rs` into `tests/end_cap_seed.rs` and add the CDC shapes. **Do not modify `tests/mv_floor.rs` yet** — it keeps its own copies until Task 7, so the suite stays green throughout (`mv-floor` must not break in the middle of the plan). The temporary duplication is deliberate and is deleted in Task 7.

Add the `rust_library` target above to `src/control-plane/postgres/BUCK`.

- [ ] **Step 2: Verify it builds**

Run: `buck2 build -v0 --console none //src/control-plane/postgres:end-cap-seed`
Expected: exit 0, silent.

Run: `buck2 build --console none --show-simple-output '//src/control-plane/postgres:end-cap-seed[clippy.txt]'`
Then `cat` the printed path: it must be **empty**. A `rust_library` gets no test-lint exemption, so an unused import or a stray `expect` without the crate-level allow fails the gate here and not later.

- [ ] **Step 3: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres/tests/end_cap_seed.rs src/control-plane/postgres/BUCK
git commit -m "test(postgres): shared seed library for the end-cap-seam fixtures"
```

---

### Task 2a: `EndCapIntent` + `guard_end_cap` — the seam, in one file

The question an end-cap must answer is **not** "may I end-cap this row?" but "**may I remove these offsets from the live set?**". Flush and compaction end-cap offsets *above* the floor and re-project those same rows at the same `(bucket, offset)` — the rows never leave the live set, so no MV can miss them. A blanket "refuse any end-cap at or above the floor" breaks both. Intent must come from the **call site**.

This task adds the seam to `mv_floor.rs` and nothing else — the SQL bounds are the whole risk here, and they get reviewed in isolation. Task 2b threads it through the primitives.

**Files:**
- Modify: `src/control-plane/postgres/src/mv_floor.rs` (append)
- Create: `src/control-plane/postgres/tests/end_cap_intent.rs`, and its `end-cap-intent` target in `src/control-plane/postgres/BUCK`

**Interfaces:**
- Consumes: `mv_floor(&mut PgConnection, …)` (Task 1); `iceberg_inline::inline_table_exists` (`pub(crate)`, `:63`) and `inline_table_name` (`pub`, `:41`) — both reachable from `mv_floor` (same crate).
- Produces:
  - `pub const MV_FLOOR_REFUSAL_PREFIX: &str = "mv-floor refuses end-cap:"` — Task 6 matches on it.
  - `pub enum EndCapIntent<'a> { Reframing, Removing, Destroying { reason: &'a str } }`, `Default = Removing` (the fail-safe: a caller who does not think about it gets the guard).
  - `pub async fn removal_blocked(conn: &mut PgConnection, table: &TableRef, tid: i64) -> Result<Option<MvFloor>>` — `Some(floor)` iff a `Removing` end-cap would take offsets at/above some MV's read position. Task 6 calls it directly.
  - `pub async fn guard_end_cap(conn: &mut PgConnection, table: &TableRef, tid: i64, intent: &EndCapIntent<'_>) -> Result<()>`.

- [ ] **Step 1: Write the failing tests**

Create `src/control-plane/postgres/tests/end_cap_intent.rs`. This task's tests drive `removal_blocked`/`guard_end_cap` **directly** (the primitives do not take an intent until Task 2b).

```rust
//! Fixture tests for the end-cap seam (`iss-end-cap-ignores-mv-floor`): a `Removing`
//! end-cap of offsets an MV has not read is blocked; a `Reframing` one (flush,
//! compaction — same rows re-projected at the same offsets) is not; a `Destroying`
//! one (catalog drop) bypasses the floor on purpose.
//!
//! Seeds come from the shared `end_cap_seed` library — a declared LOG stream table of
//! one bucket with six events landed to Parquet FILES, one registered micro-batch MV,
//! watermark advanced to 3, so offsets 3..6 are unread.

use control_plane_core::{ControlPlaneError, mv_key};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::mv_floor::{EndCapIntent, guard_end_cap, removal_blocked};
use end_cap_seed::{advance, seed_source, tref};

/// The core block: files carrying offsets the MV has not read block a `Removing`
/// end-cap, and `guard_end_cap` turns that into a `Validation` naming the laggard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removal_is_blocked_above_the_floor() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // inline = false => straight to Parquet FILES (the file tier of the guard).
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        false,
        &[("mv_a", "out_a")],
    )
    .await;
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 3).await;

    let mut conn = s.pool.acquire().await.expect("conn");
    assert!(
        removal_blocked(&mut conn, &s.src, s.tid)
            .await
            .expect("removal_blocked")
            .is_some(),
        "offsets 3..6 are unread — a removal must be blocked"
    );

    let err = guard_end_cap(&mut conn, &s.src, s.tid, &EndCapIntent::Removing)
        .await
        .expect_err("a Removing end-cap above the floor must be refused");
    match err {
        ControlPlaneError::Validation(m) => {
            assert!(
                m.starts_with("mv-floor refuses end-cap:"),
                "unexpected message: {m}"
            );
            assert!(
                m.contains("out_a"),
                "the message must name the laggard MV: {m}"
            );
        }
        other => panic!("expected Validation, got {other:?}"),
    }
}

/// The over-refusal guard: the SAME state, declared `Reframing`, is NOT refused. This is
/// what proves the seam distinguishes reframing from removal — the test that catches the
/// naive "refuse any end-cap above the floor" implementation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reframing_is_never_refused() {
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
        false,
        &[("mv_a", "out_a")],
    )
    .await;
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 3).await;

    let mut conn = s.pool.acquire().await.expect("conn");
    guard_end_cap(&mut conn, &s.src, s.tid, &EndCapIntent::Reframing)
        .await
        .expect("a reframing end-cap must never consult the floor");
}

/// The drop bypass: `Destroying` proceeds on purpose (the operator dropped the source, so
/// its MVs are dead by definition; wedging drop-GC on a dead MV forever is worse).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn destroying_bypasses_the_floor() {
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
        false,
        &[("mv_a", "out_a")],
    )
    .await;
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 3).await;

    let mut conn = s.pool.acquire().await.expect("conn");
    guard_end_cap(
        &mut conn,
        &s.src,
        s.tid,
        &EndCapIntent::Destroying {
            reason: "test: catalog drop",
        },
    )
    .await
    .expect("a destroying end-cap bypasses the floor on purpose");
}

/// The fast path: a table no MV sources has no floor, so every intent is a no-op and a
/// `Removing` end-cap proceeds byte-identically to the pre-seam behavior. This is what
/// makes the seam free for every table in the tree that is not an MV source.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_table_no_mv_reads_is_never_refused() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // No MVs registered => no reader => no floor.
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        false,
        &[],
    )
    .await;

    let mut conn = s.pool.acquire().await.expect("conn");
    assert!(
        removal_blocked(&mut conn, &s.src, s.tid)
            .await
            .expect("removal_blocked")
            .is_none(),
        "no reader => no floor => nothing blocks"
    );
    guard_end_cap(&mut conn, &s.src, s.tid, &EndCapIntent::Removing)
        .await
        .expect("a table no MV reads is never refused");
}

/// A caught-up MV releases the tail: watermark at 6 of 6 blocks nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_caught_up_mv_does_not_block_removal() {
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
        false,
        &[("mv_a", "out_a")],
    )
    .await;
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 6).await;

    let mut conn = s.pool.acquire().await.expect("conn");
    assert!(
        removal_blocked(&mut conn, &s.src, s.tid)
            .await
            .expect("removal_blocked")
            .is_none(),
        "a fully-consumed source blocks nothing"
    );
}
```

Add to `src/control-plane/postgres/BUCK`. It depends on the **`:end-cap-seed`** library from Task 2s — it needs no arrow/serde/time/uuid deps of its own, because every seed shape lives behind that library:

```python
loom_fixture_test(
    name = "end-cap-intent",
    crate = "end_cap_intent",
    srcs = ["tests/end_cap_intent.rs"],
    crate_root = "tests/end_cap_intent.rs",
    deps = [
        ":postgres",
        ":end-cap-seed",
        "//src/control-plane/core:core",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:end-cap-intent`
Expected: **compile failure** — `EndCapIntent`, `guard_end_cap`, `removal_blocked` do not exist.

- [ ] **Step 3: Implement the seam**

Append to `src/control-plane/postgres/src/mv_floor.rs`. The two SQL shapes deliberately mirror GC's (`victim_data_files` at `iceberg_gc.rs:379-389`, `delete_end_capped_inline_rows` at `:452-490`) — the same conservative bounds, evaluated in the opposite direction. Do **not** invent a second pair.

Extend the EXISTING import at `mv_floor.rs:35` — `use control_plane_core::{Result, TableRef};` becomes `use control_plane_core::{ControlPlaneError, Result, TableRef};`. Do NOT add a second `use` line (that is `E0252`).

```rust
/// The stable prefix of the seam's refusal message. `consolidate_table` matches on it
/// to turn a refusal into a clean skip-and-re-arm rather than a queue-poisoning error
/// (the gRPC status code does not survive the wire — `worker/src/stream_mv.rs`'s
/// `classify_*` uses the same idiom).
pub const MV_FLOOR_REFUSAL_PREFIX: &str = "mv-floor refuses end-cap:";

/// Why a caller is end-capping. Required by every end-cap primitive, so a future
/// retention path cannot end-cap an MV's unread offsets by simply not thinking about
/// it — the type system makes it decide. Intent CANNOT be inferred from the SQL: flush
/// and compaction end-cap offsets ABOVE the floor and re-project those same rows at the
/// same `(bucket, offset)`, so a blanket "refuse any end-cap at or above the floor"
/// would break them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EndCapIntent<'a> {
    /// The offsets SURVIVE: the same rows are re-projected into the new live set at the
    /// SAME `(bucket, offset)`. Flush (inline rows → live Parquet) and plain-coalesce
    /// compaction (small files → one big file). No floor consult — no MV can miss a row
    /// that never left the live set.
    Reframing,
    /// The offsets LEAVE the live set. Must clear the MV floor. The DEFAULT, so a new
    /// caller that starts end-capping without thinking gets the guard.
    #[default]
    Removing,
    /// Deliberate destruction; the floor is bypassed ON PURPOSE and the reason logged.
    /// The catalog drop: the operator dropped the source, so its MVs are dead by
    /// definition, and wedging drop-GC on a dead MV forever is strictly worse than
    /// stranding it (which `stranded_mv_readers` warns about).
    Destroying { reason: &'a str },
}

/// `Some(floor)` iff REMOVING the live offsets of `tid` would take rows at or above some
/// MV's read position — i.e. iff a `Removing` end-cap must be refused. `None` means
/// nothing blocks: not a declared stream table, no MV reads it, or every live offset is
/// already below the floor.
///
/// Bounds are GC's, reused exactly:
/// - **Files** — `MvFloor::min_offset()` (the cross-bucket minimum; per-file column stats
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
    let below: String = {
        let clauses: Vec<String> = floor
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
    };
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
/// on, so the refusal is atomic with the write and cannot straddle a concurrent
/// `define_transform`.
///
/// `Reframing` short-circuits (no query at all). `Destroying` logs and proceeds.
pub async fn guard_end_cap(
    conn: &mut PgConnection,
    table: &TableRef,
    tid: i64,
    intent: &EndCapIntent<'_>,
) -> Result<()> {
    match intent {
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
```

- [ ] **Step 4: Regenerate the sqlx cache**

Run: `bash tools/sqlx-prepare.sh`
Expected: a new `src/control-plane/postgres/.sqlx/query-*.json` for the file-tier `query_scalar!`. Commit it. (This is required, not optional — `sqlx-cache-check` gates it.)

- [ ] **Step 5: Run to verify it passes**

Run: `buck2 test --console none //src/control-plane/postgres:end-cap-intent //src/control-plane/postgres:sqlx-cache-check`
Expected: `Pass 5. Fail 0` and the cache check green.

- [ ] **Step 6: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A src/control-plane/postgres
git commit -m "feat(mv-floor): add the EndCapIntent seam (guard_end_cap, removal_blocked)"
```

---

### Task 2b: Thread the intent through the five end-cap primitives

Structural adoption: every end-cap primitive gains `table: &TableRef` + `intent: &EndCapIntent<'_>` and calls `guard_end_cap` before it writes. **There are eleven production call sites**, all inventoried below — plus two test sites.

**⚠️ The dangerous one: flush has TWO end-capping commits, not one.** `iceberg_flush.rs:180` (non-CDC) *and* `iceberg_flush.rs:295` (the CDC base append inside `flush_locked_cdc`) both set `end_cap: Some(..)`. With `Default = Removing`, missing either one makes a floored table unable to flush — and **no existing test registers an MV against a CDC table**, so the whole non-regression sweep would stay green while shipping it. Step 4's test is what makes that visible.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_mirror.rs:354` (`end_cap_files_by_path`), `:392` (`end_cap_live_data_files`), `:415` (`mark_dropped`), `:459` (its internal call)
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs:76` (`end_cap_live_inline_rows`), `:106` (`end_cap_inline_rows_by_id`)
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs:758`, `:759`, `:764`, `:1275`, `:1277`, `:1278`; `:450` (`apply_commit_extras` caller)
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs:89`, `:152` (`write_mirror`), `:191`, `:197`, `:437` (`commit_mirror_in_tx`)
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs:813`
- Modify: `src/control-plane/postgres/src/iceberg_flush.rs:180` **and** `:295`
- Modify: `src/control-plane/postgres/tests/iceberg_overwrite.rs:331`, `src/control-plane/postgres/tests/dataset_view.rs:100`
- Modify: `src/control-plane/postgres/tests/end_cap_intent.rs` (add the primitive-level trio)

**Interfaces:**
- Consumes: `EndCapIntent`, `guard_end_cap` (Task 2a).
- Produces: `end_cap_files_by_path(conn, table, table_id, paths, at, intent)`; `end_cap_live_data_files(conn, table, table_id, at, intent)`; `mark_dropped(conn, ns, name, at, intent)`; `end_cap_live_inline_rows(conn, table, table_id, at, intent)`; `end_cap_inline_rows_by_id(conn, table, table_id, row_ids, at, intent)`; `CommitExtras { …, intent: EndCapIntent<'a> }`.

- [ ] **Step 1: Add the primitive-level tests to `end_cap_intent.rs`**

These prove the guard fires *inside the real primitive*, on a real transaction.

**Imports.** Add `use control_plane_postgres::iceberg_mirror::{end_cap_live_data_files, next_snapshot};` and extend the `end_cap_seed` import to `use end_cap_seed::{advance, live_file_count, seed_source, tref};`. **Do not** declare a local `live_file_count` — it lives in the seed library (Task 2s). **Do not** import `SnapshotId`: nothing in this file names it as a type (`let at = next_snapshot(..)` infers it), and an unused import fails the clippy gate.

```rust
/// The primitive refuses, and the live set is untouched (the tx rolls back).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removing_end_cap_primitive_is_refused() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        &[("mv_a", "out_a")],
        3,
    )
    .await;
    let before = live_file_count(&s.pool, s.tid).await;
    assert!(before > 0, "seed must leave live files");

    let mut tx = s.pool.begin().await.expect("tx");
    let at = next_snapshot(&mut tx, None).await.expect("snapshot");
    end_cap_live_data_files(&mut tx, &s.src, s.tid, at, &EndCapIntent::Removing)
        .await
        .expect_err("the primitive itself must refuse");
    drop(tx); // rolls back

    assert_eq!(
        live_file_count(&s.pool, s.tid).await,
        before,
        "a refused end-cap must leave the live set untouched"
    );
}

/// The same primitive call, declared `Reframing`, commits — the over-refusal guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reframing_end_cap_primitive_commits() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        &[("mv_a", "out_a")],
        3,
    )
    .await;

    let mut tx = s.pool.begin().await.expect("tx");
    let at = next_snapshot(&mut tx, None).await.expect("snapshot");
    end_cap_live_data_files(&mut tx, &s.src, s.tid, at, &EndCapIntent::Reframing)
        .await
        .expect("a reframing end-cap must commit");
    tx.commit().await.expect("commit");

    assert_eq!(live_file_count(&s.pool, s.tid).await, 0);
}
```

- [ ] **Step 2: Change the five signatures**

In `iceberg_mirror.rs` — each gains `table: &TableRef` + `intent`, and calls the guard first:

```rust
pub async fn end_cap_files_by_path(
    conn: &mut PgConnection,
    table: &TableRef,
    table_id: i64,
    paths: &[String],
    at: SnapshotId,
    intent: &EndCapIntent<'_>,
) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    crate::mv_floor::guard_end_cap(&mut *conn, table, table_id, intent).await?;
    // ... existing body unchanged ...
}

pub async fn end_cap_live_data_files(
    conn: &mut PgConnection,
    table: &TableRef,
    table_id: i64,
    at: SnapshotId,
    intent: &EndCapIntent<'_>,
) -> Result<()> {
    crate::mv_floor::guard_end_cap(&mut *conn, table, table_id, intent).await?;
    // ... existing body unchanged ...
}

pub async fn mark_dropped(
    conn: &mut PgConnection,
    ns: &str,
    name: &str,
    at: SnapshotId,
    intent: &EndCapIntent<'_>,
) -> Result<()> {
    // ... existing view-dependent refusal unchanged ...
    // The internal end-cap (:459) forwards the caller's table + intent:
    let table = TableRef { schema: ns.to_owned(), name: name.to_owned() };
    end_cap_live_data_files(conn, &table, tid, at, intent).await?;
    // ... rest unchanged ...
}
```

In `iceberg_inline.rs` — both are `pub(crate)`; make `end_cap_live_data_files`'s siblings **`pub`** so the tests can drive them:

```rust
pub async fn end_cap_live_inline_rows(
    conn: &mut PgConnection,
    table: &TableRef,
    table_id: i64,
    at: control_plane_core::SnapshotId,
    intent: &EndCapIntent<'_>,
) -> control_plane_core::Result<()> {
    if !inline_table_exists(conn, table_id).await? {
        return Ok(());
    }
    crate::mv_floor::guard_end_cap(&mut *conn, table, table_id, intent).await?;
    // ... existing body unchanged ...
}

pub async fn end_cap_inline_rows_by_id(
    conn: &mut PgConnection,
    table: &TableRef,
    table_id: i64,
    row_ids: &[i64],
    at: SnapshotId,
    intent: &EndCapIntent<'_>,
) -> Result<()> {
    crate::mv_floor::guard_end_cap(&mut *conn, table, table_id, intent).await?;
    // ... existing body unchanged ...
}
```

- [ ] **Step 3: Thread `intent` through the commit path**

`write_mirror` does **not** take `CommitExtras` — `commit_mirror_in_tx` flattens the extras into bools (`commit_mirror.rs:422-434`). So `CommitExtras` gains the field, and `write_mirror` gains a parameter that `commit_mirror_in_tx` passes from it.

In `commit_mirror.rs`, add to `CommitExtras`:

```rust
    /// Why this commit end-caps (its `overwrite` file/inline caps and its `end_cap`
    /// targeted cap). Defaults to `Removing` — the fail-safe. **Flush overrides it to
    /// `Reframing`**: it end-caps inline rows and re-projects those SAME rows into live
    /// Parquet at the SAME `(bucket, offset)`, so no MV can miss one.
    pub intent: EndCapIntent<'a>,
```

`write_mirror` (`:152`) gains `table: &TableRef` and `intent: &EndCapIntent<'_>` (build the `TableRef` from the `ns`/`name` it already computes at `:168-169`, or pass it in), and its overwrite branch (`:190-198`) forwards both:

```rust
        if overwrite {
            end_cap_live_data_files(conn, table, tid, at, intent).await?;
            if blanket_inline_cap {
                crate::iceberg_inline::end_cap_live_inline_rows(conn, table, tid, at, intent).await?;
            }
        }
```

`commit_mirror_in_tx` (`:437`) passes `extras.intent` to `write_mirror` and the table to `apply_commit_extras`.

`apply_commit_extras` (`:81`) gains `table: &TableRef` and forwards `extras.intent` to `end_cap_inline_rows_by_id` (`:89`). Its **two** callers are `commit_mirror_in_tx` (`commit_mirror.rs:437`) and `iceberg_landing.rs:450` — both already have the table identity in hand.

It already carries `#[expect(clippy::too_many_arguments, …)]`; extend the reason to name the intent.

- [ ] **Step 4: Declare flush `Reframing` — BOTH commits — and pin it with a test**

`iceberg_flush.rs:180` (non-CDC) and `iceberg_flush.rs:295` (the CDC **base** append inside `flush_locked_cdc`) each add `intent: EndCapIntent::Reframing` to their `CommitExtras`. The changelog append at `:313` carries no `end_cap` and keeps the default.

On the CDC base: it end-caps *all* live inline rows but re-projects only the `+I/+U/-D` subset (`filter_out_minus_u`), so a `-U` row does leave the *base*'s live set. It is still `Reframing`, and the argument must go in the code comment: the durable **changelog retains every row including `-U`**, and `mv_delta_scan` reads **log sources only** (`mv_delta.rs:72-82`) — it never reads a CDC base — so no MV's delta can miss a row this drops. Write that comment; do not leave it implicit.

Add to `tests/end_cap_intent.rs` the regression test that would have caught this. It is constructible **without** a registered MV (Task 5 refuses that), because `advance_mv_watermark` is a bare CAS on `stream.mv_watermark` and does not check that a def exists — exactly the ghost-row state `iss-mv-watermark-ghost-rows` describes.

**`seed_floored_cdc` lives in the `end_cap_seed` library** (Task 2s), not in this file — Task 6 Step 3b reuses it. Its shape is lifted from `src/services/worker/tests/stream_consolidate_job.rs:117-165`: declare CDC **before any write** (the physical schema must carry framing from the start), then `inline_append` + `write_inline_delta`, then plant the watermark row. For reference, this is the body Task 2s must have written into the library:

```rust
// Additional imports for this test (the `iceberg_mirror` line is already declared in
// Step 1 — do not repeat it, and do NOT import SnapshotId):
//   use control_plane_core::{MergeEngine, StreamTables};
//   use control_plane_postgres::iceberg_flush::flush_table;
//   use control_plane_postgres::iceberg_inline::{current_inline_version, inline_append,
//                                                write_inline_delta};
//   use control_plane_postgres::mv_floor::mv_floor;

fn cdc_specs() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "id".into(),
            ty: "long".into(),
            nullable: false,
        },
        ColumnSpec {
            name: "val".into(),
            ty: "long".into(),
            nullable: false,
        },
    ]
}

fn row_batch(id: i64, val: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Int64Array::from(vec![val])),
        ],
    )
    .expect("row batch")
}

fn id_only_batch(id: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![id]))]).expect("id batch")
}

/// A declared CDC table with an append + an update, carrying a watermark row against it —
/// so `mv_floor` is `Some` (a ghost key: `advance_mv_watermark` is a bare CAS that never
/// checks a def exists). Declared CDC BEFORE any write, because the physical schema must
/// carry framing from the start. Returns the pieces both floored-CDC tests need.
///
/// **Extracted as a helper on purpose:** Task 6 Step 3b reuses it verbatim.
struct FlooredCdc {
    pool: sqlx::PgPool,
    catalog: SqlCatalog,
    table: TableRef,
    tid: i64,
    _wh: tempfile::TempDir,
}

async fn seed_floored_cdc(fx: &'static PgFixture, cp: &PgControlPlane, db: &str) -> FlooredCdc {
    let pool = fx.pool_for(db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    let table = tref("main", "widget");
    let cols = cdc_specs();

    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, &table.schema, &table.name, at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 1, "id", MergeEngine::LastRow)
        .await
        .expect("declare_cdc");

    inline_append(&pool, &table, &cols, &row_batch(1, 100), lineage(&table), None, None)
        .await
        .expect("seed append");
    let v0 = current_inline_version(&pool, &table, &[cols[0].clone()], "id", &id_only_batch(1))
        .await
        .expect("version");
    write_inline_delta(
        &pool,
        &table,
        &cols,
        "id",
        false,
        &row_batch(1, 200),
        Some((&cols, &row_batch(1, 100))),
        lineage(&table),
        v0,
        None,
        &[],
    )
    .await
    .expect("cdc update delta");

    // Plant the floor: a watermark row against this CDC source.
    cp.advance_mv_watermark(
        &mv_key(&tref("main", "out_a")),
        tid,
        &[WatermarkAdvance {
            bucket: 0,
            from: 0,
            to: 1,
        }],
    )
    .await
    .expect("advance watermark");
    let mut conn = pool.acquire().await.expect("conn");
    assert!(
        mv_floor(&mut conn, &table, tid)
            .await
            .expect("mv_floor")
            .is_some(),
        "sanity: the watermark row must produce a floor, or these tests prove nothing"
    );
    drop(conn);

    FlooredCdc {
        pool,
        catalog,
        table,
        tid,
        _wh: wh,
    }
}

/// A floored CDC table MUST still flush, because a flush is REFRAMING.
///
/// This is the test that catches the plan's own worst near-miss: flush has TWO commits
/// that end-cap inline rows (`iceberg_flush.rs:180` non-CDC and `:295` the CDC base), and
/// `EndCapIntent`'s default is `Removing`. Miss either and a floored table can never
/// flush again — and NO other test in the tree puts a floor on a CDC table, so the whole
/// suite would stay green while shipping it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_flush_with_a_floored_source_still_succeeds() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let s = seed_floored_cdc(fx, &cp, &db).await;

    flush_table(&s.catalog, &s.pool, &s.table, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("a CDC flush is REFRAMING and must not be refused by the MV floor")
        .expect("flush produced a snapshot");
}
```

This helper also needs `use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;` in `end_cap_intent.rs`.


- [ ] **Step 5: Update every call site with its intent**

The eleven production sites and their intents:

| Call site | Intent | Why |
| --- | --- | --- |
| `iceberg_landing.rs:758`, `:759` (`register_files`, `WriteMode::Overwrite`) | `Removing` | A replace drops the old rows. (Both callers already `pg_refuse_stream_target`, so the floor is always `None` here — the intent is the honest declaration, not a behavior change.) |
| `iceberg_landing.rs:764` (`register_files`, `WriteMode::Compact`) | `Reframing` | Compaction rewrites the same rows coalesced at the same offsets. |
| `iceberg_landing.rs:1275`, `:1277`, `:1278` (`overwrite_truncate`) | `Removing` | A truncate removes everything. |
| `iceberg_mirror.rs:459` (inside `mark_dropped`) | forwarded from the caller | — |
| `iceberg_sql_catalog/catalog.rs:813` (`mark_dropped`) | `Destroying { reason: "catalog drop" }` | Deliberate destruction. |
| `commit_mirror.rs:89` (`apply_commit_extras`), `:191`, `:197` (`write_mirror`) | `extras.intent` | Flush ⇒ `Reframing`; a consuming overwrite ⇒ `Removing`. |

Test sites: `tests/iceberg_overwrite.rs:331` and `tests/dataset_view.rs:100` — signature updates only (pass the table and `&EndCapIntent::Removing` / `&EndCapIntent::Destroying { reason: "test" }` respectively).

- [ ] **Step 6: Run the tests**

Run: `buck2 test --console none //src/control-plane/postgres/... -j 8`
Expected: all pass, including the new `cdc_flush_with_a_floored_source_still_succeeds`. Then the crates that consume these primitives:

Run: `buck2 test --console none //src/services/... -j 8`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A src/control-plane/postgres
git commit -m "feat(mv-floor): every end-cap primitive declares its intent"
```

---

### Task 3: Refuse overwriting a declared stream table; give the CDC fold a framed entrypoint

`overwrite_with_cap` explicitly supports declared stream tables (it resolves `include_framing` from `pg_stream_bucket_count`) yet carries no `pg_refuse_stream_target`. Its empty-body branch, `overwrite_truncate`, end-caps every live file **and** every live inline row with **no stream check at all**. But no production caller *wants* that — the only caller supplying framed batches is the CDC consolidate fold. Refuse it, and give the fold its own door.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs:1145-1296`
- Modify: `src/services/engine-serving/src/consolidate.rs:309`, `:323`
- Modify: `src/services/engine-serving/src/action_writer.rs:175` (the `Validation` arm)
- Modify: `src/control-plane/postgres/tests/stream_overwrite_framing.rs:131`
- Create: `src/control-plane/postgres/tests/stream_write_refuse.rs` (+ BUCK target; Task 4 appends to it)

**Interfaces:**
- Consumes: `pg_refuse_stream_target(conn, table)` (`stream.rs:373-398`) — `pub`, stable prefix `stream-table target refused:`.
- Produces: `pub async fn overwrite_stream_base(pool, catalog, table, columns, batches, lineage, consumed: Option<InlineEndCap<'_>>) -> Result<SnapshotId>`.

- [ ] **Step 1: Write the failing tests**

Create `src/control-plane/postgres/tests/stream_write_refuse.rs`:

```rust
//! The write refusals that dissolve `iss-end-cap-ignores-mv-floor`'s lossy end-cap paths
//! at the source rather than guarding them:
//!
//! * **E6** — `overwrite_parquet_snapshot` (and its empty-batch `overwrite_truncate`
//!   branch) over a DECLARED STREAM table. The only production caller that legitimately
//!   overwrites one is the CDC consolidate fold, now on `overwrite_stream_base`.
//! * **E7** — a typed UPDATE/DELETE (`write_inline_delta`) against a declared LOG table,
//!   which would set `has_shadow` and hand the table to the COW identity fold. (Task 4.)

use control_plane_core::ControlPlaneError;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::overwrite_parquet_snapshot;
use end_cap_seed::{columns, batch, lineage, live_file_count, seed_source};

/// E6, the non-empty branch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_of_a_declared_stream_table_is_refused() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(fx, &cp, &db, &wh.path().display().to_string(), 6, Some(1), false, &[]).await;
    let before = live_file_count(&s.pool, s.tid).await;

    let (_, batches) = batch(2);
    let err = overwrite_parquet_snapshot(
        &s.pool,
        &s.catalog,
        &s.src,
        &columns(),
        batches,
        Some(&lineage(&s.src)),
        &[],
    )
    .await
    .expect_err("overwrite of a declared stream table must be refused");

    match err {
        ControlPlaneError::Validation(m) => assert!(
            m.starts_with("stream-table target refused:"),
            "unexpected message: {m}"
        ),
        other => panic!("expected Validation, got {other:?}"),
    }
    assert_eq!(
        live_file_count(&s.pool, s.tid).await,
        before,
        "a refused overwrite must leave the live set untouched"
    );
}

/// E6, the WORST case: the empty-batch `overwrite_truncate` branch, which today
/// end-caps every live data file AND every live inline row with no stream check of any
/// kind — a delete-all that silently destroys the whole offset range.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncate_of_a_declared_stream_table_is_refused() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(fx, &cp, &db, &wh.path().display().to_string(), 6, Some(1), false, &[]).await;
    let before = live_file_count(&s.pool, s.tid).await;
    assert!(before > 0, "seed must leave live files");

    let err = overwrite_parquet_snapshot(
        &s.pool,
        &s.catalog,
        &s.src,
        &columns(),
        vec![], // the truncate branch
        Some(&lineage(&s.src)),
        &[],
    )
    .await
    .expect_err("truncate of a declared stream table must be refused");

    match err {
        ControlPlaneError::Validation(m) => assert!(
            m.starts_with("stream-table target refused:"),
            "unexpected message: {m}"
        ),
        other => panic!("expected Validation, got {other:?}"),
    }
    assert_eq!(
        live_file_count(&s.pool, s.tid).await,
        before,
        "a refused truncate must leave every live file in place"
    );
}

/// Non-regression: a PLAIN (undeclared) table overwrites exactly as before — the check
/// that the refusal is scoped and did not just break every overwrite in the tree.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_of_a_plain_table_still_succeeds() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // buckets = None => a plain, undeclared batch table.
    let s = seed_source(fx, &cp, &db, &wh.path().display().to_string(), 6, None, false, &[]).await;

    let (_, batches) = batch(2);
    overwrite_parquet_snapshot(
        &s.pool,
        &s.catalog,
        &s.src,
        &columns(),
        batches,
        Some(&lineage(&s.src)),
        &[],
    )
    .await
    .expect("a plain table overwrites unchanged");
}
```

BUCK: a **`loom_fixture_test`** (it boots Postgres — a bare `rust_test` runs without the fixture env and fails), named `stream-write-refuse` / crate `stream_write_refuse`, with the **same dep list as `end-cap-intent`** (`:postgres`, `:end-cap-seed`, `//src/control-plane/core:core`, `//third-party:sqlx`, `//third-party:tempfile`, `//third-party:tokio`) plus `//third-party:arrow-array` and `//third-party:arrow-schema` (Task 4's tombstone batch is built inline in this file).

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:stream-write-refuse`
Expected: the two refusal tests FAIL on `expect_err` (the overwrite currently **succeeds** — that is the bug). `overwrite_of_a_plain_table_still_succeeds` already passes.

- [ ] **Step 3: Add the refusal and the framed entrypoint**

In `iceberg_landing.rs`, give `overwrite_with_cap` a `framed: bool` parameter. With the refusal in place, `include_framing` collapses to `framed` — a non-framed overwrite can no longer target a declared stream table — so **delete the `include_framing` block (`:1221-1229`)**.

```rust
/// The CDC consolidate fold's private door: the ONLY legitimate overwrite of a declared
/// stream table. `batches` MUST carry the framing columns (`loom_change_kind` /
/// `loom_bucket` / `loom_offset`).
///
/// This bypasses the `pg_refuse_stream_target` check the two public entrypoints carry, so
/// it is the one path that can remove a stream table's offsets — and therefore the one
/// overwrite whose end-cap can be blocked by the MV floor (`EndCapIntent::Removing` →
/// `guard_end_cap`). `consolidate_table` pre-checks that with `mv_floor::removal_blocked`
/// and skips cleanly rather than erroring (Task 6).
pub async fn overwrite_stream_base(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: Option<&LineageEvent>,
    consumed: Option<InlineEndCap<'_>>,
) -> Result<SnapshotId> {
    overwrite_with_cap(
        pool, catalog, table, columns, batches, lineage, &[], consumed, true,
    )
    .await
}
```

Both public entrypoints pass `framed: false`. In `overwrite_with_cap`, refuse **before** the empty-batch early-return, and pass `framed` down:

```rust
    // Refuse a declared stream/CDC target. The framing-preserving overwrite exists for
    // exactly ONE caller — the CDC consolidate fold, via `overwrite_stream_base`. Every
    // other caller supplies user-columns-only batches; letting one land on a framed table
    // either destroys the offset range (the truncate branch) or panics in
    // `coerce_batch_to_ice` (the batch is three columns short of the framed schema).
    //
    // Pre-flight, on its own connection: the non-empty branch's commit tx is opened deep
    // inside `append_parquet_snapshot`. A stream declaration is never removed, so the only
    // window is "declared WHILE an overwrite is in flight". The truncate branch re-checks
    // INSIDE its own tx, atomically.
    if !framed {
        let mut conn = pool.acquire().await.map_err(backend)?;
        crate::stream::pg_refuse_stream_target(&mut conn, table).await?;
    }
    let rebuild_jobs = crate::vector_index::rebuild_jobs_for(pool, table).await?;
    let mut all_jobs = rebuild_jobs;
    all_jobs.extend_from_slice(jobs);
    if batches.iter().all(|b| b.num_rows() == 0) {
        return overwrite_truncate(pool, table, lineage, &all_jobs, consumed, framed).await;
    }
    append_parquet_snapshot(
        pool,
        catalog,
        table,
        columns,
        batches,
        CommitExtras {
            lineage,
            overwrite: true,
            end_cap: consumed,
            jobs: &all_jobs,
            data_trigger_tables: std::slice::from_ref(table),
            // A stream base's fold REMOVES offsets from the live set; a plain overwrite
            // removes rows from a table no MV can read. Either way: Removing.
            intent: EndCapIntent::Removing,
            ..CommitExtras::default()
        },
        framed, // was `include_framing`
    )
    .await
```

`overwrite_truncate` gains `framed: bool`, refuses **in-tx**, and passes the intent to the primitives (Task 2b changed their signatures):

```rust
    let mut tx = pool.begin().await.map_err(backend)?;
    let conn = &mut *tx;
    // In-tx, so the refusal is atomic with the truncate it prevents.
    if !framed {
        crate::stream::pg_refuse_stream_target(conn, table).await?;
    }
    let at = next_snapshot(conn, None).await?;
    let tid = ensure_table(conn, &table.schema, &table.name, at).await?;
    end_cap_live_data_files(conn, table, tid, at, &EndCapIntent::Removing).await?;
    match &consumed {
        Some(cap) => {
            end_cap_inline_rows_by_id(conn, table, cap.table_id, cap.row_ids, at, &EndCapIntent::Removing).await?;
        }
        None => end_cap_live_inline_rows(conn, table, tid, at, &EndCapIntent::Removing).await?,
    }
    // ... lineage / jobs / triggers / commit unchanged ...
```

Extend `overwrite_with_cap`'s existing `#[expect(clippy::too_many_arguments, …)]` reason to name `framed`.

- [ ] **Step 4: Map `Validation` to 4xx**

`IcebergActionWriter::overwrite_table` (`action_writer.rs:175`) currently maps **everything** to `EngineServingError::Engine` → HTTP 500. Copy `write_steps`' arm (`:141-144`):

```rust
        .await
        .map_err(|e| match e {
            ControlPlaneError::Validation(m) => EngineServingError::Validation(m),
            other => EngineServingError::Engine(other.to_string()),
        })
```

- [ ] **Step 5: Point the CDC fold at the framed entrypoint**

In `consolidate.rs`, the CDC arm's two overwrite calls (`:309` consuming, `:323` plain) collapse into one — `overwrite_stream_base` takes `Option<InlineEndCap>`:

```rust
    let snap = overwrite_stream_base(
        pool,
        catalog,
        table,
        &user_cols,
        folded,
        Some(&lineage),
        inline.as_ref().map(|(_, row_ids, _)| InlineEndCap {
            table_id: tid,
            row_ids,
        }),
    )
    .await
    .map_err(to_serving)?;
```

Update the import (drop `overwrite_parquet_snapshot`, add `overwrite_stream_base`). `overwrite_parquet_snapshot_consuming` **stays** — the COW arm at `:479` keeps using it, which is exactly what now makes the COW arm **refuse** a declared log table.

- [ ] **Step 6: Re-point the framing unit test**

`tests/stream_overwrite_framing.rs:131` calls the public `overwrite_parquet_snapshot` on a `declare_cdc` table, which now refuses. Re-point that one call at `overwrite_stream_base` (`consumed: None`). Every assertion stays; `batch_overwrite_has_no_framing` (`:194`) is untouched. Update the module doc: it now pins the framed entrypoint.

- [ ] **Step 7: Run the tests**

Run: `buck2 test --console none //src/control-plane/postgres:stream-write-refuse //src/control-plane/postgres:stream-overwrite-framing`
Expected: all pass.

Run: `buck2 test --console none //src/services/engine-serving/... //src/services/worker/... //src/services/query-api/... -j 8`
Expected: all pass — the CDC consolidate suites now go through `overwrite_stream_base` and must be byte-identical.

- [ ] **Step 8: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A src/control-plane/postgres src/services/engine-serving
git commit -m "fix(stream): refuse overwriting a declared stream table; CDC fold gets a framed door"
```

---

### Task 4: Refuse a typed UPDATE/DELETE against a declared log table

`write_inline_delta` loads `pg_stream_meta` at `:1280`, then — three statements later, at `:1421` — calls `set_has_shadow` **unconditionally**. On a declared **log** table that writes an unframed `+U`/`-D` inline row and hands the table to the COW identity fold, which folds an offset-framed event log by identity. There is no correct COW fold over a log table: log tables are the replayable event substrate (`docs/system-capabilities/stream.md:23-29` — "No identity requirement; appends only"), and `mv_delta_scan` reads log sources only.

This one refusal covers **all four** routes into the state (base-bound, view-bound, define-then-declare, declare-then-define), because it sits at the write, not at the authoring.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs:1280-1281`
- Modify: `src/services/engine-serving/src/action_writer.rs:258-261` (the `Validation` arm)
- Modify: `src/services/engine-serving/src/consolidate.rs:100` (the defensive arm)
- Modify: `src/control-plane/postgres/tests/stream_write_refuse.rs` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/stream_write_refuse.rs` (add `use control_plane_postgres::iceberg_inline::{has_shadow, write_inline_delta};` to the imports):

```rust
/// E7: a typed UPDATE/DELETE against a declared LOG table is refused. Without this,
/// `write_inline_delta` writes an unframed delta row, sets `has_shadow`, and the table
/// falls through to `consolidate_table`'s COW arm — which folds an offset-framed event
/// log by identity, end-caps every live file, and re-projects only the fold winners.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_mutation_of_a_declared_log_table_is_refused() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed(fx, &db, &wh.path().display().to_string(), Some(1)).await;

    // A one-cell `(id)` tombstone batch: what a typed DELETE lowers to.
    let id_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let id_batch = RecordBatch::try_new(id_schema, vec![Arc::new(Int64Array::from(vec![3i64]))])
        .expect("id batch");
    let id_specs = vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }];

    let err = write_inline_delta(
        &s.pool,
        &s.src,
        &id_specs,
        "id",
        true,            // tombstone
        &id_batch,
        None,            // no before-image (non-CDC)
        lineage(&s.src),
        0,               // CAS witness: no prior inline row for this id
        None,            // no consolidate threshold
        &[],
    )
    .await
    .expect_err("a typed DELETE against a declared log table must be refused");

    match err {
        ControlPlaneError::Validation(m) => assert!(
            m.starts_with("stream-table target refused:"),
            "unexpected message: {m}"
        ),
        other => panic!("expected Validation, got {other:?}"),
    }

    // The refusal is IN-TX: nothing was written and `has_shadow` was never set — which is
    // what keeps the table out of the COW arm AND keeps its byte-trigger flush alive
    // (`has_shadow` suppresses the non-CDC flush; a table wedged with the flag set would
    // grow its inline tier without bound).
    let mut conn = s.pool.acquire().await.expect("conn");
    assert!(
        !has_shadow(&mut conn, s.tid).await.expect("has_shadow"),
        "a refused mutation must not set has_shadow"
    );
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:stream-write-refuse`
Expected: FAIL — the mutation currently succeeds and sets `has_shadow`.

- [ ] **Step 3: Add the refusal**

In `iceberg_inline.rs`, immediately after the existing `let meta = …` / `let cdc = …` pair (`:1280-1281`):

```rust
    // A declared LOG table has no identity semantics — it is an offset-framed, replayable
    // event log (the substrate `mv_delta_scan` reads). A typed UPDATE/DELETE here would
    // write an UNFRAMED delta row (NULL loom_bucket/loom_offset), set `has_shadow`, and
    // hand the table to `consolidate_table`'s COW arm, which folds by identity: it
    // end-caps every live file and re-projects only the fold winners, destroying offsets
    // no MV has read. Refuse here — the ONE point where all four routes into that state
    // converge (base-bound, view-bound, define-then-declare, declare-then-define). In-tx,
    // so nothing is written and `has_shadow` is never set. Same `Validation` and stable
    // prefix the other write-path refusals use (`pg_refuse_stream_target`).
    if matches!(&meta, Some(m) if m.kind == control_plane_core::StreamKind::Log) {
        return Err(ControlPlaneError::Validation(format!(
            "stream-table target refused: {}.{} is a declared log stream table; \
             a typed UPDATE/DELETE would fold its offset-framed log by identity",
            table.schema, table.name
        )));
    }
```

- [ ] **Step 4: Map `Validation` to 4xx**

`IcebergActionWriter::write_delta` (`action_writer.rs:258-261`) maps only `Conflict`; add the `Validation` arm so the refusal is a 422, not a 500:

```rust
        .map_err(|e| match e {
            ControlPlaneError::Conflict(m) => EngineServingError::Conflict(m),
            ControlPlaneError::Validation(m) => EngineServingError::Validation(m),
            other => EngineServingError::Engine(other.to_string()),
        })
```

- [ ] **Step 5: Add the defensive `Log` arm to the consolidate dispatch**

Step 3 makes `has_shadow`-on-a-log-table unreachable **going forward**, but a table that already carries the flag would still fall through to the COW arm. In `consolidate.rs`, add an arm between the CDC arm and the `_` fallthrough. **Gate it on `has_shadow`** — otherwise it fires for every declared log table, and every ordinary log table would log a warning on each consolidate poll:

```rust
        // A declared LOG table must never reach the COW identity fold — folding an
        // offset-framed event log by identity end-caps every live file and re-projects
        // only the fold winners, destroying offsets an MV has not read. `write_inline_delta`
        // now refuses the typed mutation that sets `has_shadow`, so a shadow-bearing log
        // table can only be one mutated BEFORE that fix. Loud, and a no-op — never a fold.
        //
        // `has_shadow` is deliberately LEFT SET: the shadow tier really is unfolded, and
        // the non-CDC flush must stay suppressed rather than flush a tier we refuse to
        // fold. Such a table needs manual repair (see docs/system-capabilities/stream.md).
        // The trigger IS cleared, so the `enqueued` latch does not leak.
        Some(meta) if meta.kind == StreamKind::Log => {
            let mut conn = pool.acquire().await.map_err(to_serving)?;
            if !has_shadow(&mut conn, tid).await.map_err(to_serving)? {
                return Ok(0); // the common case: an ordinary log table, nothing to do
            }
            tracing::warn!(
                schema = %table.schema, name = %table.name, tid,
                "consolidate skipped: a declared log stream table carries has_shadow \
                 (a pre-existing typed mutation); it must not be folded by identity",
            );
            clear_consolidate_trigger(&mut conn, tid)
                .await
                .map_err(to_serving)?;
            Ok(0)
        }
```

- [ ] **Step 6: Add `tracing` to engine-serving — it is NOT currently a dependency**

The `Log` arm above (and Task 6's two `warn!`s) are the **first** `tracing` calls in this crate: `grep -rn tracing src/services/engine-serving/` returns nothing today, and `//third-party:tracing` is absent from the `rust_library` deps in `src/services/engine-serving/BUCK`. Without this step the build fails with `E0433: failed to resolve: use of undeclared crate or module 'tracing'`.

1. Add `"//third-party:tracing"` to the `engine-serving` `rust_library` `deps` in `src/services/engine-serving/BUCK`.
2. Add `tracing` to `src/services/engine-serving/Cargo.toml`, then `cargo generate-lockfile` and `./tools/buckify.sh` — the `reindeer-check` prek hook fails if `third-party/BUCK` drifts from the manifests.

(`//third-party:tracing` already exists — the postgres crate uses it. Activate the hermetic toolchain first: `eval "$(./tools/env.sh)"`.)

- [ ] **Step 7: Run the tests**

Run: `buck2 test --console none //src/control-plane/postgres:stream-write-refuse`
Expected: `Pass 4. Fail 0`.

Run: `buck2 test --console none //src/services/... -j 8`
Expected: all pass. No existing test writes an inline delta to a declared **log** table, so this breaks nothing. `multi_step_stream_refuse_http.rs` is refused earlier, by `pg_refuse_stream_target`, and is unaffected.

- [ ] **Step 8: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A src/control-plane/postgres src/services/engine-serving
git commit -m "fix(stream): refuse a typed UPDATE/DELETE against a declared log table"
```

---

### Task 5: Refuse a micro-batch MV over a CDC source

`define_transform` will register a `MicroBatch`/`MicroBatchJoin` whose **source** is a declared CDC table, but `mv_delta_scan` accepts **log sources only** (`mv_delta.rs:72-82` — "cdc sources are deferred"). So such an MV can never run, its watermark can never advance, and its floor pins the source at 0 **forever** — which after Task 6 means that table's consolidation declines on every attempt and never converges.

**The guard must be symmetric, and this is the whole point of the task.** Refusing only at registration is trivially defeated by ordering, through a path that must stay legitimate: an MV may be registered over a source that *does not exist yet* (it becomes a log stream on its first `?mode=stream` write — `tests/mv_floor.rs::registered_but_unrun_mv_floors_at_zero` depends on exactly that). Register the MV over a not-yet-existing source, then write that source with `?mode=cdc`, and you reach the identical wedge with the registration guard never firing. So:

- **Registration side** — `define_transform` refuses a `MicroBatch`/`MicroBatchJoin` whose source is **already** a declared CDC table. It already has the in-tx guard site: it resolves the def's **output** table and calls `pg_refuse_stream_target` (`transforms.rs:404-423`); the source check goes right next to it.
- **Declaration side** — `reconcile_stream_mode` (`stream.rs:56`, the production declaration path — it already takes `table: &TableRef`) refuses a **CDC** declaration on a table that a micro-batch MV **already sources**, before either `pg_declare_cdc` (`:210`) or `pg_declare_stream` (`:229`) runs.

The raw `StreamTables::declare_cdc` trait method (`stream.rs:489`) stays unguarded on purpose: it takes a bare `table_id` (no `TableRef` to resolve readers against) and is the test/admin escape hatch — the fixtures in this very plan use it to *construct* the floored-CDC state Task 6 must handle.

**Files:**
- Modify: `src/control-plane/postgres/src/transforms.rs:404-423` (registration side)
- Modify: `src/control-plane/postgres/src/stream.rs:56-210` (declaration side)
- Create: `src/control-plane/postgres/tests/mv_source_refuse.rs` (+ `loom_fixture_test` target `mv-source-refuse`)

**Interfaces:**
- Consumes: `pg_stream_meta` (`stream.rs:423`, `pub(crate)`, `PgExecutor`-generic); `pg_micro_batch_readers` (`transforms.rs:81`, `pub(crate)`, `PgExecutor`-generic); `live_table_id`; `StreamKind::Cdc`; `StreamDecl::Cdc`.
- Produces: no new public API — a `ControlPlaneError::Validation` from each side.

**Scope note:** postgres-only, exactly like `pg_refuse_stream_target` — both are catalog-aware checks and the `memory` backend has no stream registry. Do not add a testkit contract.

- [ ] **Step 1: Write the failing test**

Create `tests/mv_source_refuse.rs`. Seed a declared **CDC** table (`ensure_table` → `declare_cdc`), then try to register a micro-batch MV over it.

```rust
//! A micro-batch MV cannot source a declared CDC table: `mv_delta_scan` reads LOG
//! sources only (`engine-serving/src/mv_delta.rs` — "cdc sources are deferred"), so such
//! an MV could never run, never advance its watermark, and would pin its source's MV
//! floor at 0 forever — permanently declining that table's consolidate fold. Refuse the
//! registration instead. Lift this when `fut-mv-cdc-source` lands.

use control_plane_core::{
    ControlPlane, ControlPlaneError, MergeEngine, StreamTables, TableRef, TransformBody,
    TransformDef, TransformName,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

async fn define_mv(
    cp: &control_plane_postgres::PgControlPlane,
    source: &TableRef,
) -> Result<(), ControlPlaneError> {
    cp.transforms()
        .define_transform(TransformDef {
            name: TransformName("mv_a".into()),
            body: TransformBody::MicroBatch {
                source: source.clone(),
                output: tref("s", "out_a"),
                buckets: 1,
                sql: "select id from src".into(),
            },
            schedule: None,
            on_input_commit: false,
        })
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn micro_batch_over_a_cdc_source_is_refused() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let src = tref("s", "cdc_events");

    let mut tx = pool.begin().await.expect("tx");
    let at = next_snapshot(&mut tx, None).await.expect("snapshot");
    let tid = ensure_table(&mut tx, &src.schema, &src.name, at)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 1, "id", MergeEngine::LastRow)
        .await
        .expect("declare_cdc");

    let err = define_mv(&cp, &src)
        .await
        .expect_err("a micro-batch MV over a CDC source must be refused");
    match err {
        ControlPlaneError::Validation(m) => assert!(
            m.contains("cdc"),
            "the message must explain the CDC source: {m}"
        ),
        other => panic!("expected Validation, got {other:?}"),
    }
}

/// Non-regression: a LOG source registers fine — this is the supported configuration
/// and every MV test in the tree depends on it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn micro_batch_over_a_log_source_is_allowed() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let src = tref("s", "log_events");

    let mut tx = pool.begin().await.expect("tx");
    let at = next_snapshot(&mut tx, None).await.expect("snapshot");
    let tid = ensure_table(&mut tx, &src.schema, &src.name, at)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_stream(tid, 1).await.expect("declare_stream");

    define_mv(&cp, &src).await.expect("a log source is allowed");
}

/// Non-regression: an UNDECLARED source registers fine (it becomes a log stream on its
/// first `?mode=stream` write) — `registered_but_unrun_mv_floors_at_zero` in
/// `tests/mv_floor.rs` depends on exactly this ordering.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn micro_batch_over_an_undeclared_source_is_allowed() {
    let fx = PgFixture::shared();
    let (cp, _db) = fx.fresh_db().await;
    define_mv(&cp, &tref("s", "not_yet"))
        .await
        .expect("an undeclared source is allowed");
}

/// THE SYMMETRIC HALF — the ordering that defeats a registration-only guard. Register
/// the MV over a source that does not exist yet (legitimate, and asserted above), THEN
/// declare that source CDC via the production path (`land` with a CDC decl). The
/// declaration must be refused; otherwise a live micro-batch reader ends up sourcing a
/// CDC table, its floor pins every bucket at 0 forever, and the table's consolidate fold
/// declines on every attempt for the rest of time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declaring_cdc_on_a_table_an_mv_sources_is_refused() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let src = tref("s", "events");

    // 1. Register the MV over a source that does not exist yet — allowed.
    define_mv(&cp, &src).await.expect("undeclared source is allowed");

    // 2. Now write that source as CDC through the production declaration path. Refused.
    let err = declare_cdc_via_land(&pool, &catalog, &src)
        .await
        .expect_err("declaring CDC on a table a micro-batch MV sources must be refused");
    match err {
        ControlPlaneError::Validation(m) => assert!(
            m.contains("micro-batch"),
            "the message must name the reader: {m}"
        ),
        other => panic!("expected Validation, got {other:?}"),
    }
}

/// Non-regression: declaring a LOG stream on a table an MV sources is the SUPPORTED
/// configuration (it is what every MV in the tree does) and must still work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declaring_a_log_stream_on_a_table_an_mv_sources_is_allowed() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse");
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let src = tref("s", "events");

    define_mv(&cp, &src).await.expect("undeclared source is allowed");
    declare_log_via_land(&pool, &catalog, &src)
        .await
        .expect("a LOG declaration over an MV source is the supported configuration");
}
```

**The two declaration helpers.** `iceberg_landing::land` takes `stream_buckets: Option<i32>` and **cannot declare CDC** (`combine_stream_decl` maps `(Some(n), None) → StreamDecl::Log(n)`), so a `land_cdc` helper "over `land`" is impossible. The real CDC door — and the only production path into `reconcile_stream_mode` with `StreamDecl::Cdc` — is **`iceberg_landing::land_cdc`** (`:148`, already `pub`). Name the test helpers `declare_cdc_via_land` / `declare_log_via_land` so they do not collide with the imported `land_cdc`.

Do **not** substitute the raw `cp.declare_cdc` trait method here — that is the deliberately-unguarded escape hatch (it takes a bare `table_id`, so there is no `TableRef` to resolve readers against) and using it would make the test vacuous.

```rust
async fn declare_cdc_via_land(
    pool: &sqlx::PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
) -> Result<SnapshotId, ControlPlaneError> {
    let (schema, batches) = batch(2);
    land_cdc(
        pool,
        catalog,
        table,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(table),
        None, // stream_buckets MUST be None — passing both is an internal-caller bug
        Some(CdcDecl {
            buckets: 1,
            bucket_key: "id".into(),
            merge_engine: MergeEngine::LastRow,
        }),
        &[], // jobs
    )
    .await
}

async fn declare_log_via_land(
    pool: &sqlx::PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
) -> Result<SnapshotId, ControlPlaneError> {
    let (schema, batches) = batch(2);
    land(
        pool,
        catalog,
        table,
        &columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage(table),
        Some(1), // a LOG declaration
    )
    .await
}
```

`columns()` / `batch()` / `lineage()` / `tref()` come from the **`end_cap_seed`** library (Task 2s) — do **not** re-declare them. Imports:

```rust
use control_plane_core::{
    ControlPlane, ControlPlaneError, MergeEngine, SnapshotId, StreamTables, TableRef, TransformBody,
    TransformDef, TransformName,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{CdcDecl, InlineLimits, land, land_cdc};
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use end_cap_seed::{batch, columns, lineage, tref};
use loom_test_seed::local_sql_catalog;
```

BUCK: **`loom_fixture_test`** named `mv-source-refuse` / crate `mv_source_refuse`, deps: `"//src/testing:seed"`, `":postgres"`, `":end-cap-seed"`, `"//src/control-plane/core:core"`, `"//third-party:sqlx"`, `"//third-party:tempfile"`, `"//third-party:tokio"`.

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:mv-source-refuse`
Expected: `micro_batch_over_a_cdc_source_is_refused` FAILS on `expect_err` (the registration currently succeeds). The two non-regression tests pass already.

- [ ] **Step 3: Add the refusal**

In `transforms.rs`, right after the existing output-table `pg_refuse_stream_target` block (`:422-424`):

```rust
        // A micro-batch MV's SOURCE must be a log stream (declared, or not yet declared —
        // it becomes one on its first `?mode=stream` write). `mv_delta_scan` accepts LOG
        // sources only (`engine-serving/src/mv_delta.rs` — "cdc sources are deferred"), so
        // an MV over a CDC source could never run: its watermark would never advance, and
        // `mv_floor` would pin the source at offset 0 forever, permanently declining that
        // table's consolidate fold (`#iss-end-cap-ignores-mv-floor`). Refuse the
        // registration rather than accept an unrunnable one. Lift when `fut-mv-cdc-source`
        // lands. In-tx, so the refusal is atomic with the upsert.
        let mv_source: Option<TableRef> = match &def.body {
            TransformBody::MicroBatch { source, .. } => Some(source.clone()),
            TransformBody::MicroBatchJoin { source, .. } => Some(source.clone()),
            TransformBody::Physical { .. } | TransformBody::Typed { .. } => None,
        };
        if let Some(src) = &mv_source
            && let Some(tid) =
                crate::iceberg_mirror::live_table_id(&mut tx, &src.schema, &src.name).await?
            && let Some(meta) = crate::stream::pg_stream_meta(&mut *tx, tid).await?
            && meta.kind == control_plane_core::StreamKind::Cdc
        {
            return Err(ControlPlaneError::Validation(format!(
                "micro-batch source refused: {}.{} is a declared cdc table; a micro-batch \
                 MV reads log streams only (cdc sources are deferred)",
                src.schema, src.name
            )));
        }
```

(`TransformBody::MicroBatchJoin` does carry a `source: TableRef` field — `core/src/transforms.rs:75-83` — so the match arm above is correct as written. Let-chains are fine: edition 2024, and `stream.rs:75` already uses one.)

- [ ] **Step 3b: Add the symmetric declaration-side refusal**

In `stream.rs`, inside `reconcile_stream_mode` — **before** either declare arm (`pg_declare_cdc` at `:210`, `pg_declare_stream` at `:229`), alongside the existing bucket-count validation at `:73-80`, which is the established "reject before any declare" site:

```rust
    // The mirror of `define_transform`'s micro-batch source guard. A micro-batch MV reads
    // LOG streams only (`mv_delta_scan` — "cdc sources are deferred"), so a CDC declaration
    // on a table an MV already sources creates a reader that can never run: its watermark
    // never advances, `mv_floor` pins every bucket at 0 forever, and the table's consolidate
    // fold declines on every attempt for good (`#iss-end-cap-ignores-mv-floor`).
    //
    // Both halves are required. Guarding only `define_transform` is defeated by ordering —
    // an MV may legitimately be registered over a source that does not exist yet (it becomes
    // a log stream on its first `?mode=stream` write), and this is the path that then turns
    // that source into a CDC table. Lift both when `fut-mv-cdc-source` lands.
    if matches!(decl, StreamDecl::Cdc { .. }) {
        let readers = crate::transforms::pg_micro_batch_readers(&mut *conn, table).await?;
        if !readers.is_empty() {
            return Err(ControlPlaneError::Validation(format!(
                "cdc declaration refused: {}.{} is sourced by micro-batch MV(s) {readers:?}; \
                 a micro-batch MV reads log streams only (cdc sources are deferred)",
                table.schema, table.name
            )));
        }
    }
```

- [ ] **Step 4: Run the tests**

Run: `buck2 test --console none //src/control-plane/postgres:mv-source-refuse //src/control-plane/postgres:mv-floor //src/control-plane/postgres:transforms`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A src/control-plane/postgres
git commit -m "fix(transform): refuse a micro-batch MV over a cdc source"
```

---

### Task 6: The CDC consolidate fold skips and re-arms when the floor blocks it

After Tasks 3-5 the fold is the only path that can still meet a floor (via a **ghost watermark row** — `advance_mv_watermark` is a bare CAS that does not check a def exists, `iss-mv-watermark-ghost-rows`). It must **not** error: `RetryPolicy::Retry` has no max-attempts, so an `Err` is a 60-second failure drumbeat forever, and `Abandon` leaves the shadow tier permanently unfolded.

**Files:**
- Modify: `src/services/engine-serving/src/consolidate.rs` (`consolidate_locked`)
- Create: `src/services/engine-serving/tests/consolidate_mv_floor.rs` (+ BUCK target)

**Interfaces:**
- Consumes: `mv_floor::{removal_blocked, MV_FLOOR_REFUSAL_PREFIX}`; `iceberg_mirror::clear_consolidate_trigger` (`:708`, `pub`).

- [ ] **Step 1: Write the failing test**

Create `tests/consolidate_mv_floor.rs`. The seed is `end_cap_seed::seed_floored_cdc` (Task 2s) — an engine-serving test may depend on the postgres crate's test-support library exactly as query-api's e2e tests depend on `:e2e-support`.

**Three seed facts a naive version gets wrong — they are already handled inside `seed_floored_cdc`, do not re-derive them:**
- `declare_cdc` must come **before any write** (an unframed Iceberg table cannot be folded — the fold selects `loom_change_kind`/`loom_bucket`/`loom_offset`), and the deltas must be flushed so the base holds framed Parquet.
- `write_inline_delta` must pass `consolidate_threshold: Some(1000)` — large enough that deltas accrue without enqueuing a job. With `None` `bump_consolidate_trigger` never runs, **no `consolidate_trigger` row exists at all**, and the trigger assertion below fails with `RowNotFound`. **Task 2s's `seed_floored_cdc` must therefore pass `Some(1000)`, not `None`.**
- `consolidate_table(&cp, &sql_catalog, &pool, &table)` — control plane first, four args.

```rust
//! A CDC consolidate fold that would remove offsets an MV has not read SKIPS and
//! RE-ARMS — it never errors. `RetryPolicy::Retry` has no max-attempts (a 60-second
//! failure drumbeat forever) and `Abandon` leaves the shadow tier unfolded permanently.
//! Same posture `gc_locked` already takes against the floor: hold, warn, succeed.
//!
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use control_plane_core::{MvWatermarks, WatermarkAdvance, mv_key};
use control_plane_postgres::fixture::PgFixture;
use end_cap_seed::{live_file_count, seed_floored_cdc, tref};

/// A watermark row against the CDC source floors it: the fold returns `Ok(0)`, the live
/// files are untouched, and the trigger is cleared so a later write can re-enqueue.
/// It must NEVER error — the queue has no max-attempts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_floored_cdc_source_makes_the_fold_skip_and_rearm() {
    let fx = PgFixture::shared();
    let s = seed(fx, "widget").await;

    // The floor: an MV that has consumed offset 0 only. (A ghost watermark row —
    // `advance_mv_watermark` is a bare CAS and never checks that a def exists.)
    s.cp.advance_mv_watermark(
        &mv_key(&tref("main", "out_a")),
        s.tid,
        &[WatermarkAdvance {
            bucket: 0,
            from: 0,
            to: 1,
        }],
    )
    .await
    .expect("advance watermark");

    let before = live_file_count(&s.pool, s.tid).await;
    assert!(before > 0, "the flush must have left live files");

    let folded = engine_serving::consolidate_table(&s.cp, &s.catalog, &s.pool, &s.table)
        .await
        .expect("a blocked fold must SUCCEED as a no-op — never error, never abandon");
    assert_eq!(folded, 0, "a blocked fold folds nothing");
    assert_eq!(
        live_file_count(&s.pool, s.tid).await,
        before,
        "a blocked fold must not end-cap a single file"
    );

    let (count, enqueued): (i64, bool) = sqlx::query_as(
        "select delta_count, enqueued from iceberg_mirror.consolidate_trigger \
         where table_id = $1",
    )
    .bind(s.tid)
    .fetch_one(&s.pool)
    .await
    .expect("trigger row");
    assert_eq!(count, 0, "the trigger must be reset");
    assert!(
        !enqueued,
        "the trigger must be disarmed so a later write can re-enqueue"
    );
}

/// Non-regression: with no floor, the CDC fold runs exactly as it did before the seam.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unfloored_cdc_source_folds_unchanged() {
    let fx = PgFixture::shared();
    let s = seed(fx, "widget2").await;

    let folded = engine_serving::consolidate_table(&s.cp, &s.catalog, &s.pool, &s.table)
        .await
        .expect("consolidate_table");
    assert!(folded > 0, "an unfloored source folds normally");
}
```

BUCK: a **`loom_fixture_test`** (already loaded at `src/services/engine-serving/BUCK:2`), named `consolidate-mv-floor` / crate `consolidate_mv_floor`, deps: `":engine-serving"`, `"//src/control-plane/core:core"`, `"//src/control-plane/postgres:postgres"`, `"//src/control-plane/postgres:end-cap-seed"`, `"//third-party:sqlx"`, `"//third-party:tokio"`.

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/services/engine-serving:consolidate-mv-floor`
Expected: `a_floored_cdc_source_makes_the_fold_skip_and_rearm` FAILS — after Task 2b the fold **errors** with the seam's `Validation` rather than skipping cleanly.

- [ ] **Step 3: Pre-check, and catch the in-tx refusal**

In `consolidate_locked`, before the fold reads any files — inside the per-table advisory lock the arm already holds:

```rust
    // The fold is a `Removing` end-cap: it retires every live file and re-projects only
    // the fold winners. If an MV has not read some of those offsets, removing them leaves
    // a hole in its delta — so decline, and say why.
    //
    // Decline, do NOT error: `RetryPolicy::Retry` has no max-attempts (a permanent
    // 60-second failure drumbeat) and `Abandon` leaves the shadow tier unfolded forever.
    // `clear_consolidate_trigger` re-arms: the next `threshold` deltas enqueue a fresh
    // job — a write-proportional backoff with no timers. `has_shadow` stays SET (the
    // shadow tier really is still unfolded). Same posture `gc_locked` takes: hold, warn,
    // succeed.
    let mut conn = pool.acquire().await.map_err(to_serving)?;
    if let Some(floor) = removal_blocked(&mut conn, table, tid).await.map_err(to_serving)? {
        tracing::warn!(
            schema = %table.schema, name = %table.name, tid,
            floor = floor.min_offset(), slowest = ?floor.slowest,
            "consolidate skipped: the fold would remove offsets a micro-batch MV has not consumed",
        );
        clear_consolidate_trigger(&mut conn, tid).await.map_err(to_serving)?;
        return Ok(0);
    }
    drop(conn);
```

**The pre-check is not sufficient on its own** and the plan must not pretend otherwise: it runs on a pooled connection *outside* the fold's commit transaction, and neither `advance_mv_watermark` nor `define_transform` takes the fold's advisory lock. A watermark row committing in that window makes the fold's own in-tx `guard_end_cap` refuse — straight into the retry drumbeat this task exists to avoid. So **also** convert that error at the commit.

**⚠️ Match the MESSAGE, not the variant.** `guard_end_cap` raises `ControlPlaneError::Validation`, but that variant **does not survive the catalog boundary**: `commit_mirror_in_tx` wraps it into `iceberg::Error{Unexpected}` (`commit_mirror.rs:432-439`) and `append_parquet_snapshot` re-wraps that with `backend()` (`iceberg_landing.rs:342-344`), so it arrives as `ControlPlaneError::Backend`. An `Err(ControlPlaneError::Validation(m))` arm here is **dead code** — and because the pre-check catches the common case, both tests in this task would still pass while the race path silently retried forever. This is exactly why `MV_FLOOR_REFUSAL_PREFIX` exists, and it is the same message-sniffing idiom `worker/src/stream_mv.rs::classify_*` uses over gRPC (where the status code is likewise flattened).

(Note the asymmetry: the empty-batch `overwrite_truncate` branch never touches the catalog and *does* return a real `Validation`. Matching on the message covers both.)

```rust
    let snap = match overwrite_stream_base(
        pool,
        catalog,
        table,
        &user_cols,
        folded,
        Some(&lineage),
        inline.as_ref().map(|(_, row_ids, _)| InlineEndCap {
            table_id: tid,
            row_ids,
        }),
    )
    .await
    {
        Ok(s) => s,
        // Raced: an MV floor appeared between the pre-check and the commit. The refusal is
        // raised inside `write_mirror` and re-wrapped as `Backend` on the way out, so the
        // VARIANT is gone — only the message survives. Same decline, same re-arm; never a
        // retryable error.
        Err(e) if e.to_string().contains(MV_FLOOR_REFUSAL_PREFIX) => {
            tracing::warn!(
                schema = %table.schema, name = %table.name, tid, reason = %e,
                "consolidate skipped: the MV floor moved under the fold",
            );
            let mut conn = pool.acquire().await.map_err(to_serving)?;
            clear_consolidate_trigger(&mut conn, tid)
                .await
                .map_err(to_serving)?;
            return Ok(0);
        }
        Err(e) => return Err(to_serving(e)),
    };
```

**`consolidate.rs` needs exactly ONE new import:** `use control_plane_postgres::mv_floor::{MV_FLOOR_REFUSAL_PREFIX, removal_blocked};`. `clear_consolidate_trigger` is **already imported** (`consolidate.rs:47-49`, from `iceberg_mirror`) and so are `has_shadow` (`:41-43`) and `StreamKind` (`:34-37`) which Task 4's `Log` arm uses — re-importing any of them is `E0252`.

- [ ] **Step 3b: Pin the contract this arm depends on**

The message-match above is only correct if the prefix really does survive the commit wrap. Prove it, or a future refactor of `backend()` silently reopens the drumbeat. This test goes in **`src/control-plane/postgres/tests/end_cap_intent.rs`** (it drives a postgres entrypoint, and Task 2b Step 4 already put the `seed_floored_cdc` helper there). `overwrite_stream_base` exists from Task 3, so this step is correctly ordered after it.

Add these imports to `end_cap_intent.rs`: `use control_plane_postgres::iceberg_landing::overwrite_stream_base;`, and extend its `mv_floor` import to `{EndCapIntent, MV_FLOOR_REFUSAL_PREFIX, guard_end_cap, mv_floor, removal_blocked}`. The framed batch needs `arrow_array::Int32Array` (the dep is already listed).

**Do not copy `framed_batch()` from `tests/stream_overwrite_framing.rs`** — it is `fn framed_batch(ids: &[i64], bucket: i32, start_offset: i64)` over a **one**-user-column table, and `seed_floored_cdc`'s table is `(id, val)`. Write a local one in the column order `augment_with_framing` builds (user columns, then the three reserved):

```rust
/// A framed CDC batch for the `(id, val)` table: user columns first, then the three
/// reserved framing columns, in the order `augment_with_framing` builds them. This is what
/// the CDC consolidate fold produces and what `overwrite_stream_base` demands.
fn framed_cdc_batch(id: i64, val: i64, bucket: i32, offset: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
        Field::new("loom_change_kind", DataType::Utf8, true),
        Field::new("loom_bucket", DataType::Int32, true),
        Field::new("loom_offset", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(Int64Array::from(vec![val])),
            Arc::new(StringArray::from(vec!["+I"])),
            Arc::new(Int32Array::from(vec![bucket])),
            Arc::new(Int64Array::from(vec![offset])),
        ],
    )
    .expect("framed batch")
}

/// The refusal MESSAGE must survive the catalog commit wrap. `guard_end_cap` raises
/// `Validation` inside `write_mirror`, but `commit_mirror_in_tx` re-wraps it as
/// `iceberg::Error{Unexpected}` (`commit_mirror.rs:435-439`) and `append_parquet_snapshot`
/// re-wraps THAT with `backend()` (`iceberg_landing.rs:343`) — so the VARIANT is destroyed
/// and only the string survives. `consolidate_table`'s race arm matches on
/// `MV_FLOOR_REFUSAL_PREFIX` for exactly this reason; if this test fails, that arm is dead
/// code and a raced fold retries every 60 seconds forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_floor_refusal_message_survives_the_commit_wrap() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let s = seed_floored_cdc(fx, &cp, &db).await;

    let err = overwrite_stream_base(
        &s.pool,
        &s.catalog,
        &s.table,
        &cdc_specs(),
        vec![framed_cdc_batch(1, 300, 0, 0)],
        Some(&lineage(&s.table)),
        None,
    )
    .await
    .expect_err("a floored stream base must refuse the framed overwrite");

    assert!(
        err.to_string().contains(MV_FLOOR_REFUSAL_PREFIX),
        "the refusal message must survive the catalog wrap; got: {err}"
    );
}
```

- [ ] **Step 4: Run the tests**

Run: `buck2 test --console none //src/services/engine-serving:consolidate-mv-floor //src/control-plane/postgres:end-cap-intent`
Expected: `consolidate-mv-floor` → `Pass 2. Fail 0`; `end-cap-intent` → `Pass 9. Fail 0` (5 from Task 2a + 2 primitive + the CDC flush + `the_floor_refusal_message_survives_the_commit_wrap`). **Both targets** — Step 3b's test lives in `end-cap-intent`, so running only `consolidate-mv-floor` never executes it.

- [ ] **Step 5: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A src/services/engine-serving
git commit -m "fix(consolidate): skip and re-arm when the MV floor blocks the CDC fold"
```

---

### Task 7: Migrate `mv_floor.rs` onto the seed library; delete the synthetic end-cap helper

Two jobs, both in `tests/mv_floor.rs`:

1. **Migrate it onto `end_cap_seed`** (the operator's decision in Task 2s). Task 2s lifted its helpers into the library but deliberately left this file's copies in place so the suite stayed green mid-plan. Now delete the local `tref` / `columns` / `batch` / `lineage` / `register_mv` / `advance` / `Seeded` / `seed_source` / `age_all_snapshots` / `end_capped_inline_count` / `data_file_count` / `current_snapshot_id` and `use end_cap_seed::{…}` instead. Add `":end-cap-seed"` to the `mv-floor` BUCK target's deps and drop the deps that only the moved helpers needed (`arrow-array`, `arrow-schema`, `serde_json`, `time`, `uuid`) **only if** nothing left in the file names them — check `[clippy.txt]` is empty rather than guessing. **This is the step that makes the duplication gate come back clean;** if `mv-floor` goes red here, the library's helper is not equivalent to the copy it replaced — fix the library, do not fork it back.
2. **Delete the synthetic raw-SQL end-cap helper.** `tests/mv_floor.rs:362-372` carries `end_cap_data_files` *precisely because no guarded API existed*. It does now.

**Files:**
- Modify: `src/control-plane/postgres/tests/mv_floor.rs`
- Modify: `src/control-plane/postgres/BUCK` (the `mv-floor` target's deps)

- [ ] **Step 1: Replace the synthetic helper with the real primitive**

Delete the raw `update iceberg_mirror.data_file set end_snapshot = …` helper (`:362-372`) and drive the guarded API. Compaction is the honest intent for the state it simulates, and `Reframing` is what compaction declares:

```rust
/// End-cap every live data file of `tid` at `snap` through the REAL guarded primitive.
/// Declared `Reframing` — which is what plain-coalesce compaction is: the same rows are
/// re-projected at the same offsets, so the floor is (correctly) not consulted. Before
/// the end-cap seam existed this was raw SQL, because no guarded API did.
async fn end_cap_data_files(pool: &sqlx::PgPool, table: &TableRef, tid: i64, snap: i64) {
    let mut tx = pool.begin().await.expect("tx");
    end_cap_live_data_files(&mut tx, table, tid, SnapshotId(snap), &EndCapIntent::Reframing)
        .await
        .expect("end-cap data files");
    tx.commit().await.expect("commit");
}
```

It has **two** call sites, not four: `unrun_mv_pins_every_end_capped_row_and_file` (`:529`) and `caught_up_mv_releases_end_capped_files` (`:586`). Pass `&s.src` at each. Add `use control_plane_postgres::iceberg_mirror::end_cap_live_data_files;`, `use control_plane_postgres::mv_floor::EndCapIntent;`, and `SnapshotId` to the `control_plane_core` import.

- [ ] **Step 2: Add the real-path over-refusal test**

This is the test that catches a naive seam: flush end-caps inline rows *above* the floor and re-projects them into live Parquet at the same `(bucket, offset)`, so it must still succeed with an MV lagging.

**`flush_table(catalog, pool, table, run_id)` — catalog FIRST, and a `RunId`.** The five existing calls in this file (`:435`, `:482`, `:632`, `:665`, `:755`) are the reference.

```rust
/// The over-refusal guard, on the REAL path: a flush with an MV still at 3 of 6 must
/// SUCCEED. Flush end-caps inline rows above the floor by design and re-projects those
/// SAME rows into live Parquet at the SAME `(bucket, offset)` — the rows never leave the
/// live set, so no MV can miss one. A seam that refused any end-cap at or above the floor
/// would break flush entirely; this test is what fails when someone writes that seam.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_with_a_lagging_mv_still_succeeds() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // inline = true, so the flush has rows to move.
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        Some(1),
        true,
        &[("mv_a", "out_a")],
    )
    .await;
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 3).await;

    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("a flush is REFRAMING: it must not be refused by the MV floor");

    assert!(
        data_file_count(&s.pool, s.tid).await > 0,
        "flush re-projected the inline rows into live files"
    );
}
```

- [ ] **Step 3: Run the tests**

Run: `buck2 test --console none //src/control-plane/postgres:mv-floor`
Expected: `Pass 14. Fail 0` (13 existing + this one). `cdc_flush_with_a_floored_source_still_succeeds` lives in the **`end-cap-intent`** target, not this one — do not go hunting for a 15th here.

- [ ] **Step 4: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A src/control-plane/postgres/tests
git commit -m "test(mv-floor): drive the real end-cap primitive; pin flush as reframing"
```

---

### Task 8: Full suite, metric gate, and the documentation registers

- [ ] **Step 1: Run the full suite**

Run: `buck2 test --console none //src/... -j 8`
Expected: `Fail 0`. The `-j 8` cap is mandatory — 8 Postgres fixture boot slots; an unthrottled run gives non-deterministic 120s timeouts that look like real failures.

- [ ] **Step 2: Run the metric gate**

Run the `loom-complexity` skill with argument `diff` and the `loom-duplication` skill with argument `diff`.

Report any NEW hotspot over the census thresholds (cc > 15, cognitive > 15, MI < 20, SLOC > 100) or NEW cross-file duplication pair ≥ 20 lines. **Measure the same files on `origin/main` first** — `diff` mode reports on every touched file, so it surfaces pre-existing hotspots the branch did not create. Each finding must be fixed or explicitly justified in the PR description.

**Duplication should come back CLEAN.** The operator's decision (Task 2s) was to share the seed trio through the `end-cap-seed` `rust_library` rather than copy it per file, and Task 7 migrates `tests/mv_floor.rs` onto it too — so a duplication finding among the new test files means the library was not actually adopted somewhere. Chase it; do not justify it.

One complexity finding is expected and justifiable rather than fixable: `overwrite_with_cap` gains a ninth argument (it already carries `#[expect(clippy::too_many_arguments)]`; extend the reason to name `framed`).

- [ ] **Step 3: Close the register item**

Run the `loom-docs-update` skill. It must:
- **Remove** `iss-end-cap-ignores-mv-floor` from `docs/ISSUES.md` (registers carry open work only).
- **Leave `iss-mv-register-below-reclaimed-floor` open**, amending its prose: its *race* half ("`mv_floor` is read on the pool, outside the GC transaction") is closed by Task 1; only the registration-bootstrap half remains. Name this PR there.
- Fold the landed capability into `docs/system-capabilities/`: the `EndCapIntent` seam and its three intents; the three refusals (overwrite of a declared stream table; typed UPDATE/DELETE against a declared log table; micro-batch MV over a CDC source) and the CDC fold's `overwrite_stream_base` door; consolidate's skip-and-re-arm; and the **manual repair** for a legacy log table already carrying `has_shadow` (it will neither fold nor flush until an operator clears the flag).
- Record the CDC-source MV refusal against `fut-mv-cdc-source` in `docs/FUTURE.md`: when CDC sources become MV-readable, the Task 5 refusal must be lifted **and** the floor guard on the CDC fold becomes live for the first time.

Carry the five corrections from *Corrections to carry into `docs/ISSUES.md`* into the capability docs — the register entry's picture of the world was wrong in ways the next reader would otherwise inherit.

- [ ] **Step 4: Commit and open the PR**

```bash
buck2 run //tools:prek -- run --all-files
git add -A docs src
git commit -m "docs(stream): close iss-end-cap-ignores-mv-floor; record the end-cap seam"
```

**Before pushing**, lease-check the claim branch (another session may hold it after a stale reap):

```bash
git ls-remote origin work/iss-end-cap-ignores-mv-floor
git merge-base --is-ancestor <remote-sha> HEAD
```

If the remote tip is **not** an ancestor of your branch — someone else's commits are on it — STOP and surface the collision rather than force-pushing over live work.

Then push and open the PR with head `work/iss-end-cap-ignores-mv-floor` (this is what binds the claim to the PR), per `superpowers:finishing-a-development-branch`. The PR body must:
- name the item id and the resolved open questions;
- **state plainly that after the refusals no `Removing` end-cap is reachable on a declared log stream table, so the seam's runtime guard is future-proofing rather than a live fix** — do not overclaim it;
- carry the metric-gate findings with justifications;
- note that `iss-mv-register-below-reclaimed-floor`'s race half is closed here but the item stays open.
