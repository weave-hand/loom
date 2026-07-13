# MV Watermark-Aware GC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `gc_table` refuse to reclaim source bytes a micro-batch materialized view has not consumed yet, and ship the reusable `mv_floor` primitive that the row-retiring paths must consult.

**Architecture:** A new `mv_floor` module computes, for the live incarnation of a table, the per-bucket **MV floor** — `min(next_offset)` across every micro-batch MV reading that table as a source, where an MV with no watermark row for a bucket (including a registered-but-never-run MV) floors that bucket at 0. `gc_locked` threads that floor into its reclaim selections as an extra predicate: a **file** is reclaimable only if its `loom_offset` max stat is strictly below the *smallest* floor across buckets (per-file stats are not per-bucket, so the cross-bucket min is the conservative bound), and an **inline row** is reclaimable only if its `(loom_bucket, loom_offset)` is strictly below *its own* bucket's floor. Tables with no MV readers pass a NULL guard and behave byte-identically. The dropped-incarnation reclaim deliberately bypasses the floor (loudly), so drop-GC stays convergent.

**Tech Stack:** Rust, sqlx compile-time `query!` against Postgres, buck2 `loom_fixture_test` targets, Arrow/Iceberg mirror.

**Spec:** `docs/superpowers/specs/2026-07-12-mv-watermark-aware-gc-design.md`

## What this prevents — and what it does NOT (read first; the spec overstates this)

The spec's claim is that landing the floor on `gc_locked` makes "every current and future reclaim path inherit it". **That is not true, and the plan does not pretend it is.** Two facts, both verified in the source:

1. **`gc_table` can never remove a row an MV could read.** GC's safety invariant reclaims only rows with `end_snapshot IS NOT NULL AND end_snapshot <= H` (`iceberg_gc.rs:14-22`). An MV's delta reads *live* rows at the *current* snapshot — `files_with_stats(table, current.id)` ∪ `inline_live_batch_full(table, current.id)` (`engine-serving/src/mv_delta.rs:125-158`). An end-capped row is invisible to a current-snapshot read by construction. So the offsets a lagging MV still needs are LIVE rows, which GC never touches, and the offsets GC reclaims are ones the MV already cannot see.
2. **Therefore the C7 harm is created at END-CAP time, not at reclaim time.** A path that retires rows without a replacement (drop today; CDC changelog retention and any truncation/replay surface tomorrow) hides them from the MV the moment it end-caps them. Holding their *bytes* back in GC does not give them back. The end-capping paths do NOT route through `gc_locked` — stream/small-file compaction end-caps in `iceberg_compact::compact_table`, drop in the catalog — so they inherit nothing from this guard.

What this branch therefore delivers, stated honestly (and this is what the PR body must say):

- **The `mv_floor` primitive** — the per-bucket, reader-union floor computation. This is the reusable piece the end-cap paths must call; it is the actual load-bearing deliverable.
- **The GC-tier guard** — byte-retention defense: a lagging MV's end-capped tail is not physically destroyed while it is still behind. Correct, cheap, strictly conservative, and byte-identical for the ~all tables no MV reads. It is *defense in depth for the bytes*, **not** MV-hole prevention.
- **Observability** — `held_by_mv_floor` + a log naming the laggard, and a loud warning when a dropped source strands MVs.

**The real fix is filed, not built here** (Task 5): a register item requiring the end-cap-issuing paths to consult `mv_floor` *before* end-capping. Do not let the PR (or the capability docs) claim GC prevents MV data loss.

## Global Constraints

- **Strict clippy** (whole `pedantic` + `restriction` groups on lib code): no `unwrap()`, no `expect()`, no `panic!`, no indexing/slicing. Silence locally only with `#[expect(lint, reason = "...")]`. Test code is exempted from the panic-safety lints by the `loom_rust_test`/`loom_fixture_test` wrappers, so `.expect(...)` in tests is fine.
- **Tests are `rust_test`/`loom_fixture_test` targets only** — never inline `#[cfg(test)] mod tests` (the `no-inline-tests` prek hook fails the commit). Every new test file needs its own target in the crate's `BUCK`. Fixture (Postgres-booting) tests MUST use `loom_fixture_test`.
- **New/changed SQL in `query!`/`query_scalar!` macros requires regenerating the offline cache:** `bash tools/sqlx-prepare.sh`, then commit the `src/control-plane/postgres/.sqlx/` changes. The `sqlx-cache-check` test fails otherwise.
- **`buck2 run //tools:prek -- run --all-files` before every commit** (rustfmt is a separate hook from clippy; `git add` new files *before* the prek run, or prek skips them and you ship unformatted code).
- **Build/test commands use `--console none`:** `buck2 build -v0 --console none //src/...`, `buck2 test --console none //src/control-plane/postgres:mv-floor`. Run whole-crate suites with `-j 8` (postgres fixture boot slots).
- **GC's commit-then-delete ordering and advisory-lock discipline are unchanged** — the floor tightens *selection*, never the protocol.
- Conventional Commits are enforced on the message by a prek `commit-msg` hook (`feat(...)`, `test(...)`, `docs(...)`).

## Design decisions this plan locks in (read before Task 1)

1. **Who counts as a "reader"** — the MV set for a source is the **union** of (a) every registered `MicroBatch`/`MicroBatchJoin` transform def whose `source` is this table, keyed by `mv_key(output)`, and (b) every distinct `mv` holding a `stream.mv_watermark` row against the source's `table_id`. (a) alone would miss an ad-hoc run's watermarks; (b) alone would miss a registered-but-never-run MV (which must floor at 0). The union is the fail-safe set.
2. **The escape hatch is real** — because (b) lets stale watermark rows pin the tail, `delete_transform` on an MV def now deletes that MV's watermark rows in the same transaction (Task 3). Deleting the registration therefore actually releases the floor, as the spec's liveness trade-off promises.
3. **A bucket with no watermark row floors at 0** — matching `mv_delta_scan`'s read contract ("a bucket absent from `wm` reads as 0", `engine-serving/src/mv_delta.rs:178-183`). So an unrun MV, or an MV that has never touched bucket 3, holds that bucket entirely.
4. **No wire change.** `GcSummary.held_by_mv_floor` is asserted by tests and logged by the engine process where `gc_locked` actually runs; the `EngineControl::GcTable` response stays a 3-field message (the worker discards it today — `worker/src/handler.rs:50-52`). Adding a proto field nobody reads is YAGNI. Say so in the PR body.
5. **Where the tests live** — the spec says "extend `postgres/tests/iceberg_gc.rs`"; that file is already 760 lines and its helpers are batch-table shaped. This plan puts the floor tests in a new focused `postgres/tests/mv_floor.rs` (floor semantics + GC hold/release/bypass, sharing one stream-table seed helper) and touches `iceberg_gc.rs` only to add the new `GcSummary` field to its single struct literal. The **worker e2e goes INTO the existing `worker/tests/stream_mv_e2e.rs`** as a new `#[tokio::test]`, reusing that file's seven helpers (`tref`/`events_columns`/`events_batch`/`seed_lineage`/`build_ctx`/`run_micro_batch`/`doubled_rows` — plus `make_stream_mv_job`, which `run_micro_batch` calls). A new sibling file would mean copying ~140 lines of helpers into the same crate — a guaranteed `loom-duplication` hit and exactly what this branch's own metric gate forbids.
6. **The floor query is NOT the spec's SQL, deliberately.** The spec's `select bucket, min(next_offset) … group by bucket` is wrong on its own: an MV holding a row for bucket 0 but none for bucket 1 contributes nothing to bucket 1's `MIN`, so the other MVs would set that bucket's floor and the "absent MV reads from 0" rule would be silently lost. Task 1 fetches the rows and folds per `(bucket, reader)` in Rust with `unwrap_or(0)`. Same reason the plan writes its own query instead of the spec's named `pg_mv_watermarks` (that helper is per-`mv`, single-source). Say this in the PR body so a reviewer does not "fix" it back.
7. **Both control-plane backends must agree.** Task 3 changes a `Transforms` trait *semantic* (deleting an MV def drops its watermarks), so it lands in the postgres adapter AND the `memory` fake (`src/control-plane/memory/src/transforms.rs:101-106`, whose `MemoryControlPlane.mv_watermarks` map is keyed `(mv, source_table_id, bucket)` — `memory/src/lib.rs:66`), with a `testkit` contract test so the shared suite certifies both.
8. **The `max_value::bigint` cast must not be evaluated before its filter.** `data_file_column_stat.max_value` is `text` and holds *every* column's bound, including string columns. Postgres may reorder quals within one `WHERE`, so `column_name = 'loom_offset' AND max_value::bigint < $3` can raise `invalid input syntax for type bigint` on a table with a string column — a hard GC error. The plan therefore uses a **scalar subquery** whose projection is cast *outside* it (a subquery's `WHERE` provably runs before its projection reaches the outer cast), and a NULL result (no stat row) fails the comparison, which is exactly the fail-safe hold.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/control-plane/postgres/src/mv_floor.rs` | **New.** `MvFloor` type + `mv_floor()` (per-bucket floor for a live tid) + `stranded_mv_readers()` (for the dropped-source warning). |
| `src/control-plane/postgres/src/transforms.rs` | **Modify.** New `pg_micro_batch_readers()` (registered MV defs → `mv_key`s, by source); `delete_transform` also deletes the MV's watermark rows. Add `mv_key` to its `control_plane_core::{…}` import — it is not imported today. |
| `src/control-plane/memory/src/transforms.rs` | **Modify (Task 3).** The `memory` fake's `delete_transform` drops the MV's watermark entries too, or the two backends diverge on a documented semantic. |
| `src/control-plane/testkit/src/lib.rs` | **Modify (Task 3).** Contract test: deleting an MV def clears its watermarks — certified against BOTH backends. |
| `src/control-plane/postgres/src/lib.rs` | **Modify.** `pub mod mv_floor;` |
| `src/control-plane/postgres/src/iceberg_gc.rs` | **Modify.** `GcSummary.held_by_mv_floor`; floor lookup in `gc_locked`; guarded `reclaimable_paths` / `delete_data_files` / `delete_end_capped_inline_rows`; new `count_candidates`; two `tracing::warn!`s. |
| `src/control-plane/postgres/tests/mv_floor.rs` | **New.** Fixture tests: floor semantics + GC hold / release / unrun-pin / min-wins / non-source regression / dropped-source bypass / delete-registration-releases. |
| `src/control-plane/postgres/tests/iceberg_gc.rs` | **Modify.** One `GcSummary { … }` literal gains `held_by_mv_floor: 0`. |
| `src/control-plane/postgres/BUCK` | **Modify.** New `loom_fixture_test(name = "mv-floor")`. |
| `src/services/worker/tests/stream_mv_e2e.rs` | **Modify (Task 4).** One new `#[tokio::test]`: a lagging MV's end-capped FILE survives GC over the engine wire; catch-up + GC converges. Reuses the file's existing helpers — no new test file, no new BUCK target. |
| `docs/system-capabilities/*.md`, `docs/ROADMAP.md` | **Modify (Task 5).** Document the landed capability; close `road-mv-watermark-aware-gc`. |

---

## Task 1: The MV floor (`mv_floor` module)

Compute the per-bucket reclaim floor for a source table. Pure read path — GC is untouched in this task, so it lands and tests independently.

**Files:**
- Create: `src/control-plane/postgres/src/mv_floor.rs`
- Create: `src/control-plane/postgres/tests/mv_floor.rs` (Task 2 appends GC tests to the same file)
- Modify: `src/control-plane/postgres/src/lib.rs` (`pub mod mv_floor;`, grouped with the other `pub mod`s)
- Modify: `src/control-plane/postgres/src/transforms.rs` (add `pg_micro_batch_readers`)
- Modify: `src/control-plane/postgres/BUCK` (new `mv-floor` fixture test)

**Interfaces:**
- Consumes: `crate::stream::pg_stream_bucket_count(ex, table_id) -> Result<Option<i32>>`; `crate::transforms::de_body(serde_json::Value) -> Result<TransformBody>`; `control_plane_core::{TableRef, TransformBody, mv_key}`; `crate::backend` (sqlx → `ControlPlaneError`).
- Produces (Task 2 consumes these exact names):
  - `pub struct MvFloor { pub per_bucket: BTreeMap<i32, i64>, pub slowest: BTreeMap<i32, String> }`
  - `impl MvFloor { pub fn min_offset(&self) -> i64 }`
  - `pub async fn mv_floor(pool: &PgPool, table: &TableRef, tid: i64) -> Result<Option<MvFloor>>`
  - `pub async fn stranded_mv_readers(pool: &PgPool, table: &TableRef, tids: &[i64], include_registered: bool) -> Result<BTreeSet<String>>`
  - `pub(crate) async fn pg_micro_batch_readers<'e, E: sqlx::PgExecutor<'e>>(ex: E, table: &TableRef) -> Result<BTreeSet<String>>` (in `transforms.rs`)

- [ ] **Step 1: Write the failing test file**

Create `src/control-plane/postgres/tests/mv_floor.rs`. Its helpers serve Task 2's GC tests too — this is the ONLY place the stream-source seed shape may live (six tests reuse it; a copy-pasted seed is exactly what the duplication gate flags).

```rust
//! Fixture tests for the MV read-position floor (`mv_floor`) and the
//! watermark-aware GC guard it drives (road-mv-watermark-aware-gc).
//!
//! The floor is the per-bucket `min(next_offset)` across every micro-batch MV
//! reading a source table — where an MV with no watermark row for a bucket
//! (a registered-but-never-run MV, or one that has never touched that bucket)
//! floors it at 0, exactly as `mv_delta_scan` reads it.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    ColumnSpec, ControlPlane, DatasetId, EventType, LineageEvent, MvWatermarks, RunId, TableRef,
    TransformBody, TransformDef, TransformName, WatermarkAdvance, mv_key,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_mirror::live_table_id;
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use control_plane_postgres::mv_floor::mv_floor;
use loom_test_seed::local_sql_catalog;
use time::OffsetDateTime;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

/// An `(id: long)` schema + batch of ids `0..rows`.
fn batch(rows: i64) -> (Arc<Schema>, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let b = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    )
    .expect("batch");
    (schema, vec![b])
}

fn lineage(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "mv-floor-test" }),
    }
}

/// Register a micro-batch MV over `source` -> `s.<output>` WITHOUT a data trigger
/// (`on_input_commit: false`), so landing into the source never auto-fires a run:
/// these tests drive the watermark by hand. Registration alone is what the floor
/// keys off — an MV that never runs must pin its source at 0.
async fn register_mv(cp: &PgControlPlane, name: &str, source: &TableRef, output: &TableRef) {
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
        .expect("register mv");
}

/// CAS-advance `mv`'s watermark for `(source_tid, bucket)` from `from` to `to` —
/// what a completed micro-batch commit does (`pg_advance_mv_watermark`).
async fn advance(cp: &PgControlPlane, mv: &str, source_tid: i64, bucket: i32, from: i64, to: i64) {
    cp.advance_mv_watermark(mv, source_tid, &[WatermarkAdvance { bucket, from, to }])
        .await
        .expect("advance watermark");
}

/// The seeded world every test in this file starts from.
struct Seeded {
    pool: sqlx::PgPool,
    catalog: SqlCatalog,
    src: TableRef,
    tid: i64,
}

/// Seed `s.events` with `rows` events: a declared log stream table of
/// `buckets` buckets (`None` = a plain, non-stream table), landed INLINE when
/// `inline` is true and straight into Parquet FILES when false, with one MV
/// registered per `(transform name, output name)` in `mvs`.
async fn seed_source(
    fx: &PgFixture,
    cp: &PgControlPlane,
    db: &str,
    wh: &str,
    rows: i64,
    buckets: Option<i32>,
    inline: bool,
    mvs: &[(&str, &str)],
) -> Seeded {
    let pool = fx.pool_for(db).await;
    let catalog = local_sql_catalog(fx.pg_dsn(db), wh).await;
    let src = tref("s", "events");
    let (schema, batches) = batch(rows);
    land(
        &pool,
        &catalog,
        &src,
        &columns(),
        schema,
        batches,
        InlineLimits {
            // A 0 byte limit forces the write straight to Parquet; a large one
            // keeps every row inline until an explicit flush.
            inline_byte_limit: if inline { 1 << 20 } else { 0 },
            flush_byte_threshold: i64::MAX,
        },
        lineage(&src),
        buckets,
    )
    .await
    .expect("land source");
    for (name, output) in mvs {
        register_mv(cp, name, &src, &tref("s", output)).await;
    }
    let mut conn = pool.acquire().await.expect("conn");
    let tid = live_table_id(&mut conn, &src.schema, &src.name)
        .await
        .expect("tid")
        .expect("live tid");
    drop(conn);
    Seeded {
        pool,
        catalog,
        src,
        tid,
    }
}

// ---- floor semantics -------------------------------------------------------

/// A plain (non-stream) table has no floor at all: GC keeps its fast path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_table_has_no_floor() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        3,
        None,
        true,
        &[],
    )
    .await;

    assert_eq!(
        mv_floor(&s.pool, &s.src, s.tid).await.expect("floor"),
        None,
        "a non-stream table has no MV floor"
    );
}

/// A declared stream table nothing reads: still no floor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_table_without_readers_has_no_floor() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        4,
        Some(2),
        true,
        &[],
    )
    .await;

    assert_eq!(
        mv_floor(&s.pool, &s.src, s.tid).await.expect("floor"),
        None,
        "no MV reads this stream table -> no floor"
    );
}

/// A registered MV that has never run pins EVERY bucket at 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_but_unrun_mv_floors_at_zero() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        4,
        Some(2),
        true,
        &[("mv_a", "out_a")],
    )
    .await;

    let floor = mv_floor(&s.pool, &s.src, s.tid)
        .await
        .expect("floor")
        .expect("a registered MV reads this source");
    assert_eq!(
        floor.per_bucket,
        [(0, 0), (1, 0)].into_iter().collect(),
        "an unrun MV floors every bucket at 0"
    );
    assert_eq!(floor.min_offset(), 0, "the file guard holds everything");
}

/// Two MVs at different progress: the SLOWER one sets each bucket's floor, and an
/// MV with no row for a bucket floors THAT bucket at 0 even while ahead elsewhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slowest_mv_sets_the_floor_per_bucket() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        8,
        Some(2),
        true,
        &[("mv_a", "out_a"), ("mv_b", "out_b")],
    )
    .await;
    let a = mv_key(&tref("s", "out_a"));
    let b = mv_key(&tref("s", "out_b"));

    // mv_a consumed bucket 0 to 5 and bucket 1 to 3; mv_b consumed only bucket 0,
    // to 2, and has never touched bucket 1.
    advance(&cp, &a, s.tid, 0, 0, 5).await;
    advance(&cp, &a, s.tid, 1, 0, 3).await;
    advance(&cp, &b, s.tid, 0, 0, 2).await;

    let floor = mv_floor(&s.pool, &s.src, s.tid)
        .await
        .expect("floor")
        .expect("two MVs read this source");
    assert_eq!(
        floor.per_bucket,
        [(0, 2), (1, 0)].into_iter().collect(),
        "bucket 0: min(5, 2) = 2; bucket 1: mv_b has no row there -> 0"
    );
    assert_eq!(
        floor.slowest.get(&0).map(String::as_str),
        Some(b.as_str()),
        "bucket 0's laggard is mv_b"
    );
    assert_eq!(
        floor.min_offset(),
        0,
        "the file guard takes the cross-bucket minimum"
    );
}

/// A single caught-up MV floors at its watermark (the release case Task 2 leans on).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caught_up_mv_floors_at_its_watermark() {
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
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 4).await;

    let floor = mv_floor(&s.pool, &s.src, s.tid)
        .await
        .expect("floor")
        .expect("registered MV");
    assert_eq!(floor.per_bucket, [(0, 4)].into_iter().collect());
    assert_eq!(floor.min_offset(), 4);
}
```

- [ ] **Step 2: Add the BUCK target**

In `src/control-plane/postgres/BUCK`, beside the `iceberg-gc` target:

```python
loom_fixture_test(
    name = "mv-floor",
    crate = "mv_floor",
    srcs = ["tests/mv_floor.rs"],
    crate_root = "tests/mv_floor.rs",
    deps = [
        "//src/testing:seed",
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        # The dropped-source test (Task 2) drops through the iceberg Catalog trait.
        "//third-party:iceberg",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:mv-floor`
Expected: BUILD FAILED — `unresolved import: control_plane_postgres::mv_floor` (the module does not exist yet).

- [ ] **Step 4: Add `pg_micro_batch_readers` to `transforms.rs`**

Put it near `pg_type_tables`, which it parallels. `de_body` is already `pub(crate)` in this module, and `TableRef`/`TransformBody` are already imported — but **`mv_key` is NOT**: add it to the file's `use control_plane_core::{…}` list. `TransformBody` has exactly four variants (`Physical`, `Typed`, `MicroBatch`, `MicroBatchJoin`), so the match below is exhaustive without a wildcard arm.

```rust
/// The `mv_key`s of every registered micro-batch MV (`MicroBatch` /
/// `MicroBatchJoin`) whose SOURCE is `table` — the registration half of the GC
/// floor's reader set (`crate::mv_floor`). An undecodable body is skipped with a
/// warning: a poisoned admin artifact must not wedge GC, exactly as it must not
/// fail unrelated ingest commits (`pg_fire_data_triggers`).
pub(crate) async fn pg_micro_batch_readers<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table: &TableRef,
) -> Result<std::collections::BTreeSet<String>> {
    let rows = sqlx::query!("select name, body from transforms.transform order by name")
        .fetch_all(ex)
        .await
        .map_err(backend)?;
    let mut out = std::collections::BTreeSet::new();
    for r in rows {
        let body = match de_body(r.body) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    transform = %r.name,
                    error = %e,
                    "mv floor: skipping undecodable transform body"
                );
                continue;
            }
        };
        let (source, output) = match &body {
            TransformBody::MicroBatch { source, output, .. }
            | TransformBody::MicroBatchJoin { source, output, .. } => (source, output),
            TransformBody::Physical { .. } | TransformBody::Typed { .. } => continue,
        };
        if source.schema == table.schema && source.name == table.name {
            out.insert(mv_key(output));
        }
    }
    Ok(out)
}
```

- [ ] **Step 5: Write `mv_floor.rs`**

```rust
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
                who = mv.clone();
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
```

If sqlx infers `select distinct mv` as `Option<String>`, force non-null with `select distinct mv as "mv!"`.

- [ ] **Step 6: Register the module**

`src/control-plane/postgres/src/lib.rs`: add `pub mod mv_floor;` alongside the other `pub mod` declarations.

- [ ] **Step 7: Regenerate the sqlx cache**

Run: `bash tools/sqlx-prepare.sh`
Expected: new `.sqlx/query-*.json` entries for the three new macro queries (`stream.mv_watermark` select, `select distinct mv`, `transforms.transform` select).

- [ ] **Step 8: Run the tests**

Run: `buck2 test --console none //src/control-plane/postgres:mv-floor`
Expected: `Tests finished: Pass 5. Fail 0.`

- [ ] **Step 9: Lint + commit**

```bash
git add src/control-plane/postgres/src/mv_floor.rs src/control-plane/postgres/src/lib.rs \
        src/control-plane/postgres/src/transforms.rs src/control-plane/postgres/tests/mv_floor.rs \
        src/control-plane/postgres/BUCK src/control-plane/postgres/.sqlx
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "feat(gc): per-bucket MV read-position floor for a source table"
```

---

## Task 2: Guard `gc_locked` with the floor

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_gc.rs` (`GcSummary`, `gc_locked`, `reclaimable_paths`, `delete_data_files`, `delete_end_capped_inline_rows`, new `count_candidates`)
- Modify: `src/control-plane/postgres/tests/iceberg_gc.rs` (the single `GcSummary { … }` literal, ~line 254)
- Modify: `src/control-plane/postgres/tests/mv_floor.rs` (append the GC tests)

**Interfaces:**
- Consumes: `crate::mv_floor::{MvFloor, mv_floor, stranded_mv_readers}` (Task 1).
- Produces: `GcSummary { data_file_rows: u64, inline_rows: u64, objects_deleted: u64, held_by_mv_floor: u64 }` — the new field counts age-eligible candidates (data-file rows + end-capped inline rows) of the LIVE incarnation that the floor held back.

- [ ] **Step 1: Write the failing GC tests**

Append to `src/control-plane/postgres/tests/mv_floor.rs`. Add these imports at the top: `std::time::Duration`; `control_plane_core::Catalog`; `control_plane_postgres::iceberg_catalog::IcebergCatalog`; `control_plane_postgres::iceberg_flush::flush_table`; `control_plane_postgres::iceberg_gc::gc_table`; `control_plane_postgres::iceberg_inline::inline_table_name`.

**Before writing `end_capped_inline_count`, read `iceberg_inline::inline_table_name` and use it** — do not hand-spell the physical table name.

```rust
const SEVEN_DAYS: Duration = Duration::from_secs(7 * 24 * 3600);

/// Backdate EVERY snapshot so the whole history is aged out (H = max snapshot id).
async fn age_all_snapshots(pool: &sqlx::PgPool) {
    let old = OffsetDateTime::now_utc() - time::Duration::days(365);
    sqlx::query("update iceberg_mirror.snapshot set snapshot_time = $1")
        .bind(old)
        .execute(pool)
        .await
        .expect("age all snapshots");
}

/// End-cap every live data file of `tid` at snapshot `snap`. This is the mirror
/// state EVERY file-retiring path leaves behind (compaction, a future small-file
/// merge, stream retention) — the class the floor exists to guard. It is done in
/// SQL because the real writers of that state live ABOVE this crate (they must
/// write replacement Parquet with DataFusion first).
async fn end_cap_data_files(pool: &sqlx::PgPool, tid: i64, snap: i64) {
    sqlx::query(
        "update iceberg_mirror.data_file set end_snapshot = $1 \
         where table_id = $2 and end_snapshot is null",
    )
    .bind(snap)
    .bind(tid)
    .execute(pool)
    .await
    .expect("end-cap data files");
}

/// End-capped (GC-candidate) rows still physically present in `inline_<tid>`.
async fn end_capped_inline_count(pool: &sqlx::PgPool, tid: i64) -> i64 {
    let inline = inline_table_name(tid);
    sqlx::query_scalar(&format!(
        "select count(*) from {inline} where end_snapshot is not null"
    ))
    .fetch_one(pool)
    .await
    .expect("count end-capped inline rows")
}

/// `iceberg_mirror.data_file` rows for `tid` (any `end_snapshot`).
async fn data_file_count(pool: &sqlx::PgPool, tid: i64) -> i64 {
    sqlx::query_scalar("select count(*) from iceberg_mirror.data_file where table_id = $1")
        .bind(tid)
        .fetch_one(pool)
        .await
        .expect("count data files")
}

/// The table's current snapshot id. NOTE `Snapshot::id` is the `SnapshotId(i64)`
/// newtype (`core/src/catalog.rs:16`) — unwrap it with `.0`.
async fn current_snapshot_id(pool: &sqlx::PgPool, table: &TableRef) -> i64 {
    IcebergCatalog::new(pool.clone())
        .current_snapshot(table)
        .await
        .expect("current snapshot")
        .id
        .0
}

// ---- GC under the floor ----------------------------------------------------

/// A lagging MV holds its source's unread tail: end-capped inline rows AT OR ABOVE
/// the MV's watermark survive GC, the ones below it are reclaimed, and
/// `held_by_mv_floor` counts the difference.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lagging_mv_holds_the_unread_tail() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // 6 inline events (offsets 0..6) in one bucket, one MV registered.
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

    // The MV has consumed offsets 0,1,2 (next_offset = 3): 3,4,5 are unread.
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 3).await;

    // Flush: the 6 rows land in a live Parquet file and their inline copies are
    // end-capped — the age-eligible candidates GC would otherwise take.
    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    age_all_snapshots(&s.pool).await;
    assert_eq!(
        end_capped_inline_count(&s.pool, s.tid).await,
        6,
        "all 6 inline rows are end-capped candidates"
    );

    let summary = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    assert_eq!(
        summary.inline_rows, 3,
        "only offsets 0,1,2 — strictly below the floor — are reclaimed"
    );
    assert_eq!(
        summary.held_by_mv_floor, 3,
        "offsets 3,4,5 are held for the lagging MV"
    );
    assert_eq!(
        end_capped_inline_count(&s.pool, s.tid).await,
        3,
        "the unread tail is physically still there"
    );
}

/// Once the MV catches up, the next GC reclaims what was held: the floor releases.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caught_up_mv_releases_the_tail() {
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
        &[("mv_a", "out_a")],
    )
    .await;
    let mv = mv_key(&tref("s", "out_a"));
    advance(&cp, &mv, s.tid, 0, 0, 3).await;
    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    age_all_snapshots(&s.pool).await;
    let first = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("first gc");
    assert_eq!(first.held_by_mv_floor, 3, "the tail is held while lagging");

    // The MV consumes the rest (3 -> 6).
    advance(&cp, &mv, s.tid, 0, 3, 6).await;

    let second = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("second gc");
    assert_eq!(second.inline_rows, 3, "the held tail is now reclaimed");
    assert_eq!(second.held_by_mv_floor, 0, "nothing is held any more");
    assert_eq!(
        end_capped_inline_count(&s.pool, s.tid).await,
        0,
        "no end-capped inline rows remain"
    );
}

/// A registered-but-never-run MV pins EVERYTHING — including end-capped FILES and
/// their Parquet, which is the tier a future compaction/retention path would eat.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrun_mv_pins_every_end_capped_row_and_file() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    // Land straight to FILES; register an MV and never run it.
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

    let files_before = data_file_count(&s.pool, s.tid).await;
    assert!(files_before > 0, "the source landed at least one file");
    let snap = current_snapshot_id(&s.pool, &s.src).await;
    end_cap_data_files(&s.pool, s.tid, snap).await;
    age_all_snapshots(&s.pool).await;

    let summary = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    assert_eq!(
        summary.data_file_rows, 0,
        "every file carries offsets the unrun MV has not read (floor 0): none reclaimed"
    );
    assert_eq!(summary.objects_deleted, 0, "no Parquet object deleted");
    assert_eq!(
        summary.held_by_mv_floor,
        u64::try_from(files_before).expect("count fits u64"),
        "every end-capped file is held by the floor"
    );
    assert_eq!(
        data_file_count(&s.pool, s.tid).await,
        files_before,
        "the mirror rows survive"
    );
}

/// Two MVs at different offsets: the SLOWER one bounds the reclaim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slowest_of_two_mvs_bounds_the_reclaim() {
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
        &[("mv_a", "out_a"), ("mv_b", "out_b")],
    )
    .await;
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 5).await;
    advance(&cp, &mv_key(&tref("s", "out_b")), s.tid, 0, 0, 2).await;

    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    age_all_snapshots(&s.pool).await;

    let summary = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    assert_eq!(
        summary.inline_rows, 2,
        "offsets 0,1 only — mv_b, at 2, is the laggard"
    );
    assert_eq!(summary.held_by_mv_floor, 4, "offsets 2..6 are held");
}

/// A table no MV reads GCs exactly as before the floor existed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_source_table_gcs_unchanged() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let s = seed_source(
        fx,
        &cp,
        &db,
        &wh.path().display().to_string(),
        6,
        None,
        true,
        &[],
    )
    .await;

    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    age_all_snapshots(&s.pool).await;

    let summary = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    assert_eq!(summary.inline_rows, 6, "every end-capped inline row reclaimed");
    assert_eq!(summary.held_by_mv_floor, 0, "no floor, nothing held");
    assert_eq!(end_capped_inline_count(&s.pool, s.tid).await, 0);
}

/// Dropping the source BYPASSES the floor: the dropped incarnation is fully
/// reclaimed even with a lagging MV registered (drop-GC must converge; the run
/// logs a warning naming the stranded MVs).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_source_bypasses_the_floor() {
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
    // The MV is barely started: offset 1 of 6 — a floor that would hold everything
    // above it if this were a live table.
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 1).await;
    let files_before = data_file_count(&s.pool, s.tid).await;
    assert!(files_before > 0, "the source landed at least one file");

    // Drop the source through the iceberg Catalog trait — the same call the
    // dropped-incarnation tests in `tests/iceberg_gc.rs:535` make. Needs
    // `use iceberg::{Catalog as _, NamespaceIdent, TableIdent};` at the top of this
    // file and `//third-party:iceberg` in the BUCK target (both already added).
    let ident = TableIdent::new(NamespaceIdent::new(s.src.schema.clone()), s.src.name.clone());
    s.catalog.drop_table(&ident).await.expect("drop source");
    // Age the whole history so the drop snapshot itself is past the horizon (full
    // reclaim of the dropped incarnation).
    age_all_snapshots(&s.pool).await;

    let summary = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("gc");
    assert!(
        summary.data_file_rows > 0,
        "the dropped incarnation's files are reclaimed — the floor is bypassed"
    );
    assert_eq!(
        summary.held_by_mv_floor, 0,
        "held counts the LIVE incarnation only; there is none"
    );
    assert_eq!(
        data_file_count(&s.pool, s.tid).await,
        0,
        "no data_file row of the dropped incarnation survives"
    );
}

// NOTE: the seventh test — `deleting_the_mv_registration_releases_the_floor` —
// belongs to Task 3 (it tests Task 3's behavior). Do NOT write it here: a task
// must never end on a red suite.
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 test --console none //src/control-plane/postgres:mv-floor`
Expected: BUILD FAILED — `struct GcSummary has no field named held_by_mv_floor`.

- [ ] **Step 3: Add the field to `GcSummary`**

`src/control-plane/postgres/src/iceberg_gc.rs`:

```rust
/// Counts of what a `gc_table` run reclaimed, for observability and tests.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcSummary {
    pub data_file_rows: u64,
    pub inline_rows: u64,
    pub objects_deleted: u64,
    /// How many age-eligible reclaim candidates of the LIVE incarnation the MV
    /// watermark floor held back — a lagging materialized view has not consumed
    /// those offsets yet. UNIT: `data_file` rows PLUS end-capped inline rows, summed
    /// (the same two tiers `data_file_rows` and `inline_rows` report separately), so
    /// read it as "candidates held", not as a row count or a byte count. Always 0
    /// for a table no micro-batch MV reads.
    pub held_by_mv_floor: u64,
}
```

Then add `held_by_mv_floor: 0` to the single `GcSummary { … }` literal in `tests/iceberg_gc.rs` (~line 254).

- [ ] **Step 4: Guard the selections and count what was held**

The `$3::bigint is null or …` shape means ONE query serves the guarded and unguarded paths, and a `None` guard reduces to the old predicate exactly:

```rust
/// Parquet paths of `tid`'s data files reclaimable at horizon `h`. `guard` is the
/// MV floor's cross-bucket minimum (`MvFloor::min_offset`): when set, a file is
/// reclaimable only if it carries a `loom_offset` max stat STRICTLY BELOW it.
///
/// A file with NO `loom_offset` stat is HELD — the scalar subquery yields NULL and
/// the comparison is NULL, so the row is not selected. That is the fail-safe
/// direction, and it is reachable: `declare_stream` may be applied to a table that
/// ALREADY has data files (`stream.rs:124-145`), and those pre-declaration files
/// carry no framing column and hence no offset stat. They are held for as long as
/// any MV reads the table — bounded, visible in `held_by_mv_floor`, never lossy.
///
/// The cast lives OUTSIDE the subquery on purpose: `max_value` is `text` holding
/// every column's bound (including string columns), and Postgres may reorder quals
/// inside one `WHERE`, so an inline `column_name = 'loom_offset' and max_value::bigint`
/// can blow up with `invalid input syntax for type bigint` on a table with a string
/// column. A subquery's own `WHERE` provably runs before its projection reaches the
/// outer cast.
async fn reclaimable_paths(
    pool: &PgPool,
    tid: i64,
    h: i64,
    guard: Option<i64>,
) -> Result<Vec<String>> {
    sqlx::query_scalar!(
        "select df.path from iceberg_mirror.data_file df \
         where df.table_id = $1 and df.end_snapshot is not null and df.end_snapshot <= $2 \
           and ($3::bigint is null or ( \
                 select cs.max_value from iceberg_mirror.data_file_column_stat cs \
                 where cs.data_file_id = df.data_file_id \
                   and cs.column_name = 'loom_offset')::bigint < $3::bigint)",
        tid,
        h,
        guard,
    )
    .fetch_all(pool)
    .await
    .map_err(backend)
}
```

(If sqlx infers `path` as nullable through the alias, write `select df.path as "path!"`. The `$n::bigint is null or …` shape with an `Option<i64>` bind under the compile-time macro is already proven in-tree — `postgres/src/lineage.rs:70-77` — including repeating the same `$n`.)

`delete_data_files` takes the same `guard` and repeats the identical clause in BOTH statements — the `data_file_column_stat` child delete's subselect and the `data_file` delete — so a held file never loses its stats:

```rust
async fn delete_data_files(
    conn: &mut sqlx::PgConnection,
    tid: i64,
    h: i64,
    guard: Option<i64>,
) -> Result<u64> {
    sqlx::query!(
        "delete from iceberg_mirror.data_file_column_stat \
         where data_file_id in ( \
             select df.data_file_id from iceberg_mirror.data_file df \
             where df.table_id = $1 and df.end_snapshot is not null and df.end_snapshot <= $2 \
               and ($3::bigint is null or ( \
                     select cs.max_value from iceberg_mirror.data_file_column_stat cs \
                     where cs.data_file_id = df.data_file_id \
                       and cs.column_name = 'loom_offset')::bigint < $3::bigint))",
        tid,
        h,
        guard,
    )
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    let rows = sqlx::query!(
        "delete from iceberg_mirror.data_file df \
         where df.table_id = $1 and df.end_snapshot is not null and df.end_snapshot <= $2 \
           and ($3::bigint is null or ( \
                 select cs.max_value from iceberg_mirror.data_file_column_stat cs \
                 where cs.data_file_id = df.data_file_id \
                   and cs.column_name = 'loom_offset')::bigint < $3::bigint)",
        tid,
        h,
        guard,
    )
    .execute(&mut *conn)
    .await
    .map_err(backend)?
    .rows_affected();
    Ok(rows)
}
```

The inline delete is per-bucket precise (rows carry `loom_bucket`/`loom_offset`). Its SQL is already dynamic (`AssertSqlSafe` + `inline_<tid>`), so build the predicate from the floor map — the bucket/offset literals come from our own mirror, never from user input, exactly as `mv_delta_locked` builds its watermark predicate:

```rust
/// Delete end-capped inline rows (`end_snapshot <= h`) from `inline_<tid>`, if the
/// physical table exists. With a floor, a row is reclaimable only strictly below
/// ITS bucket's floor; a bucket at floor 0 contributes no clause (nothing in it is
/// reclaimable); an unframed row (NULL bucket/offset — impossible on a stream
/// table) is held. Returns the number of rows deleted.
async fn delete_end_capped_inline_rows(
    conn: &mut sqlx::PgConnection,
    tid: i64,
    h: i64,
    floor: Option<&MvFloor>,
) -> Result<u64> {
    let inline = inline_table_name(tid);
    let exists: Option<String> = sqlx::query_scalar(AssertSqlSafe("select to_regclass($1)::text"))
        .bind(&inline)
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
    if exists.is_none() {
        return Ok(0);
    }
    let guard = match floor {
        None => String::new(),
        Some(f) => {
            let clauses: Vec<String> = f
                .per_bucket
                .iter()
                .filter(|(_, offset)| **offset > 0)
                .map(|(bucket, offset)| {
                    format!("(loom_bucket = {bucket} and loom_offset < {offset})")
                })
                .collect();
            if clauses.is_empty() {
                " and false".to_string()
            } else {
                format!(" and ({})", clauses.join(" or "))
            }
        }
    };
    let rows = sqlx::query(AssertSqlSafe(format!(
        "delete from {inline} where end_snapshot is not null and end_snapshot <= $1{guard}"
    )))
    .bind(h)
    .execute(&mut *conn)
    .await
    .map_err(backend)?
    .rows_affected();
    Ok(rows)
}

/// Age-eligible reclaim candidates for `tid` IGNORING the floor: data-file rows +
/// end-capped inline rows. The denominator of `held_by_mv_floor` (held = candidates
/// - actually deleted). Only called when a floor is active, so the fast path pays
/// nothing.
async fn count_candidates(conn: &mut sqlx::PgConnection, tid: i64, h: i64) -> Result<u64> {
    let files = sqlx::query_scalar!(
        "select count(*) as \"n!\" from iceberg_mirror.data_file \
         where table_id = $1 and end_snapshot is not null and end_snapshot <= $2",
        tid,
        h,
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(backend)?;
    let inline = inline_table_name(tid);
    let exists: Option<String> = sqlx::query_scalar(AssertSqlSafe("select to_regclass($1)::text"))
        .bind(&inline)
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
    let rows: i64 = if exists.is_some() {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "select count(*) from {inline} where end_snapshot is not null and end_snapshot <= $1"
        )))
        .bind(h)
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?
    } else {
        0
    };
    Ok(u64::try_from(files.saturating_add(rows)).unwrap_or(0))
}
```

- [ ] **Step 5: Thread the floor through `gc_locked`**

Import `crate::mv_floor::{MvFloor, mv_floor, stranded_mv_readers}` and `std::collections::BTreeSet`. After step 2 of `gc_locked` (resolving `live` / `dropped`):

```rust
    // 2b. The MV read-position floor of the LIVE incarnation. `None` for every
    //     table no micro-batch MV reads (the overwhelming majority): the guard
    //     below goes NULL and every predicate is byte-identical to the pre-floor
    //     behavior.
    let floor = match live {
        Some(tid) => mv_floor(pool, table, tid).await?,
        None => None,
    };
    let file_guard: Option<i64> = floor.as_ref().map(MvFloor::min_offset);

    // A dropped incarnation's reclaim deliberately BYPASSES the floor: the operator
    // dropped the source, so its MVs are dead by definition, and wedging drop-GC on
    // a dead MV forever is strictly worse than a loud, bounded strand.
    //
    // The warning must NOT misfire on drop-and-recreate. `stranded_mv_readers`'s
    // REGISTERED half keys on `(schema, name)` and cannot tell incarnations apart, so
    // an MV happily reading the NEW table would be named "stranded" on every GC run
    // while the old incarnations drain. Registered readers therefore count only when
    // there is no live incarnation; a reader with watermark rows against a DROPPED
    // tid is a real strand either way. Emit AFTER the tx, and only if something was
    // actually reclaimed — a warning on a no-op run is noise.
    let stranded = if dropped.is_empty() {
        BTreeSet::new()
    } else {
        let tids: Vec<i64> = dropped.iter().map(|d| d.table_id).collect();
        stranded_mv_readers(pool, table, &tids, live.is_none()).await?
    };
```

(`stranded_mv_readers`'s `include_registered` parameter is defined in Task 1 — pass `live.is_none()`.) The warning then fires after the commit, gated on an actual reclaim:

```rust
    if !stranded.is_empty() && data_file_rows > 0 {
        tracing::warn!(
            schema = %table.schema,
            name = %table.name,
            mvs = ?stranded,
            "gc: dropped source — full reclaim bypasses the MV watermark floor, \
             stranding these materialized views"
        );
    }
```

Pass the guard into the LIVE incarnation's selections only (`reclaimable_paths(pool, tid, h, file_guard)` in step 3; the deletes in step 4), leave the dropped loop's calls unguarded (`None`), and count the held:

```rust
    let mut held_by_mv_floor = 0u64;
    if let Some(tid) = live {
        let candidates = if floor.is_some() {
            count_candidates(&mut tx, tid, h).await?
        } else {
            0
        };
        let files = delete_data_files(&mut tx, tid, h, file_guard).await?;
        let inline = delete_end_capped_inline_rows(&mut tx, tid, h, floor.as_ref()).await?;
        data_file_rows += files;
        inline_rows += inline;
        held_by_mv_floor = candidates.saturating_sub(files.saturating_add(inline));
    }
```

After the commit, log the hold — the operator's lead to a wedged MV — and return the count in `GcSummary`:

```rust
    if held_by_mv_floor > 0 {
        if let Some(f) = &floor {
            tracing::warn!(
                schema = %table.schema,
                name = %table.name,
                held = held_by_mv_floor,
                floor = ?f.per_bucket,
                slowest = ?f.slowest,
                "gc: reclaim held by the MV watermark floor — a lagging materialized view \
                 has not consumed these offsets"
            );
        }
    }
```

**If `gc_locked` trips the complexity gate after this** (cc/cognitive > 15), extract the floor lookup + dropped warning into one helper and the live-incarnation delete block into another, rather than shipping the finding.

- [ ] **Step 6: Regenerate the sqlx cache**

Run: `bash tools/sqlx-prepare.sh`
Expected: the rewritten `query!`/`query_scalar!` calls replace their old cache entries; `count_candidates`'s count query is added.

- [ ] **Step 7: Run the tests**

Run: `buck2 test --console none //src/control-plane/postgres:mv-floor //src/control-plane/postgres:iceberg-gc //src/control-plane/postgres:sqlx-cache-check`
Expected: all green — `mv-floor` at `Pass 11. Fail 0.` and `iceberg-gc` (the pre-existing GC suite) green **unchanged**, which is the non-regression proof.

- [ ] **Step 8: Lint + commit**

```bash
git add -A
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "feat(gc): hold reclaim at the MV watermark floor"
```

---

## Task 3: Deleting an MV's registration releases its floor

Without this, a deleted MV's stale watermark rows pin the source's tail forever (the reader set unions watermark rows in). The spec names "deleting the MV registration" as the escape hatch — make it real, atomically.

**This is a `Transforms` TRAIT semantic change, so it lands in BOTH backends** (postgres adapter + `memory` fake) with a `testkit` contract test, or the shared contract suite certifies a divergence.

**Files:**
- Modify: `src/control-plane/postgres/src/transforms.rs` (`delete_transform`)
- Modify: `src/control-plane/memory/src/transforms.rs` (`delete_transform` — same semantic in the fake)
- Modify: `src/control-plane/testkit/src/lib.rs` (contract test, run against both backends)
- Modify: `src/control-plane/postgres/tests/mv_floor.rs` (the GC-level test below)

**Interfaces:**
- Consumes: `de_body`, `control_plane_core::mv_key`, `TransformBody`; in `memory`, the `mv_watermarks: Arc<Mutex<HashMap<(String, i64, i32), i64>>>` map (`memory/src/lib.rs:66`, `MvWatermarkMap`).
- Produces: no signature change (`Transforms::delete_transform` keeps its shape); the behavior change is that an MV def's watermark rows/entries are deleted with it, atomically.

- [ ] **Step 1: Write the failing test**

Append to `src/control-plane/postgres/tests/mv_floor.rs` (it uses only helpers Task 2 already added):

```rust
/// Deleting the MV's registration releases the floor — the documented escape hatch
/// out of a hold by a dead MV. Its watermark rows go with the def, so the held tail
/// becomes reclaimable on the next GC.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_the_mv_registration_releases_the_floor() {
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
        &[("mv_a", "out_a")],
    )
    .await;
    advance(&cp, &mv_key(&tref("s", "out_a")), s.tid, 0, 0, 3).await;
    flush_table(&s.catalog, &s.pool, &s.src, RunId(uuid::Uuid::new_v4()))
        .await
        .expect("flush");
    age_all_snapshots(&s.pool).await;
    let first = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("first gc");
    assert_eq!(first.held_by_mv_floor, 3, "held while the MV lags");

    cp.transforms()
        .delete_transform(&TransformName("mv_a".into()))
        .await
        .expect("delete the mv registration");

    let second = gc_table(&s.catalog, &s.pool, &s.src, SEVEN_DAYS)
        .await
        .expect("second gc");
    assert_eq!(second.inline_rows, 3, "the dead MV's hold is released");
    assert_eq!(second.held_by_mv_floor, 0, "no reader, no floor");
    assert_eq!(end_capped_inline_count(&s.pool, s.tid).await, 0);
}
```

Add the matching **contract test** in `src/control-plane/testkit/src/lib.rs`, following the shape of the existing `delete_transform` contract cases (~lines 5221, 5488): define a `MicroBatch` transform, `advance_mv_watermark` on its `mv_key(output)`, `delete_transform`, then assert `mv_watermarks(mv, source_table_id)` comes back EMPTY. Wire it into the contract list the same way its neighbors are, so both backends run it.

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `buck2 test --console none //src/control-plane/postgres:mv-floor //src/control-plane/...` (the testkit contract targets)
Expected: `deleting_the_mv_registration_releases_the_floor` FAILS (the second GC still holds 3 rows — stale watermark rows keep the floor at 3), and the new contract case fails against BOTH backends.

- [ ] **Step 3: Delete the watermark rows with the def (postgres)**

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn delete_transform(&self, name: &TransformName) -> Result<()> {
        // An MV's watermark rows are part of its registration: deleting the def is
        // the documented escape hatch out of a GC floor held by a dead MV
        // (`crate::mv_floor`), so the rows must go with it — atomically, or the
        // floor would outlive the registration that justified it. The delete is
        // keyed by `mv_key(output)` alone (not by source), which drops every cursor
        // this MV holds — correct, because the MV itself is going away.
        let mut tx = self.pool().begin().await.map_err(backend)?;
        let existing = sqlx::query!(
            "select body from transforms.transform where name = $1",
            name.0,
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(backend)?;
        if let Some(row) = existing {
            match de_body(row.body) {
                Ok(
                    TransformBody::MicroBatch { output, .. }
                    | TransformBody::MicroBatchJoin { output, .. },
                ) => {
                    sqlx::query!(
                        "delete from stream.mv_watermark where mv = $1",
                        mv_key(&output),
                    )
                    .execute(&mut *tx)
                    .await
                    .map_err(backend)?;
                }
                Ok(TransformBody::Physical { .. } | TransformBody::Typed { .. }) => {}
                Err(e) => tracing::warn!(
                    transform = %name.0,
                    error = %e,
                    "delete_transform: undecodable body; deleting the def without watermark cleanup"
                ),
            }
        }
        sqlx::query!("delete from transforms.transform where name = $1", name.0)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(())
    }
```

- [ ] **Step 4: Mirror the semantic in the `memory` fake**

`src/control-plane/memory/src/transforms.rs` — today `delete_transform` (lines 101-106) removes only `defs`/`next_run_at`. It must also drop the MV's watermark entries, so the fake and the adapter agree:

```rust
    async fn delete_transform(&self, name: &TransformName) -> Result<()> {
        // Same semantic as the postgres adapter: an MV's watermarks are part of its
        // registration and go with it (the GC floor's escape hatch — see
        // `control_plane_postgres::mv_floor`). Keep the two backends in step; the
        // testkit contract certifies it.
        let mut st = self.transforms.lock();
        let mv = st.defs.get(&name.0).and_then(|def| match &def.body {
            TransformBody::MicroBatch { output, .. } | TransformBody::MicroBatchJoin { output, .. } => {
                Some(mv_key(output))
            }
            TransformBody::Physical { .. } | TransformBody::Typed { .. } => None,
        });
        st.defs.remove(&name.0);
        st.next_run_at.remove(&name.0);
        drop(st);
        if let Some(mv) = mv {
            self.mv_watermarks.lock().retain(|(m, _, _), _| *m != mv);
        }
        Ok(())
    }
```

Mind the crate's **documented lock order** (see the `submit_run` comment right below this method): do not hold `transforms` while taking another lock — hence the explicit `drop(st)` before touching `mv_watermarks`. Add whatever imports (`TransformBody`, `mv_key`) the file lacks.

- [ ] **Step 5: Regenerate the cache and run the tests**

```bash
bash tools/sqlx-prepare.sh
buck2 test --console none //src/control-plane/postgres:mv-floor
```
Expected: `Tests finished: Pass 12. Fail 0.`

Then everything that exercises `delete_transform` — the postgres transform tests, the memory fake, and the testkit contract against both:
`buck2 test --console none //src/control-plane/... -j 8`
Expected: green.

- [ ] **Step 6: Lint + commit**

```bash
git add -A && buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "feat(transforms): deleting an MV def deletes its watermark rows"
```

---

## Task 4: Worker e2e — a lagging MV's end-capped file survives GC over the wire

The end-to-end proof, run against the real engine over the wire. **Read the "What this prevents" section at the top before writing it:** the *inline* tier proves nothing on its own (flush duplicates those rows into a live file first, so reclaiming the end-capped inline copies loses nothing, and the MV never reads end-capped rows anyway). The tier this guard actually protects is the **file** tier — an end-capped Parquet file whose bytes a future retention/compaction path would reclaim. So the e2e end-caps a FILE and asserts the bytes survive; it does NOT claim "the delta had no hole", which would be true with or without this branch.

**Files:**
- Modify: `src/services/worker/tests/stream_mv_e2e.rs` — ONE new `#[tokio::test]`, reusing that file's existing helpers (`tref`, `events_columns`, `events_batch`, `seed_lineage`, `build_ctx`, `run_micro_batch`, `make_stream_mv_job`, `doubled_rows`). No new test file and no new BUCK target: a sibling file would mean copying ~140 lines of helpers into the same crate, which trips this branch's own duplication gate.
- The existing `//src/services/worker:stream-mv-e2e` target already deps `//src/control-plane/postgres`, `engine-wire`, `sqlx`, `//src/testing:flight` — everything the new test needs. Verify before adding anything.

**Interfaces:**
- Consumes: `engine_wire::client::GrpcQueueClient::gc_table(schema: String, name: String) -> Result<(u64, u64, u64)>`; `control_plane_postgres::iceberg_flush::flush_table(&SqlCatalog, &PgPool, &TableRef, RunId)`; `control_plane_postgres::iceberg_mirror::live_table_id`; `Transforms::define_transform`.
- Produces: nothing (test-only).

- [ ] **Step 1: Write the e2e test**

Append to `src/services/worker/tests/stream_mv_e2e.rs`. Add the imports it needs (`flush_table`, `live_table_id`, `TransformDef`, `TransformName`, `GrpcQueueClient` is already there) and the two SQL helpers (copy them from `postgres/tests/mv_floor.rs` — they are 6 lines each and there is no shared test-support library on this side; if the duplication gate flags them, that is the moment to lift a worker-side `e2e-support` library, not before).

```rust
/// Backdate EVERY snapshot so the whole history is aged out of the GC window.
async fn age_all_snapshots(pool: &sqlx::PgPool) {
    let old = time::OffsetDateTime::now_utc() - time::Duration::days(365);
    sqlx::query("update iceberg_mirror.snapshot set snapshot_time = $1")
        .bind(old)
        .execute(pool)
        .await
        .expect("age all snapshots");
}

/// End-cap every live data file of `tid` at snapshot `snap` — the mirror state any
/// file-retiring path (compaction, a future small-file merge, stream retention)
/// leaves behind. This is the class the MV floor guards: without it, GC reclaims
/// these files' rows and Parquet outright.
async fn end_cap_data_files(pool: &sqlx::PgPool, tid: i64, snap: i64) {
    sqlx::query(
        "update iceberg_mirror.data_file set end_snapshot = $1 \
         where table_id = $2 and end_snapshot is null",
    )
    .bind(snap)
    .bind(tid)
    .execute(pool)
    .await
    .expect("end-cap data files");
}

/// `iceberg_mirror.data_file` rows for `tid` (any `end_snapshot`).
async fn data_file_count(pool: &sqlx::PgPool, tid: i64) -> i64 {
    sqlx::query_scalar("select count(*) from iceberg_mirror.data_file where table_id = $1")
        .bind(tid)
        .fetch_one(pool)
        .await
        .expect("count data files")
}

/// GC's MV watermark floor, end to end over the engine wire
/// (road-mv-watermark-aware-gc): a source whose micro-batch MV is behind keeps the
/// BYTES of its end-capped tail — the file tier a future retention/compaction path
/// would reclaim — until the MV catches up, at which point GC converges.
///
/// Scope, stated honestly: this proves BYTE RETENTION, not hole prevention. GC only
/// ever reclaims end-capped rows, which an MV delta (a current-snapshot read) cannot
/// see in the first place — so no GC-tier guard can keep an MV's delta complete. The
/// hole is created at END-CAP time, and closing that is the follow-up item this
/// branch files.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_holds_a_lagging_mvs_end_capped_files_and_converges_on_catch_up() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let wh = tempfile::tempdir().expect("warehouse dir");
    let wh_str = wh.path().display().to_string();
    let catalog = local_sql_catalog(fx.pg_dsn(&db), &wh_str).await;

    let eng = spawn_engine_uds(
        fx,
        &db,
        &wh_str,
        EngineOpts {
            control: true,
            flight: true,
            ..EngineOpts::default()
        },
    )
    .await;
    let ctx = build_ctx(&eng.sock).await;
    let engine = GrpcQueueClient::connect(&eng.sock)
        .await
        .expect("connect control");

    let src = tref("s", "events");
    let out = tref("s", "doubled");
    let sql = "select id, val * 2 as dbl from events";

    // 1. REGISTER the MV (no data trigger: this test drives its runs by hand, so the
    //    floor comes from the registration plus the watermarks those runs commit).
    cp.transforms()
        .define_transform(TransformDef {
            name: TransformName("mv_doubled".into()),
            body: TransformBody::MicroBatch {
                source: src.clone(),
                output: out.clone(),
                buckets: 1,
                sql: sql.to_string(),
            },
            schedule: None,
            on_input_commit: false,
        })
        .await
        .expect("register mv");

    // 2. Seed 6 events into a 1-bucket log stream, landed straight to PARQUET
    //    (inline_byte_limit: 0) — the file tier is what this test is about.
    let (schema, batches) = events_batch(&[1, 2, 3, 4, 5, 6], &[10, 20, 30, 40, 50, 60]);
    land(
        &pool,
        &catalog,
        &src,
        &events_columns(),
        schema,
        batches,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        seed_lineage(&src),
        Some(1),
    )
    .await
    .expect("land events");

    // 3. Micro-batch #1: the MV consumes offsets 0,1,2 only — it is now LAGGING.
    //    (Its SQL reads the whole source, but the watermark is what the floor uses;
    //    advance it explicitly to 3 to model a run that stopped short.)
    let (_, r1) = run_micro_batch(&cp, &ctx, &src, &out, 1, sql).await;
    r1.expect("first micro-batch");

    let mut conn = pool.acquire().await.expect("conn");
    let tid = live_table_id(&mut conn, &src.schema, &src.name)
        .await
        .expect("tid")
        .expect("live tid");
    drop(conn);

    // Roll the watermark BACK to 3 to model a lagging reader: the run above consumed
    // all 6, so rewrite its cursor to the mid-point. (A watermark is per (mv, source,
    // bucket); this is the same table `pg_advance_mv_watermark` maintains.)
    sqlx::query("update stream.mv_watermark set next_offset = 3 where source_table_id = $1")
        .bind(tid)
        .execute(&pool)
        .await
        .expect("rewind the mv watermark to model a lagging reader");

    // 4. A file-retiring path runs (compaction / retention): the live Parquet file is
    //    end-capped. Age the history so it is GC-eligible.
    let snap = IcebergCatalog::new(pool.clone())
        .current_snapshot(&src)
        .await
        .expect("current snapshot")
        .id
        .0;
    let files_before = data_file_count(&pool, tid).await;
    assert!(files_before > 0, "the source landed at least one file");
    end_cap_data_files(&pool, tid, snap).await;
    age_all_snapshots(&pool).await;

    // 5. GC over the wire. The floor (next_offset = 3) covers offsets 3,4,5, which
    //    live in that file — so the file is HELD, bytes and mirror row alike.
    engine
        .gc_table(src.schema.clone(), src.name.clone())
        .await
        .expect("gc over the wire");
    assert_eq!(
        data_file_count(&pool, tid).await,
        files_before,
        "the lagging MV's unread offsets keep the end-capped file alive"
    );

    // 6. The MV catches up (watermark past the tail) — the floor releases.
    sqlx::query("update stream.mv_watermark set next_offset = 6 where source_table_id = $1")
        .bind(tid)
        .execute(&pool)
        .await
        .expect("advance the mv watermark past the tail");

    age_all_snapshots(&pool).await;
    engine
        .gc_table(src.schema.clone(), src.name.clone())
        .await
        .expect("second gc");
    assert_eq!(
        data_file_count(&pool, tid).await,
        0,
        "with the MV caught up, GC converges and reclaims the end-capped file"
    );
}
```

**Implementer's note on step 3/6:** the direct `update stream.mv_watermark` is deliberate — `pg_advance_mv_watermark` is a CAS that only moves a watermark FORWARD, and this test needs to model a reader that is *behind*, which no public API can produce after a run that consumed everything. If you can instead land the 6 events in two batches and run the micro-batch only over the first (as `stream_mv_e2e`'s own multi-batch test does), prefer that — it uses the real API end to end. Try that first; fall back to the direct update only if the framing makes it impractical, and say which you did in the PR.

- [ ] **Step 2: Run it**

Run: `buck2 test --console none //src/services/worker:stream-mv-e2e`
Expected: every test in the file passes, including the new one.

- [ ] **Step 3: Lint + commit**

```bash
git add -A && buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "test(worker): e2e — GC holds a lagging MV's end-capped files"
```

---


## Task 5: Docs — capability, register close, and FILING THE REAL FIX

**Files:**
- Modify: `docs/system-capabilities/control-plane.md` (the GC section: the floor invariant, the reader set, the escape hatch, the dropped-source bypass) and whichever capability file documents micro-batch MVs / the watermark (check `docs/system-capabilities/README.md` — likely `transform.md` or `stream.md`).
- Modify: `docs/ROADMAP.md` — remove the `road-mv-watermark-aware-gc` entry (registers carry OPEN work only).
- Modify: `docs/ROADMAP.md` / `docs/ISSUES.md` — ADD the follow-up items below.

- [ ] **Step 1: Run the docs skill**

Use `loom-docs-update`: close `road-mv-watermark-aware-gc` (remove the entry; name the id + PR in the PR body) and fold the landed capability into `docs/system-capabilities/`.

**Describe the capability honestly** — the GC-tier floor is byte-retention defense plus a reusable primitive; it does NOT prevent an MV hole, because GC only reclaims end-capped rows and an MV delta reads live rows at the current snapshot. Do not write "prevents MV data loss" anywhere.

- [ ] **Step 2: File the follow-ups this work uncovered**

These are the deliverable's most valuable output — the analysis behind them is in the "What this prevents" section at the top of this plan. Each needs a real entry (id, area, from, prose), not a one-liner:

1. **The real fix — end-cap paths must consult the MV floor** (ROADMAP or ISSUES; the spec's mechanism does not achieve its own acceptance criterion without it). The C7 harm is created when a path *end-caps* rows an MV has not read (drop today; CDC changelog retention, stream small-file compaction, and any truncation/replay surface tomorrow) — those rows vanish from the MV's current-snapshot delta immediately, and no GC-tier guard can bring them back. The end-cap-issuing paths (`iceberg_compact::compact_table`, the catalog drop, future retention) must call `mv_floor` BEFORE end-capping and refuse/defer rows at or above the floor. Cross-link `[[road-mv-watermark-aware-gc]]` (this work — which ships the `mv_floor` primitive those paths call) and `[[fut-mv-cdc-source]]`.
2. **An MV registered against an already-reclaimed source starts with a hole** (ISSUES). A newly registered MV floors at 0, but offsets below the previous floor may already be gone; today that is harmless (only redundant inline copies are reclaimable), but it becomes a silent short first delta the moment a lossy end-capping path lands. Registration should validate the source's surviving offset range, or bootstrap the watermark to it.
3. **Pre-declaration files are unreclaimable once an MV registers** (ISSUES, minor). `declare_stream` can be applied to a table that already has data files (`stream.rs:124-145`); those files carry no `loom_offset` stat, so the fail-safe guard holds them forever and they inflate `held_by_mv_floor` with no laggard to blame.

Existing deferrals stay as they are: the max-hold override knob (per the spec) and `[[fut-stream-consumer-offsets]]` (already carries the subscriber-cursor floor this mechanism was built to union in).

- [ ] **Step 3: Commit**

```bash
git add -A && buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "docs(gc): document the MV watermark floor — close road-mv-watermark-aware-gc"
```

---

## Final verification (before the PR)

- [ ] `buck2 build -v0 --console none //src/...` — clean.
- [ ] `buck2 test --console none //src/control-plane/... -j 8` — green (includes `sqlx-cache-check`, the untouched `iceberg-gc` suite, `mv-floor`, and the testkit contract against BOTH backends).
- [ ] `buck2 test --console none //src/services/... -j 8` — green.
- [ ] `buck2 run //tools:prek -- run --all-files` — clean.
- [ ] Metric gate: `loom-complexity diff` and `loom-duplication diff` — no NEW hotspot over the census thresholds (cc > 15, cognitive > 15, MI < 20, SLOC > 100) and no NEW cross-file duplication pair >= 20 lines. `gc_locked` grows in this branch: if it trips the threshold, extract helpers (Task 2, Step 5) rather than shipping the finding. The GC tests share one seed helper, and the e2e lives in the existing test file, for the same reason.

## Acceptance — what this branch actually proves

The spec's Acceptance 1 ("a lagging MV's unread source offsets survive GC") is met **in the byte-retention sense only**; see "What this prevents" at the top. Restated to what the tests genuinely demonstrate:

1. **A lagging MV's end-capped bytes survive GC** — file tier: `unrun_mv_pins_every_end_capped_row_and_file`, `gc_holds_a_lagging_mvs_end_capped_files_and_converges_on_catch_up` (e2e, over the wire). Inline tier: `lagging_mv_holds_the_unread_tail`, `slowest_of_two_mvs_bounds_the_reclaim`. Release on catch-up: `caught_up_mv_releases_the_tail`.
2. **Non-MV-source tables GC byte-identically** — `non_source_table_gcs_unchanged`, plus the pre-existing `iceberg-gc` suite green unchanged.
3. **Held reclaim is observable** — `GcSummary.held_by_mv_floor` + the `tracing::warn!` naming the per-bucket laggard; the dropped-source bypass warns and names the stranded MVs (and does NOT misfire on drop-and-recreate).
4. **The escape hatch is real** — `deleting_the_mv_registration_releases_the_floor`, certified on both backends by the testkit contract.
5. **Existing suites green** — the final verification block above.
6. **The gap the spec missed is on the register, not swept under it** — Task 5, Step 2.
