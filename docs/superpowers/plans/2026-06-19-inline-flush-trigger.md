# Inline Flush Trigger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a large-enough accumulation of inline writes automatically enqueue a `flush_table` job, so a worker (Spec 2) can compact the table.

**Architecture:** A per-table `iceberg_mirror.inline_trigger` row tracks live inline bytes since the last flush. `inline_append` bumps it inside its own transaction and, on crossing a per-table-or-global byte threshold, enqueues one `flush_table` job (debounced by an `enqueued` flag) via the existing transactional `pg_insert`. `flush_table` resets the row on completion. This spec ships **only the producer**; the consumer is Spec 2.

**Tech Stack:** Rust 2024, buck2, sqlx 0.9 compile-time `query!` + committed `.sqlx`, Postgres, arrow-57 (`RecordBatch`), `loom_fixture_test` fixtures.

## Global Constraints

- **No inline `#[cfg(test)]`** — unit tests are `rust_test`/`loom_fixture_test` targets in `tests/<name>.rs`; a prek hook fails on `#[test]` in `src/**`.
- **Fixture tests use `loom_fixture_test`** (`src/control-plane/postgres/defs.bzl`), not bare `rust_test`, or they route to RE and fail as root.
- **Compile-time SQL needs the `.sqlx` cache.** Any new `query!`/`query_scalar!` against the schema requires running `tools/sqlx-prepare.sh` (boots hermetic Postgres, applies migrations incl. the new one, attaches DuckLake, runs `cargo sqlx prepare`) and committing `src/control-plane/postgres/.sqlx`. The crate will not compile offline until the cache exists. So in a task that adds `query!`, the order is: write migration → write the `query!` code → `tools/sqlx-prepare.sh` → build/test.
- **Don't pipe `buck2 test` through `tail`/`head`** — redirect to a file and grep: `buck2 test //x > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **rustfmt is check-only** — run the formatter before committing (`buck2 run //tools:rustfmt -- <files>` or `cargo fmt` via `eval "$(./tools/env.sh)"`); the prek hook fails on a diff but does not rewrite.
- **Markdown** (the roadmap doc) — exactly one trailing newline, no trailing whitespace.
- **Commit messages** are Conventional Commits and end with the trailer:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
- **Branch:** `spec/inline-flush-trigger` (already created off `main`; the design docs are committed on it).

---

### Task 1: `inline_trigger` schema + mirror helpers

Create the per-table trigger table and the three compile-time-SQL helpers that
operate on it. Self-contained: testable by calling the helpers against a fixture
Postgres and asserting row state.

**Files:**
- Create: `src/control-plane/postgres/migrations/0015_inline_trigger.sql`
- Modify: `src/control-plane/postgres/src/iceberg_mirror.rs` (add helpers + `TriggerState`)
- Create: `src/control-plane/postgres/tests/inline_trigger_helpers.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test` target)

**Interfaces:**
- Produces:
  - `pub struct TriggerState { pub live_bytes: i64, pub effective: i64, pub enqueued: bool }`
  - `pub async fn bump_inline_trigger(conn: &mut sqlx::PgConnection, table_id: i64, add_bytes: i64, global_threshold: i64) -> control_plane_core::Result<TriggerState>`
  - `pub async fn arm_inline_trigger(conn: &mut sqlx::PgConnection, table_id: i64) -> control_plane_core::Result<()>`
  - `pub async fn reset_inline_trigger(conn: &mut sqlx::PgConnection, table_id: i64) -> control_plane_core::Result<()>`

- [ ] **Step 1: Write the migration**

Create `src/control-plane/postgres/migrations/0015_inline_trigger.sql`:

```sql
-- Per-table inline-flush trigger state, keyed by the STABLE table_id (the same
-- id that names iceberg_mirror.inline_<table_id>), NOT the MVCC-versioned
-- iceberg_mirror.table rows. live_bytes accumulates the in-memory size of live
-- inline rows since the last flush; threshold is an optional per-table override
-- of the global LOOM_FLUSH_BYTE_THRESHOLD; enqueued debounces to one pending
-- flush job per table. See docs/superpowers/specs/2026-06-19-inline-flush-trigger-design.md.
create table iceberg_mirror.inline_trigger (
    table_id   bigint  primary key,
    live_bytes bigint  not null default 0,
    threshold  bigint,
    enqueued   boolean not null default false
);
```

- [ ] **Step 2: Add the helpers to `iceberg_mirror.rs`**

Append to `src/control-plane/postgres/src/iceberg_mirror.rs` (uses the module's
existing `backend`, `Result`, compile-time `query!` style):

```rust
/// The trigger row after a bump: the new running total, the effective threshold
/// (per-table override or the passed global default), and whether a flush job is
/// already pending for this table.
#[derive(Debug, Clone, Copy)]
pub struct TriggerState {
    pub live_bytes: i64,
    pub effective: i64,
    pub enqueued: bool,
}

/// Add `add_bytes` to the table's live-inline-bytes counter (creating the row on
/// first write), returning the post-bump state plus the effective threshold
/// (`COALESCE(threshold, global_threshold)`). Idempotent row creation via upsert.
pub async fn bump_inline_trigger(
    conn: &mut PgConnection,
    table_id: i64,
    add_bytes: i64,
    global_threshold: i64,
) -> Result<TriggerState> {
    let row = sqlx::query!(
        "insert into iceberg_mirror.inline_trigger (table_id, live_bytes) \
         values ($1, $2) \
         on conflict (table_id) do update \
           set live_bytes = iceberg_mirror.inline_trigger.live_bytes + excluded.live_bytes \
         returning live_bytes as \"live_bytes!\", \
                   coalesce(threshold, $3) as \"effective!\", \
                   enqueued as \"enqueued!\"",
        table_id,
        add_bytes,
        global_threshold,
    )
    .fetch_one(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(TriggerState {
        live_bytes: row.live_bytes,
        effective: row.effective,
        enqueued: row.enqueued,
    })
}

/// Mark the table's trigger as having a pending flush job (debounce). No-op if the
/// row is absent (it is always created by a preceding `bump_inline_trigger`).
pub async fn arm_inline_trigger(conn: &mut PgConnection, table_id: i64) -> Result<()> {
    sqlx::query!(
        "update iceberg_mirror.inline_trigger set enqueued = true where table_id = $1",
        table_id,
    )
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(())
}

/// Reset the table's trigger after a flush: clear the counter and re-arm. No-op if
/// the row is absent.
pub async fn reset_inline_trigger(conn: &mut PgConnection, table_id: i64) -> Result<()> {
    sqlx::query!(
        "update iceberg_mirror.inline_trigger \
         set live_bytes = 0, enqueued = false where table_id = $1",
        table_id,
    )
    .execute(&mut *conn)
    .await
    .map_err(backend)?;
    Ok(())
}
```

- [ ] **Step 3: Regenerate the `.sqlx` cache**

The three new `query!`s won't compile offline until the cache exists and the
migration is applied. Run:

```bash
tools/sqlx-prepare.sh
```

Expected: it boots Postgres, applies migrations (incl. `0015`), and writes new
`src/control-plane/postgres/.sqlx/query-*.json` files. Stage them with the code.

- [ ] **Step 4: Write the helper test**

Create `src/control-plane/postgres/tests/inline_trigger_helpers.rs`:

```rust
//! inline_trigger helpers: bump accrues + reports the effective threshold; arm
//! and reset flip the flag/counter. loom_fixture_test (hermetic Postgres).

use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_mirror::{
    arm_inline_trigger, bump_inline_trigger, reset_inline_trigger,
};

// Fixture API (verified against tests/iceberg_flush.rs): `PgFixture::start()` is
// NOT async; `fresh_db()` and `pool_for()` are.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bump_accrues_and_reports_effective_threshold() {
    let fx = PgFixture::start();
    let (_, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let mut conn = pool.acquire().await.unwrap();
    let tid = 4242_i64;

    // First bump creates the row; effective falls back to the global default.
    let s1 = bump_inline_trigger(&mut conn, tid, 100, 1_000).await.unwrap();
    assert_eq!(s1.live_bytes, 100);
    assert_eq!(s1.effective, 1_000);
    assert!(!s1.enqueued);

    // Second bump accumulates.
    let s2 = bump_inline_trigger(&mut conn, tid, 250, 1_000).await.unwrap();
    assert_eq!(s2.live_bytes, 350);

    // Arm sets the flag; the next bump reports it.
    arm_inline_trigger(&mut conn, tid).await.unwrap();
    let s3 = bump_inline_trigger(&mut conn, tid, 1, 1_000).await.unwrap();
    assert!(s3.enqueued);

    // Reset clears counter + flag.
    reset_inline_trigger(&mut conn, tid).await.unwrap();
    let s4 = bump_inline_trigger(&mut conn, tid, 5, 1_000).await.unwrap();
    assert_eq!(s4.live_bytes, 5);
    assert!(!s4.enqueued);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_table_threshold_overrides_global() {
    let fx = PgFixture::start();
    let (_, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let mut conn = pool.acquire().await.unwrap();
    let tid = 99_i64;
    bump_inline_trigger(&mut conn, tid, 1, 10_000).await.unwrap();
    // Set a per-table override below the global.
    sqlx::query("update iceberg_mirror.inline_trigger set threshold = 50 where table_id = $1")
        .bind(tid)
        .execute(&mut *conn)
        .await
        .unwrap();
    let s = bump_inline_trigger(&mut conn, tid, 1, 10_000).await.unwrap();
    assert_eq!(s.effective, 50, "per-table threshold must beat the global");
}
```

> Note on test design: these assert the helper contract directly (state in,
> state out) against a real schema — the unit boundary is the SQL, so the test
> exercises exactly that. The fixture init (`PgFixture::start()` sync, then
> `fresh_db().await` → `(_, db)`, then `pool_for(&db).await`) is verified against
> `tests/iceberg_flush.rs:66-70`.

- [ ] **Step 5: Wire the BUCK target**

In `src/control-plane/postgres/BUCK`, add a `loom_fixture_test` mirroring the
existing `iceberg-flush` target (same deps), named `inline-trigger-helpers`,
`srcs = ["tests/inline_trigger_helpers.rs"]`. Copy the `iceberg-flush` target's
`deps`/`named_deps` verbatim (it already pulls the postgres crate, core, tokio,
the fixture, etc.).

- [ ] **Step 6: Format, build, test**

```bash
eval "$(./tools/env.sh)" && cargo fmt -p control-plane-postgres
buck2 test //src/control-plane/postgres:inline-trigger-helpers > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: `Tests finished: Pass 2. Fail 0.`

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/postgres/migrations/0015_inline_trigger.sql \
        src/control-plane/postgres/src/iceberg_mirror.rs \
        src/control-plane/postgres/tests/inline_trigger_helpers.rs \
        src/control-plane/postgres/BUCK \
        src/control-plane/postgres/.sqlx
git commit -m "feat(iceberg): inline_trigger table + bump/arm/reset mirror helpers" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: enqueue a flush job on threshold crossing

Add the opt-in trigger param to `inline_append` and the enqueue logic. Existing
callers pass `None` (behaviour-preserving); the trigger path is exercised by a
new test calling `Some(threshold)` directly.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_flush.rs` (add `FLUSH_JOB_KIND` + `FlushJob`)
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (`inline_append` signature + trigger block)
- Modify call sites to pass `None`: `src/control-plane/postgres/src/iceberg_landing.rs:67`, `src/control-plane/postgres/src/fixture.rs:659`, `src/control-plane/postgres/tests/iceberg_flush.rs` (5 calls), `src/control-plane/postgres/tests/iceberg_inline.rs` (1 call)
- Create: `src/control-plane/postgres/tests/inline_flush_trigger.rs`
- Modify: `src/control-plane/postgres/BUCK` (new `loom_fixture_test` target)

**Interfaces:**
- Consumes: `bump_inline_trigger`, `arm_inline_trigger` (Task 1); `crate::queue::pg_insert` (existing).
- Produces:
  - `pub const FLUSH_JOB_KIND: &str = "flush_table";`
  - `#[derive(serde::Serialize, serde::Deserialize, Debug)] pub struct FlushJob { pub schema: String, pub name: String }`
  - `inline_append(pool, table, columns, batch, lineage, flush_threshold: Option<i64>) -> Result<SnapshotId>` (new trailing param)

- [ ] **Step 1: Add the job contract to `iceberg_flush.rs`**

Near the top of `src/control-plane/postgres/src/iceberg_flush.rs`, add (Spec 2's
worker will deserialize `FlushJob`; defining it here makes it the shared
contract):

```rust
/// The queue `kind` for an inline-flush job.
pub const FLUSH_JOB_KIND: &str = "flush_table";

/// The payload of a `flush_table` job: which table to flush. Produced by the
/// trigger (`inline_append`), consumed by the flush worker (Spec 2).
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct FlushJob {
    pub schema: String,
    pub name: String,
}
```

(Confirm `serde` is a dep of the postgres crate's BUCK target — it is used by
`iceberg_flush`'s `compaction_event` via `serde_json::json!`; add `serde` with
`derive` if the bare crate isn't already a dep.)

- [ ] **Step 2: Write the failing trigger test**

Create `src/control-plane/postgres/tests/inline_flush_trigger.rs`. Model the
fixture/table/columns/batch setup on `tests/iceberg_flush.rs` (which already
calls `inline_append` and builds arrow-57 batches). Helper sketch + the four
cases:

```rust
//! Inline-flush triggering: inline_append accrues bytes and enqueues one
//! flush_table job on crossing the threshold, debounced. loom_fixture_test.
//! Constructors (RunId, ColumnSpec, LineageEvent, fixture) verified against
//! tests/iceberg_flush.rs.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, DatasetId, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_flush::FLUSH_JOB_KIND;
use control_plane_postgres::iceberg_inline::inline_append;

fn one_long_col() -> Vec<ColumnSpec> {
    vec![ColumnSpec { name: "n".into(), ty: "long".into(), nullable: true }]
}

fn batch(values: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
}

fn lineage(run: RunId, table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "test" }),
    }
}

async fn job_count(pool: &sqlx::PgPool, kind: &str) -> i64 {
    sqlx::query_scalar::<_, i64>("select count(*) from queue.jobs where kind = $1")
        .bind(kind)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sub_threshold_enqueues_nothing() {
    let fx = PgFixture::start();
    let (_, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef { schema: "wh".into(), name: "t".into() };
    let run = RunId(uuid::Uuid::new_v4());
    // A huge threshold: one small batch can never cross it.
    inline_append(&pool, &table, &one_long_col(), &batch(&[1, 2, 3]), lineage(run, &table), Some(1 << 40))
        .await
        .unwrap();
    assert_eq!(job_count(&pool, FLUSH_JOB_KIND).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crossing_enqueues_exactly_one_with_payload() {
    let fx = PgFixture::start();
    let (_, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef { schema: "wh".into(), name: "t".into() };
    let run = RunId(uuid::Uuid::new_v4());
    // A tiny threshold: the first batch crosses it.
    inline_append(&pool, &table, &one_long_col(), &batch(&[1, 2, 3]), lineage(run, &table), Some(1))
        .await
        .unwrap();
    assert_eq!(job_count(&pool, FLUSH_JOB_KIND).await, 1);
    let payload: serde_json::Value =
        sqlx::query_scalar("select payload from queue.jobs where kind = $1")
            .bind(FLUSH_JOB_KIND)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(payload["schema"], "wh");
    assert_eq!(payload["name"], "t");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crossing_twice_is_debounced_to_one_job() {
    let fx = PgFixture::start();
    let (_, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef { schema: "wh".into(), name: "t".into() };
    for _ in 0..2 {
        let run = RunId(uuid::Uuid::new_v4());
        inline_append(&pool, &table, &one_long_col(), &batch(&[9]), lineage(run, &table), Some(1))
            .await
            .unwrap();
    }
    assert_eq!(job_count(&pool, FLUSH_JOB_KIND).await, 1, "debounced");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn none_threshold_never_enqueues() {
    let fx = PgFixture::start();
    let (_, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef { schema: "wh".into(), name: "t".into() };
    let run = RunId(uuid::Uuid::new_v4());
    inline_append(&pool, &table, &one_long_col(), &batch(&[1]), lineage(run, &table), None)
        .await
        .unwrap();
    assert_eq!(job_count(&pool, FLUSH_JOB_KIND).await, 0);
}
```

> Atomicity-on-rollback (spec test 5) is guaranteed **by construction** — the
> `pg_insert` enqueue and the `arm` run on the same `conn`/transaction as the
> row inserts (`inline_append`'s single `tx`), so a rollback drops all three
> together. There is no fault-injection seam to force a post-enqueue failure, so
> no separate test is added; the same-tx structure is the proof. Note this in the
> task's self-review.

- [ ] **Step 3: Run the test to verify it fails**

```bash
buck2 test //src/control-plane/postgres:inline-flush-trigger > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t.log
```

Expected: FAILS to compile — `inline_append` takes 5 args, not 6 (the new param
doesn't exist yet).

- [ ] **Step 4: Add the trigger param + block to `inline_append`**

In `src/control-plane/postgres/src/iceberg_inline.rs`:

Add imports near the top:

```rust
use control_plane_core::NewJob;
use crate::iceberg_mirror::{arm_inline_trigger, bump_inline_trigger};
```

Change the signature (currently ends `lineage: LineageEvent,`):

```rust
pub async fn inline_append(
    pool: &PgPool,
    table: &TableRef,
    columns: &[ColumnSpec],
    batch: &RecordBatch,
    lineage: LineageEvent,
    flush_threshold: Option<i64>,
) -> Result<SnapshotId> {
```

Insert the trigger block **after** the row-insert loop (after the `for row in
0..batch.num_rows()` loop, before `// 4. Lineage`), so it shares the tx and runs
only when triggering is requested:

```rust
    // 3b. Flush trigger: accrue this batch's live bytes; on crossing the
    // (per-table or global) threshold, enqueue one flush_table job, atomically
    // with the rows. `None` => triggering disabled (preserves prior behaviour).
    if let Some(threshold) = flush_threshold {
        let add = batch.get_array_memory_size() as i64;
        let st = bump_inline_trigger(&mut *conn, tid, add, threshold).await?;
        if st.live_bytes >= st.effective && !st.enqueued {
            let job = NewJob {
                kind: crate::iceberg_flush::FLUSH_JOB_KIND.to_string(),
                payload: serde_json::json!({ "schema": table.schema, "name": table.name }),
                run_at: None,
                priority: 0,
            };
            crate::queue::pg_insert(&mut *conn, &job).await?;
            arm_inline_trigger(&mut *conn, tid).await?;
        }
    }
```

(`serde_json` is already used elsewhere in the crate. `get_array_memory_size` is
inherent on arrow-57 `RecordBatch`.)

- [ ] **Step 5: Update the existing call sites to `None`**

So the crate + tests compile with the new arity. Add `, None` as the last arg at:

- `src/control-plane/postgres/src/iceberg_landing.rs:67` — `inline_append(pool, table, columns, &batch, lineage, None).await` (Task 4 flips this to `Some(...)`).
- `src/control-plane/postgres/src/fixture.rs:659` — append `, None`.
- `src/control-plane/postgres/tests/iceberg_flush.rs` — all 5 `inline_append(...)` calls (lines ~78, 160, 206, 282, 299): append `, None`.
- `src/control-plane/postgres/tests/iceberg_inline.rs` — the `inline_append(...)` call: append `, None`.

- [ ] **Step 6: Wire the BUCK target + format + test**

Add an `inline-flush-trigger` `loom_fixture_test` to
`src/control-plane/postgres/BUCK` mirroring `iceberg-flush`'s deps, with
`srcs = ["tests/inline_flush_trigger.rs"]`.

```bash
eval "$(./tools/env.sh)" && cargo fmt -p control-plane-postgres
buck2 test //src/control-plane/postgres:inline-flush-trigger > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: `Tests finished: Pass 4. Fail 0.`

No `.sqlx` change in this task: the trigger block uses Task 1's helpers and the
existing `pg_insert` — all pre-existing `query!`s.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_flush.rs \
        src/control-plane/postgres/src/iceberg_inline.rs \
        src/control-plane/postgres/src/iceberg_landing.rs \
        src/control-plane/postgres/src/fixture.rs \
        src/control-plane/postgres/tests/iceberg_flush.rs \
        src/control-plane/postgres/tests/iceberg_inline.rs \
        src/control-plane/postgres/tests/inline_flush_trigger.rs \
        src/control-plane/postgres/BUCK
git commit -m "feat(iceberg): enqueue flush_table job when inline writes cross the byte threshold" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: flush resets the trigger (both paths)

`flush_table` must leave the trigger disarmed after every call, so the debounce
flag can't stick. Reset on the committing (`Some`) path and the no-op (`None`)
path.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_flush.rs` (`flush_locked`)
- Modify: `src/control-plane/postgres/tests/inline_flush_trigger.rs` (2 new tests)

**Interfaces:**
- Consumes: `reset_inline_trigger` (Task 1); `live_table_id` (`iceberg_mirror`, existing).

- [ ] **Step 1: Write the failing reset tests**

These need a real `SqlCatalog` (for `flush_table`). Add the catalog imports +
the `make_catalog` helper to the **top** of
`src/control-plane/postgres/tests/inline_flush_trigger.rs` (copied verbatim from
`tests/iceberg_flush.rs:17-21,47-59`):

```rust
use std::collections::HashMap;

use control_plane_postgres::iceberg_flush::flush_table;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;

async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), format!("file://{warehouse}"));
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

async fn trigger_row(pool: &sqlx::PgPool, schema: &str, name: &str) -> Option<(i64, bool)> {
    let tid: Option<i64> = sqlx::query_scalar(
        "select table_id from iceberg_mirror.\"table\" \
         where table_namespace=$1 and table_name=$2 and end_snapshot is null",
    )
    .bind(schema).bind(name)
    .fetch_optional(pool).await.unwrap();
    let tid = tid?;
    sqlx::query_as::<_, (i64, bool)>(
        "select live_bytes, enqueued from iceberg_mirror.inline_trigger where table_id=$1",
    )
    .bind(tid)
    .fetch_optional(pool).await.unwrap()
}
```

Then the two tests:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_resets_trigger_on_some() {
    let fx = PgFixture::start();
    let (_, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().unwrap();
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef { schema: "wh".into(), name: "t".into() };
    let run = RunId(uuid::Uuid::new_v4());

    // Cross the threshold (enqueued=true, live_bytes>0).
    inline_append(&pool, &table, &one_long_col(), &batch(&[1, 2, 3]), lineage(run, &table), Some(1))
        .await.unwrap();
    assert!(trigger_row(&pool, "wh", "t").await.unwrap().1, "armed before flush");

    // Flush: rows are live -> Some path.
    flush_table(&catalog, &pool, &table, run).await.unwrap().expect("flushed");
    assert_eq!(trigger_row(&pool, "wh", "t").await.unwrap(), (0, false), "reset after Some flush");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noop_flush_disarms_the_flag() {
    let fx = PgFixture::start();
    let (_, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().unwrap();
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let table = TableRef { schema: "wh".into(), name: "t".into() };
    let run = RunId(uuid::Uuid::new_v4());

    inline_append(&pool, &table, &one_long_col(), &batch(&[1]), lineage(run, &table), Some(1))
        .await.unwrap();
    // First flush drains the rows (Some).
    flush_table(&catalog, &pool, &table, run).await.unwrap();
    // Re-arm to simulate a stuck flag, then flush again -> None path must clear it.
    sqlx::query("update iceberg_mirror.inline_trigger set enqueued=true")
        .execute(&pool).await.unwrap();
    let r = flush_table(&catalog, &pool, &table, RunId(uuid::Uuid::new_v4())).await.unwrap();
    assert!(r.is_none(), "nothing live -> None");
    assert_eq!(trigger_row(&pool, "wh", "t").await.unwrap().1, false, "None path disarmed the flag");
}
```

- [ ] **Step 2: Run to verify failure**

```bash
buck2 test //src/control-plane/postgres:inline-flush-trigger > /tmp/t.log 2>&1; grep -E "assert|Tests finished|FAIL" /tmp/t.log
```

Expected: `flush_resets_trigger_on_some` and `noop_flush_disarms_the_flag` FAIL —
the trigger row still shows `enqueued=true` / non-zero `live_bytes` because
`flush_table` doesn't reset yet.

- [ ] **Step 3: Add the resets to `flush_locked`**

In `src/control-plane/postgres/src/iceberg_flush.rs`, import the helpers:

```rust
use crate::iceberg_mirror::{live_table_id, reset_inline_trigger};
```

In `flush_locked`, the `inline_live_batch` `None` branch currently returns
`Ok(None)` directly. Replace it to reset first:

```rust
    let Some((tid, row_ids, batch)) = ice.inline_live_batch(table, current.id).await? else {
        // Nothing live to flush — but a prior crash could have left the trigger
        // armed; disarm it so the table can re-trigger. No-op if no trigger row.
        let mut conn = pool.acquire().await.map_err(backend)?;
        if let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name).await? {
            reset_inline_trigger(&mut conn, tid).await?;
        }
        return Ok(None);
    };
```

On the `Some` path, after `append_parquet_snapshot(...)` returns, reset before
returning:

```rust
    let snap = append_parquet_snapshot(
        pool, catalog, table, &columns, vec![batch],
        Some(&lineage), Some(end_cap),
    )
    .await?;

    // Disarm the trigger now that the inline rows are file-backed. Standalone
    // (not in the cap tx): a crash between the cap and here self-heals because the
    // job is re-run and hits the None branch above. No-op if no trigger row.
    let mut conn = pool.acquire().await.map_err(backend)?;
    reset_inline_trigger(&mut conn, tid).await?;

    Ok(Some(snap))
```

(The `current_snapshot` `NotFound` early-return stays as-is — a never-written
table has no trigger row.)

- [ ] **Step 4: Format, test, full flush-suite regression**

```bash
eval "$(./tools/env.sh)" && cargo fmt -p control-plane-postgres
buck2 test //src/control-plane/postgres:inline-flush-trigger //src/control-plane/postgres:iceberg-flush > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: `inline-flush-trigger` Pass 6, `iceberg-flush` unchanged and green.

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_flush.rs \
        src/control-plane/postgres/tests/inline_flush_trigger.rs
git commit -m "feat(iceberg): reset inline_trigger on flush (both paths) so the debounce can't stick" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: config plumbing — `LOOM_FLUSH_BYTE_THRESHOLD`

Thread a global threshold from ingest config to `inline_append`, mirroring the
existing `LOOM_INLINE_BYTE_LIMIT` plumbing, and flip the real landing call from
`None` to `Some(threshold)`.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (`land` signature + the `inline_append` call)
- Modify: `src/services/ingest/src/landing.rs` (materializer config field + pass-through)
- Modify: `src/services/ingest/src/main.rs` (env read + default)
- Modify: `src/services/ingest/tests/iceberg_land.rs` (set the new field; assert a job enqueues)

**Interfaces:**
- Consumes: `inline_append(..., Some(i64))` (Task 2).
- Produces: `land(..., flush_byte_threshold: i64, ...)`; materializer field `flush_byte_threshold: i64`.

- [ ] **Step 1: Add the param to `land` and flip the inline call**

In `src/control-plane/postgres/src/iceberg_landing.rs`, add a param to `land`
(place it right after `inline_byte_limit`):

```rust
pub async fn land(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    ipc_body: &[u8],
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
    lineage: LineageEvent,
) -> Result<SnapshotId> {
```

And change the inline branch (line ~67) from `None` to the threshold:

```rust
        inline_append(pool, table, columns, &batch, lineage, Some(flush_byte_threshold)).await
```

Update the doc comment on `land` to mention `flush_byte_threshold` (the byte
total of live inline rows at/above which a `flush_table` job is enqueued).

- [ ] **Step 2: Thread it through the ingest materializer**

In `src/services/ingest/src/landing.rs`, add a field next to `inline_byte_limit`
(line ~99):

```rust
    /// Live-inline-byte total at/above which a flush_table job is enqueued.
    pub flush_byte_threshold: i64,
```

And pass it at the `land(...)` call (line ~111), in the new arg position:

```rust
            self.inline_byte_limit,
            self.flush_byte_threshold,
```

- [ ] **Step 3: Read the env in `main.rs`**

In `src/services/ingest/src/main.rs`, mirror `DEFAULT_INLINE_BYTE_LIMIT`
(lines ~21, ~38-46). Add:

```rust
/// Default live-inline-byte total that triggers a flush, overridable via
/// `LOOM_FLUSH_BYTE_THRESHOLD`. 64 MiB = 4× the inline routing limit, so a table
/// accrues several inline batches before compacting.
const DEFAULT_FLUSH_BYTE_THRESHOLD: i64 = 64 * 1024 * 1024;
```

And where `inline_byte_limit` is read:

```rust
            let flush_byte_threshold = std::env::var("LOOM_FLUSH_BYTE_THRESHOLD")
                .ok()
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(DEFAULT_FLUSH_BYTE_THRESHOLD);
```

Then set it on the materializer config alongside `inline_byte_limit`:

```rust
                inline_byte_limit,
                flush_byte_threshold,
```

- [ ] **Step 4: Update the ingest test to set the field and assert a job**

In `src/services/ingest/tests/iceberg_land.rs`, set the new field next to
`inline_byte_limit: 16 * 1024 * 1024` (line ~68):

```rust
            inline_byte_limit: 16 * 1024 * 1024,
            flush_byte_threshold: 1, // tiny: any inline landing crosses it
```

Add an assertion to the test that lands inline data: after the inline landing,
exactly one `flush_table` job exists. (Use the same pool the test already holds.)

```rust
    let n: i64 = sqlx::query_scalar("select count(*) from queue.jobs where kind = 'flush_table'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 1, "inline landing past the flush threshold enqueues one job");
```

> If the existing test lands a *Parquet*-sized body (above `inline_byte_limit`),
> the trigger won't fire (Parquet path doesn't touch the trigger). Add/keep an
> inline-sized landing for this assertion, or add a dedicated test case mirroring
> the inline one already in that file.

- [ ] **Step 5: Format, build, test ingest + postgres**

```bash
eval "$(./tools/env.sh)" && cargo fmt -p control-plane-postgres -p ingest
buck2 test //src/services/ingest/... //src/control-plane/postgres:inline-flush-trigger > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: ingest tests green (incl. the new assertion), trigger tests still green.

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_landing.rs \
        src/services/ingest/src/landing.rs \
        src/services/ingest/src/main.rs \
        src/services/ingest/tests/iceberg_land.rs
git commit -m "feat(ingest): wire LOOM_FLUSH_BYTE_THRESHOLD into the inline landing path" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: roadmap update + full-suite verification

**Files:**
- Modify: `docs/spike/ICEBERG_ROADMAP.md`

- [ ] **Step 1: Update the roadmap**

In `docs/spike/ICEBERG_ROADMAP.md`, under the "Inline flush/compaction"
"Remaining follow-ups" note (and/or the deferred item #3 "Flush triggering +
GC"), record that the **trigger producer** landed: a byte-threshold trigger on
`inline_append` enqueues `flush_table` jobs (`iceberg_mirror.inline_trigger`,
`LOOM_FLUSH_BYTE_THRESHOLD`). Note the **consumer** (a worker draining the queue)
is Spec 2, the engine-wire flush vertical
(`docs/spike/engine-wire-transport.md`). Keep GC deferred.

End the file with exactly one trailing newline; no trailing whitespace.

- [ ] **Step 2: Full suite (the cross-crate backstop)**

A signature change to `inline_append` + a new migration touch shared crates;
run the whole suite, not just the changed targets (per the reindeer/native-dep
guidance — a green per-crate build is not enough).

```bash
buck2 test //src/... > /tmp/full.log 2>&1; grep -E "Tests finished|FAIL" /tmp/full.log
```

Expected: all green, no regressions. If any DuckLake serving test fails with a
catalog-version mismatch, that's an unrelated `duckdb` downgrade — see CLAUDE.md
(`cargo update -p duckdb --precise 1.10503.1` + `./tools/buckify.sh`), not a
fault in this work.

- [ ] **Step 3: Lint (markdown + rust hooks) and commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/lint.log 2>&1; grep -E "Passed|Failed|files were modified" /tmp/lint.log
git add -A
git commit -m "docs(iceberg): mark inline flush trigger (producer) landed in the roadmap" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

(If prek modified files, they're already staged by `git add -A`; confirm the
commit includes them.)

---

## Out of scope (this plan)

The **consumer** — a worker that drains `flush_table` jobs and calls
`flush_table` — is Spec 2 (the engine-wire flush vertical), tracked separately.
Physical GC, age/count triggers, a generic trigger registry, and a governed
surface for setting per-table `threshold` are all deferred (see the spec's "Out
of scope").
