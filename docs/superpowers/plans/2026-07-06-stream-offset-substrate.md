# Stream Offset Substrate (Slice 0) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new `stream` control-plane schema and a `BucketOffsets` concern that hands out a gapless, per-`(table, bucket)` monotonic offset — the spine that turns loom's inline tier into an ordered log.

**Architecture:** Mirror the existing `queue` concern's five-layer control-plane stack (core trait → memory fake → postgres adapter → testkit contract → test targets). The allocator is a single-statement `INSERT … ON CONFLICT … DO UPDATE … RETURNING` on a counter row `stream.bucket_offset(table_id, bucket, next)`; the row's lock serializes concurrent allocations per bucket, giving gaplessness, and because the statement runs on whatever executor it is handed it participates in the caller's transaction (an offset is assigned iff that write commits). No existing write path changes — the allocator's first consumer is Slice 1.

**Tech Stack:** Rust (edition 2024), `sqlx` 0.9 compile-time `query_scalar!`, `async-trait`, Postgres 17, buck2 (`rust_library` / `rust_test` / `loom_fixture_test`), the hermetic Postgres test fixture.

**Spec:** `docs/superpowers/specs/2026-07-06-stream-engine-design.md` (Slice 0 — Offset substrate).

## Global Constraints

- **Tests are `rust_test` integration targets only — never inline `#[cfg(test)]`.** Fixture (Postgres-backed) tests use the `loom_fixture_test` macro; pure/in-memory tests use `rust_test`. The `no-inline-tests` prek hook fails otherwise.
- **After adding or changing any `sqlx::query*!` macro, regenerate the committed cache:** `tools/sqlx-prepare.sh`, then commit the `src/control-plane/postgres/.sqlx/` change. The `//src/control-plane/postgres:sqlx-cache-check` test enforces freshness.
- **Clippy is strict** (pedantic + restriction, panic-safety lints enforced on production code). No `unwrap`/`expect`/`panic`/`todo`/`unimplemented`/`dbg` in `src/**`. Test code is exempted from panic-safety lints by the `loom_rust_test`/`loom_fixture_test` wrappers.
- **Run the hooks before every commit:** `buck2 run //tools:prek -- run --all-files` (rustfmt is a separate hook from clippy — clippy-clean ≠ lint-clean). Commit whatever the hooks change.
- **Conventional Commits** are enforced on the message (`docs:`/`feat:`/`test:` … ).
- **New source files are auto-globbed** — `core`, `memory`, `postgres`, `testkit` all use `srcs = glob(["src/**/*.rs"])`, and the postgres `rust_library` globs `migrations/*.sql` via `mapped_srcs`. Only new **test targets** need a BUCK stanza.
- **Fixture tests on a root host** (cloud sessions) go to RE; the buck2 proxy shim injects `--unstable-allow-all-tests-on-re`. Locally on a non-root dev box they run on the local executor. Run the full-suite with `-j 8` to avoid starving the 8 Postgres boot-slots.

---

## File Structure

**Create:**
- `src/control-plane/postgres/migrations/0034_stream.sql` — the `stream` schema + `stream.bucket_offset` counter table.
- `src/control-plane/core/src/stream.rs` — the `BucketOffsets` trait.
- `src/control-plane/memory/src/stream.rs` — the memory fake impl.
- `src/control-plane/postgres/src/stream.rs` — the postgres adapter (`pg_allocate_offset`, `pg_peek_offset`, `impl BucketOffsets for PgControlPlane`).
- `src/control-plane/memory/tests/stream.rs` — memory contract test target.
- `src/control-plane/postgres/tests/stream.rs` — postgres fixture contract + concurrency test target.

**Modify:**
- `src/control-plane/core/src/lib.rs` — `mod stream;` + `pub use`.
- `src/control-plane/core/src/error.rs` — (no change; reuse `Result`/`ControlPlaneError`).
- `src/control-plane/memory/src/lib.rs` — add the `offsets` state field + init.
- `src/control-plane/memory/src/stream.rs` — (created above).
- `src/control-plane/postgres/src/lib.rs` — `mod stream;`.
- `src/control-plane/testkit/src/lib.rs` — add `bucket_offsets_contract` + `bucket_offsets_concurrency_contract`.
- `src/control-plane/memory/BUCK` — add the `stream` `rust_test` target.
- `src/control-plane/postgres/BUCK` — add the `stream` `loom_fixture_test` target.

**Deliberately NOT in this slice (YAGNI):** no `fn bucket_offsets()` accessor on the `ControlPlane` trait (no service consumes it yet — the contract bounds directly on `BucketOffsets`); no `Tx::allocate_offset` (the transactional call site arrives in Slice 1); no `change_kind` column (Slices 1–2).

---

### Task 1: Core trait + memory fake + testkit contract (memory-green)

Delivers the `BucketOffsets` trait, its in-memory implementation, and the backend-agnostic contract, proven green against the memory fake with no database.

**Files:**
- Create: `src/control-plane/core/src/stream.rs`
- Modify: `src/control-plane/core/src/lib.rs`
- Create: `src/control-plane/memory/src/stream.rs`
- Modify: `src/control-plane/memory/src/lib.rs:` (the `MemoryControlPlane` struct + `new`)
- Modify: `src/control-plane/testkit/src/lib.rs`
- Test: `src/control-plane/memory/tests/stream.rs`
- Modify: `src/control-plane/memory/BUCK`

**Interfaces:**
- Produces (core): the trait later tasks and slices implement/consume —
  ```rust
  #[async_trait]
  pub trait BucketOffsets {
      async fn allocate_offset(&self, table_id: i64, bucket: i32, count: i64) -> Result<i64>;
      async fn peek_offset(&self, table_id: i64, bucket: i32) -> Result<i64>;
  }
  ```
  `allocate_offset` returns the FIRST offset of a contiguous `count`-length run (offsets start at 0); `peek_offset` returns the high-water (the next offset that would be handed out, 0 if none).
- Produces (testkit): `pub async fn bucket_offsets_contract<CP: BucketOffsets>(cp: &CP)` and `pub async fn bucket_offsets_concurrency_contract<CP: BucketOffsets + Clone + Send + Sync + 'static>(cp: CP)`.
- Consumes: `control_plane_core::error::Result`, `MemoryControlPlane::new` (from the memory crate).

- [ ] **Step 1: Write the contract (the behavioral spec) in testkit**

Append to `src/control-plane/testkit/src/lib.rs` (same file `queue_contract` lives in):

```rust
use control_plane_core::BucketOffsets;

/// Contract for the per-`(table, bucket)` offset allocator. `cp` must be freshly
/// empty. Offsets start at 0, are gapless and monotonic per bucket, and buckets
/// and tables are independent sequences.
pub async fn bucket_offsets_contract<CP: BucketOffsets>(cp: &CP) {
    // a fresh (table, bucket) has high-water 0 and hands out 0 first
    assert_eq!(cp.peek_offset(1, 0).await.expect("peek"), 0, "fresh high-water is 0");
    assert_eq!(cp.allocate_offset(1, 0, 1).await.expect("alloc"), 0, "first offset is 0");
    assert_eq!(cp.allocate_offset(1, 0, 1).await.expect("alloc"), 1, "offsets are contiguous");
    assert_eq!(cp.peek_offset(1, 0).await.expect("peek"), 2, "high-water advanced to 2");
    // a batch allocation returns the first offset of the run and advances by count
    assert_eq!(cp.allocate_offset(1, 0, 3).await.expect("alloc"), 2, "batch returns its first offset");
    assert_eq!(cp.peek_offset(1, 0).await.expect("peek"), 5, "high-water advanced by count");
    assert_eq!(cp.allocate_offset(1, 0, 1).await.expect("alloc"), 5, "next offset after the batch");
    // buckets are independent
    assert_eq!(cp.allocate_offset(1, 1, 1).await.expect("alloc"), 0, "bucket 1 has its own sequence");
    // tables are independent
    assert_eq!(cp.allocate_offset(2, 0, 1).await.expect("alloc"), 0, "table 2 has its own sequence");
}

/// Concurrency contract (run against a real database): N concurrent single-row
/// allocations on one bucket must yield a gapless `0..N` with no gaps or dups.
pub async fn bucket_offsets_concurrency_contract<CP>(cp: CP)
where
    CP: BucketOffsets + Clone + Send + Sync + 'static,
{
    const N: i64 = 64;
    let mut handles = Vec::new();
    for _ in 0..N {
        let cp = cp.clone();
        handles.push(tokio::spawn(async move {
            cp.allocate_offset(7, 0, 1).await.expect("alloc")
        }));
    }
    let mut got = Vec::new();
    for h in handles {
        got.push(h.await.expect("join"));
    }
    got.sort_unstable();
    let want: Vec<i64> = (0..N).collect();
    assert_eq!(got, want, "N concurrent allocations yield a gapless 0..N with no dups");
    assert_eq!(cp.peek_offset(7, 0).await.expect("peek"), N, "high-water equals N");
}
```

- [ ] **Step 2: Define the trait in core**

Create `src/control-plane/core/src/stream.rs`:

```rust
//! The stream-offset concern: a gapless, per-`(table, bucket)` monotonic offset
//! allocator. Offsets are what make the inline tier an ordered log; a later
//! slice stamps appended rows with `(bucket, offset)`.

use async_trait::async_trait;

use crate::error::Result;

#[async_trait]
pub trait BucketOffsets {
    /// Allocate a contiguous run of `count` offsets for `(table_id, bucket)` and
    /// return the FIRST offset in the run. Offsets start at 0; allocations are
    /// gapless and monotonic per bucket. When issued inside a transaction the
    /// offsets are assigned iff that transaction commits.
    async fn allocate_offset(&self, table_id: i64, bucket: i32, count: i64) -> Result<i64>;
    /// The high-water offset for `(table_id, bucket)` — the next offset that
    /// would be handed out (0 if none has been allocated yet).
    async fn peek_offset(&self, table_id: i64, bucket: i32) -> Result<i64>;
}
```

Add to `src/control-plane/core/src/lib.rs` next to the other concern modules (e.g. after `mod queue;` / its `pub use`):

```rust
mod stream;
pub use stream::BucketOffsets;
```

- [ ] **Step 3: Add memory state + implement the fake**

In `src/control-plane/memory/src/lib.rs`, add a field to the `MemoryControlPlane` struct (beside `rows`, `catalog`, …):

```rust
    offsets: std::sync::Arc<parking_lot::Mutex<std::collections::HashMap<(i64, i32), i64>>>,
```

and initialize it in `MemoryControlPlane::new` (beside the other `Arc::new(Mutex::new(...))` inits):

```rust
            offsets: std::sync::Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
```

Create `src/control-plane/memory/src/stream.rs`:

```rust
use async_trait::async_trait;
use control_plane_core::{error::Result, BucketOffsets};

use crate::MemoryControlPlane;

#[async_trait]
impl BucketOffsets for MemoryControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn allocate_offset(&self, table_id: i64, bucket: i32, count: i64) -> Result<i64> {
        let mut map = self.offsets.lock();
        let next = map.entry((table_id, bucket)).or_insert(0);
        let first = *next;
        *next += count;
        Ok(first)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn peek_offset(&self, table_id: i64, bucket: i32) -> Result<i64> {
        Ok(self.offsets.lock().get(&(table_id, bucket)).copied().unwrap_or(0))
    }
}
```

Add the module to `src/control-plane/memory/src/lib.rs` (beside `mod queue;`):

```rust
mod stream;
```

- [ ] **Step 4: Write the memory test target**

Create `src/control-plane/memory/tests/stream.rs`:

```rust
use std::time::Duration;

use control_plane_memory::MemoryControlPlane;

#[tokio::test]
async fn memory_passes_bucket_offsets_contract() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    control_plane_testkit::bucket_offsets_contract(&cp).await;
}
```

Add the target to `src/control-plane/memory/BUCK` (mirror the existing `queue` `rust_test`):

```python
rust_test(
    name = "stream",
    crate = "stream",
    srcs = ["tests/stream.rs"],
    crate_root = "tests/stream.rs",
    edition = "2024",
    deps = [":memory", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

- [ ] **Step 5: Run the memory test — expect PASS**

Run: `buck2 test //src/control-plane/memory:stream > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`
Expected: the two-line summary shows the test passing (`Tests finished: Pass 1`). If it fails to build on a `Result`/import path, align the `use` lines in the new files with the sibling `queue.rs` in the same crate.

- [ ] **Step 6: Lint + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/core/src/stream.rs src/control-plane/core/src/lib.rs \
        src/control-plane/memory/src/stream.rs src/control-plane/memory/src/lib.rs \
        src/control-plane/memory/tests/stream.rs src/control-plane/memory/BUCK \
        src/control-plane/testkit/src/lib.rs
git commit -m "feat(stream): BucketOffsets trait + memory fake + contract"
```

---

### Task 2: Postgres adapter + migration + fixture tests (postgres-green)

Delivers the `stream` schema and the postgres implementation, proven against the hermetic Postgres fixture — including the load-bearing concurrency property (gapless allocation under contention).

**Files:**
- Create: `src/control-plane/postgres/migrations/0034_stream.sql`
- Create: `src/control-plane/postgres/src/stream.rs`
- Modify: `src/control-plane/postgres/src/lib.rs` (add `mod stream;`)
- Test: `src/control-plane/postgres/tests/stream.rs`
- Modify: `src/control-plane/postgres/BUCK`
- Regenerate: `src/control-plane/postgres/.sqlx/` (via `tools/sqlx-prepare.sh`)

**Interfaces:**
- Consumes: the `BucketOffsets` trait (Task 1); `crate::PgControlPlane` (its private `pool` field + `pool()` method); `crate::backend` (the sqlx-error mapper used across the adapter); `control_plane_postgres::fixture::PgFixture`.
- Produces: `impl BucketOffsets for PgControlPlane`, and the crate-internal executor-generic helpers `pg_allocate_offset` / `pg_peek_offset` (the transactional call site in Slice 1 will call `pg_allocate_offset` with a `&mut *tx`).

- [ ] **Step 1: Write the migration**

Create `src/control-plane/postgres/migrations/0034_stream.sql`:

```sql
-- Per-(table, bucket) monotonic offset allocator: the spine of the stream
-- engine. `next` is the next offset to hand out (offsets start at 0). An
-- allocation of `count` returns the pre-increment value as the first offset of a
-- contiguous run and advances `next` by `count`. The row's lock serializes
-- concurrent allocations per bucket, so offsets are gapless within a bucket;
-- buckets and tables are independent. The allocation runs on the caller's
-- executor, so an offset is assigned iff that write commits.
create schema if not exists stream;

create table stream.bucket_offset (
    table_id bigint not null,
    bucket   int    not null,
    next     bigint not null,
    primary key (table_id, bucket)
);
```

- [ ] **Step 2: Write the postgres adapter**

Create `src/control-plane/postgres/src/stream.rs` (align the `Result`/`backend` import paths with the sibling `src/queue.rs` if they differ):

```rust
use async_trait::async_trait;
use control_plane_core::{error::Result, BucketOffsets};

use crate::{backend, PgControlPlane};

/// Allocate a contiguous run of `count` offsets for `(table_id, bucket)` on the
/// given executor, returning the first offset. Usable inside a transaction (pass
/// `&mut *tx`) so the allocation commits with the caller's write.
pub(crate) async fn pg_allocate_offset<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
    bucket: i32,
    count: i64,
) -> Result<i64> {
    let first = sqlx::query_scalar!(
        "insert into stream.bucket_offset (table_id, bucket, next) values ($1, $2, $3) \
         on conflict (table_id, bucket) do update set next = stream.bucket_offset.next + $3 \
         returning next - $3 as \"first!\"",
        table_id,
        bucket,
        count,
    )
    .fetch_one(ex)
    .await
    .map_err(backend)?;
    Ok(first)
}

/// The high-water offset for `(table_id, bucket)` — 0 if none allocated yet.
pub(crate) async fn pg_peek_offset<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
    bucket: i32,
) -> Result<i64> {
    let next = sqlx::query_scalar!(
        "select coalesce( \
             (select next from stream.bucket_offset where table_id = $1 and bucket = $2), \
             0) as \"next!\"",
        table_id,
        bucket,
    )
    .fetch_one(ex)
    .await
    .map_err(backend)?;
    Ok(next)
}

#[async_trait]
impl BucketOffsets for PgControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn allocate_offset(&self, table_id: i64, bucket: i32, count: i64) -> Result<i64> {
        pg_allocate_offset(self.pool(), table_id, bucket, count).await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn peek_offset(&self, table_id: i64, bucket: i32) -> Result<i64> {
        pg_peek_offset(self.pool(), table_id, bucket).await
    }
}
```

Add to `src/control-plane/postgres/src/lib.rs` (beside `mod queue;`):

```rust
mod stream;
```

- [ ] **Step 3: Regenerate the sqlx cache**

The two new `query_scalar!` macros must be verified against the live schema. Run:

```bash
tools/sqlx-prepare.sh
```

Expected: new `src/control-plane/postgres/.sqlx/query-*.json` files appear for the two queries. This boots the pinned Postgres, applies all migrations (incl. `0034`), and runs `cargo sqlx prepare`.

- [ ] **Step 4: Write the postgres fixture test target**

Create `src/control-plane/postgres/tests/stream.rs`:

```rust
use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_bucket_offsets_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::bucket_offsets_contract(&cp).await;
}

#[tokio::test]
async fn postgres_passes_bucket_offsets_concurrency_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::bucket_offsets_concurrency_contract(cp).await;
}
```

Add the target to `src/control-plane/postgres/BUCK` (mirror the existing `queue` `loom_fixture_test`):

```python
loom_fixture_test(
    name = "stream",
    crate = "stream",
    srcs = ["tests/stream.rs"],
    crate_root = "tests/stream.rs",
    deps = [":postgres", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

- [ ] **Step 5: Run the postgres tests — expect PASS**

Run: `buck2 test //src/control-plane/postgres:stream > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`
Expected: `Tests finished: Pass 2. Fail 0`. The concurrency test is the load-bearing one — it proves the `ON CONFLICT` row-lock yields a gapless `0..64` across 64 concurrent allocations. (On a root/cloud host the shim routes fixture runs to RE; locally they run on the local executor.)

- [ ] **Step 6: Verify the sqlx cache is fresh, then lint + commit**

```bash
buck2 test //src/control-plane/postgres:sqlx-cache-check > /tmp/s.log 2>&1; grep -E "Tests finished|FAIL" /tmp/s.log
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres/migrations/0034_stream.sql \
        src/control-plane/postgres/src/stream.rs src/control-plane/postgres/src/lib.rs \
        src/control-plane/postgres/tests/stream.rs src/control-plane/postgres/BUCK \
        src/control-plane/postgres/.sqlx
git commit -m "feat(stream): stream schema + postgres BucketOffsets allocator"
```

---

### Task 3: Register the roadmap items

Records Slice 0 as delivered and the remaining slices/follow-ups as tracked open work, per loom's documentation registers. This is bookkeeping, not code, but it is how future sessions discover the sequenced work.

**Files:**
- Modify: `docs/ROADMAP.md`
- Modify: `docs/FUTURE.md`

- [ ] **Step 1: Add the ROADMAP entries**

Add to `docs/ROADMAP.md` (grammar: one list entry, backtick tag block on the title line). `road-stream-substrate` is the just-built item; the rest are planned slices:

```markdown
- [ ] **Stream engine — Log Tables (slice 1)** `{#road-stream-log-tables area:ingest status:planned from:2026-07-06-stream-engine-design pr:- spec:2026-07-06-stream-engine-design}`
  Append-only stream dataset flavor: appends land in the inline tier stamped with `(bucket, offset)` from the `BucketOffsets` allocator and a `change_kind = +I` column (added to the inline DDL beside `loom_tombstone`), sub-second-visible via the current-state union read, flushed to a changelog Iceberg table. Builds on `road-stream-substrate` (shipped: the offset allocator). See spec §"Sliced roadmap".
- [ ] **Stream engine — PK / CDC tables (slice 2)** `{#road-stream-pk-tables area:ontology status:planned from:2026-07-06-stream-engine-design pr:- spec:2026-07-06-stream-engine-design}`
  Emit +I/−U/+U/−D change events on the mutation path; write the dual Iceberg tables (changelog + current-state base). Builds on the shipped `road-cow-inline-shadow` (merge-on-read + CAS) and brings `fut-cow-inline-shadow` slice-2 compaction into scope. Merge engine LastRow only; others in [[fut-stream-merge-engines]].
- [ ] **Stream engine — Subscribe / tail feed (slice 3)** `{#road-stream-subscribe area:query status:planned from:2026-07-06-stream-engine-design pr:- spec:2026-07-06-stream-engine-design}`
  Governed query-api endpoint yielding a per-bucket offset-cursor feed of change events (changelog Iceberg ∪ inline tail), with LISTEN/NOTIFY + polling fallback and column projection. Folds in [[fut-serving-stream-to-http]].
- [ ] **Stream engine — Continuous / standing queries (slice 4)** `{#road-stream-continuous area:transform status:planned from:2026-07-06-stream-engine-design pr:- spec:2026-07-06-stream-engine-design}`
  Offset-watermarked micro-batch transform mode; MV output committed as its own subscribable changelog. Extends [[fut-transform-followups]].
- [ ] **Stream engine — Stream joins / delta-join analog (slice 5)** `{#road-stream-joins area:transform status:planned from:2026-07-06-stream-engine-design pr:- spec:2026-07-06-stream-engine-design}`
  Lookup-join (point-lookup against a PK index, [[fut-stream-pk-index]]) + micro-batch stream-stream join. True stateful incremental join deferred ([[fut-stream-incremental-join]]).
```

- [ ] **Step 2: Add the FUTURE (deferred) entries**

Add to `docs/FUTURE.md`:

```markdown
- [ ] **Stream engine — Arrow log on object storage** `{#fut-stream-arrow-log area:ingest status:deferred from:2026-07-06-stream-engine-design pr:- spec:-}`
  Re-back the offset-ordered live tier with sealed Arrow segments on object storage (design Approach B) if Postgres becomes the hot-log throughput bottleneck; the `BucketOffsets`/changelog logical model is unchanged.
- [ ] **Stream engine — merge engines** `{#fut-stream-merge-engines area:ontology status:deferred from:2026-07-06-stream-engine-design pr:- spec:-}`
  FirstRow / Versioned / Aggregation merge engines + partial-update, beyond slice-2's LastRow default.
- [ ] **Stream engine — primary-key index** `{#fut-stream-pk-index area:query status:deferred from:2026-07-06-stream-engine-design pr:- spec:-}`
  A primary-key index spanning the inline + Iceberg tiers for high-QPS point lookups (backs the slice-5 lookup-join).
- [ ] **Stream engine — incremental stream-stream join** `{#fut-stream-incremental-join area:transform status:deferred from:2026-07-06-stream-engine-design pr:- spec:-}`
  True stateful incremental stream-stream joins, beyond slice-5's micro-batch approximation.
- [ ] **Stream engine — partitioning above buckets** `{#fut-stream-partitioning area:ingest status:deferred from:2026-07-06-stream-engine-design pr:- spec:-}`
  Fluss-style two-level partition → bucket sharding above the single-level bucketing the slices ship.
```

- [ ] **Step 3: Validate the registers, then lint + commit**

```bash
bash tools/docs.sh validate
buck2 run //tools:prek -- run --all-files
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(stream): register stream-engine slices and deferred follow-ups"
```

Expected: `tools/docs.sh validate` reports no grammar/id/vocab/link errors. (`road-stream-substrate` itself is closed by this slice's delivery, so it is not added as open work — the shipped capability is recorded in git history and, at the next docs pass, under `docs/system-capabilities/`.)

---

## Self-Review

**Spec coverage (Slice 0 only — the slice this plan implements):**
- "new `stream` control-plane schema" → Task 2, Step 1 (migration `0034_stream.sql`). ✓
- "`BucketOffsets` concern built as the full five-layer control-plane stack (core trait, memory fake, postgres adapter, testkit contract)" → Task 1 (core trait, memory fake, testkit contract) + Task 2 (postgres adapter). The 5th layer (worker) is N/A — this concern has no queue-worker loop. ✓
- "gapless, per-`(table, bucket)` monotonic offset allocator … `allocate_offset(table_id, bucket, count) -> first_offset`" → trait in Task 1 Step 2; postgres `ON CONFLICT … RETURNING next - $3` in Task 2 Step 2. ✓
- "transaction-scoped so an offset is assigned iff the enclosing write commits" → executor-generic `pg_allocate_offset` (Task 2 Step 2) participates in the caller's tx; the committing call site lands in Slice 1 (noted, not built here). ✓
- "serialized per bucket by the counter row's lock" → `ON CONFLICT DO UPDATE` row lock; proven by the concurrency contract (Task 2 Step 5). ✓
- "Proven by a concurrency contract test (N concurrent single-row allocations … gapless `0..N`)" → `bucket_offsets_concurrency_contract`, Task 1 Step 1 + Task 2 Step 4-5. ✓
- "No change to existing write paths yet" → confirmed; only new files + `mod`/field additions. ✓
- "change_kind … not here" → explicitly excluded (File Structure note). ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code; every run step shows a command + expected output. The only "match the sibling file" note is for import-path alignment (`Result`/`backend`), which is concrete and verifiable, not a content placeholder. ✓

**Type consistency:** `allocate_offset(&self, table_id: i64, bucket: i32, count: i64) -> Result<i64>` and `peek_offset(&self, table_id: i64, bucket: i32) -> Result<i64>` are identical across the trait (core), the memory impl, the postgres impl, and both contract functions. SQL column types (`table_id bigint`, `bucket int`, `next bigint`) match the Rust `i64`/`i32`/`i64`. The `"first!"` / `"next!"` sqlx annotations force non-null `i64`. ✓

---

## Execution Handoff

After the plan is approved, implement task-by-task via **superpowers:subagent-driven-development** (fresh subagent per task, review between tasks) or **superpowers:executing-plans** (inline batch execution with checkpoints). Each task ends with green tests and a commit; the branch is `docs/stream-engine-design` (or a fresh `feat/stream-offset-substrate` if preferred) and finishes as a PR per the usual flow.
