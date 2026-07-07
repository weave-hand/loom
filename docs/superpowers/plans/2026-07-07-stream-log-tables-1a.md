# Stream Log Tables — Plan 1a (hot-tier log) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make append-only "log tables" real in the inline tier — a `StreamTables` concern + an ingest-time declaration flag, universal log-framing columns, and gapless per-bucket offset stamping on appends.

**Architecture:** A new `StreamTables` control-plane concern (`stream.stream_table`) records which datasets are append-only log tables and their bucket count, mirroring Slice 0's `BucketOffsets`. Three universal framing columns (`loom_change_kind`, `loom_bucket`, `loom_offset`) are added to the inline tier via the `loom_tombstone` precedent. `inline_append` becomes stream-aware: for a declared stream table it assigns each row a bucket (`row_index % bucket_count`) and stamps a gapless per-bucket offset reserved via Slice 0's allocator *inside the append's own transaction*. The ingest endpoint gains `?mode=stream&buckets=N`.

**Tech Stack:** Rust (edition 2024), `sqlx` 0.9 compile-time macros, `async-trait`, Postgres 17, Axum (ingest HTTP), buck2 (`rust_library`/`rust_test`/`loom_fixture_test`), hermetic Postgres fixture.

**Spec:** `docs/superpowers/specs/2026-07-07-stream-log-tables-design.md` (Plan 1a = "hot-tier log"; durable flush persistence is Plan 1b).

## Global Constraints

- **Tests are integration targets only** — never inline `#[cfg(test)]`. Postgres-backed tests use `loom_fixture_test`; pure/in-memory tests use `rust_test`. The `no-inline-tests` prek hook enforces this.
- **After any `sqlx::query*!` change, regenerate + commit the cache:** `tools/sqlx-prepare.sh` → commit `src/control-plane/postgres/.sqlx/`. The `//src/control-plane/postgres:sqlx-cache-check` test enforces freshness.
- **Clippy is strict:** no `unwrap`/`expect`/`panic`/`todo`/`unimplemented`/`dbg`/`indexing_slicing` in `src/**` production code. Test code is exempt via the test wrapper.
- **rustfmt is check-only in prek** — run `buck2 run //tools:rustfmt -- <changed .rs files>` to APPLY formatting, then run `buck2 run //tools:prek -- run --all-files` and commit whatever it changes.
- **Conventional Commits** message format.
- **New source files are auto-globbed** into their crate's `rust_library`; migrations are globbed via `mapped_srcs`. Only genuinely new test targets need a BUCK stanza — Tasks here extend the EXISTING `stream` test targets and add ingest tests.
- **Reserved namespace:** the `loom_` column prefix is reserved for physical/bookkeeping columns; it is never a user column. Framing columns keep this prefix and are absent from any table's `ColumnSpec`, so they are invisible to reads.
- **The no-selector / batch-table path must stay behavior-identical:** a dataset created without `?mode=stream` gets no `stream_table` row, `NULL` `loom_bucket`/`loom_offset`, `loom_change_kind='+I'`, and byte-identical ingest.

---

## File Structure

**Create:**
- `src/control-plane/postgres/migrations/0035_stream_table.sql` — the `stream.stream_table` mode table. (Confirm 0035 is the next free number: `ls src/control-plane/postgres/migrations | sort | tail -1`; bump if higher.)

**Modify:**
- `src/control-plane/core/src/stream.rs` — add the `StreamTables` trait (beside `BucketOffsets`).
- `src/control-plane/core/src/lib.rs` — export `StreamTables`.
- `src/control-plane/memory/src/stream.rs` — add `impl StreamTables for MemoryControlPlane`.
- `src/control-plane/memory/src/lib.rs` — add the `stream_tables` state field + init.
- `src/control-plane/postgres/src/stream.rs` — add `pg_declare_stream`, `pg_stream_bucket_count`, `impl StreamTables for PgControlPlane`.
- `src/control-plane/testkit/src/lib.rs` — add `stream_tables_contract`.
- `src/control-plane/postgres/tests/stream.rs` and `src/control-plane/memory/tests/stream.rs` — invoke the new contract (existing targets; no BUCK change).
- `src/control-plane/postgres/src/iceberg_inline.rs` — framing columns in `inline_ddl`/`ensure_inline_schema`; `loom_change_kind` in `write_inline_delta`; stream stamping + a `stream_buckets: Option<i32>` param in `inline_append`.
- `src/control-plane/postgres/src/iceberg_landing.rs` — thread `stream_buckets` through `land`.
- `src/control-plane/postgres/tests/` — a new `stream_inline.rs` fixture test (add a `loom_fixture_test` target to `src/control-plane/postgres/BUCK`).
- `src/services/ingest/src/http.rs` — parse `?mode=stream&buckets=N`, map declaration errors to 400.
- `src/services/ingest/src/landing.rs` — add `stream_buckets` to `LandRequest`; pass it through `IcebergMaterializer::land`.
- `src/services/ingest/tests/` — a new `stream_declare.rs` e2e (add a `loom_fixture_test` target to `src/services/ingest/BUCK`).

**Deliberately NOT in Plan 1a (Plan 1b / later slices):** durable persistence of the framing into Iceberg on flush + the `is_reserved` logical-schema filter (Plan 1b); large-write (Parquet-path) stream stamping (Plan 1b — 1a stamps the inline path; `land_parquet` for a stream table still declares the mode but does not stamp); the subscribe feed (Slice 3); `-U` emission and PK mutation (Slice 2).

---

### Task 1: `StreamTables` concern

Records which datasets are append-only log tables and their bucket count. Mirrors Slice 0's `BucketOffsets` layering.

**Files:**
- Create: `src/control-plane/postgres/migrations/0035_stream_table.sql`
- Modify: `src/control-plane/core/src/stream.rs`, `src/control-plane/core/src/lib.rs`
- Modify: `src/control-plane/memory/src/stream.rs`, `src/control-plane/memory/src/lib.rs`
- Modify: `src/control-plane/postgres/src/stream.rs`
- Modify: `src/control-plane/testkit/src/lib.rs`
- Test: `src/control-plane/postgres/tests/stream.rs`, `src/control-plane/memory/tests/stream.rs`
- Regenerate: `src/control-plane/postgres/.sqlx/`

**Interfaces:**
- Produces (core):
  ```rust
  #[async_trait]
  pub trait StreamTables {
      /// Declare table_id as a log table with bucket_count buckets. Idempotent:
      /// a redeclare of an already-declared table is a no-op (the first declaration's
      /// bucket_count stands — bucket count is immutable). Conflicting bucket counts
      /// are NOT rejected here; the write path (inline_append) enforces that.
      async fn declare_stream(&self, table_id: i64, bucket_count: i32) -> Result<()>;
      /// The bucket count if table_id is a declared log table, else None.
      async fn stream_bucket_count(&self, table_id: i64) -> Result<Option<i32>>;
  }
  ```
- Produces (postgres, `pub(crate)`, executor-generic for in-transaction use by `inline_append`):
  `pg_declare_stream<'e, E: sqlx::PgExecutor<'e>>(ex, table_id: i64, bucket_count: i32) -> Result<()>`;
  `pg_stream_bucket_count<'e, E: sqlx::PgExecutor<'e>>(ex, table_id: i64) -> Result<Option<i32>>`.
- Consumes: `control_plane_core::Result`, `crate::backend`, `crate::PgControlPlane`, `MemoryControlPlane`.

- [ ] **Step 1: Write the migration**

Create `src/control-plane/postgres/migrations/0035_stream_table.sql`:

```sql
-- Marks a dataset as an append-only "log table" and fixes its bucket count.
-- Row presence == "this is a log table"; bucket_count is immutable after creation.
-- Referenced by the write path to decide whether to stamp per-bucket offsets.
create table stream.stream_table (
    table_id     bigint      primary key,
    bucket_count int         not null,
    created_at   timestamptz not null default now()
);
```

(The `stream` schema already exists from migration `0034_stream.sql`.)

- [ ] **Step 2: Add the contract in testkit**

In `src/control-plane/testkit/src/lib.rs`, add (fold `StreamTables` into the existing `control_plane_core` import used for `BucketOffsets`):

```rust
/// Contract for the StreamTables mode registry. `cp` must be freshly empty.
pub async fn stream_tables_contract<CP: control_plane_core::StreamTables>(cp: &CP) {
    assert_eq!(cp.stream_bucket_count(1).await.expect("q"), None, "unknown table is not a stream");
    cp.declare_stream(1, 4).await.expect("declare");
    assert_eq!(cp.stream_bucket_count(1).await.expect("q"), Some(4), "declared bucket count is recorded");
    // idempotent: redeclaring is a no-op, first bucket_count stands (immutable)
    cp.declare_stream(1, 4).await.expect("idempotent redeclare");
    cp.declare_stream(1, 8).await.expect("redeclare with different count is ignored, not error");
    assert_eq!(cp.stream_bucket_count(1).await.expect("q"), Some(4), "bucket count is immutable — first declaration wins");
    // tables are independent
    assert_eq!(cp.stream_bucket_count(2).await.expect("q"), None, "other tables unaffected");
}
```

- [ ] **Step 3: Define the trait in core**

In `src/control-plane/core/src/stream.rs`, add below the `BucketOffsets` trait:

```rust
#[async_trait]
pub trait StreamTables {
    /// Declare table_id as a log table with bucket_count buckets. Idempotent: a
    /// redeclare is a no-op and the first declaration's bucket_count stands.
    async fn declare_stream(&self, table_id: i64, bucket_count: i32) -> Result<()>;
    /// The bucket count if table_id is a declared log table, else None.
    async fn stream_bucket_count(&self, table_id: i64) -> Result<Option<i32>>;
}
```

In `src/control-plane/core/src/lib.rs`, extend the stream re-export:

```rust
pub use stream::{BucketOffsets, StreamTables};
```

- [ ] **Step 4: Memory fake**

In `src/control-plane/memory/src/lib.rs`, add a field to `MemoryControlPlane` (beside `offsets`):

```rust
    stream_tables: std::sync::Arc<parking_lot::Mutex<std::collections::HashMap<i64, i32>>>,
```

and initialize it in `new` (beside `offsets`):

```rust
            stream_tables: std::sync::Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
```

In `src/control-plane/memory/src/stream.rs`, add (fold `StreamTables` into the existing `control_plane_core` import):

```rust
#[async_trait]
impl StreamTables for MemoryControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn declare_stream(&self, table_id: i64, bucket_count: i32) -> Result<()> {
        // Idempotent, first-wins: only insert if absent.
        self.stream_tables.lock().entry(table_id).or_insert(bucket_count);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn stream_bucket_count(&self, table_id: i64) -> Result<Option<i32>> {
        Ok(self.stream_tables.lock().get(&table_id).copied())
    }
}
```

- [ ] **Step 5: Postgres adapter**

In `src/control-plane/postgres/src/stream.rs`, add (fold `StreamTables` into the existing `control_plane_core` import):

```rust
/// Declare a log table (idempotent, first-wins on bucket_count).
pub(crate) async fn pg_declare_stream<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
    bucket_count: i32,
) -> Result<()> {
    sqlx::query!(
        "insert into stream.stream_table (table_id, bucket_count) values ($1, $2) \
         on conflict (table_id) do nothing",
        table_id,
        bucket_count,
    )
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(())
}

/// The bucket count if table_id is a declared log table, else None.
pub(crate) async fn pg_stream_bucket_count<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
) -> Result<Option<i32>> {
    let n = sqlx::query_scalar!(
        "select bucket_count from stream.stream_table where table_id = $1",
        table_id,
    )
    .fetch_optional(ex)
    .await
    .map_err(backend)?;
    Ok(n)
}

#[async_trait]
impl StreamTables for PgControlPlane {
    #[tracing::instrument(skip(self), level = "debug")]
    async fn declare_stream(&self, table_id: i64, bucket_count: i32) -> Result<()> {
        pg_declare_stream(self.pool(), table_id, bucket_count).await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn stream_bucket_count(&self, table_id: i64) -> Result<Option<i32>> {
        pg_stream_bucket_count(self.pool(), table_id).await
    }
}
```

- [ ] **Step 6: Invoke the contract from both existing stream test targets**

Append to `src/control-plane/memory/tests/stream.rs`:

```rust
#[tokio::test]
async fn memory_passes_stream_tables_contract() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    control_plane_testkit::stream_tables_contract(&cp).await;
}
```

Append to `src/control-plane/postgres/tests/stream.rs`:

```rust
#[tokio::test]
async fn postgres_passes_stream_tables_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::stream_tables_contract(&cp).await;
}
```

- [ ] **Step 7: Regenerate sqlx cache**

Run: `tools/sqlx-prepare.sh`
Expected: new `.sqlx/query-*.json` entries for the two new macros.

- [ ] **Step 8: Run tests — expect PASS**

Run: `buck2 test //src/control-plane/memory:stream //src/control-plane/postgres:stream //src/control-plane/postgres:sqlx-cache-check > /tmp/claude-1000/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/claude-1000/t1.log`
Expected: all pass. If a `Result`/import path fails to compile, align imports with the sibling `queue.rs`/`BucketOffsets` code in the same crate.

- [ ] **Step 9: Lint + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres/migrations/0035_stream_table.sql \
        src/control-plane/core/src/stream.rs src/control-plane/core/src/lib.rs \
        src/control-plane/memory/src/stream.rs src/control-plane/memory/src/lib.rs \
        src/control-plane/postgres/src/stream.rs src/control-plane/testkit/src/lib.rs \
        src/control-plane/postgres/tests/stream.rs src/control-plane/memory/tests/stream.rs \
        src/control-plane/postgres/.sqlx
git commit -m "feat(stream): StreamTables concern (stream.stream_table mode registry)"
```

---

### Task 2: Universal log-framing columns + change_kind at write sites

Adds `loom_change_kind`, `loom_bucket`, `loom_offset` to every inline table and sets the change-kind at the mutation write site. No stream stamping yet (that's Task 3) — `loom_bucket`/`loom_offset` stay `NULL`; `loom_change_kind` is `'+I'` for appends (column default) and `'-D'`/`'+U'` for deltas.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (`inline_ddl`, `ensure_inline_schema`, `write_inline_delta`)
- Test: `src/control-plane/postgres/tests/stream_inline.rs` (new) + BUCK target

**Interfaces:**
- Consumes: nothing new.
- Produces: inline tables carry `loom_change_kind text not null default '+I'`, `loom_bucket int` (nullable), `loom_offset bigint` (nullable). `write_inline_delta` sets `loom_change_kind='-D'` (tombstone) / `'+U'` (version).

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/stream_inline.rs`. Mirror the pool/columns/batch/lineage construction in the existing `src/control-plane/postgres/tests/inline_tombstone.rs` and `tests/inline_delta_cas.rs` (read them for the exact `PgFixture` setup, `ColumnSpec`/`RecordBatch` builders, and the `write_inline_delta` call shape). This first test asserts the framing columns exist and carry the right change-kind:

```rust
// After an inline_append of one row to a fresh (batch) table, then a
// write_inline_delta tombstone for an identity table, assert loom_change_kind.
#[tokio::test]
async fn append_row_is_plus_i_with_null_bucket_offset() {
    let fixture = PgFixture::shared();
    let pool = fixture.fresh_pool().await; // mirror the helper used by inline_tombstone.rs
    // ... build TableRef, columns, a 1-row RecordBatch, LineageEvent as in inline_tombstone.rs ...
    inline_append(&pool, &table, &columns, &batch, lineage, None, None).await.expect("append");
    // Query the inline table for the row's framing columns:
    let row = sqlx::query!(
        "select loom_change_kind, loom_bucket, loom_offset from iceberg_mirror.inline_{tid} ... "
        // build the relation name from the tid returned/looked up, as inline_tombstone.rs does
    );
    assert_eq!(change_kind, "+I");
    assert!(bucket.is_none() && offset.is_none(), "batch-table append has no bucket/offset");
}
```

(The exact query — resolving `inline_<tid>` and reading back — mirrors how `inline_tombstone.rs` verifies the `loom_tombstone` column. Add a second `#[tokio::test]` that does a `write_inline_delta` tombstone and asserts `loom_change_kind = "-D"`, and a version write asserting `"+U"`.)

Note the new `inline_append` arity: it gains a trailing `stream_buckets: Option<i32>` param in Task 3. To keep Task 2 compiling on its own, this test calls the CURRENT arity; when Task 3 adds the param, its own test uses `Some(..)`. If you implement Task 2 and Task 3 in sequence, pass `None` here and update after Task 3 (or write this test against the delta path only, which does not change arity). Prefer testing the delta path (`write_inline_delta`) here so Task 2's test is arity-stable.

- [ ] **Step 2: Run it — expect FAIL** (columns don't exist yet)

Run: `buck2 test //src/control-plane/postgres:stream-inline > /tmp/claude-1000/t2.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/claude-1000/t2.log`
Expected: FAIL — `column "loom_change_kind" does not exist` (or compile error if the target/columns are absent).

Add the BUCK target first so the test builds:
```python
loom_fixture_test(
    name = "stream-inline",
    crate = "stream_inline",
    srcs = ["tests/stream_inline.rs"],
    crate_root = "tests/stream_inline.rs",
    deps = [":postgres", "//src/control-plane/core:core", "//third-party:arrow", "//third-party:tokio"],
)
```
(Match deps to what `inline_tombstone`'s target uses — copy that stanza's dep list.)

- [ ] **Step 3: Add the framing columns to the inline DDL**

In `src/control-plane/postgres/src/iceberg_inline.rs`, change `inline_ddl`'s CREATE string to add the three columns after `loom_tombstone`:

```rust
    Ok(format!(
        "create table if not exists {} (\
           loom_row_id bigserial primary key, \
           begin_snapshot bigint not null, \
           end_snapshot bigint, \
           loom_tombstone boolean not null default false, \
           loom_change_kind text not null default '+I', \
           loom_bucket int, \
           loom_offset bigint{cols})",
        inline_table_name(table_id),
    ))
```

And in `ensure_inline_schema`, add idempotent `ALTER` lines for pre-existing tables (mirroring the `loom_tombstone` alter):

```rust
    run_idempotent_ddl(&mut *conn, inline_ddl(tid, columns)?).await?;
    let alter_tomb = format!(
        "alter table {} add column if not exists loom_tombstone boolean not null default false",
        inline_table_name(tid),
    );
    run_idempotent_ddl(&mut *conn, alter_tomb).await?;
    let alter_kind = format!(
        "alter table {} add column if not exists loom_change_kind text not null default '+I'",
        inline_table_name(tid),
    );
    run_idempotent_ddl(&mut *conn, alter_kind).await?;
    let alter_bucket = format!(
        "alter table {} add column if not exists loom_bucket int",
        inline_table_name(tid),
    );
    run_idempotent_ddl(&mut *conn, alter_bucket).await?;
    let alter_offset = format!(
        "alter table {} add column if not exists loom_offset bigint",
        inline_table_name(tid),
    );
    run_idempotent_ddl(&mut *conn, alter_offset).await?;
    Ok(())
```

(Pre-existing rows default to `loom_change_kind='+I'`; a tombstone-aware backfill is unnecessary — `change_kind` is only consumed for stream tables, which are new, so no existing table has a changelog reader.)

- [ ] **Step 4: Set change_kind at the delta write sites**

In `write_inline_delta`, add `loom_change_kind` to both INSERTs.

Tombstone branch:
```rust
        let sql = format!(
            "insert into {} (begin_snapshot, loom_tombstone, loom_change_kind, \"{}\") \
             values ($1, true, '-D', $2)",
            inline_table_name(tid),
            id_column.replace('"', "\"\""),
        );
```

Version branch:
```rust
        let sql = format!(
            "insert into {} (begin_snapshot, loom_tombstone, loom_change_kind, {col_list}) \
             values ($1, false, '+U', {placeholders})",
            inline_table_name(tid),
        );
```

(`inline_append` needs no change for change_kind — its INSERT omits the column, so the `'+I'` default applies.)

- [ ] **Step 5: Run the test — expect PASS**

Run: `buck2 test //src/control-plane/postgres:stream-inline > /tmp/claude-1000/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/claude-1000/t2.log`
Expected: PASS. Also run the existing inline tests to confirm no regression:
`buck2 test //src/control-plane/postgres:inline-tombstone //src/control-plane/postgres:inline-delta-cas > /tmp/claude-1000/t2b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/claude-1000/t2b.log` (use the real target names — confirm via the BUCK file).

- [ ] **Step 6: Lint + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres/src/iceberg_inline.rs src/control-plane/postgres/tests/stream_inline.rs src/control-plane/postgres/BUCK
git commit -m "feat(stream): universal loom_change_kind/loom_bucket/loom_offset inline columns"
```

---

### Task 3: Stream-aware `inline_append` — bucket assignment + offset stamping

Makes `inline_append` stamp `loom_bucket`/`loom_offset` for declared stream tables, allocating gapless per-bucket offsets inside the append's transaction. Adds declaration/validation driven by a new `stream_buckets: Option<i32>` param threaded through `land`.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (`inline_append`)
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (`land`)
- Modify: all `inline_append`/`land` call sites to add the new param (grep first)
- Modify: `src/services/ingest/src/landing.rs` (`IcebergMaterializer::land` passes `None` for now — Task 4 wires the real value)
- Test: `src/control-plane/postgres/tests/stream_inline.rs` (extend)

**Interfaces:**
- Consumes: `crate::stream::pg_stream_bucket_count`, `crate::stream::pg_declare_stream` (Task 1), `crate::stream::pg_allocate_offset` (Slice 0), `live_table_id` (existing in `iceberg_inline.rs`).
- Produces: `inline_append(pool, table, columns, batch, lineage, flush_threshold, stream_buckets: Option<i32>) -> Result<SnapshotId>` and `land(pool, catalog, table, columns, schema, batches, limits, lineage, stream_buckets: Option<i32>) -> Result<SnapshotId>`. `stream_buckets = Some(n)` means "this write declares/expects a stream table with n buckets"; `None` means "no stream intent" (append using recorded mode).

- [ ] **Step 1: Write the failing test** (extend `tests/stream_inline.rs`)

```rust
#[tokio::test]
async fn stream_append_stamps_gapless_per_bucket_offsets() {
    let fixture = PgFixture::shared();
    let pool = fixture.fresh_pool().await;
    // ... build TableRef, columns, a 4-row RecordBatch, lineage (mirror inline_tombstone.rs) ...
    // First append declares the table as a stream with 2 buckets:
    inline_append(&pool, &table, &columns, &batch4, lineage1, None, Some(2)).await.expect("append");
    // rows 0,2 -> bucket 0 (offsets 0,1); rows 1,3 -> bucket 1 (offsets 0,1)
    // Query loom_bucket/loom_offset ordered by loom_row_id and assert:
    //   bucket sequence [0,1,0,1], offset sequence [0,0,1,1]
    // Second append of 2 more rows continues each bucket's offsets:
    inline_append(&pool, &table, &columns, &batch2, lineage2, None, None).await.expect("append 2");
    //   bucket 0 next offset = 2, bucket 1 next offset = 2
    // Assert bucket 0's offsets are {0,1,2} gapless, bucket 1's are {0,1,2} gapless.
    // Conflicting bucket count is rejected:
    let err = inline_append(&pool, &table, &columns, &batch1, lineage3, None, Some(3)).await;
    assert!(matches!(err, Err(ControlPlaneError::Conflict(_))), "bucket count mismatch is a Conflict");
    // A non-stream table (no Some) stamps NULL:
    // ... separate table, inline_append(..., None) -> loom_bucket/offset NULL ...
}
```

- [ ] **Step 2: Run it — expect FAIL** (arity mismatch / no stamping)

Run: `buck2 test //src/control-plane/postgres:stream-inline > /tmp/claude-1000/t3.log 2>&1; grep -E "FAIL|error\[|Tests finished" /tmp/claude-1000/t3.log`
Expected: compile error (arity) → then after Step 3, a logical failure until stamping is implemented.

- [ ] **Step 3: Make `inline_append` stream-aware**

In `src/control-plane/postgres/src/iceberg_inline.rs`, change the signature and add reconcile + stamping. Add the param:

```rust
pub async fn inline_append(
    pool: &PgPool,
    table: &TableRef,
    columns: &[ColumnSpec],
    batch: &RecordBatch,
    lineage: LineageEvent,
    flush_threshold: Option<i64>,
    stream_buckets: Option<i32>,
) -> Result<SnapshotId> {
```

After `let tid = ensure_table(conn, &table.schema, &table.name, at).await?;`, detect pre-existence BEFORE `ensure_table` (so add, just above the `next_snapshot` call):

```rust
    // Detect whether the table already existed (for the batch->stream conversion guard).
    let pre_existing = live_table_id(&mut *conn, &table.schema, &table.name).await?.is_some();
```

Then, after `tid` is known and columns are projected (after the `ensure_inline_schema` call is fine), reconcile the stream mode:

```rust
    // Reconcile stream mode. `effective` = Some(bucket_count) iff this table is a
    // (now-)declared log table; None => batch table (no offset stamping).
    let existing = crate::stream::pg_stream_bucket_count(&mut *conn, tid).await?;
    let effective: Option<i32> = match (stream_buckets, existing) {
        (Some(n), Some(m)) if n != m => {
            return Err(ControlPlaneError::Conflict(format!(
                "stream bucket count mismatch for {}.{}: requested {n}, table has {m}",
                table.schema, table.name
            )));
        }
        (Some(_), Some(m)) => Some(m),
        (Some(n), None) => {
            if pre_existing {
                return Err(ControlPlaneError::Validation(format!(
                    "cannot convert existing batch table {}.{} to a stream table",
                    table.schema, table.name
                )));
            }
            crate::stream::pg_declare_stream(&mut *conn, tid, n).await?;
            Some(n)
        }
        (None, existing) => existing,
    };
```

Then replace the INSERT construction + row loop with a stream-aware version. When `effective` is `Some(bc)`, prepend `loom_bucket, loom_offset` to the column list, pre-allocate a per-bucket offset run, and bind bucket/offset per row:

```rust
    let col_list = columns
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");

    if let Some(bc) = effective {
        // Per-row bucket = row_index % bc (v1 simplification; bc>=1).
        let n = batch.num_rows();
        // Count rows per bucket and reserve a contiguous offset run per touched bucket.
        let mut counts = vec![0i64; bc as usize];
        for row in 0..n {
            let b = (row as i32 % bc) as usize;
            counts[b] += 1;
        }
        // cursor[b] = next offset to assign for bucket b (first of the reserved run).
        let mut cursor = vec![0i64; bc as usize];
        for b in 0..bc as usize {
            if counts[b] > 0 {
                cursor[b] = crate::stream::pg_allocate_offset(&mut *conn, tid, b as i32, counts[b]).await?;
            }
        }
        // $1 begin_snapshot, $2 loom_bucket, $3 loom_offset, then data columns from $4.
        let placeholders = (0..columns.len())
            .map(|i| format!("${}", i + 4))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_sql = format!(
            "insert into {} (begin_snapshot, loom_bucket, loom_offset, {col_list}) \
             values ($1, $2, $3, {placeholders})",
            inline_table_name(tid),
        );
        for row in 0..n {
            let b = row as i32 % bc;
            let offset = cursor[b as usize];
            cursor[b as usize] += 1;
            let cells = columns
                .iter()
                .enumerate()
                .map(|(c, spec)| cell_from_arrow(batch, c, row, &spec.ty))
                .collect::<Result<Vec<_>>>()?;
            let mut q = sqlx::query(AssertSqlSafe(insert_sql.clone()))
                .bind(at.0)
                .bind(b)
                .bind(offset);
            for cell in &cells {
                q = bind_cell(q, cell);
            }
            q.execute(&mut *conn).await.map_err(backend)?;
        }
    } else {
        // Batch table: unchanged behaviour (loom_bucket/loom_offset stay NULL,
        // loom_change_kind defaults to '+I').
        let placeholders = (0..columns.len())
            .map(|i| format!("${}", i + 2))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_sql = format!(
            "insert into {} (begin_snapshot, {col_list}) values ($1, {placeholders})",
            inline_table_name(tid),
        );
        for row in 0..batch.num_rows() {
            let cells = columns
                .iter()
                .enumerate()
                .map(|(c, spec)| cell_from_arrow(batch, c, row, &spec.ty))
                .collect::<Result<Vec<_>>>()?;
            let mut q = sqlx::query(AssertSqlSafe(insert_sql.clone())).bind(at.0);
            for cell in &cells {
                q = bind_cell(q, cell);
            }
            q.execute(&mut *conn).await.map_err(backend)?;
        }
    }
```

(`vec!`/indexing here is test-independent production code — `indexing_slicing` is a denied lint. Use `.get(b).copied().unwrap_or(0)` style or iterate safely to avoid it: replace `counts[b] += 1` and `cursor[b]` reads with checked access, e.g. accumulate counts via an iterator and read cursors with `*cursor.get_mut(b).ok_or_else(|| ControlPlaneError::Backend("bucket index".into()))? ` — the implementer must keep it panic-free per the clippy policy. `b` is always `< bc` by construction, but the lint requires non-panicking access.)

- [ ] **Step 4: Thread the param through `land` and update call sites**

In `src/control-plane/postgres/src/iceberg_landing.rs`, add `stream_buckets: Option<i32>` to `land`'s signature (last param) and pass it to `inline_append`; pass it to `land_parquet` too (add the param to `land_parquet`, which for Plan 1a **declares** the mode but does not stamp — it calls `pg_declare_stream` after its own `ensure_table` when `stream_buckets` is `Some` and the table is new; offset stamping on the Parquet path is Plan 1b, add a `// TODO(plan-1b): stamp offsets on the Parquet path` is NOT allowed — instead leave a plain doc comment noting Parquet-path stamping is out of Plan 1a scope):

```rust
        inline_append(
            pool,
            table,
            columns,
            &batch,
            lineage,
            Some(limits.flush_byte_threshold),
            stream_buckets,
        )
        .await
    } else {
        land_parquet(pool, catalog, table, columns, batches, lineage, stream_buckets).await
    }
```

Grep every caller of `inline_append` and `land` and add the new trailing argument (`None` unless the caller has stream intent):
`grep -rn "inline_append(\|iceberg_landing::land(\| land(" src/ --include=*.rs` (and the re-exported name). In `src/services/ingest/src/landing.rs`, `IcebergMaterializer::land` passes `req.stream_buckets` — but `LandRequest` gains that field in Task 4; for Task 3, pass `None` and let Task 4 replace it.

- [ ] **Step 5: Run tests — expect PASS**

Run: `buck2 test //src/control-plane/postgres:stream-inline //src/control-plane/postgres:inline-tombstone //src/control-plane/postgres:inline-delta-cas > /tmp/claude-1000/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/claude-1000/t3.log`
Expected: all pass (stream stamping works; existing inline paths unaffected since they pass `None`).

- [ ] **Step 6: Lint + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(stream): stamp per-bucket offsets on stream-table appends"
```

---

### Task 4: Ingest declaration flag (`?mode=stream&buckets=N`)

Wires the HTTP surface: parse the query params, thread the intent to `inline_append`, and map declaration errors to 400.

**Files:**
- Modify: `src/services/ingest/src/http.rs` (`land` handler)
- Modify: `src/services/ingest/src/landing.rs` (`LandRequest` + `IcebergMaterializer::land`)
- Test: `src/services/ingest/tests/stream_declare.rs` (new) + BUCK target

**Interfaces:**
- Consumes: `land(... , stream_buckets)` (Task 3).
- Produces: `LandRequest.stream_buckets: Option<i32>`; `POST /datasets/{schema}/{table}?mode=stream&buckets=N` declares a log table on first write; declaration errors → HTTP 400.

- [ ] **Step 1: Write the failing e2e test**

Create `src/services/ingest/tests/stream_declare.rs`, mirroring the harness in `src/services/ingest/tests/http_land.rs` (read it for the router/app construction, Arrow-IPC body encoding, and request helpers):

```rust
// 1. POST ?mode=stream&buckets=2 on a fresh table -> 200; stream.stream_table row present with bucket_count=2.
// 2. A second POST to the same table with ?mode=stream&buckets=3 -> 400 (bucket mismatch).
// 3. A POST with ?mode=stream to a table that already exists as batch -> 400 (no conversion).
// 4. A plain POST (no mode) to a fresh table -> 200; no stream.stream_table row.
```

- [ ] **Step 2: Run it — expect FAIL**

Run: `buck2 test //src/services/ingest:stream-declare > /tmp/claude-1000/t4.log 2>&1; grep -E "FAIL|error\[|Tests finished" /tmp/claude-1000/t4.log`
Expected: compile error / 404-or-500 (params not parsed yet).

Add the BUCK target (mirror `http-land`'s deps):
```python
loom_fixture_test(
    name = "stream-declare",
    crate = "stream_declare",
    srcs = ["tests/stream_declare.rs"],
    crate_root = "tests/stream_declare.rs",
    deps = [
        ":ingest", "//src/services/runtime:runtime", "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres", "//third-party:arrow", "//third-party:axum",
        "//third-party:http-body-util", "//third-party:iceberg", "//third-party:serde_json",
        "//third-party:sqlx", "//third-party:tempfile", "//third-party:tokio", "//third-party:tower",
    ],
)
```

- [ ] **Step 3: Add `stream_buckets` to `LandRequest` + materializer**

In `src/services/ingest/src/landing.rs`, add to `LandRequest`:
```rust
    /// Some(n) if the caller declared this as a stream (log) table with n buckets.
    pub stream_buckets: Option<i32>,
```
and in `IcebergMaterializer::land`, pass it as the new trailing arg to `iceberg_land(...)` (replacing the `None` placeholder from Task 3):
```rust
            req.lineage,
            req.stream_buckets,
```

- [ ] **Step 4: Parse the query params in the handler**

In `src/services/ingest/src/http.rs`, add a params struct and extractor:
```rust
#[derive(serde::Deserialize)]
pub(crate) struct StreamParams {
    mode: Option<String>,
    buckets: Option<i32>,
}
```
Add `Query(params): Query<StreamParams>` to the `land` handler signature (import `axum::extract::Query`), compute the intent, set it on the request, and validate `buckets`:
```rust
    let stream_buckets = if params.mode.as_deref() == Some("stream") {
        let n = params.buckets.unwrap_or(1);
        if n < 1 {
            return Err(ApiError::BadRequest(Cow::Borrowed("buckets must be >= 1")));
        }
        Some(n)
    } else {
        None
    };
```
and set `stream_buckets` on the constructed `LandRequest`.

- [ ] **Step 5: Map declaration errors to 400**

Declaration/validation failures surface as `ControlPlaneError::Conflict` / `::Validation` wrapped in `IngestError`. Ensure the handler maps these to **400**, not 500. Inspect how `IngestError` → `ApiError` maps today (`into_api`); if `Conflict`/`Validation` currently map to 500, add explicit arms so a stream mode conflict / batch-conversion / bucket-mismatch returns `ApiError::BadRequest` with the error's message. (Check `src/services/ingest/src/*` for the `IngestError`/`ApiError` mapping; add the arms there.)

- [ ] **Step 6: Run the e2e — expect PASS**

Run: `buck2 test //src/services/ingest:stream-declare //src/services/ingest:http-land > /tmp/claude-1000/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/claude-1000/t4.log`
Expected: both pass (new declaration e2e green; existing http-land unaffected — a no-`mode` request still lands as batch).

- [ ] **Step 7: Regenerate sqlx cache if any query changed, then lint + commit**

(No new `query!` is expected in Task 4; if you added one, run `tools/sqlx-prepare.sh`.)
```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(stream): ingest ?mode=stream&buckets=N declaration flag"
```

---

## Self-Review

**Spec coverage (Plan 1a scope):**
- "`StreamTables` control-plane concern + `stream.stream_table`" → Task 1. ✓
- "ingest-time declaration flag `?mode=stream&buckets=N`, immutable, 400 on conversion/mismatch, omit-allowed" → Task 4 (+ reconcile in Task 3). ✓
- "universal `loom_change_kind`/`loom_bucket`/`loom_offset` columns; `+I` default; `-D`/`+U` at delta sites; `-U` deferred" → Task 2. ✓
- "bucket assignment `row_index % bucket_count`; per-bucket gapless offset reserved in the append's transaction" → Task 3. ✓
- "batch path byte-identical" → Task 3 batch branch + Task 2 default; asserted in tests. ✓
- Out of 1a (durable flush persistence, `is_reserved` filter, Parquet-path stamping, subscribe, `-U`, PK mutation) → explicitly excluded in File Structure. ✓

**Placeholder scan:** The `stream_inline.rs` / `stream_declare.rs` tests reference sibling harnesses (`inline_tombstone.rs`, `http_land.rs`) for boilerplate batch/router construction — this is "reuse the existing test harness," with the novel assertions shown in full; not a content placeholder. No TBD/TODO. The plan explicitly forbids `TODO` comments in code (clippy `todo`/comment policy). ✓

**Type consistency:** `stream_buckets: Option<i32>` is identical across `inline_append`, `land`, `land_parquet`, `LandRequest`, and `StreamParams→Some(i32)`. `declare_stream(i64, i32)` / `stream_bucket_count(i64) -> Option<i32>` identical across core/memory/postgres/contract. `pg_allocate_offset(&mut *conn, tid, b: i32, count: i64) -> i64` matches Slice 0's signature. Framing column names (`loom_change_kind`/`loom_bucket`/`loom_offset`) identical in DDL, ALTERs, INSERTs, and test queries. ✓

---

## Execution Handoff

After the plan is approved, implement task-by-task via **superpowers:subagent-driven-development** (fresh subagent per task + two-stage review) or **superpowers:executing-plans**. Each task ends green + committed on `feat/stream-log-tables`. Plan 1b (durable flush persistence) follows as its own plan; `road-stream-log-tables` stays open until both land.
