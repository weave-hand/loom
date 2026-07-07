# Stream Log Tables — Plan 1b (durable persistence) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a stream/log table's `(loom_change_kind, loom_bucket, loom_offset)` framing durable in Iceberg across both write paths — flush-from-inline and direct large-write — while keeping the framing invisible to every logical read, and gapless/atomic on both paths.

**Architecture:** Framing columns become part of a stream table's **Iceberg physical schema** (so mirror registration is automatic via `columns_of`) and are stamped into the written Parquet. A single `is_reserved(name)` filter at the one logical-schema chokepoint (`IcebergCatalog::schema`) hides them from all reads; flush uses an unfiltered physical read to carry them through. The direct large-write Parquet path gains full parity with the inline path: atomic Conflict/Validation reconcile plus gapless per-bucket offset allocation that commits **iff** the snapshot commits — achieved by threading the offset allocation onto the same Postgres transaction as the pointer-CAS commit (a caller-provided-tx refactor of `do_update_table`).

**Tech Stack:** Rust, buck2, sqlx (dynamic `AssertSqlSafe` + compile-time macros), Arrow 58, Iceberg (pinned git), Postgres control plane, `loom_fixture_test` integration tests.

## Global Constraints

- **Tests are `rust_test` / `loom_fixture_test` integration targets only** — never inline `#[cfg(test)]`. Anything that boots Postgres/MinIO uses `loom_fixture_test` (not bare `rust_test`), added to the crate `BUCK` mirroring an existing target's deps.
- **Strict clippy (pedantic + restriction) on `src/**`**: no `unwrap`/`expect`/`panic`/`todo`/`dbg`/`unimplemented`/`indexing_slicing`/`get_unwrap`. Use `?`, `.get(..).ok_or(..)`, `try_from`. Test code is exempt (via `loom_fixture_test` / `loom_rust_test`).
- **Result import:** `use control_plane_core::{..., Result};` (the `error` module is private — `control_plane_core::error::Result` does NOT compile). Match sibling `queue.rs` / `stream.rs`.
- **Batch-table behavior MUST stay byte-identical.** A non-stream table's ingest → flush → read path must produce the exact same Parquet, mirror `column` rows, and read schema as before this plan. Every stream-specific branch is gated on `stream_bucket_count(tid).is_some()`.
- **Framing columns** are exactly three, `loom_`-prefixed: `loom_change_kind` (logical `"string"`, NOT NULL), `loom_bucket` (logical `"integer"`, nullable), `loom_offset` (logical `"long"`, nullable). Offsets are 0-indexed per bucket, gapless, monotonic.
- **rustfmt is apply-on-command:** `buck2 run //tools:rustfmt -- <files>` to APPLY (the prek hook only *checks*). Run rustfmt, then `buck2 run //tools:prek -- run --all-files`, before every commit.
- **No new compile-time `query!`/`query_scalar!` macros unless necessary.** Migration 0036 is DDL (no macro). If any macro is added/changed, regenerate with `tools/sqlx-prepare.sh` and commit `.sqlx`. The `sqlx-cache-check` test re-validates the committed cache against the live schema in the normal test sweep.
- **buck2 test hygiene:** never pipe `buck2 test` through `tail`/`head`; redirect to a file and grep (`buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`). Locally, run the full postgres suite with `-j 8` (fixture boot-slot starvation otherwise → flaky 120s timeouts).

---

## File Structure

- `src/control-plane/postgres/src/iceberg_catalog.rs` — add `is_reserved` filter to `schema()`; add an unfiltered physical-column read helper used by flush.
- `src/control-plane/postgres/src/iceberg_landing.rs` — `framing_column_specs()` helper; inject framing specs into `ice_schema`/`ensure_iceberg_table` for stream tables; the new atomic direct-large-write stream Parquet path + reconcile parity.
- `src/control-plane/postgres/src/iceberg_inline.rs` — register framing columns in the mirror at stream declaration; flush's `inline_live_batch` reads framing via the physical read + decodes them.
- `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs` — refactor `do_update_table` to run on a caller-provided tx (extract `do_update_table_in_tx`); add the stream offset-allocation extra to `CommitExtras`/`apply_commit_extras`.
- `src/control-plane/postgres/migrations/0036_stream_table_bucket_count_check.sql` — new migration, `bucket_count >= 1` CHECK.
- Tests: `tests/stream_reserved_schema.rs`, `tests/stream_bucket_check.rs`, `tests/stream_flush_persist.rs`, `tests/stream_parquet_atomic.rs` (all `loom_fixture_test`), each wired into `src/control-plane/postgres/BUCK`.

---

## Task 1: `is_reserved` filter + physical-read seam

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_catalog.rs:276-311` (the `schema` impl) + add helper
- Test: `src/control-plane/postgres/tests/stream_reserved_schema.rs` (new, `loom_fixture_test`)
- Modify: `src/control-plane/postgres/BUCK` (new test target)

**Interfaces:**
- Produces: `pub(crate) fn is_reserved(name: &str) -> bool` — `true` for `loom_`-prefixed names and the MVCC bookkeeping names `begin_snapshot`/`end_snapshot`. Used by later tasks and by flush.
- Produces: an unfiltered mirror-column read reachable from `inline_live_batch` (Task 3 consumes it): keep the raw column read available so flush sees reserved columns while `schema()` hides them.

**Context:** `IcebergCatalog::schema` is the single authoritative logical-schema derivation point; every read (engine `arrow_schema_from_mirror`, `GET /datasets`, previews, merge view) chains from it. Filtering here hides reserved columns everywhere at once. But flush's `inline_live_batch` (Task 3) also calls `schema()` to decide which inline columns to SELECT — flush must NOT be filtered, or the framing never reaches Parquet. So split the read: an internal unfiltered reader + a filtered `schema()`.

- [ ] **Step 1: Write the failing test**

`src/control-plane/postgres/tests/stream_reserved_schema.rs`:
```rust
//! `IcebergCatalog::schema` hides `loom_`-prefixed reserved columns from the
//! logical schema, while the physical read still sees them.
use control_plane_postgres::fixture::TestDb;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_core::{Catalog, TableRef};

#[tokio::test]
async fn schema_hides_reserved_columns() {
    let db = TestDb::boot().await;
    let ice = IcebergCatalog::new(db.pool.clone());
    let table = TableRef { schema: "s".into(), name: "t".into() };

    // Seed a table whose mirror carries a user column AND a synthetic reserved
    // `loom_` column at the same snapshot.
    let tid = db.seed_table_with_columns(
        &table,
        &[("amount", "long", false), ("loom_offset", "long", true)],
    ).await;

    let at = db.current_snapshot_id(&table).await;
    let logical = ice.schema(&table, at).await.expect("schema");
    let names: Vec<_> = logical.columns.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"amount"), "user column present: {names:?}");
    assert!(!names.contains(&"loom_offset"), "reserved column hidden: {names:?}");

    // The physical (unfiltered) read still sees it — flush relies on this.
    let physical = ice.physical_columns(tid, at).await.expect("physical");
    let pnames: Vec<_> = physical.iter().map(|c| c.name.as_str()).collect();
    assert!(pnames.contains(&"loom_offset"), "physical sees reserved: {pnames:?}");
}
```
> Note: `seed_table_with_columns`/`current_snapshot_id` — if the fixture lacks these helpers, seed directly via `project_columns` + `ensure_table` (see `iceberg_inline` tests for the pattern) and read `tid` via `live_table_id`. Keep the assertion shape: user col present, reserved col absent from `schema()`, present in `physical_columns()`.

- [ ] **Step 2: Run it to confirm it fails**

Run: `buck2 test //src/control-plane/postgres:stream-reserved-schema > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error\[|error:" /tmp/t1.log`
Expected: FAIL — `physical_columns` undefined and/or `loom_offset` leaks into `schema()`.

- [ ] **Step 3: Add `is_reserved` + split the read**

In `iceberg_catalog.rs`, add near the top-level fns:
```rust
/// Physical bookkeeping columns that must never appear in a table's logical
/// (user-facing) schema. `loom_`-prefixed framing/bookkeeping plus the MVCC
/// snapshot bounds (which are inline-only today, filtered here defensively).
pub(crate) fn is_reserved(name: &str) -> bool {
    name.starts_with("loom_") || name == "begin_snapshot" || name == "end_snapshot"
}
```
Extract the mirror-column read into an unfiltered helper on `IcebergCatalog` (move the existing query body here verbatim; it returns ALL columns):
```rust
/// Read the raw mirror columns for `tid` live at `at`, INCLUDING reserved
/// (`loom_`) columns. The logical `schema()` filters these out; the flush path
/// needs them to carry stream framing into Parquet.
pub(crate) async fn physical_columns(
    &self,
    tid: i64,
    at: SnapshotId,
) -> Result<Vec<ColumnDef>> {
    let rows = sqlx::query!(
        "select column_order as \"column_order!\", column_name as \"column_name!\", \
                column_type as \"column_type!\", nulls_allowed as \"nulls_allowed!\" \
         from iceberg_mirror.column \
         where table_id = $1 and begin_snapshot <= $2 and (end_snapshot is null or end_snapshot > $2) \
         order by column_order",
        tid,
        at.0,
    )
    .fetch_all(&self.pool)
    .await
    .map_err(backend)?;
    rows.into_iter()
        .map(|r| {
            let ty = logical_from_iceberg(&r.column_type)
                .map(BaseType::canonical_name)
                .ok_or_else(|| backend(format!("unknown iceberg column type `{}`", r.column_type)))?;
            Ok(ColumnDef {
                order: r.column_order,
                name: r.column_name,
                ty: ty.to_string(),
                nullable: r.nulls_allowed,
            })
        })
        .collect()
}
```
> Preserve the exact error construction the original `schema()` used for the unknown-type case (copy it verbatim rather than the `backend(format!…)` shorthand above if the original differs — do not weaken the error). This is a pure extraction: no behavior change to the query.

Then make `schema()` resolve the tid, call `physical_columns`, and filter:
```rust
async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema> {
    let tid = self.resolve_table(table, at).await?;
    let columns = self
        .physical_columns(tid, at)
        .await?
        .into_iter()
        .filter(|c| !is_reserved(&c.name))
        .collect();
    Ok(TableSchema { columns })
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `buck2 test //src/control-plane/postgres:stream-reserved-schema > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|test result" /tmp/t1.log`
Expected: PASS.

- [ ] **Step 5: Regression — every reader path still green**

The `schema()` refactor is on the hot read path. Run the readers that chain from it:
`buck2 test //src/control-plane/postgres:iceberg-flush //src/control-plane/postgres:iceberg-inline //src/services/engine-serving/... //src/services/query-api/... > /tmp/t1r.log 2>&1; grep -E "Tests finished|FAIL|test result" /tmp/t1r.log`
Expected: all green (no logical schema changed — no real table has reserved mirror columns yet).

- [ ] **Step 6: Lint + commit**

```bash
buck2 run //tools:rustfmt -- src/control-plane/postgres/src/iceberg_catalog.rs src/control-plane/postgres/tests/stream_reserved_schema.rs
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres/src/iceberg_catalog.rs src/control-plane/postgres/tests/stream_reserved_schema.rs src/control-plane/postgres/BUCK
git commit -m "feat(stream): is_reserved logical-schema filter + physical-column read seam"
```

---

## Task 2: Migration 0036 — `bucket_count >= 1` CHECK

**Files:**
- Create: `src/control-plane/postgres/migrations/0036_stream_table_bucket_count_check.sql`
- Test: `src/control-plane/postgres/tests/stream_bucket_check.rs` (new, `loom_fixture_test`)
- Modify: `src/control-plane/postgres/BUCK`

**Context:** Migrations are strictly append-only (sqlx `_sqlx_migrations` tracks applied checksums; editing 0035 would break already-migrated DBs). DB-level hardening behind Plan 1a's in-code `bucket_count >= 1` Validation guard.

- [ ] **Step 1: Write the failing test**

`src/control-plane/postgres/tests/stream_bucket_check.rs`:
```rust
//! The `bucket_count >= 1` CHECK on `stream.stream_table` rejects a non-positive
//! bucket count at the database layer (defense-in-depth under the in-code guard).
use control_plane_postgres::fixture::TestDb;
use control_plane_postgres::stream::pg_declare_stream;

#[tokio::test]
async fn declare_zero_buckets_is_rejected_by_check() {
    let db = TestDb::boot().await;
    let mut conn = db.pool.acquire().await.expect("conn");
    let err = pg_declare_stream(&mut *conn, 4242, 0).await;
    assert!(err.is_err(), "bucket_count = 0 must be rejected by the CHECK");
}
```
> `pg_declare_stream` is `pub(crate)`; this test is inside the postgres crate so it can reach it. If it is not re-exported to tests, insert directly with a raw `sqlx::query` against `stream.stream_table` instead and assert the error.

- [ ] **Step 2: Run it to confirm it fails**

Run: `buck2 test //src/control-plane/postgres:stream-bucket-check > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|test result" /tmp/t2.log`
Expected: FAIL — `bucket_count = 0` currently inserts successfully (no CHECK yet).

- [ ] **Step 3: Add the migration**

`src/control-plane/postgres/migrations/0036_stream_table_bucket_count_check.sql`:
```sql
-- A log table must have at least one bucket; a zero/negative count would make
-- the write-path modulo (`row_index % bucket_count`) meaningless. DB-level
-- backstop behind the in-code Validation guard in `inline_append`.
alter table stream.stream_table
    add constraint bucket_count_positive check (bucket_count >= 1);
```

- [ ] **Step 4: Run test to confirm it passes**

Run: `buck2 test //src/control-plane/postgres:stream-bucket-check > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|test result" /tmp/t2.log`
Expected: PASS.

- [ ] **Step 5: Confirm the sqlx cache stays fresh**

A CHECK constraint changes no column type/nullability, so the committed `.sqlx` cache is unaffected. Prove it:
`buck2 test //src/control-plane/postgres:sqlx-cache-check > /tmp/t2s.log 2>&1; grep -E "Tests finished|FAIL|test result" /tmp/t2s.log`
Expected: PASS (no `tools/sqlx-prepare.sh` run needed). If it fails, run `tools/sqlx-prepare.sh` and commit the `.sqlx` delta.

- [ ] **Step 6: Lint + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add src/control-plane/postgres/migrations/0036_stream_table_bucket_count_check.sql src/control-plane/postgres/tests/stream_bucket_check.rs src/control-plane/postgres/BUCK
git commit -m "feat(stream): bucket_count >= 1 CHECK on stream.stream_table (migration 0036)"
```

---

## Task 3: Flush-path framing persistence

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (add `framing_column_specs`; inject into `ensure_iceberg_table`/`ice_schema` for stream tables)
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (register framing in mirror at declaration; `inline_live_batch` physical read + decode)
- Test: `src/control-plane/postgres/tests/stream_flush_persist.rs` (new, `loom_fixture_test`)
- Modify: `src/control-plane/postgres/BUCK`

**Interfaces:**
- Consumes: `is_reserved`, `IcebergCatalog::physical_columns` (Task 1); `stream_bucket_count`/`pg_stream_bucket_count` (Plan 1a).
- Produces: `pub(crate) fn framing_column_specs() -> Vec<ColumnSpec>` returning the three framing specs in fixed order:
  ```rust
  vec![
      ColumnSpec { name: "loom_change_kind".into(), ty: "string".into(),  nullable: false },
      ColumnSpec { name: "loom_bucket".into(),      ty: "integer".into(), nullable: true },
      ColumnSpec { name: "loom_offset".into(),      ty: "long".into(),    nullable: true },
  ]
  ```

**Context:** For a stream table the framing must live in the **Iceberg physical schema** (then mirror registration is automatic via `columns_of(&staged_table)` in the commit path). Flush reads inline rows via `inline_live_batch`, which builds its SELECT list from a schema — switch that to the **physical** read so it picks up the framing (which are real physical columns of `inline_<tid>` since Plan 1a). Register the framing in the mirror at stream **declaration** (first inline append), so `physical_columns` returns them and `is_reserved` (Task 1) hides them from reads. Batch tables never register framing → `ice_schema`/mirror unchanged → byte-identical.

- [ ] **Step 1: Write the failing test**

`src/control-plane/postgres/tests/stream_flush_persist.rs` — two tests:
```rust
//! A stream table's framing columns survive the inline → Iceberg flush: they are
//! present in the mirror/Parquet, offsets stay gapless & ordered per bucket
//! across the flush boundary, logical reads never expose them, and a batch
//! table's ingest→flush is byte-identical (no framing, no mirror `column` churn).
use control_plane_postgres::fixture::TestDb;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_core::{Catalog, TableRef};

#[tokio::test]
async fn stream_flush_persists_gapless_framing() {
    let db = TestDb::boot().await;
    let table = TableRef { schema: "s".into(), name: "log".into() };
    // Two appends to a buckets=2 stream table (small → inline), then flush.
    db.stream_append(&table, /*buckets*/ Some(2), &[/* rows */]).await;
    db.stream_append(&table, Some(2), &[/* rows */]).await;
    db.flush(&table).await;

    let ice = IcebergCatalog::new(db.pool.clone());
    let tid = db.live_table_id(&table).await;
    let at = db.current_snapshot_id(&table).await;

    // Physical schema carries all three framing columns; logical hides them.
    let phys: Vec<_> = ice.physical_columns(tid, at).await.unwrap()
        .into_iter().map(|c| c.name).collect();
    for c in ["loom_change_kind", "loom_bucket", "loom_offset"] {
        assert!(phys.contains(&c.to_string()), "framing in physical: {phys:?}");
    }
    let logical: Vec<_> = ice.schema(&table, at).await.unwrap()
        .columns.into_iter().map(|c| c.name).collect();
    for c in ["loom_change_kind", "loom_bucket", "loom_offset"] {
        assert!(!logical.contains(&c.to_string()), "framing hidden from reads: {logical:?}");
    }

    // Offsets are gapless & ordered per bucket across the flush boundary.
    let offsets = db.parquet_framing(&table).await; // Vec<(bucket, offset)>
    assert_gapless_per_bucket(&offsets);
}

#[tokio::test]
async fn batch_flush_is_byte_identical() {
    let db = TestDb::boot().await;
    let table = TableRef { schema: "s".into(), name: "plain".into() };
    db.stream_append(&table, /*buckets*/ None, &[/* rows */]).await; // batch
    db.flush(&table).await;
    let ice = IcebergCatalog::new(db.pool.clone());
    let tid = db.live_table_id(&table).await;
    let at = db.current_snapshot_id(&table).await;
    let phys: Vec<_> = ice.physical_columns(tid, at).await.unwrap()
        .into_iter().map(|c| c.name).collect();
    for c in ["loom_change_kind", "loom_bucket", "loom_offset"] {
        assert!(!phys.contains(&c.to_string()), "batch table has NO framing in mirror: {phys:?}");
    }
}
```
> The fixture helpers (`stream_append`, `flush`, `parquet_framing`, `assert_gapless_per_bucket`) may not exist. Build minimal ones in the test file (or extend the fixture) from existing primitives: `inline_append(...)` for `stream_append`, `flush_table(...)` for `flush`, and read the flushed Parquet via the existing engine/iceberg read helpers used by `iceberg-flush` tests for `parquet_framing`. Keep the assertions exactly as above.

- [ ] **Step 2: Run it to confirm it fails**

Run: `buck2 test //src/control-plane/postgres:stream-flush-persist > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|error\[|error:" /tmp/t3.log`
Expected: FAIL — framing not registered in mirror / not read by flush.

- [ ] **Step 3: Add `framing_column_specs` + inject into the Iceberg schema for stream tables**

In `iceberg_landing.rs`, add `framing_column_specs()` (exact body in Interfaces). Thread an `include_framing: bool` through `ensure_iceberg_table` so a stream table's Iceberg schema includes framing:
```rust
pub(crate) async fn ensure_iceberg_table(
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    include_framing: bool,
) -> Result<()> {
    // ... namespace-exists unchanged ...
    if !catalog.table_exists(&ident).await.map_err(backend)? {
        let mut cols = columns.to_vec();
        if include_framing {
            cols.extend(framing_column_specs());
        }
        let creation = TableCreation::builder()
            .name(table.name.clone())
            .schema(ice_schema(&cols)?)
            .build();
        catalog.create_table(&ns, creation).await.map_err(backend)?;
    }
    Ok(())
}
```
Update `append_parquet_snapshot` (the sole caller besides tests) to pass `include_framing` = whether the table is a stream table. Compute it once from `pg_stream_bucket_count(tid).is_some()` using a `tid` resolved on a `pool.acquire()` connection (resolve via `live_table_id`; on a fresh creation the table may not exist yet — in that case derive stream-ness from the caller, see below). Add an `include_framing: bool` parameter to `append_parquet_snapshot` and let each caller pass it (flush computes from the tid it already has; the direct-write path computes it in Task 5). Batch tables pass `false` → `ice_schema(columns)` unchanged → byte-identical.

> Do NOT change `ice_schema` itself — only what it's handed. This keeps the `vector(N)`/field-id logic untouched.

- [ ] **Step 4: Register framing in the mirror at stream declaration**

In `iceberg_inline.rs`, at the point where a stream table is first declared and `project_columns(conn, tid, at, &pcols)` runs (the `live.is_empty()` first-projection branch, ~line 440-442): when `effective` is `Some(_)` (stream table), append the framing `ProjectedColumn`s to `pcols` before projecting, so the mirror carries them from creation. Build them from `framing_column_specs()` mapped through `mirror_column_type`/`iceberg_type` with `order` continuing after the user columns:
```rust
// Stream tables persist their log framing as reserved physical columns; register
// them in the mirror at declaration so flush/read see a consistent schema. Hidden
// from logical reads by `is_reserved`.
if effective.is_some() {
    let base = pcols.len() as i64;
    for (i, spec) in crate::iceberg_landing::framing_column_specs().iter().enumerate() {
        pcols.push(ProjectedColumn {
            order: base + 1 + i as i64,
            name: spec.name.clone(),
            iceberg_type: iceberg_physical_type(&spec.ty)
                .ok_or_else(|| ControlPlaneError::Backend(
                    format!("unknown framing logical type `{}`", spec.ty).into()))?
                .to_string(),
            nullable: spec.nullable,
        });
    }
}
```
> `ProjectedColumn.iceberg_type` is the iceberg primitive NAME (`"int"`,`"long"`,`"string"`) — `iceberg_physical_type("integer") == "int"`. Confirm `iceberg_physical_type` is imported (it is used elsewhere in landing; import into inline if needed).

- [ ] **Step 5: Flush reads framing via the physical read**

In `inline_live_batch` (`iceberg_inline.rs:1111`), replace the `self.schema(table, at)`-derived column list with `self.physical_columns(tid, at)` (Task 1) so the SELECT list and Arrow schema include the framing columns for stream tables. `tid` is resolved in the same function (`resolve_table`/`live_table_id`). The `column_array` decoder must handle the framing types: `loom_change_kind` → Utf8, `loom_bucket` → Int32, `loom_offset` → Int64 — these `BaseType`s (`String`/`Integer`/`Long`) are already supported by `arrow_field`/`column_array`; verify by test (Step 7), and if `Integer`/Int32 decode is missing, add the arm mirroring the `Long`/Int64 arm.
> For a **batch** table `physical_columns` returns exactly the user columns (framing never registered) → the SELECT list and batch are byte-identical to the prior `schema()`-derived list.

- [ ] **Step 6: Thread `include_framing` at the flush call site**

`flush_table`/`flush_locked` already resolves `tid` and calls `append_parquet_snapshot`. Compute `include_framing = pg_stream_bucket_count(&mut conn, tid).await?.is_some()` and pass it. The batch inline table has no framing columns in its live batch, so `include_framing == false` and nothing changes.

- [ ] **Step 7: Run tests to confirm they pass**

Run: `buck2 test //src/control-plane/postgres:stream-flush-persist > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|test result" /tmp/t3.log`
Expected: PASS (both tests).

- [ ] **Step 8: Regression — flush/inline/read suites**

`buck2 test //src/control-plane/postgres:iceberg-flush //src/control-plane/postgres:iceberg-inline //src/control-plane/postgres:iceberg-inline-vector //src/control-plane/postgres:stream-inline //src/control-plane/postgres:flush-suppression //src/control-plane/postgres:overwrite-end-caps-inline > /tmp/t3r.log 2>&1; grep -E "Tests finished|FAIL|test result" /tmp/t3r.log`
Expected: all green (byte-identical batch path proven by `iceberg-flush`).

- [ ] **Step 9: Lint + commit**

```bash
buck2 run //tools:rustfmt -- src/control-plane/postgres/src/iceberg_landing.rs src/control-plane/postgres/src/iceberg_inline.rs src/control-plane/postgres/tests/stream_flush_persist.rs
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "feat(stream): persist log framing through inline flush into Iceberg"
```

---

## Task 4: `do_update_table` caller-provided-tx refactor (pure refactor)

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs:154-246`
- Test: regression only (no behavior change) — rely on the full existing landing/flush/overwrite suites.

**Interfaces:**
- Produces: `pub(crate) async fn do_update_table_in_tx(&self, tx: &mut sqlx::Transaction<'_, Postgres>, commit: TableCommit, extras: CommitExtras<'_>) -> Result<Table>` — performs the object-store metadata write, the CAS UPDATE, `write_mirror`, and `apply_commit_extras` **on the caller's `tx`**, and does NOT begin or commit it. On a lost CAS (`rows_affected() == 0`) it returns `CatalogCommitConflicts` (retryable) **without** touching `tx` (the caller owns rollback).
- The existing `do_update_table` becomes a thin wrapper: `let mut tx = self.connection.begin()...; let out = self.do_update_table_in_tx(&mut tx, commit, extras).await?; tx.commit()...; Ok(out)` — with the caveat that on the `CatalogCommitConflicts` error path it must roll back its own tx before returning (preserving today's behavior).

**Context:** This is a mechanical extraction that changes NO behavior for existing callers. It exists solely so Task 5 can run offset allocation on the *same* tx that commits the snapshot. Keep the object-store reads/writes exactly where they are relative to the tx boundary in the caller-provided variant (the `write_to` metadata write and `added_files_of`/`columns_of` reads happen before the CAS UPDATE, as today).

- [ ] **Step 1: Extract `do_update_table_in_tx`**

Split the body at the `let mut tx = self.connection.begin()...` line (191). Everything from the metadata `write_to` through `apply_commit_extras` moves into `do_update_table_in_tx(&mut tx, ...)`, operating on the passed `tx` (drop the internal `begin`/`commit`). On `rows_affected() == 0`, return the `CatalogCommitConflicts` error WITHOUT `tx.rollback()` (caller decides). Keep `do_update_table` as the owning wrapper that begins, delegates, commits — and on error rolls back (so today's `drop(tx.rollback())` semantics are preserved end-to-end).

- [ ] **Step 2: No new test — prove via regression**

There is no observable behavior change, so no new test. Run the suites that exercise every commit path:
`buck2 test //src/control-plane/postgres:iceberg-flush //src/control-plane/postgres:iceberg-inline //src/control-plane/postgres:iceberg-landing //src/control-plane/postgres:overwrite-end-caps-inline //src/control-plane/postgres:iceberg-concurrency > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL|test result" /tmp/t4.log`
> Substitute the actual concurrency/landing target names from `BUCK` (`grep -n loom_fixture_test src/control-plane/postgres/BUCK`). Expected: all green.

- [ ] **Step 3: Lint + commit**

```bash
buck2 run //tools:rustfmt -- src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "refactor(iceberg): do_update_table_in_tx — commit on a caller-provided transaction"
```

---

## Task 5: Atomic direct-large-write Parquet stream path

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (`land_parquet` + a stream commit routine)
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs` (a `CommitExtras` field carrying the resolved offset stamping, applied in `apply_commit_extras`) — OR perform allocation in the landing routine on the shared tx before delegating (see Step 3). Choose the shared-tx approach.
- Test: `src/control-plane/postgres/tests/stream_parquet_atomic.rs` (new, `loom_fixture_test`)
- Modify: `src/control-plane/postgres/BUCK`

**Interfaces:**
- Consumes: `do_update_table_in_tx` (Task 4); `framing_column_specs`/`ensure_iceberg_table(..., include_framing=true)` (Task 3); `pg_allocate_offset`/`pg_declare_stream`/`pg_stream_bucket_count` (Plan 1a); the reconcile match from `inline_append`.

**Context:** The direct large-write path (`land_parquet`) bypasses the inline tier and writes Parquet straight to object storage, then commits the snapshot via a retrying CAS. To make offsets gapless *and* atomic, offset allocation must commit **iff** the snapshot commits. Because absolute offsets are baked into the Parquet before the write, a lost/abandoned CAS must free the allocation and (on retry) re-allocate + re-write. Implement by wrapping the whole attempt — reconcile, allocate, stamp, write Parquet, CAS-commit — in **one Postgres transaction** (via Task 4's caller-provided-tx commit), so a rollback frees the offset run and orphans the Parquet files. This holds a PG tx across the object-store write for the bulk stream path only; the common inline/flush path is untouched. Reconcile parity: reject batch→stream conversion (`Validation`) and bucket-count mismatch (`Conflict`), exactly as `inline_append`.

- [ ] **Step 1: Write the failing test**

`src/control-plane/postgres/tests/stream_parquet_atomic.rs` — three tests:
```rust
//! The direct large-write Parquet path reaches parity with the inline path for
//! stream tables: atomic Conflict/Validation reconcile + gapless per-bucket
//! offsets stamped into the written Parquet.
use control_plane_postgres::fixture::TestDb;
use control_plane_core::TableRef;

#[tokio::test]
async fn large_write_stamps_gapless_framing_in_parquet() {
    let db = TestDb::boot().await;
    let table = TableRef { schema: "s".into(), name: "biglog".into() };
    // A write above the inline byte limit → direct Parquet path.
    db.land_large_stream(&table, /*buckets*/ 2, /*rows*/ 5_000).await;
    let framing = db.parquet_framing(&table).await; // Vec<(bucket, offset)>
    assert_gapless_per_bucket(&framing);
    assert!(framing.iter().all(|(_, o)| *o >= 0));
}

#[tokio::test]
async fn large_write_rejects_batch_to_stream_conversion() {
    let db = TestDb::boot().await;
    let table = TableRef { schema: "s".into(), name: "wasbatch".into() };
    db.land_large_batch(&table, 5_000).await;               // creates a batch table
    let err = db.try_land_large_stream(&table, 2, 5_000).await; // mode=stream on existing batch
    assert!(matches!(err, Err(e) if e.is_validation()), "convert rejected: {err:?}");
}

#[tokio::test]
async fn large_write_rejects_bucket_mismatch() {
    let db = TestDb::boot().await;
    let table = TableRef { schema: "s".into(), name: "mism".into() };
    db.land_large_stream(&table, 2, 5_000).await;           // buckets=2
    let err = db.try_land_large_stream(&table, 3, 5_000).await; // buckets=3
    assert!(matches!(err, Err(e) if e.is_conflict()), "mismatch rejected: {err:?}");
}
```
> Build `land_large_stream`/`land_large_batch`/`try_land_*` in the test from `land(...)` with a batch sized above `InlineLimits.inline_byte_limit` (use the fixture's default limits; if needed set a tiny `inline_byte_limit` so a small batch routes to Parquet — check how `iceberg-landing`/`land_parquet` tests force the Parquet branch and mirror that). `is_validation()`/`is_conflict()` — add small `matches!(self, ControlPlaneError::Validation(_))` helpers in the test, or match the variant inline.

- [ ] **Step 2: Run it to confirm it fails**

Run: `buck2 test //src/control-plane/postgres:stream-parquet-atomic > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL|error\[|error:" /tmp/t5.log`
Expected: FAIL — no framing in Parquet; conversion/mismatch not rejected.

- [ ] **Step 3: Rewrite `land_parquet` for the stream case (atomic)**

Replace the post-commit best-effort declare with a pre-write atomic path. Structure:

```
land_parquet(pool, catalog, table, columns, batches, lineage, stream_buckets):
    resolve pre_existing tid (pool.acquire)
    // Reconcile stream mode BEFORE any write, mirroring inline_append's match:
    //   (Some,None)+pre_existing -> Validation; (Some(n),Some(m)) n!=m -> Conflict;
    //   declare + authoritative re-read for the fresh (Some(n),None) case.
    let effective = reconcile_stream_mode(&mut conn, tid?, stream_buckets, pre_existing, table)?;

    if let Some(bc) = effective {
        // STREAM path — one tx spans allocate + write + commit (Task 4).
        let mut tx = pool.begin()
        // 1. per-row bucket = row_index % bc; count rows per touched bucket.
        // 2. for each touched bucket b: first_b = pg_allocate_offset(&mut *tx, tid, b, count_b)
        // 3. stamp batches: append loom_change_kind ('+I'), loom_bucket, loom_offset arrays.
        // 4. ensure_iceberg_table(catalog, table, columns, /*include_framing*/ true)
        // 5. stage fast_append of the framing-augmented batches; loop:
        //      do_update_table_in_tx(&mut tx, commit, CommitExtras{lineage, data_trigger_tables})
        //      Ok -> break; Conflict & attempts left -> tx.rollback(); (re-allocate+re-write) ; Err -> tx.rollback(); return
        // 6. tx.commit()  // offsets + snapshot + framing-mirror durable together
    } else {
        // BATCH path — unchanged: append_parquet_snapshot(..., include_framing=false)
    }
```

Key implementation points:
- **Reconcile helper:** extract the reconcile match from `inline_append` into a shared `pub(crate) fn reconcile_stream_mode(conn, tid, stream_buckets, pre_existing, table) -> Result<Option<i32>>` and call it from both `inline_append` and `land_parquet` (DRY — do not copy-paste the arms). Move the exact arms verbatim (the `(Some(n),None)` declare + authoritative re-read included).
- **Stamping:** build three Arrow arrays aligned to the concatenated batch — `StringArray` of `"+I"`, `Int32Array` of buckets, `Int64Array` of offsets — and append them as columns under a schema extended with the framing fields. Offsets within a bucket are assigned in row order: the k-th row of bucket `b` gets `first_b + k`.
- **Retry + re-write:** on `CatalogCommitConflicts`, roll back (frees the offset run), then re-run allocation + re-stamp + re-write Parquet (fresh UUID files) for the next attempt, bounded by the same retry budget as `commit_append_with_retry`. Reuse `commit_backoff` for the sleep. (CAS conflicts on a bulk load are rare; re-write cost is paid only then.)
- **Long-tx note:** this holds a PG connection across the object-store Parquet write for the stream bulk path — accepted per the "full atomic parity" decision. Add a code comment stating the tradeoff and that the common inline/flush path is unaffected.
- **`include_framing`:** the stream branch calls `ensure_iceberg_table(..., true)` so the Iceberg schema (and thus `columns_of` → mirror) carries framing; the batch branch calls the existing `append_parquet_snapshot(..., false)`.

> If threading `do_update_table_in_tx` through `commit_append_with_retry` is cleaner than re-implementing the loop in `land_parquet`, add a stream-aware sibling of `append_parquet_snapshot` that takes the open `&mut tx` and the staged data files and performs the CAS-on-tx; keep the batch path calling the original. Do NOT change the batch path's short-tx behavior.

- [ ] **Step 4: Run tests to confirm they pass**

Run: `buck2 test //src/control-plane/postgres:stream-parquet-atomic > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL|test result" /tmp/t5.log`
Expected: PASS (all three).

- [ ] **Step 5: Regression — inline reconcile still correct after the extract**

The reconcile extract touches `inline_append`. Re-run the Plan 1a stream suites:
`buck2 test //src/control-plane/postgres:stream-inline //src/control-plane/postgres:stream //src/control-plane/postgres:iceberg-landing //src/services/ingest/... > /tmp/t5r.log 2>&1; grep -E "Tests finished|FAIL|test result" /tmp/t5r.log`
Expected: all green (identical reconcile behavior, now shared).

- [ ] **Step 6: Lint + commit**

```bash
buck2 run //tools:rustfmt -- src/control-plane/postgres/src/iceberg_landing.rs src/control-plane/postgres/src/iceberg_inline.rs src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs src/control-plane/postgres/tests/stream_parquet_atomic.rs
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "feat(stream): atomic gapless offset stamping on the direct large-write Parquet path"
```

---

## Task 6: Docs — close slice 1, record follow-ups

**Files:**
- Modify: `docs/ROADMAP.md` (remove `road-stream-log-tables` — slice 1 complete with 1a+1b), `docs/system-capabilities/` (document the shipped log-table capability), `docs/FUTURE.md` / `docs/ISSUES.md` (register any deferrals — e.g. hash-on-key bucketing `fut-stream-partitioning` if not present; the long-tx-across-object-store tradeoff for the bulk stream path as a note if warranted).

- [ ] **Step 1: Run the docs-update skill**

Use the `loom-docs-update` skill: close `road-stream-log-tables`, add the capability entry under `docs/system-capabilities/`, and register any new deferrals. Then `bash tools/docs.sh validate`.

- [ ] **Step 2: Commit**

```bash
git add -A && git commit -m "docs(stream): close slice-1 log tables (1a+1b), record follow-ups"
```

---

## Self-Review (author checklist — run after drafting)

1. **Spec coverage:** §1 declaration/reconcile → Task 5 (parity) + Plan 1a; §2 framing columns → Plan 1a + Task 3; §3 bucket/offset → Plan 1a + Task 5; §4 durable persistence + `is_reserved` → Tasks 1, 3, 5; §5 reads unchanged → Task 1 filter; migration CHECK → Task 2. ✅
2. **Placeholder scan:** framing specs, migration SQL, `is_reserved`, `physical_columns`, and the reconcile-extract are all concrete. Fixture helper names are flagged as "build if absent" with the primitives to build them from — acceptable (fixture surface varies), assertions are exact.
3. **Type consistency:** `framing_column_specs` (`"string"`/`"integer"`/`"long"`) ↔ `iceberg_physical_type` (`"int"`/`"long"`/`"string"`) ↔ `ColumnDef.ty` canonical names ↔ Arrow `Utf8`/`Int32`/`Int64`. `do_update_table_in_tx` signature consumed by Task 5 matches Task 4's production. ✅
