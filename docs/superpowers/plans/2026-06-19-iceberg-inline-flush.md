# Iceberg Inline Flush / Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `flush_table` library primitive that drains a table's live inline rows into a real Iceberg Parquet snapshot and end-caps the inline rows — atomically, exactly-once, time-travel-correct — emitting a compaction lineage event.

**Architecture:** Generalize the Slice B commit decorator from "optional lineage" to a `CommitExtras { lineage, end_cap }` carried into the vendored `SqlCatalog::do_update_table`'s one Postgres transaction; `project_mirror` returns the snapshot it allocates so the inline end-cap (`end_snapshot = S`) lands in that same tx alongside the new `data_file` and the lineage row. `flush_table` reconstructs the live inline rows to an arrow-57 batch, ensures the iceberg table exists, appends real Parquet through that decorator, and serializes per-table with a session advisory lock.

**Tech Stack:** Rust 2024, buck2, iceberg 0.9.1 (arrow-57 / parquet57), sqlx 0.9, Postgres control plane.

**Reference spec:** `docs/superpowers/specs/2026-06-19-iceberg-inline-flush-design.md`

**Conventions (read before starting):**
- Tests are `rust_test` / `loom_fixture_test` targets in `tests/<name>.rs` — **never** inline `#[cfg(test)]` (a prek hook fails the build).
- Fixture tests (boot Postgres) use `loom_fixture_test`, not bare `rust_test`.
- **Never** pipe `buck2 test` to `tail`/`head` — redirect to a file and grep: `buck2 test //… > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`.
- rustfmt is check-only: `buck2 run //tools:rustfmt -- <file>` (rewrites in place) before committing.
- This plan adds **no new `query!`/compile-time SQL** — the inline DDL/reads and the end-cap `UPDATE` use runtime `AssertSqlSafe` (dynamic table name `inline_<id>`), like all of `iceberg_inline.rs`. So **no `.sqlx` regeneration**. (If a build error mentions sqlx offline data, run `./tools/sqlx-prepare.sh`.)
- Commit messages: Conventional Commits, ending with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- **Branch:** this work is on `spec/iceberg-inline-flush`, stacked on Slice B (`spec/iceberg-landing-backend`, PR #90). Do not switch branches. Verify with `git branch --show-current` before each commit.

---

## Verified facts the code depends on

- Inline DDL (`iceberg_inline.rs`): `inline_<tid>` has `loom_row_id bigserial primary key, begin_snapshot bigint not null, end_snapshot bigint`, then data columns. `end_snapshot` is nullable and never set today.
- Live-row predicate (inline rows and `data_file`): `begin_snapshot <= at AND (end_snapshot IS NULL OR end_snapshot > at)`.
- `inline_table_name(tid) -> "iceberg_mirror.inline_<tid>"` (in `iceberg_inline.rs`).
- `next_snapshot(conn, Option<i64>) -> Result<SnapshotId>` (in `iceberg_mirror.rs`); `SnapshotId(pub i64)`.
- `project_mirror(&self, tx, ident, staged) -> Result<()>` in `impl SqlCatalog` (`catalog.rs` ~line 333) calls `next_snapshot` and `project_files`/`project_columns`.
- `do_update_table(&self, commit, lineage: Option<&LineageEvent>)` (Slice B, `catalog.rs`): after `project_mirror`, emits `pg_emit(&mut *tx, ev)` when `Some`, before `tx.commit()`. Trait `update_table` delegates with `None`.
- `LineageEmittingCatalog<'a>` (`iceberg_writer.rs`): decorator delegating all 14 `iceberg::Catalog` methods to `&SqlCatalog`, except `update_table` → `do_update_table(commit, Some(self.lineage))`. `append_batches_with_lineage(catalog, table, batches, lineage)` commits through it.
- `land_parquet` (`iceberg_landing.rs`): create-if-absent namespace+table (from `ColumnSpec`s via `ice_schema`/`iceberg_physical_type`), `load_table`, re-wrap batches under `iceberg::arrow::schema_to_arrow_schema(table…)`, `append_batches_with_lineage`, then read back `IcebergCatalog::new(pool).current_snapshot(table).id`.
- `IcebergCatalog::inline_parquet(&self, table, at) -> Result<Option<Vec<u8>>>` (`iceberg_inline.rs`): live-row read + arrow-57 array build + Parquet encode. `IcebergCatalog::schema(table, at) -> Result<TableSchema>` and `::files`, `::current_snapshot` are the `core::Catalog` trait methods.
- `pg_emit(&mut *conn, &LineageEvent)` is `pub(crate)` (`lineage.rs`).
- `DatasetId::from(&TableRef).dataset_ref()` gives the loom dataset ref (`name == "schema.table"`).

---

## Task 1: Generalize the commit decorator to `CommitExtras`

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs`
- Modify: `src/control-plane/postgres/src/iceberg_writer.rs`

Behaviour-preserving: introduce `CommitExtras { lineage, end_cap }`, make `do_update_table` take it, make `project_mirror` return its `SnapshotId`, and execute the end-cap `UPDATE` when present. With `end_cap == None` everywhere so far, behaviour is identical; Slice B's lineage test proves it. The end-cap is exercised by Task 4.

- [ ] **Step 1: Define `CommitExtras` + `InlineEndCap` and change `project_mirror` to return its snapshot**

In `catalog.rs`, add (near `do_update_table`):

```rust
/// Side-effects to run inside the one `do_update_table` commit tx, alongside the
/// pointer-CAS + mirror projection. Both are optional and independent.
#[derive(Default)]
pub struct CommitExtras<'a> {
    /// Emit this lineage event in the commit tx (landing / flush provenance).
    pub lineage: Option<&'a LineageEvent>,
    /// Retire these inline rows at the commit's snapshot (flush compaction).
    pub end_cap: Option<InlineEndCap<'a>>,
}

/// Mark inline rows `loom_row_id = ANY(row_ids)` of `iceberg_mirror.inline_<table_id>`
/// as ended at the commit's snapshot.
pub struct InlineEndCap<'a> {
    pub table_id: i64,
    pub row_ids: &'a [i64],
}
```

Change `project_mirror`'s signature to return the allocated snapshot. Find:

```rust
    async fn project_mirror(&self, tx: ..., ident: ..., staged: ...) -> control_plane_core::Result<()> {
        ...
        let at = next_snapshot(conn, iceberg_snap).await?;
        let tid = ensure_table(conn, &ns, name, at).await?;
        if !columns_exist(conn, tid).await? {
            project_columns(conn, tid, at, &columns_of(staged)).await?;
        }
        project_files(conn, tid, at, &files).await?;
        Ok(())
    }
```

Change the return type to `control_plane_core::Result<SnapshotId>` and return `Ok(at)` instead of `Ok(())`. (Import `SnapshotId` if not already in scope — it is, via `control_plane_core`.)

- [ ] **Step 2: Change `do_update_table` to take `CommitExtras` and run the end-cap**

Replace the `do_update_table` signature and its tail. Signature:

```rust
    pub(crate) async fn do_update_table(
        &self,
        commit: TableCommit,
        extras: CommitExtras<'_>,
    ) -> Result<Table> {
```

Where it currently calls `self.project_mirror(&mut tx, &table_ident, &staged_table)…?;` then the `if let Some(ev) = lineage { pg_emit … }` block, replace with:

```rust
        let at = self
            .project_mirror(&mut tx, &table_ident, &staged_table)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        if let Some(cap) = &extras.end_cap {
            // Retire the flushed inline rows at the same snapshot the new data file
            // becomes live, so reads never double-serve or drop them.
            let sql = format!(
                "update {} set end_snapshot = {} \
                 where loom_row_id = any($1) and end_snapshot is null",
                crate::iceberg_inline::inline_table_name(cap.table_id),
                at.0,
            );
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(cap.row_ids)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        }

        if let Some(ev) = extras.lineage {
            pg_emit(&mut *tx, ev)
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        }

        tx.commit().await.map_err(from_sqlx_error)?;
        Ok(staged_table)
```

Update the trait method to delegate with defaults:

```rust
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.do_update_table(commit, CommitExtras::default()).await
    }
```

Make `inline_table_name` reachable: it is `fn inline_table_name` in `iceberg_inline.rs` — change it to `pub(crate) fn inline_table_name` if it isn't already.

- [ ] **Step 3: Update `LineageEmittingCatalog` to pass `CommitExtras`**

In `iceberg_writer.rs`, change the decorator's `update_table`:

```rust
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.inner
            .do_update_table(
                commit,
                crate::iceberg_sql_catalog::CommitExtras {
                    lineage: Some(self.lineage),
                    ..Default::default()
                },
            )
            .await
    }
```

(Import path: `CommitExtras` is `pub` in `catalog.rs`, re-exported by the module's `pub use catalog::*`, so `crate::iceberg_sql_catalog::CommitExtras` resolves.)

- [ ] **Step 4: Build + format**

Run: `buck2 build //src/control-plane/postgres:postgres > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[" /tmp/b.log`
Then `buck2 run //tools:rustfmt -- src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs src/control-plane/postgres/src/iceberg_writer.rs`.

- [ ] **Step 5: Verify Slice B behaviour preserved**

Run: `buck2 test //src/control-plane/postgres:iceberg-writer //src/control-plane/postgres:iceberg_catalog //src/control-plane/postgres:iceberg-landing > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all pass (lineage path unchanged; end-cap is `None` everywhere).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs src/control-plane/postgres/src/iceberg_writer.rs src/control-plane/postgres/src/iceberg_inline.rs
git commit -m "refactor(iceberg): generalize do_update_table to CommitExtras

do_update_table now takes CommitExtras { lineage, end_cap }; project_mirror
returns the snapshot it allocates so an inline end-cap can land at it. The
end_cap UPDATE (end_snapshot = S WHERE loom_row_id = ANY) runs in the one commit
tx; LineageEmittingCatalog passes lineage via CommitExtras. Behaviour-preserving
(end_cap is None until flush) — Slice B tests green.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Extract `inline_live_batch` (shared reconstruction)

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs`

Split the row→array reconstruction out of `inline_parquet` so flush and the read path share it, and so flush can capture the `loom_row_id`s it reconstructs (it needs them for the end-cap). Behaviour-preserving for `inline_parquet`.

- [ ] **Step 1: Add `inline_live_batch`**

Add to `impl IcebergCatalog` (or a free fn taking `&mut PgConnection`) in `iceberg_inline.rs`:

```rust
    /// The live inline rows of `table` at `at`, as (`table_id`, `loom_row_id`s,
    /// arrow-57 batch), or `None` if there is no inline storage or no live rows.
    /// The `table_id` and row ids are returned so a flush can end-cap exactly the
    /// rows it reconstructs in the same `inline_<tid>` table. Shares the
    /// reconstruction the read path uses.
    pub async fn inline_live_batch(
        &self,
        table: &TableRef,
        at: SnapshotId,
    ) -> Result<Option<(i64, Vec<i64>, RecordBatch)>> {
        use control_plane_core::Catalog;
        let mut conn = self.pool.acquire().await.map_err(backend)?;
        let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name).await? else {
            return Ok(None);
        };
        let exists: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "select to_regclass('{}')::text",
            inline_table_name(tid)
        )))
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
        if exists.is_none() {
            return Ok(None);
        }

        let schema = self.schema(table, at).await?;
        let col_list = schema
            .columns
            .iter()
            .map(|c| format!("\"{}\"", c.name.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");

        let rows = sqlx::query(AssertSqlSafe(format!(
            "select loom_row_id, {col_list} from {} \
             where begin_snapshot <= {} and (end_snapshot is null or end_snapshot > {}) \
             order by loom_row_id",
            inline_table_name(tid),
            at.0,
            at.0,
        )))
        .fetch_all(&mut *conn)
        .await
        .map_err(backend)?;
        if rows.is_empty() {
            return Ok(None);
        }

        let row_ids: Vec<i64> = rows
            .iter()
            .map(|r| r.try_get::<i64, _>("loom_row_id").map_err(backend))
            .collect::<Result<Vec<_>>>()?;

        // Build arrow arrays per column. `column_array` indexes positional columns;
        // the data columns now start at index 1 (loom_row_id is column 0), so pass
        // `i + 1`.
        let fields = schema
            .columns
            .iter()
            .map(|c| arrow_field(&c.name, &c.ty, c.nullable))
            .collect::<Result<Vec<_>>>()?;
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());
        for (i, c) in schema.columns.iter().enumerate() {
            arrays.push(column_array(&rows, i + 1, &c.ty)?);
        }
        let arrow_schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(arrow_schema, arrays)
            .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
        Ok(Some((tid, row_ids, batch)))
    }
```

Note: `inline_parquet`'s current query selects only the data columns (`column_array(&rows, i, …)`); `inline_live_batch` selects `loom_row_id` first, so its `column_array` index is `i + 1`. Confirm `column_array(rows, idx, ty)` takes a positional column index into the PG `Row` — it does (`iceberg_inline.rs`).

- [ ] **Step 2: Repoint `inline_parquet` at the shared reconstruction**

Replace the body of `inline_parquet` (after the `tid`/exists checks it can drop, since `inline_live_batch` does them) with:

```rust
    pub async fn inline_parquet(&self, table: &TableRef, at: SnapshotId) -> Result<Option<Vec<u8>>> {
        let Some((_tid, _row_ids, batch)) = self.inline_live_batch(table, at).await? else {
            return Ok(None);
        };
        let schema = batch.schema();
        let mut buf: Vec<u8> = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema, None)
            .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
        w.write(&batch)
            .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
        w.close()
            .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
        Ok(Some(buf))
    }
```

- [ ] **Step 3: Build, format, verify the read path unchanged**

```bash
buck2 build //src/control-plane/postgres:postgres > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[" /tmp/b.log
buck2 run //tools:rustfmt -- src/control-plane/postgres/src/iceberg_inline.rs
buck2 test //src/control-plane/postgres:iceberg-landing //src/control-plane/postgres:iceberg-inline-types > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: pass — `inline_parquet` output is byte-identical (same rows, same order, same encoder), now via the shared helper. (If query-api has an inline serving test, run it too: `buck2 test //src/services/query-api/...` — but that's heavier; the postgres-side landing test already exercises `inline_parquet` via the union.)

- [ ] **Step 4: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_inline.rs
git commit -m "refactor(iceberg): extract inline_live_batch from inline_parquet

inline_live_batch returns (loom_row_ids, arrow-57 batch) for the live inline rows;
inline_parquet now encodes that batch. Flush will reuse the helper and the row ids
to end-cap exactly the rows it reconstructs. Read path output unchanged.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Extract `append_parquet_snapshot` (shared create-if-absent + append)

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs`
- Modify: `src/control-plane/postgres/src/iceberg_writer.rs`

Generalize `land_parquet`'s "create-if-absent → re-wrap → append → read-back snapshot" into a helper that takes `CommitExtras`, so flush reuses it with an end-cap. Also generalize `append_batches_with_lineage` → `append_batches_with_extras`. Behaviour-preserving; landing tests prove it.

- [ ] **Step 1: `append_batches_with_extras` in `iceberg_writer.rs`**

Rename/generalize. Replace `LineageEmittingCatalog` with a single `CommitExtrasCatalog<'a> { inner: &'a SqlCatalog, extras: CommitExtras<'a> }` whose `update_table` calls `self.inner.do_update_table(commit, /* move extras */ )` — but `extras` is borrowed per-call, so build the decorator with references and reconstruct a fresh `CommitExtras` inside `update_table` (it only holds `Option<&_>`/`Option<InlineEndCap<'_>>`, all `Copy`-ish references):

```rust
struct CommitExtrasCatalog<'a> {
    inner: &'a SqlCatalog,
    lineage: Option<&'a LineageEvent>,
    end_cap: Option<InlineEndCap<'a>>,
}
```

In `update_table`:

```rust
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.inner
            .do_update_table(
                commit,
                CommitExtras {
                    lineage: self.lineage,
                    end_cap: self.end_cap.as_ref().map(|c| InlineEndCap {
                        table_id: c.table_id,
                        row_ids: c.row_ids,
                    }),
                },
            )
            .await
    }
```

(All other `Catalog` methods delegate to `inner`, same as the old decorator — keep them.)

Then:

```rust
/// Append `batches` as real Parquet and commit, running `extras` (lineage and/or
/// inline end-cap) inside the one commit tx. Generalizes append_batches_with_lineage.
pub async fn append_batches_with_extras(
    catalog: &SqlCatalog,
    table: &Table,
    batches: Vec<RecordBatch>,
    lineage: Option<&LineageEvent>,
    end_cap: Option<InlineEndCap<'_>>,
) -> Result<Vec<WrittenFile>> {
    let data_files = write_parquet(table, batches).await?;
    let summaries = /* same WrittenFile mapping as today */;
    let wrapper = CommitExtrasCatalog { inner: catalog, lineage, end_cap };
    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx)?;
    tx.commit(&wrapper).await?;
    Ok(summaries)
}
```

Make `append_batches_with_lineage` a thin wrapper (keep its callers working):

```rust
pub async fn append_batches_with_lineage(
    catalog: &SqlCatalog,
    table: &Table,
    batches: Vec<RecordBatch>,
    lineage: &LineageEvent,
) -> Result<Vec<WrittenFile>> {
    append_batches_with_extras(catalog, table, batches, Some(lineage), None).await
}
```

Import `CommitExtras`, `InlineEndCap` from `crate::iceberg_sql_catalog`.

- [ ] **Step 2: `append_parquet_snapshot` in `iceberg_landing.rs`**

Extract the create-if-absent + re-wrap + append + read-back from `land_parquet` into:

```rust
/// Ensure the iceberg table exists (create-if-absent from `columns`), append
/// `batches` (bare arrow-57 — re-wrapped under the table's field-id schema) as a
/// real Parquet snapshot running `extras` in the commit tx, and return the mirror
/// snapshot id. Shared by the landing Parquet path and the flush path.
pub(crate) async fn append_parquet_snapshot(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: Option<&LineageEvent>,
    end_cap: Option<InlineEndCap<'_>>,
) -> Result<SnapshotId> {
    let ns = NamespaceIdent::new(table.schema.clone());
    if !catalog.namespace_exists(&ns).await.map_err(be)? {
        catalog.create_namespace(&ns, Default::default()).await.map_err(be)?;
    }
    let ident = TableIdent::new(ns.clone(), table.name.clone());
    if !catalog.table_exists(&ident).await.map_err(be)? {
        let creation = TableCreation::builder()
            .name(table.name.clone())
            .schema(ice_schema(columns)?)
            .build();
        catalog.create_table(&ns, creation).await.map_err(be)?;
    }
    let ice_table = catalog.load_table(&ident).await.map_err(be)?;

    let ice_arrow = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(ice_table.metadata().current_schema()).map_err(be)?,
    );
    let batches = batches
        .into_iter()
        .map(|b| RecordBatch::try_new(ice_arrow.clone(), b.columns().to_vec()).map_err(be))
        .collect::<Result<Vec<_>>>()?;

    append_batches_with_extras(catalog, &ice_table, batches, lineage, end_cap)
        .await
        .map_err(be)?;

    Ok(IcebergCatalog::new(pool.clone()).current_snapshot(table).await?.id)
}
```

Repoint `land_parquet` to call it:

```rust
async fn land_parquet(...) -> Result<SnapshotId> {
    append_parquet_snapshot(pool, catalog, table, columns, batches, Some(&lineage), None).await
}
```

(Import `InlineEndCap`, `append_batches_with_extras`. Make `ice_schema`/`be` reachable — they are already in this module.)

- [ ] **Step 3: Build, format, verify landing unchanged**

```bash
buck2 build //src/control-plane/postgres:postgres > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[" /tmp/b.log
buck2 run //tools:rustfmt -- src/control-plane/postgres/src/iceberg_landing.rs src/control-plane/postgres/src/iceberg_writer.rs
buck2 test //src/control-plane/postgres:iceberg-landing //src/control-plane/postgres:iceberg-writer //src/services/ingest:iceberg-land > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: all pass — landing/Parquet behaviour unchanged through the extracted helper.

- [ ] **Step 4: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_landing.rs src/control-plane/postgres/src/iceberg_writer.rs
git commit -m "refactor(iceberg): extract append_parquet_snapshot + append_batches_with_extras

One commit decorator (CommitExtrasCatalog) and one append helper now carry
CommitExtras { lineage, end_cap }; append_parquet_snapshot factors land_parquet's
create-if-absent + field-id re-wrap + append + snapshot read-back so flush reuses
it. append_batches_with_lineage stays as a thin wrapper. Behaviour-preserving.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: `flush_table` + the flush tests

**Files:**
- Create: `src/control-plane/postgres/src/iceberg_flush.rs`
- Modify: `src/control-plane/postgres/src/lib.rs` (`pub mod iceberg_flush;`)
- Modify: `src/control-plane/postgres/BUCK` (new `iceberg-flush` `loom_fixture_test`)
- Test: `src/control-plane/postgres/tests/iceberg_flush.rs`

- [ ] **Step 1: Write the failing tests**

Create `src/control-plane/postgres/tests/iceberg_flush.rs`. Use `PgFixture`, build a catalog like the `iceberg_landing` tests, drive `inline_append` (via the `IcebergWriter::inline` fixture helper or `inline_append` directly) then `flush_table`. Cover the spec's tests:

```rust
// (1) flush an inline-only table -> rows become file-backed, inline retired, exactly-once
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_inline_only_makes_rows_file_backed_exactly_once() {
    // inline_append a few rows; snap0 = current.
    // flush_table(&catalog, &pool, &table, run).await -> Some(s).
    // assert IcebergCatalog::files(table, current) is non-empty (a real parquet file);
    // assert inline_parquet(table, current) == None (rows retired at current);
    // read the union (or files + inline) at current and assert each row appears once.
}

// (5) empty flush -> Ok(None)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_with_no_live_rows_is_a_noop() {
    // a table with no inline rows -> flush_table -> Ok(None); no new snapshot.
}

// (6) compaction lineage emitted (in == out == table)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_emits_compaction_lineage() {
    // inline_append; flush_table(run); events_for(run) has 1 event with
    // inputs[0].name == outputs[0].name == "wh.t".
}

// (2) time-travel: pre-flush snapshot still sees rows inline; flush snapshot sees parquet
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_preserves_time_travel() {
    // snap0 = current after inline_append; flush -> s.
    // assert inline_parquet(table, snap0) is Some (still live below s);
    // assert files(table, snap0) excludes the flushed file (begin_snapshot == s > snap0);
    // assert files(table, s) includes it and inline_parquet(table, s) is None.
}

// (4) concurrent inline write survives the flush
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_leaves_later_inline_rows_live() {
    // inline_append row A; flush; inline_append row B (after flush);
    // assert inline_parquet(table, current) is Some and contains B only;
    // a second flush moves B; then inline_parquet is None.
}
```

Use `IcebergCatalog::new(pool.clone())` for `files`/`current_snapshot`/`schema`/`inline_parquet`. For "row appears once", decode the flushed parquet file from its `file://` path (like `tests/iceberg_writer.rs` does with `ParquetRecordBatchReaderBuilder`) and assert the row set; the simplest exactly-once check is: flushed parquet has N rows AND `inline_parquet(current)` is `None`.

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test //src/control-plane/postgres:iceberg-flush > /tmp/t.log 2>&1; grep -E "error\[|cannot find|FAIL" /tmp/t.log`
(Target added in Step 4.) Expected: FAIL — `flush_table` not found.

- [ ] **Step 3: Implement `iceberg_flush.rs`**

```rust
//! Inline flush/compaction: drain a table's live inline rows into a real Iceberg
//! Parquet snapshot and end-cap the inline rows, atomically. Library primitive —
//! no trigger policy (see the spec). Serialized per table by a session advisory
//! lock so two flushes can't both write Parquet for the same rows.

use control_plane_core::{
    Catalog, ColumnSpec, DatasetId, EventType, LineageEvent, Result, RunId, SnapshotId, TableRef,
};
use sqlx::PgPool;
use time::OffsetDateTime;

use crate::iceberg_catalog::IcebergCatalog;
use crate::iceberg_landing::append_parquet_snapshot;
use crate::iceberg_sql_catalog::{InlineEndCap, SqlCatalog};
use crate::{ControlPlaneError, backend};

/// Flush `table`'s live inline rows into a real Iceberg Parquet snapshot, retiring
/// the inline rows at the same snapshot. Returns the new mirror snapshot id, or
/// `None` if there were no live inline rows. Serialized per table.
pub async fn flush_table(
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    run_id: RunId,
) -> Result<Option<SnapshotId>> {
    // Session advisory lock on a stable key for this table, held for the whole
    // operation on a dedicated connection. Released on every return path.
    let mut lock_conn = pool.acquire().await.map_err(backend)?;
    let key = lock_key(&table.schema, &table.name);
    sqlx::query("select pg_advisory_lock($1)")
        .bind(key)
        .execute(&mut *lock_conn)
        .await
        .map_err(backend)?;

    let result = flush_locked(catalog, pool, table, run_id).await;

    // Always release, regardless of outcome.
    let _ = sqlx::query("select pg_advisory_unlock($1)")
        .bind(key)
        .execute(&mut *lock_conn)
        .await;
    result
}

async fn flush_locked(
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    run_id: RunId,
) -> Result<Option<SnapshotId>> {
    let ice = IcebergCatalog::new(pool.clone());
    let current = ice.current_snapshot(table).await?;

    // Capture the live inline rows + their ids (+ the inline table id) at current.
    let Some((tid, row_ids, batch)) = ice.inline_live_batch(table, current.id).await? else {
        return Ok(None);
    };

    // Physical schema (model/inferred logical types) for create-if-absent.
    let columns: Vec<ColumnSpec> = ice
        .schema(table, current.id)
        .await?
        .columns
        .into_iter()
        .map(|c| ColumnSpec { name: c.name, ty: c.ty, nullable: c.nullable })
        .collect();

    let lineage = compaction_event(table, run_id);
    let end_cap = InlineEndCap { table_id: tid, row_ids: &row_ids };

    let snap = append_parquet_snapshot(
        pool,
        catalog,
        table,
        &columns,
        vec![batch],
        Some(&lineage),
        Some(end_cap),
    )
    .await?;
    Ok(Some(snap))
}

/// A loom compaction lineage event: the table is both input and output.
fn compaction_event(table: &TableRef, run_id: RunId) -> LineageEvent {
    let dr = DatasetId::from(table).dataset_ref();
    LineageEvent {
        run_id,
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![dr.clone()],
        outputs: vec![dr],
        payload: serde_json::json!({ "source": "flush" }),
    }
}

/// 64-bit advisory-lock key from the table identity (stable per (schema, name)).
fn lock_key(schema: &str, name: &str) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    schema.hash(&mut h);
    "\u{1f}".hash(&mut h);
    name.hash(&mut h);
    h.finish() as i64
}
```

**Implementer notes:**
- The end-cap's `tid` comes straight from `inline_live_batch` (Task 2 returns it), so the `UPDATE` targets the exact `inline_<tid>` table the rows were read from — no re-query.
- `DefaultHasher` is not stable across Rust releases, but the key only needs to be stable **within a process** for two concurrent flushes to contend — they run in the same binary. That suffices. (If cross-process stability is ever needed, switch to a fixed hash; out of scope.)
- `backend`, `ControlPlaneError` are the crate error helpers (`crate::backend` is sqlx-specific; for non-sqlx use `ControlPlaneError::Backend(...)`).
- The advisory lock and the work run on **different** pooled connections (the lock on `lock_conn`, the reads on `IcebergCatalog`'s pool, the commit on the catalog's own connection) — that's fine: the lock only needs to be *held by this `flush_table` call* so a second concurrent flush blocks on `pg_advisory_lock($key)`. Hold `lock_conn` for the whole call; releasing/ dropping it frees the session lock.
- Register the module: add `pub mod iceberg_flush;` to `lib.rs`.

- [ ] **Step 4: Add the test target**

In `src/control-plane/postgres/BUCK`, mirror `iceberg-landing` (it boots Postgres) but the flush test reads parquet back, so include `parquet57`:

```python
loom_fixture_test(
    name = "iceberg-flush",
    crate = "iceberg_flush",
    srcs = ["tests/iceberg_flush.rs"],
    crate_root = "tests/iceberg_flush.rs",
    named_deps = {"parquet57": "//third-party:parquet57"},
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:arrow-array",
        "//third-party:iceberg",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 5: Build, test, format**

```bash
buck2 build //src/control-plane/postgres:postgres > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[" /tmp/b.log
buck2 test //src/control-plane/postgres:iceberg-flush > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|panicked|left:|right:" /tmp/t.log
buck2 run //tools:rustfmt -- src/control-plane/postgres/src/iceberg_flush.rs src/control-plane/postgres/tests/iceberg_flush.rs
```
Expected: all flush tests pass. Debug failures with systematic-debugging (read the real panic/assert, don't guess).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_flush.rs src/control-plane/postgres/src/lib.rs src/control-plane/postgres/src/iceberg_inline.rs src/control-plane/postgres/BUCK src/control-plane/postgres/tests/iceberg_flush.rs
git commit -m "feat(iceberg): flush_table drains inline rows to a Parquet snapshot

flush_table reconstructs a table's live inline rows, ensures the iceberg table
exists, appends real Parquet, and end-caps the inline rows (end_snapshot = S) in
the one commit tx via CommitExtras — exactly-once and time-travel-correct. Emits a
compaction lineage event (in == out == table); serialized per table by a session
advisory lock. Library primitive, no trigger policy.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Roadmap + full verification + review

**Files:**
- Modify: `docs/spike/ICEBERG_ROADMAP.md`

- [ ] **Step 1: Mark inline flush/compaction done**

Move item #3 (inline flush/compaction) out of "Left (deferred)" into the done section with a short Slice description (flush_table primitive; end-cap MVCC; compaction lineage; reuses do_update_table/CommitExtras; restores external visibility). Renumber the deferred list. Note the remaining follow-ups: triggering (worker/threshold/endpoint) and physical GC of end-capped rows. End the file with exactly one trailing newline, no trailing whitespace (markdown-lint).

- [ ] **Step 2: Full first-party suite**

```bash
buck2 build //src/... > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[" /tmp/b.log
buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
./tools/clippy-all.sh > /tmp/c.log 2>&1; grep -ciE "warning:|error:" /tmp/c.log
```
Expected: build SUCCEEDED; whole suite passes; clippy 0 warn/err. (No dep changes in this slice, so no reindeer/duckdb risk.)

- [ ] **Step 3: Commit the roadmap**

```bash
git add docs/spike/ICEBERG_ROADMAP.md
git commit -m "docs(iceberg): mark inline flush/compaction done

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 4: Final code review**

Capture `git diff` of this branch's flush commits (since the spec commit) to `/tmp/flush.diff` and dispatch a final read-only reviewer focused on: the end-cap atomicity (does the `UPDATE … end_snapshot = S` truly share the commit tx; can a lost CAS leave rows capped without a file or vice-versa); the advisory-lock release on every path (early-return, error, panic-safety); exactly-once under the `inline_live_batch` capture vs. concurrent `inline_append`; and the `CommitExtras`/`append_parquet_snapshot` refactor preserving Slice B + landing behaviour. Address Critical/Important findings before finishing.

---

## Self-Review notes (for the controller)

- **Spec coverage:** flush primitive → T4; CommitExtras/end-cap atomicity → T1; reconstruction reuse → T2; create-if-absent reuse → T3; compaction lineage → T4 (`compaction_event`); advisory-lock serialization → T4; the 6 spec tests → T4 (+ Slice B/landing preserved by T1/T3 existing tests); roadmap → T5.
- **Risk order:** the load-bearing refactors that other code depends on land first (T1 CommitExtras, T2 reconstruction, T3 shared append), each behaviour-preserving behind existing tests, before the new flush behaviour (T4) builds on them.
- **No dep changes** — unlike Slice B, this slice touches no `Cargo.toml`/lock, so no reindeer/duckdb-downgrade exposure.
- **Atomicity is the crux** (T1 + T4): the end-cap `UPDATE` must be inside `do_update_table`'s tx using the snapshot `project_mirror` returns; the flush tests' time-travel + exactly-once cases are what prove it.
