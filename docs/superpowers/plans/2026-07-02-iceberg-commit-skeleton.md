# Iceberg Mirror Commit Skeleton (CommitExtras Unification) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse the six copies of the mirror snapshot-commit skeleton onto one
`CommitExtras`-carried spine: one inline end-cap helper, one
`apply_commit_extras` side-effect step, `CommitExtras<'_>` threaded through the
landing/writer signatures (deleting all three `too_many_arguments` allows), and
the loom-added members of the vendored `catalog.rs` moved to a loom-owned
sibling module.

**Architecture:** This is a **behavior-preserving refactor** of
`src/control-plane/postgres` (the `control_plane_postgres` crate). The existing
fixture suite is the characterization net — no observable behavior may change.
The work: (1) move `CommitExtras`/`InlineEndCap`/`write_mirror`/`do_update_table`/
`delete_file` out of the **vendored** `iceberg_sql_catalog/catalog.rs` into a
loom-owned sibling `iceberg_sql_catalog/commit_mirror.rs` (so the vendored file
stays close to upstream for re-vendoring diffs); (2) extract the verbatim-copied
inline end-cap SQL into `iceberg_inline::end_cap_inline_rows_by_id`; (3) make
`append_batches_with_extras` / `append_parquet_snapshot` / `land_additive` carry
`CommitExtras<'_>` instead of 4 loose params, and `land` carry an
`InlineLimits` pair, deleting the three `clippy::too_many_arguments` allows;
(4) share the end-cap+lineage+jobs commit tail as
`apply_commit_extras(conn, at, &CommitExtras)` between `land_additive` and
`do_update_table`.

**Tech Stack:** Rust, buck2 (no cargo builds), sqlx (runtime `AssertSqlSafe` on
these dynamic-identifier paths — NOT compile-time `query!`, so no `.sqlx` cache
changes), iceberg-rust (pinned git main), hermetic-Postgres fixture tests.

**Spec:** `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md`
§ *road-iceberg-commit-skeleton — CommitExtras unification* (lines 159–180).

## Global Constraints

- **Behavior-preserving.** No wire, error-text, ordering, or transactional
  semantics change anywhere. The commit-tx side-effect order is load-bearing and
  must stay exactly: *(overwrite end-caps, inside projection)* → *mirror
  projection* → *inline end-cap* → *lineage* → *jobs* → *commit*.
- **Tests are `rust_test` integration targets only** — never inline
  `#[cfg(test)]` modules (buck2 never runs them; the `no-inline-tests` prek hook
  rejects them). This plan adds **no new test files**: every touched behavior is
  already pinned by the crate's fixture suite (listed per task); tasks run the
  covering targets before and after.
- **Fixture tests must run via existing `loom_fixture_test` targets** (they pin
  local execution). Never pipe `buck2 test` through `tail`/`head` — redirect to
  a file and grep: `buck2 test <targets> > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **Cloud disk cap:** never run a bare whole-tree `buck2 build`/`test //src/...`.
  Build with `-M none`; scope tests to the targets named in each task.
- **Strict clippy** (pedantic+restriction) is on for all `src/**` production
  code: no `unwrap`/`expect`/`indexing`/`panic`; every `#[allow]`/`#[expect]`
  needs a `reason`. Check with
  `buck2 build '//src/control-plane/postgres:control-plane-postgres[clippy.txt]'`
  (empty output == clean).
- **Vendored-file discipline:** `iceberg_sql_catalog/catalog.rs` and
  `iceberg_sql_catalog/error.rs` are vendored from Apache iceberg-rust. After
  Task 1 the only NEW loom edits to `catalog.rs` are visibility keywords
  (`pub(super)`) and one `use super::commit_mirror::CommitExtras;` import — no
  logic edits there in any later task.
- `iceberg_sql_catalog/mod.rs` carries `#![deny(missing_docs)]` — every `pub`
  item in the new `commit_mirror.rs` needs a doc comment.
- **External import paths must not change**: `iceberg_sql_catalog` re-exports via
  `pub use` glob, so `control_plane_postgres::iceberg_sql_catalog::{CommitExtras,
  InlineEndCap, SqlCatalog, ...}` must keep resolving for the ~40 external
  importers.
- Commit messages follow Conventional Commits (`refactor(iceberg): ...`).

## The six skeleton sites (orientation)

`begin` → `next_snapshot` → `ensure_table` → end-caps → `register_files`/project
→ `pg_emit` → jobs → `commit` appears at:

1. `iceberg_landing.rs::append_parquet_snapshot` (delegates the tx to `do_update_table` via the writer chain)
2. `iceberg_landing.rs::land_additive` (mirror-only tx, lines ~371–399)
3. `iceberg_landing.rs::overwrite_truncate` (mirror-only tx — **unchanged** by this plan; it has no end-cap-by-id/jobs)
4. `iceberg_sql_catalog/catalog.rs::write_mirror` (projection inside the commit tx)
5. `iceberg_sql_catalog/catalog.rs::do_update_table` (CAS → projection → extras → commit)
6. `iceberg_control_plane.rs::IcebergTx::commit` (**unchanged** by this plan; it already shares `register_files` and has no extras)

The verbatim duplicate is the inline end-cap SQL at `iceberg_landing.rs:374-391`
and `catalog.rs:553-567`. The spec's four design points are Tasks 2, 4+5, 6, 1
respectively.

## File Structure

- Create: `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs`
  — loom-owned commit surface: `CommitExtras`, `InlineEndCap`,
  `impl SqlCatalog { delete_file, write_mirror, do_update_table }`, and (Task 6)
  `apply_commit_extras`.
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs`
  (delete moved members; `pub(super)` on 3 fields + 8 statics; one import),
  `.../mod.rs` (wire the new module),
  `src/control-plane/postgres/src/iceberg_inline.rs` (new helper),
  `src/control-plane/postgres/src/iceberg_writer.rs` (`CommitExtras`-carrying
  decorator + signature),
  `src/control-plane/postgres/src/iceberg_landing.rs` (signatures + `InlineLimits`),
  `src/control-plane/postgres/src/iceberg_flush.rs` (call site),
  `src/services/ingest/src/landing.rs`, `src/services/engine-serving/src/action_writer.rs`
  (external `land` call sites), plus 24 test files' `land(...)` calls.
- Test (existing targets, no new files): `//src/control-plane/postgres:` →
  `iceberg-landing`, `iceberg-flush`, `inline-flush-trigger`, `iceberg-overwrite`,
  `overwrite-end-caps-inline`, `iceberg-schema-evolution-land`, `iceberg-writer`,
  `iceberg-write-roundtrip`, `sqlcatalog-execute-commit`, `iceberg-control-plane`,
  `iceberg-gc`, `flush-vector-rebuild`, `iceberg-compact`, `iceberg-tx-compact`.

---

### Task 1: Move the loom-added members of vendored `catalog.rs` into `commit_mirror.rs`

Pure verbatim move — no logic changes, no signature changes. This goes FIRST so
every later logic change happens in the loom-owned file.

**Files:**
- Create: `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs`
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs`
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/mod.rs`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `commit_mirror.rs` owning `pub struct CommitExtras<'a>`,
  `pub struct InlineEndCap<'a>`, and `impl SqlCatalog { pub async fn delete_file(&self, path: &str) -> control_plane_core::Result<()>; async fn write_mirror(...) -> control_plane_core::Result<SnapshotId>; pub(crate) async fn do_update_table(&self, commit: TableCommit, extras: CommitExtras<'_>) -> Result<Table> }`
  — all bodies byte-identical to today's. External path
  `iceberg_sql_catalog::{CommitExtras, InlineEndCap}` unchanged (glob re-export).

- [ ] **Step 1: Record the pre-change test baseline**

Run the covering suites and stash the log (these must pass identically after):

```bash
buck2 test //src/control-plane/postgres:sqlcatalog-execute-commit \
  //src/control-plane/postgres:iceberg-writer \
  //src/control-plane/postgres:iceberg-write-roundtrip \
  //src/control-plane/postgres:iceberg-landing \
  //src/control-plane/postgres:iceberg-gc \
  > /tmp/task1-before.log 2>&1
grep -E "Tests finished|FAIL" /tmp/task1-before.log
```

Expected: `Tests finished` with 0 failures.

- [ ] **Step 2: Create `commit_mirror.rs` with the moved members**

Create `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs`.
Move — byte-identical bodies, including every doc comment and inline comment —
the following from `catalog.rs`:

- `pub struct CommitExtras<'a>` + its doc comments (catalog.rs lines ~217–234)
- `pub struct InlineEndCap<'a>` + docs (lines ~236–243)
- from the `impl SqlCatalog` block: `delete_file` (lines ~404–415),
  `write_mirror` (lines ~417–459), `do_update_table` (lines ~461–583)

File skeleton (the `...` bodies are the verbatim moved code):

```rust
//! loom-owned commit surface of the vendored SQL catalog: the [`CommitExtras`]
//! side-effect aggregate, the in-tx mirror projection (`write_mirror`), the
//! pointer-CAS commit (`do_update_table`), and physical object deletion
//! (`delete_file`). Kept out of `catalog.rs` so the vendored file stays close
//! to upstream for re-vendoring diffs.

use iceberg::table::Table;
use iceberg::{Catalog, Error, ErrorKind, MetadataLocation, Result, TableCommit, TableIdent};
use sqlx::{Postgres, Transaction};

use control_plane_core::{LineageEvent, SnapshotId};

use crate::iceberg_mirror::{ProjectedColumn, ProjectedFile};
use crate::lineage::pg_emit;

use super::catalog::{
    CATALOG_FIELD_CATALOG_NAME, CATALOG_FIELD_METADATA_LOCATION_PROP,
    CATALOG_FIELD_PREVIOUS_METADATA_LOCATION_PROP, CATALOG_FIELD_RECORD_TYPE,
    CATALOG_FIELD_TABLE_NAME, CATALOG_FIELD_TABLE_NAMESPACE, CATALOG_FIELD_TABLE_RECORD_TYPE,
    CATALOG_TABLE_NAME, SqlCatalog,
};
use super::error::from_sqlx_error;

/// Side-effects to run inside the one `do_update_table` commit tx, alongside the
/// pointer-CAS + mirror projection. Both are optional and independent.
#[derive(Default)]
pub struct CommitExtras<'a> {
    ... // verbatim fields + per-field docs from catalog.rs
}

/// Mark inline rows `loom_row_id = ANY(row_ids)` of `iceberg_mirror.inline_<table_id>`
/// as ended at the commit's snapshot.
pub struct InlineEndCap<'a> {
    ... // verbatim
}

impl SqlCatalog {
    pub async fn delete_file(&self, path: &str) -> control_plane_core::Result<()> {
        ... // verbatim; uses self.fileio
    }

    async fn write_mirror(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        ident: &TableIdent,
        staged_snap: Option<i64>,
        columns: &[ProjectedColumn],
        files: &[ProjectedFile],
        overwrite: bool,
    ) -> control_plane_core::Result<SnapshotId> {
        ... // verbatim (its `use crate::iceberg_mirror::{...}` inner import moves with it)
    }

    pub(crate) async fn do_update_table(
        &self,
        commit: TableCommit,
        extras: CommitExtras<'_>,
    ) -> Result<Table> {
        ... // verbatim; uses self.load_table / self.execute / self.connection / self.name
    }
}
```

Notes for the implementer:
- `do_update_table`'s body references `crate::iceberg_mirror::added_files_of`,
  `crate::iceberg_mirror::columns_of`, `crate::iceberg_inline::inline_table_name`,
  `crate::queue::pg_insert_if_absent`, `sqlx::AssertSqlSafe` — those fully-qualified
  paths move verbatim and still resolve.
- The `Catalog` trait import is needed because `do_update_table` calls
  `self.load_table(...)` (a `Catalog` trait method).
- Keep every doc comment: `mod.rs` has `#![deny(missing_docs)]`.

- [ ] **Step 3: Delete the moved members from `catalog.rs` and open the visibility seams**

In `catalog.rs`:

1. Delete the moved items (the two structs and the three methods; keep the rest
   of the `impl SqlCatalog` block — `new`, `replace_placeholders`, `fetch_rows`,
   `execute` stay).
2. Change these three fields of `struct SqlCatalog` from private to `pub(super)`
   (loom visibility edit to the vendored file — keywords only):

```rust
pub struct SqlCatalog {
    pub(super) name: String,
    pub(super) connection: PgPool,
    warehouse_location: String,
    pub(super) fileio: FileIO,
    /// iceberg main requires a `Runtime` on every `Table::builder()`; threaded in here
    /// from the builder (defaulting to `Runtime::current()`).
    runtime: iceberg::Runtime,
}
```

3. Change these eight statics from private to `pub(super)` (keep values/comments):

```rust
pub(super) static CATALOG_TABLE_NAME: &str = "iceberg_tables";
pub(super) static CATALOG_FIELD_CATALOG_NAME: &str = "catalog_name";
pub(super) static CATALOG_FIELD_TABLE_NAME: &str = "table_name";
pub(super) static CATALOG_FIELD_TABLE_NAMESPACE: &str = "table_namespace";
pub(super) static CATALOG_FIELD_METADATA_LOCATION_PROP: &str = "metadata_location";
pub(super) static CATALOG_FIELD_PREVIOUS_METADATA_LOCATION_PROP: &str = "previous_metadata_location";
pub(super) static CATALOG_FIELD_RECORD_TYPE: &str = "iceberg_type";
pub(super) static CATALOG_FIELD_TABLE_RECORD_TYPE: &str = "TABLE";
```

4. Add the import the remaining `update_table` trait impl needs (it calls
   `self.do_update_table(commit, CommitExtras::default())`):

```rust
use super::commit_mirror::CommitExtras;
```

5. Remove now-unused imports from `catalog.rs` (the compiler + clippy will name
   them — expect some of `MetadataLocation`, `LineageEvent`, `SnapshotId`,
   `ProjectedColumn`, `ProjectedFile`, `pg_emit` to go; `TableMetadataBuilder`
   etc. stay if still used elsewhere in the file).

- [ ] **Step 4: Wire the module in `mod.rs`**

Insert the `mod commit_mirror;` and `pub use commit_mirror::*;` lines into the
EXISTING file — keep the Apache license header, the module doc comment, and the
`#![deny(missing_docs)]` inner attribute exactly as they are. The module/use
section becomes:

```rust
mod catalog;
mod commit_mirror;
mod error;
pub mod s3_storage;
pub use catalog::*;
pub use commit_mirror::*;
pub use s3_storage::S3StorageFactory;
```

- [ ] **Step 5: Build + clippy + re-run the baseline suites**

```bash
buck2 build -M none //src/control-plane/postgres:control-plane-postgres
buck2 build '//src/control-plane/postgres:control-plane-postgres[clippy.txt]' --out /tmp/clippy-t1.txt 2>/dev/null; cat /tmp/clippy-t1.txt
buck2 test //src/control-plane/postgres:sqlcatalog-execute-commit \
  //src/control-plane/postgres:iceberg-writer \
  //src/control-plane/postgres:iceberg-write-roundtrip \
  //src/control-plane/postgres:iceberg-landing \
  //src/control-plane/postgres:iceberg-gc \
  > /tmp/task1-after.log 2>&1
grep -E "Tests finished|FAIL" /tmp/task1-after.log
```

Expected: build OK, clippy output empty, same pass counts as Step 1.
(`iceberg-gc` covers `delete_file`; `sqlcatalog-execute-commit` +
`iceberg-writer` cover `do_update_table`/`write_mirror`.)

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_sql_catalog/
git commit -m "refactor(iceberg): move loom commit surface out of vendored catalog.rs

CommitExtras/InlineEndCap/write_mirror/do_update_table/delete_file move
verbatim to the loom-owned sibling commit_mirror.rs; catalog.rs gets only
pub(super) visibility seams, so the vendored file stays re-vendorable."
```

---

### Task 2: Extract `end_cap_inline_rows_by_id` into `iceberg_inline.rs`

Kills the verbatim SQL duplicate (spec design point 1).

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (add helper, next to
  `end_cap_live_inline_rows`)
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs:374-391`
  (`land_additive`'s end-cap block)
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs`
  (`do_update_table`'s end-cap block — moved there by Task 1)

**Interfaces:**
- Consumes: Task 1's `commit_mirror.rs`.
- Produces: `pub(crate) async fn end_cap_inline_rows_by_id(conn: &mut PgConnection, table_id: i64, row_ids: &[i64], at: SnapshotId) -> Result<()>`
  in `iceberg_inline.rs` (crate-relative path `crate::iceberg_inline::end_cap_inline_rows_by_id`).
  Task 6's `apply_commit_extras` calls it.

- [ ] **Step 1: Add the helper to `iceberg_inline.rs`**

Place directly after `end_cap_live_inline_rows` (its sibling: "every live row"
vs "these specific rows"):

```rust
/// End-cap the SPECIFIC inline rows `row_ids` of `table_id` at snapshot `at`
/// (`end_snapshot = at` where `loom_row_id = any($1) and end_snapshot is null`).
/// Used by commits that retire just-flushed rows at the same snapshot the new
/// Parquet becomes live, so reads never double-serve or drop them. Runs in the
/// caller's transaction.
///
/// Runtime sqlx (not a compile-time `query!`): the `inline_<table_id>` table
/// name is a dynamic identifier and `any($1)` binds a row-id array — neither is
/// expressible in a literal, schema-checked macro. Spliced via `AssertSqlSafe`.
pub(crate) async fn end_cap_inline_rows_by_id(
    conn: &mut PgConnection,
    table_id: i64,
    row_ids: &[i64],
    at: SnapshotId,
) -> Result<()> {
    let sql = format!(
        "update {} set end_snapshot = {} \
         where loom_row_id = any($1) and end_snapshot is null",
        inline_table_name(table_id),
        at.0,
    );
    sqlx::query(AssertSqlSafe(sql))
        .bind(row_ids.to_vec())
        .execute(&mut *conn)
        .await
        .map_err(backend)?;
    Ok(())
}
```

(`.bind(row_ids.to_vec())` decouples the bind's lifetime from the parameter —
row-id sets are flush-sized, the copy is trivial. `backend` is already imported
in this file.)

- [ ] **Step 2: Call it from `land_additive` (iceberg_landing.rs)**

Replace the whole `if let Some(cap) = end_cap { ... }` block (the `let sql =
format!(...)` through `.map_err(be)?;`) with:

```rust
    if let Some(cap) = end_cap {
        // Retire the flushed inline rows at the same snapshot the new files become live
        // (faithful to `do_update_table`'s inline end-cap).
        crate::iceberg_inline::end_cap_inline_rows_by_id(&mut tx, cap.table_id, cap.row_ids, at)
            .await?;
    }
```

- [ ] **Step 3: Call it from `do_update_table` (commit_mirror.rs)**

Replace that site's `if let Some(cap) = &extras.end_cap { let sql = ...; }`
block with:

```rust
        if let Some(cap) = &extras.end_cap {
            // Retire the flushed inline rows at the same snapshot the new data file
            // becomes live, so reads never double-serve or drop them.
            crate::iceberg_inline::end_cap_inline_rows_by_id(&mut tx, cap.table_id, cap.row_ids, at)
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
        }
```

(Error mapping to the iceberg `Error` type matches the surrounding
`write_mirror`/`pg_emit` calls in the same function.)

- [ ] **Step 4: Build + clippy + run the covering suites**

The by-id end-cap fires on the flush path (`do_update_table` extras) — covered
by `iceberg-flush`, `inline-flush-trigger`, and `flush-vector-rebuild`:

```bash
buck2 build -M none //src/control-plane/postgres:control-plane-postgres
buck2 build '//src/control-plane/postgres:control-plane-postgres[clippy.txt]' --out /tmp/clippy-t2.txt 2>/dev/null; cat /tmp/clippy-t2.txt
buck2 test //src/control-plane/postgres:iceberg-flush \
  //src/control-plane/postgres:inline-flush-trigger \
  //src/control-plane/postgres:flush-vector-rebuild \
  //src/control-plane/postgres:iceberg-schema-evolution-land \
  > /tmp/task2.log 2>&1
grep -E "Tests finished|FAIL" /tmp/task2.log
```

Expected: clippy empty; all pass.

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_inline.rs \
  src/control-plane/postgres/src/iceberg_landing.rs \
  src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs
git commit -m "refactor(iceberg): one end_cap_inline_rows_by_id, called from both commit sites

The inline end-cap SQL was copy-pasted verbatim between land_additive and
do_update_table; both now call the iceberg_inline helper."
```

---

### Task 3: `CommitExtras` becomes `Clone`; the writer chain carries it whole

`CommitExtrasCatalog` currently holds the four extras fields loose and manually
re-assembles a `CommitExtras` per `update_table` call. Hold the aggregate
instead (spec design point 2, writer half).

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs`
  (derive `Clone` on both structs)
- Modify: `src/control-plane/postgres/src/iceberg_writer.rs`
  (`CommitExtrasCatalog`, `append_batches_with_extras`, `append_batches_with_lineage`)
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs`
  (`append_parquet_snapshot`'s call into `append_batches_with_extras`)

**Interfaces:**
- Consumes: Task 1's `commit_mirror.rs`.
- Produces:
  `pub async fn append_batches_with_extras(catalog: &SqlCatalog, table: &Table, batches: Vec<RecordBatch>, extras: CommitExtras<'_>) -> Result<Vec<WrittenFile>>`
  (iceberg `Result`); `CommitExtras<'a>: Default + Clone`,
  `InlineEndCap<'a>: Clone`. Tasks 4–6 rely on both.

- [ ] **Step 1: Derive `Clone`**

In `commit_mirror.rs` (all fields are borrows/`bool`/`i64`, so `Clone` is a
cheap field copy — needed because the commit-retry loop may call `update_table`
more than once from a `&self` method):

```rust
#[derive(Default, Clone)]
pub struct CommitExtras<'a> {
```

```rust
#[derive(Clone)]
pub struct InlineEndCap<'a> {
```

- [ ] **Step 2: Rework `CommitExtrasCatalog` in `iceberg_writer.rs`**

Replace the struct, its `Debug` impl, and the `update_table` override:

```rust
/// A per-call `Catalog` decorator that carries `CommitExtras` (lineage and/or an
/// inline end-cap) into the one `update_table` the iceberg commit performs, so both
/// land in the same Postgres tx as the pointer CAS + mirror projection. Every other
/// method delegates to the inner `SqlCatalog`. Constructed fresh per append (holds
/// borrows), so there is no shared mutable state across concurrent commits.
struct CommitExtrasCatalog<'a> {
    inner: &'a SqlCatalog,
    extras: CommitExtras<'a>,
}

impl std::fmt::Debug for CommitExtrasCatalog<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitExtrasCatalog")
            .field("lineage", &self.extras.lineage.is_some())
            .field("end_cap", &self.extras.end_cap.is_some())
            .field("overwrite", &self.extras.overwrite)
            .field("jobs", &self.extras.jobs.len())
            .finish()
    }
}
```

and in the `Catalog` impl, `update_table` becomes (the retry loop may call this
repeatedly; each call clones the borrowed aggregate):

```rust
    /// The one method that differs: route the commit through `do_update_table`
    /// with the extras so they commit/roll back atomically with the snapshot.
    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.inner.do_update_table(commit, self.extras.clone()).await
    }
```

- [ ] **Step 3: `append_batches_with_extras` takes the aggregate**

```rust
/// Append `batches` as real Parquet and commit, running `extras` (lineage and/or
/// inline end-cap) inside the one commit tx. Generalizes
/// [`append_batches_with_lineage`]. Takes a concrete `&SqlCatalog` because the
/// [`CommitExtrasCatalog`] decorator needs the inherent `do_update_table`.
pub async fn append_batches_with_extras(
    catalog: &SqlCatalog,
    table: &Table,
    batches: Vec<RecordBatch>,
    extras: CommitExtras<'_>,
) -> Result<Vec<WrittenFile>> {
    let data_files = write_parquet(table, batches).await?;
    let summaries: Vec<WrittenFile> = data_files
        .iter()
        .map(|df| WrittenFile {
            path: df.file_path().to_string(),
            record_count: df.record_count() as i64,
            file_size_bytes: df.file_size_in_bytes() as i64,
        })
        .collect();

    let wrapper = CommitExtrasCatalog {
        inner: catalog,
        extras,
    };
    commit_append_with_retry(&wrapper, table.identifier(), table.clone(), data_files).await?;
    Ok(summaries)
}
```

and the lineage wrapper:

```rust
pub async fn append_batches_with_lineage(
    catalog: &SqlCatalog,
    table: &Table,
    batches: Vec<RecordBatch>,
    lineage: &LineageEvent,
) -> Result<Vec<WrittenFile>> {
    append_batches_with_extras(
        catalog,
        table,
        batches,
        CommitExtras {
            lineage: Some(lineage),
            ..CommitExtras::default()
        },
    )
    .await
}
```

The `use crate::iceberg_sql_catalog::{CommitExtras, InlineEndCap, SqlCatalog};`
import line: `InlineEndCap` is no longer referenced here after this step —
drop it from the import.

- [ ] **Step 4: Update the one caller in `iceberg_landing.rs`**

`append_parquet_snapshot` still has loose params in this task (Task 4 changes
its signature); at its call into the writer, assemble the aggregate:

```rust
    append_batches_with_extras(
        catalog,
        &ice_table,
        batches,
        CommitExtras {
            lineage,
            end_cap,
            overwrite,
            jobs,
        },
    )
    .await
    .map_err(be)?;
```

Add `CommitExtras` to the `use crate::iceberg_sql_catalog::{...}` import in
`iceberg_landing.rs`.

- [ ] **Step 5: Build + clippy + run covering suites**

```bash
buck2 build -M none //src/control-plane/postgres:control-plane-postgres
buck2 build '//src/control-plane/postgres:control-plane-postgres[clippy.txt]' --out /tmp/clippy-t3.txt 2>/dev/null; cat /tmp/clippy-t3.txt
buck2 test //src/control-plane/postgres:iceberg-writer \
  //src/control-plane/postgres:iceberg-write-roundtrip \
  //src/control-plane/postgres:iceberg-landing \
  //src/control-plane/postgres:iceberg-flush \
  > /tmp/task3.log 2>&1
grep -E "Tests finished|FAIL" /tmp/task3.log
```

Expected: clippy empty; all pass (`iceberg-write-roundtrip` includes the
concurrent-writer CAS-retry test, which exercises the cloned-extras retry path).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs \
  src/control-plane/postgres/src/iceberg_writer.rs \
  src/control-plane/postgres/src/iceberg_landing.rs
git commit -m "refactor(iceberg): writer chain carries CommitExtras whole

CommitExtras/InlineEndCap derive Clone; CommitExtrasCatalog holds the
aggregate and clones it per retry instead of re-assembling four loose fields."
```

---

### Task 4: Landing signatures carry `CommitExtras<'_>` (delete two allows)

`append_parquet_snapshot` and `land_additive` swap their four loose
extras params for one `CommitExtras<'_>`, dropping to 6 params each — delete
their `#[allow(clippy::too_many_arguments)]` blocks (spec design point 2,
landing half).

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs`
  (`append_parquet_snapshot`, `land_additive`, `land_parquet`,
  `overwrite_parquet_snapshot`)
- Modify: `src/control-plane/postgres/src/iceberg_flush.rs` (`flush_locked` call site)

**Interfaces:**
- Consumes: Task 3's `append_batches_with_extras(..., CommitExtras<'_>)` and
  `CommitExtras: Default + Clone`.
- Produces:
  `pub(crate) async fn append_parquet_snapshot(pool: &PgPool, catalog: &SqlCatalog, table: &TableRef, columns: &[ColumnSpec], batches: Vec<RecordBatch>, extras: CommitExtras<'_>) -> Result<SnapshotId>`
  and
  `async fn land_additive(pool: &PgPool, catalog: &SqlCatalog, table: &TableRef, columns: &[ColumnSpec], batches: Vec<RecordBatch>, extras: CommitExtras<'_>) -> Result<SnapshotId>`.
  Task 6 rewrites `land_additive`'s tail onto `apply_commit_extras`.

- [ ] **Step 1: Change `append_parquet_snapshot`**

Delete its `#[allow(clippy::too_many_arguments, reason = ...)]` attribute and
change the signature (doc comment: mention `extras` runs in the commit tx):

```rust
/// Ensure the iceberg table exists (create-if-absent from `columns`), append
/// `batches` (bare arrow — re-wrapped under the table's field-id schema) as a
/// real Parquet snapshot running `extras` in the commit tx, and return the mirror
/// snapshot id. Shared by the landing Parquet path and the flush path.
pub(crate) async fn append_parquet_snapshot(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    extras: CommitExtras<'_>,
) -> Result<SnapshotId> {
```

In the body:
- the Additive branch forwards the whole aggregate:

```rust
                return land_additive(pool, catalog, table, columns, batches, extras).await;
```

- the writer call (from Task 4 on) passes it straight through:

```rust
    append_batches_with_extras(catalog, &ice_table, batches, extras)
        .await
        .map_err(be)?;
```

- [ ] **Step 2: Change `land_additive`**

Delete its `#[allow(clippy::too_many_arguments, ...)]`; signature:

```rust
async fn land_additive(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    extras: CommitExtras<'_>,
) -> Result<SnapshotId> {
```

Body updates (tail of the function):

```rust
    if let Some(cap) = &extras.end_cap {
        // Retire the flushed inline rows at the same snapshot the new files become live
        // (faithful to `do_update_table`'s inline end-cap).
        crate::iceberg_inline::end_cap_inline_rows_by_id(&mut tx, cap.table_id, cap.row_ids, at)
            .await?;
    }
    if let Some(ev) = extras.lineage {
        pg_emit(&mut *tx, ev).await?;
    }
    for job in extras.jobs {
        crate::queue::pg_insert_if_absent(&mut *tx, job).await?;
    }
```

Append to the function's doc comment (this pins today's behavior — the old call
chain dropped the `overwrite` flag before reaching this function):

```rust
/// `extras.overwrite` is deliberately NOT applied here: an additive land is
/// always an append (`WriteMode::Append`, prior files stay live) — faithful to
/// the pre-`CommitExtras` chain, which never forwarded the flag to this path.
```

- [ ] **Step 3: Update the three internal callers**

`land_parquet`:

```rust
async fn land_parquet(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: LineageEvent,
) -> Result<SnapshotId> {
    append_parquet_snapshot(
        pool,
        catalog,
        table,
        columns,
        batches,
        CommitExtras {
            lineage: Some(&lineage),
            ..CommitExtras::default()
        },
    )
    .await
}
```

`overwrite_parquet_snapshot` (non-empty branch):

```rust
    append_parquet_snapshot(
        pool,
        catalog,
        table,
        columns,
        batches,
        CommitExtras {
            lineage,
            overwrite: true,
            ..CommitExtras::default()
        },
    )
    .await
```

`iceberg_flush.rs::flush_locked` — replace the 9-arg call with:

```rust
    let snap = append_parquet_snapshot(
        pool,
        catalog,
        table,
        &columns,
        vec![batch],
        CommitExtras {
            lineage: Some(&lineage),
            end_cap: Some(end_cap),
            jobs: &rebuild_jobs,
            ..CommitExtras::default()
        },
    )
    .await?;
```

and change its import to
`use crate::iceberg_sql_catalog::{CommitExtras, InlineEndCap, SqlCatalog};`.

Also in `iceberg_landing.rs`: after this task nothing in the file references
`InlineEndCap` anymore (the end-cap block only reads `cap.table_id`/`cap.row_ids`
through `&extras.end_cap`), so drop `InlineEndCap` from its
`use crate::iceberg_sql_catalog::{...}` import — keeping it would fail this
task's empty-clippy gate. (`iceberg_flush.rs` still constructs one; it keeps
the import.)

- [ ] **Step 4: Build + clippy + run covering suites**

```bash
buck2 build -M none //src/control-plane/postgres:control-plane-postgres
buck2 build '//src/control-plane/postgres:control-plane-postgres[clippy.txt]' --out /tmp/clippy-t4.txt 2>/dev/null; cat /tmp/clippy-t4.txt
buck2 test //src/control-plane/postgres:iceberg-landing \
  //src/control-plane/postgres:iceberg-flush \
  //src/control-plane/postgres:inline-flush-trigger \
  //src/control-plane/postgres:flush-vector-rebuild \
  //src/control-plane/postgres:iceberg-overwrite \
  //src/control-plane/postgres:overwrite-end-caps-inline \
  //src/control-plane/postgres:iceberg-schema-evolution-land \
  > /tmp/task4.log 2>&1
grep -E "Tests finished|FAIL" /tmp/task4.log
```

Expected: clippy empty (confirms the two allows really were deletable); all pass.

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_landing.rs \
  src/control-plane/postgres/src/iceberg_flush.rs
git commit -m "refactor(iceberg): landing signatures carry CommitExtras

append_parquet_snapshot and land_additive take the aggregate instead of
lineage/end_cap/overwrite/jobs loose; both too_many_arguments allows deleted."
```

---

### Task 5: `land` carries `InlineLimits` (delete the third allow) + call-site sweep

`land`'s two routing limits group into one `InlineLimits` struct, bringing it to
7 params — the last `too_many_arguments` allow in the file goes. This is the
wide-but-mechanical task: 2 production call sites + 24 test files.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (`land` + new struct)
- Modify: `src/services/ingest/src/landing.rs` (`IcebergMaterializer::land`)
- Modify: `src/services/engine-serving/src/action_writer.rs` (`write_object`)
- Modify (tests, mechanical): every file importing `iceberg_landing::land` —
  `src/control-plane/postgres/tests/{flush_vector_rebuild,iceberg_compact,iceberg_gc,iceberg_landing,iceberg_overwrite,iceberg_read,iceberg_schema_evolution_land,iceberg_tx_compact,vector_index_build,vector_index_hnsw,vector_index_ivf,vector_index_multi,vector_landing}.rs`,
  `src/services/engine-serving/tests/{vector_index_auto_rebuild,vector_search}.rs`,
  `src/services/engine/tests/{compact_wire,vector_search_flight}.rs`,
  `src/services/query-api/tests/{e2e_support,governed_flight_export_e2e,iceberg_schema_evolution_read}.rs`,
  `src/services/worker/tests/{build_vector_index,compact_e2e,e2e,flight_roundtrip}.rs`

**Interfaces:**
- Consumes: nothing new (independent of Tasks 2–4 logically, sequenced here to
  keep each diff reviewable).
- Produces:
  `#[derive(Clone, Copy, Debug)] pub struct InlineLimits { pub inline_byte_limit: usize, pub flush_byte_threshold: i64 }`
  and
  `pub async fn land(pool: &PgPool, catalog: &SqlCatalog, table: &TableRef, columns: &[ColumnSpec], ipc_body: &[u8], limits: InlineLimits, lineage: LineageEvent) -> Result<SnapshotId>`.

- [ ] **Step 1: Add `InlineLimits` and change `land` in `iceberg_landing.rs`**

Directly above `land`:

```rust
/// The inline-tier routing limits carried by [`land`]: at/below
/// `inline_byte_limit` a request inlines (mirror-only typed rows) instead of
/// writing real Parquet; at/above `flush_byte_threshold` live inline bytes an
/// inline write enqueues a `flush_table` job.
#[derive(Clone, Copy, Debug)]
pub struct InlineLimits {
    /// In-memory (uncompressed) Arrow size at/below which the request inlines.
    pub inline_byte_limit: usize,
    /// Live-inline-byte total at/above which a `flush_table` job is enqueued
    /// after an inline write.
    pub flush_byte_threshold: i64,
}
```

Delete `land`'s `#[allow(clippy::too_many_arguments, ...)]` and change:

```rust
/// Land an Iceberg request, routing by in-memory size per `limits` (see
/// [`InlineLimits`]). Returns the loom mirror snapshot id either way.
pub async fn land(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    ipc_body: &[u8],
    limits: InlineLimits,
    lineage: LineageEvent,
) -> Result<SnapshotId> {
    let (schema, batches) = decode_ipc(ipc_body)?;
    // ... (alignment comment + call unchanged) ...
    let (schema, batches) = align_to_columns(&schema, batches, columns)?;
    let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
    if bytes <= limits.inline_byte_limit {
        let batch = concat_batches(&schema, &batches).map_err(be)?;
        inline_append(
            pool,
            table,
            columns,
            &batch,
            lineage,
            Some(limits.flush_byte_threshold),
        )
        .await
    } else {
        land_parquet(pool, catalog, table, columns, batches, lineage).await
    }
}
```

- [ ] **Step 2: Update the two production call sites**

`src/services/ingest/src/landing.rs` (`IcebergMaterializer` keeps its two pub
fields; assemble at the call):

```rust
use control_plane_postgres::iceberg_landing::{InlineLimits, land as iceberg_land};
```

```rust
    async fn land(&self, req: LandRequest<'_>) -> Result<SnapshotId, IngestError> {
        iceberg_land(
            &self.pool,
            &self.catalog,
            req.table,
            req.columns,
            req.ipc_body,
            InlineLimits {
                inline_byte_limit: self.inline_byte_limit,
                flush_byte_threshold: self.flush_byte_threshold,
            },
            req.lineage,
        )
        .await
        .map_err(IngestError::from)
    }
```

`src/services/engine-serving/src/action_writer.rs` (same pattern in
`write_object`; add `InlineLimits` to the existing
`use control_plane_postgres::iceberg_landing` import — check the file's current
import shape, it uses `iceberg_landing::land(...)` qualified, so either import
`InlineLimits` or qualify it the same way):

```rust
        iceberg_landing::land(
            &self.pool,
            &self.catalog,
            table,
            columns,
            ipc,
            iceberg_landing::InlineLimits {
                inline_byte_limit: self.inline_byte_limit,
                flush_byte_threshold: self.flush_byte_threshold,
            },
            event,
        )
        .await
        .map_err(|e| EngineServingError::Engine(e.to_string()))
```

- [ ] **Step 3: Sweep the test call sites**

Every `land(` call in the 24 test files replaces its two positional limit args
(6th and 7th) with one `InlineLimits { inline_byte_limit: <old 6th>,
flush_byte_threshold: <old 7th> }` literal, and each file's import gains
`InlineLimits`, e.g.:

```rust
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
```

```rust
    land(
        &pool,
        &catalog,
        &t,
        &columns(),
        &body,
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        ev("run-1"),
    )
    .await
    .expect("land");
```

Where a test file defines its own thin `land`-wrapper helper, change the helper
once and leave its callers alone. Use the compiler as the checklist: build each
affected test target and fix every mismatched-args error it reports
(`buck2 build -M none <target>` per crate below).

- [ ] **Step 4: Build everything affected + clippy + run the landing-critical suites**

```bash
buck2 build -M none //src/control-plane/postgres: //src/services/ingest: \
  //src/services/engine-serving: //src/services/engine: \
  //src/services/query-api: //src/services/worker: > /tmp/task5-build.log 2>&1
tail -5 /tmp/task5-build.log
buck2 build '//src/control-plane/postgres:control-plane-postgres[clippy.txt]' --out /tmp/clippy-t5a.txt 2>/dev/null; cat /tmp/clippy-t5a.txt
buck2 build '//src/services/ingest:ingest[clippy.txt]' --out /tmp/clippy-t5b.txt 2>/dev/null; cat /tmp/clippy-t5b.txt
buck2 build '//src/services/engine-serving:engine-serving[clippy.txt]' --out /tmp/clippy-t5c.txt 2>/dev/null; cat /tmp/clippy-t5c.txt
buck2 test //src/control-plane/postgres:iceberg-landing \
  //src/control-plane/postgres:iceberg-read \
  //src/control-plane/postgres:vector-landing \
  //src/services/ingest:iceberg-land \
  //src/services/engine-serving:action-writer \
  > /tmp/task5.log 2>&1
grep -E "Tests finished|FAIL" /tmp/task5.log
```

(If the ingest/engine-serving test-target names differ, list them with
`buck2 targets //src/services/ingest: //src/services/engine-serving: | grep -i 'land\|action'`
and run the landing/action-writer ones.) Expected: builds green, clippy empty,
tests pass. The remaining swept test targets are compile-verified by the build
line; they run in CI's `affected` job and the final task's postgres sweep.

- [ ] **Step 5: Commit**

```bash
git add -A src/
git commit -m "refactor(iceberg): land() takes InlineLimits; last too_many_arguments allow deleted

The two inline-tier routing limits group into one named struct; ingest,
engine-serving, and the test call sites updated mechanically."
```

---

### Task 6: `apply_commit_extras` — one commit tail for `land_additive` and `do_update_table`

The end-cap + lineage + jobs tail is now the same three-block sequence in both
transactions; extract it (spec design point 3). `do_update_table` becomes
CAS → `write_mirror` → `apply_commit_extras` → commit.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs`
  (new free function + `do_update_table` tail)
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (`land_additive` tail)

**Interfaces:**
- Consumes: Task 2's `end_cap_inline_rows_by_id`, Task 4's
  `land_additive(..., extras: CommitExtras<'_>)`.
- Produces:
  `pub(crate) async fn apply_commit_extras(conn: &mut PgConnection, at: SnapshotId, extras: &CommitExtras<'_>) -> control_plane_core::Result<()>`
  in `commit_mirror.rs`, reachable crate-wide as
  `crate::iceberg_sql_catalog::apply_commit_extras` (the `pub use` glob
  re-exports it at its `pub(crate)` visibility).

- [ ] **Step 1: Add the function to `commit_mirror.rs`**

```rust
/// Apply the non-projection commit side-effects — inline end-cap, lineage
/// emit, job enqueue — in the caller's commit transaction at snapshot `at`, in
/// that (load-bearing) order. Shared by [`SqlCatalog::do_update_table`] (the
/// CAS commit) and `iceberg_landing::land_additive` (the mirror-only additive
/// commit) so the extras semantics cannot drift between them.
///
/// `extras.overwrite` is NOT applied here: overwrite ordering (end-cap the live
/// files BEFORE projecting the new ones) belongs to the projection step
/// (`write_mirror` / `register_files`), which runs before this.
pub(crate) async fn apply_commit_extras(
    conn: &mut sqlx::PgConnection,
    at: SnapshotId,
    extras: &CommitExtras<'_>,
) -> control_plane_core::Result<()> {
    if let Some(cap) = &extras.end_cap {
        // Retire the flushed inline rows at the same snapshot the new data
        // becomes live, so reads never double-serve or drop them.
        crate::iceberg_inline::end_cap_inline_rows_by_id(conn, cap.table_id, cap.row_ids, at)
            .await?;
    }
    if let Some(ev) = extras.lineage {
        pg_emit(&mut *conn, ev).await?;
    }
    for job in extras.jobs {
        crate::queue::pg_insert_if_absent(&mut *conn, job).await?;
    }
    Ok(())
}
```

- [ ] **Step 2: `do_update_table` tail uses it**

Replace the three blocks after the `write_mirror` call (the
`if let Some(cap) = &extras.end_cap {...}`, `if let Some(ev) = extras.lineage {...}`,
and `for job in extras.jobs {...}` blocks) with:

```rust
        apply_commit_extras(&mut tx, at, &extras)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;

        tx.commit().await.map_err(from_sqlx_error)?;
        Ok(staged_table)
```

- [ ] **Step 3: `land_additive` tail uses it**

In `iceberg_landing.rs`, replace the same three blocks (between
`register_files(...)` and `tx.commit()`) with:

```rust
    crate::iceberg_sql_catalog::apply_commit_extras(&mut tx, at, &extras).await?;
    tx.commit().await.map_err(be)?;
    Ok(at)
```

(The `use crate::lineage::pg_emit;` inside `land_additive` and the
`end_cap_inline_rows_by_id` call added in Task 2 become unused there — remove
them. Check the whole file for now-unused imports; the compiler will name them.)

- [ ] **Step 4: Build + clippy + run covering suites**

```bash
buck2 build -M none //src/control-plane/postgres:control-plane-postgres
buck2 build '//src/control-plane/postgres:control-plane-postgres[clippy.txt]' --out /tmp/clippy-t6.txt 2>/dev/null; cat /tmp/clippy-t6.txt
buck2 test //src/control-plane/postgres:iceberg-flush \
  //src/control-plane/postgres:inline-flush-trigger \
  //src/control-plane/postgres:flush-vector-rebuild \
  //src/control-plane/postgres:iceberg-schema-evolution-land \
  //src/control-plane/postgres:iceberg-writer \
  //src/control-plane/postgres:lineage-roundtrip \
  > /tmp/task6.log 2>&1
grep -E "Tests finished|FAIL" /tmp/task6.log
```

Expected: clippy empty; all pass (`flush-vector-rebuild` pins the jobs-in-tx
dedup; `iceberg-flush` pins end-cap + lineage atomicity).

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_sql_catalog/commit_mirror.rs \
  src/control-plane/postgres/src/iceberg_landing.rs
git commit -m "refactor(iceberg): shared apply_commit_extras commit tail

do_update_table becomes CAS -> write_mirror -> apply_commit_extras -> commit;
land_additive shares the same tail, so extras semantics cannot drift."
```

---

### Task 7: Whole-crate verification + register evidence

**Files:**
- No source changes expected (fix-forward if anything is red).
- Create (scratch, git-ignored): `.superpowers/sdd/register-evidence.md`

**Interfaces:**
- Consumes: all prior tasks.
- Produces: green full-crate evidence + before/after duplication/complexity
  numbers for the PR body.

- [ ] **Step 1: Full postgres-crate test sweep**

```bash
buck2 test //src/control-plane/postgres/... > /tmp/task7-pg.log 2>&1
grep -E "Tests finished|FAIL" /tmp/task7-pg.log
```

Expected: all targets pass (this includes `sqlx-cache-check`; no compile-time
SQL changed, so the committed `.sqlx` cache must be untouched — verify with
`git status src/control-plane/postgres/.sqlx`).

- [ ] **Step 2: Compile-verify every crate the Task-5 sweep touched**

```bash
buck2 build -M none //src/services/ingest: //src/services/engine-serving: \
  //src/services/engine: //src/services/query-api: //src/services/worker: \
  > /tmp/task7-build.log 2>&1
tail -3 /tmp/task7-build.log
```

Expected: green (their full test runs happen in CI's `affected` job).

- [ ] **Step 3: Clippy across changed crates + prek hooks**

```bash
buck2 build '//src/control-plane/postgres:control-plane-postgres[clippy.txt]' --out /tmp/c1.txt 2>/dev/null
buck2 build '//src/services/ingest:ingest[clippy.txt]' --out /tmp/c2.txt 2>/dev/null
buck2 build '//src/services/engine-serving:engine-serving[clippy.txt]' --out /tmp/c3.txt 2>/dev/null
cat /tmp/c1.txt /tmp/c2.txt /tmp/c3.txt
buck2 run //tools:prek -- run --all-files > /tmp/task7-prek.log 2>&1
tail -20 /tmp/task7-prek.log
```

Expected: clippy outputs empty; prek all green (commit any in-place hook fixes).

- [ ] **Step 4: Register evidence (duplication + complexity diff)**

Run the two code-health skills in `diff` mode (BLOCK A of each, verbatim from
`.claude/skills/loom-duplication/SKILL.md` and `.claude/skills/loom-complexity/SKILL.md`)
and record in `.superpowers/sdd/register-evidence.md`:
- the `iceberg_landing.rs:374-391` ↔ `catalog.rs:553-567` duplicate pair is gone;
- no new duplication pairs and no new complexity-register entrants in the
  touched files.

- [ ] **Step 5: Commit any residue**

```bash
git status --short
# commit prek fixes / evidence-only changes if any (docs paths only):
git add -A && git commit -m "chore(iceberg): post-refactor verification residue" || echo "clean"
```

---

## Explicitly out of scope (do not do)

- `overwrite_truncate` and `IcebergTx::commit` keep their own tx blocks — they
  have no `CommitExtras` inputs; forcing them onto the spine adds indirection
  for nothing (YAGNI).
- No change to `overwrite_parquet_snapshot`'s public signature (6 params, no
  allow) beyond its internal call rewrite in Task 4.
- No unification of `one_cell`/`cell_from_arrow` value codecs (explicitly
  rejected by the spec; tracked by `fut-coercion-taxonomy`).
- No new `#[cfg(test)]` modules, no new test files, no `.sqlx` regeneration,
  no `third-party/BUCK` changes (zero dependency changes).
- The `iss-iceberg-tx-objectstore` property (no object-store IO inside the
  commit tx) must remain type-enforced: `write_mirror` keeps its
  no-`&Table`/no-`FileIO` signature.
