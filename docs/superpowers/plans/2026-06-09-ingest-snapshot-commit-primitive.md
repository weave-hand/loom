# Transactional snapshot-commit primitive — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give loom a native DuckLake writer on the `Tx` seam — `create_table` + `append_files` that commit `ducklake_*` catalog rows atomically with lineage `emit` + queue `enqueue` in one Postgres transaction.

**Architecture:** loom writes the single-catalog, DuckDB-compatible `ducklake_*` rows itself via sqlx (no DuckDB engine in the commit path). The `Tx` accumulates staged catalog ops; `commit()` takes an advisory lock, reads the latest `ducklake_snapshot` row, allocates ids in memory, writes the exact row-set, then commits. Built incrementally against a DuckDB-engine interop test: append first (smaller row-set), then create_table.

**Tech Stack:** Rust 2024, buck2, sqlx 0.9 (compile-time `query!`), Postgres (hermetic `PgFixture`), the pinned DuckDB 1.5.3 `ducklake` extension (interop oracle).

---

## Background the implementer MUST read first

- **The exact write recipe is a committed, source-grounded reference:** `docs/superpowers/specs/2026-06-09-ducklake-single-catalog-write-recipe.md` (grounded in `duckdb/ducklake@e6a3bd0a` = DuckDB 1.5.3, spec v1.0, with `file:line` citations). Tasks below cite it as "recipe §N / Op X". **Use the exact column tuples, counter rules, and `changes_made` strings from that doc — do not re-derive them.** The most error-prone facts (already corrected there): `ducklake_schema_versions` is 3-column `(begin_snapshot, schema_version, table_id)`; `column_id` is a per-table 1-based counter (NOT from `next_catalog_id`); `next_catalog_id` is consumed by schema/table/partition/sort/view/macro only; `next_file_id` spans data+delete+mapping ids; `value_count` = non-null count; all writer INSERTs are positional (no column list).
- **The design spec:** `docs/superpowers/specs/2026-06-09-ingest-snapshot-commit-primitive-design.md`.
- **Testing model (critical):** loom runs ONLY `rust_test` integration targets (no inline `#[cfg(test)]` runner). Shared conformance lives in `testkit/src/lib.rs` as `pub` fns; each adapter has per-concern `rust_test` targets that call them. New tests are `rust_test` targets over the public API.
- **Test command:** `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...` (hermetic pg/duckdb refuse root on remote execution). Lint: `tools/clippy-all.sh` and `buck2 run //tools:prek -- run --all-files`.
- **Compile-time SQL:** the postgres adapter uses `query!`/`query_scalar!` against the committed `.sqlx` cache. After adding/changing SQL, run `tools/sqlx-prepare.sh` and commit the `.sqlx` change; the `sqlx-cache-check` rust_test gates freshness. The prepare harness already boots pg, applies migrations, and ATTACHes a real DuckLake catalog (so `ducklake_*` writes validate).
- **`PgFixture`** (`postgres/src/fixture.rs`): `PgFixture::start()` (sync) boots hermetic pg; `fresh_db().await -> (PgControlPlane, String)`; `socket_path()`. `DuckLakeWriter::new(socket, db)` + `.seed(schema, table, &[(name,ty,nullable)], &[batch_sizes]).await` ATTACHes a DuckLake catalog and creates+fills a table via the pinned duckdb-cli. This is both the catalog **bootstrap** (the bare ATTACH creates the 27 `ducklake_*` tables + snapshot 0 + `main` schema — recipe §1) and the **interop oracle**.
- **Bootstrap decision (recipe §1):** loom does NOT create the `ducklake_*` tables; a duckdb-cli `ATTACH` bootstraps them (the fixture/`sqlx-prepare.sh` already do this). loom is a pure row writer.
- **Snapshot id is allocated at commit time**, so `emit` cannot embed it; OpenLineage events key on dataset/run, not the loom snapshot id. `commit -> Option<SnapshotId>` returns it for the caller's own use.

## File Structure

- **Create** `src/control-plane/core/src/snapshot.rs` — `ColumnSpec`, `ColumnStat`, `DataFile` (the register-only input types).
- **Modify** `core/src/lib.rs` — `mod snapshot;` + re-export; `core/src/transaction.rs` — `Tx::{create_table, append_files}`, `commit -> Option<SnapshotId>`.
- **Create** `src/control-plane/postgres/src/snapshot.rs` — the native `ducklake_*` row writers (append + create_table), driven from `PgTx::commit`.
- **Modify** `postgres/src/transaction.rs` — `PgTx` stages catalog ops; `commit` does advisory-lock + allocate + write + commit.
- **Modify** `memory/src/transaction.rs` + `memory/src/catalog.rs` — stage + apply to `CatalogState`.
- **Modify** `testkit/src/lib.rs` — `snapshot_commit_contract` conformance fn.
- **Create** `postgres/tests/ducklake_interop.rs` — loom-writes / DuckDB-reads interop test.
- **Modify** `postgres/BUCK`, `memory/BUCK`, `postgres/.sqlx/` (regen).

---

## Task 1: core API — types, `Tx` methods, `commit -> Option<SnapshotId>`, memory impl

**Files:**
- Create: `src/control-plane/core/src/snapshot.rs`
- Modify: `src/control-plane/core/src/lib.rs`, `src/control-plane/core/src/transaction.rs`
- Modify: `src/control-plane/memory/src/transaction.rs`, `src/control-plane/memory/src/catalog.rs`
- Modify: `src/control-plane/postgres/src/transaction.rs` (compiling transient)
- Modify: call sites of `Tx::commit` (`testkit/src/lib.rs`, `src/control-plane/worker/src/*`)
- Test: `src/control-plane/memory/tests/snapshot.rs` (new `rust_test`)

- [ ] **Step 1: Add the input types** — create `core/src/snapshot.rs`:

```rust
//! Inputs for the native DuckLake snapshot-commit primitive (register-only: the
//! caller writes the Parquet, loom writes the catalog rows). See
//! `docs/superpowers/specs/2026-06-09-ducklake-single-catalog-write-recipe.md`.

/// A column for `Tx::create_table`. `ty` is a DuckLake type string ("int64",
/// "varchar", …) — the dialect stored in `ducklake_column.column_type`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnSpec {
    pub name: String,
    pub ty: String,
    pub nullable: bool,
}

/// Per-column statistics for one data file (values as strings, matching DuckLake's
/// VARCHAR stat encoding). `min`/`max` are `None` when absent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnStat {
    pub column_name: String,
    pub min: Option<String>,
    pub max: Option<String>,
    pub null_count: i64,
    /// Count of non-null values (DuckLake `value_count = num_values - null_count`).
    pub value_count: i64,
    pub column_size_bytes: i64,
}

/// A Parquet file the caller has already written to object storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataFile {
    /// Relative filename resolved against the catalog's data_path (e.g.
    /// "ducklake-<uuid>.parquet"); `path_is_relative` is then true.
    pub path: String,
    pub path_is_relative: bool,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub footer_size: i64,
    pub column_stats: Vec<ColumnStat>,
}
```

- [ ] **Step 2: Re-export from `lib.rs`** — add `mod snapshot;` (alphabetical, after `mod queue;`/before `mod transaction;`) and:

```rust
pub use snapshot::{ColumnSpec, ColumnStat, DataFile};
```

- [ ] **Step 3: Extend the `Tx` trait** — in `core/src/transaction.rs`, add imports `use crate::catalog::TableRef; use crate::snapshot::{ColumnSpec, DataFile}; use crate::catalog::SnapshotId;` and change the trait:

```rust
#[async_trait]
pub trait Tx: Send {
    /// Commit the unit of work. Returns the new `SnapshotId` if a catalog op
    /// (create_table/append_files) was staged, else `None`.
    async fn commit(self: Box<Self>) -> Result<Option<SnapshotId>>;
    async fn rollback(self: Box<Self>) -> Result<()>;
    async fn enqueue(&mut self, job: NewJob) -> Result<JobId>;
    async fn emit(&mut self, event: LineageEvent) -> Result<()>;
    /// Create a physical DuckLake table. Staged; applied as part of the snapshot at
    /// commit. Idempotent: a no-op if the table already exists live.
    async fn create_table(&mut self, table: &TableRef, columns: &[ColumnSpec]) -> Result<()>;
    /// Register already-written Parquet data files as part of the snapshot. Staged.
    async fn append_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()>;
}
```

- [ ] **Step 4: Update existing `commit` call sites** — every `tx.commit().await?` now yields `Option<SnapshotId>`; existing callers ignore it (`let _ = tx.commit().await?;` or just `tx.commit().await?;` — the unused `Option` is fine). Touch: `testkit/src/lib.rs` (3 sites), `worker` (its commit, if any). The postgres/memory adapter `commit` impls are updated in Steps 5–6.

- [ ] **Step 5: Implement in the memory adapter** — in `memory/src/transaction.rs`, add staged fields and methods; in `memory/src/catalog.rs` add an apply helper that mirrors `seed_catalog`. Add to `MemoryTx`:

```rust
    pub(crate) staged_tables: Vec<(TableRef, Vec<ColumnSpec>)>,
    pub(crate) staged_files: Vec<(TableRef, Vec<DataFile>)>,
```

`create_table`/`append_files` push onto these (idempotent create: still stage; apply skips if table already live). `commit` (still holding the locks) applies staged catalog ops to `CatalogState`: for each staged table not already present, `new_snapshot()` + insert `tables`/`columns` (map `ColumnSpec`→`ColumnDef{order,name,ty,nullable}`); for each staged file batch, `new_snapshot()` + push `FileRef{path,record_count,file_size_bytes}` into `files`. Return `Ok(Some(SnapshotId(last_new_snapshot)))` if any catalog op was staged, else `Ok(None)`. (Memory keeps its simple model — it does NOT mirror DuckLake's exact ids; conformance asserts behaviour, not DuckLake layout.)

Update `MemoryControlPlane::begin` to initialize the new staged vecs empty.

- [ ] **Step 6: Postgres compiling transient** — in `postgres/src/transaction.rs`, add `staged_tables`/`staged_files` `Vec`s to `PgTx`, implement `create_table`/`append_files` to push onto them, change `commit` to `-> Result<Option<SnapshotId>>`. For now, if any catalog op was staged, `commit` returns `Err(ControlPlaneError::Backend("snapshot commit not yet implemented".into()))`; otherwise it commits the inner tx and returns `Ok(None)`. (Replaced in Tasks 2–3. No postgres test stages catalog ops yet, so existing tests stay green.)

- [ ] **Step 7: Write the memory conformance test** — create `memory/tests/snapshot.rs` as a `rust_test` calling a new `testkit` fn (add it in this step to `testkit/src/lib.rs`):

```rust
// testkit/src/lib.rs
pub async fn snapshot_commit_contract<C>(cp: &C)
where C: ControlPlane + Catalog + Lineage + Queue {
    use control_plane_core::{ColumnSpec, DataFile, PageReq};
    let t = TableRef { schema: "main".into(), name: "events".into() };
    // create + append + emit + enqueue, all in one tx
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(&t, &[
        ColumnSpec { name: "id".into(), ty: "int64".into(), nullable: false },
    ]).await.unwrap();
    tx.append_files(&t, &[DataFile {
        path: "a.parquet".into(), path_is_relative: true,
        record_count: 3, file_size_bytes: 48, footer_size: 10,
        column_stats: vec![],
    }]).await.unwrap();
    let snap = tx.commit().await.unwrap();
    assert!(snap.is_some(), "catalog op produces a snapshot id");
    // visible via Catalog reads
    let snaps = cp.snapshots(&t, PageReq::unbounded()).await.unwrap();
    assert!(!snaps.is_empty());
    let latest = cp.current_snapshot(&t).await.unwrap();
    assert_eq!(cp.files(&t, latest.id, PageReq::unbounded()).await.unwrap().len(), 1);

    // rollback leaves nothing
    let t2 = TableRef { schema: "main".into(), name: "rolled".into() };
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(&t2, &[ColumnSpec { name: "x".into(), ty: "int64".into(), nullable: true }]).await.unwrap();
    tx.rollback().await.unwrap();
    assert!(matches!(cp.snapshots(&t2, PageReq::unbounded()).await, Err(_) | Ok(_)));
    // a rolled-back table must not be live:
    assert!(cp.current_snapshot(&t2).await.is_err(), "rolled-back table absent");
}
```

```rust
// memory/tests/snapshot.rs
use control_plane_memory::MemoryControlPlane;

#[tokio::test]
async fn snapshot_commit_contract() {
    let cp = MemoryControlPlane::new();
    control_plane_testkit::snapshot_commit_contract(&cp).await;
}
```

Add the `rust_test` target to `memory/BUCK`:

```python
rust_test(
    name = "snapshot",
    crate = "snapshot",
    srcs = ["tests/snapshot.rs"],
    crate_root = "tests/snapshot.rs",
    edition = "2024",
    deps = [":memory", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

- [ ] **Step 8: Build + test + lint**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:snapshot //src/control-plane/memory/... //src/control-plane/worker/...`
Expected: PASS (memory snapshot conformance passes; existing memory/worker tests unchanged). Then `tools/clippy-all.sh` clean.

- [ ] **Step 9: Commit**

```bash
git add src/control-plane/core src/control-plane/memory src/control-plane/postgres/src/transaction.rs src/control-plane/testkit/src/lib.rs src/control-plane/worker
git commit -m "feat(core): snapshot-commit Tx API (create_table/append_files); memory impl

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: postgres `append_files` native writer

Implements the smaller row-set first (recipe §3 **Op B/C**), proving the writer core: advisory lock, latest-snapshot read, id allocation, and the data-file rows. The table is created by the DuckLake fixture (DuckDB) for this task; loom's own `create_table` lands in Task 3.

**Files:**
- Create: `src/control-plane/postgres/src/snapshot.rs`
- Modify: `src/control-plane/postgres/src/transaction.rs`, `src/control-plane/postgres/src/lib.rs` (module decl)
- Modify: `src/control-plane/postgres/.sqlx/` (regen), `postgres/BUCK` (if a new test target)
- Test: `src/control-plane/postgres/tests/snapshot_append.rs` (new `rust_test`)

- [ ] **Step 1: Write the failing test** — `postgres/tests/snapshot_append.rs`: boot `PgFixture`, `fresh_db()`, use `DuckLakeWriter::new(socket, &db).seed("main", "t", &[("id".into(),"int64".into(),false)], &[])` to bootstrap the catalog + create table `t` with **zero** initial batches (table exists, no files). Then via loom: `begin()`, `append_files(&t, &[DataFile{ path:"ducklake-loom-0.parquet", path_is_relative:true, record_count:10, file_size_bytes:444, footer_size:249, column_stats: vec![ColumnStat{column_name:"id",min:Some("0"),max:Some("9"),null_count:0,value_count:10,column_size_bytes:88}] }])`, `commit()` → assert `Some`. Then assert via the existing `Catalog` read: `cp.files(&t, cp.current_snapshot(&t).await?.id, PageReq::unbounded())` returns 1 file with `record_count==10`.

(If `DuckLakeWriter::seed` requires ≥1 batch, add a `seed`-with-empty-batches path or a `bootstrap_table` helper that ATTACHes + `CREATE TABLE` only — small fixture addition; mirror the existing `seed` SQL minus the INSERT loop.)

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:snapshot-append` → FAIL (commit returns the "not yet implemented" Err from Task 1, or the writer is absent).

- [ ] **Step 2: Implement the append writer** — create `postgres/src/snapshot.rs` with functions that take `&mut sqlx::Transaction<'_, Postgres>` and write the recipe rows. Implement, in order (recipe §2 batch order; §3 Op B for exact tuples):
  1. `lock_catalog(tx)` — `sqlx::query!("SELECT pg_advisory_xact_lock($1)", CATALOG_LOCK_KEY)` (a `const CATALOG_LOCK_KEY: i64`). Serializes loom commits.
  2. `read_head(tx) -> Head { snapshot_id, schema_version, next_catalog_id, next_file_id }` — recipe §5 `GetLatestSnapshotQuery` (`SELECT ... FROM ducklake_snapshot WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM ducklake_snapshot)`).
  3. For each staged file: resolve `table_id` (`SELECT table_id FROM ducklake_table WHERE table_name=$1 AND end_snapshot IS NULL` joined via `ducklake_schema` on `schema_name`); allocate `data_file_id = next_file_id` (then `next_file_id += 1`); read/seed `ducklake_table_stats.next_row_id` for `row_id_start`.
  4. Write the rows of recipe §3 Op B (positional INSERTs): `ducklake_snapshot` (new id = head.snapshot_id+1, schema_version unchanged for DML, advanced `next_file_id`), `ducklake_table_stats` (INSERT if `stats.initialized` false else UPDATE — recipe §3 Op C UPDATE branch), `ducklake_table_column_stats` (insert/merge), `ducklake_data_file`, `ducklake_file_column_stats` (one per `ColumnStat`), `ducklake_snapshot_changes` (`inserted_into_table:<table_id>`).
  5. Resolve `column_id` for stats from `ducklake_column` (`SELECT column_id FROM ducklake_column WHERE table_id=$1 AND column_name=$2 AND end_snapshot IS NULL`).

All via compile-time `query!`. Return the new `SnapshotId`.

- [ ] **Step 3: Wire `PgTx::commit`** — replace the Task-1 transient: if `staged_files`/`staged_tables` non-empty, call the snapshot writer (`snapshot::commit_snapshot(&mut self.tx, &staged_tables, &staged_files)`) which writes create_table rows (Task 3) + append rows, returns the new `SnapshotId`; then `self.tx.commit()`. Return `Ok(Some(id))`. For this task implement the append path; leave create_table staged-but-applied in Task 3 (in this task `staged_tables` is always empty since the fixture creates the table).

- [ ] **Step 4: Add module + regen the sqlx cache**

```bash
# add `mod snapshot;` to postgres/src/lib.rs
tools/sqlx-prepare.sh   # regenerates .sqlx for the new query! calls
```

- [ ] **Step 5: Add the test target to `postgres/BUCK`** (mirror the `:lineage` env block — postgres-bin + libxml2 + migrations + duckdb-cli + duckdb-extensions, since the test uses DuckLakeWriter):

```python
rust_test(
    name = "snapshot-append",
    crate = "snapshot_append",
    srcs = ["tests/snapshot_append.rs"],
    crate_root = "tests/snapshot_append.rs",
    edition = "2024",
    env = {
        "POSTGRES_BIN_DIR": "$(location :postgres-bin)/bin",
        "POSTGRES_LD_LIBRARY_PATH": "$(location :postgres-bin)/lib:$(location :libxml2)",
        "LOOM_MIGRATIONS_DIR": "$(location :migrations)/migrations",
        "DUCKDB_BIN": "$(location :duckdb-cli)/duckdb",
        "DUCKDB_EXTENSION_DIR": "$(location :duckdb-extensions)",
    },
    deps = [":postgres", "//src/control-plane/core:core", "//third-party:tokio"],
)
```

(Confirm the exact `DUCKDB_BIN`/`DUCKDB_EXTENSION_DIR` location syntax against the existing `ducklake-smoke` target.)

- [ ] **Step 6: Test + lint**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:snapshot-append //src/control-plane/postgres:sqlx-cache-check` → PASS. `tools/clippy-all.sh` clean.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/postgres/src/snapshot.rs src/control-plane/postgres/src/transaction.rs src/control-plane/postgres/src/lib.rs src/control-plane/postgres/.sqlx src/control-plane/postgres/BUCK src/control-plane/postgres/tests/snapshot_append.rs
git commit -m "feat(postgres): native DuckLake append_files writer (single-catalog)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: postgres `create_table` native writer

Adds the DDL row-set (recipe §3 **Op A**), so loom can create a table and append in one transaction.

**Files:**
- Modify: `src/control-plane/postgres/src/snapshot.rs`, `postgres/.sqlx/` (regen)
- Test: `src/control-plane/postgres/tests/snapshot_append.rs` (extend) or a new `snapshot_create.rs`

- [ ] **Step 1: Failing test** — extend the postgres test: bootstrap the catalog with a **bare ATTACH** (no table) via a `DuckLakeWriter::bootstrap()` helper (ATTACH only — add it; it's the `seed` SQL minus `CREATE TABLE`/`INSERT`). Then loom: `begin()`, `create_table(&t, &[ColumnSpec{name:"id",ty:"int64",nullable:false}, ColumnSpec{name:"name",ty:"varchar",nullable:true}])`, `append_files(&t, &[file])`, `commit()`. Assert `Catalog::schema(&t, latest)` returns the two columns and `files` returns the file. Run → FAIL.

- [ ] **Step 2: Implement create_table rows** — in `snapshot.rs`, before the append rows, for each staged table not already live: allocate `table_id = next_catalog_id` (`next_catalog_id += 1`); assign `column_id` per-table **1-based, dense** (recipe §5 — NOT from next_catalog_id); bump `schema_version += 1` (DDL); write recipe §3 Op A rows — `ducklake_table`, `ducklake_column` (one per `ColumnSpec`, `column_order == column_id`, `column_type` = the `ty` string, `default_value='NULL'`/`default_value_type='literal'`/`default_value_dialect='duckdb'`), `ducklake_schema_versions` `(begin_snapshot, schema_version, table_id)`. The single `ducklake_snapshot` row for the commit carries the bumped `schema_version` and advanced counters; `ducklake_snapshot_changes` gets `created_table:"<schema>"."<table>"` (recipe §4). (A brand-new schema also needs a `ducklake_schema` row + `created_schema:` segment — out of scope here unless the test uses a non-`main` schema; keep to `main`.)

- [ ] **Step 3: Regen cache + test**

```bash
tools/sqlx-prepare.sh
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:snapshot-append //src/control-plane/postgres:sqlx-cache-check
```
Expected: PASS. `tools/clippy-all.sh` clean.

- [ ] **Step 4: Commit**

```bash
git add src/control-plane/postgres/src/snapshot.rs src/control-plane/postgres/.sqlx src/control-plane/postgres/src/fixture.rs src/control-plane/postgres/tests
git commit -m "feat(postgres): native DuckLake create_table writer (DDL rows)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: wire the atomic-unit conformance to postgres

Now both adapters implement the API, run the shared `snapshot_commit_contract` against postgres too, and add the atomic+enqueue+emit assertions.

**Files:**
- Modify: `src/control-plane/testkit/src/lib.rs` (extend `snapshot_commit_contract` to also assert emit+enqueue committed atomically)
- Create: `src/control-plane/postgres/tests/snapshot_conformance.rs`
- Modify: `postgres/BUCK`

- [ ] **Step 1: Extend the conformance fn** — in `snapshot_commit_contract`, after the create+append+commit, also stage an `emit` and `enqueue` in the same tx and assert post-commit that the lineage event is readable (`events_for`) and the job is enqueued (`dequeue`), proving all four legs commit together. Keep it adapter-agnostic (uses only trait methods).

- [ ] **Step 2: postgres conformance test** — `postgres/tests/snapshot_conformance.rs`: `PgFixture` + `fresh_db()` + `DuckLakeWriter::bootstrap()` (so `ducklake_*` exists), then `control_plane_testkit::snapshot_commit_contract(&cp).await`. Add the `rust_test` target to `postgres/BUCK` with the full DuckLake env block (as Task 2 Step 5).

- [ ] **Step 3: Test both adapters**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:snapshot //src/control-plane/postgres:snapshot-conformance`
Expected: PASS (same conformance fn, both adapters).

- [ ] **Step 4: Commit**

```bash
git add src/control-plane/testkit/src/lib.rs src/control-plane/postgres/tests/snapshot_conformance.rs src/control-plane/postgres/BUCK
git commit -m "test(control-plane): atomic snapshot+lineage+enqueue conformance (both adapters)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: DuckDB-engine interop guardrail

The make-or-break test: the pinned DuckDB reads a catalog loom wrote, and appends on top.

**Files:**
- Create: `src/control-plane/postgres/tests/ducklake_interop.rs`
- Modify: `postgres/BUCK`

- [ ] **Step 1: Write the interop test** — `PgFixture` + `fresh_db()` + `DuckLakeWriter::bootstrap()`. Via loom: `create_table(main.t (id int64, name varchar))` + `append_files` (one file loom claims to have written — for the test, actually write a tiny Parquet to the `DATA_PATH` dir, or assert metadata-only and skip data read; prefer writing a real 1-row Parquet via the duckdb-cli into the data dir so the read returns rows). `commit()`. Then drive the pinned duckdb-cli (reuse `DuckLakeWriter`'s duckdb invocation): `ATTACH` the same catalog and run `SELECT count(*) FROM lake.main.t` — assert it equals loom's `record_count`; and run an `INSERT INTO lake.main.t` through DuckDB — assert it succeeds (proving loom's counters/PK/snapshot rows are valid for DuckDB to build on). Assert the new snapshot id is loom's + 1.

(If writing a real Parquet from loom in a test is awkward, use the duckdb-cli to COPY a 1-row table to a Parquet at the path loom registers, then have loom `append_files` that path — the goal is DuckDB reading a loom-written *catalog*, not loom writing Parquet.)

- [ ] **Step 2: Add the target to `postgres/BUCK`** (full DuckLake env block) and test:

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:ducklake-interop`
Expected: PASS (DuckDB reads loom's catalog and appends).

- [ ] **Step 3: Commit**

```bash
git add src/control-plane/postgres/tests/ducklake_interop.rs src/control-plane/postgres/BUCK src/control-plane/postgres/src/fixture.rs
git commit -m "test(postgres): DuckDB-engine interop guardrail for loom-written catalog

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: rollback atomicity + final verification

**Files:**
- Modify: `src/control-plane/postgres/tests/snapshot_conformance.rs` (rollback assertion) if not already covered.

- [ ] **Step 1: Rollback atomicity test** — in the postgres conformance (or a dedicated test): stage `create_table` + `append_files` + `emit` + `enqueue`, then `rollback()`; assert zero `ducklake_*` rows added (snapshot count unchanged), no lineage event, no job. (Query `ducklake_snapshot` count before/after via a raw connection, like `ducklake_smoke`.)

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:snapshot-conformance` → PASS.

- [ ] **Step 2: Full sweep + lint**

Run:
```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...
tools/clippy-all.sh
buck2 run //tools:prek -- run --all-files
```
Expected: all PASS (incl. `sqlx-cache-check`, `reindeer-check`, rustfmt, clippy). New targets: `memory:snapshot`, `postgres:snapshot-append`, `postgres:snapshot-conformance`, `postgres:ducklake-interop`.

- [ ] **Step 3: Confirm the recipe was followed** — spot-check `ducklake_snapshot_changes` strings and counter values written by loom against recipe §3/§4/§5 (the interop test already proves DuckDB accepts them).

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "test(postgres): rollback atomicity for the snapshot-commit unit

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-review notes

- **Spec coverage:** API (Task 1) · loom-native single-catalog writer (Tasks 2–3, per recipe) · atomic unit incl. emit+enqueue (Task 4) · pessimistic advisory-lock serialization (Task 2 Step 2) · DuckDB-interop guardrail (Task 5) · rollback atomicity (Task 6) · both adapters + conformance (Tasks 1,4) · register-only boundary (DataFile metadata in; no Parquet writing in the library). Out-of-scope items (service shell, datafusion-ducklake read path, schema evolution, delete/compaction, GC) are not tasked — correct per spec.
- **Type consistency:** `ColumnSpec`/`ColumnStat`/`DataFile`, `Tx::{create_table,append_files}`, `commit -> Option<SnapshotId>` used identically across tasks. `column_id` per-table 1-based; `next_catalog_id` for table only; `next_file_id` for data files — per recipe §5, consistent in Tasks 2–3.
- **Bootstrap:** every postgres test ATTACHes a DuckLake catalog first (fixture) so `ducklake_*` exists before loom writes — loom never creates those tables (recipe §1).
- **Exact SQL:** deferred to the committed recipe doc (§2 order, §3 tuples, §4 changes_made, §5 counters) — a precise source-grounded reference, cited per task, not a placeholder.
