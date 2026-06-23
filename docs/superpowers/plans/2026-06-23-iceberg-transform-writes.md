# Transform output to Iceberg — polymorphic `Tx` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Spec:** [`docs/superpowers/specs/2026-06-22-iceberg-transform-writes-design.md`](../specs/2026-06-22-iceberg-transform-writes-design.md) (`road-iceberg-transform-writes`).

**Goal:** Make `ControlPlane::begin()` genuinely polymorphic so the transform worker can write its output to **Iceberg** instead of DuckLake, selected at boot by `LOOM_TRANSFORM_BACKEND`. Transform's `run.rs` is unchanged — it already stages `create_table → append_files | replace_files → emit → commit` through the format-neutral `Tx` trait; today the only `Tx` impl is DuckLake's `PgTx`. Add an Iceberg-backed `ControlPlane` + `Tx`. Flips no default; does not migrate ingest/actions onto the polymorphic `Tx`.

**Architecture & key decision — mirror-only registration (route b).** The transform worker has *already written* its Parquet to object storage (`write_dataset` → `Vec<DataFile>` with per-column stats). The Iceberg `Tx` therefore **registers** those files rather than re-writing batches. It does so by the **mirror-only** path already precedented by `overwrite_truncate` (`iceberg_landing.rs:246-264`) and the inline path (`iceberg_inline.rs`): in one Postgres transaction, `next_snapshot(conn, None)` → `ensure_table` → (overwrite: `end_cap_live_data_files`) → `project_columns`(first time) → `project_files`. It does **NOT** drive an Iceberg `fast_append`. Rationale (record in the module doc):
- loom-governed reads resolve **entirely through `iceberg_mirror.*`** (`IcebergCatalog`, `iceberg_catalog.rs`), never raw Iceberg metadata, so mirror rows make reads correct.
- The only thing a `fast_append` adds is raw-metadata faithfulness for **external** Iceberg engines — explicitly **deferred** by the spec (same accepted gap class as `iss-iceberg-inline-visibility`).
- That faithfulness is unattainable for these files anyway: the worker's Parquet is written by loom's **arrow-58** `write_dataset`, whose footers lack the `PARQUET:field_id` metadata external Iceberg readers need. A `fast_append` would reference a file external engines still can't field-map — a half-measure. (The full Iceberg-native path — `DataFileBuilder` with empty `Struct`/`partition_spec_id(0)` + field-id-bearing Parquet — is a future slice if/when external readers are in scope.)
- The loom `DataFile` already carries `column_stats: Vec<ColumnStat>`, so `ProjectedFile` is a direct field map — no footer re-read.

The real Iceberg table is still **created** (idempotent, via the catalog) so "ensure Iceberg table + mirror exist" holds and the table is loadable by the Iceberg writer if ever needed — matching how `append_parquet_snapshot` creates the namespace/table before its commit tx. Only *file registration* is mirror-only.

**Tech Stack:** Rust, iceberg 0.9, arrow/parquet 57 (this crate), sqlx 0.9 compile-time macros, Postgres, buck2.

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`/`#[test]` in `src/**.rs` (the `no-inline-tests` prek hook fails). Sibling `tests/<name>.rs`, own target in the crate `BUCK`.
- **Fixture-backed tests use `loom_fixture_test`** (boot Postgres/object-store; refuse root on RE).
- **Compile-time SQL.** Any new/changed `sqlx::query!` in the postgres crate ⇒ regenerate `.sqlx` via `tools/sqlx-prepare.sh` and commit. **This slice adds no new SQL** — it composes existing `iceberg_mirror`/`lineage`/`queue` query helpers (`next_snapshot`, `ensure_table`, `project_columns`, `project_files`, `end_cap_live_data_files`, `columns_exist`, `pg_emit`, `pg_insert`), all already in the cache. Run `sqlx-prepare.sh` anyway; expect zero diff.
- **No local buck2 on this host** (macOS has no cpython toolchain entry). Verification is **CI** (`affected`/`build-test` on linux). Reason carefully about types/signatures before pushing; a green per-crate build is the gate.
- **DuckLake path unchanged.** `PgControlPlane`/`PgTx`/`snapshot.rs` and all defaults stay byte-identical. The Iceberg path is a *new, separately-selected* `ControlPlane`. `run.rs` is **untouched**.
- **Mirror-faithful only** (see Architecture). No `DataFileBuilder`, no Iceberg `fast_append` of registered files, no raw-metadata writes for them.
- **Don't pipe `buck2 test` through `tail`/`head`.** Redirect to a file and grep.
- Markdown ends with exactly one trailing newline, no trailing whitespace.

---

## File Structure

- **Modify** `src/control-plane/postgres/src/iceberg_landing.rs` — add `pub enum WriteMode { Append, Overwrite }` and `pub async fn register_files(conn, table, columns, files, mode, at)`; extract `ensure_iceberg_table(catalog, table, columns)` from `append_parquet_snapshot`'s create-if-absent block and reuse it there.
- **Create** `src/control-plane/postgres/src/iceberg_control_plane.rs` — `IcebergControlPlane` (`ControlPlane` impl) + `IcebergTx` (`Tx` impl).
- **Modify** `src/control-plane/postgres/src/lib.rs` — `pub mod iceberg_control_plane;`.
- **Modify** `src/services/transform/src/main.rs` — `LOOM_TRANSFORM_BACKEND` selection; build the handler's `ControlPlane` (Pg or Iceberg); keep the Worker on the PG-backed queue.
- **Create** `src/services/transform/src/backend.rs` (or fold into `main.rs`) — `parse_transform_backend` mirroring ingest's `parse_landing_backend` (`ingest/src/landing.rs:30`).
- **Create** tests:
  - `src/control-plane/postgres/tests/iceberg_control_plane.rs` — `IcebergTx` contract (staged→effect, compact unsupported, atomicity) — `loom_fixture_test`.
  - `src/services/transform/tests/iceberg_backend_e2e.rs` — append + overwrite e2e through the Iceberg backend — `loom_fixture_test`.
- **Modify** `src/control-plane/postgres/BUCK`, `src/services/transform/BUCK` — wire targets (+ `transform/BUCK` gains a dep on the iceberg bits of `:postgres` and `iceberg`/`tempfile` for the e2e).
- **Refresh** `.sqlx` (expect zero diff).

---

## Task 1: `WriteMode` + `register_files` + `ensure_iceberg_table`

The data-plane primitive: register already-written `DataFile`s into the mirror, in a caller-supplied PG transaction, at a caller-allocated snapshot.

**Files:** modify `src/control-plane/postgres/src/iceberg_landing.rs`.

**Interfaces:**
```rust
/// Append = add `files` to the live set; Overwrite = end-cap all live files first
/// (the [`overwrite_parquet_snapshot`] contract, over already-written files).
pub enum WriteMode { Append, Overwrite }

/// Register already-written Parquet `files` into the `iceberg_mirror.*` projection
/// for `table` at snapshot `at`, in the caller's transaction. Mirror-only — see the
/// module/route-b rationale; drives NO Iceberg `fast_append`. The caller allocated
/// `at` (via `next_snapshot`) and owns commit. `columns` is the table's column set
/// (used to project mirror columns on first registration).
pub async fn register_files(
    conn: &mut sqlx::PgConnection,
    table: &TableRef,
    columns: &[ColumnSpec],
    files: &[DataFile],
    mode: WriteMode,
    at: SnapshotId,
) -> Result<()>
```

**Implementation:**
- `let tid = iceberg_mirror::ensure_table(conn, &table.schema, &table.name, at).await?;`
- `if let WriteMode::Overwrite = mode { iceberg_mirror::end_cap_live_data_files(conn, tid, at).await?; }` — end-cap BEFORE projecting new files (same ordering law as `write_mirror`).
- `if !iceberg_mirror::columns_exist(conn, tid).await? { iceberg_mirror::project_columns(conn, tid, at, &projected_columns(columns)?).await?; }`
- `iceberg_mirror::project_files(conn, tid, at, &projected_files(files)).await?;`
- Helpers:
  - `projected_columns(&[ColumnSpec]) -> Result<Vec<ProjectedColumn>>` — `order = i+1`, `name`, `nullable`, and `iceberg_type` from the loom logical type. **Reuse the existing logical→iceberg mapping**: `iceberg_type::iceberg_physical_type(&c.ty)` returns the iceberg primitive name (`"long"`, `"string"`, …) — the same string `columns_of`/the read path round-trips through `logical_from_iceberg`. Error on an unmapped type (matches `primitive_from`).
  - `projected_files(&[DataFile]) -> Vec<ProjectedFile>` — direct map: `path: f.path.clone()`, `file_format: "parquet".into()` (assert `f.file_format == FileFormat::Parquet`), `record_count`, `file_size_bytes`, `column_stats: f.column_stats.clone()`. (loom `DataFile` already carries stats — no footer read.)
- **Extract `ensure_iceberg_table`** from `append_parquet_snapshot` (lines 143-158, the namespace + table create-if-absent + `load_table`) into:
  ```rust
  pub(crate) async fn ensure_iceberg_table(
      catalog: &SqlCatalog, table: &TableRef, columns: &[ColumnSpec],
  ) -> Result<()>   // create namespace + table if absent; idempotent
  ```
  and call it from `append_parquet_snapshot` (behavior-preserving refactor) and from `IcebergTx::commit` (Task 2). Keep `append_parquet_snapshot`'s subsequent `load_table` where it is.

**Verification:** crate builds; `append_parquet_snapshot` callers (landing/flush/overwrite) unchanged behavior — existing iceberg tests green.

- [ ] `WriteMode`, `register_files`, `projected_columns`/`projected_files`, `ensure_iceberg_table` added
- [ ] `append_parquet_snapshot` refactored to call `ensure_iceberg_table`; behavior identical
- [ ] `sqlx-prepare.sh` run (zero diff expected); postgres tests green

---

## Task 2: `IcebergControlPlane` + `IcebergTx`

**Files:** create `src/control-plane/postgres/src/iceberg_control_plane.rs`; modify `lib.rs` (`pub mod`).

**`IcebergControlPlane`:**
```rust
pub struct IcebergControlPlane {
    pg: PgControlPlane,        // delegate ontology/acl/lineage/queue (backend-neutral schemas)
    ice: IcebergCatalog,       // the mirror-backed read surface
    catalog: Arc<SqlCatalog>,  // for ensure_iceberg_table at commit
    pool: PgPool,              // for begin()
}
impl IcebergControlPlane {
    pub fn new(pg: PgControlPlane, catalog: SqlCatalog) -> Self {
        let pool = pg.pool().clone();
        let ice = IcebergCatalog::new(pool.clone());
        Self { pg, ice, catalog: Arc::new(catalog), pool }
    }
}
#[async_trait] impl ControlPlane for IcebergControlPlane {
    fn catalog(&self)  -> &(dyn Catalog + Send + Sync)  { &self.ice }   // mirror reads
    fn ontology(&self) -> &(dyn Ontology + Send + Sync) { self.pg.ontology() }
    fn acl(&self)      -> &(dyn Acl + Send + Sync)      { self.pg.acl() }
    fn lineage(&self)  -> &(dyn Lineage + Send + Sync)  { self.pg.lineage() }
    fn queue(&self)    -> &(dyn Queue + Send + Sync)    { self.pg.queue() }
    async fn begin(&self) -> Result<Box<dyn Tx + Send>> {
        let tx = self.pool.begin().await.map_err(be)?;
        Ok(Box::new(IcebergTx { tx, catalog: self.catalog.clone(),
            staged_creates: vec![], staged_files: vec![] }))
    }
}
```
- **Confirm `IcebergCatalog: Catalog`.** `IcebergCatalog` implements `current_snapshot`/`files`/`schema` (used by `run_transform`'s input scan). Verify it implements the full `core::Catalog` trait (it does — `iceberg_catalog.rs` `impl Catalog for IcebergCatalog`). If any `Catalog` method is unimplemented/`todo!`, that's a gap to surface (transform only needs `current_snapshot`/`files`/`schema`).
- `be(sqlx::Error) -> ControlPlaneError` — local boxing helper (or reuse the crate's `backend`).

**`IcebergTx`** (mirror `PgTx`'s staging shape):
```rust
pub struct IcebergTx {
    tx: sqlx::Transaction<'static, Postgres>,
    catalog: Arc<SqlCatalog>,
    staged_creates: Vec<(TableRef, Vec<ColumnSpec>)>,
    staged_files: Vec<(TableRef, Vec<DataFile>, WriteMode)>,
}
```
| `Tx` method | behavior |
|---|---|
| `enqueue` | **immediate** on the held tx: `pg_insert(&mut *self.tx, &job)` → `JobId` (exactly like `PgTx`). |
| `emit` | **immediate** on the held tx: `pg_emit(&mut *self.tx, &event)` (like `PgTx`). |
| `create_table` | stage `(table, columns)`. |
| `append_files` | stage `(table, files, Append)`. |
| `replace_files` | stage `(table, files, Overwrite)`. |
| `compact_files` | **unsupported**: `Err(ControlPlaneError::Backend("IcebergTx::compact_files is unsupported (deferred, fut-iceberg-gc)".into()))`. No transform path calls it. |
| `rollback` | `self.tx.rollback()`. |
| `commit` | see below. |

**`IcebergTx::commit`:**
```rust
async fn commit(mut self: Box<Self>) -> Result<Option<SnapshotId>> {
    if self.staged_creates.is_empty() && self.staged_files.is_empty() {
        self.tx.commit().await.map_err(be)?;       // lineage/enqueue already applied
        return Ok(None);                            // matches PgTx
    }
    // 1. Ensure the real Iceberg table(s) exist (idempotent; catalog's own conn —
    //    same pre-tx create pattern as append_parquet_snapshot; a harmless empty
    //    table is the only artifact if the held tx later rolls back).
    for (table, cols) in &self.staged_creates {
        iceberg_landing::ensure_iceberg_table(&self.catalog, table, cols).await?;
    }
    // 2. One snapshot for this unit of work, allocated in the held tx.
    let at = iceberg_mirror::next_snapshot(&mut self.tx, None).await?;
    // 3. Apply staged file registrations at `at`, in the held tx.
    for (table, files, mode) in &self.staged_files {
        let cols = columns_for(&self.staged_creates, table)?;  // transform always create_table's first
        iceberg_landing::register_files(&mut self.tx, table, cols, files, *mode, at).await?;
    }
    self.tx.commit().await.map_err(be)?;
    Ok(Some(at))
}
```
- `next_snapshot`/`register_files` take `&mut PgConnection`; pass `&mut *self.tx` (Transaction derefs to PgConnection). Reborrow at each call (NLL).
- `columns_for` looks up the staged `create_table` columns for a table; if a registration has no matching create (shouldn't happen from `run.rs`), error clearly.
- **Atomicity (acceptance #4):** snapshot alloc + `project_files`/`end_cap` + `pg_emit` + `pg_insert` all ride `self.tx` → commit/rollback together. Table *existence* (step 1) is idempotent and outside the data tx, exactly as the landing path already does — document it.

- [ ] `IcebergControlPlane` (delegating accessors, `IcebergCatalog` read surface) + `begin`
- [ ] `IcebergTx` staging + immediate enqueue/emit + commit (snapshot alloc → register_files) + `compact_files` unsupported
- [ ] `pub mod iceberg_control_plane;` in `lib.rs`; crate builds

---

## Task 3: Transform boot selection

**Files:** modify `src/services/transform/src/main.rs`; add `parse_transform_backend` (new `backend.rs` or inline).

- `parse_transform_backend(Option<&str>) -> Result<TransformBackend>` mirroring `ingest/src/landing.rs:30` (`parse_landing_backend`): unset/empty → `DuckLake`; `"iceberg"` → `Iceberg`; `"ducklake"` → `DuckLake`; else error. (Pure-logic; unit-test it in a sibling `tests/` like ingest does.)
- In `main`: build the pool once. Build `pg = service_runtime::control_plane(pool.clone(), cfg.lock_timeout)`. Then:
  ```rust
  let cp_for_handler: Arc<dyn ControlPlane> = match backend {
      TransformBackend::Iceberg => {
          let catalog = build_iceberg_catalog(&cfg).await?;   // copy ingest's helper
          Arc::new(IcebergControlPlane::new(pg.clone(), catalog))
      }
      TransformBackend::DuckLake => Arc::new(pg.clone()),
  };
  let worker = Worker::new(pg, "transform-1", cfg.lock_timeout);   // queue stays PG-backed
  ```
  Keep `worker.run(&["transform","typed-transform"], shutdown, move |job| { let cp = cp_for_handler.clone(); … })` exactly as today. **No generics on `Worker`** — the queue is always PG-backed (backend-neutral); only the *handler's* `ControlPlane` varies. `run.rs` untouched (acceptance #3).
- Copy `build_iceberg_catalog(cfg)` from `ingest/src/main.rs:69-83` (namespace/warehouse from `cfg`). `LOOM_TRANSFORM_BACKEND=iceberg` reuses the existing `LOOM_DATA_PATH` warehouse + `pg_url` conventions.

- [ ] `parse_transform_backend` + unit test (mirrors ingest)
- [ ] `main.rs` selects the handler `ControlPlane`; Worker on PG queue; `run.rs` unchanged
- [ ] `transform/BUCK` deps updated (postgres iceberg surface, iceberg, tempfile for e2e)

---

## Task 4: Tests

`loom_fixture_test` (hermetic Postgres + object store).

**`postgres/tests/iceberg_control_plane.rs`** (`IcebergTx` contract):
- **append**: build `IcebergControlPlane` over the fixture pool + a temp-warehouse `SqlCatalog` (mirror `iceberg_overwrite.rs`'s `make_catalog`). `tx = cp.begin(); create_table(t, cols); append_files(t, &files); emit(lineage); let s = tx.commit()?.unwrap();` where `files` are real Parquet written to the temp store (reuse a small writer helper, or write via `write_dataset`/an existing fixture). Assert `cp.catalog().current_snapshot(t) == s`, `cp.catalog().files(t, s)` lists the registered files, lineage readable.
- **overwrite + time travel**: append `s1`, then a second `tx` with `replace_files` → `s2`; at `s2` only the new files live, at `s1` the old (mirror end-cap — exercises `road-iceberg-overwrite-mode` through the `Tx` seam).
- **compact unsupported**: `tx.compact_files(t, &[], &[])` returns `Err`.
- **atomicity**: a contrived commit failure (e.g. stage a registration for a table whose `create_table` columns are absent, or drop the tx via `rollback`) leaves no snapshot / no mirror rows for the output (`current_snapshot` unchanged / `NotFound`). Prefer the cheapest deterministic injection (mirror the `iceberg_overwrite.rs` rollback-of-mirror-mutations approach).

**`transform/tests/iceberg_backend_e2e.rs`** (the acceptance e2e — reuse transform's `overwrite_e2e.rs`/`output_mode.rs` harness shape):
- Seed an input dataset; run `run_transform(cp = IcebergControlPlane, store, run_id, req)` with `output_mode = Append`; assert the output is readable via the **Iceberg catalog/serving** with expected rows + lineage + advanced snapshot. *(acceptance #1)*
- `output_mode = Overwrite`: replaces live contents; a prior snapshot time-travels to the old. *(acceptance #2)*
- **polymorphism** *(acceptance #3)*: the SAME `run_transform` call drives this — assert it compiles/runs against `IcebergControlPlane` with `run.rs` unmodified (implicit, but add a comment).

**DuckLake unchanged** *(acceptance #5)*: the existing transform e2e suite (no `LOOM_TRANSFORM_BACKEND`) must still pass — don't touch it; the full `buck2 test //src/...` is the check.

- [ ] `iceberg_control_plane.rs` tests (append, overwrite+TT, compact-unsupported, atomicity)
- [ ] `iceberg_backend_e2e.rs` (append e2e, overwrite e2e)
- [ ] BUCK targets wired; existing DuckLake transform suite untouched

---

## Task 5: Full-suite green + register close

- Run the **full** `buck2 test //src/...` (DuckLake transform path + all defaults unchanged; `sqlx-cache-check` green).
- `buck2 run //tools:prek -- run --all-files`; commit hook fixes.
- At PR time, close `road-iceberg-transform-writes` in `docs/ROADMAP.md` via `loom-docs-update` (`- [ ]`→`- [x]`, `status:done`, `pr:#N`).

- [ ] `buck2 test //src/...` green
- [ ] prek clean
- [ ] register closed in the PR

---

## Acceptance criteria (from the spec)

1. `LOOM_TRANSFORM_BACKEND=iceberg` → a transform writes **append** output to Iceberg, readable via the Iceberg catalog/serving with lineage; `run.rs` unchanged. *(Tasks 2–4)*
2. **Overwrite** replaces live contents + preserves time travel via `Tx::replace_files` (drives `road-iceberg-overwrite-mode`). *(Tasks 1–2, 4)*
3. `cp.begin()` polymorphic — same transform code commits to DuckLake or Iceberg by injected `ControlPlane`. *(Task 3; `run.rs` untouched)*
4. `IcebergTx::commit` atomic — registration + mirror projection + lineage in one PG tx or roll back together. *(Task 2; Task 4 atomicity test)*
5. `buck2 test //src/...` green; DuckLake transform path + defaults unchanged. *(Task 5)*

## Risks / notes for the implementer

- **`IcebergControlPlane` `Clone`?** Boot uses `pg.clone()` and passes `pg` (not the iceberg cp) to `Worker::new`, so `IcebergControlPlane` need NOT be `Clone` — it's only ever `Arc`'d for the handler. Keep it non-`Clone` unless a test needs otherwise.
- **`sqlx::Transaction<'static>` in `IcebergTx`.** `PgTx` already holds `sqlx::Transaction<'static, Postgres>` from `pool.begin()`; replicate exactly (the pool outlives the tx via the `'static` bound the sqlx pool provides).
- **`columns_for` pairing.** `run.rs` always calls `create_table(output)` then `append/replace_files(output)` for the same `TableRef`, so the staged-create lookup is reliable; error loudly if a registration lacks a matching create (guards against misuse, never hit by `run.rs`).
- **No new SQL** ⇒ the `.sqlx` cache must be unchanged; if `sqlx-prepare.sh` produces a diff, something added an unexpected `query!` — investigate before committing.
