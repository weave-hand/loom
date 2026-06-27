# Governed UPDATE / DELETE actions (A5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the two mutating ontology action kinds — UPDATE and DELETE — that supersede one existing typed object (located by its declared `identity` value) via whole-table copy-on-write over loom's two-tier MVCC mirror, committing one snapshot + lineage atomically with time travel preserved.

**Architecture:** A new `ActionKind` discriminator on `ActionDef` routes `POST /actions/{name}` to insert/update/delete logic in `run_action`. UPDATE/DELETE do a privileged (ACL-unfiltered) full-table read over the serving engine, splice the single-object change in memory, and re-commit the whole table through the shipped A1 `overwrite_parquet_snapshot` — extended so overwrite end-caps the inline tier too. Governance (coarse `Action::Write` + fine-grained column/row-filter) is enforced on the affected row. Vector-bearing types are rejected (the scalar COW path can't represent list columns).

**Tech Stack:** Rust, buck2, axum, sqlx (compile-time `query!`), Arrow 58, iceberg-rust (pinned `main`), DataFusion (engine side), hermetic Postgres fixtures.

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`. Each test is a sibling `tests/<name>.rs` wired as its own target in the crate `BUCK`. The `no-inline-tests` prek hook enforces this.
- **Fixture (hermetic-Postgres) tests use the `loom_fixture_test` macro**, not bare `rust_test` (they route local; `initdb`/`postgres` refuse root on RE). Pure-logic tests use `rust_test` via the `loom_rust_test` wrapper.
- **Run tests:** `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`. Never pipe `buck2 test`/`bxl` through `tail`/`head`.
- **Clippy is strict** (pedantic + restriction). Enforced lints include `unwrap_used`, `expect_used`, `indexing_slicing`, `panic`, `todo`. Production code must avoid these; use `#[expect(lint, reason = "...")]` locally when unavoidable. Test code is exempted from panic-safety lints by the wrappers.
- **sqlx compile-time macros** read the committed `.sqlx` cache (`src/control-plane/postgres/.sqlx/`). After ANY SQL change in the postgres crate, regenerate with `tools/sqlx-prepare.sh` and commit the `.sqlx` diff. The `sqlx-cache-check` fixture test enforces freshness.
- **Conventional Commits** are enforced on the commit message (commit-msg hook). Use `feat(...)`/`test(...)`/`docs(...)` etc.
- **Branch:** all work lands on `claude/grimoire-agenda-progress-1hgg6o`.
- **Spec:** `docs/superpowers/specs/2026-06-27-update-delete-actions-design.md` — every task implements part of it.

---

### Task 1: `ActionKind` discriminator on `ActionDef` (core)

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs` (add enum + field)
- Modify (sweep, compiler-flagged): every `ActionDef { … }` literal in the workspace
- Test: `src/control-plane/core/tests/action_kind.rs` (new), wired in `src/control-plane/core/BUCK`

**Interfaces:**
- Produces: `pub enum ActionKind { Insert, Update, Delete }` (derives `Clone, Copy, Debug, PartialEq, Eq, Hash, Default`; `#[default] Insert`); `ActionDef` gains `pub kind: ActionKind`.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/core/tests/action_kind.rs`:

```rust
use control_plane_core::ontology::{ActionDef, ActionKind, ActionName, TypeName};

#[test]
fn action_kind_defaults_to_insert() {
    assert_eq!(ActionKind::default(), ActionKind::Insert);
}

#[test]
fn action_def_carries_kind() {
    let a = ActionDef {
        name: ActionName("a".into()),
        target: TypeName("T".into()),
        parameters: vec![],
        kind: ActionKind::Delete,
    };
    assert_eq!(a.kind, ActionKind::Delete);
}
```

- [ ] **Step 2: Wire the test target**

In `src/control-plane/core/BUCK`, mirror an existing `rust_test` (e.g. the `page` one), adding:

```python
rust_test(
    name = "action_kind",
    srcs = ["tests/action_kind.rs"],
    deps = [":core"],
)
```

(Use whatever the crate's lib target name is — match the sibling test targets.)

- [ ] **Step 3: Run test to verify it fails**

Run: `buck2 test //src/control-plane/core:action_kind > /tmp/t.log 2>&1; grep -E "error\[|cannot find|Tests finished|FAIL" /tmp/t.log`
Expected: FAIL — `ActionKind` not found / `ActionDef` has no field `kind`.

- [ ] **Step 4: Add the type + field**

In `src/control-plane/core/src/ontology.rs`, after the `ActionName` definition add:

```rust
/// Which kind of mutation an action performs against its target type. `Insert`
/// (part-1) creates a new object; `Update`/`Delete` (A5) mutate or remove one
/// existing object located by the target type's declared `identity`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum ActionKind {
    #[default]
    Insert,
    Update,
    Delete,
}
```

Add `kind` to `ActionDef`:

```rust
pub struct ActionDef {
    pub name: ActionName,
    pub target: TypeName,
    /// Ordered.
    pub parameters: Vec<ParamDef>,
    /// The mutation kind. `Insert` (part-1 default) creates; `Update`/`Delete` mutate
    /// one existing object by `target`'s declared `identity`.
    pub kind: ActionKind,
}
```

- [ ] **Step 5: Sweep all `ActionDef { … }` literals**

Build the workspace and add `kind: ActionKind::Insert,` to every `ActionDef { … }` the compiler flags. The known production site is `src/control-plane/postgres/src/ontology.rs:358` (`get_action`) — leave its value as `ActionKind::Insert` for now (Task 2 makes it read the real column). All other sites are tests/testkit:
- `src/control-plane/testkit/src/lib.rs` (~3 sites)
- `src/services/query-api/tests/{action_conformance,action_conformance_handler,action_conformance_http,action_e2e,action_run_id_http,write_denial_http,iceberg_action_e2e}.rs`

Run: `buck2 build //src/... > /tmp/b.log 2>&1; grep -E "missing field|error\[" /tmp/b.log` and fix each flagged literal by adding `kind: control_plane_core::ontology::ActionKind::Insert,` (or the locally-imported `ActionKind::Insert`).

- [ ] **Step 6: Run tests to verify they pass + workspace builds**

Run: `buck2 build //src/... > /tmp/b.log 2>&1; grep -E "error\[|BUILD SUCCEEDED|Failed" /tmp/b.log`
Run: `buck2 test //src/control-plane/core:action_kind > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: build succeeds; tests PASS.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(ontology): add ActionKind discriminator to ActionDef"
```

---

### Task 2: Persist `kind` in the postgres ontology adapter

**Files:**
- Create: `src/control-plane/postgres/migrations/0018_action_kind.sql`
- Modify: `src/control-plane/postgres/src/ontology.rs` (`define_action` insert, `get_action` select)
- Modify: `src/control-plane/postgres/.sqlx/` (regenerated)
- Test: `src/control-plane/postgres/tests/action_kind_persist.rs` (new), wired via `loom_fixture_test`

**Interfaces:**
- Consumes: `ActionKind` from Task 1.
- Produces: `ontology.action.kind` column persisted/read; `get_action` returns the stored kind.

- [ ] **Step 1: Write the failing fixture test**

Create `src/control-plane/postgres/tests/action_kind_persist.rs` (model imports/fixture boot on an existing postgres fixture test, e.g. the ontology one):

```rust
// Boot the hermetic fixture exactly as the sibling ontology fixture tests do.
use control_plane_core::ontology::{ActionDef, ActionKind, ActionName, Ontology, ParamDef, TypeName};
// ... fixture setup helper (mirror an existing tests/*.rs in this crate) ...

#[tokio::test]
async fn action_kind_round_trips() {
    let cp = /* boot fixture, get a ControlPlane / Ontology */;
    // a target type "Widget" must exist first (define_type) — mirror sibling tests.
    cp.ontology().define_type(/* Widget with identity */).await.unwrap();
    cp.ontology().define_action(ActionDef {
        name: ActionName("delWidget".into()),
        target: TypeName("Widget".into()),
        parameters: vec![ParamDef { name: "sku".into(), ty: "String".into(), required: true }],
        kind: ActionKind::Delete,
    }).await.unwrap();

    let got = cp.ontology().get_action(&ActionName("delWidget".into())).await.unwrap();
    assert_eq!(got.kind, ActionKind::Delete);
}
```

- [ ] **Step 2: Wire the target + run to verify it fails**

Add a `loom_fixture_test` target `action_kind_persist` in `src/control-plane/postgres/BUCK` (mirror an existing fixture test target).
Run: `buck2 test //src/control-plane/postgres:action_kind_persist > /tmp/t.log 2>&1; grep -E "FAIL|assert|Tests finished" /tmp/t.log`
Expected: FAIL — `kind` reads back `Insert` (column not yet persisted), assertion fails.

- [ ] **Step 3: Add the migration**

Create `src/control-plane/postgres/migrations/0018_action_kind.sql`:

```sql
alter table ontology.action
    add column kind text not null default 'insert';
```

- [ ] **Step 4: Persist + read `kind`**

In `src/control-plane/postgres/src/ontology.rs` `define_action`, change the upsert to write `kind` (map the enum to a lowercase tag):

```rust
let kind = match action.kind {
    control_plane_core::ontology::ActionKind::Insert => "insert",
    control_plane_core::ontology::ActionKind::Update => "update",
    control_plane_core::ontology::ActionKind::Delete => "delete",
};
sqlx::query!(
    "insert into ontology.action (name, target_type, kind) values ($1, $2, $3) \
     on conflict (name) do update set target_type = excluded.target_type, kind = excluded.kind",
    action.name.0,
    action.target.0,
    kind,
)
.execute(&mut *tx)
.await
.map_err(backend)?;
```

In `get_action`, select and map `kind`:

```rust
let row = sqlx::query!(
    "select target_type, kind from ontology.action where name = $1",
    name.0,
)
// ...
let kind = match row.kind.as_str() {
    "update" => control_plane_core::ontology::ActionKind::Update,
    "delete" => control_plane_core::ontology::ActionKind::Delete,
    _ => control_plane_core::ontology::ActionKind::Insert,
};
Ok(ActionDef {
    name: name.clone(),
    target: TypeName(row.target_type),
    parameters: /* unchanged */,
    kind,
})
```

- [ ] **Step 5: Regenerate the sqlx cache**

Run: `./tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; grep -E "error|prepared|query data" /tmp/sqlx.log`
This boots the pinned postgres, applies migrations (incl. 0018), and rewrites `.sqlx/`.

- [ ] **Step 6: Run the test + sqlx freshness check**

Run: `buck2 test //src/control-plane/postgres:action_kind_persist //src/control-plane/postgres:sqlx-cache-check > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(ontology): persist action kind in the postgres adapter"
```

---

### Task 3: Overwrite supersedes the inline tier (`end_cap_live_inline_rows`)

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (new helper)
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` (`write_mirror` overwrite block)
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (`overwrite_truncate`)
- Test: `src/control-plane/postgres/tests/overwrite_end_caps_inline.rs` (new, `loom_fixture_test`)

**Interfaces:**
- Produces: `pub(crate) async fn end_cap_live_inline_rows(conn: &mut sqlx::PgConnection, table_id: i64, at: SnapshotId) -> control_plane_core::Result<()>`.

- [ ] **Step 1: Write the failing fixture test**

Create `src/control-plane/postgres/tests/overwrite_end_caps_inline.rs`. Model setup on an existing inline/overwrite fixture test in this crate (e.g. a test that calls `iceberg_landing::land` for an inline write then `overwrite_parquet_snapshot`). Assert: after an inline `land` (small batch → inline row) of 1 row, then an `overwrite_parquet_snapshot` with a different 1-row batch, a read at the new snapshot returns ONLY the new row (the stale inline row is end-capped), and a read at the pre-overwrite snapshot still returns the original inline row (time travel).

```rust
// land 1 inline row {sku:"a"} -> snap S1
// overwrite with 1 row {sku:"b"} -> snap S2
// read live (S2): exactly [{sku:"b"}]  (inline "a" must NOT survive)
// read as-of S1: exactly [{sku:"a"}]
```

(Use the crate's existing live-read helper / `IcebergCatalog` to read live rows at a snapshot; mirror a sibling test's read assertion.)

- [ ] **Step 2: Wire the target + run to verify it fails**

Add `loom_fixture_test` target `overwrite_end_caps_inline` in `src/control-plane/postgres/BUCK`.
Run: `buck2 test //src/control-plane/postgres:overwrite_end_caps_inline > /tmp/t.log 2>&1; grep -E "FAIL|assert|Tests finished" /tmp/t.log`
Expected: FAIL — live read at S2 returns BOTH "a" (stale inline) and "b" (the bug this fixes).

- [ ] **Step 3: Add `end_cap_live_inline_rows`**

In `src/control-plane/postgres/src/iceberg_inline.rs`, add (the inline table is created lazily — guard existence with `to_regclass` so a never-inlined table is a no-op):

```rust
/// End-cap EVERY live inline row of `table_id` at snapshot `at` (`end_snapshot = at`
/// where `end_snapshot is null`). Used by the overwrite/replace commit so a replace
/// supersedes the inline tier as well as the file tier. No-op if the inline table was
/// never created. Runs in the caller's transaction.
pub(crate) async fn end_cap_live_inline_rows(
    conn: &mut sqlx::PgConnection,
    table_id: i64,
    at: control_plane_core::SnapshotId,
) -> control_plane_core::Result<()> {
    let name = inline_table_name(table_id);
    // to_regclass returns NULL for a non-existent relation -> skip.
    let exists: Option<String> = sqlx::query_scalar("select to_regclass($1)::text")
        .bind(&name)
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| control_plane_core::ControlPlaneError::Backend(Box::new(e)))?;
    if exists.is_none() {
        return Ok(());
    }
    let sql = format!("update {name} set end_snapshot = {} where end_snapshot is null", at.0);
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .execute(&mut *conn)
        .await
        .map_err(|e| control_plane_core::ControlPlaneError::Backend(Box::new(e)))?;
    Ok(())
}
```

Ensure it's reachable from the catalog module (it's the same crate; `pub(crate)` is fine).

- [ ] **Step 4: Call it in the non-empty overwrite path**

In `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` `write_mirror`, extend the overwrite block (currently lines ~444-446):

```rust
if overwrite {
    end_cap_live_data_files(conn, tid, at).await?;
    crate::iceberg_inline::end_cap_live_inline_rows(conn, tid, at).await?;
}
```

- [ ] **Step 5: Call it in the truncate path**

In `src/control-plane/postgres/src/iceberg_landing.rs` `overwrite_truncate`, after `end_cap_live_data_files(conn, tid, at).await?;` add:

```rust
crate::iceberg_inline::end_cap_live_inline_rows(conn, tid, at).await?;
```

- [ ] **Step 6: Regenerate sqlx cache if needed + run**

The new queries are runtime (`sqlx::query` / `query_scalar` with `AssertSqlSafe`/bind), so no `.sqlx` change. If the build complains about offline cache, run `./tools/sqlx-prepare.sh`.
Run: `buck2 test //src/control-plane/postgres:overwrite_end_caps_inline > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (live read at S2 returns only "b"; as-of S1 returns "a").

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "fix(iceberg): overwrite end-caps the inline tier, not just data files"
```

---

### Task 4: N-row Arrow batch builder `build_object_batches`

**Files:**
- Modify: `src/services/query-api/src/serving.rs`
- Test: `src/services/query-api/tests/build_object_batches.rs` (new, `rust_test`)

**Interfaces:**
- Consumes: existing `one_cell`, `SqlValue`, `ColumnSpec`.
- Produces: `pub fn build_object_batches(columns: &[String], rows: &[Vec<SqlValue>], logical_types: &[String]) -> Result<(Arc<Schema>, RecordBatch, Vec<ColumnSpec>), ServingError>` — a single multi-row batch. `rows.is_empty()` is rejected (callers handle the empty/truncate case before calling).

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/build_object_batches.rs`:

```rust
use query_api::serving::{build_object_batches, SqlValue};

#[test]
fn builds_two_rows() {
    let cols = vec!["sku".to_string(), "qty".to_string()];
    let types = vec!["String".to_string(), "Long".to_string()];
    let rows = vec![
        vec![SqlValue::Text("a".into()), SqlValue::Int(1)],
        vec![SqlValue::Text("b".into()), SqlValue::Int(2)],
    ];
    let (_schema, batch, specs) = build_object_batches(&cols, &rows, &types).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(specs.len(), 2);
}

#[test]
fn rejects_empty_rows() {
    let err = build_object_batches(&["sku".into()], &[], &["String".into()]);
    assert!(err.is_err());
}
```

- [ ] **Step 2: Wire target + verify it fails**

Add a `rust_test` target `build_object_batches` in `src/services/query-api/BUCK` (mirror a sibling pure-logic test; ensure `serving` items are `pub` enough — `build_object_batches`, `SqlValue` are already exported via `serving`).
Run: `buck2 test //src/services/query-api:build_object_batches > /tmp/t.log 2>&1; grep -E "cannot find|error\[|FAIL|Tests finished" /tmp/t.log`
Expected: FAIL — `build_object_batches` not found.

- [ ] **Step 3: Implement `build_object_batches`**

In `src/services/query-api/src/serving.rs`, add (column-major build over rows; reuses `one_cell` per cell, then assembles each column array). Because `one_cell` builds one single-row array, build per-column arrays by concatenation OR build arrays directly. Simplest correct approach: build each column's array from all rows via a per-column loop reusing the same base resolution:

```rust
use arrow::compute::concat;

/// Build a single multi-row `RecordBatch` (+ schema + `ColumnSpec`s) from `rows`
/// (row-major, each aligned to `columns`). Generalizes [`build_object_batch`] to N rows
/// for the copy-on-write overwrite path. Every field is nullable. Rejects empty `rows`
/// (the empty/truncate case is handled by the caller before this is reached) and any
/// shape mismatch or value/type mismatch (via `one_cell`).
pub fn build_object_batches(
    columns: &[String],
    rows: &[Vec<SqlValue>],
    logical_types: &[String],
) -> Result<(Arc<Schema>, RecordBatch, Vec<ColumnSpec>), ServingError> {
    if columns.is_empty() || columns.len() != logical_types.len() || rows.is_empty() {
        return Err(ServingError::Engine(format!(
            "build_object_batches: {} columns / {} types / {} rows (need >=1 col, >=1 row, equal col/type counts)",
            columns.len(), logical_types.len(), rows.len()
        )));
    }
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    let mut specs: Vec<ColumnSpec> = Vec::with_capacity(columns.len());
    for (ci, (name, logical)) in columns.iter().zip(logical_types).enumerate() {
        let base = resolve_logical(logical)
            .ok_or_else(|| ServingError::Engine(format!("unknown logical type `{logical}`")))?;
        let mut col_dt: Option<DataType> = None;
        let mut cells: Vec<ArrayRef> = Vec::with_capacity(rows.len());
        for row in rows {
            let value = row.get(ci).ok_or_else(|| {
                ServingError::Engine(format!("row shorter than columns at index {ci}"))
            })?;
            let (dt, array) = one_cell(base, value, name)?;
            col_dt = Some(dt);
            cells.push(array);
        }
        let refs: Vec<&dyn arrow::array::Array> = cells.iter().map(|a| a.as_ref()).collect();
        let array = concat(&refs).map_err(|e| ServingError::Engine(e.to_string()))?;
        let dt = col_dt.ok_or_else(|| ServingError::Engine("no rows".into()))?;
        fields.push(Field::new(name, dt, true));
        arrays.push(array);
        specs.push(ColumnSpec { name: name.clone(), ty: base.canonical_name(), nullable: true });
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| ServingError::Engine(e.to_string()))?;
    Ok((schema, batch, specs))
}
```

Add `use arrow::compute::concat;` near the other arrow imports if not present. Confirm `arrow` exposes `compute::concat` in this tree (it does on arrow 58); if the crate feature isn't enabled, use `arrow_select::concat::concat` (already a tree dep — see `iceberg_landing.rs`).

- [ ] **Step 4: Run test to verify it passes**

Run: `buck2 test //src/services/query-api:build_object_batches > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(query): N-row build_object_batches for copy-on-write overwrite"
```

---

### Task 5: `ActionEngine::overwrite_table` + `IcebergActionWriter` impl

**Files:**
- Modify: `src/services/query-api/src/serving.rs` (trait method)
- Modify: `src/services/query-api/src/serving_datafusion.rs` (impl)
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` — ensure `overwrite_parquet_snapshot` is `pub` (it is) and reachable
- Test: `src/services/query-api/tests/overwrite_table_e2e.rs` (new, `loom_fixture_test`)

**Interfaces:**
- Consumes: `build_object_batches` (Task 4), `overwrite_parquet_snapshot` (postgres crate).
- Produces: `ActionEngine::overwrite_table(&self, table, columns: &[String], rows: &[Vec<SqlValue>], logical_types: &[String], event: LineageEvent) -> Result<SnapshotId, ServingError>`.

- [ ] **Step 1: Write the failing fixture test**

Create `src/services/query-api/tests/overwrite_table_e2e.rs` modeled on `iceberg_action_e2e.rs` setup: land a 2-row table via the action insert path (or `land`), then call `action_engine.overwrite_table(table, cols, new_rows, types, event)` with a 1-row `new_rows`, and assert a governed read returns exactly that 1 row, and the lineage event is queryable by `run_id`.

- [ ] **Step 2: Wire target + verify it fails**

Add `loom_fixture_test` target `overwrite_table_e2e` in `src/services/query-api/BUCK` (deps include `:e2e-support`).
Run: `buck2 test //src/services/query-api:overwrite_table_e2e > /tmp/t.log 2>&1; grep -E "no method|error\[|FAIL|Tests finished" /tmp/t.log`
Expected: FAIL — `overwrite_table` not found.

- [ ] **Step 3: Add the trait method**

In `src/services/query-api/src/serving.rs`, add to `trait ActionEngine`:

```rust
    /// Replace the ENTIRE live contents of `table` with `rows` (the copy-on-write
    /// overwrite path for UPDATE/DELETE), committing `event` atomically with the new
    /// snapshot. `rows.is_empty()` truncates the table (delete-all). Both MVCC tiers
    /// (files + inline) are superseded; time travel is preserved.
    async fn overwrite_table(
        &self,
        table: &control_plane_core::TableRef,
        columns: &[String],
        rows: &[Vec<SqlValue>],
        logical_types: &[String],
        event: control_plane_core::LineageEvent,
    ) -> Result<control_plane_core::SnapshotId, ServingError>;
```

- [ ] **Step 4: Implement on `IcebergActionWriter`**

In `src/services/query-api/src/serving_datafusion.rs`, add to `impl ActionEngine for IcebergActionWriter`:

```rust
    async fn overwrite_table(
        &self,
        table: &control_plane_core::TableRef,
        columns: &[String],
        rows: &[Vec<SqlValue>],
        logical_types: &[String],
        event: control_plane_core::LineageEvent,
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        if rows.is_empty() {
            // Delete-all: empty batches drive the truncate branch (mirror-only end-cap).
            return iceberg_landing::overwrite_parquet_snapshot(
                &self.pool, &self.catalog, table, &[], Vec::new(), Some(&event),
            )
            .await
            .map_err(|e| ServingError::Engine(e.to_string()));
        }
        let (_schema, batch, specs) = build_object_batches(columns, rows, logical_types)?;
        iceberg_landing::overwrite_parquet_snapshot(
            &self.pool, &self.catalog, table, &specs, vec![batch], Some(&event),
        )
        .await
        .map_err(|e| ServingError::Engine(e.to_string()))
    }
```

Confirm the `overwrite_parquet_snapshot` signature matches `(pool, catalog, table, columns: &[ColumnSpec], batches: Vec<RecordBatch>, lineage: Option<&LineageEvent>)`. Import `build_object_batches` from `crate::serving`.

- [ ] **Step 5: Update other `ActionEngine` impls (test stubs)**

Build will flag any other `impl ActionEngine` (e.g. a `StubAction`/`UnsupportedActionEngine` in tests/e2e-support). Add a minimal `overwrite_table` to each — for stubs that record calls, mirror their `write_object` stub; for an unsupported engine, return `Err(ServingError::Engine("overwrite unsupported".into()))`.

Run: `buck2 build //src/... > /tmp/b.log 2>&1; grep -E "not all trait items|error\[" /tmp/b.log` and fix each flagged impl.

- [ ] **Step 6: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:overwrite_table_e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(query): ActionEngine::overwrite_table copy-on-write seam"
```

---

### Task 6: `ActionDeps.serving`, new `ActionError` variants, HTTP status mapping

**Files:**
- Modify: `src/services/query-api/src/action.rs` (`ActionDeps`, `ActionError`)
- Modify: `src/services/query-api/src/http.rs` (`post_action` deps + status mapping)

**Interfaces:**
- Produces: `ActionDeps` gains `pub serving: &'a dyn crate::serving::ServingEngine`; `ActionError` gains `NotFound(String)` and `Unsupported(String)`.

- [ ] **Step 1: Add the `ActionError` variants + deps field**

In `src/services/query-api/src/action.rs`:

```rust
pub struct ActionDeps<'a> {
    pub cp: &'a dyn ControlPlane,
    pub action_engine: &'a dyn ActionEngine,
    pub serving: &'a dyn crate::serving::ServingEngine,
}
```

Add to `enum ActionError`:

```rust
    /// The targeted object does not exist (no live row for the supplied identity).
    #[error("object not found")]
    NotFound,
    /// The mutation is unsupported for this target type (e.g. a vector-bearing type,
    /// which the scalar copy-on-write path cannot rewrite without dropping vectors).
    #[error("unsupported: {0}")]
    Unsupported(String),
```

- [ ] **Step 2: Wire deps + status mapping in http.rs**

In `src/services/query-api/src/http.rs` `post_action`, extend `ActionDeps`:

```rust
    let deps = crate::action::ActionDeps {
        cp: st.cp.as_ref(),
        action_engine: st.action_engine.as_ref(),
        serving: st.serving.as_ref(),
    };
```

Add arms to the `match` (before the catch-all `Err(e) => internal_error(...)`):

```rust
        Err(crate::action::ActionError::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(crate::action::ActionError::Unsupported(m)) => {
            (StatusCode::UNPROCESSABLE_ENTITY, m).into_response()
        }
```

- [ ] **Step 3: Update every other `ActionDeps { … }` construction site**

Build will flag test sites that construct `ActionDeps`. Add `serving: <engine>` to each (tests already have a serving engine handle; for action-only tests that lack one, pass a stub serving engine that returns empty `Rows`).

Run: `buck2 build //src/... > /tmp/b.log 2>&1; grep -E "missing field|error\[" /tmp/b.log` and fix each.

- [ ] **Step 4: Verify build is green**

Run: `buck2 build //src/services/query-api/... > /tmp/b.log 2>&1; grep -E "error\[|SUCCEEDED|Failed" /tmp/b.log`
Expected: build succeeds (no behavior change yet — `serving`/new variants unused until Task 8).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(query): thread serving engine + NotFound/Unsupported into ActionDeps"
```

---

### Task 7: Conformance for UPDATE / DELETE

**Files:**
- Modify: `src/services/query-api/src/action.rs` (`check_conformance`)
- Test: `src/services/query-api/tests/mutate_conformance.rs` (new, `rust_test`)

**Interfaces:**
- Consumes: `ActionKind`, `ObjectType.identity`.
- Produces: `check_conformance` dispatches on `action.kind`; Update/Delete rules enforced.

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/mutate_conformance.rs`. Build small `ActionDef`/`ObjectType` fixtures and assert `check_conformance` results. (Make `check_conformance` `pub` if not already.)

```rust
use query_api::action::check_conformance;
use control_plane_core::ontology::{ActionDef, ActionKind, ActionName, ObjectType, ParamDef, PropertyDef, TypeName};
use control_plane_core::TableRef;

fn widget(identity: Option<&str>) -> ObjectType {
    ObjectType {
        name: TypeName("Widget".into()),
        properties: vec![
            PropertyDef { name: "sku".into(), ty: "String".into(), required: true },
            PropertyDef { name: "qty".into(), ty: "Long".into(), required: false },
        ],
        derived: vec![],
        table: TableRef { schema: "s".into(), name: "widget".into() },
        identity: identity.map(|s| s.to_string()),
    }
}

#[test]
fn delete_requires_identity_param_only() {
    let action = ActionDef {
        name: ActionName("del".into()), target: TypeName("Widget".into()),
        parameters: vec![ParamDef { name: "sku".into(), ty: "String".into(), required: true }],
        kind: ActionKind::Delete,
    };
    assert!(check_conformance(&action, &widget(Some("sku"))).is_ok());
}

#[test]
fn delete_rejects_extra_params() {
    let action = ActionDef {
        name: ActionName("del".into()), target: TypeName("Widget".into()),
        parameters: vec![
            ParamDef { name: "sku".into(), ty: "String".into(), required: true },
            ParamDef { name: "qty".into(), ty: "Long".into(), required: false },
        ],
        kind: ActionKind::Delete,
    };
    assert!(check_conformance(&action, &widget(Some("sku"))).is_err());
}

#[test]
fn mutate_requires_declared_identity() {
    let action = ActionDef {
        name: ActionName("del".into()), target: TypeName("Widget".into()),
        parameters: vec![ParamDef { name: "sku".into(), ty: "String".into(), required: true }],
        kind: ActionKind::Delete,
    };
    assert!(check_conformance(&action, &widget(None)).is_err());
}

#[test]
fn update_allows_partial_columns() {
    // identity "sku" + one mutable column "qty"; required prop coverage relaxed for PATCH.
    let action = ActionDef {
        name: ActionName("up".into()), target: TypeName("Widget".into()),
        parameters: vec![
            ParamDef { name: "sku".into(), ty: "String".into(), required: true },
            ParamDef { name: "qty".into(), ty: "Long".into(), required: false },
        ],
        kind: ActionKind::Update,
    };
    assert!(check_conformance(&action, &widget(Some("sku"))).is_ok());
}
```

- [ ] **Step 2: Wire target + verify it fails**

Add `rust_test` target `mutate_conformance` in `src/services/query-api/BUCK`.
Run: `buck2 test //src/services/query-api:mutate_conformance > /tmp/t.log 2>&1; grep -E "error\[|FAIL|Tests finished" /tmp/t.log`
Expected: FAIL — Update/Delete fall through current insert rules incorrectly (e.g. `delete_rejects_extra_params` fails to reject).

- [ ] **Step 3: Refactor `check_conformance` to dispatch on kind**

In `src/services/query-api/src/action.rs`, rename the existing body to `check_insert_conformance`, add a shared `check_param_property_types`, and dispatch:

```rust
pub fn check_conformance(action: &ActionDef, target: &ObjectType) -> Result<(), ActionError> {
    match action.kind {
        ActionKind::Insert => check_insert_conformance(action, target),
        ActionKind::Update => check_mutate_conformance(action, target, true),
        ActionKind::Delete => check_mutate_conformance(action, target, false),
    }
}

/// Update/Delete conformance. Both require a declared `identity` and a required
/// parameter naming it; every parameter must name a real property of compatible type.
/// Delete takes ONLY the identity parameter. Update relaxes required-property coverage
/// (PATCH) — only supplied params are validated.
fn check_mutate_conformance(action: &ActionDef, target: &ObjectType, is_update: bool) -> Result<(), ActionError> {
    let target_name = &target.name.0;
    let mut violations: Vec<String> = Vec::new();
    check_param_property_types(action, target, &mut violations); // rules 1 & 2 (shared)

    match &target.identity {
        None => violations.push(format!("type `{target_name}` has no declared identity; UPDATE/DELETE require one")),
        Some(idprop) => {
            match action.parameters.iter().find(|p| &p.name == idprop) {
                None => violations.push(format!("UPDATE/DELETE on `{target_name}` requires a parameter for the identity property `{idprop}`")),
                Some(p) if !p.required => violations.push(format!("identity parameter `{idprop}` must be required")),
                Some(_) => {}
            }
            if !is_update {
                for p in &action.parameters {
                    if &p.name != idprop {
                        violations.push(format!("DELETE on `{target_name}` takes only the identity parameter; `{}` is extra", p.name));
                    }
                }
            }
        }
    }
    if violations.is_empty() { Ok(()) } else {
        Err(ActionError::Misconfigured(format!(
            "action `{}` does not conform to type `{target_name}`: {}", action.name.0, violations.join("; ")
        )))
    }
}
```

Extract `check_param_property_types(action, target, &mut violations)` from the current rules 1 & 2 loop (move that loop verbatim into the helper, appending to `violations`). Keep `check_insert_conformance` = current rules 1 & 2 + rule 3.

- [ ] **Step 4: Run test to verify it passes**

Run: `buck2 test //src/services/query-api:mutate_conformance > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(query): conformance rules for UPDATE/DELETE actions"
```

---

### Task 8: `run_update` / `run_delete` + dispatch + vector guard + COW read

**Files:**
- Modify: `src/services/query-api/src/action.rs` (dispatch in `run_action`, new `run_insert`/`run_update`/`run_delete`, `select_all_sql`, vector guard)

**Interfaces:**
- Consumes: `ActionDeps.serving`, `ActionEngine::overwrite_table`, conformance, `WriteDenialReason`/`check_write_policy`.
- Produces: `run_action` dispatches by kind; UPDATE/DELETE implemented end-to-end. (No new test target here — proven by Task 9's e2e suite.)

- [ ] **Step 1: Refactor `run_action` to dispatch by kind**

In `src/services/query-api/src/action.rs`, after resolving `action`, `target`, the coarse Write gate, and `check_conformance` (shared), dispatch:

```rust
    match action.kind {
        ActionKind::Insert => run_insert(&action, &target, body, subject, deps).await,
        ActionKind::Update => run_mutate(&action, &target, body, subject, deps, /*is_update=*/true).await,
        ActionKind::Delete => run_mutate(&action, &target, body, subject, deps, /*is_update=*/false).await,
    }
```

Move the existing parse/expand/insert tail (steps 4-8) into `run_insert(action, target, body, subject, deps) -> Result<(ObjectRows, RunId), ActionError>`.

- [ ] **Step 2: Add the vector guard + identity helpers**

```rust
/// Reject UPDATE/DELETE on a type with any vector property: the scalar copy-on-write
/// read/write path cannot represent list columns, so a whole-table rewrite would drop
/// other rows' vectors (data loss). Lifted when an Arrow-native COW read leg lands.
fn ensure_cow_supported(target: &ObjectType) -> Result<(), ActionError> {
    for p in &target.properties {
        if let Some(control_plane_core::BaseType::Vector(_)) = control_plane_core::resolve_logical(&p.ty) {
            return Err(ActionError::Unsupported(format!(
                "UPDATE/DELETE not supported on type `{}`: it has a vector column (`{}`)",
                target.name.0, p.name
            )));
        }
    }
    Ok(())
}

/// `SELECT "c1","c2",... FROM "schema"."table"` over all properties (identifiers
/// double-quoted, embedded quotes doubled). The privileged, ACL-unfiltered full-table
/// read for copy-on-write. Column order = property order.
fn select_all_sql(target: &ObjectType) -> String {
    let q = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let cols = target.properties.iter().map(|p| q(&p.name)).collect::<Vec<_>>().join(", ");
    format!("SELECT {cols} FROM {}.{}", q(&target.table.schema), q(&target.table.name))
}
```

- [ ] **Step 3: Implement `run_mutate`**

```rust
async fn run_mutate(
    action: &ActionDef,
    target: &ObjectType,
    body: &serde_json::Map<String, Value>,
    subject: &SubjectId,
    deps: &ActionDeps<'_>,
    is_update: bool,
) -> Result<(ObjectRows, RunId), ActionError> {
    ensure_cow_supported(target)?;
    let policy_target = PolicyTarget::Type(action.target.clone());

    // Identity property + the supplied identity value (parsed via the action params).
    let idprop = target.identity.clone().ok_or_else(|| {
        ActionError::Misconfigured(format!("type `{}` has no declared identity", target.name.0))
    })?;
    let pairs = parse_params(&action.parameters, body)?; // (column, SqlValue), validated/typed
    let id_value = pairs.iter().find(|(c, _)| c == &idprop).map(|(_, v)| v.clone())
        .ok_or_else(|| ActionError::Misconfigured(format!("missing identity parameter `{idprop}`")))?;

    // Privileged full-table read (ACL-unfiltered): COW must see every live row.
    let live = deps.serving.fetch_rows(&select_all_sql(target), &[]).await?;
    let columns: Vec<String> = target.properties.iter().map(|p| p.name.clone()).collect();
    let logical: Vec<String> = target.properties.iter().map(|p| p.ty.clone()).collect();
    let id_idx = columns.iter().position(|c| c == &idprop)
        .ok_or_else(|| ActionError::Misconfigured(format!("identity `{idprop}` not a property")))?;

    // Locate the target row (identity is a primary key: at most one live match).
    let mut matches: Vec<usize> = live.rows.iter().enumerate()
        .filter(|(_, r)| r.get(id_idx) == Some(&id_value))
        .map(|(i, _)| i).collect();
    if matches.is_empty() { return Err(ActionError::NotFound); }
    if matches.len() > 1 {
        return Err(ActionError::ControlPlane(ControlPlaneError::Backend(
            format!("identity `{idprop}` has {} live rows (expected <=1)", matches.len()).into())));
    }
    let target_idx = matches.remove(0);
    let existing = live.rows[target_idx].clone(); // aligned to `columns`

    // Compute the resulting row (Update = existing with named cols overwritten).
    let new_row: Option<Vec<SqlValue>> = if is_update {
        let mut row = existing.clone();
        for (col, val) in &pairs {
            if col == &idprop { continue; } // identity immutable
            if let Some(ci) = columns.iter().position(|c| c == col) {
                if let Some(slot) = row.get_mut(ci) { *slot = val.clone(); }
            }
        }
        Some(row)
    } else { None };

    // Fine-grained write policy on the affected row(s).
    let write_policies = deps.cp.acl()
        .policies_for(subject, Action::Write, &policy_target, PageReq::unbounded()).await?;
    // DELETE: existing row must pass the row-filter. UPDATE: existing AND resulting.
    enforce_row(&write_policies.items, &columns, &existing, action, "existing")?;
    if let Some(row) = &new_row {
        // column-denial on the SET columns (non-identity, non-null), like insert:
        let (set_cols, set_vals): (Vec<String>, Vec<SqlValue>) = pairs.iter()
            .filter(|(c, v)| c != &idprop && !matches!(v, SqlValue::Null))
            .map(|(c, v)| (c.clone(), v.clone())).unzip();
        let verdict = write_filter::check_write_policy(&write_policies.items, &set_cols, &set_vals);
        if let Some(reason) = WriteDenialReason::from_verdict(verdict) {
            tracing::info!(action = %action.name.0, "update write denied (set columns)");
            return Err(ActionError::WriteDenied(reason));
        }
        enforce_row(&write_policies.items, &columns, row, action, "resulting")?;
    }

    // Build the new full live set (existing minus the row, plus the new version for update).
    let mut rows: Vec<Vec<SqlValue>> = live.rows.clone();
    if is_update {
        if let (Some(row), Some(slot)) = (new_row.clone(), rows.get_mut(target_idx)) { *slot = row; }
    } else {
        rows.remove(target_idx);
    }

    // Lineage + atomic overwrite commit.
    let run_id = RunId(Uuid::new_v4());
    let op = if is_update { "update" } else { "delete" };
    let event = LineageEvent {
        run_id, event_type: EventType::Complete, event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![DatasetRef::from(&action.target)],
        outputs: vec![DatasetRef::from(&action.target)],
        payload: serde_json::json!({ "action": action.name.0, "op": op, "identity": format!("{id_value:?}") }),
    };
    deps.action_engine.overwrite_table(&target.table, &columns, &rows, &logical, event).await?;

    // Return the affected object (Update: new version; Delete: removed values).
    let returned = new_row.unwrap_or(existing);
    Ok((ObjectRows { columns, logical_types: logical, rows: vec![returned] }, run_id))
}

/// Apply the write policy's row-filter to one row; deny (RowFilter) if it fails.
fn enforce_row(
    policies: &[control_plane_core::Policy],
    columns: &[String],
    row: &[SqlValue],
    action: &ActionDef,
    which: &str,
) -> Result<(), ActionError> {
    let cols: Vec<String> = columns.to_vec();
    let vals: Vec<SqlValue> = row.to_vec();
    let verdict = write_filter::check_write_policy(policies, &cols, &vals);
    // Only the row-filter verdict matters here (column-denial handled separately for update).
    if matches!(verdict, WriteVerdict::DenyRow) {
        tracing::info!(action = %action.name.0, which, "mutate denied: row fails write policy filter");
        return Err(ActionError::WriteDenied(WriteDenialReason::RowFilter));
    }
    Ok(())
}
```

NOTE: confirm the exact `policies_for` return type and `Policy` path against `handler.rs`/`action.rs` usages (the insert path already calls `acl().policies_for(...)` and `write_filter::check_write_policy(&write_policies.items, ...)`); reuse those exact types. Adjust `check_write_policy`'s handling so passing the full row only triggers `DenyRow` on a failing row-filter (column-denial over full columns may misfire — if so, evaluate only the row-filter leg here by calling the row-filter evaluator directly rather than `check_write_policy`). Verify against `write_filter.rs` before finalizing.

- [ ] **Step 4: Build + clippy**

Run: `buck2 build //src/services/query-api/... > /tmp/b.log 2>&1; grep -E "error\[|warning:|SUCCEEDED" /tmp/b.log`
Run: `bash tools/clippy-all.sh > /tmp/c.log 2>&1; grep -E "warning|error" /tmp/c.log` (expect clean for the touched crate).
Fix any `indexing_slicing`/`unwrap_used` by using `.get()`/`?` as above.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(query): run_update/run_delete via whole-table copy-on-write"
```

---

### Task 9: End-to-end UPDATE / DELETE tests

**Files:**
- Create: `src/services/query-api/tests/update_delete_e2e.rs` (`loom_fixture_test`, deps `:e2e-support`)
- Modify: `src/services/query-api/BUCK` (new target)

**Interfaces:**
- Consumes: the full stack (HTTP router or `run_action` directly), `e2e-support` seed/ACL helpers.

- [ ] **Step 1: Write the e2e tests**

Model on `iceberg_action_e2e.rs` + `e2e_support`. Seed a `Widget` type with `identity = "sku"` (`sku: String`, `qty: Long`), define `updateWidget` (`ActionKind::Update`, params sku+qty) and `deleteWidget` (`ActionKind::Delete`, param sku). Grant the subject Write. Cover:

```text
1. update_merges_named_columns: insert {sku:"a",qty:1}; update {sku:"a",qty:9}; read -> qty==9, sku=="a".
2. update_keeps_unnamed_columns: type with a 3rd col "name"; update only qty; "name" retained.
3. delete_removes_row: insert {sku:"a"}; delete sku="a"; read -> absent.
4. time_travel_preserved: capture snapshot before mutation; read as-of -> original; read live -> mutated/removed.
5. not_found_404: update/delete sku="missing" -> ActionError::NotFound (HTTP 404).
6. inline_then_file: prove a mutation works both when the row is inline AND after a flush to Parquet
   (force a flush via the flush threshold / land a large batch), each commits correctly.
```

Where the test drives HTTP, assert the status codes (404, 201/200); where it drives `run_action`, assert the `ActionError` variant.

- [ ] **Step 2: Governance e2e tests**

Add to the same file (or a sibling `update_delete_acl_e2e.rs`), reusing `grant_read`/`subject_with_role`/policy helpers:

```text
7. update_column_denied: a Write policy denying column "qty"; update qty -> 403 write_denied {reason:"column",column:"qty"}.
8. update_row_filter_denied_new: row-filter admits qty<5; update qty:1->9 -> 403 (resulting row outside set).
9. update_row_filter_denied_existing: row-filter admits qty<5; existing qty:9; update -> 403 (existing outside set).
10. delete_row_filter_denied: row-filter admits qty<5; delete a row with qty:9 -> 403.
11. vector_guard: a type with a vector(4) property; update/delete -> 422 Unsupported; table unchanged.
```

- [ ] **Step 3: Wire targets + run**

Add `loom_fixture_test` target(s) in `src/services/query-api/BUCK`.
Run: `buck2 test //src/services/query-api:update_delete_e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (all cases).

- [ ] **Step 4: Full suite green**

Run: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: no FAIL.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test(query): end-to-end UPDATE/DELETE action coverage"
```

---

### Task 10: Registers + docs (loom-docs-update)

**Files:**
- Modify: `docs/ROADMAP.md`, `docs/FUTURE.md`, and `CLAUDE.md` "Project status" prose.

- [ ] **Step 1: Promote the FUTURE item + add ROADMAP entry**

In `docs/FUTURE.md`, set `fut-update-delete-actions` to `status:promoted` and point it at the new roadmap id. In `docs/ROADMAP.md`, add a `done`/`planned` entry:

```text
- [x] **Governed UPDATE/DELETE actions (A5)** `{#road-update-delete-actions area:ontology status:done from:grimoire-agenda pr:- spec:2026-06-27-update-delete-actions-design}`
```

with prose summarizing: `ActionKind` discriminator; whole-table copy-on-write via `overwrite_parquet_snapshot` (extended to end-cap the inline tier); identity-targeted PATCH update / delete; coarse + fine-grained governance on the affected row; vector-bearing types rejected. Link `[[fut-update-delete-actions]]`.

- [ ] **Step 2: Add the deferred follow-ons to FUTURE.md**

Add items (one list entry each, grammar per `docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`): A6 per-session check-in; file-granular COW; COW concurrency CAS guard; Arrow-native COW read leg (lifts the vector guard); identity-change / upsert.

- [ ] **Step 3: Update CLAUDE.md "Project status"**

Add UPDATE/DELETE actions to the shipped-capabilities sentence for query-api (alongside typed insert).

- [ ] **Step 4: Validate registers + run lint hooks**

Run: `bash tools/docs.sh validate > /tmp/d.log 2>&1; cat /tmp/d.log`
Run: `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -E "Failed|Passed|error" /tmp/p.log` and commit any hook fixes.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs(ontology): record A5 UPDATE/DELETE actions in the registers"
```

---

## Self-Review

**Spec coverage:**
- §1 Action kind on `ActionDef` → Task 1.
- §2 Conformance (Update/Delete) → Task 7.
- §3 Mutation flow (privileged read, locate, apply, govern, commit, return) → Task 8.
- §3a Vector guard → Task 8 (`ensure_cow_supported`) + Task 9 case 11.
- §4 Overwrite end-caps inline tier → Task 3.
- §5 Lineage → Task 8 (`LineageEvent`).
- §6 `ActionEngine::overwrite_table` → Task 5; N-row batch → Task 4; kind persistence → Task 2.
- Error handling (404/422/403/500) → Task 6 (mapping) + Task 8 (production) + Task 9 (proof).
- Concurrency limitation → documented; CAS guard deferred (Task 10).
- Testing → Tasks 3,5,7,9.
- Register impact → Task 10.

**Placeholder scan:** Two steps (Task 8 Step 3 NOTE; Task 4 Step 3 concat-crate note) ask the implementer to verify an exact local type/path before finalizing — these are verification instructions against named files (`write_filter.rs`, `handler.rs`), not vague TODOs. All code steps carry concrete code.

**Type consistency:** `ActionKind` (Insert/Update/Delete), `ActionDef.kind`, `build_object_batches(columns, rows, logical_types)`, `ActionEngine::overwrite_table(table, columns, rows, logical_types, event)`, `ActionError::{NotFound, Unsupported}`, `end_cap_live_inline_rows(conn, table_id, at)`, `overwrite_parquet_snapshot(pool, catalog, table, columns, batches, lineage)` are used consistently across tasks.

**Risk note for the implementer:** the single highest-risk integration point is Task 8 Step 3's interaction with `write_filter::check_write_policy` (does it conflate column-denial and row-filter when handed a full row?). Read `src/services/query-api/src/write_filter.rs` first and, if `check_write_policy` can't isolate the row-filter leg, call the row-filter evaluator directly for `enforce_row`.
