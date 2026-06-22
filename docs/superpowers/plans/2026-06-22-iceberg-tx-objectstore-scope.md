# Iceberg commit: object-store reads out of the PG transaction — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hoist the sole object-store manifest read out of the Postgres transaction in `SqlCatalog::do_update_table`, so the tx holds only fast local PG work, with atomicity unchanged.

**Architecture:** Today `do_update_table` opens the PG tx, runs the pointer CAS, then calls `project_mirror(&mut tx, …)` — whose first line is `added_files_of(staged)`, an object-store read (manifest list + manifests + Parquet footers) performed *while the tx is held open*. The fix splits the mirror projection into a **pre-tx read phase** (`added_files_of` + `columns_of` + staged snapshot id, all computed before `begin()`) and an **in-tx write phase** (`write_mirror`), where `write_mirror` takes precomputed `&[ProjectedFile]`/`&[ProjectedColumn]` and a `&mut Transaction` — and crucially **no `&Table`/`FileIO`**, so it is type-level incapable of reading object storage inside the tx. The CAS still guards the pointer; the manifests are immutable and already persisted before `do_update_table` runs, so reading them pre-tx yields identical results (no TOCTOU).

**Tech Stack:** Rust, `iceberg` crate (`Table`/`FileIO`), sqlx (Postgres tx), buck2 `loom_fixture_test` integration tests (hermetic Postgres).

## Global Constraints

- **Behavior-preserving refactor.** No change to CAS / conflict-retry semantics, mirror projection *content*, end-cap, or lineage emission — only *where* `added_files_of`/`columns_of` are called moves. Identical projected rows.
- **Tests are `rust_test`/`loom_fixture_test` integration targets only** — never inline `#[cfg(test)]` (the `no-inline-tests` prek hook enforces this).
- **The structural guarantee is by construction, not a runtime probe.** `write_mirror` must take precomputed slices and a `&mut Transaction` with **no `&Table` / `FileIO`** parameter. The reviewer verifies the signature; do not add a flaky timing assertion.
- **Core (`src/control-plane/core/`) untouched; no dependency/lockfile change.**
- Run fixture tests with the full `buck2 test //src/control-plane/postgres/...` sweep (they self-route local via `loom_fixture_test`). Don't pipe long-running `buck2 test` through `tail`/`head` — redirect to a file and grep it.

---

### Task 1: Hoist object-store reads; introduce the FileIO-free `write_mirror`

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` — replace `project_mirror` (`:359-386`) with `write_mirror` taking precomputed inputs; restructure `do_update_table` (`:394-477`) so `added_files_of` + `columns_of` + staged snapshot id run before `begin()`.
- Test (safety net, unchanged): the existing iceberg fixture suite under `src/control-plane/postgres/tests/iceberg_*.rs`.

**Interfaces:**
- Consumes (from `crate::iceberg_mirror`, all already public & unchanged):
  - `added_files_of(table: &Table) -> Result<Vec<ProjectedFile>>`
  - `columns_of(table: &Table) -> Vec<ProjectedColumn>`
  - `next_snapshot(conn: &mut PgConnection, iceberg_snapshot_id: Option<i64>) -> Result<SnapshotId>`
  - `ensure_table(conn, ns: &str, name: &str, at: SnapshotId) -> Result<i64>`
  - `columns_exist(conn, table_id: i64) -> Result<bool>`
  - `project_columns(conn, table_id: i64, at: SnapshotId, columns: &[ProjectedColumn]) -> Result<()>`
  - `project_files(conn, table_id: i64, at: SnapshotId, files: &[ProjectedFile]) -> Result<()>`
  - `ProjectedColumn` / `ProjectedFile` structs.
- Produces (replaces the private `project_mirror` method on `SqlCatalog`):
  ```rust
  async fn write_mirror(
      &self,
      tx: &mut Transaction<'_, Postgres>,
      ident: &TableIdent,
      staged_snap: Option<i64>,
      columns: &[ProjectedColumn],
      files: &[ProjectedFile],
  ) -> control_plane_core::Result<SnapshotId>
  ```
  Note: **no `&Table` / `FileIO`** — it cannot read object storage. Returns the allocated `SnapshotId`, exactly as `project_mirror` did, so the end-cap (`extras.end_cap`) still uses `at`.

- [ ] **Step 1: Run the iceberg suite first to confirm a green baseline**

This refactor is behavior-preserving; the existing suite is the test net (the spec is explicit: the structural guarantee is by construction, verified by the reviewer on the signature; the regression suite proves atomicity/rollback/content survived the re-sequencing). Capture the baseline before touching code.

Run: `buck2 test //src/control-plane/postgres/... > /tmp/ice-baseline.log 2>&1; grep -E "Tests finished|FAIL" /tmp/ice-baseline.log`
Expected: PASS — `Tests finished: … 0 failed`. (If anything is red on `main`'s state, stop and investigate before refactoring.)

- [ ] **Step 2: Replace `project_mirror` with the FileIO-free `write_mirror`**

In `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs`, replace the whole `project_mirror` method (lines ~359-386, including its doc comment's "Returns the mirror snapshot it allocated…" note which still applies) with:

```rust
    /// Write the mirror rows for an already-committed table state, in the caller's
    /// tx, from **precomputed** inputs only. Takes no `&Table` / `FileIO`, so it is
    /// type-level incapable of reading object storage inside the transaction — the
    /// property `iss-iceberg-tx-objectstore` requires (enforced by the signature,
    /// not by convention). The object-store read (`added_files_of`) and schema read
    /// (`columns_of`) are done by the caller before `begin()`.
    ///
    /// Returns the mirror snapshot it allocated so callers can use it for further
    /// in-tx work (e.g. end-capping inline rows at the same snapshot).
    async fn write_mirror(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        ident: &TableIdent,
        staged_snap: Option<i64>,
        columns: &[ProjectedColumn],
        files: &[ProjectedFile],
    ) -> control_plane_core::Result<SnapshotId> {
        use crate::iceberg_mirror::{
            columns_exist, ensure_table, next_snapshot, project_columns, project_files,
        };

        let ns = ident.namespace().join(".");
        let name = ident.name();

        let conn = &mut **tx;
        let at = next_snapshot(conn, staged_snap).await?;
        let tid = ensure_table(conn, &ns, name, at).await?;
        if !columns_exist(conn, tid).await? {
            project_columns(conn, tid, at, columns).await?;
        }
        project_files(conn, tid, at, files).await?;
        Ok(at)
    }
```

Add the `ProjectedColumn`/`ProjectedFile` types to the method's scope. They are referenced in the `write_mirror` signature, so import them at the top of the `impl SqlCatalog` block's module or inline. Use a `use crate::iceberg_mirror::{ProjectedColumn, ProjectedFile};` at the top of the file (near the other imports, after the existing `use` block at lines 18-40) if they are not already in scope. Verify with a build in Step 4 — if the signature compiles, they are in scope.

- [ ] **Step 3: Hoist the reads in `do_update_table` and call `write_mirror`**

In the same file, in `do_update_table`, after the `write_to(staged metadata)` call (line ~409) and **before** `let mut tx = self.connection.begin()` (line ~411), insert the pre-tx read phase. Then replace the `project_mirror` call (lines ~448-451) with a `write_mirror` call passing the precomputed inputs.

Insert after line ~409 (`.await?;` closing the `write_to`), before `let mut tx = …`:

```rust
        // Object-store reads happen here, BEFORE begin(): load the new snapshot's
        // manifests + Parquet footers and snapshot the staged schema. The manifests
        // are immutable and already persisted (fast_append wrote them; write_to wrote
        // the staged metadata above), so reading them pre-tx is identical to reading
        // them in-tx — no read-after-write hazard, and the CAS still guards the
        // pointer. The transaction below therefore holds only fast local PG work.
        let mirror_files = added_files_of(&staged_table).await?;
        let mirror_columns = columns_of(&staged_table);
        let staged_snap = staged_table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id());
```

Add `use crate::iceberg_mirror::{added_files_of, columns_of};` — either at the top-of-file `use` block or as a local `use` at the start of `do_update_table`. (Match the file's existing style; `project_mirror` previously imported them locally, so a local `use` at the top of `do_update_table` is idiomatic here.)

Then replace the old call:

```rust
        let at = self
            .project_mirror(&mut tx, &table_ident, &staged_table)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
```

with:

```rust
        let at = self
            .write_mirror(
                &mut tx,
                &table_ident,
                staged_snap,
                &mirror_columns,
                &mirror_files,
            )
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?;
```

Leave everything else in the tx unchanged: the CAS `UPDATE` + `rows_affected() == 0` rollback, the optional `extras.end_cap` end-cap (which still reads `at.0`), the optional `extras.lineage` `pg_emit`, and `tx.commit()`. The tx body is now pure local PG.

- [ ] **Step 4: Build + clippy the crate to confirm it compiles and is lint-clean**

Run: `buck2 build //src/control-plane/postgres:postgres > /tmp/ice-build.log 2>&1; grep -E "BUILD SUCCEEDED|BUILD FAILED|error\[|warning:" /tmp/ice-build.log; echo done`
Expected: `BUILD SUCCEEDED`, no `error[...]`. Resolve any "unused import" (e.g. if `added_files_of`/`columns_of` end up imported twice) or "cannot find type `ProjectedColumn`" by adjusting the `use` placement from Steps 2-3.

Run: `buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' > /tmp/ice-clippy.log 2>&1; cat /tmp/ice-clippy.log; echo "(empty == clean)"`
Expected: empty clippy output.

- [ ] **Step 5: Run the full iceberg fixture suite — atomicity/rollback/content unchanged**

The re-sequencing must leave every iceberg test green, especially `concurrent_appends_keep_the_mirror_consistent` (multi-writer integrity), the append/round-trip, flush time-travel + idempotency, landing, and snapshot-sequence tests.

Run: `buck2 test //src/control-plane/postgres/... > /tmp/ice-after.log 2>&1; grep -E "Tests finished|FAIL" /tmp/ice-after.log`
Expected: PASS — `Tests finished: … 0 failed`, identical pass set to the Step 1 baseline.

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs
git commit -m "perf(iceberg): hoist object-store manifest read out of the commit PG tx

Replace project_mirror with a FileIO-free write_mirror that takes precomputed
ProjectedFile/ProjectedColumn slices; do_update_table now runs added_files_of +
columns_of before begin(), so the transaction holds only local PG work. The CAS
still guards the pointer and the manifests are immutable, so atomicity is
unchanged (regression suite green).

Refs iss-iceberg-tx-objectstore"
```

---

### Task 2: Strengthen the multi-writer test under the shorter tx (optional, low-risk)

**Files:**
- Modify: `src/control-plane/postgres/tests/iceberg_write_roundtrip.rs:170` — bump `const N: i64 = 4;` to `8` in `concurrent_appends_keep_the_mirror_consistent`.

**Interfaces:** None — same test asserting the same invariants (N snapshots, zero orphans, N files), just more contention.

- [ ] **Step 1: Bump N from 4 to 8**

In `concurrent_appends_keep_the_mirror_consistent`, change:

```rust
    const N: i64 = 4;
```

to:

```rust
    const N: i64 = 8;
```

The assertions (`snap_count == N`, `orphans == 0`, `files == N`) are written in terms of `N` and need no other change — they exercise the shorter tx under more contention while asserting the identical invariants.

- [ ] **Step 2: Run the test and confirm it stays fast and non-flaky**

Run: `buck2 test //src/control-plane/postgres:iceberg-write-roundtrip > /tmp/ice-roundtrip.log 2>&1; grep -E "Tests finished|FAIL" /tmp/ice-roundtrip.log`
Expected: PASS, completing in comparable time to N=4 (a few seconds). **If it is slow or flaky, revert N to 4** (the spec marks this bump optional and conditional on staying fast/non-flaky) and skip the commit.

- [ ] **Step 3: Commit (only if N=8 stayed green and fast)**

```bash
git add src/control-plane/postgres/tests/iceberg_write_roundtrip.rs
git commit -m "test(iceberg): bump concurrent-append N to 8 to exercise the shorter commit tx

Refs iss-iceberg-tx-objectstore"
```

---

### Task 3: Close the register item

**Files:**
- Modify: `docs/ISSUES.md` — close `iss-iceberg-tx-objectstore`.

This is handled in the finishing step via the `loom-docs-update` skill, in the PR branch. The edit: flip `- [ ]`→`- [x]`, set `status:open`→`status:fixed`, add `pr:#<n>` (the `from:`/`spec:` stay). Do this after the PR number is known (or push, open the PR, then amend). The item already points at the `2026-06-22-iceberg-tx-objectstore-scope-design` spec.

- [ ] **Step 1: Run `loom-docs-update`** at finish to close the item alongside the work, per the loom-work-checkout pipeline. Verify with `bash tools/docs.sh validate` before pushing.
