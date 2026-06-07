# Catalog MVCC Delete Contract (Step 2a #3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Exercise the catalog's untested MVCC predicate branches (`end > s` with a non-null `end`, and `begin <= s` false) in both adapters, by adding a `drop_table` op to the `CatalogSeed` seam and a `catalog_delete_contract`.

**Architecture:** `CatalogSeed` (in `testkit`) gains `drop_table`. The memory seeder sets `end` on the table + its files/columns; the pg seeder issues a real `DROP TABLE` through the DuckDB CLI and reads back the drop snapshot. A new `catalog_delete_contract` asserts the time-travel boundaries; both adapters' existing `catalog.rs` gain a second test fn that runs it.

**Tech Stack:** Rust (edition 2024), `async-trait`, `sqlx`, the DuckDB CLI (test fixture only), buck2.

**Spec:** `docs/superpowers/specs/2026-06-07-catalog-delete-contract-design.md` — implements it exactly. Schema-evolution and file-supersession are deferred (recorded in `docs/FUTURE.md`).

---

## File Structure

- **Modify** `src/control-plane/testkit/src/lib.rs` — add `drop_table` to `CatalogSeed`; add `catalog_delete_contract`.
- **Modify** `src/control-plane/memory/src/lib.rs` — add `MemoryControlPlane::drop_table_catalog`.
- **Modify** `src/control-plane/memory/tests/catalog.rs` — impl `CatalogSeed::drop_table` for `MemSeeder`; add a test fn calling `catalog_delete_contract`.
- **Modify** `src/control-plane/postgres/src/fixture.rs` — add `DuckLakeWriter::drop_table`.
- **Modify** `src/control-plane/postgres/tests/catalog.rs` — impl `CatalogSeed::drop_table` for `PgSeeder`; add a test fn calling `catalog_delete_contract`.

No new deps, no new BUCK targets (the new tests live in the existing `catalog.rs` files).

**Sequencing note:** Task 1 adds `drop_table` to the `CatalogSeed` trait, which makes the existing `MemSeeder`/`PgSeeder` impls (in the adapter `catalog.rs` test files) incomplete — so `memory:catalog` and `postgres:catalog` won't build until Tasks 2 and 3. testkit itself still builds. Either `--no-verify` the Task 1 commit (the tree-wide clippy hook will fail on the stale impls — same pattern as the lineage cycle) and let Tasks 2–3 + the final `prek --all-files` validate, **or** do all three tasks before committing. Either is fine.

**Formatting:** prek `rustfmt` is check-only — `eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 <files>` before committing. pg tests run `--local-only`.

The branch is created by the executor before Task 1 — do **not** implement on `main`.

---

### Task 1: `drop_table` seam + `catalog_delete_contract` (testkit)

**Files:**
- Modify: `src/control-plane/testkit/src/lib.rs`

- [ ] **Step 1: Add `drop_table` to the `CatalogSeed` trait.** Find the trait and add the method after `seed`:
```rust
#[async_trait]
pub trait CatalogSeed {
    /// Create the table if absent and apply each row-batch as its own snapshot.
    /// Returns the per-batch snapshots, in order.
    async fn seed(&self, spec: SeedSpec) -> Vec<SeededSnapshot>;
    /// Drop `table`. Returns the snapshot `D` at which it was dropped: the table and
    /// its files/columns gain `D` as their `end_snapshot`, so the table is not live at
    /// `D` or later, but remains live (time-travellable) at any snapshot `< D`.
    async fn drop_table(&self, table: &TableRef) -> SnapshotId;
}
```
(`TableRef` and `SnapshotId` are already imported in `testkit/src/lib.rs` — `catalog_contract` uses them.)

- [ ] **Step 2: Append `catalog_delete_contract`** (place it right after `catalog_contract`):
```rust
/// Contract for the MVCC `end`-bound and before-existence branches of the catalog
/// read surface — the half the append-only `catalog_contract` never reaches. A table
/// dropped at snapshot `D` is not live at `D` or later (`end > s` false) but remains
/// time-travellable at any snapshot `< D` (`end > s` true with a non-null `end`); and a
/// table is not live before its `begin` (`begin <= s` false). `current_snapshot`/
/// `snapshots` stay drop-aware (latest *live* snapshot), distinct from a never-existed
/// table's `NotFound`.
pub async fn catalog_delete_contract<C, S>(catalog: &C, seeder: &S)
where
    C: Catalog,
    S: CatalogSeed,
{
    use control_plane_core::ControlPlaneError::NotFound;

    // Pre-seed an unrelated table FIRST so the global snapshot counter advances; its
    // snapshot predates the target table's existence.
    let other = TableRef {
        schema: "main".into(),
        name: "other".into(),
    };
    let pre = seeder
        .seed(SeedSpec {
            table: other.clone(),
            columns: vec![SeedColumn {
                name: "id".into(),
                ty: "BIGINT".into(),
                nullable: false,
            }],
            row_batches: vec![1],
        })
        .await;
    let before = pre[0].snapshot;

    // Seed the target table with two batches.
    let t = TableRef {
        schema: "main".into(),
        name: "events".into(),
    };
    let seeded = seeder
        .seed(SeedSpec {
            table: t.clone(),
            columns: vec![
                SeedColumn {
                    name: "id".into(),
                    ty: "BIGINT".into(),
                    nullable: false,
                },
                SeedColumn {
                    name: "name".into(),
                    ty: "VARCHAR".into(),
                    nullable: true,
                },
            ],
            row_batches: vec![10, 20],
        })
        .await;
    assert_eq!(seeded.len(), 2);
    let s1 = seeded[1].snapshot;

    // Live before the drop.
    assert_eq!(catalog.current_snapshot(&t).await.unwrap().id, s1);
    assert_eq!(catalog.files(&t, s1).await.unwrap().len(), 2);

    // before-existence: not live at a snapshot before its begin (`begin <= s` false).
    assert!(
        matches!(catalog.files(&t, before).await, Err(NotFound(_))),
        "not live before it existed (files)"
    );
    assert!(
        matches!(catalog.schema(&t, before).await, Err(NotFound(_))),
        "not live before it existed (schema)"
    );

    // Drop it.
    let d = seeder.drop_table(&t).await;
    assert!(d > s1, "drop creates a later snapshot");

    // `end > s` false: not live AT the drop snapshot.
    assert!(
        matches!(catalog.files(&t, d).await, Err(NotFound(_))),
        "not live at the drop snapshot (files)"
    );
    assert!(
        matches!(catalog.schema(&t, d).await, Err(NotFound(_))),
        "not live at the drop snapshot (schema)"
    );

    // `end > s` true (non-null end): time-travel into the live past still works.
    assert_eq!(
        catalog.files(&t, s1).await.unwrap().len(),
        2,
        "time-travel before the drop still sees files"
    );
    assert_eq!(
        catalog.schema(&t, s1).await.unwrap().columns.len(),
        2,
        "time-travel before the drop still sees the schema"
    );

    // History excludes the drop snapshot; current is still the last LIVE snapshot.
    let hist = catalog.snapshots(&t).await.unwrap();
    assert!(
        hist.iter().all(|sn| sn.id < d),
        "history excludes the drop snapshot"
    );
    assert_eq!(
        catalog.current_snapshot(&t).await.unwrap().id,
        s1,
        "current is the last live snapshot (drop-aware)"
    );

    // A never-existed table is still NotFound (distinct from dropped).
    let nope = TableRef {
        schema: "main".into(),
        name: "nope".into(),
    };
    assert!(
        matches!(catalog.current_snapshot(&nope).await, Err(NotFound(_))),
        "never-existed table is NotFound"
    );
}
```

- [ ] **Step 3: Build testkit.**
```bash
env -u BUCK_PREFER_REMOTE buck2 build //src/control-plane/testkit:testkit
```
Expected: builds clean. (`memory`/`postgres` won't build until Tasks 2–3 update their `CatalogSeed` impls — expected.)

- [ ] **Step 4: Format + commit** (see the sequencing note re: `--no-verify`).
```bash
eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 src/control-plane/testkit/src/lib.rs
git add src/control-plane/testkit/src/lib.rs
git commit -m "feat(control-plane): add CatalogSeed::drop_table + catalog_delete_contract"
```

---

### Task 2: memory `drop_table_catalog` + memory delete test

**Files:**
- Modify: `src/control-plane/memory/src/lib.rs`
- Modify: `src/control-plane/memory/tests/catalog.rs`

- [ ] **Step 1: Add `drop_table_catalog` to `MemoryControlPlane`** (next to `seed_catalog`):
```rust
    /// Test-support: drop `table` at a fresh snapshot, setting `end` on the table and
    /// its still-open files/columns so the MVCC `end`-bound is exercised at the
    /// file/column level (not just short-circuited by the table-liveness gate).
    pub fn drop_table_catalog(&self, table: &TableRef) -> SnapshotId {
        let mut cat = self.catalog.lock().unwrap();
        let key = (table.schema.clone(), table.name.clone());
        let d = cat.new_snapshot();
        if let Some(t) = cat.tables.get_mut(&key) {
            t.end = Some(d);
        }
        for f in cat.files.get_mut(&key).into_iter().flatten() {
            if f.end.is_none() {
                f.end = Some(d);
            }
        }
        for c in cat.columns.get_mut(&key).into_iter().flatten() {
            if c.end.is_none() {
                c.end = Some(d);
            }
        }
        SnapshotId(d)
    }
```
(`SnapshotId` is already imported in `memory/src/lib.rs`. `CatalogState::new_snapshot`, `tables`, `files`, `columns`, and `Versioned::{begin,end}` are all in this file. Borrow order: `new_snapshot()` returns the id (Copy) and its `&mut` borrow ends before the `get_mut` calls.)

- [ ] **Step 2: Update `memory/tests/catalog.rs`** — add the `drop_table` impl to `MemSeeder` and a second test fn:
```rust
    async fn drop_table(&self, table: &control_plane_core::TableRef) -> control_plane_core::SnapshotId {
        self.0.drop_table_catalog(table)
    }
```
(Add inside `impl CatalogSeed for MemSeeder<'_>`, after `seed`.)

Append a second test:
```rust
#[tokio::test]
async fn memory_passes_catalog_delete_contract() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::catalog_delete_contract(&cp, &MemSeeder(&cp)).await;
}
```
(`catalog_delete_contract` needs adding to the `use control_plane_testkit::{…}` import line.)

- [ ] **Step 3: Format, run, clippy.**
```bash
eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 src/control-plane/memory/src/lib.rs src/control-plane/memory/tests/catalog.rs
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:catalog
env -u BUCK_PREFER_REMOTE buck2 build '//src/control-plane/memory:memory[clippy.txt]'
```
Expected: `catalog` target = 2 tests passed (append + delete); clippy `[clippy.txt]` empty.

- [ ] **Step 4: Commit.**
```bash
git add src/control-plane/memory
git commit -m "feat(control-plane): memory drop_table_catalog + catalog delete contract"
```

---

### Task 3: pg `DuckLakeWriter::drop_table` + pg delete test

**Files:**
- Modify: `src/control-plane/postgres/src/fixture.rs`
- Modify: `src/control-plane/postgres/tests/catalog.rs`

- [ ] **Step 1: Add `drop_table` to `DuckLakeWriter`** (in `fixture.rs`, next to `seed`). It mirrors `seed`'s ATTACH preamble, drops, and reads back the end-snapshot:
```rust
    /// Drop `schema.table` via the DuckDB CLI (DuckLake records the drop, setting
    /// `end_snapshot` on the table and its files/columns). Returns that drop snapshot.
    pub async fn drop_table(&self, schema: &str, table: &str) -> i64 {
        let mut sql = String::new();
        sql.push_str(&format!(
            "SET extension_directory='{}';\n",
            self.extension_dir
        ));
        sql.push_str("LOAD ducklake;\nLOAD postgres_scanner;\n");
        sql.push_str(&format!(
            "ATTACH 'ducklake:postgres:dbname={} host={} user=postgres' AS lake (DATA_PATH '{}/', DATA_INLINING_ROW_LIMIT 0);\n",
            self.db,
            self.socket.display(),
            self._data_dir.path().display(),
        ));
        sql.push_str(&format!("DROP TABLE lake.{schema}.{table};\n"));

        let status = Command::new(&self.duckdb_bin)
            .arg("-c")
            .arg(&sql)
            .status()
            .expect("run duckdb");
        assert!(status.success(), "duckdb drop failed");

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(self.opts())
            .await
            .expect("connect to read back drop snapshot");
        sqlx::query_scalar::<_, i64>(
            "select t.end_snapshot from ducklake_table t \
             join ducklake_schema s on t.schema_id = s.schema_id \
             where s.schema_name = $1 and t.table_name = $2 and t.end_snapshot is not null \
             order by t.end_snapshot desc limit 1",
        )
        .bind(schema)
        .bind(table)
        .fetch_one(&pool)
        .await
        .expect("read back drop snapshot")
    }
```
(Uses the same `Command`, `PgPoolOptions`, `self.opts()`, `self.duckdb_bin`, `self.extension_dir`, `self.socket`, `self.db`, `self._data_dir` as `seed`. All already imported/defined.)

- [ ] **Step 2: Update `postgres/tests/catalog.rs`** — add the `drop_table` impl to `PgSeeder` and a second test fn:
```rust
    async fn drop_table(&self, table: &control_plane_core::TableRef) -> SnapshotId {
        SnapshotId(self.writer.drop_table(&table.schema, &table.name).await)
    }
```
(Add inside `impl CatalogSeed for PgSeeder`, after `seed`. `SnapshotId` is already imported.)

Append a second test (a fresh db + seeder, like the existing one):
```rust
#[tokio::test]
async fn postgres_passes_catalog_delete_contract() {
    let fixture = PgFixture::start();
    let (cp, db) = fixture.fresh_db().await;
    let seeder = PgSeeder {
        writer: DuckLakeWriter::new(fixture.socket_path(), &db),
    };
    catalog_delete_contract(&cp, &seeder).await;
}
```
(Add `catalog_delete_contract` to the `use control_plane_testkit::{…}` import line.)

- [ ] **Step 3: Format, run, clippy.**
```bash
eval "$(./tools/env.sh)" >/dev/null 2>&1 && rustfmt --edition 2024 src/control-plane/postgres/src/fixture.rs src/control-plane/postgres/tests/catalog.rs
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:catalog
env -u BUCK_PREFER_REMOTE buck2 build '//src/control-plane/postgres:postgres[clippy.txt]'
```
Expected: `catalog` target = 2 tests passed; clippy empty. The delete test drives a real `DROP TABLE` and reads back `D`; the contract's `assert!(d > s1)` catches a wrong read-back.

If the read-back returns the wrong snapshot (e.g. DuckLake records the drop differently than expected), debug the read-back query against the real catalog — do NOT weaken the contract. Likely-correct alternatives if `t.end_snapshot` is not the drop id: read `max(snapshot_id)` from `ducklake_snapshot` after the drop. Confirm whichever you use makes `d > s1` hold and the time-travel assertions pass.

- [ ] **Step 4: Commit.**
```bash
git add src/control-plane/postgres
git commit -m "feat(control-plane): pg DuckLakeWriter::drop_table + catalog delete contract"
```

---

## Final Verification

- [ ] **Branch + commits** (per `verify-branch-after-subagents`):
```bash
git branch --show-current   # feature branch, NOT main
git log --oneline -6
```

- [ ] **Full suite:**
```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...
```
Expected: all pass; the `memory:catalog` and `postgres:catalog` targets each now run **2** tests (append + delete). The overall target Pass count is unchanged (no new targets) — verify both `:catalog` lines show 2 passed.

- [ ] **Lint:**
```bash
buck2 run //tools:prek -- run --all-files
```
Expected: rustfmt, clippy, file checks, reindeer-in-sync all pass.

- [ ] Hand off to **superpowers:finishing-a-development-branch**.

---

## Self-Review Notes (for the implementer)

- **Spec coverage:** `drop_table` seam; memory sets `end` on table+files+columns; pg drives real `DROP` + reads back `D`; contract asserts `end > s` false at `D`, non-null `end > s` true at `s1`, `begin <= s` false before existence, drop-aware `current_snapshot`/`snapshots`, never-existed `NotFound`. All covered.
- **Type consistency:** `CatalogSeed::drop_table(&self, &TableRef) -> SnapshotId`; `SnapshotId: Ord` (so `d > s1`, `sn.id < d`); memory `Versioned.end: Option<i64>`. The `MemSeeder`/`PgSeeder` impls gain exactly one method.
- **Sequencing:** Task 1's trait change breaks the adapter catalog tests until Tasks 2–3; `--no-verify` the Task 1 commit or fold all three. Final `prek --all-files` validates the whole tree.
- **No new BUCK targets:** the new tests are additional `#[tokio::test]` fns in the existing `catalog.rs` files.
