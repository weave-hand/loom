# Iceberg Additive Schema Evolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn loom's silent Iceberg-mirror schema divergence into correct *additive* schema evolution — a land that appends nullable columns evolves the table and reads return the superset (null-filling old files), while every non-additive change is rejected with a typed error and the commit aborts.

**Architecture:** A single pure classifier `(live, incoming) → {Identical | Additive | error}` is the one place the additive-vs-unsupported policy lives. Both mirror-projection sites (`register_files`, `write_mirror`) call a shared `reconcile_and_project` helper built on it, replacing today's project-once `columns_exist` guard. Because loom reads are **mirror-authoritative** (they resolve entirely through `iceberg_mirror.*` and read Parquet file paths directly, not the raw Iceberg metadata), an additive land is driven through the existing **mirror-only** path (`register_files`) — it writes superset Parquet and projects the new nullable columns *without* mutating the real Iceberg metadata schema or calling `fast_append`. The read path makes the mirror schema authoritative over the file set and relies on DataFusion's default schema-adapter null-fill for old files missing the new column. `schema_version` (today hardcoded `0`) becomes the per-table schema generation, derived from the mirror.

**Tech Stack:** Rust 2024, buck2, sqlx compile-time macros (Postgres), iceberg-rust 0.9.1, DataFusion (query-api serving), arrow-57 (writer chain) / arrow-schema (provider). Tests are `rust_test` integration targets — pure-logic ones are plain `rust_test` (RE-eligible); fixture-backed ones use the `loom_fixture_test` macro.

## Global Constraints

- **No inline `#[cfg(test)]` tests.** Every test is a sibling `tests/<name>.rs` file wired as its own target in the crate's `BUCK`. The `no-inline-tests` prek hook fails the build otherwise.
- **Fixture tests use `loom_fixture_test`, not bare `rust_test`** — they boot hermetic Postgres/DuckDB which refuse to run as root on RE. Pure-logic tests use plain `rust_test`.
- **Compile-time SQL:** any new/changed `sqlx::query!`/`query_scalar!` in `src/control-plane/postgres` requires regenerating the committed `.sqlx` cache via `tools/sqlx-prepare.sh` (boots hermetic Postgres, applies migrations, attaches DuckLake, runs `cargo sqlx prepare`). The `sqlx-cache-check` test enforces freshness in the normal sweep.
- **No new third-party dependency. No migration** — `iceberg_mirror.snapshot.schema_version` already exists (`bigint NOT NULL DEFAULT 0`).
- **Iceberg backend only.** DuckLake schema evolution is the separate `fut-schema-evolution-coverage`.
- **Scope decision (deviates from spec §1 location, by design):** reconciliation is fed by the landing's declared `columns` (not `columns_of(staged_table)`), and additive lands take a **mirror-only** path. The real Iceberg metadata schema is **not** evolved, so external `ATTACH` clients do not see new columns until a later full-evolution slice — the accepted `iss-iceberg-inline-visibility` gap class. This is forced by iceberg-rust 0.9.1 having no atomic schema-evolution+append transaction action, and is consistent with loom's mirror-authoritative reads.
- **Don't pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to a file and grep it: `buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **Commit messages:** Conventional Commits (enforced by the `conventional-commit` hook).

---

### Task 1: Schema-change classifier + typed error (pure)

The single, unit-testable home of the additive-vs-unsupported policy. Pure function over `ProjectedColumn` slices; no I/O.

**Files:**
- Create: `src/control-plane/postgres/src/iceberg_schema_evolution.rs`
- Modify: `src/control-plane/postgres/src/lib.rs` (add `pub mod iceberg_schema_evolution;`, alphabetically after `iceberg_read`)
- Modify: `src/control-plane/postgres/BUCK` (add a plain `rust_test` target — NOT `loom_fixture_test`)
- Test: `src/control-plane/postgres/tests/iceberg_schema_evolution.rs`

**Interfaces:**
- Consumes: `ProjectedColumn { order: i64, name: String, iceberg_type: String, nullable: bool }` from `crate::iceberg_mirror`.
- Produces:
  - `pub enum SchemaPlan { Identical, Additive { new_columns: Vec<ProjectedColumn> } }`
  - `pub enum SchemaEvolutionError` (derives `Debug, Clone, PartialEq, thiserror::Error`) with variants (every `Display` string begins `schema evolution unsupported:`):
    - `ColumnDropped { name: String }`
    - `ColumnChangedAtPosition { position: i64, was: String, now: String }` — name mismatch in the shared prefix (rename/reorder)
    - `ColumnTypeChanged { name: String, from: String, to: String }`
    - `ColumnNullabilityChanged { name: String }`
    - `NonNullableColumnAdded { name: String }`
  - `pub fn classify_schema_change(live: &[ProjectedColumn], incoming: &[ProjectedColumn]) -> Result<SchemaPlan, SchemaEvolutionError>`

**Policy (exact):** compare **positionally**, by `(name, iceberg_type, nullable)` only — **never** by the `order` field value (the inline path stores 0-indexed `order`, the Parquet path 1-indexed; positional comparison is robust to that).
1. For `i` in `0..min(live.len(), incoming.len())`: if `live[i].name != incoming[i].name` → `ColumnChangedAtPosition`. Else if `iceberg_type` differs → `ColumnTypeChanged`. Else if `nullable` differs → `ColumnNullabilityChanged`.
2. If `incoming.len() < live.len()` → `ColumnDropped { name: live[incoming.len()].name }`.
3. The suffix `incoming[live.len()..]` are the new columns. If any has `nullable == false` → `NonNullableColumnAdded { name }`. Else if the suffix is empty → `Identical`. Else → `Additive { new_columns: suffix.to_vec() }`.

- [ ] **Step 1: Write the failing test**

```rust
// src/control-plane/postgres/tests/iceberg_schema_evolution.rs
use control_plane_postgres::iceberg_mirror::ProjectedColumn;
use control_plane_postgres::iceberg_schema_evolution::{
    classify_schema_change, SchemaEvolutionError, SchemaPlan,
};

fn col(order: i64, name: &str, ty: &str, nullable: bool) -> ProjectedColumn {
    ProjectedColumn { order, name: name.into(), iceberg_type: ty.into(), nullable }
}

fn base() -> Vec<ProjectedColumn> {
    vec![col(1, "a", "long", false), col(2, "b", "string", true)]
}

#[test]
fn identical_is_identical() {
    assert_eq!(classify_schema_change(&base(), &base()), Ok(SchemaPlan::Identical));
}

#[test]
fn append_nullable_is_additive() {
    let mut incoming = base();
    incoming.push(col(3, "c", "long", true));
    let plan = classify_schema_change(&base(), &incoming).unwrap();
    match plan {
        SchemaPlan::Additive { new_columns } => {
            assert_eq!(new_columns.len(), 1);
            assert_eq!(new_columns[0].name, "c");
        }
        other => panic!("expected Additive, got {other:?}"),
    }
}

#[test]
fn append_required_is_rejected() {
    let mut incoming = base();
    incoming.push(col(3, "c", "long", false));
    assert_eq!(
        classify_schema_change(&base(), &incoming),
        Err(SchemaEvolutionError::NonNullableColumnAdded { name: "c".into() })
    );
}

#[test]
fn drop_is_rejected() {
    let incoming = vec![col(1, "a", "long", false)];
    assert_eq!(
        classify_schema_change(&base(), &incoming),
        Err(SchemaEvolutionError::ColumnDropped { name: "b".into() })
    );
}

#[test]
fn rename_is_rejected() {
    let incoming = vec![col(1, "a", "long", false), col(2, "bb", "string", true)];
    assert!(matches!(
        classify_schema_change(&base(), &incoming),
        Err(SchemaEvolutionError::ColumnChangedAtPosition { .. })
    ));
}

#[test]
fn retype_is_rejected() {
    let incoming = vec![col(1, "a", "string", false), col(2, "b", "string", true)];
    assert!(matches!(
        classify_schema_change(&base(), &incoming),
        Err(SchemaEvolutionError::ColumnTypeChanged { .. })
    ));
}

#[test]
fn nullability_change_is_rejected() {
    let incoming = vec![col(1, "a", "long", true), col(2, "b", "string", true)];
    assert!(matches!(
        classify_schema_change(&base(), &incoming),
        Err(SchemaEvolutionError::ColumnNullabilityChanged { .. })
    ));
}

#[test]
fn middle_insert_is_rejected() {
    // inserting nullable "m" between a and b shows up as a prefix mismatch at position 1
    let incoming = vec![
        col(1, "a", "long", false),
        col(2, "m", "long", true),
        col(3, "b", "string", true),
    ];
    assert!(matches!(
        classify_schema_change(&base(), &incoming),
        Err(SchemaEvolutionError::ColumnChangedAtPosition { .. })
    ));
}

#[test]
fn from_empty_live_appends_all() {
    // creation case is the caller's concern, but classify against empty live treats
    // every incoming column as "new"; a required column from empty live is rejected,
    // so callers must special-case creation (project all) before calling classify.
    let plan = classify_schema_change(&[], &[col(1, "a", "long", true)]).unwrap();
    assert!(matches!(plan, SchemaPlan::Additive { .. }));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test //src/control-plane/postgres:iceberg-schema-evolution > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\\[" /tmp/t.log`
Expected: FAIL — module/target does not exist yet.

- [ ] **Step 3: Write the classifier module**

```rust
// src/control-plane/postgres/src/iceberg_schema_evolution.rs
//! The single home of loom's additive-vs-unsupported Iceberg schema-evolution
//! policy. Pure logic over `iceberg_mirror::ProjectedColumn` — no I/O — so it is
//! unit-testable in isolation and cannot drift between the two mirror-projection
//! sites (`register_files`, `write_mirror`) that call it via `reconcile_and_project`.

use crate::iceberg_mirror::ProjectedColumn;

/// The outcome of comparing the live mirror columns to an incoming write's columns.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaPlan {
    /// Incoming equals live — no column rows to write.
    Identical,
    /// Incoming equals live plus one or more new nullable columns appended at the end.
    Additive { new_columns: Vec<ProjectedColumn> },
}

/// A non-additive (therefore rejected) schema change. Every `Display` begins
/// `schema evolution unsupported:` so callers can surface a stable, matchable marker.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum SchemaEvolutionError {
    #[error("schema evolution unsupported: column {name:?} was dropped")]
    ColumnDropped { name: String },
    #[error("schema evolution unsupported: column at position {position} changed from {was:?} to {now:?} (rename or reorder)")]
    ColumnChangedAtPosition { position: i64, was: String, now: String },
    #[error("schema evolution unsupported: column {name:?} type changed from {from:?} to {to:?}")]
    ColumnTypeChanged { name: String, from: String, to: String },
    #[error("schema evolution unsupported: column {name:?} nullability changed")]
    ColumnNullabilityChanged { name: String },
    #[error("schema evolution unsupported: new column {name:?} is not nullable")]
    NonNullableColumnAdded { name: String },
}

/// Classify `incoming` against the `live` mirror columns. Comparison is POSITIONAL on
/// `(name, iceberg_type, nullable)` — the `order` field value is intentionally ignored.
pub fn classify_schema_change(
    live: &[ProjectedColumn],
    incoming: &[ProjectedColumn],
) -> Result<SchemaPlan, SchemaEvolutionError> {
    let shared = live.len().min(incoming.len());
    for i in 0..shared {
        let (l, n) = (&live[i], &incoming[i]);
        if l.name != n.name {
            return Err(SchemaEvolutionError::ColumnChangedAtPosition {
                position: i as i64,
                was: l.name.clone(),
                now: n.name.clone(),
            });
        }
        if l.iceberg_type != n.iceberg_type {
            return Err(SchemaEvolutionError::ColumnTypeChanged {
                name: l.name.clone(),
                from: l.iceberg_type.clone(),
                to: n.iceberg_type.clone(),
            });
        }
        if l.nullable != n.nullable {
            return Err(SchemaEvolutionError::ColumnNullabilityChanged { name: l.name.clone() });
        }
    }
    if incoming.len() < live.len() {
        return Err(SchemaEvolutionError::ColumnDropped {
            name: live[incoming.len()].name.clone(),
        });
    }
    let new_columns: Vec<ProjectedColumn> = incoming[live.len()..].to_vec();
    if let Some(req) = new_columns.iter().find(|c| !c.nullable) {
        return Err(SchemaEvolutionError::NonNullableColumnAdded { name: req.name.clone() });
    }
    if new_columns.is_empty() {
        Ok(SchemaPlan::Identical)
    } else {
        Ok(SchemaPlan::Additive { new_columns })
    }
}
```

Then add to `src/control-plane/postgres/src/lib.rs` after the `pub mod iceberg_read;` line:

```rust
pub mod iceberg_schema_evolution;
```

And ensure `ProjectedColumn` derives `Clone` (it is constructed with `.to_vec()` here). Check `src/control-plane/postgres/src/iceberg_mirror.rs`: the struct must derive `Clone` (and ideally `PartialEq, Debug` for tests). If `#[derive(...)]` is missing `Clone`/`Debug`/`PartialEq`, add them:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedColumn { /* ...unchanged fields... */ }
```

- [ ] **Step 4: Wire the BUCK target**

Add to `src/control-plane/postgres/BUCK` (mirror the existing `iceberg-type` plain `rust_test`):

```python
rust_test(
    name = "iceberg-schema-evolution",
    crate = "iceberg_schema_evolution",
    srcs = ["tests/iceberg_schema_evolution.rs"],
    crate_root = "tests/iceberg_schema_evolution.rs",
    edition = "2024",
    deps = [":postgres"],
)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `buck2 test //src/control-plane/postgres:iceberg-schema-evolution > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (all classifier cases).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_schema_evolution.rs \
        src/control-plane/postgres/src/lib.rs \
        src/control-plane/postgres/src/iceberg_mirror.rs \
        src/control-plane/postgres/BUCK \
        src/control-plane/postgres/tests/iceberg_schema_evolution.rs
git commit -m "feat(iceberg): add additive-vs-unsupported schema-change classifier"
```

---

### Task 2: Mirror helpers — `live_columns`, `stamp_schema_version`, `reconcile_and_project`

The I/O layer the policy plugs into: read live columns MVCC-correctly, project new columns on Additive, stamp the schema generation. New `sqlx` queries → `.sqlx` regen.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_mirror.rs` (add three `pub async fn`)
- Modify: `src/control-plane/postgres/.sqlx/` (regenerated, committed)
- Test: `src/control-plane/postgres/tests/iceberg_schema_evolution_mirror.rs` (new `loom_fixture_test`)
- Modify: `src/control-plane/postgres/BUCK` (add the `loom_fixture_test` target)

**Interfaces:**
- Consumes: `classify_schema_change`, `SchemaPlan` (Task 1); existing `project_columns`, `SnapshotId`, `backend`.
- Produces:
  - `pub async fn live_columns(conn: &mut PgConnection, table_id: i64, at: SnapshotId) -> Result<Vec<ProjectedColumn>>`
  - `pub async fn live_columns_for(conn: &mut PgConnection, table: &TableRef, at: SnapshotId) -> Result<Vec<ProjectedColumn>>` — resolves the live `table_id` for `table` (the `iceberg_mirror.table` lookup `where table_namespace=$1 and table_name=$2 and end_snapshot is null`), returns `Vec::new()` if no live table row exists, else delegates to `live_columns`. The landing pre-check (Task 5) uses this so it never hand-rolls a tid lookup.
  - `pub async fn stamp_schema_version(conn: &mut PgConnection, table_id: i64, at: SnapshotId) -> Result<()>`
  - `pub async fn reconcile_and_project(conn: &mut PgConnection, table_id: i64, at: SnapshotId, incoming: &[ProjectedColumn]) -> Result<()>`

**Behavior:**
- `live_columns`: **byte-for-byte the same MVCC predicate `IcebergCatalog::schema` uses** (`iceberg_catalog.rs:250` — `begin_snapshot <= $2`), returning `ProjectedColumn`s ordered by `column_order`:
  ```sql
  select column_order as "column_order!", column_name as "column_name!",
         column_type as "column_type!", nulls_allowed as "nulls_allowed!"
  from iceberg_mirror.column
  where table_id = $1 and begin_snapshot <= $2 and (end_snapshot is null or end_snapshot > $2)
  order by column_order
  ```
  Map each row to `ProjectedColumn { order: column_order, name: column_name, iceberg_type: column_type, nullable: nulls_allowed }`. **Callers pass the snapshot to read as-of directly** (the *current* snapshot for a pre-check, or the *new* `at` inside `reconcile_and_project`). Using `<= $2` is correct inside `reconcile_and_project(at)` because `live_columns` runs *before* `project_columns`, so nothing with `begin_snapshot == at` exists yet — `<= at` therefore returns exactly the pre-this-land live set. No `at + 1` arithmetic anywhere (the previous draft's `+1` dance leaned on the catalog-global snapshot sequence and is removed).
- `live_columns_for`: resolve the live `table_id`, returning `Vec::new()` if absent (so a first-ever land classifies as creation):
  ```rust
  pub async fn live_columns_for(
      conn: &mut PgConnection,
      table: &TableRef,
      at: SnapshotId,
  ) -> Result<Vec<ProjectedColumn>> {
      let tid: Option<i64> = sqlx::query_scalar!(
          "select table_id as \"id!\" from iceberg_mirror.table \
           where table_namespace = $1 and table_name = $2 and end_snapshot is null",
          table.schema, table.name,
      ).fetch_optional(&mut *conn).await.map_err(backend)?;
      match tid {
          Some(tid) => live_columns(conn, tid, at).await,
          None => Ok(Vec::new()),
      }
  }
  ```
  Add `use control_plane_core::TableRef;` if not already imported.
- `stamp_schema_version`: the per-table schema generation = count of distinct `begin_snapshot` values visible at `at`:
  ```sql
  update iceberg_mirror.snapshot set schema_version = (
      select count(distinct begin_snapshot) from iceberg_mirror.column
      where table_id = $1 and begin_snapshot <= $2
  ) where snapshot_id = $2
  ```
  (Creation → 1 distinct → generation 1. Additive at a new snapshot → +1. No-change land → unchanged count = current generation.)
- `reconcile_and_project`: the shared policy entry point.
  ```rust
  pub async fn reconcile_and_project(
      conn: &mut PgConnection,
      table_id: i64,
      at: SnapshotId,
      incoming: &[ProjectedColumn],
  ) -> Result<()> {
      let live = live_columns(conn, table_id, at).await?;
      if live.is_empty() {
          // First write (table creation): project all columns regardless of nullability.
          project_columns(conn, table_id, at, incoming).await?;
          return Ok(());
      }
      match classify_schema_change(&live, incoming)
          .map_err(|e| ControlPlaneError::Validation(e.to_string()))?
      {
          SchemaPlan::Identical => {}
          SchemaPlan::Additive { new_columns } => {
              project_columns(conn, table_id, at, &new_columns).await?;
          }
      }
      Ok(())
  }
  ```
  Add `use crate::iceberg_schema_evolution::{classify_schema_change, SchemaPlan};` and `use control_plane_core::ControlPlaneError;` to `iceberg_mirror.rs` if not present.

- [ ] **Step 1: Write the failing fixture test**

```rust
// src/control-plane/postgres/tests/iceberg_schema_evolution_mirror.rs
//! Fixture test for the mirror reconciliation helpers, driven directly against a
//! seeded mirror (no Parquet) so each policy branch is exercised in isolation.
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_mirror::{
    ensure_table, live_columns, next_snapshot, reconcile_and_project, stamp_schema_version,
    ProjectedColumn,
};

fn col(order: i64, name: &str, ty: &str, nullable: bool) -> ProjectedColumn {
    ProjectedColumn { order, name: name.into(), iceberg_type: ty.into(), nullable }
}

#[tokio::test]
async fn reconcile_creates_then_appends_then_stamps() {
    let fixture = PgFixture::start();
    let (cp, _db) = fixture.fresh_db().await;
    let pool = cp.pool().clone();
    let mut conn = pool.acquire().await.unwrap();

    // Creation: empty live → project all (incl. a required column).
    let s1 = next_snapshot(&mut conn, None).await.unwrap();
    let tid = ensure_table(&mut conn, "s", "t", s1).await.unwrap();
    let base = vec![col(1, "a", "long", false), col(2, "b", "string", true)];
    reconcile_and_project(&mut conn, tid, s1, &base).await.unwrap();
    stamp_schema_version(&mut conn, tid, s1).await.unwrap();
    assert_eq!(live_columns(&mut conn, tid, s1).await.unwrap().len(), 2);

    // Additive: append nullable c at a new snapshot.
    let s2 = next_snapshot(&mut conn, None).await.unwrap();
    let mut wider = base.clone();
    wider.push(col(3, "c", "long", true));
    reconcile_and_project(&mut conn, tid, s2, &wider).await.unwrap();
    stamp_schema_version(&mut conn, tid, s2).await.unwrap();

    // Three live columns at s2; c.begin_snapshot == s2.
    let live = live_columns(&mut conn, tid, s2).await.unwrap();
    assert_eq!(live.iter().map(|c| c.name.clone()).collect::<Vec<_>>(), ["a", "b", "c"]);
    let c_begin: i64 = sqlx::query_scalar(
        "select begin_snapshot from iceberg_mirror.column where table_id = $1 and column_name = 'c'",
    )
    .bind(tid)
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    assert_eq!(c_begin, s2.0);

    // schema_version bumped: s1 → 1, s2 → 2.
    let v1: i64 = sqlx::query_scalar("select schema_version from iceberg_mirror.snapshot where snapshot_id = $1")
        .bind(s1.0).fetch_one(&mut *conn).await.unwrap();
    let v2: i64 = sqlx::query_scalar("select schema_version from iceberg_mirror.snapshot where snapshot_id = $1")
        .bind(s2.0).fetch_one(&mut *conn).await.unwrap();
    assert_eq!((v1, v2), (1, 2));
}

#[tokio::test]
async fn reconcile_rejects_drop() {
    let fixture = PgFixture::start();
    let (cp, _db) = fixture.fresh_db().await;
    let pool = cp.pool().clone();
    let mut conn = pool.acquire().await.unwrap();
    let s1 = next_snapshot(&mut conn, None).await.unwrap();
    let tid = ensure_table(&mut conn, "s", "t", s1).await.unwrap();
    reconcile_and_project(&mut conn, tid, s1, &[col(1, "a", "long", false), col(2, "b", "string", true)])
        .await.unwrap();
    let s2 = next_snapshot(&mut conn, None).await.unwrap();
    let err = reconcile_and_project(&mut conn, tid, s2, &[col(1, "a", "long", false)]).await.unwrap_err();
    assert!(err.to_string().contains("schema evolution unsupported"), "got: {err}");
}
```

> Note for the implementer: confirm `PgControlPlane::pool()` exists (used by `iceberg_catalog.rs` tests via `cp.pool().clone()`). If `ensure_table`/`next_snapshot` are not already `pub`, they are (used across modules) — verify and widen visibility only if the build complains.

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test //src/control-plane/postgres:iceberg-schema-evolution-mirror > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\\[" /tmp/t.log`
Expected: FAIL — helpers don't exist / build error.

- [ ] **Step 3: Implement the three helpers** in `src/control-plane/postgres/src/iceberg_mirror.rs` per the Behavior section above.

- [ ] **Step 4: Wire the `loom_fixture_test` target**

Add to `src/control-plane/postgres/BUCK` (mirror an existing `loom_fixture_test`, e.g. the `iceberg_catalog` target — same deps shape: `:postgres`, `//src/control-plane/core:core`, `//third-party:tokio`, and `//third-party:sqlx` if the test calls `sqlx::query_scalar` directly):

```python
loom_fixture_test(
    name = "iceberg-schema-evolution-mirror",
    crate = "iceberg_schema_evolution_mirror",
    srcs = ["tests/iceberg_schema_evolution_mirror.rs"],
    crate_root = "tests/iceberg_schema_evolution_mirror.rs",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:sqlx",
        "//third-party:tokio",
    ],
)
```

(`loom_fixture_test` is imported at the top of the BUCK via `load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")` — confirm it's already loaded; it is, for the existing fixture targets.)

- [ ] **Step 5: Regenerate the `.sqlx` cache**

```bash
bash tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; tail -5 /tmp/sqlx.log
```

- [ ] **Step 6: Run the fixture test + the freshness check**

Run: `buck2 test //src/control-plane/postgres:iceberg-schema-evolution-mirror //src/control-plane/postgres:sqlx-cache-check > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS for both.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_mirror.rs \
        src/control-plane/postgres/.sqlx \
        src/control-plane/postgres/BUCK \
        src/control-plane/postgres/tests/iceberg_schema_evolution_mirror.rs
git commit -m "feat(iceberg): add live_columns, schema_version stamping, reconcile_and_project helpers"
```

---

### Task 3: Replace the project-once guard at both mirror-projection sites

Swap the `if !columns_exist { project_columns }` guard for `reconcile_and_project` + `stamp_schema_version` in `register_files` (mirror-only path) and `write_mirror` (fast_append catalog path), so both honor the policy and stamp the generation. This keeps existing behavior for identical re-lands and creation while making any divergence reject.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (`register_files`, ~lines 229-248)
- Modify: `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` (`write_mirror`, ~lines 365-398)
- Test: existing `//src/control-plane/postgres:iceberg-catalog`, `:iceberg-overwrite`, `:iceberg-control-plane` must stay green; add no new test here (Task 5/6 cover evolution end-to-end). The catalog contract already exercises create + identical re-append + drop, which now flow through `reconcile_and_project`.

**Interfaces:**
- Consumes: `reconcile_and_project`, `stamp_schema_version` (Task 2).
- Produces: no new public surface; behavior change only.

**`register_files`** — replace:
```rust
if !columns_exist(conn, tid).await? {
    project_columns(conn, tid, at, &projected_columns(columns)?).await?;
}
project_files(conn, tid, at, &projected_files(files)?).await?;
```
with:
```rust
reconcile_and_project(conn, tid, at, &projected_columns(columns)?).await?;
project_files(conn, tid, at, &projected_files(files)?).await?;
stamp_schema_version(conn, tid, at).await?;
```
Update the `use crate::iceberg_mirror::{...}` import to add `reconcile_and_project, stamp_schema_version` and drop `columns_exist, project_columns` if now unused (clippy will flag unused imports). Keep `ensure_table`, `end_cap_live_data_files`, `project_files`.

**`write_mirror`** — replace:
```rust
if !columns_exist(conn, tid).await? {
    project_columns(conn, tid, at, columns).await?;
}
project_files(conn, tid, at, files).await?;
Ok(at)
```
with:
```rust
reconcile_and_project(conn, tid, at, columns).await?;
project_files(conn, tid, at, files).await?;
stamp_schema_version(conn, tid, at).await?;
Ok(at)
```
Update the `use crate::iceberg_mirror::{...}` import inside `write_mirror` similarly (it currently imports `columns_exist, project_columns`; replace with `reconcile_and_project, stamp_schema_version`).

> Rationale: in loom today the fast_append path's `columns` (= `columns_of(staged_table)`) never diverges from live (the real Iceberg schema is create-only), so `write_mirror` sees `Identical` and is a no-op for columns — same as before — but it now also stamps `schema_version` and would correctly reject/evolve if the staged schema ever diverged. The actual additive evolution is driven through `register_files` by Task 5.

- [ ] **Step 1: Make the edits** to `register_files` and `write_mirror` as specified.

- [ ] **Step 2: Run the affected fixture suites**

Run: `buck2 test //src/control-plane/postgres:iceberg-catalog //src/control-plane/postgres:iceberg-overwrite //src/control-plane/postgres:iceberg-control-plane //src/control-plane/postgres:sqlx-cache-check > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (no behavioral regression; `.sqlx` already covers these queries from Task 2).

- [ ] **Step 3: Clippy clean**

Run: `buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' > /tmp/c.log 2>&1; cat $(buck2 build --show-output '//src/control-plane/postgres:postgres[clippy.txt]' 2>/dev/null | awk '{print $2}') 2>/dev/null` (empty == clean). Simpler: `bash tools/clippy-all.sh > /tmp/c.log 2>&1; grep -iE "warning|error" /tmp/c.log | head`.
Expected: no warnings for the postgres crate (fix unused-import warnings from the guard removal).

- [ ] **Step 4: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_landing.rs \
        src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs
git commit -m "refactor(iceberg): route both mirror-projection sites through reconcile_and_project"
```

---

### Task 4: Inline path — detect + reject parity

The inline accumulation path projects columns once. Give it the same policy, **reject-only** (additive on the inline path is deferred): identical re-append is a no-op, anything else (including additive) is the typed error. First write still projects all columns.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (`inline_append`, the `if !columns_exist` guard ~lines 147-167)
- Test: `src/control-plane/postgres/tests/iceberg_schema_evolution_inline.rs` (new `loom_fixture_test`)
- Modify: `src/control-plane/postgres/BUCK`

**Interfaces:**
- Consumes: `classify_schema_change`, `SchemaPlan`, `live_columns` (Tasks 1-2); existing `inline_append` signature unchanged.
- Produces: `inline_append` now returns `Err(ControlPlaneError::Validation(...))` on a divergent inline write.

**Edit** — replace the inline guard:
```rust
if !columns_exist(conn, tid).await? {
    let pcols = /* ...existing mapping of `columns` -> Vec<ProjectedColumn>... */;
    project_columns(conn, tid, at, &pcols).await?;
}
```
with:
```rust
let pcols = /* ...existing mapping unchanged... */;
let live = live_columns(conn, tid, at).await?;
if live.is_empty() {
    project_columns(conn, tid, at, &pcols).await?;
} else {
    match classify_schema_change(&live, &pcols) {
        Ok(SchemaPlan::Identical) => {}
        Ok(SchemaPlan::Additive { .. }) => {
            return Err(ControlPlaneError::Validation(
                "schema evolution unsupported: additive evolution on the inline path is deferred".into(),
            ));
        }
        Err(e) => return Err(ControlPlaneError::Validation(e.to_string())),
    }
}
```
Add the imports `use crate::iceberg_schema_evolution::{classify_schema_change, SchemaPlan};` and `use crate::iceberg_mirror::live_columns;` (and drop `columns_exist` if now unused). Keep the existing `pcols` construction (the `iceberg_physical_type` mapping) exactly as-is.

- [ ] **Step 1: Write the failing fixture test**

```rust
// src/control-plane/postgres/tests/iceberg_schema_evolution_inline.rs
//! Inline-path parity: a divergent inline write is rejected (detect+reject), an
//! identical re-append is a no-op.
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};

#[tokio::test]
async fn inline_identical_reappend_ok_divergent_rejected() {
    let fixture = PgFixture::start();
    let (cp, db) = fixture.fresh_db().await;
    let writer = IcebergWriter::new(cp.pool().clone(), fixture.pg_dsn(&db));
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    let run = uuid::Uuid::from_u128(1);

    // First inline write creates + projects.
    writer.inline("s", "t", &cols, &[(1, "a")], run).await;
    // Identical re-append: no-op, succeeds.
    writer.inline("s", "t", &cols, &[(2, "b")], uuid::Uuid::from_u128(2)).await;

    // Divergent inline write (extra nullable column) must be rejected. `inline` writes a
    // fixed (id,name) batch, so drive divergence through inline_append directly with a
    // wider ColumnSpec list — see helper below.
    let wider = {
        let mut c = cols.clone();
        c.push(("extra".to_string(), "long".to_string(), true));
        c
    };
    let err = writer.inline_expect_err("s", "t", &wider).await;
    assert!(err.contains("schema evolution unsupported"), "got: {err}");
}
```

This needs a small harness helper. Add to `IcebergWriter` in `src/control-plane/postgres/src/fixture.rs` a method that calls `inline_append` with a wider `ColumnSpec` list against the existing `(id,name)` batch and returns the error string:

```rust
/// Attempt an inline append whose declared `columns` diverge from the live mirror
/// schema, returning the error string. The batch itself stays `(id long, name string)`
/// so only the declared schema diverges — exercising the detect+reject path. Test-only.
pub async fn inline_expect_err(
    &self,
    ns: &str,
    name: &str,
    columns: &[(String, String, bool)],
) -> String {
    let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
    ]));
    let batch = arrow_array::RecordBatch::try_new(
        schema,
        vec![
            std::sync::Arc::new(arrow_array::Int64Array::from(vec![9i64])),
            std::sync::Arc::new(arrow_array::StringArray::from(vec!["z"])),
        ],
    )
    .expect("inline batch");
    let specs: Vec<control_plane_core::ColumnSpec> = columns
        .iter()
        .map(|(n, t, nullable)| control_plane_core::ColumnSpec {
            name: n.clone(),
            ty: t.clone(),
            nullable: *nullable,
        })
        .collect();
    let lineage = control_plane_core::LineageEvent {
        run_id: control_plane_core::RunId(uuid::Uuid::from_u128(99)),
        event_type: control_plane_core::EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "inline-evolution-test" }),
    };
    let table = control_plane_core::TableRef { schema: ns.into(), name: name.into() };
    crate::iceberg_inline::inline_append(&self.pool, &table, &specs, &batch, lineage, None)
        .await
        .expect_err("expected schema-evolution rejection")
        .to_string()
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test //src/control-plane/postgres:iceberg-schema-evolution-inline > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\\[" /tmp/t.log`
Expected: FAIL.

- [ ] **Step 3: Implement** the `inline_append` edit and the `inline_expect_err` harness helper.

- [ ] **Step 4: Wire the BUCK target**

```python
loom_fixture_test(
    name = "iceberg-schema-evolution-inline",
    crate = "iceberg_schema_evolution_inline",
    srcs = ["tests/iceberg_schema_evolution_inline.rs"],
    crate_root = "tests/iceberg_schema_evolution_inline.rs",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 5: Regenerate `.sqlx` (if inline added/changed queries — it should not, but run to be safe) and run**

Run: `bash tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; buck2 test //src/control-plane/postgres:iceberg-schema-evolution-inline //src/control-plane/postgres:sqlx-cache-check > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_inline.rs \
        src/control-plane/postgres/src/fixture.rs \
        src/control-plane/postgres/.sqlx \
        src/control-plane/postgres/BUCK \
        src/control-plane/postgres/tests/iceberg_schema_evolution_inline.rs
git commit -m "feat(iceberg): inline path detect+reject for schema divergence"
```

---

### Task 5: Landing-path additive routing (write side)

Make `append_parquet_snapshot` classify the landing `columns` against the live mirror and, on Additive, take a **mirror-only** path: write Parquet with the **superset** arrow schema, build loom `DataFile`s with per-column stats, and project via `register_files` + lineage in one transaction. Identical/creation keep the existing `fast_append` path. Unsupported rejects before any write.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_writer.rs` (parameterize the Parquet write by schema)
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs` (`append_parquet_snapshot` routing + a new mirror-only additive helper)
- Test: `src/control-plane/postgres/tests/iceberg_schema_evolution_land.rs` (new `loom_fixture_test`)
- Modify: `src/control-plane/postgres/BUCK`

**Interfaces:**
- Consumes: `classify_schema_change`/`SchemaPlan` (Task 1); `live_columns`, `reconcile_and_project`, `stamp_schema_version`, `next_snapshot`, `ensure_table` (Task 2); `register_files`/`WriteMode`/`projected_columns` (existing); `column_stats_from_parquet` (`iceberg_stats.rs`); `IcebergCatalog` (`current_snapshot`).
- Produces:
  - In `iceberg_writer.rs`: `pub async fn write_parquet_with_schema(table: &Table, schema: iceberg::spec::SchemaRef, batches: Vec<RecordBatch>) -> Result<Vec<DataFile>>` — same as `write_parquet` but uses the passed `schema` for `ParquetWriterBuilder` instead of `table.metadata().current_schema()`. Refactor the existing `write_parquet` to delegate: `write_parquet(table, batches) = write_parquet_with_schema(table, table.metadata().current_schema().clone(), batches)`.
  - In `iceberg_landing.rs`: a private `async fn land_additive(pool, catalog, table, columns, batches, new_columns, lineage, end_cap) -> Result<SnapshotId>`.

**Routing in `append_parquet_snapshot`** — at the top, after `ensure_iceberg_table` (so the namespace/table exist and live columns are queryable), classify and branch:

```rust
ensure_iceberg_table(catalog, table, columns).await?;
// Decide identical/create vs additive vs reject against the live mirror. We need a
// snapshot to read live columns "as of"; use the current snapshot + 1 as the read
// point (live columns have begin_snapshot < that), or an empty set if no snapshot yet.
let incoming = projected_columns(columns)?;
let icb = IcebergCatalog::new(pool.clone());
let live = match icb.current_snapshot(table).await {
    Ok(snap) => {
        let mut conn = pool.acquire().await.map_err(be)?;
        // read point strictly greater than the live columns' begin_snapshot
        live_columns_for(&mut conn, table, SnapshotId(snap.id.0 + 1)).await?
    }
    Err(_) => Vec::new(), // no snapshot yet — creation
};
if !live.is_empty() {
    match classify_schema_change(&live, &incoming) {
        Ok(SchemaPlan::Identical) => { /* fall through to existing fast_append path */ }
        Ok(SchemaPlan::Additive { .. }) => {
            return land_additive(pool, catalog, table, columns, batches, lineage, end_cap).await;
        }
        Err(e) => return Err(ControlPlaneError::Validation(e.to_string())),
    }
}
// ...existing re-wrap + append_batches_with_extras path unchanged (identical / creation)...
```

> Implementer notes:
> - `live_columns_for` (Task 2) resolves the tid internally; the landing path never hand-rolls a tid lookup.
> - The classify here is a **fast pre-check** so additive routes to the mirror-only path and unsupported rejects before any Parquet is written; `register_files` re-runs `reconcile_and_project` as the authoritative gate inside the tx (both compare physical `iceberg_type` strings — `live_columns`/`live_columns_for` return the stored physical type, and `projected_columns(columns)` maps via `iceberg_physical_type`, so the two vocabularies match).
> - Add `use crate::iceberg_mirror::live_columns_for;`, `use crate::iceberg_schema_evolution::{classify_schema_change, SchemaPlan};`, and `use control_plane_core::{ControlPlaneError, SnapshotId};` to `iceberg_landing.rs` if not already present.

**`land_additive`** (mirror-only — the additive twin of the fast_append path, modeled on `overwrite_truncate` + the transform `register_files` flow):

```rust
#[allow(clippy::too_many_arguments)]
async fn land_additive(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: Option<&LineageEvent>,
    end_cap: Option<InlineEndCap<'_>>,
) -> Result<SnapshotId> {
    // The actual new-column projection is recomputed inside the tx by
    // `register_files` → `reconcile_and_project` (the authoritative gate), so the
    // pre-check's `new_columns` is not threaded in.
    use crate::iceberg_mirror::next_snapshot;
    use crate::lineage::pg_emit;

    // 1. Load the table and build the SUPERSET arrow schema from the landing `columns`
    //    (field-ids 1..N), so the writer chain stamps c into the Parquet.
    let ns = NamespaceIdent::new(table.schema.clone());
    let ident = TableIdent::new(ns, table.name.clone());
    let ice_table = catalog.load_table(&ident).await.map_err(be)?;
    let superset = ice_schema(columns)?; // existing helper: ColumnSpec -> IceSchema, 1-based ids
    let ice_arrow = Arc::new(iceberg::arrow::schema_to_arrow_schema(&superset).map_err(be)?);
    let batches: Vec<RecordBatch> = batches
        .into_iter()
        .map(|b| RecordBatch::try_new(ice_arrow.clone(), b.columns().to_vec()).map_err(be))
        .collect::<Result<Vec<_>>>()?;

    // 2. Write Parquet with the superset schema (no fast_append).
    let superset_ref = superset.into(); // iceberg::spec::SchemaRef
    let ice_files = crate::iceberg_writer::write_parquet_with_schema(&ice_table, superset_ref, batches)
        .await
        .map_err(be)?;

    // 3. Convert iceberg DataFiles -> loom DataFiles with per-column stats. Reuse the same
    //    path the fast_append commit uses (`iceberg_mirror::added_files_of` builds loom
    //    DataFiles with stats from a staged table); here build directly from `ice_files`
    //    + `column_stats_from_parquet` reading each written file's bytes via `ice_table.file_io()`.
    let loom_files = loom_files_from_iceberg(&ice_table, &ice_files, columns).await?;

    // 4. One snapshot: project new columns (reconcile gate), files, lineage, stamp.
    let mut tx = pool.begin().await.map_err(be)?;
    let at = next_snapshot(&mut tx, None).await?;
    register_files(&mut tx, table, columns, &loom_files, WriteMode::Append, at).await?;
    if let Some(cap) = end_cap {
        // retire flushed inline rows at `at` (same as do_update_table's end_cap)
        end_cap_inline_rows(&mut tx, &cap, at).await?;
    }
    if let Some(ev) = lineage {
        pg_emit(&mut *tx, ev).await?;
    }
    // NOTE: schema_version is stamped inside `register_files` (Task 3) — do NOT stamp
    // again here.
    tx.commit().await.map_err(be)?;
    Ok(at)
}
```

> Implementer notes (resolve under TDD — the fixture test is the gate):
> - **`loom_files_from_iceberg`**: build `Vec<control_plane_core::DataFile>` from the written `iceberg::spec::DataFile`s — `path`, `record_count`, `file_size_bytes` come straight off each, and `column_stats` via `column_stats_from_parquet(bytes, &column_names)` after reading `ice_table.file_io().new_input(path)?.read().await?`. This mirrors exactly what the fast_append→`added_files_of` path produces; if `added_files_of` is reusable against the written files, prefer reusing it over a new helper.
> - **`end_cap_inline_rows`**: the inline end-cap UPDATE that `do_update_table` runs (catalog.rs ~lines 470-490). If a non-flush land (`end_cap == None`, the common case for a model-bound land), skip it. The additive fixture test uses no inline rows, so this branch is exercised only by the flush path; keep it faithful to `do_update_table`.
> - **`write_parquet_with_schema` field-ids**: `ice_schema(columns)` assigns 1-based ids over the *full* superset, so the writer stamps every column including `c`. This is internally consistent (the mirror records `c` too); it does **not** evolve the real Iceberg metadata schema (no `AddSchema`), which is the accepted gap.

- [ ] **Step 1: Write the failing fixture test**

```rust
// src/control-plane/postgres/tests/iceberg_schema_evolution_land.rs
//! Additive happy-path (write side) + non-additive rejection, driven through the real
//! landing entrypoint `land` (inline limit 0 forces real Parquet). Asserts mirror state,
//! not reads (the read path is Task 6). Setup mirrors tests/iceberg_overwrite.rs.
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_ipc57::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{ColumnSpec, EventType, LineageEvent, RunId, SnapshotId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use control_plane_core::Catalog;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;

fn col(name: &str, ty: &str, nullable: bool) -> ColumnSpec {
    ColumnSpec { name: name.into(), ty: ty.into(), nullable }
}

fn lineage(run: RunId) -> LineageEvent {
    LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![],
        payload: serde_json::json!({ "source": "schema-evolution-test" }),
    }
}

/// IPC body for (a long, b string) of `rows` rows.
fn ipc_ab(rows: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())),
            Arc::new(StringArray::from((0..rows).map(|i| format!("b{i}")).collect::<Vec<_>>())),
        ],
    ).unwrap();
    encode(&schema, &batch)
}

/// IPC body for (a long, b string, c long-nullable) of `rows` rows.
fn ipc_abc(rows: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
        Field::new("c", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>())),
            Arc::new(StringArray::from((0..rows).map(|i| format!("b{i}")).collect::<Vec<_>>())),
            Arc::new(Int64Array::from((0..rows).map(|i| i + 1000).collect::<Vec<_>>())),
        ],
    ).unwrap();
    encode(&schema, &batch)
}

/// IPC body for (a long) only — used to test a dropped column.
fn ipc_a(rows: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()))],
    ).unwrap();
    encode(&schema, &batch)
}

fn encode(schema: &Arc<Schema>, batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, schema).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), format!("file://{warehouse}"));
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn additive_land_evolves_mirror_and_bumps_schema_version() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().unwrap();
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef { schema: "s".into(), name: "t".into() };
    let ab = vec![col("a", "long", false), col("b", "string", true)];
    let abc = vec![col("a", "long", false), col("b", "string", true), col("c", "long", true)];

    let s1 = land(&pool, &catalog, &t, &ab, &ipc_ab(3), 0, i64::MAX, lineage(RunId(uuid::Uuid::new_v4())))
        .await.expect("base land");
    let s2 = land(&pool, &catalog, &t, &abc, &ipc_abc(2), 0, i64::MAX, lineage(RunId(uuid::Uuid::new_v4())))
        .await.expect("additive land");
    assert!(s2.0 > s1.0);

    // tid for the live table.
    let tid: i64 = sqlx::query_scalar(
        "select table_id from iceberg_mirror.table where table_namespace='s' and table_name='t' and end_snapshot is null",
    ).fetch_one(&pool).await.unwrap();

    // Three live columns at s2; c.begin_snapshot == s2.
    let live: Vec<(String, i64)> = sqlx::query_as(
        "select column_name, begin_snapshot from iceberg_mirror.column \
         where table_id=$1 and begin_snapshot <= $2 and (end_snapshot is null or end_snapshot > $2) \
         order by column_order",
    ).bind(tid).bind(s2.0).fetch_all(&pool).await.unwrap();
    assert_eq!(live.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>(), ["a", "b", "c"]);
    let c_begin = live.iter().find(|(n, _)| n == "c").unwrap().1;
    assert_eq!(c_begin, s2.0, "c appears at the second snapshot");

    // schema_version bumped: s1 -> 1, s2 -> 2.
    let v1: i64 = sqlx::query_scalar("select schema_version from iceberg_mirror.snapshot where snapshot_id=$1")
        .bind(s1.0).fetch_one(&pool).await.unwrap();
    let v2: i64 = sqlx::query_scalar("select schema_version from iceberg_mirror.snapshot where snapshot_id=$1")
        .bind(s2.0).fetch_one(&pool).await.unwrap();
    assert_eq!((v1, v2), (1, 2));

    // The new file is live at s2 and readable (record_count from the additive batch).
    let ice = IcebergCatalog::new(pool.clone());
    let files = ice.files_with_stats(&t, s2).await.unwrap();
    assert_eq!(files.len(), 2, "both the base and additive files are live at s2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_additive_land_is_rejected_and_mirror_unchanged() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().unwrap();
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef { schema: "s".into(), name: "t".into() };
    let ab = vec![col("a", "long", false), col("b", "string", true)];

    let s1 = land(&pool, &catalog, &t, &ab, &ipc_ab(3), 0, i64::MAX, lineage(RunId(uuid::Uuid::new_v4())))
        .await.expect("base land");

    // Drop b: land only (a long).
    let only_a = vec![col("a", "long", false)];
    let err = land(&pool, &catalog, &t, &only_a, &ipc_a(2), 0, i64::MAX, lineage(RunId(uuid::Uuid::new_v4())))
        .await.expect_err("dropping b must be rejected");
    assert!(err.to_string().contains("schema evolution unsupported"), "got: {err}");

    // Add a required column: (a long, b string, d long NOT NULL).
    let abd_req = vec![col("a", "long", false), col("b", "string", true), col("d", "long", false)];
    let body = {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("d", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(StringArray::from(vec!["b0"])),
            Arc::new(Int64Array::from(vec![7i64])),
        ]).unwrap();
        encode(&schema, &batch)
    };
    let err2 = land(&pool, &catalog, &t, &abd_req, &body, 0, i64::MAX, lineage(RunId(uuid::Uuid::new_v4())))
        .await.expect_err("required new column must be rejected");
    assert!(err2.to_string().contains("schema evolution unsupported"), "got: {err2}");

    // Mirror unchanged: still exactly the 2 columns at the still-current snapshot s1.
    let ice = IcebergCatalog::new(pool.clone());
    assert_eq!(ice.current_snapshot(&t).await.unwrap().id, s1, "rejected lands did not advance the snapshot");
    let n: i64 = sqlx::query_scalar(
        "select count(*) from iceberg_mirror.column c \
         join iceberg_mirror.table tb on c.table_id = tb.table_id \
         where tb.table_namespace='s' and tb.table_name='t' and tb.end_snapshot is null and c.end_snapshot is null",
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(n, 2, "still exactly columns a, b");
}
```

> Implementer notes: confirm the `arrow_ipc57`/`arrow_array`/`arrow_schema` import paths and BUCK deps match `tests/iceberg_overwrite.rs` (which is the template — copy its `deps` list). `land(... , 0, i64::MAX, ...)` forces the real-Parquet branch (inline byte limit 0). The first rejection case (`expect_err`) asserts the error stringifies through `ControlPlaneError::Validation`; if `land` wraps the error differently, match on the `"schema evolution unsupported"` substring regardless of the wrapping variant.

BUCK target (mirror the `iceberg-overwrite` target's deps — the template uses these):

```python
loom_fixture_test(
    name = "iceberg-schema-evolution-land",
    crate = "iceberg_schema_evolution_land",
    srcs = ["tests/iceberg_schema_evolution_land.rs"],
    crate_root = "tests/iceberg_schema_evolution_land.rs",
    named_deps = {"arrow_ipc57": "//third-party:arrow-ipc57"},
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        "//third-party:iceberg",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

(`arrow-ipc57` MUST go in `named_deps` — every existing target that `use`s `arrow_ipc57` wires it that way, e.g. the `iceberg-overwrite` stanza; a plain `deps` entry won't resolve the crate name. Copy the exact `arrow_ipc57`/`iceberg`/`time`/`tempfile` dep target names from the `iceberg-overwrite` stanza — they are authoritative.)

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test //src/control-plane/postgres:iceberg-schema-evolution-land > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\\[" /tmp/t.log`
Expected: FAIL.

- [ ] **Step 3: Implement** `write_parquet_with_schema`, the `append_parquet_snapshot` routing, and `land_additive` (+ helpers) per the Interfaces section. Fill the test bodies.

- [ ] **Step 4: Regenerate `.sqlx` and run the full postgres iceberg suite**

Run: `bash tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; buck2 test //src/control-plane/postgres:iceberg-schema-evolution-land //src/control-plane/postgres:iceberg-catalog //src/control-plane/postgres:iceberg-overwrite //src/control-plane/postgres:iceberg-control-plane //src/control-plane/postgres:sqlx-cache-check > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (new evolution test green; no regression).

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_writer.rs \
        src/control-plane/postgres/src/iceberg_landing.rs \
        src/control-plane/postgres/.sqlx \
        src/control-plane/postgres/BUCK \
        src/control-plane/postgres/tests/iceberg_schema_evolution_land.rs
git commit -m "feat(iceberg): mirror-only additive land (superset parquet + reconcile)"
```

---

### Task 6: Read path — mirror-authoritative superset + null-fill

Make the DataFusion provider use the **mirror** schema as the authoritative table schema (instead of inferring it from Parquet footers) and rely on DataFusion's default schema-adapter to null-fill columns absent from older files. This is what makes a post-evolution read return the superset with `NULL` for pre-evolution rows.

**Files:**
- Modify: `src/services/query-api/src/serving_datafusion.rs` (add `try_new_with_schema` + `arrow_schema_from_mirror`; `register_iceberg_table` → build the schema from `catalog.schema` and use the new constructor). The existing `try_new` and its two test call sites stay as-is.
- Test: `src/services/query-api/tests/iceberg_schema_evolution_read.rs` (new `loom_fixture_test`; model on `tests/iceberg_pruning_e2e.rs`)
- Modify: `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `IcebergCatalog::schema(table, at) -> TableSchema { columns: Vec<ColumnDef { order, name, ty, nullable }> }` (`ty` is the loom logical canonical name; import `control_plane_core::{TableSchema, ColumnDef}`); `resolve_logical`/`BaseType` from `control_plane_core::logical_type` for `String → BaseType`.
- Produces:
  - `IcebergMirrorTableProvider::try_new_with_schema(files: Vec<FileWithStats>, schema: SchemaRef) -> Self` — a **new** sync constructor that stores the mirror-supplied authoritative schema (no footer inference). The existing `pub async fn try_new(ctx, files)` (footer inference) is **left unchanged** so its two standalone-test call sites (`tests/iceberg_pruning_e2e.rs`, `tests/iceberg_mirror_provider.rs`) keep working — only the production `register_iceberg_table` path switches to the mirror schema.
  - A private `fn arrow_schema_from_mirror(cols: &[ColumnDef]) -> Result<SchemaRef, ServingError>` mapping each column to `Field::new(name, data_type, nullable)`.

**`arrow_schema_from_mirror`** — the `BaseType → DataType` mapping (mirror `one_cell` in `serving.rs`):
```rust
use control_plane_core::logical_type::{resolve_logical, BaseType};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

fn base_to_arrow(b: BaseType) -> DataType {
    match b {
        BaseType::Integer => DataType::Int32,
        BaseType::Long => DataType::Int64,
        BaseType::Double => DataType::Float64,
        BaseType::Boolean => DataType::Boolean,
        BaseType::String => DataType::Utf8,
        BaseType::Date => DataType::Date32,
        BaseType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
    }
}

fn arrow_schema_from_mirror(cols: &[control_plane_core::ColumnDef]) -> Result<SchemaRef, ServingError> {
    let fields = cols
        .iter()
        .map(|c| {
            let base = resolve_logical(&c.ty)
                .ok_or_else(|| ServingError::Engine(format!("unknown logical type `{}`", c.ty)))?;
            Ok(Field::new(&c.name, base_to_arrow(base), c.nullable))
        })
        .collect::<Result<Vec<_>, ServingError>>()?;
    Ok(Arc::new(Schema::new(fields)))
}
```
(Confirm `ColumnDef`/`TableSchema` import path — they come from `control_plane_core` via the `Catalog` trait; check `IcebergCatalog::schema`'s return type imports.)

**`try_new_with_schema`** — the new constructor (add alongside the unchanged `try_new`):
```rust
/// Build a provider whose authoritative schema is the mirror's (not inferred from
/// Parquet footers), so an evolved table's superset schema is presented and files
/// missing a newer column are null-filled by DataFusion's default schema adapter.
pub fn try_new_with_schema(files: Vec<FileWithStats>, schema: SchemaRef) -> Self {
    Self { schema, files }
}
```

**`register_iceberg_table`** — build the schema from the mirror and use the new constructor (the two existing `try_new` call sites in tests are untouched):
```rust
let snap = catalog.current_snapshot(table).await.map_err(to_serving)?;
let table_schema = catalog.schema(table, snap.id).await.map_err(to_serving)?;
let schema = arrow_schema_from_mirror(&table_schema.columns)?;
let files_with_stats = catalog.files_with_stats(table, snap.id).await.map_err(to_serving)?;
let file_provider = if files_with_stats.is_empty() {
    None
} else {
    Some(IcebergMirrorTableProvider::try_new_with_schema(files_with_stats, schema.clone()))
};
```

> Risk (spec §Risks): DataFusion's default `SchemaAdapter` must null-fill a table-schema column absent from a given Parquet file. `ParquetSource::new(self.schema)` + `FileScanConfigBuilder` use the default adapter, which null-fills *nullable* missing columns — and `c` is nullable. The happy-path read test is the guard. If a column is **not** null-filled (older DataFusion), the provider must set an explicit `SchemaAdapterFactory` on the `ParquetSource` in `scan()`; only do this if the test proves it necessary. `prune_files(&self.schema, …)` already tolerates a file with no stats for `c` (it is simply never pruned on `c`).

- [ ] **Step 1: Write the failing e2e read test**

```rust
// src/services/query-api/tests/iceberg_schema_evolution_read.rs
//! End-to-end: after an additive land, a current-snapshot read returns the superset
//! schema with the landed value for post-evolution rows and NULL for pre-evolution rows.
//! Lands through `control_plane_postgres::iceberg_landing::land` (inline limit 0 forces
//! Parquet), reads through `register_iceberg_table` + the embedded engine. Setup mirrors
//! tests/iceberg_pruning_e2e.rs + tests/iceberg_overwrite.rs.
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_ipc57::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{Catalog, ColumnSpec, EventType, LineageEvent, RunId, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::land;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use datafusion::prelude::SessionContext;
use query_api::serving::SqlValue;
use query_api::serving_datafusion::{batches_to_rows, register_iceberg_table};

fn col(name: &str, ty: &str, nullable: bool) -> ColumnSpec {
    ColumnSpec { name: name.into(), ty: ty.into(), nullable }
}
fn lineage() -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![], outputs: vec![],
        payload: serde_json::json!({ "source": "read-evolution-test" }),
    }
}
fn encode(schema: &Arc<Schema>, batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    { let mut w = StreamWriter::try_new(&mut buf, schema).unwrap(); w.write(batch).unwrap(); w.finish().unwrap(); }
    buf
}
async fn make_catalog(dsn: String, warehouse: &str) -> SqlCatalog {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(SQL_CATALOG_PROP_WAREHOUSE.to_string(), format!("file://{warehouse}"));
    SqlCatalogBuilder::default().with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props).await.expect("catalog")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_after_additive_land_returns_superset_with_nulls() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().unwrap();
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let pool = fx.pool_for(&db).await;
    let t = TableRef { schema: "s".into(), name: "t".into() };

    // Base land (a long, b string) — ids 0,1.
    let ab = vec![col("a", "long", false), col("b", "string", true)];
    let ab_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false), Field::new("b", DataType::Utf8, true),
    ]));
    let ab_batch = RecordBatch::try_new(ab_schema.clone(), vec![
        Arc::new(Int64Array::from(vec![0i64, 1])), Arc::new(StringArray::from(vec!["x", "y"])),
    ]).unwrap();
    land(&pool, &catalog, &t, &ab, &encode(&ab_schema, &ab_batch), 0, i64::MAX, lineage()).await.expect("base land");

    // Additive land (a long, b string, c long-nullable) — ids 2,3 with c set.
    let abc = vec![col("a", "long", false), col("b", "string", true), col("c", "long", true)];
    let abc_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false), Field::new("b", DataType::Utf8, true), Field::new("c", DataType::Int64, true),
    ]));
    let abc_batch = RecordBatch::try_new(abc_schema.clone(), vec![
        Arc::new(Int64Array::from(vec![2i64, 3])), Arc::new(StringArray::from(vec!["p", "q"])), Arc::new(Int64Array::from(vec![200i64, 300])),
    ]).unwrap();
    land(&pool, &catalog, &t, &abc, &encode(&abc_schema, &abc_batch), 0, i64::MAX, lineage()).await.expect("additive land");

    // Read at the current snapshot through the serving path.
    let cat = IcebergCatalog::new(pool.clone());
    let ctx = SessionContext::new();
    register_iceberg_table(&ctx, &cat, &t).await.expect("register");
    let df = ctx.sql("SELECT a, c FROM \"s\".\"t\" ORDER BY a").await.expect("sql");
    let rows = batches_to_rows(df.collect().await.expect("collect"));

    // Column c is present (superset schema); c is NULL for the pre-evolution rows
    // (a in {0,1}) and the landed value for post-evolution rows (a=2 -> 200, a=3 -> 300).
    assert_eq!(rows.columns, vec!["a".to_string(), "c".to_string()]);
    assert_eq!(rows.rows, vec![
        vec![SqlValue::Int(0), SqlValue::Null],
        vec![SqlValue::Int(1), SqlValue::Null],
        vec![SqlValue::Int(2), SqlValue::Int(200)],
        vec![SqlValue::Int(3), SqlValue::Int(300)],
    ]);
}
```

> Implementer notes: confirm `SqlValue::Null` is the null variant name (grep `enum SqlValue` in `query_api::serving`); adjust the assertion to whatever the actual null variant is. Confirm `batches_to_rows` is exported (it is — `tests/iceberg_pruning_e2e.rs` imports it). The BUCK deps mirror `iceberg-pruning-e2e` plus the `land`-based setup deps.

BUCK target (add to `src/services/query-api/BUCK`):

```python
loom_fixture_test(
    name = "iceberg-schema-evolution-read",
    crate = "iceberg_schema_evolution_read",
    srcs = ["tests/iceberg_schema_evolution_read.rs"],
    crate_root = "tests/iceberg_schema_evolution_read.rs",
    named_deps = {"arrow_ipc57": "//third-party:arrow-ipc57"},
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        "//third-party:datafusion",
        "//third-party:iceberg",
        "//third-party:object_store",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

(`arrow-ipc57` MUST be in `named_deps` (see Task 5 note); `tempfile` is required for `tempfile::tempdir()`. Copy the exact arrow/iceberg/time/tempfile dep spellings from the `iceberg-pruning-e2e` and `iceberg-overwrite` stanzas — they are authoritative.)

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test //src/services/query-api:iceberg-schema-evolution-read > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\\[" /tmp/t.log`
Expected: FAIL.

- [ ] **Step 3: Implement** `arrow_schema_from_mirror` and the new `try_new_with_schema` constructor, then rewire `register_iceberg_table` to use it. **Do NOT change `try_new` or its two existing call sites** (`tests/iceberg_pruning_e2e.rs`, `tests/iceberg_mirror_provider.rs`) — they keep footer inference. The only production change is `register_iceberg_table` switching to `try_new_with_schema`. Fill the test body (already concrete above).

- [ ] **Step 4: Run the query-api iceberg suite**

Run: `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (new read test green; existing serving tests unaffected — they land a single schema, so mirror schema == inferred schema).

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/src/serving_datafusion.rs \
        src/services/query-api/BUCK \
        src/services/query-api/tests/iceberg_schema_evolution_read.rs
git commit -m "feat(query-api): mirror-authoritative iceberg schema with null-fill for evolved tables"
```

---

### Task 7: Full-sweep verification + register update

**Files:**
- Modify: `docs/ROADMAP.md` (close `road-iceberg-schema-evolution`)
- Modify: `docs/FUTURE.md` (record residual deferrals)

- [ ] **Step 1: Full sweep** — `main` must stay green and the lockfile/`.sqlx` must be consistent.

Run: `buck2 test //src/... > /tmp/full.log 2>&1; grep -E "Tests finished|FAIL" /tmp/full.log`
Expected: all PASS. (Per the duckdb lockfile footgun: confirm no unrelated `query-api`/`worker` fixture failures — those would signal a `.sqlx`/dep drift, not this work.)

- [ ] **Step 2: Lint sweep** — `buck2 run //tools:prek -- run --all-files > /tmp/lint.log 2>&1; grep -iE "Failed|error" /tmp/lint.log` and commit any hook fixes.

- [ ] **Step 3: Update the registers** via `loom-docs-update`:
  - In `docs/ROADMAP.md`, flip `road-iceberg-schema-evolution` `- [ ]`→`- [x]`, set `status:done`, add `pr:#<N>` (after the PR is opened).
  - In `docs/FUTURE.md`, record the residual deferrals this slice introduced (cross-link with `[[road-iceberg-schema-evolution]]`):
    - **Additive evolution on the inline path** (currently detect+reject only).
    - **Real Iceberg metadata schema evolution** (`AddSchema`/`SetCurrentSchema` + `field_id` persistence) so external `ATTACH` clients see new columns — the full-evolution slice; rename/drop/retype/reorder ride on it.
    - **Time-travel as-of-schema reconstruction** (`iceberg_read.rs` still builds Arrow from `current_schema()`); `schema_version` is the binding it will resolve against.

- [ ] **Step 4: Commit the docs**

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(registers): close road-iceberg-schema-evolution; record residual deferrals"
```

---

## Notes for the implementer

- **`ProjectedColumn` must derive `Clone` (and `Debug, PartialEq`)** — Task 1 needs it; add the derive in `iceberg_mirror.rs` if absent.
- **Two write paths share one classifier** — never inline the policy at a call site; always go through `reconcile_and_project` (mirror sites) / `classify_schema_change` (inline + landing pre-check). If you find yourself comparing columns by hand anywhere else, stop and reuse the helper.
- **Stamping lives in `register_files`/`write_mirror`** (Task 3), so `land_additive` must *not* stamp again — it commits through `register_files`.
- **The duckdb lockfile footgun:** do not run `reindeer update`/`cargo generate-lockfile`. This work adds no dependency. If the lock changes for any reason, diff it against the merge-base for `libduckdb-sys` and run the full `buck2 test //src/...`.
</content>
</invoke>
