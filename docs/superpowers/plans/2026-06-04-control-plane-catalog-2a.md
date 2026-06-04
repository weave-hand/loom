# Control-Plane Catalog — Phase 2a (trait + fake + contract) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish loom's read-only `Catalog` surface over DuckLake — the trait + domain types in `core`, a backend-agnostic `catalog_contract` driven by a test-only `CatalogSeed` seam, and an in-memory fake adapter that passes it. Zero DuckDB; fully hermetic. (The real-DuckLake Postgres adapter is Phase 2b, designed in the spec but not built here.)

**Architecture:** `Catalog` answers four read questions (current snapshot, snapshot history, files at a snapshot, schema at a snapshot) over DuckLake's catalog-global, MVCC `begin_snapshot`/`end_snapshot` model. Because loom never *writes* the catalog, the contract has no trait method to arrange state, so it seeds through a separate test-only `CatalogSeed` trait. Crucially, the contract asserts against the **snapshot ids the seeder reports back**, never hard-coded counts — so the identical suite is both backend-agnostic and fidelity-safe (in 2b the pg seeder reports DuckLake's real ids).

**Tech Stack:** Rust 2024, buck2, `async_trait`, `time`, `tokio` (tests). No new third-party crates (async-trait/time/tokio already vendored).

---

## Background for the implementer

loom's control plane is a family of workspace crates under `src/control-plane/`: `core` (traits + domain types + `ControlPlaneError`, **runtime-free**), `testkit` (backend-agnostic contract suites), `memory` (in-memory fake), `postgres` (sqlx adapter). Phase 1 built the `Queue` concern across all of them. This is Phase 2 (catalog), cycle 2a.

Read the design first: `docs/superpowers/specs/2026-06-04-control-plane-catalog-design.md` (especially "Why a seeding seam", "The DuckLake catalog schema we read", and "Trait surface"). Everything you need is restated below.

Key facts:
- **`core` error model** (`src/control-plane/core/src/error.rs`): `ControlPlaneError` (a `thiserror` enum with a `NotFound(String)` variant) and `pub type Result<T> = std::result::Result<T, ControlPlaneError>`. Reuse `ControlPlaneError::NotFound` for missing tables/snapshots. Contract assertions check the **variant**, never the message.
- **`core` module layout** (`src/control-plane/core/src/lib.rs`): modules `error`, `queue`, `transaction`, re-exported via `pub use`. You will add a `catalog` module the same way.
- **`core` is runtime-free** — no tokio. The `Catalog` trait uses `#[async_trait]` (already a core dep) and `time::OffsetDateTime` (already a core dep). Do not add tokio to core.
- **DuckLake catalog model (what the fake mimics):** snapshots are catalog-global, identified by a monotonic `i64` (`SnapshotId`). Tables/files/columns carry a `begin_snapshot` and optional `end_snapshot`; a row is *live at* snapshot `s` when `begin <= s && (end is None || end > s)`. The minimal seeder is append-only (no deletes), so `end` is always `None` here, but implement the range check properly anyway (2b and future evolution rely on it).
- **`MemoryControlPlane`** (`src/control-plane/memory/src/lib.rs`) is the in-memory fake; it already impls `Queue` and `ControlPlane` and holds `rows`/`notify`/`lock_timeout`. You will give it a catalog store and impl `Catalog` for it (same pattern as `Queue` — the fake aggregates concerns; no `.catalog()` accessor needed yet).
- **Formatting/lint:** run `cargo fmt --all` before committing (the rustfmt hook checks, doesn't fix). Lint with `tools/clippy-all.sh` — must be clean. Use `is_none_or(...)` rather than `map_or(true, ...)`.
- **buck2:** `buck2 build //src/control-plane/...`; test with `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/...` (`--local-only` is always safe).
- **No new third-party crates** in 2a, so **no reindeer/buckify run**. testkit gains an `async-trait` dependency, but `//third-party:async-trait` already exists (core/memory/postgres use it) — you only add it to testkit's `Cargo.toml` + `BUCK`, which does not change `third-party/BUCK` (so `reindeer-check` stays green).
- Commit style: Conventional Commits; end the body with exactly:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

The domain model, fake, seam, and contract below were validated by a throwaway prototype before this plan was written.

---

## File Structure

**Task 1 — trait, domain types, seam, contract**
- Create: `src/control-plane/core/src/catalog.rs` — domain types + `Catalog` trait.
- Modify: `src/control-plane/core/src/lib.rs` — add `mod catalog;` + re-export.
- Modify: `src/control-plane/testkit/src/lib.rs` — `CatalogSeed` seam + `catalog_contract`.
- Modify: `src/control-plane/testkit/Cargo.toml`, `src/control-plane/testkit/BUCK` — add `async-trait`.

**Task 2 — memory fake adapter + green contract**
- Modify: `src/control-plane/memory/src/lib.rs` — catalog store, `Catalog` impl, inherent `seed_catalog`.
- Create: `src/control-plane/memory/tests/catalog.rs` — wire the contract via a local `CatalogSeed` newtype.
- Modify: `src/control-plane/memory/BUCK` — add the `catalog` rust_test target.

---

## Task 1: Catalog trait, domain types, seeding seam, contract

**Files:**
- Create: `src/control-plane/core/src/catalog.rs`
- Modify: `src/control-plane/core/src/lib.rs`
- Modify: `src/control-plane/testkit/src/lib.rs`, `src/control-plane/testkit/Cargo.toml`, `src/control-plane/testkit/BUCK`

- [ ] **Step 1: Write the `core` domain types + `Catalog` trait**

Create `src/control-plane/core/src/catalog.rs`:

```rust
//! The catalog concern: a read-only view over DuckLake's catalog (`ducklake.*`).
//! loom reads the catalog; DuckLake (the DuckDB client) writes it. Snapshots are
//! catalog-global and identified by a monotonic id; tables/files/columns are
//! versioned by `begin`/`end` snapshot ranges (MVCC), so reads are "this table
//! *at* that snapshot".

use async_trait::async_trait;
use time::OffsetDateTime;

use crate::error::Result;

/// A DuckLake catalog-global snapshot id (monotonic).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SnapshotId(pub i64);

/// A `schema.table` reference within the DuckLake catalog.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TableRef {
    pub schema: String,
    pub name: String,
}

/// A point-in-time version of the catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub id: SnapshotId,
    pub time: OffsetDateTime,
    pub schema_version: i64,
}

/// A Parquet file backing a table at a snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRef {
    pub path: String,
    pub record_count: i64,
    pub file_size_bytes: i64,
}

/// One column of a table's schema at a snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    pub order: i64,
    pub name: String,
    /// DuckLake's column type string, kept opaque (typing is an ontology concern).
    pub ty: String,
    pub nullable: bool,
}

/// A table's column schema at a snapshot, in column order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchema {
    pub columns: Vec<ColumnDef>,
}

#[async_trait]
pub trait Catalog {
    /// The latest snapshot at which `table` is live. `NotFound` if the table does
    /// not exist at the catalog's current snapshot.
    async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot>;
    /// All snapshots at which `table` is live, oldest first. `NotFound` if the
    /// table never existed.
    async fn snapshots(&self, table: &TableRef) -> Result<Vec<Snapshot>>;
    /// The Parquet files live for `table` at snapshot `at`. `NotFound` if the
    /// table is not live at `at`.
    async fn files(&self, table: &TableRef, at: SnapshotId) -> Result<Vec<FileRef>>;
    /// `table`'s column schema at snapshot `at`, in column order. `NotFound` if
    /// the table is not live at `at`.
    async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema>;
}
```

- [ ] **Step 2: Register and export the module**

In `src/control-plane/core/src/lib.rs`, add `mod catalog;` alongside the other modules and re-export its public items. Match the existing style (read the file first). The result should look like:

```rust
mod catalog;
mod error;
mod queue;
mod transaction;

pub use catalog::{Catalog, ColumnDef, FileRef, Snapshot, SnapshotId, TableRef, TableSchema};
pub use error::{ControlPlaneError, Result};
pub use queue::{Job, JobFailure, JobId, NewJob, Queue, RetryPolicy};
pub use transaction::{ControlPlane, Tx};
```

- [ ] **Step 3: Build core to confirm the trait compiles**

```bash
env -u BUCK_PREFER_REMOTE buck2 build --local-only //src/control-plane/core:core
```
Expected: builds clean. (No test yet — the contract comes next and only runs once the fake exists in Task 2.)

- [ ] **Step 4: Add `async-trait` to testkit**

In `src/control-plane/testkit/Cargo.toml`, add to `[dependencies]`:
```toml
async-trait = "0.1"
```
In `src/control-plane/testkit/BUCK`, add `"//third-party:async-trait",` to the `rust_library(name = "testkit")` `deps` (keep it first / alphabetical with the others).

- [ ] **Step 5: Write the `CatalogSeed` seam + `catalog_contract`**

In `src/control-plane/testkit/src/lib.rs`, add (the file already imports from `control_plane_core` and uses `async`/`assert` patterns; add the imports you need — `Catalog`, `SnapshotId`, `TableRef`, etc.):

```rust
use async_trait::async_trait;
use control_plane_core::{Catalog, SnapshotId, TableRef};

/// A column to create in a seeded table.
pub struct SeedColumn {
    pub name: String,
    pub ty: String,
    pub nullable: bool,
}

/// A request to arrange catalog state: create `table` with `columns`, then apply
/// each entry of `row_batches` as its own snapshot adding one data file of that
/// many rows.
pub struct SeedSpec {
    pub table: TableRef,
    pub columns: Vec<SeedColumn>,
    pub row_batches: Vec<usize>,
}

/// A snapshot produced by seeding one batch.
#[derive(Clone, Copy, Debug)]
pub struct SeededSnapshot {
    pub snapshot: SnapshotId,
    pub files_added: usize,
}

/// Test-only seam for arranging catalog state. Each backend implements it
/// differently (the fake builds its state directly; the pg adapter drives real
/// DuckLake). Never referenced by production code.
#[async_trait]
pub trait CatalogSeed {
    /// Create the table if absent and apply each row-batch as its own snapshot.
    /// Returns the per-batch snapshots, in order.
    async fn seed(&self, spec: SeedSpec) -> Vec<SeededSnapshot>;
}

/// Contract for the `Catalog` read surface. `catalog` and `seeder` may be the
/// same backend behind two handles. Assertions key off the snapshot ids the
/// seeder reports, so this suite is backend-agnostic and fidelity-safe.
pub async fn catalog_contract<C, S>(catalog: &C, seeder: &S)
where
    C: Catalog,
    S: CatalogSeed,
{
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
    assert_eq!(seeded.len(), 2, "two batches => two snapshots");

    // current_snapshot is the last batch's snapshot.
    let cur = catalog.current_snapshot(&t).await.expect("current_snapshot");
    assert_eq!(
        cur.id, seeded[1].snapshot,
        "current is the latest seeded snapshot"
    );

    // files: one live at the first batch, two by the second (begin_snapshot range).
    assert_eq!(
        catalog.files(&t, seeded[0].snapshot).await.unwrap().len(),
        1,
        "one file live at the first batch"
    );
    assert_eq!(
        catalog.files(&t, seeded[1].snapshot).await.unwrap().len(),
        2,
        "two files live by the second batch"
    );

    // snapshots: ascending history, includes both batch snapshots, ends at current.
    let hist = catalog.snapshots(&t).await.unwrap();
    assert!(
        hist.windows(2).all(|w| w[0].id < w[1].id),
        "snapshots are ascending"
    );
    assert!(
        hist.iter().any(|s| s.id == seeded[0].snapshot),
        "history includes the first batch snapshot"
    );
    assert_eq!(
        hist.last().unwrap().id,
        cur.id,
        "history ends at the current snapshot"
    );

    // schema at current: the two columns, in order, with type/nullability.
    let sch = catalog.schema(&t, cur.id).await.unwrap();
    assert_eq!(
        sch.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        vec!["id", "name"],
        "columns in order"
    );
    assert_eq!(sch.columns[1].ty, "VARCHAR");
    assert!(
        sch.columns[1].nullable && !sch.columns[0].nullable,
        "nullability preserved"
    );

    // a table that never existed -> NotFound (variant, not message).
    let missing = TableRef {
        schema: "main".into(),
        name: "nope".into(),
    };
    assert!(
        matches!(
            catalog.current_snapshot(&missing).await,
            Err(control_plane_core::ControlPlaneError::NotFound(_))
        ),
        "missing table current_snapshot is NotFound"
    );
    assert!(
        matches!(
            catalog.snapshots(&missing).await,
            Err(control_plane_core::ControlPlaneError::NotFound(_))
        ),
        "missing table snapshots is NotFound"
    );
}
```

- [ ] **Step 6: Build testkit**

```bash
env -u BUCK_PREFER_REMOTE buck2 build --local-only //src/control-plane/testkit:testkit
```
Expected: builds clean (the contract references only `core` traits; it runs in Task 2).

- [ ] **Step 7: Format, lint, commit**

```bash
cargo fmt --all
tools/clippy-all.sh
git add -A
git commit -m "feat(control-plane): add Catalog trait + catalog_contract seam (no adapter yet)"
```
(Append the Co-Authored-By trailer.)

---

## Task 2: In-memory fake adapter + green contract

**Files:**
- Modify: `src/control-plane/memory/src/lib.rs`
- Create: `src/control-plane/memory/tests/catalog.rs`
- Modify: `src/control-plane/memory/BUCK`

- [ ] **Step 1: Write the failing contract test (memory)**

Create `src/control-plane/memory/tests/catalog.rs`. A local newtype implements `CatalogSeed` (orphan rule: the trait is foreign but the newtype is local to this test crate), delegating to an inherent `seed_catalog` you add to the fake in Step 2:

```rust
use async_trait::async_trait;
use control_plane_memory::MemoryControlPlane;
use control_plane_testkit::{CatalogSeed, SeedSpec, SeededSnapshot, catalog_contract};

/// Adapts the testkit `CatalogSeed` seam to the fake's inherent seeding method.
struct MemSeeder<'a>(&'a MemoryControlPlane);

#[async_trait]
impl CatalogSeed for MemSeeder<'_> {
    async fn seed(&self, spec: SeedSpec) -> Vec<SeededSnapshot> {
        let cols: Vec<(String, String, bool)> = spec
            .columns
            .into_iter()
            .map(|c| (c.name, c.ty, c.nullable))
            .collect();
        self.0
            .seed_catalog(&spec.table, &cols, &spec.row_batches)
            .into_iter()
            .map(|snapshot| SeededSnapshot {
                snapshot,
                files_added: 1,
            })
            .collect()
    }
}

#[tokio::test]
async fn memory_passes_catalog_contract() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    catalog_contract(&cp, &MemSeeder(&cp)).await;
}
```

- [ ] **Step 2: Run it to confirm it fails (no `Catalog` impl / `seed_catalog` yet)**

```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:catalog 2>&1 | tail -20
```
Expected: build failure — `seed_catalog` and the `Catalog` impl don't exist yet, and the `catalog` test target isn't defined (you add it in Step 4). This step just confirms the test is wired to fail first; if the target genuinely can't be referenced yet, proceed to Step 3–4 and treat Step 5 as the real red→green run.

- [ ] **Step 3: Implement the catalog store, `Catalog` impl, and inherent seeding on the fake**

In `src/control-plane/memory/src/lib.rs`, add the catalog state and implementation. Add imports as needed (`HashMap`, the catalog types from `control_plane_core`).

Add near the top (with the other `use`s):
```rust
use std::collections::HashMap;

use control_plane_core::{
    Catalog, ColumnDef, ControlPlaneError, FileRef, Snapshot, SnapshotId, TableRef, TableSchema,
};
```

Add the catalog state types (module-level):
```rust
/// An MVCC-versioned catalog row: live at snapshot `s` when `begin <= s` and
/// (`end` is None or `end > s`).
#[derive(Clone)]
struct Versioned<T> {
    begin: i64,
    end: Option<i64>,
    val: T,
}

impl<T> Versioned<T> {
    fn live_at(&self, s: i64) -> bool {
        self.begin <= s && self.end.is_none_or(|e| e > s)
    }
}

#[derive(Default)]
struct CatalogState {
    next_snapshot: i64,
    snapshots: Vec<Snapshot>,
    tables: HashMap<(String, String), Versioned<()>>,
    columns: HashMap<(String, String), Vec<Versioned<ColumnDef>>>,
    files: HashMap<(String, String), Vec<Versioned<FileRef>>>,
}
```

Add a `catalog` field to `MemoryControlPlane` and construct it in `new`:
```rust
#[derive(Clone)]
pub struct MemoryControlPlane {
    rows: Arc<Mutex<Vec<Row>>>,
    notify: Arc<Notify>,
    catalog: Arc<Mutex<CatalogState>>,
    lock_timeout: Duration,
}
```
```rust
    pub fn new(lock_timeout: Duration) -> Self {
        Self {
            rows: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(Notify::new()),
            catalog: Arc::new(Mutex::new(CatalogState::default())),
            lock_timeout,
        }
    }
```

Add the inherent seeding method (test-support; uses only core/std types so `memory` needs no `testkit` dependency):
```rust
impl MemoryControlPlane {
    /// Test-support: create `table` (if absent) with `columns` as
    /// `(name, type, nullable)`, then apply each entry of `batches` as its own
    /// snapshot adding one data file of that many rows. Returns the per-batch
    /// snapshot ids, in order. Append-only.
    pub fn seed_catalog(
        &self,
        table: &TableRef,
        columns: &[(String, String, bool)],
        batches: &[usize],
    ) -> Vec<SnapshotId> {
        let mut cat = self.catalog.lock().unwrap();
        let key = (table.schema.clone(), table.name.clone());

        if !cat.tables.contains_key(&key) {
            let s = cat.new_snapshot();
            cat.tables.insert(
                key.clone(),
                Versioned {
                    begin: s,
                    end: None,
                    val: (),
                },
            );
            let cols = columns
                .iter()
                .enumerate()
                .map(|(i, (name, ty, nullable))| Versioned {
                    begin: s,
                    end: None,
                    val: ColumnDef {
                        order: i as i64,
                        name: name.clone(),
                        ty: ty.clone(),
                        nullable: *nullable,
                    },
                })
                .collect();
            cat.columns.insert(key.clone(), cols);
        }

        let mut out = Vec::new();
        for (i, n) in batches.iter().enumerate() {
            let s = cat.new_snapshot();
            let file = FileRef {
                path: format!("data/{}_{}.parquet", table.name, i),
                record_count: *n as i64,
                file_size_bytes: (*n as i64) * 16,
            };
            cat.files.entry(key.clone()).or_default().push(Versioned {
                begin: s,
                end: None,
                val: file,
            });
            out.push(SnapshotId(s));
        }
        out
    }
}
```

Add the `CatalogState` helper and the `Catalog` impl:
```rust
impl CatalogState {
    fn new_snapshot(&mut self) -> i64 {
        let id = self.next_snapshot;
        self.next_snapshot += 1;
        self.snapshots.push(Snapshot {
            id: SnapshotId(id),
            time: OffsetDateTime::now_utc(),
            schema_version: 0,
        });
        id
    }

    fn latest_live(&self, key: &(String, String)) -> Option<i64> {
        let t = self.tables.get(key)?;
        self.snapshots
            .iter()
            .rev()
            .map(|s| s.id.0)
            .find(|&s| t.live_at(s))
    }
}

#[async_trait]
impl Catalog for MemoryControlPlane {
    async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot> {
        let cat = self.catalog.lock().unwrap();
        let key = (table.schema.clone(), table.name.clone());
        let s = cat
            .latest_live(&key)
            .ok_or_else(|| ControlPlaneError::NotFound(format!("{}.{}", table.schema, table.name)))?;
        Ok(cat.snapshots.iter().find(|sn| sn.id.0 == s).cloned().unwrap())
    }

    async fn snapshots(&self, table: &TableRef) -> Result<Vec<Snapshot>> {
        let cat = self.catalog.lock().unwrap();
        let key = (table.schema.clone(), table.name.clone());
        let t = cat
            .tables
            .get(&key)
            .ok_or_else(|| ControlPlaneError::NotFound(format!("{}.{}", table.schema, table.name)))?;
        Ok(cat
            .snapshots
            .iter()
            .filter(|sn| t.live_at(sn.id.0))
            .cloned()
            .collect())
    }

    async fn files(&self, table: &TableRef, at: SnapshotId) -> Result<Vec<FileRef>> {
        let cat = self.catalog.lock().unwrap();
        let key = (table.schema.clone(), table.name.clone());
        let t = cat
            .tables
            .get(&key)
            .filter(|t| t.live_at(at.0))
            .ok_or_else(|| {
                ControlPlaneError::NotFound(format!("{}.{} @ {}", table.schema, table.name, at.0))
            })?;
        let _ = t;
        Ok(cat
            .files
            .get(&key)
            .into_iter()
            .flatten()
            .filter(|f| f.live_at(at.0))
            .map(|f| f.val.clone())
            .collect())
    }

    async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema> {
        let cat = self.catalog.lock().unwrap();
        let key = (table.schema.clone(), table.name.clone());
        let t = cat
            .tables
            .get(&key)
            .filter(|t| t.live_at(at.0))
            .ok_or_else(|| {
                ControlPlaneError::NotFound(format!("{}.{} @ {}", table.schema, table.name, at.0))
            })?;
        let _ = t;
        let mut cols: Vec<ColumnDef> = cat
            .columns
            .get(&key)
            .into_iter()
            .flatten()
            .filter(|c| c.live_at(at.0))
            .map(|c| c.val.clone())
            .collect();
        cols.sort_by_key(|c| c.order);
        Ok(TableSchema { columns: cols })
    }
}
```
Note: `OffsetDateTime` is already imported in `memory/src/lib.rs` (used by the queue rows). If clippy flags the `let _ = t;` lines as awkward, replace the `.filter(...).ok_or_else(...)?; let _ = t;` pattern with an explicit liveness check, e.g.:
```rust
        let key = (table.schema.clone(), table.name.clone());
        let live = cat.tables.get(&key).is_some_and(|t| t.live_at(at.0));
        if !live {
            return Err(ControlPlaneError::NotFound(format!(
                "{}.{} @ {}", table.schema, table.name, at.0
            )));
        }
```
Prefer whichever reads cleanly and passes clippy.

- [ ] **Step 4: Add the memory `catalog` test target**

In `src/control-plane/memory/BUCK`, add a second `rust_test` (mirroring the existing `queue` one):
```python
rust_test(
    name = "catalog",
    crate = "catalog",
    srcs = ["tests/catalog.rs"],
    crate_root = "tests/catalog.rs",
    edition = "2024",
    deps = [
        ":memory",
        "//src/control-plane/testkit:testkit",
        "//third-party:async-trait",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 5: Run the contract — red→green**

```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:catalog
```
Expected: `memory_passes_catalog_contract` passes.

- [ ] **Step 6: Full build, test, format, lint**

```bash
cargo fmt --all
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/...
tools/clippy-all.sh
```
Expected: the whole control plane builds and passes (queue + worker + the new catalog test); clippy clean.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(control-plane): in-memory Catalog fake passing the catalog contract"
```
(Append the Co-Authored-By trailer.)

---

## Final verification (after both tasks)

```bash
cargo fmt --all --check
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...
tools/clippy-all.sh
buck2 run //tools:prek -- run --all-files
```

All green ⇒ ready to push and open a PR. CI's `build-test` (on merge to `main`) re-runs the suite on the non-root runner.

## Notes / gotchas

- **The contract keys off seeder-reported snapshot ids, never counts.** This is deliberate: it keeps the suite backend-agnostic and makes 2b's real-DuckLake seeder a drop-in (it reports DuckLake's actual ids). Do not "simplify" the contract to assert fixed snapshot numbers.
- **`memory` must not depend on `testkit`.** The seeding seam (`CatalogSeed`/`SeedSpec`) lives in `testkit`; the fake exposes an inherent `seed_catalog` using only core/std types, and the `CatalogSeed` glue lives in `memory/tests/catalog.rs` via a local newtype (orphan rule satisfied). Keep that direction.
- **Implement the full `begin/end` range check** even though the append-only seeder never sets `end` — 2b and future schema-evolution reads depend on it, and it costs nothing now.
- **`core` stays runtime-free** — `catalog.rs` uses only `async_trait` + `time`, no tokio.
```
