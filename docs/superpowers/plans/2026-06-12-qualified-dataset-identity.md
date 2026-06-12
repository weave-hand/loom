# Qualified Dataset Identity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `DatasetId` — loom's canonical, deployment-independent identity for a governed dataset — and centralize the `TableRef ↔ DatasetRef` conversion that's hand-built (with a hardcoded `"loom-ingest"` namespace) across three call sites today.

**Architecture:** A pure `core/src/identity.rs` newtype `DatasetId(TableRef)` plus a `LOOM_DATASET_NAMESPACE = "loom"` constant and typed `TableRef ↔ DatasetRef` conversions. No new dependencies. Lineage's trait and `DatasetRef` storage are unchanged — `DatasetId` is a construction/bridge helper, so the slice is fully additive.

**Tech Stack:** Rust 2024, buck2. Pure logic, no I/O.

**Design:** `docs/superpowers/specs/2026-06-12-qualified-dataset-identity-design.md`

---

## Context for the implementer (read before starting)

- Tests are integration `rust_test` targets only — never inline `#[cfg(test)]` (a prek hook fails the build). Task 1 adds a new `identity` `rust_test` target; Task 2 edits existing test files.
- Run tests with buck2, redirected to a file then grepped (never pipe to `tail`): `buck2 test //src/control-plane/core:identity > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`.
- rustfmt is CHECK-ONLY: before committing any `.rs`, run `buck2 run //tools:rustfmt -- <files you touched>` and apply the result.
- clippy: `tools/clippy-all.sh` must be clean.
- Commits: Conventional Commits, body ending exactly `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. Do NOT use `--no-verify`. Do NOT switch branches (you are on `feat/qualified-identity-newtype`; verify with `git branch --show-current` before and after).
- `TableRef { pub schema: String, pub name: String }` lives in `core/src/catalog.rs`; `DatasetRef { pub namespace: String, pub name: String }` lives in `core/src/lineage.rs`. Both are `pub` from `control_plane_core`. Both are in `core`, so the conversion impls are not orphan-rule-blocked.

---

## Task 1: `core` — `DatasetId` + namespace + conversions

**Files:**
- Create: `src/control-plane/core/src/identity.rs`
- Modify: `src/control-plane/core/src/lib.rs` (add `mod identity;` + re-export)
- Modify: `src/control-plane/core/BUCK` (new `identity` test target)
- Test: `src/control-plane/core/tests/identity.rs`

Pure logic, no new deps. `DatasetId` wraps `TableRef`; an ontology type reaches its dataset via `ObjectType.table → DatasetId`, so no `Type` variant now.

- [ ] **Step 1: Write the failing tests** — create `src/control-plane/core/tests/identity.rs`:

```rust
use control_plane_core::{DatasetId, DatasetRef, LOOM_DATASET_NAMESPACE, TableRef};

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

#[test]
fn table_maps_to_loom_namespaced_dotted_dataset_ref() {
    let dr = DatasetId::from(&tref("main", "customer")).dataset_ref();
    assert_eq!(
        dr,
        DatasetRef {
            namespace: "loom".into(),
            name: "main.customer".into(),
        }
    );
    assert_eq!(LOOM_DATASET_NAMESPACE, "loom");
}

#[test]
fn dataset_ref_round_trips_for_loom_datasets() {
    let t = tref("analytics", "orders");
    // From<&TableRef> for DatasetRef is the call-site convenience.
    let dr: DatasetRef = (&t).into();
    assert_eq!(DatasetId::from_dataset_ref(&dr), Some(DatasetId::from(&t)));
    assert_eq!(DatasetId::from_dataset_ref(&dr).unwrap().table(), &t);
}

#[test]
fn external_namespaces_are_not_loom_datasets() {
    for ns in ["s3://bucket", "postgres://h:5432", "loom-ingest", ""] {
        let dr = DatasetRef {
            namespace: ns.into(),
            name: "main.customer".into(),
        };
        assert_eq!(DatasetId::from_dataset_ref(&dr), None, "namespace={ns}");
    }
}

#[test]
fn malformed_loom_names_do_not_parse() {
    // no dot, empty sides, empty, and multi-dot (ambiguous) all reject without panic.
    for nm in ["nodot", ".x", "x.", "", "a.b.c"] {
        let dr = DatasetRef {
            namespace: "loom".into(),
            name: nm.into(),
        };
        assert_eq!(DatasetId::from_dataset_ref(&dr), None, "name={nm}");
    }
}
```

- [ ] **Step 2: Add the test target** — in `src/control-plane/core/BUCK`, after the `logical-type` `rust_test` block, add:

```python
rust_test(
    name = "identity",
    crate = "identity",
    srcs = ["tests/identity.rs"],
    crate_root = "tests/identity.rs",
    edition = "2024",
    deps = [":core"],
)
```

- [ ] **Step 3: Run, verify it fails to compile** — `buck2 test //src/control-plane/core:identity > /tmp/t.log 2>&1; grep -E "error\[|Tests finished|FAIL" /tmp/t.log`. Expected: compile error (`DatasetId` / `LOOM_DATASET_NAMESPACE` not found).

- [ ] **Step 4: Implement `src/control-plane/core/src/identity.rs`:**

```rust
//! loom's canonical identity for a dataset it governs — a physical DuckLake table.
//! Bridges catalog `TableRef` and lineage `DatasetRef` so the two stop being joined by
//! hand-built strings. Pure logic, no I/O. See
//! docs/superpowers/specs/2026-06-12-qualified-dataset-identity-design.md.

use crate::catalog::TableRef;
use crate::lineage::DatasetRef;

/// loom's canonical logical namespace for datasets it governs. Deployment-independent:
/// a loom table's logical identity is stable regardless of which Postgres host backs the
/// catalog. External datasets (`s3://bucket`, `postgres://host`) keep their own
/// datasource-derived namespaces and are NOT loom-namespaced.
pub const LOOM_DATASET_NAMESPACE: &str = "loom";

/// loom's canonical identity for a dataset it governs — a physical DuckLake table. The
/// deployment-independent logical identity that bridges catalog `TableRef` and lineage
/// `DatasetRef`. An ontology type reaches its dataset through `ObjectType.table ->
/// DatasetId`; a type-level variant is an additive change if type-level lineage lands.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DatasetId(TableRef);

impl DatasetId {
    /// The physical table this dataset identity refers to.
    pub fn table(&self) -> &TableRef {
        &self.0
    }

    /// The OpenLineage identity for this loom dataset: the loom namespace plus a
    /// dot-qualified `schema.name`.
    pub fn dataset_ref(&self) -> DatasetRef {
        DatasetRef {
            namespace: LOOM_DATASET_NAMESPACE.to_string(),
            name: format!("{}.{}", self.0.schema, self.0.name),
        }
    }

    /// Parse a `DatasetRef` back into a loom `DatasetId`. `None` when the ref is not
    /// loom-namespaced (it names an external dataset) or its name is not a well-formed
    /// `schema.table` (loom identifiers contain no `.`, so a multi-dot name is ambiguous
    /// and rejected rather than mis-parsed).
    pub fn from_dataset_ref(dr: &DatasetRef) -> Option<DatasetId> {
        if dr.namespace != LOOM_DATASET_NAMESPACE {
            return None;
        }
        let (schema, name) = dr.name.split_once('.')?;
        if schema.is_empty() || name.is_empty() || name.contains('.') {
            return None;
        }
        Some(DatasetId(TableRef {
            schema: schema.to_string(),
            name: name.to_string(),
        }))
    }
}

impl From<&TableRef> for DatasetId {
    fn from(table: &TableRef) -> Self {
        DatasetId(table.clone())
    }
}

/// Convenience for call sites that just want the lineage ref for a loom table.
impl From<&TableRef> for DatasetRef {
    fn from(table: &TableRef) -> Self {
        DatasetId::from(table).dataset_ref()
    }
}
```

- [ ] **Step 5: Wire the module + re-export** — in `src/control-plane/core/src/lib.rs`:
  - Add `mod identity;` in the module-declaration block (alphabetically near `mod error;` / `mod lineage;`).
  - Add a re-export line next to the others: `pub use identity::{DatasetId, LOOM_DATASET_NAMESPACE};`

- [ ] **Step 6: Run tests** — `buck2 test //src/control-plane/core:identity > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`. Expected: PASS (4 tests).

- [ ] **Step 7: rustfmt + clippy** — `buck2 run //tools:rustfmt -- src/control-plane/core/src/identity.rs src/control-plane/core/src/lib.rs src/control-plane/core/tests/identity.rs` and apply; then `tools/clippy-all.sh` clean.

- [ ] **Step 8: Commit**

```bash
git add src/control-plane/core/src/identity.rs src/control-plane/core/src/lib.rs src/control-plane/core/BUCK src/control-plane/core/tests/identity.rs
git commit -m "feat(core): DatasetId — typed catalog<->lineage dataset identity

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: adopt `DatasetRef::from(&TableRef)` at the hand-built call sites

**Files:**
- Modify: `src/services/ingest/tests/materialize.rs`
- Modify: `src/services/ingest/tests/ducklake_interop.rs`
- Modify: `src/services/query-api/tests/bind_read_e2e.rs`

Each of these constructs the output lineage `DatasetRef` by hand as `DatasetRef { namespace: "loom-ingest".into(), name: "main.customer".into() }` (or a `format!`-built name). Switch each to the centralized `DatasetRef::from(&table)` — identical name, namespace canonicalized to `"loom"`. No BUCK changes (all three already depend on `//src/control-plane/core:core`, and `DatasetRef`/`TableRef` stay imported).

- [ ] **Step 1: `materialize.rs`** — in the `lineage(t: &TableRef) -> LineageEvent` helper (around line 21), replace the `outputs: vec![DatasetRef { namespace: "loom-ingest".into(), name: <…>.into() }]` expression with:

```rust
        outputs: vec![DatasetRef::from(t)],
```

Then add a test that locks the canonical wire value (so the call site can't silently drift). Add this as a new `#[test]` fn in the file:

```rust
#[test]
fn output_dataset_ref_is_the_canonical_loom_identity() {
    assert_eq!(
        DatasetRef::from(&table()),
        DatasetRef {
            namespace: "loom".into(),
            name: "main.test".into(),
        }
    );
}
```

> NOTE: the literal `name` above must match what `table()` returns. Read the `table()` helper (around line 14) and use its actual `schema`/`name` (e.g. if `table()` is `main`/`test`, the name is `"main.test"`; if it's `main`/`customer`, use `"main.customer"`). Set the literal to match — do not change `table()`.

- [ ] **Step 2: `ducklake_interop.rs`** — replace the `outputs: vec![DatasetRef { namespace: "loom-ingest".into(), name: "main.customer".into() }]` (around line 47) with:

```rust
        outputs: vec![DatasetRef::from(&t)],
```

(`t` is the `TableRef` built around line 23. Confirm the variable name; use whatever the local `TableRef` binding is called.)

- [ ] **Step 3: `bind_read_e2e.rs`** — replace the `outputs: vec![DatasetRef { namespace: "loom-ingest".into(), name: "main.customer".into() }]` (around line 56) with:

```rust
        outputs: vec![DatasetRef::from(&table)],
```

(`table` is the `TableRef` built around line 30.)

- [ ] **Step 4: Build + run the affected tests** — these are fixture tests (boot postgres/duckdb; slow first build, be patient):

```bash
buck2 test //src/services/ingest:materialize //src/services/ingest:ducklake-interop //src/services/query-api:bind-read-e2e > /tmp/t.log 2>&1
grep -E "Tests finished|FAIL|panicked" /tmp/t.log
```

Expected: PASS. The `DatasetRef` values are identical to before except the namespace is now `"loom"`; nothing asserts on the old `"loom-ingest"` string, so behavior is unchanged.

- [ ] **Step 5: rustfmt + clippy** — `buck2 run //tools:rustfmt -- src/services/ingest/tests/materialize.rs src/services/ingest/tests/ducklake_interop.rs src/services/query-api/tests/bind_read_e2e.rs` and apply; `tools/clippy-all.sh` clean (watch for a now-unused `DatasetRef` import — it should still be used via `DatasetRef::from`, so no change expected, but fix if clippy flags it).

- [ ] **Step 6: Full sweep** — `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`. Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/services/ingest/tests/materialize.rs src/services/ingest/tests/ducklake_interop.rs src/services/query-api/tests/bind_read_e2e.rs
git commit -m "refactor(ingest,query-api): build output DatasetRef via DatasetId conversion

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: roadmap + critical-review status note

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-control-plane-critical-review.md`

- [ ] **Step 1: Flip the qualified-identity finding to done.** This slice resolves the lone open high-severity item. Update its `Update 2026-06-12 — ❌ Open` callouts (Coupling [H] #1 and five-things #4) to note `DatasetId` landed: a typed `core` newtype + `LOOM_DATASET_NAMESPACE` centralizing the catalog↔lineage conversion (referential validation still deferred; property-name identity still a follow-up). Match the existing callout style. Keep the file passing the markdown hooks (exactly one trailing newline, no trailing whitespace).

- [ ] **Step 2: prek** — `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -E "Passed|Failed" /tmp/p.log`. Expected: all Passed.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-06-06-control-plane-critical-review.md
git commit -m "docs(control-plane): mark qualified-identity finding resolved (DatasetId)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-review notes (spec coverage)

- **`DatasetId` newtype + `LOOM_DATASET_NAMESPACE` + conversions** — Task 1, with the full round-trip/`None`-cases matrix.
- **Centralized `TableRef ↔ DatasetRef`** — Task 1 (`From<&TableRef> for DatasetRef`, `from_dataset_ref`).
- **Adoption at the three hand-built sites + canonical-value lock** — Task 2.
- **Additive / non-breaking** — lineage trait + `DatasetRef` storage untouched; only construction centralized.
- **Deferred (per spec):** referential validation, property/column identity, type-level `DatasetId` enum, `DatasetRef` storage migration — none touched.
- **Review bookkeeping** — Task 3 flips the resolved finding.
