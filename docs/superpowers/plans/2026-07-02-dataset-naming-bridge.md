# TableRef/TypeName → DatasetRef naming bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a deployment-aware naming bridge that maps loom's `TableRef`/`TypeName` to OpenLineage-conformant `DatasetRef`s (forward) and resolves any `DatasetRef` back to the loom object it names or `External` (reverse), as a total never-erroring function.

**Architecture:** A new postgres-free services crate `//src/services/lineage-naming` holding a plain value struct `LineageNaming` built from the deployment's `ObjectStoreConfig`. Forward methods reuse `core`'s canonical `schema.table` name formatting and swap in a storage-derived namespace; the reverse `resolve` delegates name-parsing to `core`'s existing `DatasetId`/`TypeId::from_dataset_ref` guards (recognizing both the logical `"loom"`/`"loom:type"` namespaces and this deployment's storage namespace), classifying everything else as `External`.

**Tech Stack:** Rust (edition 2024), buck2, `control_plane_core` (identity/lineage/catalog/ontology types), `store_config` (`ObjectStoreConfig`).

## Global Constraints

- **No inline tests.** Tests live in a sibling `tests/<name>.rs` file wired as its own `rust_test` target — never `#[cfg(test)] mod tests`. (The `no-inline-tests` prek hook fails on any `#[test]`/`#[tokio::test]` in `src/**`.)
- **Use the `loom_rust_test` wrapper** for the test target — `load("//src:loom_test.bzl", "rust_test")` — so tests get the panic-safety lint exemptions.
- **This crate is pure logic, no fixtures** — its `rust_test` runs on remote execution (a bare `rust_test` via the wrapper, NOT `loom_fixture_test`).
- **No `Cargo.toml`.** First-party pure crates (like `store-config`) build straight from BUCK with no manifest; do not add one. Do not run reindeer/buckify.
- **Strict clippy** (pedantic + restriction enabled as groups). The enforced panic-safety lints (`unwrap_used`, `expect_used`, `indexing_slicing`, `panic`, `todo`, `get_unwrap`, `map_err_ignore`) must not be tripped in `src/`. This code needs none of them. `must_use_candidate` is allowed, so no `#[must_use]` needed.
- **Crate name / target name:** buck target `lineage-naming`, Rust crate `lineage_naming`, at `//src/services/lineage-naming`.
- **Conventional Commits** are enforced on commit messages (`feat:`/`test:`/`docs:` …).

**Reference — reused `core` API (crate `control_plane_core`, all re-exported at crate root):**
- `TableRef { pub schema: String, pub name: String }`
- `TypeName(pub String)`
- `DatasetRef { pub namespace: String, pub name: String }`
- `LOOM_DATASET_NAMESPACE: &str = "loom"`, `LOOM_TYPE_NAMESPACE: &str = "loom:type"`
- `DatasetId`: `From<&TableRef>`, `.dataset_ref() -> DatasetRef` (namespace `"loom"`, name `"schema.name"`), `.table() -> &TableRef`, `DatasetId::from_dataset_ref(&DatasetRef) -> Option<DatasetId>` (guards: namespace == `"loom"`, name splits once on `.` into non-empty `schema`/`name` with no extra `.`).
- `TypeId`: `From<&TypeName>`, `.dataset_ref() -> DatasetRef` (namespace `"loom:type"`, name is the bare type name), `.type_name() -> &TypeName`, `TypeId::from_dataset_ref(&DatasetRef) -> Option<TypeId>` (guards: namespace == `"loom:type"`, name non-empty).

**Reference — reused `store_config` API (crate `store_config`):**
- `ObjectStoreConfig { pub warehouse_uri: String, pub backend: ObjectStoreBackend }`
- `enum ObjectStoreBackend { Local, S3(S3Backend) }`
- `struct S3Backend { pub bucket: String, .. }`
- `ObjectStoreConfig::for_s3_test(warehouse_uri, bucket, endpoint, access_key_id, secret_access_key) -> ObjectStoreConfig` (test helper).

---

## File structure

- `src/services/lineage-naming/BUCK` — the `rust_library` target `lineage-naming` and the `rust_test` target `naming`.
- `src/services/lineage-naming/src/lib.rs` — `LineageNaming`, `ResolvedDataset`, and the four methods.
- `src/services/lineage-naming/tests/naming.rs` — the full forward + reverse + round-trip test matrix.

---

### Task 1: Crate scaffold — forward mapping (`dataset_ref` / `type_ref`)

Creates the crate, the `LineageNaming` struct with its private `site_namespace`, the `ResolvedDataset` enum, `from_object_store`, and the two forward methods. `resolve` is deferred to Task 2 (added there with its own tests). Deliverable: the crate builds and the forward test matrix passes.

**Files:**
- Create: `src/services/lineage-naming/src/lib.rs`
- Create: `src/services/lineage-naming/BUCK`
- Create (test): `src/services/lineage-naming/tests/naming.rs`

**Interfaces:**
- Consumes: `control_plane_core::{DatasetRef, TableRef, TypeName, DatasetId, TypeId}` (`LOOM_DATASET_NAMESPACE` is added by Task 2 with `resolve`, not here); `store_config::{ObjectStoreConfig, ObjectStoreBackend}` (the `S3Backend` payload is bound via the `ObjectStoreBackend::S3(s)` match arm — no need to name the type in a `use`). Follow the exact `use` lines in the code blocks below, not this prose summary.
- Produces (relied on by Task 2 and downstream `road-lineage-acl-filtering`):
  - `pub struct LineageNaming { site_namespace: String }`
  - `pub enum ResolvedDataset { Table(TableRef), Type(TypeName), External(DatasetRef) }` — derives `Clone, Debug, PartialEq, Eq`.
  - `LineageNaming::from_object_store(cfg: &ObjectStoreConfig) -> LineageNaming`
  - `LineageNaming::dataset_ref(&self, table: &TableRef) -> DatasetRef`
  - `LineageNaming::type_ref(&self, ty: &TypeName) -> DatasetRef`

- [ ] **Step 1: Write the BUCK file**

Create `src/services/lineage-naming/BUCK`:

```python
load("//src:loom_test.bzl", "rust_test")
load("@prelude//rust:cargo_package.bzl", "cargo")

cargo.rust_library(
    name = "lineage-naming",
    crate = "lineage_naming",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [
        "//src/control-plane/core:core",
        "//src/services/store-config:store-config",
    ],
    visibility = ["PUBLIC"],
)

rust_test(
    name = "naming",
    crate = "naming",
    srcs = ["tests/naming.rs"],
    crate_root = "tests/naming.rs",
    edition = "2024",
    deps = [
        ":lineage-naming",
        "//src/control-plane/core:core",
        "//src/services/store-config:store-config",
    ],
)
```

- [ ] **Step 2: Write the failing forward tests**

Create `src/services/lineage-naming/tests/naming.rs`:

```rust
//! Forward + reverse naming-bridge tests for `lineage_naming`. Pure logic; no fixture.

use control_plane_core::{DatasetRef, TableRef, TypeName};
use lineage_naming::{LineageNaming, ResolvedDataset};
use store_config::{ObjectStoreBackend, ObjectStoreConfig};

fn s3_naming() -> LineageNaming {
    // Warehouse URI carries a key prefix; the derived namespace must drop it and
    // keep only the `s3://<bucket>` datasource authority.
    let cfg = ObjectStoreConfig::for_s3_test(
        "s3://bucket/warehouse/prefix".to_string(),
        "bucket".to_string(),
        "http://localhost:9000".to_string(),
        "ak".to_string(),
        "sk".to_string(),
    );
    LineageNaming::from_object_store(&cfg)
}

fn file_naming() -> LineageNaming {
    let cfg = ObjectStoreConfig {
        warehouse_uri: "file:///var/lib/loom/warehouse".to_string(),
        backend: ObjectStoreBackend::Local,
    };
    LineageNaming::from_object_store(&cfg)
}

fn table(schema: &str, name: &str) -> TableRef {
    TableRef { schema: schema.to_string(), name: name.to_string() }
}

#[test]
fn forward_s3_table_uses_storage_namespace_and_dotted_name() {
    let n = s3_naming();
    assert_eq!(
        n.dataset_ref(&table("main", "orders")),
        DatasetRef { namespace: "s3://bucket".to_string(), name: "main.orders".to_string() }
    );
}

#[test]
fn forward_s3_type_stays_on_logical_type_namespace() {
    let n = s3_naming();
    assert_eq!(
        n.type_ref(&TypeName("Customer".to_string())),
        DatasetRef { namespace: "loom:type".to_string(), name: "Customer".to_string() }
    );
}

#[test]
fn forward_file_table_uses_warehouse_root_namespace() {
    let n = file_naming();
    assert_eq!(
        n.dataset_ref(&table("main", "orders")),
        DatasetRef {
            namespace: "file:///var/lib/loom/warehouse".to_string(),
            name: "main.orders".to_string(),
        }
    );
}

#[test]
fn forward_namespace_derivation_drops_key_prefix() {
    // s3://bucket/warehouse/prefix -> namespace s3://bucket (authority only).
    let n = s3_naming();
    assert_eq!(n.dataset_ref(&table("main", "orders")).namespace, "s3://bucket");
}

#[test]
fn forward_non_default_schema_round_trips_name() {
    let n = s3_naming();
    assert_eq!(
        n.dataset_ref(&table("analytics", "report")).name,
        "analytics.report"
    );
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `buck2 test //src/services/lineage-naming:naming > /tmp/lnaming.log 2>&1; grep -E "Tests finished|FAIL|error\[|no target" /tmp/lnaming.log`
Expected: FAIL — the target/crate does not exist yet (build error, no `lib.rs`).

- [ ] **Step 4: Write the minimal implementation (struct, enum, from_object_store, forward methods)**

Create `src/services/lineage-naming/src/lib.rs`:

```rust
//! Deployment-aware naming bridge between loom's typed identities
//! (`TableRef`/`TypeName`) and OpenLineage `DatasetRef`s, and back. Built from the
//! deployment's `ObjectStoreConfig` so the physical storage location is encoded in
//! the OpenLineage namespace. Postgres-free, pure logic over config. See
//! docs/superpowers/specs/2026-07-01-dataset-naming-bridge-design.md.

use control_plane_core::{DatasetId, DatasetRef, TableRef, TypeId, TypeName};
use store_config::{ObjectStoreBackend, ObjectStoreConfig};

/// Deployment-aware bridge between loom's typed identities (`TableRef`/`TypeName`)
/// and OpenLineage `DatasetRef`s, and back. Built from the deployment's
/// `ObjectStoreConfig` so the physical storage location is encoded in the
/// OpenLineage namespace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LineageNaming {
    /// This deployment's storage-derived OpenLineage namespace: the datasource
    /// authority only — `s3://<bucket>` or the local `file://<root>` — never the key
    /// prefix, so the namespace is stable per warehouse and the `schema.table` name
    /// stays deployment-independent.
    site_namespace: String,
}

/// The reverse-resolution outcome. Total: `External` is a first-class result, never an
/// error — a `DatasetRef` may legitimately name a dataset loom does not govern.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedDataset {
    /// Names a physical table this deployment governs.
    Table(TableRef),
    /// Names an ontology type this deployment governs.
    Type(TypeName),
    /// Names a dataset outside this deployment's governance — an external datasource,
    /// or a different loom site's warehouse. Carries the raw ref verbatim.
    External(DatasetRef),
}

impl LineageNaming {
    /// Derive the bridge from parsed deployment config. Infallible: the config is
    /// already parsed/validated, and the namespace comes from the parsed backend, so
    /// derivation cannot fail.
    pub fn from_object_store(cfg: &ObjectStoreConfig) -> LineageNaming {
        let site_namespace = match &cfg.backend {
            // S3 datasource authority is the (already-parsed, non-empty) bucket; the
            // key prefix in `warehouse_uri` is dropped so the namespace is stable per
            // warehouse and the `schema.table` name stays deployment-independent.
            ObjectStoreBackend::S3(s) => format!("s3://{}", s.bucket),
            // Local disk has no bucket authority, so the warehouse root URI itself is
            // the datasource. `warehouse_uri` was validated by `ObjectStoreConfig`.
            ObjectStoreBackend::Local => cfg.warehouse_uri.clone(),
        };
        LineageNaming { site_namespace }
    }

    /// Storage-derived ref for a governed table:
    /// `{ namespace: site_namespace, name: "<schema>.<table>" }`. Reuses `core`'s
    /// canonical `schema.table` name formatting; only the namespace is swapped for
    /// this deployment's storage datasource.
    pub fn dataset_ref(&self, table: &TableRef) -> DatasetRef {
        DatasetRef {
            namespace: self.site_namespace.clone(),
            ..DatasetId::from(table).dataset_ref()
        }
    }

    /// Storage-derived ref for a governed ontology type. Types have no physical
    /// storage of their own, so this stays on the logical type namespace
    /// (`"loom:type"`), delegating entirely to `core`.
    pub fn type_ref(&self, ty: &TypeName) -> DatasetRef {
        TypeId::from(ty).dataset_ref()
    }
}
```

> Note: `LOOM_DATASET_NAMESPACE` is deliberately **not** imported in Task 1 — nothing here uses it, and an unused import would surface in this task's `[clippy.txt]`. Task 2 adds it to the `use` line when it introduces `resolve`.

- [ ] **Step 5: Run the forward tests to verify they pass**

Run: `buck2 test //src/services/lineage-naming:naming > /tmp/lnaming.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/lnaming.log`
Expected: PASS — 5 tests pass (the reverse tests do not exist yet).

- [ ] **Step 6: Clippy-clean the library**

Run: `buck2 build '//src/services/lineage-naming:lineage-naming[clippy.txt]' > /tmp/lclippy.log 2>&1; cat $(buck2 build --show-full-output '//src/services/lineage-naming:lineage-naming[clippy.txt]' 2>/dev/null | awk '{print $2}')`
Expected: empty clippy output (no warnings).

- [ ] **Step 7: Commit**

```bash
git add src/services/lineage-naming/BUCK src/services/lineage-naming/src/lib.rs src/services/lineage-naming/tests/naming.rs
git commit -m "feat(lineage-naming): forward TableRef/TypeName -> DatasetRef mapping"
```

---

### Task 2: Reverse resolution (`resolve`) + reverse/round-trip matrix

Adds the total `resolve` reverse-resolver and the reverse + round-trip test matrix. Deliverable: any `DatasetRef` resolves to `Table`/`Type`/`External` without erroring, back-compatible with the logical namespaces.

**Files:**
- Modify: `src/services/lineage-naming/src/lib.rs` (add the `resolve` method to `impl LineageNaming`)
- Modify: `src/services/lineage-naming/tests/naming.rs` (append reverse + round-trip tests)

**Interfaces:**
- Consumes: everything from Task 1, plus `control_plane_core::{DatasetId, TypeId, LOOM_DATASET_NAMESPACE}` (already imported in Task 1).
- Produces: `LineageNaming::resolve(&self, dr: &DatasetRef) -> ResolvedDataset`.

- [ ] **Step 1: Write the failing reverse + round-trip tests**

Append to `src/services/lineage-naming/tests/naming.rs`:

```rust
fn dr(namespace: &str, name: &str) -> DatasetRef {
    DatasetRef { namespace: namespace.to_string(), name: name.to_string() }
}

#[test]
fn reverse_logical_table_resolves_regardless_of_warehouse() {
    // Logical "loom" refs are emitted by control-plane producers with no storage
    // context, so they must resolve under ANY deployment's warehouse.
    for n in [s3_naming(), file_naming()] {
        assert_eq!(
            n.resolve(&dr("loom", "main.orders")),
            ResolvedDataset::Table(table("main", "orders"))
        );
    }
}

#[test]
fn reverse_logical_type_resolves_regardless_of_warehouse() {
    for n in [s3_naming(), file_naming()] {
        assert_eq!(
            n.resolve(&dr("loom:type", "Customer")),
            ResolvedDataset::Type(TypeName("Customer".to_string()))
        );
    }
}

#[test]
fn reverse_storage_derived_table_resolves_for_this_warehouse() {
    let n = s3_naming();
    assert_eq!(
        n.resolve(&dr("s3://bucket", "main.orders")),
        ResolvedDataset::Table(table("main", "orders"))
    );
}

#[test]
fn reverse_external_datasources_are_not_rejected() {
    let n = s3_naming();
    for ext in [
        dr("s3://other-bucket", "main.orders"),
        dr("postgres://h", "public.t"),
        dr("kafka://broker", "topic"),
    ] {
        assert_eq!(n.resolve(&ext), ResolvedDataset::External(ext.clone()));
    }
}

#[test]
fn reverse_malformed_under_loom_namespace_degrades_to_external() {
    let n = s3_naming();
    for bad in [
        dr("loom", "nodot"),       // no schema separator
        dr("loom", ".x"),          // empty schema
        dr("loom", "x."),          // empty table
        dr("s3://bucket", "a.b.c"), // ambiguous multi-dot under storage namespace
    ] {
        assert_eq!(n.resolve(&bad), ResolvedDataset::External(bad.clone()));
    }
}

#[test]
fn reverse_storage_namespace_with_type_shaped_name_is_external() {
    // The storage namespace only carries tables; a bare identifier (no dot) does not
    // parse as schema.table, and types live on the logical namespace, so this is
    // External, not Type.
    let n = s3_naming();
    assert_eq!(
        n.resolve(&dr("s3://bucket", "Customer")),
        ResolvedDataset::External(dr("s3://bucket", "Customer"))
    );
}

#[test]
fn round_trip_table_and_type() {
    let n = s3_naming();
    let t = table("main", "orders");
    assert_eq!(n.resolve(&n.dataset_ref(&t)), ResolvedDataset::Table(t.clone()));

    let ty = TypeName("Customer".to_string());
    assert_eq!(n.resolve(&n.type_ref(&ty)), ResolvedDataset::Type(ty.clone()));
}
```

- [ ] **Step 2: Run the tests to verify the new ones fail**

Run: `buck2 test //src/services/lineage-naming:naming > /tmp/lnaming.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/lnaming.log`
Expected: FAIL — build error: no method `resolve` on `LineageNaming`.

- [ ] **Step 3: Add the `resolve` method**

First extend the `use control_plane_core::{...}` line at the top of `src/lib.rs` to add `LOOM_DATASET_NAMESPACE`, so it reads:

```rust
use control_plane_core::{DatasetId, DatasetRef, LOOM_DATASET_NAMESPACE, TableRef, TypeId, TypeName};
```

Then add this method to `impl LineageNaming` in `src/services/lineage-naming/src/lib.rs` (after `type_ref`):

```rust
    /// Resolve any `DatasetRef` back to the governed object it names, or `External`.
    /// Total; never errors. Recognizes, in order:
    ///   1. the logical loom namespaces (`"loom"` / `"loom:type"`), via `core`'s
    ///      `DatasetId`/`TypeId::from_dataset_ref` — back-compat with the refs
    ///      control-plane producers emit with no storage context;
    ///   2. this deployment's `site_namespace` with a well-formed `schema.table`
    ///      name — reusing the same `core` parse guards by re-namespacing to the
    ///      logical form;
    ///   3. everything else — a different datasource, or a malformed name under a
    ///      loom namespace — `External(raw ref)`.
    pub fn resolve(&self, dr: &DatasetRef) -> ResolvedDataset {
        if let Some(id) = DatasetId::from_dataset_ref(dr) {
            return ResolvedDataset::Table(id.table().clone());
        }
        if let Some(ty) = TypeId::from_dataset_ref(dr) {
            return ResolvedDataset::Type(ty.type_name().clone());
        }
        if dr.namespace == self.site_namespace {
            // Same `schema.table` name shape as the logical form; delegate to core's
            // parse guards by re-namespacing, so malformed names degrade to External.
            let logical = DatasetRef {
                namespace: LOOM_DATASET_NAMESPACE.to_string(),
                name: dr.name.clone(),
            };
            if let Some(id) = DatasetId::from_dataset_ref(&logical) {
                return ResolvedDataset::Table(id.table().clone());
            }
        }
        ResolvedDataset::External(dr.clone())
    }
```

- [ ] **Step 4: Run the full test matrix to verify it passes**

Run: `buck2 test //src/services/lineage-naming:naming > /tmp/lnaming.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/lnaming.log`
Expected: PASS — all 12 tests pass.

- [ ] **Step 5: Clippy-clean the library again**

Run: `buck2 build '//src/services/lineage-naming:lineage-naming[clippy.txt]' > /tmp/lclippy.log 2>&1; cat $(buck2 build --show-full-output '//src/services/lineage-naming:lineage-naming[clippy.txt]' 2>/dev/null | awk '{print $2}')`
Expected: empty clippy output.

- [ ] **Step 6: rustfmt check**

Run: `buck2 run //tools:rustfmt -- --check src/services/lineage-naming/src/lib.rs src/services/lineage-naming/tests/naming.rs`
Expected: no diff (exit 0). If it reports a diff, run without `--check` to apply, then re-verify.

- [ ] **Step 7: Commit**

```bash
git add src/services/lineage-naming/src/lib.rs src/services/lineage-naming/tests/naming.rs
git commit -m "feat(lineage-naming): total reverse DatasetRef resolution"
```

---

### Task 3: Register update — close the roadmap item

Marks `road-dataset-naming-bridge` done in the register. (Runs via `loom-docs-update` at finish; captured here as an explicit task so it isn't forgotten.)

**Files:**
- Modify: `docs/ROADMAP.md` (the `road-dataset-naming-bridge` line)

- [ ] **Step 1: Flip the checkbox, status, and pr tag**

In `docs/ROADMAP.md`, change the `road-dataset-naming-bridge` entry from `- [ ]` to `- [x]`, `status:planned` → `status:done`, and `pr:-` → `pr:#<PR-number>` (the PR opened at finish). Leave the prose and `[[links]]` intact.

- [ ] **Step 2: Validate the registers**

Run: `bash tools/docs.sh validate`
Expected: no errors.

- [ ] **Step 3: Commit (folded into the PR)**

```bash
git add docs/ROADMAP.md
git commit -m "docs(roadmap): close road-dataset-naming-bridge"
```

---

## Verification (whole plan)

- `buck2 test //src/services/lineage-naming:naming` — all 12 tests green.
- `buck2 build //src/services/lineage-naming/...` — the crate builds clean.
- `tools/clippy-all.sh` (or the per-target `[clippy.txt]`) — no clippy warnings on the new crate.
- `buck2 run //tools:prek -- run --all-files` — lint hooks (rustfmt, file checks, docs-validate) pass; commit any in-place fixes.
