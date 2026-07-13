# Catalog Views (dataset/type decoupling) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A **view** as a first-class virtual dataset — base table + optional `RowFilter` predicate + optional column projection — sharing the `(schema, name)` namespace with physical tables, so `ObjectType.table` binds it and `PolicyTarget::Table` grants it unchanged; subsets of one physical dataset carry independent permissions without row duplication.

**Architecture:** The view store is a new catalog-concern surface (core trait methods with non-breaking defaults, memory + postgres adapters, testkit contract). The postgres/memory `Catalog` read methods **delegate a view ref to its base** (schema narrowed by projection), which makes `bind()`, `/datasets/{s}/{t}`, and the governed read compile work unchanged. View **expansion happens engine-side** at registration time (`df.into_view()` over the registered base provider), so every SQL consumer (object reads, preview, transform inputs, governed SQL) resolves views identically. Writes resolve view→base in query-api's `action.rs` and gate on the view predicate with the existing `write_filter::eval` machinery; the engine writer stays purely physical.

**Tech Stack:** Rust (edition 2024), buck2, sqlx compile-time queries (postgres), DataFusion (engine-serving only), axum (query-api/runtime), testkit contract tests, `loom_fixture_test` for anything touching Postgres.

**Spec:** `docs/superpowers/specs/2026-07-13-catalog-views-design.md` (register item `#road-catalog-views`).

## Global Constraints

- Strict clippy: `clippy::pedantic` + `clippy::restriction` on all production code; silence locally only with `#[expect(lint, reason = "...")]`. No `unwrap`/`expect`/`panic`/indexing in prod code (tests are exempt via the test macros).
- Tests are `rust_test` / `loom_fixture_test` **targets** only — never inline `#[cfg(test)]`. Any test that boots Postgres MUST use `loom_fixture_test` (from `//src/control-plane/postgres:defs.bzl`); pure-logic tests use `rust_test` (from `//src:loom_test.bzl`).
- Build/test command forms (keep them bare so permission rules match): `buck2 build -v0 --console none //src/...`, `buck2 test --console none <target>`. Scope test runs to the targets you touched; run the full `buck2 test --console none //src/...` only at final review.
- Run `buck2 run //tools:prek -- run --all-files` before every commit; commit whatever it fixes.
- After ANY change to SQL in `src/control-plane/postgres` (new migration or new `query!`), run `tools/sqlx-prepare.sh` and commit the `.sqlx/` diff. The `sqlx-cache-check` test enforces freshness.
- Conventional Commits messages (`feat(catalog): ...`, `test(...): ...`); the commit-msg hook enforces the format.
- Migration numbering: next free is **`0044`**.
- `RowFilter`/`ScalarValue`/`CompareOp` come from `control_plane_core` — do not redefine them.
- Views are **create-only** in v1 (`define_view` on an existing name is `Conflict`); no view-over-view; no stream subscribe on a view.

---

### Task 1: Core `ViewDef` + validation + `Catalog` trait methods

**Files:**
- Modify: `src/control-plane/core/src/catalog.rs`
- Modify: `src/control-plane/core/src/lib.rs`
- Create: `src/control-plane/core/tests/view_def.rs`
- Modify: `src/control-plane/core/BUCK`

**Interfaces:**
- Consumes: `TableRef`, `TableSchema`, `RowFilter`, `validate_row_filter` (all existing in `control_plane_core`), `Page`/`PageReq`, `ControlPlaneError`.
- Produces (later tasks rely on these exact names):
  - `pub struct ViewDef { pub view: TableRef, pub base: TableRef, pub predicate: Option<RowFilter>, pub columns: Option<Vec<String>> }`
  - `pub fn validate_view_shape(v: &ViewDef, base_schema: &TableSchema) -> std::result::Result<(), String>`
  - `Catalog` trait methods: `define_view(&self, view: ViewDef) -> Result<()>`, `drop_view(&self, view: &TableRef) -> Result<()>`, `get_view(&self, view: &TableRef) -> Result<Option<ViewDef>>`, `list_views(&self, page: PageReq) -> Result<Page<ViewDef>>` — all with default bodies so existing impls/stubs keep compiling.
  - Naming note: the spec's `resolve_dataset(ref) → Physical | View{...}` surface is realized as `get_view` (`None` = physical) — the read-method delegation makes a separate resolution enum unnecessary.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/core/tests/view_def.rs`:

```rust
//! `ViewDef` shape validation: predicate/projection must resolve against the
//! base schema; the RowFilter structural invariants apply.

use control_plane_core::{
    ColumnDef, CompareOp, RowFilter, ScalarValue, TableRef, TableSchema, ViewDef,
    validate_view_shape,
};

fn tref(s: &str, n: &str) -> TableRef {
    TableRef {
        schema: s.into(),
        name: n.into(),
    }
}

fn base_schema() -> TableSchema {
    TableSchema {
        columns: vec![
            ColumnDef { order: 1, name: "id".into(), ty: "long".into(), nullable: false },
            ColumnDef { order: 2, name: "region".into(), ty: "string".into(), nullable: true },
            ColumnDef { order: 3, name: "amount".into(), ty: "long".into(), nullable: true },
        ],
    }
}

fn eu_predicate() -> RowFilter {
    RowFilter::Compare {
        property: "region".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("EU".into()),
    }
}

fn vdef(predicate: Option<RowFilter>, columns: Option<Vec<String>>) -> ViewDef {
    ViewDef {
        view: tref("gov", "customers_eu"),
        base: tref("raw", "customers"),
        predicate,
        columns,
    }
}

#[test]
fn accepts_predicate_and_projection_over_base_columns() {
    let v = vdef(eu_predicate().into(), Some(vec!["id".into(), "region".into()]));
    assert_eq!(validate_view_shape(&v, &base_schema()), Ok(()));
}

#[test]
fn accepts_bare_view_no_predicate_no_projection() {
    assert_eq!(validate_view_shape(&vdef(None, None), &base_schema()), Ok(()));
}

#[test]
fn rejects_predicate_on_unknown_column() {
    let v = vdef(
        Some(RowFilter::Compare {
            property: "nope".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Text("x".into()),
        }),
        None,
    );
    let err = validate_view_shape(&v, &base_schema()).unwrap_err();
    assert!(err.contains("nope"), "error names the bad column: {err}");
}

#[test]
fn rejects_projection_with_unknown_column() {
    let v = vdef(None, Some(vec!["id".into(), "ghost".into()]));
    let err = validate_view_shape(&v, &base_schema()).unwrap_err();
    assert!(err.contains("ghost"), "error names the bad column: {err}");
}

#[test]
fn rejects_empty_projection() {
    let v = vdef(None, Some(vec![]));
    assert!(validate_view_shape(&v, &base_schema()).is_err());
}

#[test]
fn rejects_duplicate_projection_column() {
    let v = vdef(None, Some(vec!["id".into(), "id".into()]));
    assert!(validate_view_shape(&v, &base_schema()).is_err());
}

#[test]
fn rejects_caller_only_compare_ops_in_predicate() {
    // Contains is caller-predicate-only; validate_row_filter rejects it and
    // validate_view_shape must surface that.
    let v = vdef(
        Some(RowFilter::Compare {
            property: "region".into(),
            op: CompareOp::Contains,
            value: ScalarValue::Text("E".into()),
        }),
        None,
    );
    assert!(validate_view_shape(&v, &base_schema()).is_err());
}

#[test]
fn rejects_self_referential_view() {
    let mut v = vdef(None, None);
    v.base = v.view.clone();
    assert!(validate_view_shape(&v, &base_schema()).is_err());
}
```

Add the BUCK target to `src/control-plane/core/BUCK` (mirror the existing `page` test target):

```python
rust_test(
    name = "view-def",
    crate = "view_def",
    srcs = ["tests/view_def.rs"],
    crate_root = "tests/view_def.rs",
    edition = "2024",
    deps = [
        ":core",
    ],
)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/core:view-def`
Expected: BUILD FAILED — `ViewDef` / `validate_view_shape` unresolved in `control_plane_core`.

- [ ] **Step 3: Implement `ViewDef`, `validate_view_shape`, and the trait methods**

In `src/control-plane/core/src/catalog.rs`, after `TableSchema`:

```rust
/// A virtual dataset: a named row/column subset of one physical base table.
/// Shares the `(schema, name)` namespace with physical tables so it binds and
/// grants exactly like one (`ObjectType.table`, `PolicyTarget::Table`).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ViewDef {
    /// The view's own ref — must not collide with any physical table or view.
    pub view: TableRef,
    /// The physical base. v1 forbids view-over-view: this must be a table.
    pub base: TableRef,
    /// Row subset. `property` names a BASE column directly (Table-target
    /// `RowFilter` semantics). `None` = all rows.
    pub predicate: Option<crate::acl::RowFilter>,
    /// Column subset, in base-schema order significance. `None` = all columns.
    pub columns: Option<Vec<String>>,
}

/// Pure shape validation of a view definition against its base schema:
/// predicate columns and projection columns must exist in the base; the
/// projection must be non-empty and duplicate-free; the view must not name
/// itself as base. Existence/collision checks are the adapters' job (they
/// need store access); this is the shared schema-shape gate.
pub fn validate_view_shape(
    v: &ViewDef,
    base_schema: &TableSchema,
) -> std::result::Result<(), String> {
    if v.view == v.base {
        return Err(format!(
            "view {}.{} cannot use itself as base",
            v.view.schema, v.view.name
        ));
    }
    let base_cols: std::collections::HashSet<String> =
        base_schema.columns.iter().map(|c| c.name.clone()).collect();
    if let Some(f) = &v.predicate {
        crate::acl::validate_row_filter(f, Some(&base_cols))?;
    }
    if let Some(cols) = &v.columns {
        if cols.is_empty() {
            return Err("projection must name at least one column".into());
        }
        let mut seen = std::collections::HashSet::new();
        for c in cols {
            if !base_cols.contains(c) {
                return Err(format!("projection column `{c}` not in base schema"));
            }
            if !seen.insert(c) {
                return Err(format!("duplicate projection column `{c}`"));
            }
        }
    }
    Ok(())
}
```

(Check `validate_row_filter`'s error type — it returns `Result<(), String>`, so `?` works directly.)

Extend the `Catalog` trait (same file) with **defaulted** methods so every existing impl and test stub keeps compiling:

```rust
    /// Create a view. Create-only: an existing view (or a name colliding with
    /// a physical table) is `Conflict`. The base must exist, be physical (no
    /// view-over-view), and satisfy [`validate_view_shape`].
    async fn define_view(&self, view: ViewDef) -> Result<()> {
        let _ = view;
        Err(ControlPlaneError::Validation(
            "views are not supported by this catalog".into(),
        ))
    }

    /// Drop a view by its ref. `NotFound` if it does not exist.
    async fn drop_view(&self, view: &TableRef) -> Result<()> {
        Err(ControlPlaneError::NotFound(format!(
            "{}.{}",
            view.schema, view.name
        )))
    }

    /// Resolve a ref to its view definition, `None` if it is not a view.
    async fn get_view(&self, view: &TableRef) -> Result<Option<ViewDef>> {
        let _ = view;
        Ok(None)
    }

    /// All views, `(schema, name)`-ordered, single full page (parity with
    /// `list_tables`).
    async fn list_views(&self, page: PageReq) -> Result<Page<ViewDef>> {
        let _ = page;
        Ok(Page::from_full(Vec::new()))
    }
```

In `src/control-plane/core/src/lib.rs`, extend the catalog re-export:

```rust
pub use catalog::{
    Catalog, ColumnDef, FileRef, Snapshot, SnapshotId, TableRef, TableSchema, ViewDef,
    small_files, validate_view_shape,
};
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test --console none //src/control-plane/core:view-def`
Expected: `Tests finished: Pass 1. Fail 0` (one target, all cases pass).

Also confirm nothing downstream broke: `buck2 build -v0 --console none //src/...`
Expected: silent success.

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/core
git commit -m "feat(catalog): ViewDef + validate_view_shape + defaulted Catalog view methods"
```

---

### Task 2: Testkit view contract + memory adapter

**Files:**
- Modify: `src/control-plane/testkit/src/lib.rs`
- Modify: `src/control-plane/memory/src/catalog.rs`
- Create: `src/control-plane/memory/tests/catalog_views.rs`
- Modify: `src/control-plane/memory/BUCK`

**Interfaces:**
- Consumes: Task 1's `ViewDef`, `validate_view_shape`, trait methods; existing `CatalogSeed`/`SeedSpec`/`SeedColumn` seeding seam; memory `CatalogState` (`tables`/`columns` maps, `Versioned` MVCC); the memory ontology's lineage-emission pattern (`memory/src/ontology.rs:82` emits `type_table_binding_event` — mirror it).
- Produces: `pub async fn catalog_view_contract<C, S>(catalog: &C, seeder: &S) where C: Catalog, S: CatalogSeed` in testkit (Task 3 reuses it for postgres); the constant `pub const VIEW_DEFINITION_KIND: &str = "view-definition";` in `control_plane_core::identity` and `pub fn view_definition_event(v: &ViewDef) -> LineageEvent` (inputs = base dataset ref, outputs = view dataset ref) — added here because both adapters need it.

- [ ] **Step 1: Write the contract (the failing test)**

Append to `src/control-plane/testkit/src/lib.rs` (after `catalog_delete_contract`):

```rust
/// Contract for the catalog view surface: define/get/list/drop, base
/// delegation of the snapshot+schema reads, and every write-time rejection.
pub async fn catalog_view_contract<C, S>(catalog: &C, seeder: &S)
where
    C: Catalog,
    S: CatalogSeed,
{
    let base = TableRef { schema: "main".into(), name: "customers".into() };
    let seeded = seeder
        .seed(SeedSpec {
            table: base.clone(),
            columns: vec![
                SeedColumn { name: "id".into(), ty: "long".into(), nullable: false },
                SeedColumn { name: "region".into(), ty: "string".into(), nullable: true },
                SeedColumn { name: "amount".into(), ty: "long".into(), nullable: true },
            ],
            row_batches: vec![10],
        })
        .await;
    let base_snap = seeded[0].snapshot;

    let view = TableRef { schema: "gov".into(), name: "customers_eu".into() };
    let vdef = ViewDef {
        view: view.clone(),
        base: base.clone(),
        predicate: Some(RowFilter::Compare {
            property: "region".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Text("EU".into()),
        }),
        columns: Some(vec!["id".into(), "region".into()]),
    };
    catalog.define_view(vdef.clone()).await.expect("define_view");

    // get_view round-trips; a physical ref is not a view.
    assert_eq!(catalog.get_view(&view).await.unwrap(), Some(vdef.clone()));
    assert_eq!(catalog.get_view(&base).await.unwrap(), None);

    // list_views: (schema, name)-ordered single page containing the view.
    let listed = catalog.list_views(PageReq::unbounded()).await.unwrap();
    assert!(listed.next.is_none(), "single full page");
    assert!(listed.items.contains(&vdef), "defined view listed");

    // list_tables stays physical-only.
    let tables = catalog.list_tables(PageReq::unbounded()).await.unwrap();
    assert!(!tables.items.contains(&view), "views are not physical tables");

    // Read delegation: snapshot reads resolve through the base...
    assert_eq!(
        catalog.current_snapshot(&view).await.unwrap().id,
        catalog.current_snapshot(&base).await.unwrap().id,
        "current_snapshot(view) delegates to base"
    );
    // ...and schema narrows to the projection, keeping base column order/types.
    let vschema = catalog.schema(&view, base_snap).await.unwrap();
    assert_eq!(
        vschema.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        vec!["id", "region"],
        "schema(view) is the projected base schema"
    );
    assert_eq!(vschema.columns[0].ty, "long");

    // A projection-less view reads the full base schema.
    let full = TableRef { schema: "gov".into(), name: "customers_all".into() };
    catalog
        .define_view(ViewDef {
            view: full.clone(),
            base: base.clone(),
            predicate: None,
            columns: None,
        })
        .await
        .expect("define projection-less view");
    assert_eq!(
        catalog.schema(&full, base_snap).await.unwrap(),
        catalog.schema(&base, base_snap).await.unwrap(),
        "no projection = full base schema"
    );

    // Rejections. Create-only: redefining is Conflict.
    assert!(matches!(
        catalog.define_view(vdef.clone()).await,
        Err(ControlPlaneError::Conflict(_))
    ));
    // Name collision with a physical table is Conflict.
    assert!(matches!(
        catalog
            .define_view(ViewDef {
                view: base.clone(),
                base: base.clone(),
                predicate: None,
                columns: None,
            })
            .await,
        Err(ControlPlaneError::Conflict(_)) | Err(ControlPlaneError::Validation(_))
    ));
    // Missing base is NotFound.
    assert!(matches!(
        catalog
            .define_view(ViewDef {
                view: TableRef { schema: "gov".into(), name: "orphan".into() },
                base: TableRef { schema: "main".into(), name: "nope".into() },
                predicate: None,
                columns: None,
            })
            .await,
        Err(ControlPlaneError::NotFound(_))
    ));
    // View-over-view is Validation.
    assert!(matches!(
        catalog
            .define_view(ViewDef {
                view: TableRef { schema: "gov".into(), name: "nested".into() },
                base: view.clone(),
                predicate: None,
                columns: None,
            })
            .await,
        Err(ControlPlaneError::Validation(_))
    ));
    // Bad predicate column / bad projection column are Validation.
    assert!(matches!(
        catalog
            .define_view(ViewDef {
                view: TableRef { schema: "gov".into(), name: "badpred".into() },
                base: base.clone(),
                predicate: Some(RowFilter::Compare {
                    property: "ghost".into(),
                    op: CompareOp::Eq,
                    value: ScalarValue::Text("x".into()),
                }),
                columns: None,
            })
            .await,
        Err(ControlPlaneError::Validation(_))
    ));
    assert!(matches!(
        catalog
            .define_view(ViewDef {
                view: TableRef { schema: "gov".into(), name: "badproj".into() },
                base: base.clone(),
                predicate: None,
                columns: Some(vec!["ghost".into()]),
            })
            .await,
        Err(ControlPlaneError::Validation(_))
    ));

    // drop_view removes it; unknown drop is NotFound.
    catalog.drop_view(&full).await.expect("drop_view");
    assert_eq!(catalog.get_view(&full).await.unwrap(), None);
    assert!(matches!(
        catalog.drop_view(&full).await,
        Err(ControlPlaneError::NotFound(_))
    ));
    assert!(matches!(
        catalog.current_snapshot(&full).await,
        Err(ControlPlaneError::NotFound(_)),
    ), "a dropped view no longer resolves");
}
```

Add the needed imports at the top of testkit's lib.rs (`ViewDef`, `RowFilter`, `CompareOp`, `ScalarValue`, `ControlPlaneError` — extend the existing `control_plane_core::{...}` import).

Create `src/control-plane/memory/tests/catalog_views.rs` (mirror `memory/tests/catalog.rs`'s `MemSeeder` wiring):

```rust
//! Memory fake passes the catalog view contract.

use std::time::Duration;

use async_trait::async_trait;
use control_plane_memory::MemoryControlPlane;
use control_plane_testkit::{CatalogSeed, SeedSpec, SeededSnapshot, catalog_view_contract};
use control_plane_core::{SnapshotId, TableRef};

struct MemSeeder<'a>(&'a MemoryControlPlane);

#[async_trait]
impl CatalogSeed for MemSeeder<'_> {
    async fn seed(&self, spec: SeedSpec) -> Vec<SeededSnapshot> {
        self.0.seed_catalog(&spec.table, &spec.columns, &spec.row_batches)
    }
    async fn drop_table(&self, table: &TableRef) -> SnapshotId {
        self.0.drop_table_catalog(table)
    }
}

#[tokio::test]
async fn memory_passes_catalog_view_contract() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    catalog_view_contract(&cp, &MemSeeder(&cp)).await;
}
```

NOTE: copy the exact `MemSeeder` body from the existing `memory/tests/catalog.rs` — the `seed`/`drop_table` forwarding signatures there are authoritative; the sketch above matches its shape but defer to the file.

BUCK target in `src/control-plane/memory/BUCK` (mirror the existing `catalog` target):

```python
rust_test(
    name = "catalog-views",
    crate = "catalog_views",
    srcs = ["tests/catalog_views.rs"],
    crate_root = "tests/catalog_views.rs",
    edition = "2024",
    deps = [
        ":memory",
        "//src/control-plane/core:core",
        "//src/control-plane/testkit:testkit",
        "//third-party:async-trait",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/control-plane/memory:catalog-views`
Expected: FAIL — memory still uses the default `define_view` (`Validation("views are not supported...")`), so the first `.expect("define_view")` panics.

- [ ] **Step 3: Implement the lineage event helper + memory adapter**

In `src/control-plane/core/src/identity.rs`, next to `type_table_binding_event` (~line 138), add:

```rust
/// Marker kind for the base→view definition edge.
pub const VIEW_DEFINITION_KIND: &str = "view-definition";

/// The lineage edge emitted when a view is defined: base table = upstream
/// input, view = downstream output (same orientation as the type binding).
pub fn view_definition_event(v: &crate::catalog::ViewDef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::now_utc(),
        inputs: vec![DatasetId::from(&v.base).dataset_ref()],
        outputs: vec![DatasetId::from(&v.view).dataset_ref()],
        payload: serde_json::json!({ "loom.kind": VIEW_DEFINITION_KIND }),
    }
}
```

Re-export from core `lib.rs` alongside the other identity exports (find the existing `pub use identity::{...}` and add `VIEW_DEFINITION_KIND, view_definition_event`).

In `src/control-plane/memory/src/catalog.rs`:

1. Add to `CatalogState`: `pub(crate) views: std::collections::BTreeMap<TableRef, ViewDef>,` (BTreeMap gives `(schema, name)` ordering for free — `TableRef` derives `Ord`? It does NOT (only `Hash`/`Eq`); key by `(String, String)` tuple instead: `pub(crate) views: BTreeMap<(String, String), ViewDef>` keyed by `(schema.clone(), name.clone())`).
2. Implement the four trait methods on `impl Catalog for MemoryControlPlane`:

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn define_view(&self, view: ViewDef) -> Result<()> {
        // Validate under the lock, then emit lineage after (memory lineage
        // takes its own lock — mirror how define_type emits its binding event).
        {
            let mut cat = self.catalog.lock();
            let vkey = (view.view.schema.clone(), view.view.name.clone());
            if cat.views.contains_key(&vkey) {
                return Err(ControlPlaneError::Conflict(format!(
                    "view {}.{} already exists",
                    view.view.schema, view.view.name
                )));
            }
            if cat.tables.get(&view.view).is_some_and(|t| t.end.is_none()) {
                return Err(ControlPlaneError::Conflict(format!(
                    "{}.{} is a physical table",
                    view.view.schema, view.view.name
                )));
            }
            let bkey = (view.base.schema.clone(), view.base.name.clone());
            if cat.views.contains_key(&bkey) {
                return Err(ControlPlaneError::Validation(
                    "view-over-view is not supported".into(),
                ));
            }
            let live = cat.tables.get(&view.base).is_some_and(|t| t.end.is_none());
            if !live {
                return Err(ControlPlaneError::NotFound(format!(
                    "{}.{}",
                    view.base.schema, view.base.name
                )));
            }
            let base_schema = /* collect live base columns exactly as `schema()` does,
                                 at the latest snapshot */;
            validate_view_shape(&view, &base_schema).map_err(ControlPlaneError::Validation)?;
            cat.views.insert(vkey, view.clone());
        }
        self.emit_view_definition(&view); // push view_definition_event into the
                                          // memory lineage state, same pattern as
                                          // define_type's binding event
        Ok(())
    }
```

(`get_view`/`list_views`/`drop_view` are straightforward map read / ordered-values page / remove-or-NotFound. For the exact "collect live base columns" and lineage-push code, mirror the bodies of `schema()` in this file and the binding-event emission in `memory/src/ontology.rs:82` respectively.)

3. **Delegation** in the existing read methods: at the top of `current_snapshot`, `snapshot_as_of`, `snapshots`, `snapshot`, `files`, and `schema`, resolve the ref:

```rust
        // A view ref reads through its base (schema additionally narrows below).
        let (table, projection) = {
            let cat = self.catalog.lock();
            match cat.views.get(&(table.schema.clone(), table.name.clone())) {
                Some(v) => (v.base.clone(), v.columns.clone()),
                None => (table.clone(), None),
            }
        };
        let table = &table;
```

and in `schema()` only, after collecting `cols`, apply the projection:

```rust
        if let Some(proj) = projection {
            cols.retain(|c| proj.contains(&c.name));
        }
```

4. Base-drop protection: **deliberate deviation from the spec's testing bullet** (which implies contract-level coverage in both adapters). `CatalogSeed::drop_table` returns a bare `SnapshotId` — no `Result` — so a refusal cannot be expressed through the contract seam without breaking the existing delete contract. Protection is therefore enforced and tested where the production drop path lives: postgres `mark_dropped` (Task 3). Memory's `drop_table_catalog` is a test-only seam with no production drop consumer and stays unguarded. Record this in the PR description.

- [ ] **Step 4: Run to verify it passes**

Run: `buck2 test --console none //src/control-plane/memory:catalog-views`
Expected: `Tests finished: Pass 1. Fail 0`.
Also: `buck2 test --console none //src/control-plane/memory:catalog` (existing contract still green) and `buck2 build -v0 --console none //src/...`.

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/core src/control-plane/memory src/control-plane/testkit
git commit -m "feat(catalog): view contract in testkit + memory adapter with base delegation"
```

---

### Task 3: Postgres adapter — migration, `IcebergCatalog` view surface, sqlx cache

**Files:**
- Create: `src/control-plane/postgres/migrations/0044_dataset_view.sql`
- Modify: `src/control-plane/postgres/src/iceberg_catalog.rs`
- Modify: `src/control-plane/postgres/src/iceberg_mirror.rs` (base-drop protection in `mark_dropped`)
- Create: `src/control-plane/postgres/tests/dataset_view.rs`
- Modify: `src/control-plane/postgres/BUCK`
- Regenerate: `src/control-plane/postgres/.sqlx/` (via `tools/sqlx-prepare.sh`)

**Interfaces:**
- Consumes: Task 1 types; Task 2's `catalog_view_contract` + `view_definition_event`; existing `IcebergCatalog { pool }`, `resolve_table`, `backend` error mapper, `crate::lineage::pg_emit(conn, &event)`.
- Produces: `Catalog` view methods on `IcebergCatalog` (every consumer downstream gets them via the trait); `dataset_view` table; `mark_dropped` refuses when dependent views exist (`Conflict` naming them).

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/dataset_view.rs` (mirror `tests/iceberg_catalog.rs`'s `IcebergSeeder` + fixture wiring exactly — copy its seeder struct verbatim):

```rust
//! Postgres passes the catalog view contract + adapter-specific guards
//! (base-drop protection, lineage edge emission).

// ... same imports/seeder as tests/iceberg_catalog.rs, plus:
use control_plane_core::{
    Catalog, Lineage, PageReq, RowFilter, CompareOp, ScalarValue, TableRef, ViewDef,
};

#[tokio::test]
async fn postgres_passes_catalog_view_contract() {
    let fixture = PgFixture::shared();
    let (cp, db) = fixture.fresh_db().await;
    let catalog = IcebergCatalog::new(cp.pool().clone());
    let seeder = /* IcebergSeeder as in tests/iceberg_catalog.rs */;
    catalog_view_contract(&catalog, &seeder).await;
}

#[tokio::test]
async fn dropping_a_base_with_dependent_views_is_refused() {
    // seed base, define view over it, then drive the production drop path
    // (IcebergWriter::drop_table → iceberg_mirror::mark_dropped):
    // expect Err(Conflict) naming `gov.customers_eu`; drop the view, retry,
    // expect success.
}

#[tokio::test]
async fn define_view_emits_base_to_view_lineage_edge() {
    // seed base, define view; then cp.lineage().downstream(&base_ref, 1, page)
    // contains the view's DatasetRef and upstream(&view_ref, 1, page) contains
    // the base's (refs built via DatasetId::from(&table).dataset_ref()).
}
```

(Write the two sketched bodies out fully in the file — the drop path is `IcebergWriter`'s drop used by the seeder in `tests/iceberg_catalog.rs`; the lineage read is the `Lineage` trait on the control plane. Follow the existing file's idioms.)

BUCK target (`loom_fixture_test` — this boots Postgres):

```python
loom_fixture_test(
    name = "dataset_view",
    crate = "dataset_view",
    srcs = ["tests/dataset_view.rs"],
    crate_root = "tests/dataset_view.rs",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//src/control-plane/testkit:testkit",
        "//third-party:async-trait",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:dataset_view`
Expected: FAIL — default trait methods reject `define_view`.

- [ ] **Step 3: Migration**

Create `src/control-plane/postgres/migrations/0044_dataset_view.sql`:

```sql
-- Catalog views: virtual datasets over one physical base table. A view shares
-- the (schema, name) namespace with physical tables (collision enforced in the
-- adapter against iceberg_mirror.table) and carries an optional RowFilter
-- predicate (jsonb, the control-plane serde) plus an optional column
-- projection. Metadata-only: no snapshots, no files.
-- (Naming: the spec sketches `catalog.dataset_view`; `catalog` is avoided as a
-- schema name for its SQL-keyword adjacency — the store is `dataset_view.view`.)
create schema if not exists dataset_view;

create table dataset_view.view (
    view_schema text not null,
    view_name   text not null,
    base_schema text not null,
    base_name   text not null,
    predicate   jsonb,
    columns     text[],
    primary key (view_schema, view_name)
);

create index dataset_view_by_base on dataset_view.view (base_schema, base_name);
```

- [ ] **Step 4: Implement the adapter**

In `src/control-plane/postgres/src/iceberg_catalog.rs`:

1. A row→`ViewDef` helper and the reads:

```rust
async fn fetch_view(pool: &PgPool, table: &TableRef) -> Result<Option<ViewDef>> {
    let row = sqlx::query!(
        "select base_schema, base_name, predicate, columns \
         from dataset_view.view where view_schema = $1 and view_name = $2",
        table.schema,
        table.name,
    )
    .fetch_optional(pool)
    .await
    .map_err(backend)?;
    row.map(|r| {
        let predicate = r
            .predicate
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?;
        Ok(ViewDef {
            view: table.clone(),
            base: TableRef { schema: r.base_schema, name: r.base_name },
            predicate,
            columns: r.columns,
        })
    })
    .transpose()
}
```

2. `define_view`: one transaction — collision check against `dataset_view.view` AND a live `iceberg_mirror.table` row (reuse the `live_table_id` query shape); base existence + physicality (base must have a live mirror row and no `dataset_view.view` row); fetch the base schema via the existing `schema(&base, current_snapshot(&base).id)` path; `validate_view_shape` (map `Err(String)` → `Validation`); insert; `crate::lineage::pg_emit(&mut *tx, &view_definition_event(&view))` in the same tx; commit.
3. `drop_view`: `delete ... returning view_name` → `NotFound` when nothing deleted.
4. `get_view` = `fetch_view`; `list_views`: `select ... order by view_schema, view_name` → `Page::from_full`.
5. **Delegation**: add at the top of `current_snapshot`, `snapshot_as_of`, `snapshots`, `snapshot`, `files`, `schema`:

```rust
        let resolved; // borrow gymnastics: delegate view → base
        let (table, projection) = match fetch_view(&self.pool, table).await? {
            Some(v) => {
                resolved = v.base;
                (&resolved, v.columns)
            }
            None => (table, None),
        };
```

and in `schema()` apply `cols.retain(|c| proj.contains(&c.name))` when `projection` is `Some` (after the existing `is_reserved` filter). `list_tables` is untouched (physical-only). The inherent `live_tables()` (used by the engine registration loop) is untouched.

6. In `src/control-plane/postgres/src/iceberg_mirror.rs`, `mark_dropped` (or the function the drop path calls — find it by `rg "mark_dropped" src/control-plane/postgres/src`): before end-capping the table row, query

```rust
    let dependents = sqlx::query!(
        "select view_schema, view_name from dataset_view.view \
         where base_schema = $1 and base_name = $2 order by view_schema, view_name",
        ns,
        name,
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(backend)?;
    if !dependents.is_empty() {
        let names: Vec<String> = dependents
            .iter()
            .map(|d| format!("{}.{}", d.view_schema, d.view_name))
            .collect();
        return Err(ControlPlaneError::Conflict(format!(
            "table has dependent views: {}",
            names.join(", ")
        )));
    }
```

- [ ] **Step 5: Refresh the sqlx cache**

Run: `tools/sqlx-prepare.sh`
Expected: new `query-*.json` files under `src/control-plane/postgres/.sqlx/`. Stage them.

- [ ] **Step 6: Run to verify it passes**

Run: `buck2 test --console none //src/control-plane/postgres:dataset_view`
Expected: `Tests finished: Pass 1. Fail 0` (3 test fns).
Also: `buck2 test --console none //src/control-plane/postgres:iceberg_catalog //src/control-plane/postgres:sqlx-cache-check`
Expected: both green.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/postgres
git commit -m "feat(catalog): postgres dataset_view store — delegated reads, drop protection, lineage edge"
```

---

### Task 4: Engine-side view expansion

**Files:**
- Modify: `src/services/engine-serving/src/serving.rs`
- Modify: `src/services/engine-serving/src/governed.rs`
- Modify: `src/services/engine-serving/src/lib.rs` (re-export if needed)
- Create: `src/services/engine-serving/tests/view_scan.rs`
- Modify: `src/services/engine-serving/BUCK`

**Interfaces:**
- Consumes: `IcebergCatalog::list_views` (Task 3, via the `Catalog` trait), `row_filter_to_expr` (`governed.rs:59`), `register_qualified` (`serving.rs:532`), `df.into_view()` idiom (`serving.rs:207`), `GovernedTableProvider`/`policy_for` (`governed.rs`).
- Produces: `pub(crate) async fn register_catalog_views(ctx: &SessionContext, catalog: &IcebergCatalog) -> Result<(), EngineServingError>` called from `execute_query_stream`; governed analog inside `execute_governed_sql_stream`. Every SQL consumer (Flight `do_get_sql`, query-api `fetch_rows`, worker transform inputs, governed SQL) resolves view names after this task — no client changes.

- [ ] **Step 1: Write the failing test**

Create `src/services/engine-serving/tests/view_scan.rs` (a `loom_fixture_test`; wire the fixture exactly as `tests/serving_empty_table.rs` does — `PgFixture::shared()` → `fresh_db()` → `IcebergControlPlane` seeding, `IcebergCatalog::new(pool)`):

```rust
//! A catalog view scans as `SELECT <projection> FROM base WHERE <predicate>`
//! through the ordinary SQL serving path; the base is unaffected; an unknown
//! name still fails with the Plan error class.

// imports as serving_empty_table.rs, plus:
use control_plane_core::{Catalog, CompareOp, RowFilter, ScalarValue, TableRef, ViewDef};
use engine_serving::execute_query;

// Seed main.customers with columns (id long, region string) and rows:
//   (1,'EU'), (2,'US'), (3,'EU')   — use the same seeding idiom the fixture
// tests use (IcebergWriter/IcebergControlPlane batch append).

#[tokio::test(flavor = "multi_thread")]
async fn view_scan_applies_predicate_and_projection() {
    // define view gov.customers_eu = main.customers WHERE region='EU', cols [id]
    // execute_query(&catalog, "SELECT * FROM \"gov\".\"customers_eu\" ORDER BY id", None)
    // → exactly ids [1, 3]; result schema has ONLY the `id` column.
}

#[tokio::test(flavor = "multi_thread")]
async fn base_scan_is_unaffected_by_views() {
    // SELECT * FROM "main"."customers" still returns all 3 rows, all columns.
}

#[tokio::test(flavor = "multi_thread")]
async fn projectionless_predicateless_view_mirrors_base() {
    // view with predicate: None, columns: None → same rows+columns as base.
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_view_name_is_still_a_plan_error() {
    // execute_query over "SELECT * FROM \"gov\".\"nope\"" →
    // Err(EngineServingError::Plan(_)) (the serving_empty_table.rs:133 idiom).
}
```

Write the bodies out fully, following `serving_empty_table.rs` for seeding/collect/assert helpers. BUCK target (mirror `merge-on-read`):

```python
loom_fixture_test(
    name = "view-scan",
    crate = "view_scan",
    srcs = ["tests/view_scan.rs"],
    crate_root = "tests/view_scan.rs",
    deps = [
        ":engine-serving",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:datafusion",
        "//third-party:tokio",
    ],
)
```

(Match the dep list of an existing fixture test in this BUCK — some also need `arrow-array`; add what the seeding idiom uses.)

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/services/engine-serving:view-scan`
Expected: FAIL — `view_scan_applies_predicate_and_projection` gets a Plan error ("table not found") because nothing registers the view.

- [ ] **Step 3: Implement expansion**

In `serving.rs`, add after `register_iceberg_table`:

```rust
/// Register every catalog view as a DataFusion logical view over its (already
/// registered) base: `SELECT <columns> FROM base WHERE <predicate>`. A view
/// whose base is not registered (dropped/not-live at `at`) is skipped — a
/// scan of it then fails with the same Plan error as any unknown name.
pub(crate) async fn register_catalog_views(
    ctx: &SessionContext,
    catalog: &IcebergCatalog,
) -> Result<(), EngineServingError> {
    use control_plane_core::Catalog as _;
    let views = catalog
        .list_views(control_plane_core::PageReq::unbounded())
        .await
        .map_err(to_serving)?;
    for v in views.items {
        let Ok(df) = ctx
            .table(TableReference::partial(v.base.schema.clone(), v.base.name.clone()))
            .await
        else {
            continue; // dangling view: base not live here
        };
        let df = match &v.predicate {
            Some(f) => df.filter(row_filter_to_expr(f)?).map_err(to_serving)?,
            None => df,
        };
        let df = match &v.columns {
            Some(cols) => {
                let names: Vec<&str> = cols.iter().map(String::as_str).collect();
                df.select_columns(&names).map_err(to_serving)?
            }
            None => df,
        };
        register_qualified(ctx, &v.view.schema, &v.view.name, df.into_view())?;
    }
    Ok(())
}
```

Call it in `execute_query_stream` after the live-tables loop:

```rust
    for table in catalog.live_tables().await.map_err(to_serving)? {
        register_iceberg_table(&ctx, catalog, &table, serving_store, at).await?;
    }
    register_catalog_views(&ctx, catalog).await?;
```

Extract the predicate/projection folding into a shared helper so both call sites use it:

```rust
/// Fold a view's predicate + projection onto a base DataFrame.
fn fold_view(df: DataFrame, v: &ViewDef) -> Result<DataFrame, EngineServingError> {
    let df = match &v.predicate {
        Some(f) => df.filter(row_filter_to_expr(f)?).map_err(to_serving)?,
        None => df,
    };
    match &v.columns {
        Some(cols) => {
            let names: Vec<&str> = cols.iter().map(String::as_str).collect();
            df.select_columns(&names).map_err(to_serving)
        }
        None => Ok(df),
    }
}
```

(`register_catalog_views` above becomes `fold_view(df, &v)` after the `ctx.table(...)` fetch.)

In `governed.rs`'s `execute_governed_sql_stream`, after its registration loop, add the governed analog. **Normative construction** — the base's own governed status must NOT matter (spec §2: the caller needs only the view grant, never the base grant), so build the base provider **privately and ungoverned** via `build_serving_provider` and never via `ctx.table(...)` (in the governed session a base is registered only when it has its own entry, and then it is wrapped in the BASE's policy — both wrong for the view):

```rust
    for v in catalog
        .list_views(control_plane_core::PageReq::unbounded())
        .await
        .map_err(to_serving)?
        .items
    {
        // Closed-world: only views with a GovernedTable entry register.
        if governed.table_for(&v.view).is_none() {
            continue;
        }
        let policy = policy_for(governed, &v.view);
        // Ungoverned inner base provider, never registered under the base name.
        let Some(inner) = build_serving_provider(&ctx, catalog, &v.base, serving_store, at).await?
        else {
            continue; // dangling view: base not live here
        };
        let df = ctx.read_table(inner).map_err(to_serving)?;
        let provider = fold_view(df, &v)?.into_view();
        let governed_provider =
            GovernedTableProvider::new(provider, policy).map_err(to_serving)?;
        register_qualified(&ctx, &v.view.schema, &v.view.name, Arc::new(governed_provider))?;
    }
```

Match the surrounding code's actual details before trusting this snippet: `table_for`'s exact name on the `GovernedCatalog` (the closed-world skip idiom used by the existing loop at `governed.rs:302-304`), `policy_for(governed, &ref) -> TablePolicy` (NOT `Option` — absent ⇒ empty/fully-visible policy, `governed.rs:152-161`), `GovernedTableProvider::new(...)` returning `Result` (`governed.rs:315` uses `?`), and whether `execute_governed_sql_stream` has `serving_store`/`at` parameters to forward.

Add MANDATORY governed cases (the harness is `tests/governed_sql.rs`, which drives `execute_governed_sql_stream` directly) — either there or in `view_scan.rs`:
- a view with a `GovernedTable` entry carrying a mask on a projected column returns `'***'` for it;
- a view with **no** entry is not queryable (closed-world);
- a view with an entry over a base **without** one is readable (view grant suffices);
- a base policy (row filter/mask on the base's own entry) does NOT apply to the view scan.

- [ ] **Step 4: Run to verify it passes**

Run: `buck2 test --console none //src/services/engine-serving:view-scan //src/services/engine-serving:governed-sql //src/services/engine-serving:execute-query-e2e`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add src/services/engine-serving
git commit -m "feat(engine): register catalog views as logical views at serving resolution"
```

---

### Task 5: Admin routes — `POST /admin/views`, `DELETE /admin/views/{schema}/{name}`

**Files:**
- Modify: `src/services/runtime/src/admin.rs` (handlers + routes + the new `#[utoipa::path]` operations added to `AdminApiDoc`'s `paths(...)` at ~`admin.rs:1819-1821` — `admin_openapi()` at `:1900` serializes it)
- Modify: `src/services/runtime/tests/openapi_fragments.rs` (drift guard 1: `admin_fragment_documents_exactly_the_admin_routes` asserts the exact `(method, path)` set — add both new routes)
- Modify: `src/services/query-api/tests/openapi.rs` (drift guard 2: the enumerated route list at ~lines 38-56; `query-api/src/openapi.rs` itself needs NO change — it merges `service_runtime::admin_openapi()` wholesale)
- Modify: `src/services/runtime/tests/admin_management.rs` (or a new sibling test file if that file's setup doesn't fit)
- Modify: `src/services/runtime/BUCK` (only if a new test file is added)

**Interfaces:**
- Consumes: `Catalog::define_view`/`drop_view` (Tasks 1-3); the existing `POST /admin/transforms` handler pattern in `runtime/src/admin.rs` (admin gating, error mapping, JSON body shape).
- Produces: the two routes. Request body for POST:

```json
{
  "view":      {"schema": "gov", "name": "customers_eu"},
  "base":      {"schema": "raw", "name": "customers"},
  "predicate": { "Compare": { "property": "region", "op": "Eq", "value": {"Text": "EU"} } },
  "columns":   ["id", "region"]
}
```

(`predicate` is the externally-tagged serde of `RowFilter` — the same encoding the ACL policy admin surface accepts; `predicate`/`columns` optional.) Errors map: `Conflict`→409, `NotFound`→404, `Validation`→422 (match how the existing admin handlers map `ControlPlaneError` — reuse their mapper).

- [ ] **Step 1: Write the failing test**

In `src/services/runtime/tests/admin_management.rs` (mirror its existing route tests for setup/auth):

```rust
#[tokio::test]
async fn admin_defines_and_drops_a_view() {
    // seed a base table; POST /admin/views with the body above → 200/201;
    // catalog.get_view(&view) is Some; re-POST → 409;
    // POST with missing base → 404; with bad predicate column → 422;
    // DELETE /admin/views/gov/customers_eu → 200; get_view → None;
    // non-admin subject → 403 (mirror the existing admin-gate test idiom).
}
```

Write it out fully following the file's existing helpers. NOTE: `admin_management.rs` runs against `MemoryControlPlane` with **no catalog seeding** — seed the base table via the memory inherent `seed_catalog(&table, &cols, &row_batches)` (see `memory/tests/catalog.rs` for the exact column-tuple signature); Task 2 gives memory `define_view`/`get_view`, so the whole test is implementable in that harness. **Two drift guards:** adding an `/admin/*` route requires updating BOTH `runtime/tests/openapi_fragments.rs` and `query-api/tests/openapi.rs`; the second failure only shows in the full `//src/...` sweep — update both in this task.

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/services/runtime:admin_management` (confirm the target name with `rg 'admin_management' src/services/runtime/BUCK`)
Expected: FAIL — 404 on the unrouted path.

- [ ] **Step 3: Implement the routes**

In `runtime/src/admin.rs`: a `DefineViewBody { view: TableRefBody, base: TableRefBody, predicate: Option<RowFilter>, columns: Option<Vec<String>> }` deserialize struct (reuse whatever table-ref body type the file already has, or `TableRef` directly — it derives `Deserialize`); handler builds `ViewDef` and calls `st.cp.catalog().define_view(...)`; DELETE handler calls `drop_view`. Wire both into the admin router next to the transforms routes (`admin.rs:1356+` shows the handler pattern), behind the same admin gate, and add both `#[utoipa::path]` operations to `AdminApiDoc`'s `paths(...)`. Update both drift-guard TEST files with the new `(method, path)` entries, mirroring how the transforms routes appear there.

- [ ] **Step 4: Run to verify it passes**

Run: `buck2 test --console none //src/services/runtime:admin_management` then `buck2 build -v0 --console none //src/...` (catches the second drift guard early), then `buck2 test --console none //src/services/query-api:openapi` if such a target exists (find with `rg 'openapi' src/services/query-api/BUCK`).
Expected: green.

- [ ] **Step 5: Commit**

```bash
git add src/services/runtime src/services/query-api
git commit -m "feat(runtime): admin define/drop view routes with OpenAPI coverage"
```

---### Task 6: View-aware writes in query-api (`action.rs`)

**Files:**
- Modify: `src/services/query-api/src/action.rs`
- Create: `src/services/query-api/tests/view_write_e2e.rs`
- Modify: `src/services/query-api/BUCK`
- Modify: `src/services/query-api/tests/e2e_support.rs` (only if a shared helper is genuinely reusable — e.g. a `define_view` seed helper; per-test graph topologies stay local)

**Interfaces:**
- Consumes: `Catalog::get_view` (via `deps.cp.catalog()`), `write_filter::eval` (`pub`, three-valued), the existing `run_insert` (`action.rs:687`, its `write_object` call at `:776-794`), `run_mutate` (`action.rs:1050-1252`), `enforce_mutate_policy` legs (`action.rs:931-983`), `check_conformance_steps` (`action.rs:141`, clash check inside at `:160-176`), `coalesce_appends`, `deps.action_engine.write_object/write_delta` (physical, take `&TableRef`).
- Produces: writes through a view-bound type land in the **base** table gated by the view predicate. New error surface: reuse `ActionError::Forbidden` for a row outside the writer's view (no new variant unless the file's error mapping makes a dedicated message trivial — prefer `Forbidden`, matching the ACL row-filter denial posture).

Semantics to implement (from the spec):
1. **Resolve once per step target:** where the code does `deps.cp.ontology().get_type(&step.target)` and later uses `target.table`, resolve `deps.cp.catalog().get_view(&target.table)` alongside. Carry `(base: TableRef, view: Option<ViewDef>)`. ALL engine RPCs (`write_object`, `write_delta`, `write_steps` `StepLand.table`, `current_inline_version`) use `base`. The targeted mutate read (`select_object_sql`) keeps using `target.table` (the VIEW name) — the engine expands it, so out-of-view rows are invisible to targeting (leg-1 for free: 0 rows → NotFound).
2. **Insert gate:** after `check_write_policy` and constraint validation, if `view.predicate` is `Some`, build the same `BTreeMap<&str, &SqlValue>` over the FULL row (`full_columns`/`full_values`) and require `eval(pred, &row) == Some(true)`, else `Forbidden`. (NULL/unknown ⇒ reject — three-valued fail-closed, same as ACL.)
3. **Mutate gates:** in `enforce_mutate_policy` (or its caller `mutate_governance`), when the target is view-bound: treat the view predicate as an additional row filter for leg 1 (existing row must satisfy it — already guaranteed by the view-scoped read, but keep the explicit check as belt-and-braces; it is one `eval` call) and leg 3 (UPDATE's post-image must still satisfy it — this is the load-bearing "no writing a row out of your own view" rule).
4. **Multi-step clash:** in `check_conformance_steps`, compare **resolved base** tables — two steps whose targets resolve to the same physical base clash even when their `TableRef`s differ (view vs base vs sibling view). Resolve the targets' views once up front (the function is currently sync over pre-fetched `targets`; fetch the `Option<ViewDef>` per target where `targets` is built and pass a parallel slice, keeping this function pure).
5. **Append coalescing:** `coalesce_appends` keys steps by `target.table` — leave the KEY as the type's bound ref (so two different views never coalesce even over one base — their predicates differ), but the emitted `StepLand.table` must be the **base**.
6. **Projected-columns rule needs NO runtime gate:** spec §5's "insert may only touch projected columns" falls out structurally — bind (Task 7's `bind_through_view_validates_against_projected_schema`) validates type properties against the view's projected schema, and action columns come from type properties. Do not add a redundant per-write check; this sentence is the record of where the rule lives.

- [ ] **Step 1: Write the failing e2e test**

Create `src/services/query-api/tests/view_write_e2e.rs`. Setup: seed a physical `main.widget` table + define a view `gov.widget_eu = main.widget WHERE region='EU'` + bind a type over the view (use `define_widget`-style `ObjectType::build` with `table: tref("gov", "widget_eu")`) + `grant_writer_role` on that type. Reuse `e2e_support` (`setup_iceberg` seeds the customer graph — for this test prefer a local seed like `define_widget`'s, with a `region` column added). Tests:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn insert_inside_view_lands_in_base_and_reads_back() {
    // POST the create action with region='EU' → 200; read back through the
    // view-bound type (GET /objects/<type>) → row present; a direct
    // base-table SQL count (via the fixture engine) shows the row landed in
    // main.widget, and NO mirror table gov.widget_eu exists
    // (iceberg_mirror::live_table_id(gov, widget_eu) is None).
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_escaping_view_predicate_is_forbidden() {
    // region='US' → 403; base row count unchanged.
}

#[tokio::test(flavor = "multi_thread")]
async fn patch_moving_row_out_of_view_is_forbidden() {
    // create in-view row; PATCH set region='US' → 403; PATCH set amount=5
    // (stays in view) → 200.
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_targets_only_in_view_rows() {
    // seed one EU + one US row directly in the base; delete-by-identity of
    // the US row through the view-bound type → 404 (invisible); the EU row
    // deletes fine.
}
```

Write the bodies with the file's action-POST idiom (`post_action_text` / `seed_widget_create_then_update` show the shape). BUCK: a `loom_fixture_test` target `view-write-e2e` depending on `:query-api`, `:e2e-support`, `//src/control-plane/core:core`, `//src/control-plane/postgres:postgres`, `//third-party:tokio`, `//third-party:serde_json`, `//third-party:axum` (match `bind-read-e2e`'s dep list).

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/services/query-api:view-write-e2e`
Expected: FAIL — first test: the insert either errors (engine refuses a write to an unknown physical ref) or lands a NEW mirror table `gov.widget_eu` (the assert on `live_table_id` catches it).

- [ ] **Step 3: Implement (per the semantics block above)**

- [ ] **Step 4: Run to verify it passes**

Run: `buck2 test --console none //src/services/query-api:view-write-e2e`
Then the action regression suite: `buck2 test --console none //src/services/query-api:action-e2e` plus the sibling action/mutate targets (`action-mapping-e2e`, `action-computed-e2e`, `update-delete-*`, `mutate-phases` — confirm the full set via `rg 'action|mutate' src/services/query-api/BUCK`).
Expected: green.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api
git commit -m "feat(query-api): view-aware typed writes — base lowering + predicate gates"
```

---

### Task 7: `/datasets` routes, governed read e2e, lineage visibility

**Files:**
- Modify: `src/services/query-api/src/http.rs` (list/get dataset handlers)
- Modify: `src/services/query-api/src/openapi.rs` (response-shape docs)
- Create: `src/services/query-api/tests/view_read_e2e.rs`
- Modify: `src/services/query-api/tests/datasets_routes.rs` (extend for view listing/preview)
- Modify: `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `Catalog::{list_views, get_view}`; `DatasetVisibility::is_table_readable` (works on view refs unchanged — Table grant on the view ∨ types bound to the view via `types_backed_by`, whose map keys on `ObjectType.table` = the view ref); engine expansion (Task 4) which makes `dataset_preview`'s `SELECT *` and all object reads view-correct with **no handler change**; `view_definition_event` (Task 2) for the lineage closure.
- Produces: `list_datasets` unions views (each `{schema, name, project, updated, kind: "view", base: {schema, name}}`; physical entries gain `kind: "table"`); `get_dataset` adds the same `kind`/`base` fields for views (snapshot + projected columns already correct via catalog delegation).

- [ ] **Step 1: Write the failing tests**

Extend `tests/datasets_routes.rs` (it already covers the ACL-gated list/get/preview — follow its seed + `get` idioms):

```rust
#[tokio::test(flavor = "multi_thread")]
async fn views_list_with_kind_and_base_under_their_own_grant() {
    // base main.customers + view gov.customers_eu; role A: Table grant on the
    // VIEW only → /datasets lists exactly the view entry, kind == "view",
    // base == {"schema":"main","name":"customers"}; the base is NOT listed.
    // role B: Table grant on the BASE only → base listed (kind == "table"),
    // view NOT listed. Admin sees both.
}

#[tokio::test(flavor = "multi_thread")]
async fn view_get_dataset_returns_projected_columns() {
    // GET /datasets/gov/customers_eu (granted) → 200, kind "view",
    // columns == projected subset, snapshot_id == the base's current.
}
```

(The preview-narrowing test does NOT belong here: this harness wires `StubServing`, whose `fetch_rows` returns empty `Rows` — engine expansion never runs and a "only EU rows" assertion would pass vacuously. It goes in `view_read_e2e.rs` below, on the real fixture engine.)

Create `tests/view_read_e2e.rs` — the acceptance-criteria e2e:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn disjoint_view_grants_read_disjoint_slices() {
    // one base (region EU/US rows), two views (eu/us), two types bound to
    // them, two roles with Read on exactly one type each.
    // Each role: GET /objects/<their type> → only their rows;
    // GET /objects/<sibling type> → 403; and neither role can read the base
    // through /datasets (404 — the existence-oracle posture from the
    // datasets_routes suite).
}

#[tokio::test(flavor = "multi_thread")]
async fn base_grant_does_not_leak_views_and_vice_versa() {
    // Table-grant on base: /datasets/gov/customers_eu → 404; Type-grant on a
    // view-bound type: base dataset stays 404. (Exact-match ACL decoupling.)
}

#[tokio::test(flavor = "multi_thread")]
async fn view_preview_is_predicate_and_projection_narrowed() {
    // Fixture engine (not the stub): seed EU+US rows; GET
    // /datasets/gov/customers_eu/preview returns only EU rows and only
    // projected columns; preview of the base (base-granted role) returns all.
}

#[tokio::test(flavor = "multi_thread")]
async fn link_traversal_through_view_bound_type_stays_in_view() {
    // Acceptance criterion 2 names link traversal. Bind a second type over a
    // second view (or the base) and define an FK link whose endpoint type is
    // view-bound (bind_link validates backing columns against the projected
    // schema — include the FK column in the projection). GET
    // /objects/{from}/links/{link} through the view-bound endpoint returns
    // only rows inside the view; a target row outside the endpoint view's
    // predicate does not appear.
}

#[tokio::test(flavor = "multi_thread")]
async fn acl_policy_on_view_bound_type_composes_with_view_predicate() {
    // view predicate region='EU'; add a row-filter policy amount > 10 on the
    // view-bound type (grant_read_filtered) → reads return EU AND amount>10;
    // a mask on a projected column (grant_read_columns) returns '***'.
}

#[tokio::test(flavor = "multi_thread")]
async fn bind_through_view_validates_against_projected_schema() {
    // ingest::bind::bind() a type whose properties ⊆ projection → Ok;
    // a type naming a column OUTSIDE the projection → DoesNotConform.
    // (bind-read-e2e shows how to drive bind() in-process.)
}

#[tokio::test(flavor = "multi_thread")]
async fn lineage_shows_base_to_view_edge_under_view_grant() {
    // /lineage downstream of the base dataset (as a subject who can read
    // both) contains the view node; a subject with only the view grant
    // sees the view node and the base is cut per the closure's ACL-cut
    // posture (mirror the lineage_filter suite's assertions).
}
```

BUCK: `loom_fixture_test` target `view-read-e2e` (deps: `:query-api`, `:e2e-support`, `//src/services/ingest:ingest`, `//src/control-plane/core:core`, `//src/control-plane/postgres:postgres`, `//third-party:tokio`, `//third-party:serde_json`, `//third-party:axum`).

- [ ] **Step 2: Run to verify the new tests fail**

Run: `buck2 test --console none //src/services/query-api:view-read-e2e //src/services/query-api:datasets-routes` (confirm the datasets target name in BUCK)
Expected: the listing/kind tests FAIL (views absent from `/datasets`); several read-path tests may already PASS (engine expansion + catalog delegation carry them) — that is expected and fine; the failing set drives the handler change.

- [ ] **Step 3: Implement the handler changes**

In `http.rs` `list_datasets`: add `"kind": "table"` to the physical entries; after the physical loop, fetch `catalog.list_views(PageReq::unbounded())`, gate each with `vis.is_table_readable(&subject.0, &v.view)`, resolve `updated` via `catalog.current_snapshot(&v.view)` (delegates to base), and push

```rust
        datasets.push(serde_json::json!({
            "schema": v.view.schema,
            "name": v.view.name,
            "project": v.view.schema,
            "updated": updated,
            "kind": "view",
            "base": { "schema": v.base.schema, "name": v.base.name },
        }));
```

In `get_dataset`: after the existing readable-gate + snapshot/columns resolution (both already view-correct), add `kind`/`base` by checking `catalog.get_view(&table_ref)`. `dataset_preview` needs **no change**. Update the utoipa doc structs in `openapi.rs` for the two new fields (`kind: String`, `base: Option<TableRefView>`).

- [ ] **Step 4: Run to verify it passes**

Run: `buck2 test --console none //src/services/query-api:view-read-e2e //src/services/query-api:datasets-routes //src/services/query-api:view-write-e2e`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api
git commit -m "feat(query-api): views in /datasets + governed view-read e2e suite"
```

---

### Task 8: Register close + capability docs + FUTURE residues

**Files:**
- Modify: `docs/ROADMAP.md` (remove `#road-catalog-views`)
- Modify: `docs/FUTURE.md` (new deferral entries)
- Modify: `docs/system-capabilities/control-plane.md`, `docs/system-capabilities/query-api.md`, `docs/system-capabilities/engine.md` (document the landed capability per subsystem)

Follow the `loom-docs-update` skill for the mechanics. Content requirements:

- Remove the `#road-catalog-views` entry from ROADMAP (registers carry open work only; name the PR in the close).
- Add FUTURE entries (all `from:2026-07-13-catalog-views-design`, `spec:-`, `status:deferred`, area:catalog unless noted):
  - `#fut-view-nesting` — view-over-view (base must be physical in v1).
  - `#fut-view-stream-subscribe` — stream/CDC subscribe on a view (area:stream).
  - `#fut-view-sql-derived` — arbitrary-SQL (join/aggregate) read-only views.
  - `#fut-view-pushdown-stats` — predicate-pushdown statistics for view scans.
  - Annotate the existing `#fut-fgac-subject-attribute` prose: views now cover nameable static subsets; per-subject dynamic filtering remains the deferred residue (do not close it).
- `bash tools/docs.sh validate` green.

- [ ] **Step 1: Make the register + capability edits**
- [ ] **Step 2: Validate**

Run: `bash tools/docs.sh validate` and `buck2 run //tools:prek -- run --all-files`
Expected: both clean.

- [ ] **Step 3: Commit**

```bash
git add docs
git commit -m "docs(catalog): close road-catalog-views — capability docs + deferred residues"
```

---

## Final verification (before the PR)

1. Full sweep: `buck2 test --console none //src/...` — green (pre-push fixture flakes: re-run the failing target once in isolation before diagnosing).
2. Metric gate (advisory): `/loom-complexity diff` and `/loom-duplication diff` — fix or justify any new hotspot (cc > 15, cognitive > 15, MI < 20, SLOC > 100) or new ≥20-line cross-file duplication in the PR body. NOTE the multi-file `-p` flag quirk: repeat `-p` per changed path.
3. Lease-check then push: `git ls-remote origin work/road-catalog-views` — remote tip must be an ancestor of local; open the PR from `work/road-catalog-views` titled `feat(catalog): catalog views — dataset/type decoupling; close road-catalog-views`.
