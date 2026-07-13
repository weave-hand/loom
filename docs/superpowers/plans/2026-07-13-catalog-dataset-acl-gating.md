# Per-dataset catalog ACL gating Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the three catalog reads (`list_datasets`, `get_dataset`, `dataset_preview`) enforce the same per-dataset readable predicate that `/lineage` already uses, and move the transform-output read-grant from the UI into the server-side define path — closing `iss-catalog-lineage-acl-asymmetry`.

**Architecture:** Lift the readable predicate (Table∨backing-Type `Acl::check`, plus the lazy `types_backed_by` map) out of `LineageVisibility` into a reusable `DatasetVisibility` governor in the same crate; `LineageVisibility` delegates to it (behavior-preserving), the catalog handlers consume it. Point reads on an unreadable/nonexistent dataset return one canonical 404 (no existence oracle); `list_datasets` filters. Separately, `POST /admin/transforms` grants the reserved `admin` role `Read` on a physical output table as part of the define flow, and the UI's best-effort self-grant is deleted.

**Tech Stack:** Rust, axum, buck2 (`rust_test` targets only — never inline `#[test]`), the in-memory control-plane fake for route tests, DataFusion serving stub.

## Global Constraints

- Strict clippy (whole `pedantic` + `restriction` groups enforced on lib/bin code); silence locally with `#[expect(lint, reason = "...")]` (never bare `#[allow]`; `reason` is mandatory). Test code is exempt from the panic-safety lints via the `loom_rust_test` wrapper.
- **Tests are `rust_test` / `loom_fixture_test` integration targets only** — put every test in a `tests/<name>.rs` file wired as its own target in the crate `BUCK`. buck2 never runs inline `#[cfg(test)]` modules; the `no-inline-tests` prek hook fails the build if a first-party `src/**.rs` contains a `#[test]`/`#[tokio::test]`.
- Reuse `//src/services/query-api:e2e-support` helpers (`grant_read`, `subject_with_role`, `get`, `setup_iceberg`, `tref`) rather than copying them; extend the library for new shared helpers.
- Run `buck2 run //tools:prek -- run --all-files` before every commit; markdown files end with exactly one trailing newline and no trailing whitespace.
- Build/test commands: `buck2 build -v0 --console none //src/...` (silent on success) and `buck2 test --console none //src/...` (prints only the pass/fail summary). For a single target, name it explicitly, e.g. `buck2 test --console none //src/services/query-api:dataset-acl`.
- Every registered ACL grant/check goes through the `control_plane_core::{Acl, PolicyTarget, Action, Decision, Effect}` surface; `Acl::check` is exact-match by design (no Type→Table resolution inside core).

---

### Task 1: Extract the shared `DatasetVisibility` governor

Lift the readable predicate out of `LineageVisibility` into a new `dataset_acl` module. Behavior-preserving refactor: the existing `//src/services/query-api:lineage-visibility` suite must stay green (it pins that `LineageVisibility`'s public API is unchanged). Add a focused unit test for the new governor.

**Files:**
- Create: `src/services/query-api/src/dataset_acl.rs`
- Modify: `src/services/query-api/src/lib.rs` (register `pub mod dataset_acl;`)
- Modify: `src/services/query-api/src/lineage_filter.rs` (delegate to `DatasetVisibility`, prune moved code + now-unused imports)
- Create: `src/services/query-api/tests/dataset_acl.rs`
- Modify: `src/services/query-api/BUCK` (add the `dataset-acl` test target)

**Interfaces:**
- Produces:
  - `pub struct DatasetVisibility<'a>` with:
    - `pub fn new(acl: &'a (dyn Acl + Send + Sync), ontology: &'a (dyn Ontology + Send + Sync), bridge: &'a LineageNaming) -> Self`
    - `pub async fn is_readable(&self, subject: &SubjectId, r: &DatasetRef) -> Result<bool, ControlPlaneError>` — bridge-resolving (for a lineage-graph `DatasetRef`)
    - `pub async fn is_table_readable(&self, subject: &SubjectId, table: &TableRef) -> Result<bool, ControlPlaneError>` — Table∨backing-Type check (for catalog reads that already hold a `TableRef`)
  - `local_naming()` stays where it is in `lineage_filter.rs` (unchanged; both consumers use it).
- Consumes: `control_plane_core::{Acl, Action, ControlPlaneError, DatasetRef, Decision, Ontology, PageReq, PolicyTarget, SubjectId, TableRef, TypeName}`; `lineage_naming::{LineageNaming, ResolvedDataset}`.

- [ ] **Step 1: Write the failing unit test**

Create `src/services/query-api/tests/dataset_acl.rs`:

```rust
//! Unit tests for the shared `DatasetVisibility` readable predicate:
//! Table grant → readable; a backing-Type grant → readable via the fallback;
//! no grant → not readable. Memory control plane (ACL + ontology) + a real
//! file-backed naming bridge; no Postgres, RE-eligible.

use control_plane_core::{
    Acl, Action, ControlPlane, Effect, ObjectType, Ontology, PolicyTarget, RoleId, SubjectId,
    TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::dataset_acl::DatasetVisibility;
use query_api::lineage_filter::local_naming;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.into(),
        name: name.into(),
    }
}

/// A subject in a fresh role; returns (subject, role). The role starts with no grants.
async fn subject_in_role(cp: &MemoryControlPlane, name: &str) -> (SubjectId, RoleId) {
    let subj = SubjectId(name.into());
    let role = RoleId(format!("{name}-role"));
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    (subj, role)
}

#[tokio::test(flavor = "multi_thread")]
async fn table_grant_makes_table_readable() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    let (subj, role) = subject_in_role(&cp, "reader").await;
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Table(tref("main", "events")),
        Effect::Allow,
    )
    .await
    .unwrap();

    let bridge = local_naming();
    let vis = DatasetVisibility::new(cp.acl(), cp.ontology(), bridge.as_ref());
    assert!(
        vis.is_table_readable(&subj, &tref("main", "events"))
            .await
            .unwrap()
    );
    // A different table the subject has no grant on stays unreadable.
    assert!(
        !vis.is_table_readable(&subj, &tref("main", "other"))
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn backing_type_grant_makes_table_readable_via_fallback() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    // Define a type backed by main.events; grant Read on the TYPE only. Exact
    // `ObjectType` field set (verified against src/control-plane/core/src/ontology.rs):
    // name, properties, derived, table, identity: Option<String>, version: Option<String>.
    // The fallback only reads `ty.table`/`ty.name`, so empty props / no identity suffice.
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Event".into()),
            properties: vec![],
            derived: vec![],
            table: tref("main", "events"),
            identity: None,
            version: None,
        })
        .await
        .unwrap();
    let (subj, role) = subject_in_role(&cp, "typed").await;
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName("Event".into())),
        Effect::Allow,
    )
    .await
    .unwrap();

    let bridge = local_naming();
    let vis = DatasetVisibility::new(cp.acl(), cp.ontology(), bridge.as_ref());
    // No Table grant, but the backing-Type grant makes the table readable.
    assert!(
        vis.is_table_readable(&subj, &tref("main", "events"))
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_grant_is_not_readable() {
    let cp = MemoryControlPlane::new(std::time::Duration::from_millis(300));
    let (subj, _role) = subject_in_role(&cp, "nobody").await;
    let bridge = local_naming();
    let vis = DatasetVisibility::new(cp.acl(), cp.ontology(), bridge.as_ref());
    assert!(
        !vis.is_table_readable(&subj, &tref("main", "events"))
            .await
            .unwrap()
    );
}
```

The `ObjectType` field set is confirmed (`src/control-plane/core/src/ontology.rs:40-54`): `name: TypeName`, `properties: Vec<PropertyDef>`, `derived: Vec<DerivedPropertyDef>`, `table: TableRef`, `identity: Option<String>`, `version: Option<String>` — all six are required in a struct literal. A verbatim reference literal is `src/services/query-api/tests/as_of_objects_e2e.rs:79-88`.

- [ ] **Step 2: Add the BUCK target and run the test to verify it fails to compile**

Add to `src/services/query-api/BUCK` (mirror the `lineage-visibility` target, which has the same deps):

```python
rust_test(
    name = "dataset-acl",
    crate = "dataset_acl",
    srcs = ["tests/dataset_acl.rs"],
    crate_root = "tests/dataset_acl.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:tokio",
    ],
)
```

Run: `buck2 test --console none //src/services/query-api:dataset-acl`
Expected: FAIL — `query_api::dataset_acl` does not exist yet.

- [ ] **Step 3: Create the `dataset_acl` module**

Create `src/services/query-api/src/dataset_acl.rs`:

```rust
//! Shared per-dataset read governor: the readable predicate that both the
//! `/lineage` closure filter (`lineage_filter::LineageVisibility`) and the catalog
//! reads (`/datasets*`) enforce. One resolution, one soundness argument, two
//! consumers. The Table→Type fallback lives here because loom governs by type but
//! lineage emitters (and the mirror catalog) key physical datasets by table ref,
//! and `Acl::check` is deliberately exact-match.

use control_plane_core::{
    Acl, Action, ControlPlaneError, DatasetRef, Decision, Ontology, PageReq, PolicyTarget,
    SubjectId, TableRef, TypeName,
};
use lineage_naming::{LineageNaming, ResolvedDataset};

/// Borrows the ACL, the ontology, and the deployment naming bridge; owns a
/// per-request lazy `(schema, table) → backing types` map for the Table→Type
/// fallback. Construct one per request (the lazy map is not shared across requests).
pub struct DatasetVisibility<'a> {
    acl: &'a (dyn Acl + Send + Sync),
    ontology: &'a (dyn Ontology + Send + Sync),
    bridge: &'a LineageNaming,
    /// `TableRef` derives no `Ord`, so the key is its `(schema, name)` pair.
    table_types: tokio::sync::OnceCell<std::collections::BTreeMap<(String, String), Vec<TypeName>>>,
}

impl<'a> DatasetVisibility<'a> {
    /// A fresh per-request governor.
    #[must_use]
    pub fn new(
        acl: &'a (dyn Acl + Send + Sync),
        ontology: &'a (dyn Ontology + Send + Sync),
        bridge: &'a LineageNaming,
    ) -> Self {
        Self {
            acl,
            ontology,
            bridge,
            table_types: tokio::sync::OnceCell::new(),
        }
    }

    /// Classify a lineage-graph ref's readability for `subject`, resolving it through
    /// the naming bridge first. `Type` → `Acl::check(Read)`. `Table` → the Table∨Type
    /// fallback (see `is_table_readable`). `Unresolvable` (owned namespace, unparseable
    /// name) → **never readable** (denied and cut: a bridge/mapping gap can only narrow,
    /// never widen, disclosure). `External` (a genuinely foreign datasource) →
    /// default-allow: it carries no loom-ACL'd data and is a source leaf.
    pub async fn is_readable(
        &self,
        subject: &SubjectId,
        r: &DatasetRef,
    ) -> Result<bool, ControlPlaneError> {
        let table = match self.bridge.resolve(r) {
            ResolvedDataset::Table(t) => t,
            ResolvedDataset::Type(ty) => {
                return self.allows_read(subject, &PolicyTarget::Type(ty)).await;
            }
            ResolvedDataset::Unresolvable(_) => return Ok(false),
            ResolvedDataset::External(_) => return Ok(true),
        };
        self.is_table_readable(subject, &table).await
    }

    /// Readable iff the Table target allows OR a Read grant allows any ontology type
    /// *backed by* that table. The widening is sound because a type-Read grant already
    /// discloses the backing table's rows through the governed object read — seeing the
    /// table's catalog metadata / lineage node discloses strictly less. Allow-oriented:
    /// an explicit Table Deny does not veto a type Allow (consistent with the object
    /// read, which consults only the Type target).
    pub async fn is_table_readable(
        &self,
        subject: &SubjectId,
        table: &TableRef,
    ) -> Result<bool, ControlPlaneError> {
        if self
            .allows_read(subject, &PolicyTarget::Table(table.clone()))
            .await?
        {
            return Ok(true);
        }
        for ty in self.types_backed_by(table).await? {
            if self
                .allows_read(subject, &PolicyTarget::Type(ty.clone()))
                .await?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// One `Acl::check(Read)` folded to a bool. Errors propagate (never disclosed as
    /// Allow).
    async fn allows_read(
        &self,
        subject: &SubjectId,
        target: &PolicyTarget,
    ) -> Result<bool, ControlPlaneError> {
        Ok(self.acl.check(subject, Action::Read, target).await? == Decision::Allow)
    }

    /// The ontology types backed by `table`, from a per-request map built lazily on the
    /// first Table-target miss with one unbounded `list_types` read (ontology metadata is
    /// deployment-sized). Requests whose Table checks all allow never pay for it.
    async fn types_backed_by(
        &self,
        table: &TableRef,
    ) -> Result<&[TypeName], ControlPlaneError> {
        let map = self
            .table_types
            .get_or_try_init(|| async {
                let types = self.ontology.list_types(PageReq::unbounded()).await?;
                let mut map: std::collections::BTreeMap<(String, String), Vec<TypeName>> =
                    std::collections::BTreeMap::new();
                for ty in types.items {
                    map.entry((ty.table.schema, ty.table.name))
                        .or_default()
                        .push(ty.name);
                }
                Ok::<_, ControlPlaneError>(map)
            })
            .await?;
        Ok(map
            .get(&(table.schema.clone(), table.name.clone()))
            .map_or(&[], Vec::as_slice))
    }
}
```

Register it in `src/services/query-api/src/lib.rs` — add `pub mod dataset_acl;` in alphabetical position (immediately before `pub mod dataset_preview;` at line 10):

```rust
pub mod dataset_acl;
pub mod dataset_preview;
```

- [ ] **Step 4: Run the new test to verify it passes**

Run: `buck2 test --console none //src/services/query-api:dataset-acl`
Expected: PASS (3 tests).

- [ ] **Step 5: Refactor `LineageVisibility` to delegate**

In `src/services/query-api/src/lineage_filter.rs`:

1. Replace the struct fields — drop `acl`, `ontology`, `bridge`, `table_types`; add a `dv: crate::dataset_acl::DatasetVisibility<'a>`. Keep `lineage` and `scan_cap`:

```rust
pub struct LineageVisibility<'a> {
    dv: crate::dataset_acl::DatasetVisibility<'a>,
    lineage: &'a (dyn Lineage + Send + Sync),
    scan_cap: usize,
}
```

2. `new` builds the `DatasetVisibility` (signature unchanged — the four params stay so all call sites compile untouched):

```rust
#[must_use]
pub fn new(
    acl: &'a (dyn Acl + Send + Sync),
    lineage: &'a (dyn Lineage + Send + Sync),
    ontology: &'a (dyn Ontology + Send + Sync),
    bridge: &'a LineageNaming,
) -> Self {
    LineageVisibility {
        dv: crate::dataset_acl::DatasetVisibility::new(acl, ontology, bridge),
        lineage,
        scan_cap: LINEAGE_FILTER_SCAN_CAP,
    }
}
```

`with_scan_cap` is unchanged.

3. Delete the moved methods from `LineageVisibility`: `is_readable`, `allows_read`, `types_backed_by` (they now live on `DatasetVisibility`). Keep `one_hop`, `visible_closure`, `visible_upstream`, `visible_downstream`, `redact_events`, `readable_only`.

4. Rewire the two call sites that used `self.is_readable`:
   - In `visible_closure`, `if !self.is_readable(subject, seed).await?` → `if !self.dv.is_readable(subject, seed).await?` (the `?` converts `ControlPlaneError` → `LineageVisibilityError` via the existing `From` impl). Same for the frontier check `if self.is_readable(subject, &nb).await?` → `self.dv.is_readable(subject, &nb).await?`.
   - In `readable_only`, `if self.is_readable(subject, &r).await?` → `self.dv.is_readable(subject, &r).await?`.

5. Prune now-unused imports from the `use control_plane_core::{...}` line: `Acl` and `Lineage` and `Ontology` are still used (in `new`'s signature / the `lineage` field); `Action`, `Decision`, `PolicyTarget`, `TypeName`, `PageReq` are no longer used here (they moved to `dataset_acl`). Remove exactly the ones the compiler flags — run the build and delete each `unused_imports` warning's symbol. `LineageNaming`/`ResolvedDataset` from `lineage_naming` — `LineageNaming` is still used (in `new` and `local_naming`); `ResolvedDataset` is no longer used here, remove it.

- [ ] **Step 6: Run the lineage suite + clippy to verify behavior preservation**

Run: `buck2 test --console none //src/services/query-api:lineage-visibility //src/services/query-api:lineage-acl-e2e //src/services/query-api:dataset-acl`
Expected: PASS (all three green — the lineage suites prove the delegation is byte-for-byte behavior-preserving).

Run: `buck2 build --console none '//src/services/query-api:query-api[clippy.txt]'` then read the printed path and confirm it is empty (clean).
Expected: empty `clippy.txt` (no lint regressions from the pruned imports).

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/dataset_acl.rs src/services/query-api/src/lib.rs \
        src/services/query-api/src/lineage_filter.rs \
        src/services/query-api/tests/dataset_acl.rs src/services/query-api/BUCK
git commit -m "refactor(query-api): extract DatasetVisibility governor from LineageVisibility"
```

---

### Task 2: Gate `list_datasets`

`list_datasets` filters its result to refs the subject may read, via `DatasetVisibility::is_table_readable`. A subject with no grants sees an empty list; page shape unchanged.

**Files:**
- Modify: `src/services/query-api/src/http.rs` (the `list_datasets` handler, ~line 234)
- Modify: `src/services/query-api/tests/datasets_routes.rs` (grant the existing positive test's subject; add negative + fallback tests)
- Modify: `src/services/query-api/BUCK` (the `datasets-routes` target may need `//third-party:tokio` already present — verify deps)

**Interfaces:**
- Consumes: `DatasetVisibility::new` / `is_table_readable` (Task 1); `AppState.naming`, `st.cp.acl()`, `st.cp.ontology()`; the `Subject` extractor (`subject.0: SubjectId`).
- Produces: a gated `list_datasets` (200 with only readable rows).

- [ ] **Step 1: Write the failing tests (route-level, memory fake)**

In `src/services/query-api/tests/datasets_routes.rs`, the existing tests inject `Subject(SubjectId("analyst"))` with no grants — those will break once gating lands, so update them and add negatives. First adjust the shared `seeded()` helper to also define a role for `analyst` and add a helper to grant a Table read, and update the existing positive tests to grant before asserting. Add these new tests:

```rust
// Add to the imports at the top (SubjectId, TableRef already imported):
// use control_plane_core::{Acl, Action, Effect, ObjectType, Ontology, PolicyTarget, RoleId, TypeName};
// (Acl/Ontology are needed because grant/define_type are trait methods invoked on the
//  concrete MemoryControlPlane receiver — see the same note in lineage_visibility.rs.)

/// Give `analyst` a role and a Table Read grant on main.events so the positive
/// route tests still see the dataset once gating is enforced.
async fn grant_analyst_table(cp: &MemoryControlPlane) {
    let subj = SubjectId("analyst".into());
    let role = RoleId("analyst-role".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Table(TableRef {
            schema: "main".into(),
            name: "events".into(),
        }),
        Effect::Allow,
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn datasets_list_is_empty_for_ungranted_subject() {
    let (cp, _) = seeded();
    let app = app(cp);
    let (status, json) = get(&app, "/datasets").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["datasets"].as_array().unwrap().len(), 0);
}

/// Spec Testing "Type grant (backing table listed)" at the route level: a Read grant
/// on a TYPE backed by main.events makes the backing dataset appear in the list — the
/// lineage-symmetry case, exercised through the fallback in `is_table_readable`.
#[tokio::test(flavor = "multi_thread")]
async fn type_grant_lists_backing_table() {
    let (cp, _) = seeded();
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("Event".into()),
            properties: vec![],
            derived: vec![],
            table: TableRef {
                schema: "main".into(),
                name: "events".into(),
            },
            identity: None,
            version: None,
        })
        .await
        .unwrap();
    let subj = SubjectId("analyst".into());
    let role = RoleId("analyst-role".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(
        &role,
        Action::Read,
        PolicyTarget::Type(TypeName("Event".into())),
        Effect::Allow,
    )
    .await
    .unwrap();

    let app = app(cp);
    let (status, json) = get(&app, "/datasets").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["datasets"].as_array().unwrap().len(), 1);
    assert_eq!(json["datasets"][0]["name"], "events");
}
```

The `type_grant_lists_backing_table` test needs `ObjectType`, `TypeName`, and the `Ontology` trait in scope — add them to the test file's `control_plane_core` import (`Ontology` because `cp.ontology().define_type(..)` is a trait method surfaced via the `&dyn Ontology` accessor's concrete use here; if the compiler reports it unused, drop it).

`MemoryControlPlane` grant methods (`define_subject`/`define_role`/`assign_role`/`grant`) are `Acl`/trait methods on the concrete receiver, so add `use control_plane_core::{Acl, ...};` to the test file's imports (see the note in `lineage_visibility.rs` about why the trait must be in scope). Update the three existing positive tests (`datasets_lists_the_seeded_table`, `dataset_detail_composes_snapshot_and_columns`, `dataset_preview_returns_sampled_rows`) to call `grant_analyst_table(&cp).await;` after `seeded()` and before building the app — note `seeded()` returns `(cp, latest)` and the app builders take `cp` by value, so grant on the `cp` **before** moving it into `app(cp)`. Restructure each as:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn datasets_lists_the_seeded_table() {
    let (cp, _) = seeded();
    grant_analyst_table(&cp).await;
    let app = app(cp);
    // ... existing assertions unchanged ...
}
```

- [ ] **Step 2: Run to verify the new negative test fails (gating not yet added)**

Run: `buck2 test --console none //src/services/query-api:datasets-routes`
Expected: FAIL — `datasets_list_is_empty_for_ungranted_subject` fails (the ungated handler still returns the seeded row), and the updated positive tests may also fail to compile until the imports are added. Fix compile errors, confirm the negative test is the meaningful failure.

- [ ] **Step 3: Gate the handler**

In `src/services/query-api/src/http.rs`, change `list_datasets` to take the real subject and filter. Rename `_subject: Subject` → `subject: Subject` and insert the readability filter after listing:

```rust
async fn list_datasets(State(st): State<AppState>, subject: Subject) -> axum::response::Response {
    let catalog = st.cp.catalog();
    let page = match catalog.list_tables(PageReq::unbounded()).await {
        Ok(p) => p,
        Err(e) => return internal_error("catalog list_tables fault", e),
    };
    let vis = crate::dataset_acl::DatasetVisibility::new(
        st.cp.acl(),
        st.cp.ontology(),
        st.naming.as_ref(),
    );
    let mut datasets: Vec<serde_json::Value> = Vec::with_capacity(page.items.len());
    for t in &page.items {
        match vis.is_table_readable(&subject.0, t).await {
            Ok(true) => {}
            Ok(false) => continue,
            Err(e) => return cp_read_error("catalog dataset acl fault", e),
        }
        // Best-effort updated-time: a table with no readable snapshot renders "".
        let updated = match catalog.current_snapshot(t).await {
            Ok(s) => s
                .time
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
            Err(_) => String::new(),
        };
        datasets.push(serde_json::json!({
            "schema": t.schema,
            "name": t.name,
            "project": t.schema,
            "updated": updated,
        }));
    }
    Json(serde_json::json!({ "datasets": datasets })).into_response()
}
```

Also update the handler's doc comment (lines ~221-224): it currently says "auth-required but not ACL-gated" — replace with a note that catalog reads are now per-dataset ACL-gated under the same predicate as `/lineage` (Table∨backing-Type Read).

- [ ] **Step 4: Run the route tests to verify they pass**

Run: `buck2 test --console none //src/services/query-api:datasets-routes`
Expected: PASS (the ungranted list is empty; the granted positive test lists the row).

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/src/http.rs src/services/query-api/tests/datasets_routes.rs
git commit -m "feat(query-api): per-dataset ACL gating on GET /datasets"
```

---

### Task 3: Gate `get_dataset` and `dataset_preview` with a canonical 404

Both point reads check readability first; unreadable (or nonexistent-for-this-subject) → one canonical 404 body, indistinguishable from a genuinely nonexistent dataset (no existence oracle).

**Files:**
- Modify: `src/services/query-api/src/http.rs` (`get_dataset` ~line 282, `dataset_preview` ~line 387; add a shared canonical-404 helper)
- Modify: `src/services/query-api/tests/datasets_routes.rs` (unknown-dataset test unchanged in intent; add the oracle body-equality test for get + preview)

**Interfaces:**
- Consumes: `DatasetVisibility::is_table_readable` (Task 1).
- Produces: `get_dataset`/`dataset_preview` return the canonical 404 for unreadable refs.

- [ ] **Step 1: Write the failing oracle tests**

Add to `src/services/query-api/tests/datasets_routes.rs`:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn get_dataset_404_body_is_identical_for_unreadable_and_nonexistent() {
    // Ungranted `analyst`: an existing dataset and a nonexistent one must return the
    // byte-identical 404 (no existence oracle).
    let (cp1, _) = seeded();
    let app_existing = app(cp1);
    let (s_existing, b_existing) = get(&app_existing, "/datasets/main/events").await;

    let (cp2, _) = seeded();
    let app_missing = app(cp2);
    let (s_missing, b_missing) = get(&app_missing, "/datasets/main/no-such-table").await;

    assert_eq!(s_existing, StatusCode::NOT_FOUND);
    assert_eq!(s_missing, StatusCode::NOT_FOUND);
    assert_eq!(b_existing, b_missing);
}

#[tokio::test(flavor = "multi_thread")]
async fn preview_404_body_is_identical_for_unreadable_and_nonexistent() {
    let (cp1, _) = seeded();
    let app_existing = app_canned(cp1);
    let (s_existing, b_existing) = get(&app_existing, "/datasets/main/events/preview").await;

    let (cp2, _) = seeded();
    let app_missing = app_canned(cp2);
    let (s_missing, b_missing) =
        get(&app_missing, "/datasets/main/no-such-table/preview").await;

    assert_eq!(s_existing, StatusCode::NOT_FOUND);
    assert_eq!(s_missing, StatusCode::NOT_FOUND);
    assert_eq!(b_existing, b_missing);
}
```

Note: `get()` in `datasets_routes.rs` parses the body as JSON (`serde_json::from_slice(..).unwrap_or(Null)`); a plain-text 404 body decodes to `Value::Null`, so `b_existing == b_missing` holds as `Null == Null` **only if both are 404 via the same branch**. That is exactly what the canonical helper guarantees. Also add a positive test that a *granted* subject still gets 200 on get + preview:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn granted_subject_reads_get_and_preview() {
    let (cp, _) = seeded();
    grant_analyst_table(&cp).await;
    let app = app_canned(cp);
    let (s_get, _) = get(&app, "/datasets/main/events").await;
    assert_eq!(s_get, StatusCode::OK);
    let (s_prev, prev) = get(&app, "/datasets/main/events/preview?limit=5").await;
    assert_eq!(s_prev, StatusCode::OK);
    assert_eq!(prev["sampled"], serde_json::json!(true));
}
```

(`app_canned` serves the same table detail via the mirror + canned rows; `grant_analyst_table` was added in Task 2.)

- [ ] **Step 2: Run to verify the oracle tests fail**

Run: `buck2 test --console none //src/services/query-api:datasets-routes`
Expected: FAIL — the existing-dataset request currently returns 200 (get) / 200 (preview) for ungranted `analyst`, not 404, so the body-equality assertions fail.

- [ ] **Step 3: Add the canonical 404 helper and the readability guards**

In `src/services/query-api/src/http.rs`, add a shared helper near `cp_read_error`:

```rust
/// The canonical "dataset not visible" 404 — one fixed body shared by the point
/// reads so an unreadable existing dataset is byte-identical to a nonexistent one
/// (no existence oracle; the point-read analog of the lineage seed gate's empty page).
fn dataset_not_found() -> axum::response::Response {
    (StatusCode::NOT_FOUND, "dataset not found").into_response()
}
```

In `get_dataset`: rename `_subject: Subject` → `subject: Subject`, and after building `table_ref` (before `resolve_dataset_snapshot`) insert:

```rust
let vis = crate::dataset_acl::DatasetVisibility::new(
    st.cp.acl(),
    st.cp.ontology(),
    st.naming.as_ref(),
);
match vis.is_table_readable(&subject.0, &table_ref).await {
    Ok(true) => {}
    Ok(false) => return dataset_not_found(),
    Err(e) => return cp_read_error("dataset detail acl fault", e),
}
```

In `dataset_preview`: rename `_subject: Subject` → `subject: Subject`, build the `table_ref` (currently the handler works with `schema`/`table` strings — construct `let table_ref = TableRef { schema: schema.clone(), name: table.clone() };` before the limit parse or reuse the strings), and after the limit parse, before building the SQL, insert the same readability guard returning `dataset_not_found()` on `Ok(false)`. Keep the `SELECT *` semantics for readable datasets unchanged.

Update both handlers' doc comments: `get_dataset`'s responses already document 404 ("Unknown table…"); extend the description to "…or a dataset the caller may not read (indistinguishable — no existence oracle)". `dataset_preview` currently documents no 404 — add `(status = 404, description = "Unknown or unreadable dataset")` to its `responses(...)` and drop the "Coarse-auth (authenticated)" note in favor of "per-dataset ACL-gated (same predicate as /datasets)".

- [ ] **Step 4: Run the route tests to verify they pass**

Run: `buck2 test --console none //src/services/query-api:datasets-routes`
Expected: PASS — oracle body-equality holds; granted subject gets 200; existing `unknown_dataset_is_404` still 404.

- [ ] **Step 5: clippy + commit**

Run: `buck2 build --console none '//src/services/query-api:query-api[clippy.txt]'`, read the printed path, confirm empty.

```bash
git add src/services/query-api/src/http.rs src/services/query-api/tests/datasets_routes.rs
git commit -m "feat(query-api): per-dataset ACL gating on dataset detail + preview (canonical 404)"
```

---

### Task 4: Server-side transform-output grant + delete the UI self-grant

`POST /admin/transforms` grants the reserved `admin` role `Read` on a **physical** output table as part of the define flow; typed outputs get no grant (their visibility rides the bound type). The UI's best-effort `output_table_grant` is deleted.

**Files:**
- Modify: `src/control-plane/core/src/transforms.rs` (add `TransformBody::physical_output_grant_table`)
- Create: `src/control-plane/core/tests/transform_grant_target.rs` (unit test for the new method) + wire it in `src/control-plane/core/BUCK`
- Modify: `src/services/runtime/src/admin.rs` (grant in `define_transform_route`)
- Modify: `src/services/runtime/tests/admin_management.rs` (grant-visibility + idempotency tests)
- Modify: `src/ui/src/transforms.rs` (delete `output_table_grant`), `src/ui/src/lib.rs` (drop the re-export), `src/ui/src/main.rs` (delete the self-grant call), `src/ui/src/net.rs` (fix the doc comment referencing the deleted fn), `src/ui/tests/transforms_form.rs` (delete the two `output_table_grant` test cases)

**Interfaces:**
- Consumes: `TransformDef.body: TransformBody`; `control_plane_core::{ADMIN_ROLE, Acl, Action, Effect, PolicyTarget, RoleId, TableRef}`; `AdminState.cp` (has `.acl()`).
- Produces: `pub fn TransformBody::physical_output_grant_table(&self) -> Option<&TableRef>` — `Some(output)` for `Physical`, `None` for every other variant (typed + the MV variants, matching the UI's Physical-only split; MV output visibility is out of scope for this item).

- [ ] **Step 1: Write the failing unit test for the new method**

Create `src/control-plane/core/tests/transform_grant_target.rs`:

```rust
//! `TransformBody::physical_output_grant_table` returns the physical output table
//! that needs an explicit admin Read grant — `Some` for a `Physical` body, `None`
//! for a `Typed` body (governed by its bound type's grants).

use control_plane_core::{OutputMode, TableRef, TransformBody};

#[test]
fn physical_body_yields_its_output_table() {
    let body = TransformBody::Physical {
        inputs: vec![TableRef {
            schema: "main".into(),
            name: "src".into(),
        }],
        output: TableRef {
            schema: "main".into(),
            name: "dst".into(),
        },
        sql: "select * from src".into(),
        output_mode: OutputMode::default(),
    };
    assert_eq!(
        body.physical_output_grant_table(),
        Some(&TableRef {
            schema: "main".into(),
            name: "dst".into()
        })
    );
}

#[test]
fn typed_body_yields_none() {
    let body = TransformBody::Typed {
        inputs: vec!["Src".into()],
        output: "Dst".into(),
        sql: "select 1".into(),
        output_mode: OutputMode::default(),
    };
    assert_eq!(body.physical_output_grant_table(), None);
}
```

Verify `OutputMode` is re-exported from `control_plane_core` (it is used by `TransformBody`); if the import path differs, adjust. Wire the target in `src/control-plane/core/BUCK` by mirroring an existing `rust_test` (e.g. the `page` target the CLAUDE.md names) — `deps = [":core"]` plus whatever the existing transform tests use.

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test --console none //src/control-plane/core:transform-grant-target`
Expected: FAIL — `physical_output_grant_table` does not exist.

- [ ] **Step 3: Add the method**

In `src/control-plane/core/src/transforms.rs`, add to `impl TransformBody` (near `to_job`):

```rust
/// The physical (untyped) output table that must be explicitly Read-granted for
/// the output to be visible in the catalog / lineage. `Some` only for a `Physical`
/// body — a `Typed` output's visibility rides its bound type's grants, and the MV
/// variants' output governance is out of scope here (matching the UI's Physical-only
/// self-grant split this replaces).
#[must_use]
pub fn physical_output_grant_table(&self) -> Option<&TableRef> {
    match self {
        Self::Physical { output, .. } => Some(output),
        Self::Typed { .. }
        | Self::MicroBatch { .. }
        | Self::MicroBatchJoin { .. } => None,
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `buck2 test --console none //src/control-plane/core:transform-grant-target`
Expected: PASS (2 tests).

- [ ] **Step 5: Write the failing runtime grant-visibility test**

In `src/services/runtime/tests/admin_management.rs`, add tests that a physical define grants admin Read on the output table, a typed define does not, and re-define is idempotent. Mirror the existing `define_get_delete_transform` harness (`app`, `req_json`, `send`, `seed_admin_session`, `TRANSFORM_BODY`). `Acl`, `ADMIN_ROLE`, `Action`, `PolicyTarget`, `TableRef`, `Effect`, `SubjectId`, `RoleId` are needed — most are already imported at the top of the file (`ADMIN_ROLE, Acl, ...`); add `Action, Effect, PolicyTarget, TableRef` if absent.

```rust
const TYPED_TRANSFORM_BODY: &str = r#"{
    "name": "typed_daily",
    "body": {"kind": "typed",
             "inputs": ["Src"],
             "output": "Dst",
             "sql": "select 1"}
}"#;

/// A subject in the reserved admin role can read main.dst iff a Table grant exists.
async fn admin_can_read_table(cp: &MemoryControlPlane, schema: &str, name: &str) -> bool {
    // ADMIN is seeded into ADMIN_ROLE by seed_admin_session.
    cp.acl()
        .check(
            &SubjectId(ADMIN.into()),
            Action::Read,
            &PolicyTarget::Table(TableRef {
                schema: schema.into(),
                name: name.into(),
            }),
        )
        .await
        .unwrap()
        == control_plane_core::Decision::Allow
}

#[tokio::test]
async fn physical_define_grants_admin_read_on_output() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    assert!(!admin_can_read_table(&cp, "main", "dst").await);

    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/transforms", &token, TRANSFORM_BODY),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(admin_can_read_table(&cp, "main", "dst").await);

    // Re-define is idempotent: still 201, still granted.
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/transforms", &token, TRANSFORM_BODY),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(admin_can_read_table(&cp, "main", "dst").await);
}

#[tokio::test]
async fn typed_define_grants_no_table() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = seed_admin_session(&cp, ADMIN).await;
    // Seed the Src/Dst ontology types the typed body references (define validates them).
    for name in ["Src", "Dst"] {
        cp.ontology()
            .define_type(ObjectType {
                name: TypeName(name.into()),
                properties: vec![],
                derived: vec![],
                table: control_plane_core::TableRef {
                    schema: "onto".into(),
                    name: name.to_lowercase(),
                },
                identity: None,
                version: None,
            })
            .await
            .unwrap();
    }
    let (status, _) = send(
        app(cp.clone()),
        req_json("POST", "/admin/transforms", &token, TYPED_TRANSFORM_BODY),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    // No Table grant was created for a typed output.
    assert!(!admin_can_read_table(&cp, "onto", "dst").await);
}
```

Verify the `ObjectType` construction matches the real struct (same caveat as Task 1 Step 1) and that a typed body's `inputs`/`output` type names must exist — read the existing `define_transform_rejects_bad_shapes` test (which relies on unseeded types being rejected) to confirm the seeding requirement, and adjust the seeded type names to match what the typed body references.

- [ ] **Step 6: Run to verify the grant tests fail**

Run: `buck2 test --console none //src/services/runtime:admin-management`
Expected: FAIL — `physical_define_grants_admin_read_on_output` fails (no grant is issued yet).

- [ ] **Step 7: Grant in the define handler**

In `src/services/runtime/src/admin.rs`, extend `define_transform_route` to grant after a successful define:

```rust
async fn define_transform_route(
    State(st): State<AdminState>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let def: TransformDef = match serde_json::from_value(body) {
        Ok(d) => d,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid TransformDef: {e}"),
            )
                .into_response();
        }
    };
    // Capture the physical output table (if any) before the def is moved into define.
    let grant_table = def.body.physical_output_grant_table().cloned();
    if let Err(e) = st.cp.transforms().define_transform(def).await {
        return status_for(&e).into_response();
    }
    // A physical output is a fresh untyped table with no grant; grant the reserved
    // admin role Read so its catalog metadata + lineage node are visible regardless
    // of the defining client. Idempotent (no-op upsert on redefine). A grant failure
    // surfaces (the define is committed and idempotent, so a re-POST recovers — the
    // known cross-concern-atomicity gap, fut-auth-acl-provisioning-tx).
    if let Some(output) = grant_table {
        if let Err(e) = st
            .cp
            .acl()
            .grant(
                &RoleId(ADMIN_ROLE.to_string()),
                Action::Read,
                PolicyTarget::Table(output),
                Effect::Allow,
            )
            .await
        {
            return status_for(&e).into_response();
        }
    }
    (StatusCode::CREATED, "defined").into_response()
}
```

Add any missing imports to `admin.rs` (`ADMIN_ROLE`, `Action`, `Effect`, `PolicyTarget`, `RoleId` — several are likely already imported for the `grant` handler; `TableRef` too). If `def.body` is not directly accessible (private field), use the public accessor the struct exposes (read the `TransformDef` definition around `src/control-plane/core/src/transforms.rs:174` to confirm `body` is `pub`).

- [ ] **Step 8: Run the runtime tests to verify they pass**

Run: `buck2 test --console none //src/services/runtime:admin-management`
Expected: PASS — physical define grants + idempotent; typed define grants no table; existing `define_get_delete_transform`/`scheduled_transform_exposes_next_run_at` still green (the extra admin grant is additive).

- [ ] **Step 9: Delete the UI self-grant**

- `src/ui/src/transforms.rs`: delete the `output_table_grant` fn (lines ~290-304) and its doc comment.
- `src/ui/src/lib.rs`: remove `output_table_grant` from the re-export list (line ~16).
- `src/ui/src/main.rs`: delete the `let grant = loom_ui_core::output_table_grant(&form);` line (~695) and the `if let Some(g) = grant { net::post_role_grant(...).await; }` block (~699-704), leaving the `define_transform` → `Ok(())` → `editing_state.set(None); reload_list();` flow intact. Confirm `base`/`token` are still used by `define_transform` so no unused-variable warnings appear (they are).
- `src/ui/src/net.rs`: the `post_role_grant` fn stays (generic grant helper), but its doc comment (~line 311) references `output_table_grant` — reword it to not name the deleted fn (e.g. "generic role-grant POST; the admin surface implies the caller holds the reserved `admin` role").
- `src/ui/tests/transforms_form.rs`: delete the two tests `physical_form_yields_output_table_grant_body` (~104) and `typed_form_yields_no_output_table_grant` (~116) and any now-unused imports they introduced.

- [ ] **Step 10: Build + test the UI crate**

Run: `buck2 build --console none //src/ui:app` (cross-compiles wasm; must stay green after the deletions).
Run: `buck2 test --console none //src/ui:transforms-form` (verify the target name via `grep -n "transforms.form\|transforms_form" src/ui/BUCK`; run whatever target owns `tests/transforms_form.rs`).
Expected: PASS / clean build. Run clippy on the touched non-UI crates:
`buck2 build --console none '//src/services/runtime:runtime[clippy.txt]' '//src/control-plane/core:core[clippy.txt]'` and confirm both printed paths are empty.

- [ ] **Step 11: Commit**

```bash
git add src/control-plane/core/src/transforms.rs src/control-plane/core/tests/transform_grant_target.rs \
        src/control-plane/core/BUCK src/services/runtime/src/admin.rs \
        src/services/runtime/tests/admin_management.rs \
        src/ui/src/transforms.rs src/ui/src/lib.rs src/ui/src/main.rs src/ui/src/net.rs \
        src/ui/tests/transforms_form.rs
git commit -m "feat(runtime): grant admin Read on physical transform outputs at define; drop UI self-grant"
```

---

### Task 5: Full-suite verification + register close

**Files:**
- Modify: `docs/ISSUES.md` (remove the closed item) and `docs/system-capabilities/` (fold in the landed capability) — via `loom-docs-update` at finish time, not hand-edited here.

- [ ] **Step 1: Run the full query-api + runtime + core suites**

Run: `buck2 test --console none //src/services/query-api/... //src/services/runtime/... //src/control-plane/core/...`
Expected: `Tests finished: Pass N. Fail 0`. The gating changes public catalog behavior, so a previously-green e2e that seeds a subject without a Table/Type grant and then reads a `/datasets*` route will now see an empty list / 404. **The verified blast radius beyond `datasets_routes.rs` is exactly two files, and both are expected to stay green WITHOUT edits** — do not preemptively change them, just confirm they pass:
- `tests/as_of_objects_e2e.rs` — subject `alice` holds a **Type** grant on `Thing` (backed by `main.thing`) and reads `GET /datasets/main/thing...`; `is_table_readable` is `true` via the fallback.
- `tests/as_of_guards_e2e.rs` — same shape (`Thing` → `main.thing`, `/datasets/main/thing?as_of_snapshot=...`).

Other grep hits are NOT live consumers of the three routes (`dataset_preview.rs` tests the pure `preview_body`; `openapi.rs` only asserts the (method, path) list, unaffected by adding a 404 response annotation; the `lineage_*`/`http_wire`/`objects` e2e hit other routes). If the sweep surfaces any *additional* failing catalog read, grant the subject read (reuse `grant_read` for a Type-backed dataset, or a Table grant) rather than weakening the assertion.

- [ ] **Step 2: Run prek**

Run: `buck2 run //tools:prek -- run --all-files`
Expected: all hooks pass (commit anything the hooks fix).

- [ ] **Step 3: Metric gate (final-review requirement)**

Run `loom-complexity diff` and `loom-duplication diff` (changed files only, print, no commit). Report as findings any NEW hotspot over the census thresholds (cc > 15, cognitive > 15, MI < 20, SLOC > 100) or any NEW cross-file duplication pair ≥ 20 lines the branch introduced. `dataset_acl.rs` is largely moved (not new) code, so expect no net complexity increase; fix or explicitly justify any finding in the PR description.

- [ ] **Step 4: Commit any hook fixups**

```bash
git add -A
git commit -m "chore: prek fixups"
```

## Self-Review

**Spec coverage:**
- Shared governor extraction (`DatasetVisibility`, delegation-preserving) → Task 1. ✓
- `list_datasets` filters → Task 2. ✓
- `get_dataset`/`dataset_preview` 404 (canonical body, no oracle) → Task 3. ✓
- Preview keeps `SELECT *`; row-filter/mask parity out of scope → Task 3 (semantics untouched). ✓
- Server-side physical-output grant in the define path → Task 4. ✓
- Delete UI self-grant (`output_table_grant` + main.rs POST) → Task 4. ✓
- Testing: list filters — no-grants (empty, `datasets_list_is_empty_for_ungranted_subject`), Table grant (existing positive), **Type grant → backing table listed (`type_grant_lists_backing_table`, route-level fallback)** → Task 2; get/preview 404 body-equality, Type-backed fallback (unit, Task 1), lineage regression (Task 1 Step 6), transform-output grant (physical visible / typed none / re-POST idempotent, Task 4), UI self-grant deleted (Task 4). ✓ The spec's fourth list class "admin (all)" is **not** a distinct route test here: the memory ACL fake has **no admin god-mode** (`check` is pure grant-matching, `memory/src/acl.rs:328-355`), so "admin sees all" is indistinguishable from the Table-grant case with the single seeded table — an admin only sees what its explicit grants cover, which the Table-grant positive already exercises. The real-deployment "admin sees everything" property is a function of the bootstrap admin's broad grants, not a code path this item changes.
- Non-regression: lineage byte-identical (Task 1 Step 6), `/objects` untouched (no edits there), no migration → ✓.

**Type consistency:** `is_table_readable(&SubjectId, &TableRef) -> Result<bool, ControlPlaneError>` and `is_readable(&SubjectId, &DatasetRef) -> Result<bool, ControlPlaneError>` used identically in `dataset_acl.rs`, `lineage_filter.rs` (via `?`→`LineageVisibilityError`), and the three http.rs handlers. `physical_output_grant_table(&self) -> Option<&TableRef>` returns a borrow; the handler `.cloned()`s before the def is moved. `dataset_not_found()` is the single canonical 404 used by both point reads.

**Placeholder scan:** none — every code step carries full code; test bodies are complete (the two "verify the `ObjectType` field set" notes point at a concrete struct to read, not a placeholder to fill with invented logic).
