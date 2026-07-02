# query-api Governed-Read Spine (road-qa-governance-layer) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the governed-read spine (`GovernedType`/`resolve_governed`, `Projection`, `seed_predicates`, `resolve_hop`) so query-api's seven copied governance prologues collapse to one, and `read_object_page` resolves governance once instead of twice.

**Architecture:** A new `src/services/query-api/src/governed.rs` module owns the spine; `handler.rs` re-exports the moved public helpers so existing test imports keep compiling. `compile_object_read` becomes a thin wrapper over a new `compile_object_read_with(&GovernedType, …)`; every handler entry point (object read, paginated read, vector search, chain, associations, and the four graph reads) is rewired onto the spine. Behavior-preserving: HTTP semantics (403/404 split, fail-closed projection, mask marker) are pinned by the existing e2e + unit suites.

**Tech Stack:** Rust (edition 2024), buck2, `//src/control-plane/memory` fakes + stub `ServingEngine` for pure-logic tests.

**Spec:** `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md` § *road-qa-governance-layer* (Wave 1). Register item: `road-qa-governance-layer` in `docs/ROADMAP.md`. Branch: `work/road-qa-governance-layer`.

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]` modules (buck2 never runs them; the `no-inline-tests` prek hook rejects them). New test files go in `src/services/query-api/tests/` with their own target in `src/services/query-api/BUCK`, using the `rust_test` loaded from `//src:loom_test.bzl` (already loaded at the top of that BUCK file).
- **Strict clippy on production code**: no `.unwrap()`/`.expect()`/indexing/`panic!` in `src/**`; local silences need `#[expect(lint, reason = "…")]`. Test files are exempt (the `loom_rust_test` wrapper injects the allows).
- **Behavior-preserving**: no public HTTP semantic changes. `QueryError` variants, the deny-before-existence-leak ordering (`acl.check` BEFORE `get_type`), the fail-closed empty-projection `Forbidden`, the 404-vs-403 split (`UnknownType` for reads, `Forbidden` for `vector_search`), and the mask-marker contract all stay exactly as-is.
- **Known, accepted micro-divergence** (call out in the PR description): unifying the hop-resolution copies means `resolve_graph`'s and the tail walk's `links()`/`links_to()` `NotFound` now maps to `UnknownType` (the `resolve_chain` behavior) instead of propagating as `ControlPlane(NotFound)`. This branch is unreachable in practice (the current type was just fetched via `get_type`), and the defensive 404 is strictly safer than a 500. Similarly `vector_search` now loads policy up front (inside `resolve_governed`) instead of lazily after the engine call — one extra `policies_for` on the empty-hits path, identical observable behavior.
- **Cloud/disk discipline**: never a bare whole-tree `buck2 build`/`test //src/...`. Build with `-M none`; scope tests to `//src/services/query-api:` targets (plus the final full-crate sweep `buck2 test //src/services/query-api/...`). Don't pipe `buck2 test` through `head`/`tail` — redirect to a file and grep.
- **Formatting/lint**: `buck2 run //tools:rustfmt -- <files>` after each task; `buck2 run //tools:prek -- run --all-files` before push. Markdown files end with exactly one trailing newline, no trailing whitespace.
- **Conventional Commits** for every commit message (commit-msg hook enforces).

## File Structure

- **Create** `src/services/query-api/src/governed.rs` — the spine: `GovernedType`, `OnMissing`, `resolve_governed`, `Projection`, `prop_ty`, `seed_predicates`, `resolve_hop`, plus the helpers moved out of `handler.rs` (`load_policy`, `project_allowed`, `identity_is_governed`, `identity_in_predicate`, `coerce_visible_predicate`).
- **Modify** `src/services/query-api/src/lib.rs` — add `pub mod governed;`.
- **Modify** `src/services/query-api/src/handler.rs` — delete the moved helpers, re-export the public ones (`pub use crate::governed::{…}`), rewire every entry point onto the spine, add `compile_object_read_with` and `GovernedRead::into_object_rows`.
- **Create** tests `tests/resolve_governed.rs`, `tests/projection.rs`, `tests/resolve_hop.rs`, `tests/seed_predicates.rs`, `tests/read_page_single_resolve.rs` + their BUCK targets.
- **Untouched**: `http.rs`, `flight_export.rs` (they call the unchanged `compile_object_read`/`read_*` signatures), `sql.rs`, `filter.rs`. Those files' cleanups are Wave 2 (`road-qa-read-path-consolidation`).

---

### Task 1: `governed.rs` — `GovernedType`, `OnMissing`, `resolve_governed`, `prop_ty` + helper moves

**Files:**
- Create: `src/services/query-api/src/governed.rs`
- Modify: `src/services/query-api/src/lib.rs` (add module)
- Modify: `src/services/query-api/src/handler.rs` (delete moved helpers, add re-exports)
- Create: `src/services/query-api/tests/resolve_governed.rs`
- Modify: `src/services/query-api/BUCK` (new test target)

**Interfaces:**
- Consumes: `crate::handler::QueryError` (stays in handler.rs), `control_plane_core::{Acl, Action, ControlPlaneError, Decision, ObjectType, Ontology, PageReq, PolicyTarget, PropertyDef, RowFilter, SubjectId, TypeName}`.
- Produces (later tasks rely on these exact signatures):
  - `pub struct GovernedType { pub otype: ObjectType, pub row_filters: Vec<RowFilter>, pub denied: HashSet<String>, pub masked: HashSet<String> }` with `#[derive(Debug, Clone)]`, plus methods `pub fn allowed(&self) -> Vec<String>` and `pub fn identity_governed(&self) -> bool`.
  - `pub enum OnMissing { NotFound, Forbidden, Internal }` (`#[derive(Debug, Clone, Copy)]`).
  - `pub async fn resolve_governed(ontology: &(dyn Ontology + Send + Sync), acl: &(dyn Acl + Send + Sync), subject: &SubjectId, name: &TypeName, on_missing: OnMissing) -> Result<GovernedType, QueryError>`.
  - `pub fn prop_ty<'a>(otype: &'a ObjectType, name: &str) -> Option<&'a str>`.
  - `pub(crate) fn load_policy(…)`, `pub(crate) fn project_allowed(…)`, `pub(crate) fn coerce_visible_predicate(…)` (same bodies as today, moved).
  - Re-exports from handler: `pub use crate::governed::{GovernedType, OnMissing, identity_in_predicate, identity_is_governed, prop_ty, resolve_governed};` — existing test imports `query_api::handler::{identity_in_predicate, identity_is_governed}` must keep compiling.

- [ ] **Step 1: Write the failing test** — `src/services/query-api/tests/resolve_governed.rs`:

```rust
//! resolve_governed is the single governance prologue: coarse Read gate (deny-by-default,
//! BEFORE existence is revealed), type resolution with the OnMissing 404/403/internal knob,
//! and the folded row-filter/denied/masked policy.

use std::time::Duration;

use control_plane_core::{
    Acl, Action, CompareOp, ControlPlaneError, Effect, ObjectType, Ontology, Policy,
    PolicyTarget, PropertyDef, RoleId, RowFilter, ScalarValue, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::governed::{OnMissing, resolve_governed};
use query_api::handler::QueryError;

fn order_type() -> ObjectType {
    ObjectType {
        name: TypeName("Order".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "status".into(),
                ty: "String".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "secret".into(),
                ty: "String".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "orders".into(),
        },
        identity: Some("id".into()),
    }
}

async fn seeded() -> (MemoryControlPlane, SubjectId) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(order_type()).await.unwrap();
    let analyst = SubjectId("analyst".into());
    let reader = RoleId("reader".into());
    cp.define_subject(&analyst).await.unwrap();
    cp.define_role(&reader).await.unwrap();
    cp.assign_role(&analyst, &reader).await.unwrap();
    cp.grant(
        &reader,
        Action::Read,
        PolicyTarget::Type(TypeName("Order".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    (cp, analyst)
}

#[tokio::test]
async fn ungranted_subject_is_forbidden_before_existence_is_revealed() {
    let (cp, _) = seeded().await;
    let nobody = SubjectId("nobody".into());
    cp.define_subject(&nobody).await.unwrap();
    // A type that does NOT exist: an ungranted subject still gets Forbidden, not
    // UnknownType — the deny-before-existence-leak invariant.
    let err = resolve_governed(
        &cp,
        &cp,
        &nobody,
        &TypeName("Ghost".into()),
        OnMissing::NotFound,
    )
    .await
    .unwrap_err();
    assert!(matches!(err, QueryError::Forbidden));
}

#[tokio::test]
async fn granted_but_missing_type_maps_per_on_missing() {
    let (cp, analyst) = seeded().await;
    let reader = RoleId("reader".into());
    cp.grant(
        &reader,
        Action::Read,
        PolicyTarget::Type(TypeName("Ghost".into())),
        Effect::Allow,
    )
    .await
    .unwrap();

    let err = resolve_governed(&cp, &cp, &analyst, &TypeName("Ghost".into()), OnMissing::NotFound)
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::UnknownType(t) if t == "Ghost"));

    let err = resolve_governed(&cp, &cp, &analyst, &TypeName("Ghost".into()), OnMissing::Forbidden)
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::Forbidden));

    let err = resolve_governed(&cp, &cp, &analyst, &TypeName("Ghost".into()), OnMissing::Internal)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        QueryError::ControlPlane(ControlPlaneError::NotFound(_))
    ));
}

#[tokio::test]
async fn folds_row_filters_denied_and_masked_from_policy() {
    let (cp, analyst) = seeded().await;
    let reader = RoleId("reader".into());
    cp.set_policy(
        &reader,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: Some(RowFilter::Compare {
                property: "status".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("open".into()),
            }),
            deny_columns: vec!["secret".into()],
            mask_columns: vec!["status".into()],
        },
    )
    .await
    .unwrap();

    let g = resolve_governed(&cp, &cp, &analyst, &TypeName("Order".into()), OnMissing::NotFound)
        .await
        .unwrap();
    assert_eq!(g.otype.name.0, "Order");
    assert_eq!(g.row_filters.len(), 1);
    assert!(g.denied.contains("secret"));
    assert!(g.masked.contains("status"));
    // allowed() = properties minus denied, in property order.
    assert_eq!(g.allowed(), vec!["id".to_string(), "status".to_string()]);
    // identity "id" is neither denied nor masked.
    assert!(!g.identity_governed());
}

#[tokio::test]
async fn identity_governed_when_identity_is_masked() {
    let (cp, analyst) = seeded().await;
    let reader = RoleId("reader".into());
    cp.set_policy(
        &reader,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: None,
            deny_columns: vec![],
            mask_columns: vec!["id".into()],
        },
    )
    .await
    .unwrap();
    let g = resolve_governed(&cp, &cp, &analyst, &TypeName("Order".into()), OnMissing::NotFound)
        .await
        .unwrap();
    assert!(g.identity_governed());
}
```

- [ ] **Step 2: Add the BUCK target** — in `src/services/query-api/BUCK`, next to the `identity-in-predicate` target:

```python
rust_test(
    name = "resolve-governed",
    crate = "resolve_governed",
    srcs = ["tests/resolve_governed.rs"],
    crate_root = "tests/resolve_governed.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:resolve-governed > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t1.log | head -20`
Expected: FAIL to build — `query_api::governed` does not exist.

- [ ] **Step 4: Create `src/services/query-api/src/governed.rs`**

Move these items **verbatim** from `handler.rs` (delete them there): `load_policy` (make it `pub(crate)`), `project_allowed` (`pub(crate)`), `identity_is_governed` (stays `pub`), `identity_in_predicate` (stays `pub`), `coerce_visible_predicate` (`pub(crate)`). Then add the new spine:

```rust
//! The governed-read spine: the one copy of the governance prologue every query-api
//! entry point runs — coarse Read gate (deny-by-default, BEFORE existence is revealed),
//! type resolution with a 404-vs-403 knob, and the folded row/column policy — plus the
//! projection, seed-predicate, and hop-resolution helpers the read paths share.

use std::collections::HashSet;

use control_plane_core::{
    Acl, Action, ControlPlaneError, Decision, ObjectType, Ontology, PageReq, PolicyTarget,
    PropertyDef, RowFilter, SubjectId, TypeName,
};

use crate::handler::QueryError;

/// The resolved, policy-folded governance context for one `(subject, type)` pair: the
/// object type plus the subject's cumulative Read policy (row filters ANDed by the SQL
/// compiler, unioned denied + masked column sets). Produced by [`resolve_governed`] —
/// the single copy of the gate → `get_type` → policy prologue.
#[derive(Debug, Clone)]
pub struct GovernedType {
    pub otype: ObjectType,
    pub row_filters: Vec<RowFilter>,
    pub denied: HashSet<String>,
    pub masked: HashSet<String>,
}

impl GovernedType {
    /// The type's properties (in declaration order) minus denied columns — the visible
    /// physical projection.
    pub fn allowed(&self) -> Vec<String> {
        project_allowed(&self.otype.properties, &self.denied)
    }

    /// True when the declared identity column is denied or masked, so its values must
    /// not be revealed. Identity-less types are never governed here.
    pub fn identity_governed(&self) -> bool {
        identity_is_governed(&self.otype, &self.denied, &self.masked)
    }
}

/// How [`resolve_governed`] maps a granted-but-nonexistent type.
#[derive(Debug, Clone, Copy)]
pub enum OnMissing {
    /// A genuine client miss: `QueryError::UnknownType` (404). The read endpoints.
    NotFound,
    /// No-leak: `QueryError::Forbidden` (403) — the response would itself reveal
    /// existence (vector search returns identity values).
    Forbidden,
    /// An internal inconsistency, not a client fault: propagate the
    /// `ControlPlaneError::NotFound` as `QueryError::ControlPlane` (500). Used for
    /// hop-landed types — a link pointing at a missing type is corrupt ontology state.
    Internal,
}

/// The single governance prologue: coarse Read gate (deny-by-default, returned BEFORE
/// we reveal whether the type exists), type resolution (`on_missing` maps a genuine
/// miss), and the subject's folded row/column policy. Every governed read starts here
/// so the deny-before-existence-leak invariant lives in exactly one place.
pub async fn resolve_governed(
    ontology: &(dyn Ontology + Send + Sync),
    acl: &(dyn Acl + Send + Sync),
    subject: &SubjectId,
    name: &TypeName,
    on_missing: OnMissing,
) -> Result<GovernedType, QueryError> {
    let target = PolicyTarget::Type(name.clone());
    if acl.check(subject, Action::Read, &target).await? == Decision::Deny {
        return Err(QueryError::Forbidden);
    }
    let otype = match ontology.get_type(name).await {
        Ok(t) => t,
        Err(ControlPlaneError::NotFound(what)) => {
            return Err(match on_missing {
                OnMissing::NotFound => QueryError::UnknownType(name.0.clone()),
                OnMissing::Forbidden => QueryError::Forbidden,
                OnMissing::Internal => {
                    QueryError::ControlPlane(ControlPlaneError::NotFound(what))
                }
            });
        }
        Err(other) => return Err(QueryError::ControlPlane(other)),
    };
    let (row_filters, denied, masked) = load_policy(acl, subject, &target).await?;
    Ok(GovernedType {
        otype,
        row_filters,
        denied,
        masked,
    })
}

/// The declared logical type of `otype`'s property `name`, if any — the explicit
/// lookup replacing the per-site `properties.iter().find(…).map(…).unwrap_or("")`
/// sentinel chains.
pub fn prop_ty<'a>(otype: &'a ObjectType, name: &str) -> Option<&'a str> {
    otype
        .properties
        .iter()
        .find(|p| p.name == name)
        .map(|p| p.ty.as_str())
}
```

(If `ControlPlaneError::NotFound`'s payload isn't a single field named by position — check its definition in `control_plane_core` — adjust the destructuring; the intent is: re-wrap the same NotFound for `Internal`, matching how a bare `?` would convert it via `#[from]`.)

The moved helpers keep their exact current bodies and doc comments (`handler.rs:144-260`): `load_policy` (returns `(Vec<RowFilter>, HashSet<String>, HashSet<String>)`), `project_allowed(properties: &[PropertyDef], denied: &HashSet<String>) -> Vec<String>`, `identity_is_governed(otype, denied, masked) -> bool`, `identity_in_predicate(otype, denied, masked, ids) -> Result<Option<CallerPredicate>, QueryError>`, `coerce_visible_predicate(col, raw, object_type, allowed, masked) -> Result<CallerPredicate, QueryError>`. Their `crate::filter::…` references work unchanged from the new module.

- [ ] **Step 5: Wire the module and re-exports**

In `src/services/query-api/src/lib.rs` add `pub mod governed;` (alphabetical order among the existing `pub mod` lines). In `handler.rs`, where the moved items used to be, add:

```rust
pub use crate::governed::{
    GovernedType, OnMissing, identity_in_predicate, identity_is_governed, prop_ty,
    resolve_governed,
};
use crate::governed::{coerce_visible_predicate, load_policy, project_allowed};
```

(Keep `handler.rs` compiling: all internal call sites of the moved helpers resolve through these imports. Existing tests import `query_api::handler::{identity_in_predicate, identity_is_governed}` — the `pub use` preserves those paths.)

- [ ] **Step 6: Run the new test + the adjacent regression targets**

Run: `buck2 test //src/services/query-api:resolve-governed //src/services/query-api:identity-in-predicate > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS (`Tests finished: … Fail 0`).

- [ ] **Step 7: Format and commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/governed.rs src/services/query-api/src/handler.rs src/services/query-api/src/lib.rs src/services/query-api/tests/resolve_governed.rs
git add -A && git commit -m "feat(query): governed-read spine — GovernedType + resolve_governed prologue"
```

---

### Task 2: `Projection` value type + `GovernedRead::into_object_rows`

**Files:**
- Modify: `src/services/query-api/src/governed.rs` (add `Projection`)
- Modify: `src/services/query-api/src/handler.rs` (add `GovernedRead::into_object_rows`, re-export `Projection`)
- Create: `src/services/query-api/tests/projection.rs`
- Modify: `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `GovernedType` (Task 1), `crate::serving::Rows`, `crate::handler::ObjectRows`.
- Produces:
  - `pub struct Projection { pub columns: Vec<String>, pub logical_types: Vec<String>, pub masked: Vec<String> }` with:
    - `pub fn visible(g: &GovernedType) -> Result<Projection, QueryError>` — fail-closed: empty visible set → `QueryError::Forbidden`.
    - `pub fn push(&mut self, name: String, ty: String, masked: bool)` — append one derived output column.
    - `pub fn into_object_rows(self, served: Rows) -> ObjectRows` — the column-order `debug_assert_eq!` + zip.
  - `impl GovernedRead { pub fn into_object_rows(self, served: Rows) -> ObjectRows }` in handler.rs.
  - Handler re-export gains `Projection`.

- [ ] **Step 1: Write the failing test** — `src/services/query-api/tests/projection.rs`:

```rust
//! Projection owns the governed output-column set: visible physical columns in property
//! order (fail-closed on empty), the masked subset, the positional logical-type zip, and
//! the served-rows -> ObjectRows conversion.

use std::collections::HashSet;

use control_plane_core::{ObjectType, PropertyDef, TableRef, TypeName};
use query_api::governed::{GovernedType, Projection};
use query_api::handler::QueryError;
use query_api::serving::{Rows, SqlValue};

fn prop(name: &str, ty: &str) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required: false,
        constraints: control_plane_core::PropertyConstraints::default(),
    }
}

fn governed(denied: &[&str], masked: &[&str]) -> GovernedType {
    GovernedType {
        otype: ObjectType {
            name: TypeName("Order".into()),
            properties: vec![prop("id", "Long"), prop("status", "String"), prop("secret", "String")],
            derived: vec![],
            table: TableRef {
                schema: "main".into(),
                name: "orders".into(),
            },
            identity: Some("id".into()),
        },
        row_filters: vec![],
        denied: denied.iter().map(|s| (*s).to_string()).collect::<HashSet<_>>(),
        masked: masked.iter().map(|s| (*s).to_string()).collect::<HashSet<_>>(),
    }
}

#[test]
fn visible_projects_allowed_in_property_order_with_types_and_mask() {
    let p = Projection::visible(&governed(&["secret"], &["status"])).unwrap();
    assert_eq!(p.columns, vec!["id".to_string(), "status".to_string()]);
    assert_eq!(p.logical_types, vec!["Long".to_string(), "String".to_string()]);
    assert_eq!(p.masked, vec!["status".to_string()]);
}

#[test]
fn empty_visible_set_is_forbidden() {
    let err = Projection::visible(&governed(&["id", "status", "secret"], &[])).unwrap_err();
    assert!(matches!(err, QueryError::Forbidden));
}

#[test]
fn push_appends_derived_columns_and_masked_membership() {
    let mut p = Projection::visible(&governed(&["secret"], &[])).unwrap();
    p.push("orderCount".into(), "Long".into(), false);
    p.push("hiddenAgg".into(), "Long".into(), true);
    assert_eq!(
        p.columns,
        vec!["id".to_string(), "status".to_string(), "orderCount".to_string(), "hiddenAgg".to_string()]
    );
    assert_eq!(p.logical_types.len(), 4);
    assert_eq!(p.masked, vec!["hiddenAgg".to_string()]);
}

#[test]
fn into_object_rows_zips_columns_types_and_rows() {
    let p = Projection::visible(&governed(&["secret"], &[])).unwrap();
    let rows = p.into_object_rows(Rows {
        columns: vec!["id".into(), "status".into()],
        rows: vec![vec![SqlValue::Int(1), SqlValue::Text("open".into())]],
    });
    assert_eq!(rows.columns, vec!["id".to_string(), "status".to_string()]);
    assert_eq!(rows.logical_types, vec!["Long".to_string(), "String".to_string()]);
    assert_eq!(rows.rows.len(), 1);
}
```

- [ ] **Step 2: Add the BUCK target**

```python
rust_test(
    name = "projection",
    crate = "projection",
    srcs = ["tests/projection.rs"],
    crate_root = "tests/projection.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
    ],
)
```

- [ ] **Step 3: Run to verify it fails**

Run: `buck2 test //src/services/query-api:projection > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t2.log | head -20`
Expected: FAIL to build — `Projection` not found.

- [ ] **Step 4: Implement `Projection` in `governed.rs`**

```rust
/// The governed output-column set of one read, in SELECT order: the visible physical
/// columns (type properties minus denied, in property order), extended with any derived
/// columns via [`Projection::push`]. Owns the positional logical-type zip, the masked
/// subset (columns SELECTed as the mask marker, so streamed back as Utf8 — load-bearing
/// for the Flight export schema), and the served-rows conversion with its column-order
/// contract check.
#[derive(Debug)]
pub struct Projection {
    pub columns: Vec<String>,
    pub logical_types: Vec<String>,
    pub masked: Vec<String>,
}

impl Projection {
    /// The visible physical projection of `g`. Fail-closed: a subject with no visible
    /// columns gets `Forbidden`, never an empty SELECT.
    pub fn visible(g: &GovernedType) -> Result<Self, QueryError> {
        let columns = project_allowed(&g.otype.properties, &g.denied);
        if columns.is_empty() {
            return Err(QueryError::Forbidden);
        }
        let logical_types = columns
            .iter()
            .map(|name| prop_ty(&g.otype, name).map(str::to_string).unwrap_or_default())
            .collect();
        let masked = columns
            .iter()
            .filter(|c| g.masked.contains(*c))
            .cloned()
            .collect();
        Ok(Self {
            columns,
            logical_types,
            masked,
        })
    }

    /// Append one derived output column (name + declared logical type); `masked` marks
    /// it as mask-marker-SELECTed for the output mask set.
    pub fn push(&mut self, name: String, ty: String, masked: bool) {
        if masked {
            self.masked.push(name.clone());
        }
        self.columns.push(name);
        self.logical_types.push(ty);
    }

    /// Zip served rows into an `ObjectRows`. The serving engine must echo the projected
    /// columns in SELECT order — the contract that lets the renderer zip
    /// `logical_types`/`columns` onto each row's cells by position.
    pub fn into_object_rows(self, served: crate::serving::Rows) -> crate::handler::ObjectRows {
        debug_assert_eq!(
            served.columns, self.columns,
            "serving engine returned columns out of the projected order"
        );
        crate::handler::ObjectRows {
            columns: self.columns,
            logical_types: self.logical_types,
            rows: served.rows,
        }
    }
}
```

In `handler.rs`: add `Projection` to the `pub use crate::governed::{…}` list, and add next to `GovernedRead`:

```rust
impl GovernedRead {
    /// Zip served rows into an `ObjectRows` using this read's projected columns and
    /// logical types, asserting the engine echoed the SELECT column order.
    pub fn into_object_rows(self, served: crate::serving::Rows) -> ObjectRows {
        debug_assert_eq!(
            served.columns, self.columns,
            "serving engine returned columns out of the projected order"
        );
        ObjectRows {
            columns: self.columns,
            logical_types: self.logical_types,
            rows: served.rows,
        }
    }
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `buck2 test //src/services/query-api:projection > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS.

- [ ] **Step 6: Format and commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/governed.rs src/services/query-api/src/handler.rs src/services/query-api/tests/projection.rs
git add -A && git commit -m "feat(query): Projection value type — fail-closed visible set + logical-type zip"
```

---

### Task 3: `resolve_hop` + `seed_predicates`

**Files:**
- Modify: `src/services/query-api/src/governed.rs`
- Modify: `src/services/query-api/src/handler.rs` (re-exports)
- Create: `src/services/query-api/tests/resolve_hop.rs`, `src/services/query-api/tests/seed_predicates.rs`
- Modify: `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `crate::handler::{Direction, Hop}` (stay in handler.rs — public API used by http.rs and tests), `GovernedType` (Task 1), `identity_in_predicate`/`coerce_visible_predicate` (Task 1), `control_plane_core::LinkBacking`.
- Produces:
  - `pub async fn resolve_hop(ontology: &(dyn Ontology + Send + Sync), current: &TypeName, hop: &Hop) -> Result<(TypeName, LinkBacking), QueryError>` — forward: `links(current)` + find by name (`UnknownLink` on miss, `links()` NotFound → `UnknownType(current)`); inverse: `links_to(current)` + uniqueness check (`AmbiguousLink` on duplicates) + `backing.reversed()`.
  - `pub fn seed_predicates(g: &GovernedType, allowed: &[String], filters: &[(String, String)], ids: &[String]) -> Result<Vec<CallerPredicate>, QueryError>` — visibility-gated + coerced caller filters, then the `_ids` identity-In predicate. (Sync — no await inside.)
  - Handler re-export gains `resolve_hop, seed_predicates`.

- [ ] **Step 1: Write the failing tests**

`src/services/query-api/tests/seed_predicates.rs`:

```rust
//! seed_predicates is the shared visibility-gate + coerce + `_ids` loop: caller filters
//! against the allowed/masked projection (denied or masked -> BadFilter, no type-info
//! leak), then the object-set identity In predicate.

use std::collections::HashSet;

use control_plane_core::{CompareOp, ObjectType, PropertyDef, TableRef, TypeName};
use query_api::governed::{GovernedType, seed_predicates};
use query_api::handler::QueryError;
use query_api::serving::SqlValue;

fn prop(name: &str, ty: &str) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required: false,
        constraints: control_plane_core::PropertyConstraints::default(),
    }
}

fn governed(masked: &[&str]) -> GovernedType {
    GovernedType {
        otype: ObjectType {
            name: TypeName("Person".into()),
            properties: vec![prop("id", "Long"), prop("name", "String"), prop("ssn", "String")],
            derived: vec![],
            table: TableRef {
                schema: "main".into(),
                name: "person".into(),
            },
            identity: Some("id".into()),
        },
        row_filters: vec![],
        denied: HashSet::new(),
        masked: masked.iter().map(|s| (*s).to_string()).collect::<HashSet<_>>(),
    }
}

fn allowed() -> Vec<String> {
    vec!["id".into(), "name".into(), "ssn".into()]
}

#[test]
fn coerces_filters_then_appends_the_ids_in_predicate() {
    let g = governed(&[]);
    let preds = seed_predicates(
        &g,
        &allowed(),
        &[("name".to_string(), "alice".to_string())],
        &["1".to_string(), "2".to_string()],
    )
    .unwrap();
    assert_eq!(preds.len(), 2);
    assert_eq!(preds[0].column, "name");
    assert_eq!(preds[1].column, "id");
    assert_eq!(preds[1].op, CompareOp::In);
    assert_eq!(preds[1].values, vec![SqlValue::Int(1), SqlValue::Int(2)]);
}

#[test]
fn filter_on_a_column_outside_allowed_is_bad_filter() {
    let g = governed(&[]);
    let err = seed_predicates(
        &g,
        &["id".to_string(), "name".to_string()], // ssn not visible
        &[("ssn".to_string(), "x".to_string())],
        &[],
    )
    .unwrap_err();
    assert!(matches!(err, QueryError::BadFilter(c) if c == "ssn"));
}

#[test]
fn filter_on_a_masked_column_is_bad_filter() {
    let g = governed(&["name"]);
    let err = seed_predicates(
        &g,
        &allowed(),
        &[("name".to_string(), "alice".to_string())],
        &[],
    )
    .unwrap_err();
    assert!(matches!(err, QueryError::BadFilter(c) if c == "name"));
}

#[test]
fn empty_inputs_yield_no_predicates() {
    let g = governed(&[]);
    let preds = seed_predicates(&g, &allowed(), &[], &[]).unwrap();
    assert!(preds.is_empty());
}
```

`src/services/query-api/tests/resolve_hop.rs`:

```rust
//! resolve_hop is the one copy of the forward/inverse link match shared by the chain
//! and graph resolvers: forward follows `links(current)` by name; inverse follows
//! `links_to(current)` with an ambiguity check and role-swapped (reversed) backing.

use std::time::Duration;

use control_plane_core::{
    Cardinality, LinkBacking, LinkDef, ObjectType, Ontology, PropertyDef, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::governed::resolve_hop;
use query_api::handler::{Direction, Hop, QueryError};

fn simple_type(name: &str, table: &str) -> ObjectType {
    ObjectType {
        name: TypeName(name.into()),
        properties: vec![PropertyDef {
            name: "id".into(),
            ty: "Long".into(),
            required: true,
            constraints: control_plane_core::PropertyConstraints::default(),
        }],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: table.into(),
        },
        identity: Some("id".into()),
    }
}

/// Person -employer-> Company; Team -staff-> Company and Guild -staff-> Company (an
/// ambiguous inbound name at Company).
async fn seeded() -> MemoryControlPlane {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(simple_type("Person", "person")).await.unwrap();
    cp.define_type(simple_type("Company", "company")).await.unwrap();
    cp.define_type(simple_type("Team", "team")).await.unwrap();
    cp.define_type(simple_type("Guild", "guild")).await.unwrap();
    cp.define_link(LinkDef {
        name: "employer".into(),
        from: TypeName("Person".into()),
        to: TypeName("Company".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: "employer_id".into(),
            to_column: "id".into(),
        },
    })
    .await
    .unwrap();
    cp.define_link(LinkDef {
        name: "staff".into(),
        from: TypeName("Team".into()),
        to: TypeName("Company".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: "company_id".into(),
            to_column: "id".into(),
        },
    })
    .await
    .unwrap();
    cp.define_link(LinkDef {
        name: "staff".into(),
        from: TypeName("Guild".into()),
        to: TypeName("Company".into()),
        cardinality: Cardinality::One,
        backing: LinkBacking::ForeignKey {
            from_column: "company_id".into(),
            to_column: "id".into(),
        },
    })
    .await
    .unwrap();
    cp
}

#[tokio::test]
async fn forward_hop_lands_on_the_link_target() {
    let cp = seeded().await;
    let (landed, backing) = resolve_hop(&cp, &TypeName("Person".into()), &Hop::from("employer"))
        .await
        .unwrap();
    assert_eq!(landed.0, "Company");
    assert!(matches!(
        backing,
        LinkBacking::ForeignKey { ref from_column, .. } if from_column == "employer_id"
    ));
}

#[tokio::test]
async fn forward_unknown_link_is_unknown_link() {
    let cp = seeded().await;
    let err = resolve_hop(&cp, &TypeName("Person".into()), &Hop::from("nope"))
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::UnknownLink(l) if l == "nope"));
}

#[tokio::test]
async fn inverse_hop_lands_on_the_link_origin_with_reversed_backing() {
    let cp = seeded().await;
    let hop = Hop {
        link: "employer".into(),
        direction: Direction::Inverse,
    };
    let (landed, backing) = resolve_hop(&cp, &TypeName("Company".into()), &hop)
        .await
        .unwrap();
    assert_eq!(landed.0, "Person");
    // Reversed: the join now reads Company(id) -> Person(employer_id).
    assert!(matches!(
        backing,
        LinkBacking::ForeignKey { ref from_column, ref to_column }
            if from_column == "id" && to_column == "employer_id"
    ));
}

#[tokio::test]
async fn ambiguous_inbound_link_is_ambiguous_link() {
    let cp = seeded().await;
    let hop = Hop {
        link: "staff".into(),
        direction: Direction::Inverse,
    };
    let err = resolve_hop(&cp, &TypeName("Company".into()), &hop)
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::AmbiguousLink(l) if l == "staff"));
}
```

(If `LinkBacking::ForeignKey`'s reversed form differs — check `control_plane_core`'s `LinkBacking::reversed()` before finalizing the inverse assertion; assert on whatever `reversed()` actually produces for a `ForeignKey`, including the `JoinTable` variant if `ForeignKey` reversal is represented differently.)

- [ ] **Step 2: Add the BUCK targets**

```python
rust_test(
    name = "seed-predicates",
    crate = "seed_predicates",
    srcs = ["tests/seed_predicates.rs"],
    crate_root = "tests/seed_predicates.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
    ],
)

rust_test(
    name = "resolve-hop",
    crate = "resolve_hop",
    srcs = ["tests/resolve_hop.rs"],
    crate_root = "tests/resolve_hop.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run to verify they fail**

Run: `buck2 test //src/services/query-api:seed-predicates //src/services/query-api:resolve-hop > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t3.log | head -20`
Expected: FAIL to build — `seed_predicates`/`resolve_hop` not found.

- [ ] **Step 4: Implement in `governed.rs`**

```rust
use crate::handler::{Direction, Hop};

/// Resolve one directed hop from `current`: `Forward` follows an outbound link by name
/// (`links`); `Inverse` follows an inbound link backwards (`links_to`) with an ambiguity
/// check (inbound names need not be unique — links are keyed `(name, from)`) and the
/// backing column roles swapped so the symmetric join compiler reads it in reverse.
/// A missing link is `UnknownLink`; a `links()`/`links_to()` NotFound on `current` is a
/// defensive `UnknownType` (unreachable when the caller just resolved `current`).
pub async fn resolve_hop(
    ontology: &(dyn Ontology + Send + Sync),
    current: &TypeName,
    hop: &Hop,
) -> Result<(TypeName, control_plane_core::LinkBacking), QueryError> {
    match hop.direction {
        Direction::Forward => {
            let links = ontology
                .links(current, PageReq::unbounded())
                .await
                .map_err(|e| match e {
                    ControlPlaneError::NotFound(_) => QueryError::UnknownType(current.0.clone()),
                    other => QueryError::ControlPlane(other),
                })?;
            let link = links
                .items
                .into_iter()
                .find(|l| l.name == hop.link)
                .ok_or_else(|| QueryError::UnknownLink(hop.link.clone()))?;
            Ok((link.to, link.backing))
        }
        Direction::Inverse => {
            let links = ontology
                .links_to(current, PageReq::unbounded())
                .await
                .map_err(|e| match e {
                    ControlPlaneError::NotFound(_) => QueryError::UnknownType(current.0.clone()),
                    other => QueryError::ControlPlane(other),
                })?;
            let mut matches = links.items.into_iter().filter(|l| l.name == hop.link);
            let link = matches
                .next()
                .ok_or_else(|| QueryError::UnknownLink(hop.link.clone()))?;
            if matches.next().is_some() {
                return Err(QueryError::AmbiguousLink(hop.link.clone()));
            }
            // Inverse: land on the origin, backing reversed so the symmetric join
            // reaches `current` back to `link.from`.
            Ok((link.from, link.backing.reversed()))
        }
    }
}

/// The shared caller-filter + object-set seed loop: each `(column, raw)` filter is
/// visibility-gated against `allowed`/masked (denied, masked, or unknown column ->
/// `BadFilter`, no type-info leak) and coerced to a typed predicate; then `ids` lowers
/// to an `In` predicate on the declared identity. Order: filters (as given), then ids.
pub fn seed_predicates(
    g: &GovernedType,
    allowed: &[String],
    filters: &[(String, String)],
    ids: &[String],
) -> Result<Vec<crate::filter::CallerPredicate>, QueryError> {
    let mut predicates = Vec::with_capacity(filters.len() + 1);
    for (col, raw) in filters {
        predicates.push(coerce_visible_predicate(
            col, raw, &g.otype, allowed, &g.masked,
        )?);
    }
    if let Some(p) = identity_in_predicate(&g.otype, &g.denied, &g.masked, ids)? {
        predicates.push(p);
    }
    Ok(predicates)
}
```

Add `resolve_hop, seed_predicates` to handler's `pub use crate::governed::{…}` re-export list.

- [ ] **Step 5: Run to verify they pass**

Run: `buck2 test //src/services/query-api:seed-predicates //src/services/query-api:resolve-hop > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: PASS.

- [ ] **Step 6: Format and commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/governed.rs src/services/query-api/src/handler.rs src/services/query-api/tests/resolve_hop.rs src/services/query-api/tests/seed_predicates.rs
git add -A && git commit -m "feat(query): shared resolve_hop + seed_predicates helpers"
```

---

### Task 4: `compile_object_read` split + `read_object`/`read_object_page`/`vector_search` rewire

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (`compile_object_read` → wrapper + `compile_object_read_with`; rewire `read_object`, `read_object_page`, `vector_search`)
- Create: `src/services/query-api/tests/read_page_single_resolve.rs`
- Modify: `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `resolve_governed`/`OnMissing`/`GovernedType`/`Projection`/`prop_ty`/`seed_predicates` (Tasks 1–3), `GovernedRead::into_object_rows` (Task 2).
- Produces: `pub async fn compile_object_read_with(g: &GovernedType, q: &ObjectQuery, subject: &Subject, ontology: &(dyn Ontology + Send + Sync), acl: &(dyn Acl + Send + Sync), dialect: &dyn SqlDialect, limit: u32, order_by: Option<&str>, extra_predicate: Option<crate::filter::CallerPredicate>) -> Result<GovernedRead, QueryError>`. `compile_object_read` keeps its exact current signature (flight_export.rs and http.rs compile untouched).

- [ ] **Step 1: Write the failing test** — `src/services/query-api/tests/read_page_single_resolve.rs`. This pins the headline property: a paginated read runs the governance prologue **once** (today it runs twice — the test fails against current code):

```rust
//! read_object_page must resolve governance ONCE per request: exactly one acl.check and
//! one policies_for for the queried type. (Before the governed-read spine it ran its own
//! prologue and then compile_object_read ran it again — 2x acl.check / 2x get_type /
//! 2x policies_for per paginated read.)

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, Decision, Effect, ObjectType, Ontology, Page, PageReq, Policy, PolicyTarget,
    PropertyDef, Result as CpResult, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::handler::{ObjectQuery, QueryDeps, Subject, read_object_page};
use query_api::serving::{Rows, ServingEngine, ServingError, SqlValue};

/// Delegates every Acl method to the memory control plane, counting the two read-path
/// calls. The counts prove the paginated read resolves governance once.
struct CountingAcl<'a> {
    inner: &'a MemoryControlPlane,
    checks: AtomicUsize,
    loads: AtomicUsize,
}

#[async_trait]
impl Acl for CountingAcl<'_> {
    async fn define_subject(&self, id: &SubjectId) -> CpResult<()> {
        self.inner.define_subject(id).await
    }
    async fn define_role(&self, id: &RoleId) -> CpResult<()> {
        self.inner.define_role(id).await
    }
    async fn assign_role(&self, subject: &SubjectId, role: &RoleId) -> CpResult<()> {
        self.inner.assign_role(subject, role).await
    }
    async fn unassign_role(&self, subject: &SubjectId, role: &RoleId) -> CpResult<()> {
        self.inner.unassign_role(subject, role).await
    }
    async fn add_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> CpResult<()> {
        self.inner.add_role_inheritance(role, inherits).await
    }
    async fn remove_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> CpResult<()> {
        self.inner.remove_role_inheritance(role, inherits).await
    }
    async fn grant(
        &self,
        role: &RoleId,
        action: Action,
        target: PolicyTarget,
        effect: Effect,
    ) -> CpResult<()> {
        self.inner.grant(role, action, target, effect).await
    }
    async fn revoke(&self, role: &RoleId, action: Action, target: &PolicyTarget) -> CpResult<()> {
        self.inner.revoke(role, action, target).await
    }
    async fn set_policy(&self, role: &RoleId, action: Action, policy: Policy) -> CpResult<()> {
        self.inner.set_policy(role, action, policy).await
    }
    async fn clear_policy(
        &self,
        role: &RoleId,
        action: Action,
        target: &PolicyTarget,
    ) -> CpResult<()> {
        self.inner.clear_policy(role, action, target).await
    }
    async fn check(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
    ) -> CpResult<Decision> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        self.inner.check(subject, action, target).await
    }
    async fn policies_for(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
        page: PageReq,
    ) -> CpResult<Page<Policy>> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        self.inner.policies_for(subject, action, target, page).await
    }
}

/// Serves 3 canned [id, name] rows regardless of SQL (limit is applied by the engine in
/// production; here the over-fetch row count exercises the keyset truncation).
struct PageServing;

#[async_trait]
impl ServingEngine for PageServing {
    async fn fetch_rows(&self, _sql: &str, _params: &[SqlValue]) -> Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec!["id".into(), "name".into()],
            rows: vec![
                vec![SqlValue::Int(1), SqlValue::Text("a".into())],
                vec![SqlValue::Int(2), SqlValue::Text("b".into())],
                vec![SqlValue::Int(3), SqlValue::Text("c".into())],
            ],
        })
    }
}

fn person() -> ObjectType {
    ObjectType {
        name: TypeName("Person".into()),
        properties: vec![
            PropertyDef {
                name: "id".into(),
                ty: "Long".into(),
                required: true,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
            PropertyDef {
                name: "name".into(),
                ty: "String".into(),
                required: false,
                constraints: control_plane_core::PropertyConstraints::default(),
            },
        ],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "person".into(),
        },
        identity: Some("id".into()),
    }
}

#[tokio::test]
async fn paginated_read_resolves_governance_exactly_once() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(person()).await.unwrap();
    let analyst = SubjectId("analyst".into());
    let reader = RoleId("reader".into());
    cp.define_subject(&analyst).await.unwrap();
    cp.define_role(&reader).await.unwrap();
    cp.assign_role(&analyst, &reader).await.unwrap();
    cp.grant(
        &reader,
        Action::Read,
        PolicyTarget::Type(TypeName("Person".into())),
        Effect::Allow,
    )
    .await
    .unwrap();

    let acl = CountingAcl {
        inner: &cp,
        checks: AtomicUsize::new(0),
        loads: AtomicUsize::new(0),
    };
    let serving = PageServing;
    let deps = QueryDeps {
        ontology: &cp,
        acl: &acl,
        serving: &serving,
        default_limit: 100,
    };
    let q = ObjectQuery {
        type_name: "Person".into(),
        filters: vec![],
        ids: vec![],
        or_raw: vec![],
    };
    let (rows, next) = read_object_page(&q, &Subject(analyst), &deps, 2, None)
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 2);
    assert!(next.is_some());
    assert_eq!(
        acl.checks.load(Ordering::SeqCst),
        1,
        "read_object_page must run the coarse Read gate exactly once"
    );
    assert_eq!(
        acl.loads.load(Ordering::SeqCst),
        1,
        "read_object_page must load the read policy exactly once"
    );
}
```

BUCK target:

```python
rust_test(
    name = "read-page-single-resolve",
    crate = "read_page_single_resolve",
    srcs = ["tests/read_page_single_resolve.rs"],
    crate_root = "tests/read_page_single_resolve.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:async-trait",
        "//third-party:tokio",
    ],
)
```

(Check `control_plane_core`'s `Result` alias name — if the crate exports `Result<T>` under a different path, adjust the `CpResult` import; mirror whatever `src/control-plane/memory/src/` uses in its own `impl Acl`.)

- [ ] **Step 2: Run to verify it fails on the counts**

Run: `buck2 test //src/services/query-api:read-page-single-resolve > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL|assert" /tmp/t4.log | head -10`
Expected: FAIL — `checks == 2` (and `loads == 2`) against the current double-resolving implementation.

- [ ] **Step 3: Split `compile_object_read`**

Replace the body of `compile_object_read` (keep its exact signature and doc comment, trimming the prologue paragraphs into `resolve_governed`'s):

```rust
pub async fn compile_object_read(
    q: &ObjectQuery,
    subject: &Subject,
    ontology: &(dyn Ontology + Send + Sync),
    acl: &(dyn Acl + Send + Sync),
    dialect: &dyn SqlDialect,
    limit: u32,
    order_by: Option<&str>,
    extra_predicate: Option<crate::filter::CallerPredicate>,
) -> Result<GovernedRead, QueryError> {
    let type_name = TypeName(q.type_name.clone());
    let g = resolve_governed(ontology, acl, &subject.0, &type_name, OnMissing::NotFound).await?;
    compile_object_read_with(&g, q, subject, ontology, acl, dialect, limit, order_by, extra_predicate).await
}
```

(The existing `#[allow(clippy::too_many_arguments, reason = …)]` stays on both functions.)

Then add `compile_object_read_with` — the old body minus the prologue, on `Projection`:

```rust
/// The compile stage of a governed object read, over an already-resolved
/// [`GovernedType`] — so a caller that needed the governance context for its own
/// guards (`read_object_page`) resolves it exactly once. `ontology`/`acl` are still
/// needed to govern derived (aggregate-over-link) columns both-ends.
#[allow(
    clippy::too_many_arguments,
    reason = "governed-read compile function requires all builder parameters"
)]
pub async fn compile_object_read_with(
    g: &GovernedType,
    q: &ObjectQuery,
    subject: &Subject,
    ontology: &(dyn Ontology + Send + Sync),
    acl: &(dyn Acl + Send + Sync),
    dialect: &dyn SqlDialect,
    limit: u32,
    order_by: Option<&str>,
    extra_predicate: Option<crate::filter::CallerPredicate>,
) -> Result<GovernedRead, QueryError> {
    let type_name = TypeName(q.type_name.clone());

    // projection: type properties minus denied, preserving property order; fail-closed.
    let mut proj = Projection::visible(g)?;

    // Caller filters + object-set ids, visibility-gated and coerced; then pagination's
    // pre-coerced keyset predicate (trusted — read_object_page built it after its own
    // fail-closed identity guards passed).
    let mut predicates = seed_predicates(g, &proj.columns, &q.filters, &q.ids)?;
    if let Some(p) = extra_predicate {
        predicates.push(p);
    }

    // OR-groups: each `_or` param is its own parenthesized disjunction. Members reuse the
    // plain-predicate visibility + coercion, so a denied/masked column inside a group fails
    // the request exactly as a denied plain filter does — an OR-group adds a combinator, not
    // a governance bypass. Groups are ANDed above; row-filters/`_ids` are never disjoined.
    let mut or_groups: Vec<Vec<crate::filter::CallerPredicate>> =
        Vec::with_capacity(q.or_raw.len());
    for raw in &q.or_raw {
        let members = crate::filter::split_or_members(raw)?;
        let mut group = Vec::with_capacity(members.len());
        for member in &members {
            let (col, val) = crate::filter::split_member(member)?;
            group.push(coerce_visible_predicate(
                col,
                val,
                &g.otype,
                &proj.columns,
                &g.masked,
            )?);
        }
        or_groups.push(group);
    }

    // Derived properties (aggregate-over-link), governed both-ends — UNCHANGED logic:
    // keep the existing block verbatim, with these renames:
    //   `object_type` -> `g.otype`, `denied` -> `g.denied`, `masked` -> `g.masked`,
    //   `ontology`/`acl` as passed. It still fills derived_names/derived_types/
    //   derived_selects exactly as today (handler.rs:382-429).
    let mut derived_names: Vec<String> = Vec::new();
    let mut derived_types: Vec<String> = Vec::new();
    let mut derived_selects: Vec<crate::sql::DerivedSelect> = Vec::new();
    if !g.otype.derived.is_empty() {
        let links = ontology.links(&type_name, PageReq::unbounded()).await?;
        for d in &g.otype.derived {
            if g.denied.contains(&d.name) {
                continue;
            }
            if g.masked.contains(&d.name) {
                derived_names.push(d.name.clone());
                derived_types.push(d.ty.clone());
                derived_selects.push(crate::sql::DerivedSelect::Masked(d.name.clone()));
                continue;
            }
            let Some(link) = links.items.iter().find(|l| l.name == d.link) else {
                continue; // missing link -> omit (no define-time validation in part-1)
            };
            let target_pt = PolicyTarget::Type(link.to.clone());
            // Both-ends: the subject must be permitted to read the linked type.
            if acl.check(&subject.0, Action::Read, &target_pt).await? == Decision::Deny {
                continue;
            }
            let target_type = match ontology.get_type(&link.to).await {
                Ok(t) => t,
                Err(ControlPlaneError::NotFound(_)) => continue, // target type gone -> omit
                Err(other) => return Err(QueryError::ControlPlane(other)),
            };
            let (t_filters, t_denied, _t_masked) = load_policy(acl, &subject.0, &target_pt).await?;
            // Don't leak a target column the subject may not see, via an aggregate over it.
            if let Some(col) = agg_column(&d.agg)
                && t_denied.contains(col)
            {
                continue;
            }
            derived_names.push(d.name.clone());
            derived_types.push(d.ty.clone());
            derived_selects.push(crate::sql::DerivedSelect::Aggregate(Box::new(
                crate::sql::DerivedAggregate {
                    name: d.name.clone(),
                    agg: d.agg.clone(),
                    backing: link.backing.clone(),
                    target_table: target_type.table.clone(),
                    target_filters: t_filters,
                },
            )));
        }
    }

    // Compile over the PHYSICAL projection (derived columns ride in derived_selects).
    let (sql, params) = compile_select_with(
        dialect,
        &g.otype.table,
        &proj.columns,
        &proj.masked,
        &g.row_filters,
        &predicates,
        &or_groups,
        &derived_selects,
        order_by,
        limit,
    )?;
    // Output columns = physical (in order) ++ surviving derived (in order).
    for (name, ty) in derived_names.into_iter().zip(derived_types) {
        let is_masked = g.masked.contains(&name);
        proj.push(name, ty, is_masked);
    }
    Ok(GovernedRead {
        sql,
        params,
        columns: proj.columns,
        logical_types: proj.logical_types,
        masked_columns: proj.masked,
    })
}
```

- [ ] **Step 4: Rewire `read_object`** — replace its tail with `GovernedRead::into_object_rows`:

```rust
pub async fn read_object(
    q: &ObjectQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let g = compile_object_read(
        q,
        subject,
        deps.ontology,
        deps.acl,
        deps.serving.dialect(),
        deps.default_limit,
        None,
        None,
    )
    .await?;
    let served = deps.serving.fetch_rows(&g.sql, &g.params).await?;
    Ok(g.into_object_rows(served))
}
```

- [ ] **Step 5: Rewire `read_object_page`** — single resolution. Replace the prologue + `compile_object_read` call (`handler.rs:542-633`); everything from the `id_idx` extraction down is unchanged:

```rust
    let type_name = TypeName(q.type_name.clone());
    let g = resolve_governed(
        deps.ontology,
        deps.acl,
        &subject.0,
        &type_name,
        OnMissing::NotFound,
    )
    .await?;

    let identity = g
        .otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::BadPagination("type has no declared identity".to_string()))?;
    if g.identity_governed() {
        return Err(QueryError::BadPagination(
            "identity column not readable".to_string(),
        ));
    }

    let id_ty = prop_ty(&g.otype, &identity).unwrap_or("");

    // (keep the cursor_round_trips block and its comment verbatim, using `id_ty`)
    // (keep the extra_predicate block verbatim)

    let fetch_limit = limit.saturating_add(1);
    let gr = compile_object_read_with(
        &g,
        q,
        subject,
        deps.ontology,
        deps.acl,
        deps.serving.dialect(),
        fetch_limit,
        Some(&identity),
        extra_predicate,
    )
    .await?;
    let served = deps.serving.fetch_rows(&gr.sql, &gr.params).await?;
    debug_assert_eq!(
        served.columns, gr.columns,
        "serving engine returned columns out of the projected order"
    );
    // (id_idx / Page::from_keyset / ObjectRows construction: unchanged, over `gr`)
```

Delete the now-dead "Row filters aren't needed here…" comment and the discarded `load_policy` call.

- [ ] **Step 6: Rewire `vector_search`** — replace the prologue (`handler.rs:718-730`) and the post-filter policy load (`:750`); note the empty-hits early return stays BEFORE the identity guard, exactly as today:

```rust
    let type_name = TypeName(q.type_name.clone());
    // No-leak: unknown type and missing Read grant are both Forbidden.
    let g = resolve_governed(
        deps.ontology,
        deps.acl,
        &subject.0,
        &type_name,
        OnMissing::Forbidden,
    )
    .await?;

    // Engine kNN over the named index (ServingError::NoIndex/DimMismatch propagate as Serving).
    let rows = deps
        .serving
        .vector_search(
            &g.otype.table,
            &q.index_name,
            &q.query,
            q.k,
            q.nprobe,
            q.ef_search,
        )
        .await?;
    let mut hits = rows_to_hits(&rows);
    if hits.is_empty() {
        return Ok(hits);
    }

    // Row-filter post-filter. Fail closed: the search response *is* a list of identity
    // values, so a policy that denies or masks the identity column must not be silently
    // disregarded. Guard runs BEFORE the empty-filter early return so both policy shapes
    // refuse identically with a deliberate 403.
    if g.identity_governed() {
        return Err(QueryError::Forbidden);
    }
    if g.row_filters.is_empty() {
        return Ok(hits);
    }
    let identity = g
        .otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(g.otype.name.0.clone()))?;
    let candidate_strs: Vec<String> = hits.iter().map(|h| sqlvalue_to_id_string(&h.id)).collect();
    let Some(pred) = identity_in_predicate(&g.otype, &g.denied, &g.masked, &candidate_strs)?
    else {
        return Ok(hits); // no candidates to scope (empty handled above; defensive)
    };
    let limit = u32::try_from(candidate_strs.len()).unwrap_or(u32::MAX);
    let (sql, params) = compile_select_with(
        deps.serving.dialect(),
        &g.otype.table,
        std::slice::from_ref(&identity),
        &[],
        &g.row_filters,
        std::slice::from_ref(&pred),
        &[],
        &[],
        None,
        limit,
    )?;
    // (surviving-set retain: unchanged)
```

- [ ] **Step 7: Run the new test + the read-path regression targets**

Run: `buck2 test //src/services/query-api:read-page-single-resolve //src/services/query-api:sql-compile //src/services/query-api:identity-in-predicate //src/services/query-api:vector-search-filter > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log`
Expected: PASS (all listed; check the exact target names with `grep 'name = ' src/services/query-api/BUCK` and substitute if they differ — the intent is: the new counting test, the SQL compile suite, and the vector-search post-filter suite).

- [ ] **Step 8: Format and commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/handler.rs src/services/query-api/tests/read_page_single_resolve.rs
git add -A && git commit -m "refactor(query): compile_object_read over the spine; read_object_page resolves governance once"
```

---

### Task 5: `resolve_chain` + `read_linked_chain` + `read_associations` rewire

**Files:**
- Modify: `src/services/query-api/src/handler.rs`

**Interfaces:**
- Consumes: `resolve_governed(OnMissing::{NotFound,Internal})`, `resolve_hop`, `Projection`, `prop_ty`, `coerce_visible_predicate`, `identity_in_predicate` (Tasks 1–3).
- Produces: private `resolve_chain` now returns `(Vec<GovernedType>, Vec<crate::sql::ChainType>, Vec<control_plane_core::LinkBacking>)`; the `HopMeta` struct is **deleted**. Public signatures of `read_linked_objects`, `read_linked_chain`, `read_associations` unchanged.

- [ ] **Step 1: Baseline the pinning tests**

Run: `buck2 test //src/services/query-api:hop-types //src/services/query-api:link-traversal //src/services/query-api:associations //src/services/query-api:chain-filter-resolve //src/services/query-api:compile-chain-pairs > /tmp/t5-before.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5-before.log`
Expected: PASS (green baseline; substitute exact target names from `grep 'name = ' src/services/query-api/BUCK` — the intent is every chain/association/hop unit target).

- [ ] **Step 2: Rewrite `resolve_chain`** (delete `HopMeta`; keep the doc comment, updating the return description):

```rust
async fn resolve_chain(
    q: &ChainQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<
    (
        Vec<GovernedType>,
        Vec<crate::sql::ChainType>,
        Vec<control_plane_core::LinkBacking>,
    ),
    QueryError,
> {
    if q.path.is_empty() || q.path.len() > MAX_CHAIN_DEPTH {
        return Err(QueryError::BadChain(format!(
            "path length {} (allowed 1..={MAX_CHAIN_DEPTH})",
            q.path.len()
        )));
    }

    let from_name = TypeName(q.from_type.clone());
    // Read on the source (deny-by-default, before existence is revealed).
    let source = resolve_governed(
        deps.ontology,
        deps.acl,
        &subject.0,
        &from_name,
        OnMissing::NotFound,
    )
    .await?;

    let mut ctypes: Vec<crate::sql::ChainType> = vec![crate::sql::ChainType {
        table: source.otype.table.clone(),
        row_filters: source.row_filters.clone(),
        predicates: vec![],
    }];
    let mut metas: Vec<GovernedType> = vec![source];
    let mut hops: Vec<control_plane_core::LinkBacking> = Vec::with_capacity(q.path.len());

    let mut current_name = from_name;
    for hop in &q.path {
        let (next_name, backing) = resolve_hop(deps.ontology, &current_name, hop).await?;
        // Read on every reached type (the leak-free guarantee), forward or inverse. A
        // link pointing at a missing type is an internal inconsistency, not a 404.
        let next = resolve_governed(
            deps.ontology,
            deps.acl,
            &subject.0,
            &next_name,
            OnMissing::Internal,
        )
        .await?;
        hops.push(backing);
        ctypes.push(crate::sql::ChainType {
            table: next.otype.table.clone(),
            row_filters: next.row_filters.clone(),
            predicates: vec![],
        });
        metas.push(next);
        current_name = next_name;
    }

    // Caller filters, governed per position: visibility first (denied/masked or unknown
    // column -> 400, no type-info leak), then parse the raw value into a typed predicate
    // bound at the position's alias `t_i`. The per-position visible projection is
    // computed ONCE, not per filter.
    let allowed_per_position: Vec<Vec<String>> = metas.iter().map(GovernedType::allowed).collect();
    for f in &q.filters {
        let (Some(meta), Some(allowed)) = (
            metas.get(f.position),
            allowed_per_position.get(f.position),
        ) else {
            return Err(QueryError::BadFilter(f.column.clone()));
        };
        let p = coerce_visible_predicate(&f.column, &f.raw, &meta.otype, allowed, &meta.masked)?;
        ctypes
            .get_mut(f.position)
            .ok_or_else(|| QueryError::BadFilter(f.column.clone()))?
            .predicates
            .push(p);
    }

    // Object-set input: scope the SOURCE (position 0) to the given identities.
    let source = metas
        .first()
        .ok_or_else(|| QueryError::BadChain("empty chain".to_string()))?;
    if let Some(p) = identity_in_predicate(&source.otype, &source.denied, &source.masked, &q.ids)? {
        ctypes
            .first_mut()
            .ok_or_else(|| QueryError::BadChain("empty chain".to_string()))?
            .predicates
            .push(p);
    }

    Ok((metas, ctypes, hops))
}
```

Note the `coerce_visible_predicate` unification is behavior-identical to the old inline check: `!allowed.contains(col) || masked.contains(col) → BadFilter`, then `coerce_predicate`.

- [ ] **Step 3: Rewire `read_linked_chain`** — projection epilogue onto `Projection`:

```rust
pub async fn read_linked_chain(
    q: &ChainQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let (metas, ctypes, hops) = resolve_chain(q, subject, deps).await?;
    // Final-target projection, from the last position (path is non-empty => >= 2 metas).
    let target = metas
        .last()
        .ok_or_else(|| QueryError::BadChain("empty chain".to_string()))?;
    let proj = Projection::visible(target)?;

    let (sql, params) = compile_chain_with(
        deps.serving.dialect(),
        &ctypes,
        &hops,
        &proj.columns,
        &proj.masked,
        target.otype.identity.as_deref(),
        deps.default_limit,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    Ok(proj.into_object_rows(served))
}
```

- [ ] **Step 4: Rewire `read_associations`** — only the metadata accesses change (`HopMeta` fields → `GovernedType` fields + `prop_ty`); keep every check and its comment:

```rust
    let (metas, ctypes, hops) = resolve_chain(q, subject, deps).await?;
    let source = metas
        .first()
        .ok_or_else(|| QueryError::BadChain("empty chain".to_string()))?;
    let target = metas
        .last()
        .ok_or_else(|| QueryError::BadChain("empty chain".to_string()))?;

    // Both projected ends must declare an identity.
    let source_id = source
        .otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(source.otype.name.0.clone()))?;
    let target_id = target
        .otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(target.otype.name.0.clone()))?;

    // …and the identity column must be visible (not denied, not masked) on each end —
    // you cannot associate objects you cannot identify.
    let s_allowed = source.allowed();
    if !s_allowed.contains(&source_id) || source.masked.contains(&source_id) {
        return Err(QueryError::Forbidden);
    }
    let t_allowed = target.allowed();
    if !t_allowed.contains(&target_id) || target.masked.contains(&target_id) {
        return Err(QueryError::Forbidden);
    }

    let from_id_type = prop_ty(&source.otype, &source_id)
        .map(str::to_string)
        .unwrap_or_default();
    let to_id_type = prop_ty(&target.otype, &target_id)
        .map(str::to_string)
        .unwrap_or_default();
    // (compile_chain_pairs call + pairs extraction: unchanged)
```

- [ ] **Step 5: Re-run the pinning targets**

Run: `buck2 test //src/services/query-api:hop-types //src/services/query-api:link-traversal //src/services/query-api:associations //src/services/query-api:chain-filter-resolve //src/services/query-api:compile-chain-pairs > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5.log`
Expected: PASS, same test counts as the baseline.

- [ ] **Step 6: Format and commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/handler.rs
git add -A && git commit -m "refactor(query): chain + association reads over the governed spine (HopMeta deleted)"
```

---

### Task 6: `resolve_graph` + the four graph endpoints rewire

**Files:**
- Modify: `src/services/query-api/src/handler.rs`

**Interfaces:**
- Consumes: everything from Tasks 1–3.
- Produces: private `GraphResolved` shrinks to `{ g: GovernedType, identity: String, steps: Vec<crate::sql::GraphStep>, proj: Projection, seed_predicates: Vec<crate::filter::CallerPredicate> }`. Public signatures of `read_graph_reach`, `read_graph_tree`, `read_graph_reach_union`, `read_graph_reach_with_tail` unchanged.

- [ ] **Step 1: Baseline the pinning targets**

Run: `buck2 test //src/services/query-api:graph-reach //src/services/query-api:graph-tree //src/services/query-api:graph-reach-union //src/services/query-api:graph-reach-tail //src/services/query-api:compile-graph-reach //src/services/query-api:compile-graph-tree //src/services/query-api:compile-graph-reach-union //src/services/query-api:compile-graph-reach-tail > /tmp/t6-before.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6-before.log`
Expected: PASS (substitute exact target names from the BUCK file).

- [ ] **Step 2: Rewrite `GraphResolved` + `resolve_graph`**

```rust
/// Everything the two graph reads (reachable-set and shortest-path-tree) need after
/// resolving the queried type, ACL policy, and the path-cycle: the governed type (whose
/// row-filters govern the recursion start), the declared identity (the recursion's
/// dedup key), the compiler `GraphStep`s (per-intermediate governance folded in), the
/// visible/masked projection, and the coerced seed predicates.
struct GraphResolved {
    g: GovernedType,
    identity: String,
    steps: Vec<crate::sql::GraphStep>,
    proj: Projection,
    seed_predicates: Vec<crate::filter::CallerPredicate>,
}

async fn resolve_graph(
    q: &GraphQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<GraphResolved, QueryError> {
    let type_name = TypeName(q.type_name.clone());
    let g = resolve_governed(
        deps.ontology,
        deps.acl,
        &subject.0,
        &type_name,
        OnMissing::NotFound,
    )
    .await?;

    // Declared identity is the recursion's dedup key.
    let identity = g
        .otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(q.type_name.clone()))?;

    // Resolve the path-cycle: walk l1..lK from the queried type. Each landed type is
    // Read-gated and its row-filters loaded (intermediate governance). After the last
    // link the type must be the queried type again (a cycle) — else it cannot repeat.
    if q.path.is_empty() {
        return Err(QueryError::NotCyclicPath(String::new()));
    }
    let mut steps: Vec<crate::sql::GraphStep> = Vec::with_capacity(q.path.len());
    let mut current = type_name.clone();
    let last = q.path.len() - 1;
    for (i, hop) in q.path.iter().enumerate() {
        let (landed, backing) = resolve_hop(deps.ontology, &current, hop).await?;
        // Read on every reached type (intermediate + final), forward or inverse. A link
        // pointing at a missing type is an internal inconsistency, not a 404.
        let landed_g = resolve_governed(
            deps.ontology,
            deps.acl,
            &subject.0,
            &landed,
            OnMissing::Internal,
        )
        .await?;
        // Intermediates carry their own row-filters; the FINAL landing is the start
        // type, whose filters are rendered at `nxt` by the compiler -> pass empty here
        // (no double-render).
        steps.push(crate::sql::GraphStep {
            backing,
            next_table: landed_g.otype.table.clone(),
            next_filters: if i == last {
                Vec::new()
            } else {
                landed_g.row_filters
            },
        });
        current = landed;
    }
    if current != type_name {
        return Err(QueryError::NotCyclicPath(hop_path_string(&q.path)));
    }

    // Projection: visible columns minus denied; masked applied. Empty -> Forbidden.
    let proj = Projection::visible(&g)?;

    // Seed predicates: source filters (visibility-checked + coerced) then the ?_ids= set.
    let seed = seed_predicates(&g, &proj.columns, &q.filters, &q.ids)?;

    Ok(GraphResolved {
        g,
        identity,
        steps,
        proj,
        seed_predicates: seed,
    })
}
```

- [ ] **Step 3: Rewire `read_graph_reach`**

```rust
pub async fn read_graph_reach(
    q: &GraphQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let r = resolve_graph(q, subject, deps).await?;

    let (sql, params) = crate::sql::compile_graph_reach(
        deps.serving.dialect(),
        &r.g.otype.table,
        &r.identity,
        &r.steps,
        &r.seed_predicates,
        &r.g.row_filters,
        &r.proj.columns,
        &r.proj.masked,
        q.depth,
        deps.default_limit,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    Ok(r.proj.into_object_rows(served))
}
```

- [ ] **Step 4: Rewire `read_graph_tree`** — keep the tree-specific column-order assert and node split; source columns/types from `r.proj`:

```rust
    let r = resolve_graph(q, subject, deps).await?;

    // The tree projects identity (id + parent). A denied/masked identity would leak -> Forbidden.
    if r.g.identity_governed() {
        return Err(QueryError::Forbidden);
    }

    let (sql, params) = crate::sql::compile_graph_tree(
        deps.serving.dialect(),
        &r.g.otype.table,
        &r.identity,
        &r.steps,
        &r.seed_predicates,
        &r.g.row_filters,
        &r.proj.columns,
        &r.proj.masked,
        q.depth,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    // (keep the existing debug_assert_eq! block verbatim, with `r.allowed` -> `r.proj.columns`)

    let identity_type = prop_ty(&r.g.otype, &r.identity)
        .map(str::to_string)
        .unwrap_or_default();

    // Each served row is [object cells..., __depth, __parent, __id] (compile_graph_tree
    // order). Pop the three trailing columns; the remainder are the object cells.
    let ncols = r.proj.columns.len();
    // (keep the existing nodes mapping verbatim)

    let Projection {
        columns,
        logical_types,
        ..
    } = r.proj;
    Ok(ObjectTree {
        columns,
        logical_types,
        identity_type,
        nodes,
    })
```

- [ ] **Step 5: Rewire `read_graph_reach_union`** — prologue → `resolve_governed(OnMissing::NotFound)`; the self-link-set resolution block stays verbatim (it is union-specific, not a hop walk); projection → `Projection::visible(&g)`; seeds → `seed_predicates(&g, &proj.columns, &q.filters, &q.ids)`; epilogue → `proj.into_object_rows(served)`:

```rust
pub async fn read_graph_reach_union(
    q: &GraphUnionQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<ObjectRows, QueryError> {
    let type_name = TypeName(q.type_name.clone());
    let g = resolve_governed(
        deps.ontology,
        deps.acl,
        &subject.0,
        &type_name,
        OnMissing::NotFound,
    )
    .await?;

    // Declared identity is the recursion's dedup key.
    let identity = g
        .otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(q.type_name.clone()))?;

    // (self-link set resolution: keep the existing block verbatim — `q.links` empty ->
    // NotCyclicPath; links() once; per-name find -> UnknownLink; `to != type_name` ->
    // NotCyclicPath; dedup by first-seen name into `backings`)

    let proj = Projection::visible(&g)?;
    let seeds = seed_predicates(&g, &proj.columns, &q.filters, &q.ids)?;

    let (sql, params) = crate::sql::compile_graph_reach_union(
        deps.serving.dialect(),
        &g.otype.table,
        &identity,
        &backings,
        &seeds,
        &g.row_filters,
        &proj.columns,
        &proj.masked,
        q.depth,
        deps.default_limit,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    Ok(proj.into_object_rows(served))
}
```

- [ ] **Step 6: Rewire `read_graph_reach_with_tail`** — prologue → `resolve_governed(NotFound)`; tail walk → `resolve_hop` + `resolve_governed(Internal)` with the final-landing fold as a `GovernedType`; note the tail hops are forward-only, so build the hop as `Hop::from(link_name.as_str())`:

```rust
    let type_name = TypeName(q.type_name.clone());
    let g = resolve_governed(
        deps.ontology,
        deps.acl,
        &subject.0,
        &type_name,
        OnMissing::NotFound,
    )
    .await?;

    // Declared identity: the recursion's dedup key and the tail's join key back to reach.
    let identity = g
        .otype
        .identity
        .clone()
        .ok_or_else(|| QueryError::NoIdentity(q.type_name.clone()))?;

    // (tail-non-empty check: keep verbatim)
    // (core self-link resolution: keep verbatim — links() find -> UnknownLink;
    //  `core.to != type_name` -> BadGraphPath; `core_backing = core.backing.clone()`)

    // Resolve the forward tail. Position 0 is the queried type with EMPTY row-filters —
    // its governance lives in the recursive CTE; the tail constrains it by
    // reach-membership. Each tail-landed type is Read-gated and its row-filters loaded;
    // the final landing is projected.
    let mut tail_types: Vec<crate::sql::ChainType> = vec![crate::sql::ChainType {
        table: g.otype.table.clone(),
        row_filters: vec![],
        predicates: vec![],
    }];
    let mut tail_hops: Vec<control_plane_core::LinkBacking> =
        Vec::with_capacity(q.tail_links.len());
    let mut current = type_name.clone();
    let mut final_g = g.clone();
    for link_name in &q.tail_links {
        let (landed, backing) =
            resolve_hop(deps.ontology, &current, &Hop::from(link_name.as_str())).await?;
        let landed_g = resolve_governed(
            deps.ontology,
            deps.acl,
            &subject.0,
            &landed,
            OnMissing::Internal,
        )
        .await?;
        tail_hops.push(backing);
        tail_types.push(crate::sql::ChainType {
            table: landed_g.otype.table.clone(),
            row_filters: landed_g.row_filters.clone(),
            predicates: vec![],
        });
        final_g = landed_g;
        current = landed;
    }

    // Projection: the FINAL tail type's visible columns (masked -> marker). Empty -> Forbidden.
    let proj = Projection::visible(&final_g)?;

    // Seed predicates scope the recursion start (alias `s` in the CTE), governed by the
    // QUERIED type's projection.
    let source_allowed = g.allowed();
    let seeds = seed_predicates(&g, &source_allowed, &q.filters, &q.ids)?;

    let (sql, params) = crate::sql::compile_graph_reach_tail(
        deps.serving.dialect(),
        &g.otype.table,
        &identity,
        &core_backing,
        &seeds,
        &g.row_filters,
        &tail_types,
        &tail_hops,
        &proj.columns,
        &proj.masked,
        final_g.otype.identity.as_deref(),
        q.depth,
        deps.default_limit,
    )?;
    let served = deps.serving.fetch_rows(&sql, &params).await?;
    Ok(proj.into_object_rows(served))
```

- [ ] **Step 7: Re-run the pinning targets**

Run: the same target list as Step 1 → `/tmp/t6.log`.
Expected: PASS, same test counts as the baseline.

- [ ] **Step 8: Format and commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/handler.rs
git add -A && git commit -m "refactor(query): graph reads over the governed spine (GraphResolved on GovernedType/Projection)"
```

---

### Task 7: Full verification, clippy, registers evidence

**Files:**
- Modify (evidence only): none beyond what fails.

- [ ] **Step 1: Clippy the crate**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/clippy.log 2>&1 && buck2 build --out /tmp/clippy-out.txt '//src/services/query-api:query-api[clippy.txt]' 2>/dev/null; cat /tmp/clippy-out.txt`
Expected: empty output (clean). Fix any lint with `#[expect(lint, reason = "…")]` only where structurally unavoidable.

- [ ] **Step 2: Full query-api test sweep** (fixture tests route local automatically via `loom_fixture_test`)

Run: `buck2 test //src/services/query-api/... > /tmp/t7.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t7.log`
Expected: `Tests finished: … Fail 0`. The e2e suite (`governed_read`, `object_pagination_e2e`, `graph_*_e2e`, `vector_search_e2e`, `governed_flight_export_e2e`, `wire_governance_e2e`, …) pins the 403/404 split, pagination, masking, and export-schema behavior.

- [ ] **Step 3: Duplication + complexity evidence**

Invoke the `loom-duplication` skill with `diff` and the `loom-complexity` skill with `diff` (terminal-only mode; no commits). Expected: the branch's handler.rs duplication pairs (the `1221-1247 ≈ 1094-1120` family and the seed-loop family) are gone or reduced, and the handler cc hotspots drop. Save both outputs for the PR description.

- [ ] **Step 4: Lint hooks**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; tail -5 /tmp/prek.log`
Expected: all hooks pass; commit anything the fixers changed.

- [ ] **Step 5: Commit any residue**

```bash
git add -A && git diff --cached --quiet || git commit -m "chore(query): lint/format residue from governed-spine extraction"
```

Finishing (outside this plan's tasks, via superpowers:finishing-a-development-branch): push `work/road-qa-governance-layer`, open the PR (head = that branch), and in the same PR run the `loom-docs-update` skill to close `road-qa-governance-layer` in `docs/ROADMAP.md` (`- [ ]`→`- [x]`, `status:done`, `pr:#N`). The PR description must call out the two accepted micro-divergences from the Global Constraints section and paste the duplication/complexity diff evidence.
