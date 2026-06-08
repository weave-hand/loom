# Control-plane Group 4 items 2–4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire a pagination convention into every unbounded control-plane read, add property-based serde round-trip coverage, and document the unused error variants.

**Architecture:** `core` gains `Page<T>`/`PageReq`/`Cursor` types; the 8 unbounded read methods on the `Catalog`/`Ontology`/`Acl`/`Lineage` traits gain a trailing `page: PageReq` param and return `Page<T>` instead of `Vec<T>`. Both adapters (memory, postgres) wrap their existing result in `Page::from_full(rows)` and ignore the request (limiting is deferred). proptest is added as a (non-vendored) dep to `core` and `postgres` for new `rust_test` integration targets. No SQL changes, so no `.sqlx` regeneration.

**Tech Stack:** Rust 2024, buck2, `async-trait`, `sqlx` 0.9 (compile-time `query!`), `proptest` 1.x, the in-tree `PgFixture` hermetic Postgres.

---

## Background the implementer needs

**Testing model (critical):** loom runs **only** `rust_test` targets over `tests/*.rs` files. Inline `#[cfg(test)]` modules in library crates are **not** run by `buck2 test` (there is no unittest target anywhere in the tree). The shared conformance suite lives in `src/control-plane/testkit/src/lib.rs` as `pub` functions; each adapter crate has per-concern `rust_test` targets (`memory:catalog`, `postgres:catalog`, …) that call those functions against a concrete adapter. **All new tests must be `rust_test` targets exercising the public API.**

**Adapter file layout:** both `memory` and `postgres` split each concern into its own file (`catalog.rs`, `ontology.rs`, `acl.rs`, `lineage.rs`), each containing `impl <Trait> for <AdapterStruct>`.

**Build/test commands:**
- Build all: `buck2 build //src/...`
- Test one target: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:catalog`
- Test everything: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...`
- `--local-only` is required: the hermetic Postgres/DuckDB refuse to run as root on remote execution.
- Lint: `tools/clippy-all.sh` and `buck2 run //tools:prek -- run --all-files`

**Dependency workflow (reindeer, non-vendored):** to add a third-party crate, edit the crate's `Cargo.toml`, run `cargo generate-lockfile` (or `buck2 run //tools:reindeer -- update`), then `./tools/buckify.sh`, then reference it as `//third-party:<crate>`. reindeer **skips `[dev-dependencies]`** in non-vendored mode, so test-only crates must be declared under `[dependencies]` (this is the existing precedent — see `src/control-plane/worker/Cargo.toml`'s `tracing-test`). The `reindeer-check` prek hook fails if `Cargo.toml`/`Cargo.lock` and `third-party/BUCK` drift.

**Deviation from the spec (read before Task 7):** the spec's item-2 "pg helper proptest" called for a dedicated round-trip test of the private codec helpers (`event_type_to_str`/`from_str`, `cardinality_*`, `action_to_str`) in `postgres/src/lib.rs`. Those helpers are **private** and loom runs only integration tests, so they are unreachable without exposing internals or inventing a unit-test target. Instead, this plan exercises the `event_type` codec through the **public** envelope round-trip (Task 7 generates events spanning all `EventType` variants), and relies on the existing ontology/acl conformance tests to round-trip the `Cardinality`/`Action` codecs. No dedicated private-codec test is added.

---

## File Structure

- **Create** `src/control-plane/core/src/page.rs` — `Cursor`, `PageReq`, `Page<T>` (the pagination convention types). One responsibility: the page/cursor vocabulary.
- **Create** `src/control-plane/core/tests/page.rs` — `rust_test` exercising the page types' helpers + `Cursor` serde.
- **Create** `src/control-plane/core/tests/serde_roundtrip.rs` — `rust_test` with proptest serde round-trips for `RowFilter`/`ScalarValue`.
- **Create** `src/control-plane/postgres/tests/lineage_roundtrip.rs` — `rust_test` with proptest envelope round-trip (`emit` → `events_for`) over the `PgFixture`.
- **Modify** `src/control-plane/core/src/lib.rs` — `mod page;` + `pub use page::{...}`.
- **Modify** `core/src/{catalog,ontology,acl,lineage}.rs` — trait signatures.
- **Modify** `memory/src/{catalog,ontology,acl,lineage}.rs` and `postgres/src/{catalog,ontology,acl,lineage}.rs` — impl signatures.
- **Modify** `testkit/src/lib.rs` — conformance call sites.
- **Modify** `core/BUCK`, `postgres/BUCK` — new `rust_test` targets + proptest deps.
- **Modify** `core/Cargo.toml`, `postgres/Cargo.toml` — `proptest` dep; fix stale sqlx comment in postgres.
- **Modify** `core/src/error.rs` — doc comments + `Conflict` display test.

---

## Task 1: Pagination types (`Page`/`PageReq`/`Cursor`) in core

**Files:**
- Create: `src/control-plane/core/src/page.rs`
- Create: `src/control-plane/core/tests/page.rs`
- Modify: `src/control-plane/core/src/lib.rs`
- Modify: `src/control-plane/core/BUCK`

- [ ] **Step 1: Write `page.rs`**

Create `src/control-plane/core/src/page.rs`:

```rust
//! The pagination convention shared by every unbounded control-plane read.
//!
//! Read methods take a [`PageReq`] and return a [`Page<T>`]. Today every adapter
//! returns the full result set in a single page ([`Page::from_full`], `next: None`):
//! the request's `after`/`limit` are part of the stable signature but **not yet
//! enforced**. Real keyset limiting is a future adapter-only change that needs no
//! trait-signature churn — that is the whole point of fixing the convention now.

use serde::{Deserialize, Serialize};

/// An opaque keyset position. The encoding is adapter-defined and NOT part of the
/// contract — callers round-trip it verbatim (conventionally base64 of a keyset).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor(pub String);

/// A page request. `Default`/[`PageReq::unbounded`] = no limit, from the start.
///
/// `after`/`limit` are accepted but **not yet enforced** by any adapter (see the
/// module docs); a request for `limit(10)` currently still returns everything.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PageReq {
    /// Resume after this cursor (exclusive). `None` = from the start.
    pub after: Option<Cursor>,
    /// Maximum items to return. `None` = unbounded.
    pub limit: Option<u32>,
}

impl PageReq {
    /// No cursor, no limit.
    pub fn unbounded() -> Self {
        Self::default()
    }
    /// Bounded by `n`, from the start.
    pub fn limit(n: u32) -> Self {
        Self { after: None, limit: Some(n) }
    }
    /// From `c`, unbounded.
    pub fn after(c: Cursor) -> Self {
        Self { after: Some(c), limit: None }
    }
}

/// One page of results. `next == None` means there are no more.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    /// Cursor to fetch the next page, or `None` if this is the last page.
    pub next: Option<Cursor>,
}

impl<T> Page<T> {
    /// The whole result set as a single, final page (`next: None`). What every
    /// adapter returns today.
    pub fn from_full(items: Vec<T>) -> Self {
        Self { items, next: None }
    }
    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl<T> IntoIterator for Page<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}
```

- [ ] **Step 2: Re-export from `lib.rs`**

In `src/control-plane/core/src/lib.rs`, add `mod page;` alongside the other `mod` lines (keep alphabetical: after `mod ontology;`/before `mod queue;`), and add the re-export after the `ontology` re-export:

```rust
pub use page::{Cursor, Page, PageReq};
```

- [ ] **Step 3: Write the failing test `tests/page.rs`**

Create `src/control-plane/core/tests/page.rs`:

```rust
use control_plane_core::{Cursor, Page, PageReq};

#[test]
fn from_full_is_a_single_final_page() {
    let p = Page::from_full(vec![1, 2, 3]);
    assert_eq!(p.items, vec![1, 2, 3]);
    assert_eq!(p.next, None);
    assert_eq!(p.len(), 3);
    assert!(!p.is_empty());
    assert!(Page::<i32>::from_full(vec![]).is_empty());
}

#[test]
fn page_into_iter_yields_items() {
    let collected: Vec<i32> = Page::from_full(vec![10, 20]).into_iter().collect();
    assert_eq!(collected, vec![10, 20]);
}

#[test]
fn page_req_constructors() {
    assert_eq!(PageReq::unbounded(), PageReq::default());
    assert_eq!(PageReq::unbounded(), PageReq { after: None, limit: None });
    assert_eq!(PageReq::limit(10), PageReq { after: None, limit: Some(10) });
    assert_eq!(
        PageReq::after(Cursor("c".into())),
        PageReq { after: Some(Cursor("c".into())), limit: None }
    );
}

#[test]
fn cursor_json_round_trips() {
    let c = Cursor("opaque-keyset-token".into());
    let json = serde_json::to_string(&c).unwrap();
    assert_eq!(serde_json::from_str::<Cursor>(&json).unwrap(), c);
}
```

- [ ] **Step 4: Add the `rust_test` target to `core/BUCK`**

Append to `src/control-plane/core/BUCK` (this is the crate's first test target):

```python
rust_test(
    name = "page",
    crate = "page",
    srcs = ["tests/page.rs"],
    crate_root = "tests/page.rs",
    edition = "2024",
    deps = [
        ":core",
        "//third-party:serde_json",
    ],
)
```

- [ ] **Step 5: Build and test**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/core:page`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/core/src/page.rs src/control-plane/core/src/lib.rs \
        src/control-plane/core/tests/page.rs src/control-plane/core/BUCK
git commit -m "feat(core): add Page/PageReq/Cursor pagination convention types"
```

---

## Task 2: Paginate the `Catalog` reads

The transformation pattern for **every** paginated method (apply identically in Tasks 2–5):
- **Trait:** change return `Result<Vec<T>>` → `Result<Page<T>>`, add a trailing `page: PageReq` param, and add one doc line: "The `page` request is accepted but not yet enforced; results are returned as a single full page."
- **Adapter impls:** rename the param to `_page: PageReq` (unused), change the return type, and wrap the existing returned vec: `Ok(v)` → `Ok(Page::from_full(v))`. No other body change.
- **Imports:** add `Page`/`PageReq` to the `use control_plane_core::{...}` (or `crate::`) line in each file.

**Files:**
- Modify: `src/control-plane/core/src/catalog.rs:62,65`
- Modify: `src/control-plane/memory/src/catalog.rs`
- Modify: `src/control-plane/postgres/src/catalog.rs`
- Modify: `src/control-plane/testkit/src/lib.rs` (catalog conformance fns)

- [ ] **Step 1: Edit the `Catalog` trait**

In `src/control-plane/core/src/catalog.rs`, ensure `Page`/`PageReq` are imported from the crate root (add `use crate::page::{Page, PageReq};` near the top if not already pulled in via `crate::`), then change:

```rust
    /// Snapshot history for `table`, newest first. `NotFound` if the table is
    /// unknown. The `page` request is accepted but not yet enforced; results are a
    /// single full page.
    async fn snapshots(&self, table: &TableRef, page: PageReq) -> Result<Page<Snapshot>>;

    /// Files visible at snapshot `at` of `table`. `NotFound` if the table or
    /// snapshot is unknown. The `page` request is accepted but not yet enforced;
    /// results are a single full page.
    async fn files(&self, table: &TableRef, at: SnapshotId, page: PageReq)
    -> Result<Page<FileRef>>;
```

(Preserve whatever the existing doc text said about ordering/NotFound — only append the page sentence and change the signature.)

- [ ] **Step 2: Edit the memory adapter**

In `src/control-plane/memory/src/catalog.rs`, for both `snapshots` and `files`: add `Page, PageReq` to the `use control_plane_core::{...}` line, change the signatures to match the trait (param named `_page: PageReq`), and wrap the final returned vec in `Page::from_full(...)`. Example for `snapshots` (adapt the body that exists):

```rust
    async fn snapshots(&self, table: &TableRef, _page: PageReq) -> Result<Page<Snapshot>> {
        // ... existing lookup that produced `let history: Vec<Snapshot> = ...` ...
        Ok(Page::from_full(history))
    }
```

Apply the same shape to `files` (wrap its existing `Ok(files_vec)`).

- [ ] **Step 3: Edit the postgres adapter**

In `src/control-plane/postgres/src/catalog.rs`, same edit for `snapshots` and `files`: add `Page, PageReq` to imports, change signatures (`_page: PageReq`), wrap the existing `Ok(out)` / returned vec in `Page::from_full(...)`. The `sqlx::query!` calls are unchanged (no `.sqlx` regeneration).

- [ ] **Step 4: Update the catalog conformance call sites in testkit**

In `src/control-plane/testkit/src/lib.rs`, add `Page, PageReq` to the `use control_plane_core::{...}` import. Then for every `catalog.snapshots(&t)` / `catalog.files(&t, s)` call (lines ~282, 287, 293, 343, 411, 415, 429, 439, 450):
- add `PageReq::unbounded()` as the final argument, e.g. `catalog.files(&t, s1, PageReq::unbounded())`;
- `.len()` / `.is_empty()` calls work unchanged (inherent on `Page`);
- where the result is bound and indexed/compared to a `Vec` (e.g. `let hist = catalog.snapshots(&t, PageReq::unbounded()).await.unwrap();` then `hist[0]`), append `.items`: `... .await.unwrap().items;`
- `matches!(catalog.files(&t, before, PageReq::unbounded()).await, Err(NotFound(_)))` — just add the arg; the error path is unchanged.

- [ ] **Step 5: Build and test catalog**

Run:
```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:catalog //src/control-plane/postgres:catalog
```
Expected: PASS, same test counts as before (behaviour preserved).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/core/src/catalog.rs src/control-plane/memory/src/catalog.rs \
        src/control-plane/postgres/src/catalog.rs src/control-plane/testkit/src/lib.rs
git commit -m "feat(control-plane): paginate Catalog::{snapshots,files}"
```

---

## Task 3: Paginate the `Ontology` reads

**Files:**
- Modify: `src/control-plane/core/src/ontology.rs:64,66`
- Modify: `src/control-plane/memory/src/ontology.rs`
- Modify: `src/control-plane/postgres/src/ontology.rs`
- Modify: `src/control-plane/testkit/src/lib.rs` (ontology conformance fns)

- [ ] **Step 1: Edit the `Ontology` trait**

In `src/control-plane/core/src/ontology.rs`:

```rust
    /// All object types (order unspecified). The `page` request is accepted but not
    /// yet enforced; results are a single full page.
    async fn list_types(&self, page: PageReq) -> Result<Page<ObjectType>>;

    /// Links declared on `name` (order unspecified). `NotFound` if `name` is
    /// unknown. The `page` request is accepted but not yet enforced; results are a
    /// single full page.
    async fn links(&self, name: &TypeName, page: PageReq) -> Result<Page<LinkDef>>;
```

(Preserve existing doc semantics; only append the page sentence.)

- [ ] **Step 2: Edit both adapters**

In `src/control-plane/memory/src/ontology.rs` and `src/control-plane/postgres/src/ontology.rs`: add `Page, PageReq` imports, change `list_types`/`links` signatures (`_page: PageReq`), wrap the returned vec in `Page::from_full(...)`. No SQL change.

- [ ] **Step 3: Update the ontology conformance call sites in testkit**

In `src/control-plane/testkit/src/lib.rs`:
- `o.list_types()` (~line 537) → `o.list_types(PageReq::unbounded())`. The following `.await.unwrap().into_iter().map(...)` works unchanged (`Page` is `IntoIterator`).
- `o.links(&tn("Order"))` (~576) compared to `vec![link.clone()]` → `o.links(&tn("Order"), PageReq::unbounded()).await.unwrap().items` so the `assert_eq!` compares `Vec` to `Vec`.
- `let ls = o.links(&tn("Order")).await.unwrap();` (~585) → add the arg; append `.items` if `ls` is later indexed/compared as a `Vec` (otherwise leave as `Page` and use `.len()`/iteration).
- `o.links(&nope, PageReq::unbounded()).await` in the `matches!(…, Err(NotFound(_)))` (~615) — add the arg only.

- [ ] **Step 4: Build and test ontology**

Run:
```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:ontology //src/control-plane/postgres:ontology
```
Expected: PASS, counts unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/core/src/ontology.rs src/control-plane/memory/src/ontology.rs \
        src/control-plane/postgres/src/ontology.rs src/control-plane/testkit/src/lib.rs
git commit -m "feat(control-plane): paginate Ontology::{list_types,links}"
```

---

## Task 4: Paginate the `Acl` read

**Files:**
- Modify: `src/control-plane/core/src/acl.rs:134-135`
- Modify: `src/control-plane/memory/src/acl.rs`
- Modify: `src/control-plane/postgres/src/acl.rs`
- Modify: `src/control-plane/testkit/src/lib.rs` (acl conformance fns)

- [ ] **Step 1: Edit the `Acl` trait**

In `src/control-plane/core/src/acl.rs`:

```rust
    /// All policies across `subject`'s roles whose target equals `target` (order
    /// unspecified). Unknown subject → empty page. No merging. The `page` request is
    /// accepted but not yet enforced; results are a single full page.
    async fn policies_for(
        &self,
        subject: &SubjectId,
        target: &PolicyTarget,
        page: PageReq,
    ) -> Result<Page<Policy>>;
```

- [ ] **Step 2: Edit both adapters**

In `src/control-plane/memory/src/acl.rs` and `src/control-plane/postgres/src/acl.rs`: add `Page, PageReq` imports, change the `policies_for` signature (`_page: PageReq`), wrap the returned vec in `Page::from_full(...)`. No SQL change (the `row_filter` jsonb decode path is untouched).

- [ ] **Step 3: Update the acl conformance call sites in testkit**

In `src/control-plane/testkit/src/lib.rs`, every `.policies_for(&sid(...), &t...(...))` call (~lines 768, 785, 804, 812, 819, 830): add `PageReq::unbounded()` as the final argument. Where the result is compared to a `Vec`/indexed, append `.items`; `.len()`/`.is_empty()`/iteration work directly on `Page`.

- [ ] **Step 4: Build and test acl**

Run:
```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:acl //src/control-plane/postgres:acl
```
Expected: PASS, counts unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/core/src/acl.rs src/control-plane/memory/src/acl.rs \
        src/control-plane/postgres/src/acl.rs src/control-plane/testkit/src/lib.rs
git commit -m "feat(control-plane): paginate Acl::policies_for"
```

---

## Task 5: Paginate the `Lineage` reads

**Files:**
- Modify: `src/control-plane/core/src/lineage.rs:65,68,71`
- Modify: `src/control-plane/memory/src/lineage.rs`
- Modify: `src/control-plane/postgres/src/lineage.rs`
- Modify: `src/control-plane/testkit/src/lib.rs` (lineage conformance fns)

- [ ] **Step 1: Edit the `Lineage` trait**

In `src/control-plane/core/src/lineage.rs`:

```rust
    /// All events for a run, in emit order. Empty page if the run is unknown. The
    /// `page` request is accepted but not yet enforced; results are a single full page.
    async fn events_for(&self, run: &RunId, page: PageReq) -> Result<Page<LineageEvent>>;

    /// One hop upstream of `dataset` (order unspecified). Empty page if `dataset` is
    /// unknown. The `page` request is accepted but not yet enforced; results are a
    /// single full page.
    async fn upstream(&self, dataset: &DatasetRef, page: PageReq) -> Result<Page<DatasetRef>>;

    /// One hop downstream of `dataset` (order unspecified). Empty page if `dataset`
    /// is unknown. The `page` request is accepted but not yet enforced; results are a
    /// single full page.
    async fn downstream(&self, dataset: &DatasetRef, page: PageReq) -> Result<Page<DatasetRef>>;
```

(Preserve the existing one-hop / emit-order doc text; only append the page sentence.)

- [ ] **Step 2: Edit both adapters**

In `src/control-plane/memory/src/lineage.rs` and `src/control-plane/postgres/src/lineage.rs`: add `Page, PageReq` imports, change `events_for`/`upstream`/`downstream` signatures (`_page: PageReq`), wrap the returned vec in `Page::from_full(...)`. The postgres `events_for` body keeps building `out` and changes only its final `Ok(out)` → `Ok(Page::from_full(out))`. No SQL change.

- [ ] **Step 3: Update the lineage conformance call sites in testkit**

In `src/control-plane/testkit/src/lib.rs`:
- The dataset-set closure at ~line 886, `let set = |v: Vec<DatasetRef>| v.into_iter().collect::<HashSet<_>>();`, change to accept a page:
  ```rust
  let set = |p: Page<DatasetRef>| p.into_iter().collect::<HashSet<_>>();
  ```
- Every `cp.upstream(&ds(...))` / `cp.downstream(&ds(...))` call (~916, 935, 942, and the `set(...)` wrappers): add `PageReq::unbounded()` as the final arg. `set(cp.upstream(&ds(...), PageReq::unbounded()).await.unwrap())` then type-checks (closure now takes `Page`).
- Every `cp.events_for(&run)` call (~900, 907, 968, 1008, 1041, 1090, 1104, 1129): add `PageReq::unbounded()` as the final arg.
  - `.is_empty()` / `.len()` work directly on `Page`.
  - The two `assert_eq!(got, vec![...])` comparisons (~900 `got` and ~968) compare a `Page` to a `Vec` literal → append `.items` to the page value: `assert_eq!(got.items, vec![event.clone()], ...)` and `assert_eq!(cp.events_for(&run2, PageReq::unbounded()).await.unwrap().items, vec![start, complete], ...)`.

- [ ] **Step 4: Build and test lineage + full sweep**

Run:
```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:lineage //src/control-plane/postgres:lineage
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...
```
Expected: PASS everywhere (the `tx` conformance and worker tests also link testkit; confirm they still pass). Counts unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/core/src/lineage.rs src/control-plane/memory/src/lineage.rs \
        src/control-plane/postgres/src/lineage.rs src/control-plane/testkit/src/lib.rs
git commit -m "feat(control-plane): paginate Lineage::{events_for,upstream,downstream}"
```

---

## Task 6: proptest serde round-trips for `RowFilter`/`ScalarValue` (core)

**Files:**
- Modify: `src/control-plane/core/Cargo.toml`
- Modify: `Cargo.lock` (regenerated), `third-party/BUCK` (regenerated)
- Create: `src/control-plane/core/tests/serde_roundtrip.rs`
- Modify: `src/control-plane/core/BUCK`

- [ ] **Step 1: Add proptest to `core/Cargo.toml`**

In `src/control-plane/core/Cargo.toml`, under `[dependencies]` (NOT dev-dependencies — reindeer skips those; mirrors worker's `tracing-test`), add:

```toml
# Test-only (used under #[cfg(test)] / tests/), declared as a normal dep so reindeer
# emits the //third-party:proptest target in non-vendored mode.
proptest = "1"
```

- [ ] **Step 2: Regenerate lockfile + third-party rules**

Run:
```bash
cargo generate-lockfile
./tools/buckify.sh
```
Expected: `third-party/BUCK` gains `proptest` (and its transitive crates). `git status` shows `Cargo.lock` + `third-party/BUCK` changed.

- [ ] **Step 3: Write `tests/serde_roundtrip.rs`**

Create `src/control-plane/core/tests/serde_roundtrip.rs`:

```rust
//! Property-based JSON round-trips for the ACL filter types. Complements the
//! hand-built example in `acl.rs`'s inline tests with deep nesting, empty vecs, and
//! unicode property names.

use control_plane_core::{CompareOp, RowFilter, ScalarValue};
use proptest::prelude::*;

fn compare_op() -> impl Strategy<Value = CompareOp> {
    prop_oneof![
        Just(CompareOp::Eq),
        Just(CompareOp::Ne),
        Just(CompareOp::Lt),
        Just(CompareOp::Le),
        Just(CompareOp::Gt),
        Just(CompareOp::Ge),
        Just(CompareOp::In),
        Just(CompareOp::NotIn),
        Just(CompareOp::IsNull),
        Just(CompareOp::IsNotNull),
    ]
}

fn scalar_value() -> impl Strategy<Value = ScalarValue> {
    let leaf = prop_oneof![
        any::<String>().prop_map(ScalarValue::Text),
        any::<i64>().prop_map(ScalarValue::Int),
        any::<bool>().prop_map(ScalarValue::Bool),
    ];
    // Bounded recursion: lists up to depth 3, up to 5 elements (incl. empty).
    leaf.prop_recursive(3, 16, 5, |inner| {
        prop::collection::vec(inner, 0..5).prop_map(ScalarValue::List)
    })
}

fn row_filter() -> impl Strategy<Value = RowFilter> {
    let leaf = (".*", compare_op(), scalar_value())
        .prop_map(|(property, op, value)| RowFilter::Compare { property, op, value });
    // Bounded recursion: And/Or/Not trees up to depth 4, up to ~32 nodes.
    leaf.prop_recursive(4, 32, 5, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..5).prop_map(RowFilter::And),
            prop::collection::vec(inner.clone(), 0..5).prop_map(RowFilter::Or),
            inner.prop_map(|f| RowFilter::Not(Box::new(f))),
        ]
    })
}

proptest! {
    #[test]
    fn row_filter_json_round_trips(f in row_filter()) {
        let json = serde_json::to_string(&f).unwrap();
        let back: RowFilter = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(f, back);
    }

    #[test]
    fn scalar_value_json_round_trips(v in scalar_value()) {
        let json = serde_json::to_string(&v).unwrap();
        let back: ScalarValue = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(v, back);
    }
}
```

(The `".*"` regex strategy generates arbitrary unicode strings for `property`.)

- [ ] **Step 4: Add the `rust_test` target to `core/BUCK`**

Append to `src/control-plane/core/BUCK`:

```python
rust_test(
    name = "serde-roundtrip",
    crate = "serde_roundtrip",
    srcs = ["tests/serde_roundtrip.rs"],
    crate_root = "tests/serde_roundtrip.rs",
    edition = "2024",
    deps = [
        ":core",
        "//third-party:proptest",
        "//third-party:serde_json",
    ],
)
```

- [ ] **Step 5: Test**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/core:serde-roundtrip`
Expected: PASS (2 proptest cases, each runs 256 generated inputs by default).

- [ ] **Step 6: Verify reindeer is in sync**

Run: `buck2 run //tools:prek -- run reindeer-check --all-files`
Expected: PASS (Cargo manifests and `third-party/BUCK` agree).

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/core/Cargo.toml Cargo.lock third-party/BUCK \
        src/control-plane/core/tests/serde_roundtrip.rs src/control-plane/core/BUCK
git commit -m "test(core): proptest JSON round-trips for RowFilter/ScalarValue"
```

---

## Task 7: proptest lineage envelope round-trip (postgres)

This exercises the full DB envelope (`emit` → `events_for`) over arbitrary events, and covers the `event_type` codec through the public path by spanning every `EventType`. **Round-trip hazards to respect** (encoded in the strategy below):
- **Timestamps:** Postgres `timestamptz` is microsecond-precision; `OffsetDateTime` is nanosecond. Generate **second-granularity** times via `OffsetDateTime::from_unix_timestamp` so the round-trip is exact.
- **Payload:** use a JSON value strategy with **i64-only numbers** (no floats — `jsonb` normalizes float representation and `f64` equality is fragile). Object key order is irrelevant (`serde_json::Value` map equality is order-independent), so `jsonb` reordering is safe.

**Files:**
- Modify: `src/control-plane/postgres/Cargo.toml` (add proptest; fix stale sqlx comment)
- Modify: `Cargo.lock`, `third-party/BUCK` (regenerated)
- Create: `src/control-plane/postgres/tests/lineage_roundtrip.rs`
- Modify: `src/control-plane/postgres/BUCK`

- [ ] **Step 1: Add proptest to `postgres/Cargo.toml` and fix the stale comment**

In `src/control-plane/postgres/Cargo.toml`, replace the stale sqlx comment (lines ~8–10, "No TLS … no compile-time query macros yet … arrive in Phase 1") with an accurate one, and add proptest under `[dependencies]`:

```toml
# Local unix-socket connections only (no TLS). Compile-time query!/query_scalar!
# macros validate SQL against the committed .sqlx offline cache (see CLAUDE.md).
sqlx = { version = "0.9", default-features = false, features = ["runtime-tokio", "postgres", "uuid", "time", "json", "migrate", "macros"] }
```
```toml
# Test-only (tests/lineage_roundtrip.rs), declared as a normal dep so reindeer emits
# //third-party:proptest in non-vendored mode (same pattern as the worker crate).
proptest = "1"
```

- [ ] **Step 2: Regenerate**

Run:
```bash
cargo generate-lockfile
./tools/buckify.sh
```
Expected: no new third-party entries beyond what Task 6 already added (`proptest` is already present); `Cargo.lock` updates for the postgres crate's dep edge.

- [ ] **Step 3: Write `tests/lineage_roundtrip.rs`**

Create `src/control-plane/postgres/tests/lineage_roundtrip.rs`. proptest's `proptest!` macro is synchronous, so generate inside a `TestRunner` and drive the async DB calls on a manually-built Tokio runtime (one `PgFixture`/db reused across cases; a fresh `RunId` per case so `events_for` returns exactly the emitted event):

```rust
//! Property-based round-trip for the lineage envelope through the real Postgres
//! adapter: an arbitrary `LineageEvent` (arbitrary payload/inputs/outputs/unicode,
//! every EventType) emitted and read back via `events_for` must compare equal.

use control_plane_core::{DatasetRef, EventType, Lineage, LineageEvent, PageReq, RunId};
use control_plane_postgres::fixture::PgFixture;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

fn event_type() -> impl Strategy<Value = EventType> {
    prop_oneof![
        Just(EventType::Start),
        Just(EventType::Running),
        Just(EventType::Complete),
        Just(EventType::Abort),
        Just(EventType::Fail),
    ]
}

fn dataset_ref() -> impl Strategy<Value = DatasetRef> {
    (".*", ".*").prop_map(|(namespace, name)| DatasetRef { namespace, name })
}

/// JSON value with i64-only numbers (floats omitted: jsonb normalizes them and f64
/// equality is fragile). Bounded depth so cases stay small.
fn json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|n| Value::Number(n.into())),
        ".*".prop_map(Value::String),
    ];
    leaf.prop_recursive(3, 12, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::hash_map(".*", inner, 0..4)
                .prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
    })
}

fn lineage_event() -> impl Strategy<Value = LineageEvent> {
    (
        event_type(),
        // second-granularity time → exact round-trip through timestamptz
        (0i64..4_000_000_000),
        prop::collection::vec(dataset_ref(), 0..4),
        prop::collection::vec(dataset_ref(), 0..4),
        json_value(),
    )
        .prop_map(|(event_type, secs, inputs, outputs, payload)| LineageEvent {
            run_id: RunId(Uuid::nil()), // overwritten per-case below
            event_type,
            event_time: OffsetDateTime::from_unix_timestamp(secs).unwrap(),
            inputs,
            outputs,
            payload,
        })
}

#[test]
fn lineage_envelope_round_trips() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let fx = PgFixture::start();
    let (cp, _db) = rt.block_on(fx.fresh_db());

    // Bounded case count: each case is one DB round-trip.
    let mut runner = TestRunner::new(Config { cases: 16, ..Config::default() });
    runner
        .run(&lineage_event(), |mut event| {
            // Unique run per case so events_for returns exactly this event.
            event.run_id = RunId(Uuid::new_v4());
            rt.block_on(async {
                cp.emit(event.clone()).await.expect("emit");
                let got = cp
                    .events_for(&event.run_id, PageReq::unbounded())
                    .await
                    .expect("events_for");
                prop_assert_eq!(got.items, vec![event.clone()]);
                Ok(())
            })
        })
        .expect("envelope round-trip property holds");
}
```

Notes for the implementer:
- `PgFixture::start()` is sync; `fresh_db()` is async and returns `(PgControlPlane, String)`. Confirm the exact tuple/types in `src/control-plane/postgres/src/fixture.rs` and adjust the binding (`cp` must implement `Lineage`).
- If `events_for` returns inputs/outputs in a different order than emitted, this test will fail — confirm the adapter's `event_datasets` query orders deterministically (it must, since the existing conformance test asserts envelope equality). If ordering is by insertion and your generated vecs are compared by order, that's correct.

- [ ] **Step 4: Add the `rust_test` target to `postgres/BUCK`**

Append to `src/control-plane/postgres/BUCK`. Mirror the env of the existing `lineage`/`sqlx-cache-check` targets (they boot the hermetic Postgres via `PgFixture`):

```python
rust_test(
    name = "lineage-roundtrip",
    crate = "lineage_roundtrip",
    srcs = ["tests/lineage_roundtrip.rs"],
    crate_root = "tests/lineage_roundtrip.rs",
    edition = "2024",
    env = {
        "POSTGRES_BIN_DIR": "$(location :postgres-bin)/bin",
        "POSTGRES_LD_LIBRARY_PATH": "$(location :postgres-bin)/lib:$(location :libxml2)",
        "LOOM_MIGRATIONS_DIR": "$(location :migrations)/migrations",
    },
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:proptest",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```
Confirm the exact `env` keys against an existing `PgFixture`-using target in the same `BUCK` (copy whatever `:queue`/`:lineage` use — the fixture reads those env vars to locate the postgres binary and migrations).

- [ ] **Step 5: Test**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:lineage-roundtrip`
Expected: PASS (the property holds over 16 generated events).

- [ ] **Step 6: Verify reindeer in sync**

Run: `buck2 run //tools:prek -- run reindeer-check --all-files`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/postgres/Cargo.toml Cargo.lock third-party/BUCK \
        src/control-plane/postgres/tests/lineage_roundtrip.rs src/control-plane/postgres/BUCK
git commit -m "test(postgres): proptest lineage envelope round-trip; fix stale sqlx comment"
```

---

## Task 8: Document the unused error variants

**Files:**
- Modify: `src/control-plane/core/src/error.rs:15-18,25-40`

- [ ] **Step 1: Write the failing test**

In `src/control-plane/core/src/error.rs`, add a `Conflict` display assertion to the existing `variants_display_and_are_send_sync` test (after the `Unauthorized` assertion):

```rust
        assert_eq!(
            ControlPlaneError::Conflict("dup key".into()).to_string(),
            "conflict: dup key"
        );
```

- [ ] **Step 2: Add doc comments to the variants**

Change the `Conflict`/`Unauthorized` variant declarations to:

```rust
    /// A write lost an optimistic-concurrency / uniqueness race. No producer yet —
    /// today's writes are idempotent upserts; reserved for future non-idempotent
    /// writes (e.g. optimistic snapshot commit).
    #[error("conflict: {0}")]
    Conflict(String),
    /// The caller is not authorized. Reserved for the Step-3 service auth layer; the
    /// control plane itself never authenticates a caller (ACL `check` returns a
    /// `Decision`, not an error).
    #[error("unauthorized")]
    Unauthorized,
```

- [ ] **Step 3: Note the inline test does not run under buck2**

The `error.rs` inline `#[cfg(test)]` test runs only under `cargo test` (no buck2 unittest target — see Background). Verify it under cargo:

Run: `cargo test -p control-plane-core --lib error`
Expected: PASS (`variants_display_and_are_send_sync`).

(Use the hermetic cargo from the dev shell: `eval "$(./tools/env.sh)"` if `cargo` is not on PATH.)

- [ ] **Step 4: Confirm the crate still builds under buck2**

Run: `buck2 build //src/control-plane/core:core` and `tools/clippy-all.sh`
Expected: clean (doc comments + an added test line don't change the public API or trip clippy).

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/core/src/error.rs
git commit -m "docs(core): document Conflict/Unauthorized intended producers; add Conflict display test"
```

---

## Task 9: Final verification

**Files:** none (verification only).

- [ ] **Step 1: Full local test sweep**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...`
Expected: all PASS, including the new `core:page`, `core:serde-roundtrip`, `postgres:lineage-roundtrip` targets and every existing conformance/worker target. Counts for migrated targets unchanged; +3 new targets.

- [ ] **Step 2: Lint**

Run:
```bash
tools/clippy-all.sh
buck2 run //tools:prek -- run --all-files
```
Expected: both clean (rustfmt, clippy, file checks, reindeer-check all green). If rustfmt rewrites anything, `git add` it and amend the relevant commit.

- [ ] **Step 3: Confirm the convention is complete**

Run: `rg 'async fn (snapshots|files|list_types|links|policies_for|events_for|upstream|downstream)' src/control-plane/core/src`
Expected: all 8 now show `page: PageReq` and `-> Result<Page<…>>`; none return bare `Vec`.

- [ ] **Step 4: Confirm no `.sqlx` regeneration was needed**

Run: `git status --porcelain src/control-plane/postgres/.sqlx`
Expected: empty (no SQL changed, so the offline cache is untouched).

---

## Self-review notes

- **Spec coverage:** §1 pagination → Tasks 1–5 (types + all 8 methods + both adapters + testkit); §2 proptest → Task 6 (core serde) + Task 7 (envelope, with the documented private-codec deviation); §3 error docs → Task 8; the minor stale-comment cleanup → Task 7 step 1. Final verification → Task 9.
- **Type consistency:** `Page<T>`/`PageReq`/`Cursor` names and `PageReq::{unbounded,limit,after}` / `Page::{from_full,len,is_empty}` / `IntoIterator` are used identically across Tasks 1–7.
- **Deviation flagged:** the dedicated private-codec proptest from the spec is replaced by public-path coverage (Task 7 spans all `EventType`s); documented in Background and Task 7. This is the one place the plan departs from the approved spec — surface it at the execution gate.
