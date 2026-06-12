# Object-Safe `ControlPlane` Facade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add five accessor methods to the `ControlPlane` trait so `&dyn ControlPlane` / `Arc<dyn ControlPlane>` reaches every concern, then adopt the facade at `query-api::AppState`.

**Architecture:** `ControlPlane` gains `catalog()/ontology()/acl()/lineage()/queue()` returning borrowed `&(dyn Concern + Send + Sync)`. Both adapters already impl all five concerns on `self`, so each accessor is a one-line self-coercion. A new testkit contract exercises every concern through `&dyn ControlPlane` on both adapters. `AppState` swaps its two erased concern Arcs for one facade Arc.

**Tech Stack:** Rust (edition 2024), `async_trait`, buck2 (`rust_test` + `loom_fixture_test`), axum (query-api), `control-plane-{core,memory,postgres,testkit}`.

**Spec:** `docs/superpowers/specs/2026-06-12-control-plane-facade-design.md`

**Conventions (project-wide, do not violate):**
- Tests are `rust_test`/`loom_fixture_test` integration targets in `tests/<name>.rs` — NEVER inline `#[cfg(test)]` (a prek hook enforces this).
- Run the suite with plain `buck2 test //src/...`. Do NOT pipe `buck2 test` through `tail` — redirect to a file and grep: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- rustfmt is CHECK-ONLY in hooks: run `buck2 run //tools:rustfmt -- <changed .rs files>` and apply before committing any `.rs`.
- NEVER `--no-verify`. Conventional Commits, ending with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Do NOT switch branches. Confirm `git branch --show-current` is `feat/control-plane-facade` before and after each task.

---

## File Structure

**Task 1 — the facade capability (one green commit):**
- Modify `src/control-plane/core/src/transaction.rs` — add 5 accessor methods to the `ControlPlane` trait.
- Modify `src/control-plane/memory/src/lib.rs` — impl the 5 accessors on `MemoryControlPlane`.
- Modify `src/control-plane/postgres/src/lib.rs` — impl the 5 accessors on `PgControlPlane`.
- Modify `src/control-plane/testkit/src/lib.rs` — add `control_plane_facade_contract`.
- Create `src/control-plane/memory/tests/facade.rs` + add `facade` `rust_test` target to `src/control-plane/memory/BUCK`.
- Create `src/control-plane/postgres/tests/facade.rs` + add `facade` `loom_fixture_test` target to `src/control-plane/postgres/BUCK`.

**Task 2 — adoption at the one type-erased holder (one green commit):**
- Modify `src/services/query-api/src/http.rs` — `AppState` holds `Arc<dyn ControlPlane>`; `get_object` builds `QueryDeps` from `cp.ontology()`/`cp.acl()`.
- Modify `src/services/query-api/tests/http_smoke.rs` — replace `StubOntology`+`StubAcl` with a seeded `MemoryControlPlane`.
- Modify `src/services/query-api/BUCK` — add the memory adapter to the `http-smoke` test deps.

**Task 3 — close the review finding (docs commit):**
- Modify `docs/superpowers/specs/2026-06-06-control-plane-critical-review.md` — flip the `[M]` facade callout and the summaries from "open/sidestepped" to "done".

---

## Task 1: The facade — trait accessors, adapter impls, testkit contract

**Files:**
- Modify: `src/control-plane/testkit/src/lib.rs`
- Create: `src/control-plane/memory/tests/facade.rs`
- Modify: `src/control-plane/memory/BUCK`
- Modify: `src/control-plane/core/src/transaction.rs`
- Modify: `src/control-plane/memory/src/lib.rs`
- Modify: `src/control-plane/postgres/src/lib.rs`
- Create: `src/control-plane/postgres/tests/facade.rs`
- Modify: `src/control-plane/postgres/BUCK`

We write the contract + memory target first (it fails to compile — the accessors don't exist yet), then add the trait methods and both impls to make it pass, then wire postgres.

- [ ] **Step 1: Write the facade contract in testkit**

Append this function to `src/control-plane/testkit/src/lib.rs` (the `job()` helper and all imports it uses — `ControlPlane`, `Queue`, `Ontology`, `Acl`, `Lineage`, `Catalog`, `Action`, `PolicyTarget`, `SubjectId`, `TypeName`, `RunId`, `Decision`, `PageReq` — are already imported at the top of the file; `uuid` is already a dep):

```rust
/// Contract for the object-safe `ControlPlane` facade: every concern is reachable
/// through a `&dyn ControlPlane` accessor and dispatches to the live adapter impl.
/// `cp` must be freshly empty.
pub async fn control_plane_facade_contract<CP: ControlPlane>(cp: &CP) {
    // Erase to the trait object: everything below goes through the facade, not the
    // concrete adapter — that is the whole point of the accessors.
    let cp: &dyn ControlPlane = cp;

    // queue: a job enqueued through the facade is dequeued through the facade.
    let id = cp
        .queue()
        .enqueue(job("facade"))
        .await
        .expect("enqueue via facade");
    let j = cp
        .queue()
        .dequeue(&["facade".to_string()], "facade-worker")
        .await
        .expect("dequeue via facade")
        .expect("the enqueued job");
    assert_eq!(j.id, id, "facade queue() dispatches to the live queue");

    // ontology: empty on a fresh control plane, reached through the facade.
    let types = cp
        .ontology()
        .list_types(PageReq::unbounded())
        .await
        .expect("list_types via facade");
    assert!(
        types.is_empty(),
        "fresh ontology() is empty through the facade"
    );

    // acl: deny-by-default for an unknown subject, reached through the facade.
    let decision = cp
        .acl()
        .check(
            &SubjectId("nobody".into()),
            Action::Read,
            &PolicyTarget::Type(TypeName("Whatever".into())),
        )
        .await
        .expect("check via facade");
    assert_eq!(decision, Decision::Deny, "facade acl() denies by default");

    // lineage: no events for an unknown run, reached through the facade.
    let events = cp
        .lineage()
        .events_for(&RunId(uuid::Uuid::new_v4()), PageReq::unbounded())
        .await
        .expect("events_for via facade");
    assert!(
        events.is_empty(),
        "fresh lineage() has no events through the facade"
    );

    // catalog: the accessor is object-safe and returns a live trait object. Behavioral
    // catalog reads hit ducklake_* tables that need an attached catalog (covered by
    // catalog_contract); binding the ref here keeps this contract DuckLake-free so the
    // postgres facade test runs postgres-only.
    let _catalog: &(dyn Catalog + Send + Sync) = cp.catalog();
}
```

- [ ] **Step 2: Create the memory facade test + BUCK target**

Create `src/control-plane/memory/tests/facade.rs`:

```rust
use std::time::Duration;

use control_plane_memory::MemoryControlPlane;

const LOCK_TIMEOUT: Duration = Duration::from_millis(300);

#[tokio::test]
async fn memory_passes_control_plane_facade_contract() {
    let cp = MemoryControlPlane::new(LOCK_TIMEOUT);
    control_plane_testkit::control_plane_facade_contract(&cp).await;
}
```

Add this target to `src/control-plane/memory/BUCK` (mirror the existing `ontology` `rust_test` block):

```python
rust_test(
    name = "facade",
    crate = "facade",
    srcs = ["tests/facade.rs"],
    crate_root = "tests/facade.rs",
    edition = "2024",
    deps = [
        ":memory",
        "//src/control-plane/testkit:testkit",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run the memory facade test — verify it FAILS to compile**

Run: `buck2 test //src/control-plane/memory:facade > /tmp/t.log 2>&1; grep -E "error\[|no method named|Tests finished|FAIL" /tmp/t.log`
Expected: compile failure — `no method named queue`/`ontology`/`acl`/`lineage`/`catalog` found for `&dyn ControlPlane` (the accessors don't exist yet).

- [ ] **Step 4: Add the accessor methods to the `ControlPlane` trait**

In `src/control-plane/core/src/transaction.rs`, add the five concern-trait imports and the five accessor methods. The existing `use` block imports types from `catalog`, `lineage`, `queue`, `snapshot`; add the traits:

```rust
use crate::acl::Acl;
use crate::catalog::{Catalog, SnapshotId, TableRef};
use crate::lineage::{Lineage, LineageEvent};
use crate::ontology::Ontology;
use crate::queue::{JobId, NewJob, Queue};
```

(Keep `use crate::error::Result;` and `use crate::snapshot::{ColumnSpec, DataFile};` as they are. Replace the existing `use crate::catalog::{SnapshotId, TableRef};`, `use crate::lineage::LineageEvent;`, and `use crate::queue::{JobId, NewJob};` lines with the merged forms above.)

Then add the accessors to the trait, above `begin`:

```rust
#[async_trait]
pub trait ControlPlane: Send + Sync {
    /// The DuckLake catalog read surface.
    fn catalog(&self) -> &(dyn Catalog + Send + Sync);
    /// The object/link ontology.
    fn ontology(&self) -> &(dyn Ontology + Send + Sync);
    /// The access-control policy surface.
    fn acl(&self) -> &(dyn Acl + Send + Sync);
    /// The lineage event log.
    fn lineage(&self) -> &(dyn Lineage + Send + Sync);
    /// The job queue.
    fn queue(&self) -> &(dyn Queue + Send + Sync);

    /// Open a unit of work. Issue operations on the returned `Tx`, then `commit`
    /// or `rollback`.
    async fn begin(&self) -> Result<Box<dyn Tx + Send>>;
}
```

(The `Tx` trait below is unchanged.)

- [ ] **Step 5: Implement the accessors on `MemoryControlPlane`**

In `src/control-plane/memory/src/lib.rs`, add the five concern traits to the `control_plane_core` import (it currently imports `ColumnDef, ControlPlane, FileRef, NewJob, Result, SnapshotId, TableRef, Tx`):

```rust
use control_plane_core::{
    Acl, Catalog, ColumnDef, ControlPlane, FileRef, Lineage, NewJob, Ontology, Queue, Result,
    SnapshotId, TableRef, Tx,
};
```

Then add the five accessor bodies inside the existing `impl ControlPlane for MemoryControlPlane` block (alongside `begin`). Each is a self-coercion — `MemoryControlPlane` already impls every concern in its submodules:

```rust
    fn catalog(&self) -> &(dyn Catalog + Send + Sync) {
        self
    }
    fn ontology(&self) -> &(dyn Ontology + Send + Sync) {
        self
    }
    fn acl(&self) -> &(dyn Acl + Send + Sync) {
        self
    }
    fn lineage(&self) -> &(dyn Lineage + Send + Sync) {
        self
    }
    fn queue(&self) -> &(dyn Queue + Send + Sync) {
        self
    }
```

- [ ] **Step 6: Implement the accessors on `PgControlPlane`**

In `src/control-plane/postgres/src/lib.rs`, add the five concern traits to the `control_plane_core` import (it currently imports `Action, Cardinality, ControlPlane, ControlPlaneError, Effect, EventType, PolicyTarget, Result, Tx`):

```rust
use control_plane_core::{
    Acl, Action, Cardinality, Catalog, ControlPlane, ControlPlaneError, Effect, EventType, Lineage,
    Ontology, PolicyTarget, Queue, Result, Tx,
};
```

Then add the same five accessor bodies inside the existing `impl ControlPlane for PgControlPlane` block (alongside `begin`):

```rust
    fn catalog(&self) -> &(dyn Catalog + Send + Sync) {
        self
    }
    fn ontology(&self) -> &(dyn Ontology + Send + Sync) {
        self
    }
    fn acl(&self) -> &(dyn Acl + Send + Sync) {
        self
    }
    fn lineage(&self) -> &(dyn Lineage + Send + Sync) {
        self
    }
    fn queue(&self) -> &(dyn Queue + Send + Sync) {
        self
    }
```

- [ ] **Step 7: Run the memory facade test — verify it PASSES**

Run: `buck2 test //src/control-plane/memory:facade > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.`

If `clippy` later flags the `self` coercion as needing an explicit cast, the bodies are still correct — do not change behavior; rustfmt/clippy fixes only.

- [ ] **Step 8: Create the postgres facade test + BUCK target**

Create `src/control-plane/postgres/tests/facade.rs`:

```rust
use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_control_plane_facade_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::control_plane_facade_contract(&cp).await;
}
```

Add this target to `src/control-plane/postgres/BUCK` (mirror the existing `queue` `loom_fixture_test` block; default `duckdb = False` — the contract needs Postgres + migrations only, no attached DuckLake catalog):

```python
loom_fixture_test(
    name = "facade",
    crate = "facade",
    srcs = ["tests/facade.rs"],
    crate_root = "tests/facade.rs",
    deps = [":postgres", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

- [ ] **Step 9: Run the postgres facade test — verify it PASSES**

Run: `buck2 test //src/control-plane/postgres:facade > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.` (boots a hermetic Postgres; runs locally via `loom_fixture_test`.)

- [ ] **Step 10: Format, lint, full build, commit**

```bash
buck2 run //tools:rustfmt -- src/control-plane/core/src/transaction.rs src/control-plane/memory/src/lib.rs src/control-plane/postgres/src/lib.rs src/control-plane/testkit/src/lib.rs src/control-plane/memory/tests/facade.rs src/control-plane/postgres/tests/facade.rs
tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -3 /tmp/clippy.log
buck2 build //src/... > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAILED" /tmp/b.log
git add -A && git commit -m "$(cat <<'EOF'
feat(core): object-safe ControlPlane facade accessors

ControlPlane exposed only begin(); the five concern traits were impl'd on the
concrete adapters with no accessor, so a type-erased holder couldn't reach
.acl()/.ontology()/etc. Add catalog()/ontology()/acl()/lineage()/queue()
returning borrowed concern trait objects, one-line self-coercions on both
adapters, plus a testkit facade contract run on memory and postgres.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```
Expected: `BUILD SUCCEEDED`, clippy clean, commit created.

---

## Task 2: Adopt the facade at `query-api::AppState`

**Files:**
- Modify: `src/services/query-api/src/http.rs`
- Modify: `src/services/query-api/tests/http_smoke.rs`
- Modify: `src/services/query-api/BUCK`

`AppState` is the canonical type-erased holder. Swap its two erased concern Arcs for one `Arc<dyn ControlPlane>`, building the narrow `QueryDeps` from the accessors. The `http_smoke` test then needs a real `ControlPlane`, so replace its two hand-rolled stubs with a seeded `MemoryControlPlane` (deleting ~130 lines of `unimplemented!()`), keeping `StubServing` so the test stays DuckDB-free.

- [ ] **Step 1: Update `http_smoke.rs` to construct a seeded `MemoryControlPlane` (failing test first)**

Rewrite `src/services/query-api/tests/http_smoke.rs` so it no longer defines `StubOntology`/`StubAcl`, builds a seeded `MemoryControlPlane`, and constructs `AppState` with a `cp` field. Keep `StubServing` and the assertions verbatim. New file contents:

```rust
//! HTTP wiring smoke test: a GET maps into read_object and ObjectRows serialize to a typed-object JSON envelope.
//! No socket is bound (tower oneshot); a seeded in-memory control plane + a canned serving stub exercise the route, not DuckDB.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, Effect, ObjectType, Ontology, PolicyTarget, PropertyDef, RoleId, SubjectId,
    TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use query_api::http::{AppState, router};
use query_api::serving::{Rows, ServingEngine, ServingError, SqlValue};
use tower::ServiceExt;

struct StubServing;

#[async_trait]
impl ServingEngine for StubServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
    ) -> std::result::Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec!["id".into()],
            rows: vec![vec![SqlValue::Int(1)]],
        })
    }
}

/// A control plane with the `Order` type and an analyst granted `Read` on it.
async fn seeded_control_plane() -> MemoryControlPlane {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(ObjectType {
        name: TypeName("Order".into()),
        properties: vec![PropertyDef {
            name: "id".into(),
            ty: "Long".into(),
            required: true,
        }],
        table: TableRef {
            schema: "main".into(),
            name: "orders".into(),
        },
    })
    .await
    .unwrap();
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
    cp
}

#[tokio::test(flavor = "multi_thread")]
async fn get_objects_returns_json_rows() {
    let app = router(AppState {
        cp: Arc::new(seeded_control_plane().await),
        serving: Arc::new(StubServing),
    });
    let res = app
        .oneshot(
            Request::builder()
                .uri("/objects/Order")
                .header("X-Loom-Subject", "analyst")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["objects"][0]["id"], "1"); // Long -> JSON string
}
```

(`Acl`/`Ontology` are imported so the seeding method calls resolve; if clippy reports either as unused after Step 2, drop it from the import in Step 4's lint pass.)

- [ ] **Step 2: Add the memory adapter to the `http-smoke` test deps**

In `src/services/query-api/BUCK`, in the `http-smoke` test block's `deps`, add `"//src/control-plane/memory:memory"` (keep the existing entries; `async-trait` is still needed for `StubServing`).

- [ ] **Step 3: Run `http-smoke` — verify it FAILS to compile**

Run: `buck2 test //src/services/query-api:http-smoke > /tmp/t.log 2>&1; grep -E "error\[|no field|Tests finished|FAIL" /tmp/t.log`
Expected: compile failure — `AppState` has no field `cp` (still `ontology`/`acl`).

- [ ] **Step 4: Update `AppState` and `get_object` to the facade**

In `src/services/query-api/src/http.rs`:

Replace the `control_plane_core` import line (`use control_plane_core::{Acl, Ontology, SubjectId};`) with:

```rust
use control_plane_core::{ControlPlane, SubjectId};
```

Replace the `AppState` struct:

```rust
/// Shared, owned dependencies. Holds the control plane as one object-safe facade
/// (`Arc<dyn ControlPlane>`) and hands its narrow concern objects to the read path.
#[derive(Clone)]
pub struct AppState {
    pub cp: Arc<dyn ControlPlane>,
    pub serving: Arc<dyn ServingEngine>,
}
```

In `get_object`, replace the `QueryDeps` construction:

```rust
    let deps = QueryDeps {
        ontology: st.cp.ontology(),
        acl: st.cp.acl(),
        serving: st.serving.as_ref(),
    };
```

- [ ] **Step 5: Run `http-smoke` — verify it PASSES**

Run: `buck2 test //src/services/query-api:http-smoke > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.`

- [ ] **Step 6: Check for other `AppState` constructors**

Run: `grep -rn "AppState {" src/services/query-api`
Expected: only `http.rs` (the struct/usage) and `http_smoke.rs`. If the query-api binary (`src/services/query-api/src/bin` or a `main`/`router` caller) constructs `AppState`, update it to the `cp` field too. (At time of writing the binary does not wire a live `AppState`; if `grep` shows another site, fix it and note it.)

- [ ] **Step 7: Format, lint, full build + targeted tests, commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/http.rs src/services/query-api/tests/http_smoke.rs
tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -3 /tmp/clippy.log
buck2 build //src/... > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAILED" /tmp/b.log
git add -A && git commit -m "$(cat <<'EOF'
refactor(query-api): hold the ControlPlane facade in AppState

AppState carried two type-erased concern Arcs (ontology + acl) because there was
no single facade to hold. Swap them for one Arc<dyn ControlPlane> and build the
narrow QueryDeps from cp.ontology()/cp.acl() — interface segregation preserved at
the read-path boundary, concrete/multi-Arc dependency gone at the holder. The
http_smoke test trades its two hand-rolled stubs for a seeded MemoryControlPlane.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```
Expected: `BUILD SUCCEEDED`, clippy clean, commit created.

---

## Task 3: Close the review finding in the critical-review doc

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-control-plane-critical-review.md`

Flip the `[M]` facade callout and the two summary mentions from "open/sidestepped" to "done". Pure documentation; no code.

- [ ] **Step 1: Flip the `[M]` finding's Update callout**

Replace the block (currently around lines 170–173):

```markdown
> **Update 2026-06-12 — ❌ Open (sidestepped).** `ControlPlane` still exposes only
> `begin()`. In practice consumers take individual `&dyn Ontology` / `&dyn Acl`
> trait objects (e.g. `query-api::QueryDeps`, `ingest::bind`) rather than a facade —
> workable so far, but no object-safe `ControlPlane` accessor was added.
```

with:

```markdown
> **Update 2026-06-12 — ✅ Done.** `ControlPlane` now exposes object-safe accessors
> — `catalog()`/`ontology()`/`acl()`/`lineage()`/`queue()` returning borrowed concern
> trait objects — so `&dyn ControlPlane` / `Arc<dyn ControlPlane>` reaches every
> concern. `query-api::AppState` holds a single `Arc<dyn ControlPlane>` and hands its
> narrow concern objects to the read path; a testkit facade contract verifies dispatch
> on both adapters. Per-concern function signatures stay narrow (interface segregation).
```

- [ ] **Step 2: Update the "five things next" item #5**

Replace (around lines 304–306):

```markdown
   pretending `dyn ControlPlane` is useful. **[H, medium]** — ✅ **Decided:** flat seam
   kept + catalog write leg added; `dyn ControlPlane` left as-is and sidestepped by
   per-concern trait objects (no facade added).
```

with:

```markdown
   pretending `dyn ControlPlane` is useful. **[H, medium]** — ✅ **Done:** flat seam
   kept + catalog write leg added; and `dyn ControlPlane` is now genuinely useful —
   object-safe `catalog()/ontology()/acl()/lineage()/queue()` accessors landed, adopted
   at `query-api::AppState`.
```

- [ ] **Step 3: Update the closing "Update 2026-06-12" remaining-open list**

In the block around lines 311–316, remove `the \`dyn ControlPlane\` facade,` from the "Remaining open from the whole review" sentence so it reads (adjust surrounding commas/`and` so the list stays grammatical):

```markdown
> round-trips ✅ — all landed. The qualified-identity newtype (#4) since landed too
> (`core::DatasetId`), and the `dyn ControlPlane` facade since landed (object-safe
> concern accessors). Remaining open from the whole review: transitive lineage,
> ontology/retention deletion + Parquet GC, batch ops, lineage fan-in test, and
> multi-tenancy.
```

- [ ] **Step 4: Update the top status-reconciliation banner**

In the banner (around lines 16–18), the final sentence lists `and the \`dyn ControlPlane\` facade` as open feature debt. Replace:

```markdown
> is feature debt: transitive lineage, ontology/retention deletion + Parquet GC, batch
> ops, and the `dyn ControlPlane` facade.
```

with:

```markdown
> is feature debt: transitive lineage, ontology/retention deletion + Parquet GC, and
> batch ops. (The `dyn ControlPlane` facade since landed — object-safe concern
> accessors adopted at `query-api::AppState`.)
```

- [ ] **Step 5: Run the markdown hooks and commit**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -E "Passed|Failed" /tmp/prek.log
git add docs/superpowers/specs/2026-06-06-control-plane-critical-review.md && git commit -m "$(cat <<'EOF'
docs(control-plane): mark the dyn ControlPlane facade [M] as done

The object-safe accessor facade landed (catalog/ontology/acl/lineage/queue),
adopted at query-api::AppState. Flip the [M] callout, five-things #5, and the
summaries from open/sidestepped to done.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```
Expected: hooks pass (markdown EOF/whitespace clean), commit created.

---

## Final Verification (after all tasks)

- [ ] Confirm branch: `git branch --show-current` → `feat/control-plane-facade`.
- [ ] Full suite green (do NOT pipe to `tail`):

```bash
buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: all pass, 0 fail (the two new `facade` targets + reworked `http-smoke` included).

- [ ] `tools/clippy-all.sh` clean; `buck2 run //tools:prek -- run --all-files` green.
- [ ] Then hand off via superpowers:finishing-a-development-branch (PR with `--base main`).
