# Governed Lineage Read Endpoint — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose a dataset's provenance over HTTP through three governed, authenticated, paginated `GET /lineage/...` routes in query-api — a thin wire over the already-matured `Lineage` trait.

**Architecture:** Three new `GET` routes registered in query-api's `router()`, behind the existing `protect` auth gate. Each handler is a thin adapter that parses path/query params, calls one `Lineage` method (`upstream`/`downstream`/`events_for`) on the control plane, and serializes the `Page<T>` result to JSON. Pure DTOs + parameter/serialization helpers live in a new `lineage_read.rs` module (unit-testable without a router or Postgres). The lineage reads run through the **direct Postgres control plane** (lineage is not carried over the engine wire), so `WireControlPlane::lineage()` is changed from a panic to a delegation to its `direct` plane — mirroring how `queue()` already delegates.

**Tech Stack:** Rust 2024, axum, utoipa (OpenAPI), `control_plane_core` (`Lineage`, `DatasetRef`, `RunId`, `Page`, `PageReq`, `Cursor`), buck2 (`rust_test` / `loom_fixture_test`), sqlx/Postgres fixture harness.

## Global Constraints

- **Tests are `rust_test` / `loom_fixture_test` integration targets only** — never inline `#[cfg(test)]`/`#[test]` in `src/**.rs` (the `no-inline-tests` prek hook fails otherwise). Each new test file is wired as its own target in `src/services/query-api/BUCK`.
- **Fixture (Postgres-backed) tests use `loom_fixture_test`**, not bare `rust_test`, or they route to remote execution and fail as root.
- **`rust_test` targets are loaded via the `loom_rust_test` wrapper** already imported at the top of `src/services/query-api/BUCK` as `rust_test` — use `rust_test(...)` / `loom_fixture_test(...)` exactly as the existing targets do.
- **Clippy is strict** (`pedantic` + `restriction` on production code): no `unwrap()`/`expect()`/`panic!`/`todo!`/`indexing_slicing` in `src/**`. Use `?`, `match`, `.map_err(...)`, `.unwrap_or_default()` (allowed — it is not `unwrap`). Local suppressions use `#[expect(lint, reason = "...")]`. Test files get the panic-safety exemption automatically via the wrapper.
- **Cloud disk cap (~38 GiB):** build with `buck2 build -M none //src/services/query-api:query-api` and **scope** tests to the specific targets below — never a bare `buck2 build/test //src/...`. Don't pipe `buck2 test` through `tail`/`head`; redirect to a file and grep it.
- **query-api is a zero-DataFusion wire client**; this slice adds no DataFusion/engine code — it only reads the `Lineage` control-plane trait and serializes JSON.
- **Markdown lint:** any `.md` you touch must end with exactly one trailing newline and no trailing whitespace.

---

## File Structure

- `src/services/query-api/src/wire_control_plane.rs` — **modify**: `lineage()` delegates to `self.direct.lineage()` (was a panic); update module doc.
- `src/services/query-api/src/lineage_read.rs` — **create**: pure DTOs (`DatasetNode`, `DatasetClosureResponse`, `LineageEventView`, `RunEventsResponse`) + helpers (`parse_lineage_page`, `dataset_closure_body`, `run_events_body`, `lineage_event_view`, `event_type_str`).
- `src/services/query-api/src/lib.rs` — **modify**: add `pub mod lineage_read;`.
- `src/services/query-api/src/http.rs` — **modify**: three routes in `router()`, three `#[utoipa::path]` handlers, a shared closure helper + `lineage_error` mapping.
- `src/services/query-api/src/openapi.rs` — **modify**: register the three handlers in `paths(...)` and the four DTOs in `components(schemas(...))`.
- `src/services/query-api/tests/openapi.rs` — **modify**: add the three routes to the `expected()` drift-guard set.
- `src/services/query-api/tests/e2e_support.rs` — **modify**: add a `NoServing` `ServingEngine` stub + a `get_unauth` helper.
- `src/services/query-api/tests/lineage_read.rs` — **create**: pure unit test (`rust_test`) for the helpers/DTOs.
- `src/services/query-api/tests/lineage_http_e2e.rs` — **create**: `loom_fixture_test` covering the six spec scenarios over HTTP.
- `src/services/query-api/tests/wire_lineage_e2e.rs` — **create**: `loom_fixture_test` proving `WireControlPlane::lineage()` delegates on the production plane.
- `src/services/query-api/BUCK` — **modify**: add three test targets (`lineage-read`, `lineage-http-e2e`, `wire-lineage-e2e`).

---

## Task 1: `WireControlPlane::lineage()` delegates to the direct plane

**Why:** In production, query-api's `AppState.cp` is a `WireControlPlane` whose `lineage()` currently `panic!`s ("not supported"). In the e2e tests the harness passes a `PgControlPlane` directly as `cp`, so `cp.lineage()` works there — a handler calling `st.cp.lineage()` would pass tests but **panic in production**. Lineage is not carried over the engine wire; it belongs on the direct Postgres plane (exactly like `queue()`, which already delegates to `self.direct.queue()`). This task closes that trap and guards it with a fixture test that exercises the real `WireControlPlane`.

**Files:**
- Modify: `src/services/query-api/src/wire_control_plane.rs:1-6` (module doc) and `:198-204` (`lineage()`).
- Create: `src/services/query-api/tests/wire_lineage_e2e.rs`
- Modify: `src/services/query-api/BUCK` (add `wire-lineage-e2e`)

**Interfaces:**
- Consumes: `WireControlPlane::new(client: GrpcQueueClient, direct: Arc<dyn ControlPlane>)`; `ControlPlane::lineage(&self) -> &(dyn Lineage + Send + Sync)`; `Lineage::{emit, events_for, upstream, downstream}`; e2e_support `spawn_engine`, `connect_gov_client`.
- Produces: `WireControlPlane::lineage()` now returns `self.direct.lineage()` (no panic).

- [ ] **Step 1: Write the failing guard test**

Create `src/services/query-api/tests/wire_lineage_e2e.rs`:

```rust
//! Proves `WireControlPlane::lineage()` delegates to its direct Postgres plane on
//! the real production type (the e2e HTTP tests use a `PgControlPlane` as `cp`, so
//! they never exercise the wire plane — this guards the production wiring).

use std::sync::Arc;

// `ControlPlane` in scope for `cp.lineage()`/`wire.lineage()`; `Lineage` is NOT
// imported (its methods run on the returned `&dyn Lineage`, needing no trait in
// scope — an unused import would fail clippy's `unused_imports` on this test target).
use control_plane_core::{ControlPlane, DatasetRef, EventType, LineageEvent, PageReq, RunId};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{connect_gov_client, spawn_engine};
use query_api::wire_control_plane::WireControlPlane;

fn ds(ns: &str, name: &str) -> DatasetRef {
    DatasetRef {
        namespace: ns.to_string(),
        name: name.to_string(),
    }
}

fn edge(inp: DatasetRef, out: DatasetRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
        inputs: vec![inp],
        outputs: vec![out],
        payload: serde_json::json!({}),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn wire_lineage_reads_delegate_to_direct() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let cp = Arc::new(cp);
    let warehouse = tempfile::tempdir().expect("warehouse");

    // A spawned engine gives us a real gov client for WireControlPlane::new; the
    // warehouse/engine are unused by lineage reads (they go through `direct`).
    let (sock, _guard) = spawn_engine(&fx, &db, warehouse.path(), 0, i64::MAX).await;
    let client = connect_gov_client(&sock).await;
    let wire = WireControlPlane::new(client, cp.clone() as Arc<dyn ControlPlane>);

    // Seed A -> B on the DIRECT plane's lineage.
    let (a, b) = (ds("w", "wire.a"), ds("w", "wire.b"));
    cp.lineage().emit(edge(a.clone(), b.clone())).await.expect("emit");

    // Read it back THROUGH the wire plane — must not panic, must return the edge.
    let events = wire
        .lineage()
        .events_for(&RunId(uuid::Uuid::nil()), PageReq::unbounded())
        .await;
    assert!(events.is_ok(), "events_for delegates without panicking");

    let up = wire
        .lineage()
        .upstream(&b, 1, PageReq::unbounded())
        .await
        .expect("upstream via wire plane");
    let names: Vec<String> = up.items.iter().map(|d| d.name.clone()).collect();
    assert_eq!(names, vec!["wire.a".to_string()], "upstream(B) = {{A}} via delegation");
}
```

- [ ] **Step 2: Add the BUCK target and run it to verify it fails (panic)**

Add to `src/services/query-api/BUCK` (mirror the `wire-governance-e2e` block's deps):

```python
loom_fixture_test(
    name = "wire-lineage-e2e",
    crate = "wire_lineage_e2e",
    srcs = ["tests/wire_lineage_e2e.rs"],
    crate_root = "tests/wire_lineage_e2e.rs",
    deps = [
        ":query-api",
        ":e2e-support",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/services/engine:engine",
        "//src/services/engine-wire:engine-wire",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

Run: `buck2 test //src/services/query-api:wire-lineage-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|panicked" /tmp/t.log`
Expected: FAIL — the read panics with "lineage() is not supported".

- [ ] **Step 3: Change `lineage()` to delegate**

In `src/services/query-api/src/wire_control_plane.rs`, replace the panicking `lineage()` (currently lines ~198–204, including its `#[expect(clippy::panic, ...)]`) with:

```rust
    fn lineage(&self) -> &(dyn Lineage + Send + Sync) {
        // Lineage is not carried over the engine wire; read it from the direct
        // Postgres plane, exactly as `queue()` does. query-api's governed lineage
        // read endpoints resolve provenance here.
        self.direct.lineage()
    }
```

Then update the module doc comment at the top of the file so it no longer claims `lineage()` is guarded. Change the first paragraph (lines 1–6) to read:

```rust
//! A read-only governance `ControlPlane` for query-api: `acl()`/`ontology()` read
//! over the engine wire; `queue()` and `lineage()` delegate to the direct Postgres
//! plane (the GC enqueue and the governed lineage read endpoints); `catalog()`/
//! `begin()` are guarded because query-api never uses them through this plane.
//! Write/define governance methods fail loudly — query-api authorizes reads here
//! and sends pre-authorized writes via the engine's write RPCs; it never defines
//! governance.
```

- [ ] **Step 4: Run the guard test to verify it passes**

Run: `buck2 test //src/services/query-api:wire-lineage-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Clippy-check the changed library target**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5` then confirm the clippy output file is empty.
Expected: no clippy findings (the `#[expect(clippy::panic)]` was removed along with the panic, so no dangling `expect`).

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/wire_control_plane.rs src/services/query-api/tests/wire_lineage_e2e.rs src/services/query-api/BUCK
git commit -m "feat(query-api): WireControlPlane::lineage delegates to the direct plane"
```

---

## Task 2: Pure lineage DTOs + parameter/serialization helpers (`lineage_read.rs`)

**Why:** Keep `http.rs` thin (its own module doc says "All logic is in handler...; this layer only maps HTTP <-> the core"). The DTO shapes, the `after`/`limit` → `PageReq` parse, and the `Page<T>` → JSON serialization are pure and unit-testable without a router or Postgres — put them in their own module with a fast `rust_test`.

**Files:**
- Create: `src/services/query-api/src/lineage_read.rs`
- Modify: `src/services/query-api/src/lib.rs:14-27` (add `pub mod lineage_read;`)
- Create: `src/services/query-api/tests/lineage_read.rs`
- Modify: `src/services/query-api/BUCK` (add `lineage-read`)

**Interfaces:**
- Consumes: `control_plane_core::{Cursor, DatasetRef, EventType, LineageEvent, Page, PageReq}`; `time::format_description::well_known::Rfc3339`.
- Produces (used by Task 3's handlers and Task 4's tests):
  - `pub struct DatasetNode { pub namespace: String, pub name: String }` (`Serialize`, `ToSchema`), `impl From<DatasetRef> for DatasetNode`.
  - `pub struct DatasetClosureResponse { pub datasets: Vec<DatasetNode>, pub next_cursor: Option<String> }` (`Serialize`, `ToSchema`).
  - `pub struct LineageEventView { pub run_id: String, pub event_type: String, pub event_time: String, pub inputs: Vec<DatasetNode>, pub outputs: Vec<DatasetNode>, pub payload: serde_json::Value }` (`Serialize`, `ToSchema`).
  - `pub struct RunEventsResponse { pub events: Vec<LineageEventView>, pub next_cursor: Option<String> }` (`Serialize`, `ToSchema`).
  - `pub fn event_type_str(t: EventType) -> &'static str`
  - `pub fn parse_lineage_page(after: Option<String>, limit: Option<String>) -> Result<PageReq, String>`
  - `pub fn dataset_closure_body(page: Page<DatasetRef>) -> DatasetClosureResponse`
  - `pub fn lineage_event_view(e: LineageEvent) -> LineageEventView`
  - `pub fn run_events_body(page: Page<LineageEvent>) -> RunEventsResponse`

- [ ] **Step 1: Write the failing unit test**

Create `src/services/query-api/tests/lineage_read.rs`:

```rust
//! Pure unit tests for the lineage read DTOs + param/serialization helpers. No
//! router, no Postgres — runs on remote execution.

use control_plane_core::{
    Cursor, DatasetRef, EventType, LineageEvent, Page, PageReq, RunId,
};
use query_api::lineage_read::{
    dataset_closure_body, event_type_str, lineage_event_view, parse_lineage_page, run_events_body,
};

fn ds(ns: &str, name: &str) -> DatasetRef {
    DatasetRef {
        namespace: ns.to_string(),
        name: name.to_string(),
    }
}

#[test]
fn parse_page_forwards_after_and_limit() {
    let p = parse_lineage_page(Some("cur1".to_string()), Some("3".to_string())).unwrap();
    assert_eq!(p, PageReq { after: Some(Cursor("cur1".to_string())), limit: Some(3) });
}

#[test]
fn parse_page_absent_is_unbounded_from_start() {
    let p = parse_lineage_page(None, None).unwrap();
    assert_eq!(p, PageReq { after: None, limit: None });
}

#[test]
fn parse_page_rejects_non_numeric_limit() {
    let err = parse_lineage_page(None, Some("abc".to_string()));
    assert!(err.is_err(), "non-numeric limit is a caller error");
}

#[test]
fn event_type_strings_cover_all_variants() {
    assert_eq!(event_type_str(EventType::Start), "start");
    assert_eq!(event_type_str(EventType::Running), "running");
    assert_eq!(event_type_str(EventType::Complete), "complete");
    assert_eq!(event_type_str(EventType::Abort), "abort");
    assert_eq!(event_type_str(EventType::Fail), "fail");
}

#[test]
fn closure_body_serializes_datasets_and_cursor() {
    let page = Page {
        items: vec![ds("w", "a"), ds("w", "b")],
        next: Some(Cursor("nextcur".to_string())),
    };
    let json = serde_json::to_value(dataset_closure_body(page)).unwrap();
    assert_eq!(json["datasets"][0]["namespace"], "w");
    assert_eq!(json["datasets"][1]["name"], "b");
    assert_eq!(json["next_cursor"], "nextcur");
}

#[test]
fn closure_body_last_page_has_null_cursor() {
    let page = Page { items: vec![ds("w", "a")], next: None };
    let json = serde_json::to_value(dataset_closure_body(page)).unwrap();
    assert!(json["next_cursor"].is_null());
}

#[test]
fn event_view_shapes_time_type_inputs_outputs_payload() {
    let run = RunId(uuid::Uuid::nil());
    let ev = LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
        inputs: vec![ds("w", "in")],
        outputs: vec![ds("w", "out")],
        payload: serde_json::json!({ "k": 1 }),
    };
    let view = lineage_event_view(ev);
    let json = serde_json::to_value(&view).unwrap();
    assert_eq!(json["run_id"], "00000000-0000-0000-0000-000000000000");
    assert_eq!(json["event_type"], "complete");
    assert!(json["event_time"].as_str().unwrap().contains('T'), "rfc3339 time");
    assert_eq!(json["inputs"][0]["name"], "in");
    assert_eq!(json["outputs"][0]["name"], "out");
    assert_eq!(json["payload"]["k"], 1);
}

#[test]
fn run_events_body_wraps_events_and_cursor() {
    let ev = LineageEvent {
        run_id: RunId(uuid::Uuid::nil()),
        event_type: EventType::Running,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
        inputs: vec![],
        outputs: vec![ds("w", "o")],
        payload: serde_json::json!({}),
    };
    let page = Page { items: vec![ev], next: None };
    let json = serde_json::to_value(run_events_body(page)).unwrap();
    assert_eq!(json["events"][0]["event_type"], "running");
    assert!(json["next_cursor"].is_null());
}
```

The test file needs `uuid` + `time` deps — add them in Step 3's BUCK block.

- [ ] **Step 2: Run to verify it fails (module/functions not defined)**

Run: `buck2 test //src/services/query-api:lineage-read > /tmp/t.log 2>&1; grep -E "Tests finished|error\[|cannot find" /tmp/t.log`
Expected: FAIL — `lineage_read` module / target does not exist yet.

- [ ] **Step 3: Create the module, export it, and add the BUCK target**

Create `src/services/query-api/src/lineage_read.rs`:

```rust
//! Pure DTOs + parameter/serialization helpers for the governed lineage read
//! endpoints (the `/lineage` HTTP surface). The thin axum handlers live in
//! `http.rs`; this module holds the pieces that are unit-testable without a router
//! or Postgres: the response shapes, the `after`/`limit` -> `PageReq` parse, and
//! the `Page<T>` -> JSON serialization.

use control_plane_core::{Cursor, DatasetRef, EventType, LineageEvent, Page, PageReq};
use utoipa::ToSchema;

/// One provenance node: an OpenLineage `{namespace, name}` dataset identity.
#[derive(serde::Serialize, ToSchema)]
pub struct DatasetNode {
    pub namespace: String,
    pub name: String,
}

impl From<DatasetRef> for DatasetNode {
    fn from(d: DatasetRef) -> Self {
        Self {
            namespace: d.namespace,
            name: d.name,
        }
    }
}

/// Response for the upstream/downstream closure reads: a flat set of datasets plus
/// the opaque next-page cursor (`null` on the last page). No per-node depth — the
/// capability returns a set, matching its contract.
#[derive(serde::Serialize, ToSchema)]
pub struct DatasetClosureResponse {
    pub datasets: Vec<DatasetNode>,
    pub next_cursor: Option<String>,
}

/// Serialization shape for one lineage event. `event_time` is a pre-formatted
/// RFC3339 string (the `time` crate's serde-well-known feature is not enabled);
/// `payload` is the opaque OpenLineage event carried verbatim.
#[derive(serde::Serialize, ToSchema)]
pub struct LineageEventView {
    pub run_id: String,
    pub event_type: String,
    pub event_time: String,
    pub inputs: Vec<DatasetNode>,
    pub outputs: Vec<DatasetNode>,
    // `serde_json::Value` derives `ToSchema` directly (see openapi.rs) — no
    // `#[schema(value_type = ...)]` override needed.
    pub payload: serde_json::Value,
}

/// Response for the run-events read.
#[derive(serde::Serialize, ToSchema)]
pub struct RunEventsResponse {
    pub events: Vec<LineageEventView>,
    pub next_cursor: Option<String>,
}

/// Stable string tag for an event type (matches the adapter's own encoding).
#[must_use]
pub fn event_type_str(t: EventType) -> &'static str {
    match t {
        EventType::Start => "start",
        EventType::Running => "running",
        EventType::Complete => "complete",
        EventType::Abort => "abort",
        EventType::Fail => "fail",
    }
}

/// Build a `PageReq` from the raw `after` (opaque cursor) + `limit` query params.
/// A non-numeric `limit` is a caller error (`Err(message)` -> 400 at the handler).
/// An absent limit is unbounded (`None`); an absent cursor starts from the beginning.
/// The cursor is opaque — round-tripped verbatim, never decoded here.
pub fn parse_lineage_page(
    after: Option<String>,
    limit: Option<String>,
) -> Result<PageReq, String> {
    let limit = match limit {
        Some(s) => Some(
            s.parse::<u32>()
                // Carry the parse error — a bare `.map_err(|_| ...)` trips the enforced
                // `clippy::map_err_ignore` (restriction group) on production code.
                .map_err(|e| format!("limit must be a non-negative integer: {e}"))?,
        ),
        None => None,
    };
    Ok(PageReq {
        after: after.map(Cursor),
        limit,
    })
}

/// Serialize a dataset-closure page into its response DTO.
#[must_use]
pub fn dataset_closure_body(page: Page<DatasetRef>) -> DatasetClosureResponse {
    let next_cursor = page.next.map(|c| c.0);
    DatasetClosureResponse {
        datasets: page.items.into_iter().map(DatasetNode::from).collect(),
        next_cursor,
    }
}

/// Serialize one lineage event into its view DTO (RFC3339 time, string tags).
#[must_use]
pub fn lineage_event_view(e: LineageEvent) -> LineageEventView {
    let event_time = e
        .event_time
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    LineageEventView {
        run_id: e.run_id.0.to_string(),
        event_type: event_type_str(e.event_type).to_string(),
        event_time,
        inputs: e.inputs.into_iter().map(DatasetNode::from).collect(),
        outputs: e.outputs.into_iter().map(DatasetNode::from).collect(),
        payload: e.payload,
    }
}

/// Serialize a run-events page into its response DTO.
#[must_use]
pub fn run_events_body(page: Page<LineageEvent>) -> RunEventsResponse {
    let next_cursor = page.next.map(|c| c.0);
    RunEventsResponse {
        events: page.items.into_iter().map(lineage_event_view).collect(),
        next_cursor,
    }
}
```

In `src/services/query-api/src/lib.rs`, add the module declaration in alphabetical position (after `pub mod http;`):

```rust
pub mod lineage_read;
```

Add the test target to `src/services/query-api/BUCK`:

```python
rust_test(
    name = "lineage-read",
    crate = "lineage_read",
    srcs = ["tests/lineage_read.rs"],
    crate_root = "tests/lineage_read.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 4: Run the unit test to verify it passes**

Run: `buck2 test //src/services/query-api:lineage-read > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (8 tests).

> Note: `serde_json::Value` derives `ToSchema` directly (per `openapi.rs`), so `LineageEventView.payload` needs no `#[schema(value_type = ...)]` override — omitted above.

- [ ] **Step 5: Clippy-check**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5` and confirm the clippy file is empty.

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/lineage_read.rs src/services/query-api/src/lib.rs src/services/query-api/tests/lineage_read.rs src/services/query-api/BUCK
git commit -m "feat(query-api): lineage read DTOs + param/serialization helpers"
```

---

## Task 3: The three HTTP handlers, routes, and OpenAPI registration

**Why:** Wire the three `GET /lineage/...` routes into the router, each a thin adapter over one `Lineage` method, and keep the OpenAPI drift guard honest.

**Files:**
- Modify: `src/services/query-api/src/http.rs` (imports, `router()`, three handlers + shared helper + error mapping)
- Modify: `src/services/query-api/src/openapi.rs:71-91` (`paths(...)` + `components(schemas(...))`)
- Modify: `src/services/query-api/tests/openapi.rs:9-23` (`expected()`)

**Interfaces:**
- Consumes: `AppState` (`st.cp: Arc<dyn ControlPlane>`), `st.cp.lineage()`; `crate::lineage_read::{parse_lineage_page, dataset_closure_body, run_events_body, DatasetClosureResponse, RunEventsResponse, DatasetNode, LineageEventView}`; `control_plane_core::{ControlPlaneError, DatasetRef, RunId}`; `service_runtime::Subject`; `internal_error` (private in `http.rs`); `uuid::Uuid`.
- Produces: routes `get /lineage/datasets/{namespace}/{name}/upstream`, `.../downstream`, `get /lineage/runs/{run_id}/events`; handler fns `get_lineage_upstream`, `get_lineage_downstream`, `get_lineage_run_events` (referenced by `openapi.rs`).

- [ ] **Step 1: Update the drift-guard test first (it will fail until routes exist)**

In `src/services/query-api/tests/openapi.rs`, add three entries to the `expected()` set array (after the existing entries, before the `.iter()`):

```rust
        ("get", "/lineage/datasets/{namespace}/{name}/upstream"),
        ("get", "/lineage/datasets/{namespace}/{name}/downstream"),
        ("get", "/lineage/runs/{run_id}/events"),
```

- [ ] **Step 2: Run the drift guard to verify it fails**

Run: `buck2 test //src/services/query-api:openapi > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|assertion" /tmp/t.log`
Expected: FAIL — `documents_exactly_the_expected_routes` (documented set is missing the three new routes).

- [ ] **Step 3: Add the handlers, routes, and error mapping in `http.rs`**

In `src/services/query-api/src/http.rs`:

3a. Extend the `control_plane_core` import (line 20) to bring in the extra types:

```rust
use control_plane_core::{
    ControlPlane, ControlPlaneError, DatasetRef, GC_JOB_KIND, NewJob, RunId,
};
```

3b. Register the three routes in `router()` (add after the `/maintenance/...` route, before `.with_state(state)`):

```rust
        .route(
            "/lineage/datasets/:namespace/:name/upstream",
            get(get_lineage_upstream),
        )
        .route(
            "/lineage/datasets/:namespace/:name/downstream",
            get(get_lineage_downstream),
        )
        .route("/lineage/runs/:run_id/events", get(get_lineage_run_events))
```

3c. Append the handlers + helpers at the end of `http.rs`:

```rust
/// Which direction of the provenance closure a request walks.
#[derive(Clone, Copy)]
enum LineageDir {
    Upstream,
    Downstream,
}

/// Map a lineage read error to a status. A `Validation` fault (over-cap/zero depth,
/// malformed cursor) is a caller error (400); anything else is an opaque 500 logged
/// server-side. An unknown dataset is NOT an error — the capability returns an empty
/// page, which serializes as `{ "datasets": [], "next_cursor": null }`.
fn lineage_error(e: ControlPlaneError) -> axum::response::Response {
    match e {
        ControlPlaneError::Validation(m) => (StatusCode::BAD_REQUEST, m).into_response(),
        other => internal_error("lineage read fault", other),
    }
}

/// Shared upstream/downstream handler: parse `depth` (default 1, forwarded to the
/// capability which caps it), `after`/`limit` (-> `PageReq`), then call the closure
/// read and serialize the page. `depth` beyond `LINEAGE_MAX_DEPTH` (or 0) is rejected
/// BELOW by the capability as `Validation` -> 400 — the wire cannot trigger an
/// unbounded walk.
async fn lineage_closure(
    st: &AppState,
    namespace: String,
    name: String,
    params: Vec<(String, String)>,
    dir: LineageDir,
) -> axum::response::Response {
    let mut depth: u32 = 1;
    let mut after: Option<String> = None;
    let mut limit: Option<String> = None;
    for (k, v) in params {
        match k.as_str() {
            "depth" => match v.parse::<u32>() {
                Ok(d) => depth = d,
                Err(_) => {
                    return (StatusCode::BAD_REQUEST, "depth must be a positive integer")
                        .into_response();
                }
            },
            "after" => after = Some(v),
            "limit" => limit = Some(v),
            _ => {} // ignore unknown query params
        }
    }
    let page = match crate::lineage_read::parse_lineage_page(after, limit) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    let ds = DatasetRef { namespace, name };
    let lineage = st.cp.lineage();
    let res = match dir {
        LineageDir::Upstream => lineage.upstream(&ds, depth, page).await,
        LineageDir::Downstream => lineage.downstream(&ds, depth, page).await,
    };
    match res {
        Ok(page) => Json(crate::lineage_read::dataset_closure_body(page)).into_response(),
        Err(e) => lineage_error(e),
    }
}

#[utoipa::path(
    get, path = "/lineage/datasets/{namespace}/{name}/upstream",
    params(
        ("namespace" = String, Path, description = "Dataset namespace"),
        ("name" = String, Path, description = "Dataset name"),
        ("depth" = Option<u32>, Query, description = "Closure depth (default 1, capped)"),
        ("after" = Option<String>, Query, description = "Opaque next-page cursor"),
        ("limit" = Option<u32>, Query, description = "Max datasets per page"),
    ),
    responses(
        (status = 200, description = "Upstream dataset closure", body = crate::lineage_read::DatasetClosureResponse),
        (status = 400, description = "Bad depth/limit/cursor"),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "lineage",
)]
async fn get_lineage_upstream(
    State(st): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<Vec<(String, String)>>,
    _subject: Subject,
) -> axum::response::Response {
    lineage_closure(&st, namespace, name, params, LineageDir::Upstream).await
}

#[utoipa::path(
    get, path = "/lineage/datasets/{namespace}/{name}/downstream",
    params(
        ("namespace" = String, Path, description = "Dataset namespace"),
        ("name" = String, Path, description = "Dataset name"),
        ("depth" = Option<u32>, Query, description = "Closure depth (default 1, capped)"),
        ("after" = Option<String>, Query, description = "Opaque next-page cursor"),
        ("limit" = Option<u32>, Query, description = "Max datasets per page"),
    ),
    responses(
        (status = 200, description = "Downstream dataset closure", body = crate::lineage_read::DatasetClosureResponse),
        (status = 400, description = "Bad depth/limit/cursor"),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "lineage",
)]
async fn get_lineage_downstream(
    State(st): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    Query(params): Query<Vec<(String, String)>>,
    _subject: Subject,
) -> axum::response::Response {
    lineage_closure(&st, namespace, name, params, LineageDir::Downstream).await
}

#[utoipa::path(
    get, path = "/lineage/runs/{run_id}/events",
    params(
        ("run_id" = String, Path, description = "OpenLineage run id (UUID)"),
        ("after" = Option<String>, Query, description = "Opaque next-page cursor"),
        ("limit" = Option<u32>, Query, description = "Max events per page"),
    ),
    responses(
        (status = 200, description = "Events for the run", body = crate::lineage_read::RunEventsResponse),
        (status = 400, description = "Malformed run id or limit"),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "lineage",
)]
async fn get_lineage_run_events(
    State(st): State<AppState>,
    Path(run_id): Path<String>,
    Query(params): Query<Vec<(String, String)>>,
    _subject: Subject,
) -> axum::response::Response {
    let Ok(uuid) = uuid::Uuid::parse_str(&run_id) else {
        return (StatusCode::BAD_REQUEST, "run_id must be a UUID").into_response();
    };
    let mut after: Option<String> = None;
    let mut limit: Option<String> = None;
    for (k, v) in params {
        match k.as_str() {
            "after" => after = Some(v),
            "limit" => limit = Some(v),
            _ => {}
        }
    }
    let page = match crate::lineage_read::parse_lineage_page(after, limit) {
        Ok(p) => p,
        Err(e) => return (StatusCode::BAD_REQUEST, e).into_response(),
    };
    match st.cp.lineage().events_for(&RunId(uuid), page).await {
        Ok(page) => Json(crate::lineage_read::run_events_body(page)).into_response(),
        Err(e) => lineage_error(e),
    }
}
```

- [ ] **Step 4: Register the routes + DTOs in the OpenAPI document**

In `src/services/query-api/src/openapi.rs`, add the three handlers to the `paths(...)` list (after `crate::http::enqueue_gc,`):

```rust
        crate::http::get_lineage_upstream,
        crate::http::get_lineage_downstream,
        crate::http::get_lineage_run_events,
```

And add the four DTOs to `components(schemas(...))` (after `crate::http::VectorSearchRequest,`):

```rust
        crate::lineage_read::DatasetNode,
        crate::lineage_read::DatasetClosureResponse,
        crate::lineage_read::LineageEventView,
        crate::lineage_read::RunEventsResponse,
```

- [ ] **Step 5: Run the OpenAPI drift guard + build**

Run: `buck2 test //src/services/query-api:openapi > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (`documents_exactly_the_expected_routes` + `valid_openapi_document`).

Run: `buck2 build -M none //src/services/query-api:query-api 2>&1 | tail -5`
Expected: clean build.

- [ ] **Step 6: Clippy-check**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5` and confirm the clippy file is empty.

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/http.rs src/services/query-api/src/openapi.rs src/services/query-api/tests/openapi.rs
git commit -m "feat(query-api): GET /lineage upstream/downstream/run-events routes"
```

---

## Task 4: End-to-end fixture test over the HTTP surface

**Why:** Prove the six spec scenarios through the real axum router + auth gate + Postgres lineage adapter.

**Files:**
- Modify: `src/services/query-api/tests/e2e_support.rs` (add `NoServing` stub + `get_unauth` helper)
- Create: `src/services/query-api/tests/lineage_http_e2e.rs`
- Modify: `src/services/query-api/BUCK` (add `lineage-http-e2e`)

**Interfaces:**
- Consumes: `e2e_support::{get, get_unauth, NoServing}`; `PgFixture::{start, fresh_db}`; `cp.lineage().emit(...)`.
- Produces: `NoServing` (a `ServingEngine` that errors on data reads — lineage routes never touch it), `get_unauth(cp, eng, uri) -> StatusCode` (drives the router with no Authorization header).

- [ ] **Step 1: Add the `NoServing` stub + `get_unauth` helper to `e2e_support.rs`**

Append to `src/services/query-api/tests/e2e_support.rs` (the imports it needs — `ServingError`, `SqlValue`, `TableRef`, `DataFusionDialect`, `AppState`, `router`, `protect`, `AuthState`, `Request`, `Body`, `StatusCode`, `ServiceExt`, `ControlPlane` — are already imported at the top of the file):

```rust
/// A `ServingEngine` that has no data backend — every data read errors. The lineage
/// read routes never touch the serving engine, so tests of those routes wire this
/// in to satisfy `AppState` without standing up an Iceberg warehouse.
pub struct NoServing;

#[async_trait]
impl query_api::serving::ServingEngine for NoServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
    ) -> Result<query_api::serving::Rows, ServingError> {
        Err(ServingError::Engine("no serving engine configured".into()))
    }

    async fn vector_search(
        &self,
        _table: &TableRef,
        _index_name: &str,
        _query: &[f32],
        _k: usize,
        _nprobe: Option<u32>,
        _ef_search: Option<u32>,
    ) -> Result<query_api::serving::Rows, ServingError> {
        Err(ServingError::Engine("no serving engine configured".into()))
    }

    fn dialect(&self) -> &'static dyn query_api::sql::SqlDialect {
        &DataFusionDialect
    }
}

/// Drive the HTTP router (behind the auth gate) with NO Authorization header and
/// return just the status — for asserting the 401 on unauthenticated requests.
pub async fn get_unauth(
    cp: Arc<PgControlPlane>,
    eng: Arc<dyn query_api::serving::ServingEngine>,
    uri: &str,
) -> StatusCode {
    let app = protect(
        router(AppState {
            cp: cp.clone() as Arc<dyn ControlPlane>,
            serving: eng,
            action_engine: Arc::new(StubAction),
            default_limit: 1000,
        }),
        AuthState {
            auth: cp.clone(),
            session_ttl: std::time::Duration::from_secs(3600),
        },
    );
    let res = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    res.status()
}
```

- [ ] **Step 2: Write the failing e2e test**

Create `src/services/query-api/tests/lineage_http_e2e.rs`:

```rust
//! End-to-end tests for the governed lineage read endpoints over the real query-api
//! HTTP router + auth gate + Postgres lineage adapter. Seeds a known provenance
//! graph directly via `Lineage::emit` (no serving engine needed).

use std::sync::Arc;

use axum::http::StatusCode;
// `ControlPlane` is in scope for `cp.lineage()` (a trait method on the concrete
// `PgControlPlane`); `Lineage` is NOT imported — its methods are called on the
// `&dyn Lineage` the accessor returns, which needs no trait in scope (an unused
// `Lineage` import would fail the enforced clippy `unused_imports` on test targets).
use control_plane_core::{ControlPlane, DatasetRef, EventType, LineageEvent, RunId};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use e2e_support::{get, get_unauth, NoServing};

fn ds(ns: &str, name: &str) -> DatasetRef {
    DatasetRef {
        namespace: ns.to_string(),
        name: name.to_string(),
    }
}

fn edge(inp: DatasetRef, out: DatasetRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
        inputs: vec![inp],
        outputs: vec![out],
        payload: serde_json::json!({}),
    }
}

/// Percent-encode a string for use as a query-string value (opaque cursors contain
/// JSON metacharacters). Encodes everything outside the unreserved set.
fn pct(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Sorted dataset names from a `{ datasets: [...] }` closure body.
fn names(body: &serde_json::Value) -> Vec<String> {
    let mut v: Vec<String> = body["datasets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["name"].as_str().unwrap().to_string())
        .collect();
    v.sort();
    v
}

async fn fresh(fx: &PgFixture) -> Arc<PgControlPlane> {
    let (cp, _db) = fx.fresh_db().await;
    Arc::new(cp)
}

#[tokio::test(flavor = "multi_thread")]
async fn upstream_downstream_traversal() {
    let fx = PgFixture::start();
    let cp = fresh(&fx).await;
    // A -> B -> C
    cp.lineage().emit(edge(ds("w", "a"), ds("w", "b"))).await.unwrap();
    cp.lineage().emit(edge(ds("w", "b"), ds("w", "c"))).await.unwrap();

    // upstream(C, depth=2) = {A, B}
    let (status, body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/w/c/upstream?depth=2",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(names(&body), vec!["a".to_string(), "b".to_string()], "{body}");

    // downstream(A, depth=2) = {B, C}
    let (status, body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/w/a/downstream?depth=2",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(names(&body), vec!["b".to_string(), "c".to_string()], "{body}");

    // depth=1 (default) = one hop
    let (status, body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/w/c/upstream",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(names(&body), vec!["b".to_string()], "default depth 1: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn depth_over_cap_is_400() {
    let fx = PgFixture::start();
    let cp = fresh(&fx).await;
    cp.lineage().emit(edge(ds("w", "a"), ds("w", "b"))).await.unwrap();
    // LINEAGE_MAX_DEPTH is 32; 99 is over-cap -> the capability rejects (400), never
    // an unbounded walk.
    let (status, _body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/w/b/upstream?depth=99",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn pagination_pages_every_dataset_once() {
    let fx = PgFixture::start();
    let cp = fresh(&fx).await;
    // fan-out: 7 inputs each feeding Z
    for i in 0..7 {
        cp.lineage()
            .emit(edge(ds("w", &format!("in{i:02}")), ds("w", "z")))
            .await
            .unwrap();
    }

    let mut seen: Vec<String> = Vec::new();
    let mut after: Option<String> = None;
    for _ in 0..10 {
        let uri = match &after {
            Some(cur) => format!("/lineage/datasets/w/z/upstream?depth=1&limit=3&after={}", pct(cur)),
            None => "/lineage/datasets/w/z/upstream?depth=1&limit=3".to_string(),
        };
        let (status, body) = get(cp.clone(), Arc::new(NoServing), &uri, "alice").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let page = body["datasets"].as_array().unwrap();
        assert!(page.len() <= 3, "page never exceeds limit: {body}");
        for d in page {
            seen.push(d["name"].as_str().unwrap().to_string());
        }
        match body["next_cursor"].as_str() {
            Some(c) => after = Some(c.to_string()),
            None => break,
        }
    }
    seen.sort();
    let expected: Vec<String> = (0..7).map(|i| format!("in{i:02}")).collect();
    assert_eq!(seen, expected, "every dataset returned exactly once in stable order");
}

#[tokio::test(flavor = "multi_thread")]
async fn run_events_returns_the_runs_events() {
    let fx = PgFixture::start();
    let cp = fresh(&fx).await;
    let run = RunId(uuid::Uuid::new_v4());
    for i in 0..3 {
        cp.lineage()
            .emit(LineageEvent {
                run_id: run,
                event_type: EventType::Running,
                event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000 + i).unwrap(),
                inputs: vec![],
                outputs: vec![ds("w", &format!("o{i}"))],
                payload: serde_json::json!({ "i": i }),
            })
            .await
            .unwrap();
    }
    let uri = format!("/lineage/runs/{}/events", run.0);
    let (status, body) = get(cp.clone(), Arc::new(NoServing), &uri, "alice").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["events"].as_array().unwrap().len(), 3, "{body}");
    assert_eq!(body["events"][0]["event_type"], "running");
}

#[tokio::test(flavor = "multi_thread")]
async fn unauthenticated_is_401_authenticated_is_200() {
    let fx = PgFixture::start();
    let cp = fresh(&fx).await;
    cp.lineage().emit(edge(ds("w", "a"), ds("w", "b"))).await.unwrap();

    let status = get_unauth(cp.clone(), Arc::new(NoServing), "/lineage/datasets/w/b/upstream").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "no token -> 401");

    let (status, _body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/w/b/upstream",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "any verified subject -> 200");
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_run_id_is_400_unknown_dataset_is_empty() {
    let fx = PgFixture::start();
    let cp = fresh(&fx).await;

    // malformed run id UUID -> 400
    let (status, _body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/runs/not-a-uuid/events",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // unknown dataset -> empty page, not an error
    let (status, body) = get(
        cp.clone(),
        Arc::new(NoServing),
        "/lineage/datasets/w/nope/upstream",
        "alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["datasets"].as_array().unwrap().len(), 0, "{body}");
    assert!(body["next_cursor"].is_null());
}
```

- [ ] **Step 3: Add the BUCK target**

Add to `src/services/query-api/BUCK`:

```python
loom_fixture_test(
    name = "lineage-http-e2e",
    crate = "lineage_http_e2e",
    srcs = ["tests/lineage_http_e2e.rs"],
    crate_root = "tests/lineage_http_e2e.rs",
    deps = [
        ":query-api",
        ":e2e-support",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:axum",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

- [ ] **Step 4: Run the e2e test**

Run: `buck2 test //src/services/query-api:lineage-http-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|panicked|assertion" /tmp/t.log`
Expected: PASS (6 tests).

- [ ] **Step 5: Regression-check the touched test surface + clippy**

The `e2e_support.rs` change recompiles all e2e tests; run a representative slice plus the new targets:

Run: `buck2 test //src/services/query-api:lineage-read //src/services/query-api:openapi //src/services/query-api:wire-lineage-e2e //src/services/query-api:lineage-http-e2e //src/services/query-api:governed-read > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all PASS.

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5` (empty = clean). (e2e_support carries a crate-level panic-safety allow, so its clippy is governed by that.)

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/tests/e2e_support.rs src/services/query-api/tests/lineage_http_e2e.rs src/services/query-api/BUCK
git commit -m "test(query-api): e2e for the governed lineage read endpoints"
```

---

## Task 5: Register-doc update + final verification

**Why:** Close the register item and run the full affected test surface before the PR.

**Files:**
- Modify: `docs/ROADMAP.md` (the `road-lineage-http-read` entry) — done via `loom-docs-update` at finish time.

- [ ] **Step 1: Full affected build + test**

Run (scoped — do NOT build the whole tree):
```bash
buck2 build -M none //src/services/query-api:query-api //src/services/query-api:query-api-bin 2>&1 | tail -5
buck2 test //src/services/query-api:lineage-read //src/services/query-api:openapi //src/services/query-api:wire-lineage-e2e //src/services/query-api:lineage-http-e2e //src/services/query-api:http-smoke //src/services/query-api:governed-read > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: clean build; all tests PASS.

- [ ] **Step 2: Run prek hooks (format/lint) and commit any fixes**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; tail -30 /tmp/prek.log`
Commit any files the hooks rewrote.

- [ ] **Step 3: Close the register item** (handled at finish via `loom-docs-update`): flip `road-lineage-http-read` to `- [x]` / `status:done`, add `pr:#<N>` once the PR number is known.

---

## Self-Review

**1. Spec coverage:**

- *Three GET routes behind `require_auth`* → Task 3 (`router()` additions; `protect` gates them; `_subject: Subject` in each handler). ✓
- *`depth` (default 1, capped) forwarding + validation* → Task 3 `lineage_closure` (default 1, forwarded; capability caps → `Validation` → 400). e2e `depth_over_cap_is_400`. ✓
- *`after`/`limit` → `PageReq`* → Task 2 `parse_lineage_page`; Task 3 handlers; e2e `pagination_pages_every_dataset_once`. ✓
- *Malformed UUID / over-cap depth → 4xx* → Task 3 (`Uuid::parse_str` → 400; capability `Validation` → 400). e2e `bad_run_id_...`, `depth_over_cap_is_400`. ✓
- *JSON DTOs `{datasets,next_cursor}` / `{events,next_cursor}`* → Task 2 DTOs; Task 4 asserts shapes. ✓
- *`#[utoipa::path]` satisfying the drift guard* → Task 3 (annotations + `paths(...)`/`schemas(...)` + `expected()` update). ✓
- *Authenticated-only, no per-node ACL* → handlers take `_subject: Subject`, make no ACL calls. ✓
- *events endpoint resolves the `run_id` `post_action` returns* → `get_lineage_run_events` over `events_for`. ✓
- *Flat `DatasetRef` set (no per-node depth)* → `DatasetClosureResponse` has no depth field. ✓
- *Six test scenarios* → Task 4 maps 1:1 (traversal, depth cap, pagination, run events, auth, bad input). ✓
- *Additive, read-only* → no change to existing object/action routes; the only edit to shared code is `WireControlPlane::lineage()` (panic → delegate) which is strictly additive capability. ✓

**2. Placeholder scan:** No TBD/TODO/"handle errors"/"similar to". Every code step has full code. ✓

**3. Type consistency:** `DatasetNode`/`DatasetClosureResponse`/`LineageEventView`/`RunEventsResponse` and `parse_lineage_page`/`dataset_closure_body`/`run_events_body`/`lineage_event_view`/`event_type_str` are named identically in Task 2 (definition), Task 3 (handler use), and Task 4 (test use). `LineageDir` is defined and used only within `http.rs`. `st.cp.lineage()` works in tests (Pg `cp`) and production (wire `cp` delegates via Task 1). Route path templates match between axum (`:name`), utoipa (`{name}`), and the `expected()` guard. ✓

**Production-path guard (the key risk):** Task 1's `wire-lineage-e2e` exercises the real `WireControlPlane` (which the HTTP e2e does not, since the harness uses a `PgControlPlane` as `cp`) — so the panic-trap is covered by an explicit test, not left to production discovery.
