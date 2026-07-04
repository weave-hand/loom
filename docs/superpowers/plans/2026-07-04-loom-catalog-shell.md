# Loom Catalog Shell Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the design handoff's five-surface master-detail shell in the Yew UI, with Catalog + Ontology live, the other three surfaces stubbed, backed by two small new query-api routes.

**Architecture:** A presentational `Shell` component owns the chrome (app bar + list slot + drawer slot) and sets a per-surface `--loom-accent`. Concrete per-surface components (`CatalogView`, `OntologyView`, `StubView`) fill the slots and own their own data-fetching. All DOM-free logic (response parsers, the lineage-DAG builder, the `Surface` enum) lives in the lint-clean `loom_ui_core` lib and is `rust_test`'d; component rendering is verified by eye in the gallery (buck2's test runner has no DOM). Surface + per-surface selected-row/tab is in-memory state in `App` — no router yet.

**Tech Stack:** Rust; query-api (axum) for the backend routes; Yew → wasm for the UI; `stylist` CSS-in-Rust; `loom_ui_core` (pure) for testable logic.

## Global Constraints

- **Design tokens are authoritative:** use the exact colors/spacing from `~/Downloads/design_handoff_loom_catalog/README.md` → *Design Tokens*. Per-surface accents: Catalog `#3b82f6`, Pipelines `#2bb0a0`, Ontology `#8b5cf6`, Workbooks `#2da44e`, Dashboards `#d29922`.
- **No inline tests.** buck2 never runs `#[cfg(test)]`; the `no-inline-tests` prek hook fails the build if a first-party `src/**.rs` contains `#[test]`/`#[tokio::test]`. Every test is a `tests/<name>.rs` file wired as its own `rust_test` target. UI test targets use the `rust_test` wrapper loaded from `//src:loom_test.bzl` (already loaded in `src/ui/BUCK`).
- **Strict clippy** (pedantic + restriction) runs on production code: no `unwrap`/`expect`/`panic`/`indexing_slicing` in non-test `src/**`. The wasm UI crates carry crate-level `#![allow(clippy::pedantic, clippy::restriction)]` for `html!`; `loom_ui_core` stays lint-clean (real error handling, no panics).
- **Coarse auth** on both new backend routes — authenticated `Subject`, no per-dataset ACL — matching today's `list_datasets`. Fine-grained dataset ACL is deferred.
- **Run UI tests** with `buck2 test //src/ui/...`; **query-api tests** with `buck2 test //src/services/query-api/...`. Redirect long `buck2 test` output to a file and grep it — never pipe to `tail` (it stalls). Compile the wasm app with `buck2 build //src/ui:app`.
- **Commit style:** Conventional Commits. Every commit message ends with the two trailers:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>` and
  `Claude-Session: https://claude.ai/code/session_01SgtMg3E3d2yZYCcg3LXmZT`.
- **Branch:** all work lands on a branch off `main` (e.g. `feature/ui-catalog-shell`), not on `feature/ui-login` and not on `main`.

---

## Reference: existing interfaces (read before starting)

- `AppState` (`src/services/query-api/src/http.rs:69`): `cp: Arc<dyn ControlPlane>`, `serving: Arc<dyn ServingEngine>`, `action_engine`, `default_limit`, `naming`.
- `ServingEngine::fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError>` (`src/services/query-api/src/serving.rs`). `Rows { columns: Vec<String>, rows: Vec<Vec<SqlValue>> }`. `SqlValue`: `Text(String) | Int(i64) | Bool(bool) | Double(f64) | Date(time::Date) | Timestamp(time::PrimitiveDateTime) | Null`. Helpers `iso_date`, `iso_timestamp` are pub in `serving`.
- `DataFusionDialect.quote_ident(id) -> String` (`src/services/query-api/src/sql.rs`): wraps in `"…"`, doubling embedded quotes.
- Catalog: `st.cp.catalog().list_tables(PageReq::unbounded())` → `page.items: Vec<TableRef{schema,name}>`; `catalog().current_snapshot(&TableRef)` → `Snapshot{ id: SnapshotId(i64), time }`.
- `loom_ui_core` (`src/ui/src/lib.rs`): pure lib, single-file crate today (`srcs = ["src/lib.rs"]` in `src/ui/BUCK`). Existing pure fns: `url`, `parse_objects_page`, `columns_from_objects`, `cell_to_string`, `format_count`; enums `ButtonVariant`, `BadgeTone`, `Status`, `Align`.
- `loom_ui_components` (`src/ui/src/components/**`, glob'd — new files need no BUCK change): `TopNav`, `DataTable<R>`, `Panel`, `Tabs`, `Button`, `Badge`, `StatusDot`, `Input`, `GlobalStyles`.
- `TopNav` props (`src/ui/src/components/topnav.rs`): `items: Vec<NavItem{label,active}>`, `onselect: Callback<AttrValue>`, `search: Html`, `avatar: AttrValue`.
- The wasm binary `:app` (`src/ui/BUCK:49`) has `srcs = ["src/main.rs", "src/explorer.rs", "src/net.rs", "src/session.rs"]` — **adding a `src/*.rs` or `src/surfaces/*.rs` to the app crate requires adding it to this `srcs` list.**
- Backend route test template: `src/services/query-api/tests/datasets_routes.rs` (MemoryControlPlane + `tower::oneshot`, no socket). Target `//src/services/query-api:datasets-routes`.

---

## Stage A — Backend routes (query-api)

### Task 1: Enrich `GET /datasets` with `project` and `updated`

**Files:**
- Modify: `src/services/query-api/src/http.rs` — `list_datasets` (currently ~lines 224-235).
- Test: `src/services/query-api/tests/datasets_routes.rs` (extend existing `datasets_lists_the_seeded_table`).

**Interfaces:**
- Produces: `GET /datasets → { "datasets": [ { "schema", "name", "project", "updated" } ] }` where `project == schema` and `updated` is the current snapshot time RFC3339 (empty string if unavailable).

- [ ] **Step 1: Update the failing test**

In `src/services/query-api/tests/datasets_routes.rs`, replace the body of `datasets_lists_the_seeded_table` (the `assert_eq!(json, …)` block) with field-wise assertions:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn datasets_lists_the_seeded_table() {
    let (cp, _) = seeded();
    let app = app(cp);
    let (status, json) = get(&app, "/datasets").await;
    assert_eq!(status, StatusCode::OK);
    let ds = &json["datasets"][0];
    assert_eq!(ds["schema"], "main");
    assert_eq!(ds["name"], "events");
    assert_eq!(ds["project"], "main");
    assert!(
        ds["updated"].as_str().is_some_and(|t| !t.is_empty()),
        "updated must be a non-empty RFC3339 string, got {json}"
    );
    assert_eq!(json["datasets"].as_array().unwrap().len(), 1);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test //src/services/query-api:datasets-routes 2>&1 | tee /tmp/t1.log; grep -E "FAIL|Tests finished" /tmp/t1.log`
Expected: FAIL — `ds["project"]` and `ds["updated"]` are null (route doesn't emit them yet).

- [ ] **Step 3: Implement the enrichment**

In `src/services/query-api/src/http.rs`, replace the `list_datasets` map body so each entry looks up the current snapshot time. Use the same RFC3339 formatting `get_dataset` uses:

```rust
async fn list_datasets(State(st): State<AppState>, _subject: Subject) -> axum::response::Response {
    let catalog = st.cp.catalog();
    let page = match catalog.list_tables(PageReq::unbounded()).await {
        Ok(p) => p,
        Err(e) => return internal_error("catalog list_tables fault", e),
    };
    let mut datasets: Vec<serde_json::Value> = Vec::with_capacity(page.items.len());
    for t in &page.items {
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

If `PageReq` isn't already imported in `http.rs`, it is (used by the original `list_datasets`); no new import beyond `time` which `get_dataset` already pulls via the fully-qualified path.

- [ ] **Step 4: Run test to verify it passes**

Run: `buck2 test //src/services/query-api:datasets-routes 2>&1 | tee /tmp/t1.log; grep -E "FAIL|Tests finished" /tmp/t1.log`
Expected: PASS (3 tests).

- [ ] **Step 5: Clippy the crate**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5`
Expected: builds; `clippy.txt` empty.

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/http.rs src/services/query-api/tests/datasets_routes.rs
git commit -m "feat(query-api): enrich GET /datasets with project + updated"
```

---

### Task 2: New `GET /datasets/:schema/:table/preview` route

**Files:**
- Create: `src/services/query-api/src/dataset_preview.rs` (pure response-shaping helper).
- Modify: `src/services/query-api/src/lib.rs` (add `pub mod dataset_preview;`).
- Modify: `src/services/query-api/src/http.rs` (add route + handler).
- Test: `src/services/query-api/tests/datasets_routes.rs` (add a canned-rows serving stub + two tests).

**Interfaces:**
- Produces: `GET /datasets/{schema}/{table}/preview?limit=N → { "columns": [String], "rows": [[String]], "sampled": true }`. `limit` default 20, cap 200; bad limit → 400; unknown table → 500-mapped engine error is acceptable (coarse). `preview_body(&Rows) -> serde_json::Value` is the pure shaping fn.

- [ ] **Step 1: Write the failing pure-helper test**

Create `src/services/query-api/tests/dataset_preview.rs`:

```rust
//! Pure shaping of a served `Rows` into the preview wire body.
use query_api::dataset_preview::preview_body;
use query_api::serving::{Rows, SqlValue};

#[test]
fn preview_body_stringifies_cells_and_marks_sampled() {
    let rows = Rows {
        columns: vec!["id".into(), "amount".into(), "ok".into()],
        rows: vec![
            vec![SqlValue::Int(1), SqlValue::Double(12.5), SqlValue::Bool(true)],
            vec![SqlValue::Int(2), SqlValue::Null, SqlValue::Bool(false)],
        ],
    };
    let body = preview_body(&rows);
    assert_eq!(body["columns"], serde_json::json!(["id", "amount", "ok"]));
    assert_eq!(body["sampled"], serde_json::json!(true));
    assert_eq!(
        body["rows"],
        serde_json::json!([["1", "12.5", "true"], ["2", "", "false"]])
    );
}
```

Wire the target in `src/services/query-api/BUCK` (mirror the `datasets-routes` block but minimal deps):

```python
rust_test(
    name = "dataset-preview",
    crate = "dataset_preview",
    srcs = ["tests/dataset_preview.rs"],
    crate_root = "tests/dataset_preview.rs",
    edition = "2024",
    deps = [":query-api"],
)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test //src/services/query-api:dataset-preview 2>&1 | tee /tmp/t2.log; grep -E "error\[|FAIL|Tests finished" /tmp/t2.log`
Expected: FAIL to compile — `dataset_preview` module doesn't exist.

- [ ] **Step 3: Implement the pure helper**

Create `src/services/query-api/src/dataset_preview.rs`:

```rust
//! Shape a served `Rows` (raw dataset sample) into the `/datasets/*/preview` wire body.
//! Display-only: every cell is rendered to a string, so the UI needs no type vocabulary.

use crate::serving::{SqlValue, iso_date, iso_timestamp};

/// Render one sampled cell to its display string. `Null` → `""`.
#[must_use]
pub fn cell_string(v: &SqlValue) -> String {
    match v {
        SqlValue::Null => String::new(),
        SqlValue::Text(s) => s.clone(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        SqlValue::Double(d) => d.to_string(),
        SqlValue::Date(d) => iso_date(*d),
        SqlValue::Timestamp(t) => iso_timestamp(*t),
    }
}

/// `{ "columns": [..], "rows": [[..]], "sampled": true }`.
#[must_use]
pub fn preview_body(rows: &crate::serving::Rows) -> serde_json::Value {
    let out_rows: Vec<serde_json::Value> = rows
        .rows
        .iter()
        .map(|r| serde_json::Value::Array(r.iter().map(|c| serde_json::Value::String(cell_string(c))).collect()))
        .collect();
    serde_json::json!({
        "columns": rows.columns,
        "rows": out_rows,
        "sampled": true,
    })
}
```

Add `pub mod dataset_preview;` to `src/services/query-api/src/lib.rs` (near the other `pub mod` lines). Confirm `iso_date`/`iso_timestamp` signatures in `serving.rs` — they take the `time::Date` / `time::PrimitiveDateTime` by value; if they borrow, drop the `*`.

- [ ] **Step 4: Run the pure test to verify it passes**

Run: `buck2 test //src/services/query-api:dataset-preview 2>&1 | tee /tmp/t2.log; grep -E "FAIL|Tests finished" /tmp/t2.log`
Expected: PASS (1 test).

- [ ] **Step 5: Write the failing route test**

In `src/services/query-api/tests/datasets_routes.rs`, add a second serving stub that returns canned rows, a router builder that uses it, and two tests. Add near the top (after `StubServing`):

```rust
struct CannedServing;

#[async_trait]
impl ServingEngine for CannedServing {
    async fn fetch_rows(
        &self,
        _sql: &str,
        _params: &[SqlValue],
    ) -> std::result::Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec!["id".into(), "note".into()],
            rows: vec![
                vec![SqlValue::Int(1), SqlValue::Text("a".into())],
                vec![SqlValue::Int(2), SqlValue::Null],
            ],
        })
    }
}

fn app_canned(cp: MemoryControlPlane) -> axum::Router {
    router(AppState {
        cp: Arc::new(cp),
        serving: Arc::new(CannedServing),
        action_engine: Arc::new(StubAction),
        default_limit: 1000,
        naming: query_api::lineage_filter::local_naming(),
    })
}
```

Add the tests:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn dataset_preview_returns_sampled_rows() {
    let (cp, _) = seeded();
    let app = app_canned(cp);
    let (status, json) = get(&app, "/datasets/main/events/preview?limit=5").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["columns"], serde_json::json!(["id", "note"]));
    assert_eq!(json["sampled"], serde_json::json!(true));
    assert_eq!(json["rows"], serde_json::json!([["1", "a"], ["2", ""]]));
}

#[tokio::test(flavor = "multi_thread")]
async fn dataset_preview_rejects_bad_limit() {
    let (cp, _) = seeded();
    let app = app_canned(cp);
    let (status, _) = get(&app, "/datasets/main/events/preview?limit=nope").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
```

- [ ] **Step 6: Run route test to verify it fails**

Run: `buck2 test //src/services/query-api:datasets-routes 2>&1 | tee /tmp/t2b.log; grep -E "FAIL|Tests finished|404" /tmp/t2b.log`
Expected: FAIL — the preview route 404s (not registered).

- [ ] **Step 7: Register the route + handler**

In `src/services/query-api/src/http.rs`, add the route in `router()` next to the other `/datasets` routes:

```rust
        .route("/datasets/:schema/:table/preview", get(dataset_preview))
```

Add the handler (place near `get_dataset`). It parses `limit`, builds a quoted `SELECT *`, runs it through the shared serving engine, and shapes the result:

```rust
/// Sample rows from a dataset: `SELECT * FROM "schema"."table" LIMIT n` over the engine.
///
/// Coarse-auth (authenticated), mirroring `list_datasets`. `limit` defaults to 20 and is
/// capped at 200; a malformed `limit` is a 400.
#[utoipa::path(
    get, path = "/datasets/{schema}/{table}/preview",
    params(
        ("schema" = String, Path, description = "Iceberg schema"),
        ("table" = String, Path, description = "Table name"),
        ("limit" = Option<u32>, Query, description = "Max sample rows (default 20, cap 200)"),
    ),
    responses(
        (status = 200, description = "Sampled rows", body = DatasetPreviewResponse),
        (status = 400, description = "Bad limit"),
        (status = 500, description = "Serving error"),
    ),
    security(("bearer_auth" = [])),
    tag = "datasets",
)]
async fn dataset_preview(
    State(st): State<AppState>,
    Path((schema, table)): Path<(String, String)>,
    Query(params): Query<Vec<(String, String)>>,
    _subject: Subject,
) -> axum::response::Response {
    const DEFAULT_LIMIT: u32 = 20;
    const MAX_LIMIT: u32 = 200;
    let limit = match params.iter().find(|(k, _)| k == "limit").map(|(_, v)| v.as_str()) {
        None => DEFAULT_LIMIT,
        Some(raw) => match raw.parse::<u32>() {
            Ok(n) => n.clamp(1, MAX_LIMIT),
            Err(_) => return (StatusCode::BAD_REQUEST, "limit must be a positive integer").into_response(),
        },
    };
    let dialect = crate::sql::DataFusionDialect;
    let sql = format!(
        "SELECT * FROM {}.{} LIMIT {limit}",
        dialect.quote_ident(&schema),
        dialect.quote_ident(&table),
    );
    match st.serving.fetch_rows(&sql, &[]).await {
        Ok(rows) => Json(crate::dataset_preview::preview_body(&rows)).into_response(),
        Err(e) => internal_error("dataset preview serving fault", e),
    }
}
```

Add a `DatasetPreviewResponse` utoipa schema struct alongside the other response schemas (search for `DatasetDetailResponse` in the file and mirror it):

```rust
#[derive(utoipa::ToSchema)]
#[allow(dead_code)]
struct DatasetPreviewResponse {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    sampled: bool,
}
```

Bring `SqlDialect` into scope if needed for `quote_ident` (`use crate::sql::SqlDialect;` at the top, if not already imported). `internal_error` takes a message + error implementing the crate's error bound — confirm its signature against the `Err` arm in `list_datasets` and match it; if `ServingError` doesn't satisfy it, map with `internal_error("dataset preview serving fault", e)` after `e` → string via the existing pattern used by other serving-error sites (`query_error_response`/`cp_read_error`). Prefer whichever the neighbouring serving handlers already use for a `ServingError`.

**Register the schema for the OpenAPI doc.** Find where `DatasetDetailResponse` is registered (grep `DatasetDetailResponse` — it's in the `#[openapi(components(schemas(...)))]` block, likely `src/services/query-api/src/openapi.rs`, and the handler is listed in `paths(...)`). Add `DatasetPreviewResponse` to that `schemas(...)` list and `dataset_preview` to `paths(...)`, mirroring `get_dataset`. Skipping this leaves the generated `/openapi.json` incomplete (and, depending on the derive, can fail the build).

- [ ] **Step 8: Run route test to verify it passes**

Run: `buck2 test //src/services/query-api:datasets-routes 2>&1 | tee /tmp/t2b.log; grep -E "FAIL|Tests finished" /tmp/t2b.log`
Expected: PASS (5 tests).

- [ ] **Step 9: Clippy + full query-api sweep**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' 2>&1 | tail -5` (empty) then
`buck2 test //src/services/query-api/... 2>&1 | tee /tmp/qa.log; grep -E "FAIL|Tests finished" /tmp/qa.log`
Expected: all pass (no regression).

- [ ] **Step 10: Commit**

```bash
git add src/services/query-api/src/dataset_preview.rs src/services/query-api/src/lib.rs \
        src/services/query-api/src/http.rs src/services/query-api/BUCK \
        src/services/query-api/tests/dataset_preview.rs src/services/query-api/tests/datasets_routes.rs
git commit -m "feat(query-api): GET /datasets/:schema/:table/preview sampled-rows route"
```

---

## Stage B — Shell chrome + surface switcher + stubs

### Task 3: `Surface` enum in `loom_ui_core`

**Files:**
- Modify: `src/ui/src/lib.rs` (append the `Surface` enum + impl).
- Create: `src/ui/tests/surface.rs`.
- Modify: `src/ui/BUCK` (add a `surface` rust_test target).

**Interfaces:**
- Produces: `pub enum Surface { Catalog, Pipelines, Ontology, Workbooks, Dashboards }` with `pub fn label(self) -> &'static str`, `pub fn accent(self) -> &'static str` (hex), `pub fn is_live(self) -> bool`, `pub fn all() -> [Surface; 5]`. Consumed by `Shell`, `App`, the surface views.

- [ ] **Step 1: Write the failing test**

Create `src/ui/tests/surface.rs`:

```rust
use loom_ui_core::Surface;

#[test]
fn accents_match_the_design_tokens() {
    assert_eq!(Surface::Catalog.accent(), "#3b82f6");
    assert_eq!(Surface::Pipelines.accent(), "#2bb0a0");
    assert_eq!(Surface::Ontology.accent(), "#8b5cf6");
    assert_eq!(Surface::Workbooks.accent(), "#2da44e");
    assert_eq!(Surface::Dashboards.accent(), "#d29922");
}

#[test]
fn only_catalog_and_ontology_are_live() {
    let live: Vec<&str> = Surface::all().into_iter().filter(|s| s.is_live()).map(Surface::label).collect();
    assert_eq!(live, vec!["Catalog", "Ontology"]);
}

#[test]
fn all_lists_five_surfaces_in_nav_order() {
    let labels: Vec<&str> = Surface::all().into_iter().map(Surface::label).collect();
    assert_eq!(labels, vec!["Catalog", "Pipelines", "Ontology", "Workbooks", "Dashboards"]);
}
```

Add the target to `src/ui/BUCK` (mirror the `tokens` block):

```python
rust_test(
    name = "surface",
    crate = "surface",
    srcs = ["tests/surface.rs"],
    crate_root = "tests/surface.rs",
    edition = "2021",
    deps = [":ui-core"],
)
```

(Match the `edition` used by the sibling `tokens`/`objects` test targets — copy whatever they set.)

- [ ] **Step 2: Run test to verify it fails**

Run: `buck2 test //src/ui:surface 2>&1 | tee /tmp/t3.log; grep -E "error\[|FAIL|Tests finished" /tmp/t3.log`
Expected: FAIL to compile — `Surface` doesn't exist.

- [ ] **Step 3: Implement `Surface`**

Append to `src/ui/src/lib.rs`:

```rust
/// One of the app's five top-level surfaces (nav order). Backend-live surfaces are
/// Catalog and Ontology; the others render an honest "not available" stub.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Surface {
    Catalog,
    Pipelines,
    Ontology,
    Workbooks,
    Dashboards,
}

impl Surface {
    /// Nav order, left to right.
    #[must_use]
    pub fn all() -> [Surface; 5] {
        [
            Surface::Catalog,
            Surface::Pipelines,
            Surface::Ontology,
            Surface::Workbooks,
            Surface::Dashboards,
        ]
    }

    /// The nav label / list title for this surface.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Surface::Catalog => "Catalog",
            Surface::Pipelines => "Pipelines",
            Surface::Ontology => "Ontology",
            Surface::Workbooks => "Workbooks",
            Surface::Dashboards => "Dashboards",
        }
    }

    /// The per-surface accent hex (design tokens).
    #[must_use]
    pub fn accent(self) -> &'static str {
        match self {
            Surface::Catalog => "#3b82f6",
            Surface::Pipelines => "#2bb0a0",
            Surface::Ontology => "#8b5cf6",
            Surface::Workbooks => "#2da44e",
            Surface::Dashboards => "#d29922",
        }
    }

    /// Whether the backend can serve this surface (else the shell shows a stub).
    #[must_use]
    pub fn is_live(self) -> bool {
        matches!(self, Surface::Catalog | Surface::Ontology)
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `buck2 test //src/ui:surface 2>&1 | tee /tmp/t3.log; grep -E "FAIL|Tests finished" /tmp/t3.log`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/ui/src/lib.rs src/ui/tests/surface.rs src/ui/BUCK
git commit -m "feat(ui): Surface enum (labels, per-surface accents, live flag)"
```

---

### Task 4: `Shell` + `StubView` presentational components

**Files:**
- Create: `src/ui/src/components/shell.rs` (glob'd into `:ui-components` — no BUCK change).
- Create: `src/ui/src/components/stub.rs`.
- Modify: `src/ui/src/components/mod.rs` (re-export `Shell`, `ShellProps`, `StubView`, `StubViewProps`).
- Modify: `src/ui/src/gallery.rs` (render `Shell` with stub content for eye-check).

**Interfaces:**
- Produces: `Shell` component with props `{ active: Surface, on_switch: Callback<Surface>, search: Html, avatar: AttrValue, list: Html, drawer: Html }`. It renders the app bar (surface switcher across `Surface::all()`, active item accent-underlined), a main-list region (`list`), and a right drawer region (`drawer`, omitted when empty). It sets `style="--loom-accent: {active.accent()}"` on its root so descendants re-theme. `StubView` props `{ surface: Surface }` renders the centered "not available on this instance" empty state.

- [ ] **Step 1: Implement `StubView`**

Create `src/ui/src/components/stub.rs`:

```rust
use loom_ui_core::Surface;
use stylist::yew::styled_component;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct StubViewProps {
    pub surface: Surface,
}

#[styled_component(StubView)]
pub fn stub_view(props: &StubViewProps) -> Html {
    let cls = css!(
        r#"
        display: flex; flex-direction: column; align-items: center; justify-content: center;
        height: 60vh; gap: 8px; color: var(--loom-text-mut); text-align: center;
        .title { color: var(--loom-text); font-size: 15px; font-weight: 600; }
        .accent { color: var(--loom-accent); }
    "#
    );
    html! {
        <div class={cls}>
            <div class="title"><span class="accent">{ props.surface.label() }</span></div>
            <div>{ "This surface isn't available on this instance yet." }</div>
        </div>
    }
}
```

- [ ] **Step 2: Implement `Shell`**

Create `src/ui/src/components/shell.rs`:

```rust
use loom_ui_core::Surface;
use stylist::yew::styled_component;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ShellProps {
    pub active: Surface,
    pub on_switch: Callback<Surface>,
    #[prop_or_default]
    pub search: Html,
    #[prop_or_default]
    pub avatar: AttrValue,
    #[prop_or_default]
    pub list: Html,
    #[prop_or_default]
    pub drawer: Html,
}

#[styled_component(Shell)]
pub fn shell(props: &ShellProps) -> Html {
    let cls = css!(
        r#"
        min-height: 100vh; background: var(--loom-bg); color: var(--loom-text);
        .bar {
            display: flex; align-items: center; gap: 20px; height: 48px; padding: 0 16px;
            background: var(--loom-panel); border-bottom: 1px solid var(--loom-border);
        }
        .brand { display: flex; align-items: center; gap: 8px; font-weight: 700; font-size: 15px; }
        .logo { width: 18px; height: 18px; border-radius: 5px;
                background: linear-gradient(135deg, #3b82f6, #1d4ed8); }
        .nav { display: flex; gap: 20px; }
        .nav button {
            all: unset; cursor: pointer; font-size: 13px; font-weight: 500; color: var(--loom-text-mut);
            padding-bottom: 2px; border-bottom: 2px solid transparent;
        }
        .nav button.active { color: var(--loom-text); border-bottom-color: var(--loom-accent); }
        .spacer { flex: 1; }
        .avatar { width: 26px; height: 26px; border-radius: 50%;
                  background: var(--loom-accent); color: #fff;
                  display: inline-flex; align-items: center; justify-content: center; font-size: 11px; }
        .body { display: flex; align-items: stretch; }
        .list { flex: 1; min-width: 0; padding: 18px 22px; }
        .drawer { width: 428px; flex: none; background: var(--loom-panel);
                  border-left: 1px solid var(--loom-border); }
    "#
    );
    let accent_style = format!("--loom-accent: {}", props.active.accent());
    html! {
        <div class={cls} style={accent_style}>
            <div class="bar">
                <span class="brand"><span class="logo"></span>{ "loom" }</span>
                <div class="nav">
                    { for Surface::all().into_iter().map(|s| {
                        let on_switch = props.on_switch.clone();
                        let onclick = Callback::from(move |_| on_switch.emit(s));
                        let active = s == props.active;
                        html! {
                            <button class={classes!(active.then_some("active"))} {onclick}>
                                { s.label() }
                            </button>
                        }
                    }) }
                </div>
                <div class="spacer" />
                { props.search.clone() }
                if !props.avatar.is_empty() { <span class="avatar">{ &props.avatar }</span> }
            </div>
            <div class="body">
                <div class="list">{ props.list.clone() }</div>
                if !is_empty(&props.drawer) {
                    <div class="drawer">{ props.drawer.clone() }</div>
                }
            </div>
        </div>
    }
}

// Yew's `Html::default()` is `VNode::default()` (an empty list); treat that as "no drawer".
fn is_empty(h: &Html) -> bool {
    h == &Html::default()
}
```

- [ ] **Step 3: Re-export from `mod.rs`**

Add to `src/ui/src/components/mod.rs` (mirroring the existing `pub use` lines):

```rust
pub use shell::{Shell, ShellProps};
pub use stub::{StubView, StubViewProps};
```

and the module decls (`mod shell;`, `mod stub;`) in the form the file already uses.

- [ ] **Step 4: Render in the gallery for eye-check**

In `src/ui/src/gallery.rs`, add a `Shell` demo (a section rendering `<Shell active={Surface::Catalog} on_switch={Callback::noop()} avatar="DK" list={html!{<StubView surface={Surface::Pipelines} />}} />`). Import `Surface` from `loom_ui_core` and `Shell`/`StubView` from `loom_ui_components`.

- [ ] **Step 5: Build the components + gallery**

Run: `buck2 build //src/ui:ui-components 2>&1 | tail -3 && buck2 build //src/ui:gallery-bundle 2>&1 | tail -3`
Expected: both build clean.

- [ ] **Step 6: Visual check**

Run: `buck2 run //src/ui:gallery-serve` and open the served URL; confirm the app bar shows all five surfaces, Catalog active with a blue underline, and the stub body centered. (No automated assertion — rendering has no DOM in buck2, per `src/ui/CLAUDE.md`.)

- [ ] **Step 7: Commit**

```bash
git add src/ui/src/components/shell.rs src/ui/src/components/stub.rs \
        src/ui/src/components/mod.rs src/ui/src/gallery.rs
git commit -m "feat(ui): Shell chrome + StubView presentational components"
```

---

### Task 5: Wire `App` to switch surfaces (all stubbed)

**Files:**
- Modify: `src/ui/src/main.rs` (replace the post-login `Explorer` render with a `Shell` + surface state; all surfaces render `StubView` for now).

**Interfaces:**
- Consumes: `Shell`, `StubView`, `Surface`, the existing `Explorer` (kept, temporarily unused in the new path — leave it compiled; Task 6/7 replace stubs surface-by-surface).
- Produces: a navigable 5-surface shell behind login; `active_surface` state defaults to `Surface::Catalog`.

- [ ] **Step 1: Replace the authenticated render**

In `src/ui/src/main.rs`, in `app()`'s `if token.is_some()` branch, replace the `return html! { <Explorer … /> }` with a surface-switching shell. Add `use loom_ui_components::{Shell, StubView};` and `use loom_ui_core::Surface;` to the imports.

```rust
    if token.is_some() {
        let on_logout: Callback<()> = { /* unchanged from today */ };
        return html! { <Workspace token={(*token).clone().unwrap_or_default()} on_logout={on_logout} /> };
    }
```

Add a `Workspace` component (in `main.rs`) holding the surface state:

```rust
#[derive(Properties, PartialEq)]
struct WorkspaceProps {
    token: AttrValue,
    on_logout: Callback<()>,
}

#[function_component(Workspace)]
fn workspace(props: &WorkspaceProps) -> Html {
    let surface = use_state(|| Surface::Catalog);
    let on_switch = {
        let surface = surface.clone();
        Callback::from(move |s: Surface| surface.set(s))
    };
    let on_logout = props.on_logout.clone();
    let logout_btn = html! {
        <Button variant={ButtonVariant::Ghost}
            onclick={Callback::from(move |_: MouseEvent| on_logout.emit(()))}>{ "Log out" }</Button>
    };
    // Every surface is a stub until Tasks 6–7/8–9 replace Ontology and Catalog.
    let list = html! { <StubView surface={*surface} /> };
    html! {
        <>
            <GlobalStyles />
            <Shell active={*surface} on_switch={on_switch} search={logout_btn} avatar="DK" list={list} />
        </>
    }
}
```

Add `use loom_ui_components::Button;` and `use loom_ui_core::ButtonVariant;` if not present. Keep the `Explorer`/`explorer` module import so it still compiles (Task 7 re-homes it); if the compiler warns "unused", add `#[allow(unused_imports)]` on that line with a `reason` — it's removed in Task 7.

- [ ] **Step 2: Build the app**

Run: `buck2 build //src/ui:app 2>&1 | tail -5`
Expected: builds (wasm).

- [ ] **Step 3: Visual check (optional but recommended)**

Run: `buck2 build //src/ui:bundle && buck2 run //src/ui:serve` — but note this bundle has no backend; login won't complete. For a real end-to-end check use the all-in-one path if available; otherwise rely on the gallery (Task 4) for chrome and defer live check to Task 7/9.

- [ ] **Step 4: Commit**

```bash
git add src/ui/src/main.rs
git commit -m "feat(ui): surface-switching Workspace shell (all surfaces stubbed)"
```

---

## Stage C — Ontology surface

### Task 6: Ontology type-detail parser + net fetch

**Files:**
- Modify: `src/ui/src/lib.rs` (add `TypeDetail` struct + `parse_type_detail`).
- Create: `src/ui/tests/type_detail.rs`.
- Modify: `src/ui/BUCK` (add `type-detail` rust_test).
- Modify: `src/ui/src/net.rs` (add `fetch_type_detail`).

**Interfaces:**
- Produces: `pub struct TypeDetail { pub properties: Vec<PropRow>, pub links: Vec<LinkRow>, pub links_to: Vec<LinkRow> }`, `pub struct PropRow { pub name: String, pub ty: String, pub required: bool }`, `pub struct LinkRow { pub name: String, pub from: String, pub to: String, pub cardinality: String }`, `pub fn parse_type_detail(&Value) -> TypeDetail`. `net::fetch_type_detail(base, token, type_name) -> Result<TypeDetail, FetchError>`.

- [ ] **Step 1: Write the failing parser test**

Create `src/ui/tests/type_detail.rs`:

```rust
use loom_ui_core::parse_type_detail;

#[test]
fn parses_properties_and_both_link_directions() {
    let body = serde_json::json!({
        "name": "Order",
        "properties": [
            { "name": "id", "ty": "Long", "required": true },
            { "name": "note", "ty": "String", "required": false }
        ],
        "links": [ { "name": "customer", "from": "Order", "to": "Customer", "cardinality": "one" } ],
        "links_to": []
    });
    let d = parse_type_detail(&body);
    assert_eq!(d.properties.len(), 2);
    assert_eq!(d.properties[0].name, "id");
    assert!(d.properties[0].required);
    assert_eq!(d.properties[1].ty, "String");
    assert_eq!(d.links.len(), 1);
    assert_eq!(d.links[0].to, "Customer");
    assert_eq!(d.links[0].cardinality, "one");
    assert!(d.links_to.is_empty());
}

#[test]
fn missing_fields_default_to_empty() {
    let d = parse_type_detail(&serde_json::json!({}));
    assert!(d.properties.is_empty() && d.links.is_empty() && d.links_to.is_empty());
}
```

Add the `type-detail` target to `src/ui/BUCK` (mirror `surface`, dep `:ui-core`).

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test //src/ui:type-detail 2>&1 | tee /tmp/t6.log; grep -E "error\[|FAIL|Tests finished" /tmp/t6.log`
Expected: FAIL to compile.

- [ ] **Step 3: Implement the parser**

Append to `src/ui/src/lib.rs`:

```rust
/// A property row in the ontology drawer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropRow {
    pub name: String,
    pub ty: String,
    pub required: bool,
}

/// A link row (either outbound `links` or inbound `links_to`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkRow {
    pub name: String,
    pub from: String,
    pub to: String,
    pub cardinality: String,
}

/// The ontology drawer's Properties + Links data, decoded from `GET /ontology/types/{name}`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TypeDetail {
    pub properties: Vec<PropRow>,
    pub links: Vec<LinkRow>,
    pub links_to: Vec<LinkRow>,
}

fn str_field(v: &Value, k: &str) -> String {
    v.get(k).and_then(Value::as_str).unwrap_or_default().to_string()
}

fn parse_links(v: &Value, key: &str) -> Vec<LinkRow> {
    v.get(key)
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|l| LinkRow {
                    name: str_field(l, "name"),
                    from: str_field(l, "from"),
                    to: str_field(l, "to"),
                    cardinality: str_field(l, "cardinality"),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Decode a type-detail body. Total: missing arrays → empty; missing scalars → default.
#[must_use]
pub fn parse_type_detail(body: &Value) -> TypeDetail {
    let properties = body
        .get("properties")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|p| PropRow {
                    name: str_field(p, "name"),
                    ty: str_field(p, "ty"),
                    required: p.get("required").and_then(Value::as_bool).unwrap_or(false),
                })
                .collect()
        })
        .unwrap_or_default();
    TypeDetail {
        properties,
        links: parse_links(body, "links"),
        links_to: parse_links(body, "links_to"),
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `buck2 test //src/ui:type-detail 2>&1 | tee /tmp/t6.log; grep -E "FAIL|Tests finished" /tmp/t6.log`
Expected: PASS (2 tests).

- [ ] **Step 5: Add the net fetch**

In `src/ui/src/net.rs`, add (mirroring `fetch_types`):

```rust
use loom_ui_core::{TypeDetail, parse_type_detail};

/// GET /ontology/types/{name} with the bearer token.
pub async fn fetch_type_detail(base: &str, token: &str, type_name: &str) -> Result<TypeDetail, FetchError> {
    let resp = Request::get(&url(base, &format!("/ontology/types/{type_name}")))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 200 {
        return Err(fetch_status_err(resp.status()));
    }
    let body: serde_json::Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(parse_type_detail(&body))
}
```

- [ ] **Step 6: Build the app**

Run: `buck2 build //src/ui:app 2>&1 | tail -5`
Expected: builds (may warn `fetch_type_detail` unused until Task 7 — acceptable this task; if the strict gate rejects dead_code add `#[allow(dead_code, reason = "used by OntologyView in the next task")]`).

- [ ] **Step 7: Commit**

```bash
git add src/ui/src/lib.rs src/ui/tests/type_detail.rs src/ui/BUCK src/ui/src/net.rs
git commit -m "feat(ui): parse_type_detail + net.fetch_type_detail for the ontology drawer"
```

---

### Task 7: `OntologyView` — port Explorer into the Shell

**Files:**
- Create: `src/ui/src/surfaces/mod.rs`, `src/ui/src/surfaces/ontology.rs`.
- Modify: `src/ui/BUCK` (add `src/surfaces/mod.rs`, `src/surfaces/ontology.rs` to `:app` `srcs`).
- Modify: `src/ui/src/main.rs` (declare `mod surfaces;`; render `OntologyView` when `surface == Ontology`).
- Delete usage: retire the old top-level `Explorer` render (the `explorer.rs` file's logic moves into `ontology.rs`; keep or remove `explorer.rs` — remove it and its BUCK `srcs` entry).

**Interfaces:**
- Consumes: `net::{fetch_types, fetch_page, fetch_type_detail}`, `Shell` slots, `DataTable`, `Panel`, `Tabs`, `TypeDetail`.
- Produces: `OntologyView` component with props `{ token: AttrValue, on_logout: Callback<()> }` that renders its own list (type sidebar + objects table with Load-more) and drawer (Properties + Links tabs). Rendered by `Workspace` inside the `Shell` list slot; the drawer is passed to `Shell`'s drawer slot.

> This task ports the existing `explorer.rs` behavior. Reuse its `ObjectRow`, `to_rows`, `to_columns`, the three `use_state` load flows (types, page, load-more), and the row/logout callbacks **verbatim** — the only changes are: (a) it renders into the `Shell`'s `list`/`drawer` slots instead of its own top-level `TopNav`+fl: layout, and (b) the drawer gains **Properties**/**Links** tabs fed by `fetch_type_detail` (fetched on type-select), replacing the raw key/value `<dl>` dump.

- [ ] **Step 1: Create the surfaces module**

Create `src/ui/src/surfaces/mod.rs`:

```rust
mod ontology;
pub use ontology::OntologyView;
```

- [ ] **Step 2: Implement `OntologyView`**

Create `src/ui/src/surfaces/ontology.rs` by moving the body of `explorer.rs` in and adapting it. Keep the type-list + objects-table + load-more logic identical. Add a `type_detail` state loaded when a type is selected, and render the drawer as `Tabs` (Properties · Links) over the `TypeDetail`. Structure:

```rust
use crate::net::{self, FetchError};
use loom_ui_components::{Column, DataTable, Panel, TabItem, TableRow, Tabs};
use loom_ui_core::{Align, TypeDetail, cell_to_string, columns_from_objects};
use serde_json::{Map, Value};
use yew::prelude::*;

// ObjectRow, to_rows, to_columns, LoadStatus: copy verbatim from explorer.rs.

#[derive(Properties, PartialEq)]
pub struct OntologyViewProps {
    pub token: AttrValue,
    pub on_list: Callback<Html>,   // hand the composed list Html up to the Shell
    pub on_drawer: Callback<Html>, // hand the composed drawer Html up to the Shell
    pub on_logout: Callback<()>,
}
```

> **Slot mechanism:** rather than `on_list`/`on_drawer` callbacks (awkward in Yew), have `Workspace` render `OntologyView` directly into the `Shell` `list` slot and let `OntologyView` render the type-sidebar + table; the **drawer** is a separate small component (`OntologyDrawer`) that `Workspace` places in the `Shell` `drawer` slot, driven by shared state lifted to `Workspace`. Simpler concrete approach: **lift the selected-type / selected-row / type-detail state into `Workspace`** and pass it down to both an `OntologyList` and an `OntologyDrawer`. Implement it that way:

- `Workspace` (in `main.rs`) owns, per the Ontology surface: `selected_type`, `objs`, `columns`, `next`, `selected_row`, `type_detail`, and the load effects (moved from `explorer.rs`).
- `surfaces/ontology.rs` exposes two pure-ish presentational components: `OntologyList { types, selected_type, objs, columns, next, on_select_type, on_row, on_load_more, … }` and `OntologyDrawer { selected_type, selected_obj: Option<Map>, detail: Option<TypeDetail>, active_tab, on_tab }`.

Keep the Properties tab rendering the `TypeDetail.properties` (`name` in accent-blue via a class, `ty` monospace, a `required`/`pii` annotation), and the Links tab rendering `links` + `links_to` (verb arrow + target + cardinality) per the handoff's *Ontology › Properties/Links* spec.

- [ ] **Step 3: Wire `Workspace` to render Ontology live**

In `main.rs` `Workspace`, branch on `*surface`:

```rust
let (list, drawer) = match *surface {
    Surface::Ontology => ontology_slots(/* state + callbacks */),
    Surface::Catalog => (html! { <StubView surface={Surface::Catalog} /> }, Html::default()), // Task 9
    other => (html! { <StubView surface={other} /> }, Html::default()),
};
html! {
    <>
        <GlobalStyles />
        <Shell active={*surface} on_switch={on_switch} search={logout_btn} avatar="DK"
               list={list} drawer={drawer} />
    </>
}
```

- [ ] **Step 4: Update BUCK + drop `explorer.rs`**

In `src/ui/BUCK`, change `:app` `srcs` to include `src/surfaces/mod.rs`, `src/surfaces/ontology.rs` and remove `src/explorer.rs`. Delete `src/ui/src/explorer.rs`. Remove `mod explorer;` from `main.rs`.

- [ ] **Step 5: Build the app**

Run: `buck2 build //src/ui:app 2>&1 | tail -8`
Expected: builds. Fix any move-related import errors.

- [ ] **Step 6: Clippy the app crate**

Run: `buck2 build '//src/ui:app[clippy.txt]' 2>&1 | tail -5`
Expected: empty (crate-level pedantic/restriction allow already covers `html!`).

- [ ] **Step 7: Live visual check**

Boot the app against a backend (all-in-one binary per `src/ui/CLAUDE.md`, or a running query-api with `LOOM_UI_DIR` pointed at the bundle). Log in, confirm Ontology surface lists types → objects with Load-more, and the drawer shows Properties/Links tabs. If no live backend is available in this environment, note it and rely on `buck2 build` + gallery; flag for the reviewer.

- [ ] **Step 8: Commit**

```bash
git add src/ui/src/surfaces/ src/ui/src/main.rs src/ui/BUCK
git rm src/ui/src/explorer.rs
git commit -m "feat(ui): OntologyView — Explorer re-homed into the Shell with Properties/Links tabs"
```

---

## Stage D — Catalog surface (list + Schema/Preview/History)

### Task 8: Catalog parsers + net fetches

**Files:**
- Modify: `src/ui/src/lib.rs` (add `DatasetRow`, `DatasetDetail`, `PreviewData` structs + `parse_datasets`, `parse_dataset_detail`, `parse_preview`).
- Create: `src/ui/tests/catalog.rs`.
- Modify: `src/ui/BUCK` (add `catalog` rust_test).
- Modify: `src/ui/src/net.rs` (add `fetch_datasets`, `fetch_dataset_detail`, `fetch_preview`).

**Interfaces:**
- Produces:
  - `pub struct DatasetRow { pub schema: String, pub name: String, pub project: String, pub updated: String }`, `pub fn parse_datasets(&Value) -> Vec<DatasetRow>`.
  - `pub struct SchemaCol { pub name: String, pub ty: String, pub nullable: bool }`, `pub struct DatasetDetail { pub snapshot_time: String, pub columns: Vec<SchemaCol> }`, `pub fn parse_dataset_detail(&Value) -> DatasetDetail`.
  - `pub struct PreviewData { pub columns: Vec<String>, pub rows: Vec<Vec<String>>, pub sampled: bool }`, `pub fn parse_preview(&Value) -> PreviewData`.
  - `net::{fetch_datasets(base,token) -> Result<Vec<DatasetRow>,_>, fetch_dataset_detail(base,token,schema,table) -> Result<DatasetDetail,_>, fetch_preview(base,token,schema,table,limit) -> Result<PreviewData,_>}`.

- [ ] **Step 1: Write the failing parser tests**

Create `src/ui/tests/catalog.rs`:

```rust
use loom_ui_core::{parse_dataset_detail, parse_datasets, parse_preview};

#[test]
fn parses_dataset_list_rows() {
    let body = serde_json::json!({ "datasets": [
        { "schema": "main", "name": "txns", "project": "main", "updated": "2026-07-01T00:00:00Z" }
    ] });
    let rows = parse_datasets(&body);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "txns");
    assert_eq!(rows[0].project, "main");
    assert_eq!(rows[0].updated, "2026-07-01T00:00:00Z");
}

#[test]
fn parses_dataset_detail_columns() {
    let body = serde_json::json!({
        "snapshot_time": "2026-07-01T00:00:00Z",
        "columns": [ { "name": "id", "ty": "Long", "nullable": false } ]
    });
    let d = parse_dataset_detail(&body);
    assert_eq!(d.snapshot_time, "2026-07-01T00:00:00Z");
    assert_eq!(d.columns.len(), 1);
    assert_eq!(d.columns[0].name, "id");
    assert!(!d.columns[0].nullable);
}

#[test]
fn parses_preview() {
    let body = serde_json::json!({
        "columns": ["id", "note"], "rows": [["1", "a"], ["2", ""]], "sampled": true
    });
    let p = parse_preview(&body);
    assert_eq!(p.columns, vec!["id", "note"]);
    assert_eq!(p.rows, vec![vec!["1", "a"], vec!["2", ""]]);
    assert!(p.sampled);
}
```

Add the `catalog` rust_test target to `src/ui/BUCK` (dep `:ui-core`).

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test //src/ui:catalog 2>&1 | tee /tmp/t8.log; grep -E "error\[|FAIL|Tests finished" /tmp/t8.log`
Expected: FAIL to compile.

- [ ] **Step 3: Implement the parsers**

Append to `src/ui/src/lib.rs`:

```rust
/// A row in the Catalog list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetRow {
    pub schema: String,
    pub name: String,
    pub project: String,
    pub updated: String,
}

/// Decode `GET /datasets`. Missing array → empty; missing scalars → "".
#[must_use]
pub fn parse_datasets(body: &Value) -> Vec<DatasetRow> {
    body.get("datasets")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|d| DatasetRow {
                    schema: str_field(d, "schema"),
                    name: str_field(d, "name"),
                    project: str_field(d, "project"),
                    updated: str_field(d, "updated"),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// A schema column in the Catalog › Schema tab.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaCol {
    pub name: String,
    pub ty: String,
    pub nullable: bool,
}

/// Decode `GET /datasets/{schema}/{table}`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DatasetDetail {
    pub snapshot_time: String,
    pub columns: Vec<SchemaCol>,
}

#[must_use]
pub fn parse_dataset_detail(body: &Value) -> DatasetDetail {
    let columns = body
        .get("columns")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|c| SchemaCol {
                    name: str_field(c, "name"),
                    ty: str_field(c, "ty"),
                    nullable: c.get("nullable").and_then(Value::as_bool).unwrap_or(false),
                })
                .collect()
        })
        .unwrap_or_default();
    DatasetDetail { snapshot_time: str_field(body, "snapshot_time"), columns }
}

/// Decode `GET /datasets/{schema}/{table}/preview`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PreviewData {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub sampled: bool,
}

#[must_use]
pub fn parse_preview(body: &Value) -> PreviewData {
    let columns = body
        .get("columns")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(|v| v.as_str().map(ToOwned::to_owned)).collect())
        .unwrap_or_default();
    let rows = body
        .get("rows")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .map(|r| {
                    r.as_array()
                        .map(|cells| cells.iter().map(cell_to_string).collect())
                        .unwrap_or_default()
                })
                .collect()
        })
        .unwrap_or_default();
    PreviewData { columns, rows, sampled: body.get("sampled").and_then(Value::as_bool).unwrap_or(false) }
}
```

(`str_field` was added in Task 6; if Task 6 wasn't merged first, add it here.)

- [ ] **Step 4: Run to verify it passes**

Run: `buck2 test //src/ui:catalog 2>&1 | tee /tmp/t8.log; grep -E "FAIL|Tests finished" /tmp/t8.log`
Expected: PASS (3 tests).

- [ ] **Step 5: Add net fetches**

In `src/ui/src/net.rs`, add `fetch_datasets`, `fetch_dataset_detail`, `fetch_preview` mirroring `fetch_type_detail` (bearer header, non-200 → `fetch_status_err`, decode via the parsers). `fetch_preview` builds `/datasets/{schema}/{table}/preview?limit={limit}`.

- [ ] **Step 6: Build the app**

Run: `buck2 build //src/ui:app 2>&1 | tail -5`
Expected: builds (add `#[allow(dead_code, reason = "used by CatalogView next task")]` on the new net fns if the gate complains).

- [ ] **Step 7: Commit**

```bash
git add src/ui/src/lib.rs src/ui/tests/catalog.rs src/ui/BUCK src/ui/src/net.rs
git commit -m "feat(ui): Catalog parsers (datasets/detail/preview) + net fetches"
```

---

### Task 9: `CatalogView` — list + Schema/Preview/History drawer

**Files:**
- Create: `src/ui/src/surfaces/catalog.rs`.
- Modify: `src/ui/src/surfaces/mod.rs` (export `CatalogView` pieces).
- Modify: `src/ui/BUCK` (add `src/surfaces/catalog.rs` to `:app` `srcs`).
- Modify: `src/ui/src/main.rs` (`Workspace` renders Catalog list + drawer when `surface == Catalog`).

**Interfaces:**
- Consumes: `net::{fetch_datasets, fetch_dataset_detail, fetch_preview}`, `DataTable`, `Tabs`, `Panel`, `DatasetRow`, `DatasetDetail`, `PreviewData`.
- Produces: a Catalog list (columns Name · Project · Rows(`—`) · Updated) that on row-select drives a drawer with tabs **Schema** (columns from `fetch_dataset_detail`), **Preview** (`fetch_preview`, sampled table + "Showing N · sampled" footnote), **Lineage** (placeholder text this task — real in Task 11), **History** (honest stub). Selected-dataset + active-tab state lifted into `Workspace` (same pattern as Ontology).

- [ ] **Step 1: Implement `CatalogView`**

Create `src/ui/src/surfaces/catalog.rs`. Follow the Ontology pattern: a `CatalogList` presentational component (a `DataTable` over `DatasetRow`s, row swatch + owner-less columns per the token spec) and a `CatalogDrawer` component (`Tabs` Schema · Preview · Lineage · History). Lift into `Workspace`: `datasets: Vec<DatasetRow>`, `selected_dataset: Option<usize>`, `detail: Option<DatasetDetail>`, `preview: Option<PreviewData>`, `catalog_tab: CatalogTab`. Load `datasets` on entering the Catalog surface (effect keyed on surface); load `detail` on row-select; load `preview` when the Preview tab is first activated for the selected row.

Row type:

```rust
#[derive(Clone, PartialEq)]
struct CatalogRow { name: String, project: String, rows: String, updated: String }

impl TableRow for CatalogRow {
    fn cells(&self) -> Vec<Html> {
        vec![
            html! { <span>{ &self.name }</span> },
            html! { { &self.project } },
            html! { { &self.rows } },     // always "—" for now
            html! { { &self.updated } },
        ]
    }
}
```

Schema tab renders each `SchemaCol` as `name type` (name in accent, `nullable` annotation). Preview tab renders a plain `<table>` of `PreviewData` with the footnote `format!("Showing {} · sampled", p.rows.len())`. History tab renders: `"Per-dataset run history isn't available on this instance yet."` Lineage tab this task: `"Lineage — coming in the next step."` (replaced in Task 11).

- [ ] **Step 2: Export + wire**

Add exports to `surfaces/mod.rs`. In `Workspace`, replace the Catalog arm of the `match *surface` with the composed `(list, drawer)`.

- [ ] **Step 3: Update BUCK**

Add `src/surfaces/catalog.rs` to `:app` `srcs` in `src/ui/BUCK`.

- [ ] **Step 4: Build + clippy**

Run: `buck2 build //src/ui:app 2>&1 | tail -8 && buck2 build '//src/ui:app[clippy.txt]' 2>&1 | tail -3`
Expected: builds; clippy empty.

- [ ] **Step 5: Live visual check**

Against a live backend: Catalog lists datasets; selecting one shows Schema columns and a Preview of sampled rows; History shows the stub. If no live backend here, note it and defer to the reviewer.

- [ ] **Step 6: Commit**

```bash
git add src/ui/src/surfaces/catalog.rs src/ui/src/surfaces/mod.rs src/ui/src/main.rs src/ui/BUCK
git commit -m "feat(ui): CatalogView — dataset list + Schema/Preview/History drawer tabs"
```

---

## Stage E — Catalog Lineage (mini-DAG)

### Task 10: `lineage_dag` builder in `loom_ui_core`

**Files:**
- Modify: `src/ui/src/lib.rs` (add `LineageDag`, `DagNode`, `DagEdge`, `NodeKind`, `lineage_dag`).
- Create: `src/ui/tests/lineage_dag.rs`.
- Modify: `src/ui/BUCK` (add `lineage-dag` rust_test).

**Interfaces:**
- Produces: `pub enum NodeKind { Upstream, Current, Downstream }`, `pub struct DagNode { pub id: String, pub label: String, pub kind: NodeKind, pub column: usize }`, `pub struct DagEdge { pub from: String, pub to: String }`, `pub struct LineageDag { pub nodes: Vec<DagNode>, pub edges: Vec<DagEdge> }`, `pub fn lineage_dag(current: (&str,&str), upstream: &[(String,String)], downstream: &[(String,String)]) -> LineageDag`. Node `id` is `"{namespace}.{name}"`; `column` is 0 (upstream), 1 (current), 2 (downstream) for layout. Edges: each upstream → current; current → each downstream. De-duplicates a dataset that appears in both closures (keep the first/closest side); the current node is never also an up/down node.

- [ ] **Step 1: Write the failing test**

Create `src/ui/tests/lineage_dag.rs`:

```rust
use loom_ui_core::{NodeKind, lineage_dag};

#[test]
fn builds_three_columns_with_edges_through_current() {
    let up = vec![("main".to_string(), "raw".to_string())];
    let down = vec![("main".to_string(), "report".to_string())];
    let dag = lineage_dag(("main", "txns"), &up, &down);

    // 3 nodes: raw (col 0), txns (col 1, current), report (col 2).
    assert_eq!(dag.nodes.len(), 3);
    let current = dag.nodes.iter().find(|n| n.kind == NodeKind::Current).unwrap();
    assert_eq!(current.id, "main.txns");
    assert_eq!(current.column, 1);
    assert_eq!(dag.nodes.iter().find(|n| n.id == "main.raw").unwrap().column, 0);
    assert_eq!(dag.nodes.iter().find(|n| n.id == "main.report").unwrap().column, 2);

    // Edges: raw -> txns, txns -> report.
    assert!(dag.edges.iter().any(|e| e.from == "main.raw" && e.to == "main.txns"));
    assert!(dag.edges.iter().any(|e| e.from == "main.txns" && e.to == "main.report"));
    assert_eq!(dag.edges.len(), 2);
}

#[test]
fn current_wins_over_a_self_reference_in_a_closure() {
    // A closure that echoes the current dataset must not create a duplicate node.
    let up = vec![("main".to_string(), "txns".to_string())];
    let dag = lineage_dag(("main", "txns"), &up, &[]);
    assert_eq!(dag.nodes.len(), 1);
    assert_eq!(dag.nodes[0].kind, NodeKind::Current);
    assert!(dag.edges.is_empty());
}

#[test]
fn empty_closures_yield_only_the_current_node() {
    let dag = lineage_dag(("w", "z"), &[], &[]);
    assert_eq!(dag.nodes.len(), 1);
    assert_eq!(dag.nodes[0].id, "w.z");
    assert!(dag.edges.is_empty());
}
```

Add the `lineage-dag` rust_test target to `src/ui/BUCK` (dep `:ui-core`).

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test //src/ui:lineage-dag 2>&1 | tee /tmp/t10.log; grep -E "error\[|FAIL|Tests finished" /tmp/t10.log`
Expected: FAIL to compile.

- [ ] **Step 3: Implement `lineage_dag`**

Append to `src/ui/src/lib.rs`:

```rust
/// Where a node sits relative to the current dataset in the mini-DAG.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Upstream,
    Current,
    Downstream,
}

/// A node in the lineage mini-DAG. `column` is 0 (upstream) / 1 (current) / 2 (downstream).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagNode {
    pub id: String,
    pub label: String,
    pub kind: NodeKind,
    pub column: usize,
}

/// A directed edge (producer → consumer) between two node ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagEdge {
    pub from: String,
    pub to: String,
}

/// The assembled lineage mini-DAG.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LineageDag {
    pub nodes: Vec<DagNode>,
    pub edges: Vec<DagEdge>,
}

fn dataset_id(ns: &str, name: &str) -> String {
    format!("{ns}.{name}")
}

/// Build the three-column mini-DAG for `current` from its upstream/downstream closures.
/// Datasets equal to `current` are dropped from the closures (the current node is unique);
/// edges run producer → current → consumer.
#[must_use]
pub fn lineage_dag(
    current: (&str, &str),
    upstream: &[(String, String)],
    downstream: &[(String, String)],
) -> LineageDag {
    let (cur_ns, cur_name) = current;
    let cur_id = dataset_id(cur_ns, cur_name);
    let mut nodes = vec![DagNode {
        id: cur_id.clone(),
        label: cur_name.to_string(),
        kind: NodeKind::Current,
        column: 1,
    }];
    let mut edges = Vec::new();

    for (ns, name) in upstream {
        let id = dataset_id(ns, name);
        if id == cur_id {
            continue;
        }
        nodes.push(DagNode { id: id.clone(), label: name.clone(), kind: NodeKind::Upstream, column: 0 });
        edges.push(DagEdge { from: id, to: cur_id.clone() });
    }
    for (ns, name) in downstream {
        let id = dataset_id(ns, name);
        if id == cur_id {
            continue;
        }
        nodes.push(DagNode { id: id.clone(), label: name.clone(), kind: NodeKind::Downstream, column: 2 });
        edges.push(DagEdge { from: cur_id.clone(), to: id });
    }
    LineageDag { nodes, edges }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `buck2 test //src/ui:lineage-dag 2>&1 | tee /tmp/t10.log; grep -E "FAIL|Tests finished" /tmp/t10.log`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/ui/src/lib.rs src/ui/tests/lineage_dag.rs src/ui/BUCK
git commit -m "feat(ui): lineage_dag builder (3-column nodes+edges from closures)"
```

---

### Task 11: Lineage tab SVG render + `LineageFullStub` + net fetch

**Files:**
- Modify: `src/ui/src/net.rs` (add `fetch_lineage`).
- Create: `src/ui/src/components/lineage.rs` (the SVG mini-DAG component; glob'd into `:ui-components`).
- Modify: `src/ui/src/components/mod.rs` (export `LineageDagView`).
- Modify: `src/ui/src/surfaces/catalog.rs` (Lineage tab renders `LineageDagView` + an "Open full view ↗" button → `LineageFullStub`; wire `fetch_lineage` for upstream+downstream on tab activation).
- Modify: `src/ui/src/components/stub.rs` (add `LineageFullStub`).

**Interfaces:**
- Consumes: `LineageDag`, `lineage_dag`, `net::fetch_lineage`.
- Produces: `net::fetch_lineage(base, token, namespace, name, dir: &str /* "upstream" | "downstream" */) -> Result<Vec<(String,String)>, FetchError>` (decodes `{datasets:[{namespace,name}]}` → `(namespace,name)` pairs). `LineageDagView { dag: LineageDag }` renders nodes in 3 columns on the dotted-grid canvas with the current node accent-glowed and straight SVG connectors. `LineageFullStub` is the deferred full-canvas placeholder.

> **Namespace note:** the Catalog dataset's `schema` is used as the lineage `namespace` (`fetch_lineage(base, token, row.schema, row.name, …)`). If a deployment's lineage namespace differs from the Iceberg schema, the closures come back empty — acceptable for the MVP; record `fut-ui-lineage-namespace-reconcile` in `docs/FUTURE.md` in the closing docs update.

- [ ] **Step 1: Add `fetch_lineage`**

In `src/ui/src/net.rs`:

```rust
/// GET /lineage/datasets/{ns}/{name}/{dir} → the closure's (namespace,name) pairs.
/// `dir` is "upstream" or "downstream".
pub async fn fetch_lineage(
    base: &str,
    token: &str,
    namespace: &str,
    name: &str,
    dir: &str,
) -> Result<Vec<(String, String)>, FetchError> {
    let path = format!("/lineage/datasets/{namespace}/{name}/{dir}");
    let resp = Request::get(&url(base, &path))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 200 {
        return Err(fetch_status_err(resp.status()));
    }
    let body: serde_json::Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(body
        .get("datasets")
        .and_then(|d| d.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|d| {
                    let ns = d.get("namespace")?.as_str()?.to_string();
                    let nm = d.get("name")?.as_str()?.to_string();
                    Some((ns, nm))
                })
                .collect()
        })
        .unwrap_or_default())
}
```

- [ ] **Step 2: Implement `LineageDagView`**

Create `src/ui/src/components/lineage.rs`. Render a positioned layout: group nodes by `column` (0/1/2), lay each column out vertically, draw the dotted-grid background (`radial-gradient(#1b222b 1px, transparent 1px) 20px 20px` per tokens), draw straight SVG `<line>`s for each edge (blue for edges touching the current node, `#2d3640` otherwise), and box each node (current node gets `box-shadow: 0 0 0 4px rgba(59,130,246,.16)` and accent border). Props: `#[derive(Properties, PartialEq)] pub struct LineageDagViewProps { pub dag: LineageDag }`. A simple, robust layout: a CSS grid of 3 columns; nodes stacked in each; SVG overlay sized to the container with computed y-offsets. Keep it presentational — no fetching.

- [ ] **Step 3: Add `LineageFullStub`**

In `src/ui/src/components/stub.rs`, add a `LineageFullStub` styled_component: the same coming-soon treatment with copy "Full-canvas lineage is coming soon." Export it from `mod.rs`.

- [ ] **Step 4: Wire the Lineage tab**

In `src/ui/src/surfaces/catalog.rs`, replace the Lineage tab placeholder: when the Lineage tab activates for the selected dataset, `Workspace` fetches upstream + downstream via `fetch_lineage`, builds `lineage_dag((schema, name), &up, &down)`, and renders `<LineageDagView dag={dag} />` plus a caption `format!("{} upstream · {} downstream", up.len(), down.len())` and an "Open full view ↗" `Button` that toggles a `show_full_lineage` state rendering `<LineageFullStub />` in place of the DAG.

- [ ] **Step 5: Build + clippy**

Run: `buck2 build //src/ui:app 2>&1 | tail -8 && buck2 build '//src/ui:app[clippy.txt]' 2>&1 | tail -3 && buck2 build //src/ui:gallery-bundle 2>&1 | tail -3`
Expected: all build; clippy empty. Optionally add a `LineageDagView` demo to the gallery with a canned `LineageDag`.

- [ ] **Step 6: Visual check**

Gallery (with a canned DAG) and/or live backend: the Lineage tab shows the 3-column DAG, current node glowing, connectors drawn; "Open full view ↗" swaps in the stub.

- [ ] **Step 7: Commit**

```bash
git add src/ui/src/net.rs src/ui/src/components/lineage.rs src/ui/src/components/stub.rs \
        src/ui/src/components/mod.rs src/ui/src/surfaces/catalog.rs
git commit -m "feat(ui): Catalog Lineage tab — SVG mini-DAG + full-view stub"
```

---

## Final verification (after all tasks)

- [ ] **UI test sweep:** `buck2 test //src/ui/... 2>&1 | tee /tmp/ui.log; grep -E "FAIL|Tests finished" /tmp/ui.log` — all green (surface, type-detail, catalog, lineage-dag, plus the pre-existing logic/tokens/objects).
- [ ] **query-api sweep:** `buck2 test //src/services/query-api/... 2>&1 | tee /tmp/qa.log; grep -E "FAIL|Tests finished" /tmp/qa.log` — all green.
- [ ] **Clippy:** `bash tools/clippy-all.sh 2>&1 | tail -20` — clean for the touched crates.
- [ ] **prek (markdown/format hooks):** `buck2 run //tools:prek -- run --all-files 2>&1 | tail -20` — commit any hook fixes.
- [ ] **Docs update:** run the `loom-docs-update` skill — record deferrals as `docs/FUTURE.md` items: `fut-ui-dataset-preview-acl` (coarse preview auth), `fut-ui-lineage-namespace-reconcile` (schema-vs-namespace), `fut-ui-full-canvas-lineage` (wireframe 2a), `fut-ui-dataset-runs-history` (History tab route), `fut-ui-catalog-routing`, `fut-ui-catalog-row-counts`, `fut-ui-server-side-catalog-sort`.

---

## Notes for the implementer

- **Slot state lives in `Workspace`.** Both live surfaces (Ontology, Catalog) lift their selected-row/tab/detail state into `Workspace` (`main.rs`) and pass presentational list/drawer components into the `Shell` slots. This keeps `Shell` pure chrome and avoids callback-passing gymnastics. Per-surface state is independent (a `struct` per surface, or separate `use_state`s gated by the active surface).
- **`loom_ui_core` stays lint-clean** — no `unwrap`/`expect`/`panic`/indexing. All parsers are total (missing/absent → default). This is why they're the test seam.
- **Components can't be unit-tested** (no DOM in buck2). Their "test" is `buck2 build //src/ui:app` compiling + the gallery eye-check. Do not add inline `#[test]` to any component (`no-inline-tests` hook fails the build).
- **Editions:** copy the `edition` field from the sibling test targets in `src/ui/BUCK` when adding new `rust_test`s — don't guess.
- **If `internal_error`/error-mapping in Task 2 doesn't accept `ServingError`,** copy whatever the neighbouring serving-fault handlers in `http.rs` use (`query_error_response`, `cp_read_error`, or a `map_err` to string) — match the file's existing pattern rather than inventing one.
