# Object-explorer UI (slice 1b) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the login app's post-login placeholder with a working object explorer — type sidebar → paginated object table ("Load more") → detail drawer — on the `loom_ui_components` primitives, driven by the live `/ontology/types` + paginated `/objects/{type}` endpoints.

**Architecture:** Pure response-parsing logic lands in the lint-clean `loom_ui_core` lib (`rust_test`-covered). `net.rs` gains Bearer-auth GET helpers. A new `src/explorer.rs` in the `app` crate composes the primitives into the `Explorer` component, which becomes the authenticated view. `//src/ui:app` gains a `//src/ui:ui-components` dep.

**Tech Stack:** Rust 2024, Yew 0.21, gloo-net, serde_json, wasm-bindgen/web-sys, buck2. All already deps of `:app` except `:ui-components` (first-party, added).

## Global Constraints

- Tests are `rust_test` integration targets only — never inline `#[cfg(test)]` (`no-inline-tests` hook). Pure-logic tests in `src/ui/tests/*.rs` via `load("//src:loom_test.bzl", "rust_test")`.
- `loom_ui_core` stays **lint-clean** under strict clippy (pedantic + restriction) — NO crate-level allow; use local `#[expect(lint, reason=…)]` if needed. `explorer.rs`/`main.rs` `html!` is covered by the crate-level allow already on `app` (`main.rs:1-8`).
- Wasm: `:app` stays `default_target_platform = "//platforms:wasm"`.
- **Don't pipe `buck2 test` through `tail`** — redirect to a file and grep.
- IGNORE rust-analyzer/LSP diagnostics — this repo has no in-session rust-project, so LSP type/`None`-snake_case/unlinked-file diagnostics are false positives. Trust buck build/clippy/test only.
- No new third-party dep is expected. If one seems needed, STOP and report (it triggers the reindeer/buckify cycle).
- The login flow, `session` storage, and `config.js` API base are unchanged.
- **`loom_ui_components` primitive APIs** (exact — verified on main):
  - `TopNav { items: Vec<NavItem>, onselect: Callback<AttrValue> (prop_or_default), search: Html (prop_or_default), avatar: AttrValue (prop_or_default) }`; `NavItem { label: AttrValue, active: bool }`.
  - `DataTable<R> { columns: Vec<Column>, rows: Vec<R>, selected: Option<usize> (prop_or_default), onrow: Callback<usize> (prop_or_default) }` where `R: PartialEq + Clone + TableRow + 'static`; `Column { label: AttrValue, align: Align }`; `trait TableRow { fn cells(&self) -> Vec<Html>; }` (one `Html` per `Column`, in order).
  - `Panel { title: Option<AttrValue>, children: Children }`.
  - `Tabs { tabs: Vec<TabItem>, active: AttrValue, onselect: Callback<AttrValue> (prop_or_default) }`; `TabItem { id: AttrValue, label: AttrValue }`.
  - `Button { variant: ButtonVariant, disabled: bool, onclick: Callback<MouseEvent>, children }`.
  - `GlobalStyles` — no props; render once at the root of the authenticated view (the login view stays unstyled; the explorer needs the `:root` tokens).
  - `Align` (Start/End) and `ButtonVariant` come from `loom_ui_core`.

---

### Task 1: Pure response-parsing logic in `loom_ui_core` (TDD)

**Files:**
- Modify: `src/ui/src/lib.rs`
- Create: `src/ui/tests/objects.rs`
- Modify: `src/ui/BUCK` (add the `objects` rust_test target, mirroring `:tokens`)

**Interfaces produced** (consumed by Tasks 2-3):
- `struct ObjectsPage { pub rows: Vec<serde_json::Map<String, serde_json::Value>>, pub next: Option<String> }` (derive `Debug, Clone, PartialEq`)
- `fn parse_objects_page(body: &serde_json::Value) -> ObjectsPage` — reads `{"objects":[…], "next": <str>|null}`; a missing/!array `objects` → empty rows; non-object array members are skipped; missing/null `next` → `None`.
- `fn columns_from_objects(rows: &[serde_json::Map<String, serde_json::Value>]) -> Vec<String>` — union of keys in first-seen order across the rows.
- `fn cell_to_string(v: &serde_json::Value) -> String` — `Null → ""`, `String → the string (unquoted)`, `Bool/Number → its display`, `Array/Object → compact `serde_json::to_string``.

- [ ] **Step 1: Write the failing test**

Create `src/ui/tests/objects.rs`:

```rust
use loom_ui_core::{cell_to_string, columns_from_objects, parse_objects_page};
use serde_json::json;

#[test]
fn parses_envelope_with_next() {
    let body = json!({ "objects": [ {"id": 1, "name": "a"}, {"id": 2, "name": "b"} ], "next": "cur42" });
    let page = parse_objects_page(&body);
    assert_eq!(page.rows.len(), 2);
    assert_eq!(page.next.as_deref(), Some("cur42"));
    assert_eq!(page.rows[0].get("name").unwrap(), &json!("a"));
}

#[test]
fn parses_last_page_null_next() {
    let body = json!({ "objects": [ {"id": 1} ], "next": serde_json::Value::Null });
    assert_eq!(parse_objects_page(&body).next, None);
}

#[test]
fn parses_missing_fields_gracefully() {
    assert_eq!(parse_objects_page(&json!({})).rows.len(), 0);
    assert_eq!(parse_objects_page(&json!({})).next, None);
    // non-object members skipped, not panicking
    let p = parse_objects_page(&json!({ "objects": [ 7, {"id": 1} ] }));
    assert_eq!(p.rows.len(), 1);
}

#[test]
fn columns_are_key_union_in_first_seen_order() {
    let rows = vec![
        serde_json::from_value(json!({"id": 1, "name": "a"})).unwrap(),
        serde_json::from_value(json!({"id": 2, "status": "ok"})).unwrap(),
    ];
    assert_eq!(columns_from_objects(&rows), vec!["id", "name", "status"]);
}

#[test]
fn cells_render_by_kind() {
    assert_eq!(cell_to_string(&json!(null)), "");
    assert_eq!(cell_to_string(&json!("hi")), "hi");
    assert_eq!(cell_to_string(&json!(42)), "42");
    assert_eq!(cell_to_string(&json!(true)), "true");
    assert_eq!(cell_to_string(&json!([1, 2])), "[1,2]");
}
```

Add to `src/ui/BUCK` after the `:tokens` target:

```python
rust_test(
    name = "objects",
    crate = "objects",
    srcs = ["tests/objects.rs"],
    crate_root = "tests/objects.rs",
    edition = "2024",
    deps = ["//third-party:serde_json", ":ui-core"],
)
```

- [ ] **Step 2: Run — verify it fails to build**

Run: `buck2 test //src/ui:objects > /tmp/t1.log 2>&1; grep -E "error|FAIL|Tests finished" /tmp/t1.log`
Expected: build failure — the functions/type don't exist, and `loom_ui_core` may need `serde_json` as a dep (it already is per `:app`; if `:ui-core` lacks it, add `//third-party:serde_json` to the `:ui-core` rust_library deps in BUCK).

- [ ] **Step 3: Implement in `lib.rs`**

Append to `src/ui/src/lib.rs` (uses `serde_json`; add the dep to `:ui-core` if the build says it's missing):

```rust
use serde_json::{Map, Value};

/// One page of a governed object read: the decoded rows plus the forward cursor
/// (`None` = last page). Mirrors the `{ "objects": [...], "next": ... }` wire shape.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectsPage {
    pub rows: Vec<Map<String, Value>>,
    pub next: Option<String>,
}

/// Parse the `GET /objects/{type}` envelope. Total: missing/!array `objects` → no
/// rows; non-object members are skipped; missing/null `next` → `None`.
#[must_use]
pub fn parse_objects_page(body: &Value) -> ObjectsPage {
    let rows = body
        .get("objects")
        .and_then(Value::as_array)
        .map(|arr| arr.iter().filter_map(|v| v.as_object().cloned()).collect())
        .unwrap_or_default();
    let next = body.get("next").and_then(Value::as_str).map(ToOwned::to_owned);
    ObjectsPage { rows, next }
}

/// The union of object keys across `rows`, in first-seen order — stable table columns.
#[must_use]
pub fn columns_from_objects(rows: &[Map<String, Value>]) -> Vec<String> {
    let mut cols = Vec::new();
    for row in rows {
        for k in row.keys() {
            if !cols.iter().any(|c| c == k) {
                cols.push(k.clone());
            }
        }
    }
    cols
}

/// Render a JSON value for a table cell / drawer field. Total, display-only.
#[must_use]
pub fn cell_to_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Bool(_) | Value::Number(_) => v.to_string(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(v).unwrap_or_default(),
    }
}
```

Note: `serde_json::to_string(...).unwrap_or_default()` — if `unwrap_or_default` still trips a restriction lint, it shouldn't (it's not `unwrap`); if any lint fires, add a local `#[expect(..., reason=...)]`.

- [ ] **Step 4: Run — verify pass**

Run: `buck2 test //src/ui:objects > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: `Pass 5`.

- [ ] **Step 5: Clippy + commit**

```bash
tools/clippy-all.sh 2>&1 | grep -iE "ui-core|error" | tail   # clean
git add src/ui/src/lib.rs src/ui/tests/objects.rs src/ui/BUCK
git commit -m "feat(ui): object-read response parsing in loom_ui_core"
```

---

### Task 2: `net.rs` fetch helpers + `Explorer` component + wiring

Fetch helpers and the component ship together so the helpers are used immediately (no dead-code interim). This is the UI task; only the compile gate + manual render verify it (no DOM in buck2).

**Files:**
- Modify: `src/ui/src/net.rs` (add `fetch_types`, `fetch_page`, `FetchError`)
- Create: `src/ui/src/explorer.rs` (the `Explorer` component + `ObjectRow`)
- Modify: `src/ui/src/main.rs` (render `<Explorer …/>` in the authenticated branch; `mod explorer;`)
- Modify: `src/ui/BUCK` (`:app` gains `src/explorer.rs` in `srcs` and `:ui-components` in `deps`)

**Interfaces:**
- Consumes: Task 1's `ObjectsPage`/`parse_objects_page`/`columns_from_objects`/`cell_to_string`; the `loom_ui_components` primitives (see Global Constraints for exact APIs); the existing `net::api_base()`, `session::clear()`.
- Produces:
  - `net::FetchError` enum: `Unauthorized`, `Network`, `Server(u16)` (a `Display` for messages).
  - `net::fetch_types(base: &str, token: &str) -> Result<Vec<String>, FetchError>` — `GET /ontology/types`, decode `{"types":[…]}`.
  - `net::fetch_page(base: &str, token: &str, type_name: &str, cursor: Option<&str>, limit: u32) -> Result<ObjectsPage, FetchError>` — `GET /objects/{type}?limit=&cursor=`, decode via `parse_objects_page`. Build the query string with `limit` always and `cursor` only when `Some`.
  - `Explorer` component with props `ExplorerProps { token: AttrValue, on_logout: Callback<()> }`.

- [ ] **Step 1: `net.rs` fetch helpers**

Add to `src/ui/src/net.rs` (mirror the existing `logout` Bearer pattern at `net.rs:52-58`; map non-200: 401 → `Unauthorized`, else `Server(status)`; transport/decode → `Network`):

```rust
use loom_ui_core::{ObjectsPage, parse_objects_page, url};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchError { Unauthorized, Network, Server(u16) }

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthorized => write!(f, "your session has expired — please sign in again"),
            Self::Server(c) => write!(f, "server error ({c})"),
            Self::Network => write!(f, "could not reach the server"),
        }
    }
}

fn fetch_status_err(status: u16) -> FetchError {
    if status == 401 { FetchError::Unauthorized } else { FetchError::Server(status) }
}

pub async fn fetch_types(base: &str, token: &str) -> Result<Vec<String>, FetchError> {
    let resp = Request::get(&url(base, "/ontology/types"))
        .header("Authorization", &format!("Bearer {token}"))
        .send().await.map_err(|_| FetchError::Network)?;
    if resp.status() != 200 { return Err(fetch_status_err(resp.status())); }
    let body: serde_json::Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(body.get("types").and_then(|t| t.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str().map(ToOwned::to_owned)).collect())
        .unwrap_or_default())
}

pub async fn fetch_page(base: &str, token: &str, type_name: &str, cursor: Option<&str>, limit: u32)
    -> Result<ObjectsPage, FetchError>
{
    let mut path = format!("/objects/{type_name}?limit={limit}");
    if let Some(c) = cursor {
        // percent-encode the cursor value; a helper or `js_sys::encode_uri_component` is fine.
        path.push_str(&format!("&cursor={}", encode_cursor(c)));
    }
    let resp = Request::get(&url(base, &path))
        .header("Authorization", &format!("Bearer {token}"))
        .send().await.map_err(|_| FetchError::Network)?;
    if resp.status() != 200 { return Err(fetch_status_err(resp.status())); }
    let body: serde_json::Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(parse_objects_page(&body))
}
```

Implement `encode_cursor` via `js_sys::encode_uri_component(c).into()` (or an equivalent) so a cursor with URL-special chars round-trips. If `serde_json` isn't already a `net.rs`-visible dep it is (it's an `:app` dep).

- [ ] **Step 2: `Explorer` component**

Create `src/ui/src/explorer.rs`. Structure (fill in the `html!`):

```rust
use loom_ui_components::{
    Button, Column, DataTable, GlobalStyles, NavItem, Panel, TabItem, Tabs, TableRow, TopNav,
};
use loom_ui_core::{Align, ButtonVariant, cell_to_string, columns_from_objects};
use serde_json::{Map, Value};
use yew::prelude::*;

#[derive(Clone, PartialEq)]
struct ObjectRow { cells: Vec<String> }   // one string per column, in `columns` order

impl TableRow for ObjectRow {
    fn cells(&self) -> Vec<Html> {
        self.cells.iter().map(|c| html! { { c.clone() } }).collect()
    }
}

fn to_rows(objs: &[Map<String, Value>], columns: &[String]) -> Vec<ObjectRow> {
    objs.iter().map(|o| ObjectRow {
        cells: columns.iter().map(|c| o.get(c).map(cell_to_string).unwrap_or_default()).collect(),
    }).collect()
}

#[derive(Properties, PartialEq)]
pub struct ExplorerProps {
    pub token: AttrValue,
    pub on_logout: Callback<()>,
}

#[function_component(Explorer)]
pub fn explorer(props: &ExplorerProps) -> Html { /* see state + effects below */ }
```

State (`use_state`): `types: Vec<String>`, `selected_type: Option<String>`, `objs: Vec<Map<String,Value>>` (raw, accumulated), `columns: Vec<String>`, `next: Option<String>`, `selected_row: Option<usize>`, `status: Status` where `enum Status { Idle, Loading, Error(String) }`.

Effects/handlers:
- **On mount** (`use_effect_with((), …)`): `spawn_local` `fetch_types(&api_base(), &token)`; on `Ok` set `types`; on `Err(Unauthorized)` call `on_logout.emit(())`; on other `Err` set `status = Error(e.to_string())`.
- **On type select** (a sidebar item's `onclick` sets `selected_type`; a `use_effect_with(selected_type, …)` resets `objs/columns/next/selected_row`, sets `Loading`, and fetches page 1 via `fetch_page(base, token, ty, None, 50)`). On `Ok(page)`: `columns = columns_from_objects(&page.rows)`, `objs = page.rows`, `next = page.next`, `status = Idle`. `Err` handling as above (401 → `on_logout`).
- **Load more** (`Button` shown only when `next.is_some()`): `onclick` → `spawn_local` `fetch_page(base, token, ty, next.as_deref(), 50)`; on `Ok`: extend `objs`, recompute `columns = columns_from_objects(&objs)` (a later page may add keys), set `next`.
- **Row click**: `DataTable`'s `onrow: Callback<usize>` → `selected_row.set(Some(i))`.
- **Log out**: a `TopNav` action / `Button` → `on_logout.emit(())`.

Render:
- `<GlobalStyles />` once, then a `TopNav` (avatar e.g. first letter of nothing-known → use `"·"` or omit; a "Log out" via `Button` in the `search`/actions slot is acceptable — TopNav has no dedicated action slot, so render the Button beside it or pass it via `search`).
- A flex row: sidebar (`types` list, highlight `selected_type`), the table area, and the drawer.
- Table area: if `status == Loading` show "Loading…"; if `Error(m)` show `m`; if `selected_type` set and `objs` empty (and Idle) show "No objects"; else `<DataTable<ObjectRow> columns={to_columns(&columns)} rows={to_rows(&objs,&columns)} selected={*selected_row} onrow={…} />` followed by the Load-more `Button` when `next.is_some()`. `to_columns` maps each name → `Column { label: name.into(), align: Align::Start }`.
- Drawer: when `selected_row` is `Some(i)`, a `<Panel title={selected_type}>` containing `<Tabs tabs={vec![TabItem{id:"object".into(),label:"Object".into()}]} active="object" />` and the fields of `objs[i]` as key/value rows (`cell_to_string` each value).

- [ ] **Step 3: Wire into `main.rs` + BUCK**

In `src/ui/src/main.rs`: add `mod explorer;`; replace the authenticated placeholder block (`main.rs:33-39`, the `<main><h1>loom</h1><p>You are logged in.</p>…`) with:
```rust
return html! { <Explorer token={(*token).clone().unwrap_or_default()} on_logout={on_logout.clone()} /> };
```
where `on_logout` is the existing callback (it already does best-effort `net::logout` + `session::clear()` + `token.set(None)`) — adapt its type to `Callback<()>` (it currently takes `_`; a `Callback<()>` works). Import `use explorer::Explorer;`.

In `src/ui/BUCK`, the `:app` target: add `"src/explorer.rs"` to `srcs` and `":ui-components"` to `deps`.

- [ ] **Step 4: Build gate + clippy**

```bash
buck2 build //src/ui:bundle > /tmp/b2.log 2>&1; tail -5 /tmp/b2.log   # BUILD SUCCEEDED
tools/clippy-all.sh 2>&1 | grep -iE "\bapp\b|ui-core|error" | tail    # clean
```
Expected: the bundle builds (compiles the explorer + the primitive instantiations + fetch helpers). A type/prop mismatch against the primitive APIs fails here.

- [ ] **Step 5: Manual render note (no automated DOM test)**

Component rendering can't be `rust_test`'d (no DOM in buck2). Note in the report that visual/interactive verification is deferred to running the bundle against a live backend (`buck2 build //src/ui:bundle`, serve via query-api with `LOOM_UI_DIR`, or the all-in-one binary), and the browser e2e is `fut-ui-browser-test-fixture`. Do NOT claim you rendered it.

- [ ] **Step 6: Commit**

```bash
git add src/ui/src/net.rs src/ui/src/explorer.rs src/ui/src/main.rs src/ui/BUCK
git commit -m "feat(ui): object-explorer view (types sidebar, paginated table, drawer)"
```

---

### Task 3: Docs + register + final verification

**Files:**
- Modify: `src/ui/CLAUDE.md`, `docs/ROADMAP.md`, `docs/FUTURE.md`

- [ ] **Step 1: Document**

In `src/ui/CLAUDE.md`, add a short note: the login `app` now renders the `Explorer` (types sidebar → paginated object table with "Load more" → drawer) once authenticated, built on `loom_ui_components` and driven by `/ontology/types` + `/objects/{type}?limit=&cursor=`; response parsing is pure in `loom_ui_core` (`parse_objects_page`/`columns_from_objects`/`cell_to_string`, `rust_test`'d); rendering is verified against a live backend (no DOM in buck2).

- [ ] **Step 2: Register**

Promote via `loom-docs-update` (preferred) or by hand: `fut-object-explorer-ui` → `road-object-explorer-ui` (area `ui`, status `done`, `spec:2026-07-02-object-explorer-ui-design`). Record deferrals as FUTURE items: drawer Links/Schema tabs, object filtering/search, URL routing. Run `bash tools/docs.sh validate` → OK. (Reminder: no inline backticks in a register item's **title** line — they break the `{#id …}` tag parse.)

- [ ] **Step 3: Full sweep + fmt**

```bash
buck2 test //src/... > /tmp/tf.log 2>&1; grep -E "Tests finished|FAIL" /tmp/tf.log
tools/clippy-all.sh 2>&1 | tail
buck2 run //tools:rustfmt -- $(git ls-files 'src/ui/*.rs')
```
Expected: full sweep green, clippy clean, rustfmt no-op (or apply + amend). (If a fixture test fails to boot Postgres with `initdb failed`, that's leaked SysV shm from a long session — reclaim your own `nattch=0` segments via `ipcrm` and re-run; not a code failure.)

- [ ] **Step 4: Commit**

```bash
git add src/ui/CLAUDE.md docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(ui): document object-explorer; register slice 1b"
```

---

## Self-review notes (for the executor)

- **Spec coverage:** Task 1 = pure parsing (tested); Task 2 = fetch helpers + Explorer (sidebar/table/load-more/drawer/states) + wiring; Task 3 = docs/register/sweep. All spec sections map.
- **Type consistency:** `ObjectsPage`/`parse_objects_page`/`columns_from_objects`/`cell_to_string` defined in Task 1, consumed in Task 2. `ObjectRow: TableRow` aligns with `DataTable<R>`'s bound. `Explorer` props `{ token, on_logout }` match the `main.rs` call site. Primitive prop names (esp. `TopNav::onselect`, `DataTable::onrow`) match the Global Constraints list.
- **Fail-closed on auth:** a 401 from either fetch → `on_logout.emit(())` → session cleared, back to login. No silent failure — loading/empty/error all rendered.
- **Honest limits:** only Task 1 is `rust_test`-covered; the component is compile-gated + live-backend-verified (no DOM), consistent with the component-library slice and `fut-ui-browser-test-fixture`.
- **No new deps.** If the implementer reaches for one (e.g. a URL-encode crate), stop — `js_sys::encode_uri_component` covers the cursor.
