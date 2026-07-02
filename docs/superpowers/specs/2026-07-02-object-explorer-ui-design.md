# Object-explorer UI — slice 1b (design)

**Status:** approved for planning
**Date:** 2026-07-02
**Area:** ui
**Depends on (both merged to main):** the component library (`2026-07-01-ui-component-library-design`, PR #279 → `loom_ui_components`) and object-read pagination + `/ontology/types` (`2026-07-01-object-read-pagination-ontology-types-design`, PR #290).

## Goal

Turn loom's post-login UI from a placeholder into a working **object explorer**: a
three-pane / master-detail view — object-type sidebar → paginated object table →
detail drawer — built from the `loom_ui_components` primitives and driven by the live
governed endpoints. This is slice 1b of the composite-Foundry-screens arc (1a was the
backend read surface). Thin vertical slice: prove the end-to-end governed-read loop
against real data; layer richness (Links/Schema tabs, filtering, routing) in later
slices.

## Non-goals (this slice)

- Drawer Links & Schema tabs (only an "Object" tab showing the selected row's fields).
- Filtering/search, column sorting, URL routing (`yew-router`).
- Per-type identity/column metadata — **row selection is by index**, not identity.
- The dataset-catalog and lineage-DAG screens (separate arcs).
- Automated browser tests (deferred — `fut-ui-browser-test-fixture`).

## Architecture

The login `app` binary's **authenticated view** (`src/ui/src/main.rs:33-39`, currently
`<h1>loom</h1><p>You are logged in.</p><button>Log out</button>`) is replaced by an
`Explorer` component. `//src/ui:app` gains a dependency on `//src/ui:ui-components` and
a new `src/explorer.rs` module (keeping `main.rs` lean). The login flow, session
storage, and `App`'s token gate are unchanged — the explorer simply becomes what renders
once `token.is_some()`.

```
//src/ui
  :ui-core        loom_ui_core   — + pure response-parsing logic (rust_test'd)
  :ui-components  loom_ui_components — primitives (unchanged; consumed)
  :app            app — + src/explorer.rs, + net.rs fetch helpers; deps ▶ :ui-components
```

### Data layer

- **Pure, testable logic → `loom_ui_core`** (the only `rust_test`-able part; no web-sys):
  - `struct ObjectsPage { rows: Vec<serde_json::Map<String, Value>>, next: Option<String> }`
    (or `Vec<Value>` rows) + `parse_objects_page(&str | Value) -> Result<ObjectsPage, ...>`
    parsing the `{"objects":[…], "next": <str>|null}` envelope.
  - `columns_from_objects(rows: &[…]) -> Vec<String>` — the union of object keys in
    first-seen order, so the table has stable columns across a heterogeneous page.
  - `cell_to_string(&Value) -> String` — render a JSON scalar/array to a display string
    for a table cell / drawer value (total; objects/arrays get a compact form).
  These get a `tests/*.rs` `rust_test` with concrete cases (envelope with/without `next`,
  key-union ordering, null/number/string/array cell rendering).

- **`net.rs`** gains authenticated GET helpers (mirroring the existing `logout` Bearer
  pattern, `net.rs:52-58`):
  - `fetch_types(base, token) -> Result<Vec<String>, FetchError>` → `GET /ontology/types`,
    decoding `{"types":[…]}`.
  - `fetch_page(base, token, type_name, cursor: Option<&str>, limit: u32) ->
    Result<ObjectsPage, FetchError>` → `GET /objects/{type}?limit=&cursor=`, decoding via
    `parse_objects_page`.
  - A small `FetchError` enum: `Unauthorized` (401 → the UI clears the session and returns
    to login), `Network`, `Server(u16)`. (Reuses the `status_to_error`/`AuthError` idiom
    where it fits, or a sibling type — implementer's call, kept in `loom_ui_core` if pure.)

### Explorer component (`src/explorer.rs`, using `loom_ui_components`)

- **Shell:** `TopNav` (brand, avatar, a "Log out" `NavItem`/`Button` wired to the existing
  logout callback).
- **Sidebar:** the type list from `fetch_types` (fetched once via `use_effect_with` on
  mount); each type a clickable item; `use_state` `selected_type: Option<String>`.
- **Table:** `DataTable<ObjectRow>` for the selected type. State: `use_state`
  `rows: Vec<ObjectRow>` (accumulated), `next: Option<String>`, `columns: Vec<Column>`.
  Selecting a type resets and fetches page 1 (`use_effect_with(selected_type)`).
  A **"Load more"** `Button` (shown only when `next.is_some()`) fetches the next page and
  **appends**. `ObjectRow` implements `TableRow::cells()` → each column's value via
  `cell_to_string`; columns computed by `columns_from_objects` on the first page (extended
  if later pages add keys). Row click sets `selected_row: Option<usize>` (`DataTable`'s
  `selected`/`onrow`).
- **Drawer:** `Panel` + `Tabs` (single active "Object" tab) rendering `rows[selected_row]`'s
  fields as key/value rows. No extra fetch — the row is already in hand. Hidden when no
  row is selected.
- **States (all real):** *loading* (text/spinner while a fetch is in flight),
  *empty* (selected type returned zero rows), *error* (`Network`/`Server` → an inline
  message; `Unauthorized` → `session::clear()` + drop the token, returning to `Login`),
  and the *no-type-selected* initial state (prompt to pick a type).

## Error handling

Every fetch returns `Result<_, FetchError>`; the component renders loading/empty/error
explicitly (no silent failures). A 401 is the one that mutates app state (logout). JSON
decode failures map to `Network`/a parse error and surface as an inline error, never a
panic — `parse_objects_page` and `cell_to_string` are total.

## Testing

- **Pure logic** (`parse_objects_page`, `columns_from_objects`, `cell_to_string`) →
  `rust_test` in `loom_ui_core` with real assertions. This is the genuinely-covered part.
- **Component rendering** (`html!`) → **not** DOM-testable in buck2 (no browser). Verified
  by running the bundle against a live backend: `buck2 build //src/ui:bundle`, serve via a
  query-api with `LOOM_UI_DIR` set (or the planned all-in-one binary). The automated
  full-page browser harness stays deferred (`fut-ui-browser-test-fixture`).
- No new third-party dep is anticipated (gloo-net/serde_json/web-sys already deps).

## Global constraints

- Tests are `rust_test` targets only — never inline `#[cfg(test)]` (`no-inline-tests`
  hook). Pure-logic tests in `src/ui/tests/*.rs`.
- Strict clippy (pedantic + restriction): `loom_ui_core` additions stay lint-clean; the
  `html!` in `explorer.rs`/`main.rs` is covered by the crate-level allow already on `app`.
- Wasm: `:app` stays `default_target_platform = //platforms:wasm`.
- The login flow, `session` storage, and `config.js` runtime API base are unchanged.

## File structure

- **Modify** `src/ui/src/lib.rs` (`loom_ui_core`) — `ObjectsPage`, `parse_objects_page`,
  `columns_from_objects`, `cell_to_string` (+ `FetchError` if kept pure).
- **Create** `src/ui/tests/objects.rs` — `rust_test` for the parsing logic.
- **Modify** `src/ui/src/net.rs` — `fetch_types`, `fetch_page`.
- **Create** `src/ui/src/explorer.rs` — the `Explorer` component (+ `ObjectRow`).
- **Modify** `src/ui/src/main.rs` — replace the authenticated placeholder with `<Explorer …/>`.
- **Modify** `src/ui/BUCK` — add `:ui-components` to `:app` deps; add `src/explorer.rs` to
  `:app` srcs; add the `objects` `rust_test` target.

## Register updates

- `docs/ROADMAP.md`: promote `fut-object-explorer-ui` → `road-object-explorer-ui`
  (area `ui`, status `planned` then `done`), `spec:` this file.
- On completion, `loom-docs-update` closes it and records deferrals (Links/Schema tabs,
  filtering, routing) as FUTURE items.
