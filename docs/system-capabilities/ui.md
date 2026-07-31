# UI capabilities

loom's web UI is framed by its founding spec as an **experiment**, not a committed
product surface: the interesting work was teaching the buck2 build to cross-compile
Rust to `wasm32-unknown-unknown`, with the [Yew](https://yew.rs) app riding on top.
The experiment has since grown real substance — a design-system component library
and a working post-login object explorer over the live governed endpoints — but the
composite Foundry-style screens (dataset catalog, lineage DAG) remain separate,
un-started arcs, and the UI should still be read as an exploratory slice of the
platform rather than a finished front end.

_As of 4861433b._

## What the UI does today

The `app` binary (`src/ui/`) is a Yew 0.21 single-page app with a login flow and an
object explorer. Unauthenticated, it renders a login form that calls `POST
/auth/login` against the existing backend auth (bearer token stored in
`sessionStorage` under `loom_token`; the API base is resolved at runtime from
`config.js`, so one bundle works both served-by-query-api and detached/CORS).
Once authenticated, the `Explorer` (`src/ui/src/explorer.rs`) renders a three-pane,
master-detail object browser: a type sidebar fed by `GET /ontology/types`, a
paginated object `DataTable` with a "Load more" button appending the next
keyset-cursor page from `GET /objects/{type}?limit=&cursor=`, and a detail drawer
(single "Object" tab) showing the selected row's fields. A 401 from any fetch fails
closed to logout. Row selection is by index, not identity — a deliberate
thin-slice choice.

Underneath sits the component library, `loom_ui_components`
(`src/ui/src/components/`): a design-token layer (`GlobalStyles` injects `:root
{ --loom-* }` custom properties — a dark Foundry-style palette — so the theme is a
one-file swap) plus `stylist` CSS-in-Rust primitives: `Button`, `Input`
(Text/Password/Search), `Badge`, `StatusDot`, `Tabs`, `Panel`, a generic
`DataTable<R>` (callers `impl TableRow` for their own row type), and `TopNav`.
Components are controlled/stateless; interactive state lives in the caller. A
dev-only gallery bundle (`buck2 build //src/ui:gallery-bundle`, `src/gallery.rs` +
`gallery.html`) renders every primitive and its variants; it has no backend and
never ships in the login bundle.

The **Catalog surface** (`src/ui/src/surfaces/catalog.rs`) lists the mirror datasets
with a **controls bar** (#619): project **filter chips** (All + one per distinct
project) and a row of **Sort** key buttons (Name / Project / Updated / Rows) with a
direction toggle. These drive **server-side** sort/filter via
`GET /datasets?sort=&dir=&project=` (see [query-api.md](query-api.md)) rather than
reshaping the list client-side. The query string is built by a pure
`dataset_list_query` helper in `loom_ui_core` (`rust_test`'d alongside a
`distinct_projects` chip-option derivation); the load effect re-fetches whenever a
control changes, guarded by its own `FetchGeneration` counter so an out-of-order
arrival can't stale the list. The chip options are refreshed only from an
**unfiltered** load, so selecting a project never collapses them to the filtered
subset, and changing any control clears the drawer selection (row indices shift when
the list reorders). The sort control is rendered as buttons, not a `<select>` —
reading a `<select>` value would need `web_sys::HtmlSelectElement`, which this crate
does not enable.

The **Transforms admin surface** (`src/ui/src/surfaces/transforms.rs`) is the first
product consumer of the `SqlEditor` (#394): a list+drawer
surface over the `/admin/transforms` control-plane routes that lets an admin browse,
author, run, and delete both **physical** (SQL-over-tables) and **typed**
(ontology-vocabulary) transforms. The list is a `DataTable` of
name / kind / schedule / on-input-commit; selecting a row opens a drawer with a
**Definition** tab (a metadata block + a read-only `SqlEditor` + Edit/Run/Delete
buttons) and a **Runs** tab (the `GET /:name/runs` history as a state/trigger/timing
table). The editor form multi-selects inputs (checkboxes) and feeds the `SqlEditor`
an **input-scoped** `CompletionSchema` built only from the chosen inputs'
columns/properties (physical: per-table `GET /datasets/{schema}/{table}`; typed: the
selected ontology types' properties), remounting the editor on a schema-content key
so fresh completions take effect despite Monaco's mount-time prop capture. Admin
gating reuses the login session's bearer token and renders a `403` as a "requires
admin" empty-state (no separate admin login); the selection-keyed fetches are
**epoch-guarded** (three per-stream generation counters) so a slow prior-selection
fetch cannot stale the current view.

The drawer's **actions surface their failures** (#437). `TransformDrawer` carries an
`action_error` slot rendered beside its buttons, so a 400/404/network failure on **Run
saved** or **Delete** shows the rejection body — previously those arms swallowed
everything that wasn't a `401`, which made a failed delete look like a no-op. A `401`
still fails closed to logout, unchanged. The decision (logout / show this message /
clear the error and bump the runs epoch) is a **pure function** in `:ui-core`
(`run_action_effect` / `delete_action_effect` → `DrawerActionEffect`) that `main.rs`
routes through, so it is unit-testable natively despite the UI having no component
render harness (`#fut-ui-component-test-fixture`). The same change closes a related
edge: the runs-history effect is keyed on a **runs epoch** alongside
`(selection, tab)`, so a Run fired while the Runs tab is *already* active refetches
rather than just clearing the list. The nav `Pipelines` stub was renamed to
`Transforms`. Pure parse / form-to-request / completion-schema / display logic lives
in `loom_ui_core` (`rust_test`'d — `transforms-parse`/`-form`/`-schema`/`-display`);
the Yew view glue is browser-verified. The shared `Shell` also gained a **resizable,
persisted drawer** (left-edge drag handle, width clamp, localStorage) that every
surface inherits.

The Catalog drawer's **Lineage tab** now opens a real **full-canvas lineage view**
(#615). Its "Open full view ↗" button previously swapped in a `LineageFullStub`
placeholder; it now renders `LineageCanvasView` — the same producer → current →
consumer DAG as the compact mini-DAG, but at a generous full-page scale with a
header caption and both-axis scrolling. Both views share one source of geometry: the
pure, `rust_test`'d `loom_ui_core::lineage_layout` (`:lineage-layout`), which the
mini-DAG renders at `MINI` scale and the canvas at `CANVAS` scale — so the pixel
layout (column grouping, non-empty-column slotting, vertical centering, edge
endpoints, current-node accenting) is unit-tested once and never duplicated across
the two component files. No backend change: the view is driven by the
upstream/downstream closures already fetched for the drawer.

## How it's built, served, and tested

The crate lives inside `//src/...`, so CI and the strict pedantic/restriction
clippy gate cover it; `default_target_platform = //platforms:wasm` makes the
native sweep cross-compile it rather than fail native-building yew. The build
chain the experiment established: a wasm `rust-std` dist and toolchain variant, a
wasm cxx toolchain (rustc's bundled `rust-lld`), reindeer `[platform]` config so
`cfg(not(wasm32))` deps don't leak into the wasm graph, and a version-locked
`wasm-bindgen` CLI. `buck2 build //src/ui:bundle` produces `dist/{app.js,
app_bg.wasm, index.html}`; `buck2 run //src/ui:serve` serves it over HTTP (the
`--target web` glue `fetch()`es the wasm, so `file://` won't load it), and a
backend can serve the same bundle via `LOOM_UI_DIR`.

Testing is split by what can actually execute. Pure logic — the token enums and
`format_count` in `loom_ui_core`, and the explorer's response parsing
(`parse_objects_page`/`columns_from_objects`/`cell_to_string`) — has real
`rust_test` coverage (`src/ui/tests/`). Component rendering (`html!`) is not
`rust_test`-able (buck2's runner has no DOM) and is verified by eye in the
gallery. Full-page behaviour is covered by the **login e2e**
(`//src/ui/e2e:login`): a `fantoccini` (Rust WebDriver) test that boots the
composite — fresh DB on the shared `PgFixture`, `standalone::run` serving the
bundle — and drives a vendored headless Chromium (`//third-party/browser`, Chrome
for Testing) through render, bad-credentials error, and success into the
Explorer. That test now runs **hermetically on Remote Execution**: a custom RBE
container image (`tools/ci/rbe-browser/`, the flame-public base plus the
`chrome-headless-shell` runtime libs, published to public
`ghcr.io/weave-hand/loom-rbe-browser` and digest-pinned in `platforms/defs.bzl`)
gives the RE workers the browser's runtime libraries while the browser binary
stays vendored, so the login flow runs for real in CI instead of auto-skipping. A
publish-time smoke test gates the image on a real `chrome-headless-shell
--version`; locally the test auto-skips only where a browser can't start
(`LOOM_UI_E2E=1` turns that into a hard error).

## Key decisions

- **Inside the main sweep, not an experiments cell** — the UI pays the same CI,
  clippy, and no-inline-tests costs as every other crate; the `html!`/`css!`
  crates carry a reasoned crate-level lint allow because macro expansion isn't
  lint-clean.
- **Pure/testable split** — everything that can run without a DOM (`loom_ui_core`)
  is kept free of `yew`/`web-sys` and `rust_test`'d; rendering verification is
  honestly deferred to the gallery and the browser e2e rather than faked.
- **Runtime API base, sessionStorage token** — one wasm artifact serves both the
  same-origin and detached deploy topologies; tokens survive reload but not tab
  close.
- **Vendored browser binary, image-supplied libs** — the glibc-coupled library
  closure is deliberately not vendored as buck archives; the executor image (host
  locally, custom RBE image on RE) provides it. amd64-only for now.
- **Generic `DataTable<R>` via a `TableRow` trait** — rows map themselves to
  cells, sidestepping yew's `PartialEq`-props constraint on closures.
- **Generation-guarded selection-scoped fetches** — the Catalog drawer's lazy
  detail/preview/lineage `spawn_local` fetches are guarded by a single shared
  `FetchGeneration` counter (in `loom_ui_core`, `rust_test`'d) bumped on dataset
  selection change; each fetch captures the generation it was spawned under and
  commits its result only if the selection hasn't advanced — so an out-of-order
  network arrival from a superseded selection can't stale the drawer body. A
  non-401 fetch error on the Schema or Preview tab surfaces a small error line
  (styled with `--loom-danger`) that takes precedence over the "Loading…"
  placeholder — an error no longer leaves the tab hung on "Loading…" (Schema) or
  silently empty (Preview); the Lineage tab already degraded gracefully.
- **Catalog History tab** (#616) — the drawer's History tab is now real: it lists
  the runs that touched the selected dataset (newest-first, `event_type · time ·
  role · run-id`) from `GET /lineage/datasets/{ns}/{name}/runs`, replacing the
  honest "not available on this instance" stub. The pure `parse_dataset_runs` lives
  in `loom_ui_core` (`rust_test`'d); the fetch (reset-on-selection + lazy load when
  the tab is active, under the same `FetchGeneration` guard) is encapsulated in a
  `use_dataset_run_history` custom hook so the tab's state/effects stay out of the
  `Workspace` component. A denied/unknown dataset yields an empty list (seed-gated
  server-side); a non-401 error surfaces the same danger-styled line as the other
  tabs.

## Known gaps

- `#fut-object-explorer-drawer-tabs` — the drawer's Links and Schema tabs
  (link traversal, per-type property definitions).
- `#fut-object-explorer-filtering` — filtering and search over the object table.
- `#fut-object-explorer-routing` — URL routing and deep-linking (`yew-router`).
- `#fut-ui-sql-completion-polish` — unqualified SQL completion now **dedups**
  column names shared across input tables (a column present in two inputs is
  offered once; the first table's type wins and the table origin is dropped),
  so the Transforms editor no longer shows duplicate `Field` rows (#622). The
  Transforms editor still works *around* Monaco's mount-time prop capture with a
  schema-content remount key; the remaining multi-editor-provider and
  live-prop-swap polish is carved to a follow-up (both need the deferred
  component DOM-test harness to verify).

## SqlEditor diagnostics (#620)

The `SqlEditor` now squiggles two classes of problem client-side, with no backend
round trip: unknown tables in `FROM`/`JOIN` position (CTE-aware — a query's own
`WITH` names are never flagged — and conservative, skipping qualified/quoted/
aliased identifiers and plain column references, and emitting nothing when the
passed-in `CompletionSchema` is empty) and unbalanced parentheses. Both are pure
functions in `loom_ui_core::sql_diagnostics` (`rust_test`'d), published to Monaco
as `IMarkerData` under the `loom` marker owner on every model change. The north
star #620 named — the server-side EXPLAIN validate endpoint — has since landed
(#657, below), and it is what closed out client-side *column* diagnostics: those
were **dropped**, not deferred, because the engine now checks columns correctly
and a client-side alias/scope heuristic would only re-add false-positive risk.

## SQL query console (#621)

A sixth live surface, **Query** (accent `#e06c75`), is the second `SqlEditor`
consumer: a read-only "run this SQL, get rows back" console. Unlike the other
surfaces (whose state lives in `Workspace`), `QueryView` is self-contained — it
owns the SQL text, the last result, and the in-flight flag, so `Workspace` gains
only a one-line dispatch arm. A **Run** button POSTs the editor's SQL to the new
governed `POST /sql` endpoint via `net::run_sql` and renders the returned
`{columns, rows, truncated}` through the shared `DataTable` primitive (a
runtime-columned `StringRow`, so arbitrary result shapes render without a
per-query row type); a `401` fails closed to logout and a `400` surfaces the
engine's own plan message. Governance is entirely server-side (see the
`POST /sql` record in [query-api.md](query-api.md)) — the console sends SQL and
renders whatever the governed engine returns. The response parser
(`parse_query_result`, sharing `parse_columns_rows` with the dataset preview) is
pure and `rust_test`'d; the component itself is verified in the gallery/e2e like
every other surface. Deferred: feeding the live `/datasets` schema into the
editor's completion provider (today the console's editor takes an empty schema).

## Server-side SQL diagnostics in the console (#657)

The console's editor now also squiggles what the *engine* thinks, not only what
the client-side heuristics can guess. `SqlEditor` gained an optional `validate`
prop (`Callback<ValidateRequest>`): when set, the editor debounces 500 ms after
the last keystroke and emits a `ValidateRequest { sql, respond }`. The component
owns *when* to ask and *what to do with the answer*; the caller owns the
transport — the split exists because Yew callbacks are synchronous, so the
async fetch cannot live inside the editor. The initial value is validated
through that same debounce rather than immediately, so a mount that is typed
into straight away produces one request, not a mount request plus a typing one.
`QueryView` answers by calling `net::validate_sql` (`POST /sql/validate`, see
[query-api.md](query-api.md)) and parsing the body with the pure
`loom_ui_core::parse_server_diagnostics` — the server's typed positions when it
has them, and `anchor_diagnostic` when they are `null`, which is every *plan*
error. Anchoring resolves the identifiers the message names (the offending
field first, then each quoted span's last dotted segment) as whole words in the
*live* editor text, falls back to the whole first non-empty line, and drops the
diagnostic outright when there is no non-empty line to point at. Both functions
are `rust_test`'d (`//src/ui:anchor-diagnostic`); a malformed body degrades to
"no squiggles" rather than an error.

Four properties are load-bearing:

- **Two marker owners.** Client diagnostics publish under `loom`, server ones
  under `loom-server`. Monaco's `set_model_markers` replaces only the named
  owner's set, so the two engines publish independently and neither clears the
  other's squiggles (an empty server list clears the server's alone).
- **Stale responses are dropped.** The response echoes the `sql` that was
  validated; if the model's current text no longer matches it, the answer is
  discarded instead of published, so markers never land on text the user has
  already edited past.
- **Unmount drops both the armed debounce and any in-flight answer.** Cleanup
  cancels the pending `setTimeout` before the model is disposed, and takes the
  model out of the slot the response callback reads — so a request that cannot
  be cancelled becomes a no-op rather than a `get_value()` on a disposed model
  (which throws).
- **A failed validation is silent.** A transport blip must not shout at someone
  who is only typing, so the client-side squiggles simply remain and nothing is
  reported; a 401 still fails closed to logout like every other fetch on this
  surface.

An `SqlEditor` with no `validate` prop is unchanged — only the client-side
engine publishes markers — so the Transforms drawer editors and the gallery are
untouched and the Query console is the only opt-in.
