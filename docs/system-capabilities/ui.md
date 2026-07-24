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
as `IMarkerData` under the `loom` marker owner on every model change. Deferred to
a follow-up: client-side *column* diagnostics (need alias/scope resolution) and
the server-side EXPLAIN validate endpoint, which #620 names as the north star.
