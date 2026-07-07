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
  network arrival from a superseded selection can't stale the drawer body.

## Known gaps

- `#fut-object-explorer-drawer-tabs` — the drawer's Links and Schema tabs
  (link traversal, per-type property definitions).
- `#fut-object-explorer-filtering` — filtering and search over the object table.
- `#fut-object-explorer-routing` — URL routing and deep-linking (`yew-router`).
