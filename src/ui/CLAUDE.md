# CLAUDE.md — `src/ui/` (Yew → WASM web UI)

A [Yew](https://yew.rs) app that cross-compiles to `wasm32-unknown-unknown` and
bundles to browser-loadable JS via wasm-bindgen.

Once authenticated, the `app` renders the **`Explorer`** (`src/explorer.rs`): a
three-pane object browser — type sidebar (`GET /ontology/types`) → paginated object
`DataTable` with a **"Load more"** button appending the next cursor page
(`GET /objects/{type}?limit=&cursor=`) → detail drawer (Object tab) — composed from
the `loom_ui_components` primitives. Response parsing is pure in `loom_ui_core`
(`parse_objects_page`/`columns_from_objects`/`cell_to_string`, `rust_test`'d); `net.rs`
holds the Bearer-auth `fetch_types`/`fetch_page` (a 401 fails closed to logout).
Rendering is verified against a live backend (no DOM in buck2 `rust_test`; browser e2e
deferred → `fut-ui-browser-test-fixture`). Spec/plan:
`docs/superpowers/{specs,plans}/2026-07-02-object-explorer-ui*`.

- `buck2 build //src/ui:bundle` → `dist/{app.js, app_bg.wasm, index.html}`
- `buck2 run //src/ui:serve` serves `dist/` over HTTP — the `--target web` glue
  `fetch()`es the wasm, so `file://` will **not** load it; it must be served over HTTP.
  (This serves the bundle *alone*, with no backend — for an end-to-end login against a
  real backend, the planned all-in-one binary `fut-embedded-postgres-all-in-one` (embedded
  PG + engine + query-api in one process, serving this bundle via `LOOM_UI_DIR`) is the
  intended local run target.)
- This crate lives **inside** `//src/...` (so CI + the strict pedantic/restriction
  clippy gate cover it) and survives the native `buck2 build //src/...` sweep via
  `default_target_platform = //platforms:wasm` — that makes the sweep cross-compile it
  rather than try (and fail) to native-build yew. The `html!` macro is not lint-clean
  under the strict gate, so `src/main.rs` carries a crate-level `#![allow(clippy::pedantic,
  clippy::restriction, reason = …)]` (`allow`, not `expect` — which group members fire
  depends on the macro expansion).

Spec/plan: `docs/superpowers/{specs,plans}/2026-06-30-yew-wasm-ui-experiment*`.

## Component library (`loom_ui_components`)

The design-system primitives live in a **separate wasm `rust_library`**,
`//src/ui:ui-components` (crate `loom_ui_components`, crate root
`src/components/mod.rs`, glob'd `src/components/**/*.rs` — new component files need
no BUCK change). Styling is **`stylist`** (CSS-in-Rust, features `["yew","parser"]`
— `parser` is required for `&str`→`StyleSource`, e.g. `<Global css={r#"…"#} />`).
Like `:app`, the crate carries a crate-level `#![allow(clippy::pedantic,
clippy::restriction)]` because `html!`/`css!` expansion isn't lint-clean.

- **Token layer:** `GlobalStyles` (a `stylist` `<Global>`) injects `:root { --loom-* }`
  custom properties (dark Foundry palette) + base body/font once at the app root.
  Every component's `css!` references `var(--loom-*)`, so the theme is a one-file swap.
- **Pure/testable split:** the token *enums* (`ButtonVariant`, `BadgeTone`, `Status`,
  `Align`) and `format_count` live in the lint-clean `loom_ui_core` lib (`src/lib.rs`),
  covered by the `//src/ui:tokens` `rust_test`. Components map those enums → css vars.
- **Primitives:** `Button`, `Badge`, `StatusDot`, `Input` (Text/Password/Search),
  `Tabs`, `Panel`, generic `DataTable<R>` (callers `impl TableRow` for their row type),
  `TopNav`. Components are controlled/stateless; interactive state lives in the caller.
- **`SqlEditor`** (`src/components/sql_editor.rs`) — a reusable Monaco-backed SQL
  editor, built on the `monaco` crate (features `api`+`workers`, deliberately **not**
  `yew-components`, which pins yew 0.23 against this app's yew 0.21). It's
  controlled-with-guard (`value`/`on_change`, only force-writes the model when the
  incoming prop actually differs from the live buffer) and takes a `schema` prop
  (`CompletionSchema`) that feeds a registered Monaco `CompletionItemProvider` for
  keyword/table/column completion. The pure completion engine (`sql_completions`,
  `cursor_context`) lives in `loom_ui_core`, lint-clean and `rust_test`'d via
  `//src/ui:sql-completions` and `//src/ui:cursor-context` — the DOM-free logic is
  fully covered even though the Monaco-hosting component itself isn't. Editor chrome
  uses a defined `loom-dark` Monaco theme (`monaco::sys::editor::define_theme`, an
  `IStandaloneThemeData` inheriting `vs-dark` with `editor.background`/`editor.foreground`
  overridden to the `--loom-bg`/`--loom-text` values) rather than the builtin `vs-dark`.
  Monaco's JS ships **vendored inside the crate** via wasm-bindgen module snippets — the
  `--target web` build emits `snippets/` into `dist/`, already captured by the `:bundle`/
  `:gallery-bundle` genrules' `out=dist`; no CDN, no separate copy step. **Known
  limitations:** props (`on_change`/`read_only`/`schema`) are captured at mount time by
  the mount effect, so a caller changing them post-mount won't see it take effect; and the
  completion provider is registered once per mounted editor instance, so two
  concurrently-mounted `SqlEditor`s would double-register Monaco's `sql` provider (fine
  for the current single-editor gallery/product surfaces; multi-editor de-duplication is
  a follow-up).
- **Gallery:** `buck2 build //src/ui:gallery-bundle` then `buck2 run //src/ui:gallery-serve`
  serves a dev-only "kitchen sink" (`:gallery` binary, `src/gallery.rs` + `gallery.html`)
  rendering every primitive with its variants. It has **no backend/config.js** and never
  ships in the prod login bundle.
- **Testing limit (deliberate):** component *rendering* (`html!`) is **not**
  `rust_test`-able — buck2's runner has no DOM. It's verified by eye in the gallery; the
  per-component headless-wasm render harness is still deferred
  (`fut-ui-component-test-fixture`). Don't add an inline
  `#[test]` for a component (the `no-inline-tests` hook fails the build regardless).
- **Login e2e (`//src/ui/e2e:login`)** — the full-page layer that *does* exercise the
  rendered DOM: a `fantoccini` (Rust WebDriver) test that boots the composite (fresh DB on
  the shared `PgFixture`, `standalone::run` serving this bundle via `LOOM_UI_DIR`) and
  drives a **vendored** headless Chromium (`//third-party/browser`, Chrome for Testing,
  x86_64-linux) through render / bad-creds error / success → Explorer. The Postgres side is
  hermetic; the **browser binary is vendored and its libs come from the executor
  image** — from the host locally, and from the custom RBE image
  (`tools/ci/rbe-browser/`, pinned in `platforms/defs.bzl`) on the RE workers — so it
  **always runs, and hard-fails if the browser can't start** (the hermetic RE image
  plus dev-box host libs guarantee a browser everywhere; there is no auto-skip and
  no `LOOM_UI_E2E` variable). Run it: `buck2 test //src/ui/e2e:login`. Stable DOM
  hooks it depends on: `#login-username`, `#login-password`, `.signin`, `p.error`,
  and the Shell's `<nav>`; session token in sessionStorage key `loom_token`.
  Hermetic-RE image: `tools/ci/rbe-browser/` (spec 2026-07-02-ui-e2e-hermetic-rbe-image).

Spec/plan: `docs/superpowers/{specs,plans}/2026-07-01-ui-component-library*`.

## Gotchas (in rough order of how much each bit during the build-out)

- **Cross-compiling Rust needs a wasm *cxx* toolchain, not just a rust one.** The rust
  prelude routes the final link through the cxx toolchain (`prelude/rust/build.bzl`
  injects `-Clinker=<cxx linker>`), so a target-triple change alone leaves the host
  `clang++` linking the wasm. `toolchains/cxx_dist.bzl:wasm_cxx_toolchain` reports
  `LinkerType("wasm")` with rustc's bundled `rust-lld` (the LLVM dist's `lld` needs a
  `libxml2.so.2` the hermetic env lacks; `rust-lld` is statically self-contained).
  `toolchains//:cxx` is a `toolchain_alias` that `select()`s native vs wasm on
  `//platforms/constraints:wasm32`; the native branch (`:cxx-native`) is the unchanged
  `system_cxx_toolchain`. The wasm link is RE-eligible (`exec_dep` dists,
  `link_*_locally = False`).

- **reindeer needs `[platform]` config for any cross-compile target, or `cfg(not(wasm32))`
  deps leak into the wasm graph.** Symptom: `tokio` → `socket2` got pulled into the wasm
  build and failed with "Socket2 doesn't support the compile target". Fix lives in
  `reindeer.toml` (`[platform.*]` with `execution-platform` flags — host platforms `true`,
  wasm `false`, so build-script/proc-macro std features run on the host and do **not** leak
  onto wasm) plus `PACKAGE` `set_reindeer_platforms`, which maps `//platforms:wasm` onto
  reindeer's `wasm32` name (loom's wasm platform carries host os/cpu so exec deps resolve,
  so the default os/cpu select would otherwise mis-resolve it to `linux-x86_64`). **Defining
  any `[platform]` overrides reindeer's built-in default set** — that's why the regenerated
  `third-party/BUCK` dropped the macos/windows arms (loom is linux-only) and split the
  non-wasm deps into the `linux-*` platform arms. After any change here, re-run the full
  `buck2 test //src/...` (the platform set is global to every third-party crate).

- **buck2 `genrule` `cmd` gotchas** (all three hit while wiring `:bundle`/`:serve`):
  - `$(...)` is buck macro syntax. A shell `$(...)` command substitution fails attribute
    coercion — use backticks for shell substitution; keep `$(exe …)`/`$(location …)` for
    buck macros.
  - `$(location …)` needs a *target*, not a source filename. Pass source files (e.g.
    `index.html`) via the genrule's `srcs` and read them from `$SRCDIR`.
  - `default_target_platform` does **not** propagate through a genrule dep. A genrule that
    consumes `:app` via `$(location :app)` must itself set
    `default_target_platform = //platforms:wasm`, or `:app` configures under the genrule's
    (default) platform and the cross-config `$(location)` path won't resolve.

- **wasm-bindgen: the crate and the CLI must be the same version, and new enough for the
  rustc.** Single source of truth is `tools/wasm_bindgen.bzl` (`WASM_BINDGEN_VERSION`),
  consumed by the CLI `http_file` in `tools/BUCK` and the `=`-pinned crate in
  `Cargo.toml`; the `:bundle` genrule asserts the CLI reports that version. `0.2.100`
  panicked interpreting the wasm the 2026 nightly emits (newer wasm target features) —
  `0.2.126` works. To bump: change the constant, the crate pin, and the CLI sha256 together.
