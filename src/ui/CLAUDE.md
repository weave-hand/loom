# CLAUDE.md — `src/ui/` (Yew → WASM web UI)

A [Yew](https://yew.rs) app that cross-compiles to `wasm32-unknown-unknown` and
bundles to browser-loadable JS via wasm-bindgen.

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
