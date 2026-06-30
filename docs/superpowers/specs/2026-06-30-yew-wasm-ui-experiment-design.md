# Yew WASM UI experiment — design

**Date:** 2026-06-30
**Status:** approved (brainstorm), pending implementation plan
**Area:** build/devx (toolchain), UI

## Goal

An experiment: teach loom's buck2 build to cross-compile Rust to
`wasm32-unknown-unknown`, then build a **minimal [Yew](https://yew.rs) UI** that
produces a browser-loadable bundle (`app_bg.wasm` + `app.js` + `index.html`) via
`buck2 build`. The interesting work is the wasm toolchain, not the UI code — the
yew app is intentionally trivial (one heading + a click counter).

Scope decisions made during brainstorming:

- **How far:** full browser-runnable bundle (`buck2 build //src/ui:bundle → dist/`),
  not just a `.wasm` artifact and not just the dep import.
- **Placement:** lives at `src/ui/`, **inside** the existing `//src/...` CI sweep and
  the strict pedantic+restriction clippy gate — not a separate experiments cell.

## Background / current state

The buck2 rust toolchain is **native-only**. `toolchains/BUCK` (`hermetic_rust_toolchain`
at line 230, name `"rust"` → `toolchains//:rust`) fetches `rust-std` dists only for
`x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`. `toolchains/rust_dist.bzl`
assembles a per-triple sysroot and bakes a host-linker override
(`-Clinker=<rustc-dist>/.../bin/rust-lld`). There is no `wasm32` std component, no wasm
platform, and no wasm toolchain. Third-party Rust is imported by reindeer in
non-vendored mode (`reindeer.toml` → workspace `Cargo.toml`); `./tools/buckify.sh`
regenerates `third-party/BUCK`. CI runs `buck2 build //src/...` + `buck2 test //src/...`
against the **default (native) target platform**.

## Architecture

Five components, listed in build/dependency order.

### 1. wasm rust-std dist (`toolchains/BUCK`)

Add an `http_archive` mirroring the existing std dists, for
`rust-std-nightly-wasm32-unknown-unknown` at the same `RUST_NIGHTLY = "2026-03-28"`:

```
url:          https://static.rust-lang.org/dist/2026-03-28/rust-std-nightly-wasm32-unknown-unknown.tar.xz
strip_prefix: rust-std-nightly-wasm32-unknown-unknown/rust-std-wasm32-unknown-unknown
```

Target name e.g. `:rust-std-wasm32`. (sha256 filled in at implementation time.)

### 2. wasm toolchain variant + platform + constraint

- **Constraint:** a new `constraint_setting` + `constraint_value` marking "build Rust for
  wasm" — e.g. `//platforms/constraints:rust_target` setting with a `wasm32` value
  (`//platforms/constraints:wasm32`). A custom constraint is used (rather than a cpu
  constraint) because target selection here is about the Rust target triple, not host CPU.
- **Toolchain variant:** a second `hermetic_rust_toolchain` (e.g. `:rust-native` keeps the
  current config; add `:rust-wasm`) with `rustc_target_triple = "wasm32-unknown-unknown"`
  and `std_dist = ":rust-std-wasm32"`. **It must NOT bake the host-linker override** —
  rustc self-links wasm via its internal `wasm-ld`; forcing the host `rust-lld` breaks the
  link. This requires `rust_dist.bzl`'s `assemble_sysroot`/toolchain rule to make the
  `-Clinker=` flag conditional on the target being a non-wasm triple (the riskiest change;
  see Risks).
- **`toolchains//:rust`:** becomes an `alias` whose `actual` is
  `select({ "//platforms/constraints:wasm32": ":rust-wasm", "DEFAULT": ":rust-native" })`,
  so toolchain resolution follows the target platform. (Confirm at implementation time
  that an `alias` cleanly forwards the `RustToolchainInfo` provider; if not, use the
  prelude's toolchain-alias rule or a `select()` on the toolchain attrs directly.)
- **Platform:** `//platforms:wasm` — a `platform` carrying the `wasm32` constraint value
  (plus a host os/cpu so exec deps still resolve). Native platforms omit the constraint,
  so the `DEFAULT` branch (native toolchain) is unchanged for the rest of the tree.

### 3. wasm-bindgen-cli as a buck tool (`tools/BUCK`)

Pinned `http_archive` for `wasm-bindgen-cli`, following the existing dev-tool pattern
(`//tools:jq` etc.). **Its version is locked to the `wasm-bindgen` crate version reindeer
resolves** (see Risks — this is the #1 failure mode). Exposed as `//tools:wasm-bindgen`.

### 4. reindeer import (`src/ui/Cargo.toml`, `third-party/BUCK`)

Add `yew` (latest stable, default features for CSR) to `src/ui/Cargo.toml`, then
`cargo generate-lockfile` (hermetic cargo via `eval "$(./tools/env.sh)"`) and
`./tools/buckify.sh`. Expect `yew`, `wasm-bindgen`, `wasm-bindgen-futures`, `web-sys`,
`js-sys`, `gloo-*` and friends to land in `third-party/BUCK`. Build-script fixups are
likely needed (e.g. `third-party/fixups/wasm-bindgen-*/fixups.toml` with `run = true`);
the plan budgets for iterating these until `buckify` is warning-free.

### 5. `src/ui/` crate + bundle genrule (`src/ui/BUCK`, `src/ui/src/main.rs`, `src/ui/index.html`)

- `src/ui:app` — a `rust_binary` with `default_target_platform = "//platforms:wasm"` and
  `deps = ["//third-party:yew", ...]`, producing `app.wasm`. Pinning the platform on the
  target is what lets it survive the native `buck2 build //src/...` sweep — the sweep
  cross-compiles it to wasm instead of attempting a native yew build.
- `src/ui:bundle` — a `genrule` that runs `$(exe //tools:wasm-bindgen) --target web
  --no-typescript --out-dir $OUT --out-name app $(location :app)`, then copies
  `index.html` into `$OUT`. Output: `dist/{app_bg.wasm, app.js, index.html}`. The genrule
  also asserts the `wasm-bindgen` crate version equals the CLI version (fail-fast on skew).
- `src/ui:serve` — a convenience `command_alias`/genrule running the hermetic Python
  (`python -m http.server`) rooted at the built `dist/`, because the `--target web` glue
  uses `fetch()` and will not load over `file://`.

### Data flow

```
src/ui/src/main.rs ──(rust_binary @ //platforms:wasm)──▶ app.wasm
app.wasm ──(genrule: wasm-bindgen --target web)──▶ dist/app_bg.wasm + dist/app.js
src/ui/index.html ──(cp)─────────────────────────▶ dist/index.html
buck2 run //src/ui:serve ──▶ http.server over dist/ ──▶ live UI in browser
```

## Build sequence (staged to de-risk)

- **Milestone A — wasm toolchain proves out, no yew.** Components 1–2 + a *trivial*
  `rust_binary` (`fn main() {}`, no deps) pinned to `//platforms:wasm`.
  `buck2 build //src/ui:app` emits a `.wasm`. Isolates the sysroot/linker change from the
  dependency tree.
- **Milestone B — bundle pipeline, still trivial binary.** Component 3 + the `:bundle`
  genrule + `index.html`. `buck2 build //src/ui:bundle → dist/`.
- **Milestone C — yew.** Components 4–5: import yew, replace `main` with a minimal
  `<App/>` (heading + click counter). Rebuild `:bundle`; confirm live via `:serve`.

## Risks & mitigations

- **wasm-bindgen crate ↔ CLI version skew (most likely failure).** Mismatched glue panics
  at module load. Mitigation: after reindeer import, read the exact `wasm-bindgen` version
  from `Cargo.lock` and pin `//tools:wasm-bindgen` to it byte-for-byte; the `:bundle`
  genrule asserts crate-version == CLI-version and fails the build on mismatch.
- **wasm linker.** Baking the host `-Clinker` breaks wasm linking. Mitigation: the
  `:rust-wasm` toolchain omits the host-linker override (rustc self-links wasm); make the
  flag conditional on triple in `rust_dist.bzl`. Milestone A surfaces this immediately.
- **reindeer fixups.** wasm-bindgen ships build scripts; expect to add `run = true` fixups.
  Budgeted in Milestone C.
- **`reindeer update` blast radius.** Per the repo's known footgun, `reindeer update`
  re-resolves the whole graph and can silently downgrade unrelated crates. Mitigation:
  diff `Cargo.lock` against the merge-base for native/`links` crates and run the **full**
  `buck2 build //src/...` + `buck2 test //src/...` before committing the dep change.
- **Strict clippy on `html!`.** yew macros may trip pedantic/restriction. Mitigation:
  crate-level `#![expect(lint, reason = "yew html! macro")]` in `src/ui/src/main.rs`
  rather than weakening the global `CLIPPY_ALLOWS` gate.
- **CI native sweep.** Handled by the `default_target_platform` pin (cross-compiles rather
  than native-builds yew). `buck2 test //src/...` finds no native test target in `src/ui`,
  which is correct for a wasm UI crate.

## Testing

Buck2 cannot run wasm in-process, and the repo convention is integration-only tests. The
**build assertion is the test**: `buck2 build //src/ui:bundle` succeeding transitively
proves toolchain, std dist, dep resolution, wasm-bindgen glue, and version-lockstep. The
genrule's crate-vs-CLI version equality check is the one explicit assertion. No `rust_test`
target — there is nothing native to exercise. Manual confirmation: `buck2 run //src/ui:serve`
+ browser.

## Out of scope

- Trunk / hot-reload / a real dev server beyond `python -m http.server`.
- CSS/styling beyond what proves rendering works.
- Any integration with loom's actual services (query-api, ingest) — this is a toolchain
  + rendering experiment only.
- aarch64 wasm-bindgen-cli host binary (x86_64 host only initially, matching rustfmt's
  precedent), unless trivial to add.
