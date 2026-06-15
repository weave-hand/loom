# Rust test-coverage prototype (config-gated, `control-plane/core`)

**Status:** design approved, pre-implementation
**Date:** 2026-06-15
**Scope:** a local-dev coverage signal for one crate, proving the
`-Cinstrument-coverage` → `llvm-profdata` → `llvm-cov` flow under buck2 before
generalizing.

## Goal

Give a developer a one-command coverage report for `src/control-plane/core`:
a per-file line/region/function table printed to the terminal, plus an
`lcov.info` file for editor gutter plugins. Nothing more — no CI gate, no
upload, no HTML, no per-target churn.

This is **Rust source-based coverage** (LLVM instrumentation), not a
third-party tool (no tarpaulin/grcov). All building blocks already exist in the
pinned toolchain:

- The prelude's Rust rules add `-Cinstrument-coverage` when coverage is on
  (`prelude/rust/rust_toolchain.bzl:62`, `prelude/rust/build.bzl:1274`), but we
  reach the same flag more simply through the toolchain's `rustc_flags`.
- The pinned **LLVM 22.1.2 dist** (`toolchains//:llvm-x86_64-linux`) ships
  `llvm-profdata` and `llvm-cov` in its `bin/` — no new download.
- The nightly `rust-std` dist includes `profiler_builtins`, which
  `-Cinstrument-coverage` needs at link time.

## Why `control-plane/core` is the prototype crate

Its six tests (`page`, `error-display`, `identity`, `logical-type`,
`row-filter-validation`, `serde-roundtrip`) are all pure-logic `rust_test`
targets — **no fixture tests**. So the local-only/non-root constraint that
`loom_fixture_test` handles (Postgres/DuckDB refusing to run as root) does not
apply here. The flow can be proven without that complication; extending to
fixture crates is explicitly deferred (see Non-goals).

## Design

### 1. Toggle — parse-time buckconfig gate in `toolchains/BUCK`

The toolchain's `rustc_flags` is today `["-Copt-level=2"]`. Add:

```python
_COVERAGE = native.read_config("loom", "coverage", "") in ("true", "1")
```

and append `["-Cinstrument-coverage"]` to `rustc_flags` when `_COVERAGE` is set.

- Default builds are **byte-for-byte unchanged** — the flag is absent unless the
  config is passed.
- `buck2 build --config loom.coverage=true //…` instruments everything in that
  configuration. (`-Copt-level=2` is kept; source-based coverage is fine under
  optimization and keeps the coverage build close to a normal one.)
- A coverage run is therefore a **distinct build configuration** and recompiles
  the world once under instrumentation. Accepted cost for a dev-signal tool.

This is the whole "config-driven" surface — no per-target `coverage = True`, no
macro, no edits to any `src/**/BUCK`.

### 2. Coverage as a build artifact — a `genrule` per crate

Coverage `lcov.info` is produced by a buck2 `genrule`, **not** orchestrated in
bash. The genrule lives in a dedicated dev-only package, `//tools/coverage`
(CLAUDE.md already documents `//tools` as dev-only and never built in CI), so it
is **outside** `//src/...` — CI's `buck2 build/test //src/...` never sees it and
normal builds are unaffected. It references the crate's test targets by absolute
label, e.g. `//tools/coverage:core`:

```python
genrule(
    name = "core",
    # one output dir holding both artifacts
    outs = {"lcov": ["lcov.info"], "report": ["report.txt"]},
    cmd = "$(exe :run) ...",   # or an inline bash cmd; see below
    # fixture crates ADD a local label here (e.g. one of the prelude's
    # _GENRULE_LOCAL_LABELS, mirroring loom_fixture_test's remote_execution).
    # control-plane/core needs no label — pure-logic tests run anywhere.
)
```

The genrule's `cmd`:

1. Receives each instrumented test binary via `$(location //src/control-plane/core:page)`
   etc., and the LLVM tools via `$(location toolchains//:llvm-x86_64-linux)`
   (`<dist>/bin/llvm-profdata`, `<dist>/bin/llvm-cov`).
2. Runs each test binary with `LLVM_PROFILE_FILE="$TMP/%p-%m.profraw"`.
3. `llvm-profdata merge -sparse "$TMP"/*.profraw -o "$TMP/coverage.profdata"`.
4. Emits the two declared outputs, passing **every** test binary as a `--object`
   and the merged profile:
   - `llvm-cov export --format=lcov … <objects…> --sources src/control-plane/core/src > $OUT/lcov.info`
   - `llvm-cov report … <objects…> --sources src/control-plane/core/src > $OUT/report.txt`

**Instrumentation is the config gate (§1).** The genrule's test-binary deps are
only instrumented when built under `--config loom.coverage=true`. Because the
target lives outside `//src/...` it is never built without that flag in practice;
if someone does, the genrule detects the missing coverage map and **fails with an
explicit message** (build with `--config loom.coverage=true`) rather than
emitting empty/misleading data — no silent fallback.

**Why multiple `--object`s + one merged profile:** each `rust_test` statically
links `control_plane_core`, so the instrumented library regions live in every
test binary. Merging all profraws and passing all binaries yields the true
*union* of coverage across the six test files, attributed back to the lib
sources.

### 3. Driver — `tools/coverage.sh` (thin wrapper)

The script no longer runs binaries; it just drives the build target and surfaces
the artifacts. Argument: a crate name, default `core`. Steps:

1. `buck2 build --config loom.coverage=true //tools/coverage:<crate> --show-simple-output`.
2. Copy the built `lcov.info` out of `buck-out` into `.loom/coverage/lcov.info`
   for editor gutter plugins (`.loom/` is already gitignored, `.gitignore:7`).
3. `cat` the built `report.txt` to the terminal for the at-a-glance table.

Execution placement (local vs RE) is owned by the genrule's labels, so the
wrapper carries **no** routing logic — that is the whole point of modelling
coverage as a build target.

## Verification

1. `buck2 build --config loom.coverage=true //tools/coverage:core` succeeds and
   produces non-empty `lcov.info` + `report.txt` outputs.
2. Run `tools/coverage.sh`. Confirm it prints a non-zero coverage table for
   `src/control-plane/core/src/**` and writes a non-empty, well-formed
   `.loom/coverage/lcov.info`.
3. Sanity check attribution: a type/function known to be exercised by
   `tests/page.rs` (e.g. the page/cursor logic) shows >0% line coverage; the
   report lists the crate's source files, not test or third-party files.
4. Confirm `//src/...` is untouched: a plain `buck2 build //src/...` (no
   `--config`) neither builds `//tools/coverage:core` nor instruments anything —
   the gate is off by default and the coverage target is out of that graph.
5. Confirm the explicit-failure path: `buck2 build //tools/coverage:core`
   *without* the config fails with the "build with `--config loom.coverage=true`"
   message, not an empty report.

## Non-goals (deferred)

- **CI job / coverage gate / threshold enforcement** — local signal only.
- **Codecov or other upload.**
- **HTML report** (`llvm-cov show --format=html`).
- **Fixture-crate coverage** (postgres/ingest/query-api). Those crates boot
  `initdb`/`postgres`/`duckdb`, which refuse to run as root on RE. Because
  coverage is a `genrule` (§2), each such crate's coverage target just carries a
  local label from the prelude's `_GENRULE_LOCAL_LABELS` set — the genrule
  equivalent of `loom_fixture_test`'s `remote_execution = "disabled"` — so
  placement is correct **by construction**, with no routing logic in
  `tools/coverage.sh`. Adding those targets is the known next step once the
  core-crate flow is proven; the wrapper and the merge/report cmd are unchanged.
- **Per-target `coverage = True` / a coverage-aware test macro** — the
  config-gate makes per-target wiring unnecessary for this scope.
