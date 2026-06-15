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

### 2. Driver — `tools/coverage.sh`

Argument: a crate directory, default `src/control-plane/core`. Steps:

1. Enumerate the crate's test targets:
   `buck2 uquery "kind('rust_test', //<crate>:)"`.
2. Build them instrumented:
   `buck2 build --config loom.coverage=true <targets> --show-simple-output`
   → instrumented test-binary paths.
3. Resolve LLVM tools once:
   `buck2 build toolchains//:llvm-x86_64-linux --show-simple-output`
   → `<dist>/bin/llvm-profdata`, `<dist>/bin/llvm-cov`.
4. Run each test binary with
   `LLVM_PROFILE_FILE="$OUT/%p-%m.profraw"` (plain executables; no fixtures in
   this crate, so no Postgres/root concerns).
5. Merge: `llvm-profdata merge -sparse "$OUT"/*.profraw -o "$OUT/coverage.profdata"`.
6. Report + export, passing **every** test binary as a `--object` and the merged
   profile:
   - `llvm-cov report --instr-profile="$OUT/coverage.profdata" <objects…> --sources <crate>/src`
     → terminal table.
   - `llvm-cov export --format=lcov --instr-profile=… <objects…> --sources <crate>/src > "$OUT/lcov.info"`.

**Why multiple `--object`s + one merged profile:** each `rust_test` statically
links `control_plane_core`, so the instrumented library regions live in every
test binary. Merging all profraws and passing all binaries yields the true
*union* of coverage across the six test files, attributed back to the lib
sources.

### 3. Output location

`.loom/coverage/` — `.loom/` is already gitignored (`.gitignore:7`), so the
profraws, `coverage.profdata`, and `lcov.info` are all untracked. The script
creates the dir and clears stale `*.profraw` at the start of each run.

## Verification

1. Run `tools/coverage.sh`. Confirm it prints a non-zero coverage table for
   `src/control-plane/core/src/**` and writes a non-empty, well-formed
   `.loom/coverage/lcov.info`.
2. Sanity check attribution: a type/function known to be exercised by
   `tests/page.rs` (e.g. the page/cursor logic) shows >0% line coverage; the
   table lists the crate's source files, not test or third-party files.
3. Confirm a plain `buck2 build //src/...` (no `--config`) still produces an
   un-instrumented build — i.e. the gate is truly off by default.

## Non-goals (deferred)

- **CI job / coverage gate / threshold enforcement** — local signal only.
- **Codecov or other upload.**
- **HTML report** (`llvm-cov show --format=html`).
- **Fixture-crate coverage** (postgres/ingest/query-api). Those crates boot
  `initdb`/`postgres`/`duckdb`, which the existing `loom_fixture_test` macro
  pins to local, non-root execution. A general `tools/coverage.sh` over `//src/...`
  must thread the same local-only routing; that is the known next step once the
  core-crate flow is proven.
- **Per-target `coverage = True` / a coverage-aware test macro** — the
  config-gate makes per-target wiring unnecessary for this scope.
