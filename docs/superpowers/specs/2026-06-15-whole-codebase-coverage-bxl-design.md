# Whole-codebase Rust coverage via BXL

**Status:** design approved, pre-implementation
**Date:** 2026-06-15
**Supersedes (on completion):** the per-crate genrule prototype
(`2026-06-15-rust-coverage-prototype-design.md`) — its `//tools/coverage:core`
genrule, `tools/coverage/cover.sh`, and the build logic in `tools/coverage.sh`
are retired once the BXL path proves out. The prototype's `read_config` flag gate in `toolchains/BUCK` is
**replaced** by a config-free constraint-modifier mechanism: a
`constraint_value` `//tools/coverage:coverage_enabled`, a root `PACKAGE` file
that registers the prelude's cfg-constructor (making `modifiers=` effective),
and `cov.bxl` applying the constraint via `modifiers = ["root//tools/coverage:coverage_enabled"]`
in `ctx.configured_targets(...)`. What is kept from the prototype is the
llvm-cov mechanics, not the config flag.

## Goal

One command that produces coverage for the **whole** `//src` tree — a combined
codebase-wide table (to stdout, for agentic/terminal use) plus per-crate and
combined `lcov.info` / HTML — covering both pure-logic crates and the
fixture-backed crates (which boot postgres/duckdb), with no hand-maintained list
of tests or crates.

## Why BXL (not a genrule or a custom rule)

The prototype used a per-crate `genrule`. Extending it to the whole codebase hit
two walls that BXL removes:

1. **No auto-discovery.** A `genrule`'s inputs must be hand-enumerated via
   `$(location …)`; buck2's genrule has no query-macro support (`attrs.query()`
   exists only on dedicated rules). So every test would be listed by hand.
2. **Heterogeneous execution.** Fixture tests must run **locally** with postgres
   env; pure-logic tests run on **RE** with none. A genrule is one action / one
   execution platform / one env — it cannot be both, so the whole codebase would
   be forced local in one giant action with no per-crate breakdown.

**BXL** (Buck Extension Language, `buck2 bxl`) is the buck2-native scripting
layer for exactly this "discover targets, build them, run actions, orchestrate"
task. It is confirmed available in the pinned buck2, with prelude examples
(`prelude/bxl/*.bxl`, `prelude/erlang/elp.bxl`) demonstrating the needed API:

- `ctx.cquery().kind("rust_test", "//src/...")` — dynamic discovery.
- `node.attrs_lazy().get("remote_execution"/"env"/"labels")` — read a target's
  attrs (used to auto-classify fixtures).
- `ctx.analysis(target).providers()[DefaultInfo].default_outputs` — the built
  (instrumented) binary artifact.
- `ctx.bxl_actions().actions.run(...)` / `.declare_output(...)` — register
  actions, with per-action `local_only` and `env`.
- `ctx.output.ensure(...)` / `.print(...)` / `.stream(...)` — materialize and
  emit outputs.

This makes execution heterogeneity a per-**action** property (`local_only` set
only on fixture test runs), not a per-genrule one — dissolving wall #2 — and
discovery dynamic — dissolving wall #1.

## What the prototype keeps

Not wasted — the BXL reuses the prototype's hard-won mechanics verbatim: the
`-Cinstrument-coverage` instrumentation (now triggered via the constraint
modifier, not a config flag), the `-ignore-filename-regex='^/|^third-party/|/tests/'`
filter (LLVM 22 ignores positional source filters), `llvm-profdata merge` +
`llvm-cov export/report/show` invocation shapes, the `LLVM_PROFILE_FILE`
discipline (so no `default_*.profraw` leaks into the tree), and the proof that
instrumentation links against the pinned nightly `rust-std`.

## Design

### Component: `tools/coverage/cov.bxl` (function `cov`)

Invoked:

```
buck2 bxl //tools/coverage:cov.bxl:cov -- [--crate <name>]
```

- No `--crate` → whole codebase (all `rust_test` under `//src/...`).
- `--crate core` → only that crate's package.

### Data flow (inside the BXL)

1. **Discover:** `tests = ctx.cquery().kind("rust_test", "//src/...")` (narrowed to
   one package when `--crate` is given). The set is computed at analysis time —
   new tests are picked up automatically.
2. **Classify:** for each node, `is_fixture = node.attrs_lazy().get("remote_execution")`
   resolves to `"disabled"`. (`loom_fixture_test` sets exactly that; pure-logic
   `rust_test`s leave it unset.) No hand-maintained fixture table.
3. **Build:** `bin = ctx.analysis(node).providers()[DefaultInfo].default_outputs` —
   instrumented because `cov.bxl` configures the test targets with the
   `//tools/coverage:coverage_enabled` modifier (the root `PACKAGE` registers the
   cfg-constructor that makes `modifiers=` effective).
4. **Run as actions:** for each test, declare a `profraw` output and
   `actions.run(cmd_args(bin), env = run_env(node), local_only = is_fixture,
   category = "coverage_run", identifier = <label>)`, where `run_env` is
   `{LLVM_PROFILE_FILE: <profraw path>}` plus the fixture env for fixture tests.
5. **Merge:** group profraws by `node.label.package`; one `llvm-profdata merge`
   action per crate → `<crate>.profdata`, and one merging all → `all.profdata`
   (merging already-merged profiles is valid).
6. **Report:** per crate, and combined, run `llvm-cov export -format=lcov` and
   `llvm-cov report` actions with that group's binaries (first positional, rest
   `-object`), the merged profile, and `-ignore-filename-regex='^/|^third-party/|/tests/'`.
   HTML (`llvm-cov show -format=html`) is produced for the combined set (and is
   the one place that needs the source tree — see Open question O3).
7. **Output:** `ctx.output.ensure(...)` the artifacts; the wrapper copies them
   into `.loom/coverage/<crate>/` and `.loom/coverage/combined/`, and the
   combined `report.txt` is streamed to **stdout**.

### Fixture env (single source of truth)

The fixture env is **built in the BXL from the source targets**, not duplicated
as strings and not relying on macro-resolution of the test node's `env` attr:

```
POSTGRES_BIN_DIR        = <analysis(//src/control-plane/postgres:postgres-bin)>/bin
POSTGRES_LD_LIBRARY_PATH= <…postgres-bin>/lib:<…:libxml2>
LOOM_MIGRATIONS_DIR     = <analysis(//src/control-plane/postgres:migrations)>/migrations
DUCKDB_BIN              = <analysis(//src/control-plane/postgres:duckdb-cli)>
DUCKDB_EXTENSION_DIR    = <analysis(//src/control-plane/postgres:duckdb-extensions)>
```

This mirrors exactly what `loom_fixture_test` injects, derived from the same
targets — so it cannot drift, and `loom_fixture_test` itself is not modified.
The full (postgres + duckdb) env is applied to every fixture test in a crate; a
non-duckdb test simply ignores the extra vars.

### Wrapper: `tools/coverage.sh`

Reduced to: pick crate arg, run the `buck2 bxl` invocation under the config
(honouring `LOOM_COVERAGE_JOBS` → `-j`), and surface artifacts. It no longer
contains coverage logic — that all lives in the BXL.

## Outputs / UX

```
.loom/coverage/
├── <crate>/{lcov.info, report.txt}      # one dir per crate
└── combined/{lcov.info, report.txt, html/}
```

- `tools/coverage.sh` (no arg) → whole codebase; combined table to stdout.
- `tools/coverage.sh core` → one crate.
- `.loom/` is gitignored; `*.profraw`/`*.profdata` are gitignored.

## Verification

1. Pure-logic path: `--crate core` produces a non-empty combined table matching
   the prototype's numbers (~97% regions) with only `src/control-plane/core/src/*`
   files (no stdlib/third-party/tests).
2. Fixture path: `--crate postgres` boots postgres locally, produces a non-empty
   report for `src/control-plane/postgres/src/*`, and leaves **zero**
   `default_*.profraw` in the repo root.
3. Whole codebase: no-arg run produces a combined table spanning all 7 crates
   plus per-crate dirs; pure-logic test actions run on RE, fixture ones local.
4. `//src/...` untouched: a plain `buck2 build //src/...` neither runs the BXL
   nor instruments anything.
5. Retirement: after 1–3 pass, the prototype's `//tools/coverage:core` genrule,
   `tools/coverage/cover.sh`, and the build logic they backed are removed; the
   config gate and `.gitignore` entries remain.

## Implementation note: spike first

The plan's first task is a **BXL spike** on pure-logic `core` proving the
riskiest mechanic end to end (discover → build instrumented → run-as-action →
collect profraw → `llvm-cov report`), before the fixture path or the
multi-crate aggregation is built. This mirrors the prototype's spike-first
discipline.

## Open questions (resolved during the spike, with fallbacks)

- **O1 — `actions.run` executing a test binary.** Confirm a built `rust_test`
  binary can be run as a BXL action producing a `profraw` output (not merely
  built). *Fallback:* if BXL can't run binaries directly, declare a dynamic
  output that runs them via a small wrapped script — still BXL-driven discovery,
  just one extra indirection.
- **O2 — fixture env source.** Design assumes the env is rebuilt from source
  targets via `ctx.analysis`. If reading the node's resolved `env` attr turns
  out cleaner, use that. Either way: no `loom_fixture_test` change.
- **O3 — HTML + source tree.** `llvm-cov show -format=html` needs the source
  files. BXL actions run sandboxed without the repo source. *Fallback:* generate
  combined HTML in the `tools/coverage.sh` wrapper (where the repo source is
  present) from the BXL-produced `all.profdata`, exactly as the prototype's
  driver did — keeping HTML out of the sandboxed actions.
- **O4 — RE for instrumented pure-logic actions.** Confirm the instrumented
  binaries + `llvm-*` tool run acceptably on RE; if RE placement of the coverage
  actions is problematic, fall back to `local_only` for all (slower, still
  correct) and revisit.

## Non-goals (carried forward)

- No CI job / coverage gate / threshold — local dev signal only.
- No codecov/upload.
