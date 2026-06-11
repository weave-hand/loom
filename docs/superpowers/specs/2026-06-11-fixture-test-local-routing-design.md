# Auto-route hermetic fixture tests to local execution — Design

**Date:** 2026-06-11
**Status:** Approved (brainstorm)

## Problem

loom's hermetic test fixtures (`PgFixture`, `DuckLakeWriter`, and the query-api
`EmbeddedDuckDb`) boot real `initdb`/`postgres`/`duckdb` processes. These refuse
to run **as root**, and BuildBuddy's remote-execution (RE) container runs as root.
So any `rust_test` that uses a fixture must have its **test command** run locally.

Today this is forced globally at the invocation level:

```
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...
```

`--local-only` pins *every* test (and its build) local, and `BUCK_PREFER_REMOTE`
must be dropped because it injects `--prefer-remote`, which conflicts with
`--local-only`. This ceremony is required in CI (both the `build-test` and
`affected` jobs) and in local dev, and it needlessly forces pure-logic tests
local too.

## Goal

A `rust_test` that boots a fixture runs its test command locally (non-root)
**without any flag**, while its build stays on RE. Pure-logic tests keep running
on RE. Drop the `env -u … --local-only` ceremony everywhere.

## Mechanism (spike-proven)

buck2 separates **build execution** from **test-run execution**. The `rust_test`
rule exposes a `remote_execution` attribute (via `re_test_common.test_args()`).
Setting:

```python
remote_execution = "disabled"
```

makes buck2 give that test a run executor of
`CommandExecutorConfig(local_enabled = True, remote_enabled = False)` — the test
*command* cannot go to RE, so it runs locally, **without** changing the build
execution platform. The build's actions still run on the default RE platform.

### Spike evidence

`BUCK_PREFER_REMOTE=true buck2 test //src/control-plane/postgres:queue` (no
`--local-only`), with `remote_execution = "disabled"` on `:queue`:

- `✓ Pass: …:queue (0.9s)` — 3/3 contract tests passed.
- Ran as user `jackm` (not root) — `initdb` succeeded.
- `Commands: 2 (cached: 0, remote: 2, local: 0)` — **build** actions on RE, only
  the **test command** pinned local. Exactly the intended split.

### Rejected alternative — execution platforms + `exec_compatible_with`

Registering a second, local-only execution platform carrying a
`//constraints:local` constraint and tagging fixture tests with
`exec_compatible_with = ["//constraints:local"]` is the textbook buck2 way to
route a *target* to a platform — and the spike confirmed it **resolves**
correctly (the tagged test picked the local platform; untagged tests stayed on
the default). But it **breaks the Rust link**: the test's `rustc link` action and
its dependency rlibs end up in different exec-configuration trees, so the link
fails with `E0463: can't find crate for control_plane_postgres` (and `tokio`,
`async_trait`). This failed identically with and without RE involved, so it is
not a remote/local-split issue — it is fundamental to putting a `rust_test` on a
different execution platform than its deps. Fixing it would require routing the
*entire* dependency subgraph (`:postgres` + all third-party + toolchains) local,
defeating RE. Rejected.

## Design

### Component: a shared `loom_fixture_test` macro

New file `src/control-plane/postgres/defs.bzl` defines one macro that wraps
`rust_test`, injecting (a) the shared fixture `env` block and (b)
`remote_execution = "disabled"`:

```python
def loom_fixture_test(
        name,
        crate,
        srcs,
        crate_root,
        deps,
        duckdb = False,
        edition = "2024",
        env = {},
        **kwargs):
    fixture_env = {
        "POSTGRES_BIN_DIR": "$(location //src/control-plane/postgres:postgres-bin)/bin",
        "POSTGRES_LD_LIBRARY_PATH": "$(location //src/control-plane/postgres:postgres-bin)/lib:$(location //src/control-plane/postgres:libxml2)",
        "LOOM_MIGRATIONS_DIR": "$(location //src/control-plane/postgres:migrations)/migrations",
    }
    if duckdb:
        fixture_env["DUCKDB_BIN"] = "$(location //src/control-plane/postgres:duckdb-cli)"
        fixture_env["DUCKDB_EXTENSION_DIR"] = "$(location //src/control-plane/postgres:duckdb-extensions)"
    fixture_env.update(env)  # caller extras, e.g. LOOM_SQLX_DIR
    rust_test(
        name = name,
        crate = crate,
        srcs = srcs,
        crate_root = crate_root,
        edition = edition,
        env = fixture_env,
        remote_execution = "disabled",
        deps = deps,
        **kwargs
    )
```

**Design points:**

- **Absolute labels.** The `$(location //src/control-plane/postgres:…)` labels are
  absolute, so the macro produces identical env whether called from postgres/,
  query-api/, or worker/ BUCK files. Loaded with
  `load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")`.
- **`duckdb = False` default.** PG-only fixture tests omit it; DuckDB-backed ones
  pass `duckdb = True` to add the two `DUCKDB_*` vars.
- **`env = {}` extras.** Merged last, for the one outlier (`sqlx-cache-check` adds
  `LOOM_SQLX_DIR`).
- **No name derivation.** `crate`/`srcs`/`crate_root` are passed explicitly —
  crate names don't derive cleanly from target names (e.g. `postgres-integration`
  → crate `worker_postgres_integration`).
- **Single enforcement point.** `remote_execution = "disabled"` lives only in the
  macro, so a new fixture test gets correct routing by construction. This is the
  guard (per decision: macro-only, no separate prek hook).

**Open verification (plan task 1):** confirm `rust_test` is callable from a
`.bzl` directly; if buck2 requires it, prefix with `native.` or the appropriate
prelude load. A trivial build of one converted target settles this before the
bulk rewrite.

### Targets to convert (18)

All keep their existing `crate`/`srcs`/`crate_root`/`deps`; only the `env` block
and `remote_execution` move into the macro call.

**`src/control-plane/postgres/BUCK`** (14):
- PG-only: `queue`, `ontology`, `acl`, `lineage`, `tx`, `lineage-roundtrip`
- `duckdb = True`: `ducklake-smoke`, `snapshot-append`, `snapshot-create`,
  `ducklake-interop`, `snapshot-rollback`, `snapshot-conformance`, `catalog`
- `duckdb = True` + `env = {"LOOM_SQLX_DIR": "$(location :sqlx-cache)/.sqlx"}`:
  `sqlx-cache-check` (in-package, so the caller passes the extra-env label
  package-relative: `env = {"LOOM_SQLX_DIR": "$(location :sqlx-cache)/.sqlx"}`)

**`src/services/query-api/BUCK`** (3, all `duckdb = True`): `serving-engine`,
`governed-read`, `spike-duckdb`

**`src/control-plane/worker/BUCK`** (1, PG-only): `postgres-integration`

### Targets left as plain `rust_test` (run on RE)

`sql-compile` and `http-smoke` (query-api), `worker_test` (worker), and all of
core's tests (`page`, `error-display`, `serde-roundtrip`, `row-filter-validation`).
These have no fixture env and benefit from RE.

### CI changes — `.github/workflows/ci.yml`

Both test invocations lose the ceremony:

- `build-test` job: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...`
  → `buck2 test //src/...`
- `affected` job: same change for the `"${targets[@]}"` invocation.

The `buck2 build -M none //src/...` lines and `BUCK_PREFER_REMOTE: "true"` global
env stay. Net effect on CI: pure-logic tests now run on RE (cheaper); fixture
tests self-pin local (same materialization cost CI already paid under blanket
`--local-only`). The explanatory comments about `--local-only`/`env -u` are
replaced with a one-line note that fixture tests self-route via the macro.

### Docs — `CLAUDE.md`

The "## Testing" section's run command updates from
`env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...` to plain
`buck2 test //src/...`, with a sentence explaining that fixture tests pin
themselves local via `loom_fixture_test` (`remote_execution = "disabled"`), so the
flag dance is no longer needed. Other `--local-only` mentions in CLAUDE.md
(e.g. the sqlx-cache-check description) are reworded consistently. `--local-only`
remains valid as a manual override; it is simply no longer required.

## Testing the change

1. **Macro-availability smoke (task 1):** convert one target (`:queue`), build it,
   confirm the `.bzl` macro resolves and the target builds.
2. **Routing proof:** `BUCK_PREFER_REMOTE=true buck2 test //src/control-plane/postgres:queue`
   (no `--local-only`) passes and runs as non-root (matches the spike).
3. **Full suite, no flag:** `buck2 test //src/...` (with `BUCK_PREFER_REMOTE`
   unset *and* set) — all tests pass; fixture tests run local, logic tests are
   eligible for RE.
4. **clippy/rustfmt/prek** clean; commit.

## Out of scope

- The two-execution-platform / `constraints/` machinery (spike scaffolding, not
  needed and rejected above).
- A separate prek hook to detect un-routed fixture tests (decided against; the
  macro is the single source of truth).
- Any change to production code or the fixtures themselves.
