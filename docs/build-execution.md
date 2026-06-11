# Build & CI execution model

How loom decides **where** each build action and each test runs (BuildBuddy
remote execution vs the local machine), and the cost model that drives those
choices. This is the *why* behind the knobs; for the operational job list see
the *Continuous integration* section of `CLAUDE.md`, and for day-to-day commands
see `DEVELOPING.md`.

The short version: **compute is cached on BuildBuddy and runs remotely by
default; the cost we manage is not compute but the *materialization* of outputs
onto a machine.** Everything below follows from that.

## Two execution axes

buck2 makes two independent placement decisions. Conflating them is the usual
source of confusion.

### 1. Build execution (compiling, codegen, archive extraction)

Governed by the execution platform's `CommandExecutorConfig`, built in
`platforms/defs.bzl` from the `project.remote_enabled` buckconfig:

- `project.remote_enabled` set → **limited hybrid**: `local_enabled = True`,
  `remote_enabled = True`, `use_limited_hybrid = not remote_only`. Actions can
  run on BuildBuddy RE (container `rbe-ubuntu24-04`) or locally.
- unset → **pure local**: `remote_enabled = False`. A secretless, RE-free build
  (e.g. `buck2 build --config project.remote_enabled= //…`).

Two knobs tune the hybrid case:

- **`BUCK_PREFER_REMOTE=true`** injects `--prefer-remote`, so hybrid-eligible
  actions prefer RE. Used in CI so nothing is pulled local just to build it.
- **`-M none`** (`--materializations=none`) tells buck2 not to download final
  build artifacts to the runner — you only wanted to know it *builds*, not to
  hold the outputs.

A handful of actions are *pinned local* regardless: genrules labelled `uses_xz`
(`libxml2`, `duckdb-cli`, `duckdb-extensions`) and the `uses_local_filesystem_abspaths`
tool wrappers (`rustfmt`, `clippy`). Keep new `local_only` / `uses_local_*`
actions **off the common build path** — every local action must materialize its
inputs from CAS on a fresh runner, which is exactly the download we are trying to
avoid.

### 2. Test-run execution (the test *command*, after it is built)

Separate from build placement. buck2 derives a test's run executor from the
`remote_execution` attribute (`re_test_common`). loom uses exactly one value:

- **`remote_execution = "disabled"`** → run executor
  `CommandExecutorConfig(local_enabled = True, remote_enabled = False)`. The test
  *command* runs locally; **the build of the test binary is untouched** and still
  uses axis 1 (RE).

This per-test local pin is what replaced the old global
`env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...` ceremony.

## Fixture-test local routing

The hermetic fixtures (`PgFixture`, `DuckLakeWriter`, the query-api
`EmbeddedDuckDb`) boot real `initdb` / `postgres` / `duckdb` processes. Those
**refuse to run as root**, and BuildBuddy's RE container runs as root — so a
fixture test's *command* must run locally. Its *build* should still go to RE.

That split is exactly axis 2 (`remote_execution = "disabled"`) without touching
axis 1. To avoid hand-tagging — and forgetting to tag — every fixture target, the
attribute lives in one macro:

**`loom_fixture_test`** (`src/control-plane/postgres/defs.bzl`) wraps `rust_test`,
injecting the shared fixture `env` block (POSTGRES_*, LOOM_MIGRATIONS_DIR, and —
with `duckdb = True` — the DUCKDB_* vars) **and** `remote_execution = "disabled"`.
All 18 fixture targets across `src/control-plane/postgres`, `src/services/query-api`,
and `src/control-plane/worker` use it; pure-logic tests (`sql-compile`,
`http-smoke`, `worker_test`, the `control-plane-core` tests) stay plain
`rust_test` and run on RE. **A new fixture test must use `loom_fixture_test`** or
it will route to RE and fail as root.

### Rejected alternative: `exec_compatible_with` + a local execution platform

The textbook buck2 way to force a *target* onto a platform is to register a
local-only execution platform carrying a `//constraints:local` constraint and tag
the test with `exec_compatible_with = ["//constraints:local"]`. A spike confirmed
this **resolves** correctly (the tagged test picks the local platform; untagged
tests stay on the default) — but it **breaks the Rust link**: the test's
`rustc link` action and its dependency rlibs end up in different
exec-configuration trees, so the link fails with `E0463: can't find crate for
control_plane_postgres`. This failed identically with and without RE, so it is
fundamental to placing a `rust_test` on a different execution platform than its
deps; fixing it would mean routing the whole dependency subgraph local, defeating
RE. `remote_execution = "disabled"` avoids the problem entirely by changing only
the run executor, not the build platform.

## The CI jobs

GitHub Actions, `.github/workflows/ci.yml`. All jobs install the pinned buck2
release (`BUCK2_RELEASE`, kept aligned with the vendored prelude submodule) and
set `BUCK_PREFER_REMOTE: "true"`.

| Job | Trigger | What it runs | Placement |
| --- | --- | --- | --- |
| `build-test` | pushes to `main` | `buck2 build -M none //src/...` then `buck2 test //src/...` | build on RE (no download); fixture tests local, logic tests RE |
| `affected` | PRs | `//tools:supertd` snapshots the base-SHA graph, `//tools:btd` maps the diff to impacted targets, then `buck2 build -M none` + `buck2 test` on just those | same as above, scoped to impacted targets |
| `lint` | all events | `buck2 run //tools:prek -- run --all-files` (rustfmt, clippy, file hygiene, reindeer-in-sync) | hermetic via buck2 |

Scope is `//src/...` (first-party + their third-party deps). The `//tools`
targets are dev-only — some are local-only genrules — and are deliberately not
built in CI.

## Materialization cost model

Because compute is cached, a green CI run does almost no *compute* — a typical
`affected` run reports `Cache hits: 99%`, `remote: 0`, a few `local`. The wire
cost is **materializing outputs from CAS onto the runner**, and it is paid only
for things that must exist locally.

- **The build job downloads single-digit MiB.** `-M none` skips final-artifact
  download; `BUCK_PREFER_REMOTE` keeps actions on RE so their inputs never
  materialize; the toolchain's `assemble_sysroot` action is RE-eligible (not
  `local_only`) so the rustc/std dists are not forced local. This took a cached
  build from ~4 GiB down to single-digit MiB.
- **The test job downloads ~110 MiB on fixture-heavy runs.** Fixture tests run
  *locally* (axis 2), so their test binaries **and** the real DB tool binaries
  they `exec` must be materialized. This is *lower* than the old blanket
  `--local-only`, which also pulled every non-fixture test's inputs.

The ~110 MiB is dominated by pinned, rarely-changing tool binaries, not per-PR
code:

| Artifact | Materialized size | Changes when |
| --- | --- | --- |
| `duckdb-extensions` (ducklake + postgres_scanner) | ~90 MiB | `DUCKDB_VERSION` bump |
| `duckdb-cli` | ~59 MiB | `DUCKDB_VERSION` bump |
| `postgres-bin` (theseus-rs dist) | ~36 MiB | `PG_VERSION` bump |
| `libxml2` | ~1.5 MiB | ~never |
| each fixture test binary (debug) | ~12 MiB | the relevant code changes |

`DUCKDB_VERSION` (`v1.5.3`) and `PG_VERSION` (`17.9.0`) move only on a deliberate
bump, so the heavyweight inputs are effectively constant across PRs.

### Why we don't cache buck-out

Tempting, but wrong on two counts:

1. **It is a buck2 anti-pattern.** buck2 delegates *all* cross-run caching to the
   RE action-cache + CAS — which is precisely why runs already show ~99% cache
   hits with `remote: 0`. `buck-out` is local scratch tied to daemon and
   materializer state, not a portable cache.
2. **It would not even help.** A fresh runner starts a fresh buck2 daemon with
   empty materializer state. Even if `actions/cache` restored the `buck-out`
   files, buck2 does not *know* they are materialized — its state says "absent" —
   so it re-materializes (re-downloads) from CAS regardless. You would pay the
   cache restore *and* the download. Restoring `buck-out` behind buck2's back is
   also a correctness footgun (stale/partial trees).

If CI egress ever becomes a real cost, the robust levers are, in order: a
**self-hosted / persistent runner** with a warm daemon and `buck-out` (the
buck2-intended way to make materialization persist across runs), shrinking the
heavy pinned inputs, or a region-local BuildBuddy CAS to make the download
*faster* rather than *smaller*. Caching `buck-out` via `actions/cache` is not on
that list.

## Invariants

- New `local_only` / `uses_local_*` actions stay **off the common build path**, or
  every CI run pays to materialize their inputs.
- New fixture-backed tests use **`loom_fixture_test`**, never a bare `rust_test`.
- `BUCK2_RELEASE` and the vendored prelude submodule are bumped **together**.
