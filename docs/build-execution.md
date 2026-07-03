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
(`libxml2`) and the `uses_local_filesystem_abspaths`
tool wrappers (`rustfmt`, `clippy`). Keep new `local_only` / `uses_local_*`
actions **off the common build path** — every local action must materialize its
inputs from CAS on a fresh runner, which is exactly the download we are trying to
avoid.

### 2. Test-run execution (the test *command*, after it is built)

Separate from build placement. buck2/tpx runs test-*run* actions on the **local
executor by default** and only dispatches them to RE under
`--unstable-allow-all-tests-on-re`; there is **no remote test-result cache**, so
tests re-run on every invocation either way. loom sets no per-target
`remote_execution` attribute — **placement is an invocation-level choice**:

- **Non-root hosts** (dev machines, the non-root BuildBuddy CI runners): the
  default local run executor is fine, including for hermetic-Postgres fixtures.
- **Root hosts** (cloud sessions — `initdb`/`postgres` refuse to run as root):
  pass `--unstable-allow-all-tests-on-re` so test runs go to the RE workers,
  which execute as the non-root `buildbuddy` user (the platform's `dockerUser`,
  `platforms/defs.bzl`). The cloud buck2 shim (`tools/ci/buck2-proxy-shim.sh`)
  injects the flag for every `buck2 test`; CI passes it explicitly in
  `buildbuddy.yaml`.

History: the old global `--local-only` ceremony was first replaced by a
per-target `remote_execution = "disabled"` pin in the fixture macro; that pin
was retired once the RE platform ran non-root — it would force fixtures onto RE
in *every* environment and break local dev without an RE backend.

## Fixture-test wiring

The hermetic fixture (`PgFixture`, plus MinIO where a test needs S3) boots real
`initdb` / `postgres` processes, so a fixture test needs the pinned tool
binaries and shared throttle state in its environment. To avoid hand-wiring —
and mis-wiring — every fixture target, the env lives in one macro:

**`loom_fixture_test`** (`src/control-plane/postgres/defs.bzl`) wraps `rust_test`,
injecting the shared fixture `env` block (`POSTGRES_BIN_DIR`,
`POSTGRES_LD_LIBRARY_PATH` incl. `libxml2`, the boot-throttle
`LOOM_PG_FIXTURE_SLOT_DIR`, and — with `minio = True` — `MINIO_BIN`) plus the
test panic-lint allowances. It deliberately sets **no** `remote_execution`
profile (see axis 2 above). Fixture targets across `src/control-plane/postgres`,
the services, and the worker use it; pure-logic tests stay plain `rust_test`.
**A new fixture test must use `loom_fixture_test`** or it runs without the
fixture env and fails to boot Postgres.

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
RE. Steering only the *run executor* (the retired per-target pin then, the
invocation-level `--unstable-allow-all-tests-on-re` now) avoids the problem
entirely by never touching the build platform.

## The CI jobs

BuildBuddy Workflows, `buildbuddy.yaml`. Each action's first step is the shared
`tools/ci/buildbuddy-setup.sh`, which installs the pinned buck2 release
(`BUCK2_RELEASE`, kept aligned with the vendored prelude submodule); the actions
set `BUCK_PREFER_REMOTE: "true"`.

| Action | Trigger | What it runs | Placement |
| --- | --- | --- | --- |
| `build-test` | pushes to `main` | `buck2 build -M none //src/...` then `buck2 test //src/... --unstable-allow-all-tests-on-re` | build on RE (no download); test runs on RE (non-root workers, fixtures included) |
| `affected` | PRs | `//tools:supertd` snapshots the base graph from a persistent `_base` worktree, `//tools:btd` maps the diff to impacted targets, then `buck2 build -M none` + `buck2 test --unstable-allow-all-tests-on-re` on just those | same as above, scoped to impacted targets |
| `lint` | push + PR | `buck2 run //tools:prek -- run --all-files` (rustfmt, clippy, file hygiene, reindeer-in-sync) | hermetic via buck2 |

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
- **The CI test job materializes almost nothing.** With
  `--unstable-allow-all-tests-on-re`, test-run actions execute on the RE
  workers, so the runner never downloads the test binaries or the DB tool
  binaries they `exec`.
- **A *local* fixture run (dev machine, or a deliberate local invocation) pays
  the fixture inputs**: the pinned, rarely-changing tool binaries plus each
  fixture test binary.

| Artifact | Materialized size | Changes when |
| --- | --- | --- |
| `postgres-bin` (theseus-rs dist) | ~36 MiB | `PG_VERSION` bump |
| `minio-bin` (S3 fixtures only) | tens of MiB | MinIO pin bump |
| `libxml2` | ~1.5 MiB | ~never |
| each fixture test binary (debug) | ~12 MiB | the relevant code changes |

`PG_VERSION` (`17.9.0`) moves only on a deliberate bump, so the heavyweight
inputs are effectively constant across invocations.

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

## Cloud routines: the ~38 GiB disk cap

Cloud/automated Claude sessions (the code-health routines, `loom-work-checkout`,
and any scheduled agent) run in a container **quota-capped to ~38 GiB writable**.
The raw `df -h .` `Size` column reports the shared device (~252 GiB), but the
`Use%` / `Avail` columns reflect the quota — e.g. `Used 11G  Avail 27G  Use% 28%`
implies a ~38 GiB denominator, not 252. The same materialization CI dodges will
**ENOSPC** here: a whole-tree `buck2 build //src/...` *without* `-M none`
downloads every final artifact — hundreds of static, debug-info Rust binaries
over the arrow-58 / DataFusion / iceberg graph (the `engine` binary alone is
~189 MiB; ≈ 29 GiB fully materialized) — and fills the box.

So cloud routines follow CI's discipline, applied per-invocation:

- **Build verification:** `buck2 build -M none //src/...` — never bare
  `buck2 build //src/...`. `-M none` validates the build on RE without downloading
  the outputs (single-digit MiB instead of ~29 GiB).
- **Tests:** scope to the crates/targets the change touches (or the btd-affected
  set). A test binary you *run* must materialize, so `-M none` cannot help here —
  bound the cost by **scope**, and never `buck2 test //src/...` whole-tree in the box.
- **`BUCK_PREFER_REMOTE` is already on** in cloud: the buck2 shim
  (`tools/ci/buck2-proxy-shim.sh`) defaults it so hybrid actions keep their inputs
  on RE. It stops *input* materialization, not final-*output* download — it is
  complementary to `-M none`, never a substitute for it.
- **Housekeeping:** `buck2 clean` between heavy phases reclaims the full ~29 GiB
  if a session does approach the wall.

## Invariants

- New `local_only` / `uses_local_*` actions stay **off the common build path**, or
  every CI run pays to materialize their inputs.
- New fixture-backed tests use **`loom_fixture_test`**, never a bare `rust_test`.
- `BUCK2_RELEASE` and the vendored prelude submodule are bumped **together**.
- In a cloud routine, build with **`-M none`** and **scope** tests — a bare
  whole-tree `buck2 build`/`test //src/...` ENOSPCs the ~38 GiB container.
