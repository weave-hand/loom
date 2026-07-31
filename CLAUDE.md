# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

loom is well past the scaffold stage — there is substantial application code, and all three service pillars are live. The **control plane** (`src/control-plane/`: `core` traits + domain types, `memory` fake, `postgres` adapter, `testkit` contracts, `worker`) implements all five concerns — queue, catalog, ontology, ACL, lineage — and its hardening pass is complete (concurrency test, handler-panic policy, per-concern split, pagination, proptest, committed `.sqlx`, and a `tracing` pass). Three **services** ship on top. `src/services/ingest/` has the transactional snapshot-commit primitive, the landing materializer (Arrow → inferred Iceberg schema → Parquet → snapshot+lineage commit), dataset→model binding (validated promotion of a landed dataset to an ontology type), and a DataFusion-driven multi-file write path (SessionContext → size-estimated repartition → N Snappy Parquet files → per-file Iceberg stats); `src/services/query-api/` has the governed object-read path with typed-object JSON serialization — query-api is a **zero-DataFusion wire client** over internal Flight SQL to the **engine service** (the sole serving path, which runs DataFusion against the Iceberg mirror), plus governed link traversal (relational reads across FK- and join-table-backed links), governed typed-insert actions, and governed UPDATE/DELETE actions (identity-targeted PATCH + delete via whole-table copy-on-write, with coarse + fine-grained ACL enforcement on the affected row). **Transform** ships as queue-driven jobs on the zero-pool worker (`src/services/worker/src/transform.rs`): physical SQL and typed (ontology-vocabulary) transforms — inputs stream over Arrow Flight, compute is DataFusion in the worker, and the commit is a single `EngineControl::CommitTransform` RPC; typed outputs are conformance-gated before any write. **The networked service shells are built:** both services run as binaries on the shared `service_runtime` and expose plain-HTTP endpoints (ingest: `POST /datasets/{schema}/{table}` over Arrow IPC; query-api: `GET /objects/{type}` and `GET /objects/{from}/links/{link}`). An MVP **deploy** (apko/Wolfi OCI images + a Helm chart) ships from the `deploy//` buck2 cell. **Not built yet / deferred:** ontology actions (typed governed write-backs); the **external SQL wire** (exposing loom's internal Flight SQL surface to external clients — deliberately deferred; the internal `CommandStatementQuery` surface is built, adding a TCP listener + auth is the remaining step); distributed DataFusion / Ballista; and richer reads (derived/aggregate properties, multi-hop traversal). The slice-by-slice status of record lives in the GitHub issue tracker — labeled `roadmap` (committed work), `idea` (deferred), and `bug` (known defects); consult and update it as capabilities land (see **Work tracking (GitHub Issues)** below). The rest of this file documents the build system, which is the part most likely to bite you.

## What loom is

An **open-source take on Palantir Foundry** — a typed-object data platform with built-in lineage and governance. Three Rust services (Ingest, Transform workers, Query API) on DataFusion, with **Iceberg as the sole table format** and **Postgres as a unified control plane** that holds the Iceberg mirror catalog, ontology, ACL policy, job queue, and lineage events in separate schemas of the same database. Parquet files live on S3/MinIO; the mirror catalog references them. The **engine service** is the sole serving path, running DataFusion against the Iceberg mirror; **query-api** is a zero-DataFusion wire client over internal Flight SQL (`CommandStatementQuery`) — it inlines params, sends SQL to the engine, and decodes the Arrow IPC result. No embedded DuckDB.

`README.md` is the public-facing pitch (including the section-by-section Foundry → loom capability mapping and explicit non-goals). **Read `ARCHITECTURE.md` before doing design work** — it covers the candidate component layout, why Postgres-as-control-plane was chosen, and the open questions that still need answers (multi-writer ingest, ACL pushdown limits, ontology migration, Ballista escalation, GC, queue reliability, tenancy). The architecture is explicitly framed as exploratory; treat its choices as defaults to argue with, not as committed decisions.

## Build system

- Build everything: `buck2 build //...`
- Build a specific target: `buck2 build //:hello_world`
- Run a target's output: `buck2 run //:<target>`
- Test: `buck2 test //src/...` (see **Testing** below).
- Build outputs land under `/buck-out` (gitignored).

The repo vendors the **buck2 prelude as a git submodule** at `prelude/` (see `.gitmodules`), pinned to a commit dated alongside the local `buck2` binary's build. Fresh clones need `git submodule update --init --recursive`. To bump the prelude: `cd prelude && git fetch && git checkout <commit>`, then commit the submodule pointer change in the parent repo. Pick a commit at or near the date of the `buck2` binary you're using; mismatched prelude/binary versions can break the build in obscure ways.

`toolchains/BUCK` uses `system_demo_toolchains()` from the prelude — fine for prototyping, but real projects are expected to copy/paste and configure those toolchains explicitly.

Remote execution runs through BuildBuddy (configured under `[buck2_re_client]` in `.buckconfig`); requires `BUILDBUDDY_API_KEY` in the env. Execution platforms come from `root//platforms:default` (see `platforms/BUCK`), which currently wires linux x86_64 and aarch64.

## Testing

- **Tests are `rust_test` integration targets only — NOT inline `#[cfg(test)]` modules.** buck2 builds a `rust_library`/`rust_binary`'s inline `#[cfg(test)] mod tests` but **never runs it** (there's no inline test runner in the build); such tests silently never execute. Put unit tests in a sibling `tests/<name>.rs` file wired as its own `rust_test` target in the crate's `BUCK` (mirror an existing one, e.g. `//src/control-plane/core:page`). The **`no-inline-tests` prek hook** (`tools/check-inline-tests.sh`) enforces this — it fails if any first-party `src/**.rs` file (outside a `tests/` dir) contains a `#[test]`/`#[tokio::test]`.
- **Run the suite:** `buck2 test //src/...`. Builds always go to RE (the RE platform runs as the non-root `buildbuddy` user — `platforms/defs.bzl`); test *runs* land on the local executor by default, since buck2/tpx only dispatches test-run actions to RE under `--unstable-allow-all-tests-on-re` (no remote test-result cache — tests re-run each invocation). Placement is an invocation-level choice: on a non-root dev machine, fixture tests (hermetic Postgres) run locally fine; on a root host (cloud sessions — `initdb`/`postgres` refuse root) pass `--unstable-allow-all-tests-on-re` so test runs go to RE — the cloud buck2 shim (`tools/ci/buck2-proxy-shim.sh`) injects it for `buck2 test`, and CI passes it explicitly in `buildbuddy.yaml`. The **`loom_fixture_test`** macro (`src/control-plane/postgres/defs.bzl`) deliberately sets no `remote_execution` profile; it injects the shared fixture env (PG binaries, `libxml2`, MinIO, the boot-throttle slot dir). **New fixture tests must use `loom_fixture_test`, not a bare `rust_test`**, or they run without that env and fail to boot the fixture.
- **Prefer `--console none` (and `-v0` for builds) over redirect-and-grep — it keeps the command a single clean `buck2 …` invocation.** `buck2`'s superconsole is what floods output and what stalls when piped to `tail`/`head`; `--console none` disables it, so you get concise output straight to stdout with no logfile, no pipe, and no compound command. Verified forms:
  - **Build:** `buck2 build -v0 --console none //src/...` — silent on success (exit 0); a failure still prints `BUILD FAILED` + the actual error (exit ≠ 0). No `> log; grep` needed.
  - **Test:** `buck2 test --console none //src/...` — prints only the `Tests finished: Pass N. Fail 0` summary line.
  - **Output/clippy path:** `buck2 build --console none --show-simple-output //x[clippy.txt]` prints the path directly — read it from the command output and `cat` that literal path in a separate step, rather than capturing via `$(buck2 …)`.
  - Keeping each command a bare `buck2 build …`/`buck2 test …` (no leading `cd`/`eval`/`OUT=$(…)`, no `; grep`) is also what lets the `Bash(buck2 …)` permission allow-rules match, so these don't prompt.
- **Only reach for redirect-to-a-file when you genuinely need the full log** (e.g. debugging a specific failure) or for `buck2 bxl`, whose script output isn't console-gated: `buck2 bxl … > /tmp/cov.log 2>&1; cat /tmp/cov.log`, or capture into `$(buck2 bxl …)` (the substitution fully consumes stdout). **Never** pipe a superconsole `buck2 test`/`bxl` through `tail`/`head` — it stalls on the unconsumed pipe and leaves zombie processes that look like a hang.
- **Shared e2e test setup lives in a support library, not copied per file.** The query-api end-to-end tests share their seed/setup and HTTP/ACL plumbing via the **`//src/services/query-api:e2e-support`** `rust_library` (`tests/e2e_support.rs`); test files `use e2e_support::{…}` and add `":e2e-support"` to their `deps`. It exports the generic seed helpers (`tref`/`land`/`prop`), the customer→orders→line_items `setup` fixture, the router driver + ACL helpers shared by the graph/object-set tests (`get`/`StubAction`/`subject_with_role`/`grant_read`), and the two id extractors (`ids` for `ObjectRows`, `ids_i64` for raw `{objects:[…]}` JSON). Per-test graph topologies stay local — each `setup` that seeds a *different* graph is intentional divergence, not duplication. When adding or touching an e2e test, reuse these helpers (and extend the library for new shared ones) rather than re-copying them — that copy-paste is exactly the duplication the `loom-duplication-fix` routine collapses.
- **Error-path logging is asserted with `#[traced_test]` + `logs_contain` (the standard).** When an error path logs an operator-visible fault (e.g. `ApiError::internal` → `tracing::error!` in `ingest/src/http.rs`), the test that exercises it asserts the logged context via `tracing_test::traced_test` + `logs_contain(...)`, not just the HTTP status — see `ingest/tests/api_error.rs`, `query-api/tests/serving_fault_logging.rs`, and `postgres/tests/fixture_timing.rs`. Gotcha: `#[traced_test]` names the captured span after the test fn, so `logs_contain("<fn-name-substring>")` can be trivially true — assert on the *message/field* content, not the fn name (see the note in `fixture_timing.rs`).
- **Test coverage (BXL, whole `//src` tree — all 22 crates incl. fixtures):** `./tools/coverage.sh` (whole codebase) or `./tools/coverage.sh control-plane/core` (one crate) runs the pipeline, prints the combined `llvm-cov` table to stdout, and writes per-crate + `combined/` `lcov.info`/`report.txt`/`combined.profdata` plus a combined `html/` into the gitignored `.loom/coverage/`. (The underlying engine is `buck2 bxl //tools/coverage:cov.bxl:cov [-- --crate <pkg>]`; the wrapper adds copy-out, HTML, and the `LOOM_COVERAGE_JOBS`→`-j` cap.) Mechanism: `tools/coverage/cov.bxl` discovers every `rust_test` under `//src` and applies the `//tools/coverage:coverage_enabled` constraint modifier (config-free — `-Cinstrument-coverage` is off by default; normal builds and `buck2 test` can never trip it), then runs each as a buck2 action — **fixtures local with postgres env, pure-logic on RE** — collecting `.profraw` via `run_one.sh`, merging and rendering with the pinned LLVM dist. **Footguns:** (1) the first instrumented build is heavy — cap it with `LOOM_COVERAGE_JOBS=<n>`; subsequent runs are cache hits. (2) **Never** run an instrumented binary outside `cov.bxl` (i.e. never `buck2 build -m //tools/coverage:coverage_enabled <test>` and execute it manually, and never `buck2 test` with the modifier) — those execute without `LLVM_PROFILE_FILE` set and litter `default_*.profraw` into the repo root. Design docs in git history: `2026-06-15-whole-codebase-coverage-bxl*`.

## Dev tools

- **`//tools:reindeer`** — third-party Rust dependency importer ([facebookincubator/reindeer](https://github.com/facebookincubator/reindeer)), pinned to a release version in `tools/BUCK`. Invoke as `buck2 run //tools:reindeer -- <args>` (but normally go through `./tools/buckify.sh` — see *Third-party Rust deps* below). The decompress genrule is labeled `uses_xz` (a borrowed label) so it runs locally rather than on RE — the host must have `zstd` on PATH. To bump reindeer: update `REINDEER_VERSION` in `tools/BUCK` and refresh each `sha256` from the new release's `.zst` assets.
- **`//tools:prek`** — git pre-commit hook runner ([j178/prek](https://github.com/j178/prek)), a fast Rust reimplementation of pre-commit, pinned in `tools/BUCK`. Invoke as `buck2 run //tools:prek -- <args>`; hooks are configured in `prek.toml` at the repo root (run them with `… -- run --all-files`, install the git hooks with `… -- install`). `prek.toml` sets `default_install_hook_types = ["pre-commit", "commit-msg", "pre-push"]` so a single `install` wires all three shim types — without it `install` only writes the pre-commit shim and the commit-msg/pre-push hooks silently never fire. Hooks span stages: most run pre-commit; `conventional-commit` (a local regex check enforcing [Conventional Commits](https://www.conventionalcommits.org/) on the message, see `tools/check-commit-msg.sh`) runs commit-msg; `buck2-build`/`buck2-test` run pre-push. The `lint` CI job only runs the pre-commit-stage hooks (`--all-files` doesn't trigger commit-msg), so the commit-message check is local-only. Ships as `.tar.gz` (not a bare `.zst` like reindeer), so the genrule untars with `tar`/`gzip` and needs no `uses_xz` label — it builds fine on RE. To bump prek: update `PREK_VERSION` in `tools/BUCK` and refresh each `sha256` from the matching `<asset>.tar.gz.sha256` sidecar on the release page.
- **`//tools:rustfmt`** — the formatter from the Rust nightly the toolchain is pinned to (`toolchains/BUCK`). Invoke as `buck2 run //tools:rustfmt -- <args>` (e.g. `-- --check src/hello/src/main.rs`). It's a wrapper genrule that points `LD_LIBRARY_PATH` at the rustc dist's libs and execs the rustfmt-preview dist binary; it bakes absolute paths, so the `uses_local_filesystem_abspaths` label keeps it off RE. x86_64 host only.
- **clippy** — two flavors. (1) The buck2-native lint, already wired via `clippy_driver` in the rust toolchain: run it as a sub-target on any Rust rule, e.g. `buck2 build '//src/hello:hello[clippy.txt]'` (or `[clippy.json]`). buck2 has no `//...[clippy.txt]` syntax, so to lint *all* first-party Rust at once use **`tools/clippy-all.sh`** — it queries every Rust target and checks each one's `[clippy.txt]` (empty == clean), so new crates are covered automatically. That script is the preferred path for CI and is what the prek `clippy` hook runs. (2) **`//tools:clippy`** — a standalone `cargo clippy` (`buck2 run //tools:clippy -- <args>`) for the ad-hoc Cargo workflow. Because the rustc dist ships no host std, **`:rust-host-toolchain`** assembles a full host toolchain with a sysroot (rustc + merged rust-std + cargo + clippy-driver) that `:clippy` wraps; same local-only label as rustfmt. (That same assembled toolchain is also what `tools/buckify.sh` puts on PATH so reindeer's `cargo metadata` is hermetic — no host rustup anywhere.) To bump either: they follow `RUST_NIGHTLY` in `toolchains/BUCK`.
- **Clippy lint policy (strict).** loom enables the **whole `clippy::pedantic` + `clippy::restriction` groups** on production lib/bin code, configured once on the `:rust` toolchain (`toolchains/BUCK`): the groups go in `warn_lints`; a large reasoned allowlist of the stylistic/contradictory/numeric lints rides in `rustc_flags` as `-Aclippy::*` (the `CLIPPY_ALLOWS` list). **Allows for an enabled *group* MUST live in `rustc_flags`, not `allow_lints`** — the prelude emits lint flags before `rustc_flags` (`build.bzl:494`) so only `rustc_flags` can override a group `-W`; the toolchain `allow_lints`/`deny_lints` fields only work for standalone (non-group) lints (see the FOOTGUN note in `toolchains/rust_dist.bzl`). The lints left *enforced* are the high-signal panic-safety/error set (`unwrap_used`, `expect_used`, `indexing_slicing`, `panic`, `get_unwrap`, `map_err_ignore`, `let_underscore_must_use`, `dbg_macro`, `todo`, ...). **Test code** is exempted from the *panic-safety* lints only (tests legitimately `unwrap`) via the **`loom_rust_test` wrapper** (`src/loom_test.bzl`, loaded into each BUCK with `rust_test`; `loom_fixture_test` injects the same `LOOM_TEST_LINT_ALLOWS`) — it injects the `-A`s via per-target `rustc_flags`, deliberately NOT the prelude's `rustc_test_flags` toolchain field (that field is unusable: `rust_binary.bzl:561` mutates the frozen provider list, breaking every `rust_test` build when it is non-empty). The three test-support sources that are not `rust_test` targets (`query-api`'s `tests/e2e_support.rs`, `testkit/src/lib.rs`, `postgres/src/fixture.rs`) carry a crate/module `#![allow(...reason)]`. To silence a lint **globally** add it to `CLIPPY_ALLOWS` with a reason; **locally** use `#[expect(lint, reason = "...")]` (bare `#[allow]` is permitted but `allow_attributes_without_reason` requires the `reason`). A nightly toolchain bump can add new group members and redden the gate. The `map_err_ignore` error-handling debt papered over during adoption has since been **fully paid down** (every tracked `#[expect]` deleted; source errors are now carried) — [`docs/error-handling-debt.md`](docs/error-handling-debt.md) is the resolution log. See the design doc in git history: `2026-06-26-stricter-clippy-config-design`.
- **python** is hermetic too: `toolchains/BUCK` wires the prelude's `remote_python_toolchain` over a pinned python-build-standalone CPython (`CPYTHON_VERSION` / `CPYTHON_RELEASE`), giving `toolchains//:python_bootstrap` (the buck2 bootstrap toolchain) and `toolchains//:python` (full). `tools/buckify.sh` and `tools/env.sh` put that same interpreter on `PATH`, so no system `python3` is needed anywhere — locally, in CI, or on RE. To bump: update `CPYTHON_VERSION` / `CPYTHON_RELEASE` and the sha256s in `toolchains/BUCK` from a python-build-standalone release.

- **`//tools:btd` / `//tools:supertd`** — Buck Target Determinator ([buck2-change-detector](https://github.com/facebookincubator/buck2-change-detector)): maps a set of changed files to the affected buck2 targets, so hooks/CI can build & test only what a diff impacts. Upstream ships no prebuilt binaries, so loom consumes them from a fork that adds a release workflow — **[rsJames-ttrpg/buck2-change-detector](https://github.com/rsJames-ttrpg/buck2-change-detector)** (tag a new `vX.Y.Z` there to cut a release). Each release tarball holds both binaries; `tools/BUCK` extracts each per-arch. Flow: `buck2 run //tools:supertd -- targets root//... --output base.jsonl` (graph snapshot), then `buck2 run //tools:btd -- --changes changes.txt --base base.jsonl --universe root//...`. **Changes-file format** is sapling/`hg status`-style — one line per file, `<M|A|R|D><space><project-relative-path>` (e.g. `M src/hello/src/main.rs`); bare paths are rejected. btd prints impacted targets grouped by level (0 = direct, N = Nth-order dependents). To bump: update `BTD_VERSION` in `tools/BUCK` and refresh each `sha256` from the release's `.tar.gz.sha256` sidecars.
- **`//tools:jq`** — [jqlang/jq](https://github.com/jqlang/jq), the JSON processor, vendored as a static binary so the code-health routines (`loom-complexity`, `loom-duplication`) render deterministically without depending on host jq. The Linux release assets are raw static binaries (no archive), so the genrule is a bare `cp + chmod` and builds fine on RE. Invoke as `buck2 run //tools:jq -- <args>`. Two naming quirks: jq's tags carry a `jq-` prefix (`jq-X.Y.Z`, not `vX.Y.Z`), so `JQ_VERSION` in `tools/BUCK` carries that prefix verbatim; the release assets use `amd64`/`arm64` arch suffixes (not Rust triples). To bump: update `JQ_VERSION` in `tools/BUCK` and refresh each `sha256` from the `<asset>.sha256` sidecar on the release page (or `sha256sum` the downloaded asset).
- **`//tools:rust-code-analysis`** — Mozilla's multi-language code-metrics CLI, consumed from a loom fork ([weave-hand/rust-code-analysis](https://github.com/weave-hand/rust-code-analysis)) that adds a release workflow. Ships as `.tar.gz` per-arch with a `…-<triple>/` wrapper dir holding the `rust-code-analysis-cli` binary; the genrule strips the wrapper with `--strip-components=1`. Drives the `loom-complexity` routine. Invoke as `buck2 run //tools:rust-code-analysis -- -m -O json -p src -o <dir>`; the binary is exposed on PATH as `rust-code-analysis-cli` via `tools/env.sh`. To bump: update `RCA_VERSION` in `tools/BUCK` and refresh each `sha256` from the new release's `.tar.gz` assets.
- **`//tools:lucidshark-duplo`** — duplicate-code detector ([toniantunovi/lucidshark-duplo](https://github.com/toniantunovi/lucidshark-duplo)); drives the `loom-duplication` routine. Ships as `.tar.gz` with the `lucidshark-duplo` binary at the archive root (no wrapper dir). Invoke as `buck2 run //tools:lucidshark-duplo -- <file-list> --json -m 20`. To bump: update `DUPLO_VERSION` in `tools/BUCK` and refresh each `sha256` from the new release's `.tar.gz` assets.
- **`//tools:muntjac`** — uv.lock → buck2 rules importer ([weave-hand/muntjac](https://github.com/weave-hand/muntjac)), the Python analog of reindeer; consumed from a loom fork since upstream ships no prebuilts. Ships as `.tar.gz` with the `muntjac` binary at the archive root (no wrapper dir). Invoke as `buck2 run //tools:muntjac -- <args>`, but normally go through `./tools/pybuckify.sh` — see *Third-party Python deps* below. To bump: update `MUNTJAC_VERSION` in `tools/BUCK` and refresh each `sha256` from the new release's `.tar.gz` sidecars.
- **`//tools:uv`** — [astral-sh/uv](https://github.com/astral-sh/uv), the resolver muntjac shells out to (`uv lock`); vendored so `pybuckify.sh` needs no host uv. Ships as `.tar.gz` with a `uv-<triple>/` wrapper dir, like prek (`--strip-components=1`); each asset has a `.sha256` sidecar. To bump: update `UV_VERSION` in `tools/BUCK` and refresh each `sha256`.
- **`tools/env.sh` / `tools/loom-refresh`** — dev-shell activation. `eval "$(./tools/env.sh)"` (or `direnv allow` for the checked-in `.envrc`) puts the hermetic Rust toolchain (`cargo`/`rustc`/`rustfmt`, `cargo clippy`) and the dev-tool binaries (`reindeer`/`prek`/`btd`/`supertd`) on `PATH` via symlinks under `.loom/bin` (gitignored). The Rust toolchain's real `bin/` goes on PATH (sysroot stays auto-detected); dev tools point at the concrete per-arch genrules, not the `command_alias` trampolines (those break when symlinked). First-party `//src` binaries are exposed by name but only built/repointed by `tools/loom-refresh`, never on activation.

## Code navigation

Navigate loom's Rust with `Grep` / `Glob` / `Read` (and `Explore` subagents for
breadth). The **`rust-analyzer` LSP is NOT available in cloud / automated
sessions** — it was backed out of the cloud setup because driving it makes the
buck2 rust-project integration run check builds in a *second* `rust-analyzer`
isolation-dir buck-out, and cloud sessions lack the disk for it. So the
`rust-analyzer-lsp` plugin is not enabled in this repo's `.claude/settings.json`,
`tools/cloud-setup.sh` no longer pre-warms `//tools:rust-analyzer`, and
`tools/cloud-session-start.sh` no longer regenerates `rust-project.json`. **Don't
reach for the `LSP` tool** — `ToolSearch "select:LSP"` returns nothing here. The
**`loom-code-navigation`** skill documents grep-based navigation patterns. Local
dev can still run rust-analyzer if a developer wires it up themselves (see
`DEVELOPING.md` + their own user-global plugin enablement) — that's a per-developer
choice, not something these routines depend on.

## Third-party Rust deps

Managed by reindeer in **non-vendored (http_archive) mode** — generated rules download each crate's `.crate` from crates.io at build time; sources are not checked in. Config is `reindeer.toml` at the repo root (paths in it are relative to the repo root), pointing at the **workspace** `Cargo.toml`. First-party crates (workspace members like `src/hello`) are written by hand; reindeer only emits third-party rules.

Workflow to add/update a dependency:
1. Add it to a crate's `Cargo.toml` (e.g. `src/hello/Cargo.toml`) and refresh the workspace lock: `cargo generate-lockfile` (or `buck2 run //tools:reindeer -- update`).
2. Regenerate `third-party/BUCK`: **`./tools/buckify.sh`** — wraps `reindeer buckify` and applies an ordering fix (reindeer emits `buildscript_run` before the crate's `http_archive`, which breaks build scripts that read source files; the script moves each archive ahead of its run).
3. Depend on it from your crate's BUCK as `//third-party:<crate>` (e.g. `deps = ["//third-party:clap"]`).

Notes:
- **Build-script crates** need a decision in `third-party/fixups/<crate>/fixups.toml` (`[buildscript]\nrun = true|false`) or reindeer warns. Current fixups: `proc-macro2` (run), `quote` (no-run).
- **Cargo env macros**: crates using `env!("CARGO_PKG_*")` (e.g. clap's `#[command(version)]` → `CARGO_PKG_VERSION`) get those set automatically for third-party crates but **not** for hand-written first-party targets — supply them via the rule's `env = {...}` (see `src/hello/BUCK`).
- The prek `reindeer-check` hook runs `buckify.sh` and `git diff --exit-code third-party/BUCK` when a `Cargo.toml`/`Cargo.lock` changes, so generated rules can't drift from the manifests.
- **`object_store`'s `aws` feature has a wide blast radius.** Enabling it (required for the Iceberg S3 backend) unions graph-wide via reindeer, pulling `quick-xml`, `ring`, `reqwest` (with `rustls-native-certs`), and `md-5` into the tree — their appearance in `third-party/BUCK` is expected and not a mistake.
- **`iceberg` is sourced from a pinned `main` commit, not crates.io — the tree's only git dependency.** The three consuming manifests (`src/control-plane/postgres`, `src/services/engine`, `src/services/worker`) declare `iceberg = { git = "https://github.com/apache/iceberg-rust", rev = "<sha>" }`; reindeer's non-vendored mode emits a `git_fetch` rule for it (the prelude ships `git_fetch`; iceberg has no build script, so no ordering concern). This carries the arrow/parquet **58** migration ahead of the post-0.9.1 crates.io release, so the **whole tree is on a single arrow major (58)** — there are no `arrow-*-57`/`parquet-57` target families. **To bump the pin:** change the `rev` in those three `Cargo.toml`s, `cargo generate-lockfile` (hermetic cargo via `eval "$(./tools/env.sh)"`), then `./tools/buckify.sh` and run the **full** `buck2 test //src/...` (the iceberg `Table::builder()` now requires a `Runtime`, threaded through `SqlCatalog`; a runtime/API regression shows up only in the fixture suite, not the per-crate build). **Exit when iceberg publishes** the post-0.9.1 release: swap each git dep to `version = "0.x"`, arrow stays 58, drop this note. Deferred follow-up: `iceberg_stats.rs` can now share its parquet-footer merge logic with `datafusion_io::write::file_stats_from_bytes` (both on parquet 58) — see #570.
- **`reindeer update` can silently downgrade unrelated crates.** Step 1's `reindeer update` (= `cargo update`) re-resolves the **whole** graph, so adding one dep can move others — including downgrades of native/`links` crates. The failures land in fixture tests for crates your diff never touched — which is the tell it's a shared-dep regression, not your logic. **Guard:** after any `reindeer update`, diff the lock against the merge-base (not the current state) for native/`links` crates (`zstd-sys`, `ring`), and run the **full** `buck2 test //src/...` before committing — a green per-crate build is not enough.

## Third-party Python deps

Managed by muntjac in **non-vendored mode** — generated rules download each wheel via `http_file` at build time; sources are not checked in (the Python analog of reindeer's `http_archive` mode). Config is `muntjac.toml` at the repo root (paths relative to the repo root), pointing at `src/sdk/python/pyproject.toml` + its `uv.lock`. There is currently one manifest (`loom-sdk`); new Python packages add a manifest and a `muntjac.toml` entry the same way a new Rust workspace member adds a `Cargo.toml`.

Workflow to add/update a dependency:
1. Add it to `src/sdk/python/pyproject.toml` and refresh the lock: `buck2 run //tools:uv -- lock --project src/sdk/python`.
2. Regenerate `third-party/python/`: **`./tools/pybuckify.sh`** — `uv lock` (skipped under `--frozen`) → `muntjac vendor` → `muntjac buckify`, the Python analog of `buckify.sh`.
3. Depend on it from your crate's/target's BUCK as `//third-party/python:<pkg>` (e.g. `deps = ["//third-party/python:pyarrow"]`).

Notes:
- The prek **`muntjac-check`** hook runs `pybuckify.sh --frozen` and `git diff --exit-code third-party/python src/sdk/python/uv.lock` when `pyproject.toml`/`uv.lock`/`muntjac.toml` changes, so generated rules can't drift from the manifests — the Python `reindeer-check`.
- **`--frozen` does not catch a stale-by-mtime lock.** muntjac's staleness gate for its own (non-frozen) re-lock is a raw mtime comparison, which is meaningless against arbitrary CI checkout mtimes — that's why `pybuckify.sh --frozen`/the hook skip it entirely rather than trip false-positive in CI. Dogfooded: touching `pyproject.toml` (making it newer than `uv.lock`) with no content change still leaves `muntjac-check` green under `--frozen`. Lock freshness against manifest edits is enforced by review + the SDK's own CI builds re-resolving on a real `uv lock`, not by this hook.
- **Scoped `PACKAGE` cfg-modifier wiring.** The root `PACKAGE` registers the cfg constructor (`set_cfg_constructor`, shared with the Rust coverage-modifier path); the actual Python modifiers — host os/cpu → `//third-party/python/config:*` platform constraint, plus the single pinned `py313` version — are set by `third-party/python/PACKAGE` and `src/sdk/PACKAGE` (`set_cfg_modifiers(cfg_modifiers = MUNTJAC_HOST_MODIFIERS + ["root//third-party/python/config:py313"])`), deliberately **not** the root `PACKAGE`, so Rust target configurations stay untouched. A new Python-target directory outside `src/sdk` needs the same two-line `PACKAGE` (load `MUNTJAC_HOST_MODIFIERS` from `//third-party/python:wiring.bzl`, call `set_cfg_modifiers`) or its targets silently build unconfigured.
- **Python 3.13-only**, matching `CPYTHON_VERSION` in `toolchains/BUCK` (3.13.6) — `muntjac.toml`'s `python_versions = ["3.13"]` is the only entry; there is no multi-version matrix.
- **`manylinux = "2_28"`** on both platforms (not the older `2_17`/`2_14` tags): pyarrow's cp313 wheels ship `manylinux_2_28`-only, and 2_28 still accepts the older-tag wheels other deps (e.g. pydantic-core) publish, so it's the strictest tag that covers the whole set. Linux-only, like the Rust toolchain.
- **`[fixups] registry = "none"`** — no fixups directory configured yet; add one the day a wheel needs a build-time override (the muntjac analog of `third-party/fixups/<crate>/fixups.toml`).
- **`python_test` targets must set `remote_execution = RE_TEST_PROPS`** (from `//platforms:defs.bzl`): the prelude's inplace-par bootstrap bakes the hermetic CPython interpreter's absolute path — as recorded from the (RE) par-build action's sandbox — into the generated entrypoint's shebang, so a locally-executed test can't exec it (`/usr/bin/env: ... No such file or directory`). Pinning the test's own execution to RE is the workaround; escape hatch for RE-less environments is `-c fbcode.disable_re_tests=True` (the test then fails at exec regardless, so in practice python tests require RE until `fut-python-par-local-shebang` lands upstream).
- **muntjac ≥ v0.2.2 required.** v0.2.1's buckify silently dropped `[project.optional-dependencies]` subtrees (13→8 rules, no diagnostic) — found dogfooding the `loom-sdk` pydantic extra. Fixed upstream on `weave-hand/muntjac`; `MUNTJAC_VERSION` in `tools/BUCK` is pinned to `v0.2.2`, which walks extras too (plus a debug log and a regression fixture). Don't pin muntjac below this.

## `loom-sdk` (Python SDK)

`src/sdk/python/` ships `loom_sdk` (distribution `loom-sdk`, import `loom_sdk`): a
hand-written sync `Client` + `AsyncClient`, identical surfaces over a shared
sans-IO core (`_core.py`/`_arrow.py`/`errors.py`), covering loom's write path
(Arrow-IPC `datasets.land`/`models.land`), the admin ontology surface
(`admin.define_model`/`define_link`, which is served by **query-api**, not
ingest), and verification reads (`ontology.types`/`.type`, `datasets.list`/`.get`/
`.preview`). **Two-URL model:** ingest writes route to `ingest_url`, everything
else (reads + `/admin/*`) to `query_url`; a single `url` sets both for a
co-deployed host. `pydantic` is an optional extra (`loom-sdk[pydantic]`,
buck target `:loom-sdk-pydantic`): `LoomModel` classes whose annotations declare
ontology properties/links (`Identity[T]`, `Link[Other]`), an idempotent
bootstrap-aware `client.ontology.apply(*models)`, and `client.models.land_instances(...)`.
Run its tests with `buck2 test //src/sdk/python:units //src/sdk/python:units-pydantic //src/sdk/python:e2e`
(all three are RE-pinned — see the inplace-par shebang footgun above — so a
bare local `buck2 test` on them fails to exec; RE is required). See
`docs/system-capabilities/python-sdk.md` for the capability writeup.

## Compile-time SQL (postgres adapter)

The postgres adapter (`src/control-plane/postgres`) uses sqlx **compile-time** `query!`/`query_scalar!` for its queue/catalog/ontology/acl/lineage concerns — the SQL is verified against a real schema at build time. (`fixture.rs` deliberately stays runtime `AssertSqlSafe` — it's the test harness, not production query paths.) Requires **sqlx 0.9+**, where `SQLX_OFFLINE_DIR` is honored from the env.

- **The `.sqlx` cache** lives at `src/control-plane/postgres/.sqlx/`, is committed, and is generated by **`tools/sqlx-prepare.sh`** (the GENERATE/refresh tool): it boots the pinned `:postgres-bin`, applies the loom migrations, then runs `cargo sqlx prepare`. Run it after changing any SQL and commit the resulting `.sqlx` change.
- **Offline build wiring**: the `rust_library`'s `env` sets `SQLX_OFFLINE=true`, `SQLX_OFFLINE_DIR=$(location :sqlx-cache)/.sqlx`, and `CARGO_MANIFEST_DIR="."` — the last because buck2's clippy-driver canonicalizes `CARGO_MANIFEST_DIR` to locate `clippy.toml`, and a fake value breaks clippy. No `cargo`/`cargo metadata` runs at build time; the macros read the cache only.
- **Freshness** is enforced by the **`//src/control-plane/postgres:sqlx-cache-check` rust_test** (`tests/sqlx_cache.rs`), which runs in the normal `buck2 test //src/...` sweep — NOT a hook, no CI job, no sqlx-cli. It boots the hermetic postgres, applies migrations, then for every committed `.sqlx/query-*.json` re-runs sqlx's own `Executor::describe` against the live schema and asserts the result still matches: describe succeeds, column count/names, nullable vector, parameter count, and column types (exact — `serde_json::to_value(type_info)` reuses sqlx's `Serialize` for `PgTypeInfo`, since sqlx-core is built with the `offline` feature). The committed cache is a declared buck input via `LOOM_SQLX_DIR = $(location :sqlx-cache)/.sqlx`, so the test re-runs only when the cache (or schema) changes. CI's normal build is still a backstop — a stale/missing cache makes the `query!` macros fail the build.

## Continuous integration

**BuildBuddy Workflows are the source of truth for CI.** `buildbuddy.yaml` at the
repo root defines three actions mirroring the jobs below — `build-test` (push to
`main`, full `buck2 build`/`test //src/...`), `affected` (PRs, btd-driven impacted
build/test), and `lint` (prek hooks on all events). They run on BuildBuddy runners
co-located with the RE/cache, with VMs snapshotted/reused, so the shared per-action
setup (`tools/ci/buildbuddy-setup.sh`: pinned buck2 + zstd/bsdtar/jq + prelude
submodule init) is near-instant on warm runs. **Prerequisite:** an org secret named
`BUILDBUDDY_API_KEY` (BuildBuddy UI → Secrets) — `.buckconfig`'s `[buck2_re_client]`
reads `$BUILDBUDDY_API_KEY`, which the runner does not otherwise expose to buck2.

(The previous GitHub Actions *CI* workflow `ci.yml` and the `install-bsdtar` composite
action were removed once the BuildBuddy workflow was proven green. `release.yml` — image
and Helm publishing — and `claude.yml` (the Claude bot) remain on GitHub Actions, so
`setup-buck2` stays.)

For the *execution model* behind these actions — RE-vs-local placement, fixture-test local routing, and the materialization cost model (incl. why we don't cache buck-out) — see **`docs/build-execution.md`**. The three actions run, respectively:
- **`build-test`** (pushes to `main` only) — full `buck2 build //src/...` + `buck2 test //src/...`; `main` must always be fully green.
- **`affected`** (PRs only) — builds/tests just the first-party targets the diff impacts, via btd: it snapshots the base graph with `//tools:supertd` from a persistent `_base` worktree, then runs `//tools:btd` (`--base` + `--diff`, `--json-lines`) and feeds the impacted `root//src/...` targets into `buck2 build`/`test`. Empty impact ⇒ nothing built.
- **`lint`** (push + PR) — `buck2 run //tools:prek -- run --all-files`, so CI enforces exactly the pre-commit hooks defined in `prek.toml` (rustfmt, clippy, file checks, reindeer-in-sync) with no duplicated config. Fully hermetic via buck2 — no host Rust install (the `reindeer-check` hook's `cargo metadata` uses loom's own toolchain cargo; see `tools/buckify.sh`).

- **buck2 is pinned** via the `BUCK2_RELEASE` env (currently `2026-05-18`) to the dated [facebook/buck2 release](https://github.com/facebook/buck2/releases) — keep it aligned with the prelude submodule pin, or builds break in obscure ways. Bump both together.
- **Remote execution** runs on BuildBuddy just like local dev; the key comes from the `BUILDBUDDY_API_KEY` repo secret. (loom's executor falls back to pure-local when `[project] remote_enabled` is unset — e.g. `buck2 build --config project.remote_enabled= //…` — so a secretless local-only CI is possible if ever needed.)
- **Scope** is `//src/...` (first-party + their third-party deps). The `//tools` targets are dev-only and some are local-only genrules, so they're deliberately not built in CI.
- **buck2 install** in CI is done by the shared, idempotent `tools/ci/buildbuddy-setup.sh` (pinned buck2 + zstd/bsdtar/jq + prelude submodule init), run as each action's first step; the reused VM snapshot makes it near-instant on warm runs. (`release.yml` still installs buck2 via the `.github/actions/setup-buck2` composite action — that's why it isn't deleted.) Bump the version via the `BUCK2_RELEASE` in `tools/ci/buildbuddy-setup.sh` (kept aligned with `tools/cloud-setup.sh` + the prelude pin).
- **Avoid per-run toolchain downloads.** The workflow sets `BUCK_PREFER_REMOTE: "true"` and builds with `-M none`. Compute is already cached on BuildBuddy (~95% action-cache hits), but on a fresh runner any action that runs *locally* must materialize its inputs (LLVM, rustc, std — multiple GiB) from CAS. Preferring remote keeps those actions on RE so nothing is pulled down; `-M none` skips downloading final artifacts too. The toolchain's `assemble_sysroot` action (in `toolchains/rust_dist.bzl`) is also RE-eligible (not `local_only`) for the same reason — otherwise it forces the rustc/std dists local on every build. Net effect: a cached CI build downloads single-digit MiB (`local: 0`), versus ~4 GiB before. Keep any new `local_only`/`uses_local_*` actions off the common build path, or CI pays to materialize their inputs every run.

## Cloud routines (scheduled code-health runs)

The code-health skills (`loom-complexity`, `loom-duplication`, and the `*-fix`
remediation skills) can run as scheduled cloud sessions. Two committed artifacts
wire the environment; see `docs/build-execution.md` for the RE cost model they lean on.

- **`tools/cloud-setup.sh`** — the **setup script** (paste into the environment's
  "Setup script" field; committed for review). Runs once as root; its filesystem is
  snapshotted and reused, so it does the heavy one-time work: `apt install gh zstd`,
  installs the pinned buck2 (`BUCK2_RELEASE`, kept aligned with CI + the prelude) to
  `/usr/local/bin`, inits the prelude submodule, and **pre-warms the routines' tools**
  (`buck2 build --config project.remote_enabled= //tools:jq //tools:rust-code-analysis
  //tools:lucidshark-duplo` — forced local since the BuildBuddy key isn't available
  at setup time). Keep it under ~5 min so the snapshot can build. Needs network to
  `github.com` + `*.githubusercontent.com` in the env's allowed hosts.
- **`tools/cloud-session-start.sh`** — the **SessionStart hook** (wired in
  `.claude/settings.json`). NO-OP unless `REMOTE_ENV=true`, so it's inert for local
  dev. The cloud session injects `BUILDBUDDY_API_KEY` (remote execution — `.buckconfig`
  reads `$BUILDBUDDY_API_KEY`) and `GITHUB_TOKEN` (gh PR landing); since each Bash
  tool call starts a fresh shell from the profile, the hook persists these (and buck2's
  PATH) into `~/.bashrc` idempotently, then activates `tools/env.sh` (its builds now go
  over RE). `REMOTE_ENV` is just the "this is a cloud routine" marker — RE is driven by
  the key being present. To bump buck2: change `BUCK2_RELEASE` in `cloud-setup.sh`
  alongside `ci.yml` and the submodule pin.
- **Disk cap — mind ENOSPC.** The cloud container is **~38 GiB writable** (not the
  ~252 GiB the raw `df` Size shows), and a whole-tree `buck2 build //src/...` *without*
  `-M none` materializes ≈ 29 GiB of Rust binaries → ENOSPC. In a cloud routine, build
  with **`buck2 build -M none //src/...`** and **scope** tests to the touched/btd-affected
  targets — never a bare whole-tree `buck2 build`/`test //src/...`; `buck2 clean` between
  heavy phases reclaims the space. `BUCK_PREFER_REMOTE` is defaulted on by the buck2 shim
  (`tools/ci/buck2-proxy-shim.sh`). Full rationale: [`docs/build-execution.md`](docs/build-execution.md) → *Cloud routines: the ~38 GiB disk cap*.

## Work tracking (GitHub Issues)

All open work lives in the GitHub issue tracker, not in the tree — the
in-repo registers (ROADMAP/FUTURE/ISSUES) and the `docs/superpowers/`
spec/plan trees were migrated in #466 (their content is in git history).

- **Labels:** `area:<a>` (controlled vocab: query, iceberg, acl, ui,
  transform, ontology, devx, deploy, catalog, lineage, test, quality,
  cross-cutting, ingest, build) crossed with a kind — `roadmap` (committed),
  `idea` (deliberately deferred), `bug` (defect in shipped code) — plus
  `ready` (spec present in the issue body — claimable) and `meta` (tracking
  issue for a larger workstream; children are attached as native sub-issues
  and listed as a `- [ ] #N` checklist; metas are never claimed).
- **Lifecycle:** file → `loom-work-plan` writes a `## Spec` into the body and
  labels it `ready` → a work agent claims it via `loom-work-checkout`
  (assign + timestamped `Claimed:` comment; assignment is the mutex, the
  comment is the staleness record, grace window 240 min) → PR with
  `Closes #N` → merge closes it; the landed capability is recorded in
  `docs/system-capabilities/`.
- **Specs live in issue bodies; implementation plans are ephemeral** session
  files — planning writes zero repo files.
- **The `loom v1` Projects board** (org project #1, id `PVT_kwDOEV2iVs4BeJ8b`,
  Status field `PVTSSF_lADOEV2iVs4BeJ8bzhYmQKk`) mirrors the labels for the
  workstream overview: Backlog → Ready (spec lands, with the `ready` label) →
  In progress (claimed) → In review (PR open) → Done (PR merged). All board
  transitions go through **`tools/board-status.sh <issue> <option-id>`** — the
  single home for the ProjectV2 mutation (`61e4505c` Ready, `47fc9ee4` In
  progress, `df73e18b` In review — which nothing auto-populates — `98236657`
  Done). Labels are authoritative; a **cloud** session drives the board with the
  ambient gh token (the same fine-grained PAT that already has `project`
  permission), reaching Projects GraphQL around the App proxy via the
  `api.github.com` NO_PROXY bypass the helper self-injects — and if that egress
  is blocked it skips softly so the next local session reconciles.
- Query it like: `gh issue list --label roadmap --state open`,
  `--label ready --no-assignee`, `--label "area:acl"`. Cloud sessions use the
  GitHub MCP tools as the primary path (the App proxy 403s `gh` API calls to
  `api.github.com`; `ls-remote`/pushes still work over the git lane). The same
  `api.github.com` NO_PROXY bypass the board helper self-injects also makes `gh`
  reach the whole REST/GraphQL API in cloud — prefix `NO_PROXY=api.github.com`
  and pass `-R weave-hand/loom` (the `origin` remote points at the proxy) — which
  is how a cloud session runs `gh issue develop` for the native issue→branch
  link; MCP stays primary.

## Cell layout

Cells declared in `.buckconfig`:
- `root` → repo root (where targets like `//:hello_world` live)
- `prelude` → vendored buck2 prelude (git submodule at `prelude/`)
- `toolchains` → `toolchains/`
- `none` → alias for `fbcode`, `fbsource`, `fbcode_macros`, `buck` (so prelude rules that reference Meta-internal cells resolve without breaking)

The `config` and `ovr_config` aliases both point at `prelude`, which is what prelude rules expect when reading select() configs.
