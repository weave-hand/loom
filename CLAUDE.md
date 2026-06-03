# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

This is a freshly scaffolded [buck2](https://buck2.build) project. There is no application code yet, only the build-system skeleton and design docs. Update this file as real code lands.

## What loom is

An **open-source take on Palantir Foundry** — a typed-object data platform with built-in lineage and governance. Three Rust services (Ingest, Transform workers, Query API) on DataFusion, with DuckLake as the table format and **Postgres as a unified control plane** that holds the DuckLake catalog, ontology, ACL policy, job queue, and lineage events in separate schemas of the same database. Parquet files live on S3/MinIO; the catalog references them. All three services expose a [Quack](https://duckdb.org/docs/current/quack/overview) endpoint as the common wire protocol — clients `ATTACH` loom services like remote DuckDB catalogs, even though the engine underneath is DataFusion.

`README.md` is the public-facing pitch (including the section-by-section Foundry → loom capability mapping and explicit non-goals). **Read `ARCHITECTURE.md` before doing design work** — it covers the candidate component layout, why Postgres-as-control-plane was chosen, and the open questions that still need answers (multi-writer ingest, ACL pushdown limits, ontology migration, Ballista escalation, GC, queue reliability, tenancy). The architecture is explicitly framed as exploratory; treat its choices as defaults to argue with, not as committed decisions.

## Build system

- Build everything: `buck2 build //...`
- Build a specific target: `buck2 build //:hello_world`
- Run a target's output: `buck2 run //:<target>`
- Test: `buck2 test //...` (no tests defined yet)
- Build outputs land under `/buck-out` (gitignored).

The repo vendors the **buck2 prelude as a git submodule** at `prelude/` (see `.gitmodules`), pinned to a commit dated alongside the local `buck2` binary's build. Fresh clones need `git submodule update --init --recursive`. To bump the prelude: `cd prelude && git fetch && git checkout <commit>`, then commit the submodule pointer change in the parent repo. Pick a commit at or near the date of the `buck2` binary you're using; mismatched prelude/binary versions can break the build in obscure ways.

`toolchains/BUCK` uses `system_demo_toolchains()` from the prelude — fine for prototyping, but real projects are expected to copy/paste and configure those toolchains explicitly.

Remote execution runs through BuildBuddy (configured under `[buck2_re_client]` in `.buckconfig`); requires `BUILDBUDDY_API_KEY` in the env. Execution platforms come from `root//platforms:default` (see `platforms/BUCK`), which currently wires linux x86_64 and aarch64.

## Dev tools

- **`//tools:reindeer`** — third-party Rust dependency importer ([facebookincubator/reindeer](https://github.com/facebookincubator/reindeer)), pinned to a release version in `tools/BUCK`. Invoke as `buck2 run //tools:reindeer -- <args>` (but normally go through `./tools/buckify.sh` — see *Third-party Rust deps* below). The decompress genrule is labeled `uses_xz` (a borrowed label) so it runs locally rather than on RE — the host must have `zstd` on PATH. To bump reindeer: update `REINDEER_VERSION` in `tools/BUCK` and refresh each `sha256` from the new release's `.zst` assets.
- **`//tools:prek`** — git pre-commit hook runner ([j178/prek](https://github.com/j178/prek)), a fast Rust reimplementation of pre-commit, pinned in `tools/BUCK`. Invoke as `buck2 run //tools:prek -- <args>`; hooks are configured in `prek.toml` at the repo root (run them with `… -- run --all-files`, install the git hooks with `… -- install`). `prek.toml` sets `default_install_hook_types = ["pre-commit", "commit-msg", "pre-push"]` so a single `install` wires all three shim types — without it `install` only writes the pre-commit shim and the commit-msg/pre-push hooks silently never fire. Hooks span stages: most run pre-commit; `conventional-commit` (a local regex check enforcing [Conventional Commits](https://www.conventionalcommits.org/) on the message, see `tools/check-commit-msg.sh`) runs commit-msg; `buck2-build`/`buck2-test` run pre-push. The `lint` CI job only runs the pre-commit-stage hooks (`--all-files` doesn't trigger commit-msg), so the commit-message check is local-only. Ships as `.tar.gz` (not a bare `.zst` like reindeer), so the genrule untars with `tar`/`gzip` and needs no `uses_xz` label — it builds fine on RE. To bump prek: update `PREK_VERSION` in `tools/BUCK` and refresh each `sha256` from the matching `<asset>.tar.gz.sha256` sidecar on the release page.
- **`//tools:rustfmt`** — the formatter from the Rust nightly the toolchain is pinned to (`toolchains/BUCK`). Invoke as `buck2 run //tools:rustfmt -- <args>` (e.g. `-- --check src/hello/src/main.rs`). It's a wrapper genrule that points `LD_LIBRARY_PATH` at the rustc dist's libs and execs the rustfmt-preview dist binary; it bakes absolute paths, so the `uses_local_filesystem_abspaths` label keeps it off RE. x86_64 host only.
- **clippy** — two flavors. (1) The buck2-native lint, already wired via `clippy_driver` in the rust toolchain: run it as a sub-target on any Rust rule, e.g. `buck2 build '//src/hello:hello[clippy.txt]'` (or `[clippy.json]`). buck2 has no `//...[clippy.txt]` syntax, so to lint *all* first-party Rust at once use **`tools/clippy-all.sh`** — it queries every Rust target and checks each one's `[clippy.txt]` (empty == clean), so new crates are covered automatically. That script is the preferred path for CI and is what the prek `clippy` hook runs. (2) **`//tools:clippy`** — a standalone `cargo clippy` (`buck2 run //tools:clippy -- <args>`) for the ad-hoc Cargo workflow. Because the rustc dist ships no host std, **`:rust-host-toolchain`** assembles a full host toolchain with a sysroot (rustc + merged rust-std + cargo + clippy-driver) that `:clippy` wraps; same local-only label as rustfmt. (That same assembled toolchain is also what `tools/buckify.sh` puts on PATH so reindeer's `cargo metadata` is hermetic — no host rustup anywhere.) To bump either: they follow `RUST_NIGHTLY` in `toolchains/BUCK`.

- **`//tools:btd` / `//tools:supertd`** — Buck Target Determinator ([buck2-change-detector](https://github.com/facebookincubator/buck2-change-detector)): maps a set of changed files to the affected buck2 targets, so hooks/CI can build & test only what a diff impacts. Upstream ships no prebuilt binaries, so loom consumes them from a fork that adds a release workflow — **[rsJames-ttrpg/buck2-change-detector](https://github.com/rsJames-ttrpg/buck2-change-detector)** (tag a new `vX.Y.Z` there to cut a release). Each release tarball holds both binaries; `tools/BUCK` extracts each per-arch. Flow: `buck2 run //tools:supertd -- targets root//... --output base.jsonl` (graph snapshot), then `buck2 run //tools:btd -- --changes changes.txt --base base.jsonl --universe root//...`. **Changes-file format** is sapling/`hg status`-style — one line per file, `<M|A|R|D><space><project-relative-path>` (e.g. `M src/hello/src/main.rs`); bare paths are rejected. btd prints impacted targets grouped by level (0 = direct, N = Nth-order dependents). To bump: update `BTD_VERSION` in `tools/BUCK` and refresh each `sha256` from the release's `.tar.gz.sha256` sidecars.
- **`tools/env.sh` / `tools/loom-refresh`** — dev-shell activation. `eval "$(./tools/env.sh)"` (or `direnv allow` for the checked-in `.envrc`) puts the hermetic Rust toolchain (`cargo`/`rustc`/`rustfmt`, `cargo clippy`) and the dev-tool binaries (`reindeer`/`prek`/`btd`/`supertd`) on `PATH` via symlinks under `.loom/bin` (gitignored). The Rust toolchain's real `bin/` goes on PATH (sysroot stays auto-detected); dev tools point at the concrete per-arch genrules, not the `command_alias` trampolines (those break when symlinked). First-party `//src` binaries are exposed by name but only built/repointed by `tools/loom-refresh`, never on activation.

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

## Continuous integration

GitHub Actions, at `.github/workflows/ci.yml` (repo: `rsJames-ttrpg/loom`). All jobs install the pinned buck2 release and check out the prelude submodule recursively:
- **`build-test`** (pushes to `main` only) — full `buck2 build //src/...` + `buck2 test //src/...`; `main` must always be fully green.
- **`affected`** (PRs only) — builds/tests just the first-party targets the diff impacts, via btd. It does a second checkout at the PR base SHA, snapshots that graph with `//tools:supertd`, then runs `//tools:btd` (`--base` + `--universe`, `--json-lines`) and feeds the impacted `root//src/...` targets into `buck2 build`/`test`. Empty impact ⇒ nothing built.
- **`lint`** (all events) — `buck2 run //tools:prek -- run --all-files`, so CI enforces exactly the pre-commit hooks defined in `prek.toml` (rustfmt, clippy, file checks, reindeer-in-sync) with no duplicated config. Fully hermetic via buck2 — no host Rust install (the `reindeer-check` hook's `cargo metadata` uses loom's own toolchain cargo; see `tools/buckify.sh`).

- **buck2 is pinned** via the `BUCK2_RELEASE` env (currently `2026-05-18`) to the dated [facebook/buck2 release](https://github.com/facebook/buck2/releases) — keep it aligned with the prelude submodule pin, or builds break in obscure ways. Bump both together.
- **Remote execution** runs on BuildBuddy just like local dev; the key comes from the `BUILDBUDDY_API_KEY` repo secret. (loom's executor falls back to pure-local when `[project] remote_enabled` is unset — e.g. `buck2 build --config project.remote_enabled= //…` — so a secretless local-only CI is possible if ever needed.)
- **Scope** is `//src/...` (first-party + their third-party deps). The `//tools` targets are dev-only and some are local-only genrules, so they're deliberately not built in CI.
- **buck2 install** is the local composite action `.github/actions/setup-buck2`, shared by all jobs. It restores the binary from an `actions/cache` keyed on the release tag (so only the first run per release downloads/decompresses) and adds it to `PATH`. Bump the version via the `BUCK2_RELEASE` env in `ci.yml`.
- **Avoid per-run toolchain downloads.** The workflow sets `BUCK_PREFER_REMOTE: "true"` and builds with `-M none`. Compute is already cached on BuildBuddy (~95% action-cache hits), but on a fresh runner any action that runs *locally* must materialize its inputs (LLVM, rustc, std — multiple GiB) from CAS. Preferring remote keeps those actions on RE so nothing is pulled down; `-M none` skips downloading final artifacts too. The toolchain's `assemble_sysroot` action (in `toolchains/rust_dist.bzl`) is also RE-eligible (not `local_only`) for the same reason — otherwise it forces the rustc/std dists local on every build. Net effect: a cached CI build downloads single-digit MiB (`local: 0`), versus ~4 GiB before. Keep any new `local_only`/`uses_local_*` actions off the common build path, or CI pays to materialize their inputs every run.

## Cell layout

Cells declared in `.buckconfig`:
- `root` → repo root (where targets like `//:hello_world` live)
- `prelude` → vendored buck2 prelude (git submodule at `prelude/`)
- `toolchains` → `toolchains/`
- `none` → alias for `fbcode`, `fbsource`, `fbcode_macros`, `buck` (so prelude rules that reference Meta-internal cells resolve without breaking)

The `config` and `ovr_config` aliases both point at `prelude`, which is what prelude rules expect when reading select() configs.
