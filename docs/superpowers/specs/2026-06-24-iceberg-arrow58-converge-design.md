# Converge the tree on arrow 58 via iceberg-`main`

_Design spec. Direction-setting only — the implementation plan is written separately by the work agent that claims `road-iceberg-arrow58-converge`._

## Problem

loom straddles two arrow/parquet majors. The **iceberg** stack is pinned to arrow/parquet **57** (`iceberg 0.9.1`, the latest crates.io release, pulls `arrow-array 57.3.1` / `parquet 57.3.1`), while **DataFusion 54** + **DuckDB** + the `ingest` / `query-api` services are on arrow/parquet **58**. iceberg is the laggard.

The split has concrete costs:

- Per-boundary `RecordBatch` conversions between the 57 and 58 arrow types wherever the iceberg write path meets the DataFusion serving path.
- Duplicated `arrow-*-57` / `parquet-57` target families in `third-party/BUCK` shadowing the default 58 family.
- A `parquet-57`-vs-default-`parquet` footgun (two parquet majors in one graph).
- `road-engine-wire-flight` had to pin `arrow-flight` to **57** to match the iceberg chain, holding the Flight wire a major behind the rest of the engine.

The arrow-58 migration is **already merged on iceberg-rust `main`** (its `Cargo.toml` pins `arrow = "58"` / `parquet = "58"`) but is **not yet in a published crate** — the latest release `0.9.1` (2026-05-06) is still arrow 57. The original deferral (`fut-iceberg-arrow58-converge`) waited for iceberg-rust to *publish* the post-0.9.1 release. This spec commits to the other strategy: **source iceberg from a pinned `main` commit now** and converge the tree to one arrow major (58) without waiting for the release.

Converge **up** (iceberg → 58), never down. Downgrading DataFusion to an older arrow-57 release is a real feature regression, and removing DuckDB would not free DataFusion's arrow pin.

## Goal

One arrow major everywhere. After this slice:

- iceberg is sourced from a pinned `main` SHA carrying the arrow-58 migration.
- `arrow-flight` and the first-party arrow-57 consumers (`control-plane-postgres`, `engine`, `worker`) are on arrow/parquet 58 in lockstep.
- The `arrow-*-57` / `parquet-57` target families in `third-party/BUCK` are gone, collapsed into the single 58 family.
- The per-boundary 57↔58 batch conversions are deleted.
- The iceberg side shares arrow types directly with the DataFusion serving path (helps an eventual `fut-replace-ducklake-decision` / DuckDB-removal story).

## Scope

**One atomic slice.** iceberg cannot move to arrow 58 in isolation: the first-party crates that share its arrow types (`RecordBatch`, `Schema`, the parquet reader, the Flight codec, the stats footer reader) must move in the same commit or the build breaks. The work has two coupled internal phases — (1) re-source iceberg, (2) the arrow 57→58 converge — but they land together.

Out of scope: any iceberg feature work beyond what the API delta forces; the DuckLake/DuckDB-removal decision (`fut-replace-ducklake-decision`); the eventual swap back to a published crate (covered under *Exit* below, a one-line follow-up when iceberg-rust publishes).

## Approach

### Sourcing mechanism — pinned git dependency

reindeer runs in non-vendored http_archive mode: every third-party dep downloads a `.crate` tarball from crates.io, and the tree has **zero** git-sourced deps today. The prelude ships a `git_fetch` rule (`prelude/git/git_fetch.bzl`), and reindeer in non-vendored mode emits a `git_fetch` rule for any crate whose `Cargo.lock` source is `git+...#<sha>`. So the git-dependency path is mechanically supported; this slice introduces the tree's first git source.

Steps:

1. In the three consuming manifests (`src/control-plane/postgres/Cargo.toml`, `src/services/engine/Cargo.toml`, `src/services/worker/Cargo.toml`), change `iceberg = "0.9"` to a pinned git dependency:
   `iceberg = { git = "https://github.com/apache/iceberg-rust", rev = "<pinned-sha>" }`
   Pin a **specific commit SHA**, not a floating branch — reproducible builds, and a bump is an explicit reviewable change like every other loom pin (buck2 release, prelude submodule, duckdb).
2. `cargo generate-lockfile` (hermetic cargo via `eval "$(./tools/env.sh)"`) so `Cargo.lock` gains the `source = "git+https://github.com/apache/iceberg-rust#<sha>"` entry.
3. `./tools/buckify.sh` regenerates `third-party/BUCK`; reindeer emits a `git_fetch` rule for iceberg and any sibling iceberg-rust member crates it pulls (iceberg is a workspace member, so the rule strips into the right subdir via `crate_root`).
4. Document the pin in CLAUDE.md's third-party section alongside the other pins, with the bump procedure (change the SHA, re-lock, re-buckify).

### Arrow 57→58 converge

Bump `arrow-flight` and the first-party arrow-57 consumers from 57 → 58 in the same commit, absorbing:

- The **arrow major API churn** at `RecordBatch` / `Schema` / parquet-reader call sites.
- The **Flight encode/decode** path — `road-engine-wire-flight`'s `arrow-flight = 57` pin comes off and moves to 58.
- The **stats footer reader** (`src/control-plane/postgres/src/iceberg_stats.rs`), a parquet-57-typed footer decode.
- The **iceberg `0.9 → 0.x` API delta** across the ~20 `iceberg::` symbols loom imports — the writer chain (`DataFileWriterBuilder`, `ParquetWriterBuilder`, `RollingFileWriterBuilder`, location generator), `io::{StorageFactory, StorageConfig, LocalFsStorageFactory}`, `arrow::schema_to_arrow_schema`, and the `spec::*` types (`Schema`, `NestedField`, `PrimitiveType`, `DataFile`).
- The `arrow-*-57` / `parquet-57` target families in `third-party/BUCK` collapse to the single 58 family; the `parquet-57` named-dep wiring in the consuming BUCK files is removed.

## Acceptance gates

- **Full** `buck2 test //src/...` green — not per-crate. The documented `reindeer update`/duckdb-downgrade footgun lands failures in crates the diff never touched (`query-api`/`worker` fixture tests), so a green per-crate build is not sufficient evidence.
- The `.sqlx` cache stays fresh (the `sqlx-cache-check` test passes) and the `duckdb 1.10503.1` pin is held.
- `tools/clippy-all.sh` clean; rustfmt clean.
- `third-party/BUCK` contains no `arrow-*-57` / `parquet-57` targets after the converge.

## Risks & mitigations

- **`main` API instability between pin bumps.** Mitigated by pinning a specific SHA; the tree only sees `main` as of a reviewed commit, never a moving target.
- **`reindeer update` re-resolving and downgrading `duckdb`** (1.10503.1 → 1.10501.0), which breaks every DuckLake serving test with a catalog-version mismatch. The documented guard applies: after re-locking, diff `Cargo.lock` against the merge-base for native/`links` crates (`libduckdb-sys`, `zstd-sys`, `ring`); if duckdb moved, `cargo update -p duckdb --precise 1.10503.1` then re-buckify. Run the **full** suite before committing.
- **git-fetch build-script ordering in `buckify.sh`.** The script's archive-ordering fix currently reorders only `http_archive` ahead of its `buildscript_run`. A git-fetched crate with a build script may need the same treatment; verify and extend the fix if a build script regresses on a git source.
- **Arrow sub-dep patch conflict.** iceberg `main` may pull an arrow-58 patch that differs from DataFusion's exact 58 patch; resolve the graph to a single arrow-58 patch so the families genuinely collapse.

## Exit when iceberg publishes

When iceberg-rust publishes the post-0.9.1 release carrying arrow 58, the git dependency becomes a one-line swap back to a registry dependency: `iceberg = "0.x"` in the three manifests, arrow stays at 58, drop the SHA-pin note from CLAUDE.md, re-lock, re-buckify. No arrow churn — that work is already done by this slice. Track as a small follow-up; not a blocker.
