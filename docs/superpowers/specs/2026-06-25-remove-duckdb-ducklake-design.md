# Remove DuckDB & DuckLake — Iceberg as the sole table format and serving engine

**Date:** 2026-06-25
**Status:** design approved, pending spec review
**Supersedes:** the 2026-06-22 direction recorded in `fut-replace-ducklake-decision` ("Iceberg-default, DuckLake kept as fallback + test oracle"). This spec **overturns** that decision: DuckDB and DuckLake are removed outright.

## Goal

Delete every DuckDB runtime dependency **and** the DuckLake table format from loom, leaving **Iceberg** (data + catalog, served by DataFusion inside the engine process) as the only way loom writes and reads tables. This is a single-PR "big bang" removal, executed under supervision.

Non-goal: migrating any existing DuckLake data. There is no committed DuckLake data anyone depends on (dev/exploratory only); DuckLake catalogs and Parquet are dropped, not converted. Fresh Iceberg catalogs going forward.

## Background

DuckDB and DuckLake entered loom as the original serving engine + table format. Two name-distinct things became coupled:

1. **DuckDB the runtime** — the `duckdb` crate (bundled), `EmbeddedDuckDb` (`ServingEngine` impl in `query-api/src/serving.rs`), `DuckLakeActionWriter`, the hermetic `duckdb-cli` test fixtures, and the `spike_duckdb` ABI gate. This is the only code with an actual `duckdb` dependency.
2. **DuckLake the table format** — loom's own native writer (`snapshot.rs`), type/stat mapping (`ducklake_type.rs`), the DuckLake `Catalog` reader impl (`catalog.rs`), the landing materializer, and the `ducklake_*` Postgres catalog tables. None of this depends on the `duckdb` crate, but its sole purpose is to produce catalogs DuckDB can read.

A complete DataFusion + Iceberg alternative already exists behind the `ServingEngine` trait seam: query-api reads via `EngineServingClient` → the engine process over gRPC (the documented "query-api as wire client" direction, PR #181). Removing DuckDB simply makes that the only path.

### Why this is safe now (verified against the tree)

- **Iceberg write path is complete.** `src/control-plane/postgres/src/iceberg_landing.rs` covers what `land_ducklake` did (Arrow → schema → Parquet → snapshot+lineage commit), with one accepted gap (`iss-iceberg-inline-visibility`).
- **Transactional commit is preserved/strengthened.** `iceberg_sql_catalog/catalog.rs::do_update_table` commits snapshot + mirror projection + lineage + inline end-cap in ONE Postgres transaction; object-store reads happen pre-transaction (manifests are immutable/content-addressed), so the tx body is pure local PG guarded by a pointer CAS.
- **`IcebergCatalog` fully implements the `Catalog` trait** (`current_snapshot`/`snapshots`/`files`/`schema`), plus `files_with_stats` for pruning-aware reads that DuckLake never had.
- **The `ducklake_*` tables are not loom migrations.** DuckDB's ducklake extension materializes them at `ATTACH` time. Removal touches **zero** files in `migrations/`.

## The one architectural consequence

query-api stops being able to serve reads in-process. After this change it is **purely a wire client**: it requires `LOOM_ENGINE_SOCKET` and a running engine process to answer any read. We make this a fail-fast boot check (error if `LOOM_ENGINE_SOCKET` is unset). This drives a deploy change (see Deploy).

## Scope of removal

### Pure deletions (Rust)

- `src/services/query-api/src/serving.rs` — delete `EmbeddedDuckDb`, `to_duck`/`from_duck`, `DuckLakeActionWriter`. Keep the `ServingEngine` trait and `EngineServingClient`.
- `src/control-plane/postgres/src/snapshot.rs` — delete the whole file (the ~60 compile-time `query!`/`query_scalar!` calls against `ducklake_*`).
- `src/control-plane/postgres/src/ducklake_type.rs` — delete the whole file.
- `src/control-plane/postgres/src/catalog.rs` — delete `impl Catalog for PgControlPlane` (the DuckLake reader). `IcebergCatalog` becomes the sole `Catalog`.
- `src/services/ingest/src/landing.rs` — delete `DuckLakeMaterializer`; `LandingMaterializer` collapses to the Iceberg impl.
- `src/services/transform/` — delete the DuckLake backend module.
- Tests/fixtures: `query-api/tests/spike_duckdb.rs`; both `ducklake_interop.rs` (postgres + ingest); `ducklake_smoke.rs`; the `DuckLakeWriter` helper in `src/control-plane/postgres/src/fixture.rs`.

### Change-in-place (Rust)

- `query-api/src/main.rs`, `ingest/src/main.rs`, `transform/src/main.rs` — delete the `parse_*_backend` functions and the backend enum match; wire Iceberg unconditionally. Delete `LOOM_SERVING_BACKEND`, `LOOM_LANDING_BACKEND`, `LOOM_TRANSFORM_BACKEND`. Keep `LOOM_ENGINE_SOCKET`; make it mandatory for query-api with a fail-fast boot check.
- The ~25 serving e2e tests in `query-api/tests/` — re-point from `EmbeddedDuckDb` to the engine-client serving path. These now stand up the engine process in their fixture instead of duckdb-cli. The shared `e2e-support` library is the place to add the engine-backed fixture helper so the change is made once, not per file.

### Build targets & fixtures (explicit, exhaustive)

- `src/services/query-api/Cargo.toml` — drop `duckdb`. Then `./tools/buckify.sh` regenerates `third-party/BUCK`, removing `duckdb`/`libduckdb-sys` (and any crates that were only pulled in via the duckdb feature union).
- `src/control-plane/postgres/BUCK` — delete the `:duckdb-cli` target, the `:duckdb-extensions` target, and the vendored ducklake-extension target(s)/http_archives they reference.
- `src/control-plane/postgres/defs.bzl` — remove the `duckdb` parameter from `loom_fixture_test` (and the `DUCKDB_BIN` / `DUCKDB_EXTENSION_DIR` env wiring it adds).
- Every BUCK target carrying `duckdb = True` — remove the flag. (Enumerate via `grep -rn "duckdb = True" src/`.)
- `tools/sqlx-prepare.sh` — remove the `ATTACH ducklake` block and the duckdb-cli/extensions build steps; regenerate `.sqlx`.
- `src/control-plane/postgres/tests/sqlx_cache.rs` — drop the `DuckLakeWriter::seed` step; the freshness test then validates only Iceberg-path queries (and will catch any surviving `ducklake_*` query reference).

### Docs

- `CLAUDE.md` — delete the "re-assert the duckdb 1.10503.1 pin" guidance, the `duckdb`-downgrade footgun note's duckdb specifics, and the DuckLake-as-table-format paragraphs (they become wrong).
- `ARCHITECTURE.md` — rewrite the "DuckDB as the serving engine" and "DuckLake as the table format" principles to Iceberg + DataFusion-in-engine.
- `README.md` — update the Foundry→loom mapping and any DuckDB/Quack-serving mention to reflect the engine-wire reality.

### Untouched

- Everything in `migrations/` (the `ducklake_*` tables were never loom migrations).
- The `iceberg_mirror.*` schema and projections.
- The engine service and `LOOM_ENGINE_SOCKET` plumbing.
- The `ServingEngine` trait and `EngineServingClient`.

## External oracle: deferred to FUTURE

The independent-engine validation that `ducklake_interop` provided (a real external engine reads loom's output) would ideally be replaced by a **pyiceberg** guardrail. During planning we found loom has **no Python-dependency machinery** — a hermetic CPython 3.13.6 is wired in `toolchains/BUCK`, but there is no pip flow, no vendored wheels, no `rules_python`, and not one `python_test` target in `src/`. Building a pyiceberg oracle therefore means net-new third-party-Python infrastructure, with no clean option:

- **Genrule + `pip install` at build** — needs network at build time, which buck2 RE forbids; would have to be a local-only genrule (like the `uses_xz` ones) and need allowed-hosts in cloud CI. Not hermetic.
- **Vendor pyiceberg + its full transitive wheel set as `http_archive` targets** — hermetic and RE-clean, but large/tedious; likely bigger than the DuckDB removal itself.
- **DuckDB's iceberg extension as oracle** — re-introduces a duckdb-cli fixture immediately after deleting it; self-defeating.

**Decision:** defer the external oracle. This removal ships with loom's own DataFusion + iceberg-rust reader (via `InProcessServingEngine` over `IcebergCatalog`) as the test — self-consistent, not independent. The external pyiceberg oracle becomes a deferred `fut-` item recording the three infra options above. This keeps the removal PR all-deletion plus in-place simplification, with **no** net-new third-party dependency.

This reverses the brainstorming-time choice to add the oracle in-scope: that choice was made before the absence of Python-dep infra was known.

## Deploy

query-api can no longer self-serve, so deployment must guarantee an engine endpoint:

- Audit `deploy//` (apko/Wolfi images + Helm chart).
- Ensure the query-api deployment has `LOOM_ENGINE_SOCKET` wired and a hard dependency on a reachable engine (sidecar or service), consistent with the fail-fast boot check.

## Documentation registers

Handled via `loom-docs-update` as the work lands:

- `fut-replace-ducklake-decision` → resolved/dropped (this spec overturns it).
- `fut-engine-serving-ducklake-relocation` → dropped (no DuckLake left to relocate).
- `fut-ducklake-s3-routing` → dropped (moot).
- `iss-multi-file-limit-misread` → closed (it was a DuckDB-specific `ORDER BY`/`LIMIT` workaround; the code is gone).
- Add a `road-` item recording this removal, linked to this spec.
- Add a `fut-` item: external pyiceberg (or other independent-engine) Iceberg oracle, recording the three infra options (local-only pip genrule / vendored wheels / rules_python) and that it was deferred for lack of Python-dependency machinery.

## Testing strategy

- **Primary gate:** full `buck2 test //src/...` green. A per-crate green build is *not* sufficient — the buckify cascade and any `ducklake_*` query straggler surface only in the full fixture sweep (`query-api`/`worker`/`control-plane` fixture tests).
- **Residue check:** `grep -rni duckdb src/` returns zero (or only intentional, documented residue).
- **`.sqlx` freshness:** the `sqlx-cache-check` test passes with no live ducklake catalog attached — proving no surviving compile-time query references `ducklake_*`.
- **Self-consistent serving validation:** the re-pointed e2e suite (now `InProcessServingEngine` over `IcebergCatalog`, seeded by `IcebergWriter`) stays green — this is the read-path correctness gate in lieu of an external oracle.
- **third-party/lock diff:** diff `third-party/BUCK` and `Cargo.lock` against the merge-base for native/`links` crates (`libduckdb-sys`, `zstd-sys`, `ring`) to catch an unintended downgrade/cascade from the re-lock.

## Risks (live during supervised execution)

1. **buckify cascade.** Dropping `duckdb` may ripple through the reindeer feature union and move/remove unrelated crates. Mitigation: diff `third-party/BUCK` + lock against merge-base; run full `buck2 test //src/...`.
2. **e2e fixture rewrite is the bulk of the work.** The ~25 serving tests must stand up the engine process in-fixture instead of duckdb-cli. Verify the engine-backed fixture pattern exists (or build it once in `e2e-support`) before mass-porting.
3. **`.sqlx` regen without a live ducklake catalog.** Confirm no surviving `query!` references `ducklake_*`; the `sqlx_cache.rs` freshness test is the backstop.
4. **Deploy regression.** A query-api pod without an engine endpoint now fails to serve. The fail-fast boot check makes this loud rather than silent.

## Verification gate (definition of done)

- `buck2 test //src/...` fully green.
- `grep -rni duckdb src/` clean.
- `third-party/BUCK` / lock diff shows only the expected duckdb-family removals, no unintended native-crate downgrades.
- Docs registers, `ARCHITECTURE.md`, `README.md`, `CLAUDE.md` updated.
