# Remove DuckDB & DuckLake Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete every DuckDB runtime dependency and the DuckLake table format, leaving Iceberg (+ DataFusion in the engine process) as loom's only table format and serving engine.

**Architecture:** Iceberg already has complete write (`IcebergMaterializer`/`iceberg_land`/`IcebergControlPlane`), read (`IcebergCatalog` implements the `Catalog` trait), serving (`EngineServingClient` over gRPC in prod; `InProcessServingEngine` over `execute_query` in tests), and seeding (`IcebergWriter` in `fixture.rs`) paths. This change deletes the parallel DuckLake/DuckDB paths and flips every backend selector to Iceberg-only. It is a single "big-bang" PR, executed under supervision; tasks are ordered so the tree returns to green at the end of each task wherever possible.

**Tech Stack:** Rust 2024, buck2 + reindeer (third-party), sqlx compile-time queries, DataFusion + arrow/parquet 58, iceberg-rust (pinned `main`), hermetic Postgres fixtures.

**Spec:** `docs/superpowers/specs/2026-06-25-remove-duckdb-ducklake-design.md`

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`. Fixture-backed tests use the `loom_fixture_test` macro (`src/control-plane/postgres/defs.bzl`).
- **Never pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to a file and grep: `buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`. `buck2 build … | tail` is fine.
- **The full sweep is the gate, not per-crate builds:** `buck2 test //src/...`. Shared-dep regressions (the reindeer/duckdb downgrade footgun) surface only in the full fixture sweep.
- **After any reindeer re-lock, re-assert `duckdb`'s replacement is clean:** diff `Cargo.lock` + `third-party/BUCK` against `origin/main` for native/`links` crates (`libduckdb-sys`, `zstd-sys`, `ring`). (Once `duckdb` is gone, the `duckdb 1.10503.1` re-assert dance in CLAUDE.md is obsolete — delete that note in Task 8.)
- **Hermetic toolchain for any cargo step:** `eval "$(./tools/env.sh)"`.
- **Commit message style:** Conventional Commits (enforced by the `conventional-commit` hook); end commit bodies with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- **Branch:** `plan/remove-duckdb-ducklake` (already created; the spec is committed there).
- **No external pyiceberg oracle in this PR** — deferred to a FUTURE item (Task 8). Read-path correctness is gated by the re-pointed e2e suite on Iceberg serving.
- **Rebased onto `origin/main` (2026-06-25):** this plan accounts for two same-day merges on the target surface. **#195 (Auth slice 1)** added `AuthState` + `service_runtime::protect(...).merge(login_routes).merge(session_routes)` + `bootstrap_admin` around the query-api **and** ingest routers (ingest's `AppState` gained a `cp` field); the e2e `get`/`setup` already mint session tokens. Preserve all auth wiring when collapsing backend matches. **#189 (Flight SQL)** already rewrote `EngineServingClient` to a `FlightSqlClient` internally — `connect(socket)` is unchanged, so no serving-transport work is needed. **#193 (config seam) is docs-only** — the `env::var`/`parse_*` deletion targets remain valid.

---

## File Structure

**Deleted files:**
- `src/control-plane/postgres/src/snapshot.rs` (DuckLake native writer, ~60 `query!` calls)
- `src/control-plane/postgres/src/ducklake_type.rs` (type/stat mapping)
- `src/control-plane/postgres/tests/ducklake_smoke.rs`, `tests/ducklake_interop.rs`
- `src/services/ingest/tests/ducklake_interop.rs`
- `src/services/query-api/tests/spike_duckdb.rs`, `tests/inline_write_spike.rs`, `tests/quack_serving.rs`, `tests/multi_file_limit_guard.rs`, `tests/serving_backend_parse.rs`
- `src/services/transform/src/backend.rs` (collapses to Iceberg-only — see Task 5)

**Modified files (delete a branch / a struct, keep the Iceberg path):**
- `src/services/query-api/src/main.rs`, `src/serving.rs`, `src/serving_datafusion.rs`
- `src/services/query-api/tests/e2e_support.rs` (swap fixture to Iceberg serving)
- `src/services/ingest/src/main.rs`, `src/landing.rs`
- `src/services/transform/src/main.rs`
- `src/control-plane/postgres/src/catalog.rs` (delete DuckLake `Catalog` impl), `src/fixture.rs` (delete `DuckLakeWriter`), `src/lib.rs` (drop `mod snapshot;`/`mod ducklake_type;`), `tests/sqlx_cache.rs`
- `src/services/query-api/Cargo.toml` (drop `duckdb`)
- `src/control-plane/postgres/BUCK`, `src/control-plane/postgres/defs.bzl`, `src/services/query-api/BUCK`, `src/services/ingest/BUCK`, `src/services/transform/BUCK` (drop targets + `duckdb = True`)
- `tools/sqlx-prepare.sh`, `third-party/BUCK` (regen), `.sqlx/` (regen)
- `CLAUDE.md`, `ARCHITECTURE.md`, `README.md`, `docs/ROADMAP.md`, `docs/FUTURE.md`, `docs/ISSUES.md`
- `deploy/` (Helm/images — query-api engine dependency)

**Untouched:** everything in `migrations/` (the `ducklake_*` tables are created by DuckDB's extension at ATTACH time, never by a loom migration), `iceberg_mirror.*`, the engine service, the `ServingEngine` trait, `EngineServingClient`, `IcebergWriter`, `IcebergMaterializer`, `IcebergControlPlane`.

---

## Task 1: Prove one e2e test ports to Iceberg serving (de-risk the fixture swap)

The whole removal hinges on the 20 query-api e2e tests running against Iceberg serving instead of `EmbeddedDuckDb`. `e2e_support.rs` already contains `InProcessServingEngine` (wraps `engine_serving::execute_query` over an `IcebergCatalog`) and `fixture.rs` already exports `IcebergWriter`. This task proves the seam end-to-end on ONE representative test before fanning out, so we discover any Iceberg-serving gap on a small surface.

**Files:**
- Modify: `src/services/query-api/tests/e2e_support.rs`
- Modify: `src/services/query-api/tests/governed_read.rs` (representative e2e)
- Modify: `src/services/query-api/BUCK` (the `governed-read` target only, for now)

**Interfaces:**
- Consumes: `InProcessServingEngine::new(catalog: IcebergCatalog) -> InProcessServingEngine` (in `e2e_support.rs`); `IcebergWriter` (in `control_plane_postgres::fixture`); `IcebergCatalog::new(pool: PgPool) -> IcebergCatalog` (in `control_plane_postgres::iceberg_catalog`).
- Produces: a new `setup` returning an Iceberg-backed serving engine, and a `get` taking `Arc<dyn ServingEngine>`. Exact new signatures defined below; Tasks 2–5 rely on them.

- [ ] **Step 1: Read the current fixture and the Iceberg seam.**

Read these in full before editing — they define the exact types you must wire:
- `src/services/query-api/tests/e2e_support.rs` (`setup`, `get`, `InProcessServingEngine`, `land`, `prop`, helpers)
- `src/control-plane/postgres/src/fixture.rs` lines 594–916 (`IcebergWriter`, `SeedCol`)
- `src/services/engine-serving/src/serving.rs` lines 454–465 (`execute_query` signature)

- [ ] **Step 2: Change only the `get` parameter type to a trait object.**

The current `get` already mints a session token (`session_token(&cp, subject)`) and wraps the router with `service_runtime::protect(...)` for auth (added by #195). **Change ONLY the `eng` parameter type** from the concrete `Arc<EmbeddedDuckDb>` to `Arc<dyn query_api::serving::ServingEngine>` — leave the body (session_token, AppState construction, `protect` wrapping, request/header building, response parsing) exactly as-is. `EmbeddedDuckDb: ServingEngine`, so existing callers keep compiling; new Iceberg callers now work too.

```rust
// e2e_support.rs — change ONLY this line:
//   was: eng: Arc<EmbeddedDuckDb>,
//   now: eng: Arc<dyn query_api::serving::ServingEngine>,
```

- [ ] **Step 3: Change `setup` to seed Iceberg and return an Iceberg serving engine.**

Rewrite `setup` to seed via `IcebergWriter` (not `DuckLakeWriter`) and return an `Arc<dyn ServingEngine>` built from `InProcessServingEngine` over an `IcebergCatalog`. Keep the same ontology/link topology the test relies on. Note the exact current signatures (verified against the rebased tree):
- `IcebergWriter::new(pool: PgPool, pg_dsn: String) -> IcebergWriter` — **synchronous, no `.await`** (fields: `pool`, `pg_dsn`, `warehouse: TempDir`).
- `InProcessServingEngine::new(catalog: IcebergCatalog) -> InProcessServingEngine`.
- `IcebergCatalog::new(pool: PgPool) -> IcebergCatalog`.

```rust
// e2e_support.rs — new signature (was: -> (PgControlPlane, EmbeddedDuckDb, DuckLakeWriter))
pub async fn setup(fx: &PgFixture) -> (PgControlPlane, Arc<dyn query_api::serving::ServingEngine>) {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let pg_dsn = fx.pg_dsn(&db); // DSN helper used by IcebergWriter (see fixture.rs / engine wire.rs)
    let writer = IcebergWriter::new(pool.clone(), pg_dsn); // SYNC — no .await
    // ... seed the same three tables (customer / orders / line_items) via `writer`,
    //     define the same ontology types + links as today, using land()/prop() ...
    let catalog = IcebergCatalog::new(pool.clone());
    let eng: Arc<dyn query_api::serving::ServingEngine> =
        Arc::new(InProcessServingEngine::new(catalog));
    (cp, eng)
}
```

(Fill the seed body by translating each existing `DuckLakeWriter`-based `land()` in today's `setup` to the `IcebergWriter` equivalent — read the current `IcebergWriter` methods in `fixture.rs` (~line 603+) for the seed API; the table/column shapes are identical, only the writer changes. The two-value return means every caller's `let (cp, eng, _writer) = setup(...)` becomes `let (cp, eng) = setup(...)`.)

- [ ] **Step 4: Update `governed_read.rs` call sites to the new signatures.**

Change `let (cp, eng, _writer) = setup(&fx).await;` to `let (cp, eng) = setup(&fx).await;` and pass `eng` (already `Arc<dyn ServingEngine>`) into `get(...)`. Remove any `DuckLakeWriter`/`EmbeddedDuckDb` imports from this file.

- [ ] **Step 5: Flip the `governed-read` BUCK target off duckdb.**

In `src/services/query-api/BUCK`, on the `governed-read` target remove `duckdb = True` (the test no longer shells out to duckdb-cli; it uses the in-process Iceberg engine + Postgres fixture).

- [ ] **Step 6: Run the representative test.**

Run: `buck2 test //src/services/query-api:governed-read > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass`. If it fails because `execute_query` can't see seeded Parquet (the `serving_store: None` argument), STOP and resolve the warehouse/object-store wiring here — that is the gap this task exists to surface, and it must be fixed before fanning out.

- [ ] **Step 7: Commit.**

```bash
git add src/services/query-api/tests/e2e_support.rs src/services/query-api/tests/governed_read.rs src/services/query-api/BUCK
git commit -m "test(query-api): port governed_read e2e to in-process Iceberg serving"
```

---

## Task 2: Port the remaining e2e tests off DuckDB (4 sub-tasks)

> **Revised after Task 1 discovery.** The e2e tests do NOT share one fixture — porting is a *mix* of six shapes. Each sub-task implementer reads `/workspace/.superpowers/sdd/e2e-port-reference.md` (the proven `governed_read.rs` pattern + per-file bucket map + seed/serve signatures) as its requirements. Rules common to all: preserve every assertion and data value; translate seeding (→ `IcebergWriter::seed_arrays`) and serving (→ `InProcessServingEngine`) only; remove all duck imports; drop `duckdb = True` from each ported BUCK target. Do NOT delete `EmbeddedDuckDb`/`e2e_support::setup`/`land` here — Task 3/6 remove them once nothing depends on them. Each sub-task ends green on its own targets and commits.

### Task 2A — Bucket A (shared-fixture) + the Iceberg seed helpers
- Add `setup_iceberg(fx) -> (PgControlPlane, Arc<dyn ServingEngine>, IcebergWriter)` to `e2e_support.rs`, seeding the same customer→orders→line_items chain + ontology + two FK links via `IcebergWriter`, serving via `InProcessServingEngine`.
- Port `multi_hop_traversal_e2e.rs`, `inverse_hops_e2e.rs` to call it.
- Gate: `buck2 test //src/services/query-api:multi-hop-traversal-e2e //src/services/query-api:inverse-hops-e2e > /tmp/t2a.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t2a.log` → Pass.
- Commit: `test(query-api): port shared-fixture e2e tests (multi-hop, inverse-hops) to Iceberg serving`.

### Task 2B — Bucket B (land()-based HTTP route tests)
- Port `association_e2e.rs`, `graph_reach_e2e.rs`, `graph_path_e2e.rs`, `graph_union_e2e.rs`, `graph_tail_e2e.rs`: replace each local `land()` call with `IcebergWriter::seed_arrays`, serve via `InProcessServingEngine` through `e2e_support::get`.
- Gate: the five matching targets green (`graph-reach-e2e`, `graph-path-e2e`, `graph-union-e2e`, `graph-tail-e2e`, `association-e2e`).
- Commit: `test(query-api): port graph/association HTTP e2e tests to Iceberg serving`.

### Task 2C — Bucket C (self-contained tests)
- Port `bind_read_e2e.rs`, `link_traversal.rs`, `derived_properties_e2e.rs`, `typed_filter_e2e.rs`, `action_e2e.rs`: rewrite each local seed to `IcebergWriter::seed_arrays` and serve via `InProcessServingEngine`. `action_e2e.rs` also swaps `DuckLakeActionWriter` → `IcebergActionWriter` (constructor args per `http_wire_e2e.rs`'s Iceberg arm).
- Gate: the five matching targets green.
- Commit: `test(query-api): port self-contained read/action e2e tests to Iceberg serving`.

### Task 2D — Deletions + dual-backend trim
- DELETE (duck-specific / engine-being-removed): `serving_engine.rs`, `serving_types.rs` (low-level `EmbeddedDuckDb` mechanics — engine is going away; engine-serving crate has its own tests), `spike_duckdb.rs`, `inline_write_spike.rs`, `quack_serving.rs`, `multi_file_limit_guard.rs` (guards a DuckDB-engine `LIMIT` bug, `iss-multi-file-limit-misread`, removed with the engine), `serving_backend_parse.rs` (tests `parse_serving_backend`, deleted in Task 3). Remove each file's BUCK target.
- TRIM `http_wire_e2e.rs`: delete the `ducklake_backend()` arm + its helpers; keep the `iceberg_backend()` arm; remove `duckdb = True`.
- Gate: `buck2 test //src/services/query-api/... > /tmp/t2d.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t2d.log` → Pass. (query-api still builds against the `duckdb` crate here — `EmbeddedDuckDb` is deleted in Task 3.) At this point NO query-api e2e target sets `duckdb = True`.
- Commit: `test(query-api): delete DuckDB-specific e2e tests; trim http_wire dual-backend to Iceberg`.

---

## Task 3: Make query-api Iceberg-only and drop the `duckdb` crate

**Files:**
- Modify: `src/services/query-api/src/main.rs` (delete the `ServingBackend::DuckLake` arm + `EmbeddedDuckDb`/`DuckLakeActionWriter` construction)
- Modify: `src/services/query-api/src/serving_datafusion.rs` (delete `ServingBackend` enum's DuckLake variant + `parse_serving_backend`)
- Modify: `src/services/query-api/src/serving.rs` (delete `EmbeddedDuckDb`, `to_duck`, `from_duck`, `DuckLakeActionWriter`)
- Modify: `src/services/query-api/Cargo.toml` (remove `duckdb`)
- Modify: `src/services/query-api/BUCK` (remove `//third-party:duckdb` from `query-api` deps)

**Interfaces:**
- Consumes: `EngineServingClient::connect(socket: impl Into<String>)`, `IcebergActionWriter::new(...)`, `build_iceberg_catalog(&cfg)` (all already present in `main.rs`'s Iceberg arm).
- Produces: a `main()` with no backend match — Iceberg unconditionally; `LOOM_ENGINE_SOCKET` required (fail-fast).

- [ ] **Step 1: Collapse the serving-backend match to the Iceberg path — preserving auth wiring.**

> ⚠️ Drift note (rebased onto #195 auth + #189 Flight SQL): `main.rs` now builds `AuthState`, optionally runs `bootstrap_admin`, and wraps the router with `service_runtime::protect(...).merge(login_routes(...)).merge(session_routes(...))` before `serve`. Do NOT rewrite the whole tail — make the MINIMAL deletion below and leave all auth/serve code after it untouched. `EngineServingClient` already uses Flight SQL internally; `connect(socket)` is unchanged, so no serving-transport change is needed.

Make exactly these deletions:
- Delete `let backend = parse_serving_backend(std::env::var("LOOM_SERVING_BACKEND")...)?;`.
- Replace the whole `let (serving, action_engine): (...) = match backend { ServingBackend::DuckLake => {...} ServingBackend::Iceberg => { <BODY> } };` with the Iceberg arm's `<BODY>` inlined, assigned to the same `(serving, action_engine)` bindings the rest of `main` already uses:

```rust
// replaces the entire `match backend { ... }` expression; keep the same bindings:
let (serving, action_engine): (Arc<dyn ServingEngine>, Arc<dyn ActionEngine>) = {
    let inline_byte_limit = std::env::var("LOOM_INLINE_BYTE_LIMIT")
        .ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(DEFAULT_INLINE_BYTE_LIMIT);
    let flush_byte_threshold = std::env::var("LOOM_FLUSH_BYTE_THRESHOLD")
        .ok().and_then(|v| v.parse::<i64>().ok()).unwrap_or(DEFAULT_FLUSH_BYTE_THRESHOLD);
    let engine_socket = std::env::var("LOOM_ENGINE_SOCKET")
        .map_err(|_| -> Box<dyn std::error::Error> {
            "LOOM_ENGINE_SOCKET must be set (query-api serves reads only via the engine)".into()
        })?;
    let catalog = Arc::new(build_iceberg_catalog(&cfg).await?);
    let action: Arc<dyn ActionEngine> = Arc::new(IcebergActionWriter::new(
        catalog, pool.clone(), inline_byte_limit, flush_byte_threshold,
    ));
    (Arc::new(EngineServingClient::connect(engine_socket).await?), action)
};
```

Leave the subsequent `AuthState` / `bootstrap_admin` / `protect(router(AppState { cp, serving, action_engine }), auth_state).merge(...)` / `serve` lines EXACTLY as they are. Remove now-unused imports (`EmbeddedDuckDb`, `DuckLakeActionWriter`, `ServingBackend`, `parse_serving_backend`, `local_store` if unused).

- [ ] **Step 2: Delete `parse_serving_backend` + the `ServingBackend` enum.**

In `serving_datafusion.rs` delete the `ServingBackend` enum and `parse_serving_backend` (lines ~99–115). Grep the crate for any remaining reference: `grep -rn "ServingBackend\|parse_serving_backend" src/services/query-api/src` → expect none.

- [ ] **Step 3: Delete `EmbeddedDuckDb` and friends from `serving.rs`.**

Delete the `EmbeddedDuckDb` struct + its `ServingEngine` impl, `to_duck`, `from_duck`, and `DuckLakeActionWriter`. Keep the `ServingEngine` trait, `Rows`, `ServingError`, `SqlValue` mapping used by the engine client.

- [ ] **Step 4: Drop the `duckdb` dependency.**

In `src/services/query-api/Cargo.toml` remove the `duckdb = { version = "1", features = ["bundled"] }` line. In `src/services/query-api/BUCK` remove `"//third-party:duckdb"` from the `query-api` library `deps`.

- [ ] **Step 5: Regenerate third-party rules and re-lock.**

```bash
eval "$(./tools/env.sh)"
cargo generate-lockfile
./tools/buckify.sh
```
Then diff for native-crate drift: `git diff origin/main -- Cargo.lock third-party/BUCK | grep -iE "duckdb|libduckdb|zstd-sys|ring|duckdb-loadable" | head -50`
Expected: only `duckdb`/`libduckdb-sys` (and crates pulled in solely by duckdb) removed; no downgrade of `zstd-sys`/`ring`. If `duckdb` re-asserted-pin or an unrelated native crate moved, STOP and reconcile per the CLAUDE.md footgun.

- [ ] **Step 6: Build + test query-api with no duckdb crate.**

Run: `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass`. Confirm no `duckdb` build: `grep -rn "duckdb" src/services/query-api/src` → expect none.

- [ ] **Step 7: Commit.**

```bash
git add src/services/query-api Cargo.lock third-party/BUCK
git commit -m "feat(query-api): Iceberg-only serving; remove EmbeddedDuckDb and the duckdb crate"
```

---

## Task 4: Make ingest Iceberg-only

**Files:**
- Modify: `src/services/ingest/src/main.rs` (delete the `LandingBackend::DuckLake` arm)
- Modify: `src/services/ingest/src/landing.rs` (delete `LandingBackend` enum, `parse_landing_backend`, `DuckLakeMaterializer`; `LandingMaterializer` keeps only the Iceberg impl)
- Modify: `src/services/ingest/tests/` (re-point `runtime_land`, `bind` to Iceberg; DELETE `ducklake_interop.rs`)
- Modify: `src/services/ingest/BUCK` (remove `duckdb = True` from `runtime-land`/`bind`; delete `ducklake-interop` target)

- [ ] **Step 1: Collapse the landing-backend match — preserving auth wiring.**

> ⚠️ Drift note (rebased onto #195 auth): ingest `main.rs` now builds `AuthState`, optionally runs `bootstrap_admin`, constructs the router as `router(AppState { materializer, cp })` (note the added `cp` field), and wraps it with `service_runtime::protect(...).merge(login_routes(...)).merge(session_routes(...))`. Make the MINIMAL deletion below; leave all auth/`cp`/serve code untouched.

Delete `let backend = parse_landing_backend(std::env::var("LOOM_LANDING_BACKEND")...)?;` and replace the `match backend { LandingBackend::DuckLake => {...} LandingBackend::Iceberg => { <BODY> } }` with the Iceberg `<BODY>` inlined, bound to the same `materializer`:

```rust
// replaces the entire `match backend { ... }`; keep the same `materializer` binding:
let materializer: Arc<dyn LandingMaterializer> = {
    let inline_byte_limit = std::env::var("LOOM_INLINE_BYTE_LIMIT")
        .ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(DEFAULT_INLINE_BYTE_LIMIT);
    let flush_byte_threshold = std::env::var("LOOM_FLUSH_BYTE_THRESHOLD")
        .ok().and_then(|v| v.parse::<i64>().ok()).unwrap_or(DEFAULT_FLUSH_BYTE_THRESHOLD);
    let catalog = Arc::new(build_iceberg_catalog(&cfg).await?);
    Arc::new(IcebergMaterializer { catalog, pool, inline_byte_limit, flush_byte_threshold })
};
```

Leave the subsequent `AuthState` / `bootstrap_admin` / `protect(router(AppState { materializer, cp }), auth_state).merge(...)` / `serve` lines EXACTLY as they are. (If the deleted DuckLake arm was the only consumer of a `let pg = ...`/`control_plane(pool, ...)` binding that auth now also needs for `cp`, keep that binding — auth depends on it.)

- [ ] **Step 2: Delete the DuckLake landing code.**

In `landing.rs` delete the `LandingBackend` enum, `parse_landing_backend`, the `DuckLakeMaterializer` struct + impl, and the `land_ducklake` import. Keep `LandRequest`, the `LandingMaterializer` trait, `IcebergMaterializer`. (If `LandRequest::schema`/`batches` are now only used by the deleted DuckLake path, leave them — Iceberg uses `ipc_body`/`columns`; removing them is optional cleanup, not required. Note in commit if kept.)

- [ ] **Step 3: Re-point ingest tests; delete the interop test.**

Re-point `runtime_land.rs` and `bind.rs` to assert against the Iceberg landing path (they already exercise `land` through the `AppState`; ensure they construct `IcebergMaterializer`, not the duck path). Delete `tests/ducklake_interop.rs` and its `ducklake-interop` BUCK target. Remove `duckdb = True` from `runtime-land`/`bind`.

- [ ] **Step 4: Test ingest.**

Run: `buck2 test //src/services/ingest/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass`.

- [ ] **Step 5: Commit.**

```bash
git add src/services/ingest
git commit -m "feat(ingest): Iceberg-only landing; remove DuckLakeMaterializer"
```

---

## Task 5: Make transform Iceberg-only

**Files:**
- Delete: `src/services/transform/src/backend.rs` (whole module — both variants/parse collapse to Iceberg)
- Modify: `src/services/transform/src/main.rs` (drop `parse_transform_backend`; build `IcebergControlPlane` unconditionally)
- Modify: `src/services/transform/src/lib.rs` (drop `mod backend;` / re-exports)
- Modify: `src/services/transform/tests/` (re-point `transform_e2e`, `overwrite_e2e`, `compact_e2e`, `typed_transform_e2e` to Iceberg)
- Modify: `src/services/transform/BUCK` (remove `duckdb = True`; drop `backend.rs` from srcs)

- [ ] **Step 1: Collapse `main.rs`.**

> Drift note: transform is a queue-driven worker (no HTTP router/`AppState`), so — unlike query-api/ingest — it gained no auth wiring from #195. Confirm there is no `protect(...)`/`serve` tail here before editing; if there isn't (expected), this is a plain collapse.

Delete `let backend = parse_transform_backend(...)?;` and the match. Build the handler control-plane straight-line:

```rust
let pg = service_runtime::control_plane(pool, cfg.lock_timeout);
let store: Arc<dyn ObjectStore> = Arc::new(service_runtime::local_store(&cfg.data_path)?);
let catalog = build_iceberg_catalog(&cfg).await?;
let cp_for_handler: Arc<dyn ControlPlane> = Arc::new(IcebergControlPlane::new(pg.clone(), catalog));
// ... worker setup unchanged ...
```

(`store`/`local_store` may now be unused — remove if so.)

- [ ] **Step 2: Delete `backend.rs` and its wiring.**

Delete the file; remove `mod backend;` and any `pub use backend::*;` from `lib.rs`/`main.rs`; remove `backend.rs` from the BUCK `srcs`.

- [ ] **Step 3: Re-point transform tests.**

Update the four e2e tests to construct the Iceberg handler path (they should already mostly target the backend-neutral queue; ensure no `TransformBackend::DuckLake` / duck seeding remains). Remove `duckdb = True` from each target.

- [ ] **Step 4: Test transform.**

Run: `buck2 test //src/services/transform/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass`.

- [ ] **Step 5: Commit.**

```bash
git add src/services/transform
git commit -m "feat(transform): Iceberg-only output; remove transform DuckLake backend"
```

---

## Task 6: Remove the DuckLake format from the control plane

**Files:**
- Delete: `src/control-plane/postgres/src/snapshot.rs`, `src/ducklake_type.rs`
- Delete: `src/control-plane/postgres/tests/ducklake_smoke.rs`, `tests/ducklake_interop.rs`
- Modify: `src/control-plane/postgres/src/catalog.rs` (delete `impl Catalog for PgControlPlane` — the DuckLake reader; `IcebergCatalog` is the sole `Catalog`)
- Modify: `src/control-plane/postgres/src/lib.rs` (drop `mod snapshot;`, `mod ducklake_type;` and re-exports)
- Modify: `src/control-plane/postgres/src/fixture.rs` (delete `DuckLakeWriter` struct + all its methods; keep `PgFixture`, `IcebergWriter`, `MinioFixture`, `BootThrottle`, `SeedCol`)
- Modify: `src/control-plane/postgres/tests/sqlx_cache.rs` (drop the `DuckLakeWriter::seed` probe step)
- Modify: `src/control-plane/postgres/BUCK` (delete the DuckLake-specific test targets; re-point the snapshot/catalog tests — see below)

- [ ] **Step 1: Audit who calls the DuckLake `Catalog` impl.**

The query-api Iceberg path uses `IcebergCatalog`, not `PgControlPlane`'s `Catalog`. Confirm: `grep -rn "impl Catalog for PgControlPlane\|\.catalog()" src/` and verify no surviving production caller depends on the DuckLake `Catalog` reader. (If a `ControlPlane::catalog()` accessor returns the DuckLake impl and is still referenced, re-point it to `IcebergCatalog` or delete the accessor.)

- [ ] **Step 2: Delete the DuckLake writer/type/catalog code.**

Delete `snapshot.rs`, `ducklake_type.rs`, the `impl Catalog for PgControlPlane` block in `catalog.rs`, and the matching `mod`/`use` lines in `lib.rs`. Delete `DuckLakeWriter` from `fixture.rs`.

- [ ] **Step 3: Handle the snapshot/catalog tests.**

These targets (`snapshot-append`, `snapshot-create`, `snapshot-rollback`, `snapshot-conformance`, `snapshot-replace`, `catalog`, `ducklake-smoke`, `ducklake-interop`) test the DuckLake writer/reader. Delete the ones that test the deleted DuckLake writer (`snapshot-*`, `ducklake-*`). For `catalog` — if it tests the DuckLake `Catalog` impl, delete it; the Iceberg `Catalog` is covered by `iceberg_catalog`-targeted tests. Remove every deleted target and every `duckdb = True` flag from `src/control-plane/postgres/BUCK`.

- [ ] **Step 4: Update `sqlx_cache.rs`.**

Remove the `let writer = DuckLakeWriter::new(...); writer.seed(...)` probe (it exists only to make `ducklake_*` tables present). The test then boots Postgres, applies migrations, and re-`describe`s each committed `.sqlx/query-*.json` — which, after Task 6's deletions, reference only loom-owned schemas. Remove `duckdb = True` from the `sqlx-cache-check` target.

- [ ] **Step 5: Build the postgres crate (it must compile with no `ducklake_*` queries).**

Run: `buck2 build //src/control-plane/postgres:postgres > /tmp/t.log 2>&1; grep -E "BUILD SUCCEEDED|error\[|failed" /tmp/t.log`
Expected: success. A `query!` referencing a deleted `ducklake_*` table would fail the offline-cache build here. (The `.sqlx` cache still contains stale `ducklake_*` entries; that's fixed in Task 7. The build reads the cache, so it still succeeds — the freshness test in Step 6 is the real gate.)

- [ ] **Step 6: Run the postgres test sweep.**

Run: `buck2 test //src/control-plane/postgres/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass`. Note: `sqlx-cache-check` may FAIL here if stale `ducklake_*` entries remain in `.sqlx/` — that is expected and fixed in Task 7. If only `sqlx-cache-check` fails on `ducklake_*` describe, proceed; any other failure must be resolved now.

- [ ] **Step 7: Commit.**

```bash
git add src/control-plane/postgres
git commit -m "feat(control-plane): remove DuckLake writer, type mapping, Catalog impl, and DuckLakeWriter fixture"
```

---

## Task 7: Build/toolchain cleanup — delete duckdb targets, regen `.sqlx`

**Files:**
- Modify: `src/control-plane/postgres/defs.bzl` (remove the `duckdb` param + `DUCKDB_BIN`/`DUCKDB_EXTENSION_DIR` wiring)
- Modify: `src/control-plane/postgres/BUCK` (delete `:duckdb-cli`, `:duckdb-cli.gz`, `:duckdb-extensions`, `:ducklake-ext.gz`, `:postgres-scanner-ext.gz`, `:quack-ext.gz`, and `DUCKDB_VERSION`)
- Modify: `tools/sqlx-prepare.sh` (delete the ATTACH-ducklake block)
- Regenerate: `src/control-plane/postgres/.sqlx/`

- [ ] **Step 1: Remove the `duckdb` param from the macro.**

In `defs.bzl`, delete the `duckdb = False` parameter and the `if duckdb:` block that sets `DUCKDB_BIN`/`DUCKDB_EXTENSION_DIR`. Confirm no caller still passes it: `grep -rn "duckdb = True" src/` → expect none (all removed in Tasks 2–6).

- [ ] **Step 2: Delete the duckdb build targets.**

In `src/control-plane/postgres/BUCK` delete the `DUCKDB_VERSION` constant, `:duckdb-cli.gz`, `:duckdb-cli`, `:ducklake-ext.gz`, `:postgres-scanner-ext.gz`, `:quack-ext.gz`, and `:duckdb-extensions`. Keep `:postgres-bin`, `:libxml2`, `:minio-bin`, `:migrations`.

- [ ] **Step 3: Strip duckdb from `sqlx-prepare.sh`.**

Delete the "4b. ATTACH a DuckLake catalog" block (the `DUCKDB=…`, `EXTDIR=…`, and the `"$DUCKDB" -c "…ATTACH 'ducklake:…'…"` lines). Remove `DLDATA` from the `mktemp`/cleanup lines if now unused. The script then goes: boot postgres → migrations → `cargo sqlx prepare`.

- [ ] **Step 4: Regenerate the `.sqlx` cache.**

```bash
./tools/sqlx-prepare.sh
```
Then confirm the stale `ducklake_*` entries are gone: `grep -rl "ducklake_" src/control-plane/postgres/.sqlx/ || echo "clean"`
Expected: `clean`.

- [ ] **Step 5: Verify the freshness test now passes.**

Run: `buck2 test //src/control-plane/postgres:sqlx-cache-check > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass`.

- [ ] **Step 6: Commit.**

```bash
git add src/control-plane/postgres tools/sqlx-prepare.sh
git commit -m "build: drop duckdb-cli/extension targets, fixture flag, and sqlx ducklake ATTACH; regen .sqlx"
```

---

## Task 8: Docs, registers, deploy, and the final full-tree gate

**Files:**
- Modify: `CLAUDE.md`, `ARCHITECTURE.md`, `README.md`
- Modify: `docs/ROADMAP.md`, `docs/FUTURE.md`, `docs/ISSUES.md`
- Modify: `deploy/` (Helm chart + query-api image as needed)

- [ ] **Step 1: Update the registers (use the `loom-docs-update` conventions).**

- `docs/FUTURE.md`: `fut-replace-ducklake-decision` → `status:dropped` (overturned by this removal); `fut-engine-serving-ducklake-relocation` → `dropped`; `fut-ducklake-s3-routing` → `dropped`. Add a new `fut-iceberg-external-oracle` item (`status:deferred`) recording the three infra options (local-only pip genrule / vendored wheels / rules_python) and why deferred (no Python-dep machinery).
- `docs/ISSUES.md`: `iss-multi-file-limit-misread` → `status:fixed` (DuckDB-specific workaround removed with the engine).
- `docs/ROADMAP.md`: add a `road-remove-duckdb-ducklake` item `status:done`, `spec:2026-06-25-remove-duckdb-ducklake-design`, `pr:-`.
- Run `bash tools/docs.sh validate` → expect no errors.

- [ ] **Step 2: Rewrite the prose docs.**

- `CLAUDE.md`: delete the DuckLake-as-table-format paragraphs, the "re-assert the `duckdb 1.10503.1` pin" guidance, and the duckdb specifics in the reindeer-downgrade footgun (keep the general native-crate-drift warning). Update the "Project status" / non-goals to say Iceberg is the sole format + the engine is the sole serving path.
- `ARCHITECTURE.md`: rewrite the "DuckDB as the serving engine" and "DuckLake as the table format" principles to Iceberg + DataFusion-in-engine; drop the Quack-serving open questions tied to DuckDB.
- `README.md`: update the Foundry→loom mapping and any DuckDB/Quack mention to the engine-wire reality.

- [ ] **Step 3: Audit and update deploy.**

Inspect `deploy/` (apko/Wolfi images + Helm chart). Ensure the query-api deployment sets `LOOM_ENGINE_SOCKET` and has a hard dependency on a reachable engine (sidecar or service), consistent with the fail-fast boot check from Task 3. Remove any DuckDB/DuckLake env defaults (`LOOM_SERVING_BACKEND`/`LOOM_LANDING_BACKEND`/`LOOM_TRANSFORM_BACKEND`) from chart values/images.

- [ ] **Step 4: Residue sweep.**

Run: `grep -rni duckdb src/ deploy/ tools/ ; grep -rni ducklake src/ deploy/ tools/`
Expected: no hits (or only intentional, e.g. a historical note in a docs/spec file — verify each remaining hit is deliberate).

- [ ] **Step 5: Full-tree gate.**

Run: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass` with zero failures. This is the definition-of-done gate — a per-crate green is not sufficient.

- [ ] **Step 6: Final native-crate drift check.**

Run: `git diff origin/main -- Cargo.lock third-party/BUCK | grep -iE "^\-.*(duckdb|libduckdb)" | head; git diff origin/main -- Cargo.lock | grep -iE "zstd-sys|^\+.*ring " | head`
Expected: `duckdb`/`libduckdb-sys` removed; `zstd-sys`/`ring` unchanged (no downgrade).

- [ ] **Step 7: Commit.**

```bash
git add CLAUDE.md ARCHITECTURE.md README.md docs/ deploy/
git commit -m "docs/deploy: Iceberg as sole format/engine; retire DuckDB/DuckLake notes; defer external oracle"
```

---

## Self-Review notes (addressed in this plan)

- **Spec coverage:** serving removal (T1–T3), format removal (T6), landing/transform (T4–T5), build/fixtures/targets (T2–T7), sqlx (T6–T7), migrations (untouched — stated), deploy (T8), registers/docs (T8), external-oracle deferral (T8). All spec sections map to a task.
- **Transactional-commit property:** no code change needed — `IcebergControlPlane`/`do_update_table` already own it; the plan only deletes the DuckLake parallel path.
- **Ordering rationale:** e2e tests are re-pointed to Iceberg serving (T1–T2) *before* `EmbeddedDuckDb` is deleted (T3), so the suite never goes red for lack of a serving engine. The single known intentional red window is `sqlx-cache-check` between T6 and T7 (stale `.sqlx` entries), explicitly called out and closed in T7.
- **Risk surfaced first:** T1 isolates the one real unknown (does in-process Iceberg serving read `IcebergWriter`-seeded data, incl. the `serving_store` argument) on a single test before fan-out.
