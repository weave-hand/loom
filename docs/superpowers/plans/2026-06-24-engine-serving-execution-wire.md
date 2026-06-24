# Relocate DataFusion Query Execution into the Engine Service — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move loom's DataFusion/Iceberg read execution out of `query-api` into a new engine-owned `engine-serving` crate, expose it over the engine wire as a unary `ExecuteQuery` RPC carrying Arrow-58 IPC, and make `query-api` a thin gRPC client — so query execution lives in the execution tier (reusable by transforms/compaction), not the API tier.

**Architecture:** `query-api` keeps governance + SQL compilation, inlines params, and calls the engine over its existing UDS; the engine runs DataFusion and streams back Arrow-58 IPC bytes; `query-api` decodes IPC → `Rows`. Arrow stays 58 end-to-end on the query path (no arrow-flight, which is 57-pinned for compaction); the engine crate itself only ever passes opaque IPC `bytes`, so all arrow-58/datafusion types are confined to the `engine-serving` crate.

**Tech Stack:** Rust 2024, DataFusion 54 + arrow 58 (engine-serving + query-api decode), arrow-57 (engine compaction Flight, untouched), tonic 0.14 / prost (engine wire), sqlx, buck2.

## Global Constraints

- **DataFusion 54 / arrow 58** for all new execution + decode code. The engine's compaction Flight plane stays **arrow 57** and is NOT touched. Do not add `arrow-flight` 58.
- **The query path carries Arrow IPC bytes only** across the wire (proto `bytes`); the `engine` crate must NOT name arrow-58/datafusion types — it depends on `engine-serving` and passes `Vec<u8>`. This keeps the arrow-major split contained.
- **Params are inlined client-side** (query-api `inline_params`, the injection-safe `?`→literal renderer) before the wire call; the engine receives one ready-to-run SQL string. Dynamic SQL inside providers stays `AssertSqlSafe` (existing precedent).
- **No inline `#[cfg(test)]` tests** — sibling `tests/<name>.rs` as their own targets. Fixture-backed tests (hermetic Postgres) use `loom_fixture_test`, not bare `rust_test`. Pure-logic tests use `rust_test`.
- **Engine reachability:** query-api dials the engine's existing UDS via `LOOM_ENGINE_SOCKET` (co-located; no new socket, no TCP this slice).
- **Scope:** only the DataFusion/Iceberg read backend relocates. `EmbeddedDuckDb`/DuckLake serving and `IcebergActionWriter` (writes) **stay in query-api**.
- **buck2, not cargo.** Build: `buck2 build //path:target`. Test: `buck2 test //path:target > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log` — never pipe `buck2 test` through tail/head. Clippy: `buck2 build '//path:target[clippy.txt]'` (empty == clean).
- Reworks PR #181 on branch `work/road-df-postgres-tableprovider`.

## File-structure map

- **New crate `src/services/engine-serving/`** (arrow-58, datafusion-54): the relocated execution.
  - `src/lib.rs` — exports `execute_query_to_ipc`, `EngineServingError`; `mod`s.
  - `src/provider.rs` — `PgTableProvider` (moved verbatim from query-api `pg_table_provider.rs`, error type swapped).
  - `src/serving.rs` — `IcebergMirrorTableProvider`, `register_iceberg_table`, `prune_files`, `FileSetStatistics`, `stat_to_scalar`, `base_to_arrow`, `arrow_schema_from_mirror`, `EngineServingError`, and the new `execute_query_to_ipc`.
  - `tests/` — moved provider/execution tests.
- **`src/services/engine-wire/`** — proto gains `EngineQuery`/`ExecuteQuery`; `client.rs` gains `EngineQueryClient::connect`.
- **`src/services/engine/`** — new `src/query.rs` (`EngineQueryService`); `main.rs` registers it; depends on `engine-serving`.
- **`src/services/query-api/`** — `serving_datafusion.rs` slimmed to keep only `IcebergActionWriter`/`ServingBackend`/`parse_serving_backend`/`encode_ipc_stream`/`batches_to_rows`/`arrow_to_sqlvalue`; new `EngineServingClient`; `main.rs` Iceberg arm rewired. Drops `datafusion` dep.

---

### Task 1: New `engine-serving` crate — move execution + providers, add `execute_query_to_ipc`

**Files:**
- Create: `src/services/engine-serving/Cargo.toml`, `src/services/engine-serving/BUCK`, `src/services/engine-serving/src/lib.rs`, `.../src/provider.rs`, `.../src/serving.rs`
- Move into the new crate (from query-api): the entire `src/services/query-api/src/pg_table_provider.rs`; and from `src/services/query-api/src/serving_datafusion.rs` the items `IcebergMirrorTableProvider`, `register_iceberg_table`, `prune_files`, `FileSetStatistics`, `stat_to_scalar`, `base_to_arrow`, `arrow_schema_from_mirror`, `build_inline_provider`, `to_serving`.
- Move test: only `pg_scan_sql.rs` → `src/services/engine-serving/tests/pg_scan_sql.rs` (pure-logic; its only query-api import is `query_api::pg_table_provider::build_scan_sql` → `engine_serving::provider::build_scan_sql`). Create one NEW engine-serving fixture test `execute_query_e2e.rs` (Step 4b). The other provider/execution tests (`iceberg_mirror_provider`, `iceberg_pruning_e2e`, `datafusion_register`, `datafusion_serving`, `datafusion_inline_union`, `inline_pg_provider_e2e`, `recursive_cte_over_datafusion`, `iceberg_schema_evolution_read`) **stay in query-api** and are re-pointed/rewritten in Task 4 — they assert via `query_api`'s `batches_to_rows`/`SqlValue`, which stay in query-api, so moving them would create a cross-crate `Rows` dep that can't resolve until query-api builds.

**Interfaces:**
- Produces:
  - `pub enum EngineServingError { Engine(String) }` (thiserror, `#[error("engine serving: {0}")]`) — replaces the moved code's use of `query_api::serving::ServingError`. Map every prior `ServingError::Engine(...)` → `EngineServingError::Engine(...)`. engine-serving gets its OWN `to_serving<E>(e) -> EngineServingError` copy (query-api keeps its own `-> ServingError` copy — see Task 4).
  - `pub async fn execute_query(catalog: &control_plane_postgres::iceberg_catalog::IcebergCatalog, sql: &str) -> Result<Vec<arrow::array::RecordBatch>, EngineServingError>` — the batch-level execution entry (registers live tables, runs already-inlined SQL, collects batches).
  - `pub async fn execute_query_to_ipc(catalog: &IcebergCatalog, sql: &str) -> Result<Vec<u8>, EngineServingError>` — wraps `execute_query` + Arrow-IPC-encodes the batches.
  - `pub async fn register_iceberg_table(ctx: &SessionContext, catalog: &IcebergCatalog, table: &TableRef) -> Result<(), EngineServingError>` (moved)
  - `pub struct PgTableProvider`, `pub struct IcebergMirrorTableProvider`, `pub fn prune_files(...)` (moved; re-exported from the crate root so query-api tests can import them as `engine_serving::{IcebergMirrorTableProvider, prune_files, register_iceberg_table}`)

- [ ] **Step 1: Scaffold the crate (Cargo.toml + BUCK + lib.rs)**

`src/services/engine-serving/Cargo.toml`:
```toml
[package]
name = "engine-serving"
version = "0.1.0"
edition = "2024"

[dependencies]
control-plane-core = { path = "../../control-plane/core" }
control-plane-postgres = { path = "../../control-plane/postgres" }
arrow = "58"
datafusion = { version = "54", default-features = false, features = ["parquet", "sql"] }
object_store = "0.13"
sqlx = { version = "0.9", default-features = false, features = ["runtime-tokio", "postgres"] }
async-trait = "0.1"
thiserror = "1"
time = "=0.3.47"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

`src/services/engine-serving/BUCK` — model the library target on `query-api`'s lib (same third-party arrow/datafusion/object_store/sqlx/thiserror/time/async-trait deps), `crate = "engine_serving"`, `crate_root = "src/lib.rs"`, `srcs = glob(["src/**/*.rs"])`, `deps` include `//src/control-plane/core:core`, `//src/control-plane/postgres:postgres`, `//third-party:{arrow,datafusion,object_store,sqlx,thiserror,time,async-trait,tokio}`, `visibility = ["PUBLIC"]`. Load `loom_fixture_test` from `//src/control-plane/postgres:defs.bzl` for the fixture tests.

`src/services/engine-serving/src/lib.rs`:
```rust
//! loom engine-serving: the DataFusion execution tier for Iceberg reads. Builds a
//! SessionContext over the mirror's live tables and runs compiled, param-inlined
//! SQL, returning Arrow-58 IPC bytes. Hosted by the `engine` binary and reusable by
//! transform/compaction. See
//! docs/superpowers/specs/2026-06-24-engine-serving-execution-wire-design.md.

pub mod provider;
pub mod serving;

pub use provider::PgTableProvider;
pub use serving::{
    EngineServingError, IcebergMirrorTableProvider, execute_query, execute_query_to_ipc,
    prune_files, register_iceberg_table,
};
```

(The crate-root re-exports let query-api's retained tests import `engine_serving::{register_iceberg_table, IcebergMirrorTableProvider, prune_files}` and `engine_serving::execute_query`.)

- [ ] **Step 2: Move `pg_table_provider.rs` → `engine-serving/src/provider.rs`**

`git mv src/services/query-api/src/pg_table_provider.rs src/services/engine-serving/src/provider.rs`. Then in `provider.rs` replace `use crate::serving::ServingError;` with `use crate::serving::EngineServingError as ServingError;` (keeps the body's `ServingError::Engine(...)` calls unchanged). Everything else (struct, `build_scan_sql`, `pg_rows_to_arrays`, `TableProvider` impl) is verbatim.

- [ ] **Step 3: Move the read-serving items → `engine-serving/src/serving.rs` and add the error type**

Create `serving.rs` by moving the listed items out of query-api's `serving_datafusion.rs` verbatim, with these adjustments:
- Add at top:
```rust
use crate::provider::PgTableProvider;

/// Any execution/mirror/DataFusion error → opaque engine-serving error.
#[derive(Debug, thiserror::Error)]
pub enum EngineServingError {
    #[error("engine serving: {0}")]
    Engine(String),
}
```
- Add engine-serving's OWN copy of `to_serving`: `pub(crate) fn to_serving<E: std::fmt::Display>(e: E) -> EngineServingError { EngineServingError::Engine(e.to_string()) }`. (This is a COPY — query-api retains its own `-> ServingError` version for `encode_ipc_stream`; see Task 4 Step 1. Do NOT delete query-api's `to_serving`.)
- Move `IcebergMirrorTableProvider`, `register_iceberg_table`, `build_inline_provider`, `prune_files`, `FileSetStatistics`, `stat_to_scalar`, `base_to_arrow`, `arrow_schema_from_mirror` verbatim; change every `ServingError` → `EngineServingError`. `build_inline_provider` returns `Result<Option<PgTableProvider>, EngineServingError>` and constructs `crate::provider::PgTableProvider`.
- The moved `register_iceberg_table` referenced `self.catalog`/`crate::serving::*` — it takes `catalog: &IcebergCatalog` explicitly (it already does). Keep its signature `pub async fn register_iceberg_table(ctx: &SessionContext, catalog: &IcebergCatalog, table: &TableRef) -> Result<(), EngineServingError>`.

- [ ] **Step 4: Add `execute_query_to_ipc` (the new execution entry) + a focused test**

Append to `serving.rs`:
```rust
use arrow::array::RecordBatch;
use datafusion::execution::context::SessionContext;

/// Execute already-compiled, param-inlined read-only `sql` against all live Iceberg
/// tables and return the result batches. (This is the body of the old
/// `DataFusionServingEngine::fetch_rows` minus the `Rows` flattening.)
pub async fn execute_query(
    catalog: &IcebergCatalog,
    sql: &str,
) -> Result<Vec<RecordBatch>, EngineServingError> {
    let ctx = SessionContext::new();
    for table in catalog.live_tables().await.map_err(to_serving)? {
        register_iceberg_table(&ctx, catalog, &table).await?;
    }
    let df = ctx.sql(sql).await.map_err(to_serving)?;
    df.collect().await.map_err(to_serving)
}

/// Execute `sql` and return the full result as one Arrow-58 IPC *stream* (schema
/// message + all batches). Bounded by the compiled query's LIMIT, so a single blob
/// is fine. Empty result → empty `Vec<u8>` (the client reads it as zero rows).
pub async fn execute_query_to_ipc(
    catalog: &IcebergCatalog,
    sql: &str,
) -> Result<Vec<u8>, EngineServingError> {
    let batches = execute_query(catalog, sql).await?;
    let mut buf = Vec::new();
    if let Some(first) = batches.first() {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &first.schema())
            .map_err(to_serving)?;
        for b in &batches {
            w.write(b).map_err(to_serving)?;
        }
        w.finish().map_err(to_serving)?;
    }
    Ok(buf)
}
```

- [ ] **Step 4b: Add engine-serving's self-contained execution test (batch-level)**

Create `src/services/engine-serving/tests/execute_query_e2e.rs` — engine-serving's own coverage of file+inline execution, asserting on `RecordBatch`es (NOT `Rows`/`SqlValue`, so no query-api dep). Seed a table with file rows + live inline rows via `control_plane_postgres::fixture::IcebergWriter` (mirror the seeding in the old `datafusion_inline_union.rs`), then:
```rust
let batches = engine_serving::execute_query(&catalog, "select \"id\" from \"sales\".\"orders\" order by \"id\"")
    .await
    .expect("execute_query");
let ids: Vec<i64> = batches.iter()
    .flat_map(|b| {
        let a = b.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
    })
    .collect();
assert_eq!(ids, vec![/* file ids + inline ids, sorted */]);
```
Add it as a `loom_fixture_test` `execute-query-e2e` in `engine-serving/BUCK` (deps: `:engine-serving`, `//src/control-plane/postgres:postgres`, `//third-party:{arrow,tokio,uuid}`).

- [ ] **Step 5: Move `pg_scan_sql` + wire BUCK test targets**

`git mv src/services/query-api/tests/pg_scan_sql.rs src/services/engine-serving/tests/pg_scan_sql.rs`; change its import `use query_api::pg_table_provider::build_scan_sql;` → `use engine_serving::provider::build_scan_sql;`. Add a `rust_test` `pg-scan-sql` target in `engine-serving/BUCK` (deps `:engine-serving`, `//third-party:{arrow,datafusion}`) and REMOVE the `pg-scan-sql` target from `query-api/BUCK`. (No other test files move in this task — the rest are handled in Task 4.)

- [ ] **Step 6: Build + run the crate's tests**

Run:
```
buck2 build //src/services/engine-serving:engine-serving > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[" /tmp/b.log
buck2 test //src/services/engine-serving:pg-scan-sql //src/services/engine-serving:execute-query-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
buck2 build '//src/services/engine-serving:engine-serving[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log
```
Expected: build SUCCEEDED, both tests PASS, clippy clean. (query-api won't build until Task 4 — expected mid-plan. Do NOT run `//src/...` here.)

- [ ] **Step 7: Commit**
```bash
git add src/services/engine-serving src/services/query-api/BUCK
git rm src/services/query-api/src/pg_table_provider.rs
git commit -m "refactor(engine-serving): extract DataFusion Iceberg execution into engine-serving crate"
```

---

### Task 2: Engine-wire — add the `ExecuteQuery` RPC + client

**Files:**
- Modify: `src/services/engine-wire/proto/engine_control.proto`
- Modify: `src/services/engine-wire/src/client.rs`

**Interfaces:**
- Produces: generated `pb::engine_query_server::{EngineQuery, EngineQueryServer}`, `pb::engine_query_client::EngineQueryClient`, `pb::ExecuteQueryRequest { sql: String }`, `pb::ExecuteQueryResponse { ipc: Vec<u8> }`; and `EngineQueryClient::connect(socket) -> Result<Self>` + `EngineQueryClient::execute_query(&self, sql: String) -> Result<Vec<u8>>`.

- [ ] **Step 1: Add the service + messages to the proto**

Append to `src/services/engine-wire/proto/engine_control.proto` (same `package loom.engine.v1`):
```proto
// Read-query execution. query-api inlines params and sends ready-to-run SQL;
// the engine runs DataFusion and returns the full result as Arrow IPC bytes.
service EngineQuery {
  rpc ExecuteQuery (ExecuteQueryRequest) returns (ExecuteQueryResponse);
}
message ExecuteQueryRequest  { string sql = 1; }
message ExecuteQueryResponse { bytes  ipc = 1; }   // Arrow IPC stream (schema + batches)
```
No BUCK change — the codegen genrule compiles the whole `.proto`.

- [ ] **Step 2: Verify codegen produces the stubs**

Run: `buck2 build //src/services/engine-wire:engine-wire > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[" /tmp/b.log`
Expected: SUCCEEDED (the `pb` module now contains `engine_query_server`/`engine_query_client`).

- [ ] **Step 3: Add the query client wrapper**

Append to `src/services/engine-wire/src/client.rs`:
```rust
use crate::pb::engine_query_client::EngineQueryClient as PbEngineQueryClient;

/// A cloneable gRPC client for the engine's `EngineQuery` service over a UDS.
#[derive(Clone)]
pub struct EngineQueryClient {
    inner: PbEngineQueryClient<Channel>,
}

impl EngineQueryClient {
    /// Connect to the engine's `EngineQuery` service at the given UDS path.
    pub async fn connect(socket: impl Into<String>) -> Result<Self> {
        let channel = crate::uds_channel(socket.into()).await?;
        Ok(Self {
            inner: PbEngineQueryClient::new(channel),
        })
    }

    /// Execute already-compiled, param-inlined SQL; return the Arrow IPC result bytes.
    pub async fn execute_query(&self, sql: String) -> Result<Vec<u8>> {
        let resp = self
            .inner
            .clone()
            .execute_query(pb::ExecuteQueryRequest { sql })
            .await
            .map_err(be)?
            .into_inner();
        Ok(resp.ipc)
    }
}
```
(`be`, `Channel`, `pb`, `uds_channel` are already imported/defined in this file/crate.)

- [ ] **Step 4: Build + commit**

Run: `buck2 build //src/services/engine-wire:engine-wire > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[" /tmp/b.log` (Expected: SUCCEEDED).
```bash
git add src/services/engine-wire/proto/engine_control.proto src/services/engine-wire/src/client.rs
git commit -m "feat(engine-wire): add EngineQuery.ExecuteQuery RPC + client"
```

---

### Task 3: Engine binary — host the `EngineQuery` service

**Files:**
- Create: `src/services/engine/src/query.rs`
- Modify: `src/services/engine/src/lib.rs` (add `pub mod query;`), `src/services/engine/src/main.rs`, `src/services/engine/Cargo.toml`, `src/services/engine/BUCK`

**Interfaces:**
- Consumes: `engine_serving::execute_query_to_ipc`, `engine_wire::pb::engine_query_server::{EngineQuery, EngineQueryServer}`, `engine_wire::pb::{ExecuteQueryRequest, ExecuteQueryResponse}`, `IcebergCatalog`.
- Produces: `pub struct EngineQueryService { pub catalog: IcebergCatalog }`.

- [ ] **Step 1: Add the dep**

`src/services/engine/Cargo.toml`: add `engine-serving = { path = "../engine-serving" }`. `src/services/engine/BUCK` (engine binary target + lib target deps): add `//src/services/engine-serving:engine-serving`. The engine crate must NOT add arrow-58/datafusion — it only handles `Vec<u8>`.

- [ ] **Step 2: Write the service**

`src/services/engine/src/query.rs`:
```rust
//! The `EngineQuery` tonic service: runs compiled, param-inlined read SQL through
//! the engine-serving DataFusion tier and returns Arrow IPC bytes. The engine crate
//! itself never touches arrow/datafusion types — only the opaque IPC `Vec<u8>`.

use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use engine_wire::pb::engine_query_server::EngineQuery;
use engine_wire::pb::{ExecuteQueryRequest, ExecuteQueryResponse};
use tonic::{Request, Response, Status};

pub struct EngineQueryService {
    pub catalog: IcebergCatalog,
}

#[tonic::async_trait]
impl EngineQuery for EngineQueryService {
    async fn execute_query(
        &self,
        request: Request<ExecuteQueryRequest>,
    ) -> Result<Response<ExecuteQueryResponse>, Status> {
        let sql = request.into_inner().sql;
        let ipc = engine_serving::execute_query_to_ipc(&self.catalog, &sql)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(ExecuteQueryResponse { ipc }))
    }
}
```

- [ ] **Step 3: Register it in `main.rs`**

In `src/services/engine/src/main.rs`: add `use engine::query::EngineQueryService;`, `use engine_wire::pb::engine_query_server::EngineQueryServer;`, and `use control_plane_postgres::iceberg_catalog::IcebergCatalog;`. Build the catalog and add the service:
```rust
    let query = EngineQueryService {
        catalog: IcebergCatalog::new(pool.clone()),
    };
```
and in the `Server::builder()` chain add `.add_service(EngineQueryServer::new(query))` alongside the existing two services. (`pool` is already in scope; clone before the existing `flight` move if needed — `FlightDataService` takes `pool` by value, so construct `query` with `pool.clone()` before `flight`.)

- [ ] **Step 4: Build + commit**

Run: `buck2 build //src/services/engine:engine-bin > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[" /tmp/b.log` (find the real bin target name with `grep -nE 'name = ' src/services/engine/BUCK`). Expected: SUCCEEDED. Clippy: `buck2 build '//src/services/engine:engine[clippy.txt]'` clean.
```bash
git add src/services/engine
git commit -m "feat(engine): serve EngineQuery.ExecuteQuery via the engine-serving tier"
```

---

### Task 4: query-api — `EngineServingClient`; slim `serving_datafusion`; rewire `main.rs`

**Files:**
- Modify: `src/services/query-api/src/serving_datafusion.rs` (remove moved items; keep the rest)
- Create: `src/services/query-api/src/engine_client.rs`
- Modify: `src/services/query-api/src/lib.rs`, `src/services/query-api/src/main.rs`, `src/services/query-api/Cargo.toml`, `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `engine_wire::client::EngineQueryClient`, `crate::serving::{ServingEngine, Rows, SqlValue, ServingError, inline_params}`, `crate::serving_datafusion::batches_to_rows`.
- Produces: `pub struct EngineServingClient` implementing `ServingEngine` (`fetch_rows` over the wire; `dialect()` → `DataFusionDialect`), with `pub async fn connect(socket: impl Into<String>) -> Result<Self, ServingError>`.

- [ ] **Step 1: Slim `serving_datafusion.rs`**

Delete from `src/services/query-api/src/serving_datafusion.rs` everything now in `engine-serving`: `DataFusionServingEngine`, `register_iceberg_table`, `build_inline_provider`, `IcebergMirrorTableProvider`, `prune_files`, `FileSetStatistics`, `stat_to_scalar`, `base_to_arrow`, `arrow_schema_from_mirror`, and the now-unused imports (datafusion, object_store, the provider imports). **Keep:** `IcebergActionWriter`, `ServingBackend`, `parse_serving_backend`, `encode_ipc_stream`, `batches_to_rows`, `arrow_to_sqlvalue`, **and query-api's own `to_serving<E>(e) -> ServingError`** (it is used by `encode_ipc_stream` — do NOT delete it; engine-serving has a separate copy), and their imports (`arrow` array/ipc types, `control_plane_postgres::iceberg_landing`, `SqlCatalog`, `build_object_batch`, `inline_params`). `batches_to_rows` must stay `pub` (the client + retained tests use it); `arrow_to_sqlvalue` stays as-is (private is fine — only `batches_to_rows` calls it).

- [ ] **Step 2: Write the wire client**

`src/services/query-api/src/engine_client.rs`:
```rust
//! `EngineServingClient` — a `ServingEngine` that runs reads on the engine service
//! over the engine wire: inline params, send compiled SQL, decode the Arrow-58 IPC
//! result into `Rows`. Replaces the in-process DataFusion engine.

use arrow::ipc::reader::StreamReader;
use async_trait::async_trait;
use engine_wire::client::EngineQueryClient;

use crate::serving::{Rows, ServingError, SqlValue, inline_params};
use crate::serving_datafusion::batches_to_rows;
use crate::sql::SqlDialect;

pub struct EngineServingClient {
    client: EngineQueryClient,
}

impl EngineServingClient {
    /// Connect to the engine's `EngineQuery` service at `socket` (a UDS path).
    pub async fn connect(socket: impl Into<String>) -> Result<Self, ServingError> {
        let client = EngineQueryClient::connect(socket)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl crate::serving::ServingEngine for EngineServingClient {
    async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError> {
        // Same param inlining the in-process DataFusion engine used; the engine has
        // no positional bind slot.
        let inlined = inline_params(sql, params);
        let ipc = self
            .client
            .execute_query(inlined)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        // Empty result → empty Rows (no column names). This matches the OLD
        // DataFusion path (which also returned empty Rows for a zero-row result);
        // it differs from EmbeddedDuckDb (which preserves columns for empty
        // results), but is behavior-preserving for the Iceberg backend — not a
        // regression introduced here.
        if ipc.is_empty() {
            return Ok(Rows::default());
        }
        let reader = StreamReader::try_new(std::io::Cursor::new(ipc), None)
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        Ok(batches_to_rows(batches))
    }
    fn dialect(&self) -> &'static dyn SqlDialect {
        &crate::sql::DataFusionDialect
    }
}
```

- [ ] **Step 2b: Declare the module** — add `pub mod engine_client;` to `src/services/query-api/src/lib.rs`.

- [ ] **Step 3: Rewire `main.rs`**

In `src/services/query-api/src/main.rs`:
- Change the import: `use query_api::serving_datafusion::{IcebergActionWriter, ServingBackend, parse_serving_backend};` (drop `DataFusionServingEngine`) and add `use query_api::engine_client::EngineServingClient;` and `use control_plane_postgres` stays for `IcebergCatalog`? — no longer needed for serving (the action writer still uses `pool`, not `IcebergCatalog`). Keep `IcebergCatalog` import only if still referenced; otherwise remove.
- In the `ServingBackend::Iceberg` arm, replace the serving construction:
```rust
            let engine_socket = std::env::var("LOOM_ENGINE_SOCKET")
                .map_err(|_| -> Box<dyn std::error::Error> {
                    "LOOM_ENGINE_SOCKET must be set for the Iceberg serving backend".into()
                })?;
            (
                Arc::new(EngineServingClient::connect(engine_socket).await?),
                action,
            )
```
The `IcebergActionWriter` construction (using `pool.clone()`) is unchanged; `pool` is no longer moved into a serving `IcebergCatalog`, so the earlier "clone before move" comment can go.

- [ ] **Step 4: Re-point / rewrite the retained query-api tests** (the move-list gap)

Eight query-api tests reference symbols that moved to `engine-serving`. They STAY in query-api (they assert via `batches_to_rows`/`SqlValue`, which stay here) but must be re-pointed, and each target gains a `//src/services/engine-serving:engine-serving` dep. Two groups:

**(A) Re-point imports only** (they call `register_iceberg_table`/providers directly with their own `SessionContext`, then `batches_to_rows`): `iceberg_mirror_provider.rs`, `iceberg_pruning_e2e.rs`, `datafusion_register.rs`, `iceberg_schema_evolution_read.rs`. Change `use query_api::serving_datafusion::{IcebergMirrorTableProvider, prune_files, register_iceberg_table, batches_to_rows}` → split: `use engine_serving::{IcebergMirrorTableProvider, prune_files, register_iceberg_table};` and keep `use query_api::serving_datafusion::batches_to_rows;` + `use query_api::serving::SqlValue;`. These keep using a real `SessionContext`, so their BUCK targets KEEP `//third-party:datafusion` and add `:engine-serving`.

**(B) Rewrite the deleted `DataFusionServingEngine`** (these used `DataFusionServingEngine::new(IcebergCatalog::new(pool)).fetch_rows(sql, params)`): `datafusion_serving.rs`, `datafusion_inline_union.rs`, `inline_pg_provider_e2e.rs`, `recursive_cte_over_datafusion.rs`. Replace each `engine.fetch_rows(sql, params)` call with:
```rust
// was: DataFusionServingEngine::new(IcebergCatalog::new(pool)).fetch_rows(sql, &params)
let catalog = IcebergCatalog::new(pool);
let inlined = query_api::serving::inline_params(sql, &params); // [] when no params
let rows = query_api::serving_datafusion::batches_to_rows(
    engine_serving::execute_query(&catalog, &inlined).await.expect("execute_query"),
);
```
Drop the `use query_api::serving_datafusion::DataFusionServingEngine;` and `ServingEngine` imports; keep `SqlValue`/`Rows` (and `recursive_cte`'s `query_api::sql::{...}`). These no longer build a `SessionContext` directly, so their BUCK targets DROP `//third-party:datafusion` and add `:engine-serving`. (`inline_pg_provider_e2e`'s time-travel assertion via `catalog.inline_live_batch` is `control_plane_postgres` — unchanged.)

`datafusion_value_map.rs` is UNCHANGED (only `batches_to_rows`/`SqlValue`/arrow — all still in query-api).

- [ ] **Step 4c: Update deps**

`src/services/query-api/Cargo.toml`: remove `datafusion` (no library code uses it after the move); keep `arrow` (IPC decode + action batch), `object_store` (DuckLake `local_store`), `sqlx`. Add `engine-wire = { path = "../engine-wire" }` and `engine-serving = { path = "../engine-serving" }` (the latter for the re-pointed tests).
`src/services/query-api/BUCK`: on the `query-api` LIB target, remove `//third-party:datafusion`, add `//src/services/engine-wire:engine-wire`. The `pg-scan-sql` target is already gone (Task 1). Apply the per-target `:engine-serving` add (and datafusion keep/drop) from Step 4 to the eight test targets.

- [ ] **Step 5: Build query-api + clippy**

Run:
```
buck2 build //src/services/query-api:query-api //src/services/query-api:query-api-bin > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[|warning: unused" /tmp/b.log
buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log
```
Expected: SUCCEEDED, clippy clean. If a query-api test target fails to compile because it referenced `DataFusionServingEngine`/providers, that test moved in Task 1 — confirm it's gone from `query-api/BUCK`.

- [ ] **Step 6: Commit**
```bash
git add src/services/query-api
git commit -m "refactor(query-api): serve Iceberg reads via the engine wire (EngineServingClient)"
```

---

### Task 5: Cross-wire e2e + full sweep + registers

**Files:**
- Create: `src/services/query-api/tests/engine_wire_serving_e2e.rs` + its `loom_fixture_test` target in `query-api/BUCK`
- Modify (if needed): `src/services/query-api/tests/e2e_support.rs` (a helper to boot the engine query service)
- Modify: `docs/ROADMAP.md`, `docs/ISSUES.md`, `docs/FUTURE.md`

**Interfaces:**
- Consumes: `engine::query::EngineQueryService`, `engine_wire::pb::engine_query_server::EngineQueryServer`, `query_api::engine_client::EngineServingClient`, the fixture `PgFixture`/`IcebergWriter`.

- [ ] **Step 1: Write the cross-wire e2e**

`src/services/query-api/tests/engine_wire_serving_e2e.rs`: boot a tonic `Server` with `EngineQueryServer::new(EngineQueryService { catalog })` on a temp UDS (mirror the worker `flight-roundtrip`/`wire` test's UDS server setup), seed a table with file + inline rows via `IcebergWriter`, connect an `EngineServingClient` to that UDS, call `fetch_rows("select \"id\" from \"sales\".\"orders\" order by \"id\"", &[])`, and assert the exact unioned id set. This proves the arrow-58 IPC round-trip end to end.

- [ ] **Step 2: Add the BUCK target**

Add a `loom_fixture_test` `engine-wire-serving-e2e` to `query-api/BUCK` with deps `:query-api`, `:e2e-support`, `//src/services/engine:engine`, `//src/services/engine-wire:engine-wire`, `//src/control-plane/postgres:postgres`, `//third-party:{tonic,tokio,arrow,uuid}` (match what the test imports).

- [ ] **Step 3: Run the new test + the migrated governed-read e2e**

Run: `buck2 test //src/services/query-api:engine-wire-serving-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS. Then check the broader query-api e2e suite still builds/passes (any Iceberg-backend governed-read test that previously built `DataFusionServingEngine` must now use the boot helper or be the DuckLake backend).

- [ ] **Step 4: Full sweep + prek**

Run:
```
buck2 test //src/... > /tmp/full.log 2>&1; grep -E "Tests finished|FAIL" /tmp/full.log
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; echo "PREK=$?"; tail -5 /tmp/prek.log
```
Expected: full suite PASS (0 FAIL), prek clean.

- [ ] **Step 5: Registers (loom-docs-update)**

- Close `road-df-postgres-tableprovider` (done, `pr:#181`) and `iss-iceberg-inline-reparse` (fixed, `pr:#181`) — already staged on the branch; confirm prose now says the provider lives in `engine-serving`, served over the wire.
- Add a new ROADMAP item `road-engine-serving-wire` (status `done`, `pr:#181`, `spec:2026-06-24-engine-serving-execution-wire-design`, area `iceberg`) describing the relocation.
- Note deferrals: networked engine endpoint + Helm + TLS ([[fut-engine-wire-multi-tls]], [[fut-deploy-followups]]); relocating `IcebergActionWriter`/writes; moving the DuckLake path. Run `bash tools/docs.sh validate`.

- [ ] **Step 6: Commit**
```bash
git add src/services/query-api/tests docs/ROADMAP.md docs/ISSUES.md docs/FUTURE.md
git commit -m "test(query-api): engine-wire serving e2e; close + record register items"
```

---

## Self-Review

**1. Spec coverage:**
- Seam (trait stays; execution moves) → Tasks 1 & 4. ✓
- `ExecuteQuery` arrow-58 IPC over engine gRPC, params inlined client-side → Tasks 2 (proto/client), 3 (server), 4 (client `fetch_rows` inlines). ✓ (Spec said "stream"; plan uses **unary** returning one IPC blob — justified: governed reads are LIMIT-bounded and every existing engine RPC is unary. Documented in Global Constraints / Task 4.)
- Engine binary hosts the service on the existing UDS → Task 3. ✓
- query-api becomes client, drops datafusion → Task 4. ✓
- Arrow-major confinement (engine passes only `bytes`) → Task 3 Step 1 constraint + the `engine_serving` boundary. ✓
- Co-located UDS via `LOOM_ENGINE_SOCKET` → Task 4 Step 3. ✓
- Out of scope (DuckLake, `IcebergActionWriter`, network endpoint) honored → Task 1/4 keep them in query-api. ✓
- Testing (behavior preserved via moved tests; cross-wire e2e; full sweep) → Tasks 1 & 5. ✓

**2. Placeholder scan:** New code (proto, `execute_query_to_ipc`, `EngineQueryService`, `EngineServingClient`, client wrapper, IPC decode) is complete. Moved code is specified as verbatim relocation with the one mechanical change named (error type `ServingError`→`EngineServingError`); reproducing ~600 moved lines is intentionally avoided per relocation pragmatics — the source file is the truth. Test-target names to confirm against BUCK are flagged (`engine-bin`, the real engine bin name).

**3. Type consistency:** `execute_query_to_ipc(catalog: &IcebergCatalog, sql: &str) -> Result<Vec<u8>, EngineServingError>` is identical across Task 1 (def), Task 3 (call). `execute_query(...) -> Result<Vec<RecordBatch>, EngineServingError>` matches Task 1 (def) and Task 4 group-B test rewrites (call). `EngineQueryClient::execute_query(sql: String) -> Result<Vec<u8>>` matches Task 2 (def) and Task 4 (call). `ExecuteQueryRequest{sql}` / `ExecuteQueryResponse{ipc}` consistent across proto/server/client. `batches_to_rows` stays `pub` in query-api, used by `EngineServingClient` and the retained tests. IPC: engine writes a `StreamWriter`, client reads a `StreamReader` — matching framing.

**4. Plan-review fixes incorporated (opus gate, PASS WITH FIXES):**
- (blocking #1) `to_serving` is NOT moved — duplicated: query-api keeps its `-> ServingError` copy for `encode_ipc_stream`; engine-serving has its own `-> EngineServingError` copy. (Task 1 Step 3, Task 4 Step 1.)
- (blocking #2) The two orphaned tests (`recursive_cte_over_datafusion`, `iceberg_schema_evolution_read`) plus the other six provider/execution tests are explicitly handled in Task 4 Step 4 (re-point or rewrite, stay in query-api) — so the full sweep can go green.
- (blocking #3) The `Rows`/`SqlValue` cross-crate problem is avoided: those tests stay in query-api (which owns `batches_to_rows`/`SqlValue`) and gain a `:engine-serving` dep for the moved symbols; engine-serving's own tests (`pg_scan_sql`, `execute_query_e2e`) assert on `RecordBatch`es only — no query-api dep, so Task 1 builds standalone. Enabled by splitting out `execute_query -> Vec<RecordBatch>`.
- (minor #4) `datafusion` dropped from the query-api LIB; kept on the four group-A test targets that build a `SessionContext`, dropped from the four group-B targets that now call `execute_query`. (Task 4 Step 4/4c.)
- (minor #5) Empty-result column-less `Rows` documented as behavior-preserving vs. the old DataFusion path. (Task 4 Step 2.)
