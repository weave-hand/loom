# Relocate DataFusion query execution into the engine service — Design

> Moves loom's DataFusion read-serving execution out of `query-api` (where it
> runs in-process) and into the `engine` service, with `query-api` calling it
> over the engine wire. Reworks PR #181 / `road-df-postgres-tableprovider` in
> place: the `PgTableProvider` work landed in the wrong tier (query-api), and
> this puts the whole serving engine where it belongs — the execution tier the
> transform/compaction pillars will also consume.

## Problem

`query-api` currently executes **all** governed reads in-process: the handler
compiles governed SQL, then calls `ServingEngine::fetch_rows` on an
`Arc<dyn ServingEngine>` that is either `EmbeddedDuckDb` or
`DataFusionServingEngine` — both running the query engine inside the query-api
process (`query-api/src/main.rs:33-74`, `handler.rs:298-308`).

That conflates two tiers. `engine` is meant to be the **execution service**
(it already owns Postgres, the catalog, the object store, and the Arrow Flight
data plane for compaction); `query-api` is meant to be the **API / governance
tier** that compiles governed SQL and talks to the engine over a wire. Building
`PgTableProvider` inside `query-api` (PR #181) deepened the misplacement — and it
guarantees duplication, because the transform/compaction jobs that run in the
engine will need the same "DataFusion scans Iceberg + Postgres-inline" capability.

This slice relocates the **DataFusion/Iceberg read execution** into the engine
and makes query-api a thin wire client. (DuckLake/`EmbeddedDuckDb` stays
in-process in query-api for now — out of scope, see below.)

## The seam — what stays vs. moves

The `ServingEngine` trait (`query-api/src/serving.rs:177-188`,
`fetch_rows(sql, params) -> Rows` + `dialect()`) is already format/transport
neutral. It is the relocation seam:

- **Stays in `query-api`:** the trait itself; HTTP handlers; ACL/ontology
  governance; SQL **compilation** (`compile_select_with`); the `EmbeddedDuckDb`
  + DuckLake path; `Rows`/`SqlValue`; param inlining; Arrow-IPC → `Rows`
  decoding (`batches_to_rows`, `arrow_to_sqlvalue` — these are arrow-58 and move
  with the *client* decode, see below).
- **Moves into the engine tier (new `engine-serving` crate, arrow-58):**
  `DataFusionServingEngine`'s execution body (build `SessionContext`, register
  live Iceberg tables, run SQL, collect batches); `register_iceberg_table`;
  `IcebergMirrorTableProvider`; `PgTableProvider` + `build_scan_sql` +
  `pg_rows_to_arrays`; `arrow_schema_from_mirror` / `base_to_arrow`; the file
  pruning (`prune_files`, `FileSetStatistics`). The pure-logic `pg-scan-sql`
  test and the inline-provider e2e move with them.
- **Out of scope (stays in query-api this slice):** `IcebergActionWriter` /
  `ActionEngine::write_object` — it is a control-plane *landing write*
  (`iceberg_landing::land` over the pool), not DataFusion query execution, so it
  is a separate seam; relocating writes is a follow-up. `EmbeddedDuckDb`/DuckLake
  execution stays in-process in query-api.

## The wire — `ExecuteQuery` over the engine gRPC, arrow-58 IPC payload

Add one RPC to the engine wire (`engine-wire/proto/engine_control.proto`,
served by the engine binary alongside the existing control + Flight services):

```proto
service EngineQuery {
  // Execute already-compiled, param-inlined read-only SQL; stream the result
  // as Arrow IPC stream chunks (arrow-58 on both ends).
  rpc ExecuteQuery(ExecuteQueryRequest) returns (stream QueryResultChunk);
}
message ExecuteQueryRequest { string sql = 1; }   // params pre-inlined by query-api
message QueryResultChunk    { bytes  ipc = 1; }   // Arrow IPC stream bytes
```

**Why gRPC-streaming-IPC, not Arrow Flight:** arrow-flight is only vendored at
**57**; DataFusion is **arrow-58**. Reusing the existing arrow-57 Flight plane
would force an in-engine 58→57 batch conversion on every query. A plain
`bytes`-carrying gRPC stream lets the query path be **arrow-58 end to end** —
engine encodes DataFusion's arrow-58 batches to IPC (the existing
`encode_ipc_stream` pattern), query-api decodes arrow-58 IPC (it is already
arrow-58) — with **no arrow-flight dependency and no cross-major conversion**.
The engine's existing arrow-57 compaction Flight plane is untouched. (Rejected
alternatives: extend Flight `do_get` with a SQL ticket — drags 58→57 conversion
in; add arrow-flight-58 — a new third-party major + reindeer churn for no gain
over raw IPC.)

**Params:** query-api inlines params into the SQL string before the call (it
already does this — `inline_params`, the injection-safe `?`→literal renderer),
so the wire carries one ready-to-run SQL string and the engine just executes it.
This keeps the engine side identical to today's `DataFusionServingEngine`
(`inline_params` then `ctx.sql`).

**Dialect:** unchanged — query-api compiles with `DataFusionDialect` for this
backend exactly as today; the engine receives dialect-correct SQL.

## Engine binary changes

- New crate `src/services/engine-serving` (arrow-58, datafusion-54): hosts the
  relocated execution + providers, exposing
  `async fn execute_query(catalog: &IcebergCatalog, sql: &str) -> Result<Vec<RecordBatch>>`
  (or a batch stream) — the body of today's `DataFusionServingEngine::fetch_rows`
  minus the `Rows` flattening (the client flattens).
- The engine binary gains an `EngineQuery` tonic service that holds the
  `IcebergCatalog` (built from the engine's existing `PgPool`) and, per request,
  runs `engine_serving::execute_query`, encodes each batch to Arrow IPC, and
  streams `QueryResultChunk`s.
- Served on the **same UDS** as the existing `EngineControl` + Flight services
  (`main.rs` adds the third service to the tonic router). No new socket.

## query-api client

- New `EngineServingClient` implementing `ServingEngine` (replaces
  `DataFusionServingEngine` in the `ServingBackend::Iceberg` arm of
  `query-api/src/main.rs`): holds an `EngineQuery` gRPC client over the engine
  UDS; `fetch_rows(sql, params)` inlines params, calls `ExecuteQuery`, decodes
  the streamed arrow-58 IPC chunks to `RecordBatch`es, and runs the existing
  `batches_to_rows` → `Rows`. `dialect()` returns `DataFusionDialect`.
- `query-api` drops its `datafusion` + provider code and deps for this path; it
  keeps `arrow`/`arrow-ipc` (58) for decode and `tonic`/`engine-wire` for the
  client. The DuckLake path's `EmbeddedDuckDb` is unchanged.

## Deployment / reachability

`engine` is UDS-only (`LOOM_ENGINE_SOCKET`) and not yet in the Helm chart.
query-api reaching it requires co-location with a shared UDS — which mirrors the
existing ingest+query-api co-location on one node with a shared store. **This
slice assumes engine + query-api are co-scheduled sharing the engine UDS**
(query-api reads `LOOM_ENGINE_SOCKET`). A network endpoint (TCP/TLS) for engine
and its Helm wiring is a **follow-up** ([[fut-engine-wire-multi-tls]],
[[fut-deploy-followups]]); this slice proves the relocation over the local UDS
the engine already serves, with a co-located integration test.

## Testing

- **Behavior preserved:** the governed-read e2e (`object_set_e2e`,
  `iceberg_*_e2e`) must serve identical rows with the Iceberg backend now going
  query-api → engine UDS → DataFusion. The existing
  `datafusion-inline-union` / `datafusion-serving` move to the engine-serving
  crate as engine-side execution tests (same assertions).
- **New cross-wire e2e (`loom_fixture_test`):** boot the engine service on a temp
  UDS, point an `EngineServingClient` at it, and assert `fetch_rows` returns the
  correct rows for a file+inline table — proving the IPC round-trip and the
  arrow-58 wire.
- `pg-scan-sql` and `inline-pg-provider-e2e` move with the provider into the
  engine-serving crate.
- Full `buck2 test //src/...` green; prek clean.

## Scope boundary / out of scope

- DuckLake/`EmbeddedDuckDb` execution stays in query-api (only the
  DataFusion/Iceberg backend relocates).
- `IcebergActionWriter` (governed write-back) stays in query-api — not DataFusion
  query execution; relocating writes is a separate slice.
- engine network endpoint + Helm + TLS/auth on the query wire — follow-up
  ([[fut-engine-wire-multi-tls]], [[fut-deploy-followups]]).
- Transform/compaction consuming `engine-serving` for DataFusion-over-PG — the
  reuse this unblocks, but not wired here.
- `iss-flight-ticket-path-unchecked` (the existing compaction Flight ticket
  membership check) is unrelated and unchanged.

## PR / branch

Reworks `work/road-df-postgres-tableprovider` (PR #181) in place — the diff grows
from "PgTableProvider in query-api" to "serving execution in engine + query-api
wire client." The register item `road-df-postgres-tableprovider` and
`iss-iceberg-inline-reparse` close on this PR; a new ROADMAP item
(`road-engine-serving-wire`, this spec) tracks the relocation.
