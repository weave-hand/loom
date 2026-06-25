# Stream query-api reads over internal Flight SQL

_Design spec. Direction-setting only — the implementation plan is written separately by the work agent that claims `road-engine-flight-sql-read`._

## Problem

After PR #181 the DataFusion serving execution moved into the engine (`engine-serving`), and query-api became a wire client: it sends governed, param-inlined SQL to the engine and receives the result back. That read crosses the engine→query-api boundary over a **unary** gRPC RPC — `EngineQuery.ExecuteQuery(ExecuteQueryRequest{sql}) → ExecuteQueryResponse{ipc: bytes}` — where the engine serializes **all** result batches into one Arrow IPC `Vec<u8>` and returns them in a single gRPC message.

That single-message design has two real costs:

- **The ~4MB gRPC message cap.** A wide or large governed read serializes past the default gRPC max message size and fails — a correctness ceiling, not just a performance one.
- **Double buffering.** The engine builds the entire IPC blob in memory and query-api holds the entire blob before decoding; nothing is incremental.

The arrow-58 converge ([[road-iceberg-arrow58-converge]]) unified the arrow major across the engine's Flight plane, `engine-serving`, and query-api — so arrow batches can now flow over Arrow Flight between these services without any cross-major conversion. Arrow Flight is purpose-built for streaming arrow batches; this slice uses it for exactly that.

## Goal

Replace the unary `EngineQuery` read RPC with a **streamed** read over **internal Flight SQL** (`CommandStatementQuery`). The engine streams the DataFusion result batches; query-api consumes the stream and assembles its rows as before. The result-size ceiling and the double-buffering both go away, and the internal read becomes the **first consumer of a Flight SQL surface** that a future *external* wire ([[fut-external-sql-wire]]) can expose with mostly auth + TCP added — execution and streaming are done here.

Non-goal: changing query-api's governance/SQL-compile layer, query-api's HTTP response shape, or the worker's file-ticket `do_get` plane.

## Scope

**One slice**, internal-only (the engine's existing Unix-domain socket). It spans three crates that move together: `engine-serving` (a streaming execution entry point), `engine` (the Flight SQL command handler, replacing the unary handler), and `query-api` (the Flight SQL client, replacing the unary client). The unary `EngineQuery` RPC — new in #181 with a single consumer — is removed in the same slice so there are not two read paths.

Out of scope (recorded as deferrals): the external Flight SQL wire (TCP + auth + governance), the rest of the Flight SQL command surface (prepared statements, catalog-metadata commands), and streaming all the way through to query-api's HTTP client.

## Approach

### Transport: internal Flight SQL `CommandStatementQuery`

The engine already runs a `FlightService` on its UDS (today only the file-ticket `do_get`, used by the worker/compaction plane). That file-ticket path serves *raw file rows by path* — it has no SQL/DataFusion in it, so it is the wrong shape for query-api's governed SQL reads and is left untouched. Instead, add the Flight SQL command flow (via the `arrow_flight::sql` server helpers):

- `get_flight_info(CommandStatementQuery{ query })` → a `FlightInfo` whose endpoint ticket carries the SQL string.
- `do_get(ticket)` → decode the SQL, run it through `engine-serving`, and stream the result `RecordBatch`es back via `FlightDataEncoderBuilder` (schema message first, then batches).

Only `CommandStatementQuery` is implemented; the other Flight SQL commands return `unimplemented` (internal plane, single consumer). The handler lives alongside the existing file-ticket `do_get` on the same `FlightService`/UDS.

Chosen over **server-streaming gRPC** (changing `ExecuteQuery` to `returns (stream ...)`): that is the smaller change and also removes the size ceiling, but it does not use Arrow Flight and builds nothing toward the external wire. Flight SQL is the strategic choice — the internal read and any future external read share one surface.

### Streaming execution in `engine-serving`

`engine-serving::execute_query` collects to `Vec<RecordBatch>` and `execute_query_to_ipc` serializes that to one blob. Add a streaming sibling — `execute_query_stream(catalog, sql, serving_store) -> SendableRecordBatchStream` — that registers the same live tables (`register_iceberg_table` / `PgTableProvider`) into a `SessionContext`, runs the same compiled SQL, and returns DataFusion's `df.execute_stream()` result instead of collecting. The engine's Flight `do_get` encodes that stream directly, so the engine never holds the whole result. `execute_query` / `execute_query_to_ipc` are removed once the unary RPC is gone (or kept only if a test needs the collected form).

### query-api Flight SQL client

`EngineServingClient::fetch_rows(sql, params) -> Rows` keeps its signature and its callers. Internally it swaps the unary `EngineQuery::execute_query` call for the Flight SQL dance: build `CommandStatementQuery{ query: inline_params(sql, params) }`, `get_flight_info`, then `do_get` the returned ticket and consume a `FlightRecordBatchStream`, collecting batches into `Rows` via the existing `batches_to_rows`. The param-inlining and the governance/compile layers above are unchanged. (query-api still materializes `Rows` for its HTTP handler — streaming further to the HTTP client is deferred.)

### Removing the unary path

The `EngineQuery` service (proto), the engine handler (`engine/src/query.rs`), and query-api's unary client call are deleted once the Flight SQL path is in place. No compatibility window is needed — both ends ship in this slice and the only consumer is query-api itself.

## Acceptance gates

- **Full** `buck2 test //src/...` green (not per-crate — shared-fixture and duckdb-pin regressions surface across crates).
- `engine-serving`: a fixture test that `execute_query_stream` yields the same rows as the previous collected path.
- `engine`: a wire test (mirroring `engine/tests/wire.rs`) that issues a `CommandStatementQuery` over the UDS and reassembles the streamed batches.
- `query-api`: the existing Iceberg-backend read e2e passes unchanged (same rows) now that it runs over Flight SQL, **plus** a payoff test — a result large enough to have exceeded the ~4MB unary gRPC message cap now succeeds over the stream.
- `tools/clippy-all.sh` clean; `.sqlx` cache unaffected (no SQL changes); `duckdb 1.10503.1` pin held if any re-lock occurs.

## Risks & mitigations

- **Flight SQL handler surface.** Implementing the `arrow_flight::sql` server trait pulls in several command methods; keep all but `CommandStatementQuery` as `unimplemented` to bound the slice, and document that the surface is internal-only.
- **Ticket size / SQL in the ticket.** The compiled SQL travels in the Flight ticket; it is already param-inlined and bounded by query-api's compile step — no new injection surface beyond what the unary RPC already carried (same trusted internal client).
- **Error mapping.** DataFusion/stream errors must map to the same `ServingError` taxonomy query-api expects, so HTTP error codes are unchanged from the unary path. Verify with a malformed-SQL test.
- **Removing the unary RPC.** A clean removal (single consumer, same slice) — but make sure no test or binary still references `EngineQuery` before deleting the proto service.

## Deferrals

- **External Flight SQL wire** — exposing this Flight SQL surface to external clients (Python/JDBC Flight SQL drivers) over TCP with auth + SQL-governance. This slice is the internal-first foundation; the external story stays in [[fut-external-sql-wire]].
- **Full Flight SQL command surface** — prepared statements, `CommandGetTables` and other catalog-metadata commands — deferred until a consumer needs them (`fut-flight-sql-surface`).
- **Streaming to the HTTP client** — query-api still collects the Flight stream into `Rows` before responding; carrying the stream through to the HTTP response is a separate follow-on (`fut-serving-stream-to-http`).
