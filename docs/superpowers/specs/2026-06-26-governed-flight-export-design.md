# Governed Arrow Flight export (external columnar egress)

_Design spec. 2026-06-26._

## Context

The Grimoire/personal-KG consumer (agenda **A4**) hydrates a disposable fast tier
(pgvector) at session start by **bulk-reading a governed slice** of loom's typed
objects — entities, text chunks, and their `vector(N)` embeddings — as **columnar
Arrow**, fast. Today loom has no egress for this: every read terminates in
`LIMIT 1000` JSON (`GET /objects/{type}` → `render.rs`), and the one Arrow producer
(the engine's Flight-SQL `do_get`) is **internal-only (UDS)** and is immediately
flattened to scalar `Rows` by query-api's `batches_to_rows`/`SqlValue` path — which
has no list variant, so it would **stringify a vector column**.

Everything needed for *governed slicing* already exists and is reused unchanged:
- **Governance is compiled into SQL** by `compile_select_with` (`query-api/src/sql.rs:397`):
  the subject's ACL `RowFilter` tree + column projection/masking + caller `eq`/object-set
  filters are lowered into the `WHERE`/`SELECT`. The engine applies **no** ACL.
- **The engine streams that governed SQL result over Flight SQL** (`engine/src/flight.rs`
  `do_get_sql` → `engine_serving::execute_query_stream` → `FlightDataEncoderBuilder`),
  and that path **already carries `list<float>` natively** (`engine-serving/src/serving.rs`
  `base_to_arrow`: `Vector → List<Float32>`).

So A4 is **purely egress**: expose a governed, authenticated, external **Arrow Flight**
endpoint that streams a governed object slice out columnar — bypassing the JSON/`SqlValue`
flattening. **Decision (operator): query-api hosts it** (option A), keeping governance
where it already lives rather than exposing the trusted internal engine and moving ACL
into it.

## Decision — query-api hosts a governing Flight proxy

query-api gains a **second listener**: a TCP **Arrow Flight server** (`FlightServiceServer`,
tonic) alongside its axum HTTP server. It is a **governing proxy** — it authenticates the
caller, builds the governed SQL exactly as the HTTP read path does, runs it over the
engine's existing internal Flight-SQL (UDS), and **streams the engine's `RecordBatch`
result straight out** as Flight data. No arbitrary SQL ever crosses the external wire.

### Why not arbitrary Flight SQL

loom governs by *building* the query from a typed-object request + the subject's ACL. A
raw Flight-SQL server accepting consumer SQL would let a caller `SELECT * FROM anything`
and bypass governance. So the external wire carries a **loom export *command*** (which
type, which slice) — not SQL — and loom compiles the ACL'd SQL server-side. This is Flight
as the *transport* (streaming, Arrow-native, standard clients), not arbitrary Flight SQL.

## Surface

### The export command (JSON in the Flight descriptor `cmd`, mirroring `FlightTicket`'s JSON convention)

```jsonc
// ExportCommand — the governed slice to export. Mirrors GET /objects/{type} params.
{ "type": "Chunk",
  "filters": { "sourcebook": "PHB" },   // optional eq_filters (validated vs allowed/masked cols)
  "ids": [/* optional object-set identity values → an `In` predicate */] }
```

### Flight flow (stateless — the ticket carries the command, not SQL)

- **`get_flight_info(descriptor.cmd = ExportCommand)`**: authenticate (below) → validate the
  type/filters exist and are readable (the same `acl.check(Read, Type)` deny-by-default gate
  + `load_policy` as `read_object`) → build the **projected result schema** from the mirror
  columns minus ACL-denied columns (`arrow_schema_from_mirror`, which maps `vector(N)` →
  `List<Float32>`) → return `FlightInfo { schema, endpoints: [{ ticket: <ExportCommand bytes> }] }`.
  No row work yet.
- **`do_get(ticket = ExportCommand)`**: authenticate → **re-derive the governed SQL for the
  authenticated subject** (`load_policy` + `compile_select_with` with **no `LIMIT`** — export
  is the full slice; a configurable safety cap `LOOM_EXPORT_MAX_ROWS` guards runaway) →
  run it over the engine's internal Flight SQL **streaming** (new `FlightSqlClient::execute_stream`,
  §below) → re-encode the `RecordBatch` stream with `FlightDataEncoderBuilder` and stream it
  out. **Vectors carried natively; `batches_to_rows`/`SqlValue` is bypassed entirely.**

  Re-governing in `do_get` (rather than caching a prepared SQL under a server handle) is the
  load-bearing security choice: the ticket is a **governed command**, so a forged/replayed
  ticket is still just another export request, governed for *that call's* authenticated
  subject — no arbitrary-SQL injection, no server-side prepared-statement state, no
  cross-subject ticket theft (the ACL is applied per-`do_get`, per-subject).

### Auth — reuse the bearer-token seam

The caller presents the **same bearer session token** loom already issues (`service_runtime::auth`,
resolved by `Auth::resolve_session`) in the gRPC **`authorization` metadata** on each call.
The Flight server reads it, resolves it to a `SubjectId` (the `Auth` trait, same producer as
HTTP `require_auth`), and governs for that subject. A missing/invalid token → `Unauthenticated`.
No Flight `do_handshake` token-exchange is needed (loom issues tokens via its HTTP auth routes);
`do_handshake` stays `unimplemented`. Other Flight verbs (`do_put`, `do_action`, `list_flights`)
are unimplemented — read-export only.

### Streaming client — `FlightSqlClient::execute_stream`

`engine_wire::flight::FlightSqlClient::execute` currently `try_collect`s the whole result into
`Vec<RecordBatch>` (`engine-wire/src/flight.rs`). Add `execute_stream(&self, sql) ->
Result<impl Stream<Item = Result<RecordBatch>>>` that returns the decoded `do_get` stream
**without buffering**, so query-api's `do_get` forwards engine→consumer back-pressured. (The
existing `execute` stays for `EngineServingClient::fetch_rows`.)

### Config / wiring

- `LOOM_FLIGHT_BIND_ADDR` (e.g. `0.0.0.0:50051`) — the external Flight TCP bind. Unset →
  Flight server not started (HTTP-only, today's behaviour; opt-in).
- query-api `main`: spawn the Flight `Server` (tonic) on the bind addr in parallel with
  `service_runtime::serve` (axum), sharing the same `AppState` deps (`acl`, `serving`
  engine client, ontology/catalog). TLS is deferred (`fut-flight-export-tls`).

## Data flow

```
consumer (Flight client, bearer token)
  → query-api Flight server : get_flight_info(ExportCommand)
        auth → SubjectId; acl gate; projected schema  ⇒ FlightInfo{schema, ticket=ExportCommand}
  → query-api Flight server : do_get(ticket=ExportCommand)
        auth → SubjectId; load_policy; compile_select_with(no LIMIT) ⇒ governed SQL
        engine FlightSqlClient.execute_stream(governed SQL)  (UDS, internal)
        engine do_get_sql → execute_query_stream (DataFusion, ACL already in SQL)
        ⇐ RecordBatch stream (list<float> native) ⇒ Flight-encode ⇒ consumer
```

## Error handling

- Unauthenticated / bad token → gRPC `Unauthenticated`. Type denied (deny-by-default) →
  `PermissionDenied` (mirrors the HTTP `Forbidden`, before revealing type existence).
- Unknown type / bad filter (column not allowed/masked) → `InvalidArgument` (same validation
  as `read_object`).
- Row cap exceeded (`LOOM_EXPORT_MAX_ROWS`) → the stream ends with an error after the cap, so
  a partial-but-truthful result is never silently truncated.
- Engine/stream fault mid-export → the `do_get` stream surfaces the error (the consumer sees a
  failed stream, not a silent short read).

## Testing

`loom_fixture_test` (hermetic Postgres + object store; engine over UDS). Reuse the query-api
e2e ACL/seed harness (`e2e_support`).

- **Governed export e2e**: seed a typed object with scalar + a `vector(4)` column over a few
  rows; an authed subject `do_get`s the export → assert the **Arrow `RecordBatch`** carries the
  rows, the **vector column as `List<Float32>` value-exact** (NOT stringified), schema-first.
- **ACL applied**: a row-filtered subject's export returns only the permitted rows; a
  column-masked subject's export has the masked column projected as the mask; a type-denied
  subject gets `PermissionDenied`.
- **Auth**: missing/invalid bearer token → `Unauthenticated`.
- **No `LIMIT`**: an export over >1000 rows returns all of them (vs the HTTP path's `LIMIT 1000`).
- **Streaming/back-pressure** (smoke): a multi-batch result arrives as multiple `FlightData`
  messages (not one buffered blob) — assert via the decoded stream.
- **Defaults unchanged**: `LOOM_FLIGHT_BIND_ADDR` unset → no Flight server; the HTTP read path
  and all existing tests are byte-identical.

## Scope boundary

- **In:** a query-api-hosted external Arrow Flight server (`get_flight_info` + `do_get` for the
  `ExportCommand`); bearer-token auth from gRPC metadata; governed SQL reuse (`load_policy` +
  `compile_select_with`, no `LIMIT`, `LOOM_EXPORT_MAX_ROWS` cap); `FlightSqlClient::execute_stream`;
  native vector-column carriage; the tests above.
- **Out (deferred, tracked):** TLS/mTLS on the Flight wire (`fut-flight-export-tls`); arbitrary
  Flight SQL / the full Flight-SQL command surface ([[fut-external-sql-wire]] / [[fut-flight-sql-surface]]);
  Flight `do_put`/write-back; pagination/cursors/resumable export; column-level export selection
  beyond ACL projection; non-typed-object (raw table) export. Realizes the egress half of
  [[fut-external-sql-wire]]; the arbitrary-SQL half stays deferred.

## Acceptance criteria

1. With `LOOM_FLIGHT_BIND_ADDR` set, an authenticated Flight client `get_flight_info`/`do_get`s
   an `ExportCommand` and receives the governed typed-object slice as a **columnar Arrow stream**,
   with `vector(N)` columns carried as `List<Float32>` value-exact (no JSON/`SqlValue` flattening).
2. Governance is identical to `GET /objects/{type}`: deny-by-default type gate, row filters, and
   column masking are applied (re-derived per `do_get` for the authenticated subject); no arbitrary
   SQL crosses the wire.
3. The export is the **full slice** (no `LIMIT 1000`), bounded only by `LOOM_EXPORT_MAX_ROWS`, and
   streams without buffering the whole result in the client.
4. Missing/invalid token → `Unauthenticated`; type-denied → `PermissionDenied`.
5. `buck2 test //src/...` green; `LOOM_FLIGHT_BIND_ADDR` unset leaves the HTTP path and all
   defaults unchanged.
