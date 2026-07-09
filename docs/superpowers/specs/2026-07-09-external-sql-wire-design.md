# External SQL wire — slice 2: TCP Flight SQL listener + bearer auth + per-connection governed catalog Design

> **Status:** design (direction). This spec makes `road-external-sql-wire`
> (promoted from `fut-external-sql-wire`) build-ready. A separate work agent
> writes the implementation plan from it and builds it. Slice 1 — the governed
> catalog primitive — landed as `road-external-sql-governed-catalog` (#274);
> this slice ships the external wire that feeds it. Slice 3+ stays deferred
> (see Out of scope).

## Goal

An **external Arrow Flight SQL endpoint**: a TCP `FlightServiceServer` an
external client (ADBC driver, `arrow-flight` client, future DuckDB `ATTACH`)
can dial, authenticate to with a **bearer token** (login session or service
token), and send **arbitrary SQL** — getting back Arrow record batches that
obey the caller's ACL row-for-row and column-for-column. Governance is not a
SQL rewriter: every query runs over a **per-connection governed catalog** in
which each visible ontology type is registered as an enforcing
`GovernedTableProvider`, so any join / aggregate / subquery the client writes
is governed **by construction** (the slice-1 primitive).

This is also the substrate the UI SQL console ([[fut-ui-sql-query-console]])
needs — the catalog-resolution seam this slice builds (`resolve_governed_catalog`)
is exactly the "governed execution over arbitrary SQL" half that a later HTTP
console endpoint reuses.

## Decision record (2026-06-30, reaffirmed here)

Recorded on `fut-external-sql-wire` and in the slice-1 spec
(`f1183587`, `docs(query): spec external SQL wire slice 1`):

- **Protocol: Flight SQL over TCP.** The engine already speaks
  `CommandStatementQuery` internally (`engine/src/flight.rs:268`); query-api
  already hosts an externally-bound TCP Flight listener with bearer auth (the
  governed Flight export, `road-governed-flight-export`). Flight SQL is the
  standard the ecosystem's clients (ADBC/JDBC drivers) speak. Quack /
  Postgres-wire variants stay rejected for now.
- **Governance: per-connection governed catalog.** Each type the subject may
  read is registered as an enforcing `GovernedTableProvider`
  (`engine-serving/src/governed.rs:174`) so arbitrary client SQL is governed by
  construction — never by parsing/rewriting the client's SQL.
- **Placement: query-api authenticates and resolves policy; the engine
  executes blindly** — "query-api resolving `load_policy` into the catalog".
  Reconciled with the current engine-wire architecture below.

### One amendment to a slice-1 "decided" point (closed-world registration)

Slice 1 decided *"a table with no `GovernedTable` entry is registered fully
visible (empty policy)"*, rationalized as *"an omitted table is unreachable
anyway"* (`core/src/governed.rs:24-27`). That rationale does not survive
contact with the implementation: `execute_governed_sql_stream` registers
**every** live table from `IcebergCatalog::live_tables()`
(`engine-serving/src/governed.rs:296`, `iceberg_catalog.rs:184`) — including
landed datasets bound to **no** ontology type — and `policy_for` returns the
empty (fully-visible) policy for absent entries (`governed.rs:152`). An
external subject with an empty catalog would see every unbound dataset,
ungoverned. **This slice flips the semantics to closed-world:** the engine
registers **only** tables that have a `GovernedTable` entry in the request's
catalog; an unlisted live table is simply not resolvable (the client gets the
same "table not found" plan error a nonexistent table gets — no existence
leak). Deny-by-default is then enforced at **both** the edge (query-api never
lists an ungranted type) and the engine (an omitted table does not exist for
the session) — defense in depth. The blast radius is nil: the governed-SQL
ticket has **no production caller today** (slice 1 landed the path + tests
only; grep shows `GovernedStatementQuery` construction only in
`engine/tests/governed_flight.rs` and `engine-wire/tests/governed_ticket.rs`),
so only slice-1 tests pinning the old semantics change
(`engine-serving/tests/governed_sql.rs:275`,
`empty_policy_is_full_visibility`).

## Context — what exists, and the seams slice 2 plugs into

- **The governed primitive (slice 1, landed):** `GovernedTableProvider`
  (`engine-serving/src/governed.rs:174`) — enforcing by construction: `scan`
  reads the inner provider over its full schema (`:238`), unconditionally ANDs
  the policy row filters (`:242`), then drops denied / masks (`'***'`) columns
  (`:254-273`). `execute_governed_sql_stream` (`:289`) runs client SQL over a
  `SessionContext` of governed providers. The engine dispatches an internal
  Flight ticket to it: `EngineTicket::GovernedSql` →
  `do_get_governed_sql` (`engine/src/flight.rs:117`, dispatch `:247-253`).
- **The wire payload already crosses the internal wire:**
  `GovernedStatementQuery { sql, catalog: GovernedCatalog }`
  (`engine-wire/src/flight.rs:93`) — JSON ticket, `deny_unknown_fields`,
  decoded at `flight.rs:220`. `GovernedCatalog`/`GovernedTable` are pure-data
  core types (`core/src/governed.rs:29`), serde round-trip proven. **What is
  missing is a client method**: `FlightSqlClient` (`engine-wire/src/flight.rs:323`)
  has `execute_stream` (`:352`, ungoverned) and `execute_as_of` (`:391`) but no
  governed variant.
- **The external-listener precedent:** query-api's governed Flight export
  (`flight_export.rs:103`, spawned in `serve.rs:95-101` when
  `LOOM_FLIGHT_BIND_ADDR` is set). Its posture — authenticate the gRPC
  `authorization` metadata per verb (`flight_export.rs:161`), re-derive
  governance **per call** so a forged/replayed ticket is still a governed
  request, stream engine batches straight out with a row cap, scrub internal
  detail from every error (`:182-202`) — carries over verbatim. Where this
  slice **diverges**: the ticket carries **SQL** (a standard
  `TicketStatementQuery`), not a loom `ExportCommand`; no schema is attached to
  `get_flight_info` (arbitrary SQL — the client reads the schema from the
  `do_get` stream's first message, exactly like the engine's internal surface,
  `engine/src/flight.rs:281-283`); and the row cap cannot use the export's
  `LIMIT max_rows+1` compile trick (we do not rewrite client SQL), so the cap
  is stream-side counting only.
- **Auth:** `service_runtime::auth::resolve_bearer` (`runtime/src/auth.rs:82`)
  resolves a token hash session-first-then-service-token — but it is private,
  and the Flight export's own `authenticate` (`flight_export.rs:161`) predates
  service tokens and checks `resolve_session` **only**. This slice makes
  `resolve_bearer` `pub`, extracts one shared gRPC-metadata `authenticate` for
  both Flight surfaces, and thereby also fixes the export to accept service
  tokens (the credential external/headless consumers actually hold —
  `Auth::create_service_token`, `core/src/auth.rs:200`).
- **Policy resolution:** `load_policy` (`query-api/src/governed.rs:103`) folds
  a subject's `policies_for` into `(row_filters, denied, masked)` per type;
  the coarse Read gate is `acl.check(subject, Read, Type) == Decision::Deny`
  (`governed.rs:67`). Types enumerate via `Ontology::list_types`
  (`core/src/ontology.rs:919`), which query-api reaches over the engine wire
  (`wire_control_plane.rs:175`) — no new RPC needed.
- **Config:** query-api has a typed layered config (`QueryApiConfig`,
  `config.rs:42`, `loom_config::LayeredConfig` at `:46`). The export's knobs
  are raw env reads ([[fut-flight-export-config-seam]] tracks folding them in);
  **the new knobs must use the typed seam from day one.**

## Architecture

### Who hosts the listener, and how policy travels

**query-api hosts the external TCP listener; the resolved policy travels to
the engine inside the existing `GovernedStatementQuery` ticket over the
internal UDS Flight wire.** Rationale, reconciling the 2026-06-30 decision
with the current engine-wire split:

- query-api is the **edge**: it owns bearer-token resolution (its remaining
  direct-Postgres use is exactly auth resolution) and already hosts the one
  externally-bound TCP Flight listener (the export). The engine deliberately
  has **no auth surface** — "authorize at the edge, execute blindly" (slice-1
  spec), and every engine listener today is a UDS.
- query-api is **zero-DataFusion** — it cannot run the SQL. It does not need
  to: slice 1 put the entire execution path engine-side and left the
  `GovernedStatementQuery { sql, catalog }` ticket as the seam. query-api's
  job is precisely what the 2026-06-30 note says: *resolve `load_policy` into
  the catalog*, then forward.
- The alternative (engine hosts the TCP listener, calls back to query-api for
  auth/policy) would add an auth dependency and a reverse RPC to the engine
  for zero benefit — the batches would still transit the same processes.

Per external request, query-api:

1. **authenticates** the gRPC `authorization` bearer via the shared helper
   (session → service token);
2. **resolves the per-connection governed catalog** for that subject (below);
3. builds `GovernedStatementQuery { sql, catalog }` and calls the new
   `FlightSqlClient::execute_governed_stream` against the engine's UDS;
4. **re-encodes** the resulting batch stream to the external client with the
   row cap applied — the same back-pressured, never-materialized relay the
   export uses (`flight_export.rs:254-293`).

### The new external service: `FlightSqlWireService` (query-api)

A second raw `FlightService` impl (like the export and the engine — not
arrow-flight's `FlightSqlService` helper trait, for consistency and zero new
dep surface), in `query-api/src/flight_sql.rs`:

- **`get_flight_info`** — authenticate; decode the descriptor `cmd` as an
  Any-packed `CommandStatementQuery` (anything else →
  `unimplemented`/`invalid_argument`, mirroring `engine/src/flight.rs:273-280`);
  return a `FlightInfo` whose endpoint ticket is a `TicketStatementQuery`
  carrying the SQL bytes, **no schema attached** (client reads it from the
  `do_get` stream).
- **`do_get`** — authenticate; decode the ticket as an Any-packed
  `TicketStatementQuery` **only**. **Load-bearing rejection:** the external
  surface must never decode loom-native JSON tickets — a client-supplied
  `GovernedStatementQuery` carrying its own permissive catalog would be a
  total governance bypass. The governed catalog is constructed **server-side,
  per call, from the authenticated subject** — a forged/replayed ticket is
  still a governed request (the export's posture). Then steps 2–4 above.
- **Row cap** — count rows on the outgoing stream; past `max_rows`, emit a
  stream error (explicit failure, never silent truncation). No `+1` sentinel
  is possible on arbitrary SQL; the divergence from the export is documented
  at the knob.
- **Error scrubbing** — planning faults (bad SQL, unknown/unlisted table)
  surface as `invalid_argument` with the engine's plan message (the client's
  own SQL vocabulary); everything else is an opaque `internal` with detail
  logged server-side, mirroring `flight_export.rs:182-202`. An unlisted
  (ungranted or unbound) table fails as "table not found" — indistinguishable
  from nonexistent, no existence leak.
- All other Flight verbs: `unimplemented` (slice 3+ fills in metadata
  commands / prepared statements / `do_put`).

### Per-connection governed catalog resolution (query-api)

`resolve_governed_catalog(ontology, acl, subject) -> Result<GovernedCatalog, QueryError>`
in `query-api/src/governed.rs`, next to `load_policy` (`:103`):

- `list_types(PageReq::unbounded())`; for each type, coarse Read gate
  (`acl.check == Decision::Deny` ⇒ **omit** — deny-by-default at the edge);
  else `load_policy` → `GovernedTable { table: otype.table, row_filters,
  denied, masked }`.
- **Fail closed:** any ACL/ontology error aborts the whole resolution — never
  an empty-policy (fully-visible) fallback entry.
- **Duplicate table binding:** if two allowed types bind the same `TableRef`,
  the first entry wins (`GovernedCatalog::table_for` is first-match,
  `core/src/governed.rs:36`) and a `tracing::warn!` fires. This leaks nothing
  beyond existing capability: each type's policy is already independently
  reachable over the HTTP read path, so serving any one allowed type's policy
  for the shared table stays within what the subject can already read.
- Resolution is **per request** (both Flight verbs), not cached per
  connection: policy changes take effect on the next call, matching the HTTP
  path and the export (a "per-connection" catalog in the decision's sense —
  each request's session context — not a long-lived cached one).

### Closed-world registration (engine-serving)

`execute_governed_sql_stream` (`engine-serving/src/governed.rs:289`) changes
its loop: a live table with **no** `GovernedTable` entry is **skipped**, not
registered fully-visible. The `GovernedCatalog` doc comment
(`core/src/governed.rs:24-27`) is rewritten to the closed-world contract.
Unbound datasets, ungranted types, and any future table kind are thereby
invisible to external SQL unless the edge explicitly lists them.

### Auth flow

```
external client ──TCP gRPC──▶ FlightSqlWireService (query-api)
  authorization: Bearer <token>        │ token_sha256 (crypto.rs:50)
                                       ▼
             service_runtime::resolve_bearer (auth.rs:82, made pub)
               ├─ Auth::resolve_session        (login sessions)
               └─ Auth::resolve_service_token  (service accounts)
                                       │ SubjectId (else Unauthenticated)
                                       ▼
             resolve_governed_catalog(subject)  — deny-by-default per type
                                       │ GovernedCatalog
                                       ▼
  GovernedStatementQuery{sql, catalog} ──UDS Flight──▶ engine do_get
                                       ▼
             execute_governed_sql_stream — closed-world governed providers
```

Missing/invalid/expired token → `Unauthenticated`; authenticated but nothing
granted → every table unresolvable (`invalid_argument` plan error). Both
Flight verbs authenticate independently (the export's per-verb posture).

### Config (typed seam — not raw env reads)

New `SqlWireTuning` domain on `QueryApiConfig` (`config.rs:42`), loaded through
`loom_config::LayeredConfig` (defaults < file < env), honoring
[[fut-flight-export-config-seam]]'s complaint from day one:

- `sql_wire.bind_addr: Option<String>` (`LOOM_SQL_WIRE_BIND_ADDR`) — unset ⇒
  the listener does not start (opt-in, like the export). `validate()` parses
  it as a `SocketAddr` — a malformed value is a startup error, never a
  silently-down listener.
- `sql_wire.max_rows: u32` (`LOOM_SQL_WIRE_MAX_ROWS`, default `1_000_000`,
  must be ≥ 1) — the stream-side row cap.

`serve.rs` spawns the listener from the typed config (eager bind, hard
startup failure on a bad port — mirroring `spawn_flight_export`,
`serve.rs:107`). The export's own raw-env knobs stay as they are —
[[fut-flight-export-config-seam]] remains open for them.

## Non-regression

- **The one behavioral change to existing code** is the closed-world flip in
  `execute_governed_sql_stream` — a path with no production caller; the
  slice-1 tests that pinned "absent ⇒ visible" are rewritten to pin the new
  contract (entry-with-empty-policy = fully visible stays; absent = invisible
  is new).
- The internal ungoverned read plane (`EngineTicket::Sql`), the as-of plane,
  vector search, the file-ticket plane, and every HTTP read path are
  untouched.
- The Flight **export** gains service-token acceptance via the shared
  `authenticate` (a strict widening: session tokens keep working); its e2e
  suite must stay green otherwise unchanged.

## Acceptance criteria (e2e, over real TCP with a real Flight client)

1. **Arbitrary governed SQL:** an external Flight SQL client authenticates
   with a bearer token, runs a multi-table `JOIN`/`GROUP BY` it authored, and
   receives exactly the rows its row-filter policy admits, with masked columns
   streamed as `'***'` `Utf8` — proven against seeded policies
   (`grant_read_filtered` / `grant_read_columns`, `e2e_support.rs:647/:671`).
2. **Denied column:** absent from `SELECT *`'s result schema; naming it is a
   plan error (`invalid_argument`), not a value leak.
3. **Deny-by-default:** a type without a Read grant — and a landed dataset
   bound to no type — is unresolvable, indistinguishable from nonexistent.
4. **Auth:** no bearer token → `Unauthenticated`; a well-formed but unknown
   token → `Unauthenticated`; a **service token**
   (`create_service_account` + `create_service_token`) works end-to-end.
5. **No ticket bypass:** a hand-forged loom-native JSON ticket (a
   `GovernedStatementQuery` carrying a permissive catalog) sent to the
   external `do_get` is rejected (`invalid_argument`), never executed.
6. **Row cap:** a result exceeding `max_rows` fails the stream explicitly.

## Out of scope (slice 3+, stays deferred — explicitly)

- **Catalog-metadata commands** (`CommandGetTables`/`CommandGetDbSchemas`/
  `CommandGetCatalogs`), **prepared statements**, and **`do_put`** ingest —
  [[fut-flight-sql-surface]]. Clients discover schemas from result streams
  until then.
- **TLS/mTLS** on the external wire — [[fut-flight-export-tls]] (covers both
  external Flight listeners). v1 posture matches the export: plaintext TCP,
  deploy behind the operator's TLS terminator.
- **Governed inner-scan pushdown** — [[fut-governed-scan-pushdown]] stays
  deferred; this wire is what will make its latency win measurable.
- **The UI SQL console's HTTP endpoint** — [[fut-ui-sql-query-console]]; it
  reuses `resolve_governed_catalog` + the governed engine path, but the HTTP
  surface is its own item.
- Multi-statement/transaction semantics; per-subject rate limiting; catalog
  caching across requests.

## Interfaces (names the plan consumes)

- Consumes: `GovernedTableProvider` / `TablePolicy` / `policy_for` /
  `execute_governed_sql_stream` (`engine-serving/src/governed.rs:174/:143/:152/:289`);
  `GovernedCatalog::table_for` (`core/src/governed.rs:36`);
  `GovernedStatementQuery` + `EngineTicket::GovernedSql`
  (`engine-wire/src/flight.rs:93/:153`); `FlightSqlClient`
  (`engine-wire/src/flight.rs:323`); `do_get_governed_sql`
  (`engine/src/flight.rs:117`); `authenticate` + the relay/cap/scrub shapes
  (`query-api/src/flight_export.rs:161/:244-293`); `resolve_bearer`
  (`runtime/src/auth.rs:82`) + `token_sha256` (`runtime/src/crypto.rs:50`);
  `load_policy` (`query-api/src/governed.rs:103`); `Ontology::list_types`
  (`core/src/ontology.rs:919`; wire: `wire_control_plane.rs:175`);
  `QueryApiConfig`/`LayeredConfig` (`query-api/src/config.rs:42/:46`);
  `spawn_flight_uds`/`EngineGuard` (`src/testing/flight.rs:144/:67`);
  e2e-support seed/ACL helpers (`e2e_support.rs`).
- Produces (later plan tasks rely on these EXACT names/types):
  - Closed-world `execute_governed_sql_stream` (skip tables absent from the
    catalog) + the rewritten `GovernedCatalog` contract doc.
  - `FlightSqlClient::execute_governed_stream(sql: String, catalog:
    GovernedCatalog) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch>>
    + Send>>>` in `engine-wire`.
  - `pub async fn resolve_governed_catalog(ontology, acl, subject) ->
    Result<GovernedCatalog, QueryError>` in `query-api/src/governed.rs`.
  - `pub(crate) mod flight_auth` in query-api: `authenticate(auth, metadata)
    -> Result<SubjectId, Status>` (session + service token), shared by
    `flight_export` and `flight_sql`; `service_runtime::resolve_bearer` made
    `pub`.
  - `FlightSqlWireService` in `query-api/src/flight_sql.rs` + `spawn_sql_wire`
    in `serve.rs`.
  - `SqlWireTuning { bind_addr: Option<String>, max_rows: u32 }` on
    `QueryApiConfig` (`LOOM_SQL_WIRE_BIND_ADDR` / `LOOM_SQL_WIRE_MAX_ROWS`).
