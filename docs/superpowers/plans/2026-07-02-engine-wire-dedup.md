# road-engine-wire-dedup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Behavior-preserving dedup of the engine wire surface plus structured
wire error classes. The engine's four-plane `do_get` (cc 21, census
`docs/code-health/complexity.md:64`) becomes a flat match over a new
`EngineTicket` decode enum that lives in engine-wire beside the ticket types
whose `deny_unknown_fields` disjointness it depends on; the 4×-copied Flight
encode tail collapses into one `encode_response`; `FlightTicketReq` (a 1:1
field copy of `FlightTicket`) is deleted. On the client, `execute` becomes
`execute_stream(sql).await?.try_collect()`, the 4×-copied stream-decode block
becomes `decode_batches`, and the 10 clone-shaped `gov_*` getters collapse
under a `gov_rpc!` macro. Finally, `EngineServingError` gains a
`Plan(DataFusionError)` class (only `ctx.sql()` faults — conservative), the
engine maps it via one total `serving_status` fn (`Plan → invalid_argument`),
and the client inverts it (`sql_status`: `InvalidArgument → Validation`) so a
planning-class SQL fault reaches query-api as `ServingError::Plan` → HTTP 400
instead of an opaque 500 — the item's small, enumerated behavior-change
whitelist (three entries, below).

**Register drift (corrections to the ROADMAP/spec prose, verified against the
tree 2026-07-02, branch `work/road-engine-wire-dedup` off current `main`
`8779361` — none of today's adjacent PRs touched this item's files: #313 is
ingest-only, #314 touched `query-api/src/{sql,handler}.rs` + sql tests, #315
touched the `action.rs` family, #310 touched the query-api read path only. No
target was pre-absorbed; all locations below are current):**

- **"~20 clone-shaped governance getters (~200 lines)" is drift.** There are
  exactly **10** `gov_*` methods (`engine-wire/src/client.rs:214-378`, ~165
  lines including docs): `gov_check`, `gov_policies_for`, `gov_get_type`,
  `gov_resolve`, `gov_links`, `gov_links_to`, `gov_list_types`,
  `gov_get_action`, `gov_vector_indexes_for`, `gov_get_vector_index`. The
  spec's "multi-field methods stay hand-written" caveat is unnecessary with
  the chosen macro shape (the request's field initializers are macro
  arguments, so `se(subject)?`-style multi-field requests fit) — all 10 go
  through `gov_rpc!`. The 7 data-plane RPCs (`flush_table` … `compact_table`)
  and the `Queue` impl stay hand-written: different error mapping (`be`, not
  `cp_status`) and typed non-JSON responses.
- **"one `From<EngineServingError> for tonic::Status`" cannot land where the
  spec implies.** The orphan rule puts that impl in engine-serving, which has
  **no tonic dependency by design** (it is the DataFusion library; the wire is
  the engine's concern — verified `src/services/engine-serving/BUCK` deps).
  Deviation: one **total** `pub fn serving_status(e: EngineServingError) ->
  Status` in `engine/src/flight.rs`, unit-tested from a pure `rust_test`
  (precedent: `all_in_live_set` / the `flight-membership-helper` target).
  Same single mapping, no new cross-crate dep.
- **"so query-api returns 400" needs a query-api leg the spec section does
  not spell out.** `EngineServingClient::fetch_rows`
  (`query-api/src/engine_client.rs:34-46`) flattens every client error to
  `ServingError::Engine(e.to_string())`, and #310's total
  `query_error_response` (`http.rs:420-444`) routes non-NoIndex/DimMismatch
  `Serving` members to the opaque-500 arm. The class survives only with a new
  `ServingError::Plan` variant, a mapping arm in `fetch_rows`, and an explicit
  400 arm in `query_error_response` (Tasks 7).
- **The census pair for the client dance is confirmed at today's lines:**
  `FlightSqlClient::execute` (`engine-wire/src/flight.rs:206-239`) ≈
  `execute_stream` (`:247-286`) — census `duplication.md:92`
  (`flight.rs:223-254 ≈ 179-207` at `a20c351`). The stream-decode block
  additionally appears in `fetch` (`:145-149`) and `vector_search`
  (`:174-177`) — 4 sites total.
- **engine-wire's `Cargo.toml` deliberately under-declares** (`thiserror`,
  `futures`, `arrow-array` are BUCK-only deps today). The one new third-party
  need, `arrow-schema` (for the `Any::unpack` error type in `TicketError`),
  therefore lands as a **BUCK-only dep** following the crate's existing
  convention — no `Cargo.toml`/lockfile/`buckify.sh` churn, and the
  `reindeer-check` hook is not triggered.

**Architecture (key decisions, verified against the tree):**

- **`EngineTicket` + `TicketError` live in `engine-wire/src/flight.rs`**, next
  to `FlightTicket`/`VectorSearchTicket`/`GovernedStatementQuery` — the
  decode-order invariant is a property of those types' `deny_unknown_fields`
  disjointness, so it belongs with them. The load-bearing comments (protobuf
  `Any` first because JSON starts with `{`; decode-then-`is::<>()` ordering;
  disjoint-fields guarantees) move **verbatim** from `engine/src/flight.rs`.
  `TicketError`'s four variants reproduce the current Status messages
  byte-for-byte via their `Display` (`bad flight-sql ticket: {e}` /
  `flight-sql ticket unpack returned None` / `non-utf8 sql: {e}` /
  `bad flight ticket: {e}`), and `impl From<TicketError> for tonic::Status`
  keeps the current code split (`FlightSqlEmpty → internal`, everything else
  `→ invalid_argument`). engine-wire already depends on tonic, so the From
  impl is orphan-legal there.
- **The fall-through semantics are preserved exactly:** a valid `Any` that is
  not a `TicketStatementQuery`, or bytes that fail the governed/kNN JSON
  decodes, fall through to the file-ticket decode whose error is the terminal
  one — but a matched `TicketStatementQuery` that fails unpack/utf8 does NOT
  fall through (it errors immediately), exactly as today.
- **`encode_response` is an inherent fn on `FlightDataService`** taking
  `impl Stream<Item = Result<RecordBatch, FlightError>> + Send + 'static`,
  owning the `FlightDataEncoderBuilder … map_err(Status::internal)` tail and
  the `Response::new(Box::pin(…))` — the four call sites keep their own input
  stream construction (DataFusion-error mapping / one-shot batch / batch vec).
- **Conservative `Plan` classification:** exactly the three `ctx.sql(sql)`
  call sites become `EngineServingError::Plan`
  (`engine-serving/src/serving.rs:468` `execute_query`, `:488`
  `execute_query_stream`, `governed.rs:319` `execute_governed_sql_stream`).
  `df.collect()` / `df.execute_stream()` / stream items / catalog and
  provider errors stay `Engine` (execution/backend → internal/500). The
  variant carries the `DataFusionError` **source** (no Display-flattening —
  the paid-down `map_err_ignore` debt stays paid).
- **`sql_status` sits beside `cp_status` in `engine-wire/src/client.rs`** and
  is applied ONLY to the Flight **SQL** plane's `get_flight_info`/`do_get`
  call errors in `FlightSqlClient::execute_stream` (after Task 4, `execute`
  delegates so there is one body). Mid-stream `FlightError` items, the
  missing-ticket fault, and the file/kNN planes keep `be` — `InvalidArgument`
  never legitimately reaches those paths from a well-formed client, and the
  kNN plane already has its own class-preserving mapping
  (`VectorSearchError`).
- **`ServingError::Plan(String)` in query-api has a pass-through Display**
  (`#[error("{0}")]`) — the wire message is already the engine's
  `query planning failed: {df}`, so no double prefix. The 400 body echoes the
  wire message, following the existing `DimMismatch` precedent
  (`http.rs:436-438` echoes engine-built messages).
- **`gov_rpc!` is a `macro_rules!` in `client.rs`** whose invocation carries
  the doc comment, the method name + argument list, the RPC/response-field
  names, and the pb request as one `$req:expr` — the generated body is the
  exact
  `self.inner.clone().$rpc($req).await.map_err(cp_status)?.into_inner(); de(&resp.$out)`
  shape all 10 getters share today. The matcher captures the argument list as
  `$($args:tt)*` and the request as a trailing `expr` deliberately: the naive
  `$arg:ident : $ty:ty` / `$f:ident : $v:expr` shapes violate `macro_rules!`
  follow-set rules (`ty` may not precede `)`, `expr` may not precede `}`).
  `be` (the flatten-everything mapping) gains the spec's warning comment:
  after this item it is legitimate only for transport faults and planes with
  no error classes.
- **Pins before refactor (the byte-identity lesson from #310):** the survey
  found three wire behaviors with no coverage — the ticket-decode error
  statuses/messages (no test sends a garbage or non-utf8 ticket), the
  wrong-`Any`-type fall-through, and the kNN `DimMismatch → invalid_argument`
  wire mapping (query-api's dim-mismatch 400 e2e, `vector_search_e2e.rs:165`,
  runs the **in-process** engine, not the wire; `vector_search_flight.rs`
  pins only `NoIndex`). Task 1 adds these pins FIRST, green against the
  current code.

**Tech Stack:** Rust (edition 2024), buck2, `loom_rust_test` (two new pure
targets: `engine-wire:engine-ticket`, `engine:serving-status`) +
`loom_fixture_test` (one new: `engine:ticket-errors`). No third-party
version changes, no `Cargo.toml`/lockfile/`.sqlx` changes; one BUCK-only dep
addition (`//third-party:arrow-schema` to `engine-wire`).

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md`
  (§ "road-engine-wire-dedup"), with the drift corrections above. Register:
  `docs/ROADMAP.md` `#road-engine-wire-dedup` (locate by id, line numbers are
  stale).
- **Behavior-preserving:** the wire encoding (JSON tickets, protobuf `Any`,
  Arrow IPC framing), the SQL text sent over the wire, the ticket decode
  order, every Status code/message, and the `QueryError` → HTTP mapping stay
  byte-identical **except** the three whitelisted changes below. The file
  plane's membership guard, the governance RPC JSON envelopes, and the kNN
  plane's NoIndex/DimMismatch statuses are pinned and unchanged.
- **Existing tests pass unmodified.** No existing test function or assertion
  is edited. The only test files touched are appends
  (`engine/tests/vector_search_flight.rs`, `engine/tests/flight_sql.rs`,
  `engine-wire/tests/cp_status.rs`,
  `query-api/tests/{query_error_http,engine_wire_serving_e2e}.rs`, and the
  in-process fidelity arm in `query-api/tests/e2e_support.rs`), plus the three
  new files `engine/tests/ticket_errors.rs`, `engine/tests/serving_status.rs`,
  `engine-wire/tests/engine_ticket.rs`.
- **TDD:** every new fn/type lands with its test written first and observed
  red; pure-refactor tasks run their pinning suite green before AND after.
- Tests are separate `rust_test`/`loom_fixture_test` targets wired in BUCK —
  never inline `#[cfg(test)]` (`no-inline-tests` hook). New fixture tests use
  `loom_fixture_test`, never bare `rust_test`.
- Clippy pedantic+restriction on prod code: no
  unwrap/expect/panic/indexing/print; `map_err` closures use named bindings
  (`|_|` trips `map_err_ignore`); **no new `#[expect]`** (the one existing
  `expect_used` expect on `GovernedStatementQuery::encode` is untouched).
- **Never pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to
  a file and grep it. Fixture runs use `-j 8` (postgres boot-slot
  starvation). Whole-tree checks only via `buck2 build -M none //src/...`;
  scope tests to the engine/engine-wire/engine-serving/worker/query-api
  packages.
- `buck2 run //tools:prek -- run --all-files` must report zero `Failed`
  before **every** commit (rustfmt is a separate hook from clippy).
  Conventional Commits; one commit per task.

## Current wire contract (documented, preserved except the whitelist)

`do_get` decode order (`engine/src/flight.rs:129-219`), each stage's error
behavior — this table is what Task 1's pins + Task 2's unit tests freeze:

| Stage | Match | On error | Status (message) |
| --- | --- | --- | --- |
| 1 | protobuf `Any` wrapping `TicketStatementQuery` | unpack fails | `invalid_argument` (`bad flight-sql ticket: {e}`) — no fall-through |
| 1 | ″ | unpack returns `None` | `internal` (`flight-sql ticket unpack returned None`) — no fall-through |
| 1 | ″ | handle not UTF-8 | `invalid_argument` (`non-utf8 sql: {e}`) — no fall-through |
| 1 | valid `Any`, different type | — | falls through to 2 |
| 2 | `GovernedStatementQuery` JSON | decode fails | falls through to 3 |
| 3 | `VectorSearchTicket` JSON | decode fails | falls through to 4 |
| 4 | `FlightTicket` JSON | decode fails | `invalid_argument` (`bad flight ticket: {e}`) — terminal |

Downstream statuses: SQL/governed planes map every `EngineServingError` to
`internal` (`:51`, `:72`); the kNN plane hand-matches `NoIndex → not_found`,
`DimMismatch → invalid_argument`, other → `internal` (`:102-106`); the file
plane's membership guard returns `invalid_argument` without echoing paths
(`:180-204`). Clients: `be` flattens to `ControlPlaneError::Backend`;
`cp_status` inverts the governance mapping (`NotFound`/`Aborted` classed);
`FlightTableClient::vector_search` inverts the kNN mapping
(`VectorSearchError`).

## Verified claim inventory (register claim → current evidence)

| Register claim | Verdict | Current evidence |
| --- | --- | --- |
| engine `do_get` cc 21, four-stage dispatch, decode-order comments load-bearing | CONFIRMED | `engine/src/flight.rs:129-219`; census `complexity.md:64` |
| encode tail copied 4× | CONFIRMED | `flight.rs:53-56, 74-77, 108-111, 215-218` (`FlightDataEncoderBuilder::new().build(…).map_err(internal)` + `Response::new(Box::pin(…))`) |
| `FlightTicketReq` is a 1:1 field copy of `FlightTicket` | CONFIRMED | `flight.rs:311-328`; sole consumer is `do_get:164` |
| `execute` = `execute_stream().try_collect()`; census pair | CONFIRMED | `engine-wire/src/flight.rs:206-239 ≈ 247-286`; identical `CommandStatementQuery` dance, identical error mapping (stream items already `be`-mapped) — delegation is byte-identical |
| shared `decode_batches` | CONFIRMED (4 sites) | `flight.rs:145-149` (fetch), `:174-177` (vector_search), `:233-237` (execute), `:280-284` (execute_stream) |
| `gov_rpc!` for ~20 getters / ~200 lines | DRIFTED (10 / ~165) | `client.rs:214-378`; all 10 share `inner.clone().rpc(pb::Req{…}).map_err(cp_status)?.into_inner()` + `de(&resp.field)`; all 10 fit the macro |
| `Engine(String)` + `to_serving` erase classes; bad SQL → 500 | CONFIRMED | `EngineServingError` (`serving.rs:37-50`), `to_serving` (`:53-55`); `do_get_sql` maps all to `internal` (`flight.rs:51`); `fetch_rows` flattens to `ServingError::Engine` (`engine_client.rs:41`); `query_error_response` routes it to opaque 500 (`http.rs:439-441`) |
| only `ctx.sql()` → Plan | CONFIRMED (3 sites) | `serving.rs:468,488`, `governed.rs:319` |
| client maps `InvalidArgument → Validation`, mirrors `cp_status` | CONFIRMED (variant exists) | `ControlPlaneError::Validation` (`core/src/error.rs:31`); `cp_status` precedent `client.rs:23-32` + pinned by `engine-wire/tests/cp_status.rs` |
| #314 (SelectInputs) absorbed targets here | REFUTED | #314 touched `query-api/src/{sql,handler}.rs` only; zero overlap with engine/engine-wire/engine-serving. #313 (ingest ApiError) and #315 (action decomposition) likewise |
| wire behaviors pinned by tests | PARTIAL | Pinned: SQL roundtrip + malformed-SQL-errors (`flight_sql.rs`, code-agnostic `is_err`), governed plane (`governed_flight.rs`), kNN top-k + `NoIndex → not_found` (`vector_search_flight.rs:291`), membership guard (`flight_ticket_membership.rs`), queue/compact/write RPCs (`wire.rs`, `compact_wire.rs`, `write_wire.rs`), gov getters over the wire (query-api `wire_governance_e2e`, `wire_governed_read_e2e`, `wire_lineage_e2e`), streaming client (`engine_wire_serving_e2e`, `governed_flight_export_e2e`, worker `flight_roundtrip`). **Unpinned:** ticket-decode error statuses/messages, wrong-`Any`-type fall-through, kNN `DimMismatch` over the wire → Task 1 pins first |

## Call-site inventory (verified by grep; the plan updates every one)

| Symbol | Call sites | Updated in |
| --- | --- | --- |
| `FlightTicketReq` (private) | `engine/src/flight.rs:164` | Task 3 (deleted) |
| `FlightSqlClient::execute` | `query-api/src/engine_client.rs:39` | body only (Task 4); caller untouched |
| `FlightSqlClient::execute_stream` | `query-api/src/flight_export.rs:255` | body only (Tasks 4, 6); caller untouched |
| `FlightTableClient::fetch` | `worker/src/compact.rs:54` | body only (Task 4); caller untouched |
| `FlightTableClient::vector_search` | `query-api/src/engine_client.rs:71` | body only (Task 4); caller untouched |
| `gov_*` (10 methods) | `query-api/src/wire_control_plane.rs` (11 sites) | bodies regenerated by macro (Task 5); signatures identical, callers untouched |
| `EngineServingError` construction | `engine-serving/src/{serving,governed,vector_search,provider,action_writer}.rs` | only the three `ctx.sql` sites change class (Task 6) |
| `EngineServingError` consumers | `engine/src/flight.rs:51,72,102-106` (→ `serving_status`, Task 6); `engine/src/service.rs:254,280` (writer paths — **untouched**, stay `internal`); `query-api/tests/e2e_support.rs:177-181` (gains `Plan` arm, Task 7) |
| `ServingError` HTTP mapping | `http.rs:420-444` (total mapping) | explicit `Plan` arm (Task 7) |
| `be` / `cp_status` / `to_serving` | engine-wire `client.rs:15,23`; engine-serving `serving.rs:53` | comment-only (Tasks 5, 6) |

## Deliberate behavior changes (the whitelist — everything else is byte-identical)

1. **Wire: planning-class SQL faults change Status.** A statement that fails
   DataFusion **planning** (`ctx.sql`) on the Flight SQL or governed plane —
   old: `internal`, message `engine serving: {df}`; new: `invalid_argument`,
   message `query planning failed: {df}`. Execution/stream/catalog faults are
   NOT reclassified. Pinned by: new `flight_sql.rs::malformed_sql_is_validation_class`
   (red first, Task 6) + `serving_status` unit tests. The only pre-existing
   pins on this path assert `is_err()` only (`flight_sql.rs:115-117`,
   `engine_wire_serving_e2e.rs:103-107`) and stay green.
2. **engine-wire client: `InvalidArgument` on the SQL plane reclassifies.**
   `FlightSqlClient` errors for `InvalidArgument` statuses — old:
   `ControlPlaneError::Backend(status.to_string())`; new:
   `ControlPlaneError::Validation(status.message())` via `sql_status`
   (mirroring `cp_status`). Other codes, mid-stream faults, and the
   file/kNN planes keep `be`. Pinned by: `cp_status.rs` appends (Task 6).
3. **query-api HTTP: planning-class read faults become 400.** A governed read
   whose engine execution fails at planning — old: opaque 500 (logged); new:
   `400` with the plan message as body (`ServingError::Plan` arm in the total
   mapping; body-echo precedent: `DimMismatch`). Unreachable via today's HTTP
   routes in a healthy deployment (loom compiles all SQL itself and every
   bound type has a live backing table) — the register notes it "matters
   before `fut-external-sql-wire` exposes the path". Pinned by:
   `query_error_http.rs` append + new
   `engine_wire_serving_e2e.rs::malformed_sql_is_plan_class` (Task 7). The
   existing 500-pins (`query_error_http.rs::internal_variants_are_opaque_500`,
   `serving_fault_logging.rs`) use `ServingError::Engine` and are unaffected.

There is no fourth item. In particular: ticket-decode error statuses AND
messages, `NoIndex`/`DimMismatch` wire statuses and messages, the membership
guard, all governance/queue/write RPC envelopes, the SQL text sent over the
wire, and the Arrow stream framing are pinned byte-identical.

---

### Task 1: Pin the unpinned wire behaviors (green against the CURRENT code)

Three refactor-relevant behaviors have zero coverage: the ticket-decode error
statuses/messages, the wrong-`Any`-type fall-through, and the kNN
`DimMismatch → invalid_argument` wire mapping. Add pins FIRST; they must pass
against the unmodified tree — a failure here means a mis-written pin: fix the
TEST, never the production code.

**Files:**
- Create: `src/services/engine/tests/ticket_errors.rs`
- Test (append): `src/services/engine/tests/vector_search_flight.rs`
  (existing target `//src/services/engine:vector-search-flight` — no BUCK
  change)
- Modify: `src/services/engine/BUCK` (new `ticket-errors` fixture target)

**Interfaces:**
- Consumes: `engine::flight::FlightDataService`, raw
  `arrow_flight::flight_service_client::FlightServiceClient` over a
  hand-rolled UDS channel (the wrapper clients cannot send malformed bytes),
  `arrow_flight::sql::{CommandStatementQuery, TicketStatementQuery,
  ProstMessageExt}`.
- Produces: four pins Tasks 2-3 and 6 keep green.

- [x] **Step 1: Create `ticket_errors.rs`**

```rust
//! Pins for `do_get`'s ticket-decode error contract — the statuses and messages
//! the four-stage decode emits for malformed tickets. No test previously sent a
//! garbage or non-utf8 ticket, so these behaviors were unpinned; they must stay
//! byte-identical when the dispatch moves onto `engine_wire::flight::EngineTicket`
//! (road-engine-wire-dedup). Green against the pre-refactor code.

use std::sync::Arc;
use std::time::Duration;

use arrow_flight::Ticket;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, TicketStatementQuery};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use engine::flight::FlightDataService;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use prost::Message;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Endpoint, Server, Uri};

/// Boot a `FlightDataService` on a UDS and return a RAW `FlightServiceClient`
/// (the wrapper clients only send well-formed tickets; these pins need to put
/// arbitrary bytes in `Ticket.ticket` and read the tonic `Status` directly).
async fn spawn_raw(
    fx: &PgFixture,
    db: &str,
    warehouse: &str,
) -> (tempfile::TempDir, FlightServiceClient<tonic::transport::Channel>) {
    let sock_dir = tempfile::tempdir().expect("socket dir");
    let sock_path = sock_dir.path().join("engine.sock");
    let sock = sock_path.to_string_lossy().to_string();

    let pool = fx.pool_for(db).await;
    let mut props = std::collections::HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), fx.pg_dsn(db));
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{warehouse}"),
    );
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("loom", props)
        .await
        .expect("catalog");
    let svc = FlightDataService {
        catalog,
        serving_catalog: IcebergCatalog::new(pool.clone()),
        serving_store: None,
        pool,
    };

    let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind uds");
    let incoming = UnixListenerStream::new(listener);
    tokio::spawn(async move {
        drop(
            Server::builder()
                .add_service(FlightServiceServer::new(svc))
                .serve_with_incoming(incoming)
                .await,
        );
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let channel = Endpoint::try_from("http://[::]:50051")
        .expect("endpoint")
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let sock = sock.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(sock).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
        .expect("connect uds");
    (sock_dir, FlightServiceClient::new(channel))
}

async fn do_get_err(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    ticket: Vec<u8>,
) -> tonic::Status {
    // match, not expect_err: the Ok side (Response<Streaming<..>>) has no
    // useful Debug and must never be printed anyway.
    match client
        .do_get(Ticket {
            ticket: ticket.into(),
        })
        .await
    {
        Ok(_) => panic!("malformed ticket must be rejected"),
        Err(s) => s,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ticket_decode_error_contract() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let (_sock_dir, mut client) =
        spawn_raw(fx, &db, &wh.path().display().to_string()).await;

    // Stage 4 terminal: bytes that are no known ticket -> the FILE plane's decode
    // error (the last stage in the fall-through chain), never a schema/path leak.
    let err = do_get_err(&mut client, b"not json".to_vec()).await;
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "got: {err}");
    assert!(
        err.message().starts_with("bad flight ticket: "),
        "terminal error is the file-plane decode, got: {}",
        err.message()
    );

    // Stage 1, no fall-through: a matched TicketStatementQuery whose handle is
    // not UTF-8 errors immediately (it must NOT be retried as JSON).
    let tsq = TicketStatementQuery {
        statement_handle: vec![0xff, 0xfe, 0xfd].into(),
    };
    let err = do_get_err(&mut client, tsq.as_any().encode_to_vec()).await;
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "got: {err}");
    assert!(
        err.message().starts_with("non-utf8 sql: "),
        "matched flight-sql ticket fails in place, got: {}",
        err.message()
    );

    // Stage 1 -> 4 fall-through: a VALID protobuf Any of the WRONG type is not a
    // flight-sql ticket; it falls through the JSON stages to the terminal error.
    let cmd = CommandStatementQuery {
        query: "SELECT 1".into(),
        transaction_id: None,
    };
    let err = do_get_err(&mut client, cmd.as_any().encode_to_vec()).await;
    assert_eq!(err.code(), tonic::Code::InvalidArgument, "got: {err}");
    assert!(
        err.message().starts_with("bad flight ticket: "),
        "wrong Any type falls through to the file-plane decode, got: {}",
        err.message()
    );
}
```

- [x] **Step 2: Append the kNN DimMismatch wire pin to
  `vector_search_flight.rs`**

The seed prologue is a deliberate clone of `vector_search_flight_top_k`'s
(this file's setup block is already a census cluster owned by
`road-test-wire-harness`; do not half-extract it here).

```rust
// ---------------------------------------------------------------------------
// WIRE PIN — a wrong-dimension query must surface as `invalid_argument`
// (client-side: `VectorSearchError::DimMismatch`). Previously pinned only via
// the in-process engine (query-api's vector_search_e2e), never over the wire.
// Seed prologue cloned from vector_search_flight_top_k (harness item collapses
// these later).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vector_search_dim_mismatch_is_invalid_argument() {
    use control_plane_postgres::PgControlPlane;

    let fx = PgFixture::shared();
    let (_cp_init, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("wh");
    let pool = fx.pool_for(&db).await;
    let catalog = make_catalog(fx.pg_dsn(&db), &wh.path().display().to_string()).await;
    let cp = PgControlPlane::new(pool.clone(), Duration::from_secs(5));

    let table = TableRef {
        schema: "wh".into(),
        name: "dimdocs".into(),
    };
    cp.ontology()
        .define_type(ObjectType {
            name: TypeName("DimDocs".into()),
            table: table.clone(),
            properties: vec![
                PropertyDef {
                    name: "id".into(),
                    ty: "Long".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
                PropertyDef {
                    name: "embedding".into(),
                    ty: "vector(4)".into(),
                    required: true,
                    constraints: control_plane_core::PropertyConstraints::default(),
                },
            ],
            derived: vec![],
            identity: Some("id".into()),
        })
        .await
        .expect("define_type");
    cp.ontology()
        .define_vector_index(VectorIndexDef {
            name: "by_flat".into(),
            type_name: TypeName("DimDocs".into()),
            property: "embedding".into(),
            metric: Metric::Cosine,
            spec: IndexSpec::Flat,
        })
        .await
        .expect("define_vector_index");

    let run = RunId(uuid::Uuid::new_v4());
    let rows: &[(i64, [f32; 4])] = &[(1, [1.0, 0.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0, 0.0])];
    land(
        &pool,
        &catalog,
        &table,
        &columns(),
        &ipc_body(rows),
        InlineLimits {
            inline_byte_limit: 0,
            flush_byte_threshold: i64::MAX,
        },
        lineage_evt(run, &table),
    )
    .await
    .expect("land rows");
    build_vector_index(&catalog, &pool, &table, "by_flat", RunId(uuid::Uuid::new_v4()))
        .await
        .expect("build_vector_index");

    let (_sock_dir, sock) = spawn_flight(fx, &db, &wh.path().display().to_string()).await;
    let client = FlightTableClient::connect(&sock).await.expect("connect");

    // Index dim is 4; query has length 2 -> DimMismatch (invalid_argument on the
    // wire), NOT NoIndex and NOT an opaque Engine error.
    let err = client
        .vector_search(VectorSearchTicket {
            schema: "wh".into(),
            name: "dimdocs".into(),
            index_name: "by_flat".into(),
            query: vec![1.0, 0.0],
            k: 1,
            nprobe: None,
            ef_search: None,
        })
        .await
        .expect_err("wrong-dim query must be rejected");
    assert!(
        matches!(&err, engine_wire::flight::VectorSearchError::DimMismatch(_)),
        "expected DimMismatch off the invalid_argument status, got: {err:?}"
    );
}
```

- [x] **Step 3: Wire the new BUCK target**

Add to `src/services/engine/BUCK`, after the `vector-search-flight` target:

```python
loom_fixture_test(
    name = "ticket-errors",
    crate = "ticket_errors",
    srcs = ["tests/ticket_errors.rs"],
    crate_root = "tests/ticket_errors.rs",
    deps = [
        ":engine",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow-flight",
        "//third-party:hyper-util",
        "//third-party:iceberg",
        "//third-party:prost",
        "//third-party:tempfile",
        "//third-party:tokio",
        "//third-party:tokio-stream",
        "//third-party:tonic",
        "//third-party:tower",
    ],
)
```

- [x] **Step 4: Run — the pins are green against the UNMODIFIED code**

Run: `buck2 test -j 8 //src/services/engine:ticket-errors //src/services/engine:vector-search-flight > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS. A failure here means a mis-written pin — fix the test.

- [x] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p1.log 2>&1; grep -c Failed /tmp/p1.log` — expected `0`.

```bash
git add src/services/engine/tests/ticket_errors.rs src/services/engine/tests/vector_search_flight.rs src/services/engine/BUCK
git commit -m "test(engine): pin ticket-decode error contract and kNN dim-mismatch wire status

The wire survey found three do_get behaviors unpinned ahead of the dispatch
refactor: the terminal bad-ticket status/message, the matched-flight-sql
no-fall-through (non-utf8 handle), the wrong-Any-type fall-through, and the
DimMismatch -> invalid_argument kNN mapping (previously pinned only through
the in-process engine). Pins added green against the current code.

Part of road-engine-wire-dedup."
```

---

### Task 2: engine-wire — `EngineTicket` decode enum + `TicketError`

**Files:**
- Modify: `src/services/engine-wire/src/flight.rs`
- Create: `src/services/engine-wire/tests/engine_ticket.rs`
- Modify: `src/services/engine-wire/BUCK` (lib gains
  `//third-party:arrow-schema`; new `engine-ticket` test target)

**Interfaces:**
- Produces (`pub` in `engine_wire::flight`):

```rust
pub enum EngineTicket { Sql(String), GovernedSql(GovernedStatementQuery),
                        VectorSearch(VectorSearchTicket), Files(FlightTicket) }
pub enum TicketError { FlightSqlUnpack(ArrowError), FlightSqlEmpty,
                       NonUtf8Sql(FromUtf8Error), BadFileTicket(serde_json::Error) }
impl EngineTicket { pub fn decode(bytes: &[u8]) -> Result<Self, TicketError>; }
impl From<TicketError> for tonic::Status;
```

  Task 3's `do_get` consumes all of it.

- [x] **Step 1: Write the failing tests**

Create `src/services/engine-wire/tests/engine_ticket.rs`:

```rust
//! Unit pins for `EngineTicket::decode` — the four-plane ticket dispatch that
//! previously lived inline in the engine's `do_get`. Pins the decode ORDER
//! (protobuf `Any` first, then governed/kNN/file JSON fall-through), the
//! disjointness the ticket types' `deny_unknown_fields` guarantees, and the
//! exact error statuses/messages the engine's Status conversion emits.
//! Pure logic — no running engine, runs on RE.

use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, TicketStatementQuery};
use control_plane_core::GovernedCatalog;
use engine_wire::flight::{
    EngineTicket, FlightTicket, GovernedStatementQuery, TicketError, VectorSearchTicket,
};
use prost::Message;

fn tsq_bytes(handle: Vec<u8>) -> Vec<u8> {
    TicketStatementQuery {
        statement_handle: handle.into(),
    }
    .as_any()
    .encode_to_vec()
}

#[test]
fn flight_sql_ticket_decodes_to_sql() {
    let t = EngineTicket::decode(&tsq_bytes(b"SELECT 1".to_vec())).expect("decode");
    assert!(matches!(t, EngineTicket::Sql(s) if s == "SELECT 1"));
}

#[test]
fn governed_ticket_decodes_to_governed() {
    let q = GovernedStatementQuery {
        sql: "SELECT 1".into(),
        catalog: GovernedCatalog { tables: vec![] },
    };
    let t = EngineTicket::decode(&q.encode()).expect("decode");
    assert!(matches!(t, EngineTicket::GovernedSql(g) if g == q));
}

#[test]
fn vector_ticket_decodes_to_vector_search() {
    let v = VectorSearchTicket {
        schema: "wh".into(),
        name: "docs".into(),
        index_name: "by_flat".into(),
        query: vec![1.0, 0.0],
        k: 2,
        nprobe: None,
        ef_search: None,
    };
    let t = EngineTicket::decode(&v.encode()).expect("decode");
    assert!(matches!(t, EngineTicket::VectorSearch(got) if got == v));
}

#[test]
fn file_ticket_decodes_to_files() {
    let f = FlightTicket {
        schema: "wh".into(),
        name: "orders".into(),
        files: vec!["data/loom-abc.parquet".into()],
    };
    let t = EngineTicket::decode(&f.encode()).expect("decode");
    assert!(matches!(t, EngineTicket::Files(got) if got == f));
}

// --- error contract (must match the pre-refactor do_get statuses verbatim) ---

#[test]
fn garbage_is_the_terminal_file_ticket_error() {
    let err = EngineTicket::decode(b"not json").expect_err("garbage must fail");
    assert!(matches!(&err, TicketError::BadFileTicket(_)), "got: {err:?}");
    assert!(err.to_string().starts_with("bad flight ticket: "), "got: {err}");
    let s = tonic::Status::from(err);
    assert_eq!(s.code(), tonic::Code::InvalidArgument);
    assert!(s.message().starts_with("bad flight ticket: "));
}

#[test]
fn non_utf8_flight_sql_handle_fails_in_place() {
    // A MATCHED TicketStatementQuery must not fall through to the JSON stages.
    let err =
        EngineTicket::decode(&tsq_bytes(vec![0xff, 0xfe, 0xfd])).expect_err("non-utf8 must fail");
    assert!(matches!(&err, TicketError::NonUtf8Sql(_)), "got: {err:?}");
    assert!(err.to_string().starts_with("non-utf8 sql: "), "got: {err}");
    let s = tonic::Status::from(err);
    assert_eq!(s.code(), tonic::Code::InvalidArgument);
    assert!(s.message().starts_with("non-utf8 sql: "));
}

#[test]
fn wrong_any_type_falls_through_to_the_file_plane() {
    // A valid protobuf Any of a DIFFERENT type is not a flight-sql ticket; it
    // must fall through the JSON stages and fail as a file ticket.
    let cmd = CommandStatementQuery {
        query: "SELECT 1".into(),
        transaction_id: None,
    };
    let err = EngineTicket::decode(&cmd.as_any().encode_to_vec())
        .expect_err("wrong Any type must fall through and fail");
    assert!(matches!(&err, TicketError::BadFileTicket(_)), "got: {err:?}");
}

#[test]
fn unpack_none_maps_to_internal() {
    // FlightSqlEmpty is an arrow-flight invariant violation (is::<T>() matched but
    // unpack returned None) — the server's fault: internal, not invalid_argument.
    let s = tonic::Status::from(TicketError::FlightSqlEmpty);
    assert_eq!(s.code(), tonic::Code::Internal);
    assert_eq!(s.message(), "flight-sql ticket unpack returned None");
}

#[test]
fn json_planes_stay_disjoint() {
    // deny_unknown_fields keeps the three JSON ticket shapes mutually exclusive —
    // the property the fall-through decode order depends on.
    let f = FlightTicket {
        schema: "s".into(),
        name: "t".into(),
        files: vec![],
    };
    assert!(matches!(
        EngineTicket::decode(&f.encode()),
        Ok(EngineTicket::Files(_))
    ));
    let v = VectorSearchTicket {
        schema: "s".into(),
        name: "t".into(),
        index_name: "i".into(),
        query: vec![],
        k: 1,
        nprobe: None,
        ef_search: None,
    };
    assert!(matches!(
        EngineTicket::decode(&v.encode()),
        Ok(EngineTicket::VectorSearch(_))
    ));
}
```

Add to `src/services/engine-wire/BUCK` after the `governed-ticket` target:

```python
rust_test(
    name = "engine-ticket",
    crate = "engine_ticket",
    srcs = ["tests/engine_ticket.rs"],
    crate_root = "tests/engine_ticket.rs",
    edition = "2024",
    deps = [
        ":engine-wire",
        "//src/control-plane/core:core",
        "//third-party:arrow-flight",
        "//third-party:prost",
        "//third-party:tonic",
    ],
)
```

and add `"//third-party:arrow-schema",` to the `engine-wire` library's `deps`
(BUCK-only, the crate's existing convention for `thiserror`/`futures`).

- [x] **Step 2: Run to see them fail**

Run: `buck2 test //src/services/engine-wire:engine-ticket > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t2.log`
Expected: FAIL — compile error, `EngineTicket`/`TicketError` not found.

- [x] **Step 3: Implement in `engine-wire/src/flight.rs`**

Extend the sql import (line 10) to
`use arrow_flight::sql::{Any, CommandStatementQuery, ProstMessageExt, TicketStatementQuery};`
and add `use arrow_schema::ArrowError;`. Then add, below
`GovernedStatementQuery`'s impl block:

```rust
/// A decoded engine `do_get` ticket — one variant per serving plane. The decode
/// ORDER is load-bearing and lives here, next to the ticket types whose
/// `deny_unknown_fields` disjointness it depends on: the protobuf Flight SQL
/// ticket is tried first (a legacy JSON ticket always starts with `{`, an invalid
/// protobuf `Any`, so the file path is never misrouted), then the three JSON
/// shapes fall through in order, the file ticket terminal.
#[derive(Debug, Clone, PartialEq)]
pub enum EngineTicket {
    /// Flight SQL read plane: the SQL carried in a `TicketStatementQuery` handle.
    Sql(String),
    /// Governed-SQL plane: arbitrary client SQL + a caller-resolved governed catalog.
    GovernedSql(GovernedStatementQuery),
    /// k-NN vector-search plane.
    VectorSearch(VectorSearchTicket),
    /// File-ticket data plane: an explicit live-file set to stream.
    Files(FlightTicket),
}

/// Why a `do_get` ticket failed to decode. Each variant's `Display` is the exact
/// wire message the engine emitted before this enum existed (pinned by
/// `engine/tests/ticket_errors.rs`); the `From<TicketError> for tonic::Status`
/// below fixes the code split.
#[derive(Debug, thiserror::Error)]
pub enum TicketError {
    /// A protobuf `Any` matched `TicketStatementQuery` but failed to unpack.
    /// No fall-through: a matched Flight SQL ticket fails in place.
    #[error("bad flight-sql ticket: {0}")]
    FlightSqlUnpack(#[source] ArrowError),
    /// `Any::unpack` returned `None` after `is::<TicketStatementQuery>()` matched —
    /// an arrow-flight invariant violation, the server's fault, never the client's.
    #[error("flight-sql ticket unpack returned None")]
    FlightSqlEmpty,
    /// The statement handle of a matched Flight SQL ticket is not UTF-8.
    #[error("non-utf8 sql: {0}")]
    NonUtf8Sql(#[source] std::string::FromUtf8Error),
    /// The terminal failure of the fall-through chain: a ticket that is neither
    /// Flight SQL, governed, nor kNN must be a file ticket.
    #[error("bad flight ticket: {0}")]
    BadFileTicket(#[source] serde_json::Error),
}

impl From<TicketError> for tonic::Status {
    fn from(e: TicketError) -> Self {
        match &e {
            // Server-side invariant violation, not a client fault.
            TicketError::FlightSqlEmpty => tonic::Status::internal(e.to_string()),
            TicketError::FlightSqlUnpack(_)
            | TicketError::NonUtf8Sql(_)
            | TicketError::BadFileTicket(_) => tonic::Status::invalid_argument(e.to_string()),
        }
    }
}

impl EngineTicket {
    /// Decode a `Ticket.ticket` payload into its serving plane.
    ///
    /// Flight SQL read path first: a `TicketStatementQuery` (Any-wrapped) carrying
    /// the SQL. Try the protobuf decode first; a legacy JSON ticket always starts
    /// with `{` (an invalid protobuf `Any`), so this never misroutes the file path.
    /// (The decode-then-`is::<>()` ordering is load-bearing.) The JSON planes then
    /// fall through in order — `deny_unknown_fields` on all three JSON shapes makes
    /// each stage unambiguous — with the file ticket terminal.
    pub fn decode(bytes: &[u8]) -> std::result::Result<Self, TicketError> {
        if let Ok(any) = Any::decode(bytes)
            && any.is::<TicketStatementQuery>()
        {
            let tsq = any
                .unpack::<TicketStatementQuery>()
                .map_err(TicketError::FlightSqlUnpack)?
                .ok_or(TicketError::FlightSqlEmpty)?;
            let sql = String::from_utf8(tsq.statement_handle.to_vec())
                .map_err(TicketError::NonUtf8Sql)?;
            return Ok(Self::Sql(sql));
        }
        // loom-native governed SQL ticket (JSON): disjoint fields
        // (deny_unknown_fields) from the other JSON tickets.
        if let Ok(gq) = GovernedStatementQuery::decode(bytes) {
            return Ok(Self::GovernedSql(gq));
        }
        // loom-native k-NN ticket (JSON). Disjoint fields from FlightTicket
        // (deny_unknown_fields on both) make this unambiguous.
        if let Ok(vs) = VectorSearchTicket::decode(bytes) {
            return Ok(Self::VectorSearch(vs));
        }
        // File-ticket data plane: a JSON `FlightTicket` naming data files.
        FlightTicket::decode(bytes)
            .map(Self::Files)
            .map_err(TicketError::BadFileTicket)
    }
}
```

(Note `std::result::Result` — the module aliases `Result` to
`control_plane_core::Result`, matching the existing `decode` methods' style.)

- [x] **Step 4: Run to green**

Run: `buck2 test //src/services/engine-wire/... > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS (new unit target + the pre-existing ticket/convert/status tests).
Run: `buck2 build '//src/services/engine-wire:engine-wire[clippy.txt]' > /tmp/c2.log 2>&1; cat /tmp/c2.log` — artifact empty.

- [x] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p2.log 2>&1; grep -c Failed /tmp/p2.log` — expected `0`.

```bash
git add src/services/engine-wire/src/flight.rs src/services/engine-wire/tests/engine_ticket.rs src/services/engine-wire/BUCK
git commit -m "feat(engine-wire): EngineTicket decode enum + TicketError

The four-plane ticket dispatch moves next to the ticket types whose
deny_unknown_fields disjointness it depends on, carrying the load-bearing
decode-order comments verbatim. TicketError reproduces the engine's existing
wire messages byte-for-byte via Display; From<TicketError> for Status keeps
the code split (FlightSqlEmpty -> internal, rest -> invalid_argument).
Unit-tested without a running engine (decode order, fall-through, error
contract). The engine's do_get adopts it next.

Part of road-engine-wire-dedup."
```

---

### Task 3: engine — flat-match `do_get` + `encode_response`, delete `FlightTicketReq`

Pure substitution: dispatch and error construction now come from Task 2's
enum (pinned byte-identical by Task 1 + the plane e2es); the four encode
tails collapse into one inherent helper.

**Files:**
- Modify: `src/services/engine/src/flight.rs`

**Interfaces:**
- Consumes: `engine_wire::flight::{EngineTicket, FlightTicket}` (Task 2).
- Produces: private `FlightDataService::{encode_response, do_get_files}`;
  `FlightTicketReq` deleted. No public-surface change.

- [x] **Step 1: Run the pinning suite BEFORE (baseline green)**

Run: `buck2 test -j 8 //src/services/engine/... > /tmp/t3a.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3a.log`
Expected: PASS.

- [x] **Step 2: Refactor `engine/src/flight.rs`**

Imports: add `use arrow_array::RecordBatch;` and
`use engine_wire::flight::EngineTicket;` (keep the `arrow_flight::sql`
imports — `get_flight_info` still builds the `TicketStatementQuery` ticket;
keep `prost::Message` for its `Any::decode`).

Add the encode helper and the file-plane method to
`impl FlightDataService`:

```rust
    /// Flight-encode a `RecordBatch` stream (schema message first, then batches)
    /// and box it as the `do_get` response. Encoder/stream errors map to
    /// `Status::internal`, matching the old unary handler's mapping so query-api's
    /// HTTP error codes are unchanged. The shared tail of all four serving planes.
    fn encode_response(
        batches: impl futures::Stream<Item = Result<RecordBatch, FlightError>> + Send + 'static,
    ) -> Response<<Self as FlightService>::DoGetStream> {
        let out = FlightDataEncoderBuilder::new()
            .build(batches)
            .map_err(|e| Status::internal(e.to_string()));
        Response::new(Box::pin(out))
    }
```

Rewrite the three plane methods onto it (bodies otherwise verbatim):

```rust
    async fn do_get_sql(
        &self,
        sql: String,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let stream = engine_serving::execute_query_stream(
            &self.serving_catalog,
            &sql,
            self.serving_store.as_ref(),
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Self::encode_response(
            stream.map_err(|e| FlightError::from_external_error(Box::new(e))),
        ))
    }
```

(`do_get_governed_sql` identically; `do_get_vector_search` keeps its
hand-rolled error match — Task 6 replaces it — and ends with
`Ok(Self::encode_response(futures::stream::iter(std::iter::once(Ok::<_, FlightError>(batch)))))`.)

Move the file-plane body (the membership guard + read, `do_get:164-218`,
comments verbatim) into:

```rust
    /// File-ticket data plane: stream an explicit live-file set. Every
    /// ticket-named path must belong to the table's live snapshot (see the
    /// defense-in-depth comment inline).
    async fn do_get_files(
        &self,
        req: engine_wire::flight::FlightTicket,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let table = TableRef {
            schema: req.schema,
            name: req.name,
        };
        // … (the existing membership-guard + read_files_as_batches body, verbatim,
        //    with `req.files` in place of the old FlightTicketReq fields) …
        Ok(Self::encode_response(futures::stream::iter(
            batches.into_iter().map(Ok::<_, FlightError>),
        )))
    }
```

`do_get` becomes the flat match (`From<TicketError> for Status` rides `?`):

```rust
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        // Decode order + fall-through invariants live with the ticket types:
        // engine_wire::flight::EngineTicket (and its unit pins).
        match EngineTicket::decode(&ticket.ticket)? {
            EngineTicket::Sql(sql) => self.do_get_sql(sql).await,
            EngineTicket::GovernedSql(q) => self.do_get_governed_sql(q).await,
            EngineTicket::VectorSearch(vs) => self.do_get_vector_search(vs).await,
            EngineTicket::Files(ft) => self.do_get_files(ft).await,
        }
    }
```

Delete `FlightTicketReq` (`:311-328`) and the now-unused
`engine_wire::flight::FlightTicket` decode path in this file.

- [x] **Step 3: Run to green (dispatch + all four planes pinned)**

Run: `buck2 test -j 8 //src/services/engine/... > /tmp/t3b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3b.log`
Expected: PASS — Task 1's `ticket-errors` pins the decode-error contract;
`flight-sql`/`governed-flight`/`vector-search-flight`/`wire`/`compact-wire`/
`write-wire`/`flight-ticket-membership` pin the four planes.
Run: `buck2 build '//src/services/engine:engine[clippy.txt]' > /tmp/c3.log 2>&1; cat /tmp/c3.log` — artifact empty.

- [x] **Step 4: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p3.log 2>&1; grep -c Failed /tmp/p3.log` — expected `0`.

```bash
git add src/services/engine/src/flight.rs
git commit -m "refactor(engine): do_get flat match over EngineTicket + shared encode_response

Dispatch and decode-error construction move to engine-wire's EngineTicket
(Task 1's pins prove the statuses/messages byte-identical); the 4x-copied
Flight encode tail collapses into one encode_response; FlightTicketReq (a
1:1 field copy of FlightTicket) is deleted; the file plane's membership
guard moves verbatim into do_get_files.

Part of road-engine-wire-dedup."
```

---

### Task 4: engine-wire client — `decode_batches` + `execute` delegates to `execute_stream`

Pure substitution, pinned by the streaming e2es. The stream items of
`execute_stream` are already `be`-mapped, so `try_collect` over it yields the
same error class/messages `execute` produced.

**Files:**
- Modify: `src/services/engine-wire/src/flight.rs`

- [ ] **Step 1: Run the pinning suite BEFORE (baseline green)**

Run: `buck2 test -j 8 //src/services/engine:flight-sql //src/services/query-api:engine-wire-serving-e2e //src/services/query-api:governed-flight-export-e2e //src/services/worker:flight-roundtrip > /tmp/t4a.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4a.log`
Expected: PASS.

- [ ] **Step 2: Refactor**

Extend the root import to
`use arrow_flight::{FlightData, FlightDescriptor, Ticket};` and add the
module-level helper below the ticket types:

```rust
/// Decode a `do_get` response's schema-first `FlightData` stream into
/// `RecordBatch`es, mapping inbound `tonic::Status` items to
/// `FlightError::Tonic` (via `From`). The shared decode step of every
/// `do_get` consumer in this module; error mapping onto the caller's domain
/// stays at each call site.
fn decode_batches(
    resp: tonic::Response<tonic::Streaming<FlightData>>,
) -> FlightRecordBatchStream {
    FlightRecordBatchStream::new_from_flight_data(
        resp.into_inner()
            .map_err(arrow_flight::error::FlightError::from),
    )
}
```

Rewrite the four sites:

- `FlightTableClient::fetch` tail:

```rust
        let batches: Vec<RecordBatch> = decode_batches(resp)
            .try_collect()
            .await
            .map_err(crate::client::be)?;
        Ok(batches)
```

- `FlightTableClient::vector_search` tail:

```rust
        decode_batches(resp)
            .try_collect()
            .await
            .map_err(|e| VectorSearchError::Engine(e.to_string()))
```

- `FlightSqlClient::execute` becomes the delegation (whole body):

```rust
    /// Execute already-compiled, param-inlined `sql` and collect the streamed result.
    /// Each `RecordBatch` arrives as its own Flight message, so a wide/large result
    /// never serialises into a single oversized gRPC message (the unary path's cap).
    /// Buffering wrapper over [`execute_stream`](Self::execute_stream) — the stream's
    /// items are already mapped to control-plane errors, so collecting preserves the
    /// error class/messages of the old hand-rolled body.
    pub async fn execute(&self, sql: String) -> Result<Vec<RecordBatch>> {
        self.execute_stream(sql).await?.try_collect().await
    }
```

- `FlightSqlClient::execute_stream` tail:

```rust
        // Decode the schema-first FlightData stream into RecordBatches, mapping the
        // stream's FlightError items to control-plane errors (same `be` mapping the
        // buffered path uses). The stream owns the (cloned) response, so it is 'static.
        Ok(Box::pin(decode_batches(resp).map_err(crate::client::be)))
```

- [ ] **Step 3: Run to green**

Run: `buck2 test -j 8 //src/services/engine-wire/... //src/services/engine:flight-sql //src/services/query-api:engine-wire-serving-e2e //src/services/query-api:governed-flight-export-e2e //src/services/worker:flight-roundtrip > /tmp/t4b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4b.log`
Expected: PASS — `flight_sql` pins `execute` (roundtrip + malformed-SQL
`is_err`), `engine_wire_serving_e2e` pins the large-result streaming path,
the export e2e pins `execute_stream`, the worker roundtrip pins `fetch`.
Run: `buck2 build '//src/services/engine-wire:engine-wire[clippy.txt]' > /tmp/c4.log 2>&1; cat /tmp/c4.log` — artifact empty.

- [ ] **Step 4: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p4.log 2>&1; grep -c Failed /tmp/p4.log` — expected `0`.

```bash
git add src/services/engine-wire/src/flight.rs
git commit -m "refactor(engine-wire): shared decode_batches; execute delegates to execute_stream

Collapses the census pair (execute's hand-rolled CommandStatementQuery dance
was a copy of execute_stream's) and the 4x-copied FlightRecordBatchStream
decode block. Error classes/messages are unchanged: the stream's items were
already be-mapped, so try_collect over execute_stream reproduces execute's
errors byte-identically.

Part of road-engine-wire-dedup."
```

---

### Task 5: engine-wire client — `gov_rpc!` macro for the 10 governance getters

Pure substitution: each generated body is the exact shape the hand-written
methods share; signatures, docs, request fields, and error mapping are
identical, so the wire is untouched. Pinned by the query-api wire e2es
(`wire_governance_e2e`, `wire_governed_read_e2e`, `wire_lineage_e2e` — they
drive `WireControlPlane`, which calls every getter) and the `cp_status`
unit tests.

**Files:**
- Modify: `src/services/engine-wire/src/client.rs`

- [ ] **Step 1: Run the pinning suite BEFORE (baseline green)**

Run: `buck2 test -j 8 //src/services/query-api:wire-governance-e2e //src/services/query-api:wire-governed-read-e2e //src/services/query-api:wire-lineage-e2e //src/services/engine:wire > /tmp/t5a.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5a.log`
Expected: PASS.

- [ ] **Step 2: Add the macro and regenerate the 10 getters**

Below `se` (client.rs:40-42), add:

```rust
/// Expand a governance-read RPC method. Every governance getter shares one body
/// shape — build the pb request, call the RPC, map `Status` via [`cp_status`]
/// (class-preserving), JSON-decode the named response field via `de` — so the
/// macro takes the method name + args, the RPC and response-field names, and the
/// request expression, and generates exactly that body. Data-plane RPCs
/// (flush/gc/write/…) stay hand-written: different error mapping (`be`) and
/// typed non-JSON responses.
///
/// Matcher shape note: the argument list is captured as `$($args:tt)*` and the
/// request as one trailing `$req:expr` because the naive per-field fragments
/// (`$ty:ty` before `)`, `$v:expr` before `}`) violate `macro_rules!`
/// follow-set rules.
macro_rules! gov_rpc {
    (
        $(#[$meta:meta])*
        fn $name:ident($($args:tt)*) -> $ret:ty;
        rpc $rpc:ident, out $out:ident, req $req:expr
    ) => {
        $(#[$meta])*
        pub async fn $name(&self, $($args)*) -> Result<$ret> {
            let resp = self
                .inner
                .clone()
                .$rpc($req)
                .await
                .map_err(cp_status)?
                .into_inner();
            de(&resp.$out)
        }
    };
}
```

Then, inside `impl GrpcQueueClient`, replace the 10 hand-written getters
(`client.rs:213-378`) with invocations — docs verbatim, one per method:

```rust
    gov_rpc! {
        /// Governance: check whether `subject` may perform `action` on `target`.
        fn gov_check(subject: &SubjectId, action: Action, target: &PolicyTarget) -> Decision;
        rpc check, out decision_json, req pb::CheckRequest {
            subject_json: se(subject)?,
            action_json: se(&action)?,
            target_json: se(target)?,
        }
    }

    gov_rpc! {
        /// Governance: list policies granting `subject` `action` on `target`.
        fn gov_policies_for(subject: &SubjectId, action: Action, target: &PolicyTarget, page: &PageReq) -> Page<Policy>;
        rpc policies_for, out page_json, req pb::PoliciesForRequest {
            subject_json: se(subject)?,
            action_json: se(&action)?,
            target_json: se(target)?,
            page_json: se(page)?,
        }
    }

    gov_rpc! {
        /// Governance: fetch the ontology definition of a named object type.
        fn gov_get_type(name: &TypeName) -> ObjectType;
        rpc get_type, out object_type_json, req pb::GetTypeRequest {
            type_name: name.0.clone(),
        }
    }

    gov_rpc! {
        /// Governance: resolve a type name to its underlying catalog `TableRef`.
        fn gov_resolve(name: &TypeName) -> TableRef;
        rpc resolve, out table_ref_json, req pb::ResolveRequest {
            type_name: name.0.clone(),
        }
    }

    gov_rpc! {
        /// Governance: list links declared on a named object type.
        fn gov_links(name: &TypeName, page: &PageReq) -> Page<LinkDef>;
        rpc links, out page_json, req pb::LinksRequest {
            type_name: name.0.clone(),
            page_json: se(page)?,
        }
    }

    gov_rpc! {
        /// Governance: list links that target a named object type.
        fn gov_links_to(name: &TypeName, page: &PageReq) -> Page<LinkDef>;
        rpc links_to, out page_json, req pb::LinksToRequest {
            type_name: name.0.clone(),
            page_json: se(page)?,
        }
    }

    gov_rpc! {
        /// Governance: list all defined object types.
        fn gov_list_types(page: &PageReq) -> Page<ObjectType>;
        rpc list_types, out page_json, req pb::ListTypesRequest {
            page_json: se(page)?,
        }
    }

    gov_rpc! {
        /// Governance: fetch the definition of a named action.
        fn gov_get_action(name: &ActionName) -> ActionDef;
        rpc get_action, out action_def_json, req pb::GetActionRequest {
            action_name: name.0.clone(),
        }
    }

    gov_rpc! {
        /// Governance: list all vector index definitions for a named object type.
        fn gov_vector_indexes_for(type_name: &TypeName) -> Vec<VectorIndexDef>;
        rpc vector_indexes_for, out indexes_json, req pb::VectorIndexesForRequest {
            type_name: type_name.0.clone(),
        }
    }

    gov_rpc! {
        /// Governance: fetch a specific named vector index definition for a type.
        fn gov_get_vector_index(type_name: &TypeName, name: &str) -> Option<VectorIndexDef>;
        rpc get_vector_index, out index_json, req pb::GetVectorIndexRequest {
            type_name: type_name.0.clone(),
            name: name.to_string(),
        }
    }
```

Also add the spec's warning comment on `be` (`client.rs:15`):

```rust
/// Flatten ANY error to an opaque `ControlPlaneError::Backend` string.
/// WARNING: class-erasing — after road-engine-wire-dedup this is legitimate
/// only for transport faults and planes with no error classes; class-carrying
/// planes must use [`cp_status`] (governance) / `sql_status` (Flight SQL).
pub(crate) fn be<E: std::fmt::Display>(e: E) -> ControlPlaneError {
```

- [ ] **Step 3: Run to green**

Run: `buck2 test -j 8 //src/services/engine-wire/... //src/services/query-api:wire-governance-e2e //src/services/query-api:wire-governed-read-e2e //src/services/query-api:wire-lineage-e2e //src/services/engine:wire > /tmp/t5b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5b.log`
Expected: PASS.
Run: `buck2 build '//src/services/engine-wire:engine-wire[clippy.txt]' > /tmp/c5.log 2>&1; cat /tmp/c5.log` — artifact empty.

- [ ] **Step 4: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p5.log 2>&1; grep -c Failed /tmp/p5.log` — expected `0`.

```bash
git add src/services/engine-wire/src/client.rs
git commit -m "refactor(engine-wire): gov_rpc! macro for the 10 governance getters

The register's count was drift (10 methods, ~165 lines, not ~20/200); all 10
fit the macro because the pb request is a single expr argument, so the
multi-field getters need no hand-written exception (the matcher captures args
as tt and the request as expr — the naive per-field fragments violate
macro_rules follow-set rules). Method names, args, docs,
request fields, and the cp_status error mapping are byte-identical; pinned by
the query-api wire-governance e2e family. be() gains the class-erasure
warning comment.

Part of road-engine-wire-dedup."
```

---

### Task 6: WHITELISTED CHANGES 1+2 — `Plan` error class end-to-end on the wire

`EngineServingError::Plan(DataFusionError)` at the three `ctx.sql` sites; one
total `serving_status` in the engine (replacing three ad-hoc mappings);
`sql_status` on the client. TDD: the flight-level pin lands RED first
(current code returns `internal`/`Backend`).

**Files:**
- Modify: `src/services/engine-serving/src/serving.rs`,
  `src/services/engine-serving/src/governed.rs`
- Modify: `src/services/engine/src/flight.rs`
- Modify: `src/services/engine-wire/src/client.rs`,
  `src/services/engine-wire/src/flight.rs`
- Create: `src/services/engine/tests/serving_status.rs`
- Test (append): `src/services/engine/tests/flight_sql.rs`,
  `src/services/engine-wire/tests/cp_status.rs` (existing targets)
- Modify: `src/services/engine/BUCK` (new pure `serving-status` target)

**Interfaces:**
- Produces: `EngineServingError::Plan(#[source] DataFusionError)`;
  `pub fn engine::flight::serving_status(EngineServingError) -> Status`;
  `pub fn engine_wire::client::sql_status(Status) -> ControlPlaneError`.
  Task 7 consumes the `Validation` class in query-api.

- [ ] **Step 1: Write the failing tests**

Create `src/services/engine/tests/serving_status.rs` (pure — runs on RE):

```rust
//! Unit pins for `serving_status` — the one total `EngineServingError` -> gRPC
//! `Status` mapping on the engine's Flight data plane. NoIndex/DimMismatch carry
//! the INNER message only (no enum prefix — the wire contract the clients'
//! inverse mappings were built against); Plan/Engine carry their full Display.

use datafusion::error::DataFusionError;
use engine::flight::serving_status;
use engine_serving::EngineServingError;

#[test]
fn no_index_is_not_found_with_inner_message() {
    let s = serving_status(EngineServingError::NoIndex("no index `by_flat`".into()));
    assert_eq!(s.code(), tonic::Code::NotFound);
    assert_eq!(s.message(), "no index `by_flat`");
}

#[test]
fn dim_mismatch_is_invalid_argument_with_inner_message() {
    let s = serving_status(EngineServingError::DimMismatch("query dim 2 != 4".into()));
    assert_eq!(s.code(), tonic::Code::InvalidArgument);
    assert_eq!(s.message(), "query dim 2 != 4");
}

#[test]
fn plan_is_invalid_argument_with_full_display() {
    let s = serving_status(EngineServingError::Plan(DataFusionError::Plan(
        "no table".into(),
    )));
    assert_eq!(s.code(), tonic::Code::InvalidArgument);
    assert!(
        s.message().starts_with("query planning failed: "),
        "got: {}",
        s.message()
    );
}

#[test]
fn engine_is_internal_with_full_display() {
    let s = serving_status(EngineServingError::Engine("boom".into()));
    assert_eq!(s.code(), tonic::Code::Internal);
    assert_eq!(s.message(), "engine serving: boom");
}
```

BUCK target (after `flight-membership-helper`):

```python
rust_test(
    name = "serving-status",
    crate = "serving_status",
    srcs = ["tests/serving_status.rs"],
    crate_root = "tests/serving_status.rs",
    deps = [
        ":engine",
        "//src/services/engine-serving:engine-serving",
        "//third-party:datafusion",
        "//third-party:tonic",
    ],
)
```

Append to `src/services/engine-wire/tests/cp_status.rs` (extend the import to
`use engine_wire::client::{cp_status, sql_status};`):

```rust
// --- sql_status: the Flight SQL plane's inverse mapping ----------------------

#[test]
fn sql_invalid_argument_maps_to_validation() {
    // WHITELISTED (road-engine-wire-dedup): the engine classifies only ctx.sql()
    // planning faults as invalid_argument on this plane; the client must carry
    // the class as Validation so query-api can return 400 instead of 500.
    let e = sql_status(Status::invalid_argument("query planning failed: no table"));
    assert!(
        matches!(e, ControlPlaneError::Validation(m) if m == "query planning failed: no table")
    );
}

#[test]
fn sql_other_codes_stay_backend() {
    let e = sql_status(Status::internal("boom"));
    assert!(matches!(e, ControlPlaneError::Backend(_)));
}
```

Append the whitelist wire pin to `src/services/engine/tests/flight_sql.rs`
(extend imports with `use control_plane_core::ControlPlaneError;`):

```rust
// ---------------------------------------------------------------------------
// WHITELISTED (road-engine-wire-dedup): a statement that fails DataFusion
// PLANNING is the client's fault — invalid_argument on the wire, Validation off
// the client — no longer an opaque internal/Backend. (The pre-existing
// malformed-SQL assertion above is code-agnostic and stays green.)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_sql_is_validation_class() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let (_sock_dir, sock) = spawn_flight(fx, &db, &wh.path().display().to_string()).await;
    let client = FlightSqlClient::connect(&sock).await.expect("connect");

    let err = client
        .execute("SELECT FROM nope".to_string())
        .await
        .expect_err("malformed SQL must error");
    assert!(
        matches!(&err, ControlPlaneError::Validation(m) if m.starts_with("query planning failed: ")),
        "planning fault must carry the Validation class, got: {err:?}"
    );
}
```

- [ ] **Step 2: Run to see them fail**

Run: `buck2 test -j 8 //src/services/engine:serving-status //src/services/engine-wire:cp-status //src/services/engine:flight-sql > /tmp/t6a.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t6a.log`
Expected: FAIL — compile errors (`Plan` variant, `serving_status`,
`sql_status` not found); the flight pin would be red even after compilation
(current wire returns `internal` → `Backend`).

- [ ] **Step 3: Implement**

`engine-serving/src/serving.rs` — add the variant (below `Engine`):

```rust
    /// The SQL failed DataFusion *planning* (`ctx.sql(...)`) — a parse/logical-plan
    /// fault in the statement itself: the client's error class, never the engine's.
    /// Wire callers map this to `invalid_argument` (query-api surfaces 400);
    /// execution/stream/catalog faults stay [`Engine`](Self::Engine) (internal/500).
    /// Classified conservatively: ONLY the `ctx.sql()` call sites construct it.
    #[error("query planning failed: {0}")]
    Plan(#[source] datafusion::error::DataFusionError),
```

and change the two `ctx.sql` sites (`execute_query`, `execute_query_stream`):

```rust
    let df = ctx.sql(sql).await.map_err(EngineServingError::Plan)?;
```

`governed.rs:319` identically. Extend `to_serving`'s doc with the class
warning:

```rust
/// Any error (mirror/Postgres, DataFusion, object_store, URL) -> opaque engine-serving error.
/// WARNING: class-erasing — never use this on a `ctx.sql()` planning fault
/// (that is `EngineServingError::Plan`, the client-fault class).
```

`engine/src/flight.rs` — add the total mapping and adopt it:

```rust
/// The one total `EngineServingError` -> gRPC `Status` mapping for the engine's
/// Flight data plane. Class-preserving: `Plan` (bad SQL — the client's fault) ->
/// `invalid_argument`; `NoIndex` -> `not_found`; `DimMismatch` ->
/// `invalid_argument`; `Engine` (execution/backend) -> `internal`. The
/// NoIndex/DimMismatch arms carry the INNER message only (no enum prefix),
/// preserving the wire messages the clients' inverse mappings decode.
pub fn serving_status(e: engine_serving::EngineServingError) -> Status {
    use engine_serving::EngineServingError as E;
    match e {
        E::NoIndex(m) => Status::not_found(m),
        E::DimMismatch(m) => Status::invalid_argument(m),
        e @ E::Plan(_) => Status::invalid_argument(e.to_string()),
        e @ E::Engine(_) => Status::internal(e.to_string()),
    }
}
```

In `do_get_sql` and `do_get_governed_sql`, replace
`.map_err(|e| Status::internal(e.to_string()))?` with
`.map_err(serving_status)?` (and update their doc comments: "planning faults
map to `invalid_argument`, execution faults to `internal`"). In
`do_get_vector_search`, replace the hand-rolled three-arm match with
`.map_err(serving_status)` (byte-identical for its reachable variants —
pinned by the `NoIndex` e2e and Task 1's `DimMismatch` pin).

`engine-wire/src/client.rs` — add below `cp_status`:

```rust
/// Map a tonic [`tonic::Status`] from the engine's Flight **SQL** plane back to a
/// [`ControlPlaneError`], inverting the engine-side `serving_status` mapping so the
/// planning-error class survives the wire: `InvalidArgument` (the engine classifies
/// only `ctx.sql()` planning faults this way on the SQL plane) -> `Validation`;
/// everything else stays an opaque `Backend`. Mirrors [`cp_status`].
#[must_use]
pub fn sql_status(s: tonic::Status) -> ControlPlaneError {
    match s.code() {
        tonic::Code::InvalidArgument => ControlPlaneError::Validation(s.message().to_string()),
        _ => be(s),
    }
}
```

`engine-wire/src/flight.rs` — in `FlightSqlClient::execute_stream` (the one
body after Task 4), change the `get_flight_info` and `do_get` call-error
mappings from `.map_err(crate::client::be)` to
`.map_err(crate::client::sql_status)`. The missing-ticket fault and the
mid-stream item mapping keep `be` (execution-class).

- [ ] **Step 4: Run to green**

Run: `buck2 test -j 8 //src/services/engine:serving-status //src/services/engine-wire/... //src/services/engine/... > /tmp/t6b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6b.log`
Expected: PASS — the new pins go green; every pre-existing engine/engine-wire
test is unaffected (`internal` untouched for execution faults; kNN statuses
byte-identical).
Run: `buck2 test -j 8 //src/services/query-api:engine-wire-serving-e2e //src/services/query-api:governed-flight-export-e2e //src/services/worker:flight-roundtrip > /tmp/t6c.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6c.log`
Expected: PASS (the query-api `is_err` malformed-SQL assertion is
code-agnostic; the export path's plan faults still surface as its own
`internal` — its mapping is untouched).
Run: `buck2 build '//src/services/engine:engine[clippy.txt]' '//src/services/engine-wire:engine-wire[clippy.txt]' '//src/services/engine-serving:engine-serving[clippy.txt]' > /tmp/c6.log 2>&1; cat /tmp/c6.log` — artifacts empty.

- [ ] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p6.log 2>&1; grep -c Failed /tmp/p6.log` — expected `0`.

```bash
git add src/services/engine-serving/src/serving.rs src/services/engine-serving/src/governed.rs src/services/engine/src/flight.rs src/services/engine/tests/serving_status.rs src/services/engine/tests/flight_sql.rs src/services/engine/BUCK src/services/engine-wire/src/client.rs src/services/engine-wire/src/flight.rs src/services/engine-wire/tests/cp_status.rs
git commit -m "feat(engine): structured Plan error class over the Flight SQL wire

EngineServingError gains Plan(DataFusionError) at exactly the three ctx.sql
sites (conservative: execution/stream/catalog faults stay Engine). One total
serving_status fn replaces the engine's three ad-hoc Status mappings (the
spec's From impl is orphan-blocked: engine-serving deliberately has no tonic
dep); the client's sql_status inverts it (InvalidArgument -> Validation,
mirroring cp_status). Whitelist items 1+2: planning faults are now
invalid_argument/Validation instead of internal/Backend — pinned red-first by
the flight_sql wire pin; NoIndex/DimMismatch statuses and messages are
byte-identical (Task 1 pins + serving-status unit pins).

Part of road-engine-wire-dedup."
```

---

### Task 7: WHITELISTED CHANGE 3 — query-api surfaces the Plan class as 400

**Files:**
- Modify: `src/services/query-api/src/serving.rs` (`ServingError`),
  `src/services/query-api/src/engine_client.rs` (`fetch_rows`),
  `src/services/query-api/src/http.rs` (`query_error_response`)
- Modify: `src/services/query-api/tests/e2e_support.rs` (in-process fidelity
  arm — the only e2e-support edit; additive match arm)
- Test (append): `src/services/query-api/tests/query_error_http.rs`,
  `src/services/query-api/tests/engine_wire_serving_e2e.rs` (existing
  targets — no BUCK change)

**Interfaces:**
- Produces: `ServingError::Plan(String)`. No signature changes; the total
  `QueryError → Response` mapping gains one arm (it has no `Serving`
  inner-variant exhaustiveness, so the arm must be explicit or the class
  silently falls to the 500 arm).

- [ ] **Step 1: Write the failing tests**

Append to `tests/query_error_http.rs`:

```rust
#[test]
fn plan_class_is_400_with_message_body() {
    // WHITELISTED (road-engine-wire-dedup): an engine PLANNING fault is the
    // statement's fault -> 400 echoing the engine's plan message (the same
    // engine-built-message echo precedent as DimMismatch). Execution faults
    // (ServingError::Engine) stay opaque 500 — pinned above.
    assert_eq!(
        status(QueryError::Serving(ServingError::Plan(
            "query planning failed: no table".into()
        ))),
        StatusCode::BAD_REQUEST
    );
}
```

Append to `tests/engine_wire_serving_e2e.rs` (extend the serving import to
`use query_api::serving::{ServingEngine, ServingError, SqlValue};`):

```rust
// ---------------------------------------------------------------------------
// WHITELISTED (road-engine-wire-dedup): over the wire, a planning-class SQL
// fault now reaches query-api as ServingError::Plan (HTTP 400), not an opaque
// Engine 500. (The code-agnostic is_err assertion above stays green.)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_sql_is_plan_class() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let wh = tempfile::tempdir().expect("warehouse");
    let (_sock_dir, sock) = spawn_flight(fx, &db, &wh.path().display().to_string()).await;
    let client = EngineServingClient::connect(&sock).await.expect("connect");

    let err = client
        .fetch_rows("SELECT FROM nope", &[])
        .await
        .expect_err("malformed SQL must error");
    assert!(
        matches!(&err, ServingError::Plan(m) if m.starts_with("query planning failed: ")),
        "planning fault must carry the Plan class, got: {err:?}"
    );
}
```

- [ ] **Step 2: Run to see them fail**

Run: `buck2 test -j 8 //src/services/query-api:query-error-http //src/services/query-api:engine-wire-serving-e2e > /tmp/t7a.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t7a.log`
Expected: FAIL — compile error, `ServingError::Plan` not found.

- [ ] **Step 3: Implement**

`serving.rs` — add to `ServingError` (below `Engine`):

```rust
    /// The engine rejected the SQL at *planning* time (the `Validation` class off
    /// the Flight SQL wire; in-process: `EngineServingError::Plan`) → 400. The
    /// message is the engine's `query planning failed: …` — pass-through Display,
    /// no re-prefixing.
    #[error("{0}")]
    Plan(String),
```

`engine_client.rs::fetch_rows` — replace the flatten with the class split:

```rust
        let batches = self.sql.execute(inlined).await.map_err(|e| match e {
            control_plane_core::ControlPlaneError::Validation(m) => ServingError::Plan(m),
            other => ServingError::Engine(other.to_string()),
        })?;
```

`http.rs::query_error_response` — add the arm directly after the
`DimMismatch` arm, and extend the fn's doc comment ("`Plan` is the
planning-fault 400 — constructed only off the engine wire/in-process planner"):

```rust
        QueryError::Serving(crate::serving::ServingError::Plan(m)) => {
            (StatusCode::BAD_REQUEST, m).into_response()
        }
```

`tests/e2e_support.rs` — in `InProcessServingEngine::fetch_rows`
(`:150`), keep wire fidelity for the in-process twin:

```rust
        .map_err(|e| match e {
            // Full Display, not the inner DataFusionError: the wire path's message
            // is the engine's `query planning failed: {df}` (serving_status uses
            // e.to_string()), and the in-process twin must match it byte-for-byte.
            e @ engine_serving::EngineServingError::Plan(_) => ServingError::Plan(e.to_string()),
            other => ServingError::Engine(other.to_string()),
        })?;
```

(The vector-search match at `:177-181` keeps its catch-all — `Plan` cannot
arise there, matching the wire client's `VectorSearchError` domain.)

- [ ] **Step 4: Run to green (the class + every read-path e2e family that rides ServingError)**

Run: `buck2 test -j 8 //src/services/query-api:query-error-http //src/services/query-api:engine-wire-serving-e2e //src/services/query-api:serving-fault-logging //src/services/query-api:vector_search_e2e //src/services/query-api:http-wire-e2e > /tmp/t7b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t7b.log`
(Target names verified against `src/services/query-api/BUCK` —
`vector_search_e2e` uses underscores.)
Expected: PASS — `internal_variants_are_opaque_500` and
`serving_fault_logging` (both `ServingError::Engine`) are unaffected; the
in-process dim-mismatch 400 e2e is unaffected.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c7.log 2>&1; cat /tmp/c7.log` — artifact empty.

- [ ] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p7.log 2>&1; grep -c Failed /tmp/p7.log` — expected `0`.

```bash
git add src/services/query-api/src/serving.rs src/services/query-api/src/engine_client.rs src/services/query-api/src/http.rs src/services/query-api/tests/e2e_support.rs src/services/query-api/tests/query_error_http.rs src/services/query-api/tests/engine_wire_serving_e2e.rs
git commit -m "feat(query-api): surface the engine Plan class as HTTP 400

Whitelist item 3 of road-engine-wire-dedup: ServingError::Plan carries the
wire's Validation class through fetch_rows into an explicit 400 arm of the
total query_error_response mapping (without it the class would silently fall
to the opaque-500 arm — the Serving inner enum has a catch-all there). Body
echoes the engine's plan message, the DimMismatch precedent. Unreachable via
today's HTTP routes in a healthy deployment; matters before
fut-external-sql-wire. Execution faults stay opaque 500 (pinned).
InProcessServingEngine gains the same arm so the in-process twin matches the
wire.

Part of road-engine-wire-dedup."
```

---

### Task 8: Affected-package sweep + dedup/complexity proof + register close

**Files:**
- Modify: `docs/ROADMAP.md` (`road-engine-wire-dedup` — locate by id)

- [ ] **Step 1: Affected-package suites + whole-tree build check**

Run: `buck2 build -M none //src/... > /tmp/b8.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)" /tmp/b8.log`
Expected: `BUILD SUCCEEDED`.
Run: `buck2 test -j 8 //src/services/engine/... //src/services/engine-wire/... //src/services/engine-serving/... //src/services/worker/... //src/services/query-api/... > /tmp/t8.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t8.log`
Expected: PASS (fixture-heavy — keep `-j 8`; if an unrelated fixture test
flakes on a boot-slot timeout, re-run that target alone before
investigating).

- [ ] **Step 2: Prove the metric drops**

Invoke the `loom-duplication` skill with args `diff` — expected: census pair
`duplication.md:92` (`engine-wire/src/flight.rs` execute ≈ execute_stream)
gone; no new production pairs (the Task 1 vector-test prologue clone is a
known census cluster owned by `road-test-wire-harness`). Invoke the
`loom-complexity` skill with args `diff` — expected: `engine/flight.rs::do_get`
drops from cc 21 (census `complexity.md:64`) to low single digits; no new fn
at cc ≥ 15. Record observed numbers for the register prose (do NOT edit the
census files — the scheduled routines refresh them).

- [ ] **Step 3: Close the register item**

In `docs/ROADMAP.md`, flip `road-engine-wire-dedup` to `[x] … status:done`
(keep `pr:-` until the PR number exists) and replace the prose:

```markdown
- [x] **Engine wire dedup + structured error classes** `{#road-engine-wire-dedup area:quality status:done from:2026-07-02-pillar-idioms-audit-design pr:- spec:2026-07-02-pillar-idioms-audit-design}`
  Done (PR #-). **Ticket dispatch:** `engine_wire::flight::EngineTicket::decode` owns the four-plane decode order beside the `deny_unknown_fields` ticket types (load-bearing comments moved verbatim; `TicketError` reproduces the old Status messages byte-for-byte, pinned by a new decode-error contract test added green FIRST — the statuses/messages and the wrong-`Any`-type fall-through previously had zero coverage); engine `do_get` (cc 21) is a flat match, the 4×-copied encode tail is one `encode_response`, `FlightTicketReq` deleted. **Client:** `execute` = `execute_stream().try_collect()` (census pair `flight.rs` closed), 4×-copied stream decode → `decode_batches`, and the governance getters collapse under `gov_rpc!` — the register's "~20 getters/~200 lines" was drift: 10 methods/~165 lines, and ALL fit the macro (the pb request is one expr argument, so no hand-written multi-field exception); data-plane RPCs + the `Queue` impl stay hand-written (different error mapping). **Errors:** `EngineServingError::Plan(DataFusionError)` at exactly the three `ctx.sql()` sites; the spec's `From<EngineServingError> for tonic::Status` was orphan-blocked (engine-serving deliberately has no tonic dep), so the single total mapping is `engine::flight::serving_status` (unit-pinned; NoIndex/DimMismatch statuses+messages byte-identical, incl. a new kNN DimMismatch wire pin); client `sql_status` inverts it. THREE whitelisted changes: planning faults are `invalid_argument` (message `query planning failed: …`) instead of `internal` on the wire; `FlightSqlClient` classes `InvalidArgument` as `ControlPlaneError::Validation`; query-api surfaces `ServingError::Plan` as 400 echoing the plan message (explicit arm in the total mapping — needed a query-api leg the spec section didn't spell out; unreachable via today's HTTP routes, matters before [[fut-external-sql-wire]]). Everything else byte-identical; every pre-existing wire/e2e test passed unmodified.
```

Run: `bash tools/docs.sh validate`
Expected: exit 0.

- [ ] **Step 4: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p8.log 2>&1; grep -c Failed /tmp/p8.log` — expected `0`.

```bash
git add docs/ROADMAP.md
git commit -m "docs(registers): close road-engine-wire-dedup

Register prose records the verified drift (10 gov getters not ~20; the From
impl orphan-blocked onto serving_status; the query-api 400 leg the spec
implied but did not spell out), the pins added ahead of the refactor
(ticket-decode error contract, kNN DimMismatch wire status), and the
three-item behavior-change whitelist. pr:- updated when the PR opens.

Part of road-engine-wire-dedup."
```

(When the branch's PR is opened, update `pr:-` to `pr:#N` in the same PR.)
