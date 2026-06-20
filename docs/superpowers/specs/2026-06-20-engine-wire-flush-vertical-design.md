# Engine-Wire Flush Vertical — Design

> Spec 2 of the flush-automation split, and the first vertical of loom's
> **engine-wire**: a new `engine` process owns Postgres and exposes a typed
> tonic (gRPC) `EngineControl` service over a unix-domain socket; a new `worker`
> process is the first **zero-pool client** — its loop drains `flush_table` jobs
> (the producer from Spec 1) and runs the flush, every DB touch a wire call.
> Control-plane only: flush moves no bulk data, so Arrow Flight is not exercised
> here. Transport decision record: `docs/spike/engine-wire-transport.md`.

## Goal

Spec 1 ships the *producer*: `inline_append` enqueues a `flush_table` job when a
table crosses `LOOM_FLUSH_BYTE_THRESHOLD`. Nothing consumes those jobs. This spec
ships the *consumer* — but deliberately as the first cut of the engine-wire, not
as a co-located worker. It proves the architecture loom is moving toward: one
process owns the `PgPool`/engine, other roles become clients over a local socket,
and a role can do real work owning **zero** Postgres knowledge. Flush is the
motivating first consumer because it needs only the control plane (no bulk data
crosses the wire).

Success criterion (the money test): `inline_append` past the threshold → the real
`Worker` (a zero-pool process) drains the job over the wire → the engine flushes
the table. Producer → queue → worker → engine → flush, end to end over a UDS.

## Background (verified)

- **`Worker<Q: Queue + Send + Sync>`** (`worker/src/lib.rs:22`) is generic over the
  `Queue` trait: `Worker::new(queue, worker_id, lease)` +
  `run(&kinds, shutdown: CancellationToken, handler: Fn(Job) -> Future<Output =
  Result<(), JobFailure>>)`. It dequeues, runs the handler, completes on `Ok` /
  `fail`s on `JobFailure`, heartbeats during the handler, and stops between jobs
  on cancellation. **It needs no changes** — a wire-backed `Queue` impl drives it
  unchanged.
- **`Queue`** (`core/src/queue.rs:56`): `enqueue`/`dequeue`/`complete`/`fail`/
  `heartbeat`/`await_jobs`. `await_jobs` is a best-effort wakeup — spurious
  returns allowed, the timeout is a polling fallback. Types: `Job { id: JobId(Uuid),
  kind, payload: serde_json::Value, attempts: i32, run_at: OffsetDateTime }`,
  `RetryPolicy::Retry { delay } | Abandon`, `JobFailure { error, policy }`.
- **`flush_table(catalog: &SqlCatalog, pool: &PgPool, table: &TableRef, run_id:
  RunId) -> Result<Option<SnapshotId>>`** (`postgres/src/iceberg_flush.rs:40`).
  `SnapshotId(i64)`. `None` = nothing live to flush.
- **`FlushJob { schema, name }` + `FLUSH_JOB_KIND = "flush_table"`** currently in
  `postgres/src/iceberg_flush.rs` (Spec 1). The job payload is
  `{"schema","name"}` JSON.
- **`service_runtime`** (`runtime/src/lib.rs`): `Config::from_env()` (LOOM_DB_*,
  LOOM_DATA_PATH, LOOM_LOCK_TIMEOUT_MS), `build_pool(&db)`, `control_plane(pool,
  lock_timeout) -> PgControlPlane`, `serve(addr, router)` (axum/TCP — the engine
  needs a tonic/UDS variant), `DbConfig::pg_connect_options()`/`pg_url()` with the
  unix-socket form (`host` starting with `/`).
- **Transport** (`engine-wire-transport.md`): tonic + `protox` (no `protoc`) for
  codegen; Arrow Flight is the *data*-plane standard, deferred to a future read
  vertical. tonic/prost/protox are pure Rust — no `links`/native crates.

## Architecture

### Crate layout — zero-pool enforced by the dependency graph

```
src/services/engine-wire/   shared wire contract — NO postgres dep
  engine_control.proto · generated tonic client+server (codegen genrule)
  proto↔core translation · GrpcQueueClient (impl Queue) + flush_table client method
  deps: tonic, prost, control-plane-core, tokio, tower/hyper-util/tokio-stream

src/services/engine/        the engine BINARY — owns PG
  EngineControlService (impl generated server trait; holds PgControlPlane + SqlCatalog + PgPool)
  main.rs (UDS tonic server)
  deps: engine-wire, control-plane-postgres, control-plane-core, service-runtime, tonic, tokio

src/services/worker/        the worker BINARY — zero-pool client
  flush handler · main.rs (Worker<GrpcQueueClient>)
  deps: engine-wire, control-plane-worker, control-plane-core, tokio
        — NO control-plane-postgres: zero-pool is structurally impossible to violate
```

### Spec 1 refactor: move the flush-job contract to `core`

So the worker can read the job contract without pulling in `sqlx`/PG, **move
`FlushJob` + `FLUSH_JOB_KIND` from `control-plane-postgres` to
`control-plane-core`** (a cross-cutting job contract belongs in `core`). The
producer (`inline_append`) re-imports from `core`; behavior unchanged.

### The `EngineControl` surface

Six RPCs. **`enqueue` is absent** — the producer enqueues directly in PG; the
worker is consumer-only. `GrpcQueueClient` still satisfies `Queue` but its
`enqueue` returns an "unsupported over the engine-wire" error (never called by
`Worker<Q>`).

```protobuf
syntax = "proto3";
package loom.engine.v1;

service EngineControl {
  rpc Dequeue   (DequeueRequest)    returns (DequeueResponse);
  rpc Complete  (CompleteRequest)   returns (CompleteResponse);
  rpc Fail      (FailRequest)       returns (FailResponse);
  rpc Heartbeat (HeartbeatRequest)  returns (HeartbeatResponse);
  rpc AwaitJobs (AwaitJobsRequest)  returns (AwaitJobsResponse);   // unary long-poll
  rpc FlushTable(FlushTableRequest) returns (FlushTableResponse);
}

message Job {
  string id = 1;          // Uuid as string
  string kind = 2;
  string payload = 3;     // serde_json::Value as a JSON string
  int32  attempts = 4;
  int64  run_at = 5;      // unix microseconds
}
message DequeueRequest  { repeated string kinds = 1; string worker = 2; }
message DequeueResponse { optional Job job = 1; }     // absent = no job (NOT an error)

message CompleteRequest  { string id = 1; }   message CompleteResponse  {}
message HeartbeatRequest { string id = 1; }   message HeartbeatResponse {}

message FailRequest { string id = 1; string error = 2; RetryPolicy policy = 3; }
message RetryPolicy { oneof kind { int64 retry_delay_ms = 1; Abandon abandon = 2; } }
message Abandon {}
message FailResponse {}

message AwaitJobsRequest  { repeated string kinds = 1; uint64 timeout_ms = 2; }
message AwaitJobsResponse {}                            // returns on notify or timeout

message FlushTableRequest  { string schema = 1; string name = 2; }
message FlushTableResponse { optional int64 snapshot_id = 1; }   // absent = None
```

**Error model — gRPC `Status` for faults, typed `optional` for legitimate
empties.** `ControlPlaneError` → `tonic::Status` at the server boundary
(`NotFound → NOT_FOUND`, `Backend → INTERNAL`); `GrpcQueueClient` maps `Status`
back to `ControlPlaneError`, preserving the `Queue` `Result` contract. `Dequeue`
absent-`job` and `FlushTable` absent-`snapshot_id` are `Ok(None)`, not errors.

**`FlushTable` run-id provenance:** the request carries only `{schema, name}`; the
**engine mints a fresh `RunId`** when it runs `flush_table` (the worker holds no
logic, so the executor owns the run). The compaction lineage event ties to it.

### The engine process

```rust
#[tokio::main]
async fn main() -> Result<…> {
    let cfg  = service_runtime::Config::from_env()?;       // LOOM_DB_* unix-socket host
    let pool = service_runtime::build_pool(&cfg.db).await?;
    let cp   = service_runtime::control_plane(pool.clone(), cfg.lock_timeout);
    let catalog = make_catalog(cfg.db.pg_url(), &cfg.data_path).await?;   // SqlCatalog
    let svc  = EngineControlService { cp, catalog, pool };

    let path = std::env::var("LOOM_ENGINE_SOCKET")?;       // e.g. /run/loom/engine.sock
    let _ = std::fs::remove_file(&path);                   // clear a stale socket
    let listener = tokio::net::UnixListener::bind(&path)?;
    Server::builder()
        .add_service(EngineControlServer::new(svc))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown_signal())
        .await?;
    Ok(())
}
```

`EngineControlService` delegates: `dequeue`/`complete`/`fail`/`heartbeat`/
`await_jobs` → the held `PgControlPlane` (already `impl Queue`, incl. the
`PgListener`-backed `await_jobs` the long-poll handler awaits); `flush_table` →
mint `RunId`, call `flush_table(&catalog, &pool, &TableRef{schema,name}, run_id)`.
`make_catalog` reuses the iceberg-test helper shape (`SqlCatalogBuilder` over the
unix-socket dsn + `file://data_path` warehouse).

### The worker process

Its own minimal config (NO `service_runtime::Config`, which mandates DB fields a
zero-pool worker must not have): `LOOM_ENGINE_SOCKET`, a worker id (hostname/uuid),
a lease.

```rust
#[tokio::main]
async fn main() -> Result<…> {
    let client = GrpcQueueClient::connect(socket_path).await?;   // tonic over UDS; impl Queue
    let flush  = client.clone();                                 // second handle for the handler
    let worker = Worker::new(client, worker_id, lease);          // UNCHANGED Worker<Q>
    worker.run(&["flush_table".into()], shutdown_token(), move |job| {
        let flush = flush.clone();
        async move {
            let FlushJob { schema, name } = serde_json::from_value(job.payload)
                .map_err(|e| JobFailure { error: e.to_string(), policy: RetryPolicy::Abandon })?;
            flush.flush_table(schema, name).await
                .map_err(|e| JobFailure { error: e.to_string(),
                                          policy: RetryPolicy::Retry { delay: backoff(job.attempts) } })?;
            Ok(())
        }
    }).await
}
```

`GrpcQueueClient` (in `engine-wire`) wraps `EngineControlClient<Channel>`:
queue ops are wire calls (`Status`→`ControlPlaneError`); `enqueue` is the
unsupported error; `flush_table(schema,name)` for the handler. `Channel` is
`Clone`, so one handle drives the loop and a clone runs flushes over the one UDS
connection. The loop completes/fails/heartbeats via the `Queue` calls.

Bad payload → `Abandon` (won't self-heal); a flush error → `Retry` with backoff
(transient). A long flush is kept alive by the loop's heartbeat (wire
`Heartbeat`), so the lease doesn't expire mid-flush.

### `AwaitJobs` — NOTIFY over the wire (unary long-poll)

`AwaitJobs(kinds, timeout_ms)` blocks server-side: the engine delegates to
`PgControlPlane::await_jobs`, which `LISTEN`s the `loom_queue:<kind>` channels and
returns on the first notify or the timeout. The worker wakes immediately on a new
job — no polling. This mirrors the existing `await_jobs` exactly (a fresh
`PgListener` per wait). The client sets its gRPC deadline to `timeout_ms` + slack
so a hung server can't block the loop forever. (A *persistent* server-stream that
holds one listener across waits is a later efficiency optimization — out of scope.)

### Codegen + vendoring

- **Vendor (reindeer):** `tonic`, `prost`, `prost-types`, `prost-build`, `protox`,
  `tonic-prost-build`, and the transitive `tower`/`hyper-util`/`tokio-stream`.
  All pure Rust (no `protoc`, no `links`/native). Add to a `Cargo.toml`,
  `reindeer update`, `./tools/buckify.sh`, then run the **full** `buck2 test
  //src/...` (the dep-graph guard — a `reindeer update` can silently move
  `duckdb`; diff the lock vs merge-base for native crates).
- **Codegen genrule** (mirrors loom's `$(location …)` env trick for the sqlx
  cache): a first-party codegen `rust_binary` runs `protox::compile([...], ["."])`
  → `FileDescriptorSet` → `tonic_prost_build::configure().build_client(true)
  .build_server(true).compile_fds(fds)`, writing `engine_control.rs` to
  `$OUT_DIR`. A `genrule` runs it over `engine_control.proto`. The `engine-wire`
  library sets `env = {"ENGINE_PB": "$(location :engine-control-gen)"}` and
  `include!(concat!(env!("ENGINE_PB"), "/engine_control.rs"))` in a `pb` module.
- **De-risk first:** the first plan task proves this pipe with a trivial `Ping`
  proto (one RPC) compiling end to end + a round-trip unit test, *before* the real
  surface — so any buck2 codegen surprise hits one RPC, not six.

## Error handling

| Situation | Behavior |
|---|---|
| `dequeue` finds no job | `DequeueResponse { job: None }` → worker `Ok(None)`, loop waits via `await_jobs` |
| `flush_table` returns `None` | `FlushTableResponse { snapshot_id: None }` → handler `Ok(())`, job completed |
| `ControlPlaneError` in any op | `tonic::Status` (INTERNAL/NOT_FOUND) → client maps back to `ControlPlaneError` |
| Worker can't reach the engine (socket down) | `dequeue` errors → loop surfaces it; the worker process exits/retries (supervisor restarts) |
| Long flush exceeds lease | loop heartbeats over the wire during the handler → lease held |
| Bad job payload | handler → `JobFailure { Abandon }` — terminal, retained for inspection |
| Transient flush failure | handler → `JobFailure { Retry { backoff } }` — re-dequeued later |
| Duplicate/stale flush job | `flush_table` is idempotent + advisory-locked → no-op or harmless re-flush |

## Testing

- **`engine-wire` unit tests** — `core ↔ proto` round-trips (`Job`, `RetryPolicy`
  oneof, `JobId`↔string, `run_at`↔micros); pure, no fixtures.
- **Engine integration (`loom_fixture_test`)** — spawn `EngineControlService`
  (fixture `PgControlPlane` + `SqlCatalog`) on a temp UDS, connect a
  `GrpcQueueClient`, assert over the wire: dequeue/complete round-trip; `fail`
  with `Retry` and `Abandon`; `await_jobs` returns *before* timeout when a job is
  enqueued after the call begins (proves the NOTIFY bridge); `FlushTable` makes an
  inline table file-backed.
- **Full e2e (the money test)** — `inline_append` past the threshold (Spec 1
  enqueues a real `flush_table` job) → run the actual `Worker<GrpcQueueClient>`
  for one drain against the live engine → assert the job completed and the table
  flushed.
- **Structural guard** — a buck2 `uquery` (or reviewed dep list) asserting the
  `worker` crate does not depend on `control-plane-postgres`.

## Files

- **Create** `src/services/engine-wire/` — `engine_control.proto`, the codegen
  `rust_binary` + genrule + `BUCK`, `src/lib.rs` (`pb` include, translation,
  `GrpcQueueClient`), unit tests.
- **Create** `src/services/engine/` — `EngineControlService` (`src/lib.rs`),
  `src/main.rs`, `BUCK`, integration tests.
- **Create** `src/services/worker/` — flush handler + `src/main.rs`, `BUCK`, the
  e2e + structural tests.
- **Modify** `control-plane-core` — add `FlushJob` + `FLUSH_JOB_KIND` (moved from
  postgres); **modify** `control-plane-postgres` — re-import them from `core`.
- **Modify** `service-runtime` — none required; the tonic/UDS server is inlined in
  the engine binary for now (one consumer). Promote a shared `serve` helper only
  when a second tonic service appears.
- **Modify** `third-party/` (reindeer) + workspace `Cargo.toml` — the tonic stack.
- **Modify** `docs/spike/{ICEBERG_ROADMAP,engine-wire-transport}.md` — mark the
  flush vertical landed; note the consumer shipped.

## Out of scope (deferred)

- **Arrow Flight / the data plane** — no bulk data crosses the wire for flush;
  `DoGet`/`DoPut` + the arrow-major lane come with the read/write vertical.
- **`enqueue` over the wire** — the producer enqueues in PG directly.
- **Persistent-stream `AwaitJobs`** — the unary long-poll is the slice-1 form.
- **Deploy** — single-computer, two local processes; apko/Helm stays deferred.
- **Multiple engines / connection pooling / TLS / auth on the socket** — one
  engine, one UDS, local trust.

## Branch

Off `main` (PR #103 merged: the flush trigger + `FlushJob`/`FLUSH_JOB_KIND` are
present to move to `core`).
