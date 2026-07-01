# Standalone `loom` binary — in-process composition (embedded-Postgres slice 3)

Status: design. Author: brainstorm session 2026-07-01.

## Why

The single-binary arc has, across slices 1 and 2, made loom able to run with **no
external Postgres and no PG files on disk beyond the binary**:

- Slice 1 ([`2026-06-28-embedded-postgres-lifecycle-design.md`](2026-06-28-embedded-postgres-lifecycle-design.md))
  added the `EmbeddedPg` lifecycle (initdb → spawn → socket → migrate → clean
  `pg_ctl stop`) behind `service_runtime::build_pool_managed`.
- Slice 2 removed the two on-disk dependencies: migrations
  ([`2026-06-28-embedded-migrations-embed-design.md`](2026-06-28-embedded-migrations-embed-design.md),
  via `sqlx::migrate!`) and the PG distribution itself
  ([`2026-06-29-embedded-pg-binaries-embed-design.md`](2026-06-29-embedded-pg-binaries-embed-design.md),
  via `managed-postgres-embed`'s `include_bytes!` + `extract_pg`).

What is still missing is the thing that ties them into a product: a **single
process** you run as `./loom` that boots the embedded Postgres **once** and serves
all three of loom's HTTP/gRPC surfaces — engine (tonic over a UDS), ingest (HTTP),
query-api (HTTP) — against the local warehouse, shutting the cluster down cleanly
on a signal. Today those are three separate binaries, each of which would boot its
*own* Postgres.

This slice is also the **first runtime consumer of `extract_pg`**. Slice 2 shipped
the self-extracting embed crate but left it unwired — `Config::from_env` still
requires `LOOM_PG_BIN_DIR` to point at a pre-staged `bin/`. The standalone binary
is where the baked-in distribution is finally extracted and booted with no
external path, completing the "genuine single file" story.

## Scope

**In:**

- A new crate `src/services/standalone/` producing a binary named **`loom`**, which
  composes the three services in one tokio runtime over one embedded (or external)
  Postgres.
- A **library serve seam** extracted from each of `engine`, `ingest`, and
  `query-api`, called by both that service's own lean `main.rs` **and** by `loom`,
  so the wiring exists once (no copy-paste between mains and the composite).
- Wiring `managed-postgres-embed::extract_pg` into the embedded boot path so the
  standalone binary needs no pre-staged `LOOM_PG_BIN_DIR`.
- A `loom_fixture_test` proving the composite boots embedded, serves all three
  surfaces end-to-end, and shuts down cleanly (PG stopped, data dir intact).

**Out (deferred / explicitly not this slice):**

- The lean per-service binaries stay as they are (the multi-process / Kubernetes
  deploy is unchanged). `loom` is additive.
- TLS, connection-pool tuning, and richer graceful-shutdown semantics beyond the
  signal wiring described here → [[fut-graceful-shutdown-tls]].
- GC of stale extracted PG caches → [[fut-embedded-pg-cache-gc]]; `pg_upgrade` of
  an existing data dir on a major bump → [[fut-embedded-postgres-pg-upgrade]].
- Merging the two HTTP services onto a single port. They run on two ports (see
  §Ports); route-merging would double-register the shared auth/session/openapi
  routes and is not worth the conflict.
- Windows. Unix (Linux/macOS) only, consistent with the rest of loom.

## Design

### The serve seam (all three services)

Every service's `main.rs` today constructs its service-specific state **inline**
and serves inline. To compose them in-process without duplication, each service
crate grows a library function that owns exactly its own construction + serving,
and both the lean `main.rs` and `loom` call it. Indicative signatures (exact
shapes settled by the implementation plan):

```rust
// engine/src/lib.rs — takes a PRE-BOUND listener + a readiness sender.
pub async fn run(
    listener: tokio::net::UnixListener,
    cfg: &service_runtime::Config,
    pool: sqlx::PgPool,
    ready: tokio::sync::oneshot::Sender<()>,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<(), Box<dyn std::error::Error>>;

// ingest/src/lib.rs
pub async fn serve(
    cfg: &service_runtime::Config,
    pool: sqlx::PgPool,
    auth: service_runtime::AuthState,
    bind: std::net::SocketAddr,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<(), Box<dyn std::error::Error>>;

// query-api/src/lib.rs
pub async fn serve(
    cfg: &service_runtime::Config,
    pool: sqlx::PgPool,
    auth: service_runtime::AuthState,
    engine_socket: &str,
    bind: std::net::SocketAddr,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<(), Box<dyn std::error::Error>>;
```

**Division of labour — shared vs owned.** The genuinely *once-only* work is done by the caller: `init_tracing`, booting the
`PgPool`, the one-time `bootstrap_admin`, and building the shared `AuthState` — all
passed in (or, for the `PgControlPlane`, cheaply re-wrapped from the shared `pool`
inside each seam, since it is a thin wrapper and a concrete type differs per
consumer). Each seam owns only its *service-specific* state:

- engine — the three `SqlCatalog` instances + the `IcebergActionWriter` (+ the
  `LOOM_INLINE_BYTE_LIMIT` / `LOOM_FLUSH_BYTE_THRESHOLD` reads that live in
  `engine/main.rs` today);
- ingest — the `IcebergMaterializer` and the `IngestConfig` layered load;
- query-api — the engine serving/action/governance clients, the `QueryApiConfig`
  load, the admin gate, and the optional Flight export listener.

This split means `bootstrap_admin` runs **once** in the composite, not redundantly
per service, and the lean mains become thin wrappers that build their own single
service's singletons and call the seam.

`service_runtime` gains a shutdown-aware serve helper (today `serve(bind, router)`
runs forever) — e.g. `serve_with_shutdown(bind, router, shutdown)` wrapping
`axum::serve(...).with_graceful_shutdown(...)` — reused by the ingest/query-api
seams. The engine seam uses tonic's `serve_with_incoming_shutdown`.

### Boot sequence in `loom`

```text
init_tracing()
cfg = Config::from_env()                              # once
if migrate_requested() { run_migrations(&cfg.db); exit 0 }   # migrate-and-exit image still works
if cfg.embedded && bin_dir unset:                    # NEW: first consumer of extract_pg
    ex = extract_pg(<LOOM_DATA_PATH>/cache)
    set embedded bin_dir = ex.bin_dir, ld_library_path = ex.lib_dir
(pool, pg) = build_pool_managed(&cfg)                # boots embedded PG once + migrates once
cp   = control_plane(pool.clone(), cfg.lock_timeout)
bootstrap_admin(cp, …)                               # once, if creds present

# --- engine first, with an explicit readiness gate ---
listener = UnixListener::bind(LOOM_ENGINE_SOCKET)    # socket file exists immediately
(ready_tx, ready_rx) = oneshot()
spawn engine::run(listener, &cfg, pool.clone(), ready_tx, shutdown_sub())
ready_rx.await                                        # engine is serving before clients connect

# --- the two HTTP services ---
spawn ingest::serve(&cfg, pool.clone(), auth, ingest_bind, shutdown_sub())
spawn query_api::serve(&cfg, pool.clone(), auth, engine_socket, qapi_bind, shutdown_sub())

await_signal()                                       # SIGINT or SIGTERM
broadcast shutdown                                   # every shutdown_sub() future resolves
join all three serve tasks
if let Some(pg) = pg { pg.shutdown().await }          # clean pg_ctl stop LAST
```

**Readiness (race-free).** The composite binds the engine's `UnixListener`
*synchronously* before spawning, so the socket file is guaranteed present. The
engine seam then fires `ready_tx` as it enters its serve loop. The composite awaits
`ready_rx` before it constructs the ingest / query-api engine clients. Because the
listener is bound before the signal and tonic's channel connects lazily, the
client handshakes cannot lose a race against socket creation. No connect-retry is
needed on the primary path.

### Ports

`loom` reads **two** HTTP bind addresses so the two HTTP services do not collide
(both lean binaries bind the single `LOOM_BIND_ADDR` today):

- `LOOM_QUERY_API_BIND_ADDR` — default `0.0.0.0:8080`
- `LOOM_INGEST_BIND_ADDR` — default `0.0.0.0:8081`
- `LOOM_ENGINE_SOCKET` — the engine UDS (as today)
- `LOOM_FLIGHT_BIND_ADDR` — optional governed Flight export (as today), its own
  address

The lean per-service binaries keep using `LOOM_BIND_ADDR` unchanged.

In embedded mode `LOOM_PG_BIN_DIR` becomes **optional** for `loom` — `extract_pg`
supplies it — which incidentally closes part of the ergonomics gap tracked by
[[fut-embedded-pg-db-vars-optional]] (that item's `LOOM_DB_*` half is separate and
stays open).

### Shutdown

One signal handler listens for **both** SIGINT and SIGTERM (SIGTERM is what a
container runtime sends) and drives a shared `tokio::sync::watch<bool>`. Each
seam's `shutdown` future is a `watch::Receiver` that resolves on the first `true`
(axum `with_graceful_shutdown`; tonic `serve_with_incoming_shutdown`). The
composite awaits all three serve tasks so they drain and release their pool
connections, **then** calls `pg.shutdown().await` for the clean `pg_ctl stop`.
Ordering matters: the cluster is stopped only after its clients are gone.

### Error handling

- Any singleton-construction failure (config parse, `extract_pg`, PG boot,
  migration) fails startup before any service task is spawned — a hard, clear exit.
- A serve task that exits with an error triggers a shutdown of the whole composite
  (a dead engine makes ingest/query-api useless), reported with the originating
  service; the composite still runs `pg.shutdown()` on the way out so a crash does
  not leak a postmaster (the `EmbeddedPg` `kill_on_drop` backstop remains as the
  last resort).
- `migrate_requested()` (`LOOM_MIGRATE=apply`) is honored so the `loom` image can
  also be used as the chart's one-shot migrator, exactly like the lean images.

## Testing

A `loom_fixture_test` (local execution via the `loom_fixture_test` macro — it boots
a real cluster) that:

1. Points `LOOM_PG_MODE=embedded` at a temp `LOOM_DATA_PATH`, picks free ports for
   the two HTTP services and a temp path for the engine UDS, and boots the `loom`
   composite as a task.
2. Waits for readiness, then exercises **all three surfaces end-to-end in one
   flow**: an ingest `POST /datasets/{schema}/{table}` (Arrow IPC), then a query-api
   `GET /objects/{type}` that reads the landed rows back — which only succeeds if
   the query-api → engine UDS wiring came up. This round-trip is the proof (chosen
   over three independent health probes because it verifies the *composition*, not
   just three live listeners).
3. Sends SIGTERM and asserts a graceful exit: all serve tasks stop, the embedded PG
   is stopped cleanly (no orphaned postmaster — check via `postmaster.pid` /
   `/proc`), and the data dir survives so a second boot on the same dir does not
   re-`initdb`.

Reuses the slice-1/2 fixture plumbing.

## Register changes

- Promote `fut-embedded-postgres-all-in-one` → `- [x] status:promoted`, prose noting
  the standalone binary shipped it.
- Mint `road-embedded-postgres-standalone` (`- [ ] status:planned`, `area:deploy`,
  `spec:2026-07-01-embedded-postgres-standalone-design`, `[[fut-embedded-postgres-all-in-one]]`
  backlink).
