# Design: real-HTTP e2e smoke (over-the-wire, both engines)

> **Status:** approved design (2026-06-23). Adds loom's first **over-the-wire**
> end-to-end test layer: boot the services on a real `TcpListener` via
> `service_runtime::serve` and drive them with a real HTTP client (`reqwest`),
> proving the network path — `serve()` glue + routers + body/JSON serialization +
> status codes — holds end-to-end across the **ingest → query-api vertical**, over
> **both** storage backends (DuckLake and Iceberg). Closes the deliberate deferral
> recorded in `2026-06-13-service-runtime-and-binaries-design.md`
> (the "socket round-trip" was left untested to avoid an HTTP-client dependency).
>
> Register: promotes `fut-socket-roundtrip-test` → `road-e2e-http-client` (area `test`).

## Goal

Every current e2e test drives the axum router **in-process** via
`tower::ServiceExt::oneshot` — the network is bypassed entirely, so
`service_runtime::serve` (the real `TcpListener` bind) and the request/response
path over an actual socket have **zero** coverage. This slice adds a thin
real-HTTP layer on top of the existing behavioral suite: a harness that spawns the
real routers on a bound TCP port and a single fixture test that exercises the
ingest → query-api vertical over `reqwest`, asserting the happy path plus two error
status codes serialize correctly over the wire.

The existing `oneshot` suite stays as the behavioral coverage. This is a **smoke of
the wire**, not a second exhaustive endpoint suite.

## Why now

The 2026-06-13 service-runtime spec deferred this deliberately ("it would pull in a
reqwest/hyper client dependency for ~2 lines of glue … a socket smoke test arrives
when a client dep is justified"). Two things make it justified now:

1. `reqwest` 0.12.28 is **already vendored** in `third-party/BUCK` (transitively via
   iceberg/parquet) with the `json` + `rustls-tls` features — exposing it is an alias
   + dev-dependency step, not a new download.
2. loom now has **two** storage backends behind the same HTTP surface (DuckLake and
   Iceberg, the "Iceberg-default parity" theme). Proving the governed network path
   holds over **both** is exactly the kind of cross-cutting guarantee a wire smoke
   gives that per-backend unit tests do not.

## Scope

### In

1. **Expose `reqwest`** as `//third-party:reqwest`. It is already resolved at 0.12.28
   with `json` + `rustls-tls` + `blocking`; add it as a **dev-dependency** of the test
   crate so reindeer emits the public alias, run `./tools/buckify.sh`, and — per the
   CLAUDE.md reindeer guard — run the **full** `buck2 test //src/...` and confirm the
   `duckdb` pin did **not** downgrade (`cargo update -p duckdb --precise 1.10503.1` if
   it did). The test uses the async client; no feature change is expected.

2. **A wire-harness helper**, extending the `//src/services/query-api:e2e-support`
   library:
   - `spawn_http(router) -> (base_url, guard)` — bind `127.0.0.1:0`, `tokio::spawn`
     `service_runtime::serve(addr, router)`, await readiness, return the base URL and a
     liveness guard that keeps the serving task (and any tempdirs) alive for the test.
     Shape mirrors `src/services/engine/tests/wire.rs::spawn_server` (spawn server on a
     task, hand back the address).
   - **Iceberg-side construction helpers** the DuckLake-only `setup` lacks: build the
     Iceberg SQL catalog (over the same Postgres + a `file://` warehouse), the
     `IcebergMaterializer` (ingest write side), and the `DataFusionServingEngine` /
     `IcebergActionWriter` (query-api read/action side) — cribbed from the two
     `main.rs` backend-selection arms (see **Backend seams** below).

3. **One `loom_fixture_test(duckdb=True)`** — `src/services/query-api/tests/http_wire_e2e.rs`
   (it already hosts `e2e-support`) — **parametrized over both backends** `{DuckLake,
   Iceberg}`. For each backend:
   - seed the ontology + dataset→model binding (the *not-under-test* scaffolding) using
     the existing `e2e-support` `setup`/`tref`/`prop` helpers;
   - build the **ingest** router with the matching `LandingMaterializer` and the
     **query-api** router with the matching `ServingEngine` + `ActionEngine`, both over
     the **same** Postgres + warehouse;
   - `spawn_http` both routers on ephemeral ports;
   - drive `reqwest`:
     - **happy path** — `POST` Arrow IPC to ingest `/datasets/main/customer` → `200`
       with a `snapshot_id` JSON body; authorized `GET` query-api `/objects/customer`
       (with `X-Loom-Subject` granted read) → `200` with the landed rows in the JSON
       body;
     - **deny** — `GET /objects/customer` as an unauthorized subject → `403`;
     - **malformed 4xx** — a bad request (garbage Arrow body, or an unknown type) →
       `4xx`.

### Backend seams (what the harness reuses)

The harness builds each router through the **same** backend-selection seams the
binaries use, so the test proves real wiring rather than a test-only assembly:

- **ingest** (`src/services/ingest/src/main.rs`): `LOOM_LANDING_BACKEND` →
  `parse_landing_backend` → `Arc<dyn LandingMaterializer>` =
  `DuckLakeMaterializer{cp,store}` | `IcebergMaterializer{…}` (via `build_iceberg_catalog`).
  The router carries the materializer in its state (`http.rs`: "the configured landing
  backend, chosen at boot").
- **query-api** (`src/services/query-api/src/main.rs`): `parse_serving_backend` →
  `(Arc<dyn ServingEngine>, Arc<dyn ActionEngine>)` = `EmbeddedDuckDb` +
  `DuckLakeActionWriter` | `DataFusionServingEngine(IcebergCatalog)` +
  `IcebergActionWriter`.

The work plan should prefer driving these `parse_*` + construction paths directly
(extracting the per-backend assembly into helpers the harness and `main.rs` can
share, if that reduces duplication) over re-implementing backend selection in the test.

### Out (residual deferrals)

- **Actual-binary-subprocess smoke** — booting the built `ingest-bin` / `query-api-bin`
  as a subprocess to also cover `main.rs` + `Config::from_env` over a spawned process.
  Stays deferred: `main.rs`/`from_env` are covered today by the `runtime` config unit
  test + the `runtime_land` fixture, and the in-process spawn covers `serve()` + the
  routers over a real socket. Record as a residual FUTURE item at completion.
- **No migration** of the in-process `oneshot` suite to real HTTP — the two layers
  coexist (oneshot = behavioral breadth, wire = network smoke).
- **No exhaustive per-endpoint coverage** over the wire — only the land/read vertical
  plus the deny + malformed cases.
- **TLS / graceful shutdown / signal handling** — unchanged, under `fut-graceful-shutdown-tls`.

## Harness shape (illustrative)

```rust
// in e2e-support
pub async fn spawn_http(router: axum::Router) -> (String, ServeGuard) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });
    // (or call service_runtime::serve and bind/await readiness equivalently)
    (format!("http://{addr}"), ServeGuard(handle))
}
```

The fixture test loops over the two backends, builds the routers, `spawn_http`s
them, and runs the three assertion blocks with a `reqwest::Client`.

## BUCK / deps

- New `http_wire_e2e` `loom_fixture_test(duckdb=True)` in
  `src/services/query-api/BUCK`, deps: `:e2e-support`, `query-api` (lib),
  `//src/services/ingest` (its lib + materializers), `//src/services/runtime`,
  `//third-party:reqwest`, plus the usual `//third-party:{tokio, arrow, serde_json,
  tempfile, …}` already used by the e2e tests, and the postgres fixture.
- `e2e-support` gains `reqwest` + `service_runtime` deps for `spawn_http`, and the
  Iceberg-side construction helpers gain the iceberg/datafusion deps the query-api
  `main.rs` already pulls.
- `reqwest` added as a dev-dependency to the owning crate's `Cargo.toml`; regenerate
  `third-party/BUCK` via `./tools/buckify.sh`.

## Verification

The test **is** the deliverable. Green via `buck2 test //src/...` (the fixture test
routes local automatically via `loom_fixture_test`). The two named risks:

- **reindeer pin drift** — adding `reqwest` re-resolves the lock; confirm the `duckdb`
  pin stayed at 1.10503.1 and the full suite (not just the touched crate) is green,
  per the CLAUDE.md reindeer guard.
- **doubled backend wiring** — the Iceberg arm needs the SQL catalog + warehouse set
  up exactly as the binary does; cribbing from `main.rs` (not re-inventing) keeps the
  test faithful and avoids a divergent assembly path.

## Open risks

- **Readiness race** — `spawn_http` must not return before the listener accepts
  connections. Binding the `TcpListener` synchronously before spawning `serve`
  (as above) removes the race; a `sleep` fallback (cf. `engine/tests/wire.rs`) is the
  cheap backstop if needed.
- **Shared warehouse/Postgres across two spawned services** — both routers must point
  at the **same** Postgres db and object-store root so a land via ingest is visible to
  a read via query-api; the fixture must thread one `db`/warehouse into both arms.
- **Iceberg binding/landing ordering** — the dataset→model binding and the
  HTTP-driven land must be sequenced so the typed read resolves; the work plan
  resolves the exact order (reusing the e2e-support binding helpers).
