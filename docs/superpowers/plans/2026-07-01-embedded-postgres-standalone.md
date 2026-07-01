# Standalone `loom` binary (embedded-Postgres slice 3) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a single self-contained `loom` binary that boots the embedded Postgres once and runs engine (tonic/UDS) + ingest (HTTP) + query-api (HTTP) as tasks in one tokio runtime, with graceful signal shutdown.

**Architecture:** Extract a library serve seam from each service crate (`engine::run`, `ingest::serve`, `query_api::serve`) so both the lean per-service binary and a new `standalone` crate call it. The `standalone` crate's `lib.rs` composite (`run`) binds all listeners synchronously (bind-first, race-free readiness), gates ingest/query-api on an explicit engine-ready `oneshot`, fans one SIGINT/SIGTERM out to all three via a `watch<bool>`, then stops the embedded PG last. The thin `main.rs` self-extracts the baked-in PG (`extract_pg`) and injects its bin dir before `Config::from_map`.

**Tech Stack:** Rust 2024, buck2, axum, tonic, tokio, sqlx, `service_runtime`, `managed-postgres`, `managed-postgres-embed`. Tests are `rust_test` / `loom_fixture_test` integration targets (never inline `#[cfg(test)]`).

## Global Constraints

- **No inline tests.** Every test is a sibling `tests/<name>.rs` wired as its own `rust_test` (pure logic) or `loom_fixture_test` (boots Postgres) target. The `no-inline-tests` prek hook fails on any `#[test]`/`#[tokio::test]` under `src/**`.
- **Fixture tests use `loom_fixture_test`** (`src/control-plane/postgres/defs.bzl`), never a bare `rust_test`, or they route to RE and fail as root.
- **Clippy is strict** (`pedantic` + `restriction`): production code must not `unwrap`/`expect`/`panic`/`todo`/index-slice/`dbg!`. Return `Result` and propagate. Test code is exempted from panic-safety lints via the `loom_rust_test`/`loom_fixture_test` wrappers.
- **Don't pipe `buck2 test`/`bxl` through `tail`/`head`** — redirect to a file and grep it: `buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **`service_runtime` is buck-only** (no Cargo manifest) — its dependency lives in each crate's `BUCK` `deps`, never in `Cargo.toml`.
- **Binary name is `loom`; crate dir is `src/services/standalone/`.** The three lean binaries (`engine-bin`, `ingest-bin`, `query-api-bin`) keep their names and behaviour.
- **Two HTTP ports** for the composite: `LOOM_QUERY_API_BIND_ADDR` (default `0.0.0.0:8080`), `LOOM_INGEST_BIND_ADDR` (default `0.0.0.0:8081`). Engine keeps `LOOM_ENGINE_SOCKET` (UDS); Flight export keeps `LOOM_FLIGHT_BIND_ADDR` (optional).
- **Behaviour-preserving extraction:** tasks 2–4 must keep every existing per-crate test green; the router/service construction is *moved*, not changed.

---

## File structure

| File | Responsibility |
|------|----------------|
| `src/services/runtime/src/lib.rs` (modify) | Add `serve_with_shutdown(TcpListener, Router, shutdown)`; refactor `serve` to delegate. |
| `src/services/runtime/tests/serve_shutdown.rs` (create) | Unit test: shutdown future ends the server. |
| `src/services/engine/src/run.rs` (create) + `lib.rs` (modify) | `engine::run(UnixListener, &Config, PgPool, oneshot::Sender<()>, shutdown)` seam. |
| `src/services/engine/src/main.rs` (modify) | Thin: bind UDS, call `engine::run`. |
| `src/services/engine/BUCK` (modify) | Add `service_runtime` + serving/wire/iceberg/tonic/tokio-stream deps to the `:engine` library. |
| `src/services/ingest/src/serve.rs` (create) + `lib.rs` (modify) | `ingest::serve(&Config, PgPool, AuthState, TcpListener, shutdown)` seam. |
| `src/services/ingest/src/main.rs` (modify) | Thin: shared setup, bind TCP, call `ingest::serve`. |
| `src/services/ingest/BUCK` (modify) | Add `tokio` to the `:ingest` library deps. |
| `src/services/query-api/src/serve.rs` (create) + `lib.rs` (modify) | `query_api::serve(&Config, Arc<PgControlPlane>, AuthState, engine_socket, TcpListener, shutdown)` seam (no `PgPool` — reads/writes go over the engine wire). |
| `src/services/query-api/src/main.rs` (modify) | Thin: shared setup, bind TCP, call `query_api::serve`. |
| `src/services/query-api/BUCK` (modify) | Add `tokio-stream` to the `:query-api` library deps. |
| `src/services/standalone/src/lib.rs` (create) | `StandaloneAddrs` + `run(Config, StandaloneAddrs, shutdown, ready)` composite. |
| `src/services/standalone/src/main.rs` (create) | `extract_pg` wiring, env resolution, SIGINT/SIGTERM handler, migrate-and-exit. |
| `src/services/standalone/{Cargo.toml,BUCK}` (create) | `standalone` library + `loom` binary + fixture-test target. |
| `src/services/standalone/tests/composite_e2e.rs` (create) | Fixture test: boot composite → ingest POST → query-api GET → SIGTERM → clean stop. |
| `docs/deploy.md` (modify) | Document the `loom` single binary + its env vars. |

---

### Task 1: `serve_with_shutdown` helper in `service_runtime`

**Files:**
- Modify: `src/services/runtime/src/lib.rs` (the `serve` fn at ~line 364-371)
- Modify: `src/services/runtime/BUCK` (add a `rust_test` target)
- Test: `src/services/runtime/tests/serve_shutdown.rs`

**Interfaces:**
- Produces: `pub async fn serve_with_shutdown(listener: tokio::net::TcpListener, router: axum::Router, shutdown: impl std::future::Future<Output = ()> + Send + 'static) -> Result<(), RuntimeError>` and unchanged `pub async fn serve(bind_addr: SocketAddr, router: Router) -> Result<(), RuntimeError>`.

- [ ] **Step 1: Write the failing test**

Create `src/services/runtime/tests/serve_shutdown.rs`:

```rust
//! `serve_with_shutdown` returns once its shutdown future resolves.
use std::time::Duration;

use axum::{Router, routing::get};

#[tokio::test]
async fn shutdown_future_stops_the_server() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let router = Router::new().route("/health", get(|| async { "ok" }));
    let handle = tokio::spawn(async move {
        service_runtime::serve_with_shutdown(listener, router, async move {
            let _ = rx.await;
        })
        .await
    });

    // Server is accepting: a TCP connect to the bound port succeeds.
    tokio::net::TcpStream::connect(addr).await.unwrap();

    // Fire shutdown; the serve task must return Ok promptly.
    tx.send(()).unwrap();
    let joined = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("serve_with_shutdown did not return within 5s");
    assert!(joined.unwrap().is_ok());
}
```

- [ ] **Step 2: Wire the test target and run it to see it fail**

Add to `src/services/runtime/BUCK` (mirror an existing `rust_test`; use the `loom_rust_test` wrapper already loaded there):

```python
loom_rust_test(
    name = "serve-shutdown",
    crate = "serve_shutdown",
    srcs = ["tests/serve_shutdown.rs"],
    crate_root = "tests/serve_shutdown.rs",
    deps = [
        ":runtime",
        "//third-party:axum",
        "//third-party:tokio",
    ],
)
```

Run: `buck2 test //src/services/runtime:serve-shutdown > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`
Expected: FAIL — `serve_with_shutdown` does not exist (compile error).

- [ ] **Step 3: Implement `serve_with_shutdown` and refactor `serve`**

In `src/services/runtime/src/lib.rs`, replace the existing `serve` fn with:

```rust
/// Bind `bind_addr` and serve `router` until the process is terminated.
pub async fn serve(bind_addr: SocketAddr, router: Router) -> Result<(), RuntimeError> {
    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(RuntimeError::Bind)?;
    serve_with_shutdown(listener, router, std::future::pending()).await
}

/// Serve `router` on an already-bound `listener`, returning once `shutdown` resolves.
/// Binding before the caller spawns this lets the caller guarantee the socket is
/// accepting before it signals readiness.
pub async fn serve_with_shutdown(
    listener: tokio::net::TcpListener,
    router: Router,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), RuntimeError> {
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(RuntimeError::Serve)
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test //src/services/runtime:serve-shutdown > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Lint + commit**

Run: `buck2 build '//src/services/runtime:runtime[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty == clean).

```bash
git add src/services/runtime/src/lib.rs src/services/runtime/BUCK src/services/runtime/tests/serve_shutdown.rs
git commit -m "feat(runtime): serve_with_shutdown over a pre-bound listener"
```

---

### Task 2: Extract `engine::run` seam

**Files:**
- Create: `src/services/engine/src/run.rs`
- Modify: `src/services/engine/src/lib.rs` (add `pub mod run;` and re-export)
- Modify: `src/services/engine/src/main.rs`
- Modify: `src/services/engine/BUCK` (add deps to the `:engine` library)

**Interfaces:**
- Consumes: `service_runtime::{Config, build_storage_factory, build_serving_object_store, control_plane}`.
- Produces: `pub async fn engine::run(listener: tokio::net::UnixListener, cfg: &service_runtime::Config, pool: sqlx::PgPool, ready: tokio::sync::oneshot::Sender<()>, shutdown: impl std::future::Future<Output = ()> + Send) -> Result<(), Box<dyn std::error::Error + Send + Sync>>`. Fires `ready` immediately before entering the serve loop.

- [ ] **Step 1: Add the run module and re-export**

In `src/services/engine/src/lib.rs`:

```rust
pub mod flight;
pub mod run;
pub mod service;

pub use run::run;
```

- [ ] **Step 2: Write `src/services/engine/src/run.rs`**

Move the state construction that currently lives in `src/services/engine/src/main.rs` (lines ~32-101, the three `SqlCatalog` builds, the `LOOM_INLINE_BYTE_LIMIT`/`LOOM_FLUSH_BYTE_THRESHOLD` reads, the `IcebergActionWriter`, `EngineControlService`, `FlightDataService`) into this function body verbatim, with three changes: (a) the `cp`/`pool` come from parameters, (b) the listener is a parameter (no `LOOM_ENGINE_SOCKET` read, no bind, no stale-file removal here), (c) `ready` fires before serving and the serve uses the passed `shutdown`.

```rust
//! The engine service seam: build the control + flight services over a shared
//! pool and serve them on a pre-bound Unix socket until `shutdown` resolves.
use std::collections::HashMap;
use std::future::Future;

use arrow_flight::flight_service_server::FlightServiceServer;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use tokio::net::UnixListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

use engine_wire::pb::engine_control_server::EngineControlServer;

use crate::flight::FlightDataService;
use crate::service::EngineControlService;

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

/// Build and serve the engine on `listener`. Fires `ready` once the services are
/// built and the serve loop is about to run; returns when `shutdown` resolves.
pub async fn run(
    listener: UnixListener,
    cfg: &service_runtime::Config,
    pool: sqlx::PgPool,
    ready: oneshot::Sender<()>,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), BoxErr> {
    let cp = service_runtime::control_plane(pool.clone(), cfg.lock_timeout);

    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), cfg.db.pg_url());
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        cfg.object_store.warehouse_uri.clone(),
    );
    let props_for_writer = props.clone();
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)
        .load("loom", props.clone())
        .await?;
    let flight_catalog = SqlCatalogBuilder::default()
        .with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)
        .load("loom", props)
        .await?;

    let inline_byte_limit: usize = parse_env_or("LOOM_INLINE_BYTE_LIMIT", 16 * 1024 * 1024)?;
    let flush_byte_threshold: i64 = parse_env_or("LOOM_FLUSH_BYTE_THRESHOLD", 64 * 1024 * 1024)?;

    let writer_catalog = SqlCatalogBuilder::default()
        .with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)
        .load("loom", props_for_writer)
        .await?;
    let writer = engine_serving::IcebergActionWriter::new(
        std::sync::Arc::new(writer_catalog),
        pool.clone(),
        inline_byte_limit,
        flush_byte_threshold,
    );

    let control = EngineControlService {
        cp,
        catalog,
        pool: pool.clone(),
        retention: cfg.gc_retention,
        writer,
    };
    let flight = FlightDataService {
        catalog: flight_catalog,
        serving_catalog: IcebergCatalog::new(pool.clone()),
        serving_store: service_runtime::build_serving_object_store(&cfg.object_store)?,
        pool,
    };

    // Signal readiness: the caller binds `listener` before spawning us, so the
    // socket already accepts; this tells the caller the serve loop is starting.
    let _ = ready.send(());

    Server::builder()
        .add_service(EngineControlServer::new(control))
        .add_service(FlightServiceServer::new(flight))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown)
        .await?;
    Ok(())
}

fn parse_env_or<T>(key: &str, default: T) -> Result<T, BoxErr>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(v) => v.parse().map_err(|e| format!("{key}: {e}").into()),
        Err(_) => Ok(default),
    }
}
```

- [ ] **Step 3: Rewrite `src/services/engine/src/main.rs` to the thin binary**

```rust
//! engine binary: bind the UDS from the environment and serve via `engine::run`.
use tokio::net::UnixListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg = service_runtime::Config::from_env()?;
    if service_runtime::migrate_requested() {
        service_runtime::run_migrations(&cfg.db).await?;
        return Ok(());
    }
    let (pool, _pg) = service_runtime::build_pool_managed(&cfg).await?;

    let socket_path = std::env::var("LOOM_ENGINE_SOCKET")?;
    drop(std::fs::remove_file(&socket_path)); // remove stale socket (missing is fine)
    let listener = UnixListener::bind(&socket_path)?;

    let (ready_tx, _ready_rx) = tokio::sync::oneshot::channel();
    engine::run(listener, &cfg, pool, ready_tx, async {
        drop(tokio::signal::ctrl_c().await);
    })
    .await
}
```

- [ ] **Step 4: Update `src/services/engine/BUCK` — move shared deps into the library**

The `:engine` library now needs everything the seam uses. Set its `deps` to include (add the new ones to the existing list): `"//src/services/runtime:runtime"`, `"//third-party:iceberg"`, `"//third-party:tokio"`, `"//third-party:tokio-stream"`. Keep the existing `:engine` deps. The `engine-bin` binary keeps `:engine`, `//src/services/runtime:runtime`, `//third-party:tokio`. Verify no dependency cycle: `service_runtime` does not depend on `engine` (it is lower-level), so adding `runtime` to `:engine` is safe.

- [ ] **Step 5: Build + run the existing engine tests**

Run: `buck2 test //src/services/engine/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS (behaviour-preserving; the control/flight services are unchanged).

- [ ] **Step 6: Lint + commit**

Run: `buck2 build '//src/services/engine:engine[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty == clean).

```bash
git add src/services/engine/src/run.rs src/services/engine/src/lib.rs src/services/engine/src/main.rs src/services/engine/BUCK
git commit -m "refactor(engine): extract engine::run serve seam over a pre-bound listener"
```

---

### Task 3: Extract `ingest::serve` seam

**Files:**
- Create: `src/services/ingest/src/serve.rs`
- Modify: `src/services/ingest/src/lib.rs` (add `pub mod serve;` + re-export)
- Modify: `src/services/ingest/src/main.rs`
- Modify: `src/services/ingest/BUCK` (add `//third-party:tokio` to the `:ingest` library deps)

**Interfaces:**
- Consumes: `service_runtime::{Config, AuthState, protect, login_routes, session_routes, service_account_routes, with_openapi, load, env_map, serve_with_shutdown}`; `ingest::{build_openapi, http::{AppState, router}, landing::{IcebergMaterializer, LandingMaterializer}, config::IngestConfig}`.
- Produces: `pub async fn ingest::serve(cfg: &service_runtime::Config, pool: sqlx::PgPool, cp: std::sync::Arc<dyn control_plane_core::ControlPlane>, auth: service_runtime::AuthState, admin_subject: Option<control_plane_core::SubjectId>, max_ttl: std::time::Duration, listener: tokio::net::TcpListener, shutdown: impl std::future::Future<Output = ()> + Send + 'static) -> Result<(), Box<dyn std::error::Error + Send + Sync>>`.

- [ ] **Step 1: Add the serve module + re-export**

In `src/services/ingest/src/lib.rs` add `pub mod serve;` (with the other `pub mod`s) and `pub use serve::serve;` (with the other `pub use`s).

- [ ] **Step 2: Write `src/services/ingest/src/serve.rs`**

Move the materializer construction + router assembly from `main.rs` (lines ~48-79) into the body. The shared singletons (`pool`, `cp`, `auth`, admin bootstrap) are built by the caller and passed in; move the `build_iceberg_catalog` helper here too.

```rust
//! The ingest service seam: build the landing router over a shared pool and
//! serve it on a pre-bound TCP listener until `shutdown` resolves.
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use control_plane_core::{ControlPlane, SubjectId};
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;

use crate::http::{AppState, router};
use crate::landing::{IcebergMaterializer, LandingMaterializer};

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

pub async fn serve(
    cfg: &service_runtime::Config,
    pool: sqlx::PgPool,
    cp: Arc<dyn ControlPlane>,
    auth: service_runtime::AuthState,
    admin_subject: Option<SubjectId>,
    max_ttl: Duration,
    listener: tokio::net::TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), BoxErr> {
    let env = service_runtime::env_map();
    let app_cfg: crate::config::IngestConfig = service_runtime::load(&env)?;

    let materializer: Arc<dyn LandingMaterializer> = {
        let catalog = Arc::new(build_iceberg_catalog(cfg).await?);
        Arc::new(IcebergMaterializer {
            catalog,
            pool,
            inline_byte_limit: app_cfg.routing.inline_byte_limit,
            flush_byte_threshold: app_cfg.routing.flush_byte_threshold,
        })
    };

    let app = service_runtime::protect(router(AppState { materializer, cp }), auth.clone())
        .merge(service_runtime::login_routes(auth.clone()))
        .merge(service_runtime::session_routes(auth.clone()))
        .merge(service_runtime::service_account_routes(auth, admin_subject, max_ttl));
    let app = service_runtime::with_openapi(app, crate::build_openapi());
    service_runtime::serve_with_shutdown(listener, app, shutdown).await?;
    Ok(())
}

async fn build_iceberg_catalog(cfg: &service_runtime::Config) -> Result<SqlCatalog, BoxErr> {
    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), cfg.db.pg_url());
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        cfg.object_store.warehouse_uri.clone(),
    );
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(service_runtime::build_storage_factory(&cfg.object_store)?)
        .load("loom", props)
        .await?;
    Ok(catalog)
}
```

- [ ] **Step 3: Rewrite `src/services/ingest/src/main.rs` to the thin binary**

```rust
//! ingest binary: shared setup, bind the HTTP listener, serve via `ingest::serve`.
use std::sync::Arc;

use control_plane_core::ControlPlane;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    service_runtime::init_tracing();
    let cfg = service_runtime::Config::from_env()?;
    if service_runtime::migrate_requested() {
        service_runtime::run_migrations(&cfg.db).await?;
        return Ok(());
    }
    let (pool, _pg) = service_runtime::build_pool_managed(&cfg).await?;

    let pg = Arc::new(service_runtime::control_plane(pool.clone(), cfg.lock_timeout));
    let cp: Arc<dyn ControlPlane> = pg.clone();
    let auth = service_runtime::AuthState {
        auth: pg.clone(),
        session_ttl: service_runtime::session_ttl_from_env(),
    };
    if let (Ok(user), Ok(pass)) = (
        std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME"),
        std::env::var("LOOM_BOOTSTRAP_ADMIN_PASSWORD"),
    ) {
        service_runtime::bootstrap_admin(pg.as_ref(), &user, &pass).await?;
    }
    let admin_subject = std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME")
        .ok()
        .map(control_plane_core::SubjectId);
    let max_ttl = service_runtime::service_token_max_ttl_from_env();

    let listener = tokio::net::TcpListener::bind(cfg.bind_addr).await?;
    ingest::serve(&cfg, pool, cp, auth, admin_subject, max_ttl, listener, std::future::pending()).await
}
```

- [ ] **Step 4: Update `src/services/ingest/BUCK`**

Add `"//third-party:tokio"` to the `:ingest` library `deps` (the seam binds a `TcpListener` and calls `serve_with_shutdown`). The `ingest-bin` deps are unchanged.

- [ ] **Step 5: Build + run the existing ingest tests**

Run: `buck2 test //src/services/ingest/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS.

- [ ] **Step 6: Lint + commit**

Run: `buck2 build '//src/services/ingest:ingest[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty == clean).

```bash
git add src/services/ingest/src/serve.rs src/services/ingest/src/lib.rs src/services/ingest/src/main.rs src/services/ingest/BUCK
git commit -m "refactor(ingest): extract ingest::serve seam over a pre-bound listener"
```

---

### Task 4: Extract `query_api::serve` seam

**Files:**
- Create: `src/services/query-api/src/serve.rs`
- Modify: `src/services/query-api/src/lib.rs` (add `pub mod serve;` + re-export)
- Modify: `src/services/query-api/src/main.rs`
- Modify: `src/services/query-api/BUCK` (add `//third-party:tokio-stream` to the `:query-api` library deps)

**Interfaces:**
- Consumes: `service_runtime::{Config, AuthState, AdminState, protect, login_routes, session_routes, admin_routes, service_account_routes, with_openapi_provider, load, env_map, serve_with_shutdown}`; `query_api::{live_openapi, http::{AppState, router}, engine_client::EngineServingClient, engine_action_client::EngineActionClient, wire_control_plane::WireControlPlane, serving::{ServingEngine, ActionEngine}, flight_export::FlightExportService, web_static, config::QueryApiConfig}`; `engine_wire::{client::GrpcQueueClient, flight::FlightSqlClient}`.
- Produces: `pub async fn query_api::serve(cfg: &service_runtime::Config, pg: std::sync::Arc<control_plane_postgres::PgControlPlane>, auth: service_runtime::AuthState, engine_socket: String, listener: tokio::net::TcpListener, shutdown: impl std::future::Future<Output = ()> + Send + 'static) -> Result<(), Box<dyn std::error::Error + Send + Sync>>`. Builds the engine clients internally (engine must already be serving on `engine_socket`). **No `pool` param** — query-api's reads/writes all go over the engine wire; it needs only the concrete `pg` control plane (for the wire-CP wrapper, admin/auth, and dynamic OpenAPI `list_types`).

> **Note:** pass the concrete `Arc<PgControlPlane>` (`pg`) because the dynamic OpenAPI provider and admin/auth wiring need `list_types` and the concrete ACL/Auth types the wire client does not implement — exactly as `main.rs` does today.

- [ ] **Step 1: Add the serve module + re-export**

In `src/services/query-api/src/lib.rs` add `pub mod serve;` and `pub use serve::serve;`.

- [ ] **Step 2: Write `src/services/query-api/src/serve.rs`**

Move the engine-client construction + router/admin/openapi/web_static/CORS + optional Flight-export block from `main.rs` (lines ~40-168) into the body, taking `pool`/`pg`/`auth`/`engine_socket`/`listener`/`shutdown` as parameters. The `DEFAULT_EXPORT_MAX_ROWS` const moves here too. The Flight export listener is spawned as an independent task (unchanged behaviour); the main HTTP server uses `serve_with_shutdown`.

```rust
//! The query-api service seam: connect the engine clients, build the read router,
//! and serve it on a pre-bound TCP listener until `shutdown` resolves.
use std::future::Future;
use std::sync::Arc;

use control_plane_core::ControlPlane;

use crate::engine_client::EngineServingClient;
use crate::http::{AppState, router};
use crate::serving::{ActionEngine, ServingEngine};

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

/// Default per-export row cap (`LOOM_EXPORT_MAX_ROWS`).
const DEFAULT_EXPORT_MAX_ROWS: u32 = 1_000_000;

pub async fn serve(
    cfg: &service_runtime::Config,
    pg: Arc<control_plane_postgres::PgControlPlane>,
    auth: service_runtime::AuthState,
    engine_socket: String,
    listener: tokio::net::TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), BoxErr> {
    let env = service_runtime::env_map();
    let app_cfg: crate::config::QueryApiConfig = service_runtime::load(&env)?;

    let gov_client = engine_wire::client::GrpcQueueClient::connect(engine_socket.clone()).await?;
    let cp: Arc<dyn ControlPlane> = Arc::new(crate::wire_control_plane::WireControlPlane::new(
        gov_client,
        pg.clone() as Arc<dyn ControlPlane>,
    ));
    let (serving, action_engine): (Arc<dyn ServingEngine>, Arc<dyn ActionEngine>) = (
        Arc::new(EngineServingClient::connect(engine_socket.clone()).await?),
        Arc::new(crate::engine_action_client::EngineActionClient::connect(engine_socket.clone()).await?),
    );

    let cp_flight = cp.clone();
    let auth_flight: Arc<dyn control_plane_core::Auth + Send + Sync> = pg.clone();

    let admin_username =
        std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME").unwrap_or_else(|_| "admin".to_string());
    let admin_state = service_runtime::AdminState {
        auth: pg.clone(),
        acl: pg.clone(),
        admin_username,
    };
    let admin_subject = std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME")
        .ok()
        .map(control_plane_core::SubjectId);
    let max_ttl = service_runtime::service_token_max_ttl_from_env();

    let app = service_runtime::protect(
        router(AppState {
            cp,
            serving,
            action_engine,
            default_limit: app_cfg.serving.default_limit,
        }),
        auth.clone(),
    )
    .merge(service_runtime::login_routes(auth.clone()))
    .merge(service_runtime::session_routes(auth.clone()))
    .merge(service_runtime::admin_routes(admin_state, auth.clone()))
    .merge(service_runtime::service_account_routes(auth, admin_subject, max_ttl));

    let openapi_cp: Arc<dyn ControlPlane + Send + Sync> = pg.clone();
    let app = service_runtime::with_openapi_provider(app, move || {
        let cp = openapi_cp.clone();
        async move { crate::live_openapi(cp).await }
    });

    let app = crate::web_static::with_static(
        app,
        std::env::var("LOOM_UI_DIR").ok().map(std::path::PathBuf::from),
    );
    let origins = crate::web_static::parse_allowed_origins(
        &std::env::var("LOOM_CORS_ALLOWED_ORIGINS").unwrap_or_default(),
    );
    let app = crate::web_static::with_cors(app, &origins);

    if let Ok(bind) = std::env::var("LOOM_FLIGHT_BIND_ADDR") {
        spawn_flight_export(&bind, &engine_socket, auth_flight, cp_flight).await?;
    }

    service_runtime::serve_with_shutdown(listener, app, shutdown).await?;
    Ok(())
}

async fn spawn_flight_export(
    bind: &str,
    engine_socket: &str,
    auth_flight: Arc<dyn control_plane_core::Auth + Send + Sync>,
    cp_flight: Arc<dyn ControlPlane>,
) -> Result<(), BoxErr> {
    use arrow_flight::flight_service_server::FlightServiceServer;
    use crate::flight_export::FlightExportService;

    let max_rows = std::env::var("LOOM_EXPORT_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(DEFAULT_EXPORT_MAX_ROWS);
    let addr: std::net::SocketAddr = bind
        .parse()
        .map_err(|e| -> BoxErr { format!("LOOM_FLIGHT_BIND_ADDR `{bind}` invalid: {e}").into() })?;
    let flight_engine = engine_wire::flight::FlightSqlClient::connect(engine_socket.to_string()).await?;
    let export = FlightExportService::new(auth_flight, cp_flight, flight_engine, max_rows);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| -> BoxErr { format!("binding LOOM_FLIGHT_BIND_ADDR `{addr}` failed: {e}").into() })?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    tokio::spawn(async move {
        tracing::info!(%addr, "starting governed Flight export server");
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(export))
            .serve_with_incoming(incoming)
            .await
        {
            tracing::error!(error = %e, "Flight export server exited");
        }
    });
    Ok(())
}
```

- [ ] **Step 3: Rewrite `src/services/query-api/src/main.rs` to the thin binary**

```rust
//! query-api binary: shared setup, bind the HTTP listener, serve via `query_api::serve`.
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    service_runtime::init_tracing();
    let cfg = service_runtime::Config::from_env()?;
    if service_runtime::migrate_requested() {
        service_runtime::run_migrations(&cfg.db).await?;
        return Ok(());
    }
    // query-api reads/writes over the engine wire; it needs the concrete control
    // plane but not the pool directly, so `pool` is consumed building `pg`.
    let (pool, _pg) = service_runtime::build_pool_managed(&cfg).await?;

    let pg = Arc::new(service_runtime::control_plane(pool, cfg.lock_timeout));
    let auth = service_runtime::AuthState {
        auth: pg.clone(),
        session_ttl: service_runtime::session_ttl_from_env(),
    };
    if let (Ok(user), Ok(pass)) = (
        std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME"),
        std::env::var("LOOM_BOOTSTRAP_ADMIN_PASSWORD"),
    ) {
        service_runtime::bootstrap_admin(pg.as_ref(), &user, &pass).await?;
    }

    let engine_socket = std::env::var("LOOM_ENGINE_SOCKET").map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("LOOM_ENGINE_SOCKET must be set for the Iceberg serving backend: {e}").into()
        },
    )?;

    let listener = tokio::net::TcpListener::bind(cfg.bind_addr).await?;
    query_api::serve(&cfg, pg, auth, engine_socket, listener, std::future::pending()).await
}
```

- [ ] **Step 4: Update `src/services/query-api/BUCK`**

Add `"//third-party:tokio-stream"` to the `:query-api` library `deps` (the Flight-export helper uses `TcpListenerStream`). `tokio` and `tonic` are already there. The `query-api-bin` deps are unchanged (they still resolve transitively).

- [ ] **Step 5: Build + run the existing query-api tests**

Run: `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS.

- [ ] **Step 6: Lint + commit**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty == clean).

```bash
git add src/services/query-api/src/serve.rs src/services/query-api/src/lib.rs src/services/query-api/src/main.rs src/services/query-api/BUCK
git commit -m "refactor(query-api): extract query_api::serve seam over a pre-bound listener"
```

---

### Task 5: `standalone` crate — composite `run` + fixture test (TDD anchor)

**Files:**
- Create: `src/services/standalone/src/lib.rs`
- Create: `src/services/standalone/Cargo.toml`
- Create: `src/services/standalone/BUCK`
- Create: `src/services/standalone/tests/composite_e2e.rs`

**Interfaces:**
- Consumes: `engine::run`, `ingest::serve`, `query_api::serve`, `service_runtime::{Config, build_pool_managed, control_plane, AuthState, session_ttl_from_env, service_token_max_ttl_from_env, bootstrap_admin}`.
- Produces:
  - `pub struct StandaloneAddrs { pub query_api: std::net::SocketAddr, pub ingest: std::net::SocketAddr, pub engine_socket: String }`
  - `pub async fn run(cfg: service_runtime::Config, addrs: StandaloneAddrs, shutdown: impl std::future::Future<Output = ()> + Send + 'static, ready: tokio::sync::oneshot::Sender<()>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>`. Binds engine UDS → spawns `engine::run` → awaits engine-ready → binds both TCP listeners → spawns `ingest::serve` + `query_api::serve` → fires `ready` → on `shutdown`, fans out to all three, joins them, then stops the embedded PG.

- [ ] **Step 1: Write the fixture test first (`src/services/standalone/tests/composite_e2e.rs`)**

This is the feature's real proof. It boots the composite against the fixture-provided PG binaries in embedded mode, drives an ingest→query-api round-trip, then shuts down. Use the query-api `e2e-support` seeding helpers where useful; the HTTP calls go over real TCP with `reqwest`.

```rust
//! End-to-end: the standalone composite boots embedded PG, serves engine+ingest+
//! query-api together, round-trips a dataset (ingest POST -> query-api GET over the
//! engine UDS), and shuts down cleanly on signal.
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use e2e_support::{define_widget, grant_read, session_token, subject_with_role};
use standalone::StandaloneAddrs;

/// Build an embedded-mode Config pointed at the fixture PG binaries + a temp data
/// dir. `POSTGRES_BIN_DIR` / `POSTGRES_LD_LIBRARY_PATH` are injected by the
/// `loom_fixture_test` macro. `Config::from_map` derives the embedded data dir as
/// `<LOOM_DATA_PATH>/pgdata` and the socket dir as `<LOOM_DATA_PATH>/pgrun`
/// (verified in `src/services/runtime/src/lib.rs:193-194`) — so `LOOM_DB_HOST`
/// MUST be `<LOOM_DATA_PATH>/pgrun` (there are no `LOOM_PG_DATA_DIR`/
/// `LOOM_PG_SOCKET_DIR` env vars; do not invent them).
fn embedded_config(data_path: &std::path::Path) -> service_runtime::Config {
    use std::collections::HashMap;
    let bin_dir = std::env::var("POSTGRES_BIN_DIR").unwrap();
    let ld = std::env::var("POSTGRES_LD_LIBRARY_PATH").unwrap();
    let mut v: HashMap<String, String> = HashMap::new();
    v.insert("LOOM_BIND_ADDR".into(), "127.0.0.1:0".into()); // unused by the composite
    v.insert("LOOM_DB_HOST".into(), data_path.join("pgrun").display().to_string());
    v.insert("LOOM_DB_PORT".into(), "5432".into());
    v.insert("LOOM_DB_USER".into(), "postgres".into());
    v.insert("LOOM_DB_PASSWORD".into(), "postgres".into());
    v.insert("LOOM_DB_NAME".into(), "loom".into());
    v.insert("LOOM_DATA_PATH".into(), data_path.display().to_string());
    v.insert("LOOM_WAREHOUSE_URI".into(), format!("file://{}", data_path.join("warehouse").display()));
    v.insert("LOOM_PG_MODE".into(), "embedded".into());
    v.insert("LOOM_PG_BIN_DIR".into(), bin_dir);
    v.insert("LOOM_PG_LD_LIBRARY_PATH".into(), ld);
    service_runtime::Config::from_map(&v).expect("config")
}

async fn free_port() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    l.local_addr().unwrap().port()
}

/// Arrow IPC stream for two `main.widget` rows (id, name, qty). Mirrors the
/// `ipc_bytes(sample_batch())` pattern in `src/services/ingest/tests/http_model.rs`.
fn widget_ipc() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("qty", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![10_i64, 20])),
        ],
    )
    .unwrap();
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

#[tokio::test]
async fn composite_round_trips_and_shuts_down_cleanly() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join("warehouse")).unwrap();

    // `cfg` moves into the composite; clone it so the test can open its own direct
    // control-plane pool to the same embedded PG for out-of-band ontology + auth seeding.
    let cfg = embedded_config(tmp.path());
    let cfg_direct = cfg.clone();

    let engine_socket = tmp.path().join("engine.sock").display().to_string();
    let addrs = StandaloneAddrs {
        query_api: format!("127.0.0.1:{}", free_port().await).parse().unwrap(),
        ingest: format!("127.0.0.1:{}", free_port().await).parse().unwrap(),
        engine_socket,
    };
    let qapi = addrs.query_api;
    let ingest = addrs.ingest;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        standalone::run(cfg, addrs, async move { let _ = shutdown_rx.await; }, ready_tx).await
    });

    // Composite is up once every listener is bound + the engine is serving.
    tokio::time::timeout(Duration::from_secs(90), ready_rx)
        .await
        .expect("composite did not become ready in 90s")
        .expect("ready channel dropped");

    // Direct control-plane pool to the same embedded PG for seeding auth + ontology.
    let pool = service_runtime::build_pool(&cfg_direct.db).await.expect("direct pool");
    let cp = service_runtime::control_plane(pool, cfg_direct.lock_timeout);
    let (_subj, role) = subject_with_role(&cp, "reader").await;
    let token = session_token(&cp, "reader").await;

    let client = reqwest::Client::new();

    // (1) Ingest POST lands `main.widget` over HTTP — proves the ingest composition
    //     (HTTP -> materializer -> Iceberg write -> snapshot commit on the shared PG).
    let land = client
        .post(format!("http://{ingest}/datasets/main/widget"))
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/vnd.apache.arrow.stream")
        .body(widget_ipc())
        .send()
        .await
        .expect("ingest POST");
    assert!(land.status().is_success(), "ingest landing failed: {}", land.status());

    // (2) Define the Widget ontology type over the landed `main.widget` table + grant read.
    define_widget(&cp).await;
    grant_read(&cp, &role, "Widget").await;

    // (3) query-api GET /objects/Widget — proves the query-api -> engine UDS serving
    //     wiring, reading back the rows ingest just landed through the one shared catalog.
    let read = client
        .get(format!("http://{qapi}/objects/Widget"))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("query-api GET");
    assert_eq!(read.status(), reqwest::StatusCode::OK, "read status");
    let body: serde_json::Value = read.json().await.expect("json body");
    let objects = body.get("objects").and_then(|o| o.as_array()).expect("objects array");
    assert_eq!(objects.len(), 2, "expected 2 landed widgets, got {body}");

    // (4) Graceful shutdown: signal -> composite returns Ok, embedded PG stopped cleanly.
    shutdown_tx.send(()).unwrap();
    let res = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("composite did not shut down within 30s");
    assert!(res.unwrap().is_ok());

    // Data dir survives (no re-initdb next boot) and PG stopped cleanly (no orphan postmaster).
    assert!(tmp.path().join("pgdata").join("PG_VERSION").exists(), "data dir gone");
    assert!(
        !tmp.path().join("pgdata").join("postmaster.pid").exists(),
        "postmaster.pid left behind — PG not stopped cleanly"
    );
}
```

> **Implementer note on the ingest token:** the `reader` session token authenticates the ingest `POST /datasets/main/widget` (raw dataset landing is authN-gated, not type-ACL-gated). If landing returns 403 in practice, grant the `reader` role the required action (mirror how `src/services/ingest/tests/http_model.rs` authorizes its landing POSTs) rather than weakening the assertion. The `GET /objects/Widget` read is type-ACL-gated, hence the explicit `grant_read(&cp, &role, "Widget")`.
>
> **Implementer note on the type↔table mapping:** `e2e_support::define_widget` maps `Widget` → `TableRef { schema: "main", name: "widget" }`, so the ingest POST targets `/datasets/main/widget` to land into that exact table. If `define_widget`'s table ref differs in the current tree, align the POST path to it.

- [ ] **Step 2: Create `src/services/standalone/Cargo.toml`**

```toml
[package]
name = "standalone"
version = "0.1.0"
edition = "2024"

# service-runtime, engine, ingest, query-api are buck-only first-party crates
# (no Cargo manifest dep); their deps live in BUCK only. This manifest exists so
# reindeer/workspace resolution and rust-analyzer see the third-party crates.
[dependencies]
tokio = { workspace = true }
sqlx = { workspace = true }
```

- [ ] **Step 3: Create `src/services/standalone/BUCK` (library + test only for now)**

```python
load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")

rust_library(
    name = "standalone",
    crate = "standalone",
    srcs = ["src/lib.rs"],
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [
        "//src/control-plane/core:core",
        "//src/services/engine:engine",
        "//src/services/ingest:ingest",
        "//src/services/query-api:query-api",
        "//src/services/runtime:runtime",
        "//src/services/managed-postgres:managed-postgres",
        "//third-party:sqlx",
        "//third-party:tokio",
        "//third-party:tracing",
    ],
    visibility = ["PUBLIC"],
)

loom_fixture_test(
    name = "composite-e2e",
    crate = "composite_e2e",
    srcs = ["tests/composite_e2e.rs"],
    crate_root = "tests/composite_e2e.rs",
    deps = [
        ":standalone",
        "//src/services/query-api:e2e-support",
        "//src/services/runtime:runtime",
        "//third-party:arrow",
        "//third-party:reqwest",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

> Confirmed by the plan review: `//third-party:reqwest` exists (`third-party/BUCK`), the `loom_fixture_test` load path is correct, and `//src/services/query-api:e2e-support` exports `define_widget`/`subject_with_role`/`grant_read`/`session_token`. `rust_library`/`rust_binary` are native prelude rules needing no `load`. If `reqwest`'s client feature is not enabled on the target, prefer enabling it over swapping to a raw `hyper` client — the composite test needs a real TCP client (it exercises bound listeners, not in-process routers).

- [ ] **Step 4: Run the test to verify it fails**

Run: `buck2 test //src/services/standalone:composite-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL — `standalone` crate / `run` does not exist.

- [ ] **Step 5: Implement `src/services/standalone/src/lib.rs`**

```rust
//! The standalone composite: boot the embedded Postgres once and run engine
//! (tonic/UDS) + ingest (HTTP) + query-api (HTTP) as tasks in one runtime.
use std::future::Future;
use std::sync::Arc;

use control_plane_core::ControlPlane;

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

/// The three listen addresses the composite binds.
pub struct StandaloneAddrs {
    pub query_api: std::net::SocketAddr,
    pub ingest: std::net::SocketAddr,
    pub engine_socket: String,
}

/// Boot the composite. Binds all listeners before spawning their serve loops so
/// readiness is race-free; `ready` fires once every listener is bound. On
/// `shutdown` the three servers stop, then the embedded PG is stopped last.
pub async fn run(
    cfg: service_runtime::Config,
    addrs: StandaloneAddrs,
    shutdown: impl Future<Output = ()> + Send + 'static,
    ready: tokio::sync::oneshot::Sender<()>,
) -> Result<(), BoxErr> {
    // Shared singletons, built once.
    let (pool, pg_handle) = service_runtime::build_pool_managed(&cfg).await?;
    let pg = Arc::new(service_runtime::control_plane(pool.clone(), cfg.lock_timeout));
    let auth = service_runtime::AuthState {
        auth: pg.clone(),
        session_ttl: service_runtime::session_ttl_from_env(),
    };
    if let (Ok(user), Ok(pass)) = (
        std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME"),
        std::env::var("LOOM_BOOTSTRAP_ADMIN_PASSWORD"),
    ) {
        service_runtime::bootstrap_admin(pg.as_ref(), &user, &pass).await?;
    }
    let admin_subject = std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME")
        .ok()
        .map(control_plane_core::SubjectId);
    let max_ttl = service_runtime::service_token_max_ttl_from_env();

    // One shutdown source fanned out to all three servers via a watch channel.
    let (sd_tx, sd_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown.await;
        let _ = sd_tx.send(true);
    });
    let sub = |rx: tokio::sync::watch::Receiver<bool>| async move {
        let mut rx = rx;
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    };

    // Engine first: bind UDS synchronously, spawn, await engine-ready.
    drop(std::fs::remove_file(&addrs.engine_socket));
    let engine_listener = tokio::net::UnixListener::bind(&addrs.engine_socket)?;
    let (eng_ready_tx, eng_ready_rx) = tokio::sync::oneshot::channel();
    let engine_cfg = cfg.clone();
    let engine_pool = pool.clone();
    let engine_sd = sub(sd_rx.clone());
    let engine = tokio::spawn(async move {
        engine::run(engine_listener, &engine_cfg, engine_pool, eng_ready_tx, engine_sd).await
    });
    eng_ready_rx
        .await
        .map_err(|_| -> BoxErr { "engine exited before signalling ready".into() })?;

    // Bind both HTTP listeners, then spawn their serves.
    let ingest_listener = tokio::net::TcpListener::bind(addrs.ingest).await?;
    let qapi_listener = tokio::net::TcpListener::bind(addrs.query_api).await?;

    let ingest_cfg = cfg.clone();
    let ingest_cp: Arc<dyn ControlPlane> = pg.clone();
    let ingest_auth = auth.clone();
    let ingest_sd = sub(sd_rx.clone());
    let ingest = tokio::spawn(async move {
        ingest::serve(
            &ingest_cfg, pool.clone(), ingest_cp, ingest_auth,
            admin_subject.clone(), max_ttl, ingest_listener, ingest_sd,
        )
        .await
    });

    let qapi_cfg = cfg.clone();
    let qapi_pg = pg.clone();
    let qapi_auth = auth.clone();
    let qapi_socket = addrs.engine_socket.clone();
    let qapi_sd = sub(sd_rx);
    let qapi = tokio::spawn(async move {
        query_api::serve(&qapi_cfg, qapi_pg, qapi_auth, qapi_socket, qapi_listener, qapi_sd).await
    });

    // All listeners are bound and serving: signal composite readiness.
    let _ = ready.send(());

    // Await all three; surface the first error.
    let (e, i, q) = tokio::join!(engine, ingest, qapi);
    let outcome = join_result(e).and(join_result(i)).and(join_result(q));

    // Stop the embedded PG last (after clients released their pool connections).
    if let Some(pg_handle) = pg_handle {
        pg_handle.shutdown().await?;
    }
    outcome
}

fn join_result(r: Result<Result<(), BoxErr>, tokio::task::JoinError>) -> Result<(), BoxErr> {
    match r {
        Ok(inner) => inner,
        Err(e) => Err(Box::new(e)),
    }
}
```

> **Implementer note (pool ownership):** `engine::run` and `ingest::serve` each take a `pool` (clone once per consumer, as shown). `query_api::serve` takes **no** pool — it only needs the shared `pg` control plane — so there is no pool to juggle for query-api. `pg` was built from `pool.clone()` at the top, so all three ultimately share one embedded-PG pool.

- [ ] **Step 6: Run the fixture test to verify it passes**

Run: `buck2 test //src/services/standalone:composite-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS. If it flakes on cloud contention, re-run once (see CLAUDE.md fixture-flake note) and confirm a clean local sweep.

- [ ] **Step 7: Lint + commit**

Run: `buck2 build '//src/services/standalone:standalone[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty == clean).

```bash
git add src/services/standalone/
git commit -m "feat(standalone): composite run() over one embedded PG, proven by fixture e2e"
```

---

### Task 6: `standalone` binary `loom` — extract_pg wiring + signals

**Files:**
- Create: `src/services/standalone/src/main.rs`
- Modify: `src/services/standalone/BUCK` (add the `loom` `rust_binary` target)

**Interfaces:**
- Consumes: `standalone::{run, StandaloneAddrs}`, `managed_postgres_embed::extract_pg`, `service_runtime::{Config, env_map, migrate_requested, run_migrations}`.
- Produces: the `loom` binary.

- [ ] **Step 1: Write `src/services/standalone/src/main.rs`**

```rust
//! `loom`: the single self-contained binary. Self-extracts the embedded Postgres
//! (when embedded and no external bin dir is set), then runs the composite with a
//! SIGINT/SIGTERM-driven graceful shutdown.
use std::collections::HashMap;

use standalone::StandaloneAddrs;

type BoxErr = Box<dyn std::error::Error + Send + Sync>;

const DEFAULT_QUERY_API_ADDR: &str = "0.0.0.0:8080";
const DEFAULT_INGEST_ADDR: &str = "0.0.0.0:8081";

#[tokio::main]
async fn main() -> Result<(), BoxErr> {
    service_runtime::init_tracing();
    let mut env = service_runtime::env_map();

    // Migrate-and-exit works for the loom image too (chart one-shot migrator).
    // This path targets an EXTERNAL/managed PG (it connects to `cfg.db`), so it runs
    // before any embedded `extract_pg` and does not set `LOOM_PG_MODE=embedded`.
    if service_runtime::migrate_requested() {
        let cfg = service_runtime::Config::from_map(&env)?;
        service_runtime::run_migrations(&cfg.db).await?;
        return Ok(());
    }

    // Embedded + no external bin dir: self-extract the baked-in PG and inject it.
    let embedded = env.get("LOOM_PG_MODE").map(String::as_str) == Some("embedded");
    if embedded && !env.contains_key("LOOM_PG_BIN_DIR") {
        let data_path = env
            .get("LOOM_DATA_PATH")
            .ok_or_else(|| -> BoxErr { "LOOM_DATA_PATH is required in embedded mode".into() })?;
        let cache_root = std::path::Path::new(data_path).join("cache");
        std::fs::create_dir_all(&cache_root)?;
        let ex = managed_postgres_embed::extract_pg(&cache_root)?;
        env.insert("LOOM_PG_BIN_DIR".into(), ex.bin_dir.display().to_string());
        env.insert("LOOM_PG_LD_LIBRARY_PATH".into(), ex.lib_dir.display().to_string());
    }

    let cfg = service_runtime::Config::from_map(&env)?;
    let addrs = resolve_addrs(&env)?;

    standalone::run(cfg, addrs, shutdown_signal(), ready_noop()).await
}

fn resolve_addrs(env: &HashMap<String, String>) -> Result<StandaloneAddrs, BoxErr> {
    let parse = |key: &str, default: &str| -> Result<std::net::SocketAddr, BoxErr> {
        let raw = env.get(key).map_or(default, String::as_str);
        raw.parse().map_err(|e| -> BoxErr { format!("{key} `{raw}` invalid: {e}").into() })
    };
    let engine_socket = env
        .get("LOOM_ENGINE_SOCKET")
        .cloned()
        .ok_or_else(|| -> BoxErr { "LOOM_ENGINE_SOCKET is required".into() })?;
    Ok(StandaloneAddrs {
        query_api: parse("LOOM_QUERY_API_BIND_ADDR", DEFAULT_QUERY_API_ADDR)?,
        ingest: parse("LOOM_INGEST_BIND_ADDR", DEFAULT_INGEST_ADDR)?,
        engine_socket,
    })
}

/// The composite requires a `ready` sender; the binary does not consume readiness,
/// so it hands over a channel whose receiver it immediately drops.
fn ready_noop() -> tokio::sync::oneshot::Sender<()> {
    let (tx, _rx) = tokio::sync::oneshot::channel();
    tx
}

/// Resolve on SIGINT or SIGTERM (container runtimes send SIGTERM).
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(_) => return,
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
```

- [ ] **Step 2: Add the `loom` binary target to `src/services/standalone/BUCK`**

```python
rust_binary(
    name = "loom",
    crate = "loom",
    srcs = ["src/main.rs"],
    crate_root = "src/main.rs",
    edition = "2024",
    deps = [
        ":standalone",
        "//src/services/runtime:runtime",
        "//src/services/managed-postgres-embed:managed-postgres-embed",
        "//third-party:tokio",
    ],
    visibility = ["PUBLIC"],
)
```

> Confirm the `managed-postgres-embed` target name via `buck2 targets //src/services/managed-postgres-embed/...`.

- [ ] **Step 3: Build the binary**

Run: `buck2 build //src/services/standalone:loom > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[|FAILED" /tmp/b.log`
Expected: BUILD SUCCEEDED.

- [ ] **Step 4: Lint + commit**

Run: `buck2 build '//src/services/standalone:loom[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty == clean).

```bash
git add src/services/standalone/src/main.rs src/services/standalone/BUCK
git commit -m "feat(standalone): loom binary with extract_pg wiring and SIGTERM shutdown"
```

---

### Task 7: Document the `loom` binary

**Files:**
- Modify: `docs/deploy.md`

- [ ] **Step 1: Add a "Single-binary (`loom`)" section to `docs/deploy.md`**

Document: what `loom` is (one process, embedded PG, all three services); the env it reads (`LOOM_PG_MODE=embedded`, `LOOM_DATA_PATH`, `LOOM_ENGINE_SOCKET`, `LOOM_QUERY_API_BIND_ADDR` default `0.0.0.0:8080`, `LOOM_INGEST_BIND_ADDR` default `0.0.0.0:8081`, optional `LOOM_FLIGHT_BIND_ADDR`, optional `LOOM_BOOTSTRAP_ADMIN_*`); that in embedded mode `LOOM_PG_BIN_DIR` is optional (self-extracted); and that it honours `LOOM_MIGRATE=apply`. Keep it consistent with the existing deploy prose. End the file with exactly one trailing newline; no trailing whitespace (the `lint` job checks markdown).

- [ ] **Step 2: Verify markdown hooks + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; grep -E "Passed|Failed" /tmp/p.log` and commit any hook fixes.

```bash
git add docs/deploy.md
git commit -m "docs(deploy): document the single-binary loom composite"
```

> **Register close** (`- [ ] road-embedded-postgres-standalone` → `- [x]`, add `pr:#N`) is done in the finishing step via `loom-docs-update`, not here.

---

## Self-Review

**Spec coverage:**
- New `src/services/standalone/` crate + `loom` binary — Task 5 (lib) + Task 6 (binary). ✓
- Library serve seam per service, called by lean main + composite — Tasks 2, 3, 4. ✓
- `extract_pg` first runtime consumer; `LOOM_PG_BIN_DIR` optional in embedded — Task 6 Step 1 (embedded branch injects into the env map before `from_map`). ✓
- Explicit engine-readiness gate (bind-first + oneshot) — Task 5 Step 5 (`eng_ready_rx.await` before binding/serving HTTP). ✓
- Two HTTP ports (8080/8081) — Task 6 `resolve_addrs` defaults + Global Constraints. ✓
- One SIGINT/SIGTERM handler → fan-out → PG stopped last — Task 6 `shutdown_signal` + Task 5 watch fan-out + `pg_handle.shutdown()` after `join!`. ✓
- Fixture test: boot embedded → **authed ingest POST `main.widget` → define type → authed query-api GET `/objects/Widget` reading the landed rows back** → SIGTERM → clean stop (`PG_VERSION` survives, no `postmaster.pid` orphan) — Task 5 Step 1. This is the spec's mandated composition round-trip (not health probes), and it only passes if `LOOM_DB_HOST` points at the real embedded socket dir `<LOOM_DATA_PATH>/pgrun`. ✓
- Migrate-and-exit for the loom image — Task 6 Step 1 (external/managed PG path). ✓
- Behaviour-preserving lean binaries — Tasks 2–4 keep existing tests green. ✓

**Placeholder scan:** No "TBD"/"handle edge cases". The `> Implementer note` blocks flag genuine verify-against-source points (the ingest token's authZ; the `define_widget` table ref; the `reqwest` client feature) with exact fixes, not deferred work.

**Type consistency:** `run(listener, &cfg, pool, ready, shutdown)` (engine), `serve(&cfg, pool, cp, auth, admin_subject, max_ttl, listener, shutdown)` (ingest), `serve(&cfg, pg, auth, engine_socket, listener, shutdown)` (query-api — **no `pool` param**), `run(cfg, addrs, shutdown, ready)` (composite), `serve_with_shutdown(listener, router, shutdown)` (runtime) — every call site in the thin mains and the composite matches these. The earlier `qapi_pg.pool_ref()` mismatch is resolved by dropping query-api's dead `pool` param entirely.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-01-embedded-postgres-standalone.md`.
