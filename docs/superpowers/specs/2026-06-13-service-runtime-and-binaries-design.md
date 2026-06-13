# Design: shared service runtime + ingest & query-api binaries

> **Status:** approved design (2026-06-13). Slice 2 of the ingest service shell, widened
> to a **shared runtime crate** that un-stubs **both** service binaries (ingest and
> query-api). Turns the two services from libraries-with-stub-`main`s into runnable
> processes: parse config from the environment, build a real Postgres-backed control
> plane + object store (+ DuckDB serving for query-api), and serve their existing axum
> routers over a TCP socket.

## Goal

Both services today are libraries with a stub `main.rs` (query-api prints "serving-tier
wiring is a later spec"; ingest has no binary at all). This slice adds the **shared runtime
wiring** — environment config, Postgres pool construction, control-plane/store/serving
assembly, and a `serve` helper — in one `service_runtime` crate, then gives each service a
thin `main.rs` that composes its router from that runtime and serves it. After this slice,
`buck2 run //src/services/ingest:ingest-bin` (and the query-api equivalent) is a real
process that binds a port and serves the built endpoints against a real Postgres.

## Why a shared crate (not per-service wiring)

The wiring is genuinely shared: both binaries parse the same env config, build the same
Postgres pool + `PgControlPlane`, root the same `LocalFileSystem` object store, and serve an
axum router the same way. There are **two real consumers today**, so a shared `service_runtime`
crate is DRY for a concrete repeated concern — not speculative pluggability. Each service's
`main` stays thin and owns only its service-specific assembly (ingest: `cp` + `store`;
query-api: `cp` + a DuckDB `ServingEngine`).

## The `service_runtime` crate (`src/services/runtime`)

```rust
/// Discrete Postgres connection fields — feeds BOTH sqlx (the control-plane pool)
/// and DuckLake's ATTACH connection string, with no URL parsing in between.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
}

impl DbConfig {
    /// sqlx connection options for the control-plane pool.
    pub fn pg_connect_options(&self) -> sqlx::postgres::PgConnectOptions;
    /// libpq-style connection string for DuckLake's `ATTACH 'ducklake:postgres:<...>'`.
    /// e.g. "dbname=loom host=db.internal port=5432 user=loom password=secret".
    pub fn ducklake_libpq(&self) -> String;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub bind_addr: std::net::SocketAddr,
    pub db: DbConfig,
    pub data_path: std::path::PathBuf,
    pub lock_timeout: std::time::Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError { /* MissingVar(name), Invalid { var, detail } */ }

impl Config {
    /// Pure parse from a key->value map (testable without mutating process env).
    pub fn from_map(vars: &std::collections::HashMap<String, String>) -> Result<Config, ConfigError>;
    /// Read the same keys from `std::env`.
    pub fn from_env() -> Result<Config, ConfigError>;
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError { /* wraps sqlx, io, object_store */ }

/// Connect a control-plane pool from the DB config.
pub async fn build_pool(db: &DbConfig) -> Result<sqlx::PgPool, RuntimeError>;
/// Wrap a pool as a PgControlPlane.
pub fn control_plane(pool: sqlx::PgPool, lock_timeout: std::time::Duration)
    -> control_plane_postgres::PgControlPlane;
/// A LocalFileSystem object store rooted at `data_path`.
pub fn local_store(data_path: &std::path::Path)
    -> Result<object_store::local::LocalFileSystem, RuntimeError>;
/// Bind `bind_addr` and serve `router` (axum::serve over a TcpListener).
pub async fn serve(bind_addr: std::net::SocketAddr, router: axum::Router) -> Result<(), RuntimeError>;
```

### Environment keys (`from_env` / `from_map`)

| Key | → | Notes |
|---|---|---|
| `LOOM_BIND_ADDR` | `bind_addr` | e.g. `0.0.0.0:8080`; parse error → `Invalid` |
| `LOOM_DB_HOST` | `db.host` | required |
| `LOOM_DB_PORT` | `db.port` | required, `u16` |
| `LOOM_DB_USER` | `db.user` | required |
| `LOOM_DB_PASSWORD` | `db.password` | required (empty string allowed) |
| `LOOM_DB_NAME` | `db.dbname` | required |
| `LOOM_DATA_PATH` | `data_path` | required; the object-store root |
| `LOOM_LOCK_TIMEOUT_MS` | `lock_timeout` | optional, default `5000` |

Missing required key → `ConfigError::MissingVar`; unparseable value → `ConfigError::Invalid`.

## The one library change: generalize `EmbeddedDuckDb::attach`

Today: `attach(socket: &Path, db: &str, data_path: &Path)` builds
`ATTACH 'ducklake:postgres:dbname={db} host={socket} user=postgres' ...` — a unix-socket
shape with no port/password, unusable against a production TCP Postgres.

Change to: `attach(pg_conn: &str, data_path: &Path)`, where `pg_conn` is a libpq connection
string the caller supplies. The `ATTACH` becomes
`ATTACH 'ducklake:postgres:{pg_conn}' AS lake (DATA_PATH '{data_path}/', DATA_INLINING_ROW_LIMIT 0)`.

- **Production** (query-api binary): `cfg.db.ducklake_libpq()`.
- **The 5 existing test call sites** (`governed_read`, `bind_read_e2e`, `serving_engine`,
  `serving_types`, `quack_serving`) pass
  `&format!("dbname={db} host={} user=postgres", fx.socket_path().display())` — behavior
  identical to today (same dbname/host/user), just assembled by the caller. No test
  assertion changes.

This is the minimal seam that lets the same engine attach to either a unix-socket fixture or
a real TCP Postgres.

## The binaries

Both are thin; the shared runtime does the work.

**ingest** (`src/services/ingest/src/main.rs`, target `ingest-bin`):
```rust
let cfg = service_runtime::Config::from_env()?;
let pool = service_runtime::build_pool(&cfg.db).await?;
let cp = std::sync::Arc::new(service_runtime::control_plane(pool, cfg.lock_timeout));
let store = std::sync::Arc::new(service_runtime::local_store(&cfg.data_path)?);
let router = ingest::http::router(ingest::http::AppState { cp, store });
service_runtime::serve(cfg.bind_addr, router).await?;
```

**query-api** (`src/services/query-api/src/main.rs`, replacing the stub):
```rust
let cfg = service_runtime::Config::from_env()?;
let pool = service_runtime::build_pool(&cfg.db).await?;
let cp: Arc<dyn ControlPlane> = Arc::new(service_runtime::control_plane(pool, cfg.lock_timeout));
let serving = Arc::new(EmbeddedDuckDb::attach(&cfg.db.ducklake_libpq(), &cfg.data_path).await?);
let router = query_api::http::router(AppState { cp, serving });
service_runtime::serve(cfg.bind_addr, router).await?;
```

`main` returns `Result<(), Box<dyn std::error::Error>>` and is `#[tokio::main]`.

## Testing (no new third-party deps)

1. **`runtime` config parse** (`tests/config.rs`, plain `rust_test`, pure): `from_map` with a
   full key set → the expected `Config`; a missing required key → `MissingVar`; a bad
   `LOOM_BIND_ADDR` and a bad `LOOM_DB_PORT` → `Invalid`; `LOOM_LOCK_TIMEOUT_MS` absent →
   defaults to 5000ms. Also assert `DbConfig::ducklake_libpq()` renders the expected string.
2. **ingest real-wiring land** (`tests/runtime_land.rs`, `loom_fixture_test(duckdb=True)`):
   bootstrap a DuckLake catalog on a fixture Postgres (mirror `ducklake_interop`), build the
   pool from the fixture's connect options, assemble the **real** `AppState` via
   `service_runtime::control_plane(...)` + `service_runtime::local_store(tempdir)`, then
   `ingest::http::router(state).oneshot(POST Arrow)` → `200` + a snapshot id, and read it back
   through a fresh `PgControlPlane` on the same DB (`cp.catalog().current_snapshot`). Proves
   the production control-plane wiring lands data through the route. (Uses `oneshot`, not a
   socket — no HTTP-client dep.)
3. The `attach` generalization is covered by the **existing** governed-read / serving tests,
   updated to the new signature (they must still pass unchanged in behavior).

**Deliberate deferral — the socket round-trip.** `serve` (`axum::serve` over a real
`TcpListener`, driven by an HTTP client) is **not** integration-tested: it would pull in a
reqwest/hyper client dependency for ~2 lines of glue. `serve` compiles and is exercised by
both `main`s; a socket smoke test arrives when a client dep is justified (e.g. the Quack
slice). Flagged, not hidden.

## BUCK / deps

- New `//src/services/runtime:runtime` (`service_runtime`) `rust_library`, deps:
  `//src/control-plane/core`, `//src/control-plane/postgres`, `//third-party:{axum, tokio,
  sqlx, object_store, thiserror}`. Plus a `config` `rust_test` and a `runtime-land`
  `loom_fixture_test(duckdb=True)` (deps include `:runtime`, `ingest`, `arrow`,
  `http-body-util`, `tower`, `serde_json`, `tempfile`, postgres fixture).
- `ingest-bin` `rust_binary` (`src/main.rs`) deps `:ingest`, `:runtime`, `//third-party:tokio`.
- query-api `main.rs` updated; `query-api-bin` deps gain `//src/services/runtime:runtime` (and
  whatever it now references). The `query-api` lib already deps postgres + duckdb.
- All aliases (`axum`, `tokio`, `sqlx`, `object_store`, `thiserror`, `arrow`, `tower`,
  `http-body-util`, `serde_json`, `tempfile`) already exist in `third-party/BUCK` — **no
  `Cargo.toml`/lock change** unless a brand-new crate is introduced (none is).

## Scope / non-goals

- **In:** the `service_runtime` crate (Config + pool/cp/store/serve helpers); the `attach`
  generalization + its 5 call-site updates; the two thin `main.rs` binaries; the config unit
  test + the ingest real-wiring land fixture test.
- **Out:**
  - **S3 / remote object store** — `LocalFileSystem` only; S3 is a later store slice.
  - **Migrations on startup** — assume the DB is already migrated (`run_migrations` exists for
    ops/tests; the binary does not run it).
  - **The socket round-trip integration test** — deferred (no HTTP-client dep), above.
  - **The Quack endpoint** (slice 4), **DataFusion compute path** (slice 3), **write-path ACL**.
  - **Graceful shutdown / signal handling, TLS, structured logging/tracing** — the binary is a
    minimal `serve`; these are follow-ons.
  - **Connection-pool tuning** — a single sane default (`PgPoolOptions` defaults); no env knobs.

## Open risks

- **`attach` signature change touches 5 tests.** All in query-api; the new call form is a
  pure caller-side string assembly with identical semantics. Low risk, but the whole
  query-api fixture suite must stay green.
- **Config surface is discrete fields, not a `DATABASE_URL`.** Chosen so the same fields feed
  sqlx and DuckLake without lossy URL→libpq conversion. If a `DATABASE_URL` form is wanted
  later it's an additive `from_url` constructor.
- **Untested `serve` glue.** Mitigated by keeping it to `axum::serve(TcpListener::bind(addr)?,
  router)` — the smallest possible surface — and by both `main`s exercising it at runtime.
