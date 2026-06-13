# Shared Service Runtime + Binaries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A shared `service_runtime` crate (env config + Postgres pool + control-plane/store assembly + a `serve` helper) that un-stubs both the ingest and query-api binaries.

**Architecture:** `service_runtime` owns `Config`/`DbConfig` (from env), `build_pool`, `control_plane`, `local_store`, and `serve(bind_addr, router)`. `EmbeddedDuckDb::attach` is generalized from a socket path to a libpq connection string so query-api can attach to a real Postgres. Each service gets a thin `main.rs` composing its router from the runtime. `DbConfig` supports both TCP and unix-socket Postgres (host starting with `/`), which lets the fixture (socket-only) drive a real end-to-end land test.

**Tech Stack:** Rust (edition 2024), axum 0.7, sqlx (Postgres), object_store, tokio, buck2.

**Spec:** `docs/superpowers/specs/2026-06-13-service-runtime-and-binaries-design.md`

**Conventions (do not violate):**
- Tests are `rust_test`/`loom_fixture_test` integration targets in `tests/<name>.rs` — NEVER inline `#[cfg(test)]`. Fixture tests that boot Postgres/DuckDB MUST use `loom_fixture_test` (loaded from `//src/control-plane/postgres:defs.bzl`), not bare `rust_test`.
- Run the suite with plain `buck2 test //src/...`. Do NOT pipe `buck2 test` through `tail` — redirect to a file and grep: `buck2 test //target > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`.
- rustfmt is CHECK-ONLY: run `buck2 run //tools:rustfmt -- <changed .rs files>` and apply before committing any `.rs`.
- NEVER `--no-verify`. Conventional Commits ending with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Do NOT switch branches. Confirm `git branch --show-current` is `feat/service-runtime-binaries` before and after each task.
- All `//third-party:*` aliases used here already exist — no `Cargo.toml`/`Cargo.lock` change. If a build error claims an alias is missing, STOP and report.

---

## File Structure

**Task 1 — the `service_runtime` crate:**
- Create `src/services/runtime/BUCK`, `src/services/runtime/src/lib.rs`, `src/services/runtime/tests/config.rs`.

**Task 2 — generalize `EmbeddedDuckDb::attach`:**
- Modify `src/services/query-api/src/serving.rs` and the 5 call sites in `query-api/tests/{governed_read,bind_read_e2e,serving_engine,serving_types,quack_serving}.rs`.

**Task 3 — ingest binary + real-wiring land test:**
- Create `src/services/ingest/src/main.rs`, `src/services/ingest/tests/runtime_land.rs`; modify `src/services/ingest/BUCK`.

**Task 4 — query-api binary (un-stub):**
- Modify `src/services/query-api/src/main.rs` and `src/services/query-api/BUCK`.

---

## Task 1: The `service_runtime` crate

**Files:**
- Create: `src/services/runtime/src/lib.rs`
- Create: `src/services/runtime/tests/config.rs`
- Create: `src/services/runtime/BUCK`

- [ ] **Step 1: Write the failing config test**

Create `src/services/runtime/tests/config.rs`:

```rust
use std::collections::HashMap;
use std::time::Duration;

use service_runtime::{Config, ConfigError, DbConfig};

fn full() -> HashMap<String, String> {
    [
        ("LOOM_BIND_ADDR", "0.0.0.0:8080"),
        ("LOOM_DB_HOST", "db.internal"),
        ("LOOM_DB_PORT", "5432"),
        ("LOOM_DB_USER", "loom"),
        ("LOOM_DB_PASSWORD", "secret"),
        ("LOOM_DB_NAME", "loom"),
        ("LOOM_DATA_PATH", "/var/loom/data"),
        ("LOOM_LOCK_TIMEOUT_MS", "750"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

#[test]
fn parses_a_full_config() {
    let cfg = Config::from_map(&full()).expect("parse");
    assert_eq!(cfg.bind_addr, "0.0.0.0:8080".parse().unwrap());
    assert_eq!(cfg.db.host, "db.internal");
    assert_eq!(cfg.db.port, 5432);
    assert_eq!(cfg.db.user, "loom");
    assert_eq!(cfg.db.password, "secret");
    assert_eq!(cfg.db.dbname, "loom");
    assert_eq!(cfg.data_path, std::path::PathBuf::from("/var/loom/data"));
    assert_eq!(cfg.lock_timeout, Duration::from_millis(750));
}

#[test]
fn lock_timeout_defaults_to_5000ms() {
    let mut v = full();
    v.remove("LOOM_LOCK_TIMEOUT_MS");
    assert_eq!(Config::from_map(&v).unwrap().lock_timeout, Duration::from_millis(5000));
}

#[test]
fn missing_required_var_errors() {
    let mut v = full();
    v.remove("LOOM_DB_HOST");
    assert!(matches!(Config::from_map(&v), Err(ConfigError::MissingVar(k)) if k == "LOOM_DB_HOST"));
}

#[test]
fn invalid_values_error() {
    let mut bad_addr = full();
    bad_addr.insert("LOOM_BIND_ADDR".into(), "not-an-addr".into());
    assert!(matches!(Config::from_map(&bad_addr), Err(ConfigError::Invalid { .. })));

    let mut bad_port = full();
    bad_port.insert("LOOM_DB_PORT".into(), "99999999".into());
    assert!(matches!(Config::from_map(&bad_port), Err(ConfigError::Invalid { .. })));
}

#[test]
fn ducklake_libpq_renders_all_fields() {
    let db = DbConfig {
        host: "db.internal".into(),
        port: 5432,
        user: "loom".into(),
        password: "secret".into(),
        dbname: "loom".into(),
    };
    assert_eq!(
        db.ducklake_libpq(),
        "dbname=loom host=db.internal port=5432 user=loom password=secret"
    );
}
```

- [ ] **Step 2: Create the crate `BUCK`**

Create `src/services/runtime/BUCK`:

```python
rust_library(
    name = "runtime",
    crate = "service_runtime",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:axum",
        "//third-party:object_store",
        "//third-party:sqlx",
        "//third-party:thiserror",
        "//third-party:tokio",
    ],
    visibility = ["PUBLIC"],
)

rust_test(
    name = "config",
    crate = "config",
    srcs = ["tests/config.rs"],
    crate_root = "tests/config.rs",
    edition = "2024",
    deps = [":runtime"],
)
```

- [ ] **Step 3: Run the test — verify it FAILS to compile**

Run: `buck2 test //src/services/runtime:config > /tmp/t.log 2>&1; grep -E "error|unresolved|Tests finished|FAIL" /tmp/t.log`
Expected: failure — `service_runtime` crate / its items do not exist yet.

- [ ] **Step 4: Implement the crate**

Create `src/services/runtime/src/lib.rs`:

```rust
//! Shared service runtime: parse config from the environment, build a Postgres-backed
//! control plane + object store, and serve an axum router. Used by the ingest and
//! query-api binaries so each `main` stays thin.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use axum::Router;
use control_plane_postgres::PgControlPlane;
use object_store::local::LocalFileSystem;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

/// Discrete Postgres connection fields. Feeds both the sqlx control-plane pool and
/// DuckLake's ATTACH connection string, with no URL parsing in between.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
}

impl DbConfig {
    /// sqlx connect options. A `host` beginning with `/` is a unix-socket directory
    /// (libpq convention); otherwise a TCP host:port.
    pub fn pg_connect_options(&self) -> PgConnectOptions {
        let base = if self.host.starts_with('/') {
            PgConnectOptions::new().socket(&self.host)
        } else {
            PgConnectOptions::new().host(&self.host).port(self.port)
        };
        base.username(&self.user)
            .password(&self.password)
            .database(&self.dbname)
    }

    /// libpq-style connection string for `ATTACH 'ducklake:postgres:<...>'`.
    pub fn ducklake_libpq(&self) -> String {
        format!(
            "dbname={} host={} port={} user={} password={}",
            self.dbname, self.host, self.port, self.user, self.password
        )
    }
}

/// Fully-resolved service configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub db: DbConfig,
    pub data_path: PathBuf,
    pub lock_timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing required environment variable: {0}")]
    MissingVar(String),
    #[error("invalid value for {var}: {detail}")]
    Invalid { var: String, detail: String },
}

impl Config {
    /// Parse from a key->value map. `from_env` wraps this with `std::env::vars()`.
    pub fn from_map(vars: &HashMap<String, String>) -> Result<Config, ConfigError> {
        let req = |k: &str| {
            vars.get(k)
                .cloned()
                .ok_or_else(|| ConfigError::MissingVar(k.to_string()))
        };
        let invalid = |var: &str, detail: String| ConfigError::Invalid {
            var: var.to_string(),
            detail,
        };

        let bind_addr = req("LOOM_BIND_ADDR")?
            .parse()
            .map_err(|e: std::net::AddrParseError| invalid("LOOM_BIND_ADDR", e.to_string()))?;
        let port = req("LOOM_DB_PORT")?
            .parse::<u16>()
            .map_err(|e| invalid("LOOM_DB_PORT", e.to_string()))?;
        let lock_timeout = match vars.get("LOOM_LOCK_TIMEOUT_MS") {
            Some(s) => Duration::from_millis(
                s.parse::<u64>()
                    .map_err(|e| invalid("LOOM_LOCK_TIMEOUT_MS", e.to_string()))?,
            ),
            None => Duration::from_millis(5000),
        };

        Ok(Config {
            bind_addr,
            db: DbConfig {
                host: req("LOOM_DB_HOST")?,
                port,
                user: req("LOOM_DB_USER")?,
                password: req("LOOM_DB_PASSWORD")?,
                dbname: req("LOOM_DB_NAME")?,
            },
            data_path: PathBuf::from(req("LOOM_DATA_PATH")?),
            lock_timeout,
        })
    }

    /// Read the config keys from the process environment.
    pub fn from_env() -> Result<Config, ConfigError> {
        let vars: HashMap<String, String> = std::env::vars().collect();
        Self::from_map(&vars)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("connect pool: {0}")]
    Pool(sqlx::Error),
    #[error("object store: {0}")]
    Store(object_store::Error),
    #[error("bind {0}")]
    Bind(std::io::Error),
    #[error("serve: {0}")]
    Serve(std::io::Error),
}

/// Connect a control-plane pool from the DB config.
pub async fn build_pool(db: &DbConfig) -> Result<PgPool, RuntimeError> {
    PgPoolOptions::new()
        .connect_with(db.pg_connect_options())
        .await
        .map_err(RuntimeError::Pool)
}

/// Wrap a pool as a `PgControlPlane`.
pub fn control_plane(pool: PgPool, lock_timeout: Duration) -> PgControlPlane {
    PgControlPlane::new(pool, lock_timeout)
}

/// A `LocalFileSystem` object store rooted at `data_path`.
pub fn local_store(data_path: &Path) -> Result<LocalFileSystem, RuntimeError> {
    LocalFileSystem::new_with_prefix(data_path).map_err(RuntimeError::Store)
}

/// Bind `bind_addr` and serve `router` until the process is terminated.
pub async fn serve(bind_addr: SocketAddr, router: Router) -> Result<(), RuntimeError> {
    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(RuntimeError::Bind)?;
    axum::serve(listener, router).await.map_err(RuntimeError::Serve)
}
```

- [ ] **Step 5: Run the test — verify it PASSES**

Run: `buck2 test //src/services/runtime:config > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.` (one target; 5 test fns pass — see the "running N tests" line in the full log).

If `axum::serve`, `PgConnectOptions::socket`, or `connect_with` fails to resolve, STOP and report the exact error (do not change deps beyond the existing aliases).

- [ ] **Step 6: Format, lint, build, commit**

```bash
buck2 run //tools:rustfmt -- src/services/runtime/src/lib.rs src/services/runtime/tests/config.rs
tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -3 /tmp/clippy.log
buck2 build //src/... > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAILED" /tmp/b.log
git add -A && git commit -m "$(cat <<'EOF'
feat(runtime): shared service_runtime crate (config + pool + serve)

A service_runtime crate both binaries will share: Config/DbConfig parsed from the
environment (DbConfig handles TCP and unix-socket Postgres), build_pool,
control_plane, local_store, and a serve helper over axum::serve. Pure from_map
parser unit-tested.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```
Expected: `BUILD SUCCEEDED`, clippy clean, commit created.

---

## Task 2: Generalize `EmbeddedDuckDb::attach`

**Files:**
- Modify: `src/services/query-api/src/serving.rs`
- Modify: `src/services/query-api/tests/{governed_read,bind_read_e2e,serving_engine,serving_types,quack_serving}.rs`

- [ ] **Step 1: Change the `attach` signature**

In `src/services/query-api/src/serving.rs`, replace the `attach` function. Old signature:
`pub async fn attach(socket: &std::path::Path, db: &str, data_path: &std::path::Path)`.
New:

```rust
    /// `pg_conn` is a libpq connection string (e.g. "dbname=loom host=/sock user=postgres"
    /// or "dbname=loom host=db.internal port=5432 user=loom password=secret").
    /// `data_path` must match the dir the writer used (relative file paths resolve under it).
    pub async fn attach(
        pg_conn: &str,
        data_path: &std::path::Path,
    ) -> Result<Self, ServingError> {
        let ext_dir = std::env::var("DUCKDB_EXTENSION_DIR")
            .map_err(|_| ServingError::Engine("DUCKDB_EXTENSION_DIR unset".into()))?;
        let attach_sql = format!(
            "SET extension_directory='{}';\nLOAD ducklake;\nLOAD postgres_scanner;\n\
             ATTACH 'ducklake:postgres:{}' AS lake \
             (DATA_PATH '{}/', DATA_INLINING_ROW_LIMIT 0);\nUSE lake;",
            ext_dir,
            pg_conn,
            data_path.display(),
        );
        Ok(Self { attach_sql })
    }
```

(Only the signature and the two interpolations into the `ATTACH` string change; the rest of `serving.rs` is untouched.)

- [ ] **Step 2: Update the 5 call sites**

Each currently reads `EmbeddedDuckDb::attach(fx.socket_path(), &db, writer.data_path())` (or `db`/`data_path` locals). Replace the first two args with a libpq string built from the socket path + db, preserving the existing user (`postgres`):

- `src/services/query-api/tests/governed_read.rs:168`
- `src/services/query-api/tests/bind_read_e2e.rs:119`
- `src/services/query-api/tests/serving_engine.rs:14`
- `src/services/query-api/tests/serving_types.rs:16`
- `src/services/query-api/tests/quack_serving.rs:121`

New form (adjust the `db`/`data_path` expressions to match each file's local names — `&db` vs `db`, `writer.data_path()` vs `data_path`):

```rust
    EmbeddedDuckDb::attach(
        &format!("dbname={} host={} user=postgres", db, fx.socket_path().display()),
        writer.data_path(),
    )
```

For `serving_engine.rs` / `serving_types.rs` / `quack_serving.rs`, use whatever the fixture handle is named in that file (e.g. `fx` vs `fixture`) and the local `db`/`data_path` bindings already present. Do not change any assertion.

- [ ] **Step 3: Run the affected query-api fixture tests — verify all PASS**

Run: `buck2 test //src/services/query-api:governed-read //src/services/query-api:bind-read-e2e //src/services/query-api:serving-engine //src/services/query-api:serving-types //src/services/query-api:quack-serving > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: all targets pass (behavior is identical — same dbname/host/user as before).

(Target names follow the `name = "..."` in `query-api/BUCK`; if a name differs, use the BUCK value.)

- [ ] **Step 4: Format, lint, build, commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/serving.rs src/services/query-api/tests/governed_read.rs src/services/query-api/tests/bind_read_e2e.rs src/services/query-api/tests/serving_engine.rs src/services/query-api/tests/serving_types.rs src/services/query-api/tests/quack_serving.rs
tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -3 /tmp/clippy.log
buck2 build //src/... > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAILED" /tmp/b.log
git add -A && git commit -m "$(cat <<'EOF'
refactor(query-api): EmbeddedDuckDb::attach takes a libpq connection string

Generalize attach from (socket, db, data_path) to (pg_conn, data_path) so it can
attach to a real TCP Postgres, not just a unix-socket fixture. The 5 fixture call
sites build the same dbname/host/user string they used before; behavior unchanged.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```
Expected: `BUILD SUCCEEDED`, clippy clean, commit created.

---

## Task 3: ingest binary + real-wiring land test

**Files:**
- Create: `src/services/ingest/tests/runtime_land.rs`
- Create: `src/services/ingest/src/main.rs`
- Modify: `src/services/ingest/BUCK`

- [ ] **Step 1: Write the real-wiring land fixture test**

Create `src/services/ingest/tests/runtime_land.rs`:

```rust
//! Real production-wiring land: build the ingest AppState via the service_runtime
//! helpers against a real (fixture) Postgres + bootstrapped DuckLake catalog, POST an
//! Arrow IPC stream through the router, and read the snapshot back from the catalog.
//! Exercises build_pool (over a unix socket), control_plane, and local_store.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{ControlPlane, TableRef};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use http_body_util::BodyExt;
use ingest::http::{AppState, router};
use service_runtime::DbConfig;
use tower::ServiceExt;

fn ipc_bytes() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

#[tokio::test(flavor = "multi_thread")]
async fn lands_through_real_runtime_wiring() {
    let fixture = PgFixture::start();
    // Creates + migrates a fresh db; we rebuild our own pool through the runtime below.
    let (_seed, db) = fixture.fresh_db().await;
    let writer = DuckLakeWriter::new(fixture.socket_path(), &db);
    writer.bootstrap().await;

    // Build the real AppState via service_runtime — a unix-socket DbConfig (host = the
    // fixture's socket dir), so build_pool exercises the production connect path.
    let db_cfg = DbConfig {
        host: fixture.socket_path().to_string_lossy().into_owned(),
        port: 5432, // ignored for a socket host
        user: "postgres".into(),
        password: String::new(),
        dbname: db.clone(),
    };
    let pool = service_runtime::build_pool(&db_cfg).await.expect("build pool");
    let cp: Arc<dyn ControlPlane> =
        Arc::new(service_runtime::control_plane(pool, Duration::from_millis(300)));
    let store = Arc::new(service_runtime::local_store(writer.data_path()).expect("store"));

    let res = router(AppState {
        cp: cp.clone(),
        store,
    })
    .oneshot(
        Request::builder()
            .method("POST")
            .uri("/datasets/main/customer")
            .body(Body::from(ipc_bytes()))
            .unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let snapshot_id = json["snapshot_id"].as_i64().expect("snapshot_id");

    let table = TableRef {
        schema: "main".into(),
        name: "customer".into(),
    };
    let snap = cp
        .catalog()
        .current_snapshot(&table)
        .await
        .expect("current snapshot after landing");
    assert_eq!(snap.id.0, snapshot_id, "returned id matches the real catalog");
}
```

- [ ] **Step 2: Add the `runtime-land` test target to ingest BUCK**

In `src/services/ingest/BUCK`, add the fixture test (mirror the `materialize`/`http-land` blocks; this one needs Postgres **and** DuckLake, so `loom_fixture_test(duckdb = True)`). Do NOT add the binary target yet — that comes after `main.rs` exists, so the full build never references a missing crate root:

```python
loom_fixture_test(
    name = "runtime-land",
    crate = "runtime_land",
    srcs = ["tests/runtime_land.rs"],
    crate_root = "tests/runtime_land.rs",
    duckdb = True,
    deps = [
        ":ingest",
        "//src/services/runtime:runtime",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow",
        "//third-party:axum",
        "//third-party:http-body-util",
        "//third-party:serde_json",
        "//third-party:tokio",
        "//third-party:tower",
    ],
)
```

- [ ] **Step 3: Run the land test — verify it PASSES**

Run: `buck2 test //src/services/ingest:runtime-land > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.` This is an integration test of the Task 1 runtime wiring (it depends only on `:runtime` + `:ingest`, not the binary), so it passes as soon as the wiring is correct. If it FAILS to connect, the likely cause is `DbConfig::pg_connect_options` not handling the socket-host branch — fix that in `service_runtime` (Task 1's code), not the test.

- [ ] **Step 4: Create the ingest binary**

Create `src/services/ingest/src/main.rs`:

```rust
//! ingest binary: build the landing AppState from the environment via service_runtime
//! and serve the HTTP API.

use std::sync::Arc;

use ingest::http::{AppState, router};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;
    let cp = Arc::new(service_runtime::control_plane(pool, cfg.lock_timeout));
    let store = Arc::new(service_runtime::local_store(&cfg.data_path)?);
    let app = router(AppState { cp, store });
    service_runtime::serve(cfg.bind_addr, app).await?;
    Ok(())
}
```

- [ ] **Step 5: Add the `ingest-bin` binary target to BUCK**

Now that `src/main.rs` exists, add the binary to `src/services/ingest/BUCK` (after the `ingest` lib). The lib's `glob(["src/**/*.rs"])` will list `src/main.rs` too, but it is inert there (not reachable via `mod` from `src/lib.rs`, so not compiled into the lib — same as query-api, which already coexists with its `main.rs`). No glob exclude needed:

```python
rust_binary(
    name = "ingest-bin",
    crate = "ingest_bin",
    srcs = ["src/main.rs"],
    crate_root = "src/main.rs",
    edition = "2024",
    deps = [
        ":ingest",
        "//src/services/runtime:runtime",
        "//third-party:tokio",
    ],
    visibility = ["PUBLIC"],
)
```

- [ ] **Step 6: Build the binary — verify it compiles**

Run: `buck2 build //src/services/ingest:ingest-bin > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAILED|error\[" /tmp/b.log`
Expected: `BUILD SUCCEEDED`.

- [ ] **Step 7: Format, lint, build, commit**

```bash
buck2 run //tools:rustfmt -- src/services/ingest/src/main.rs src/services/ingest/tests/runtime_land.rs
tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -3 /tmp/clippy.log
buck2 build //src/... > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAILED" /tmp/b.log
git add -A && git commit -m "$(cat <<'EOF'
feat(ingest): runnable binary wired on service_runtime

Add src/main.rs that builds the landing AppState from env config via
service_runtime and serves it. A loom_fixture_test lands an Arrow IPC POST
through the real production wiring (build_pool over the fixture socket,
PgControlPlane, local store) and reads the snapshot back from the catalog.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```
Expected: `BUILD SUCCEEDED`, clippy clean, commit created.

---

## Task 4: query-api binary (un-stub)

**Files:**
- Modify: `src/services/query-api/src/main.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Replace the stub `main.rs`**

Replace the contents of `src/services/query-api/src/main.rs`:

```rust
//! query-api binary: build the read AppState from env config via service_runtime —
//! a Postgres control plane + an embedded DuckDB serving engine attached to the same
//! DuckLake catalog — and serve the HTTP API.

use std::sync::Arc;

use control_plane_core::ControlPlane;
use query_api::http::{AppState, router};
use query_api::serving::EmbeddedDuckDb;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;
    let cp: Arc<dyn ControlPlane> =
        Arc::new(service_runtime::control_plane(pool, cfg.lock_timeout));
    let serving = Arc::new(EmbeddedDuckDb::attach(&cfg.db.ducklake_libpq(), &cfg.data_path).await?);
    let app = router(AppState { cp, serving });
    service_runtime::serve(cfg.bind_addr, app).await?;
    Ok(())
}
```

- [ ] **Step 2: Update the `query-api-bin` deps**

In `src/services/query-api/BUCK`, the `query-api-bin` `rust_binary` `deps` (currently `[":query-api", "//third-party:tokio"]`) become:

```python
    deps = [
        ":query-api",
        "//src/services/runtime:runtime",
        "//src/control-plane/core:core",
        "//third-party:tokio",
    ],
```

- [ ] **Step 3: Build — verify it compiles**

Run: `buck2 build //src/services/query-api:query-api-bin > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAILED|error\[" /tmp/b.log`
Expected: `BUILD SUCCEEDED`. (No new test — the `main` is glue; `attach`'s new signature is covered by the Task 2 fixture tests. If `EmbeddedDuckDb` or `AppState` is not reachable from the binary's deps, fix the deps minimally.)

- [ ] **Step 4: Format, lint, build, commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/main.rs
tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -3 /tmp/clippy.log
buck2 build //src/... > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAILED" /tmp/b.log
git add -A && git commit -m "$(cat <<'EOF'
feat(query-api): un-stub the binary, wired on service_runtime

Replace the stub main with real wiring: build the control plane + an embedded
DuckDB serving engine (attached to the DuckLake catalog via the libpq connection
string) from env config, and serve the read API.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```
Expected: `BUILD SUCCEEDED`, clippy clean, commit created.

---

## Final Verification (after all tasks)

- [ ] Confirm branch: `git branch --show-current` → `feat/service-runtime-binaries`.
- [ ] Full suite green (do NOT pipe to `tail`):

```bash
buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: all pass, 0 fail (new `config` + `runtime-land` targets included; the 5 updated query-api fixture tests still green).

- [ ] `tools/clippy-all.sh` clean; `buck2 run //tools:prek -- run --all-files` green.
- [ ] Both binaries build: `buck2 build //src/services/ingest:ingest-bin //src/services/query-api:query-api-bin`.
- [ ] Hand off via superpowers:finishing-a-development-branch (PR with `--base main`).
```
