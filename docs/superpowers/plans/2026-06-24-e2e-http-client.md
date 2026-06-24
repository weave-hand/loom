# Real-HTTP e2e smoke (over-the-wire, both engines) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add loom's first over-the-wire e2e test: boot the ingest + query-api routers on real ephemeral TCP ports via `service_runtime::serve`, drive them with a `reqwest` client, and prove the land→read vertical (plus a deny and a malformed case) holds over **both** storage backends (DuckLake and Iceberg).

**Architecture:** A generic `spawn_http` harness in the `e2e-support` library spawns any `axum::Router` on a real socket and returns its base URL. A new fixture test file `http_wire_e2e.rs` assembles each backend's two routers exactly as the binaries' `main.rs` do (cribbed faithfully), spawns them, and runs one parametrized `reqwest`-driven vertical per backend. The existing in-process `oneshot` suite stays as behavioral coverage; this is a smoke of the wire.

**Tech Stack:** Rust (edition 2024), buck2 + reindeer (third-party deps), axum, `reqwest` 0.12 (async, rustls), tokio, Apache Iceberg / DataFusion, DuckLake/DuckDB, hermetic Postgres fixture (`loom_fixture_test`).

## Global Constraints

- **Tests are `rust_test` integration targets only — never inline `#[cfg(test)]`/`#[test]` modules.** The `no-inline-tests` prek hook fails on any `#[test]`/`#[tokio::test]` in a first-party `src/**.rs` file. All new tests live in `tests/<name>.rs` wired as their own buck target.
- **Fixture tests (those that boot Postgres/DuckDB) MUST use the `loom_fixture_test` macro, not a bare `rust_test`** — a bare target routes to remote execution and fails as root. Pure-socket tests (no Postgres/DuckDB) may stay a bare `rust_test`.
- **`reqwest` MUST be exposed without `default` features.** It is currently resolved at 0.12.28 with a rustls-only feature set (`json`, `rustls-tls`, `blocking`, the `__rustls*`/`__tls` internals — but **not** `default`/`default-tls`). The Cargo.toml edge MUST use `default-features = false, features = ["json", "rustls-tls"]` so the feature union is unchanged and no native-tls/openssl edge is pulled in.
- **reindeer pin guard (CLAUDE.md):** after `./tools/buckify.sh`, the `duckdb` lock pin MUST stay at `1.10503.1`. Verify against the merge-base; if it moved, run `cargo update -p duckdb --precise 1.10503.1` (hermetic cargo via `eval "$(./tools/env.sh)"`) and re-buckify. A green per-crate build is **not** enough — run the **full** `buck2 test //src/...`.
- **buck2 test output:** never pipe `buck2 test` through `tail`/`head` (it stalls on an unconsumed pipe). Redirect to a file and grep it: `buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **Markdown lint:** any `.md` file edited ends with exactly one trailing newline and no trailing whitespace (`end-of-file-fixer` + `trim trailing whitespace` hooks run on all events).
- **Conventional Commits:** commit subjects must match Conventional Commits (`feat:`/`test:`/`chore:` …) — the commit-msg hook enforces it locally.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `src/services/query-api/Cargo.toml` | Add `reqwest` as a `[dev-dependencies]` edge so reindeer emits the public alias. |
| `third-party/BUCK` | **Generated** by `./tools/buckify.sh` — gains `alias(name="reqwest", actual=":reqwest-0.12", visibility=["PUBLIC"])`. Do not hand-edit. |
| `Cargo.lock` | **Generated** — gains the direct `reqwest` edge for query-api. |
| `src/services/query-api/tests/e2e_support.rs` | Gains the generic `spawn_http` harness + `ServeGuard` (reusable wire infra). |
| `src/services/query-api/tests/wire_harness_smoke.rs` | **New.** Pure-socket unit test for `spawn_http` (no Postgres) — bare `rust_test`. |
| `src/services/query-api/tests/http_wire_e2e.rs` | **New.** The backend-context builders (DuckLake + Iceberg, cribbed from `main.rs`) and the two parametrized wire verticals — `loom_fixture_test(duckdb=True)`. |
| `src/services/query-api/BUCK` | Add `tokio` + `runtime` to `e2e-support` deps; add the `wire-harness-smoke` and `http-wire-e2e` targets. |
| `docs/FUTURE.md` / `docs/ROADMAP.md` | Close `road-e2e-http-client`; mark `fut-socket-roundtrip-test` promoted; record the residual `fut-binary-subprocess-smoke`. (Done via `loom-docs-update` at PR time — Task 5.) |

## Reference snippets (verified against the codebase)

These are the exact upstream shapes the new code mirrors. Read them once; the tasks reference them.

- **`service_runtime::serve`** (`src/services/runtime/src/lib.rs:171`): `pub async fn serve(bind_addr: SocketAddr, router: Router) -> Result<(), RuntimeError>` — binds a real `TcpListener` internally, then `axum::serve`.
- **ingest router** (`src/services/ingest/src/http.rs:31`): `AppState { materializer: Arc<dyn LandingMaterializer> }`; `router(state) -> Router` with `POST /datasets/:schema/:table`. Garbage body → `400` ("invalid arrow ipc stream"); success → `200` `{ "snapshot_id": <i64>, "dataset": "<schema>.<table>" }`.
- **query-api router** (`src/services/query-api/src/http.rs:32`): `AppState { cp: Arc<dyn ControlPlane>, serving: Arc<dyn ServingEngine>, action_engine: Arc<dyn ActionEngine> }`; `GET /objects/:type_name`. Authorized → `200` `{ "objects": [ { "id": "1", ... } ] }` (`Long` ids render as JSON **strings**); `QueryError::Forbidden` → `403`; `QueryError::UnknownType` → `404`.
- **DuckLake assembly** (`src/services/query-api/tests/e2e_support.rs:177` `setup`, `src/services/ingest/tests/runtime_land.rs`): `DuckLakeWriter::new(fx.socket_path(), &db)` + `.bootstrap()`; `LocalFileSystem::new_with_prefix(writer.data_path())`; `DuckLakeMaterializer { cp, store }`; `EmbeddedDuckDb::attach("dbname={db} host={sock} user=postgres", writer.data_path())`; `DuckLakeActionWriter::new(cp, store)`.
- **Iceberg assembly** (`src/services/query-api/tests/iceberg_action_e2e.rs`, `src/services/ingest/tests/iceberg_land.rs`): build `SqlCatalog` via `SqlCatalogBuilder::default().with_storage_factory(Arc::new(LocalFsStorageFactory)).load("loom", props)` with props `{SQL_CATALOG_PROP_URI: fx.pg_dsn(&db), SQL_CATALOG_PROP_WAREHOUSE: format!("file://{}", warehouse.path().display())}`; `IcebergMaterializer { catalog, pool, inline_byte_limit, flush_byte_threshold }`; `IcebergActionWriter::new(catalog, pool, inline, flush)`; `DataFusionServingEngine::new(IcebergCatalog::new(pool))`. `define_type` does **not** require the table to pre-exist.
- **Module paths:** `ingest::http::{AppState, router}`, `ingest::landing::{DuckLakeMaterializer, IcebergMaterializer}`, `query_api::http::{AppState, router}`, `query_api::serving::{EmbeddedDuckDb, DuckLakeActionWriter}`, `query_api::serving_datafusion::{DataFusionServingEngine, IcebergActionWriter}`, `control_plane_postgres::iceberg_catalog::IcebergCatalog`, `control_plane_postgres::iceberg_sql_catalog::{SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder}`, `iceberg::CatalogBuilder`, `iceberg::io::LocalFsStorageFactory`.
- **e2e-support helpers reused:** `tref`, `prop`, `subject_with_role(&PgControlPlane, name) -> (SubjectId, RoleId)`, `grant_read(&PgControlPlane, &RoleId, type_name)`, `ids_i64(&serde_json::Value) -> Vec<i64>` (parses `{"objects":[{"id":"…"}]}` ids).

---

### Task 1: Expose `reqwest` as `//third-party:reqwest`

**Files:**
- Modify: `src/services/query-api/Cargo.toml` (`[dev-dependencies]`)
- Generate: `third-party/BUCK`, `Cargo.lock` (via `./tools/buckify.sh`)

**Interfaces:**
- Produces: a `//third-party:reqwest` public buck alias usable from `rust_test`/`rust_library` `deps`, resolving to reqwest 0.12.28 with `json` + `rustls-tls`.

- [ ] **Step 1: Add the dev-dependency edge**

In `src/services/query-api/Cargo.toml`, under the existing `[dev-dependencies]` block (which has `tokio`, `tempfile`, `http-body-util`, `tower`), add:

```toml
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
```

- [ ] **Step 2: Sync the lock minimally (do NOT broadly re-resolve)**

```bash
eval "$(./tools/env.sh)"
cargo update -p reqwest --precise 0.12.28
git diff --stat Cargo.lock
```

Expected: `Cargo.lock` changes are confined to `reqwest` (and edges it already pulled — all already present transitively). **Confirm the `duckdb` package is untouched:**

```bash
git diff Cargo.lock | grep -E '^[-+].*"(duckdb|libduckdb-sys)"' || echo "duckdb pin unchanged"
```

Expected: `duckdb pin unchanged`. If duckdb moved, run `cargo update -p duckdb --precise 1.10503.1` and re-check.

- [ ] **Step 3: Regenerate third-party/BUCK**

```bash
./tools/buckify.sh
```

Expected: prints `buckify complete`.

- [ ] **Step 4: Verify the public alias appeared**

```bash
grep -n -A2 'name = "reqwest"' third-party/BUCK | grep -E 'name = "reqwest"|actual = ":reqwest|PUBLIC'
```

Expected: an `alias(name = "reqwest", actual = ":reqwest-0.12", visibility = ["PUBLIC"])` block (the alias `name = "reqwest"` line, an `actual = ":reqwest-0.12"` line, and a `PUBLIC` visibility line). If no public alias was emitted, STOP and investigate (reindeer only aliases workspace-member direct deps — confirm Step 1 landed in the right `Cargo.toml`).

- [ ] **Step 5: Verify duckdb pin held**

```bash
grep -A1 'name = "duckdb"' Cargo.lock | grep version
```

Expected: `version = "1.10503.1"`.

- [ ] **Step 6: Build the alias to prove it resolves**

```bash
buck2 build //third-party:reqwest > /tmp/reqwest_build.log 2>&1; tail -1 /tmp/reqwest_build.log
```

Expected: build succeeds (`BUILD SUCCEEDED`).

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/Cargo.toml third-party/BUCK Cargo.lock
git commit -m "chore(third-party): expose reqwest as //third-party:reqwest"
```

---

### Task 2: Add the `spawn_http` wire harness to `e2e-support`

**Files:**
- Modify: `src/services/query-api/tests/e2e_support.rs` (add `spawn_http` + `ServeGuard`)
- Create: `src/services/query-api/tests/wire_harness_smoke.rs`
- Modify: `src/services/query-api/BUCK` (e2e-support deps; new `wire-harness-smoke` target)

**Interfaces:**
- Produces:
  - `pub struct ServeGuard` — RAII guard; on drop aborts the serving task.
  - `pub async fn spawn_http(router: axum::Router) -> (String, ServeGuard)` — binds an ephemeral `127.0.0.1` port, spawns `service_runtime::serve` on it, polls until it accepts, returns `("http://127.0.0.1:<port>", guard)`.

- [ ] **Step 1: Write the failing harness smoke test**

Create `src/services/query-api/tests/wire_harness_smoke.rs`:

```rust
//! Pure-socket smoke for the `spawn_http` wire harness: spawn a trivial axum
//! router on a real ephemeral port via `service_runtime::serve`, hit it with a
//! real `reqwest` client, and assert the round-trip. No Postgres/DuckDB — this
//! is a bare `rust_test`, not a fixture test.

use axum::Router;
use axum::routing::get;
use e2e_support::spawn_http;

#[tokio::test(flavor = "multi_thread")]
async fn spawn_http_serves_over_a_real_socket() {
    let router = Router::new().route("/ping", get(|| async { "ok" }));
    let (base, _guard) = spawn_http(router).await;

    let resp = reqwest::get(format!("{base}/ping")).await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "ok");
}
```

- [ ] **Step 2: Wire the BUCK targets (so the test compiles against the new symbols)**

In `src/services/query-api/BUCK`, add `//third-party:tokio` and `//src/services/runtime:runtime` to the `e2e-support` `rust_library` `deps` list (alphabetical-ish, matching the file's style):

```starlark
        "//src/services/runtime:runtime",
        "//third-party:tokio",
```

Then add a new target near the other test targets:

```starlark
rust_test(
    name = "wire-harness-smoke",
    crate = "wire_harness_smoke",
    srcs = ["tests/wire_harness_smoke.rs"],
    crate_root = "tests/wire_harness_smoke.rs",
    edition = "2024",
    deps = [
        ":e2e-support",
        "//third-party:axum",
        "//third-party:reqwest",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails (symbol missing)**

```bash
buck2 test //src/services/query-api:wire-harness-smoke > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|error\[|cannot find" /tmp/t2.log | head
```

Expected: a **compile failure** — `cannot find function spawn_http in crate e2e_support` (the harness isn't implemented yet).

- [ ] **Step 4: Implement `spawn_http` + `ServeGuard` in `e2e_support.rs`**

Append to `src/services/query-api/tests/e2e_support.rs` (after the existing items):

```rust
/// A spawned in-process HTTP server bound to an ephemeral `127.0.0.1` port.
/// Holds the serving task; dropping the guard aborts the server, so the caller
/// must keep it alive for the duration of the test.
pub struct ServeGuard(tokio::task::JoinHandle<()>);

impl Drop for ServeGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Bind an ephemeral local port, spawn `service_runtime::serve` on it (the real
/// `TcpListener` bind that the binaries use), and wait until the listener
/// accepts connections before returning.
///
/// Returns the base URL (no trailing slash) and a guard that keeps the serving
/// task alive. A short probe-bind discovers a free port, which `serve` then
/// re-claims; the readiness poll below closes the (tiny) re-bind race.
pub async fn spawn_http(router: axum::Router) -> (String, ServeGuard) {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("probe-bind ephemeral port");
    let addr = probe.local_addr().expect("probe local_addr");
    drop(probe);

    let handle = tokio::spawn(async move {
        let _ = service_runtime::serve(addr, router).await;
    });

    // Readiness: poll-connect until the server accepts (or give up clearly).
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return (format!("http://{addr}"), ServeGuard(handle));
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("spawn_http: server never became ready at {addr}");
}
```

No new `use` lines are required — `axum`, `tokio`, and `service_runtime` are referenced by fully-qualified or already-imported paths (`axum::Router` is used in the existing `get` helper; `tokio`/`service_runtime` are referenced with full paths above).

- [ ] **Step 5: Run the test to verify it passes**

```bash
buck2 test //src/services/query-api:wire-harness-smoke > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log
```

Expected: `Tests finished: PASS 1. FAIL 0.`

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/tests/e2e_support.rs src/services/query-api/tests/wire_harness_smoke.rs src/services/query-api/BUCK
git commit -m "test(query-api): add spawn_http wire harness with a socket smoke test"
```

---

### Task 3: DuckLake wire vertical (land → read, deny, malformed)

**Files:**
- Create: `src/services/query-api/tests/http_wire_e2e.rs`
- Modify: `src/services/query-api/BUCK` (add the `http-wire-e2e` `loom_fixture_test`)

**Interfaces:**
- Consumes: `e2e_support::{spawn_http, subject_with_role, grant_read, ids_i64, tref, prop}`; `ingest::http`, `ingest::landing::DuckLakeMaterializer`; `query_api::http`, `query_api::serving::{EmbeddedDuckDb, DuckLakeActionWriter}`.
- Produces (within this file, reused by Task 4):
  - `struct WireBackend { ingest: axum::Router, query: axum::Router, cp: Arc<PgControlPlane>, _keep: Box<dyn Any + Send> }`
  - `async fn run_wire_vertical(backend: WireBackend)` — the parametrized assertion body.
  - `fn customer_ipc() -> Vec<u8>` — Arrow IPC for `customer(id, region)` rows `(1,'CA'),(2,'NY')`.

- [ ] **Step 1: Write the test file with the DuckLake arm only (failing — target not yet wired)**

Create `src/services/query-api/tests/http_wire_e2e.rs`:

```rust
//! Over-the-wire e2e smoke: boot the ingest + query-api routers on real
//! ephemeral TCP ports (via `service_runtime::serve`) and drive the land→read
//! vertical with a real `reqwest` client, over BOTH storage backends.
//!
//! This complements the in-process `oneshot` suite (behavioral breadth) with a
//! smoke of the network path: `serve()` + routers + body/JSON + status codes.
//! Backend assembly is cribbed faithfully from each binary's `main.rs`.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::{ControlPlane, ObjectType, Ontology, TypeName};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use e2e_support::{grant_read, ids_i64, prop, spawn_http, subject_with_role, tref};
use ingest::landing::DuckLakeMaterializer;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use query_api::serving::{DuckLakeActionWriter, EmbeddedDuckDb};

/// The two assembled routers for one backend, plus the control plane (for
/// not-under-test seeding) and a keep-alive for the backend's tempdirs/writers.
struct WireBackend {
    ingest: axum::Router,
    query: axum::Router,
    cp: Arc<PgControlPlane>,
    _keep: Box<dyn Any + Send>,
}

/// Arrow IPC stream for `customer(id Int64 non-null, region Utf8 nullable)`
/// rows `(1,'CA'),(2,'NY')`.
fn customer_ipc() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(StringArray::from(vec![Some("CA"), Some("NY")])),
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

/// Assemble the DuckLake-backed ingest + query routers over one fresh db +
/// bootstrapped DuckLake warehouse (mirrors the `main.rs` DuckLake arms).
async fn ducklake_backend(fx: &PgFixture) -> WireBackend {
    let (cp, db) = fx.fresh_db().await;
    let cp = Arc::new(cp);
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(writer.data_path()).unwrap());

    let ingest = ingest::http::router(ingest::http::AppState {
        materializer: Arc::new(DuckLakeMaterializer {
            cp: cp.clone() as Arc<dyn ControlPlane>,
            store: store.clone(),
        }),
    });

    let eng = EmbeddedDuckDb::attach(
        &format!(
            "dbname={} host={} user=postgres",
            db,
            fx.socket_path().display()
        ),
        writer.data_path(),
    )
    .await
    .unwrap();

    let query = query_api::http::router(query_api::http::AppState {
        cp: cp.clone() as Arc<dyn ControlPlane>,
        serving: Arc::new(eng),
        action_engine: Arc::new(DuckLakeActionWriter::new(
            cp.clone() as Arc<dyn ControlPlane>,
            store,
        )),
    });

    WireBackend {
        ingest,
        query,
        cp,
        _keep: Box::new(writer),
    }
}

/// Seed the not-under-test scaffolding (ontology type + ACL grant), spawn both
/// routers, and run the happy / deny / malformed assertions over `reqwest`.
async fn run_wire_vertical(backend: WireBackend) {
    // Ontology: bind type `Customer` -> table `main.customer`. (define_type does
    // not require the table to pre-exist; the land below creates it.)
    backend
        .cp
        .define_type(ObjectType {
            name: TypeName("Customer".into()),
            properties: vec![prop("id", "Long", true), prop("region", "String", false)],
            derived: vec![],
            table: tref("main", "customer"),
            identity: None,
        })
        .await
        .unwrap();
    // ACL: `reader` may read Customer; `intruder` is ungranted.
    let (_subj, role) = subject_with_role(&backend.cp, "reader").await;
    grant_read(&backend.cp, &role, "Customer").await;

    let (ingest_url, _ig) = spawn_http(backend.ingest).await;
    let (query_url, _qg) = spawn_http(backend.query).await;
    let client = reqwest::Client::new();

    // Happy path 1/2 — land over the wire.
    let resp = client
        .post(format!("{ingest_url}/datasets/main/customer"))
        .body(customer_ipc())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "land status");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["snapshot_id"].is_number(), "land body: {body}");

    // Happy path 2/2 — authorized typed read returns the landed rows.
    let resp = client
        .get(format!("{query_url}/objects/Customer"))
        .header("X-Loom-Subject", "reader")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "read status");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(ids_i64(&body), vec![1i64, 2], "read body: {body}");

    // Deny — ungranted subject -> 403.
    let resp = client
        .get(format!("{query_url}/objects/Customer"))
        .header("X-Loom-Subject", "intruder")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403, "deny status");

    // Malformed 1/2 — garbage Arrow body to ingest -> 400.
    let resp = client
        .post(format!("{ingest_url}/datasets/main/customer"))
        .body(vec![0u8, 1, 2, 3])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "garbage-arrow status");

    // Malformed 2/2 — unknown type to query-api -> 404.
    let resp = client
        .get(format!("{query_url}/objects/Nope"))
        .header("X-Loom-Subject", "reader")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "unknown-type status");
}

#[tokio::test(flavor = "multi_thread")]
async fn ducklake_wire_vertical() {
    let fx = PgFixture::start();
    let backend = ducklake_backend(&fx).await;
    run_wire_vertical(backend).await;
}
```

- [ ] **Step 2: Wire the `http-wire-e2e` BUCK target**

In `src/services/query-api/BUCK`, add near the other `loom_fixture_test` targets:

```starlark
loom_fixture_test(
    name = "http-wire-e2e",
    crate = "http_wire_e2e",
    srcs = ["tests/http_wire_e2e.rs"],
    crate_root = "tests/http_wire_e2e.rs",
    duckdb = True,
    deps = [
        ":query-api",
        ":e2e-support",
        "//src/services/ingest:ingest",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow",
        "//third-party:axum",
        "//third-party:iceberg",
        "//third-party:object_store",
        "//third-party:reqwest",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

(`iceberg`/`tempfile` are listed now so Task 4 needs no BUCK change; they are unused by the DuckLake-only file at this step, which is fine — buck deps may exceed current `use`s, and Task 4 adds the imports.)

- [ ] **Step 3: Run the DuckLake vertical and verify it passes**

```bash
buck2 test //src/services/query-api:http-wire-e2e > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t3.log | head
```

Expected: `Tests finished: PASS 1. FAIL 0.` (only `ducklake_wire_vertical` exists yet). If the read returns `404`/empty, re-check the type name casing (`Customer`) and that the land returned `200` before the read.

- [ ] **Step 4: Commit**

```bash
git add src/services/query-api/tests/http_wire_e2e.rs src/services/query-api/BUCK
git commit -m "test(query-api): add DuckLake over-the-wire land->read e2e smoke"
```

---

### Task 4: Iceberg wire vertical (same vertical, Iceberg backend)

**Files:**
- Modify: `src/services/query-api/tests/http_wire_e2e.rs` (add the Iceberg arm + a second test fn)

**Interfaces:**
- Consumes: `ingest::landing::IcebergMaterializer`; `query_api::serving_datafusion::{DataFusionServingEngine, IcebergActionWriter}`; `control_plane_postgres::iceberg_catalog::IcebergCatalog`; `control_plane_postgres::iceberg_sql_catalog::{SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder}`; `iceberg::CatalogBuilder`; `iceberg::io::LocalFsStorageFactory`. Reuses `WireBackend` + `run_wire_vertical` from Task 3.

- [ ] **Step 1: Add the Iceberg imports**

At the top of `src/services/query-api/tests/http_wire_e2e.rs`, add to the `use` block:

```rust
use std::collections::HashMap;

use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalogBuilder,
};
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use ingest::landing::IcebergMaterializer;
use query_api::serving_datafusion::{DataFusionServingEngine, IcebergActionWriter};
```

- [ ] **Step 2: Write the Iceberg backend builder + the second test fn**

Add to `http_wire_e2e.rs` (after `ducklake_backend`, and a new `#[tokio::test]` after `ducklake_wire_vertical`):

```rust
const INLINE_BYTE_LIMIT: usize = 16 * 1024 * 1024;

/// Assemble the Iceberg-backed ingest + query routers over one fresh db + a
/// `file://` warehouse (mirrors the `main.rs` Iceberg arms). `flush_byte_threshold
/// = i64::MAX` keeps the small land inline (no async flush job / worker needed);
/// the DataFusion serving engine reads the inline rows directly.
async fn iceberg_backend(fx: &PgFixture) -> WireBackend {
    let (cp, db) = fx.fresh_db().await;
    let cp = Arc::new(cp);
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let warehouse = tempfile::tempdir().expect("warehouse");

    let mut props = HashMap::new();
    props.insert(SQL_CATALOG_PROP_URI.to_string(), dsn);
    props.insert(
        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
        format!("file://{}", warehouse.path().display()),
    );
    let catalog = Arc::new(
        SqlCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load("loom", props)
            .await
            .expect("build SqlCatalog"),
    );

    let ingest = ingest::http::router(ingest::http::AppState {
        materializer: Arc::new(IcebergMaterializer {
            catalog: catalog.clone(),
            pool: pool.clone(),
            inline_byte_limit: INLINE_BYTE_LIMIT,
            flush_byte_threshold: i64::MAX,
        }),
    });

    let query = query_api::http::router(query_api::http::AppState {
        cp: cp.clone() as Arc<dyn ControlPlane>,
        serving: Arc::new(DataFusionServingEngine::new(IcebergCatalog::new(pool.clone()))),
        action_engine: Arc::new(IcebergActionWriter::new(
            catalog.clone(),
            pool,
            INLINE_BYTE_LIMIT,
            i64::MAX,
        )),
    });

    WireBackend {
        ingest,
        query,
        cp,
        _keep: Box::new(warehouse),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn iceberg_wire_vertical() {
    let fx = PgFixture::start();
    let backend = iceberg_backend(&fx).await;
    run_wire_vertical(backend).await;
}
```

- [ ] **Step 3: Run BOTH verticals and verify they pass**

```bash
buck2 test //src/services/query-api:http-wire-e2e > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t4.log | head
```

Expected: `Tests finished: PASS 2. FAIL 0.` (`ducklake_wire_vertical` + `iceberg_wire_vertical`). If the Iceberg read returns empty, confirm `flush_byte_threshold: i64::MAX` (inline) and that the same `pool`/warehouse is shared by both routers.

- [ ] **Step 4: Commit**

```bash
git add src/services/query-api/tests/http_wire_e2e.rs
git commit -m "test(query-api): add Iceberg over-the-wire land->read e2e smoke"
```

---

### Task 5: Full-suite reindeer guard + register update

**Files:**
- Modify: `docs/ROADMAP.md`, `docs/FUTURE.md` (via `loom-docs-update`)

**Interfaces:** none (verification + docs only).

- [ ] **Step 1: Run the FULL suite (reindeer guard — a green per-crate build is not enough)**

```bash
buck2 test //src/... > /tmp/full.log 2>&1; grep -E "Tests finished|FAIL" /tmp/full.log | tail
```

Expected: `Tests finished: PASS <n>. FAIL 0.` — in particular the `query-api`/`worker` DuckLake serving fixtures pass (no `DuckLake catalog version mismatch`), confirming Task 1 did not drift the `duckdb` pin.

- [ ] **Step 2: Run the lint hooks and commit any fixups**

```bash
buck2 run //tools:prek -- run --all-files > /tmp/lint.log 2>&1; grep -E "Passed|Failed|files were modified" /tmp/lint.log | tail
git diff --quiet || git commit -am "chore: apply prek hook fixups"
```

Expected: all hooks pass (rustfmt, clippy, file checks, reindeer-in-sync). If hooks modified files, commit them.

- [ ] **Step 3: Update the documentation registers**

Invoke the `loom-docs-update` skill. It should:
- Close `road-e2e-http-client` in `docs/ROADMAP.md`: flip `- [ ]` → `- [x]`, set `status:done`, set `pr:#<N>` once the PR number is known.
- Mark the promoted source in `docs/FUTURE.md`: `fut-socket-roundtrip-test` → `status:promoted` (it became `road-e2e-http-client`), if not already.
- Record the residual deferral from the spec's "Out" as a new `docs/FUTURE.md` item, e.g. `fut-binary-subprocess-smoke` (area `test`, `status:deferred`): "Boot the built `ingest-bin`/`query-api-bin` as a subprocess to cover `main.rs` + `Config::from_env` over a spawned process — the in-process `spawn_http` covers `serve()` + routers; subprocess smoke remains deferred."

Validate: `bash tools/docs.sh validate`.

- [ ] **Step 4: Commit the register changes**

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(registers): close road-e2e-http-client; record subprocess-smoke deferral"
```

(The `pr:#N` backfill may land in a follow-up commit once the PR number exists.)

---

## Self-Review

**1. Spec coverage:**
- Expose `reqwest` (`//third-party:reqwest`, dev-dep, buckify, duckdb-pin guard, full suite) → Task 1 + Task 5 Step 1. ✓
- `spawn_http` harness in `e2e-support` (bind `127.0.0.1:0`, spawn `service_runtime::serve`, await readiness, liveness guard) → Task 2. ✓ (uses the probe-port + poll-readiness form to genuinely exercise `service_runtime::serve`, the spec's headline-uncovered path; the readiness poll closes the re-bind race the spec flags.)
- One `loom_fixture_test(duckdb=True)` parametrized over both backends, seeding ontology + ACL, building ingest router (matching materializer) + query router (matching serving + action engine) over the same Postgres + warehouse, driving happy / deny / malformed → Tasks 3 + 4. ✓
- Backend seams cribbed from `main.rs` (not re-invented) → `ducklake_backend`/`iceberg_backend` mirror the `main.rs` arms. ✓
- Residual deferrals recorded (binary-subprocess smoke; no oneshot migration; no exhaustive per-endpoint) → Task 5 Step 3. ✓
- Named risks: reindeer pin drift → Task 1 Steps 2/5 + Task 5 Step 1; doubled backend wiring (faithful crib) → reference snippets + Tasks 3/4; readiness race → `spawn_http` poll loop; shared warehouse/Postgres → single `db`/`pool`/warehouse threaded into both routers per arm; binding/landing ordering → define_type-then-land-then-read sequence (justified by `iceberg_action_e2e`). ✓

**Deliberate deviation (documented):** the spec lists Iceberg-side construction helpers as e2e-support additions. This plan keeps the backend builders **local to `http_wire_e2e.rs`** and exports only the genuinely-generic `spawn_http` from `e2e-support`. Rationale: (a) the builders have exactly one consumer (this file) → YAGNI; (b) isolating the heavier Iceberg/DataFusion wiring to one test file avoids increasing the blast radius of the shared lib that 8+ e2e targets link; (c) the spec's preference for shared helpers is explicitly conditional ("if that reduces duplication"). Spec intent — prove real `main.rs` wiring, not a test-only assembly — is still met by cribbing faithfully. A future second wire test can promote the builders.

**2. Placeholder scan:** No TBD/TODO/"add error handling"/"similar to" — every code step shows complete code; every command shows expected output. ✓

**3. Type consistency:** `WireBackend`/`run_wire_vertical`/`customer_ipc`/`ducklake_backend`/`iceberg_backend` are used with identical signatures across Tasks 3–4. `spawn_http(axum::Router) -> (String, ServeGuard)` is defined in Task 2 and called identically in Tasks 2–3. Module paths (`ingest::landing::*`, `query_api::serving::*`, `query_api::serving_datafusion::*`, `control_plane_postgres::iceberg_*`) match the verified reference snippets. `ids_i64(&serde_json::Value) -> Vec<i64>` reused as-is. ✓
