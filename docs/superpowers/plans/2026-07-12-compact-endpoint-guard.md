# Guarded Operator Compact Endpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close `iss-compact-endpoint-unguarded`: make `POST /tables/{schema}/{table}/compact` admin-gated, guarded (stream/changelog/shadow tables refused), deduped (routed through `maybe_enqueue_compact`), and honest about its result (404 unknown table / 202 `{job_id}` / 200 `{job_id: null}`) — instead of an unconditional, non-deduped 202 available to any authenticated caller.

**Architecture:** Ingest's `AppState` gains the two handles the shared guard helper needs (`pool`, `compact_small_file_bytes`); the compact handler stops calling `Queue::enqueue` and calls `control_plane_postgres::iceberg_compact::maybe_enqueue_compact` with `min_small_files: 2` (the worker's own convergence floor — operator = eager policy, same correctness guards). The route moves out of `router()` into a new self-gating `compact_routes()` builder that layers `require_admin` under `require_auth`, mirroring `service_runtime::admin_routes`. No new SQL, no migration, no `ControlPlane` trait change, no worker/auto-trigger change. Spec: `docs/superpowers/specs/2026-07-12-compact-endpoint-guard-design.md`.

**Tech Stack:** Rust, axum (`route_layer` + `from_fn_with_state` middleware), sqlx (no `.sqlx` change), buck2 `loom_fixture_test`, hermetic Postgres fixture, `//src/testing:seed`'s `local_sql_catalog`, tower `oneshot`.

## Global Constraints

Carried from the spec + CLAUDE.md; every task implicitly includes these:

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** New/changed fixture tests MUST use `loom_fixture_test` (`src/control-plane/postgres/defs.bzl`) or they run without the fixture env and fail to boot Postgres. The `no-inline-tests` prek hook fails the build on any inline `#[test]` under `src/**`.
- **No `.sqlx` impact.** This change adds **no** `query!`/`query_scalar!` SQL: it reuses `maybe_enqueue_compact`, whose queries are already prepared and committed. Do NOT run `tools/sqlx-prepare.sh`. If the implementation drifts into new compile-time SQL, stop and reconsider (a cloud session cannot regenerate the cache — `initdb` refuses root).
- **Clippy is strict (pedantic + restriction)** on production lib/bin code: no `unwrap`/`expect`/`indexing_slicing`/`panic`/`unreachable`/`todo`. Every new production line here is `match`/`?`/`map_err` — no panics. Test code is exempted from the panic-safety lints via `loom_fixture_test`/`loom_rust_test`.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files`. `git add` new files FIRST — prek skips untracked files. Markdown ends with exactly one trailing newline, no trailing whitespace.
- **Build/test commands:** build `buck2 build -v0 --console none //src/...`; test `buck2 test --console none <targets>`. Never pipe buck2 through `tail`/`head`. In a cloud session build with `-M none` and scope tests to touched targets.
- **Registers:** `docs/ISSUES.md` carries open work only — the `iss-compact-endpoint-unguarded` entry is **deleted** in the closing PR (Task 5), and the landed capability is documented in `docs/system-capabilities/ingest.md`.

---

## Ground truth (verified against the tree at `work/iss-compact-endpoint-unguarded`, base `b11ef18d`)

Read this before Task 1 — two spec statements are imprecise, and one build-graph fact is not in the spec.

1. **`current_snapshot` is `Result<Snapshot>`, not `Result<Option<…>>`.** The spec says "404 unless `cp.catalog().current_snapshot(&table)` is `Some`". The real trait (`src/control-plane/core/src/catalog.rs:71`) is:

   ```rust
   async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot>;
   ```

   An unknown table is `Err(ControlPlaneError::NotFound(_))`. The `schedule_table_check` precedent the spec cites gets this right (`src/services/runtime/src/admin.rs:1672-1684`): `Ok(_) => None, Err(ControlPlaneError::NotFound(_)) => Some(400…), Err(e) => Some(status_for(&e))`. Follow the code, not the spec's phrasing.

2. **`require_admin` needs an `AdminState`, and cannot be layered onto ingest's whole `router()`.** Its real form (`src/services/runtime/src/admin.rs:45`) is:

   ```rust
   pub async fn require_admin(State(st): State<AdminState>, req: Request, next: Next) -> Response
   ```

   It reads `Subject` **from request extensions** (injected upstream by `require_auth`) — missing → 401, non-admin → 403, lookup error → 403. It only touches `st.cp` (`acl().has_role(&sid, &RoleId(ADMIN_ROLE))`); `AdminState::auth` is a required struct field it never reads. It composes with `protect` exactly as `admin_routes` does (`admin.rs:1814-1816`): inner `.route_layer(from_fn_with_state(admin, require_admin))`, then `protect(inner, auth)` adds `require_auth` **outside** it, so authn runs first and injects the `Subject` the gate needs. The spec's "per-route `route_layer` in ingest's router" is therefore only half-right: `Router::route_layer` applies to **every** route in that `Router`, so it cannot be aimed at one route of the existing ingest router. **Working alternative (this plan):** keep `router()` as the data-plane surface (landing routes, authn-only) and add a sibling builder `compact_routes(state, auth)` that owns the compact route, layers `require_admin` inside and `protect` outside, and is `.merge()`d in `serve()` — the same per-gate sub-router shape `service_runtime` already uses (`login_routes`, `session_routes`, `service_account_routes`, `admin_routes`). This keeps `router(state)`'s signature unchanged, so the 8 other `router(...)` call sites do not move.

3. **`AppState` construction sites — every one must gain the two new fields (miss one and the build breaks).** Nine, one of them in *another crate*:

   | # | File | Line |
   |---|------|------|
   | 1 | `src/services/ingest/src/serve.rs` | 44 |
   | 2 | `src/services/ingest/tests/compact_endpoint.rs` | 34 |
   | 3 | `src/services/ingest/tests/iceberg_land.rs` | 64 |
   | 4 | `src/services/ingest/tests/http_land.rs` | 78 (inside `app_state`) |
   | 5 | `src/services/ingest/tests/runtime_land.rs` | 88 |
   | 6 | `src/services/ingest/tests/stream_declare.rs` | 75 (inside `app_state`) |
   | 7 | `src/services/ingest/tests/model_cdc_declare.rs` | 85 (inside `app_state`) |
   | 8 | `src/services/ingest/tests/http_model.rs` | 87 (inside `app_state`) |
   | 9 | `src/services/query-api/tests/http_wire_e2e.rs` | 90 (`ingest::http::AppState`) |

   All nine already have a `PgPool` in scope (each builds an `IcebergMaterializer { pool, .. }` from `fx.pool_for(&db)`), **except #2** (`compact_endpoint.rs`, which uses a `StubMaterializer` and no pool) — that one adds `let pool = fx.pool_for(&db).await;`. Verify with:
   `grep -rn "AppState {" --include=*.rs src/ | grep -v "pub struct"`

4. **A known collision with in-flight PR #432** (`work/iss-stream-log-vs-cdc-declare`) — see Task 0.

---

## File Structure

**Modify (production):**
- `src/services/ingest/src/http.rs` — `AppState` gains `pool` + `compact_small_file_bytes`; `ApiError` gains `NotFound`; `router()` drops the compact route; new `compact_routes()`; `compact` handler rewritten onto `maybe_enqueue_compact`; OpenAPI annotation updated.
- `src/services/ingest/src/serve.rs` — clone the pool before it moves into the materializer, pass `routing.compact_small_file_bytes` into `AppState`, merge `compact_routes(state, auth)`.

**Modify (tests):**
- `src/services/ingest/tests/compact_endpoint.rs` — rewritten: gated router driver + seeding + all six spec cases.
- `src/services/ingest/tests/api_error.rs` — one new case for the `NotFound` → 404 mapping.
- `src/services/ingest/tests/{iceberg_land,http_land,runtime_land,stream_declare,model_cdc_declare,http_model}.rs` — two new `AppState` fields each.
- `src/services/query-api/tests/http_wire_e2e.rs` — two new `AppState` fields.

**Modify (build):**
- `src/services/ingest/BUCK` — `compact-endpoint` target gains deps (`//src/services/runtime:runtime`, `//src/testing:seed`, arrow, sqlx, tempfile, time, uuid, http-body-util).

**Modify (docs):**
- `docs/system-capabilities/ingest.md` — the compact endpoint's new contract + gate.
- `docs/ISSUES.md` — delete the `iss-compact-endpoint-unguarded` entry.

---

## Task 0: Rebase onto main and reconcile with PR #432 (`e2e-support`)

**Do this FIRST, before writing any code**, and again if main moves under you.

PR #432 (branch `work/iss-stream-log-vs-cdc-declare`) introduces **`//src/services/ingest:e2e-support`** — a `rust_library` over `src/services/ingest/tests/e2e_support.rs` — and rewires `model_cdc_declare.rs` + the new `stream_log_vs_cdc_http.rs` to use it. It is **not** on this branch today (`ls src/services/ingest/tests/e2e_support.rs` → no such file) but will merge to main first. Its exported helpers (verified on `origin/work/iss-stream-log-vs-cdc-declare`):

```rust
pub async fn app_state(fx: &PgFixture, db: &str)
    -> (Arc<PgControlPlane>, PgPool, tempfile::TempDir, AppState);   // builds the AppState literal
pub fn protected(state: AppState, pg: Arc<PgControlPlane>) -> Router; // protect(router(state), AuthState{..})
pub async fn session_token(pg: &PgControlPlane, subject: &str) -> String;
pub async fn grant_write_absent_type(pg: &PgControlPlane, pool: &PgPool, subject: &str, type_name: &str);
pub fn sample_batch() -> RecordBatch;  pub fn ipc_bytes(batch: &RecordBatch) -> Vec<u8>;
pub async fn post_model_q(...); pub async fn post_dataset_q(...); pub async fn stream_meta_row(...);
```

- [ ] **Step 1: Fetch and rebase**

```bash
git fetch origin
git rebase origin/main
```

- [ ] **Step 2: If `src/services/ingest/tests/e2e_support.rs` now exists, treat it as a TENTH `AppState` construction site**

`e2e_support::app_state` contains an `AppState { materializer, cp }` literal (line ~101 on #432's branch). It **must** gain `pool: pool.clone()` and `compact_small_file_bytes` in Task 1 alongside the other nine, or `//src/services/ingest:e2e-support` (and every test depending on it) fails to build. Re-run the census after the rebase:

```bash
grep -rn "AppState {" --include=*.rs src/ | grep -v "pub struct"
```

- [ ] **Step 3: If `e2e-support` exists, REUSE it in `compact_endpoint.rs` rather than adding a sixth copy of the auth harness**

In Task 2's test file, `use e2e_support::session_token;` (add `":e2e-support"` to the `compact-endpoint` BUCK deps) instead of re-copying `session_token`. Add the two genuinely new shared helpers to `e2e_support.rs` (not to the test file) so the next admin-gated ingest test inherits them:

```rust
/// Seed a subject holding the reserved `admin` role and mint its session token.
pub async fn admin_session_token(pg: &PgControlPlane, subject: &str) -> String {
    let token = session_token(pg, subject).await;
    let subj = SubjectId(subject.into());
    let role = RoleId(control_plane_core::ADMIN_ROLE.to_string());
    pg.define_subject(&subj).await.unwrap();
    pg.define_role(&role).await.unwrap();
    pg.assign_role(&subj, &role).await.unwrap();
    token
}

/// The admin-gated maintenance router (`compact_routes`), wired exactly as the binary does.
pub fn compact_app(state: AppState, pg: Arc<PgControlPlane>) -> Router {
    ingest::http::compact_routes(
        state,
        AuthState {
            auth: pg,
            session_ttl: Duration::from_secs(3600),
            lockout: service_runtime::LockoutPolicy::default(),
        },
    )
}
```

`e2e_support::app_state` builds an `IcebergMaterializer` (unused by the compact handler but harmless), so it fits the compact tests; the compact tests additionally need a `SqlCatalog` to seed small files, which `app_state` does not return — build that with `loom_test_seed::local_sql_catalog(fx.pg_dsn(&db), &wh.path().display().to_string())` (Task 2 shows the shape). **If `e2e_support.rs` does NOT exist after the rebase, write Task 2's file self-contained exactly as spelled out there** and do not create the library (that is #432's deliverable, not this one).

- [ ] **Step 4: Confirm the tree still builds before changing anything**

Run: `buck2 build -v0 --console none //src/services/ingest/... //src/services/query-api/...`
Expected: exit 0, no output.

---

## Task 1: Thread `pool` + `compact_small_file_bytes` into `AppState`

Mechanical type change — there is no behaviour to test-drive here; its gate is that the *existing* suite still compiles and passes. The failing-test cycle starts in Task 2.

**Files:**
- Modify: `src/services/ingest/src/http.rs:113-117` (`AppState`)
- Modify: `src/services/ingest/src/serve.rs:21-58` (`serve`)
- Modify: all nine (ten, post-#432) `AppState` construction sites from the Ground-truth census

**Interfaces:**
- Produces: `ingest::http::AppState { materializer: Arc<dyn LandingMaterializer>, cp: Arc<dyn ControlPlane>, pool: sqlx::PgPool, compact_small_file_bytes: i64 }` — Tasks 2 and 3 consume both new fields.
- Consumes: `crate::config::RoutingTuning::compact_small_file_bytes: i64` (`src/services/ingest/src/config.rs:52`; default `128 * 1024 * 1024`, validated `>= 1`).

- [ ] **Step 1: Extend `AppState`**

In `src/services/ingest/src/http.rs`, replace the struct (keep `#[derive(Clone)]` — `PgPool` is an `Arc`'d handle and `i64` is `Copy`, so the derive still holds):

```rust
/// Shared, owned dependencies: the configured landing backend (Iceberg),
/// chosen at boot, the control plane, and — for the operator maintenance
/// surface — a raw pool handle plus the small-file cutoff the compaction
/// guard needs. `ControlPlane` exposes no raw pool, and the compact endpoint
/// is inherently a postgres-deployment surface (it enqueues a physical
/// compaction job), so the concrete dependency is honest.
#[derive(Clone)]
pub struct AppState {
    pub materializer: Arc<dyn LandingMaterializer>,
    pub cp: Arc<dyn ControlPlane>,
    /// Cloned from the pool the materializer owns; used only by `compact`.
    pub pool: sqlx::PgPool,
    /// `LOOM_COMPACT_THRESHOLD_BYTES` (routing config): files strictly smaller
    /// than this count as "small" for the compaction guard.
    pub compact_small_file_bytes: i64,
}
```

- [ ] **Step 2: Wire it in `serve()`**

In `src/services/ingest/src/serve.rs`, the pool is currently **moved** into `IcebergMaterializer` (line 37). Clone it first:

```rust
    let compact_small_file_bytes = app_cfg.routing.compact_small_file_bytes;
    let state_pool = pool.clone();

    let materializer: Arc<dyn LandingMaterializer> = {
        let catalog = Arc::new(build_iceberg_catalog(cfg, &app_cfg.routing).await?);
        Arc::new(IcebergMaterializer {
            catalog,
            pool,
            inline_byte_limit: app_cfg.routing.inline_byte_limit,
            flush_byte_threshold: app_cfg.routing.flush_byte_threshold,
        })
    };

    let sa_cp = cp.clone();
    let state = AppState {
        materializer,
        cp: cp.clone(),
        pool: state_pool,
        compact_small_file_bytes,
    };
    let app = service_runtime::protect(router(state), auth.clone())
        .merge(service_runtime::login_routes(auth.clone()))
        .merge(service_runtime::session_routes(auth.clone()))
        .merge(service_runtime::service_account_routes(auth, sa_cp, max_ttl));
```

(Task 2 adds the `compact_routes` merge here; leave the rest of `serve()` untouched.)

- [ ] **Step 3: Update every other construction site**

Each of the eight test sites already has `pool` in scope. Add the two fields to each literal, e.g. in `src/services/ingest/tests/http_land.rs:78`:

```rust
    let state = AppState {
        materializer: Arc::new(IcebergMaterializer {
            catalog: Arc::new(catalog),
            pool: pool.clone(),
            inline_byte_limit: 16 * 1024 * 1024,
            flush_byte_threshold: 64 * 1024 * 1024,
        }),
        cp: cp.clone(),
        pool: pool.clone(),
        compact_small_file_bytes: 1 << 20,
    };
```

Apply the identical two-field addition (`pool: pool.clone(), compact_small_file_bytes: 1 << 20`) at:
`tests/iceberg_land.rs:64`, `tests/http_land.rs:78`, `tests/runtime_land.rs:88`, `tests/stream_declare.rs:75`, `tests/model_cdc_declare.rs:85`, `tests/http_model.rs:87`, `src/services/query-api/tests/http_wire_e2e.rs:90`, and — post-#432 — `tests/e2e_support.rs`'s `app_state`. `tests/compact_endpoint.rs:34` is rewritten wholesale in Task 2; for now just add `let pool = fx.pool_for(&db).await;` above it and the same two fields, so the tree builds.

`1 << 20` (1 MiB) is a deliberate test-only value: the fixture's Parquet files are a few hundred bytes, so any non-zero cutoff works, and 1 MiB matches the `CompactTriggerCfg` used by `postgres/tests/compact_trigger.rs:110-115`.

- [ ] **Step 4: Build both affected crates**

Run: `buck2 build -v0 --console none //src/services/ingest/... //src/services/query-api/...`
Expected: exit 0, silent. A `missing field` error names any site you missed.

- [ ] **Step 5: Run the affected suites (unchanged behaviour)**

Run: `buck2 test --console none //src/services/ingest/... //src/services/query-api:http-wire-e2e`
Expected: `Tests finished: Pass N. Fail 0` (the pre-existing `compact_endpoint_enqueues_job` still passes — the handler is untouched).

- [ ] **Step 6: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "refactor(ingest): thread pool + compact_small_file_bytes into AppState"
```

---

## Task 2: Admin-gate the compact route (`compact_routes`)

TDD: the two gate tests fail first (the route is not on the gated router yet), then the router change makes them pass. The handler still has its old always-202 body at the end of this task — Task 3 replaces it.

**Files:**
- Modify: `src/services/ingest/src/http.rs:119-125` (`router`), new `compact_routes` below it
- Modify: `src/services/ingest/src/serve.rs` (merge the new sub-router)
- Modify: `src/services/ingest/tests/compact_endpoint.rs` (rewrite the harness + two gate tests)
- Modify: `src/services/ingest/BUCK` (`compact-endpoint` deps)

**Interfaces:**
- Consumes: `service_runtime::{AdminState, AuthState, protect, require_admin}` (re-exported from `src/services/runtime/src/lib.rs:6,16`); `control_plane_core::ADMIN_ROLE`; `Acl::{define_subject, define_role, assign_role}`; `Auth::{create_user, create_session}`; `service_runtime::{control_plane, generate_session_token, hash_password, token_sha256}`.
- Produces: `pub fn ingest::http::compact_routes(state: AppState, auth: service_runtime::AuthState) -> axum::Router` — Task 3's tests drive this router.

- [ ] **Step 1: Write the failing tests**

Rewrite `src/services/ingest/tests/compact_endpoint.rs` with the gated harness plus the two gate cases. (Post-#432: import `session_token`/`admin_session_token`/`compact_app` from `e2e_support` instead of the local copies — see Task 0 Step 3.)

```rust
//! POST /tables/{schema}/{table}/compact — the guarded operator surface
//! (iss-compact-endpoint-unguarded): admin-only, guarded (stream/changelog/
//! shadow refused), deduped through `maybe_enqueue_compact`, and honest about
//! its result (404 / 202 {job_id} / 200 {job_id: null}).
//! loom_fixture_test (Postgres).

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use axum::Router;
use axum::body::Body;
use axum::http::header::AUTHORIZATION;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Auth, COMPACT_JOB_KIND, ColumnSpec, ControlPlane, DatasetId, EventType, LineageEvent,
    NewUser, RoleId, RunId, SnapshotId, SubjectId, TableRef,
};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_landing::{InlineLimits, land};
use control_plane_postgres::iceberg_sql_catalog::SqlCatalog;
use http_body_util::BodyExt;
use ingest::http::{AppState, compact_routes};
use ingest::landing::{LandRequest, LandingMaterializer};
use loom_test_seed::local_sql_catalog;
use service_runtime::{AuthState, generate_session_token, token_sha256};
use sqlx::PgPool;
use time::OffsetDateTime;
use tower::ServiceExt;

const SMALL_FILE_BYTES: i64 = 1 << 20; // 1 MiB: the fixture's Parquet files all qualify

/// Stub materializer — the compact endpoint never lands, so `land` is unreachable.
struct StubMaterializer;

#[async_trait::async_trait]
impl LandingMaterializer for StubMaterializer {
    async fn land(&self, _req: LandRequest<'_>) -> Result<SnapshotId, ingest::IngestError> {
        unreachable!("compact endpoint does not call materializer")
    }
}

/// The fixture db + a temp warehouse: the AppState the router runs on, the pool,
/// the concrete control plane (auth/ACL/stream seeding), and a real SqlCatalog to
/// land small files through.
async fn harness(
    fx: &PgFixture,
    db: &str,
) -> (
    Arc<PgControlPlane>,
    PgPool,
    SqlCatalog,
    tempfile::TempDir,
    AppState,
) {
    let pool = fx.pool_for(db).await;
    let wh = tempfile::tempdir().unwrap();
    let catalog = local_sql_catalog(fx.pg_dsn(db), &wh.path().display().to_string()).await;
    let pg = Arc::new(service_runtime::control_plane(
        pool.clone(),
        Duration::from_millis(300),
    ));
    let state = AppState {
        materializer: Arc::new(StubMaterializer),
        cp: pg.clone() as Arc<dyn ControlPlane>,
        pool: pool.clone(),
        compact_small_file_bytes: SMALL_FILE_BYTES,
    };
    (pg, pool, catalog, wh, state)
}

/// The admin-gated maintenance router, wired exactly as the binary does.
fn compact_app(state: AppState, pg: Arc<PgControlPlane>) -> Router {
    compact_routes(
        state,
        AuthState {
            auth: pg,
            session_ttl: Duration::from_secs(3600),
            lockout: service_runtime::LockoutPolicy::default(),
        },
    )
}

/// Create the user (ensures the ACL subject exists) and mint a live bearer token.
async fn session_token(pg: &PgControlPlane, subject: &str) -> String {
    let phc = service_runtime::hash_password("e2e-password").expect("hash");
    pg.create_user(&NewUser {
        subject_id: SubjectId(subject.into()),
        username: subject.into(),
        password_phc: phc,
    })
    .await
    .expect("create_user");
    let token = generate_session_token();
    let expires = OffsetDateTime::now_utc() + time::Duration::hours(1);
    pg.create_session(&SubjectId(subject.into()), &token_sha256(&token), expires)
        .await
        .expect("create_session");
    token
}

/// A subject holding the reserved `admin` role, plus its session token.
async fn admin_session_token(pg: &PgControlPlane, subject: &str) -> String {
    let token = session_token(pg, subject).await;
    let subj = SubjectId(subject.into());
    let role = RoleId(control_plane_core::ADMIN_ROLE.to_string());
    pg.define_subject(&subj).await.expect("define_subject");
    pg.define_role(&role).await.expect("define_role");
    pg.assign_role(&subj, &role).await.expect("assign_role");
    token
}

/// POST the compact route with a bearer token; return (status, body JSON).
async fn post_compact(
    app: Router,
    schema: &str,
    table: &str,
    token: &str,
) -> (StatusCode, serde_json::Value) {
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/tables/{schema}/{table}/compact"))
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

fn columns() -> Vec<ColumnSpec> {
    vec![ColumnSpec {
        name: "id".into(),
        ty: "long".into(),
        nullable: false,
    }]
}

fn lineage(table: &TableRef) -> LineageEvent {
    LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(table).dataset_ref()],
        payload: serde_json::json!({ "source": "compact-endpoint-test" }),
    }
}

/// Land `n` one-row Parquet files (`inline_byte_limit: 0` forces the Parquet
/// branch, so each call emits exactly one small file). Mirrors
/// `postgres/tests/compact_trigger.rs::land_n_small`.
async fn land_n_small(pool: &PgPool, catalog: &SqlCatalog, table: &TableRef, n: i64) {
    for id in 0..n {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![id]))])
                .expect("batch");
        land(
            pool,
            catalog,
            table,
            &columns(),
            schema,
            vec![batch],
            InlineLimits {
                inline_byte_limit: 0,
                flush_byte_threshold: i64::MAX,
            },
            lineage(table),
            None,
        )
        .await
        .expect("land");
    }
}

async fn available_jobs(pool: &PgPool) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "select count(*) from queue.jobs where kind = $1 and state = 'available'",
    )
    .bind(COMPACT_JOB_KIND)
    .fetch_one(pool)
    .await
    .unwrap()
}

fn table(name: &str) -> TableRef {
    TableRef {
        schema: "wh".into(),
        name: name.into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_admin_subject_is_forbidden() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, catalog, _wh, state) = harness(fx, &db).await;
    let t = table("orders");
    land_n_small(&pool, &catalog, &t, 2).await;

    let token = session_token(&pg, "alice").await; // authenticated, NOT admin
    let (status, _body) = post_compact(compact_app(state, pg.clone()), "wh", "orders", &token).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(available_jobs(&pool).await, 0, "a denied POST enqueues nothing");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_bearer_is_unauthorized() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, _pool, _catalog, _wh, state) = harness(fx, &db).await;

    let res = compact_app(state, pg)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables/wh/orders/compact")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}
```

Delete the old `compact_endpoint_enqueues_job` for now — Task 3 reintroduces it under the new contract. (Leaving it would assert the *old* always-202-for-a-nonexistent-table behaviour, which Task 3 deliberately breaks.)

Update `src/services/ingest/BUCK`'s `compact-endpoint` target deps to:

```python
loom_fixture_test(
    name = "compact-endpoint",
    crate = "compact_endpoint",
    srcs = ["tests/compact_endpoint.rs"],
    crate_root = "tests/compact_endpoint.rs",
    deps = [
        ":ingest",
        "//src/services/runtime:runtime",
        "//src/testing:seed",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        "//third-party:async-trait",
        "//third-party:axum",
        "//third-party:http-body-util",
        "//third-party:serde_json",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:tower",
        "//third-party:uuid",
    ],
)
```

(Post-#432, add `":e2e-support"` and drop whichever deps the reused helpers cover.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 test --console none //src/services/ingest:compact-endpoint`
Expected: FAIL — compile error `cannot find function 'compact_routes' in module 'ingest::http'`.

- [ ] **Step 3: Split the router and add the admin gate**

In `src/services/ingest/src/http.rs`, replace `router` and add `compact_routes`:

```rust
/// The ingest data plane: landing routes, authentication-only (the binary wraps
/// this in `service_runtime::protect`). The operator maintenance surface lives
/// in [`compact_routes`], which additionally requires the reserved `admin` role.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/datasets/:schema/:table", post(land))
        .route("/models/:type", post(land_model))
        .with_state(state)
}

/// The operator maintenance surface: `POST /tables/{schema}/{table}/compact`,
/// gated `require_auth` (401) THEN `require_admin` (403) — the `/admin/*`
/// posture (`service_runtime::admin_routes`). Kept out of [`router`] because
/// landing is a data-plane surface (authn only) while compaction is an
/// operator cost lever.
///
/// Layer order matters: `require_admin` reads the `Subject` that `require_auth`
/// injects into request extensions, so `protect` must wrap (i.e. run before)
/// the admin `route_layer` — exactly as `admin_routes` composes them.
pub fn compact_routes(state: AppState, auth: service_runtime::AuthState) -> Router {
    // `require_admin` reads only `AdminState::cp` (the reserved-role ACL check);
    // the `auth` field is required by the struct and unused by the gate.
    let admin = service_runtime::AdminState {
        auth: auth.auth.clone(),
        cp: state.cp.clone(),
    };
    service_runtime::protect(
        Router::new()
            .route("/tables/:schema/:table/compact", post(compact))
            .with_state(state)
            .route_layer(axum::middleware::from_fn_with_state(
                admin,
                service_runtime::require_admin,
            )),
        auth,
    )
}
```

In `src/services/ingest/src/serve.rs`, merge it (the `state` binding from Task 1 Step 2 becomes a clone):

```rust
    let app = service_runtime::protect(router(state.clone()), auth.clone())
        .merge(crate::http::compact_routes(state, auth.clone()))
        .merge(service_runtime::login_routes(auth.clone()))
        .merge(service_runtime::session_routes(auth.clone()))
        .merge(service_runtime::service_account_routes(auth, sa_cp, max_ttl));
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `buck2 test --console none //src/services/ingest:compact-endpoint`
Expected: `Tests finished: Pass 2. Fail 0`.

- [ ] **Step 5: Prove nothing else regressed** (the router signature is unchanged, but `runtime_land`/`http_wire_e2e` exercise the real serve wiring)

Run: `buck2 test --console none //src/services/ingest/... //src/services/query-api:http-wire-e2e`
Expected: `Fail 0`.

- [ ] **Step 6: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(ingest): admin-gate the operator compact route"
```

---

## Task 3: Guarded, deduped, honest handler

**Files:**
- Modify: `src/services/ingest/src/http.rs:40-54` (`ApiError` + `IntoResponse`), `:129-169` (annotation + handler)
- Modify: `src/services/ingest/tests/api_error.rs` (one new case)
- Modify: `src/services/ingest/tests/compact_endpoint.rs` (the six spec cases)

**Interfaces:**
- Consumes: `control_plane_postgres::iceberg_compact::{CompactTriggerCfg, maybe_enqueue_compact}` — exact current signature (`src/control-plane/postgres/src/iceberg_compact.rs:43-47`, verified):

  ```rust
  pub async fn maybe_enqueue_compact(
      conn: &mut sqlx::PgConnection,
      table: &TableRef,
      cfg: &CompactTriggerCfg,           // { small_file_bytes: i64, min_small_files: i64 }
  ) -> Result<Option<control_plane_core::JobId>>;
  ```

  It resolves the live `table_id`, refuses declared stream tables / changelog tables (`stream.stream_table.table_id` or `.changelog_table_id`) / shadow-flagged tables (`iceberg_mirror.shadow_flag`), counts live files `< cfg.small_file_bytes`, and enqueues via `pg_insert_if_absent` — so operator and auto-trigger jobs dedup against each other on `(kind, payload)`.
  Also consumes `ControlPlane::catalog().current_snapshot(&table) -> Result<Snapshot>` (`Err(NotFound)` for a never-written table) and `AppState::{pool, compact_small_file_bytes}` from Task 1.
- Produces: `ApiError::NotFound(Cow<'static, str>)` → HTTP 404; the compact contract 404 / 202 `{"job_id": "<uuid>"}` / 200 `{"job_id": null}`.

- [ ] **Step 1: Write the failing tests**

Add to `src/services/ingest/tests/api_error.rs`:

```rust
#[test]
fn not_found_is_404() {
    assert_eq!(
        status_of(ApiError::NotFound("unknown table: wh.orders".into())),
        StatusCode::NOT_FOUND
    );
}
```

Add the six spec cases to `src/services/ingest/tests/compact_endpoint.rs` (harness from Task 2):

```rust
/// Eager policy + the 202 contract: exactly 2 small files — below the
/// auto-trigger's default 8 — compacts on an explicit operator request, and the
/// enqueued job carries the same `{schema, name}` payload the auto-trigger builds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_post_enqueues_job_at_two_small_files() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, catalog, _wh, state) = harness(fx, &db).await;
    let t = table("orders");
    land_n_small(&pool, &catalog, &t, 2).await;

    let token = admin_session_token(&pg, "root").await;
    let (status, body) = post_compact(compact_app(state, pg.clone()), "wh", "orders", &token).await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body["job_id"].is_string(), "202 carries the job id: {body}");
    assert_eq!(available_jobs(&pool).await, 1);

    let job = pg
        .queue()
        .dequeue(&[COMPACT_JOB_KIND.to_string()], "test")
        .await
        .unwrap()
        .expect("a compact_table job was enqueued");
    assert_eq!(job.kind, COMPACT_JOB_KIND);
    assert_eq!(job.payload["schema"], "wh");
    assert_eq!(job.payload["name"], "orders");
}

/// Dedup: a second POST while the first job is still pending adds nothing and
/// says so honestly (200 `{job_id: null}`), instead of the old unconditional 202.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeat_post_dedups_to_one_job() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, catalog, _wh, state) = harness(fx, &db).await;
    land_n_small(&pool, &catalog, &table("orders"), 2).await;
    let token = admin_session_token(&pg, "root").await;

    let (s1, b1) = post_compact(
        compact_app(state.clone(), pg.clone()),
        "wh",
        "orders",
        &token,
    )
    .await;
    assert_eq!(s1, StatusCode::ACCEPTED);
    assert!(b1["job_id"].is_string());

    let (s2, b2) = post_compact(compact_app(state, pg.clone()), "wh", "orders", &token).await;
    assert_eq!(s2, StatusCode::OK, "suppressed: a job is already pending");
    assert!(b2["job_id"].is_null());

    assert_eq!(available_jobs(&pool).await, 1, "exactly one pending job");
}

/// Cross-producer dedup: an auto-trigger job already pending absorbs the
/// operator POST (same `(kind, payload)` → `pg_insert_if_absent`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_after_auto_trigger_adds_nothing() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, catalog, _wh, state) = harness(fx, &db).await;
    let t = table("orders");
    land_n_small(&pool, &catalog, &t, 2).await;

    // The auto-trigger's own entrypoint, invoked exactly as the catalog does.
    let mut conn = pool.acquire().await.expect("acquire");
    let auto = maybe_enqueue_compact(
        &mut conn,
        &t,
        &CompactTriggerCfg {
            small_file_bytes: SMALL_FILE_BYTES,
            min_small_files: 2,
        },
    )
    .await
    .expect("auto trigger");
    drop(conn);
    assert!(auto.is_some(), "auto-trigger enqueued");
    assert_eq!(available_jobs(&pool).await, 1);

    let token = admin_session_token(&pg, "root").await;
    let (status, body) = post_compact(compact_app(state, pg.clone()), "wh", "orders", &token).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["job_id"].is_null());
    assert_eq!(available_jobs(&pool).await, 1, "still exactly one");
}

/// Eligibility: a declared stream table is refused (its own consolidation owns
/// that data; compaction could resurrect tombstoned rows).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declared_stream_table_is_refused() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, catalog, _wh, state) = harness(fx, &db).await;
    let t = table("events");
    land_n_small(&pool, &catalog, &t, 2).await;

    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, &t.schema, &t.name)
        .await
        .expect("live_table_id")
        .expect("table has a live row");
    drop(conn);
    pg.declare_stream(tid, 4).await.expect("declare_stream");

    let token = admin_session_token(&pg, "root").await;
    let (status, body) = post_compact(compact_app(state, pg.clone()), "wh", "events", &token).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["job_id"].is_null());
    assert_eq!(available_jobs(&pool).await, 0);
}

/// Eligibility: a CDC table's changelog table is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changelog_table_is_refused() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, catalog, _wh, state) = harness(fx, &db).await;
    let base = table("base");
    let changelog = table("base__changelog");
    land_n_small(&pool, &catalog, &changelog, 2).await;
    land_n_small(&pool, &catalog, &base, 1).await;

    let mut conn = pool.acquire().await.expect("acquire");
    let base_tid = live_table_id(&mut conn, &base.schema, &base.name)
        .await
        .expect("live_table_id")
        .expect("base has a live row");
    let clog_tid = live_table_id(&mut conn, &changelog.schema, &changelog.name)
        .await
        .expect("live_table_id")
        .expect("changelog has a live row");
    drop(conn);
    // set_changelog_table_id UPDATEs an existing stream_table row, so declare first.
    pg.declare_cdc(base_tid, 1, "id", control_plane_core::MergeEngine::LastRow)
        .await
        .expect("declare_cdc");
    pg.set_changelog_table_id(base_tid, clog_tid)
        .await
        .expect("set_changelog_table_id");

    let token = admin_session_token(&pg, "root").await;
    let (status, body) = post_compact(
        compact_app(state, pg.clone()),
        "wh",
        "base__changelog",
        &token,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["job_id"].is_null());
    assert_eq!(available_jobs(&pool).await, 0);
}

/// Eligibility: a shadow-flagged table is refused (COW merge-on-read takes the
/// highest `begin_snapshot`; re-projecting files could resurrect tombstones).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shadow_flagged_table_is_refused() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, catalog, _wh, state) = harness(fx, &db).await;
    let t = table("orders");
    land_n_small(&pool, &catalog, &t, 2).await;

    let mut conn = pool.acquire().await.expect("acquire");
    let tid = live_table_id(&mut conn, &t.schema, &t.name)
        .await
        .expect("live_table_id")
        .expect("table has a live row");
    set_has_shadow(&mut conn, tid).await.expect("set_has_shadow");
    drop(conn);

    let token = admin_session_token(&pg, "root").await;
    let (status, body) = post_compact(compact_app(state, pg.clone()), "wh", "orders", &token).await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["job_id"].is_null());
    assert_eq!(available_jobs(&pool).await, 0);
}

/// 404: a never-written table. (The pre-fix endpoint happily enqueued here —
/// `tests/compact_endpoint.rs`'s original `compact_endpoint_enqueues_job` did
/// exactly that.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_table_is_404() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (pg, pool, _catalog, _wh, state) = harness(fx, &db).await;

    let token = admin_session_token(&pg, "root").await;
    let (status, _body) = post_compact(compact_app(state, pg.clone()), "wh", "ghost", &token).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(available_jobs(&pool).await, 0);
}
```

Add these imports to the test file's use-block:

```rust
use control_plane_core::{MergeEngine, Queue, StreamTables};
use control_plane_postgres::iceberg_compact::{CompactTriggerCfg, maybe_enqueue_compact};
use control_plane_postgres::iceberg_inline::set_has_shadow;
use control_plane_postgres::iceberg_mirror::live_table_id;
```

(`Queue` is needed for `pg.queue().dequeue(..)`; `StreamTables` for `declare_stream`/`declare_cdc`/`set_changelog_table_id` — the same imports `postgres/tests/compact_trigger.rs` uses.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `buck2 test --console none //src/services/ingest:compact-endpoint //src/services/ingest:api-error`
Expected: FAIL — `api-error` fails to compile (`no variant named NotFound`); `compact-endpoint` fails to compile (`unresolved import`… once fixed, the contract tests fail: the old handler returns 202 for every case, and `available_jobs` reads 2 after a repeat POST).

- [ ] **Step 3: Add the `NotFound` variant**

In `src/services/ingest/src/http.rs`, add to `ApiError` and its `IntoResponse`:

```rust
    /// The addressed resource does not exist — 404 with a safe, client-visible
    /// message (the operator surface names the table; it is not a data leak,
    /// the caller is an admin).
    NotFound(Cow<'static, str>),
```

```rust
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg).into_response(),
```

- [ ] **Step 4: Rewrite the handler**

Replace the `#[utoipa::path]` annotation and body of `compact` in `src/services/ingest/src/http.rs`:

```rust
/// Operator action: request compaction of `{schema}.{table}`. Admin-gated
/// (`compact_routes`). Routes through the shared guard helper the auto-trigger
/// uses, so operator and auto jobs dedup against each other and the same
/// eligibility guards (declared stream tables, changelog tables, shadow-flagged
/// tables) refuse. Eager policy: `min_small_files: 2` — the worker's own
/// convergence floor — so an explicit request compacts any compactable pair
/// without waiting for `LOOM_COMPACT_TRIGGER_FILES`.
#[utoipa::path(
    post, path = "/tables/{schema}/{table}/compact",
    params(
        ("schema" = String, Path, description = "Iceberg schema"),
        ("table" = String, Path, description = "Table name"),
    ),
    responses(
        (status = 202, description = "Compaction job enqueued", body = JobAck),
        (status = 200, description = "Suppressed — `{\"job_id\": null}`: the table is ineligible (declared stream / changelog / shadow-flagged), has nothing to compact (< 2 small files), or a compact_table job is already pending"),
        (status = 403, description = "Caller does not hold the reserved admin role"),
        (status = 404, description = "Unknown table (never written)"),
        (status = 500, description = "Internal error"),
    ),
    security(("bearer_auth" = [])),
    tag = "tables",
)]
pub(crate) async fn compact(
    State(st): State<AppState>,
    Path((schema, table)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let table = TableRef {
        schema,
        name: table,
    };
    // 404 an unknown table rather than enqueueing a job that can never succeed
    // (the `schedule_table_check` posture, service_runtime/admin.rs:1657-1685).
    match st.cp.catalog().current_snapshot(&table).await {
        Ok(_) => {}
        Err(ControlPlaneError::NotFound(_)) => {
            return Err(ApiError::NotFound(Cow::Owned(format!(
                "unknown table: {}.{}",
                table.schema, table.name
            ))));
        }
        Err(e) => return Err(ApiError::internal("ingest compact: current_snapshot", e)),
    }

    let mut conn = st
        .pool
        .acquire()
        .await
        .map_err(|e| ApiError::internal("ingest compact: acquire connection", e))?;
    let cfg = CompactTriggerCfg {
        small_file_bytes: st.compact_small_file_bytes,
        // The worker's own no-op floor: fewer than 2 small files cannot coalesce.
        min_small_files: 2,
    };
    // Opaque on backend faults (governance-fronted service), logged server-side.
    let job = maybe_enqueue_compact(&mut conn, &table, &cfg)
        .await
        .map_err(|e| ApiError::internal("ingest compact: enqueue job", e))?;

    match job {
        Some(id) => Ok((
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "job_id": id.0.to_string() })),
        )
            .into_response()),
        // Suppressed: ineligible (stream / changelog / shadow), nothing to do
        // (< 2 small files), or deduped against a pending job. v1 deliberately
        // does not distinguish the reasons — the guard returns no granularity.
        None => Ok((
            StatusCode::OK,
            Json(serde_json::json!({ "job_id": null })),
        )
            .into_response()),
    }
}
```

Fix the imports at the top of `http.rs`: `COMPACT_JOB_KIND`, `CompactJob`, and `NewJob` are now unused (they had exactly one use each, in the old handler) — **remove them from the `control_plane_core` use-list** or clippy fails the build. Add:

```rust
use control_plane_postgres::iceberg_compact::{CompactTriggerCfg, maybe_enqueue_compact};
```

(`control_plane_postgres` is already a dep of the `ingest` library — `http.rs:20` imports `control_plane_postgres::iceberg_landing::CdcDecl`.) `ControlPlaneError`, `TableRef`, and `Cow` are already imported.

Note on the connection type: `st.pool.acquire()` yields `sqlx::pool::PoolConnection<Postgres>`, which `DerefMut`s to `PgConnection` — `&mut conn` coerces to the `&mut sqlx::PgConnection` the helper takes. No `&mut *conn` needed.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `buck2 test --console none //src/services/ingest:compact-endpoint //src/services/ingest:api-error //src/services/ingest:openapi`
Expected: `Fail 0` (9 compact-endpoint cases; `openapi` is unaffected — the document is built from the `#[utoipa::path]` annotations listed in `openapi.rs:111`, not from the router, so splitting `router()` does not drop `/tables/{schema}/{table}/compact` from the doc).

- [ ] **Step 6: Clippy the changed crate**

Run: `buck2 build --console none --show-simple-output '//src/services/ingest:ingest[clippy.txt]'`
Then `cat` the printed path. Expected: empty file (clean).

- [ ] **Step 7: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "fix(ingest): guard + dedup the operator compact endpoint — 404/202/200 contract"
```

---

## Task 4: Full-suite verification

**Files:** none (verification only).

- [ ] **Step 1: Build everything**

Run: `buck2 build -v0 --console none //src/...`
Expected: exit 0, silent. (Cloud session: `buck2 build -M none //src/...`.)

- [ ] **Step 2: Run the whole suite**

Run: `buck2 test --console none //src/... -- -j 8`
Expected: `Tests finished: Pass N. Fail 0`. The `-j 8` cap matters locally: the full suite otherwise starves the 8 Postgres boot slots and throws non-deterministic 120s timeouts. (Cloud/root host: add `--unstable-allow-all-tests-on-re`, or rely on the buck2 proxy shim, and scope to the touched targets.)

- [ ] **Step 3: Lint gate**

Run: `buck2 run //tools:prek -- run --all-files`
Expected: all hooks pass (rustfmt is a separate hook from clippy — clippy-clean is not lint-clean).

---

## Task 5: Docs — capability note + close the issue

**Files:**
- Modify: `docs/system-capabilities/ingest.md:9`
- Modify: `docs/ISSUES.md:18-19` (delete the entry)

- [ ] **Step 1: Update the capability doc**

In `docs/system-capabilities/ingest.md`, replace the last sentence of the "Landing path" paragraph (line 9), which currently reads:

> The router also exposes an operator action, `POST /tables/{schema}/{table}/compact`, which enqueues a compaction job (202 + job id) for a zero-pool worker to perform asynchronously.

with:

> The router also exposes an operator action, `POST /tables/{schema}/{table}/compact`, mounted on a separate admin-gated sub-router (`compact_routes` — `require_auth` then `require_admin`, the `/admin/*` posture; the landing routes stay authentication-only). It routes through the same shared guard helper the compaction auto-trigger uses (`iceberg_compact::maybe_enqueue_compact`), so it dedups against a pending auto-trigger job and refuses the same ineligible tables (declared stream tables, changelog tables, shadow-flagged tables — where re-projecting files at a higher `begin_snapshot` could resurrect tombstoned rows). The operator's only policy difference is eagerness: `min_small_files: 2` (the worker's own convergence floor) rather than `LOOM_COMPACT_TRIGGER_FILES`, so an explicit request compacts any compactable pair. Responses are honest: 404 for an unknown table, 202 `{job_id}` when a job is enqueued, 200 `{job_id: null}` when the request is suppressed (ineligible, nothing to do, or already pending) — there is no force flag, because the guards prevent row resurrection, not policy.

- [ ] **Step 2: Close the register item**

Delete the whole `iss-compact-endpoint-unguarded` entry (the `- [ ] **Operator `POST …/compact` enqueue…**` title line + its prose line) from the `## catalog` section of `docs/ISSUES.md`. The registers carry open work only. Do **not** add any new register item — this plan defers nothing new (the spec's out-of-scope items `#fut-stream-smallfile-compaction` and `#fut-compact-trigger-pertable-override` are already tracked in FUTURE; suppression-reason granularity is deliberately not filed until an operator asks).

- [ ] **Step 3: Validate the registers**

Run: `bash tools/docs.sh validate`
Expected: no errors.

- [ ] **Step 4: Commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "docs(ingest): guarded compact endpoint — close iss-compact-endpoint-unguarded"
```

---

## Self-Review

**1. Spec coverage**

| Spec requirement | Task |
|---|---|
| Admin-gated (`require_admin`, `/admin/schedules` posture) | Task 2 (`compact_routes`), tests `non_admin_subject_is_forbidden` / `missing_bearer_is_unauthorized` / admin 202 in Task 3 |
| `maybe_enqueue_compact` with `min_small_files: 2` (eager policy, same correctness guards, no force flag) | Task 3 handler; test `admin_post_enqueues_job_at_two_small_files` |
| Threading: `AppState { pool, compact_small_file_bytes }`, pool cloned before it moves into the materializer, no new `ControlPlane` trait method | Task 1 |
| 404 unknown table | Task 3 (`ApiError::NotFound`); test `unknown_table_is_404` |
| 202 `{job_id}` / 200 `{job_id: null}` | Task 3; tests `admin_post_enqueues_job_at_two_small_files`, `repeat_post_dedups_to_one_job` |
| Dedup (repeat POST; POST after auto-trigger) | Tests `repeat_post_dedups_to_one_job`, `post_after_auto_trigger_adds_nothing` |
| Eligibility refusals (stream / changelog / shadow) | Tests `declared_stream_table_is_refused`, `changelog_table_is_refused`, `shadow_flagged_table_is_refused` |
| Existing test updated for the new contract (it enqueued for a never-created table) | Task 2 Step 1 deletes it; Task 3 reintroduces it as `admin_post_enqueues_job_at_two_small_files` (table created first) and `unknown_table_is_404` pins the behaviour that changed |
| OpenAPI documents the admin requirement + new statuses | Task 3 Step 4 annotation |
| Response-contract change documented in `docs/system-capabilities/` | Task 5 |
| No migration, no new SQL, `.sqlx` untouched | Global Constraints; nothing in any task adds a `query!` |
| Auto-trigger / worker / `CompactJob` / `maybe_enqueue_compact` unchanged | No task modifies `worker/` or `iceberg_compact.rs`; Task 4 re-runs `//src/...` incl. `//src/control-plane/postgres:compact-trigger` |

**2. Placeholder scan.** No `TBD`/`TODO`/"add error handling"/"similar to Task N". Every code step carries the literal code; every run step carries the literal buck2 command and its expected output.

**3. Type consistency.**
- `AppState.pool: sqlx::PgPool` / `compact_small_file_bytes: i64` — declared Task 1, consumed Task 3 (`st.pool.acquire()`, `CompactTriggerCfg { small_file_bytes: st.compact_small_file_bytes }`), set at every construction site listed in Ground truth §3. `CompactTriggerCfg.small_file_bytes` is `i64` (`iceberg_compact.rs:25`) — matches.
- `compact_routes(state: AppState, auth: service_runtime::AuthState) -> Router` — one name, used identically in `serve.rs`, the test harness's `compact_app`, and (post-#432) `e2e_support::compact_app`.
- `maybe_enqueue_compact(&mut conn, &table, &cfg) -> Result<Option<JobId>>` — the `Some`/`None` arms are the 202/200 split; `id.0.to_string()` matches the old handler's `JobId` field access.
- `ApiError::NotFound(Cow<'static, str>)` — the same variant name in `http.rs`, `api_error.rs`, and the handler's `Cow::Owned(format!(...))`.
- Test helper names are used consistently across Tasks 2 and 3: `harness`, `compact_app`, `session_token`, `admin_session_token`, `post_compact`, `land_n_small`, `available_jobs`, `table`, `SMALL_FILE_BYTES`.

**4. Failure-mode honesty (where this plan could bite).**
- **The `AppState` census is the #1 break risk.** Nine sites today, ten after #432 merges (`tests/e2e_support.rs`), one of them in a *different crate* (`query-api/tests/http_wire_e2e.rs`). Task 1 Step 4 builds **both** crates precisely to catch that. Re-run the grep after any rebase.
- **Layer order is load-bearing and silent when wrong.** If `require_admin` ended up *outside* `require_auth`, no `Subject` would be in extensions and every request — including an admin's — would 401, not 403. `missing_bearer_is_unauthorized` + `non_admin_subject_is_forbidden` + the admin-202 test together pin all three outcomes (401 / 403 / 202), so a mis-ordered layer cannot pass.
- **`Router::route_layer` is per-router, not per-route.** Applying it to ingest's existing `router()` would gate `/datasets` and `/models` on admin too — a silent data-plane outage that the landing tests *would* catch (they use non-admin subjects), but the split into `compact_routes` avoids the trap structurally. Do not "simplify" it back into one router.
- **The refusal tests are only discriminating if the table is otherwise compactable.** Each seeds **2** small files first (so `small_count >= min_small_files`) and only then declares stream / sets the changelog link / sets the shadow flag; without the seed, a `200 {job_id: null}` would prove nothing (the nothing-to-do path would produce it anyway). Keep the `land_n_small(.., 2)` in all three.
- **`land` with `inline_byte_limit: 0` is what makes files real.** The default inline path writes mirror-only rows and no `iceberg_mirror.data_file` rows, so the guard's `small_count` would read 0 and every POST would 200-null. `InlineLimits { inline_byte_limit: 0, flush_byte_threshold: i64::MAX }` is deliberate (and `flush_byte_threshold: i64::MAX` keeps a `flush_table` job from polluting the `queue.jobs` counts).
- **`available_jobs` filters on `kind = 'compact_table'`** — a stray `flush_table` job would not corrupt the counts, but a `dequeue` in one test does mutate state; each test uses its own `fresh_db()`, so there is no cross-test bleed.
- **Cloud/root hosts:** the fixture tests need RE (`initdb` refuses root). If `buck2 test` hangs or the fixture fails to boot, that is the placement issue described in CLAUDE.md's Testing section, not this change.
