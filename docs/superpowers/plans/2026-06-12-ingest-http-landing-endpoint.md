# Ingest HTTP Landing Endpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an axum HTTP landing endpoint to `ingest` that decodes an Arrow IPC request and drives the existing `materialize` orchestrator, returning the new snapshot id.

**Architecture:** A new `ingest::http` module — `AppState { cp: Arc<dyn ControlPlane>, store: Arc<dyn ObjectStore> }`, a `router()`, and a `land` handler that decodes `POST /datasets/:schema/:table` (Arrow IPC body + optional `X-Loom-Model`/`X-Loom-Run-Id` headers) into a `MaterializeRequest`, calls `materialize`, and maps the `Result` to HTTP. No binary; tested in-process via tower `oneshot` with a `MemoryControlPlane` + a `LocalFileSystem`/`tempfile` store. Mirrors `query-api`'s built `http.rs`/`http_smoke`.

**Tech Stack:** Rust (edition 2024), axum, arrow (IPC reader/writer), object_store, serde/serde_json, time, uuid, buck2.

**Spec:** `docs/superpowers/specs/2026-06-12-ingest-http-landing-endpoint-design.md`

**Conventions (do not violate):**
- Tests are `rust_test`/`loom_fixture_test` integration targets in `tests/<name>.rs` — NEVER inline `#[cfg(test)]` (a prek hook enforces this). This slice's test is hermetic (no Postgres/DuckDB), so a plain `rust_test`, NOT `loom_fixture_test`.
- Run the suite with plain `buck2 test //src/...`. Do NOT pipe `buck2 test` through `tail` — redirect to a file and grep: `buck2 test //target > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`.
- rustfmt is CHECK-ONLY in hooks: run `buck2 run //tools:rustfmt -- <changed .rs files>` and apply before committing any `.rs`.
- NEVER `--no-verify`. Conventional Commits, ending with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Do NOT switch branches. Confirm `git branch --show-current` is `feat/ingest-http-landing` before and after each task.
- Adding a `Cargo.toml` dependency requires a `Cargo.lock` refresh or the `reindeer-check` hook fails. **This plan adds only `//third-party:*` aliases that already exist in `third-party/BUCK` (axum, serde, serde_json, time, uuid, tempfile, http-body-util, tower) to BUCK targets — no new crate, no `Cargo.toml`/lock change.** If a build error claims an alias is missing, STOP and report rather than editing `Cargo.toml`.

---

## File Structure

**Task 1 — happy path (un-modeled land) + wiring:**
- Create `src/services/ingest/src/http.rs` — `AppState`, `router()`, the `land` handler (Arrow IPC decode → `materialize` → `200`; decode failure → `400`; any `IngestError` → `500` for now), the `decode_ipc` helper.
- Modify `src/services/ingest/src/lib.rs` — add `pub mod http;`.
- Modify `src/services/ingest/BUCK` — add `axum`, `serde`, `serde_json`, `time`, `uuid` to the `ingest` lib deps; add the `http-land` `rust_test` target.
- Create `src/services/ingest/tests/http_land.rs` — the un-modeled success case with catalog read-back.

**Task 2 — model gate + run-id header + full error mapping:**
- Modify `src/services/ingest/src/http.rs` — add the `LandModel`/`LandColumn` DTO + `X-Loom-Model` parsing, `X-Loom-Run-Id` parsing, and split the `IngestError` match into `DoesNotConform` → `422` (+ violations), `Infer` → `400`, rest → `500`.
- Modify `src/services/ingest/tests/http_land.rs` — add the modeled-success, non-conforming-422, garbage-body-400, and bad-model-header-400 cases.

---

## Task 1: Happy-path landing endpoint + wiring

**Files:**
- Create: `src/services/ingest/src/http.rs`
- Modify: `src/services/ingest/src/lib.rs`
- Modify: `src/services/ingest/BUCK`
- Create: `src/services/ingest/tests/http_land.rs`

- [ ] **Step 1: Write the failing smoke test (un-modeled land)**

Create `src/services/ingest/tests/http_land.rs`:

```rust
//! Hermetic landing-endpoint smoke: POST an Arrow IPC stream, assert the land
//! happened end-to-end through `materialize` (memory control plane + a temp-dir
//! object store; tower oneshot, no socket / Postgres / DuckDB).

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{ControlPlane, TableRef};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use ingest::http::{AppState, router};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use tower::ServiceExt;

/// A 2-row batch: id: Int64 (required), name: Utf8 (nullable).
fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap()
}

/// Encode a batch as an Arrow IPC *stream* (schema + batch messages).
fn ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

fn app_state(dir: &std::path::Path) -> (Arc<dyn ControlPlane>, AppState) {
    let cp: Arc<dyn ControlPlane> = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    let state = AppState {
        cp: cp.clone(),
        store,
    };
    (cp, state)
}

#[tokio::test(flavor = "multi_thread")]
async fn unmodeled_land_succeeds_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let (cp, state) = app_state(dir.path());
    let res = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/datasets/main/customer")
                .body(Body::from(ipc_bytes(&sample_batch())))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["dataset"], "main.customer");
    let snapshot_id = json["snapshot_id"].as_i64().expect("snapshot_id is an integer");

    // Prove the land actually happened: the memory catalog now has a current
    // snapshot for the table, reached through the facade.
    let table = TableRef {
        schema: "main".into(),
        name: "customer".into(),
    };
    let snap = cp
        .catalog()
        .current_snapshot(&table)
        .await
        .expect("table has a current snapshot after landing");
    assert_eq!(snap.id.0, snapshot_id, "returned snapshot id matches the catalog");
}
```

- [ ] **Step 2: Add the `http-land` test target + lib deps to BUCK**

In `src/services/ingest/BUCK`:

(a) Add these aliases to the `ingest` `rust_library` `deps` (keep the existing `arrow`/`bytes`/`parquet`/`object_store`/`thiserror`/`core`):

```python
        "//third-party:axum",
        "//third-party:serde",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:uuid",
```

(b) Add a new test target (mirror the existing `materialize` `rust_test` block; this is a plain `rust_test`, NOT `loom_fixture_test` — the test is hermetic):

```python
rust_test(
    name = "http-land",
    crate = "http_land",
    srcs = ["tests/http_land.rs"],
    crate_root = "tests/http_land.rs",
    edition = "2024",
    deps = [
        ":ingest",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:arrow",
        "//third-party:axum",
        "//third-party:http-body-util",
        "//third-party:object_store",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:tokio",
        "//third-party:tower",
    ],
)
```

- [ ] **Step 3: Run the test — verify it FAILS to compile**

Run: `buck2 test //src/services/ingest:http-land > /tmp/t.log 2>&1; grep -E "error\[|unresolved|Tests finished|FAIL" /tmp/t.log`
Expected: failure — `ingest::http` does not exist yet.

- [ ] **Step 4: Create `http.rs` with the happy path**

Create `src/services/ingest/src/http.rs`:

```rust
//! HTTP landing surface for ingest. Decodes an Arrow IPC request into what
//! `materialize` consumes (schema + batches + optional model gate + lineage),
//! drives the in-process land pipeline, and maps the result to HTTP. All landing
//! logic lives in `materialize`; this layer only does decode <-> HTTP mapping.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::post;
use control_plane_core::{ControlPlane, DatasetId, EventType, LineageEvent, RunId, TableRef};
use object_store::ObjectStore;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::materialize::{MaterializeRequest, materialize};

/// Shared, owned dependencies: the control-plane facade + an object store.
#[derive(Clone)]
pub struct AppState {
    pub cp: Arc<dyn ControlPlane>,
    pub store: Arc<dyn ObjectStore>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/datasets/:schema/:table", post(land))
        .with_state(state)
}

/// Decode an Arrow IPC stream into its schema and record batches.
fn decode_ipc(body: &[u8]) -> Result<(Arc<Schema>, Vec<RecordBatch>), arrow::error::ArrowError> {
    let reader = StreamReader::try_new(std::io::Cursor::new(body), None)?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
}

async fn land(
    State(st): State<AppState>,
    Path((schema_name, table_name)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let (schema, batches) = match decode_ipc(&body) {
        Ok(sb) => sb,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid arrow ipc stream").into_response(),
    };

    let table = TableRef {
        schema: schema_name,
        name: table_name,
    };
    let file_name = format!("part-{}.parquet", Uuid::new_v4());
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(&table).dataset_ref()],
        payload: serde_json::json!({ "source": "http-land" }),
    };

    let req = MaterializeRequest {
        table: &table,
        schema,
        batches: &batches,
        file_name: &file_name,
        gate: None,
        lineage,
    };

    match materialize(st.cp.as_ref(), st.store.as_ref(), req).await {
        Ok(snap) => Json(serde_json::json!({
            "snapshot_id": snap.0,
            "dataset": format!("{}.{}", table.schema, table.name),
        }))
        .into_response(),
        // Refined into per-variant mapping in the next task.
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}
```

- [ ] **Step 5: Export the module**

In `src/services/ingest/src/lib.rs`, add to the module list (alongside `pub mod materialize;`):

```rust
pub mod http;
```

- [ ] **Step 6: Run the test — verify it PASSES**

Run: `buck2 test //src/services/ingest:http-land > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 1. Fail 0.`

If `arrow::ipc` fails to resolve (it should not — the `//third-party:arrow` alias enables the `ipc` feature), STOP and report; do not edit `Cargo.toml`.

- [ ] **Step 7: Format, lint, build, commit**

```bash
buck2 run //tools:rustfmt -- src/services/ingest/src/http.rs src/services/ingest/src/lib.rs src/services/ingest/tests/http_land.rs
tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -3 /tmp/clippy.log
buck2 build //src/... > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAILED" /tmp/b.log
git add -A && git commit -m "$(cat <<'EOF'
feat(ingest): HTTP landing endpoint (un-modeled path)

Add ingest::http with AppState (the ControlPlane facade + an object store),
a router, and a land handler: POST /datasets/:schema/:table decodes an Arrow IPC
stream and drives the in-process materialize orchestrator, returning the new
snapshot id. Hermetic smoke test (memory control plane + temp-dir store) asserts
the land happened end-to-end via a catalog read-back. Model gate + full error
mapping land in the next commit.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```
Expected: `BUILD SUCCEEDED`, clippy clean, commit created.

---

## Task 2: Model gate, run-id header, and full error mapping

**Files:**
- Modify: `src/services/ingest/src/http.rs`
- Modify: `src/services/ingest/tests/http_land.rs`

- [ ] **Step 1: Add the new test cases (failing)**

Append these tests to `src/services/ingest/tests/http_land.rs`. (Add `use ingest::http::{AppState, router};` is already present; these reuse the `sample_batch`/`ipc_bytes`/`app_state` helpers.)

```rust
fn model_header(json: &str) -> (axum::http::HeaderName, axum::http::HeaderValue) {
    (
        axum::http::HeaderName::from_static("x-loom-model"),
        axum::http::HeaderValue::from_str(json).unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn modeled_land_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let (_cp, state) = app_state(dir.path());
    let model = r#"{"columns":[{"name":"id","ty":"int64","required":true},{"name":"name","ty":"varchar","required":false}]}"#;
    let (hn, hv) = model_header(model);
    let res = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/datasets/main/customer")
                .header(hn, hv)
                .body(Body::from(ipc_bytes(&sample_batch())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn nonconforming_model_is_422_with_violations() {
    let dir = tempfile::tempdir().unwrap();
    let (cp, state) = app_state(dir.path());
    // Requires a column the batch does not have.
    let model = r#"{"columns":[{"name":"missing","ty":"int64","required":true}]}"#;
    let (hn, hv) = model_header(model);
    let res = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/datasets/main/customer")
                .header(hn, hv)
                .body(Body::from(ipc_bytes(&sample_batch())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["violations"][0]["column"], "missing");
    assert_eq!(json["violations"][0]["reason"], "missing_required");

    // Nothing was written.
    let table = TableRef {
        schema: "main".into(),
        name: "customer".into(),
    };
    assert!(
        cp.catalog().current_snapshot(&table).await.is_err(),
        "a rejected land writes no catalog rows"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn garbage_body_is_400() {
    let dir = tempfile::tempdir().unwrap();
    let (_cp, state) = app_state(dir.path());
    let res = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/datasets/main/customer")
                .body(Body::from(b"not arrow ipc".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_model_header_is_400() {
    let dir = tempfile::tempdir().unwrap();
    let (_cp, state) = app_state(dir.path());
    let (hn, hv) = model_header("not json");
    let res = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/datasets/main/customer")
                .header(hn, hv)
                .body(Body::from(ipc_bytes(&sample_batch())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}
```

Note: `nonconforming_model_is_422_with_violations` asserts `current_snapshot` is `Err` after rejection. That holds because the gate rejects *before* `cp.begin()`, so no table is ever created. If the memory adapter's `current_snapshot` returns `Ok` for an absent table in some form, adjust the assertion to check the returned value represents "no table/snapshot" — but per the catalog contract an absent table is a `NotFound` error.

- [ ] **Step 2: Run the tests — verify the new ones FAIL**

Run: `buck2 test //src/services/ingest:http-land > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: the modeled/422/400 cases fail (the handler ignores `X-Loom-Model` and maps every error to `500`); `unmodeled_land_succeeds_end_to_end` still passes.

- [ ] **Step 3: Add the model DTO + run-id parsing + error mapping to `http.rs`**

In `src/services/ingest/src/http.rs`:

(a) Add imports — `HeaderMap`, the gate types, and `serde::Deserialize`:

```rust
use axum::http::HeaderMap;
use control_plane_core::{ControlPlane, DatasetId, EventType, LineageEvent, RunId, TableRef};
use serde::Deserialize;

use crate::IngestError;
use crate::gate::{ColumnShape, ModelShape, Violation, ViolationReason};
use crate::materialize::{MaterializeRequest, materialize};
```

(b) Add the inbound model DTO + conversion and a violations-to-JSON helper (above `land`):

```rust
/// Inbound model wire DTO. The gate types are serde-free domain types, so the
/// HTTP layer owns this representation and converts.
#[derive(Deserialize)]
struct LandModel {
    columns: Vec<LandColumn>,
}

#[derive(Deserialize)]
struct LandColumn {
    name: String,
    ty: String,
    required: bool,
}

impl From<LandModel> for ModelShape {
    fn from(m: LandModel) -> Self {
        ModelShape {
            columns: m
                .columns
                .into_iter()
                .map(|c| ColumnShape {
                    name: c.name,
                    ty: c.ty,
                    required: c.required,
                })
                .collect(),
        }
    }
}

/// Build the 422 body from gate violations (the domain enum is serde-free).
fn violations_json(violations: &[Violation]) -> serde_json::Value {
    let items: Vec<serde_json::Value> = violations
        .iter()
        .map(|v| match &v.reason {
            ViolationReason::MissingRequired => {
                serde_json::json!({ "column": v.column, "reason": "missing_required" })
            }
            ViolationReason::TypeMismatch { expected, found } => serde_json::json!({
                "column": v.column,
                "reason": "type_mismatch",
                "expected": expected,
                "found": found,
            }),
            ViolationReason::Unsupported => {
                serde_json::json!({ "column": v.column, "reason": "unsupported" })
            }
        })
        .collect();
    serde_json::json!({ "violations": items })
}
```

(c) Replace the `land` signature and body to add `HeaderMap`, parse the two headers, pass the gate, and map errors per the spec table:

```rust
async fn land(
    State(st): State<AppState>,
    Path((schema_name, table_name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Optional model gate from X-Loom-Model (JSON).
    let gate: Option<ModelShape> = match headers.get("X-Loom-Model") {
        None => None,
        Some(v) => match v
            .to_str()
            .ok()
            .and_then(|s| serde_json::from_str::<LandModel>(s).ok())
        {
            Some(m) => Some(m.into()),
            None => return (StatusCode::BAD_REQUEST, "invalid X-Loom-Model").into_response(),
        },
    };

    // Optional run id from X-Loom-Run-Id.
    let run_id = match headers.get("X-Loom-Run-Id") {
        None => RunId(Uuid::new_v4()),
        Some(v) => match v.to_str().ok().and_then(|s| Uuid::parse_str(s).ok()) {
            Some(u) => RunId(u),
            None => return (StatusCode::BAD_REQUEST, "invalid X-Loom-Run-Id").into_response(),
        },
    };

    let (schema, batches) = match decode_ipc(&body) {
        Ok(sb) => sb,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid arrow ipc stream").into_response(),
    };

    let table = TableRef {
        schema: schema_name,
        name: table_name,
    };
    let file_name = format!("part-{}.parquet", Uuid::new_v4());
    let lineage = LineageEvent {
        run_id,
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(&table).dataset_ref()],
        payload: serde_json::json!({ "source": "http-land" }),
    };

    let req = MaterializeRequest {
        table: &table,
        schema,
        batches: &batches,
        file_name: &file_name,
        gate: gate.as_ref(),
        lineage,
    };

    match materialize(st.cp.as_ref(), st.store.as_ref(), req).await {
        Ok(snap) => Json(serde_json::json!({
            "snapshot_id": snap.0,
            "dataset": format!("{}.{}", table.schema, table.name),
        }))
        .into_response(),
        Err(IngestError::DoesNotConform(violations)) => {
            (StatusCode::UNPROCESSABLE_ENTITY, Json(violations_json(&violations))).into_response()
        }
        Err(IngestError::Infer(_)) => {
            (StatusCode::BAD_REQUEST, "unsupported column type").into_response()
        }
        // Opaque for backend faults: a governance-fronted service must not echo
        // internal detail (SQL, paths) to the client.
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
    }
}
```

- [ ] **Step 4: Run the tests — verify ALL pass**

Run: `buck2 test //src/services/ingest:http-land > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 5. Fail 0.`

- [ ] **Step 5: Format, lint, build, commit**

```bash
buck2 run //tools:rustfmt -- src/services/ingest/src/http.rs src/services/ingest/tests/http_land.rs
tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -3 /tmp/clippy.log
buck2 build //src/... > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAILED" /tmp/b.log
git add -A && git commit -m "$(cat <<'EOF'
feat(ingest): model gate + structured errors on the landing endpoint

Parse the optional X-Loom-Model (JSON -> ModelShape gate) and X-Loom-Run-Id
headers, and map outcomes per the wire contract: 200 + snapshot id; 422 +
violations for a non-conforming model; 400 for bad Arrow IPC / bad headers /
unsupported column type; 500 opaque for backend faults.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```
Expected: `BUILD SUCCEEDED`, clippy clean, commit created.

---

## Final Verification (after all tasks)

- [ ] Confirm branch: `git branch --show-current` → `feat/ingest-http-landing`.
- [ ] Full suite green (do NOT pipe to `tail`):

```bash
buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: all pass, 0 fail (the new `http-land` target's 5 tests included).

- [ ] `tools/clippy-all.sh` clean; `buck2 run //tools:prek -- run --all-files` green.
- [ ] Hand off via superpowers:finishing-a-development-branch (PR with `--base main`).
