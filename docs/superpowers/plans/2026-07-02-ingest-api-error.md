# Ingest typed ApiError + honest violations DTO — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace ingest's per-arm HTTP error handling with a single typed `ApiError` (logging structural in `IntoResponse`), one `IngestError → status` mapping (`into_api`), a generic `parse_header<T>`, and one wire-violation DTO (`WireViolation`) that is simultaneously the serialized 422 body and the OpenAPI documentation schema — all behavior-preserving (identical status codes and byte-identical response bodies).

**Architecture:** ingest's three handlers (`land`, `land_model`, `compact` in `src/services/ingest/src/http.rs`) currently return `Response` and inline four copies of the `IngestError → status` match, a hand-rolled `violations_json`, and two bespoke header parsers; several opaque-500 arms log nothing (defect `iss-ingest-model-500-unlogged`). This plan makes handlers return `Result<Response, ApiError>`. `ApiError`'s `IntoResponse` renders each variant and — for `Internal { context, detail }` — emits a `tracing::error!` server-side (logging becomes structural, not a per-arm chore, closing the defect *as a class*). `IngestError::into_api(context)` collapses the four copied matches. `parse_header<T>` collapses the two header parsers. `WireViolation` (fields declared alphabetically, optionals `skip_serializing_if`) replaces both `http::violations_json` and the doc-only `openapi::Violation`; because loom's `serde_json` has no `preserve_order` feature, `serde_json::Value` objects serialize with sorted keys, so a struct with alphabetically-ordered fields yields byte-identical JSON to the old `json!` shape.

**Tech Stack:** Rust 2024, axum 0.7.9 (`IntoResponse`, `Json`, `HeaderMap`, `StatusCode`), utoipa 5 (`ToSchema`), serde/serde_json, thiserror, tracing, buck2.

## Global Constraints

- **Tests are `rust_test` integration targets only, never inline `#[cfg(test)]`** — put every test in a sibling `tests/<name>.rs` wired as its own target in `src/services/ingest/BUCK`. The `no-inline-tests` prek hook fails on any `#[test]`/`#[tokio::test]` inside `src/**`.
- **Fixture-backed tests use `loom_fixture_test`, pure-logic tests use `rust_test`** (`loom_fixture_test` is already `load`-ed in `src/services/ingest/BUCK`).
- **Strict clippy (pedantic + restriction) on production code:** no `unwrap`/`expect`/`panic`/`indexing_slicing`/`todo`/`dbg`; do not drop an error's source silently where diagnostics matter; `#[allow]`/`#[expect]` require a `reason =`. Test code is exempted from the panic-safety lints via the `rust_test` wrapper.
- **Behavior-preserving:** every response's status code and body bytes stay identical for all existing paths. The only intended observable change is new server-side `tracing::error!` lines on the internal-fault arms and the OpenAPI component schema rename `Violation` → `WireViolation`.
- Run buck2 test scoped to ingest, redirecting to a file (never pipe `buck2 test` through `head`/`tail`): `buck2 test //src/services/ingest/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`.
- Commit messages follow Conventional Commits (a `commit-msg` prek hook enforces this).

---

## File Structure

- `src/services/ingest/src/openapi.rs` — **modify.** Replace the doc-only `Violation` struct with `WireViolation` (derives `Serialize + ToSchema`); make `ViolationsBody` hold `Vec<WireViolation>` and derive `Serialize`; add `From<&crate::gate::Violation> for WireViolation`; update `components(schemas(...))`. This becomes the single domain→wire conversion point.
- `src/services/ingest/src/http.rs` — **modify.** Add `pub enum ApiError` + `impl IntoResponse` + `ApiError::internal` constructor; add `impl IngestError { pub fn into_api(self, context: &'static str) -> ApiError }`; add `parse_header<T>`; rewrite `land`, `land_model`, `compact` to return `Result<Response, ApiError>`; delete `violations_json`.
- `src/services/ingest/tests/wire_violation.rs` — **create.** Pure test: `WireViolation`/`ViolationsBody` serialize byte-identically to the old hand-rolled `json!` shape for all four `ViolationReason`s.
- `src/services/ingest/tests/api_error.rs` — **create.** Pure test: each `ApiError` variant → its status code; `IngestError::into_api` maps `DoesNotConform`→422, `NoSnapshot`→500.
- `src/services/ingest/tests/http_land.rs` — **modify.** Add `bad_run_id_is_400` covering the `X-Loom-Run-Id` parse path that currently has no test (locks `parse_header` behavior).
- `src/services/ingest/BUCK` — **modify.** Add `wire-violation` and `api-error` `rust_test` targets.

---

## Task 1: `WireViolation` — one wire-violation DTO (byte-identical, doubles as OpenAPI schema)

**Files:**
- Modify: `src/services/ingest/src/openapi.rs`
- Create/Test: `src/services/ingest/tests/wire_violation.rs`
- Modify: `src/services/ingest/BUCK`

**Interfaces:**
- Consumes: `crate::gate::{Violation, ViolationReason}` (re-exported as `ingest::{Violation, ViolationReason}`). `ViolationReason` variants: `MissingRequired`, `TypeMismatch { expected: String, found: String }`, `Unsupported`, `Constraint { rule: String }`. `Violation { column: String, reason: ViolationReason }`.
- Produces (used by Task 2 and the handler rewrite in Task 3):
  - `pub struct WireViolation { pub column: String, pub expected: Option<String>, pub found: Option<String>, pub reason: String, pub rule: Option<String> }` — **fields in this exact (alphabetical) order**, deriving `serde::Serialize + utoipa::ToSchema`, with `#[serde(skip_serializing_if = "Option::is_none")]` on `expected`, `found`, `rule`.
  - `impl From<&crate::gate::Violation> for WireViolation`.
  - `pub struct ViolationsBody { pub violations: Vec<WireViolation> }` deriving `serde::Serialize + utoipa::ToSchema`.

- [ ] **Step 1: Write the failing test** — `src/services/ingest/tests/wire_violation.rs`

```rust
//! The wire-violation DTO (`WireViolation`/`ViolationsBody`) must serialize
//! byte-identically to the hand-rolled `serde_json::json!` shape it replaced, so
//! the 422 body on the wire is unchanged. Pure: no router/Postgres. loom's
//! serde_json has no `preserve_order`, so `Value` objects sort their keys — the
//! reference below is therefore key-sorted, matching `WireViolation`'s
//! alphabetically-declared fields.

use ingest::openapi::{ViolationsBody, WireViolation};
use ingest::{Violation, ViolationReason};

/// The pre-refactor hand-rolled shape (a verbatim copy of the deleted
/// `http::violations_json`), used only as the byte-identity oracle.
fn reference_json(violations: &[Violation]) -> serde_json::Value {
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
            ViolationReason::Constraint { rule } => {
                serde_json::json!({ "column": v.column, "reason": "constraint", "rule": rule })
            }
        })
        .collect();
    serde_json::json!({ "violations": items })
}

fn all_reasons() -> Vec<Violation> {
    vec![
        Violation { column: "a".into(), reason: ViolationReason::MissingRequired },
        Violation {
            column: "b".into(),
            reason: ViolationReason::TypeMismatch { expected: "long".into(), found: "string".into() },
        },
        Violation { column: "c".into(), reason: ViolationReason::Unsupported },
        Violation {
            column: "d".into(),
            reason: ViolationReason::Constraint { rule: "pattern".into() },
        },
    ]
}

#[test]
fn wire_body_is_byte_identical_to_the_hand_rolled_shape() {
    let violations = all_reasons();
    let body = ViolationsBody {
        violations: violations.iter().map(WireViolation::from).collect(),
    };
    let got = serde_json::to_string(&body).unwrap();
    let want = serde_json::to_string(&reference_json(&violations)).unwrap();
    assert_eq!(got, want);
}

#[test]
fn absent_reason_fields_are_omitted_not_null() {
    let body = ViolationsBody {
        violations: vec![WireViolation::from(&Violation {
            column: "a".into(),
            reason: ViolationReason::MissingRequired,
        })],
    };
    let s = serde_json::to_string(&body).unwrap();
    assert_eq!(s, r#"{"violations":[{"column":"a","reason":"missing_required"}]}"#);
    assert!(!s.contains("null"), "skip_serializing_if must drop absent fields");
}
```

- [ ] **Step 2: Add the BUCK target** — append to `src/services/ingest/BUCK` (near the other pure `rust_test` targets):

```python
# WireViolation byte-identical wire shape — pure logic, RE-eligible (no fixture).
rust_test(
    name = "wire-violation",
    crate = "wire_violation",
    srcs = ["tests/wire_violation.rs"],
    crate_root = "tests/wire_violation.rs",
    edition = "2024",
    deps = [
        ":ingest",
        "//third-party:serde_json",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/services/ingest:wire-violation > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|cannot find" /tmp/t.log`
Expected: build/compile failure — `WireViolation` / `ViolationsBody` do not exist yet (`cannot find ... in ... openapi`).

- [ ] **Step 4: Rewrite `openapi.rs`** to the following (replaces the `Violation` struct, updates `ViolationsBody`, adds `Serialize` + the `From` conversion, updates `components`):

```rust
//! ingest's static OpenAPI document. `build_openapi()` returns it as a value (the
//! ontology hook for slice 2). DTOs are documentation shapes for the JSON the
//! handlers emit; the request body is binary Arrow IPC, documented as such.

use serde::Serialize;
use utoipa::{OpenApi, ToSchema};

use crate::gate::{Violation, ViolationReason};

/// Documentation shape for the land 200 acknowledgement.
#[derive(ToSchema)]
pub struct LandAck {
    /// The committed Iceberg snapshot id.
    pub snapshot_id: i64,
    /// `schema.table` of the landed dataset.
    pub dataset: String,
}

/// Documentation shape for the typed-model land 200 acknowledgement.
#[derive(ToSchema)]
pub struct ModelLandAck {
    /// The committed Iceberg snapshot id.
    pub snapshot_id: i64,
    /// The ontology type the rows were landed as.
    #[schema(rename = "type")]
    pub type_name: String,
}

/// Documentation shape for a 202 job-enqueue acknowledgement.
#[derive(ToSchema)]
pub struct JobAck {
    /// The enqueued job's id (UUID string).
    pub job_id: String,
}

/// The 422 model-gate violations body: `{ "violations": [ WireViolation, ... ] }`.
/// Serializes on the wire AND documents the response schema — one shape, no drift.
#[derive(Serialize, ToSchema)]
pub struct ViolationsBody {
    pub violations: Vec<WireViolation>,
}

/// One gate violation on the wire (a column, the reason token, and the
/// reason-specific fields). This single type is BOTH the serialized 422 body
/// element and the OpenAPI documentation schema, replacing the former split
/// between `http::violations_json` and a doc-only struct.
///
/// Fields are declared alphabetically and the reason-specific ones are
/// `skip_serializing_if` — loom's `serde_json` has no `preserve_order`, so the
/// `serde_json::Value` shape this replaced serialized its keys sorted; matching
/// that order + omitting absent keys keeps the bytes identical.
#[derive(Serialize, ToSchema)]
pub struct WireViolation {
    /// The offending column.
    pub column: String,
    /// The model-declared type — present only for `type_mismatch`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<String>,
    /// The inferred Arrow type — present only for `type_mismatch`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub found: Option<String>,
    /// `missing_required` | `type_mismatch` | `unsupported` | `constraint`.
    pub reason: String,
    /// The failed constraint rule (`range`|`length`|`pattern`|`one_of`) — present
    /// only for `constraint`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule: Option<String>,
}

impl From<&Violation> for WireViolation {
    fn from(v: &Violation) -> Self {
        match &v.reason {
            ViolationReason::MissingRequired => WireViolation {
                column: v.column.clone(),
                expected: None,
                found: None,
                reason: "missing_required".to_string(),
                rule: None,
            },
            ViolationReason::TypeMismatch { expected, found } => WireViolation {
                column: v.column.clone(),
                expected: Some(expected.clone()),
                found: Some(found.clone()),
                reason: "type_mismatch".to_string(),
                rule: None,
            },
            ViolationReason::Unsupported => WireViolation {
                column: v.column.clone(),
                expected: None,
                found: None,
                reason: "unsupported".to_string(),
                rule: None,
            },
            ViolationReason::Constraint { rule } => WireViolation {
                column: v.column.clone(),
                expected: None,
                found: None,
                reason: "constraint".to_string(),
                rule: Some(rule.clone()),
            },
        }
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "loom ingest",
        description = "Arrow-IPC landing + compaction API"
    ),
    paths(crate::http::land, crate::http::land_model, crate::http::compact),
    components(schemas(LandAck, ModelLandAck, JobAck, ViolationsBody, WireViolation))
)]
pub struct ApiDoc;

/// Build the static OpenAPI document (a value — the slice-2 ontology hook).
#[must_use]
pub fn build_openapi() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test //src/services/ingest:wire-violation > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 2. Fail 0.`

Note: `http.rs` still uses `violations_json` and still compiles (it imports `ViolationsBody` from openapi only for the `#[utoipa::path]` `body = ViolationsBody` annotation, which still resolves). The openapi doc drift test (`//src/services/ingest:openapi`) checks routes, not schema names, so it stays green.

- [ ] **Step 6: Verify the openapi + full ingest build is unbroken**

Run: `buck2 test //src/services/ingest:openapi > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 2. Fail 0.`

- [ ] **Step 7: Commit**

```bash
git add src/services/ingest/src/openapi.rs src/services/ingest/tests/wire_violation.rs src/services/ingest/BUCK
git commit -m "refactor(ingest): one WireViolation DTO for the 422 body + OpenAPI schema"
```

---

## Task 2: `ApiError` type + `IntoResponse` + `IngestError::into_api`

**Files:**
- Modify: `src/services/ingest/src/http.rs`
- Create/Test: `src/services/ingest/tests/api_error.rs`
- Modify: `src/services/ingest/BUCK`

**Interfaces:**
- Consumes: `crate::IngestError` (variants `DoesNotConform(Vec<Violation>)`, `Infer(_)`, `Write(_)`, `ControlPlane(_)`, `NoSnapshot`), `crate::openapi::{ViolationsBody, WireViolation}`, `crate::gate::Violation`.
- Produces (used by Task 3):
  - `pub enum ApiError { BadRequest(std::borrow::Cow<'static, str>), Forbidden, Violations(Vec<crate::gate::Violation>), Internal { context: &'static str, detail: String } }`.
  - `impl ApiError { pub fn internal(context: &'static str, e: impl std::fmt::Display) -> Self }`.
  - `impl axum::response::IntoResponse for ApiError` — `BadRequest`→400 (message body), `Forbidden`→403 (empty body), `Violations`→422 (`ViolationsBody` JSON), `Internal`→`tracing::error!(error = %detail, "{context}")` then 500 `"internal error"`.
  - `impl IngestError { pub fn into_api(self, context: &'static str) -> ApiError }` — `DoesNotConform(v)`→`Violations(v)`, `Infer(_)`→`BadRequest("unsupported column type")`, everything else→`ApiError::internal(context, self)`.

- [ ] **Step 1: Write the failing test** — `src/services/ingest/tests/api_error.rs`

```rust
//! `ApiError` variant → HTTP status mapping and `IngestError::into_api` routing.
//! Pure: `IntoResponse::into_response` is synchronous and `Response::status()`
//! needs no body collection, so no Postgres/tokio.

use axum::http::StatusCode;
use axum::response::IntoResponse;
use ingest::IngestError;
use ingest::http::ApiError;

fn status_of(e: ApiError) -> StatusCode {
    e.into_response().status()
}

#[test]
fn bad_request_is_400() {
    assert_eq!(status_of(ApiError::BadRequest("nope".into())), StatusCode::BAD_REQUEST);
}

#[test]
fn forbidden_is_403() {
    assert_eq!(status_of(ApiError::Forbidden), StatusCode::FORBIDDEN);
}

#[test]
fn violations_is_422() {
    assert_eq!(status_of(ApiError::Violations(vec![])), StatusCode::UNPROCESSABLE_ENTITY);
}

#[test]
fn internal_is_500() {
    assert_eq!(
        status_of(ApiError::internal("ctx", "boom")),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[test]
fn into_api_maps_conformance_to_422() {
    assert_eq!(
        IngestError::DoesNotConform(vec![]).into_api("ctx").into_response().status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[test]
fn into_api_maps_no_snapshot_to_500() {
    assert_eq!(
        IngestError::NoSnapshot.into_api("ctx").into_response().status(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}
```

- [ ] **Step 2: Add the BUCK target** — append to `src/services/ingest/BUCK`:

```python
# ApiError status mapping + IngestError::into_api — pure logic, RE-eligible.
rust_test(
    name = "api-error",
    crate = "api_error",
    srcs = ["tests/api_error.rs"],
    crate_root = "tests/api_error.rs",
    edition = "2024",
    deps = [
        ":ingest",
        "//third-party:axum",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/services/ingest:api-error > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|cannot find" /tmp/t.log`
Expected: compile failure — `ApiError` / `into_api` do not exist yet.

- [ ] **Step 4: Add `ApiError` + impls to `http.rs`.** Insert the following block right after the imports (before `struct AppState`), and add `use std::borrow::Cow;` and `use crate::openapi::{ViolationsBody, WireViolation};` to the imports (the existing `use crate::openapi::{JobAck, LandAck, ModelLandAck, ViolationsBody};` line becomes `use crate::openapi::{JobAck, LandAck, ModelLandAck, ViolationsBody, WireViolation};`):

```rust
/// The HTTP error surface for ingest handlers. Handlers return
/// `Result<Response, ApiError>`; `IntoResponse` renders each variant, and for
/// `Internal` it logs the fault detail server-side — so fault logging is
/// structural (one place), not a per-arm chore. The client-facing bytes match
/// the former hand-rolled responses exactly (opaque `"internal error"` for 500,
/// empty 403, the message for 400, the `ViolationsBody` JSON for 422).
pub enum ApiError {
    /// A deterministic client error with a safe, client-visible message (bad IPC,
    /// bad header, unsupported column type, identity names an absent column).
    BadRequest(Cow<'static, str>),
    /// ACL deny — 403 with an empty body (no existence leak).
    Forbidden,
    /// Model-gate / conformance failures — the 422 body listing the violations.
    Violations(Vec<crate::gate::Violation>),
    /// A backend/internal fault. `detail` is logged server-side (operator-only);
    /// the response body stays the opaque `"internal error"`.
    Internal { context: &'static str, detail: String },
}

impl ApiError {
    /// Build an `Internal` fault, capturing `e`'s `Display` for the server-side log.
    pub fn internal(context: &'static str, e: impl std::fmt::Display) -> Self {
        ApiError::Internal { context, detail: e.to_string() }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            ApiError::Forbidden => StatusCode::FORBIDDEN.into_response(),
            ApiError::Violations(violations) => {
                let body = ViolationsBody {
                    violations: violations.iter().map(WireViolation::from).collect(),
                };
                (StatusCode::UNPROCESSABLE_ENTITY, Json(body)).into_response()
            }
            ApiError::Internal { context, detail } => {
                // The single place ingest logs a backend fault: opaque to the
                // client, diagnosable for the operator. Closes iss-ingest-model-500-unlogged.
                tracing::error!(error = %detail, "{context}");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
            }
        }
    }
}

impl IngestError {
    /// Map a landing fault onto the HTTP surface: a conformance failure is the
    /// 422 body, an unsupported-column-type infer error is a 400 (client data),
    /// and any backend fault is an opaque 500 logged with `context`.
    pub fn into_api(self, context: &'static str) -> ApiError {
        match self {
            IngestError::DoesNotConform(violations) => ApiError::Violations(violations),
            IngestError::Infer(_) => ApiError::BadRequest(Cow::Borrowed("unsupported column type")),
            other => ApiError::internal(context, other),
        }
    }
}
```

`use crate::IngestError;` is already present (line 25); confirm `axum::response::{IntoResponse, Json, Response}` and `axum::http::StatusCode` are imported (they are, lines 14-16).

**Also add the `tracing` dep to the ingest lib.** `tracing::error!` is a macro *path*, so `tracing` must be a **direct** dependency of the `ingest` `rust_library` — buck2 only `--extern`s direct deps, and today ingest does not depend on `tracing` (only `main.rs` calls `service_runtime::init_tracing()`). Add `"//third-party:tracing",` to the `rust_library`'s `deps` in `src/services/ingest/BUCK` (the block at lines ~10-29, alongside the other `//third-party:*` entries). Without this, the Step 5 build fails with `error[E0433]: failed to resolve: use of undeclared crate or module tracing`. `//third-party:tracing` already exists in `third-party/BUCK`.

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test //src/services/ingest:api-error > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|warning:" /tmp/t.log`
Expected: `Tests finished: Pass 6. Fail 0.`

Note: `ApiError` and `into_api` are `pub` but not yet used by the handlers. Because they are `pub`, no `dead_code` warning fires; `violations_json` is still present and used. The whole ingest lib must still build clean.

- [ ] **Step 6: Verify the ingest lib clippy is clean**

Run: `buck2 build '//src/services/ingest:ingest[clippy.txt]' > /tmp/c.log 2>&1; cat $(buck2 build --show-output '//src/services/ingest:ingest[clippy.txt]' 2>/dev/null | awk '{print $2}') 2>/dev/null; grep -E "error|warning" /tmp/c.log`
Expected: no clippy findings (empty `clippy.txt`).

- [ ] **Step 7: Commit**

```bash
git add src/services/ingest/src/http.rs src/services/ingest/tests/api_error.rs src/services/ingest/BUCK
git commit -m "refactor(ingest): typed ApiError with structural fault logging"
```

---

## Task 3: Rewrite handlers over `ApiError` + `parse_header`; delete `violations_json`

**Files:**
- Modify: `src/services/ingest/src/http.rs`
- Modify: `src/services/ingest/tests/http_land.rs` (add `bad_run_id_is_400`)

**Interfaces:**
- Consumes: `ApiError`, `ApiError::internal`, `IngestError::into_api` (Task 2); `WireViolation`/`ViolationsBody` (Task 1, via `ApiError::Violations`).
- Produces: `land`, `land_model`, `compact` all return `Result<Response, ApiError>`. New private helper `fn parse_header<T>(headers: &HeaderMap, name: &str, err: &'static str, parse: impl Fn(&str) -> Option<T>) -> Result<Option<T>, ApiError>`.

- [ ] **Step 1: Write the failing test** — add to `src/services/ingest/tests/http_land.rs` (after `bad_model_header_is_400`):

```rust
#[tokio::test(flavor = "multi_thread")]
async fn bad_run_id_is_400() {
    let fx = PgFixture::shared();
    let (_seed, db) = fx.fresh_db().await;
    let (_cp, _pool, _wh, state) = app_state(fx, &db).await;
    let res = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/datasets/main/customer")
                .header("x-loom-run-id", "not-a-uuid")
                .body(Body::from(ipc_bytes(&sample_batch())))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}
```

- [ ] **Step 2: Run it to verify it passes against the CURRENT code** (this is a characterization test locking existing behavior before the refactor)

Run: `buck2 test //src/services/ingest:http-land > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS (the current `land` already 400s on a bad `X-Loom-Run-Id`). This test now guards the `parse_header` migration.

- [ ] **Step 3: Add `parse_header` to `http.rs`** — insert after `decode_ipc`:

```rust
/// Parse an optional request header via `parse`: absent → `Ok(None)`, present and
/// parseable → `Ok(Some(_))`, present but unparseable → `Err(ApiError::BadRequest(err))`.
/// Collapses the per-header `match headers.get(..)` ladders into one shape.
fn parse_header<T>(
    headers: &HeaderMap,
    name: &str,
    err: &'static str,
    parse: impl Fn(&str) -> Option<T>,
) -> Result<Option<T>, ApiError> {
    match headers.get(name) {
        None => Ok(None),
        Some(v) => match v.to_str().ok().and_then(parse) {
            Some(t) => Ok(Some(t)),
            None => Err(ApiError::BadRequest(std::borrow::Cow::Borrowed(err))),
        },
    }
}
```

- [ ] **Step 4: Rewrite `land`** to return `Result<Response, ApiError>` — replace the whole `pub(crate) async fn land(...)` body (keep the `#[utoipa::path]` attribute unchanged):

```rust
pub(crate) async fn land(
    State(st): State<AppState>,
    Path((schema_name, table_name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Optional model gate from X-Loom-Model (JSON) and run id from X-Loom-Run-Id.
    let gate: Option<ModelShape> = parse_header(&headers, "X-Loom-Model", "invalid X-Loom-Model", |s| {
        serde_json::from_str::<LandModel>(s).ok().map(ModelShape::from)
    })?;
    let run_id = parse_header(&headers, "X-Loom-Run-Id", "invalid X-Loom-Run-Id", |s| {
        Uuid::parse_str(s).ok().map(RunId)
    })?
    .unwrap_or_else(|| RunId(Uuid::new_v4()));

    let (schema, batches) =
        decode_ipc(&body).map_err(|_| ApiError::BadRequest(Cow::Borrowed("invalid arrow ipc stream")))?;

    let table = TableRef { schema: schema_name, name: table_name };
    let file_prefix = Uuid::new_v4().to_string();
    let lineage = LineageEvent {
        run_id,
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetId::from(&table).dataset_ref()],
        payload: serde_json::json!({ "source": "http-land" }),
    };

    // Gate + schema resolution: backend-agnostic, run once before dispatch.
    let columns = resolve_columns(&schema, gate.as_ref()).map_err(|e| e.into_api("ingest land: resolve columns"))?;

    let req = LandRequest {
        table: &table,
        schema: schema.clone(),
        columns: &columns,
        batches: &batches,
        ipc_body: body.as_ref(),
        file_prefix: &file_prefix,
        lineage,
    };

    let snap = st.materializer.land(req).await.map_err(|e| e.into_api("ingest land: materialize"))?;
    Ok(Json(serde_json::json!({
        "snapshot_id": snap.0,
        "dataset": format!("{}.{}", table.schema, table.name),
    }))
    .into_response())
}
```

Note: `Cow` is used directly here; ensure `use std::borrow::Cow;` was added in Task 2. `decode_ipc`'s error is dropped in the 400 arm exactly as before (bad IPC is a client error, not logged — behavior-preserving).

- [ ] **Step 5: Rewrite `land_model`** to return `Result<Response, ApiError>` — replace the whole `pub(crate) async fn land_model(...)` body (keep the `#[utoipa::path]` attribute unchanged):

```rust
pub(crate) async fn land_model(
    State(st): State<AppState>,
    Path(type_name): Path<String>,
    Query(q): Query<ModelQuery>,
    subject: Subject,
    body: Bytes,
) -> Result<Response, ApiError> {
    let type_name = TypeName(type_name);

    // 1. Coarse ACL gate BEFORE anything is revealed. An authenticated subject
    //    without a Write grant is 403 whether or not the type exists (no leak).
    match st
        .cp
        .acl()
        .check(&subject.0, Action::Write, &PolicyTarget::Type(type_name.clone()))
        .await
    {
        Ok(Decision::Allow) => {}
        Ok(Decision::Deny) => return Err(ApiError::Forbidden),
        Err(e) => return Err(ApiError::internal("ingest model: acl check", e)),
    }

    // 2. Decode the Arrow IPC body (both branches need the schema).
    let (schema, batches) =
        decode_ipc(&body).map_err(|_| ApiError::BadRequest(Cow::Borrowed("invalid arrow ipc stream")))?;

    // 3. Resolve the type, or — when absent and authorized — infer, create, re-resolve.
    let otype = match st.cp.ontology().get_type(&type_name).await {
        Ok(t) => t,
        Err(ControlPlaneError::NotFound(_)) => {
            let inferred = infer_object_type(&type_name, &schema, q.identity.as_deref()).map_err(|e| match e {
                InferTypeError::UnsupportedColumns(violations) => ApiError::Violations(violations),
                InferTypeError::IdentityNotFound(col) => ApiError::BadRequest(Cow::Owned(format!(
                    "identity column `{col}` is not present in the batch"
                ))),
            })?;
            st.cp
                .ontology()
                .define_type(inferred)
                .await
                .map_err(|e| ApiError::internal("ingest model: define_type (infer-and-create)", e))?;
            st.cp
                .ontology()
                .get_type(&type_name)
                .await
                .map_err(|e| ApiError::internal("ingest model: re-resolve type after define_type", e))?
        }
        Err(e) => return Err(ApiError::internal("ingest model: get_type", e)),
    };

    // 4. Derive the conformance shape from the (resolved or just-created) type.
    let shape = model_shape_from_type(&otype);

    // 5. Gate + resolve the physical schema (422 + violations on mismatch).
    let columns = resolve_columns(&schema, Some(&shape)).map_err(|e| e.into_api("ingest model: resolve columns"))?;

    // 5b. Per-value constraint validation over the decoded batches (422 on violation).
    validate_values(&shape, &batches).map_err(ApiError::Violations)?;

    // 6. Land into the type's table with type-named lineage.
    let table = otype.table.clone();
    let type_label = type_name.0.clone();
    let file_prefix = Uuid::new_v4().to_string();
    let lineage = LineageEvent {
        run_id: RunId(Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: OffsetDateTime::now_utc(),
        inputs: vec![],
        outputs: vec![DatasetRef::from(&type_name)],
        payload: serde_json::json!({ "source": "http-model", "type": type_label }),
    };
    let req = LandRequest {
        table: &table,
        schema: schema.clone(),
        columns: &columns,
        batches: &batches,
        ipc_body: body.as_ref(),
        file_prefix: &file_prefix,
        lineage,
    };

    let snap = st.materializer.land(req).await.map_err(|e| e.into_api("ingest model: materialize"))?;
    Ok(Json(serde_json::json!({
        "snapshot_id": snap.0,
        "type": type_label,
    }))
    .into_response())
}
```

- [ ] **Step 6: Rewrite `compact`** to return `Result<Response, ApiError>` (keep the `#[utoipa::path]` attribute unchanged):

```rust
pub(crate) async fn compact(
    State(st): State<AppState>,
    Path((schema, table)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let payload = serde_json::to_value(CompactJob { schema, name: table })
        .map_err(|e| ApiError::internal("ingest compact: serialize job payload", e))?;
    let job = NewJob {
        kind: COMPACT_JOB_KIND.to_string(),
        payload,
        run_at: None,
        priority: 0,
    };
    let id = st
        .cp
        .queue()
        .enqueue(job)
        .await
        .map_err(|e| ApiError::internal("ingest compact: enqueue job", e))?;
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "job_id": id.0.to_string() })),
    )
        .into_response())
}
```

- [ ] **Step 7: Delete `violations_json`** — remove the entire `fn violations_json(...)` function (lines ~142-165 of the original) and its `use crate::gate::{... Violation, ViolationReason ...}` items that are now unused. After the rewrite, `http.rs` still needs `validate_values`, `ColumnShape`, `ModelShape` from `crate::gate`, but no longer `Violation`/`ViolationReason` directly (they now live behind `ApiError::Violations` / the openapi `From`). Adjust the import `use crate::gate::{ColumnShape, ModelShape, Violation, ViolationReason, validate_values};` to `use crate::gate::{ColumnShape, ModelShape, validate_values};`. Also drop the now-unused `use crate::IngestError;` **only if** unused — it is still used by `into_api`'s `impl IngestError` block, so keep it.

- [ ] **Step 8: Run the ingest test suite**

Run: `buck2 test //src/services/ingest/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|warning:" /tmp/t.log`
Expected: all ingest tests pass — `http_land` (incl. new `bad_run_id_is_400`), `http_model`, `compact_endpoint`, `wire_violation`, `api_error`, `openapi`, and the rest. `Fail 0`.

- [ ] **Step 9: Verify clippy is clean on the ingest lib**

Run: `./tools/clippy-all.sh 2>&1 | tail -20` (or scope: `buck2 build '//src/services/ingest:ingest[clippy.txt]'` and confirm the output file is empty)
Expected: no clippy findings for ingest.

- [ ] **Step 10: Commit**

```bash
git add src/services/ingest/src/http.rs src/services/ingest/tests/http_land.rs
git commit -m "refactor(ingest): handlers over ApiError + parse_header; drop violations_json

Closes the class of unlogged opaque-500 arms (iss-ingest-model-500-unlogged):
every backend fault now routes through ApiError::Internal, which logs the
detail server-side in IntoResponse. Behavior-preserving: status codes and
response bodies unchanged."
```

---

## Task 4: Close the register item (loom-docs-update at finish)

This is done in the finishing step, not as a code task. In the PR:
- `docs/ISSUES.md`: flip `iss-ingest-model-500-unlogged` `- [ ]`→`- [x]`, `status:open`→`status:fixed`, add `pr:#N`.
- `docs/ROADMAP.md`: flip `road-ingest-api-error` `- [ ]`→`- [x]`, `status:planned`→`status:done`, add `pr:#N`.
- Record the follow-on (`service_runtime` hoist) as a FUTURE item if not already tracked (it is out of scope per the spec).

Run `bash tools/docs.sh validate` after editing the registers.

---

## Verification (whole-plan)

1. `buck2 build //src/services/ingest/... > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|FAIL|error" /tmp/b.log` — clean build.
2. `buck2 test //src/services/ingest/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log` — all pass.
3. `./tools/clippy-all.sh` — no findings.
4. Confirm behavior-preservation by inspection against the original: land 200/400/422; land_model 200/400/403/422/500; compact 202/500 — all status codes and bodies unchanged; the only new observable is `tracing::error!` on internal-fault arms.
```

## Self-Review

**1. Spec coverage** (spec section `road-ingest-api-error`):
- "Handlers return `Result<Response, ApiError>`" → Task 3 (all three handlers).
- "`ApiError::Internal { context, source }` logs structurally in `IntoResponse`" → Task 2 (`Internal { context, detail }`, logged in `IntoResponse`; `detail` captures the source's `Display`). *Note: spec says `source`; plan uses `detail: String` capturing the source's Display — functionally the structured `error = %..` log the spec requires, and avoids a boxed-error field. Documented deviation.*
- "closing `iss-ingest-model-500-unlogged` as a class — the 4×-copied `IngestError → status` mapping becomes one `into_api`" → Task 2 (`into_api`) + Task 3 (all four call sites + the non-IngestError 500 arms now `ApiError::internal`, all logged).
- "`Violations(Vec<_>)` owns the 422 body" → Task 2 (`ApiError::Violations`, rendered in `IntoResponse`).
- "Header parsing collapses into `parse_header<T>`" → Task 3.
- "`violations_json` + parallel doc-only OpenAPI `Violation` become one `WireViolation` deriving `Serialize + ToSchema` (byte-identical JSON asserted in tests)" → Task 1 (`WireViolation`, byte-identity test).
- "the single conversion point that will absorb `fut-conformance-enum-consolidation`" → Task 1 (`From<&Violation> for WireViolation` is the one place).
- "Follow-on (not in scope): hoist into `service_runtime`" → Task 4 records as FUTURE; not implemented.

**2. Placeholder scan:** No TBD/TODO/"handle errors appropriately"; every code step shows full code. ✓

**3. Type consistency:** `ApiError` variants and `into_api`/`internal`/`parse_header` signatures are identical across Tasks 2 and 3. `WireViolation`/`ViolationsBody` field names/order identical across Task 1 and its use in Task 2's `IntoResponse`. `ViolationReason` variant shapes match `gate.rs`. ✓
