# Serving-fault server-side logging — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Install a `tracing-subscriber` on both service binaries and log backend/serving faults server-side with full detail while keeping the opaque `"internal error"` client response unchanged. Closes `iss-serving-faults-not-logged`.

**Spec:** `docs/superpowers/specs/2026-06-21-serving-fault-logging-design.md`

**Architecture:** Two orthogonal changes land together: (1) `service_runtime::init_tracing()` — a shared idempotent subscriber installer used by both binaries; (2) a shared `internal_error(context, e)` helper in `query-api/src/http.rs` that logs the detail and returns the opaque 500. The control-plane spans already emit via the `tracing` facade; they just have no subscriber until now.

**Tech Stack:** Rust, buck2, axum, tracing-subscriber. Tests are `rust_test` integration targets (no inline `#[cfg(test)]`). The `init_tracing` smoke test is a plain (non-fixture) `rust_test`; the `serving_fault_logging` test uses `#[traced_test]` from `tracing-test`.

## Global Constraints

- **Tests are `rust_test` integration targets only** — NO inline `#[cfg(test)]`/`#[test]` in `src/**.rs` (the `no-inline-tests` prek hook fails).
- **`tracing-subscriber` is already in `third-party/BUCK`** with `env-filter` feature enabled — no `buckify.sh` run needed for this dep.
- **`tracing-test` is already in `third-party/BUCK`** — no new third-party dep.
- **No Cargo.toml changes needed** — `src/services/runtime` is not a Cargo workspace member; deps are BUCK-only.
- **Lockfile footgun does not apply here** — no new Cargo dep is added; `Cargo.lock` and `third-party/BUCK` are unchanged.
- **Client response bodies are byte-for-byte unchanged** — the four opaque-500 arms keep `"internal error"` exactly.
- Commit messages: Conventional Commits.
- Run tests with file-redirect pattern: `buck2 test //src/services/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error" /tmp/t.log`.

---

### Task 1: `service_runtime::init_tracing()` + smoke test

**Files:**
- Modify: `src/services/runtime/src/lib.rs` — add `init_tracing()` at end of file
- Modify: `src/services/runtime/BUCK` — add `//third-party:tracing-subscriber` to `:runtime` deps; add `init-tracing` rust_test target
- Create: `src/services/runtime/tests/init_tracing.rs`

**Interfaces:**
- Produces (used by Task 2): `pub fn init_tracing()` in `service_runtime` crate

- [ ] **Step 1: Write the failing test**

Create `src/services/runtime/tests/init_tracing.rs`:

```rust
use service_runtime::init_tracing;

#[test]
fn double_call_does_not_panic() {
    init_tracing();
    init_tracing();
}
```

Add to `src/services/runtime/BUCK`:

```python
rust_test(
    name = "init-tracing",
    crate = "init_tracing",
    srcs = ["tests/init_tracing.rs"],
    crate_root = "tests/init_tracing.rs",
    edition = "2024",
    deps = [":runtime"],
)
```

- [ ] **Step 2: Implement `init_tracing()`**

Add to `src/services/runtime/src/lib.rs` (after the `serve` function at the end of the file):

```rust
/// Install a `tracing-subscriber` for the process. Uses `RUST_LOG` env (default
/// `info`). Idempotent — a second call from a test harness or re-entrant path does
/// not panic.
pub fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt().with_env_filter(filter).try_init();
}
```

Add `//third-party:tracing-subscriber` to the `deps` of the `:runtime` rule in `src/services/runtime/BUCK`.

- [ ] **Step 3: Verify test passes**

```
buck2 test //src/services/runtime:init-tracing > /tmp/t.log 2>&1
grep -E "Tests finished|FAIL|error" /tmp/t.log
```

---

### Task 2: Wire `init_tracing()` into both binaries

**Files:**
- Modify: `src/services/query-api/src/main.rs` — first statement of `main()`
- Modify: `src/services/ingest/src/main.rs` — first statement of `main()`

- [ ] **Step 1: Add to query-api main**

In `src/services/query-api/src/main.rs`, add `service_runtime::init_tracing();` as the first statement of the `main()` body (before `Config::from_env()`).

- [ ] **Step 2: Add to ingest main**

In `src/services/ingest/src/main.rs`, add `service_runtime::init_tracing();` as the first statement of the `main()` body (before `Config::from_env()`).

- [ ] **Step 3: Build to confirm compilation**

```
buck2 build //src/services/query-api:query-api //src/services/ingest:ingest > /tmp/b.log 2>&1
grep -E "FAILED|error" /tmp/b.log
```

---

### Task 3: `internal_error` helper + fix 4 sites in http.rs

**Files:**
- Modify: `src/services/query-api/src/http.rs` — add `internal_error` helper; bind + log at 4 sites; remove `TODO(serving-tier)` comment block

**Interfaces:**
- Produces (used by Task 4): `fn internal_error(context: &str, e: impl Display) -> axum::response::Response`

- [ ] **Step 1: Add `internal_error` helper**

Near the top of the file (after imports, before the first handler), add:

```rust
use std::fmt::Display;

/// Log a backend/serving fault server-side, then return the opaque 500 the client
/// sees. The detail (`error = %e`) is for operators only — the response body
/// carries no internal detail (SQL fragments, table/column names).
fn internal_error(context: &str, e: impl Display) -> axum::response::Response {
    tracing::error!(error = %e, "{context}");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response()
}
```

- [ ] **Step 2: Fix `read_object` (line ~96)**

Replace:
```rust
        // Return an opaque body for backend/serving faults: a governance-fronted
        // service must not echo internal error detail (SQL fragments, table/column
        // names) to the client. TODO(serving-tier): log `e` server-side once a
        // tracing subscriber is wired in the binary.
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
```
With:
```rust
        Err(e) => internal_error("object read serving fault", e),
```

- [ ] **Step 3: Fix `chain_error` (line ~259)**

Replace:
```rust
        // Opaque body for backend/serving faults (no internal detail leaked).
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
```
With:
```rust
        other => internal_error("chain/association read serving fault", other),
```

- [ ] **Step 4: Fix `graph_error` (line ~458)**

Replace:
```rust
        // Opaque body for backend/serving faults (no internal detail leaked).
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
```
With:
```rust
        other => internal_error("graph read serving fault", other),
```

- [ ] **Step 5: Fix `post_action` (line ~617)**

Replace:
```rust
        // Opaque body for backend/serving faults (no internal detail leaked).
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error").into_response(),
```
With:
```rust
        Err(e) => internal_error("action serving fault", e),
```

- [ ] **Step 6: Build to confirm compilation**

```
buck2 build //src/services/query-api:query-api > /tmp/b.log 2>&1
grep -E "FAILED|error" /tmp/b.log
```

---

### Task 4: `serving_fault_logging` test

**Files:**
- Create: `src/services/query-api/tests/serving_fault_logging.rs`
- Modify: `src/services/query-api/BUCK` — add `serving-fault-logging` rust_test target

**Spec test:** Pin both halves of the `internal_error` contract — the opaque 500 body and the server-side log line — for `internal_error` directly, plus `chain_error` and `graph_error`. `read_object` and `post_action` are async handlers that need a full serving backend; they are covered indirectly via the shared `internal_error` helper.

- [ ] **Step 1: Write the test file**

Create `src/services/query-api/tests/serving_fault_logging.rs`:

```rust
//! internal_error logs the detail server-side and returns an opaque 500 to the client.
//! chain_error and graph_error delegate to internal_error for their catch-all arms.

use axum::body::to_bytes;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use tracing_test::traced_test;

use query_api::http::internal_error;
use query_api::query::QueryError;
use query_api::serving::ServingError;

#[tokio::test]
#[traced_test]
async fn internal_error_opaque_body_and_logs_detail() {
    let resp = internal_error("object read serving fault", "boom").into_response();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), b"internal error");
    assert!(!std::str::from_utf8(&body).unwrap().contains("boom"), "detail must not leak to client");
    assert!(logs_contain("boom"));
}

#[tokio::test]
#[traced_test]
async fn chain_error_catch_all_logs_serving_error() {
    let e = QueryError::Serving(ServingError::Engine("chain-boom".into()));
    let resp = query_api::http::chain_error(e);
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), b"internal error");
    assert!(logs_contain("chain-boom"));
}

#[tokio::test]
#[traced_test]
async fn graph_error_catch_all_logs_serving_error() {
    let e = QueryError::Serving(ServingError::Engine("graph-boom".into()));
    let resp = query_api::http::graph_error(e);
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.as_ref(), b"internal error");
    assert!(logs_contain("graph-boom"));
}
```

- [ ] **Step 2: Add BUCK target**

In `src/services/query-api/BUCK`, add:

```python
rust_test(
    name = "serving-fault-logging",
    crate = "serving_fault_logging",
    srcs = ["tests/serving_fault_logging.rs"],
    crate_root = "tests/serving_fault_logging.rs",
    edition = "2024",
    deps = [
        ":query-api-lib",
        "//third-party:axum",
        "//third-party:tokio",
        "//third-party:tracing-test",
    ],
)
```

- [ ] **Step 3: Run test**

```
buck2 test //src/services/query-api:serving-fault-logging > /tmp/t.log 2>&1
grep -E "Tests finished|FAIL|error" /tmp/t.log
```

---

### Task 5: Full suite + docs update

- [ ] **Step 1: Run full test suite**

```
buck2 test //src/... > /tmp/full.log 2>&1
grep -E "Tests finished|FAIL" /tmp/full.log
```

- [ ] **Step 2: Update docs/ISSUES.md**

Mark `iss-serving-faults-not-logged` as fixed: `[x]`, `status:fixed`, add `pr:#<n>` once PR is opened.

- [ ] **Step 3: Commit and push**

```
git add -p
git commit -m "fix(query-api): install tracing subscriber and log serving faults server-side"
git push -u origin work/iss-serving-faults-not-logged
```
