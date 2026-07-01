# Filter error contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make an uncoercible query-param filter value return a structured `{error, column, expected, value}` 400 body, and flip `POST /actions` semantic param-validation failures from 400 to 422 (malformed bodies stay 400).

**Architecture:** Two localized, independent changes in `src/services/query-api`. (A) Enrich the `filter::FilterError` coercion error to carry the structured triple it already half-has, and render it as a JSON body at the three HTTP `BadFilterValue` mapping sites. (B) Change one status mapping (`BadParams` 400→422) and its OpenAPI response doc. No SQL, no governance, no write-path logic changes.

**Tech Stack:** Rust, axum, utoipa (OpenAPI), buck2 (`rust_test`/`loom_fixture_test`), `MemoryControlPlane` for in-memory router tests.

## Global Constraints

- **Tests are `rust_test` integration targets only — never inline `#[cfg(test)]`.** Every new/changed test lives in a `tests/<name>.rs` file wired as a `rust_test` target in `src/services/query-api/BUCK`. The `no-inline-tests` prek hook fails on any `#[test]` under `src/**`.
- **`rust_test` in this BUCK is the `loom_rust_test` wrapper** (`load("//src:loom_test.bzl", "rust_test")` at the top of `src/services/query-api/BUCK`), which injects the test-only lint allows. Use it, not a bare prelude `rust_test`.
- **Clippy is strict** (pedantic + restriction on lib/bin). Production code must not `unwrap`/`expect`/`panic`/index-slice; carry source errors. Test code is exempt via the wrapper.
- **`filter.rs` stays "pure logic, no I/O"** — the JSON body construction lives at the HTTP boundary (`http.rs`), matching the codebase convention that `serde_json` construction happens where the scalar values live, not in the coercion module.
- **Run tests scoped, not whole-tree** (cloud disk cap): `buck2 build -M none //src/services/query-api/...` then the specific `buck2 test` targets. Don't pipe `buck2 test` through `tail`/`head` — redirect to a file and grep it.
- **Commit messages follow Conventional Commits** (`feat:`/`test:`/`docs:`/`refactor:`), enforced by the `conventional-commit` hook.

---

## File Structure

- `src/services/query-api/src/filter.rs` — **modify.** Enrich `FilterError`: split into `Coerce { column, expected, value, detail }` (value-coercion failures from `coerce_filter`) and `BadValue(column, msg)` (grammar/arity failures from `coerce_predicate`). `coerce_filter`'s error closures build `Coerce`; `coerce_predicate`'s `bad` closure keeps building `BadValue`.
- `src/services/query-api/src/http.rs` — **modify.** Add a shared `bad_filter_value_response(&FilterError) -> Response` helper; route the three `BadFilterValue` mapping sites through it. Flip the `BadParams` mapping to 422. Update the `post_action` OpenAPI `responses(...)` doc.
- `src/services/query-api/src/openapi.rs` — **no change** (the `post_action` responses doc lives inline on the handler in `http.rs`; no new schema — structured bodies for errors other than `BadFilterValue` are out of scope, and `BadFilterValue` itself is documented by description only, per spec).
- `src/services/query-api/tests/filter_coerce.rs` — **modify.** The 8 `coerce_filter` failure assertions move from `FilterError::BadValue(_, _)` to `FilterError::Coerce { .. }`; add field assertions on `column`/`expected`/`value`.
- `src/services/query-api/tests/filter_error_http.rs` — **create.** Router-level, in-memory (mirrors `constraints_action_http.rs`): structured 400 body for an uncoercible value, and the visibility-denial 400 that echoes no value.
- `src/services/query-api/tests/constraints_action_http.rs` — **modify.** Add `missing_param_is_422` and `malformed_body_is_400` router tests alongside the existing constraint-422 tests (same `createWidget` seed).
- `src/services/query-api/tests/openapi.rs` — **modify.** Add an assertion that `POST /actions/{action_name}` documents both 422 and 400.
- `src/services/query-api/BUCK` — **modify.** Add the `filter-error-http` `rust_test` target.

---

## Task 1: Enrich `FilterError` with the structured coercion triple

**Files:**
- Modify: `src/services/query-api/src/filter.rs` (the `FilterError` enum ~lines 9-13; `coerce_filter` ~lines 30-35; `coerce_predicate` `bad` closure ~line 121)
- Test: `src/services/query-api/tests/filter_coerce.rs` (8 `BadValue` assertions at lines 36, 52, 56, 80, 84, 92, 96, 100)

**Interfaces:**
- Produces:
  - `enum FilterError { Coerce { column: String, expected: String, value: String, detail: String }, BadValue(String, String) }` — public, `#[derive(Debug, thiserror::Error, PartialEq)]`.
  - `Display` for `Coerce` renders `"filter {column}: {detail}"` (unchanged wording vs today's `"filter {0}: {1}"`, so `flight_export.rs`'s `e.to_string()` and logs keep the same text).
  - `coerce_filter(name, logical_ty, raw)` returns `Err(FilterError::Coerce { column: name, expected: logical_ty, value: raw, detail })` on **every** failure (parse fault, wrong bool, bad date/timestamp, unknown logical type, vector column).
  - `coerce_predicate(column, logical_ty, raw)` returns `Err(FilterError::BadValue(column, msg))` on grammar/arity faults; a value that fails coercion propagates the `Coerce` variant unchanged from `coerce_filter`.
- Consumes: nothing from other tasks.

- [ ] **Step 1: Update the unit tests to expect `Coerce` (write the failing test)**

In `src/services/query-api/tests/filter_coerce.rs`, replace each of the 8 `coerce_filter` failure assertions of the form:

```rust
    assert!(matches!(
        coerce_filter("id", "Long", "1.5"),
        Err(FilterError::BadValue(_, _))
    ));
```

with the `Coerce` variant. The 8 sites and their expected `column`/`expected`/`value` are:

```rust
    // line 34-37: Long / "1.5"
    assert!(matches!(
        coerce_filter("id", "Long", "1.5"),
        Err(FilterError::Coerce { .. })
    ));
    // line 50-53: Boolean / "maybe"
    assert!(matches!(
        coerce_filter("a", "Boolean", "maybe"),
        Err(FilterError::Coerce { .. })
    ));
    // line 54-57: Boolean / "1"
    assert!(matches!(
        coerce_filter("a", "Boolean", "1"),
        Err(FilterError::Coerce { .. })
    ));
    // line 78-81: Date / "nope"
    assert!(matches!(
        coerce_filter("d", "Date", "nope"),
        Err(FilterError::Coerce { .. })
    ));
    // line 82-85: Timestamp / "2026-06-16"
    assert!(matches!(
        coerce_filter("t", "Timestamp", "2026-06-16"),
        Err(FilterError::Coerce { .. })
    ));
    // line 90-93: Double / "abc"
    assert!(matches!(
        coerce_filter("x", "Double", "abc"),
        Err(FilterError::Coerce { .. })
    ));
    // line 94-97: Integer / "abc"
    assert!(matches!(
        coerce_filter("x", "Integer", "abc"),
        Err(FilterError::Coerce { .. })
    ));
    // line 98-101: Nonsense (unknown type) / "1"
    assert!(matches!(
        coerce_filter("x", "Nonsense", "1"),
        Err(FilterError::Coerce { .. })
    ));
```

Then add a new test that pins the structured fields (this is the core new behavior):

```rust
#[test]
fn coerce_error_carries_column_expected_value() {
    let err = coerce_filter("amount", "double", "abc").unwrap_err();
    match err {
        FilterError::Coerce {
            column,
            expected,
            value,
            ..
        } => {
            assert_eq!(column, "amount");
            assert_eq!(expected, "double");
            assert_eq!(value, "abc");
        }
        other => panic!("expected Coerce, got {other:?}"),
    }
}

#[test]
fn coerce_predicate_grammar_fault_stays_bad_value() {
    // Bad arity (scalar op, no operand) is a grammar fault, not a coercion fault.
    match coerce_predicate("amount", "double", "gt").unwrap_err() {
        FilterError::BadValue(col, _) => assert_eq!(col, "amount"),
        other => panic!("expected BadValue, got {other:?}"),
    }
    // A value that fails to coerce inside a predicate surfaces the Coerce variant.
    assert!(matches!(
        coerce_predicate("amount", "double", "gt:abc").unwrap_err(),
        FilterError::Coerce { .. }
    ));
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `buck2 build -M none //src/services/query-api:filter-coerce 2>&1 | tail -20`
Expected: FAIL — `FilterError` has no variant `Coerce` (the enum is still `BadValue`-only).

- [ ] **Step 3: Enrich the `FilterError` enum**

In `src/services/query-api/src/filter.rs`, replace the enum (lines 9-13):

```rust
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum FilterError {
    /// A value could not be coerced to its column's declared logical type. Carries the
    /// structured triple echoed to the caller in a `400` body — `column`, `expected` (the
    /// property's declared logical type name), `value` (the caller's own offending operand)
    /// — plus a human `detail` kept for the `Display`/logs. The value echo is safe: it is
    /// the caller's own input, and this variant is a *coercion* fault, never a permission
    /// signal (that is `handler::QueryError::BadFilter`).
    #[error("filter {column}: {detail}")]
    Coerce {
        column: String,
        expected: String,
        value: String,
        detail: String,
    },
    /// A malformed predicate grammar (bad operator arity, bad set-operand escape). Column +
    /// message only — there is no single offending value/type to echo.
    #[error("filter {0}: {1}")]
    BadValue(String, String),
}
```

- [ ] **Step 4: Route `coerce_filter`'s error closures through `Coerce`**

In `src/services/query-api/src/filter.rs`, replace the two closures at the top of `coerce_filter` (lines 31-35):

```rust
    let bad = |m: &str| FilterError::BadValue(name.to_string(), m.to_string());
    // Like `bad`, but folds the discarded source error into the message for diagnostics.
    let bad_src = |m: &str, e: &dyn std::fmt::Display| {
        FilterError::BadValue(name.to_string(), format!("{m}: {e}"))
    };
```

with (the body of `coerce_filter` below is unchanged — it already calls `bad`/`bad_src`):

```rust
    // Every `coerce_filter` failure is a value-coercion fault: carry the structured triple
    // {column, expected, value} for the HTTP body plus a `detail` for the Display/logs.
    let coerce = |detail: String| FilterError::Coerce {
        column: name.to_string(),
        expected: logical_ty.to_string(),
        value: raw.to_string(),
        detail,
    };
    let bad = |m: &str| coerce(m.to_string());
    // Like `bad`, but folds the discarded source error into the `detail` for diagnostics.
    let bad_src = |m: &str, e: &dyn std::fmt::Display| coerce(format!("{m}: {e}"));
```

Leave `coerce_predicate`'s own `bad` closure (line 121, `FilterError::BadValue(column.to_string(), m.to_string())`) exactly as-is — grammar/arity faults stay `BadValue`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `buck2 test //src/services/query-api:filter-coerce > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t1.log`
Expected: PASS (all `filter_coerce` tests, including the two new ones).

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/filter.rs src/services/query-api/tests/filter_coerce.rs
git commit -m "refactor(query-api): enrich FilterError with structured coercion triple"
```

---

## Task 2: Render the structured 400 body at the HTTP `BadFilterValue` sites

**Files:**
- Modify: `src/services/query-api/src/http.rs` (mapping sites at lines 177-179, 364, 586; add a helper near them)
- Create: `src/services/query-api/tests/filter_error_http.rs`
- Modify: `src/services/query-api/BUCK` (new `filter-error-http` target)

**Interfaces:**
- Consumes: `filter::FilterError::{Coerce, BadValue}` from Task 1.
- Produces: `fn bad_filter_value_response(e: &crate::filter::FilterError) -> axum::response::Response` (module-private in `http.rs`) — a 400 whose JSON body is `{error:"bad_filter_value", column, expected, value}` for `Coerce`, `{error:"bad_filter_value", column}` for `BadValue`.

- [ ] **Step 1: Write the failing router test**

Create `src/services/query-api/tests/filter_error_http.rs`:

```rust
//! The GET /objects/:type filter path renders an uncoercible value as a structured 400
//! body { error, column, expected, value }, while a visibility denial (a denied filter
//! column) stays a bare-column 400 that echoes no value. In-memory: the coercion / column
//! check fires before any serving call, so a stub engine suffices (no fixture).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, ControlPlane, Effect, ObjectType, Ontology, Policy, PolicyTarget, PropertyDef,
    RoleId, SnapshotId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, Rows, ServingEngine, ServingError, SqlValue};
use service_runtime::Subject;
use tower::ServiceExt;

struct StubServing;

#[async_trait]
impl ServingEngine for StubServing {
    async fn fetch_rows(&self, _sql: &str, _params: &[SqlValue]) -> Result<Rows, ServingError> {
        Ok(Rows {
            columns: vec![],
            rows: vec![],
        })
    }
}

struct StubAction;

#[async_trait]
impl ActionEngine for StubAction {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        _event: control_plane_core::LineageEvent,
    ) -> Result<SnapshotId, ServingError> {
        Err(ServingError::Engine("unused".into()))
    }

    async fn overwrite_table(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _rows: &[Vec<SqlValue>],
        _logical_types: &[String],
        _event: control_plane_core::LineageEvent,
    ) -> Result<SnapshotId, ServingError> {
        Err(ServingError::Engine("unused".into()))
    }
}

fn prop(name: &str, ty: &str) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required: false,
        constraints: control_plane_core::PropertyConstraints::default(),
    }
}

/// Seed `Order(id long identity, amount double)` and grant coarse Read to `analyst`.
/// When `deny_amount` is set, also attach a Read policy denying the `amount` column.
async fn seed(deny_amount: bool) -> MemoryControlPlane {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(ObjectType {
        name: TypeName("Order".into()),
        properties: vec![prop("id", "long"), prop("amount", "double")],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "order".into(),
        },
        identity: Some("id".into()),
    })
    .await
    .unwrap();

    let analyst = SubjectId("analyst".into());
    let reader = RoleId("reader".into());
    cp.define_subject(&analyst).await.unwrap();
    cp.define_role(&reader).await.unwrap();
    cp.assign_role(&analyst, &reader).await.unwrap();
    cp.grant(
        &reader,
        Action::Read,
        PolicyTarget::Type(TypeName("Order".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    if deny_amount {
        cp.set_policy(
            &reader,
            Action::Read,
            Policy {
                target: PolicyTarget::Type(TypeName("Order".into())),
                row_filter: None,
                deny_columns: vec!["amount".into()],
                mask_columns: vec![],
            },
        )
        .await
        .unwrap();
    }
    cp
}

async fn get(cp: MemoryControlPlane, subject: &str, uri: &str) -> (StatusCode, String) {
    let state = AppState {
        cp: Arc::new(cp) as Arc<dyn ControlPlane>,
        serving: Arc::new(StubServing),
        action_engine: Arc::new(StubAction),
        default_limit: 1000,
    };
    let app = router(state);
    let mut req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    req.extensions_mut()
        .insert(Subject(SubjectId(subject.into())));
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn uncoercible_value_is_structured_400() {
    let cp = seed(false).await;
    let (status, body) = get(cp, "analyst", "/objects/Order?amount=gt:abc").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    let json: serde_json::Value = serde_json::from_str(&body).expect("structured JSON body");
    assert_eq!(json["error"], "bad_filter_value");
    assert_eq!(json["column"], "amount");
    assert_eq!(json["expected"], "double");
    assert_eq!(json["value"], "abc");
}

#[tokio::test(flavor = "multi_thread")]
async fn valid_filter_is_unaffected() {
    let cp = seed(false).await;
    let (status, body) = get(cp, "analyst", "/objects/Order?amount=gt:5").await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn denied_column_filter_is_400_without_value_echo() {
    // Filtering on a denied column is a visibility signal (BadFilter), NOT a coercion error:
    // it stays a bare-column 400 and never echoes the caller's value.
    let cp = seed(true).await;
    let (status, body) = get(cp, "analyst", "/objects/Order?amount=gt:99").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "body: {body}");
    assert_eq!(body, "amount");
    assert!(!body.contains("99"), "must not echo the filtered value");
    assert!(
        !body.contains("bad_filter_value"),
        "a visibility denial is not a coercion error"
    );
}
```

- [ ] **Step 2: Wire the BUCK target**

In `src/services/query-api/BUCK`, add after the `constraints-action-http` target (~line 1027):

```python
rust_test(
    name = "filter-error-http",
    crate = "filter_error_http",
    srcs = ["tests/filter_error_http.rs"],
    crate_root = "tests/filter_error_http.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//src/services/runtime:runtime",
        "//third-party:async-trait",
        "//third-party:axum",
        "//third-party:http-body-util",
        "//third-party:serde_json",
        "//third-party:tokio",
        "//third-party:tower",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:filter-error-http > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|PASS|error\[" /tmp/t2.log`
Expected: `uncoercible_value_is_structured_400` FAILS — the current mapping returns a plain-text `e.to_string()` body, so `serde_json::from_str` fails / fields don't match. (`valid_filter_is_unaffected` and `denied_column_filter_is_400_without_value_echo` already pass.)

- [ ] **Step 4: Add the shared response helper in `http.rs`**

In `src/services/query-api/src/http.rs`, add this helper immediately above `chain_error` (~line 354):

```rust
/// Render an uncoercible filter value (`QueryError::BadFilterValue`) as a structured `400`
/// body. A coercion failure (`FilterError::Coerce`) echoes `{error, column, expected, value}`;
/// a grammar/arity failure (`FilterError::BadValue`) carries `{error, column}` only. A
/// *visibility* denial is a separate `QueryError::BadFilter` (bare column, no value echo) and
/// never reaches here.
fn bad_filter_value_response(e: &crate::filter::FilterError) -> axum::response::Response {
    use crate::filter::FilterError;
    let body = match e {
        FilterError::Coerce {
            column,
            expected,
            value,
            ..
        } => serde_json::json!({
            "error": "bad_filter_value",
            "column": column,
            "expected": expected,
            "value": value,
        }),
        FilterError::BadValue(column, _) => serde_json::json!({
            "error": "bad_filter_value",
            "column": column,
        }),
    };
    (StatusCode::BAD_REQUEST, Json(body)).into_response()
}
```

- [ ] **Step 5: Route the three mapping sites through the helper**

In `src/services/query-api/src/http.rs`, replace the `get_object` arm (lines 177-179):

```rust
        Err(QueryError::BadFilterValue(e)) => {
            (StatusCode::BAD_REQUEST, e.to_string()).into_response()
        }
```

with:

```rust
        Err(QueryError::BadFilterValue(e)) => bad_filter_value_response(&e),
```

Replace the `chain_error` arm (line 364):

```rust
        QueryError::BadFilterValue(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
```

with:

```rust
        QueryError::BadFilterValue(e) => bad_filter_value_response(&e),
```

Replace the `graph_error` arm (line 586) — identical text — with the same:

```rust
        QueryError::BadFilterValue(e) => bad_filter_value_response(&e),
```

Leave `flight_export.rs:183` (`Status::invalid_argument(e.to_string())`) unchanged — the Flight wire keeps the `Display` string; only the HTTP body becomes structured.

- [ ] **Step 6: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:filter-error-http > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t2.log`
Expected: PASS (all three tests).

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/http.rs src/services/query-api/tests/filter_error_http.rs src/services/query-api/BUCK
git commit -m "feat(query-api): structured 400 body for uncoercible filter values"
```

---

## Task 3: Flip `POST /actions` `BadParams` to 422 + update OpenAPI doc

**Files:**
- Modify: `src/services/query-api/src/http.rs` (`BadParams` mapping line 773-775; `post_action` OpenAPI `responses(...)` lines 707-713)
- Test: `src/services/query-api/tests/constraints_action_http.rs` (add two tests) and `src/services/query-api/tests/openapi.rs` (add one)

**Interfaces:**
- Consumes: the existing `constraints_action_http.rs` `seed()`/`post_json()` helpers (`createWidget` with required params `id: Long`, `code: String`) and `openapi.rs` `build_openapi()`.
- Produces: no new symbols — a status change (400→422 for `BadParams`) and doc text.

- [ ] **Step 1: Write the failing action tests**

In `src/services/query-api/tests/constraints_action_http.rs`, add after `acl_denial_is_403_distinct_from_constraint_422` (end of file):

```rust
#[tokio::test(flavor = "multi_thread")]
async fn missing_required_param_is_422() {
    // A well-formed JSON body that omits the required `code` param fails SEMANTIC
    // validation → 422 (was 400), aligning with the constraint-violation 422 above.
    let cp = seed().await;
    let writes = Arc::new(AtomicUsize::new(0));
    let (status, body) = post_json(cp, writes.clone(), "analyst", json!({"id": "5"})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "body: {body}");
    // The body names the offending param (plain-text `ParamError` Display).
    assert!(
        body.as_str().is_some_and(|s| s.contains("code"))
            || body.to_string().contains("code"),
        "expected the body to name the missing `code` param, got {body}"
    );
    assert_eq!(writes.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn mistyped_param_is_422() {
    // `id` must be a Long (JSON string); passing a JSON number is a semantic type mismatch.
    let cp = seed().await;
    let writes = Arc::new(AtomicUsize::new(0));
    let (status, body) =
        post_json(cp, writes.clone(), "analyst", json!({"id": 5, "code": "AB"})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "body: {body}");
    assert_eq!(writes.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_body_is_400() {
    // A JSON array is not an action-envelope object → the request itself is malformed → 400.
    let cp = seed().await;
    let writes = Arc::new(AtomicUsize::new(0));
    let (status, _body) = post_json(cp, writes.clone(), "analyst", json!([1, 2, 3])).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(writes.load(Ordering::SeqCst), 0);
}
```

Note on `mistyped_param_is_422`: `id` is declared `Long`, whose JSON repr is a numeric *string* (`parse_value`'s `NumericString` arm calls `v.as_str()`), so a JSON number `5` fails coercion → `ParamError::BadValue` → `BadParams` → 422.

- [ ] **Step 2: Add the OpenAPI doc assertion (failing test)**

In `src/services/query-api/tests/openapi.rs`, add:

```rust
#[test]
fn post_action_documents_422_and_400() {
    let doc = query_api::build_openapi();
    let json = serde_json::to_value(&doc).unwrap();
    let responses = &json["paths"]["/actions/{action_name}"]["post"]["responses"];
    assert!(
        responses.get("422").is_some(),
        "POST /actions must document 422 (semantic failure), got {responses}"
    );
    assert!(
        responses.get("400").is_some(),
        "POST /actions must document 400 (malformed body), got {responses}"
    );
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `buck2 test //src/services/query-api:constraints-action-http //src/services/query-api:openapi > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t3.log`
Expected: `missing_required_param_is_422` and `mistyped_param_is_422` FAIL (currently 400). `post_action_documents_422_and_400` passes today (422 already documented for constraints; 400 already present) — it is a **guard** that the doc edit in Step 5 does not drop either status. `malformed_body_is_400` already passes.

- [ ] **Step 4: Flip the `BadParams` mapping to 422**

In `src/services/query-api/src/http.rs`, replace the `BadParams` arm (lines 773-775):

```rust
        Err(crate::action::ActionError::BadParams(e)) => {
            (StatusCode::BAD_REQUEST, e.to_string()).into_response()
        }
```

with:

```rust
        // A well-formed body whose params fail SEMANTIC validation (missing required param,
        // type mismatch, uncoercible value) is 422 — understood, but unprocessable. Malformed
        // / undecodable bodies never reach here: axum's `Json` extractor 400s invalid JSON,
        // and the non-object envelope guard above returns 400. Aligns with the
        // ConstraintViolation 422 on this same write path.
        Err(crate::action::ActionError::BadParams(e)) => {
            (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response()
        }
```

- [ ] **Step 5: Update the `post_action` OpenAPI response doc**

In `src/services/query-api/src/http.rs`, replace the `post_action` `responses(...)` block (lines 707-713):

```rust
    responses(
        (status = 201, description = "Action applied; created/affected object"),
        (status = 400, description = "Bad params"),
        (status = 403, description = "Write denied by ACL policy", body = WriteDeniedBody),
        (status = 404, description = "Unknown action"),
        (status = 422, description = "A value violates a property constraint, or an unsupported action shape", body = crate::openapi::ConstraintViolationsBody),
    ),
```

with:

```rust
    responses(
        (status = 201, description = "Action applied; created/affected object"),
        (status = 400, description = "Malformed or undecodable request body (not a JSON action envelope)"),
        (status = 403, description = "Write denied by ACL policy", body = WriteDeniedBody),
        (status = 404, description = "Unknown action"),
        (status = 422, description = "Semantic validation failure: bad or missing action params, a property-constraint violation, or an unsupported action shape", body = crate::openapi::ConstraintViolationsBody),
    ),
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `buck2 test //src/services/query-api:constraints-action-http //src/services/query-api:openapi > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t3.log`
Expected: PASS (all constraints-action-http tests incl. the three new ones; all openapi tests incl. the new guard).

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/http.rs src/services/query-api/tests/constraints_action_http.rs src/services/query-api/tests/openapi.rs
git commit -m "feat(query-api): 422 for semantic action param failures, 400 for malformed bodies"
```

---

## Task 4: Full-crate regression + register close

**Files:**
- Modify: `docs/ROADMAP.md` (close `road-filter-error-contract`) — done at the finishing step via `loom-docs-update`, listed here for completeness.

- [ ] **Step 1: Build + test the touched crate and its dependents**

Run: `buck2 build -M none //src/services/query-api/... > /tmp/build.log 2>&1; grep -E "BUILD SUCCEEDED|FAIL" /tmp/build.log`
Then: `buck2 test //src/services/query-api/... > /tmp/all.log 2>&1; grep -E "Tests finished|FAIL" /tmp/all.log`
Expected: build succeeds; tests finish with no failures. Pay attention to any test that matched `FilterError::BadValue` on a `coerce_filter` result elsewhere (grep confirmed only `filter_coerce.rs` does; if the build surfaces another, update it to `Coerce`).

- [ ] **Step 2: Run clippy on the crate**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/clippy.log 2>&1; cat $(buck2 build --show-output '//src/services/query-api:query-api[clippy.txt]' 2>/dev/null | awk '{print $2}') 2>/dev/null || cat /tmp/clippy.log`
Expected: empty clippy output (clean). Fix any pedantic/restriction finding in the new code (e.g. no `unwrap`/`expect` in `http.rs`/`filter.rs`).

- [ ] **Step 3: Close the register item (at finish)**

Handled by `loom-docs-update` during `superpowers:finishing-a-development-branch`: flip `docs/ROADMAP.md` `road-filter-error-contract` `- [ ]`→`- [x]`, set `status:done`, add `pr:#N`.

---

## Self-Review

**1. Spec coverage:**
- Richer `BadFilterValue` body `{column, expected, value}` → Task 1 (enrich `FilterError`) + Task 2 (render at all three HTTP sites). ✓
- `Display` kept for logs → Task 1 keeps `#[error("filter {column}: {detail}")]`; `flight_export` uses it unchanged. ✓
- `BadFilter` (visibility) untouched, no value echo → Task 2 test `denied_column_filter_is_400_without_value_echo`; mapping left as bare column. ✓
- `POST /actions` `BadParams` → 422 (semantic), 400 (malformed) → Task 3 mapping flip + `missing_required_param_is_422` / `mistyped_param_is_422` / `malformed_body_is_400`. ✓
- OpenAPI doc updated (422 semantic + 400 malformed) → Task 3 doc edit + `post_action_documents_422_and_400`. ✓
- GET typed-filter stays 400 → Task 2 keeps `StatusCode::BAD_REQUEST` in the helper. ✓
- Alignment with `road-model-constraints` 422 → Task 3 co-locates the tests; both are 422 on `POST /actions`. ✓
- Out of scope respected: no error-envelope rework; `BadFilter` semantics unchanged; no 422 elsewhere; no structured body for errors other than `BadFilterValue`. ✓

**2. Placeholder scan:** No TBD/TODO; every code step shows the full replacement text; test bodies are complete. ✓

**3. Type consistency:** `FilterError::Coerce { column, expected, value, detail }` field names are used identically in Task 1 (definition, `coerce_filter` closure, `filter_coerce.rs` assertions) and Task 2 (`bad_filter_value_response` match). `bad_filter_value_response(&FilterError)` takes a reference; the three call sites pass `&e` (the arms bind owned `e`). `AppState`/`router`/`ServingEngine`/`ActionEngine`/`Rows`/`SqlValue`/`Policy` field names mirror `constraints_action_http.rs` verbatim. ✓
