# Structured action write-denial reason — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface a caller-scoped, structured reason in the `403` body when a
governed action's **fine-grained** Write policy denies an insert — distinguishing
column denial (naming the offending column) from row-filter denial (no predicate
disclosed) — instead of the current bodyless 403. Closes `road-structured-write-denial`.

**Spec:** `docs/superpowers/specs/2026-06-22-structured-write-denial-design.md`

**Architecture:** The denial decision is unchanged; only its *rendering* changes.
`check_write_policy` already returns a rich `WriteVerdict` (`write_filter.rs`).
`run_action` collapses a denying verdict to a new `ActionError::WriteDenied(WriteDenialReason)`
(a small query-api enum, `Column(String)` / `RowFilter`), keeping the existing
`tracing::info!` lines. The HTTP layer (`http.rs::post_action`) gains one arm that
serializes the reason to the caller-scoped JSON body; the existing `Forbidden => 403`
arm stays for the **coarse** Write gate and is untouched. No control-plane, ontology,
or ACL-model changes — the verdict already exists, this only stops discarding it.

**Tech Stack:** Rust, buck2, axum, serde_json. Tests are `rust_test` integration
targets (no inline `#[cfg(test)]`). The reason→body mapping test is a pure
(non-fixture) `rust_test`; the HTTP body tests use `MemoryControlPlane` + stub
engines (denial fires before any DuckLake access, so no Postgres/DuckDB fixture);
the existing fixture-backed `action-e2e` test is updated to assert the new variant.

## Global Constraints

- **Tests are `rust_test` integration targets only** — NO inline `#[cfg(test)]`/`#[test]`
  in `src/**.rs` (the `no-inline-tests` prek hook fails).
- **No new third-party deps** — `serde_json`, `axum`, `tokio`, `tower`,
  `http-body-util`, `control-plane-memory` are all already wired into query-api
  test targets. No `Cargo.toml` / `Cargo.lock` / `third-party/BUCK` changes; the
  lockfile/`duckdb`-downgrade footgun does not apply.
- **The coarse-gate `Forbidden => 403` path is byte-for-byte unchanged** — this
  slice only adds the fine-grained `WriteDenied` arm.
- **Confidentiality invariant:** the body never contains the `row_filter`
  predicate, policy id, or role — only the caller-scoped reason (and, for column
  denials, the column the caller themselves supplied). The predicate stays in the
  server log only.
- Commit messages: Conventional Commits.
- Run tests with the file-redirect pattern (never pipe `buck2 test` through
  `tail`/`head`): `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`.

---

### Task 1: `WriteDenialReason` + `ActionError::WriteDenied`, wire `run_action`, pure mapping test

**Files:**
- Modify: `src/services/query-api/src/action.rs` — add `WriteDenialReason` enum with
  `from_verdict` + `to_body`; add `ActionError::WriteDenied`; restructure the deny
  arm in `run_action`.
- Create: `src/services/query-api/tests/write_denial_reason.rs` — pure mapping/shaping test.
- Modify: `src/services/query-api/BUCK` — add `write-denial-reason` rust_test target.

**Interfaces:**
- Produces (used by Task 2): `pub enum WriteDenialReason { Column(String), RowFilter }`
  with `pub fn from_verdict(v: WriteVerdict) -> Option<Self>` and
  `pub fn to_body(&self) -> serde_json::Value`; `ActionError::WriteDenied(WriteDenialReason)`.

- [ ] **Step 1: Write the failing pure test**

Create `src/services/query-api/tests/write_denial_reason.rs`:

```rust
//! Pure shaping test for the structured write-denial reason: WriteVerdict ->
//! WriteDenialReason -> caller-scoped 403 JSON body. No fixture.

use query_api::action::WriteDenialReason;
use query_api::write_filter::WriteVerdict;
use serde_json::json;

#[test]
fn allow_verdict_has_no_reason() {
    assert_eq!(WriteDenialReason::from_verdict(WriteVerdict::Allow), None);
}

#[test]
fn deny_column_maps_to_column_reason_and_body() {
    let reason = WriteDenialReason::from_verdict(WriteVerdict::DenyColumn("ssn".into()))
        .expect("a column denial has a reason");
    assert_eq!(reason, WriteDenialReason::Column("ssn".into()));
    assert_eq!(
        reason.to_body(),
        json!({ "error": "write_denied", "reason": "column", "column": "ssn" })
    );
}

#[test]
fn deny_row_maps_to_row_filter_reason_and_body() {
    let reason = WriteDenialReason::from_verdict(WriteVerdict::DenyRow)
        .expect("a row-filter denial has a reason");
    assert_eq!(reason, WriteDenialReason::RowFilter);
    let body = reason.to_body();
    assert_eq!(body, json!({ "error": "write_denied", "reason": "row_filter" }));
    // The row-filter body discloses no column (and never the predicate).
    assert!(
        body.get("column").is_none(),
        "row_filter body must not carry a column field"
    );
}
```

Add to `src/services/query-api/BUCK`:

```python
rust_test(
    name = "write-denial-reason",
    crate = "write_denial_reason",
    srcs = ["tests/write_denial_reason.rs"],
    crate_root = "tests/write_denial_reason.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//third-party:serde_json",
    ],
)
```

- [ ] **Step 2: Implement `WriteDenialReason` + `ActionError::WriteDenied`**

In `src/services/query-api/src/action.rs`, add a new public type (place it after
the `ActionError` enum). Keep the existing `use crate::write_filter::{self, WriteVerdict};`
import (already present):

```rust
/// The caller-scoped reason a fine-grained Write policy denied an insert, rendered
/// into the structured `403` body. Discloses only what the caller already supplied
/// (the offending column name) — never the `row_filter` predicate, policy id, or
/// role, which stay server-side (logged only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteDenialReason {
    /// An inserted column is denied by a Write policy. Names the column (the caller
    /// supplied it, so naming it discloses nothing new).
    Column(String),
    /// The inserted row fails a Write policy's `row_filter`. The predicate itself
    /// is never disclosed.
    RowFilter,
}

impl WriteDenialReason {
    /// Map a `WriteVerdict` to its caller-scoped reason. `Allow` has no reason
    /// (`None`); a denying verdict maps to the matching `WriteDenialReason`.
    pub fn from_verdict(verdict: WriteVerdict) -> Option<Self> {
        match verdict {
            WriteVerdict::Allow => None,
            WriteVerdict::DenyColumn(col) => Some(WriteDenialReason::Column(col)),
            WriteVerdict::DenyRow => Some(WriteDenialReason::RowFilter),
        }
    }

    /// The caller-scoped `403` JSON body: a stable machine-readable
    /// `error: "write_denied"` tag plus `reason` (`"column"` | `"row_filter"`) and,
    /// for column denials, the offending `column`.
    pub fn to_body(&self) -> serde_json::Value {
        match self {
            WriteDenialReason::Column(col) => serde_json::json!({
                "error": "write_denied",
                "reason": "column",
                "column": col,
            }),
            WriteDenialReason::RowFilter => serde_json::json!({
                "error": "write_denied",
                "reason": "row_filter",
            }),
        }
    }
}
```

Add a variant to `ActionError` (keep the existing unit `Forbidden` for the coarse
gate and other denials):

```rust
    /// A fine-grained Write policy denied the concrete insert (column or row-filter).
    /// Carries the caller-scoped reason for the structured `403` body; the predicate,
    /// policy id, and role stay server-side (logged only).
    #[error("write denied")]
    WriteDenied(WriteDenialReason),
```

- [ ] **Step 3: Wire `run_action`'s deny arm**

In `src/services/query-api/src/action.rs`, replace the verdict match (currently
`action.rs:177-190`) so the denying verdict maps through `WriteDenialReason`,
keeping the existing `tracing::info!` lines and returning the structured error:

```rust
    let verdict =
        write_filter::check_write_policy(&write_policies.items, &set_columns, &set_values);
    if let Some(reason) = WriteDenialReason::from_verdict(verdict) {
        match &reason {
            WriteDenialReason::Column(col) => {
                tracing::info!(action = action_name, column = %col, "write denied: policy denies column");
            }
            WriteDenialReason::RowFilter => {
                tracing::info!(
                    action = action_name,
                    "write denied: row fails write policy filter"
                );
            }
        }
        return Err(ActionError::WriteDenied(reason));
    }
```

Update the inline comment block just above it (`action.rs:157-160`) so it no longer
says "The HTTP body stays a generic 403; the reason is logged only" — replace that
sentence with: "Fail-closed (deny on UNKNOWN). A denial maps to a structured
`WriteDenied` reason rendered into the 403 body (caller-scoped: column name only),
and is still logged server-side."

- [ ] **Step 4: Run the pure test**

```
buck2 test //src/services/query-api:write-denial-reason > /tmp/t.log 2>&1
grep -E "Tests finished|FAIL|error\[" /tmp/t.log
```

---

### Task 2: HTTP mapping + body tests (MemoryControlPlane, no fixture)

**Files:**
- Modify: `src/services/query-api/src/http.rs` — add the `WriteDenied` arm in `post_action`.
- Create: `src/services/query-api/tests/write_denial_http.rs` — body tests via the router.
- Modify: `src/services/query-api/BUCK` — add `write-denial-http` rust_test target.

- [ ] **Step 1: Add the HTTP arm**

In `src/services/query-api/src/http.rs::post_action`, add a new match arm *before*
the existing `ActionError::Forbidden => StatusCode::FORBIDDEN.into_response()` arm
(leave that arm in place for the coarse gate). `Json` is already imported:

```rust
        Err(crate::action::ActionError::WriteDenied(reason)) => {
            (StatusCode::FORBIDDEN, Json(reason.to_body())).into_response()
        }
```

- [ ] **Step 2: Write the body tests**

Create `src/services/query-api/tests/write_denial_http.rs`. Mirror
`tests/action_conformance_http.rs` for the harness shape (MemoryControlPlane, stub
serving + ok write engine, `router` + `oneshot` POST). Seed a `Widget` type +
`createWidget` action, grant the coarse Write, then `set_policy` the fine-grained
Write policy under test. Helper `post_json` returns `(StatusCode, serde_json::Value)`.

```rust
//! The /actions/:name route renders a fine-grained Write-policy denial as a
//! structured 403 body (column vs row_filter), keeps Allow at 201, and leaves the
//! coarse-gate denial as a bodyless 403. In-memory: the denial fires before any
//! DuckLake access, so a stub serving engine + no-op write engine suffice (no fixture).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, ActionDef, ActionName, CompareOp, ControlPlane, Effect, ObjectType, Ontology,
    ParamDef, Policy, PolicyTarget, PropertyDef, RoleId, RowFilter, ScalarValue, SubjectId,
    TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, Rows, ServingEngine, ServingError, SqlValue};
use serde_json::json;
use tower::ServiceExt;

struct StubServing;

#[async_trait]
impl ServingEngine for StubServing {
    async fn fetch_rows(&self, _sql: &str, _params: &[SqlValue]) -> Result<Rows, ServingError> {
        Ok(Rows { columns: vec![], rows: vec![] })
    }
}

/// No-op write engine: the conforming (Allow) path reaches it; the denial paths do not.
struct OkEngine;

#[async_trait]
impl ActionEngine for OkEngine {
    async fn write_object(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
        _logical_types: &[String],
        _event: control_plane_core::LineageEvent,
    ) -> Result<control_plane_core::SnapshotId, ServingError> {
        Ok(control_plane_core::SnapshotId(1))
    }
}

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef { name: name.into(), ty: ty.into(), required }
}
fn param(name: &str, ty: &str, required: bool) -> ParamDef {
    ParamDef { name: name.into(), ty: ty.into(), required }
}

/// Seed the type + action + coarse Write grant; return the cp (so the caller can
/// `set_policy` the fine-grained policy under test) and the writer role.
async fn seed() -> (MemoryControlPlane, RoleId) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(ObjectType {
        name: TypeName("Widget".into()),
        properties: vec![prop("id", "Long", true), prop("name", "String", false)],
        derived: vec![],
        table: TableRef { schema: "main".into(), name: "widget".into() },
        identity: Some("id".into()),
    })
    .await
    .unwrap();
    cp.define_action(ActionDef {
        name: ActionName("createWidget".into()),
        target: TypeName("Widget".into()),
        parameters: vec![param("id", "Long", true), param("name", "String", false)],
    })
    .await
    .unwrap();

    let analyst = SubjectId("analyst".into());
    let writer = RoleId("writer".into());
    cp.define_subject(&analyst).await.unwrap();
    cp.define_role(&writer).await.unwrap();
    cp.assign_role(&analyst, &writer).await.unwrap();
    cp.grant(
        &writer,
        Action::Write,
        PolicyTarget::Type(TypeName("Widget".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    (cp, writer)
}

async fn post_json(
    cp: MemoryControlPlane,
    subject: &str,
    action: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let state = AppState {
        cp: Arc::new(cp) as Arc<dyn ControlPlane>,
        serving: Arc::new(StubServing),
        action_engine: Arc::new(OkEngine),
    };
    let app = router(state);
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/actions/{action}"))
                .header("X-Loom-Subject", subject)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}

#[tokio::test(flavor = "multi_thread")]
async fn column_denial_body_names_the_column() {
    let (cp, writer) = seed().await;
    cp.set_policy(
        &writer,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(TypeName("Widget".into())),
            row_filter: None,
            deny_columns: vec!["name".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    let (status, body) = post_json(cp, "analyst", "createWidget", json!({"id": "1", "name": "x"})).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "body: {body}");
    assert_eq!(
        body,
        json!({ "error": "write_denied", "reason": "column", "column": "name" })
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn row_filter_denial_body_discloses_no_predicate() {
    let (cp, writer) = seed().await;
    cp.set_policy(
        &writer,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(TypeName("Widget".into())),
            row_filter: Some(RowFilter::Compare {
                property: "name".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("gadget".into()),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    let (status, body) =
        post_json(cp, "analyst", "createWidget", json!({"id": "1", "name": "widget"})).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "body: {body}");
    assert_eq!(body, json!({ "error": "write_denied", "reason": "row_filter" }));
    assert!(body.get("column").is_none(), "no column on a row-filter denial");
    // The predicate string ("gadget") must never appear in the caller-facing body.
    assert!(
        !body.to_string().contains("gadget"),
        "row_filter predicate must not leak to the caller: {body}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn allowed_write_is_201() {
    let (cp, writer) = seed().await;
    cp.set_policy(
        &writer,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(TypeName("Widget".into())),
            row_filter: Some(RowFilter::Compare {
                property: "name".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("gadget".into()),
            }),
            deny_columns: vec![],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    let (status, _body) =
        post_json(cp, "analyst", "createWidget", json!({"id": "1", "name": "gadget"})).await;
    assert_eq!(status, StatusCode::CREATED);
}

#[tokio::test(flavor = "multi_thread")]
async fn coarse_gate_denial_is_bodyless_403() {
    // A subject with no coarse Write grant: the coarse gate denies, unchanged by
    // this slice — a bodyless 403 (not the structured write_denied body).
    let (cp, _writer) = seed().await;
    let stranger = SubjectId("stranger".into());
    cp.define_subject(&stranger).await.unwrap();
    let (status, body) =
        post_json(cp, "stranger", "createWidget", json!({"id": "1", "name": "gadget"})).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body, serde_json::Value::Null, "coarse-gate 403 stays bodyless");
}
```

Add to `src/services/query-api/BUCK`:

```python
rust_test(
    name = "write-denial-http",
    crate = "write_denial_http",
    srcs = ["tests/write_denial_http.rs"],
    crate_root = "tests/write_denial_http.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:async-trait",
        "//third-party:axum",
        "//third-party:http-body-util",
        "//third-party:serde_json",
        "//third-party:tokio",
        "//third-party:tower",
    ],
)
```

- [ ] **Step 2b: Confirm `ParamDef` is exported from `control_plane_core`**

`action_conformance_http.rs` already imports `ParamDef` from `control_plane_core`,
so the import above is valid; no change needed. If a symbol is missing, grep the
re-exports in `src/control-plane/core/src/lib.rs`.

- [ ] **Step 3: Run the body tests**

```
buck2 test //src/services/query-api:write-denial-http > /tmp/t.log 2>&1
grep -E "Tests finished|FAIL|error\[" /tmp/t.log
```

---

### Task 3: Update the existing fixture e2e to assert the new variant

**Files:**
- Modify: `src/services/query-api/tests/action_e2e.rs` — in
  `write_policy_enforces_row_filter_and_deny_column`, the row-filter and
  deny-column denials now return `ActionError::WriteDenied(...)`, not `Forbidden`.

- [ ] **Step 1: Update the row-filter assertion**

The denial at `action_e2e.rs:372-375` (row filter) must now match the structured
variant:

```rust
    assert!(
        matches!(
            err,
            ActionError::WriteDenied(query_api::action::WriteDenialReason::RowFilter)
        ),
        "row filter denies non-gadget: {err:?}"
    );
```

- [ ] **Step 2: Update the deny-column assertion**

The denial at `action_e2e.rs:424-427` (deny column on `name`) must now match:

```rust
    assert!(
        matches!(
            &err,
            ActionError::WriteDenied(query_api::action::WriteDenialReason::Column(c)) if c == "name"
        ),
        "deny-column blocks setting name: {err:?}"
    );
```

Leave `ungranted_subject_is_forbidden` (the coarse-gate test at
`action_e2e.rs:249`) asserting `ActionError::Forbidden` — that path is unchanged.

- [ ] **Step 3: Run the fixture e2e**

```
buck2 test //src/services/query-api:action-e2e > /tmp/t.log 2>&1
grep -E "Tests finished|FAIL|error\[" /tmp/t.log
```

---

### Task 4: Full suite, lint, docs, commit/push

- [ ] **Step 1: Full query-api + control-plane suite**

```
buck2 test //src/... > /tmp/full.log 2>&1
grep -E "Tests finished|FAIL" /tmp/full.log
```

(The whole `//src/...` sweep guards against the shared-dep regressions CLAUDE.md
warns about, even though this diff adds no new dep.)

- [ ] **Step 2: clippy + lint hooks**

```
./tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -5 /tmp/clippy.log
buck2 run //tools:prek -- run --all-files > /tmp/lint.log 2>&1; tail -20 /tmp/lint.log
```

Commit whatever the hooks change (markdown EOF/whitespace, rustfmt).

- [ ] **Step 3: Update `docs/ROADMAP.md`**

Mark `road-structured-write-denial` done: `- [ ]`→`- [x]`, `status:planned`→`status:done`,
add `pr:#<n>` once the PR is opened (via `loom-docs-update`).

- [ ] **Step 4: Commit and push**

```
git add -A
git commit -m "feat(query-api): structured caller-scoped reason for action write-denial"
git push -u origin work/road-structured-write-denial
```
