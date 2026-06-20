# Actions param↔property conformance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Validate at invoke time that an `ActionDef` conforms to its target `ObjectType` (param↔property name match, compatible logical types, required-property coverage), surfacing a misconfigured action as a clear error instead of an opaque insert-time 500.

**Architecture:** A pure `check_conformance(&ActionDef, &ObjectType) -> Result<(), ActionError>` in query-api's `action.rs`, called inside `run_action` after the coarse `Action::Write` gate and before `parse_params`. A new `ActionError::Misconfigured(String)` carries the joined violation message; `post_action` maps it to 500-with-body. Entirely localized to query-api — no control-plane trait change.

**Tech Stack:** Rust, buck2, axum, `control_plane_core` (`ActionDef`, `ObjectType`, `resolve_logical`), `MemoryControlPlane` fake for tests.

## Global Constraints

- **Tests are `rust_test` integration targets only** — NO inline `#[cfg(test)]`/`#[test]` in `src/**.rs`. Each test is a sibling `tests/<name>.rs` wired in `src/services/query-api/BUCK`.
- **This slice needs no fixture (Postgres/DuckDB) tests.** Conformance fails *before* any DuckLake access, so all three test targets are plain `rust_test` over the in-memory `MemoryControlPlane` + stub engines. (This realizes the spec's "real router → 500 + body, nothing written" intent without a `loom_fixture_test`: the recording stub engine proves no insert is attempted.)
- **Enforce at `run_action` only** (invoke time). Do NOT touch the `Ontology` trait, `define_action`, or the control-plane adapters.
- **Conformance ordering in `run_action`:** after the coarse `Action::Write` gate (no definition-validity leak to unauthorized callers), before `parse_params`/insert.
- **Type compatibility = same `BaseType`** via `control_plane_core::resolve_logical` (no Integer/Long widening), matching `satisfies` semantics.
- **`Misconfigured` → HTTP 500 with the descriptive body** (distinct from the catch-all opaque 500, and from `BadParams`→400).
- Commit messages: Conventional Commits; end every commit body with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- Run tests with the file-redirect pattern (never pipe `buck2 test` through `tail`/`head`): `buck2 test //src/services/query-api:<target> > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`.

---

### Task 1: `check_conformance` + `ActionError::Misconfigured`

**Files:**
- Modify: `src/services/query-api/src/action.rs` (add the `Misconfigured` variant to `ActionError` ~line 35; add `pub fn check_conformance` after the enum, before `run_action`)
- Create: `src/services/query-api/tests/action_conformance.rs`
- Modify: `src/services/query-api/BUCK` (add the `action-conformance` target)

**Interfaces:**
- Consumes (existing): `control_plane_core::{ActionDef, ActionName, ParamDef, ObjectType, PropertyDef, TypeName, resolve_logical}`; the `ActionDef { name: ActionName, target: TypeName, parameters: Vec<ParamDef> }`, `ParamDef { name: String, ty: String, required: bool }`, `PropertyDef { name: String, ty: String, required: bool }`, `ObjectType { name: TypeName, properties: Vec<PropertyDef>, derived, table, identity }` shapes.
- Produces (used by Tasks 2–3): `ActionError::Misconfigured(String)`; `pub fn check_conformance(action: &ActionDef, target: &ObjectType) -> Result<(), ActionError>`.

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/action_conformance.rs`:

```rust
//! check_conformance validates that an ActionDef's parameters mirror its target ObjectType's
//! properties: every param names a real property of a compatible logical type (same BaseType),
//! and every required property is covered by a required param. Pure; collects ALL violations
//! into one ActionError::Misconfigured message.

use control_plane_core::{
    ActionDef, ActionName, ObjectType, ParamDef, PropertyDef, TableRef, TypeName,
};
use query_api::action::{ActionError, check_conformance};

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
    }
}

fn param(name: &str, ty: &str, required: bool) -> ParamDef {
    ParamDef {
        name: name.into(),
        ty: ty.into(),
        required,
    }
}

/// Widget: id (Long, required), name (String, optional).
fn widget(props: Vec<PropertyDef>) -> ObjectType {
    ObjectType {
        name: TypeName("Widget".into()),
        properties: props,
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "widget".into(),
        },
        identity: Some("id".into()),
    }
}

fn action(params: Vec<ParamDef>) -> ActionDef {
    ActionDef {
        name: ActionName("createWidget".into()),
        target: TypeName("Widget".into()),
        parameters: params,
    }
}

fn msg(err: &ActionError) -> String {
    match err {
        ActionError::Misconfigured(m) => m.clone(),
        other => panic!("expected Misconfigured, got {other:?}"),
    }
}

#[test]
fn exact_mirror_conforms() {
    let target = widget(vec![prop("id", "Long", true), prop("name", "String", false)]);
    let act = action(vec![param("id", "Long", true), param("name", "String", false)]);
    assert!(check_conformance(&act, &target).is_ok());
}

#[test]
fn param_matching_no_property_is_rejected() {
    let target = widget(vec![prop("id", "Long", true)]);
    let act = action(vec![param("id", "Long", true), param("naem", "String", false)]);
    let m = msg(&check_conformance(&act, &target).unwrap_err());
    assert!(
        m.contains("parameter `naem` matches no property of type `Widget`"),
        "got: {m}"
    );
}

#[test]
fn type_mismatch_is_rejected() {
    // Integer and Long are distinct base types (no widening).
    let target = widget(vec![prop("id", "Long", true)]);
    let act = action(vec![param("id", "Integer", true)]);
    let m = msg(&check_conformance(&act, &target).unwrap_err());
    assert!(
        m.contains("parameter `id` type `Integer` is incompatible with property `id` type `Long`"),
        "got: {m}"
    );
}

#[test]
fn unknown_param_type_is_rejected() {
    let target = widget(vec![prop("id", "Long", true)]);
    let act = action(vec![param("id", "Lng", true)]);
    let m = msg(&check_conformance(&act, &target).unwrap_err());
    assert!(
        m.contains("parameter `id` has unknown logical type `Lng`"),
        "got: {m}"
    );
}

#[test]
fn unknown_property_type_is_rejected() {
    let target = widget(vec![prop("id", "Lng", true)]);
    let act = action(vec![param("id", "Long", true)]);
    let m = msg(&check_conformance(&act, &target).unwrap_err());
    assert!(
        m.contains("property `id` of type `Widget` has unknown logical type `Lng`"),
        "got: {m}"
    );
}

#[test]
fn uncovered_required_property_is_rejected() {
    // name is required but no param covers it.
    let target = widget(vec![prop("id", "Long", true), prop("name", "String", true)]);
    let act = action(vec![param("id", "Long", true)]);
    let m = msg(&check_conformance(&act, &target).unwrap_err());
    assert!(
        m.contains("required property `name` of type `Widget` is not covered by any parameter"),
        "got: {m}"
    );
}

#[test]
fn required_property_covered_by_optional_param_is_rejected() {
    let target = widget(vec![prop("id", "Long", true)]);
    let act = action(vec![param("id", "Long", false)]);
    let m = msg(&check_conformance(&act, &target).unwrap_err());
    assert!(
        m.contains("required property `id` is covered by optional parameter `id`"),
        "got: {m}"
    );
}

#[test]
fn all_violations_are_collected() {
    // naem matches nothing AND required id is uncovered: both appear in one message.
    let target = widget(vec![prop("id", "Long", true)]);
    let act = action(vec![param("naem", "String", true)]);
    let m = msg(&check_conformance(&act, &target).unwrap_err());
    assert!(m.contains("parameter `naem` matches no property"), "got: {m}");
    assert!(
        m.contains("required property `id` of type `Widget` is not covered"),
        "got: {m}"
    );
}
```

- [ ] **Step 2: Add the BUCK target**

In `src/services/query-api/BUCK`, after the `params` target (ends ~line 686), add:

```python
rust_test(
    name = "action-conformance",
    crate = "action_conformance",
    srcs = ["tests/action_conformance.rs"],
    crate_root = "tests/action_conformance.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:action-conformance > /tmp/c1.log 2>&1; grep -E "Tests finished|FAIL|error\[|cannot find" /tmp/c1.log`
Expected: FAIL — `check_conformance` / `ActionError::Misconfigured` not found.

- [ ] **Step 4: Add the `Misconfigured` variant**

In `src/services/query-api/src/action.rs`, in the `ActionError` enum (ends ~line 35), add after the `BadParams` variant:

```rust
    /// The action's definition does not conform to its target type (a server-side
    /// configuration fault). Carries a descriptive message naming every violation, surfaced
    /// to the operator (distinct from the opaque catch-all 500) so the ActionDef can be fixed.
    #[error("action misconfigured: {0}")]
    Misconfigured(String),
```

- [ ] **Step 5: Implement `check_conformance`**

In `src/services/query-api/src/action.rs`, add the import and the function. Extend the `control_plane_core` use (line 6–9) to include `resolve_logical`:

```rust
use control_plane_core::{
    Action, ActionDef, ActionName, ControlPlane, ControlPlaneError, DatasetRef, Decision,
    EventType, LineageEvent, ObjectType, PageReq, PolicyTarget, RunId, SubjectId, resolve_logical,
};
```

(Add `ActionDef`, `ObjectType`, and `resolve_logical` to the existing import list; keep the others.)

Then add the function immediately after the `ActionError` enum (before `run_action`):

```rust
/// Validate that `action`'s parameters conform to `target`'s properties: every parameter names a
/// real property of a compatible logical type (same `BaseType`), and every required property is
/// covered by a required parameter. Pure; collects ALL violations into one message so an operator
/// sees every problem at once. `Ok(())` if conformant, else `ActionError::Misconfigured`.
pub fn check_conformance(action: &ActionDef, target: &ObjectType) -> Result<(), ActionError> {
    let target_name = &target.name.0;
    let mut violations: Vec<String> = Vec::new();

    // Rules 1 & 2: every param names a real property, of a compatible (same-BaseType) logical type.
    for p in &action.parameters {
        match target.properties.iter().find(|prop| prop.name == p.name) {
            None => violations.push(format!(
                "parameter `{}` matches no property of type `{}`",
                p.name, target_name
            )),
            Some(prop) => {
                let prop_base = resolve_logical(&prop.ty);
                let param_base = resolve_logical(&p.ty);
                if prop_base.is_none() {
                    violations.push(format!(
                        "property `{}` of type `{}` has unknown logical type `{}`",
                        prop.name, target_name, prop.ty
                    ));
                } else if param_base.is_none() {
                    violations.push(format!(
                        "parameter `{}` has unknown logical type `{}`",
                        p.name, p.ty
                    ));
                } else if prop_base != param_base {
                    violations.push(format!(
                        "parameter `{}` type `{}` is incompatible with property `{}` type `{}`",
                        p.name, p.ty, prop.name, prop.ty
                    ));
                }
            }
        }
    }

    // Rule 3: every required property is covered by a required parameter.
    for prop in &target.properties {
        if prop.required {
            match action.parameters.iter().find(|p| p.name == prop.name) {
                None => violations.push(format!(
                    "required property `{}` of type `{}` is not covered by any parameter",
                    prop.name, target_name
                )),
                Some(p) if !p.required => violations.push(format!(
                    "required property `{}` is covered by optional parameter `{}` (it could be omitted, writing NULL)",
                    prop.name, p.name
                )),
                Some(_) => {}
            }
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(ActionError::Misconfigured(format!(
            "action `{}` does not conform to type `{}`: {}",
            action.name.0,
            target_name,
            violations.join("; ")
        )))
    }
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:action-conformance > /tmp/c1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/c1.log`
Expected: PASS (8 tests).

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/action.rs src/services/query-api/tests/action_conformance.rs src/services/query-api/BUCK
git commit -m "$(cat <<'EOF'
feat(query): check_conformance for action param-property conformance

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Wire `check_conformance` into `run_action` + handler test

**Files:**
- Modify: `src/services/query-api/src/action.rs` (call `check_conformance` in `run_action`, after the Write gate ~line 71, before `parse_params` ~line 73)
- Create: `src/services/query-api/tests/action_conformance_handler.rs`
- Modify: `src/services/query-api/BUCK` (add the `action-conformance-handler` target)

**Interfaces:**
- Consumes (Task 1): `check_conformance(&action, &target)?` returning `ActionError::Misconfigured`.
- Consumes (existing): `run_action(action_name: &str, body: &serde_json::Map<String, Value>, subject: &SubjectId, deps: &ActionDeps<'_>) -> Result<ObjectRows, ActionError>`; `ActionDeps { cp: &dyn ControlPlane, action_engine: &dyn ActionEngine }`; `crate::serving::{ActionEngine, ServingError, SqlValue}`.

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/action_conformance_handler.rs`:

```rust
//! run_action runs the conformance check after the coarse Write gate and before the insert:
//! a misconfigured action is rejected with ActionError::Misconfigured and the engine is never
//! called; a Write-denied subject still gets Forbidden (gate precedes conformance); a conformant
//! action runs the happy path and calls the engine once.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use control_plane_core::{
    Acl, Action, ActionDef, ActionName, Effect, ObjectType, Ontology, ParamDef, PolicyTarget,
    PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use query_api::action::{ActionDeps, ActionError, run_action};
use query_api::serving::{ActionEngine, ServingError, SqlValue};
use serde_json::json;

/// An ActionEngine that records how many times insert_row was called.
struct RecordingEngine {
    calls: Mutex<u32>,
}

impl RecordingEngine {
    fn new() -> Self {
        Self {
            calls: Mutex::new(0),
        }
    }
    fn calls(&self) -> u32 {
        *self.calls.lock().unwrap()
    }
}

#[async_trait]
impl ActionEngine for RecordingEngine {
    async fn insert_row(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
    ) -> Result<(), ServingError> {
        *self.calls.lock().unwrap() += 1;
        Ok(())
    }
}

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
    }
}

fn param(name: &str, ty: &str, required: bool) -> ParamDef {
    ParamDef {
        name: name.into(),
        ty: ty.into(),
        required,
    }
}

/// MemoryControlPlane with a Widget type (id Long req, name String opt), a conformant
/// `createWidget` action, a misconfigured `createBad` action (extra param `naem`), and a
/// subject `analyst` granted Action::Write on Widget.
async fn seeded() -> (MemoryControlPlane, SubjectId) {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(ObjectType {
        name: TypeName("Widget".into()),
        properties: vec![prop("id", "Long", true), prop("name", "String", false)],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "widget".into(),
        },
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
    cp.define_action(ActionDef {
        name: ActionName("createBad".into()),
        target: TypeName("Widget".into()),
        // `naem` matches no property; `id`/`name` are fine.
        parameters: vec![
            param("id", "Long", true),
            param("name", "String", false),
            param("naem", "String", false),
        ],
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
    (cp, analyst)
}

#[tokio::test(flavor = "multi_thread")]
async fn misconfigured_action_is_rejected_before_insert() {
    let (cp, subj) = seeded().await;
    let engine = RecordingEngine::new();
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
    };
    let body = json!({"id": "1", "name": "g", "naem": "x"});
    let err = run_action("createBad", body.as_object().unwrap(), &subj, &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, ActionError::Misconfigured(m) if m.contains("matches no property")),
        "expected Misconfigured, got {err:?}"
    );
    assert_eq!(engine.calls(), 0, "insert must not run for a misconfigured action");
}

#[tokio::test(flavor = "multi_thread")]
async fn write_denied_subject_is_forbidden_not_misconfigured() {
    // The coarse Write gate precedes conformance: an ungranted subject sees Forbidden, never
    // learning the action is also misconfigured.
    let (cp, _granted) = seeded().await;
    let stranger = SubjectId("stranger".into());
    cp.define_subject(&stranger).await.unwrap();
    let engine = RecordingEngine::new();
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
    };
    let body = json!({"id": "1", "name": "g", "naem": "x"});
    let err = run_action("createBad", body.as_object().unwrap(), &stranger, &deps)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, ActionError::Forbidden),
        "expected Forbidden (gate precedes conformance), got {err:?}"
    );
    assert_eq!(engine.calls(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn conformant_action_runs_the_insert() {
    let (cp, subj) = seeded().await;
    let engine = RecordingEngine::new();
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
    };
    let body = json!({"id": "42", "name": "gadget"});
    let rows = run_action("createWidget", body.as_object().unwrap(), &subj, &deps)
        .await
        .unwrap();
    assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(engine.calls(), 1, "conformant action inserts once");
}
```

- [ ] **Step 2: Add the BUCK target**

In `src/services/query-api/BUCK`, after the `action-conformance` target from Task 1, add:

```python
rust_test(
    name = "action-conformance-handler",
    crate = "action_conformance_handler",
    srcs = ["tests/action_conformance_handler.rs"],
    crate_root = "tests/action_conformance_handler.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:async-trait",
        "//third-party:serde_json",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:action-conformance-handler > /tmp/c2.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/c2.log`
Expected: FAIL — `misconfigured_action_is_rejected_before_insert` fails: without the call site, `run_action("createBad", …)` proceeds past conformance and calls `insert_row` (engine.calls() == 1) rather than returning `Misconfigured`.

- [ ] **Step 4: Add the call site in `run_action`**

In `src/services/query-api/src/action.rs`, in `run_action`, between the coarse Write gate (the `if … == Decision::Deny { return Err(ActionError::Forbidden); }` block ending ~line 71) and the `// 4. Parse + validate the typed params` step (~line 73), insert:

```rust
    // 3b. Conformance: the action's parameters must mirror the target type's properties (names,
    //     compatible logical types, required-property coverage). A misconfigured ActionDef is
    //     surfaced here as a clear error instead of an opaque insert-time fault. Runs after the
    //     Write gate (no definition-validity leak to unauthorized callers) and before any insert.
    check_conformance(&action, &target)?;
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:action-conformance-handler > /tmp/c2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/c2.log`
Expected: PASS (3 tests).

- [ ] **Step 6: Verify the existing action suite still passes**

Run: `buck2 test //src/services/query-api:action-e2e > /tmp/c2b.log 2>&1; grep -E "Tests finished|FAIL" /tmp/c2b.log`
Expected: PASS (the existing action e2e uses conformant actions, so the new check is a no-op for it).

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/action.rs src/services/query-api/tests/action_conformance_handler.rs src/services/query-api/BUCK
git commit -m "$(cat <<'EOF'
feat(query): run conformance check in run_action before insert

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: `post_action` HTTP mapping + router test

**Files:**
- Modify: `src/services/query-api/src/http.rs` (add the `Misconfigured` arm in `post_action` ~line 605, before the catch-all `Err(_)`)
- Create: `src/services/query-api/tests/action_conformance_http.rs`
- Modify: `src/services/query-api/BUCK` (add the `action-conformance-http` target)

**Interfaces:**
- Consumes (Task 2): `run_action` now returns `ActionError::Misconfigured` for a misconfigured action, reachable through `post_action`.
- Consumes (existing): `query_api::http::{AppState, router}`; `AppState { cp: Arc<dyn ControlPlane>, serving: Arc<dyn ServingEngine>, action_engine: Arc<dyn ActionEngine> }`; the route `POST /actions/:action_name`.

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/action_conformance_http.rs`:

```rust
//! The /actions/:name route maps a misconfigured action to 500 with the descriptive conformance
//! body (NOT the opaque catch-all 500), and a conformant action to 201 CREATED. In-memory: the
//! conformance check fails before any DuckLake access, so a stub serving engine + a no-op action
//! engine suffice (no fixture).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use control_plane_core::{
    Acl, Action, ActionDef, ActionName, ControlPlane, Effect, ObjectType, Ontology, ParamDef,
    PolicyTarget, PropertyDef, RoleId, SubjectId, TableRef, TypeName,
};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use query_api::http::{AppState, router};
use query_api::serving::{ActionEngine, Rows, ServingEngine, ServingError, SqlValue};
use serde_json::json;
use tower::ServiceExt;

/// Serving engine that is never called on the action path (reads only). Returns empty.
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

/// No-op write engine: accepts the insert (the conformant path reaches it).
struct OkEngine;

#[async_trait]
impl ActionEngine for OkEngine {
    async fn insert_row(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
    ) -> Result<(), ServingError> {
        Ok(())
    }
}

fn prop(name: &str, ty: &str, required: bool) -> PropertyDef {
    PropertyDef {
        name: name.into(),
        ty: ty.into(),
        required,
    }
}

fn param(name: &str, ty: &str, required: bool) -> ParamDef {
    ParamDef {
        name: name.into(),
        ty: ty.into(),
        required,
    }
}

async fn seeded_state() -> AppState {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.define_type(ObjectType {
        name: TypeName("Widget".into()),
        properties: vec![prop("id", "Long", true), prop("name", "String", false)],
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: "widget".into(),
        },
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
    cp.define_action(ActionDef {
        name: ActionName("createBad".into()),
        target: TypeName("Widget".into()),
        parameters: vec![
            param("id", "Long", true),
            param("name", "String", false),
            param("naem", "String", false),
        ],
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

    AppState {
        cp: Arc::new(cp) as Arc<dyn ControlPlane>,
        serving: Arc::new(StubServing),
        action_engine: Arc::new(OkEngine),
    }
}

async fn post(state: AppState, action: &str, body: serde_json::Value) -> (StatusCode, String) {
    let app = router(state);
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/actions/{action}"))
                .header("X-Loom-Subject", "analyst")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn misconfigured_action_is_500_with_descriptive_body() {
    let (status, body) = post(seeded_state().await, "createBad", json!({"id": "1"})).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "body: {body}");
    assert!(
        body.contains("parameter `naem` matches no property of type `Widget`"),
        "descriptive conformance body, not opaque: {body}"
    );
    assert_ne!(body, "internal error", "must not be the opaque catch-all body");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformant_action_is_201() {
    let (status, body) = post(
        seeded_state().await,
        "createWidget",
        json!({"id": "42", "name": "gadget"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");
}
```

- [ ] **Step 2: Add the BUCK target**

In `src/services/query-api/BUCK`, after the `action-conformance-handler` target, add:

```python
rust_test(
    name = "action-conformance-http",
    crate = "action_conformance_http",
    srcs = ["tests/action_conformance_http.rs"],
    crate_root = "tests/action_conformance_http.rs",
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

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:action-conformance-http > /tmp/c3.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/c3.log`
Expected: FAIL — `misconfigured_action_is_500_with_descriptive_body` fails: without the new arm, `Misconfigured` hits the catch-all `Err(_) => (500, "internal error")`, so the body is "internal error", not the conformance message.

- [ ] **Step 4: Add the `Misconfigured` arm in `post_action`**

In `src/services/query-api/src/http.rs`, in `post_action`'s `match`, add an arm immediately before the catch-all `Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error")` arm (~line 605):

```rust
        // A misconfigured action is a server-side config fault, surfaced with detail (distinct
        // from the opaque catch-all 500 below) so the operator can fix the ActionDef.
        Err(crate::action::ActionError::Misconfigured(m)) => {
            (StatusCode::INTERNAL_SERVER_ERROR, m).into_response()
        }
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:action-conformance-http > /tmp/c3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/c3.log`
Expected: PASS (2 tests).

- [ ] **Step 6: Lint**

Run: `./tools/clippy-all.sh > /tmp/c3b.log 2>&1; grep -iE "warning|error|clean|FAIL" /tmp/c3b.log | head`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/http.rs src/services/query-api/tests/action_conformance_http.rs src/services/query-api/BUCK
git commit -m "$(cat <<'EOF'
feat(query): map misconfigured action to 500 with descriptive body

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Mark the conformance follow-on delivered in the docs

**Files:**
- Modify: `docs/FUTURE.md` (the Actions section — mark the param↔property conformance follow-on delivered; remove/annotate the deferred bullet)
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md` (add a delivered note; drop conformance from the deferred actions list)

**Interfaces:** none (documentation only).

- [ ] **Step 1: Update `docs/FUTURE.md`**

Open `docs/FUTURE.md`, find the **Actions (Step 3)** section. Locate the deferred bullet beginning **"Action parameter ↔ property conformance."** (it describes that part-1 does not cross-check parameters against the target type's properties, surfacing a misconfigured ActionDef as an opaque insert-time 500). Replace that bullet's body with a DELIVERED note, matching the file's existing style for delivered items:

```markdown
- **Action parameter ↔ property conformance.** ✅ DELIVERED. `run_action` validates the
  resolved `ActionDef` against its target `ObjectType` before the insert (after the coarse
  `Action::Write` gate): every parameter must name a property of a compatible logical type
  (same `BaseType`), and every required property must be covered by a required parameter. A
  mismatch is surfaced as `ActionError::Misconfigured` → HTTP 500 with a descriptive body
  naming every violation, instead of an opaque insert-time 500. Invoke-time only (validates
  against the live ontology, so drift is caught); `define_action` fail-fast remains deferred.
```

- [ ] **Step 2: Update `docs/superpowers/specs/2026-06-06-loom-roadmap.md`**

Open `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, find the Actions slice entries (part-1 delivered + the deferred actions list). Add a delivered note alongside the part-1 entry (mirror its phrasing):

```markdown
- Actions param↔property conformance: DELIVERED — `run_action` validates the `ActionDef` against
  its target type (param↔property names, compatible logical types, required-property coverage)
  before the insert, surfacing a misconfigured action as a descriptive 500 rather than an opaque
  insert-time fault.
```

Then remove the "parameter ↔ property conformance" item from the deferred actions list in that file (leave update/delete, custom-logic/multi-step, Iceberg `ActionEngine`, and lineage atomicity intact).

- [ ] **Step 3: Verify the docs no longer list conformance as deferred**

Run: `grep -rn "parameter ↔ property conformance\|param↔property conformance\|parameter ↔ property" docs/FUTURE.md docs/superpowers/specs/2026-06-06-loom-roadmap.md`
Expected: matches appear only in the new DELIVERED text, not under a "deferred"/"follow-up" heading.

- [ ] **Step 4: Commit**

```bash
git add docs/FUTURE.md docs/superpowers/specs/2026-06-06-loom-roadmap.md
git commit -m "$(cat <<'EOF'
docs(query): mark action param-property conformance delivered

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Self-Review

**1. Spec coverage:**
- Invoke-time check in `run_action` after Write gate, before insert → Task 2 (call site) + Task 1 (function). ✓
- Full-mirror rules (name / `resolve_logical` base-type equality / required coverage), all violations collected → Task 1, asserted per-rule + the `all_violations_are_collected` test. ✓
- `ActionError::Misconfigured` → 500 with descriptive body (distinct from opaque catch-all and from BadParams) → Task 3 (http arm + `assert_ne!(body, "internal error")`). ✓
- No control-plane trait change (invoke-time only) → all changes in query-api `action.rs`/`http.rs`. ✓
- Tests: pure unit (Task 1), handler over MemoryControlPlane + recording stub asserting no-insert + forbidden-precedes-conformance (Task 2), router test asserting 500+body and 201 (Task 3). The spec's "DuckLake fixture e2e" is realized as an in-memory router test (Global Constraints note) — equivalent coverage, since conformance fails before any DuckLake access. ✓
- Docs delivered + dropped from deferred → Task 4. ✓

**2. Placeholder scan:** No TBD/TODO/"similar to"/"add validation" — every code step is verbatim. ✓

**3. Type consistency:** `check_conformance(&ActionDef, &ObjectType) -> Result<(), ActionError>` identical in Task 1 (Produces, implementation) and Task 2 (call site). `ActionError::Misconfigured(String)` identical across Tasks 1/3. The violation message substrings asserted in tests (`"matches no property of type \`Widget\`"`, `"is incompatible with property"`, `"is not covered by any parameter"`, `"covered by optional parameter"`, `"has unknown logical type"`) match the `format!` strings in `check_conformance` exactly. `ParamDef`/`PropertyDef`/`ObjectType`/`ActionDef` fields match `control_plane_core`. ✓

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-20-actions-param-property-conformance.md`.
