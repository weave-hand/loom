# Multi-object action response envelope — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a multi-step action's HTTP response return **all** steps' affected objects as an ordered `{steps:[{bind,target,objects}]}` envelope, instead of only the first step's object.

**Architecture:** `run_action` currently returns `(ObjectRows, RunId)` and `run_multi_step` discards every step's affected object but the first. Introduce an `ActionOutcome` enum — `Single(ObjectRows)` from the single-step path (byte-compatible with today) and `Multi(Vec<StepResult>)` from `run_multi_step` (one entry per declared step, in order). `post_action` branches on the outcome: `Single` renders today's bare object at 201; `Multi` renders the `{steps:[...]}` envelope. Static + dynamic (per-action) OpenAPI document the envelope. No lineage change (it already lists every step's target), no re-read, no single-step behavior change.

**Tech Stack:** Rust, axum, utoipa (OpenAPI), serde_json; buck2 `rust_test` (fixture e2e + handler-level + openapi).

## Global Constraints

- **Single-step actions are byte-unchanged.** A lone bind-less step returns the bare affected object at `201 Created` with the `X-Loom-Run-Id` header, exactly as today. Only the multi-step response shape changes (from bare-first-object to the `steps` envelope) — the item records there is **no** caller depending on the bare-first-object multi-step shape.
- **No lineage change, no re-read, no new step semantics.** The envelope echoes each step's already-computed resolved write image (the same `affected_object(target, cols, vals)` the first-object path uses) — not a governed read-back. Update/Delete steps are simply *included*.
- **Ordered array, not a bind-keyed map.** `bind` is `Option<String>` and two unbound steps can share a target (two `LineItem`s), so the response is an ordered `Vec` labelled by both `bind` (optional) and `target` (always present); step order is the disambiguator. Capture per-step results in the loop **before** `coalesce_appends` (which merges same-target appends in the *write* path only).
- **`X-Loom-Run-Id` header** is emitted for both outcomes, unchanged.
- **The `run_action` signature change is a ripple.** Changing its return type to `ActionOutcome` breaks *every* caller — `post_action` and ~6 test files that destructure the tuple and use the `ObjectRows`. All callers are fixed in Task 1 (the same commit as the signature change), or the tree does not compile. Task 1's gate is the **full** `//src/services/query-api/...` sweep.
- Tests are `rust_test` integration targets. Clippy strict on production code (no `unwrap`/`expect`/indexing; `?`/`map_err`). Commit messages end with the two required trailers; Conventional Commits subjects.

---

### Task 1: Response behavior change — `ActionOutcome`, `run_multi_step` collects every step, `post_action` renders the envelope, all callers updated

**Files:**
- Modify: `src/services/query-api/src/action.rs` (add `ActionOutcome`/`StepResult`; change `run_action` + `run_multi_step` return types; single-step arms wrap `Single`; multi-step loop collects `StepResult`s)
- Modify: `src/services/query-api/src/http.rs` (`post_action` renders `Single`/`Multi`)
- Modify test call sites: `src/services/query-api/tests/{action_e2e,update_delete_e2e,action_mapping_e2e,action_conformance_handler,iceberg_action_e2e}.rs` (destructure `ActionOutcome::Single`)
- Test: `src/services/query-api/tests/action_multi_object_e2e.rs` (extend the `createOrderWithLines` case); new `src/services/query-api/tests/action_response_http.rs` (+ BUCK target)

**Interfaces:**
- Produces:
  ```rust
  pub struct StepResult { pub bind: Option<String>, pub target: String, pub rows: ObjectRows }
  pub enum ActionOutcome { Single(ObjectRows), Multi(Vec<StepResult>) }
  ```
  `run_action(...) -> Result<(ActionOutcome, RunId), ActionError>`.
- Consumes: `affected_object(target: &ObjectType, columns: Vec<String>, row: Vec<SqlValue>) -> ObjectRows` (`action.rs:653`); `ObjectRows { columns, logical_types, rows }` (`handler.rs:18`); `ActionStep.bind: Option<String>` / `.target: TypeName` (`ontology.rs:507`); `render::objects_to_json(rows, next) -> {objects, next}` (`render.rs:17`).

- [ ] **Step 1: Extend the multi-object e2e to assert all three step results (failing)**

In `src/services/query-api/tests/action_multi_object_e2e.rs`, the `createOrderWithLines` case binds the result as `_created` (`~L204-212`). Replace the `run_action` call + `_created` binding with the full-outcome assertions. **`Order.id`/`LineItem.id`/`LineItem.orderId` are declared logical type `"Long"`, which `params.rs` maps `NumericString → SqlValue::Int`, so identities are `SqlValue::Int`, NOT `Text`:**

```rust
    let body = json!({ "oid": "500", "li1": "1", "li2": "2" });
    let (outcome, _run_id) = run_action(
        "createOrderWithLines",
        body.as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("multi-step action runs");

    let steps = match outcome {
        query_api::action::ActionOutcome::Multi(s) => s,
        query_api::action::ActionOutcome::Single(_) => panic!("multi-step action must yield Multi"),
    };
    assert_eq!(steps.len(), 3, "one result per declared step, in order");
    // Step 0: the bound Order (id 500).
    assert_eq!(steps[0].bind.as_deref(), Some("order"));
    assert_eq!(steps[0].target, "Order");
    assert_eq!(steps[0].rows.rows.len(), 1);
    let oidc = steps[0].rows.columns.iter().position(|c| c == "id").expect("Order has id");
    assert_eq!(steps[0].rows.rows[0][oidc], SqlValue::Int(500));
    // Steps 1 & 2: the two unbound LineItems, carrying the parent's resolved id.
    for (i, want_id) in [(1usize, 1i64), (2usize, 2i64)] {
        assert_eq!(steps[i].bind, None, "LineItem steps are unbound");
        assert_eq!(steps[i].target, "LineItem");
        let idc = steps[i].rows.columns.iter().position(|c| c == "id").expect("LineItem has id");
        let fkc = steps[i].rows.columns.iter().position(|c| c == "orderId").expect("LineItem has orderId");
        assert_eq!(steps[i].rows.rows[0][idc], SqlValue::Int(want_id));
        assert_eq!(steps[i].rows.rows[0][fkc], SqlValue::Int(500), "cross-step FK wired");
    }
```

(If unsure of the `SqlValue` variant, confirm against `src/services/query-api/src/params.rs` `"Long"` handling — do NOT weaken the assert to ignore the value. `SqlValue` + `run_action` are already in scope in this test.)

- [ ] **Step 2: Run it to verify it fails (RED)**

Run: `buck2 test --console none //src/services/query-api:action_multi_object_e2e`
Expected: FAIL to COMPILE — `run_action` still returns `(ObjectRows, RunId)` and `ActionOutcome` doesn't exist yet.

- [ ] **Step 3: Add `StepResult` + `ActionOutcome`; collect every step in `run_multi_step`**

In `src/services/query-api/src/action.rs`, add near the public types by `affected_object`:

```rust
/// One multi-step action step's affected object, labelled for the response
/// envelope. `bind` is the step's declared name (the semantic key, when it
/// declared one); `target` is the step's type (always present); array order
/// disambiguates unbound same-target steps.
pub struct StepResult {
    pub bind: Option<String>,
    pub target: String,
    pub rows: ObjectRows,
}

/// The affected-object payload an action produces. `Single` is a lone
/// bind-less step (today's byte-compatible bare-object response); `Multi`
/// carries every step's result in declared order.
pub enum ActionOutcome {
    Single(ObjectRows),
    Multi(Vec<StepResult>),
}
```

Change `run_multi_step`'s return type (`action.rs:1249`) to `Result<(ActionOutcome, RunId), ActionError>`. Replace the `first_affected` accumulation:
- Before the loop: replace `let mut first_affected = None;` with `let mut step_results: Vec<StepResult> = Vec::with_capacity(action.steps.len());`.
- Inside the loop, replace the `if first_affected.is_none() { … }` block (`~1335-1339`) with an unconditional per-step capture — in the SAME position the old block held (before the existing `step_env.insert(...)`/`writes.push(write)`), so ordering and the surrounding borrows are unchanged. `pairs` is a `Vec<(String, SqlValue)>` that is *borrowed* (not moved) throughout the loop, so cloning here is exactly what `first_affected` did:

```rust
        let cols: Vec<String> = pairs.iter().map(|(c, _)| c.clone()).collect();
        let vals: Vec<SqlValue> = pairs.iter().map(|(_, v)| v.clone()).collect();
        step_results.push(StepResult {
            bind: step.bind.clone(),
            target: step.target.0.clone(),
            rows: affected_object(target, cols, vals),
        });
```

(`step.target` is a `TypeName` newtype — `.0` is the `String`.)
- Replace the final return (`~1371-1373`):

```rust
    if step_results.is_empty() {
        return Err(ActionError::Misconfigured("action has no steps".into()));
    }
    Ok((ActionOutcome::Multi(step_results), run_id))
```

`coalesce_appends(writes)`, the lineage `outputs` build, and the commit stay exactly as they are (they operate on `writes`, not `step_results`).

- [ ] **Step 4: Change `run_action`'s signature; wrap the single-step arms in `Single`**

Change the signature (`action.rs:527`) return type to `Result<(ActionOutcome, RunId), ActionError>`. The single-step branch (`~577-579`) calls `run_insert`/`run_mutate` (each `Result<(ObjectRows, RunId), ActionError>`); wrap the rows:

```rust
    let (rows, run_id) = match single_step.kind {
        ActionKind::Insert => run_insert(&action, &target, body, subject, deps).await?,
        ActionKind::Update => run_mutate(&action, &target, body, subject, deps, true).await?,
        ActionKind::Delete => run_mutate(&action, &target, body, subject, deps, false).await?,
    };
    Ok((ActionOutcome::Single(rows), run_id))
```

The early `return run_multi_step(...).await;` now type-matches (both return `(ActionOutcome, RunId)`).

- [ ] **Step 5: Render `Single`/`Multi` in `post_action`**

In `src/services/query-api/src/http.rs`, replace the success arm (`~1019-1032`, the `objects_to_json` + unwrap-first) with a match on the outcome, preserving the `X-Loom-Run-Id` header:

```rust
    match crate::action::run_action(&action_name, &obj, &subject.0, &deps).await {
        Ok((outcome, run_id)) => {
            let body = match outcome {
                crate::action::ActionOutcome::Single(rows) => {
                    // Byte-compatible single-object response: the bare affected object.
                    crate::render::objects_to_json(&rows, None)
                        .get("objects")
                        .and_then(|a| a.as_array())
                        .and_then(|a| a.first())
                        .cloned()
                        .unwrap_or(serde_json::Value::Null)
                }
                crate::action::ActionOutcome::Multi(steps) => {
                    let steps_json: Vec<serde_json::Value> = steps
                        .iter()
                        .map(|s| {
                            let objects = crate::render::objects_to_json(&s.rows, None)
                                .get("objects")
                                .cloned()
                                .unwrap_or_else(|| serde_json::json!([]));
                            serde_json::json!({ "bind": s.bind, "target": s.target, "objects": objects })
                        })
                        .collect();
                    serde_json::json!({ "steps": steps_json })
                }
            };
            let mut resp = (StatusCode::CREATED, Json(body)).into_response();
            if let Ok(v) = axum::http::HeaderValue::from_str(&run_id.0.to_string()) {
                resp.headers_mut().insert("X-Loom-Run-Id", v);
            }
            resp
        }
        Err(e) => /* unchanged error mapping */,
    }
```

Keep the `Err(e) => …` mapping exactly as is. (Per-step `next` from `objects_to_json` is dropped — a write image isn't a paged read.)

- [ ] **Step 6: Update every other `run_action` call site (same commit — the signature change breaks them)**

The other callers destructure `(ObjectRows, RunId)` and use the object; they're all **single-step** actions, so the fix is mechanical. Grep them all: `grep -rn 'run_action(' src/services/query-api/tests`. For each that uses the returned object (not just `run_id`), change e.g.:

```rust
let (created, run_id) = run_action(...).await.unwrap();   // created: ObjectRows
```
to:
```rust
let (outcome, run_id) = run_action(...).await.unwrap();
let created = match outcome {
    query_api::action::ActionOutcome::Single(rows) => rows,
    query_api::action::ActionOutcome::Multi(_) => panic!("single-step action yields Single"),
};
```

Known-affected files (confirm + fix each; grep for any not listed):
- `tests/action_e2e.rs` (~189-194, `objects_to_json(&created, None)`)
- `tests/update_delete_e2e.rs` (~51-61, `objects_to_json(&affected, None)`)
- `tests/action_mapping_e2e.rs` (~198-209 and ~444-456)
- `tests/action_conformance_handler.rs` (~218-221, uses `rows.columns`)
- `tests/iceberg_action_e2e.rs` (~126-131)

(A call site that binds the object as `_` and never uses it needs no change.)

- [ ] **Step 7: Handler-level HTTP test — multi-step envelope + single-step back-compat (failing, then green)**

Create `src/services/query-api/tests/action_response_http.rs`, driving `POST /actions/{name}` through the router with auth. The shared driver `e2e_support::post_search` returns only `(StatusCode, Value)` — the `X-Loom-Run-Id` assert needs headers, so add a sibling `post_action_raw` to `tests/e2e_support.rs` that returns `(StatusCode, HeaderMap, Value)` (mirror `post_search`'s router/auth plumbing, but return `resp.headers().clone()` too). Seed the `createOrderWithLines` multi-step action (replicate the `ActionDef` seed from `action_multi_object_e2e.rs`, or factor it into `e2e-support`) and a single-step bind-less Insert action. Assert:

```rust
// Multi-step: 201 with the steps envelope, all three entries, run-id header present.
let (status, headers, body) = post_action_raw(/* router, "createOrderWithLines", json!({"oid":"500","li1":"1","li2":"2"}), admin subject */).await;
assert_eq!(status, StatusCode::CREATED);
assert!(headers.get("X-Loom-Run-Id").is_some(), "run-id header emitted for multi");
let steps = body.get("steps").and_then(|s| s.as_array()).expect("steps envelope");
assert_eq!(steps.len(), 3);
assert_eq!(steps[0]["bind"], json!("order"));
assert_eq!(steps[0]["target"], json!("Order"));
assert_eq!(steps[0]["objects"].as_array().unwrap()[0]["id"], json!("500")); // rendered ids are JSON strings (render_cell for Long)
assert_eq!(steps[1]["bind"], json!(null));
assert_eq!(steps[1]["target"], json!("LineItem"));
assert_eq!(steps[1]["objects"].as_array().unwrap()[0]["orderId"], json!("500"));

// Single-step: 201 with the BARE object (no `steps` wrapper), run-id header present.
let (status, headers, body) = post_action_raw(/* single-step bind-less Insert action */).await;
assert_eq!(status, StatusCode::CREATED);
assert!(headers.get("X-Loom-Run-Id").is_some());
assert!(body.get("steps").is_none(), "single-step response is the bare object, unchanged");
assert!(body.get("id").is_some(), "bare affected object rendered at top level");
```

(The HTTP body renders `Long` ids via `render_cell` as JSON strings — assert `json!("500")`, not a number. Confirm against `render.rs` `render_cell` if unsure.) Wire the new test in `src/services/query-api/BUCK` mirroring `action_multi_object_e2e`'s target (`:query-api`, `:e2e-support`, and the same third-party deps).

- [ ] **Step 8: Run the full query-api sweep (GREEN)**

Run: `buck2 test --console none //src/services/query-api/...`
Expected: `Pass N. Fail 0` — the whole suite compiles (every updated call site from Step 6, plus `post_action`) and passes: the multi-object e2e's three ordered step results, the handler test's `{steps:[...]}` envelope + run-id header + single-step bare-object back-compat, and every pre-existing action test still green.

- [ ] **Step 9: Commit**

```bash
git add src/services/query-api/src/action.rs src/services/query-api/src/http.rs \
        src/services/query-api/tests/ src/services/query-api/BUCK
git commit -m "feat(query-api): multi-step action response returns every step's affected object"
```

---

### Task 2: OpenAPI — document the envelope (static schema + per-action generator)

**Files:**
- Modify: `src/services/query-api/src/openapi.rs` (add `ActionStepsBody` + `ActionStepResult` `ToSchema`; register in `components(schemas(...))`)
- Modify: `src/services/query-api/src/http.rs` (`post_action`'s `#[utoipa::path]` 201 description)
- Modify: `src/services/query-api/src/openapi_gen.rs` (`action_op`'s multi-step 201 branch, `~309-341`)
- Test: `src/services/query-api/tests/openapi.rs` (assert the new schema is present)

**Interfaces:**
- Consumes: the `#[derive(ToSchema)]` + `components(schemas(...))` pattern (mirror `WriteDeniedBody`, `openapi.rs:51-61` + `195-218`); `build_openapi()` (`openapi.rs:227`); the live per-action generator `action_op` (`openapi_gen.rs:309-341`), merged via `live_openapi()`→`ontology_openapi()`.

- [ ] **Step 1: Add the schema structs**

In `src/services/query-api/src/openapi.rs`, near `WriteDeniedBody`:

```rust
/// One step's affected object in a multi-step action response.
#[derive(ToSchema)]
pub struct ActionStepResult {
    /// The step's declared bind name, when it declared one.
    pub bind: Option<String>,
    /// The step's target type name (always present).
    pub target: String,
    /// The step's affected object rows (same shape as an object read).
    pub objects: Vec<serde_json::Value>,
}

/// The 201 body of a MULTI-step action: one entry per declared step, in order.
/// (A single-step action returns the bare affected object instead.)
#[derive(ToSchema)]
pub struct ActionStepsBody {
    pub steps: Vec<ActionStepResult>,
}
```

- [ ] **Step 2: Register both in `components(schemas(...))`**

Add `ActionStepsBody,` and `ActionStepResult,` to the `components(schemas( … ))` list (`openapi.rs:195-218`), alongside `WriteDeniedBody`.

- [ ] **Step 3: Update the static `post_action` 201 doc**

In `src/services/query-api/src/http.rs`, change the 201 line in `post_action`'s `#[utoipa::path]` (`~993`):

```rust
        (status = 201, description = "Action applied. Single-step actions return the bare created/affected object; multi-step actions return an ordered `steps` envelope (one entry per step, labelled by bind + target)", body = crate::openapi::ActionStepsBody),
```

- [ ] **Step 4: Fix the DYNAMIC per-action generator**

In `src/services/query-api/src/openapi_gen.rs`, `action_op` (`~309-341`) currently documents a multi-step action's 201 as a ref to the *primary/root type's* schema with description "Primary object (the first step's affected row)" (`~340`). That is now factually wrong. Update the multi-step branch's 201 response so its description states the multi-step action returns the `{steps:[...]}` envelope and its `body`/schema ref points at `ActionStepsBody` (mirror however single-step actions reference their target schema — replace the primary-object ref with a ref to `ActionStepsBody`). Read `action_op` in full first: keep the single-step branch pointing at the target object's schema (unchanged), change only the multi-step branch.

- [ ] **Step 5: Assert the schema appears in `build_openapi()`**

In `src/services/query-api/tests/openapi.rs`, add:

```rust
#[test]
fn documents_action_steps_envelope() {
    let doc = query_api::openapi::build_openapi();
    let schemas = &doc.components.as_ref().expect("components").schemas;
    assert!(schemas.contains_key("ActionStepsBody"), "ActionStepsBody schema registered");
    assert!(schemas.contains_key("ActionStepResult"), "ActionStepResult schema registered");
}
```

(`components.schemas` is a `BTreeMap<String, RefOr<Schema>>` in utoipa 5.5 — `contains_key` is correct. Match the crate's existing access idiom if it differs.)

- [ ] **Step 6: Run the openapi + full query-api sweep (GREEN)**

Run: `buck2 test --console none //src/services/query-api/...`
Expected: PASS — the new schema is registered and referenced; the existing `post_action_documents_422_and_400`, route-set, and per-action-generator tests still pass; Task 1's action tests stay green.

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/openapi.rs src/services/query-api/src/openapi_gen.rs \
        src/services/query-api/src/http.rs src/services/query-api/tests/openapi.rs
git commit -m "docs(query-api): document the multi-step action steps envelope (static + per-action OpenAPI)"
```
