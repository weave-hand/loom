# Kind-true action status codes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `POST /actions/{name}` status **kind-true** — `201 Created` for `Insert`, `200 OK` for `Update` and `Delete` (currently always `201`) — keeping the body and `X-Loom-Run-Id` header unchanged, and updating the generated + static OpenAPI so both stay truthful.

**Architecture:** The status is chosen from the **primary step's `ActionKind`** (single-step: the lone step; multi-step: `action.steps.first()` — the same "primary" the docs already key on). `run_action` already returns `(ActionOutcome, RunId)` (from the just-landed multi-step-envelope work — this plan builds on that, NOT on the spec's stale `(ObjectRows, RunId)`); this plan adds the primary kind as a third element `(ActionOutcome, RunId, ActionKind)` (`ActionKind` is `Copy`). `post_action` selects the status; `action_op` documents the matching status per kind.

**Tech Stack:** Rust, axum, utoipa; buck2 `rust_test`.

## Global Constraints

- **Status only.** Body shape and the `X-Loom-Run-Id` header are UNCHANGED for every kind — only the success status line changes (`201` Insert / `200` Update+Delete). The `400/403/404/422/500` error statuses are unchanged.
- **`post_action` only.** No other endpoint's status changes.
- **Primary-step kind.** Multi-step actions key on their FIRST step's kind (e.g. `createOrderWithLines` is `Insert`-first → still `201`).
- **`run_action` gains a third return element** `(ActionOutcome, RunId, ActionKind)`. This is a ripple: the one production caller (`post_action`) plus **every** test call site that destructures the tuple must add `, _kind` (or use it) — in the SAME commit, or the tree won't compile. **Do not trust a hardcoded list — `grep -rn 'run_action(' src/services/query-api/tests` and fix every 2-tuple destructure.** The known sites (verify + fix each): `action_e2e.rs`, `action_conformance_handler.rs`, `update_delete_e2e.rs`, `action_mapping_e2e.rs` (×2), `iceberg_action_e2e.rs`, `multi_step_run.rs` (~214), and `action_multi_object_e2e.rs` (**3 sites** — ~178, ~570, ~713). Task 1's gate is the FULL `//src/services/query-api/...` sweep, which is what actually catches any missed site.
- **Generated + static docs move together.** `action_op` (`openapi_gen.rs`), `docs/guides/actions.md`, and `docs/system-capabilities/query-api.md` must all state the kind-true statuses. The *Breaking* release note (no CHANGELOG file exists) goes in the PR body.
- Tests are `rust_test` integration targets. Clippy strict on production code. Commit trailers + Conventional Commits.

---

### Task 1: Thread the primary `ActionKind`; `post_action` selects a kind-true status; per-kind HTTP e2e

**Files:**
- Modify: `src/services/query-api/src/action.rs` (`run_action` + `run_multi_step` return `(ActionOutcome, RunId, ActionKind)`)
- Modify: `src/services/query-api/src/http.rs` (`post_action` selects status from kind)
- Modify call sites (add `, _kind`): `src/services/query-api/tests/{action_e2e,action_conformance_handler,update_delete_e2e,action_mapping_e2e,iceberg_action_e2e,multi_step_run,action_multi_object_e2e}.rs` (grep-verified — see Global Constraints)
- Test: extend `src/services/query-api/tests/action_response_http.rs` (the Postgres + real-engine fixture test) with per-kind HTTP status assertions. **NOT `action_run_id_http.rs`** — it uses `MemoryControlPlane` + a fake `CapturingEngine` whose default `write_delta`/`write_steps` error, so it cannot execute Update/Delete/multi-step; its existing Insert→`CREATED` assertion stays as-is (insert is unchanged).

**Interfaces:**
- Consumes: `ActionKind::{Insert,Update,Delete}` (`core/src/ontology.rs:368`, `#[derive(Clone,Copy,...)]`); `ActionStep.kind: ActionKind`; `single_step.kind` (`action.rs:577`); `action.steps.first()`.
- Produces: `run_action(...) -> Result<(ActionOutcome, RunId, ActionKind), ActionError>`.

- [ ] **Step 1: Per-kind status HTTP e2e (failing)**

Add a new `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]` to `src/services/query-api/tests/action_response_http.rs` — the Postgres + real `InProcessServingEngine` fixture test, whose `post_action_raw` path can execute Insert/Update/Delete/multi (the fake-engine `action_run_id_http.rs` cannot). Read that file's existing test first to reuse its exact `PgFixture::shared()`/`fresh_db()`/warehouse/`InProcessServingEngine`/`writer_on`/`post_action_raw` setup.

Seed a `Widget` type with an identity property, define single-step `insertWidget`/`updateWidget`/`deleteWidget` `ActionDef`s (mirror `update_delete_e2e.rs`'s Update/Delete action seeds for the exact `ActionKind::{Update,Delete}` `ActionDef` shape and identity targeting), grant Write via the file's `writer_on` helper, then:

```rust
// Insert → 201 Created (mints a row Update/Delete then target).
let (s, _h, _b) = post_action_raw(cp.clone(), eng.clone(), "insertWidget", &json!({ ... }), "writer").await;
assert_eq!(s, StatusCode::CREATED, "Insert is 201");
// Update → 200 OK; run-id header still present.
let (s, h, _b) = post_action_raw(cp.clone(), eng.clone(), "updateWidget", &json!({ /* identity + delta */ }), "writer").await;
assert_eq!(s, StatusCode::OK, "Update is kind-true 200");
assert!(h.get("X-Loom-Run-Id").is_some());
// Delete → 200 OK.
let (s, h, _b) = post_action_raw(cp.clone(), eng.clone(), "deleteWidget", &json!({ /* identity */ }), "writer").await;
assert_eq!(s, StatusCode::OK, "Delete is kind-true 200");
assert!(h.get("X-Loom-Run-Id").is_some());
```

Add a **multi-step primary-kind guard** in the same (or a sibling) test: `e2e_support::define_create_order_with_lines_action(&cp).await` (takes `&PgControlPlane` — compatible), grant Write on `Order`/`LineItem`, POST it, and assert `StatusCode::CREATED` (Insert-first → 201, proving "first step wins"). (Match `post_action_raw`'s real signature from `e2e_support.rs:1145` — the pseudo-args above are illustrative.) No BUCK change — `action_response_http.rs` is already a `loom_fixture_test`. The existing `action_run_id_http.rs` Insert→`CREATED` assertion is left untouched (insert is unchanged).

- [ ] **Step 2: Run it to verify it fails (RED)**

Run: `buck2 test --console none //src/services/query-api:action_response_http`
Expected: FAIL — Update/Delete currently return `201`, so the `StatusCode::OK` asserts fail (or it won't compile until `run_action`'s signature changes — either is the RED signal).

- [ ] **Step 3: Add the primary kind to `run_action` + `run_multi_step`**

In `src/services/query-api/src/action.rs`:
- Change `run_action`'s return type (`~533`) to `Result<(ActionOutcome, RunId, ActionKind), ActionError>`. The single-step path already binds `single_step.kind`; return it:
  ```rust
      let kind = single_step.kind;
      let (rows, run_id) = match kind {
          ActionKind::Insert => run_insert(&action, &target, body, subject, deps).await?,
          ActionKind::Update => run_mutate(&action, &target, body, subject, deps, true).await?,
          ActionKind::Delete => run_mutate(&action, &target, body, subject, deps, false).await?,
      };
      Ok((ActionOutcome::Single(rows), run_id, kind))
  ```
- Change `run_multi_step`'s return type (`~1286`) to `Result<(ActionOutcome, RunId, ActionKind), ActionError>`. Its primary kind is the first step's:
  ```rust
      let primary_kind = action.steps.first().map(|s| s.kind).unwrap_or_default();
  ```
  (place near the top, before the empty-steps guard is a fine spot — or reuse the empty check: `run_multi_step` already errors on no steps, so `first()` is `Some` at the return; `unwrap_or_default()` keeps it total without an `expect`.) Change its final return to `Ok((ActionOutcome::Multi(step_results), run_id, primary_kind))`.
- The early `return run_multi_step(...).await;` in `run_action` (`~555`) now type-matches (both are the 3-tuple).

- [ ] **Step 4: `post_action` selects the status from the kind**

In `src/services/query-api/src/http.rs`, change the success arm (`~1018-1050`): destructure the third element and compute the status; the body/header logic is otherwise unchanged:

```rust
        Ok((outcome, run_id, kind)) => {
            let body = match outcome {
                // ... unchanged Single/Multi rendering ...
            };
            let status = match kind {
                crate::action::ActionKind::Insert => StatusCode::CREATED,
                crate::action::ActionKind::Update | crate::action::ActionKind::Delete => StatusCode::OK,
            };
            let mut resp = (status, Json(body)).into_response();
            if let Ok(v) = axum::http::HeaderValue::from_str(&run_id.0.to_string()) {
                resp.headers_mut().insert("X-Loom-Run-Id", v);
            }
            resp
        }
```

(Import `ActionKind` via the path used elsewhere — `crate::action::ActionKind` re-export or `control_plane_core::ActionKind`; match how `action.rs`/`http.rs` already name it. The `Err` mapping is unchanged.)

- [ ] **Step 5: Add `, _kind` to every other `run_action` destructure site (same commit)**

The signature change breaks each test that binds the tuple. Grep: `grep -rn 'run_action(' src/services/query-api/tests`. For each `let (outcome, run_id) = run_action(...)` (or `(_, _)` etc.), add the third binding `, _kind` (or `, _`):

```rust
let (outcome, run_id, _kind) = run_action(...).await.unwrap();
```

Known sites (confirm + fix each; the multi-step-envelope work left them at a 2-tuple): `action_e2e.rs`, `action_conformance_handler.rs`, `update_delete_e2e.rs`, `action_mapping_e2e.rs` (2 sites), `iceberg_action_e2e.rs`, `action_multi_object_e2e.rs`. (A site that already binds `run_action(...).await.expect(...)` without destructuring the tuple needs no change.)

- [ ] **Step 6: Run the full query-api sweep (GREEN)**

Run: `buck2 test --console none //src/services/query-api/...`
Expected: `Pass N. Fail 0` — the whole suite compiles (all call sites) and passes; Insert→201, Update→200, Delete→200, multi-step-Insert-first→201; every pre-existing test green (`action_op` still emits `201` for all kinds, so `openapi_gen.rs`'s existing 201-for-update/delete assertion stays green until Task 2 flips it).

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/action.rs src/services/query-api/src/http.rs \
        src/services/query-api/tests/
git commit -m "feat(query-api): kind-true action status (200 for Update/Delete, 201 for Insert)"
```

---

### Task 2: Truthful OpenAPI + static docs

**Files:**
- Modify: `src/services/query-api/src/openapi_gen.rs` (`action_op` status string keyed on primary kind; doc comment)
- Modify: `src/services/query-api/src/http.rs` (`post_action`'s static `#[utoipa::path]` — the *generic* `/actions/{action_name}` entry in `ApiDoc`, distinct from the per-action `action_op` paths; both are merged into the live doc)
- Modify: `src/services/query-api/tests/openapi_gen.rs` (rewrite the 201-for-update/delete assertion → 200)
- Modify: `docs/guides/actions.md` (on-success line ~159; status table ~191-200)
- Modify: `docs/system-capabilities/query-api.md` (Actions section status claim)

**Interfaces:**
- Consumes: `action_op(action, primary)` (`openapi_gen.rs:320`); the primary step is `primary` (its `.kind`). Currently hardcodes `.response("201", json_response(ok_schema, ok_desc))` (`~362`).

- [ ] **Step 1: Rewrite the openapi_gen assertion (failing)**

In `src/services/query-api/tests/openapi_gen.rs`, replace `update_and_delete_actions_document_201_like_the_handler` (`~360`) — rename to e.g. `update_and_delete_actions_document_200` and invert it:

```rust
#[test]
fn update_and_delete_actions_document_200() {
    // Kind-true: Update/Delete document 200 OK (not 201); Insert still 201.
    let update = ActionDef::single_step(ActionName("updateCustomer".into()), TypeName("Customer".into()), ActionKind::Update, vec![], vec![]);
    let delete = ActionDef::single_step(ActionName("deleteCustomer".into()), TypeName("Customer".into()), ActionKind::Delete, vec![], vec![]);
    let (paths, _schemas) = ontology_openapi(&[customer()], &[], &[update, delete]);

    let up = op_json(&paths, "/actions/updateCustomer", "post");
    assert!(up["responses"]["200"].is_object(), "Update documents 200 OK");
    assert!(up["responses"]["201"].is_null(), "Update no longer documents 201");
    // 2xx body still refs the target type's component schema.
    assert_eq!(up["responses"]["200"]["content"]["application/json"]["schema"]["$ref"], "#/components/schemas/Customer");

    let del = op_json(&paths, "/actions/deleteCustomer", "post");
    assert!(del["responses"]["200"].is_object(), "Delete documents 200 OK");
    assert!(del["responses"]["201"].is_null(), "Delete no longer documents 201");
}
```

Keep the existing Insert-documents-201 assertion (`~350`) unchanged — Insert still refs `#/components/schemas/Customer` at `201`.

- [ ] **Step 2: Run it to verify it fails (RED)**

Run: `buck2 test --console none //src/services/query-api:openapi_gen`
Expected: FAIL — `action_op` still emits `201` for Update/Delete.

- [ ] **Step 3: Key `action_op`'s status string on the primary kind**

In `src/services/query-api/src/openapi_gen.rs`, `action_op` (`~320-374`): the success `.response("201", …)` (`~362`) is hardcoded. Compute the status string from `primary.kind`:

```rust
    let ok_status = match primary.kind {
        ActionKind::Insert => "201",
        ActionKind::Update | ActionKind::Delete => "200",
    };
    // ...
    op.security(bearer())
        .request_body(Some(body))
        .response(ok_status, json_response(ok_schema, ok_desc))
        // ... the 400/403/404/422 responses unchanged ...
```

(`primary` is `action_op`'s second param — the primary step. Multi-step keys on the first step's kind, same rule.) The `ok_desc`/`ok_schema` per-kind logic is unchanged.

- [ ] **Step 4: Update the stale doc comment**

In `openapi_gen.rs` (`~314-319`), the comment says "Every kind documents `201`: `post_action`'s single Ok arm responds `StatusCode::CREATED` regardless of kind … Kind-true statuses are a registered follow-up." Rewrite it to state the status is now **kind-true** — Insert documents `201 Created`, Update/Delete document `200 OK`, matching `post_action`.

- [ ] **Step 4b: Fix the second static OpenAPI source — `post_action`'s `#[utoipa::path]`**

`post_action`'s own `#[utoipa::path]` attribute (`src/services/query-api/src/http.rs:989-999`) is registered in the static `ApiDoc` (`openapi.rs`, `paths(... crate::http::post_action ...)`) and merged into the live doc *alongside* the per-action `action_op` entries (`live_openapi` does `doc.paths.paths.extend(...)`, keyed on the generic `/actions/{action_name}` template — it is NOT replaced by the concrete per-action paths). It still unconditionally documents `201` for every action. Since this generic entry covers ALL kinds, document BOTH statuses. Change its `responses(...)` so it carries a `201` (Insert) **and** a `200` (Update/Delete) entry — e.g.:

```rust
        (status = 201, description = "Insert action applied: the created object (single-step) or the `steps` envelope (multi-step)", body = crate::openapi::ActionStepsBody),
        (status = 200, description = "Update/Delete action applied: the affected object (Update) or pre-deletion values (Delete); the per-action `/docs` entry states each action's exact status", body = crate::openapi::ActionStepsBody),
```

(Keep the existing 400/403/404/422 entries. The per-action `action_op` docs remain the authoritative per-action shape; this generic entry now at least stops advertising `201` as the only success status.)

- [ ] **Step 5: Static docs**

- `docs/guides/actions.md`:
  - On-success line (`~159`): change "On success: **`201 Created`**…" to state Insert → `201 Created`, Update/Delete → `200 OK` (body = affected object / pre-deletion values; `X-Loom-Run-Id` header set).
  - Status-code table (`~191-200`): replace the single `| 201 Created | committed; … |` row with two rows: `| 201 Created | Insert committed; body = new object, X-Loom-Run-Id set |` and `| 200 OK | Update/Delete committed; body = affected/pre-deletion object, X-Loom-Run-Id set |`.
- `docs/system-capabilities/query-api.md`: the Actions section currently says "A single-step action's 201 body is the bare affected object…". Change "201 body" to reflect kind-true status — e.g. "A single-step action's success body is the bare affected object (`201 Created` for Insert, `200 OK` for Update/Delete)…". Keep the multi-step envelope description; the status rule (primary kind) applies to it too.

- [ ] **Step 6: Run the openapi + full sweep (GREEN)**

Run: `buck2 test --console none //src/services/query-api/...`
Expected: PASS — Update/Delete document `200` and no `201`; Insert still `201`; the route-set + `post_action_documents_422_and_400` tests unaffected; Task 1's handler tests still green.

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/openapi_gen.rs src/services/query-api/tests/openapi_gen.rs \
        docs/guides/actions.md docs/system-capabilities/query-api.md
git commit -m "docs(query-api): document kind-true action statuses (generated + static)"
```

(The *Breaking* release note goes in the PR body — no CHANGELOG file exists: "Action responses are now kind-true — `POST /actions/{name}` returns `200 OK` for Update/Delete (was `201`); Insert stays `201`. Body + `X-Loom-Run-Id` unchanged.")
