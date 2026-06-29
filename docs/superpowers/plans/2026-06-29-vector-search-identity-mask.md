# `/search` Fail-Closed on Governed Identity — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `POST /search/{type}/{index_name}` return a deliberate `403 Forbidden` whenever the subject's policy denies or masks the type's identity column, so identity values can never leak through the vector-search endpoint.

**Architecture:** Extract the inline "is the identity column governed?" check (currently buried inside `identity_in_predicate`) into a small pure helper `identity_is_governed`, re-express `identity_in_predicate`'s column check in terms of it (so the two sites cannot drift), and add one guard to `vector_search` that runs **before** the `row_filters.is_empty()` early return. Both governed-identity policy shapes (no-row-filter, and with-row-filter) then fail closed identically with `QueryError::Forbidden`.

**Tech Stack:** Rust, axum, the query-api crate (`src/services/query-api`), `loom_fixture_test` hermetic-Postgres integration tests.

## Global Constraints

- **Tests are `rust_test` integration targets only — never inline `#[cfg(test)]`.** New fixture tests (hermetic Postgres) MUST use the `loom_fixture_test` macro, not a bare `rust_test`, or they route to RE and fail as root.
- **Clippy is strict** (pedantic + restriction on production code): no `unwrap`/`expect`/`panic`/`indexing_slicing`/`todo` in non-test code; test code is exempted from the panic-safety lints via the `loom_fixture_test` wrapper.
- **No behavior change** to ungoverned or identity-less paths — the change is confined to subjects whose policy governs the identity column.
- **Run the suite** with `buck2 test //src/...` (or the targeted query-api targets); fixture tests pin local execution via `loom_fixture_test`. Don't pipe long-running `buck2 test` through `tail`/`head` — redirect to a file and grep it.
- New error response is the endpoint's **existing** `QueryError::Forbidden` variant (→ HTTP 403). No new error kinds.

---

### Task 1: Extract `identity_is_governed` and re-express the existing column check on it

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (add helper near `project_allowed`/`identity_in_predicate` around lines 137–185; re-express the check at `handler.rs:166-169`)
- Test: `src/services/query-api/tests/identity_in_predicate.rs` (existing `rust_test` — already covers the `BadFilter` characterization via `denied_identity_is_bad_filter`/`masked_identity_is_bad_filter`; extend with a direct `identity_is_governed` unit test since the helper is `pub`)

**Interfaces:**
- Consumes: `control_plane_core::ObjectType` (field `identity: Option<String>`, `properties: Vec<PropertyDef>`), `std::collections::HashSet<String>` for `denied`/`masked`.
- Produces: `pub fn identity_is_governed(otype: &ObjectType, denied: &HashSet<String>, masked: &HashSet<String>) -> bool` — `true` when the type's declared identity column is in `denied` or `masked`; `false` when the type has no declared identity. Used by Task 2 (`vector_search` guard) and by `identity_in_predicate`.

- [ ] **Step 1: Confirm the existing characterization tests lock the `BadFilter` contract**

The `identity_in_predicate` function already returns `Err(QueryError::BadFilter(identity))` when the identity column is denied or masked (`handler.rs:166-169`). The existing test file `src/services/query-api/tests/identity_in_predicate.rs` **already** has `denied_identity_is_bad_filter` (line 69) and `masked_identity_is_bad_filter` (line 82) using the fixture builder `customer(identity: Option<String>)`. These are the characterization tests that prove the Step 3 refactor is behavior-preserving — no new ones are needed here. Read the file to confirm before refactoring.

- [ ] **Step 2: Run the characterization tests to verify they pass against current code**

Run: `buck2 test //src/services/query-api:identity_in_predicate > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t1.log`
Expected: PASS (`denied_identity_is_bad_filter`, `masked_identity_is_bad_filter`, and the rest all green — they document the existing `BadFilter` behavior the refactor must preserve).

- [ ] **Step 3: Add the `identity_is_governed` helper and re-express the column check**

Add the helper immediately after `project_allowed` (after `handler.rs:147`):

```rust
/// True when the type's declared identity column is denied or masked by policy,
/// so its values must not be revealed. Identity-less types are never governed here.
pub fn identity_is_governed(
    otype: &ObjectType,
    denied: &std::collections::HashSet<String>,
    masked: &std::collections::HashSet<String>,
) -> bool {
    match &otype.identity {
        Some(id) => denied.contains(id) || masked.contains(id),
        None => false,
    }
}
```

Then re-express the column check inside `identity_in_predicate`. Replace the existing block at `handler.rs:166-169`:

```rust
    let allowed = project_allowed(&otype.properties, denied);
    if !allowed.contains(&identity) || masked.contains(&identity) {
        return Err(QueryError::BadFilter(identity));
    }
```

with:

```rust
    // The identity column must be a permitted filter column: not denied, not masked.
    // Re-expressed via the shared `identity_is_governed` helper so the /search guard
    // and this filter-lowering check cannot drift. (`project_allowed` computes the
    // same deny membership via `!denied.contains`; `identity_is_governed` checks
    // `denied.contains(id) || masked.contains(id)` directly — equivalent.)
    if identity_is_governed(otype, denied, masked) {
        return Err(QueryError::BadFilter(identity));
    }
```

Note: `identity` here is the already-unwrapped identity name (from `otype.identity.clone().ok_or_else(...)` above), so the `BadFilter(identity)` payload is unchanged. `identity_is_governed` re-reads `otype.identity` internally — it is `Some` on this path (we returned early on `None` via `NoIdentity`), so the two agree.

- [ ] **Step 4: Add a direct unit test for `identity_is_governed`**

In `src/services/query-api/tests/identity_in_predicate.rs`, import the helper (extend the existing `use query_api::handler::{QueryError, identity_in_predicate};` to add `identity_is_governed`) and add:

```rust
#[test]
fn identity_is_governed_flags_denied_and_masked() {
    use query_api::handler::identity_is_governed;
    let denied: HashSet<String> = ["id".to_string()].into_iter().collect();
    let masked: HashSet<String> = ["id".to_string()].into_iter().collect();
    // Denied identity → governed.
    assert!(identity_is_governed(&customer(Some("id".into())), &denied, &empty()));
    // Masked identity → governed.
    assert!(identity_is_governed(&customer(Some("id".into())), &empty(), &masked));
    // Ungoverned identity → not governed.
    assert!(!identity_is_governed(&customer(Some("id".into())), &empty(), &empty()));
    // Identity-less type → never governed, even if "id" is denied.
    assert!(!identity_is_governed(&customer(None), &denied, &masked));
}
```

This reuses the file's existing `customer(identity: Option<String>)` and `empty()` helpers — no new fixture. It locks the identity-less → `false` branch that has no path through `identity_in_predicate` (which errors `NoIdentity` first).

- [ ] **Step 5: Run the predicate + helper tests to verify the refactor is behavior-preserving**

Run: `buck2 test //src/services/query-api:identity_in_predicate > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t1.log`
Expected: PASS — all existing `identity_in_predicate` cases plus the new `identity_is_governed_flags_denied_and_masked` pass; the `BadFilter` contract for `identity_in_predicate` is unchanged.

- [ ] **Step 6: Lint the changed crate**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/clippy1.log 2>&1; cat $(buck2 build --show-output '//src/services/query-api:query-api[clippy.txt]' 2>/dev/null | awk '{print $2}') 2>/dev/null || echo "see /tmp/clippy1.log"`
Expected: empty clippy output (clean). The helper is a pure `match`, no panic-safety lint triggers.

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/handler.rs src/services/query-api/tests/identity_in_predicate.rs
git commit -m "refactor(query-api): extract identity_is_governed from identity_in_predicate"
```

---

### Task 2: Guard `vector_search` to fail closed on a governed identity column

**Files:**
- Modify: `src/services/query-api/src/handler.rs` — `vector_search` (the block at `handler.rs:481-485`, immediately after `load_policy` and before the `row_filters.is_empty()` check)

**Interfaces:**
- Consumes: `identity_is_governed` (Task 1), the already-resolved `otype: ObjectType`, and the `(row_filters, denied, masked)` triple returned by `load_policy` inside `vector_search`.
- Produces: no new public surface — `vector_search` keeps its signature `pub async fn vector_search(q, subject, deps) -> Result<Vec<VectorHit>, QueryError>`. Behavior: returns `Err(QueryError::Forbidden)` when the identity column is governed.

- [ ] **Step 1: Insert the guard before the row-filter branch**

In `vector_search`, the current code (`handler.rs:481-485`) reads:

```rust
    // Row-filter post-filter. Empty filters (unrestricted) → return engine hits unchanged.
    let (row_filters, denied, masked) = load_policy(deps.acl, &subject.0, &target).await?;
    if row_filters.is_empty() {
        return Ok(hits);
    }
```

Change it to:

```rust
    // Row-filter post-filter. Empty filters (unrestricted) → return engine hits unchanged.
    let (row_filters, denied, masked) = load_policy(deps.acl, &subject.0, &target).await?;
    // Fail closed: the search response *is* a list of identity values, so a policy that
    // denies or masks the identity column must not be silently disregarded. Guard runs
    // BEFORE the empty-filter early return so both policy shapes (no row filter, and with
    // a row filter) refuse identically with a deliberate 403 instead of leaking ids
    // (empty-filter path) or an incidental BadFilter/500 (row-filter path).
    if identity_is_governed(&otype, &denied, &masked) {
        return Err(QueryError::Forbidden);
    }
    if row_filters.is_empty() {
        return Ok(hits);
    }
```

The rest of `vector_search` (lines 486+: `identity` clone, `identity_in_predicate`, `compile_select_with`, post-filter retain) is unchanged — the guard makes the downstream `identity_in_predicate` call unreachable for governed-identity subjects, but leaving it is correct and harmless (it would also reject, now as dead-for-this-input defensive code).

- [ ] **Step 2: Verify it compiles**

Run: `buck2 build //src/services/query-api:query-api > /tmp/b2.log 2>&1; grep -E "BUILD SUCCEEDED|BUILD FAILED|error\[" /tmp/b2.log || tail -5 /tmp/b2.log`
Expected: BUILD SUCCEEDED.

- [ ] **Step 3: Lint**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/clippy2.log 2>&1; tail -3 /tmp/clippy2.log`
Expected: clean (empty clippy.txt).

- [ ] **Step 4: Commit**

```bash
git add src/services/query-api/src/handler.rs
git commit -m "fix(query-api): /search fails closed when the identity column is governed"
```

---

### Task 3: E2E governance tests — `/search` refuses a governed identity column

**Files:**
- Modify: `src/services/query-api/tests/e2e_support.rs` — add a column-governance grant helper (deny/mask columns), mirroring `grant_read_filtered`.
- Modify: `src/services/query-api/tests/vector_search_e2e.rs` — add the four fixture cases.
- Modify: `src/services/query-api/BUCK` — the `vector_search_e2e` and `e2e-support` targets already exist; no new target needed (extends existing files). Verify deps are sufficient.

**Interfaces:**
- Consumes: the e2e harness — `PgFixture`, `seed_vector_type`, `subject_with_role`, `grant_read`, `post_search` (from `e2e_support`), and `cp.set_policy(...)` for the column policy.
- Produces: `grant_read_columns(cp, role, type_name, deny_columns: Vec<String>, mask_columns: Vec<String>)` in `e2e_support.rs` — coarse `Read` grant plus a `Policy` with the given denied/masked columns and no row filter; mirrors `grant_read_filtered`. Reused by the new test cases.

- [ ] **Step 1: Add the column-governance helper to `e2e_support.rs`**

Read `grant_read_filtered` (`tests/e2e_support.rs:520-541`) for the exact pattern. Add immediately after it:

```rust
/// Coarse `Read` Allow plus a column policy with the given denied/masked columns and
/// no row filter — the shape that governs the identity column. Mirrors
/// `grant_read_filtered` but exercises column governance instead of a row filter.
pub async fn grant_read_columns(
    cp: &PgControlPlane,
    role: &RoleId,
    type_name: &str,
    deny_columns: Vec<String>,
    mask_columns: Vec<String>,
) {
    grant_read(cp, role, type_name).await;
    cp.set_policy(
        role,
        Action::Read,
        Policy {
            target: PolicyTarget::Type(TypeName(type_name.into())),
            row_filter: None,
            deny_columns,
            mask_columns,
        },
    )
    .await
    .unwrap();
}
```

Confirm `Policy`, `PolicyTarget`, `TypeName`, `Action`, `RoleId` are already imported in `e2e_support.rs` (they are — `grant_read_filtered`/`grant_read` use them). If `Policy` is not yet in scope, add it to the existing `use control_plane_core::{...}` group.

- [ ] **Step 2: Write the four failing test cases in `vector_search_e2e.rs`**

The `Docs` type seeded by `seed_vector_type` has identity column `id` (Long). Add to `tests/vector_search_e2e.rs`, importing the new helper (extend the `use e2e_support::{...}` list to add `grant_read_columns`):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_forbidden_when_identity_denied_no_row_filter() {
    let fx = PgFixture::start();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(&fx, &db).await;
    let (_subj, role) = subject_with_role(&cp, "dave").await;
    // Coarse Read + deny the identity column `id`, no row filter.
    grant_read_columns(&cp, &role, "Docs", vec!["id".into()], vec![]).await;
    let cp = Arc::new(cp);

    let (status, _body) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 2 }),
        "dave",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "denied identity column must fail closed, not leak ids"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_forbidden_when_identity_masked_no_row_filter() {
    let fx = PgFixture::start();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(&fx, &db).await;
    let (_subj, role) = subject_with_role(&cp, "erin").await;
    // Coarse Read + mask the identity column `id`, no row filter.
    grant_read_columns(&cp, &role, "Docs", vec![], vec!["id".into()]).await;
    let cp = Arc::new(cp);

    let (status, _body) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 2 }),
        "erin",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "masked identity column must fail closed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_forbidden_when_identity_governed_with_row_filter() {
    let fx = PgFixture::start();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(&fx, &db).await;
    let (_subj, role) = subject_with_role(&cp, "frank").await;
    // Coarse Read first, then a policy that BOTH denies `id` AND carries a row filter.
    // Previously this path returned an incidental BadFilter/500; now a deliberate 403.
    grant_read(&cp, &role, "Docs").await;
    cp.set_policy(
        &role,
        control_plane_core::Action::Read,
        control_plane_core::Policy {
            target: control_plane_core::PolicyTarget::Type(control_plane_core::TypeName(
                "Docs".into(),
            )),
            row_filter: Some(control_plane_core::RowFilter::Compare {
                property: "id".into(),
                op: control_plane_core::CompareOp::Gt,
                value: control_plane_core::ScalarValue::Int(0),
            }),
            deny_columns: vec!["id".into()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();
    let cp = Arc::new(cp);

    let (status, _body) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 2 }),
        "frank",
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "governed identity + row filter must also be a deliberate 403 (symmetry)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_ok_when_identity_ungoverned_regression() {
    let fx = PgFixture::start();
    let (_init, db) = fx.fresh_db().await;
    let (cp, serving, _writer) = seed_vector_type(&fx, &db).await;
    let (_subj, role) = subject_with_role(&cp, "grace").await;
    // Plain Read, identity column not governed → unchanged behavior, value-exact hits.
    grant_read(&cp, &role, "Docs").await;
    let cp = Arc::new(cp);

    let (status, body) = post_search(
        cp.clone(),
        serving.clone(),
        "/search/Docs/by_sim",
        &serde_json::json!({ "query": [1.0, 0.0, 0.0, 0.0], "k": 2 }),
        "grace",
    )
    .await;
    let res = results(status, &body);
    assert_eq!(res.len(), 2, "ungoverned identity returns kNN hits unchanged");
    assert_eq!(res[0]["id"], serde_json::json!(1), "exact match id=1 first");
}
```

Note: imports — `StatusCode`, `Arc`, `PgFixture`, `seed_vector_type`, `subject_with_role`, `grant_read`, `post_search`, `results` are all already used in the file (see lines 9–23). Add only `grant_read_columns` to the `use e2e_support::{...}` import group. `control_plane_core` is referenced fully-qualified in the row-filter case, so no new top-level import is strictly required — but if the file already does `use control_plane_core::{CompareOp, RowFilter, ScalarValue};` (it does, line 11), you may use those short names instead of the fully-qualified forms in the third test for readability. Keep `Action`, `Policy`, `PolicyTarget`, `TypeName` fully-qualified or add them to that `use` group — match whichever the file already imports.

- [ ] **Step 3: Run the new tests to verify they pass against the Task-2 fix**

Run: `buck2 test //src/services/query-api:vector_search_e2e > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|PASS|error\[" /tmp/t3.log`
Expected: all cases PASS — the three governed cases return 403, the regression case returns 200 with id=1 first. (If a case unexpectedly fails to compile because the `vector_search_e2e` target lacks a dep the new code needs, add it to that target's `deps` in `src/services/query-api/BUCK`; the row-filter types come from `//src/control-plane/core` which is already a dep.)

- [ ] **Step 4: Confirm the bug would have been caught — sanity-check the guard is load-bearing**

Temporarily comment out the `identity_is_governed` guard added in Task 2, re-run `search_forbidden_when_identity_denied_no_row_filter`, and confirm it now FAILS (returns 200 with leaked ids). Then restore the guard and confirm PASS again. This proves the test is load-bearing, not vacuous.

Run (after restoring): `buck2 test //src/services/query-api:vector_search_e2e > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: Tests finished, 0 FAIL.

- [ ] **Step 5: Lint the test files**

Run: `buck2 build '//src/services/query-api:vector_search_e2e[clippy.txt]' '//src/services/query-api:e2e-support[clippy.txt]' > /tmp/clippy3.log 2>&1; tail -3 /tmp/clippy3.log`
Expected: clean. Test code is exempt from panic-safety lints via the `loom_fixture_test`/`e2e-support` wrappers.

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/tests/e2e_support.rs src/services/query-api/tests/vector_search_e2e.rs src/services/query-api/BUCK
git commit -m "test(query-api): /search governed-identity fail-closed e2e matrix"
```

---

### Task 4: Update the documentation register

**Files:**
- Modify: `docs/ISSUES.md` — close `iss-vector-search-identity-mask`.

**Interfaces:**
- Consumes: nothing.
- Produces: the register entry flipped to done.

- [ ] **Step 1: Flip the issue entry to resolved**

In `docs/ISSUES.md`, find the `iss-vector-search-identity-mask` line (line ~88). Change `- [ ]` → `- [x]`, set `status:open` → `status:fixed`, and `pr:-` → `pr:#<PR-number>` (fill the real PR number once the PR is opened — this step is finalized during the finishing-a-development-branch step, may be staged with a placeholder and amended). Use `loom-docs-update` to do this so the grammar/validation stays correct.

- [ ] **Step 2: Validate the registers**

Run: `bash tools/docs.sh validate > /tmp/docsval.log 2>&1; cat /tmp/docsval.log`
Expected: validation passes (no grammar/id/vocab/link errors).

- [ ] **Step 3: Commit (done during finishing-a-development-branch, with the real PR number)**

```bash
git add docs/ISSUES.md
git commit -m "docs(issues): close iss-vector-search-identity-mask"
```

---

## Self-Review

**1. Spec coverage:**
- Spec "Part 1 — extract the predicate" → Task 1 (the `identity_is_governed` helper + re-expressing `identity_in_predicate`'s check). ✓
- Spec "Part 2 — guard `/search` before the row-filter branch" → Task 2 (guard before `row_filters.is_empty()`, returns `Forbidden`). ✓
- Spec "Unchanged cases" (no identity / ungoverned identity) → covered by `identity_is_governed` returning `false` (Task 1) and the regression test `search_ok_when_identity_ungoverned_regression` (Task 3 case 4). ✓
- Spec "Testing" cases 1–4 → Task 3 four cases (denied/no-filter, masked/no-filter, governed+row-filter symmetry, ungoverned regression). ✓
- Spec "Scope": helper + one guard + re-expressed check + e2e — all present; nothing out-of-scope added (no column masking on `/search`, no `/objects`/Flight/engine changes). ✓
- Register close → Task 4. ✓

**2. Placeholder scan:** No TBD/TODO/"handle edge cases"; every code step shows the actual code. The only deferred value is the PR number in Task 4 (legitimately not known until the PR exists; flagged as amended during finishing).

**3. Type consistency:** `identity_is_governed(otype: &ObjectType, denied: &HashSet<String>, masked: &HashSet<String>) -> bool` is used identically in Task 1 (definition + `identity_in_predicate` call) and Task 2 (`vector_search` guard). `grant_read_columns(cp, role, type_name, deny_columns: Vec<String>, mask_columns: Vec<String>)` defined in Task 3 Step 1 and called in Task 3 Step 2 with matching argument order/types. `QueryError::Forbidden` is the existing variant (`handler.rs:218,234,455`). Names consistent.

---

## Execution Handoff

Plan complete. Execution is via **subagent-driven-development** per the loom-work-checkout pipeline: one fresh subagent per task, each followed by the mandatory two-stage review (spec-compliance, then code-quality), with a final whole-implementation review before finishing.
