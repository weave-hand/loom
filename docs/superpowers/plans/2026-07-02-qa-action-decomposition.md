# road-qa-action-decomposition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Behavior-preserving decomposition of query-api's action write path:
`action.rs::run_mutate` (cc 44, 165 lines — the census's highest non-vector
hotspot) splits onto named, unit-tested phase seams (`locate_unique_row`,
`enforce_mutate_policy`), `run_insert` yields its two pure phases
(`value_constraint_violations`, `expand_to_full_row`), and both paths share one
response epilogue (`affected_object`) built on the governance layer's
`Projection` zip (PR #299). Exactly ONE whitelisted behavior change: UPDATE
actions gain the per-value constraint validation INSERT already enforces.

**Register drift (corrections to the ROADMAP prose, verified against the tree
2026-07-02, HEAD `1a567ea` — `action.rs` is untouched since PR #269; neither
#299 nor #310 changed it, so the audit's claims mostly still hold):**

- **`expand_to_full_row` is a `run_insert` phase, not a `run_mutate` phase.**
  The register lists it among run_mutate's splits, but the NULL-expansion to
  the full property set exists only in `run_insert` (action.rs:461-480). The
  COW mutate path reads existing FULL rows from the table — there is nothing
  to NULL-expand. It is extracted from `run_insert` (Task 5).
- **"`validate_constraints` (moves from run_insert so both paths share it)" is
  a behavior change, not a pure move.** Constraint validation exists ONLY in
  `run_insert` (action.rs:428-459); `run_mutate` has no equivalent, so today
  an UPDATE can write a value the equivalent INSERT rejects with 422. "Both
  paths share it" therefore means the UPDATE path *gains* enforcement — this
  plan's single whitelist item (Task 7), verified unpinned by any existing
  test. The extracted fn is also **renamed `value_constraint_violations`**:
  the register's name collides with `control_plane_core::validate_constraints`
  (the define-time *declaration* validator, re-exported at `core/src/lib.rs:35`
  and called by both ontology adapters) — importing the register name into
  action.rs would shadow it.
- **The corrupt-PK guard has ZERO test coverage** (survey of all 15
  action/mutate test files): no test lands a duplicated identity. Task 1 adds
  the e2e pin FIRST, green against the current code.
- **The three policy legs are each individually e2e-pinned**
  (`update_delete_governance_e2e.rs` tests 1-4) **but the cross-leg ORDER is
  not** — no test uses a policy carrying both `deny_columns` AND a
  `row_filter`. Task 1 adds two both-legs order pins FIRST.
- **What #310 landed is NOT reused here**: `http.rs::query_error_response` is
  the READ-path `QueryError → Response` mapping; `post_action`'s
  `ActionError → Response` match (http.rs:659-734) is a disjoint error domain
  and stays untouched. The governance-layer piece this item reuses is #299's
  `Projection`/`prop_ty` (`governed.rs:93,131`), which gains two small
  write-path constructors (Task 2).

**Architecture (key decisions, verified against the tree):**

- **All phase fns stay in `action.rs` as `pub` fns** (the crate's precedent:
  `check_conformance` is `pub` and tested from `tests/action_conformance.rs`;
  buck2 tests are external `rust_test` targets, so seams must be visible).
  Extraction keeps the `tracing` target (`query_api::action`) and every log
  message byte-identical.
- **`locate_unique_row(rows, id_idx, id_value, idprop)`** is the pure locate
  phase: no match → `NotFound` (the caller's 404); >1 match → the corrupt-PK
  `ControlPlaneError::Backend` fault (500) with the existing message text. The
  old defensive re-index after the uniqueness check
  (`live.rows.get(target_idx).cloned().ok_or_else(NotFound)`, unreachable) is
  dropped — the row is cloned from the first match. Not observable.
- **`enforce_mutate_policy(policies, columns, existing, set_pairs, new_row,
  action_name)`** owns the three ordered legs — (1) existing-row filter (UPDATE
  + DELETE), (2) deny-column over the SET columns (UPDATE only), (3)
  resulting-row filter (UPDATE only) — including their `tracing::info!` lines.
  `row_filter_admits` stays private behind it. The order is security-relevant
  (see *Current enforcement order*) and is pinned at BOTH the e2e level
  (Task 1) and the unit level (Task 4) before/during the extraction.
- **`value_constraint_violations(target, pairs) -> Result<Vec<ConstraintViolation>, ActionError>`**
  returns the collected violations so each caller keeps its own byte-identical
  `tracing` + `Err(ConstraintViolation(...))` tail (insert logs
  `"insert rejected: …"`, the new update leg logs `"update rejected: …"`). The
  pre-existing documented `#[expect(clippy::cast_precision_loss)]` moves with
  the `Int` arm — not a new expect.
- **The shared epilogue reuses `Projection`**: `Projection::of_columns(otype,
  columns)` (logical types via `prop_ty`, unknown column zips to `""` exactly
  like the old inline `find(...).map(...).unwrap_or_default()`; `masked`
  empty — masking is a read-render concept and the caller already cleared the
  Write gate for these columns) + `Projection::object_rows(rows)` (the zip
  without a serving-engine echo; `into_object_rows` delegates to it after its
  `debug_assert_eq!`). `action.rs::affected_object(target, columns, row)`
  composes the two for both write paths.
- **UPDATE constraint placement** (whitelist): after `enforce_mutate_policy`,
  before the live-set rebuild — the same relative order as INSERT (policy 403
  before constraint 422). Validated over `set_pairs` only: DELETE sets nothing
  (empty → structurally unaffected), the identity is a locator (not a written
  value) and omitted optionals are already excluded from `set_pairs`.

**Tech Stack:** Rust (edition 2024), buck2, `loom_rust_test` (two new pure
targets: `mutate-phases`, `insert-phases`) + existing `loom_fixture_test` e2e
targets (hermetic Postgres). No third-party dep changes, no `Cargo.toml` /
lockfile / `.sqlx` changes, no BUCK changes beyond the two new test targets.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-02-pillar-idioms-audit-design.md`
  (§ "road-qa-action-decomposition"), with the register-drift corrections
  above. Register: `docs/ROADMAP.md#road-qa-action-decomposition` (line 294).
- **Behavior-preserving:** wire responses, error bodies, status codes, tracing
  messages, and the ACL enforcement ORDER stay byte-identical except the
  single whitelisted change below. This is a **governed write path** — the
  order coarse-deny → conformance → [mutate: existing-row filter →
  deny-column → resulting-row filter] / [insert: deny-column → row-filter →
  constraints] is security-relevant; it is documented in *Current enforcement
  order* and preserved exactly.
- **Existing e2e tests pass unmodified.** No existing test function or
  assertion is edited. The ONLY test files this plan may touch are appends to
  `tests/update_delete_e2e.rs`, `tests/update_delete_governance_e2e.rs`
  (Tasks 1, 7), and `tests/projection.rs` (Task 2), plus the two new files
  `tests/mutate_phases.rs` and `tests/insert_phases.rs`.
  `tests/e2e_support.rs` is not touched. Named per task: which e2es pin what.
- **Byte-identity lesson applied:** where the survey found existing coverage
  too weak to pin the refactor (corrupt-PK guard: zero coverage; policy
  cross-leg order: never combined in one policy), Task 1 adds pinning tests
  FIRST, verified green against the CURRENT code, before anything moves.
- **TDD:** every new fn/type lands with its test written first and observed
  red; pure-refactor steps run their pinning suite green before AND after.
- Tests are separate `rust_test` targets wired in
  `src/services/query-api/BUCK` — never inline `#[cfg(test)]` (the
  `no-inline-tests` hook enforces this).
- Clippy pedantic+restriction is on for prod code: no
  unwrap/expect/panic/indexing/print in `src/**.rs`. This item adds **no new
  `#[expect]`** (the one `cast_precision_loss` expect moves verbatim with its
  code and keeps its reason).
- **Never pipe `buck2 test`/`buck2 bxl` through `tail`/`head`** — redirect to
  a file and grep it. Fixture (e2e) runs use `-j 8` (postgres boot-slot
  starvation). Cloud sessions: `buck2 build -M none` for any whole-tree build;
  scope tests to the targets each task lists.
- `buck2 run //tools:prek -- run --all-files` must report zero `Failed`
  before **every** commit (rustfmt is a separate hook from clippy).
  Conventional Commits messages; one commit per task.

## Current enforcement order (documented, preserved exactly)

Verified against `action.rs` at HEAD `1a567ea`. `post_action`
(http.rs:659-734) maps each `ActionError` to the status/body in brackets.

**Shared prologue — `run_action` (action.rs:321-368):**

1. Resolve the action — unknown → `UnknownAction` [404, name as text body].
2. Resolve the target type — missing → `ControlPlane` fault [opaque 500]
   (a broken ActionDef, not a client error).
3. **Coarse Write gate** (`acl.check(subject, Write, Type)` — deny-by-default)
   → `Forbidden` [bodyless 403]. Runs BEFORE conformance so an unauthorized
   caller learns nothing about the definition's validity.
4. Conformance (`check_conformance`) → `Misconfigured` [500 with descriptive
   body].
5. Dispatch on `ActionKind`.

**INSERT — `run_insert` (action.rs:373-530):**

6. `resolve_action_row` (parse/coerce params + constants) → `BadParams`
   [422, plain text].
7. **Fine-grained Write policy** over the SET (non-null) columns via
   `write_filter::check_write_policy`: **deny-column first, then row-filter**
   on the inserted row (evaluated fail-closed, UNKNOWN ⇒ deny) →
   `WriteDenied` [403, `{"error":"write_denied","reason":…}` body — column
   name only, never the predicate].
8. **Constraint validation** over all pairs (action.rs:428-459) →
   `ConstraintViolation` [422, `{"violations":[{"property","rule"}]}`].
9. Expand to the full property row (NULLs for unset) (action.rs:461-480).
10. Mint `run_id` + lineage event UP FRONT; atomic `write_object` (row +
    lineage in one Tx).
11. Epilogue: `ObjectRows` over the action-provided columns (logical types
    looked up per column, unknown → `""`) [201 + `X-Loom-Run-Id` header].

**UPDATE/DELETE — `run_mutate` (action.rs:591-755):**

6. `ensure_cow_supported` (vector column) → `Unsupported` [422, text]. Fires
   before any row lookup.
7. Identity resolution: declared identity required → `Misconfigured` [500];
   `resolve_action_row` → `BadParams` [422]; identity value must be among the
   pairs → `Misconfigured` [500].
8. Privileged (ACL-unfiltered) full-table read via `select_all_sql`.
9. **Locate the unique row**: no match → `NotFound` [404, bodyless]; >1 match
   → the **corrupt-PK guard**, `ControlPlaneError::Backend("identity `{id}`
   matches more than one live row")` [opaque 500] — corrupt invariant, not a
   client error.
10. Compute `set_pairs` (non-identity, non-null — omitted optionals excluded
    from BOTH the deny check and the overwrite) + the resulting row (UPDATE
    only).
11. **Fine-grained policy legs, in this order** (action.rs:675-712):
    1. the EXISTING row must pass every policy `row_filter` (UPDATE and
       DELETE) → `WriteDenied(RowFilter)` [403 row_filter body];
    2. UPDATE only: **deny-column over the SET columns** →
       `WriteDenied(Column)` [403 column body];
    3. UPDATE only: the RESULTING row must pass every `row_filter` →
       `WriteDenied(RowFilter)`.
    Leg 1 before leg 2 means a subject who cannot address the row learns
    nothing about column policies; leg 2 before leg 3 means a denied column
    is reported as such even when the resulting row would also fail.
12. **No constraint validation** (today — the whitelisted change adds step
    11½ ≡ INSERT's step 8, after the policy legs).
13. Rebuild the full live set (replace/remove the target row); mint `run_id`;
    atomic `overwrite_table` (COW + lineage in one Tx).
14. Epilogue: `ObjectRows` over the FULL property columns (UPDATE: new
    version; DELETE: removed values) [201 + `X-Loom-Run-Id`].

## Verified claim inventory (register claim → current evidence)

| Register claim | Verdict | Current evidence |
| --- | --- | --- |
| `run_mutate` cc 44, 165 lines, six concerns | CONFIRMED | `action.rs:591-755` (165 lines); census `docs/code-health/complexity.md:74`. Concerns: COW guard + identity/param resolution; privileged read + unique-row locate (corrupt-PK guard); PATCH set-pair/resulting-row computation; three-leg policy enforcement; live-set rebuild + lineage + atomic commit; response epilogue |
| `locate_unique_row` keeps the corrupt-PK guard | CONFIRMED (+coverage gap) | guard `action.rs:641-645`; **no existing test** exercises it → Task 1 pins first |
| `enforce_mutate_policy` = three policy legs | CONFIRMED (+order gap) | legs `action.rs:684-712`; each leg pinned singly (`update_delete_governance_e2e.rs` tests 1-4) but **cross-leg order never pinned** (no test combines `deny_columns` + `row_filter` in one policy) → Task 1 pins first |
| `validate_constraints` moves from `run_insert` | CLARIFIED | exists ONLY in `run_insert` `action.rs:428-459`; sharing with the mutate path = the ONE whitelisted behavior change; renamed `value_constraint_violations` (name collision with `control_plane_core::validate_constraints`, `core/src/lib.rs:35`) |
| `expand_to_full_row` (listed as run_mutate phase) | DRIFTED | it is `run_insert`'s step 5 (`action.rs:461-480`); the COW path reads existing full rows and never NULL-expands. Extracted from `run_insert` |
| response epilogue shared, reusing the `Projection` zip | CONFIRMED | `run_insert` epilogue `action.rs:509-529` re-implements the `find(…).map(…).unwrap_or_default()` logical-type zip that `governed::prop_ty`/`Projection` (#299) owns; `run_mutate` epilogue `action.rs:745-754` is the full-columns variant. `Projection` gains `of_columns`/`object_rows` (Task 2) |
| e2e family pins the path | CONFIRMED | 15 test files / 72 tests surveyed. Mutate e2es call `run_action` directly (real PG + in-process engine) and assert `ActionError` variants + post-write table state; the insert HTTP tests assert **exact** status + JSON bodies (`write_denial_http.rs:178-182,211-215`, `constraints_action_http.rs:209`, `action_run_id_http.rs:149-162`) |
| #310 landed pieces to reuse | REFUTED (for this item) | `query_error_response` maps READ-path `QueryError` only; `post_action`'s `ActionError` mapping is disjoint and untouched. The reusable governance piece is #299's `Projection`/`prop_ty` |

## Call-site inventory (verified by grep; the plan updates every one)

| Symbol | Call sites | Updated in |
| --- | --- | --- |
| `run_insert` (private) | `action.rs:364` (dispatch) | Tasks 5, 6 (body only; signature unchanged) |
| `run_mutate` (private) | `action.rs:365,366` (dispatch) | Tasks 3, 4, 6, 7 (body only; signature unchanged) |
| `run_action` (pub) | `http.rs:674`; 13 e2e/handler test files | untouched |
| `row_filter_admits` (private) | `action.rs:685,705` | Task 4 (becomes internal to `enforce_mutate_policy`) |
| `Projection::into_object_rows` | `handler.rs` read spine (post-#310) | Task 2 (delegates to `object_rows`; callers untouched) |
| `post_action` `ActionError` match | `http.rs:659-734` | untouched (mapping already total over the variants) |
| `check_conformance` / `WriteDenialReason` (pub) | conformance + denial test files | untouched |

## Deliberate behavior changes (everything else is byte-identical)

1. **UPDATE actions now validate declared per-property constraints on the SET
   values** (Task 7 — the register's "moves from `run_insert` so both paths
   share it"). Previously `run_mutate` skipped constraint validation entirely,
   so an UPDATE could write a value the equivalent INSERT rejects — e.g. a
   `qty` with `range ≤ 100` could be PATCHed to 999. Now the SET values run
   through the same `core` `PropertyValidator` and a violation returns the
   same structured 422 `{"violations":[{"property","rule"}]}` body INSERT
   produces, placed AFTER the three policy legs (403 before 422, mirroring
   INSERT's policy-then-constraints order) and after the locate phase (a
   missing identity is still `NotFound`). DELETE is structurally unaffected
   (`set_pairs` is empty); the identity value is a locator, not a written
   value, and is not re-validated; omitted optional params are already
   excluded from `set_pairs`. New server-side log line:
   `"update rejected: constraint violation"` (action + count fields, mirroring
   the insert line). The survey verified **no existing test pins the bypass**.

There is no second item. In particular: every status code, error body, tracing
message, lineage payload, snapshot semantics, and the response `ObjectRows`
column/type shape of both paths are pinned byte-identical by the existing e2es
plus Task 1's added pins.

---

### Task 1: Pin the unpinned behaviors (green against the CURRENT code)

The survey found two refactor-relevant behaviors with no or weak pins: the
corrupt-PK guard (zero coverage) and the policy cross-leg order (each leg
pinned only in isolation). Add e2e pins FIRST; they must pass against the
unmodified `action.rs` — if one fails here, the pin is mis-written: fix the
TEST, never the production code.

**Files:**
- Test (append): `src/services/query-api/tests/update_delete_e2e.rs`
  (existing target `//src/services/query-api:update-delete-e2e` — no BUCK
  change)
- Test (append): `src/services/query-api/tests/update_delete_governance_e2e.rs`
  (existing target `//src/services/query-api:update-delete-governance-e2e` —
  no BUCK change)

**Interfaces:**
- Consumes: `e2e_support::{define_widget, grant_writer, grant_writer_role,
  InProcessServingEngine, spawn_engine_writer}` (`tests/e2e_support.rs:842,
  889,910,1049`), `query_api::action::{run_action, ActionError,
  WriteDenialReason}`.
- Produces: three new `#[tokio::test]` fns that later tasks keep green.

- [x] **Step 1: Append the corrupt-PK pin to `update_delete_e2e.rs`**

Add `use control_plane_core::ControlPlaneError;` to the file's imports, then
append:

```rust
// ---------------------------------------------------------------------------
// Corrupt-PK guard: a duplicated identity (two live rows with id=1 — the insert
// path append-writes without PK enforcement, so corrupt/landed data can carry
// duplicates) is a corrupt invariant. The mutate locate phase must surface it
// as a Backend fault (the operator's 500) — NOT NotFound, and NOT a silent
// pick-one mutate.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_identity_is_a_backend_fault() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed the SAME identity twice.
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "a", "qty": "1" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert 1");
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "b", "qty": "2" })
            .as_object()
            .unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert 2 (duplicate id)");

    // UPDATE {id:1}: two live matches -> corrupt-PK Backend fault, not NotFound.
    let err = run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            &err,
            ActionError::ControlPlane(ControlPlaneError::Backend(_))
        ),
        "expected a Backend fault for a duplicated identity, got: {err:?}"
    );
    assert!(
        err.to_string().contains("matches more than one live row"),
        "fault message names the invariant, got: {err}"
    );

    drop(warehouse);
}
```

- [x] **Step 2: Append the two cross-leg order pins to
  `update_delete_governance_e2e.rs`**

```rust
// ---------------------------------------------------------------------------
// ORDER PIN — one policy carrying BOTH legs. The mutate enforcement order is
// security-relevant: the existing-row row-filter leg runs BEFORE deny-column
// (a subject who cannot address the row learns nothing about column policies),
// and deny-column runs BEFORE the resulting-row filter (a denied column is
// reported as such even when the resulting row would also fail).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_order_existing_row_filter_beats_deny_column() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let (subj, role) = grant_writer_role(&cp, &widget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, qty:9} BEFORE the policy (qty=9 will fail the filter).
    run_action(
        "createWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert (no policy yet)");

    // ONE policy with BOTH legs: row_filter `qty < 5` AND deny_columns ["qty"].
    cp.set_policy(
        &role,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: Some(RowFilter::Compare {
                property: "qty".into(),
                op: CompareOp::Lt,
                value: ScalarValue::Int(5),
            }),
            deny_columns: vec!["qty".to_string()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // UPDATE {id:1, qty:1}: the existing row (qty=9) fails the filter AND `qty`
    // is a denied column. The existing-row leg must win: reason row_filter.
    let err = run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, ActionError::WriteDenied(WriteDenialReason::RowFilter)),
        "existing-row filter leg runs before deny-column, got: {err:?}"
    );

    drop(warehouse);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_order_deny_column_beats_resulting_row_filter() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let widget = define_widget(&cp).await;
    let (subj, role) = grant_writer_role(&cp, &widget).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, qty:1} BEFORE the policy (qty=1 passes the filter).
    run_action(
        "createWidget",
        json!({ "id": "1", "qty": "1" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert (no policy yet)");

    // ONE policy with BOTH legs, as above.
    cp.set_policy(
        &role,
        Action::Write,
        Policy {
            target: PolicyTarget::Type(widget.clone()),
            row_filter: Some(RowFilter::Compare {
                property: "qty".into(),
                op: CompareOp::Lt,
                value: ScalarValue::Int(5),
            }),
            deny_columns: vec!["qty".to_string()],
            mask_columns: vec![],
        },
    )
    .await
    .unwrap();

    // UPDATE {id:1, qty:9}: the existing row (qty=1) PASSES the filter; `qty` is
    // denied AND the resulting row (qty=9) would fail. Deny-column must win.
    let err = run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            &err,
            ActionError::WriteDenied(WriteDenialReason::Column(c)) if c == "qty"
        ),
        "deny-column leg runs before the resulting-row filter, got: {err:?}"
    );

    drop(warehouse);
}
```

- [x] **Step 3: Run both fixture targets — the pins are green against the
  UNMODIFIED code**

Run: `buck2 test -j 8 //src/services/query-api:update-delete-e2e //src/services/query-api:update-delete-governance-e2e > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS (3 new + 8 pre-existing tests). A failure here means a
mis-written pin — fix the test.

- [x] **Step 4: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p1.log 2>&1; grep -c Failed /tmp/p1.log` — expected `0`.

```bash
git add src/services/query-api/tests/update_delete_e2e.rs src/services/query-api/tests/update_delete_governance_e2e.rs
git commit -m "test(query-api): pin corrupt-PK guard and mutate policy-leg order

The action-write-path survey found the two refactor-critical behaviors of
run_mutate unpinned: the corrupt-PK guard (>1 live row for an identity ->
Backend 500, never NotFound) had zero coverage, and the three policy legs were
each pinned only in isolation — no test combined deny_columns + row_filter in
one policy, so the security-relevant leg order (existing-row filter before
deny-column before resulting-row filter) was unenforced by the suite. Three
e2e pins added, green against the current code, ahead of the decomposition.

Part of road-qa-action-decomposition."
```

---

### Task 2: governed.rs — `Projection::of_columns` + `Projection::object_rows`

The write-path constructors for the governance layer's `Projection` (#299):
`of_columns` zips logical types for an explicit caller-ordered column list
(unknown column → `""`, the pre-existing sentinel the action epilogues use),
`object_rows` converts rows the caller already holds (no serving-engine echo
to cross-check). `into_object_rows` delegates to `object_rows` after its
column-order `debug_assert_eq!`, so the read paths are untouched.

**Files:**
- Modify: `src/services/query-api/src/governed.rs` (`impl Projection`,
  :137-189)
- Test (append): `src/services/query-api/tests/projection.rs` (existing
  target `//src/services/query-api:projection` — no BUCK change)

**Interfaces:**
- Produces (`pub` in `query_api::governed`):
  `Projection::of_columns(otype: &ObjectType, columns: Vec<String>) -> Projection`,
  `Projection::object_rows(self, rows: Vec<Vec<SqlValue>>) -> ObjectRows`.
  Task 6's `affected_object` consumes both.

- [x] **Step 1: Write the failing tests**

Append to `src/services/query-api/tests/projection.rs` (its existing imports
cover everything — `governed`, `Projection`, `SqlValue`):

```rust
#[test]
fn of_columns_zips_logical_types_in_caller_order_unknown_to_empty() {
    let g = governed(&[], &[]);
    let p = Projection::of_columns(
        &g.otype,
        vec!["status".to_string(), "id".to_string(), "ghost".to_string()],
    );
    assert_eq!(
        p.columns,
        vec!["status".to_string(), "id".to_string(), "ghost".to_string()]
    );
    // Types follow the caller's column order; an unknown column zips to "" —
    // the same sentinel the action epilogues produced inline.
    assert_eq!(
        p.logical_types,
        vec!["String".to_string(), "Long".to_string(), String::new()]
    );
    assert!(p.masked.is_empty(), "write path never masks");
}

#[test]
fn object_rows_zips_rows_without_a_serving_echo() {
    let g = governed(&[], &[]);
    let p = Projection::of_columns(&g.otype, vec!["id".to_string(), "status".to_string()]);
    let rows = p.object_rows(vec![vec![SqlValue::Int(1), SqlValue::Text("open".into())]]);
    assert_eq!(rows.columns, vec!["id".to_string(), "status".to_string()]);
    assert_eq!(
        rows.logical_types,
        vec!["Long".to_string(), "String".to_string()]
    );
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Int(1), SqlValue::Text("open".into())]]
    );
}
```

- [x] **Step 2: Run to see them fail**

Run: `buck2 test //src/services/query-api:projection > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t2.log`
Expected: FAIL — compile error, `of_columns`/`object_rows` not found.

- [x] **Step 3: Implement**

In `src/services/query-api/src/governed.rs`, inside `impl Projection`, add
below `visible` (:163):

```rust
    /// The ungoverned projection of explicit `columns` against `otype`'s declared
    /// properties: logical types via [`prop_ty`] in the caller's column order (an
    /// unknown column zips to `""`, the pre-existing sentinel), no masked columns.
    /// The ACTION write path's response epilogue — the affected row echoes columns
    /// the caller already cleared the Write gate for; masking is a read-render
    /// concept and does not apply to a write echo.
    pub fn of_columns(otype: &ObjectType, columns: Vec<String>) -> Self {
        let logical_types = columns
            .iter()
            .map(|name| {
                prop_ty(otype, name)
                    .map(str::to_string)
                    .unwrap_or_default()
            })
            .collect();
        Self {
            columns,
            logical_types,
            masked: Vec::new(),
        }
    }
```

and below `push` (:173), the zip without an engine echo, with
`into_object_rows` delegating to it:

```rust
    /// Zip rows the caller already holds into an `ObjectRows` — the write path's
    /// conversion, where there is no serving-engine echo to cross-check.
    /// [`Self::into_object_rows`] delegates here after its column-order contract
    /// check.
    pub fn object_rows(self, rows: Vec<Vec<crate::serving::SqlValue>>) -> crate::handler::ObjectRows {
        crate::handler::ObjectRows {
            columns: self.columns,
            logical_types: self.logical_types,
            rows,
        }
    }
```

Rewrite `into_object_rows` (:178-189) to delegate:

```rust
    /// Zip served rows into an `ObjectRows`. The serving engine must echo the projected
    /// columns in SELECT order — the contract that lets the renderer zip
    /// `logical_types`/`columns` onto each row's cells by position.
    pub fn into_object_rows(self, served: crate::serving::Rows) -> crate::handler::ObjectRows {
        debug_assert_eq!(
            served.columns, self.columns,
            "serving engine returned columns out of the projected order"
        );
        self.object_rows(served.rows)
    }
```

- [x] **Step 4: Run to green (unit + the read paths that ride `into_object_rows`)**

Run: `buck2 test //src/services/query-api:projection //src/services/query-api:graph-reach //src/services/query-api:graph-reach-union //src/services/query-api:graph-reach-tail > /tmp/t2.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c2.log 2>&1; cat /tmp/c2.log` — artifact empty.

- [x] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p2.log 2>&1; grep -c Failed /tmp/p2.log` — expected `0`.

```bash
git add src/services/query-api/src/governed.rs src/services/query-api/tests/projection.rs
git commit -m "refactor(query-api): Projection::of_columns/object_rows write-path zip

The governance layer's Projection gains the two constructors the action
write-path epilogues need: of_columns (logical types via prop_ty over an
explicit caller-ordered column list, unknown -> \"\" — the sentinel the
epilogues produced inline) and object_rows (the zip without a serving-engine
echo; into_object_rows now delegates to it after its order check). Read paths
untouched.

Part of road-qa-action-decomposition."
```

---

### Task 3: action.rs — extract `locate_unique_row` (the corrupt-PK guard seam)

**Files:**
- Modify: `src/services/query-api/src/action.rs` (`run_mutate` locate block
  :632-651; new pub fn above `run_mutate`)
- Create: `src/services/query-api/tests/mutate_phases.rs`
- Modify: `src/services/query-api/BUCK` (new `mutate-phases` target)

**Interfaces:**
- Produces (`pub` in `query_api::action`):

```rust
pub fn locate_unique_row(
    rows: &[Vec<SqlValue>],
    id_idx: usize,
    id_value: &SqlValue,
    idprop: &str,
) -> Result<(usize, Vec<SqlValue>), ActionError>
```

  Task 4 appends to the same test file; `run_mutate`'s signature is unchanged.

- [x] **Step 1: Write the failing tests**

Create `src/services/query-api/tests/mutate_phases.rs`:

```rust
//! Pure unit tests for the mutate write-path phases extracted from
//! `action.rs::run_mutate`: `locate_unique_row` (identity match + corrupt-PK
//! guard) and `enforce_mutate_policy` (the three ordered policy legs).
//! No fixture; the e2e twins live in update_delete_e2e / _governance_e2e.

use control_plane_core::ControlPlaneError;
use query_api::action::{ActionError, locate_unique_row};
use query_api::serving::SqlValue;

fn row(id: i64, qty: i64) -> Vec<SqlValue> {
    vec![SqlValue::Int(id), SqlValue::Int(qty)]
}

#[test]
fn locate_finds_the_single_match() {
    let rows = vec![row(1, 10), row(2, 20)];
    let (idx, existing) = locate_unique_row(&rows, 0, &SqlValue::Int(2), "id").unwrap();
    assert_eq!(idx, 1);
    assert_eq!(existing, row(2, 20));
}

#[test]
fn locate_no_match_is_not_found() {
    let rows = vec![row(1, 10)];
    let err = locate_unique_row(&rows, 0, &SqlValue::Int(9), "id").unwrap_err();
    assert!(matches!(err, ActionError::NotFound), "got: {err:?}");
}

#[test]
fn locate_duplicate_identity_is_a_backend_fault() {
    // >1 live row for a primary key is a corrupt invariant, not a client error.
    let rows = vec![row(1, 10), row(1, 20)];
    let err = locate_unique_row(&rows, 0, &SqlValue::Int(1), "id").unwrap_err();
    assert!(
        matches!(&err, ActionError::ControlPlane(ControlPlaneError::Backend(_))),
        "got: {err:?}"
    );
    assert!(
        err.to_string().contains("matches more than one live row"),
        "got: {err}"
    );
}

#[test]
fn locate_matches_on_the_identity_column_only() {
    // qty=10 in the id slot of no row: values in other columns never match.
    let rows = vec![row(1, 10)];
    let err = locate_unique_row(&rows, 0, &SqlValue::Int(10), "id").unwrap_err();
    assert!(matches!(err, ActionError::NotFound), "got: {err:?}");
}
```

Add to `src/services/query-api/BUCK`, directly after the
`write-denial-reason` target (:908-919):

```python
rust_test(
    name = "mutate-phases",
    crate = "mutate_phases",
    srcs = ["tests/mutate_phases.rs"],
    crate_root = "tests/mutate_phases.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
    ],
)
```

- [x] **Step 2: Run to see it fail**

Run: `buck2 test //src/services/query-api:mutate-phases > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t3.log`
Expected: FAIL — compile error, `locate_unique_row` not found.

- [x] **Step 3: Extract the fn and rewire `run_mutate`**

In `src/services/query-api/src/action.rs`, add above `run_mutate` (below
`row_filter_admits`, :583):

```rust
/// Locate the single live row whose `id_idx` cell equals `id_value` (the supplied,
/// already-typed identity). The identity is a primary key, so at most one live match:
/// no match is `NotFound` (the caller's 404); more than one is a corrupt invariant —
/// surfaced as a `Backend` fault (500), never a client error and never a silent
/// pick-one mutate. Returns the matched row's index and a clone of the row. Pure;
/// the unit-test seam for the mutate locate phase.
pub fn locate_unique_row(
    rows: &[Vec<SqlValue>],
    id_idx: usize,
    id_value: &SqlValue,
    idprop: &str,
) -> Result<(usize, Vec<SqlValue>), ActionError> {
    let mut matches = rows
        .iter()
        .enumerate()
        .filter(|(_, r)| r.get(id_idx) == Some(id_value));
    let (target_idx, existing) = match matches.next() {
        None => return Err(ActionError::NotFound),
        Some((i, r)) => (i, r.clone()),
    };
    if matches.next().is_some() {
        // >1 live row for a primary key is a corrupt invariant, not a client error.
        return Err(ActionError::ControlPlane(ControlPlaneError::Backend(
            format!("identity `{idprop}` matches more than one live row").into(),
        )));
    }
    Ok((target_idx, existing))
}
```

Replace `run_mutate`'s locate block (:632-651 — from
`// Locate the target row.` through the `existing` binding) with:

```rust
    // Locate the target row. The identity is a primary key, so at most one live match.
    let (target_idx, existing) = locate_unique_row(&live.rows, id_idx, &id_value, &idprop)?;
```

(The old defensive re-index `live.rows.get(target_idx).cloned().ok_or_else(…)`
disappears — unreachable, the index came from the same iteration.)

- [x] **Step 4: Run to green (unit + the e2e pins)**

Run: `buck2 test -j 8 //src/services/query-api:mutate-phases //src/services/query-api:update-delete-e2e //src/services/query-api:update-delete-governance-e2e //src/services/query-api:update-delete-tiers-e2e > /tmp/t3.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t3.log`
Expected: PASS — Task 1's `duplicate_identity_is_a_backend_fault` +
`not_found` pin the extraction; the tiers e2e pins COW semantics.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c3.log 2>&1; cat /tmp/c3.log` — artifact empty.

- [x] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p3.log 2>&1; grep -c Failed /tmp/p3.log` — expected `0`.

```bash
git add src/services/query-api/src/action.rs src/services/query-api/tests/mutate_phases.rs src/services/query-api/BUCK
git commit -m "refactor(query-api): extract locate_unique_row from run_mutate

The pure locate phase (identity match over the privileged full-table read,
NotFound on no match, corrupt-PK Backend fault on >1) becomes a unit-tested
seam. Error variants and the fault message are byte-identical, pinned by the
Task-1 e2es; the unreachable defensive re-index after the uniqueness check is
dropped.

Part of road-qa-action-decomposition."
```

---

### Task 4: action.rs — extract `enforce_mutate_policy` (the three ordered legs)

**Files:**
- Modify: `src/services/query-api/src/action.rs` (`run_mutate` policy block —
  post-Task-3 the `// Fine-grained Write policy …` through the
  `if let Some(row) = &new_row { … }` close; new pub fn above `run_mutate`)
- Test (append): `src/services/query-api/tests/mutate_phases.rs` (existing
  target — no BUCK change)

**Interfaces:**
- Consumes: `row_filter_admits` (stays private, action.rs:573),
  `WriteDenialReason` (:69).
- Produces (`pub` in `query_api::action`):

```rust
pub fn enforce_mutate_policy(
    policies: &[Policy],
    columns: &[String],
    existing: &[SqlValue],
    set_pairs: &[(String, SqlValue)],
    new_row: Option<&[SqlValue]>,
    action_name: &str,
) -> Result<(), ActionError>
```

- [x] **Step 1: Write the failing tests**

Append to `src/services/query-api/tests/mutate_phases.rs` (extend the imports
to
`use control_plane_core::{CompareOp, ControlPlaneError, Policy, PolicyTarget, RowFilter, ScalarValue, TypeName};`
and add `enforce_mutate_policy`, `WriteDenialReason` to the
`query_api::action` import):

```rust
// --- enforce_mutate_policy: the three ordered legs ------------------------

/// One policy carrying a `qty < 5` row-filter and the given deny columns.
fn qty_lt_5_policy(deny: Vec<String>) -> Policy {
    Policy {
        target: PolicyTarget::Type(TypeName("Widget".into())),
        row_filter: Some(RowFilter::Compare {
            property: "qty".into(),
            op: CompareOp::Lt,
            value: ScalarValue::Int(5),
        }),
        deny_columns: deny,
        mask_columns: vec![],
    }
}

fn cols() -> Vec<String> {
    vec!["id".to_string(), "qty".to_string()]
}

#[test]
fn no_policies_admit_update_and_delete() {
    let existing = row(1, 9);
    let new = row(1, 1);
    let set = vec![("qty".to_string(), SqlValue::Int(1))];
    enforce_mutate_policy(&[], &cols(), &existing, &set, Some(&new), "updateWidget").unwrap();
    enforce_mutate_policy(&[], &cols(), &existing, &[], None, "deleteWidget").unwrap();
}

#[test]
fn leg1_existing_row_filter_runs_first() {
    // Existing row fails the filter AND the SET column is denied: the
    // existing-row leg must win (RowFilter) — pins the enforcement order.
    let p = qty_lt_5_policy(vec!["qty".to_string()]);
    let existing = row(1, 9); // fails qty < 5
    let new = row(1, 1);
    let set = vec![("qty".to_string(), SqlValue::Int(1))];
    let err = enforce_mutate_policy(&[p], &cols(), &existing, &set, Some(&new), "updateWidget")
        .unwrap_err();
    assert!(
        matches!(err, ActionError::WriteDenied(WriteDenialReason::RowFilter)),
        "got: {err:?}"
    );
}

#[test]
fn leg2_deny_column_runs_before_leg3_resulting_row() {
    // Existing row passes; the SET column is denied AND the resulting row
    // would fail: deny-column must win (Column) — pins the order.
    let p = qty_lt_5_policy(vec!["qty".to_string()]);
    let existing = row(1, 1); // passes
    let new = row(1, 9); // would fail
    let set = vec![("qty".to_string(), SqlValue::Int(9))];
    let err = enforce_mutate_policy(&[p], &cols(), &existing, &set, Some(&new), "updateWidget")
        .unwrap_err();
    assert!(
        matches!(
            &err,
            ActionError::WriteDenied(WriteDenialReason::Column(c)) if c == "qty"
        ),
        "got: {err:?}"
    );
}

#[test]
fn leg3_resulting_row_filter_denies_update() {
    let p = qty_lt_5_policy(vec![]);
    let existing = row(1, 1);
    let new = row(1, 9);
    let set = vec![("qty".to_string(), SqlValue::Int(9))];
    let err = enforce_mutate_policy(&[p], &cols(), &existing, &set, Some(&new), "updateWidget")
        .unwrap_err();
    assert!(
        matches!(err, ActionError::WriteDenied(WriteDenialReason::RowFilter)),
        "got: {err:?}"
    );
}

#[test]
fn delete_checks_only_the_existing_row() {
    // DELETE (new_row = None): deny-column is irrelevant (nothing is set);
    // only the existing-row leg applies.
    let p = qty_lt_5_policy(vec!["qty".to_string()]);
    let existing = row(1, 1); // passes the filter
    enforce_mutate_policy(&[p], &cols(), &existing, &[], None, "deleteWidget").unwrap();
    let failing = row(1, 9);
    let err =
        enforce_mutate_policy(&[qty_lt_5_policy(vec![])], &cols(), &failing, &[], None, "deleteWidget")
            .unwrap_err();
    assert!(
        matches!(err, ActionError::WriteDenied(WriteDenialReason::RowFilter)),
        "got: {err:?}"
    );
}

#[test]
fn unknown_filter_truth_fails_closed() {
    // A row-filter over a column absent from the row evaluates UNKNOWN -> denied
    // (three-valued logic, mirroring "an UNKNOWN WHERE row is excluded").
    let p = Policy {
        target: PolicyTarget::Type(TypeName("Widget".into())),
        row_filter: Some(RowFilter::Compare {
            property: "missing".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Int(1),
        }),
        deny_columns: vec![],
        mask_columns: vec![],
    };
    let existing = row(1, 1);
    let err = enforce_mutate_policy(&[p], &cols(), &existing, &[], None, "deleteWidget")
        .unwrap_err();
    assert!(
        matches!(err, ActionError::WriteDenied(WriteDenialReason::RowFilter)),
        "got: {err:?}"
    );
}
```

- [x] **Step 2: Run to see them fail**

Run: `buck2 test //src/services/query-api:mutate-phases > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t4.log`
Expected: FAIL — compile error, `enforce_mutate_policy` not found.

- [x] **Step 3: Extract the fn and rewire `run_mutate`**

Add to `action.rs` below `locate_unique_row` (the moved bodies are the
verbatim leg blocks from `run_mutate`, tracing lines included):

```rust
/// The three ordered fine-grained Write-policy legs of a mutate — a whole-row verdict,
/// deliberately NOT the INSERT gate (`write_filter::check_write_policy`), whose
/// deny-column-over-ALL-columns order is wrong for a PATCH. The order is
/// security-relevant and pinned by e2e + unit tests:
///   1. the EXISTING row must pass every policy `row_filter` (UPDATE and DELETE) —
///      a subject may not touch a row it cannot address, and learns nothing about
///      column policies when it cannot;
///   2. UPDATE only: no SET column may be policy-denied;
///   3. UPDATE only: the RESULTING row must pass every `row_filter` — a PATCH may
///      not move a row out of the subject's writable region.
/// Fail-closed (an UNKNOWN filter truth denies). Denials are logged server-side; the
/// returned `WriteDenied` reason is caller-scoped (column name only, never the
/// predicate). Pure; the unit-test seam for the mutate policy phase.
pub fn enforce_mutate_policy(
    policies: &[Policy],
    columns: &[String],
    existing: &[SqlValue],
    set_pairs: &[(String, SqlValue)],
    new_row: Option<&[SqlValue]>,
    action_name: &str,
) -> Result<(), ActionError> {
    // The existing row must pass every row-filter (both UPDATE and DELETE).
    if !row_filter_admits(policies, columns, existing) {
        tracing::info!(
            action = action_name,
            "mutate denied: existing row fails write policy filter"
        );
        return Err(ActionError::WriteDenied(WriteDenialReason::RowFilter));
    }
    if let Some(row) = new_row {
        // Deny-column over the SET columns only (UPDATE writes those columns).
        if let Some(col) = set_pairs.iter().map(|(c, _)| c).find(|c| {
            policies
                .iter()
                .any(|p| p.deny_columns.iter().any(|d| d == *c))
        }) {
            tracing::info!(action = action_name, column = %col, "update write denied: policy denies column");
            return Err(ActionError::WriteDenied(WriteDenialReason::Column(
                col.clone(),
            )));
        }
        // The resulting row must also pass every row-filter.
        if !row_filter_admits(policies, columns, row) {
            tracing::info!(
                action = action_name,
                "update denied: resulting row fails write policy filter"
            );
            return Err(ActionError::WriteDenied(WriteDenialReason::RowFilter));
        }
    }
    Ok(())
}
```

In `run_mutate`, replace the block from
`// The existing row must pass every row-filter (both UPDATE and DELETE).`
through the close of `if let Some(row) = &new_row { … }` (:684-712) with:

```rust
    enforce_mutate_policy(
        &write_policies.items,
        &columns,
        &existing,
        &set_pairs,
        new_row.as_deref(),
        action_name,
    )?;
```

(keeping the preceding `policies_for` fetch and its comment; the local
`let policies = &write_policies.items;` binding is deleted.)

- [x] **Step 4: Run to green (unit + the order-pinning e2es)**

Run: `buck2 test -j 8 //src/services/query-api:mutate-phases //src/services/query-api:update-delete-governance-e2e //src/services/query-api:update-delete-e2e > /tmp/t4.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t4.log`
Expected: PASS — the Task-1 order pins (`update_order_*`) and the four
per-leg governance e2es prove the extraction byte-identical.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c4.log 2>&1; cat /tmp/c4.log` — artifact empty.

- [x] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p4.log 2>&1; grep -c Failed /tmp/p4.log` — expected `0`.

```bash
git add src/services/query-api/src/action.rs src/services/query-api/tests/mutate_phases.rs
git commit -m "refactor(query-api): extract enforce_mutate_policy from run_mutate

The three ordered policy legs (existing-row filter -> deny-column-on-SET ->
resulting-row filter) become one pure, unit-tested seam; tracing lines and
WriteDenied reasons move verbatim. The leg order is pinned at both the e2e
level (Task-1 both-legs pins) and the unit level.

Part of road-qa-action-decomposition."
```

---

### Task 5: action.rs — extract `value_constraint_violations` + `expand_to_full_row` from `run_insert`

**Files:**
- Modify: `src/services/query-api/src/action.rs` (`run_insert` blocks
  :428-459 and :461-480; two new pub fns above `run_insert`)
- Create: `src/services/query-api/tests/insert_phases.rs`
- Modify: `src/services/query-api/BUCK` (new `insert-phases` target)

**Interfaces:**
- Produces (`pub` in `query_api::action`):

```rust
pub fn value_constraint_violations(
    target: &ObjectType,
    pairs: &[(String, SqlValue)],
) -> Result<Vec<ConstraintViolation>, ActionError>

pub fn expand_to_full_row(
    target: &ObjectType,
    pairs: &[(String, SqlValue)],
) -> (Vec<String>, Vec<SqlValue>, Vec<String>)
```

  Task 7 calls `value_constraint_violations` from `run_mutate`; Task 6
  appends the epilogue test to this test file.

- [x] **Step 1: Write the failing tests**

Create `src/services/query-api/tests/insert_phases.rs`:

```rust
//! Pure unit tests for the insert write-path phases extracted from
//! `action.rs::run_insert`: `value_constraint_violations` (per-value declared-
//! constraint check over resolved write pairs) and `expand_to_full_row`
//! (full-property NULL expansion), plus the shared `affected_object` response
//! epilogue. No fixture; the e2e twins live in constraints_action_http /
//! action_e2e.

use control_plane_core::{ConstraintRule, ObjectType, PropertyConstraints, RangeConstraint};
use query_api::action::{expand_to_full_row, value_constraint_violations};
use query_api::serving::SqlValue;

/// Gauge(id Long required, qty Long [0..=100], note String) — one constrained
/// property, one unconstrained, one required-uncovered-by-constraints.
fn gauge() -> ObjectType {
    ObjectType::build("Gauge", ("main", "gauge"))
        .prop_req("id", "Long")
        .prop_with(
            "qty",
            "Long",
            false,
            PropertyConstraints {
                range: Some(RangeConstraint {
                    min: Some(0.0),
                    max: Some(100.0),
                }),
                ..PropertyConstraints::default()
            },
        )
        .prop("note", "String")
        .identity("id")
        .done()
}

#[test]
fn in_range_values_have_no_violations() {
    let pairs = vec![
        ("id".to_string(), SqlValue::Int(1)),
        ("qty".to_string(), SqlValue::Int(50)),
    ];
    assert!(
        value_constraint_violations(&gauge(), &pairs)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn out_of_range_value_is_collected() {
    let pairs = vec![("qty".to_string(), SqlValue::Int(999))];
    let v = value_constraint_violations(&gauge(), &pairs).unwrap();
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].property, "qty");
    assert_eq!(v[0].rule, ConstraintRule::Range);
}

#[test]
fn empty_null_and_unconstrained_cells_are_skipped() {
    // Empty pair set: trivially clean (the DELETE path's shape after Task 7).
    assert!(value_constraint_violations(&gauge(), &[]).unwrap().is_empty());
    let pairs = vec![
        // Omitted optional (NULL): no value to check.
        ("qty".to_string(), SqlValue::Null),
        // No constraints declared on `note`.
        ("note".to_string(), SqlValue::Text("x".into())),
        // Not a property: skipped (conformance rejects the shape upstream).
        ("ghost".to_string(), SqlValue::Int(1)),
    ];
    assert!(
        value_constraint_violations(&gauge(), &pairs)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn expand_fills_unset_properties_with_null_in_declared_order() {
    let pairs = vec![("qty".to_string(), SqlValue::Int(7))];
    let (cols, vals, logical) = expand_to_full_row(&gauge(), &pairs);
    assert_eq!(cols, vec!["id", "qty", "note"]);
    assert_eq!(
        vals,
        vec![SqlValue::Null, SqlValue::Int(7), SqlValue::Null]
    );
    assert_eq!(logical, vec!["Long", "Long", "String"]);
}
```

Add to `src/services/query-api/BUCK`, directly after the new `mutate-phases`
target:

```python
rust_test(
    name = "insert-phases",
    crate = "insert_phases",
    srcs = ["tests/insert_phases.rs"],
    crate_root = "tests/insert_phases.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
    ],
)
```

- [x] **Step 2: Run to see it fail**

Run: `buck2 test //src/services/query-api:insert-phases > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t5.log`
Expected: FAIL — compile error, neither fn exists.

- [x] **Step 3: Extract the two fns and rewire `run_insert`**

Add to `action.rs` above `run_insert` (bodies are the verbatim moved blocks;
the `cast_precision_loss` expect moves WITH its arm — it is pre-existing and
keeps its reason):

```rust
/// Collect every declared-constraint violation over resolved `(property, value)` write
/// pairs, using the same `core` [`PropertyValidator`] that gates the ingest land path —
/// both write paths enforce identical rules. A NULL cell carries no value to check
/// (omitted optionals pass); a pair naming no property is skipped (conformance rejects
/// that shape upstream); a property with no declared constraints is skipped. Pure; the
/// unit-test seam for the constraint phase.
///
/// NOTE: named `value_constraint_violations`, not the register's `validate_constraints` —
/// that name is `control_plane_core::validate_constraints`, the define-time *declaration*
/// validator, and shadowing it here would invite exactly the wrong import.
pub fn value_constraint_violations(
    target: &ObjectType,
    pairs: &[(String, SqlValue)],
) -> Result<Vec<ConstraintViolation>, ActionError> {
    let mut cviol: Vec<ConstraintViolation> = Vec::new();
    for (col, val) in pairs {
        let Some(prop) = target.properties.iter().find(|p| &p.name == col) else {
            continue;
        };
        if prop.constraints.is_empty() {
            continue;
        }
        let validator = PropertyValidator::new(prop)?;
        match val {
            SqlValue::Text(s) => validator.check_str(s, &mut cviol),
            #[expect(
                clippy::cast_precision_loss,
                reason = "range bounds are f64; i64->f64 is acceptable for validation"
            )]
            SqlValue::Int(i) => validator.check_num(*i as f64, &mut cviol),
            SqlValue::Double(d) => validator.check_num(*d, &mut cviol),
            SqlValue::Bool(_) | SqlValue::Date(_) | SqlValue::Timestamp(_) | SqlValue::Null => {}
        }
    }
    Ok(cviol)
}

/// Expand resolved write pairs to the target type's FULL property set (declared order):
/// the parsed value when the action set the column, else NULL. The loom-owned Parquet
/// write must carry every column so the file schema matches the table (part-1
/// unspecified columns default to NULL). Returns the parallel
/// `(columns, values, logical_types)`. Pure; the unit-test seam for the expansion phase.
pub fn expand_to_full_row(
    target: &ObjectType,
    pairs: &[(String, SqlValue)],
) -> (Vec<String>, Vec<SqlValue>, Vec<String>) {
    use std::collections::HashMap;
    let parsed: HashMap<&str, &SqlValue> = pairs.iter().map(|(c, v)| (c.as_str(), v)).collect();
    let mut full_columns: Vec<String> = Vec::with_capacity(target.properties.len());
    let mut full_values: Vec<SqlValue> = Vec::with_capacity(target.properties.len());
    let mut full_logical: Vec<String> = Vec::with_capacity(target.properties.len());
    for p in &target.properties {
        full_columns.push(p.name.clone());
        full_values.push(
            parsed
                .get(p.name.as_str())
                .copied()
                .cloned()
                .unwrap_or(SqlValue::Null),
        );
        full_logical.push(p.ty.clone());
    }
    (full_columns, full_values, full_logical)
}
```

In `run_insert`, replace step 4c (:428-459) with (comment retained; tracing
stays at the call site so the message is byte-identical):

```rust
    // 4c. Per-value constraint validation: reject values violating their property's
    //     declared constraints with a structured 422 (distinct from the 403 ACL denial).
    //     An omitted optional (NULL) carries no value to check. The same `core` validator
    //     drives the ingest land path, so both write paths enforce identical rules.
    let cviol = value_constraint_violations(target, &pairs)?;
    if !cviol.is_empty() {
        tracing::info!(
            action = action_name,
            count = cviol.len(),
            "insert rejected: constraint violation"
        );
        return Err(ActionError::ConstraintViolation(cviol));
    }
```

and replace step 5 (:461-480, from `// 5. Expand to the target type's FULL
property set` through the `for p in &target.properties { … }` loop) with:

```rust
    // 5. Expand to the target type's FULL property set (declared order).
    let (full_columns, full_values, full_logical) = expand_to_full_row(target, &pairs);
```

(the stray `use std::collections::HashMap;` inside `run_insert` moves into
`expand_to_full_row`.)

- [x] **Step 4: Run to green (unit + every insert pin)**

Run: `buck2 test -j 8 //src/services/query-api:insert-phases //src/services/query-api:constraints-action-http //src/services/query-api:write-denial-http //src/services/query-api:action-run-id-http //src/services/query-api:action-conformance-handler //src/services/query-api:action-e2e //src/services/query-api:action-mapping-e2e //src/services/query-api:iceberg-action-e2e > /tmp/t5.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t5.log`
Expected: PASS — `constraints_action_http` pins the exact 422 violations body
and the no-write guarantee; the action e2es pin the full-row write (NULL
expansion) + atomic lineage.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c5.log 2>&1; cat /tmp/c5.log` — artifact empty.

- [x] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p5.log 2>&1; grep -c Failed /tmp/p5.log` — expected `0`.

```bash
git add src/services/query-api/src/action.rs src/services/query-api/tests/insert_phases.rs src/services/query-api/BUCK
git commit -m "refactor(query-api): extract value_constraint_violations + expand_to_full_row

run_insert's two pure phases become unit-tested seams. Renamed from the
register's validate_constraints: that name is control_plane_core's define-time
declaration validator and shadowing it invites the wrong import. Tracing stays
at the call site (message byte-identical); the pre-existing documented
cast_precision_loss expect moves verbatim with its arm. 422 bodies and the
no-write-on-violation guarantee pinned by constraints_action_http; NULL
expansion pinned by the action e2es.

Part of road-qa-action-decomposition."
```

---

### Task 6: action.rs — one response epilogue (`affected_object`) over the `Projection` zip

**Files:**
- Modify: `src/services/query-api/src/action.rs` (`run_insert` epilogue
  :509-529; `run_mutate` epilogue :745-754; new pub fn)
- Test (append): `src/services/query-api/tests/insert_phases.rs` (existing
  target — no BUCK change)

**Interfaces:**
- Consumes: `Projection::of_columns` / `Projection::object_rows` (Task 2).
- Produces (`pub` in `query_api::action`):

```rust
pub fn affected_object(
    target: &ObjectType,
    columns: Vec<String>,
    row: Vec<SqlValue>,
) -> ObjectRows
```

- [x] **Step 1: Write the failing test**

Append to `src/services/query-api/tests/insert_phases.rs` (add
`affected_object` to the `query_api::action` import):

```rust
#[test]
fn affected_object_zips_logical_types_per_column() {
    // Caller column order is preserved; each type is looked up per property —
    // the shared epilogue of run_insert (action-provided columns) and
    // run_mutate (full property columns).
    let rows = affected_object(
        &gauge(),
        vec!["qty".to_string(), "id".to_string()],
        vec![SqlValue::Int(7), SqlValue::Int(1)],
    );
    assert_eq!(rows.columns, vec!["qty", "id"]);
    assert_eq!(rows.logical_types, vec!["Long", "Long"]);
    assert_eq!(rows.rows, vec![vec![SqlValue::Int(7), SqlValue::Int(1)]]);
}
```

- [x] **Step 2: Run to see it fail**

Run: `buck2 test //src/services/query-api:insert-phases > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t6.log`
Expected: FAIL — compile error, `affected_object` not found.

- [x] **Step 3: Implement and rewire both epilogues**

Add to `action.rs` (below `expand_to_full_row`), plus
`use crate::governed::Projection;` in the file's imports:

```rust
/// The shared response epilogue of both write paths: the affected object as a
/// single-row `ObjectRows`, its logical types zipped per column via the governance
/// layer's [`Projection`] (`of_columns` reuses `prop_ty`; an unknown column zips to
/// `""` exactly as the old inline lookups did). INSERT echoes the action-provided
/// columns; UPDATE/DELETE echo the full property set.
pub fn affected_object(
    target: &ObjectType,
    columns: Vec<String>,
    row: Vec<SqlValue>,
) -> ObjectRows {
    Projection::of_columns(target, columns).object_rows(vec![row])
}
```

Replace `run_insert`'s epilogue (:509-529, from `// 8. Return the created
object …` through the final `Ok((…))`) with:

```rust
    // 8. Return the created object (action-provided columns only, as part-1 returns)
    //    plus the run_id so the caller can locate the action's lineage.
    Ok((affected_object(target, columns, values), run_id))
```

Replace `run_mutate`'s epilogue (:745-754) with:

```rust
    // Return the affected object (UPDATE: the new version; DELETE: the removed values).
    let returned = new_row.unwrap_or(existing);
    Ok((affected_object(target, columns, returned), run_id))
```

(`columns` is still borrowed by the earlier `overwrite_table` call and moved
here; `logical` remains a local for `overwrite_table` only — the epilogue now
recomputes types per column, which for the full property set yields the
identical vector, pinned by the update/delete e2e body assertions.)

- [x] **Step 4: Run to green (unit + every response-body pin)**

Run: `buck2 test -j 8 //src/services/query-api:insert-phases //src/services/query-api:action-e2e //src/services/query-api:action-mapping-e2e //src/services/query-api:update-delete-e2e //src/services/query-api:update-delete-tiers-e2e //src/services/query-api:action-run-id-http //src/services/query-api:write-denial-http > /tmp/t6.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t6.log`
Expected: PASS — `action_mapping_e2e` pins the property-keyed (not
param-keyed) columns + constant-filled columns; `update_delete_e2e` pins the
UPDATE/DELETE echo (`objects_to_json` renders `qty`/`name` from the returned
`ObjectRows`, so a logical-type drift would break its string-rendered
assertions); `action_run_id_http` pins the 201 body shape + header.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c6.log 2>&1; cat /tmp/c6.log` — artifact empty.

- [x] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p6.log 2>&1; grep -c Failed /tmp/p6.log` — expected `0`.

```bash
git add src/services/query-api/src/action.rs src/services/query-api/tests/insert_phases.rs
git commit -m "refactor(query-api): shared affected_object epilogue via Projection

Both write-path epilogues (run_insert's action-provided columns, run_mutate's
full property set) collapse onto one affected_object fn built on the
governance layer's Projection::of_columns/object_rows — deleting the last
inline logical-type zip the register flagged. Response bodies pinned
byte-identical by the action/update-delete e2es and the HTTP body tests.

Part of road-qa-action-decomposition."
```

---

### Task 7: WHITELISTED CHANGE — UPDATE actions validate declared value constraints

The single deliberate behavior change (whitelist item 1): `run_mutate` calls
`value_constraint_violations` over the SET values, after the policy legs
(403 before 422, mirroring INSERT). The survey verified no existing test pins
the old bypass. TDD: the e2e lands RED against the current code first.

**Files:**
- Test (append FIRST): `src/services/query-api/tests/update_delete_governance_e2e.rs`
  (existing target — no BUCK change)
- Modify: `src/services/query-api/src/action.rs` (`run_mutate`, after the
  `enforce_mutate_policy` call)

**Interfaces:**
- Consumes: `value_constraint_violations` (Task 5).
- Produces: no new symbols; `run_mutate` gains one phase call.

- [ ] **Step 1: Write the failing e2e**

Append to `src/services/query-api/tests/update_delete_governance_e2e.rs`
(extend the `control_plane_core` import with `PropertyConstraints,
RangeConstraint` — `ActionDef`, `ActionKind`, `ObjectType`, `TypeName` are
already imported):

```rust
// ---------------------------------------------------------------------------
// WHITELISTED CHANGE (road-qa-action-decomposition): UPDATE enforces declared
// per-value constraints on the SET values — previously the mutate path skipped
// them, so an UPDATE could write a value the equivalent INSERT rejects.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_constraint_violation_is_rejected() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // Gauge(id Long identity, qty Long [0..=100]) + insert/update actions.
    let gauge = TypeName("Gauge".into());
    cp.ontology()
        .define_type(
            ObjectType::build("Gauge", ("main", "gauge"))
                .prop_req("id", "Long")
                .prop_with(
                    "qty",
                    "Long",
                    false,
                    PropertyConstraints {
                        range: Some(RangeConstraint {
                            min: Some(0.0),
                            max: Some(100.0),
                        }),
                        ..PropertyConstraints::default()
                    },
                )
                .identity("id")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(
            ActionDef::build("createGauge", "Gauge", ActionKind::Insert)
                .param_req("id", "Long")
                .param("qty", "Long")
                .done(),
        )
        .await
        .unwrap();
    cp.ontology()
        .define_action(
            ActionDef::build("updateGauge", "Gauge", ActionKind::Update)
                .param_req("id", "Long")
                .param_req("qty", "Long")
                .done(),
        )
        .await
        .unwrap();
    let (subj, _role) = grant_writer_role(&cp, &gauge).await;

    let (engine, _eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // Seed {id:1, qty:50} — in range.
    run_action(
        "createGauge",
        json!({ "id": "1", "qty": "50" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("seed insert (in range)");

    // UPDATE {id:1, qty:999}: violates qty <= 100 -> ConstraintViolation
    // (INSERT of the same value is already rejected; UPDATE now matches it).
    let err = run_action(
        "updateGauge",
        json!({ "id": "1", "qty": "999" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            &err,
            ActionError::ConstraintViolation(v) if v.len() == 1 && v[0].property == "qty"
        ),
        "expected ConstraintViolation on qty, got: {err:?}"
    );

    // A conforming UPDATE still runs.
    run_action(
        "updateGauge",
        json!({ "id": "1", "qty": "60" }).as_object().unwrap(),
        &subj,
        &deps,
    )
    .await
    .expect("in-range update runs");

    drop(warehouse);
}
```

- [ ] **Step 2: Run to see it fail (RED — the current mutate path writes 999)**

Run: `buck2 test -j 8 //src/services/query-api:update-delete-governance-e2e > /tmp/t7.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t7.log`
Expected: FAIL — `update_constraint_violation_is_rejected` panics at
`unwrap_err()` (the update currently succeeds). Every other test in the file
stays green.

- [ ] **Step 3: Wire the phase into `run_mutate`**

In `action.rs`, directly after the `enforce_mutate_policy(...)` call (Task 4)
and before the `// Build the new full live set` block, insert:

```rust
    // Per-value constraint validation on the SET values — the same rules INSERT
    // enforces, so an UPDATE can no longer write a value the equivalent INSERT
    // rejects (whitelisted change, road-qa-action-decomposition). Ordered after the
    // policy legs (403 before 422, mirroring INSERT) and after the locate phase (a
    // missing identity stays NotFound). DELETE sets nothing (`set_pairs` is empty),
    // so it is structurally unaffected; the identity is a locator, not a written
    // value, and is not re-validated.
    let cviol = value_constraint_violations(target, &set_pairs)?;
    if !cviol.is_empty() {
        tracing::info!(
            action = action_name,
            count = cviol.len(),
            "update rejected: constraint violation"
        );
        return Err(ActionError::ConstraintViolation(cviol));
    }
```

- [ ] **Step 4: Run to green (the whole mutate family + the insert 422 pins)**

Run: `buck2 test -j 8 //src/services/query-api:update-delete-governance-e2e //src/services/query-api:update-delete-e2e //src/services/query-api:update-delete-tiers-e2e //src/services/query-api:mutate-phases //src/services/query-api:constraints-action-http > /tmp/t7.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t7.log`
Expected: PASS — the new e2e goes green; every pre-existing mutate e2e
(unconstrained Widget type — zero constraint checks fire) and the INSERT 422
pins are untouched.
Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c7.log 2>&1; cat /tmp/c7.log` — artifact empty.

- [ ] **Step 5: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p7.log 2>&1; grep -c Failed /tmp/p7.log` — expected `0`.

```bash
git add src/services/query-api/src/action.rs src/services/query-api/tests/update_delete_governance_e2e.rs
git commit -m "feat(query-api): UPDATE actions enforce declared value constraints

The one whitelisted behavior change of road-qa-action-decomposition: run_mutate
now runs value_constraint_violations over the SET values (the register's
'validate_constraints moves from run_insert so both paths share it'),
returning the same structured 422 violations body INSERT produces. Placed
after the policy legs (403 before 422, mirroring INSERT); DELETE is
structurally unaffected (empty SET) and the identity is a locator, not
re-validated. Closes the gap where an UPDATE could write a value the
equivalent INSERT rejects; the survey confirmed no test pinned the bypass.

Part of road-qa-action-decomposition."
```

---

### Task 8: Full-suite sweep + complexity proof + register close

**Files:**
- Modify: `docs/ROADMAP.md` (`road-qa-action-decomposition`, line 294-295)

- [ ] **Step 1: Full query-api suite**

Run: `buck2 build -M none //src/services/query-api/... > /tmp/b8.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)" /tmp/b8.log`
Expected: `BUILD SUCCEEDED`.
Run: `buck2 test -j 8 //src/services/query-api/... > /tmp/t8.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t8.log`
Expected: PASS (fixture-heavy — the `-j 8` cap avoids postgres boot-slot
starvation). If any unrelated fixture test flakes on timeout, re-run that
target alone before investigating.

- [ ] **Step 2: Prove the metric drop**

Invoke the `loom-complexity` skill with args `diff` (this is a complexity
item — the spec requires the proof). Expected: `run_mutate` drops from cc 44
(census `docs/code-health/complexity.md:74`) to single digits; `run_insert`
from cc 15; no new extracted fn appears at cc ≥ 15. Record the observed
numbers for the register prose (do NOT edit the census — the scheduled
routine refreshes it).

- [ ] **Step 3: Close the register item**

In `docs/ROADMAP.md:294-295`, flip the checkbox/status and replace the prose:

```markdown
- [x] **Action write-path decomposition (run_mutate/run_insert)** `{#road-qa-action-decomposition area:quality status:done from:2026-07-02-pillar-idioms-audit-design pr:- spec:2026-07-02-pillar-idioms-audit-design}`
  Done (PR #-). `run_mutate` (cc 44, 165 lines) decomposed onto named, unit-tested phase seams: `locate_unique_row` (identity match + corrupt-PK guard — previously ZERO coverage; now pinned by a duplicate-identity e2e and unit tests), `enforce_mutate_policy` (the three ordered policy legs, with the security-relevant cross-leg order — existing-row filter → deny-column-on-SET → resulting-row filter — pinned by both-legs e2es added ahead of the extraction; no prior test combined the legs in one policy), and a shared `affected_object` response epilogue over the governance layer's `Projection` (new `of_columns`/`object_rows`, replacing both inline logical-type zips). `run_insert` yielded `value_constraint_violations` + `expand_to_full_row` (both were insert phases — the register listed `expand_to_full_row` under run_mutate, but the COW path reads existing full rows and never NULL-expands; and the register's `validate_constraints` name collides with `control_plane_core::validate_constraints`, the define-time declaration validator, hence the rename). ONE whitelisted behavior change: UPDATE actions now validate declared per-property constraints on the SET values (422 violations body, after the policy legs — 403 before 422, mirroring INSERT; DELETE structurally unaffected), closing the gap where an UPDATE could write a value the equivalent INSERT rejects — verified unpinned before the change. Everything else byte-identical; the update/delete + insert e2e families passed unmodified.
```

Run: `bash tools/docs.sh validate`
Expected: exit 0, no grammar/id/vocab errors.

- [ ] **Step 4: prek + commit**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/p8.log 2>&1; grep -c Failed /tmp/p8.log` — expected `0`.

```bash
git add docs/ROADMAP.md
git commit -m "docs(registers): close road-qa-action-decomposition

Register prose records the verified drift (expand_to_full_row was a run_insert
phase; validate_constraints renamed for the core name collision; the corrupt-PK
guard and the policy cross-leg order were unpinned and got pins first) and the
single-item behavior-change whitelist (UPDATE constraint enforcement). pr:-
updated when the PR opens.

Part of road-qa-action-decomposition."
```

(When the branch's PR is opened, update `pr:-` to `pr:#N` in the same PR.)
