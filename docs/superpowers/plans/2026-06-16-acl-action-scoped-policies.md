# Action-Scoped Policies (Write-Governance Slice 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `acl.policy` action-scoped — a policy is keyed by `(role, action, target)` so a role can hold independent read and write policies — by threading `Action` through `set_policy`/`clear_policy`/`policies_for` across the control plane and its consumers.

**Architecture:** Mirror the already-action-scoped `acl.role_grant`: add an `action` column to `acl.policy`'s row and primary key (migration), thread `action: Action` through the three `Acl` trait methods, update the Postgres adapter SQL (+ regenerate the committed `.sqlx`) and the memory fake's map key, extend the cross-adapter testkit contract with the action-scoping property, and migrate the sole live consumer (the query-api read path) to `Action::Read` — keeping reads behaviorally identical.

**Tech Stack:** Rust 2024, buck2, sqlx compile-time `query!` (committed `.sqlx` cache), hermetic Postgres+DuckDB fixture tests, the ports-&-adapters control-plane contract harness.

**Spec:** `docs/superpowers/specs/2026-06-16-acl-action-scoped-policies-design.md`

**Conventions for every task:**
- **Never run two buck2 commands concurrently** (single daemon — they hang). One at a time, wait for each.
- Don't pipe `buck2 test`/`buck2 bxl` through `tail`/`head` (stalls). Redirect + grep:
  `buck2 test //src/control-plane/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
- Tests are integration `rust_test` / `loom_fixture_test` targets only — never inline `#[cfg(test)]`.
- The control-plane sweep + the query-api e2es boot hermetic Postgres/DuckDB — minutes each. Run sequentially.
- Commit at the end of each task; end commit messages with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- Markdown files end with exactly one trailing newline, no trailing whitespace.

---

## File Structure

- `src/control-plane/postgres/migrations/0011_acl_policy_action.sql` — **create.** Add `action` to `acl.policy`'s row + PK.
- `src/control-plane/core/src/acl.rs` — **modify.** Add `action: Action` to the three trait method signatures.
- `src/control-plane/postgres/src/acl.rs` — **modify.** Thread `action` (reusing `action_to_str`) into the three methods' SQL.
- `src/control-plane/postgres/.sqlx/` — **regenerate.** Via `tools/sqlx-prepare.sh`.
- `src/control-plane/memory/src/acl.rs` — **modify.** `policies` map key gains `Action`; thread through the three methods.
- `src/control-plane/testkit/src/lib.rs` — **modify.** Thread `Action::Read` through existing policy-contract sites (Task 1); add the action-scoping assertion block (Task 2).
- `src/services/query-api/src/handler.rs` — **modify.** `load_policy` passes `Action::Read`.
- `src/services/query-api/tests/{governed_read,derived_properties_e2e,link_traversal,multi_hop_traversal_e2e}.rs` — **modify.** `set_policy` call sites pass `Action::Read`.
- `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, `docs/FUTURE.md` — **modify.** Delivered note (Task 3).

---

## Task 1: Action-scope `acl.policy` across the stack (atomic refactor)

The `Acl` trait signature change breaks every caller, so the whole workspace must change in one commit. This task is behavior-preserving: every existing policy operation becomes `Read`-scoped, so the existing test suite stays green. (Task 2 adds the discriminating Read-vs-Write assertion.)

**Files:** the migration, `core/src/acl.rs`, `postgres/src/acl.rs` (+ `.sqlx`), `memory/src/acl.rs`, `testkit/src/lib.rs`, `query-api/src/handler.rs`, the four query-api e2e test files.

- [ ] **Step 1: Write the migration.**

Create `src/control-plane/postgres/migrations/0011_acl_policy_action.sql`:

```sql
-- Action-scope acl.policy: a policy is keyed by (role, action, target), so a role can
-- hold independent read and write policies on the same target. Mirrors acl.role_grant,
-- which is already action-scoped. Existing rows backfill to 'read' (reads were policy's
-- only enforcer until now); the default is then dropped so the app must always specify.
alter table acl.policy add column action text not null default 'read';
alter table acl.policy drop constraint policy_pkey;
alter table acl.policy add primary key (role_id, action, target_kind, target_a, target_b);
alter table acl.policy alter column action drop default;
```

- [ ] **Step 2: Change the three trait signatures.**

In `src/control-plane/core/src/acl.rs`, update the trait methods (around lines 202-224) to add `action: Action`, and update their doc comments to say the policy is keyed by `(role, action, target)`:

```rust
    /// Create or replace the row/column policy for `(role, action, policy.target)`. Role
    /// must exist, else `NotFound`. Upsert. Read and write policies are independent.
    async fn set_policy(&self, role: &RoleId, action: Action, policy: Policy) -> Result<()>;
    /// Remove the policy for `(role, action, target)`. Idempotent (no-op if absent).
    async fn clear_policy(&self, role: &RoleId, action: Action, target: &PolicyTarget) -> Result<()>;
```

```rust
    /// All policies across `subject`'s roles for `(action, target)` (order unspecified).
    /// Unknown subject → empty vec. No merging. The `page` request is accepted but not yet
    /// enforced; results are a single full page.
    async fn policies_for(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
        page: PageReq,
    ) -> Result<Page<Policy>>;
```

- [ ] **Step 3: Update the Postgres adapter.**

In `src/control-plane/postgres/src/acl.rs`, thread `action` through the three methods, reusing the existing `action_to_str` helper (already used by `check`/`grant` to map `Action` → `'read'|'write'`).

`set_policy` — change the signature and the insert (add the `action` column + bind, and include `action` in the conflict key). The role-exists check and the row-filter ontology validation above it are unchanged:

```rust
    async fn set_policy(&self, role: &RoleId, action: Action, policy: Policy) -> Result<()> {
```

```rust
        sqlx::query!(
            "insert into acl.policy \
                 (role_id, action, target_kind, target_a, target_b, row_filter, deny_columns, mask_columns) \
             values ($1, $2, $3, $4, $5, $6, $7, $8) \
             on conflict (role_id, action, target_kind, target_a, target_b) do update set \
                 row_filter = excluded.row_filter, \
                 deny_columns = excluded.deny_columns, \
                 mask_columns = excluded.mask_columns",
            &role.0,
            action_to_str(action),
            kind,
            &a,
            &b,
            row_filter,
            &policy.deny_columns,
            &policy.mask_columns,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }
```

`clear_policy` — add `action` and the `and action = $2` clause (renumber the remaining binds):

```rust
    async fn clear_policy(&self, role: &RoleId, action: Action, target: &PolicyTarget) -> Result<()> {
        let (kind, a, b) = target_cols(target);
        sqlx::query!(
            "delete from acl.policy where role_id = $1 and action = $2 and target_kind = $3 \
             and target_a = $4 and target_b = $5",
            &role.0,
            action_to_str(action),
            kind,
            &a,
            &b,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }
```

`policies_for` — add `action` and the `p.action = $2` filter (renumber the remaining binds):

```rust
    async fn policies_for(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
        _page: PageReq,
    ) -> Result<Page<Policy>> {
        let (kind, a, b) = target_cols(target);
        let rows = sqlx::query!(
            "with recursive eff(role_id) as ( \
                 select role_id from acl.role_member where subject_id = $1 \
                 union \
                 select ri.inherits_id from acl.role_inherits ri \
                   join eff on ri.role_id = eff.role_id \
             ) \
             select p.row_filter, p.deny_columns, p.mask_columns \
             from eff join acl.policy p on p.role_id = eff.role_id \
             where p.action = $2 and p.target_kind = $3 and p.target_a = $4 and p.target_b = $5",
            &subject.0,
            action_to_str(action),
            kind,
            &a,
            &b,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
```

(The rest of `policies_for` — the row→`Policy` mapping — is unchanged.)

- [ ] **Step 4: Update the memory fake.**

In `src/control-plane/memory/src/acl.rs`, change the `policies` map key to include `Action` (mirroring the `grants` map, which already keys on `(String, Action, TargetKey)`):

The field declaration (around line 27):

```rust
    policies: HashMap<(String, Action, TargetKey), Policy>, // (role, action, target) -> policy
```

`set_policy` — signature + key (the role-exists check and row-filter validation are unchanged):

```rust
    async fn set_policy(&self, role: &RoleId, action: Action, policy: Policy) -> Result<()> {
```
```rust
        // insert (short acl lock)
        let key = (role.0.clone(), action, target_key(&policy.target));
        self.acl.lock().unwrap().policies.insert(key, policy);
        Ok(())
```

`clear_policy` — signature + key:

```rust
    async fn clear_policy(&self, role: &RoleId, action: Action, target: &PolicyTarget) -> Result<()> {
        self.acl
            .lock()
            .unwrap()
            .policies
            .remove(&(role.0.clone(), action, target_key(target)));
        Ok(())
    }
```

`policies_for` — signature + lookup key:

```rust
    async fn policies_for(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
        _page: PageReq,
    ) -> Result<Page<Policy>> {
```
and in its body change the per-role lookup from `acl.policies.get(&(role.clone(), tk.clone()))` to:
```rust
                .filter_map(|role| acl.policies.get(&(role.clone(), action, tk.clone())).cloned())
```

- [ ] **Step 5: Thread `Action::Read` through the testkit contract (mechanical).**

In `src/control-plane/testkit/src/lib.rs`, add `Action::Read` to **every** existing `set_policy`, `clear_policy`, and `policies_for` call (the acl policy contract, the role-inheritance test, and the set_policy-validation tests). Do NOT change any assertion — every existing policy op becomes `Read`-scoped, behavior-identical. `Action` is already imported (the contract uses `Action::Read`/`Action::Write` for grants/`check`).

The pattern, applied to each site:
- `a.set_policy(&rid("reader"), pol.clone())` → `a.set_policy(&rid("reader"), Action::Read, pol.clone())`
- `a.policies_for(&sid("alice"), &ttype("Customer"), PageReq::unbounded())` → `a.policies_for(&sid("alice"), Action::Read, &ttype("Customer"), PageReq::unbounded())`
- `a.clear_policy(&rid("reader"), &ttype("Customer"))` → `a.clear_policy(&rid("reader"), Action::Read, &ttype("Customer"))`

(Sites are around lines 1026, 1029, 1045, 1047, 1068, 1070, 1078, 1085, 1092, 1096, 1110, 1114, 1130, 1150, 1306, 1318, 1351, 1365, 1379, 1392 — search the file for `set_policy`/`policies_for`/`clear_policy` and convert each.)

- [ ] **Step 6: Migrate the query-api read consumer.**

In `src/services/query-api/src/handler.rs`, `load_policy` calls `acl.policies_for(subject, target, PageReq::unbounded())` — change to:

```rust
        .policies_for(subject, Action::Read, target, PageReq::unbounded())
```

`Action` is already imported in `handler.rs` (used in the `check` calls). Then add `Action::Read` to the `set_policy` call sites in the four query-api e2e tests: `tests/governed_read.rs`, `tests/derived_properties_e2e.rs`, `tests/link_traversal.rs`, `tests/multi_hop_traversal_e2e.rs` (each `cp.set_policy(&role, Policy {...})` → `cp.set_policy(&role, Action::Read, Policy {...})`; `Action` is already imported in each via its `grant` calls).

- [ ] **Step 7: Regenerate the `.sqlx` cache.**

The `set_policy`/`clear_policy`/`policies_for` `query!` macros changed, so the committed cache is stale. Run the GENERATE tool (it boots the pinned Postgres, applies the migrations including `0011`, attaches DuckLake, runs `cargo sqlx prepare`):

Run: `./tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; tail -5 /tmp/sqlx.log`
Expected: it completes and writes/updates files under `src/control-plane/postgres/.sqlx/`. `git status src/control-plane/postgres/.sqlx/` should show changes (new/updated `query-*.json`).

- [ ] **Step 8: Build the whole control plane — expect it compiles.**

Run: `buck2 build //src/control-plane/... //src/services/query-api/... > /tmp/b.log 2>&1; grep -E "BUILD (SUCCEEDED|FAILED)|error\[" /tmp/b.log`
Expected: `BUILD SUCCEEDED`. (If a caller was missed, the build names it — fix and rebuild.)

- [ ] **Step 9: Run the control-plane sweep — existing behavior green.**

Run: `buck2 test //src/control-plane/... > /tmp/cp.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/cp.log`
Expected: `Fail 0`. This runs the `acl` contract on BOTH the memory fake and the hermetic Postgres adapter (now Read-scoped, behavior-identical), plus the `sqlx-cache-check` test validating the regenerated `.sqlx` against the migrated schema.

- [ ] **Step 10: Regression — a query-api read e2e stays green.**

Run: `buck2 test //src/services/query-api:governed-read > /tmp/gr.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/gr.log`
Expected: `Fail 0` — reads behave identically (now explicitly Read-scoped).

- [ ] **Step 11: Commit.**

```bash
git add src/control-plane/postgres/migrations/0011_acl_policy_action.sql \
        src/control-plane/core/src/acl.rs \
        src/control-plane/postgres/src/acl.rs \
        src/control-plane/postgres/.sqlx \
        src/control-plane/memory/src/acl.rs \
        src/control-plane/testkit/src/lib.rs \
        src/services/query-api/src/handler.rs \
        src/services/query-api/tests/governed_read.rs \
        src/services/query-api/tests/derived_properties_e2e.rs \
        src/services/query-api/tests/link_traversal.rs \
        src/services/query-api/tests/multi_hop_traversal_e2e.rs
git commit -m "feat(acl): action-scope acl.policy (set/clear/policies_for take Action)

acl.policy gains an action column + composite PK (mirroring role_grant); the three
policy trait methods thread Action through both adapters. Read path migrates to
Action::Read (behavior-identical). Enables independent read/write policies.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Action-scoping contract assertion (the new behavior)

Add the discriminating property to the cross-adapter `acl` contract: a Read policy and a Write policy on the same `(role, target)` are independent, and `clear_policy` is action-scoped. This runs on both the memory fake and Postgres, so they're proven to agree.

**Files:**
- Modify: `src/control-plane/testkit/src/lib.rs`

- [ ] **Step 1: Add the action-scoping assertion block.**

In `src/control-plane/testkit/src/lib.rs`, insert this block into the acl policy contract immediately AFTER the `mask_columns` round-trip assertion (after the `assert_eq!(got_mask.items, vec![pol_mask], ...)` near line 1121) and BEFORE the `// --- grant / set_policy on a missing role -> NotFound ---` section. It uses `ttype("Ticket")` with `row_filter: None` so no ontology fixture is needed (set_policy only validates the row_filter's properties when a filter is present), and `reader` (already assigned to alice):

```rust
    // --- action scoping: read and write policies are independent ---
    // Distinct Read and Write policies on the SAME (role, target). row_filter: None so no
    // ontology validation is needed; the point is storage keyed by action.
    let ticket_read = Policy {
        target: ttype("Ticket"),
        row_filter: None,
        deny_columns: vec!["priority".into()],
        mask_columns: vec![],
    };
    let ticket_write = Policy {
        target: ttype("Ticket"),
        row_filter: None,
        deny_columns: vec!["assignee".into()],
        mask_columns: vec![],
    };
    a.set_policy(&rid("reader"), Action::Read, ticket_read.clone())
        .await
        .unwrap();
    a.set_policy(&rid("reader"), Action::Write, ticket_write.clone())
        .await
        .unwrap();
    // A Read query returns only the Read policy; a Write query only the Write policy.
    let r = a
        .policies_for(&sid("alice"), Action::Read, &ttype("Ticket"), PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(
        r.items,
        vec![ticket_read.clone()],
        "Read query returns only the Read-scoped policy"
    );
    let w = a
        .policies_for(&sid("alice"), Action::Write, &ttype("Ticket"), PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(
        w.items,
        vec![ticket_write.clone()],
        "Write query returns only the Write-scoped policy"
    );
    // clear_policy is action-scoped: clearing Read leaves Write intact.
    a.clear_policy(&rid("reader"), Action::Read, &ttype("Ticket"))
        .await
        .unwrap();
    assert!(
        a.policies_for(&sid("alice"), Action::Read, &ttype("Ticket"), PageReq::unbounded())
            .await
            .unwrap()
            .is_empty(),
        "Read policy cleared"
    );
    assert_eq!(
        a.policies_for(&sid("alice"), Action::Write, &ttype("Ticket"), PageReq::unbounded())
            .await
            .unwrap()
            .items,
        vec![ticket_write],
        "Write policy survives clearing the Read policy"
    );
```

- [ ] **Step 2: Run the control-plane sweep — expect PASS on both adapters.**

Run: `buck2 test //src/control-plane/... > /tmp/cp.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/cp.log`
Expected: `Fail 0` — the new assertions pass against both the memory fake and the Postgres adapter (proving the storage keying agrees across adapters).

- [ ] **Step 3: Commit.**

```bash
git add src/control-plane/testkit/src/lib.rs
git commit -m "test(acl): contract proves read/write policy isolation

A Read and a Write policy on the same (role, target) are independent; policies_for
filters by action; clear_policy removes only the named action's policy. Runs on
both the memory fake and Postgres.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Docs — slice 1 delivered, slice 2 pending

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`
- Modify: `docs/FUTURE.md`

- [ ] **Step 1: Roadmap note.**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, under the **Actions** bullets' `*Later:*` line (which lists "fine-grained write governance (row-filter / deny-column on `Write`)"), add a delivered sub-note immediately after that `*Later:*` paragraph:

```markdown
  - *Fine-grained write governance — part 1 (control plane)* ✅ DELIVERED
    (`2026-06-16-acl-action-scoped-policies-design.md`). `acl.policy` is now action-scoped:
    `set_policy`/`clear_policy`/`policies_for` key on `(role, action, target)`, so a role holds
    independent read and write policies (parity with the already-action-scoped `role_grant`). The
    read path is explicitly `Read`-scoped (unchanged). Part 2 (service enforcement: `run_action`
    consumes the `Write` policy — deny-write-column + row-filter-on-insert) is the next slice.
```

- [ ] **Step 2: Update the FUTURE.md write-governance follow-up.**

In `docs/FUTURE.md`, under the **Actions (Step 3)** section, find the bullet beginning "**Fine-grained write governance.**" and replace it with:

```markdown
- **Fine-grained write governance.** Part 1 (control plane) ✅ DELIVERED
  (`2026-06-16-acl-action-scoped-policies-design.md`): `acl.policy` is action-scoped, so read and
  write policies are independent. Part 2 (service enforcement) remains: `run_action` loads the
  `Write` policy and enforces deny-write-column (reject if a param sets a denied column) +
  row-filter-on-insert (a pure in-memory `RowFilter` evaluator — the inserted row must satisfy the
  predicate). `mask_columns` on a `Write` policy is expected to be ignored (masking is read-only) —
  to be confirmed in part 2.
```

- [ ] **Step 3: Run the markdown lint hooks; ensure clean.**

Run: `buck2 run //tools:prek -- run --all-files > /tmp/lint.log 2>&1; grep -iE "Failed|Passed" /tmp/lint.log | tail -20`
Expected: `end-of-file-fixer` / `trim trailing whitespace` pass (or fix in place — include any fixes in the commit). Both `.md` files end with exactly one trailing newline.

- [ ] **Step 4: Commit.**

```bash
git add docs/superpowers/specs/2026-06-06-loom-roadmap.md docs/FUTURE.md
git commit -m "docs(acl): action-scoped policies delivered (write-governance part 1)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- Migration (action column + PK, backfill read, drop default) → Task 1 Step 1. ✓
- Trait signatures gain `action` → Task 1 Step 2. ✓
- Postgres adapter SQL (set/clear/policies_for) + `.sqlx` regen → Task 1 Steps 3, 7. ✓
- Memory fake map key + 3 methods → Task 1 Step 4. ✓
- Testkit contract threaded + action-scoping property → Task 1 Step 5 (thread) + Task 2 (property). ✓
- Read-path consumer → `Action::Read` (handler + 4 e2es), reads unchanged → Task 1 Step 6 + Steps 9-10 verify green. ✓
- Cross-adapter agreement + sqlx-cache-check → Task 1 Step 9, Task 2 Step 2. ✓
- Docs delivered note → Task 3. ✓

**Placeholder scan:** No TBD/TODO; every code step shows full code; the testkit threading (Step 5) is mechanical and enumerated by pattern + line hints. ✓

**Type consistency:** `set_policy(role, action, policy)`, `clear_policy(role, action, target)`, `policies_for(subject, action, target, page)` are identical across the trait (Step 2), Postgres (Step 3), memory (Step 4), contract (Step 5 / Task 2), and consumer (Step 6). The memory map key `(String, Action, TargetKey)` matches the lookups. `action_to_str(action)` is the existing Postgres helper. ✓

**Note on atomicity:** Task 1 is necessarily one commit — the trait signature change breaks all callers simultaneously, so the workspace only builds once every caller (both adapters, contract, query-api handler + 4 e2es) is updated. It is behavior-preserving (all ops become `Read`-scoped); Task 2 adds the discriminating Read-vs-Write test.
