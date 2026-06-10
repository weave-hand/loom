# ACL Deny-Override Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an Allow/Deny `effect` to coarse ACL grants with deny-wins precedence, so policy can express "allow broadly, then deny exceptions."

**Architecture:** A new core `Effect` enum threads through `Acl::grant`; both adapters (memory, postgres) persist it (postgres via a new `effect` column, upserted); `check` applies deny-wins (any matching Deny → Deny; else any Allow → Allow; else default-deny). The fine row/column policy layer and the query-api `read_object`/`compile_select` are unchanged — the read gate already calls `check`, so it inherits deny-override.

**Tech Stack:** Rust, buck2, sqlx compile-time queries (postgres adapter), hermetic Postgres via `PgFixture`, shared `control-plane-testkit` contract run on both adapters.

**Reference spec:** `docs/superpowers/specs/2026-06-10-acl-deny-override-design.md`.

**Conventions inherited from this repo (do not deviate):**
- Tests run as `rust_test` integration targets via `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...` (hermetic Postgres refuses root on RE).
- **rustfmt is a check-only commit hook** — run `buck2 run //tools:rustfmt -- <files>` before every commit or it silently aborts; confirm the SHA changed afterward.
- Lint: `tools/clippy-all.sh` + `buck2 run //tools:prek -- run --all-files`. Keep the branch lint-clean; NO `--no-verify`.
- The postgres adapter uses compile-time `sqlx::query!`. After changing any SQL, regenerate the committed `.sqlx` cache with `tools/sqlx-prepare.sh` and commit it; the `//src/control-plane/postgres:sqlx-cache-check` test gates freshness.
- Conventional Commits; each commit ends with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Never weaken a test assertion to make it pass.

---

## File Structure

- `src/control-plane/core/src/acl.rs` — **Modify:** add `Effect` enum; add `effect` param to `Acl::grant`.
- `src/control-plane/core/src/lib.rs` — **Modify:** re-export `Effect`.
- `src/control-plane/postgres/migrations/0005_acl_grant_effect.sql` — **Create:** add the `effect` column.
- `src/control-plane/postgres/src/acl.rs` — **Modify:** `grant` upserts effect; `check` applies deny-wins; add `effect_to_str`.
- `src/control-plane/postgres/.sqlx/` — **Regenerate:** committed query cache.
- `src/control-plane/memory/src/acl.rs` — **Modify:** grants map holds `Effect`; `grant` upserts; `check` applies deny-wins.
- `src/control-plane/testkit/src/lib.rs` — **Modify:** update existing `grant(...)` calls to pass `Effect::Allow`; extend the acl contract with deny-override cases.
- `src/services/query-api/tests/governed_read.rs` — **Modify:** the oracle's `grant(...)` call passes `Effect::Allow`; add a deny-override end-to-end assertion.

---

## Task 1: Plumbing — `Effect` type, `grant` signature, persistence (no behavior change yet)

This task adds the `Effect` enum, threads it through `grant`, persists it in both adapters, and updates all call sites — but `check` still ignores effect (allow-if-any), so **all existing tests stay green**. The deny-wins behavior lands in Task 2.

**Files:**
- Modify: `src/control-plane/core/src/acl.rs`, `src/control-plane/core/src/lib.rs`
- Create: `src/control-plane/postgres/migrations/0005_acl_grant_effect.sql`
- Modify: `src/control-plane/postgres/src/acl.rs`, `src/control-plane/memory/src/acl.rs`, `src/control-plane/testkit/src/lib.rs`, `src/services/query-api/tests/governed_read.rs`
- Regenerate: `src/control-plane/postgres/.sqlx/`

- [ ] **Step 1: Add the `Effect` enum and change the `grant` signature in core.**

In `src/control-plane/core/src/acl.rs`, add the enum near `Decision` (after the `Action` enum):
```rust
/// Whether a grant permits or forbids its `(action, target)`. Deny wins over Allow
/// in [`Acl::check`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    Allow,
    Deny,
}
```
Change the `grant` method in the `Acl` trait (currently
`async fn grant(&self, role: &RoleId, action: Action, target: PolicyTarget) -> Result<()>;`) to:
```rust
    /// Grant or deny a coarse `(action, target)` to a role. Upserts by
    /// `(role, action, target)`: re-granting the same key replaces its effect. Role
    /// must exist, else `NotFound`. Idempotent for a fixed effect.
    async fn grant(
        &self,
        role: &RoleId,
        action: Action,
        target: PolicyTarget,
        effect: Effect,
    ) -> Result<()>;
```
Leave `revoke`, `check`, `set_policy`, `policies_for` signatures unchanged.

- [ ] **Step 2: Re-export `Effect` from core.**

In `src/control-plane/core/src/lib.rs`, add `Effect` to the `pub use acl::{...}` list (alphabetically near `Decision`), e.g.:
```rust
pub use acl::{
    Acl, Action, CompareOp, Decision, Effect, Policy, PolicyTarget, RoleId, RowFilter, ScalarValue,
    SubjectId,
};
```

- [ ] **Step 3: Memory adapter — store the effect (check still allow-if-any).**

In `src/control-plane/memory/src/acl.rs`:
- Change the grants field from a set to a map. Find `grants: HashSet<(String, Action, TargetKey)>,` and replace with:
```rust
    grants: HashMap<(String, Action, TargetKey), Effect>, // (role, action, target) -> effect
```
- Add `Effect` to the `use control_plane_core::{...}` import list.
- Update `grant` to take and store the effect (upsert via `insert`):
```rust
    async fn grant(
        &self,
        role: &RoleId,
        action: Action,
        target: PolicyTarget,
        effect: Effect,
    ) -> Result<()> {
        let mut acl = self.acl.lock().unwrap();
        if !acl.roles.contains(&role.0) {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        acl.grants
            .insert((role.0.clone(), action, target_key(&target)), effect);
        Ok(())
    }
```
- `revoke` removes by key — change `.remove(&(...))` target from the set to the map (same call, `HashMap::remove` takes the key ref): the existing `.grants.remove(&(role.0.clone(), action, target_key(target)));` compiles unchanged against a `HashMap`.
- Leave `check` AS-IS for now, but it must compile against the map: the current body uses `acl.grants.contains(&(role.clone(), action, tk.clone()))`. `HashMap` has no `contains`; change that one call to `.contains_key(&(role.clone(), action, tk.clone()))`. Behavior is still allow-if-any (effect ignored) — deny-wins comes in Task 2.

- [ ] **Step 4: Postgres migration — add the `effect` column.**

Create `src/control-plane/postgres/migrations/0005_acl_grant_effect.sql`:
```sql
-- Deny-override: a grant now carries an effect ('allow' | 'deny'); deny wins in check().
-- Existing rows default to 'allow' (backward-compatible; no backfill needed).
alter table acl.role_grant
    add column effect text not null default 'allow';
```

- [ ] **Step 5: Postgres adapter — upsert the effect (check unchanged this task).**

In `src/control-plane/postgres/src/acl.rs`:
- Add a helper near `action_to_str`:
```rust
fn effect_to_str(effect: Effect) -> &'static str {
    match effect {
        Effect::Allow => "allow",
        Effect::Deny => "deny",
    }
}
```
- Add `Effect` to the `use control_plane_core::{...}` imports.
- Change `grant` to accept `effect` and upsert it (replace the existing `insert ... on conflict do nothing`):
```rust
    async fn grant(
        &self,
        role: &RoleId,
        action: Action,
        target: PolicyTarget,
        effect: Effect,
    ) -> Result<()> {
        let r_exists = sqlx::query_scalar!(
            "select exists (select 1 from acl.role where id = $1)",
            &role.0,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?
        .unwrap_or(false);
        if !r_exists {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        let (kind, a, b) = target_cols(&target);
        sqlx::query!(
            "insert into acl.role_grant (role_id, action, target_kind, target_a, target_b, effect) \
             values ($1, $2, $3, $4, $5, $6) \
             on conflict (role_id, action, target_kind, target_a, target_b) \
             do update set effect = excluded.effect",
            &role.0,
            action_to_str(action),
            kind,
            &a,
            &b,
            effect_to_str(effect),
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }
```
Leave `check` exactly as-is this task (it ignores `effect`; still allow-if-any).

- [ ] **Step 6: Update all `grant(...)` call sites to pass `Effect::Allow`.**

These are the only first-party callers; each gains a trailing `Effect::Allow` argument. Add `Effect` to each file's imports.
- `src/control-plane/testkit/src/lib.rs` lines ~679, ~712, ~729, ~732, ~883 — e.g.
  `a.grant(&rid("reader"), Action::Read, ttype("Customer"))` →
  `a.grant(&rid("reader"), Action::Read, ttype("Customer"), Effect::Allow)`. (The line 883 case is the "grant on nonexistent role" error test — it also gets `, Effect::Allow`.) Add `Effect` to the testkit `use control_plane_core::{...}` list.
- `src/services/query-api/tests/governed_read.rs` line ~143 —
  `cp.grant(&role, Action::Read, PolicyTarget::Type(TypeName("Order".into())))` →
  `cp.grant(&role, Action::Read, PolicyTarget::Type(TypeName("Order".into())), Effect::Allow)`. Add `Effect` to its `use control_plane_core::{...}` list.

- [ ] **Step 7: Regenerate the sqlx cache.**

Run: `tools/sqlx-prepare.sh`
Expected: it boots the pinned Postgres, applies migrations (incl. the new `0005`), and rewrites `src/control-plane/postgres/.sqlx/`. The `grant` query's cache entry now reflects the 6-column insert.

- [ ] **Step 8: Build + run the full sweep — everything still green (no behavior change).**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...`
Expected: all PASS, including `memory:acl`, `postgres:acl`, the testkit acl contract, `postgres:sqlx-cache-check`, and `query-api:governed-read`. (Effect is stored but unused by `check`, and every grant is `Allow`, so nothing changes behaviorally.)

- [ ] **Step 9: Commit.**
```bash
buck2 run //tools:rustfmt -- src/control-plane/core/src/acl.rs src/control-plane/core/src/lib.rs src/control-plane/postgres/src/acl.rs src/control-plane/memory/src/acl.rs src/control-plane/testkit/src/lib.rs src/services/query-api/tests/governed_read.rs
git add -A
git commit -m "feat(acl): thread Allow/Deny effect through grant (persist only)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Behavior — deny-wins in `check` + the shared contract case

Now `check` applies deny-wins on both adapters, and the shared testkit contract proves it (running on the in-memory fake AND real Postgres).

**Files:**
- Modify: `src/control-plane/memory/src/acl.rs`, `src/control-plane/postgres/src/acl.rs`, `src/control-plane/testkit/src/lib.rs`
- Regenerate: `src/control-plane/postgres/.sqlx/`

- [ ] **Step 1: Write the failing contract case (deny-override).**

In `src/control-plane/testkit/src/lib.rs`, inside the acl contract function (the one containing the grants around line 679), append these assertions before its end. They use the existing helpers `rid`, `sid`, `ttype`, `a.assign_role` (the contract already sets up subjects/roles; reuse its `alice`/`reader` or add a second role as below):
```rust
    // --- deny-override (deny wins over allow) ---
    // alice is in `reader` (Allow Read Customer) from earlier in this contract.
    // Put her in a second role `blocked` that DENIES Read Customer; deny must win.
    a.define_role(&rid("blocked")).await.expect("define blocked");
    a.assign_role(&sid("alice"), &rid("blocked"))
        .await
        .expect("assign blocked");
    a.grant(&rid("reader"), Action::Read, ttype("Customer"), Effect::Allow)
        .await
        .expect("re-allow reader");
    a.grant(&rid("blocked"), Action::Read, ttype("Customer"), Effect::Deny)
        .await
        .expect("deny via blocked");
    assert_eq!(
        a.check(&sid("alice"), Action::Read, &ttype("Customer"))
            .await
            .expect("check deny-override"),
        Decision::Deny,
        "a Deny grant in any of the subject's roles overrides Allow",
    );
    // Remove the deny → Allow is restored.
    a.revoke(&rid("blocked"), Action::Read, &ttype("Customer"))
        .await
        .expect("revoke deny");
    assert_eq!(
        a.check(&sid("alice"), Action::Read, &ttype("Customer"))
            .await
            .expect("check after revoke"),
        Decision::Allow,
        "revoking the deny restores Allow",
    );
    // Upsert flips effect: granting Deny on the existing Allow key denies.
    a.grant(&rid("reader"), Action::Read, ttype("Customer"), Effect::Deny)
        .await
        .expect("flip reader to deny");
    assert_eq!(
        a.check(&sid("alice"), Action::Read, &ttype("Customer"))
            .await
            .expect("check after flip"),
        Decision::Deny,
        "re-granting the same key with Deny upserts the effect",
    );
```
(If the contract's earlier section already revokes/leaves `reader`'s Customer grant in a particular state, restore the Allow first as shown — the `a.grant(reader, ..., Allow)` line above does this so the block is self-contained regardless of prior state.)

- [ ] **Step 2: Run it — verify it fails.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:acl //src/control-plane/postgres:acl`
Expected: FAIL — both adapters currently return `Allow` (deny-wins not implemented), so the first deny assertion fails.

- [ ] **Step 3: Implement deny-wins in the memory adapter.**

In `src/control-plane/memory/src/acl.rs`, replace the body of `check` with a deny-wins fold over the subject's roles' matching grants:
```rust
    async fn check(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
    ) -> Result<Decision> {
        let acl = self.acl.lock().unwrap();
        let tk = target_key(target);
        let mut saw_allow = false;
        for role in acl
            .members
            .iter()
            .filter(|(s, _)| s == &subject.0)
            .map(|(_, r)| r)
        {
            match acl.grants.get(&(role.clone(), action, tk.clone())) {
                Some(Effect::Deny) => return Ok(Decision::Deny),
                Some(Effect::Allow) => saw_allow = true,
                None => {}
            }
        }
        Ok(if saw_allow {
            Decision::Allow
        } else {
            Decision::Deny
        })
    }
```

- [ ] **Step 4: Implement deny-wins in the postgres adapter.**

In `src/control-plane/postgres/src/acl.rs`, replace the `check` body's query + decision (the `query_scalar!` `select exists(...)` and the `if allow {...}`) with a two-flag aggregate:
```rust
        let (kind, a, b) = target_cols(target);
        let row = sqlx::query!(
            "select \
                 bool_or(g.effect = 'deny') as has_deny, \
                 bool_or(g.effect = 'allow') as has_allow \
             from acl.role_member m \
             join acl.role_grant g on g.role_id = m.role_id \
             where m.subject_id = $1 and g.action = $2 \
               and g.target_kind = $3 and g.target_a = $4 and g.target_b = $5",
            &subject.0,
            action_to_str(action),
            kind,
            &a,
            &b,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?;
        Ok(if row.has_deny == Some(true) {
            Decision::Deny
        } else if row.has_allow == Some(true) {
            Decision::Allow
        } else {
            Decision::Deny
        })
```
(`bool_or` over zero matching rows yields SQL `NULL` → `Option<bool>` `None` → falls through to default-deny.)

- [ ] **Step 5: Regenerate the sqlx cache (the `check` query changed).**

Run: `tools/sqlx-prepare.sh`
Expected: `.sqlx/` updated for the new `check` aggregate query (two nullable `bool` columns).

- [ ] **Step 6: Run the contract on both adapters — verify it passes.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:acl //src/control-plane/postgres:acl`
Expected: PASS on both.

- [ ] **Step 7: Commit.**
```bash
buck2 run //tools:rustfmt -- src/control-plane/memory/src/acl.rs src/control-plane/postgres/src/acl.rs src/control-plane/testkit/src/lib.rs
git add -A
git commit -m "feat(acl): deny-wins precedence in check (both adapters) + contract

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: query-api end-to-end deny-override

Prove deny-override flows through the live read gate: a subject Allowed via one role but Denied via another is rejected by `read_object`.

**Files:**
- Modify: `src/services/query-api/tests/governed_read.rs`

- [ ] **Step 1: Add the failing end-to-end assertion.**

In `src/services/query-api/tests/governed_read.rs`, after the existing assertions in `governed_object_read` (the deny-by-default `stranger` block added earlier ends the test), append a deny-override case. It reuses the `Order` type + `analyst` subject + `analysts` role already set up; add a second role that denies and assert `Forbidden`:
```rust
    // 8. Deny-override: analyst keeps the Allow grant via `analysts`, but a second
    //    role with a Deny grant on Order must override it -> Forbidden.
    let blocked = RoleId("blocked".into());
    cp.define_role(&blocked).await.unwrap();
    cp.assign_role(&subj, &blocked).await.unwrap();
    cp.grant(
        &blocked,
        Action::Read,
        PolicyTarget::Type(TypeName("Order".into())),
        Effect::Deny,
    )
    .await
    .unwrap();
    let denied = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![],
        },
        &Subject(subj.clone()),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(denied, QueryError::Forbidden),
        "deny grant overrides allow at the read gate, got {denied:?}"
    );
```
Note: `subj` must still be available here. In the current test `subj` is moved into the earlier `&Subject(subj)` call (step 6) — change that earlier call to `&Subject(subj.clone())` so `subj` remains usable. Add `Action`, `Effect`, `RoleId` to the test's `use control_plane_core::{...}` import (the deny-by-default block already imports `read_object`, `QueryError`, etc.).

- [ ] **Step 2: Run it — verify it passes (deny-wins already implemented in Task 2).**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/services/query-api:governed-read`
Expected: PASS. (The gate calls `check`, which now returns `Deny` for `analyst` on `Order` due to the `blocked` role, so `read_object` returns `Forbidden`.)

- [ ] **Step 3: Commit.**
```bash
buck2 run //tools:rustfmt -- src/services/query-api/tests/governed_read.rs
git add -A
git commit -m "test(query-api): deny-override rejects a denied subject at the read gate

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Full sweep, lint, final review, finish branch

**Files:** none (verification + finish).

- [ ] **Step 1: Full test sweep.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...`
Expected: all PASS (memory + postgres acl contract with deny-override, `sqlx-cache-check`, `query-api:governed-read`, and every pre-existing target).

- [ ] **Step 2: Lint.**

Run:
```bash
tools/clippy-all.sh
buck2 run //tools:prek -- run --all-files
```
Expected: all PASS (rustfmt, clippy, `reindeer in sync`, `sqlx`-cache freshness via the test, file checks).

- [ ] **Step 3: Confirm against the spec.** Spot-check: `Effect` enum + `grant(effect)` (Task 1); deny-wins precedence — any Deny → Deny, else Allow, else default-deny — on both adapters (Task 2); `revoke` unchanged/effect-agnostic; fine policy layer and `read_object`/`compile_select` untouched; end-to-end deny at the gate (Task 3). Out-of-scope (deny on fine policies, masking, role hierarchy, validation) not implemented — correct per spec.

- [ ] **Step 4: Finish the branch.** Use superpowers:finishing-a-development-branch (verify tests pass → present options). Branch: `feat/acl-deny-override` (already carries the spec commit).

---

## Self-review notes

- **Spec coverage:** `Effect` enum + `grant` effect param (Task 1) · upsert-by-key, both adapters persist (Task 1, postgres migration `0005` + memory map) · deny-wins `check` on both adapters (Task 2) · `revoke` effect-agnostic, unchanged (Task 1, untouched) · fine policy layer + `read_object`/`compile_select` unchanged (no task touches them) · shared contract deny-override case (Task 2) · query-api end-to-end (Task 3). Non-goals (deny on fine policies, masking, role hierarchy, validation, Type↔Table resolution) are not tasked — correct per spec §"Non-goals".
- **Type consistency:** `Effect { Allow, Deny }`, `grant(&self, &RoleId, Action, PolicyTarget, Effect)` used identically in core/memory/postgres and all call sites; `check` signature unchanged across tasks; memory `grants: HashMap<(String, Action, TargetKey), Effect>`; postgres `effect` column `text` with `'allow'`/`'deny'` via `effect_to_str`.
- **Migration ordering:** `0005_acl_grant_effect.sql` is the next free migration number (existing: 0001–0004); `PgFixture` applies all migrations fresh per test DB, so the `add column ... default 'allow'` needs no backfill.
- **sqlx regen happens twice:** Task 1 (grant insert gains the `effect` column) and Task 2 (`check` becomes a two-flag aggregate). Each task that changes a `query!` must run `tools/sqlx-prepare.sh` and commit `.sqlx/`, or `sqlx-cache-check` fails.
