# ACL Role Hierarchy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A role can inherit another role's grants + policies; `check`/`policies_for` resolve over a subject's effective-role closure (direct roles + everything they transitively inherit). The inheritance graph is a DAG — cycles rejected at add time.

**Architecture:** A new role→role edge (`acl.role_inherits` table / a memory `HashSet`). `add_role_inheritance(role, inherits)` makes `role` gain `inherits`'s permissions, rejecting (`Conflict`) any edge that would form a cycle (reachability check). `check`/`policies_for` expand the subject's direct roles to the transitive closure — postgres via `WITH RECURSIVE` CTEs, memory via BFS over the edge set. Deny-wins and the query-api are unchanged.

**Tech Stack:** Rust, buck2, sqlx compile-time queries (postgres, incl. recursive CTEs), hermetic Postgres via `PgFixture`, shared `control-plane-testkit` contract on both adapters.

**Reference spec:** `docs/superpowers/specs/2026-06-11-acl-role-hierarchy-design.md`.

**Conventions (do not deviate):**
- Tests: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...`. **Do NOT pipe `buck2 test` through `tail`** (it can stall) — redirect to a file: `buck2 test ... > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL|error\[|panicked" /tmp/t.log`.
- **rustfmt is a check-only hook** — run `buck2 run //tools:rustfmt -- <files>` before each commit or it silently aborts; confirm the SHA changed.
- After changing any `query!` or a migration, run `tools/sqlx-prepare.sh` and commit `.sqlx/`; `//src/control-plane/postgres:sqlx-cache-check` gates it.
- Lint: `tools/clippy-all.sh` + `buck2 run //tools:prek -- run --all-files`. NO `--no-verify`.
- Conventional Commits; end each commit with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Never weaken a test assertion.

---

## File Structure

- `src/control-plane/core/src/acl.rs` — **Modify:** add `add_role_inheritance` / `remove_role_inheritance` to the `Acl` trait.
- `src/control-plane/postgres/migrations/0007_acl_role_inherits.sql` — **Create:** the `role_inherits` table.
- `src/control-plane/postgres/src/acl.rs` — **Modify:** implement the two methods (Task 1); rewrite `check`/`policies_for` to resolve the closure via `WITH RECURSIVE` (Task 2).
- `src/control-plane/memory/src/acl.rs` — **Modify:** `inherits` edge set + `effective_roles`/`reaches` helpers; the two methods (Task 1); `check`/`policies_for` over the closure (Task 2).
- `src/control-plane/postgres/.sqlx/` — **Regenerate** (Task 1 add/reachability queries; Task 2 closure CTEs).
- `src/control-plane/testkit/src/lib.rs` — **Modify:** contract cases (edge invariants Task 1; inheritance resolution Task 2).
- `src/services/query-api/tests/http_smoke.rs` — **Modify:** `StubAcl` implements the two new methods (`unimplemented!()`).

---

## Task 1: Edge management — trait methods, table, add (cycle-reject) / remove

Adds the role→role edge and its add/remove methods (with cycle rejection), but does NOT yet change `check`/`policies_for` resolution (Task 2). Tests cover the invariants that don't depend on resolution: NotFound, self-edge + cycle `Conflict`, idempotent add, remove.

**Files:**
- Modify: `src/control-plane/core/src/acl.rs`
- Create: `src/control-plane/postgres/migrations/0007_acl_role_inherits.sql`
- Modify: `src/control-plane/postgres/src/acl.rs`, `src/control-plane/memory/src/acl.rs`, `src/services/query-api/tests/http_smoke.rs`, `src/control-plane/testkit/src/lib.rs`
- Regenerate: `src/control-plane/postgres/.sqlx/`

- [ ] **Step 1: Add the trait methods.**

In `src/control-plane/core/src/acl.rs`, in the `Acl` trait (after `unassign_role`, around line 124), add:
```rust
    /// `role` gains all grants + policies of `inherits` (transitively, via the
    /// effective-role closure used by `check`/`policies_for`). Both roles must exist,
    /// else `NotFound`. Rejected with `Conflict` if the edge would form a cycle
    /// (including the self-edge `role == inherits`). Idempotent for an existing edge.
    async fn add_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()>;
    /// Remove a role-inheritance edge. Idempotent (no-op if absent).
    async fn remove_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()>;
```

- [ ] **Step 2: Migration — the edge table.**

Create `src/control-plane/postgres/migrations/0007_acl_role_inherits.sql`:
```sql
-- Role hierarchy: `role_id` inherits all grants + policies of `inherits_id`
-- (transitively). check()/policies_for() resolve over the effective-role closure.
-- The graph is kept acyclic by add_role_inheritance (cycle -> Conflict).
create table acl.role_inherits (
    role_id     text not null references acl.role (id) on delete cascade,
    inherits_id text not null references acl.role (id) on delete cascade,
    primary key (role_id, inherits_id)
);
```

- [ ] **Step 3: Postgres — implement add/remove.**

In `src/control-plane/postgres/src/acl.rs`, add the two methods to the `impl Acl for PgControlPlane` block (e.g. after `unassign_role`):
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn add_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()> {
        for id in [&role.0, &inherits.0] {
            let exists = sqlx::query_scalar!(
                "select exists (select 1 from acl.role where id = $1)",
                id,
            )
            .fetch_one(&self.pool)
            .await
            .map_err(backend)?
            .unwrap_or(false);
            if !exists {
                return Err(ControlPlaneError::NotFound(format!("role {id}")));
            }
        }
        // Would adding role -> inherits create a cycle? It does iff `role` is already
        // reachable from `inherits` (the closure of `inherits`, which includes
        // `inherits` itself -> also catches the self-edge role == inherits).
        let creates_cycle = sqlx::query_scalar!(
            "with recursive clo(role_id) as ( \
                 select $1::text \
                 union \
                 select ri.inherits_id from acl.role_inherits ri \
                   join clo on ri.role_id = clo.role_id \
             ) \
             select exists (select 1 from clo where role_id = $2)",
            &inherits.0,
            &role.0,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?
        .unwrap_or(false);
        if creates_cycle {
            return Err(ControlPlaneError::Conflict(format!(
                "role inheritance {} -> {} would create a cycle",
                role.0, inherits.0
            )));
        }
        sqlx::query!(
            "insert into acl.role_inherits (role_id, inherits_id) values ($1, $2) \
             on conflict do nothing",
            &role.0,
            &inherits.0,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn remove_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()> {
        sqlx::query!(
            "delete from acl.role_inherits where role_id = $1 and inherits_id = $2",
            &role.0,
            &inherits.0,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
        Ok(())
    }
```
Leave `check`/`policies_for` unchanged this task.

- [ ] **Step 4: Memory — edge set + add/remove + a reachability helper.**

In `src/control-plane/memory/src/acl.rs`:
- Add to `AclState` (after `policies`):
```rust
    inherits: HashSet<(String, String)>, // (role, inherits): role gains inherits's perms
```
- Add a free function near the top of the file (after the `target_key` helper):
```rust
/// True if `target` is reachable from `start` following role->inherits edges
/// (i.e. `start` transitively inherits `target`). Visited-set guards cycles.
fn reaches(edges: &HashSet<(String, String)>, start: &str, target: &str) -> bool {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut stack = vec![start];
    while let Some(r) = stack.pop() {
        if r == target {
            return true;
        }
        if seen.insert(r) {
            for (_, b) in edges.iter().filter(|(a, _)| a == r) {
                stack.push(b);
            }
        }
    }
    false
}
```
- Add the two methods to `impl Acl for MemoryControlPlane` (after `unassign_role`):
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn add_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()> {
        let mut acl = self.acl.lock().unwrap();
        if !acl.roles.contains(&role.0) {
            return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
        }
        if !acl.roles.contains(&inherits.0) {
            return Err(ControlPlaneError::NotFound(format!("role {}", inherits.0)));
        }
        // role -> inherits creates a cycle iff `role` is already reachable from
        // `inherits` (covers the self-edge role == inherits).
        if role.0 == inherits.0 || reaches(&acl.inherits, &inherits.0, &role.0) {
            return Err(ControlPlaneError::Conflict(format!(
                "role inheritance {} -> {} would create a cycle",
                role.0, inherits.0
            )));
        }
        acl.inherits.insert((role.0.clone(), inherits.0.clone()));
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn remove_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()> {
        self.acl
            .lock()
            .unwrap()
            .inherits
            .remove(&(role.0.clone(), inherits.0.clone()));
        Ok(())
    }
```
(`HashSet` is already imported. Leave `check`/`policies_for` unchanged this task.)

- [ ] **Step 5: StubAcl in http_smoke — implement the new methods.**

In `src/services/query-api/tests/http_smoke.rs`, the `StubAcl` `impl Acl` block: add the two methods (the route never calls them):
```rust
    async fn add_role_inheritance(&self, _role: &RoleId, _inherits: &RoleId) -> Result<()> {
        unimplemented!()
    }
    async fn remove_role_inheritance(&self, _role: &RoleId, _inherits: &RoleId) -> Result<()> {
        unimplemented!()
    }
```

- [ ] **Step 6: Write the failing contract assertions (edge invariants only).**

In `src/control-plane/testkit/src/lib.rs`, in the ACL contract, append (after the existing assertions — read the function to find the end and confirm helpers `rid`, `define_role`). Use FRESH roles so nothing else is disturbed:
```rust
    // --- role inheritance: edge invariants ---
    a.define_role(&rid("h_parent")).await.expect("define h_parent");
    a.define_role(&rid("h_child")).await.expect("define h_child");
    // unknown role -> NotFound
    assert!(matches!(
        a.add_role_inheritance(&rid("h_parent"), &rid("h_nonexistent")).await,
        Err(ControlPlaneError::NotFound(_)),
    ));
    // self-edge -> Conflict
    assert!(matches!(
        a.add_role_inheritance(&rid("h_parent"), &rid("h_parent")).await,
        Err(ControlPlaneError::Conflict(_)),
    ));
    // valid edge, idempotent
    a.add_role_inheritance(&rid("h_parent"), &rid("h_child")).await.expect("add edge");
    a.add_role_inheritance(&rid("h_parent"), &rid("h_child")).await.expect("add edge idempotent");
    // reverse edge would cycle -> Conflict
    assert!(matches!(
        a.add_role_inheritance(&rid("h_child"), &rid("h_parent")).await,
        Err(ControlPlaneError::Conflict(_)),
    ));
    // remove is idempotent
    a.remove_role_inheritance(&rid("h_parent"), &rid("h_child")).await.expect("remove edge");
    a.remove_role_inheritance(&rid("h_parent"), &rid("h_child")).await.expect("remove idempotent");
```
Ensure `ControlPlaneError` is imported in the testkit (it's used elsewhere in the contract; if not, add it to the `use control_plane_core::{...}`).

- [ ] **Step 7: Regenerate sqlx + run.**

Run: `tools/sqlx-prepare.sh` (the new add-reachability CTE + insert + delete queries enter the cache).
Then: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/... > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all PASS (incl. the new edge-invariant assertions on both adapters; `check`/`policies_for` unchanged so all prior behavior holds).

- [ ] **Step 8: Commit.**
```bash
buck2 run //tools:rustfmt -- src/control-plane/core/src/acl.rs src/control-plane/postgres/src/acl.rs src/control-plane/memory/src/acl.rs src/services/query-api/tests/http_smoke.rs src/control-plane/testkit/src/lib.rs
git add -A
git commit -m "feat(acl): role-inheritance edges (add cycle-reject / remove)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Closure resolution in `check` + `policies_for`

Now `check` and `policies_for` resolve over the effective-role closure on both adapters, so inherited grants/policies take effect. The shared contract proves it.

**Files:**
- Modify: `src/control-plane/postgres/src/acl.rs`, `src/control-plane/memory/src/acl.rs`, `src/control-plane/testkit/src/lib.rs`
- Regenerate: `src/control-plane/postgres/.sqlx/`

- [ ] **Step 1: Write the failing inheritance-resolution contract cases.**

In `src/control-plane/testkit/src/lib.rs`, append after the Task-1 edge-invariant block (fresh subjects/roles):
```rust
    // --- role inheritance: resolution through check/policies_for ---
    a.define_subject(&sid("h_user")).await.expect("define h_user");
    a.define_role(&rid("senior")).await.expect("define senior");
    a.define_role(&rid("junior")).await.expect("define junior");
    a.define_role(&rid("base")).await.expect("define base");
    a.assign_role(&sid("h_user"), &rid("senior")).await.expect("assign senior");
    a.add_role_inheritance(&rid("senior"), &rid("junior")).await.expect("senior inherits junior");
    a.add_role_inheritance(&rid("junior"), &rid("base")).await.expect("junior inherits base");

    // Inherited allow: junior grants Read Widget; h_user (in senior) sees it.
    a.grant(&rid("junior"), Action::Read, ttype("Widget"), Effect::Allow).await.expect("junior allow");
    assert_eq!(
        a.check(&sid("h_user"), Action::Read, &ttype("Widget")).await.expect("check inherited allow"),
        Decision::Allow,
        "senior inherits junior's allow grant",
    );
    // Transitive: base grants Read Gadget; h_user (senior -> junior -> base) sees it.
    a.grant(&rid("base"), Action::Read, ttype("Gadget"), Effect::Allow).await.expect("base allow");
    assert_eq!(
        a.check(&sid("h_user"), Action::Read, &ttype("Gadget")).await.expect("check transitive allow"),
        Decision::Allow,
        "inheritance is transitive (senior -> junior -> base)",
    );
    // Inherited deny wins: base denies Read Widget -> overrides junior's allow.
    a.grant(&rid("base"), Action::Read, ttype("Widget"), Effect::Deny).await.expect("base deny");
    assert_eq!(
        a.check(&sid("h_user"), Action::Read, &ttype("Widget")).await.expect("check inherited deny"),
        Decision::Deny,
        "an inherited Deny wins over an inherited Allow",
    );
    // Inherited policy: a policy on junior is returned for h_user.
    a.set_policy(&rid("junior"), Policy {
        target: ttype("Widget"),
        row_filter: None,
        deny_columns: vec!["cost".into()],
        mask_columns: vec![],
    }).await.expect("junior policy");
    let inh_pols = a
        .policies_for(&sid("h_user"), &ttype("Widget"), PageReq::unbounded())
        .await
        .expect("inherited policies_for");
    assert!(
        inh_pols.items.iter().any(|p| p.deny_columns == vec!["cost".to_string()]),
        "policies_for returns an inherited policy",
    );
    // Remove drops the inheritance: senior no longer inherits junior -> Gadget gone.
    a.remove_role_inheritance(&rid("senior"), &rid("junior")).await.expect("remove senior->junior");
    assert_eq!(
        a.check(&sid("h_user"), Action::Read, &ttype("Gadget")).await.expect("check after remove"),
        Decision::Deny,
        "removing the edge drops transitively-inherited grants",
    );
```

- [ ] **Step 2: Run — verify it fails.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:acl //src/control-plane/postgres:acl > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: FAIL — `check`/`policies_for` only consult direct roles, so the inherited-allow assertion fails (`h_user` has no direct grant).

- [ ] **Step 3: Memory — resolve over the closure.**

In `src/control-plane/memory/src/acl.rs`, add an `effective_roles` free function (near `reaches`):
```rust
/// The transitive closure of `direct` over role->inherits edges (includes `direct`).
fn effective_roles(
    edges: &HashSet<(String, String)>,
    direct: impl IntoIterator<Item = String>,
) -> HashSet<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut stack: Vec<String> = direct.into_iter().collect();
    while let Some(r) = stack.pop() {
        if seen.insert(r.clone()) {
            for (_, b) in edges.iter().filter(|(a, _)| a == &r) {
                if !seen.contains(b) {
                    stack.push(b.clone());
                }
            }
        }
    }
    seen
}
```
Rewrite `check` to iterate the closure:
```rust
    async fn check(
        &self,
        subject: &SubjectId,
        action: Action,
        target: &PolicyTarget,
    ) -> Result<Decision> {
        let acl = self.acl.lock().unwrap();
        let tk = target_key(target);
        let direct = acl
            .members
            .iter()
            .filter(|(s, _)| s == &subject.0)
            .map(|(_, r)| r.clone());
        let effective = effective_roles(&acl.inherits, direct);
        let mut saw_allow = false;
        for role in &effective {
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
Rewrite `policies_for` to gather across the closure:
```rust
    async fn policies_for(
        &self,
        subject: &SubjectId,
        target: &PolicyTarget,
        _page: PageReq,
    ) -> Result<Page<Policy>> {
        let acl = self.acl.lock().unwrap();
        let tk = target_key(target);
        let direct = acl
            .members
            .iter()
            .filter(|(s, _)| s == &subject.0)
            .map(|(_, r)| r.clone());
        let effective = effective_roles(&acl.inherits, direct);
        Ok(Page::from_full(
            effective
                .iter()
                .filter_map(|role| acl.policies.get(&(role.clone(), tk.clone())).cloned())
                .collect(),
        ))
    }
```

- [ ] **Step 4: Postgres — resolve via `WITH RECURSIVE`.**

In `src/control-plane/postgres/src/acl.rs`, rewrite `check`'s query to seed the recursion from the subject's direct roles and expand over `role_inherits`:
```rust
        let (kind, a, b) = target_cols(target);
        let row = sqlx::query!(
            "with recursive eff(role_id) as ( \
                 select role_id from acl.role_member where subject_id = $1 \
                 union \
                 select ri.inherits_id from acl.role_inherits ri \
                   join eff on ri.role_id = eff.role_id \
             ) \
             select bool_or(g.effect = 'deny') as has_deny, \
                    bool_or(g.effect = 'allow') as has_allow \
             from eff join acl.role_grant g on g.role_id = eff.role_id \
             where g.action = $2 and g.target_kind = $3 \
               and g.target_a = $4 and g.target_b = $5",
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
And `policies_for`'s query similarly:
```rust
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
             where p.target_kind = $2 and p.target_a = $3 and p.target_b = $4",
            &subject.0,
            kind,
            &a,
            &b,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
```
(Leave the row→`Policy` reconstruction loop below unchanged.)

- [ ] **Step 5: Regenerate sqlx + run.**

Run: `tools/sqlx-prepare.sh` (the `check`/`policies_for` recursive CTEs update the cache).
Then: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:acl //src/control-plane/postgres:acl > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS on both. Then the broader sweep `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/... > /tmp/s.log 2>&1; grep -E "Tests finished|FAIL" /tmp/s.log` → all PASS (existing direct-role behavior is preserved: a subject with no inheritance edges has effective == direct).

- [ ] **Step 6: Commit.**
```bash
buck2 run //tools:rustfmt -- src/control-plane/postgres/src/acl.rs src/control-plane/memory/src/acl.rs src/control-plane/testkit/src/lib.rs
git add -A
git commit -m "feat(acl): check/policies_for resolve the effective-role closure

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Full sweep, lint, final review, finish branch

**Files:** none (verification + finish).

- [ ] **Step 1: Full sweep.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/... > /tmp/sweep.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL" /tmp/sweep.log`
Expected: all PASS (acl contract with edge invariants + inheritance resolution on both adapters, sqlx-cache-check, and every pre-existing target).

- [ ] **Step 2: Lint.**

Run:
```bash
tools/clippy-all.sh > /tmp/clippy.log 2>&1; echo "clippy warnings: $(grep -cE 'warning|error' /tmp/clippy.log)"
buck2 run //tools:prek -- run --all-files
```
Expected: clippy 0 warnings; prek all PASS.

- [ ] **Step 3: Confirm against the spec.** Spot-check: `add_role_inheritance`/`remove_role_inheritance` (Task 1); cycle + self-edge → `Conflict`, unknown role → `NotFound`, idempotent (Task 1 contract); closure resolution in BOTH `check` and `policies_for` on both adapters — inherited allow, transitive, inherited-deny-wins, inherited policy, remove-drops (Task 2 contract); deny-wins unchanged; query-api untouched. Out-of-scope (effect precedence changes, public effective-roles read, per-edge constraints) not implemented — correct.

- [ ] **Step 4: Finish the branch.** Use superpowers:finishing-a-development-branch (verify tests pass → present options). Branch: `feat/acl-role-hierarchy` (already carries the spec commit).

---

## Self-review notes

- **Spec coverage:** trait methods (Task 1) · `role_inherits` table migration 0007 + memory edge set (Task 1) · add cycle/self reject via reachability + NotFound + idempotent (Task 1, both adapters + contract) · remove idempotent (Task 1) · closure resolution in `check` (recursive CTE / BFS) + `policies_for` (Task 2, both adapters) · inherited allow / transitive / inherited-deny-wins / inherited policy / remove-drops (Task 2 contract) · StubAcl methods (Task 1) · deny-wins + query-api unchanged (no task touches them). Non-goals (effect precedence, per-edge constraints, public effective-roles read, query-api) untasked — correct.
- **Type consistency:** `add_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()>` and `remove_role_inheritance(...)` identical across core/postgres/memory/StubAcl; memory `inherits: HashSet<(String, String)>` with `(role, inherits)` ordering used by `reaches`, `effective_roles`, add, remove consistently; postgres `acl.role_inherits(role_id, inherits_id)`; the `WITH RECURSIVE eff` seed (`role_member`) + edge step (`ri.role_id = eff.role_id` → `ri.inherits_id`) matches the memory BFS direction (follow `role -> inherits`).
- **Cycle semantics:** `add(role, inherits)` rejects iff `role` is reachable from `inherits` (postgres: `clo` seeded at `inherits` contains `role`; memory: `role == inherits || reaches(edges, inherits, role)`). The postgres `clo` seed includes `inherits`, so `role == inherits` is caught without a separate check; the memory path adds the explicit `role.0 == inherits.0` for clarity/short-circuit. Both correct.
- **sqlx regen twice:** Task 1 (add reachability CTE + insert + delete) and Task 2 (check/policies_for recursive CTEs). Each task runs `tools/sqlx-prepare.sh` and commits `.sqlx/`, or `sqlx-cache-check` fails.
- **No regression:** a subject with no inheritance edges has `effective == direct`, so `check`/`policies_for` behave exactly as before — the existing direct-role contract assertions (deny-override, masking, etc.) stay green.
- **Contract placement:** all new assertions use fresh role/subject names (`h_*`, `senior`/`junior`/`base`, `Widget`/`Gadget`/`Invoice`) so they don't disturb earlier assertions; append at the end of the contract fn.
