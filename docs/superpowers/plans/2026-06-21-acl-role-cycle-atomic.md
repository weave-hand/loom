# ACL role-inheritance cycle-check atomicity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `PgControlPlane::add_role_inheritance`'s cycle check and edge insert one atomic unit so two concurrent opposite-edge inserts (`A->B` and `B->A`) can never both commit and form a cycle.

**Architecture:** Wrap the existence checks, the recursive-CTE cycle check, and the `insert` in a single `sqlx::Transaction` guarded by a **transaction-scoped advisory lock** (`pg_advisory_xact_lock`) on a fixed key. All edge inserts serialize on that one lock, so the second of two racing opposite-edge calls observes the first's committed edge and is rejected with `Conflict`. This mirrors the existing per-database catalog lock in `src/control-plane/postgres/src/snapshot.rs` (`CATALOG_LOCK_KEY` + `lock_catalog`). No trait, no migration, and no SQL-string changes — only execution moves from the pool onto a locked transaction.

**Tech Stack:** Rust 2024, sqlx 0.9 compile-time `query!`/`query_scalar!` macros, Postgres, buck2 (`loom_fixture_test` targets), tokio multi-thread test runtime.

## Global Constraints

- **DAG invariant (verbatim from `2026-06-11-acl-role-hierarchy-design.md`):** `add_role_inheritance(role, inherits)` is rejected with `Conflict` if the edge would create a cycle — i.e. if `inherits` already transitively inherits `role`; the self-edge `role == inherits` is the degenerate cycle and is likewise `Conflict`. Both roles must already exist, else `NotFound`. Adding an edge that already exists is idempotent. **This task preserves every one of these behaviors unchanged** — it only makes the check+insert atomic.
- **Trait signature is unchanged:** `async fn add_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()>`.
- **Tests are `rust_test` integration targets only** — NO inline `#[cfg(test)]`. New fixture tests (boot real Postgres) MUST use the `loom_fixture_test` macro in `BUCK`, never a bare `rust_test`, or they route to remote execution and fail as root.
- **Never pipe `buck2 test` through `tail`/`head`** — redirect to a file and grep it.
- **Run `rustfmt` on every changed `.rs` file before committing** (`buck2 run //tools:rustfmt -- <files>`); the prek `lint` hook fails CI on any formatting diff.
- **Commit messages** follow Conventional Commits and end with the `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>` trailer.

---

### Task 1: Make `add_role_inheritance` atomic under concurrency

**Files:**
- Create: `src/control-plane/postgres/tests/acl_inheritance_concurrency.rs`
- Modify: `src/control-plane/postgres/BUCK` (add a `loom_fixture_test` target for the new test)
- Modify: `src/control-plane/postgres/src/acl.rs:88-139` (add lock constant; move body onto an advisory-locked transaction)

**Interfaces:**
- Consumes:
  - `control_plane_postgres::fixture::PgFixture` — `PgFixture::start() -> PgFixture`, `async fn fresh_control_plane(&self) -> PgControlPlane`.
  - `PgControlPlane` — `#[derive(Clone)]` (wraps a cheap-to-clone `PgPool`); implements the `Acl` trait.
  - `control_plane_core::{Acl, RoleId, ControlPlaneError}` — `RoleId(pub String)`; `ControlPlaneError::Conflict(String)`; `Acl::define_role(&self, &RoleId) -> Result<()>` and `Acl::add_role_inheritance(&self, &RoleId, &RoleId) -> Result<()>`.
  - Existing module-private helper `backend` (in `crate::`, already imported in `acl.rs`) mapping `sqlx::Error -> ControlPlaneError`.
- Produces: no new public API. Adds a module-private `const ROLE_INHERITS_LOCK_KEY: i64` in `acl.rs`.

- [ ] **Step 1: Write the failing concurrency test**

Create `src/control-plane/postgres/tests/acl_inheritance_concurrency.rs`:

```rust
//! `add_role_inheritance` must keep the inheritance graph acyclic even when two
//! opposite-direction edges race. The cycle check + insert run inside one
//! advisory-locked transaction, so of two concurrent `A->B` / `B->A` calls exactly
//! one commits and the other is rejected with `Conflict` — they can never both
//! commit and form a cycle. Regression guard for iss-acl-role-cycle-atomic.
use control_plane_core::{Acl, ControlPlaneError, RoleId};
use control_plane_postgres::fixture::PgFixture;

fn rid(s: &str) -> RoleId {
    RoleId(s.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_opposite_edges_cannot_both_commit() {
    let fx = PgFixture::start();
    let cp = fx.fresh_control_plane().await;

    // Many fresh role pairs: pre-fix the check/insert race fires intermittently
    // (some iteration sees both edges commit -> a cycle -> assert fails). Post-fix
    // the advisory lock serializes the pair, so every iteration is (1 ok, 1 conflict).
    const ITERS: usize = 25;
    for i in 0..ITERS {
        let a = rid(&format!("a_{i}"));
        let b = rid(&format!("b_{i}"));
        cp.define_role(&a).await.expect("define a");
        cp.define_role(&b).await.expect("define b");

        let cp1 = cp.clone();
        let cp2 = cp.clone();
        let (a1, b1) = (a.clone(), b.clone());
        let (a2, b2) = (a.clone(), b.clone());
        let h1 = tokio::spawn(async move { cp1.add_role_inheritance(&a1, &b1).await });
        let h2 = tokio::spawn(async move { cp2.add_role_inheritance(&b2, &a2).await });
        let r1 = h1.await.expect("join 1");
        let r2 = h2.await.expect("join 2");

        let mut oks = 0;
        let mut conflicts = 0;
        for r in [&r1, &r2] {
            match r {
                Ok(()) => oks += 1,
                Err(ControlPlaneError::Conflict(_)) => conflicts += 1,
                Err(e) => panic!("iter {i}: unexpected error {e:?}"),
            }
        }
        assert_eq!(
            (oks, conflicts),
            (1, 1),
            "iter {i}: exactly one edge commits and one is rejected as a cycle; \
             got r1={r1:?} r2={r2:?}"
        );
    }
}
```

- [ ] **Step 2: Wire the BUCK target**

In `src/control-plane/postgres/BUCK`, add a new `loom_fixture_test` target (place it alongside the other ACL/concurrency targets, e.g. right after the existing `name = "acl"` target near line 214). It needs `core` for the `Acl` trait + `RoleId` + `ControlPlaneError`, and `tokio` for the multi-thread runtime:

```python
loom_fixture_test(
    name = "acl-inheritance-concurrency",
    crate = "acl_inheritance_concurrency",
    srcs = ["tests/acl_inheritance_concurrency.rs"],
    crate_root = "tests/acl_inheritance_concurrency.rs",
    deps = [":postgres", "//src/control-plane/core:core", "//third-party:tokio"],
)
```

- [ ] **Step 3: Run the test to verify it fails (RED)**

Run:
```bash
buck2 test //src/control-plane/postgres:acl-inheritance-concurrency > /tmp/acl_red.log 2>&1; grep -E "Tests finished|FAIL|PASS|panicked|exactly one edge" /tmp/acl_red.log
```
Expected: **FAIL** — at least one iteration reports `(oks, conflicts)` of `(2, 0)` (both edges committed → a cycle slipped through), tripping the `assert_eq!`. The race is timing-dependent, so if a run happens to pass, re-run a few times to observe the failure; the deterministic GREEN in Step 6 is the real gate. Do **not** proceed to the fix until you have seen it fail at least once.

- [ ] **Step 4: Add the advisory-lock constant**

In `src/control-plane/postgres/src/acl.rs`, immediately after the `use crate::{...}` import line (currently line 7), add:

```rust
/// Fixed advisory-lock key serializing role-inheritance edge inserts within one
/// database, making the cycle check + insert in `add_role_inheritance` atomic
/// against concurrent opposite-edge writes. Arbitrary but stable (ASCII "acl_inhr"),
/// and distinct from `snapshot.rs`'s catalog lock so the two never contend.
const ROLE_INHERITS_LOCK_KEY: i64 = 0x6163_6c5f_696e_6872u64 as i64;
```

- [ ] **Step 5: Rewrite `add_role_inheritance` onto an advisory-locked transaction**

Replace the entire current method body (`src/control-plane/postgres/src/acl.rs:88-139`) with the version below. Changes: begin a transaction, take the xact advisory lock first, run every existing query on `&mut *tx` instead of `&self.pool`, and `commit()` at the end. The SQL strings are unchanged. Early `return Err(...)` (and any `?`) drops the owned `tx`, which rolls back and releases the lock automatically.

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn add_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()> {
        // The cycle check and the edge insert must be one atomic unit: two concurrent
        // calls inserting opposite edges (A->B and B->A) would each pass an independent
        // check and both insert, forming a cycle. Serialize all edge inserts on a
        // transaction-scoped advisory lock (auto-released on commit/rollback, including
        // drop-on-panic, so it can never leak onto a pooled connection). The loser then
        // observes the winner's committed edge and is rejected with Conflict. Mirrors
        // the per-database catalog lock in snapshot.rs.
        let mut tx = self.pool.begin().await.map_err(backend)?;
        sqlx::query!("select pg_advisory_xact_lock($1)", ROLE_INHERITS_LOCK_KEY)
            .execute(&mut *tx)
            .await
            .map_err(backend)?;

        for id in [&role.0, &inherits.0] {
            let exists =
                sqlx::query_scalar!("select exists (select 1 from acl.role where id = $1)", id,)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(backend)?
                    .unwrap_or(false);
            if !exists {
                return Err(ControlPlaneError::NotFound(format!("role {id}")));
            }
        }
        // role -> inherits creates a cycle iff `role` is already reachable from
        // `inherits` (closure of `inherits` includes itself -> catches self-edge).
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
        .fetch_one(&mut *tx)
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
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(())
    }
```

- [ ] **Step 6: Run the concurrency test to verify it passes (GREEN)**

Run:
```bash
buck2 test //src/control-plane/postgres:acl-inheritance-concurrency > /tmp/acl_green.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/acl_green.log
```
Expected: **PASS** — all 25 iterations are `(1, 1)`. Run it 2-3 times to confirm it is now deterministic (the lock removes the race).

- [ ] **Step 7: Run the existing ACL contract to verify no behavior regression**

The single-writer semantics (NotFound / Conflict / idempotent / transitive resolution) are covered by `control_plane_testkit::acl_contract`, exercised by the `acl` target. It must stay green.

Run:
```bash
buck2 test //src/control-plane/postgres:acl > /tmp/acl_contract.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/acl_contract.log
```
Expected: **PASS**.

- [ ] **Step 8: Refresh and verify the sqlx cache**

No SQL strings changed (`pg_advisory_xact_lock($1)` is byte-identical to the one already cached for `snapshot.rs`, and the existence/cycle/insert strings are untouched), so this should produce **no diff** — but run the generator and the freshness test to be certain.

Run:
```bash
bash tools/sqlx-prepare.sh > /tmp/sqlx_prep.log 2>&1; git status --porcelain src/control-plane/postgres/.sqlx
buck2 test //src/control-plane/postgres:sqlx-cache-check > /tmp/sqlx_check.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/sqlx_check.log
```
Expected: `git status` shows no changes under `.sqlx/` (if it does, stage them); `sqlx-cache-check` is **PASS**.

- [ ] **Step 9: Format and lint the changed Rust files**

Run (rustfmt first — a formatting diff fails the `lint` CI job; clippy second):
```bash
buck2 run //tools:rustfmt -- src/control-plane/postgres/src/acl.rs src/control-plane/postgres/tests/acl_inheritance_concurrency.rs
buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' > /tmp/clippy.log 2>&1; echo "exit=$?"; cat /tmp/clippy.log
```
Expected: rustfmt makes no further changes (or apply whatever it changes); clippy is clean (`exit=0`, empty `clippy.txt`).

- [ ] **Step 10: Commit**

```bash
git add src/control-plane/postgres/src/acl.rs \
        src/control-plane/postgres/tests/acl_inheritance_concurrency.rs \
        src/control-plane/postgres/BUCK \
        src/control-plane/postgres/.sqlx
git commit -m "$(cat <<'EOF'
fix(acl): make role-inheritance cycle check atomic under concurrency

Wrap add_role_inheritance's existence check, recursive-CTE cycle check, and
edge insert in one transaction guarded by a transaction-scoped advisory lock
(pg_advisory_xact_lock), mirroring snapshot.rs's catalog lock. Concurrent
opposite-edge inserts now serialize, so the loser observes the winner's
committed edge and is rejected with Conflict instead of both committing and
forming a cycle. Adds a fixture-backed concurrency regression test.

Closes iss-acl-role-cycle-atomic.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Close the register item

**Files:**
- Modify: `docs/ISSUES.md` (the `iss-acl-role-cycle-atomic` entry)

> Run this via the `loom-docs-update` skill as part of finishing the branch (it stages the register change alongside the code and validates the grammar). The content below is the exact target state.

- [ ] **Step 1: Flip the entry to fixed**

In `docs/ISSUES.md`, change the `iss-acl-role-cycle-atomic` list item from:

```markdown
- [ ] **Role-inheritance cycle check not atomic** `{#iss-acl-role-cycle-atomic area:acl status:open from:acl-role-hierarchy pr:- spec:2026-06-11-acl-role-hierarchy-design}`
  `add_role_inheritance` does the cycle check and the edge insert as two round-trips, so two concurrent calls inserting opposite edges of a cycle could both pass. Safe single-writer; wrap in a SERIALIZABLE tx (or lock) if concurrent edge writes land.
```

to (checkbox `[x]`, `status:fixed`, real `pr:#N` — substitute the PR number opened in the finish step):

```markdown
- [x] **Role-inheritance cycle check not atomic** `{#iss-acl-role-cycle-atomic area:acl status:fixed from:acl-role-hierarchy pr:#NNN spec:2026-06-11-acl-role-hierarchy-design}`
  Fixed (PR #NNN): `add_role_inheritance` now runs its existence check, recursive-CTE cycle check, and edge insert in one transaction guarded by a transaction-scoped advisory lock (`pg_advisory_xact_lock`, the same pattern as `snapshot.rs`'s catalog lock). Concurrent opposite-edge inserts serialize on that lock, so the loser observes the winner's committed edge and is rejected with `Conflict` rather than both committing and forming a cycle. A fixture-backed concurrency test (`acl-inheritance-concurrency`) asserts that two racing `A->B`/`B->A` calls yield exactly one commit and one conflict.
```

- [ ] **Step 2: Validate the registers**

Run:
```bash
bash tools/docs.sh validate
```
Expected: no errors.

- [ ] **Step 3: Commit (if not already staged by `loom-docs-update`)**

```bash
git add docs/ISSUES.md
git commit -m "docs(issues): close iss-acl-role-cycle-atomic (atomic cycle check)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Notes for the implementer

- **Why an advisory lock and not SERIALIZABLE?** The issue offers "a SERIALIZABLE tx (or lock)". An advisory lock is chosen because it is the established pattern in this adapter (`snapshot.rs::lock_catalog`, `iceberg_flush.rs`), needs no serialization-failure retry loop, and serializes only the rare role-inheritance writes. A transaction-scoped lock (`pg_advisory_xact_lock`, not the session variant) auto-releases on commit/rollback/drop, so it cannot leak onto a pooled connection.
- **Locking only `add_role_inheritance` is sufficient.** Removing an edge can only reduce reachability, never create a cycle, so `remove_role_inheritance` needs no lock; only inserts can introduce a cycle, and they all contend on the one key.
- **The memory adapter already satisfies this** — `MemoryControlPlane::add_role_inheritance` does the check+insert under a single `Mutex` guard, so it is atomic by construction and needs no change. The contract test (`acl_contract`) is adapter-agnostic and stays green for both.
