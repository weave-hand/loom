# Design: role hierarchy (role inheritance)

> **Status:** approved design (2026-06-11). Third slice of **Step 3, full ACL
> semantics** (after deny-override `2026-06-10-acl-deny-override-design.md` and column
> masking `2026-06-11-acl-column-masking-design.md`). Adds role-to-role inheritance.

## Goal

Let a role inherit another role's grants and policies, so `check` and `policies_for`
resolve over a subject's directly-assigned roles **plus everything those roles
transitively inherit**. The inheritance graph is a DAG — cycles are rejected at add
time.

## Scope: one slice of full ACL semantics

Takes ONLY role-to-role inheritance + the closure resolution. Out of scope:

- **Changes to grant effect precedence** — deny-wins (`2026-06-10` slice) is unchanged;
  it simply applies across the larger effective-role set.
- **Per-edge constraints / depth limits / weighting.**
- **A public "effective roles" read** — closure resolution stays internal to the
  adapters; no new read method on the trait.
- **query-api changes** — `read_object` calls `check`/`policies_for` and transparently
  benefits from the expanded resolution; no handler change.

## Model & semantics

- A role→role edge: `add_role_inheritance(role, inherits)` means **`role` gains all of
  `inherits`'s grants and policies**. Transitive: `A inherits B` and `B inherits C`
  ⇒ A's effective roles are `{A, B, C}`.
- **Effective roles** of a subject = the transitive closure of its directly-assigned
  roles (`role_member`) over inheritance edges. `check` and `policies_for` both resolve
  over this closure rather than just the direct roles.
- **Deny-wins is unchanged.** `check` still returns `Deny` if any grant in the effective
  closure is `Deny`, else `Allow` if any is `Allow`, else default-deny. Inheritance only
  enlarges the set of roles whose grants are considered.
- **`policies_for`** returns the union of policies across the effective closure (the
  query API already unions row filters, denied, and masked columns across the returned
  policies — unchanged).
- **DAG invariant.** `add_role_inheritance(role, inherits)` is rejected with `Conflict`
  if the edge would create a cycle — i.e. if `inherits` already transitively inherits
  `role`. The trivial self-edge (`role` == `inherits`) is the degenerate cycle and is
  likewise `Conflict`. Closure traversal additionally carries a visited-set as a
  backstop so it always terminates.
- Both roles must already exist, else `NotFound` (consistent with `assign_role`).
  Adding an edge that already exists is idempotent.

## Trait surface (`src/control-plane/core/src/acl.rs`)

```rust
    /// `role` gains all grants + policies of `inherits` (transitively, through the
    /// effective-role closure used by check/policies_for). Both roles must exist, else
    /// `NotFound`. Rejected with `Conflict` if the edge would form a cycle (including
    /// the self-edge `role == inherits`). Idempotent for an already-present edge.
    async fn add_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()>;
    /// Remove a role-inheritance edge. Idempotent (no-op if absent).
    async fn remove_role_inheritance(&self, role: &RoleId, inherits: &RoleId) -> Result<()>;
```

`check`, `policies_for`, `grant`, `set_policy`, `assign_role` signatures are unchanged —
only the internal role resolution in `check`/`policies_for` expands to the closure. No
new read method is added; effective-role resolution is an adapter internal.

## Adapters (both, behind the shared contract)

### postgres (`src/control-plane/postgres`)
- Migration `0007_acl_role_inherits.sql`:
  ```sql
  create table acl.role_inherits (
      role_id     text not null references acl.role (id) on delete cascade,
      inherits_id text not null references acl.role (id) on delete cascade,
      primary key (role_id, inherits_id)
  );
  ```
- **Effective-role closure via `WITH RECURSIVE`.** `check` and `policies_for` each seed
  the recursion with the subject's direct roles (`role_member`), recursively add
  `role_inherits` edges (`role_id -> inherits_id`), and join the resulting distinct
  effective-role set to `role_grant` (the deny-wins `bool_or` aggregate) / `acl.policy`.
  Postgres `WITH RECURSIVE` deduplicates via `UNION`, so a DAG (or even an accidental
  cycle) terminates.
- **`add_role_inheritance`** validates both roles exist (`NotFound`), then runs a
  recursive-CTE reachability check — does `inherits`'s inheritance closure contain
  `role`? (and is `role == inherits`?) — returning `Conflict` if so, before the
  `insert ... on conflict do nothing`.
- **`remove_role_inheritance`** deletes the `(role_id, inherits_id)` row.
- All via compile-time `query!`; **regenerate the committed `.sqlx` cache**
  (`tools/sqlx-prepare.sh`); keep `sqlx-cache-check` green.

### memory (`src/control-plane/memory`)
- Add `inherits: HashSet<(String, String)>` (`(role, inherits)`) to `AclState`.
- A private `effective_roles(&self, direct: impl Iterator<Item=String>) -> HashSet<String>`
  does a BFS/DFS over `inherits` edges with a visited-set, returning the closure.
  `check` and `policies_for` build their direct-role set from `members`, expand via
  `effective_roles`, and iterate the closure.
- `add_role_inheritance` checks both roles exist, rejects the self-edge and any edge
  whose insertion would make `role` reachable from `inherits` (traverse from `inherits`;
  if `role` is reached, `Conflict`), then inserts.
- `remove_role_inheritance` removes the pair.

## Testing (testkit contract + both adapters)

Extend the ACL contract (`src/control-plane/testkit/src/lib.rs`); it runs on the
in-memory fake and real Postgres:
- **Inherited allow:** `A inherits B`; `B` Allow-granted `(Read, T)`; a subject in `A`
  (only) → `check` = `Allow`.
- **Inherited deny wins:** `B` Deny-granted `(Read, T)`; subject in `A` → `check` =
  `Deny` (deny-wins propagates through inheritance).
- **Inherited policy:** a policy set on `B` → `policies_for(subject-in-A, T)` returns it.
- **Transitive:** `A inherits B`, `B inherits C`; `C` grants `(Read, T)` Allow; subject
  in `A` → `Allow`.
- **Cycle rejected:** `A inherits B` then `B inherits A` → `Conflict`; self-edge
  `A inherits A` → `Conflict`.
- **Remove:** after `remove_role_inheritance(A, B)`, the subject in `A` no longer sees
  `B`'s grant (`Deny`/absent).
- **Unknown role:** `add_role_inheritance` with a nonexistent role → `NotFound`.

## Migration / call-site impact

`0007_acl_role_inherits.sql` is the next migration (existing 0001–0006). The two new
trait methods are additive — every `Acl` implementor (postgres, memory, and the
`StubAcl` test double in `src/services/query-api/tests/http_smoke.rs`) must implement
them; the stub can `unimplemented!()` them (the query-api route never calls them). No
existing call site changes.

## Non-goals (restated)

Effect-precedence changes; per-edge constraints/depth; public effective-roles read;
query-api changes; Type↔Table target resolution; tenancy.
