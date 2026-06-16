# Design: action-scoped policies (slice 1 of fine-grained write governance)

> **Status:** approved design (2026-06-16). The control-plane enabler for fine-grained write
> governance. Today `acl.policy` (the row-filter + column-deny/mask store powering `policies_for`)
> keys on `(role, target)` — it is **action-agnostic**, so one policy is shared by read and write.
> Grants (`role_grant`, powering `check`) are already action-scoped (`'read' | 'write'`). This slice
> brings policies to parity: a policy is keyed by `(role, action, target)`, so a subject can hold
> independent read and write scopes on a type. **Slice 2** (a separate spec) consumes the write
> policy in `run_action` to enforce deny-write-column + row-filter-on-insert; this slice only adds
> and stores the action dimension (reads stay behaviorally identical).

## Goal

Make `acl.policy` action-scoped so `set_policy`/`clear_policy`/`policies_for` carry an `Action`
(`Read`/`Write`), and a `Read` policy and a `Write` policy for the same `(role, target)` are
independent rows. The read path becomes explicitly `Read`-scoped (behavior unchanged). This unblocks
slice 2, where a `Write` policy's `row_filter` constrains which rows an action may insert and its
`deny_columns` block columns an action may set — without coupling those to the subject's read scope.

## Scope

**In scope:**
- An `acl.policy` schema migration adding an `action` column to the row and the primary key.
- Threading `action: Action` through `Acl::set_policy`, `Acl::clear_policy`, `Acl::policies_for`
  (mirroring `grant`/`revoke`/`check`).
- Both adapters (Postgres SQL + committed `.sqlx`; memory fake map key) and the cross-adapter testkit
  contract, extended with the action-scoping property.
- Migrating the sole live consumer (the query-api read path + its e2e test fixtures) to pass
  `Action::Read`, keeping `main` green with identical read behavior.

**NOT in scope (slice 2 / later):**
- **Write enforcement.** `run_action` does not yet load or enforce the `Write` policy — that is
  slice 2. This slice only stores the dimension; nothing enforces the write side yet.
- **`mask_columns` write semantics.** Masking is a read-render concept; a `Write` policy may carry
  `mask_columns` but no consumer interprets it. Slice 2 decides (expected: ignored for writes).
- **`Policy` struct changes.** `Policy { target, row_filter, deny_columns, mask_columns }` is
  unchanged — `action` is a sibling argument, not a payload field.
- **Per-action grants change.** Grants are already action-scoped; `check` is untouched.

## Design

### 1. Schema migration (`0011_acl_policy_action.sql`)

`acl.policy` currently keys on `(role_id, target_kind, target_a, target_b)` (see `0003_acl.sql`).
`acl.role_grant` already carries `action text not null -- 'read' | 'write'` in its key — mirror it:

```sql
alter table acl.policy add column action text not null default 'read';
alter table acl.policy drop constraint policy_pkey;
alter table acl.policy add primary key (role_id, action, target_kind, target_a, target_b);
alter table acl.policy alter column action drop default;
```

Existing rows backfill to `'read'` (reads were the only enforcer of policy until now). The default is
then dropped so the application must always supply an action — consistent with `role_grant`, which
has no default. (In practice greenfield: the deploy provisions Postgres but ships no migration job
yet, so the migration applies to a fresh schema.)

### 2. Trait / API change (`core/src/acl.rs`)

Add `action: Action` to the three policy methods, mirroring the already-action-scoped
`grant`/`revoke`/`check`:

```rust
async fn set_policy(&self, role: &RoleId, action: Action, policy: Policy) -> Result<()>;
async fn clear_policy(&self, role: &RoleId, action: Action, target: &PolicyTarget) -> Result<()>;
async fn policies_for(
    &self,
    subject: &SubjectId,
    action: Action,
    target: &PolicyTarget,
    page: PageReq,
) -> Result<Page<Policy>>;
```

- `set_policy` upserts by `(role, action, target)` — setting a `Write` policy does not disturb the
  `Read` policy for the same `(role, target)`, and vice versa.
- `clear_policy(role, action, target)` removes only that action's policy.
- `policies_for(subject, action, target)` returns only policies whose action matches (unioned across
  the subject's effective roles, as today).
- `check` and the `Policy` struct are unchanged. The `Action`→`'read'|'write'` string mapping reuses
  whatever `grant`/`revoke` already use (no new mapping).

### 3. Adapters

**Postgres (`postgres/src/acl.rs`):**
- `set_policy`: include `action` in the `insert into acl.policy ... on conflict (...) do update`
  column list and conflict key.
- `clear_policy`: add `and action = $N` to the `delete`.
- `policies_for`: add `and p.action = $N` to the select's `where`.
- Regenerate the committed `.sqlx` cache via `tools/sqlx-prepare.sh` (the `query!`/`query_scalar!`
  macros change). The existing `set_policy` row-filter write-time validation (validating the
  `RowFilter` against the ontology for `Type` targets) is unchanged.

**Memory (`memory/src/acl.rs`):**
- The `policies` map key changes from `(String role, TargetKey)` to `(String role, Action,
  TargetKey)` (`Action` already derives `Hash`/`Eq`). `set_policy`/`clear_policy`/`policies_for`
  thread `action` into the key. The same `set_policy` ontology validation is unchanged.

### 4. Testkit contract (`testkit/src/lib.rs`)

The `acl` policy contract (run by **both** adapters, so Postgres and memory are proven to agree)
gains the **action-scoping property** — the load-bearing new behavior:
- A policy set under `set_policy(role, Read, ..)` is returned by `policies_for(.., Read, ..)` but
  **not** by `policies_for(.., Write, ..)`; a `Write` policy is the mirror.
- `clear_policy(role, Read, target)` removes the `Read` policy and leaves any `Write` policy intact.
- All existing policy-contract call sites thread the action argument. The existing `"writer"`-role
  fixture in the contract gains a genuine `Write`-scoped assertion (it was action-agnostic before).

### 5. Consumer migration (keep `main` green)

The read path is the only live `policies_for` consumer:
- `query-api/src/handler.rs::load_policy` passes `Action::Read`.
- Every `set_policy` / `policies_for` / `clear_policy` call site in the query-api e2es
  (`tests/governed_read.rs`, `tests/derived_properties_e2e.rs`, `tests/link_traversal.rs`,
  `tests/multi_hop_traversal_e2e.rs`) adds `Action::Read`.

Read behavior is identical — reads just become *explicitly* `Read`-scoped. No query-api behavior or
e2e assertion changes; only the policy-setup/lookup calls gain the argument.

### File structure

- **Create:** `src/control-plane/postgres/migrations/0011_acl_policy_action.sql`.
- **Modify:** `src/control-plane/core/src/acl.rs` (3 trait signatures).
- **Modify:** `src/control-plane/postgres/src/acl.rs` (3 methods' SQL) + regenerate
  `src/control-plane/postgres/.sqlx/`.
- **Modify:** `src/control-plane/memory/src/acl.rs` (map key + 3 methods).
- **Modify:** `src/control-plane/testkit/src/lib.rs` (acl contract: thread action + action-scoping
  assertions).
- **Modify:** `src/services/query-api/src/handler.rs` (read path → `Action::Read`) and the four
  query-api e2e test files' policy call sites.

## Testing

- **Cross-adapter `acl` contract** (memory + hermetic Postgres): the new action-scoping property
  (Read policy invisible under Write and vice versa; `clear_policy` isolation by action) plus the
  threaded existing assertions. Both adapters run identical contract code.
- **`sqlx-cache-check`** re-validates the regenerated `.sqlx` against the live migrated schema.
- **Full `buck2 test //src/...`** stays green — the read e2es behave identically (now explicitly
  `Read`-scoped); no read assertion changes.

## Decisions

- `action` is a **column in `acl.policy` and part of its primary key**, mirroring `acl.role_grant`;
  existing rows backfill to `'read'`, then the default is dropped.
- `action` is a **sibling parameter** to `set_policy`/`clear_policy`/`policies_for` (not a `Policy`
  field), consistent with `grant`/`revoke`/`check`.
- Read and write policies for the same `(role, target)` are **independent rows**; `policies_for`
  filters by action; `clear_policy` removes only the named action's policy.
- The read path becomes **explicitly `Read`-scoped**, behaviorally identical.

## Follow-ups (later slices)

- **Slice 2 — fine-grained write enforcement (service).** `run_action` loads the `Write` policy and
  enforces deny-write-column (reject if a param sets a denied column) + row-filter-on-insert (a new
  pure in-memory `RowFilter` evaluator: the inserted row must satisfy the predicate). This slice is
  its prerequisite.
- **`mask_columns` for writes** — decide and document (expected: ignored; masking is read-only).

## Roadmap

Lands under Step 3 governance hardening as the control-plane half of **fine-grained write
governance**. It brings `acl.policy` to action parity with `acl.role_grant`, so the platform can
express divergent read/write scopes; slice 2 makes the write scope load-bearing at the
`POST /actions/{name}` front door, bringing write governance to parity with the rich read-side ACL
(row-filter + column policy) the query read path already enforces.
