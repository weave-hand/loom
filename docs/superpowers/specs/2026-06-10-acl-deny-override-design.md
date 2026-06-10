# Design: explicit-deny / deny-override on ACL grants

> **Status:** approved design (2026-06-10). First slice of **Step 3, full ACL
> semantics** (the deferred ACL work). Builds directly on the deny-by-default read
> gate shipped with the query slice (`2026-06-10-query-governed-object-read-slice-design.md`).
> Promotes the ACL grant model from allow-only to Allow/Deny with deny-wins precedence.

## Goal

Add an Allow/Deny `effect` to coarse ACL grants with **deny-wins** precedence, so
governance can express "allow broadly, then carve out exceptions" (e.g. "role
`analysts` may read `Order`, but subject/role `contractors` is denied"). This
completes the deny-by-default access gate `read_object` already enforces: today the
gate is default-deny + allow-only union; this slice makes a Deny grant override Allow.

## Scope: one slice of full ACL semantics

The acl design spec (`2026-06-04-control-plane-acl-design.md`) lists four deferred ACL
features as non-goals: explicit-deny precedence, column masking, role hierarchy, and
semantic/robustness validation. This spec takes **only explicit-deny on coarse
grants**. Explicitly out of scope (each a candidate later slice):

- **Deny on fine row/column policies** (subtractive row-sets: "allow these rows AND
  NOT those") — the harder row-set algebra; not here.
- **Column masking** (NULL/hash values vs. today's project-out).
- **Role hierarchy / inheritance.**
- **Fallible `compile_select` / semantic filter validation.**

Targets are still matched **as stored** (no Type→Table resolution), consistent with
the current `check`/`grant`.

## Model & semantics

- New core enum `Effect { Allow, Deny }`.
- A grant's identity remains `(role, action, target)`; `effect` is an attribute of
  that grant, not part of its key. Re-granting the same `(role, action, target)`
  **upserts** the effect (flips Allow↔Deny). An Allow and a Deny grant for the same
  key therefore cannot coexist — which deny-wins would render pointless regardless.
- **`check(subject, action, target)`**: gather every grant whose `(action, target)`
  matches exactly across all roles assigned to `subject` (unchanged matching — no
  Type→Table resolution), then apply precedence:
  1. any matching grant with `effect = Deny` → **`Deny`**;
  2. else any matching grant with `effect = Allow` → **`Allow`**;
  3. else → **`Deny`** (default-deny, unchanged).

  An unknown subject or a subject with no matching grant stays `Deny` exactly as
  today. Precedence is **pure deny-wins** — no specificity/recency ranking.

## Trait surface (`core`, `src/control-plane/core/src/acl.rs`)

```rust
/// Whether a grant permits or forbids the (action, target). Deny wins over Allow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    Allow,
    Deny,
}
```

- `grant` gains an `effect` parameter and upserts by key:
  ```rust
  async fn grant(&self, role: &RoleId, action: Action, target: PolicyTarget, effect: Effect) -> Result<()>;
  ```
- `revoke(&self, role: &RoleId, action: Action, target: &PolicyTarget) -> Result<()>`
  is **unchanged** — it removes the grant for `(role, action, target)` regardless of
  effect (idempotent no-op if absent, as today).
- `check` keeps its signature `(&self, subject, action, target) -> Result<Decision>`;
  only its internal precedence changes.
- `policies_for` and the entire fine row/column policy layer are **unchanged**.
- No new grant-listing read is added; `check` remains the only reader of grants.

## Enforcement (no change downstream)

`read_object` (query-api) already gates with `acl.check(subject, Read, target) ==
Decision::Deny → QueryError::Forbidden`. With deny-wins folded into `check`, deny-
override takes effect **end to end with no change to `read_object` or
`compile_select`**. This slice lands almost entirely in the ACL concern (core trait +
both adapters + contract); the query path inherits it.

## Schema & adapters

Both adapters implement the same backend-agnostic `Acl` contract.

- **postgres** (`src/control-plane/postgres`): add an `effect` column to the grants
  table via a new migration — `NOT NULL DEFAULT 'allow'`, stored as text (`'allow'` /
  `'deny'`), so any pre-existing grant rows remain Allow (backward-compatible). `grant`
  becomes an upsert that sets `effect` (`ON CONFLICT (role, action, target) DO UPDATE
  SET effect = …`). The `check` query selects the matching grants' effects and applies
  deny-wins (e.g. returns `Deny` if any matching row is `'deny'`, else `Allow` if any
  is `'allow'`, else the caller treats empty as `Deny`). All via compile-time `query!`;
  **regenerate the committed `.sqlx` cache** (`tools/sqlx-prepare.sh`) and keep the
  `sqlx-cache-check` test green.
- **memory** (`src/control-plane/memory`): the grants store is keyed by `(role,
  action, target)` with an `Effect` value; `grant` upserts; `check` gathers the
  subject's roles' matching grants and applies the same deny-wins precedence.

## Testing

- **Shared testkit contract** (`acl_contract`, runs on both the in-memory fake and
  real Postgres): extend with a deny-override case — define a subject in two roles,
  Allow-grant `(Read, Type(T))` via role A → `check` = `Allow`; add a Deny-grant
  `(Read, Type(T))` via role B → `check` = `Deny` (deny-wins); `revoke` the deny from B
  → `check` = `Allow` again; re-`grant` `(Read, Type(T), Deny)` on an existing Allow
  key → `check` = `Deny` (upsert flips effect). Keep the existing allow-only assertions
  passing (now calling `grant(..., Effect::Allow)`).
- **query-api end-to-end** (`governed_read`): add an assertion that a subject Allowed
  via one role but Denied via another is rejected by `read_object` with `Forbidden`,
  proving deny-override through the live gate.

## Migration / call-site impact

The `grant` signature change is small and the project is pre-alpha. Updated call sites:
the testkit acl contract, the postgres/memory acl adapter tests, and the query-api
`governed_read` oracle's `grant(role, Action::Read, PolicyTarget::Type(...))` →
`…, Effect::Allow)`. Existing persisted grant rows default to `Allow` via the column
default, so the migration needs no data backfill.

## Non-goals (restated)

Deny on fine row/column policies; column masking; role hierarchy; fallible-compile /
semantic validation; Type↔Table target resolution; tenancy. Each is a separate later
slice of full ACL semantics.
