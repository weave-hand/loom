# Existence validation for loom-owned ontology references — Design

> Scopes and closes the loom-owned portion of `iss-existence-validation`. Three
> control-plane write paths store a reference to an **ontology type** without
> checking it exists, so dangling references accumulate and the two adapters
> diverge (Postgres has one FK that the in-memory fake does not replicate). This
> slice makes the existence check explicit and consistent across both adapters,
> with no new core seam. The catalog-table and external-lineage portions of the
> issue — which need new seams/conventions — are carved into follow-on items.

## Goal

Reject, at the control-plane write, a reference to a non-existent ontology type
on the three loom-owned paths — consistently on both the Postgres adapter and the
in-memory fake — with a clear error and a both-adapter contract test.

## Scope

**In scope** — references to an ontology type (`ontology.object_type`), which is
loom-owned and resolvable:

1. **`Ontology::define_action(action)`** (`…/postgres/src/ontology.rs:287`,
   `…/memory/src/ontology.rs:98`) — `action.target: TypeName` must exist. Postgres
   already has an FK `ontology.action.target_type → ontology.object_type(name)`
   (migration `0009_actions.sql`), but it surfaces as a raw backend error and the
   **memory fake does not replicate it** — the adapters diverge today. Make the
   check explicit so both behave identically.
2. **`Acl::grant(role, action, target, effect)`** (`…/postgres/src/acl.rs:154`,
   `…/memory/src/acl.rs:147`) — when `target = PolicyTarget::Type(name)`, `name`
   must exist. Today: no check at all.
3. **`Acl::set_policy(role, action, policy)`** (`…/postgres/src/acl.rs:210`,
   `…/memory/src/acl.rs:174`) — when `policy.target = PolicyTarget::Type(name)`,
   `name` must exist. Today the type is checked only when a `row_filter` is present
   (to validate the filter's columns); make it **always** check the target's
   existence.

**Out of scope** — carved into follow-on items (see below): catalog-table
references, external lineage `DatasetRef`s, and `define_link` backing columns.

## Mechanism

An explicit existence check in **both adapters**, returning
`ControlPlaneError::Validation("… references unknown type \`X\`")` (matching the
message `set_policy` already emits). This is the contract-testable, adapter-
consistent mechanism:

- **Postgres**: a `select 1 from ontology.object_type where name = $1` guard
  before the store (the same shape `define_link` already uses to validate its
  endpoint types, and `set_policy` uses for its row-filter type check).
- **Memory**: read the ontology map (the cross-concern pattern `set_policy`
  already uses via `type_properties` — take the `acl` lock for the role check,
  release it, then take the `ontology` lock; lock order acl→ontology, no new
  deadlock surface).

**No new core seam, no trait change, no new error variant.** `ControlPlaneError`
is `#[non_exhaustive]` and already carries `Validation`; this honors the issue's
"no new core seam needed." The existing `define_action` Postgres FK stays as an
atomic backstop. **No new FK constraints are added** for the ACL paths: the
polymorphic `(target_kind, target_a, target_b)` target encoding (a type name only
when `target_kind = 'type'`) does not map to a simple column FK, and the in-Rust
check is the uniform both-adapter mechanism.

### Error variant

`ControlPlaneError::Validation` for all three, consistent with `set_policy`'s
existing `"policy references unknown type {name}"`. (`define_link` uses `NotFound`
for the same situation — a pre-existing inconsistency this slice does not churn;
the new checks match the nearer sibling, `set_policy`.) Messages:
`"grant references unknown type \`{name}\`"`,
`"policy references unknown type \`{name}\`"` (unchanged),
`"action \`{name}\` references unknown target type \`{target}\`"`.

### Concurrency

The ACL checks are **best-effort / non-transactional** — a pre-store existence
query, so a concurrent type deletion between the check and the store is tolerated.
This matches `set_policy`'s already-documented stance and the codebase's existing
single-writer posture (cf. `iss-acl-role-cycle-atomic`); it is not a regression.
`define_action`'s check runs inside its existing transaction, with the FK closing
the window atomically. (A fully race-free guarantee for the ACL paths is not a
goal here and would need the same SERIALIZABLE/locking treatment tracked by
`iss-acl-role-cycle-atomic`.)

### `Table` targets stay unvalidated

`PolicyTarget::Table(TableRef)` references the DuckLake catalog, which is
DuckDB-owned and not in loom's Postgres — un-FK-able and not cheaply checkable
without a new `Catalog::exists` read. This slice leaves `Table` targets
unvalidated (deferred), and the contract test asserts a `Table` grant is **not**
rejected, to pin that boundary explicitly.

## Testing

A new `existence_validation_contract<CP: Acl + Ontology>(cp: &CP)` in the testkit
(`…/control-plane/testkit/src/lib.rs`), run by **both** adapter suites via the
established pattern (a `memory_passes_existence_validation_contract` in
`…/memory/tests/` and a `postgres_passes_existence_validation_contract` in
`…/postgres/tests/`, each calling the one generic contract). It seeds a type `T`
and a role `R`, then asserts:

- `grant(R, Read, Type("T"))` → `Ok`; `grant(R, Read, Type("Nope"))` → `Validation`.
- `set_policy(R, Read, policy{target: Type("T")})` → `Ok` (with and without a
  `row_filter`); `set_policy(…, Type("Nope"))` → `Validation`.
- `define_action(target: "T")` → `Ok`; `define_action(target: "Nope")` →
  `Validation`.
- `grant(R, Read, Table(some_ref))` → `Ok` (deferred boundary: `Table` targets are
  not validated).

This closes the today-divergence: before the change, `define_action("Nope")` is
rejected by Postgres (FK) but accepted by memory; after, both reject it the same
way.

## Out of scope → follow-on items (mint at close)

When the work agent closes `iss-existence-validation`, split off what this slice
does not cover so it is not lost:

- **`iss-` / `fut-` catalog-reference validation** — validate `define_type`'s
  backing `TableRef` and ACL `Table` targets against the catalog. Needs a
  `Catalog::exists(&TableRef)` read seam (the DuckLake catalog is external to
  loom's Postgres). `area:catalog`/`cross-cutting`.
- **`fut-` lineage `DatasetRef` validation** — needs an internal-vs-external
  namespace convention first (a `DatasetRef` is deliberately opaque and may name
  an external dataset; validating naively would reject legitimate external
  lineage). `area:lineage`.
- **`fut-define-link-validation`** — `define_link` backing columns; already
  tracked, unchanged.

## Files

- Modify: `src/control-plane/postgres/src/acl.rs` (grant + set_policy type-exists
  guard), `src/control-plane/postgres/src/ontology.rs` (define_action target
  guard).
- Modify: `src/control-plane/memory/src/acl.rs` (grant + set_policy),
  `src/control-plane/memory/src/ontology.rs` (define_action).
- Modify: `src/control-plane/testkit/src/lib.rs` (add
  `existence_validation_contract`).
- Create/modify: `src/control-plane/memory/tests/…` and
  `src/control-plane/postgres/tests/…` to run the new contract; the relevant
  `BUCK` test targets.
- Core (`src/control-plane/core/`) is **untouched** (no new seam/variant).
- Modify: `docs/ISSUES.md` — close `iss-existence-validation` (scoped) and add the
  catalog-reference + lineage-ref follow-on items.
