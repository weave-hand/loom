# Actions param↔property conformance — Design

> A follow-on to the actions part-1 slice (`2026-06-15-actions-part1-design.md`,
> delivered). Part-1 ships governed typed insert (`POST /actions/{name}`,
> `Action::Write` enforcement, inline DuckLake write behind `ActionEngine`) but
> validates the request body only against the `ActionDef`'s declared parameters —
> it never cross-checks that those parameters actually mirror the target type's
> properties. A misconfigured `ActionDef` therefore surfaces as an opaque
> insert-time 500. This slice closes that gap with an invoke-time conformance
> check that produces a clear, descriptive error instead.

## Goal

Validate, at invoke time, that a resolved `ActionDef` conforms to its target
`ObjectType` — same parameter/property names, compatible logical types, and full
coverage of the target's required properties — and surface any mismatch as a
clear, descriptive error before any write is attempted.

## Scope

**In scope:**
- A pure conformance function over `(ActionDef, ObjectType)`.
- Its invocation inside `run_action`, before the insert.
- A new `ActionError` variant + its HTTP mapping for the clear error.
- Unit + handler tests (+ an optional DuckLake e2e).

**Out of scope (explicitly):**
- **`define_action`-time (fail-fast) validation.** This would change the core
  `Ontology` trait contract (`define_action` could fail validation and would need
  to read the target type) across both adapters (memory + postgres) and the
  testkit contract, and force type-before-action ordering. Deferred; invoke-time
  validation against the live ontology already catches misconfiguration (and
  drift) at the point it matters.
- **Update/delete actions, custom-logic/multi-step actions, Iceberg
  `ActionEngine`, lineage atomicity.** Other part-1 follow-ons, untouched here.
- **Request-body validation.** `parse_params` already validates the request body
  against the `ActionDef`'s parameters; conformance is the orthogonal
  `ActionDef`↔`ObjectType` check and does not change `parse_params`.

## Where it runs

Inside `run_action` (`src/services/query-api/src/action.rs`), in this order:

1. Resolve the action (`get_action`) → `UnknownAction` (404) if absent.
2. Resolve the target type (`get_type`).
3. **Coarse `Action::Write` gate** (`acl.check`) → `Forbidden` (403) if denied.
4. **Conformance check** (new) → `Misconfigured` (500, descriptive) if it fails.
5. `parse_params` (request body) → `BadParams` (400) if invalid.
6. Fine-grained write policy (`check_write_policy`) → `Forbidden` (403) if denied.
7. `insert_row` + best-effort lineage + return the created object.

Conformance runs **after** the coarse Write gate so an unauthorized caller cannot
probe an action's definition validity (leak-safety, consistent with the
deny-by-default ordering elsewhere), and **before** `parse_params`/insert so a
structurally-broken action fails clearly regardless of the request body. It is a
cheap in-memory comparison, evaluated per invoke (so it validates against the
*live* ontology — drift, e.g. a property removed after the action was defined, is
caught for free).

## The conformance check

```
fn check_conformance(action: &ActionDef, target: &ObjectType) -> Result<(), ActionError>
```

Returns `Ok(())` if the action conforms, else `Err(ActionError::Misconfigured(msg))`
where `msg` joins **all** detected violations (so an operator sees every problem
in one pass, not one-at-a-time). Rules, evaluated against the live target type:

1. **Name.** Every `param.name` must equal some `target.properties[].name`. A
   param matching no property is a violation: *"parameter `<p>` matches no
   property of type `<T>`"*.
2. **Type.** For each param that *does* match a property, `resolve_logical(param.ty)`
   and `resolve_logical(property.ty)` must both resolve and yield the **same
   `BaseType`** (the `satisfies` semantics — no implicit Integer/Long widening).
   Distinct messages:
   - property type unrecognized → *"property `<name>` of type `<T>` has unknown
     logical type `<property.ty>`"* (an authoring error in the type);
   - param type unrecognized → *"parameter `<name>` has unknown logical type
     `<param.ty>`"*;
   - base-type mismatch → *"parameter `<name>` type `<param.ty>` is incompatible
     with property `<name>` type `<property.ty>`"*.
3. **Required coverage.** Every `target` property with `required == true` must be
   covered by a param that is itself `required`. Violations:
   - no param for a required property → *"required property `<name>` of type `<T>`
     is not covered by any parameter"*;
   - a required property covered by an *optional* param → *"required property
     `<name>` is covered by optional parameter `<name>` (it could be omitted,
     writing NULL)"*.

The function is pure (no I/O); it reads `ActionDef.parameters` (each `ParamDef {
name, ty, required }`) and `ObjectType.properties` (each `PropertyDef { name, ty,
required }`) and uses `control_plane_core::resolve_logical`.

Rationale for the type rule using `resolve_logical`-equality (rather than calling
`satisfies` directly): `satisfies(property_ty, column_ty)` folds an unrecognized
param type into `Ok(false)`, which would report it as a generic mismatch. Resolving
both sides explicitly lets the check emit the three distinct, actionable messages
above while preserving identical base-type-equality semantics.

## Error surfacing

A new variant on `ActionError` (`src/services/query-api/src/action.rs`):

```rust
/// The action's definition does not conform to its target type (a server-side
/// configuration fault, surfaced with detail so an operator can fix the ActionDef).
Misconfigured(String),
```

HTTP mapping in `post_action` (`src/services/query-api/src/http.rs`):
`ActionError::Misconfigured(msg)` → **500 Internal Server Error** with `msg` as the
body. This is deliberately distinct from the catch-all `_ => 500 opaque` arm: the
message is surfaced because a misconfigured action is an operator-fixable
configuration fault, not sensitive internal detail (the caller already named the
action and is authorized to write through it). It is also distinct from
`BadParams` → 400 (which is about the caller's request body): the caller's request
is valid; the *action definition* is broken, so a 5xx is the honest classification.

## Testing

All `rust_test` integration targets (no inline `#[cfg(test)]`); the e2e via
`loom_fixture_test`.

- **Unit** (`tests/action_conformance.rs`, pure `check_conformance`):
  - exact mirror (params == properties, matching types, required covered) → `Ok`;
  - param matching no property → `Misconfigured`;
  - type mismatch (e.g. param `Integer` vs property `Long`) → `Misconfigured`;
  - unknown logical type on the param, and on the property → `Misconfigured` (each
    with its own message);
  - a required property with no param → `Misconfigured`;
  - a required property covered by an optional param → `Misconfigured`;
  - assert the joined message names every violation when several co-occur.
- **Handler** (`run_action`, in-memory control plane + stub `ActionEngine`):
  - a misconfigured action → `ActionError::Misconfigured` surfaced **before** any
    `insert_row` (the stub records that it was never called);
  - a conformant action → unchanged happy path (created object returned);
  - a Write-denied subject on a misconfigured action → still `Forbidden` (the
    coarse gate precedes conformance — no definition-validity leak).
- **e2e** (`tests/action_conformance_e2e.rs`, DuckLake fixture): define a
  mismatched action, invoke over the real router → **500** with the descriptive
  body, and assert nothing was written (a follow-up governed read returns no row).

## Files

- Modify: `src/services/query-api/src/action.rs` — add `check_conformance`, the
  `ActionError::Misconfigured` variant, and the call site in `run_action` (after
  the coarse Write gate, before `parse_params`).
- Modify: `src/services/query-api/src/http.rs` — map `ActionError::Misconfigured`
  → 500 with the message in `post_action`.
- Create: `src/services/query-api/tests/action_conformance.rs` (pure unit),
  `src/services/query-api/tests/action_conformance_handler.rs` (a `run_action`
  handler test over `MemoryControlPlane` + a recording stub `ActionEngine`), and
  `src/services/query-api/tests/action_conformance_e2e.rs` (DuckLake fixture).
- Modify: `src/services/query-api/BUCK` — new test targets.
- Modify: `docs/FUTURE.md` and `docs/superpowers/specs/2026-06-06-loom-roadmap.md`
  — mark the conformance follow-on delivered; drop it from the deferred list.
