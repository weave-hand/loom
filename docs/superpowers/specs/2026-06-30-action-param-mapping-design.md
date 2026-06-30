# Custom-logic actions slice 1 — declarative param→property mapping

- **Date:** 2026-06-30
- **Area:** ontology
- **Register items:** promotes [[fut-custom-logic-actions]] → mints [[road-action-param-mapping]]; records [[fut-action-computed-assignments]] + [[fut-action-multi-object]] + [[fut-action-enqueue-downstream]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

An action is a **named operation with its own parameter shape**, not a 1:1 echo of its
target type's properties. A caller invokes `createCustomer(displayName, …)` whose params are
named for the operation and mapped — declaratively — onto the underlying properties, with
some properties filled by constants the caller never supplies. This is the first slice of
the custom-logic-actions arc: the **decoupling** that turns actions into a real API surface,
without opening the door to arbitrary code.

## Current state (reconciled)

Actions exist with three kinds — `ActionKind::{Insert, Update, Delete}` (`ontology.rs`;
Update/Delete shipped in [[road-update-delete-actions]], so the fut's "only typed insert" is
**stale**). But an action's `parameters` are matched to the target type's **properties 1:1
by name and type**: `check_insert_conformance` (`query-api/src/action.rs:155`) requires every
required property be covered by a same-named required param, and `check_param_property_types`
requires each param name a real property of a compatible type. The write path then reads each
property's value from the **identically-named** param.

So an action cannot: rename a param away from its property, supply a property the caller
doesn't pass (a constant/default), or have any param that isn't a verbatim property. The
"custom-logic" frontier — params that **differ from properties**, **bespoke logic**, and
**enqueue-downstream** — is all gated behind this 1:1 coupling. This slice breaks the
coupling with a **declarative mapping** (the foundation); the heavier asks are sequenced
follow-ons (below).

`ParamDef` is `{ name, ty, required }`; `ActionDef` is `{ name, target, parameters, kind }`.
`define_action` upserts them; the ontology stores params per action.

## Design

### The mapping — rename + constant/default assignments (declarative)

Two additive pieces decouple params from properties:

- **`ParamDef.binds: Option<String>`** — the **property** this param writes. `None` defaults
  to `name` (so existing actions, whose params are named for their properties, are
  unchanged). A non-`None` `binds` lets the param be named for the *operation* while writing
  a differently-named property (rename).
- **Constant assignments on `ActionDef`** — an ordered `Vec<{ property, value }>` filling a
  property with a **declared constant** when no param supplies it (the default/fixed-value
  case — e.g. `status = "active"`, `kind = "manual"`). `value` is a typed scalar
  (string/int/float/bool) matching the property's type (reusing the canonical scalar
  representation).

A property's written value resolves, in order: the **param bound to it**, else its
**constant assignment**, else **unset** (NULL for a nullable insert property).

No expressions, no computation, no code — purely declarative wiring of params + constants
onto properties. Computed values (`total = qty*price`, `now()`) are the **next** slice
([[fut-action-computed-assignments]]).

### Generalized conformance (define-time)

`define_action` conformance generalizes the current name-equality rules to the mapping:

- every `binds` (and every constant-assignment `property`) **names a real property** of the
  target;
- the bound param's `ty` (and the constant's type) is **compatible** with that property's
  type (today's `check_param_property_types`, retargeted through `binds`);
- **no property is double-bound** — covered by both a param and a constant → a misconfigured
  action (rejected), and no two params bind the same property;
- **every required property is covered** by exactly one of: a required param binding it, or a
  constant assignment (the generalization of today's "rule 3").

These run for **Insert** and **Update** (whose params resolve which properties the PATCH
sets); **Delete** is unchanged (identity-only). The identity param for U/D binds the identity
property like any other.

### Write path — resolve, then the existing gates

`run_action` builds the object row by **resolving each target property from the mapping**
(bound param value, else constant, else unset) instead of reading a same-named param. Then
the path is **unchanged**: the resolved row flows through the existing ACL `write_filter`
(denied-column / row-filter enforcement on the resolved row) and, composing cleanly, the
per-value validation of [[road-model-constraints]] — both gate the *resolved* row exactly as
they gate a direct insert today. Only **row construction** changes; the governance and
validation gates, the snapshot+lineage commit, and the I/U/D primitives are untouched.

### Storage

The ontology gains: a nullable **`binds`** column on the action-parameter row, and the
constant assignments (a child `ontology.action_assignment(action, property, value, ty)` table
or a JSON column on the action) — a forward-only migration. `define_action`/`get_action`
carry them; the memory fake mirrors; the testkit `Ontology` action contract is extended.

### Decided (not open)

- **Declarative mapping only** (rename + constants/defaults) — no computed expressions
  ([[fut-action-computed-assignments]]), no multi-object ([[fut-action-multi-object]]), no
  enqueue ([[fut-action-enqueue-downstream]]), no sandboxed/WASM bespoke logic (the far
  frontier).
- **`binds` defaults to the param name** — existing actions are byte-for-byte unchanged.
- **Reject double-binding** at define time rather than defining param-vs-constant precedence —
  ambiguity is a misconfiguration, not a silent winner.
- **Applies to Insert + Update**; Delete unchanged.
- **Gates unchanged** — the mapping sits *before* ACL + constraints; it composes, it does not
  alter enforcement.

## Scope

In scope:

- `ParamDef.binds` (rename) + `ActionDef` constant assignments (typed scalar values).
- Generalized define-time conformance (real-property + type-compat through `binds`/constants;
  no double-bind; required coverage via param-or-constant).
- Write-path row resolution from the mapping, feeding the unchanged ACL/constraints/commit
  path.
- Ontology storage (`binds` column + constant assignments) + migration + both adapters +
  testkit action contract.

Out of scope:

- **Computed assignments** ([[fut-action-computed-assignments]]) — a property = a bounded
  expression over params (`now()`, arithmetic, concat); needs an expression evaluator + its
  typing/safety. The natural slice 2.
- **Multi-object / multi-step actions** ([[fut-action-multi-object]]) — one action mutating
  several objects in one transaction.
- **Enqueue-downstream** ([[fut-action-enqueue-downstream]]) — atomic `Tx.enqueue` of a job on
  action commit (write-then-derive).
- **Arbitrary sandboxed/WASM bespoke logic** — the far frontier beyond the declarative slices;
  not scoped here.

## Testing

testkit `Ontology` action contract (both adapters) + `loom_fixture_test` e2e through the
action path:

1. **Round-trip:** `define_action` with renamed `binds` + constant assignments → `get_action`
   returns them unchanged on the memory fake and postgres; an action with neither (params
   named as properties) round-trips identically (back-compat).
2. **Define-time rejection:** `binds`/assignment naming an unknown property, a type-mismatched
   param/constant, a **double-bound** property (param + constant, or two params), and a
   required property left uncovered are each rejected with a clear misconfiguration error.
3. **Rename e2e:** an Insert action whose param names differ from the properties writes the
   values into the **bound** properties; a read returns them correctly.
4. **Constant fill e2e:** a property with **no** param is filled by its constant assignment
   (incl. a *required* property covered only by a constant); the row lands with that value.
5. **Gate composition:** the resolved row is subject to the **same** ACL `write_filter` 403
   and (where present) [[road-model-constraints]] 422 as a direct insert — a denied column or
   a constraint violation on a *constant-filled* or *renamed* property behaves identically.
6. **Update mapping:** an Update action with renamed params PATCHes the bound properties;
   Delete (identity-only) is unchanged.

## Risk

- **Generalized conformance is the new logic** — a bug could accept a misconfigured action or
  reject a valid one; pinned by the define-time rejection matrix (2) and the round-trip (1).
  `binds`-defaults-to-name makes the change **additive** for every existing action.
- **Row-construction change on the write path** is the other moving part; bounded because the
  ACL/constraints/commit gates are *unchanged* and run on the resolved row (5) — the mapping
  cannot weaken enforcement, only change which value reaches a property.
- **Scope discipline:** the declarative-only boundary is what keeps this slice safe and
  governable; the powerful (and riskier) computed/multi-object/enqueue/bespoke capabilities
  are explicitly sequenced, not smuggled in.
- Composes with [[road-model-constraints]] (validation of constant-filled/renamed values) and
  reuses the proven I/U/D + ACL + commit machinery, so blast radius is the mapping resolution
  + the generalized conformance.
