# Custom-logic actions slice 2 — computed assignments (expression-valued properties)

- **Date:** 2026-07-01
- **Area:** ontology
- **Register items:** promotes [[fut-action-computed-assignments]] → mints [[road-action-computed-assignments]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

An action property can be set by a **bounded expression** over the action's inputs, not
only by a bare parameter or a fixed constant. `createOrder(qty, unitPrice)` can declare
`total = qty * unitPrice`, `createdAt = now()`, `tier = if total > 100 then "gold" else
"std"`. This is slice 2 of the custom-logic-actions arc: it turns the declarative
param→property *wiring* of slice 1 ([[road-action-param-mapping]]) into declarative
*computation* — still no arbitrary code, no I/O, no loops, but real derived values.

## Current state

Slice 1 ([[road-action-param-mapping]], shipped #269) is fully landed. An action property's
written value resolves, in `crate::params::resolve_action_row` (`query-api/src/params.rs:54`),
in order: the **param bound to it** (`ParamDef.binds_property()`), else its **constant
assignment** (`ConstAssignment{property, value}`, `ontology.rs:199`), else **unset** (NULL).
Constants are typed JSON scalars validated at define time by `params::validate_const`
(`params.rs:93`) against the property's logical type. The canonical scalar carrier throughout
is `SqlValue` (`serving.rs:22`: `Text|Int|Bool|Double|Date|Timestamp|Null`). Every resolved
row flows unchanged through the ACL `write_filter` gate and the [[road-model-constraints]]
per-value validator before the atomic snapshot+lineage commit (`run_insert`, `action.rs:373`).

What is missing: a property can be a param value or a fixed constant, but **not a value
computed from the params**. `total = qty * unitPrice` cannot be expressed — the caller must
compute it client-side and pass it, which is exactly the coupling actions are meant to remove.

## Design

### The expression grammar (closed, total, deterministic)

An assignment's source becomes either a constant (today) or an **expression string** parsed
into a typed AST. The grammar is small and closed — no user-defined functions, no I/O, no
loops, no recursion, deterministic except `now()`:

- **Operands:** literals (`42`, `3.14`, `"txt"`, `true`); **param refs** (a bare identifier
  naming a `ParamDef`); **property refs** (`@prop`, naming another property of the target
  resolved earlier in the same action).
- **Arithmetic:** `+ - * / %` over numerics (Int/Double, with the existing Int/Double
  coercion rules).
- **String:** `++` concat.
- **Comparison:** `= != < <= > >=` → Bool.
- **Boolean:** `and or not`.
- **Conditional:** `if <cond> then <a> else <b>` (both arms same type).
- **Whitelisted functions:** `now()` (→ Timestamp), `upper/lower(str)`, `substr(str, start,
  len)`, `length(str)` (→ Int), `coalesce(a, b, …)`, `cast(x as <type>)` (the declared logical
  types).

Parsing is a small hand-written Pratt/precedence parser in query-api (no new third-party dep
required; if one is warranted, a `nom`-style grammar is acceptable — a plan-time call). The
AST is a typed enum; evaluation is a pure recursive walk producing a `SqlValue`.

### Property-reference ordering

Property refs (`@prop`) let one assignment build on another (`total = qty*price`; `tax =
total * 0.2`). Resolution is **single-pass in declared assignment order** over an accumulating
`{property → SqlValue}` environment seeded with the resolved params. A `@prop` that names a
property not yet resolved (later in the order, or never assigned) is a **define-time error** —
no forward refs, no cycles, no fixpoint. This keeps evaluation total and its order obvious.

### Evaluation location — query-api, before the gates

The evaluator runs inside `resolve_action_row` (`params.rs`), replacing the const-or-param
lookup with: param bound → its value; else an assignment → evaluate its source (const literal
or expression) against the environment; else unset. The produced `SqlValue` row is then
**unchanged** downstream — the same ACL `write_filter`, the same [[road-model-constraints]]
422 validator, the same atomic commit. Computation sits *before* governance; it cannot weaken
enforcement, only decide which value reaches a property.

### Define-time typing + safety

`define_action` conformance (`action.rs:check_assignments_and_binds`) generalizes to
expressions:

- every ref (param or `@property`) **resolves** (real param / earlier-resolved property);
- the expression **type-checks** — operator/function arg types and the result type are
  computed bottom-up and the result must be **compatible with the assigned property's logical
  type** (reusing the slice-1 type-compat rules);
- unknown functions, arity errors, type errors, forward/cyclic `@refs`, and slice-1's
  **double-bind** (a property covered by both a param and an assignment, or two assignments)
  are each rejected with a clear misconfiguration error.

Runtime-only failures the type system can't rule out — integer division/modulo by zero, a
`cast` that can't represent the value, `substr` out of range — surface as a **422** (same
class as a constraint violation), never a 500.

### Storage

`ConstAssignment{property, value}` generalizes to `Assignment { property, source }` where
`source` is `Const(serde_json::Value)` or `Expr(String)`. A forward-only migration adds a
nullable `expr` column (or a discriminated `source_kind`) to the assignment row; existing rows
map to `Const`. `define_action`/`get_action` carry it; the memory fake mirrors; the testkit
`Ontology` action contract is extended. Back-compat: an action with only const/param
assignments round-trips byte-for-byte.

### Decided (not open)

- **Closed grammar only** — the listed operators/functions; extension is a later slice, not an
  open-ended DSL. No user code, no I/O, no loops/recursion.
- **Single-pass, declared-order property resolution** — no forward refs, no cycles, no
  fixpoint solver.
- **Evaluate in query-api before the gates** — governance and validation are unchanged and run
  on the computed row.
- **Runtime arithmetic faults → 422**, not 500.
- Applies to **Insert + Update** (Update's assignments compute the PATCHed values); **Delete**
  is identity-only, unchanged.

## Scope

In scope: the expression AST + Pratt parser + pure evaluator (→ `SqlValue`); the generalized
`Assignment` model (const|expr) + migration + both adapters + testkit; define-time typing
(ref resolution, type inference, compatibility, arity, no forward/cyclic refs, double-bind);
write-path evaluation slotted into `resolve_action_row` feeding the unchanged
ACL/constraints/commit path; the `now()`/string/`cast`/`coalesce` function set.

Out of scope: aggregates / subqueries / cross-row references (a property computed from *other
objects*); user-defined or sandboxed/WASM functions ([[fut-custom-logic-actions]] far
frontier); multi-object steps ([[fut-action-multi-object]]); enqueue on commit
([[fut-action-enqueue-downstream]]). Non-`now()` nondeterministic sources (random, external
lookups) are excluded by construction.

## Testing

testkit `Ontology` action contract (both adapters) + `loom_fixture_test` e2e through the
action path:

1. **Round-trip:** `define_action` with expression assignments → `get_action` returns them
   unchanged on the memory fake and postgres; a const-only action round-trips identically
   (back-compat).
2. **Define-time rejection matrix:** unknown ref, type-mismatched expression vs property,
   unknown function / wrong arity, forward or cyclic `@property` ref, and a double-bound
   property are each rejected with a clear error.
3. **Arithmetic e2e:** `total = qty * unitPrice` writes the product; a read returns it exactly
   (Int and Double paths).
4. **Conditional + string e2e:** `tier = if total > 100 then "gold" else "std"` and a
   `upper/substr/++` expression each write the expected value.
5. **`now()` e2e:** a `createdAt = now()` property lands a Timestamp within the request window.
6. **Runtime fault → 422:** integer div-by-zero / out-of-range `substr` returns 422, no 500,
   nothing committed.
7. **Gate composition:** a computed value that trips the ACL `write_filter` (denied column) or
   a [[road-model-constraints]] constraint behaves **identically** to the same literal value —
   computation does not bypass governance.
8. **Update mapping:** an Update action whose PATCH values are computed writes the computed
   delta; Delete unchanged.

## Risk

- **The parser + type inferencer are the new logic** — a bug could accept a mistyped
  expression (caught by the runtime `SqlValue` type mismatch, but ideally at define time) or
  reject a valid one; pinned by the rejection matrix (2) and the arithmetic/conditional e2es
  (3, 4). Keeping the grammar **closed and total** bounds the surface.
- **Evaluation-order correctness** for `@property` refs — the single-pass declared-order rule
  with no forward refs makes the order a define-time property, not a runtime surprise.
- **Governance is unchanged** — the computed row is gated exactly as a literal row (7), so the
  slice cannot weaken ACL/constraints; blast radius is the evaluator + generalized conformance.
- **Size:** this is the largest of the three action follow-ons (a parser + typer + evaluator).
  The closed grammar and the "evaluate then hand to the existing gates" seam keep it a single
  coherent plan; if the parser proves heavy, the function set (`substr/cast/coalesce`) can be
  a fast-follow within the same arc without changing the AST/typing spine.
