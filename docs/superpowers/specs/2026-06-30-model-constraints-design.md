# Model constraints — per-value validation on object types

- **Date:** 2026-06-30
- **Area:** ontology
- **Register items:** promotes [[fut-model-constraints]] → mints [[road-model-constraints]]; records [[fut-model-constraint-uniqueness]] + [[fut-model-constraint-backfill]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

An object type's properties can declare **validation rules** — a numeric range, a string
length bound, a regex pattern, an allowed-value set — and loom **rejects writes that
violate them** on every write path. This is the data-quality floor a typed-object platform
needs: the ontology stops being purely structural (shape + presence) and starts enforcing
*content*.

## Current state

`PropertyDef` (`control-plane/core/src/ontology.rs:25`) carries only `name`, `ty` (the
logical type), and `required` (presence/not-null). `ObjectType.identity` names a single
primary-key property. So today's only enforced rules are **presence** (`required`) and
**single-column identity** — there is **no per-value validation** (no range, length,
pattern, or enum).

There are **two write paths**, neither of which checks values beyond shape:

- The **governed typed-insert action** (query-api `action.rs`) builds a one-row batch and
  writes it; ACL `write_filter` checks denied columns/rows, not value validity.
- The **ingest model-binding land** (`ingest/src/gate.rs` `ModelShape::validate`) checks the
  Arrow batch **schema** (column presence + types) — *shape conformance*, not row values.

Both already surface a structured conformance failure (`Violation`/`ViolationReason`, a
**422** with `violations_json`), so there is a violation-reporting machinery to extend
rather than invent. The canonical scalar representation (`core::ScalarValue`,
query-api's `SqlValue`) is what a validator would inspect. `regex` is already vendored
(`//third-party:regex`, transitive today).

## Design

### Constraint model on `PropertyDef`

Add a structured, all-optional `constraints` field to `PropertyDef`:

```
PropertyConstraints {
  range:   Option<{ min: Option<f64>, max: Option<f64> }>,   // numeric props
  length:  Option<{ min: Option<u32>, max: Option<u32> }>,   // string props
  pattern: Option<String>,                                    // string regex
  one_of:  Option<Vec<String>>,                               // allowed-value set
}
```

Each rule is independent and optional; absence = unconstrained (back-compatible — existing
types deserialize to empty constraints). **Applicability is type-tied**: `range` on
numerics, `length`/`pattern`/`one_of` on strings. A constraint declared on an incompatible
`ty`, or an **invalid `pattern` regex**, is rejected at **`define_type` time** (a
constraint-declaration error — fail at definition, never silently at write). `required`
stays the presence rule; identity stays the PK.

**Storage:** a new **nullable JSON column** on the ontology property row (a forward-only
migration; empty/NULL for existing rows). `define_type` persists it; `get_type` /
`list_types` return it; the memory fake mirrors it; the testkit `Ontology` contract gains a
constraints round-trip (and the type-mismatch / bad-regex rejection).

### Shared per-value validator

A **pure** value-level check in `core` (no I/O): given an `ObjectType`'s property
constraints and a property's `ScalarValue`, produce zero or more `ConstraintViolation`s
(property name + which rule + the offending value class — value kept caller-side per the
existing confidentiality posture). Violations **aggregate** (all failures for a row, not
just the first). This single check is driven by **both** write paths:

- **Typed-insert action (query-api):** validate the one-row insert's values **before
  commit**; any violation → **422** with the violations (reusing the structured
  conformance/`Violation` 422 body), **nothing written** — composing with the existing ACL
  `write_filter` gate (ACL denial and constraint violation are distinct rejections).
- **Ingest model-binding land:** after the existing `ModelShape` *shape* check, validate the
  Arrow batch **row values** column-wise through the same value-level check; violations →
  the existing `DoesNotConform` / 422 path **before** materializing.

The check is identical across paths; only the feeding differs (one `ScalarValue` row vs an
iterated batch). `pattern` uses the vendored `regex` crate, **compiled at define-time**
(so an invalid pattern is a definition error, and write-time matching reuses the compiled
form).

### Violation reporting

Reuse the conformance `Violation`/`ViolationReason` taxonomy + the structured 422, adding a
**constraint** violation reason (property + rule). Adding a third violation kind alongside
`BindViolation` and the gate `Violation` raises the value of
[[fut-conformance-enum-consolidation]] — referenced, not undertaken here.

### Decided (not open)

- **Per-row value kinds only** (range/length/pattern/enum) — all checkable row-by-row with
  no cross-row lookup; **uniqueness** is a cross-row/index concern (like identity) →
  [[fut-model-constraint-uniqueness]].
- **Both write paths via one shared validator** — bulk-landed and action-written data are
  held to the same rules.
- **Constraints on `PropertyDef` + JSON column** (not a separate table) — a property and its
  rules stay together; extensible without new joins.
- **Fail at `define_type`** for type-mismatch / bad regex; **forward-only** — defining a
  constraint does **not** re-validate existing rows ([[fut-model-constraint-backfill]]).

## Scope

In scope:

- `PropertyConstraints` (range/length/pattern/one_of) on `PropertyDef`; the ontology JSON
  column + migration; `define_type`/`get_type`/`list_types` carry; both adapters + testkit
  round-trip; define-time rejection of type-mismatch + invalid regex.
- The shared `core` value-level constraint validator (aggregating `ConstraintViolation`s).
- Enforcement wired into **both** the typed-insert action (422) and the ingest land gate
  (`DoesNotConform`/422), composing with the existing shape + ACL gates.
- A constraint `ViolationReason` in the structured 422.

Out of scope:

- **Uniqueness** ([[fut-model-constraint-uniqueness]]) — cross-row existence/index check.
- **Cross-field / row-level constraints** (e.g. `start < end`), constraints on **derived**
  properties, and **link** constraints.
- **Retroactive validation / backfill** ([[fut-model-constraint-backfill]]) — re-validating
  existing data when a constraint is added/tightened.
- **ReDoS-hardened regex** (size/complexity bounds beyond compile-time validity) and the
  `BindViolation`/`Violation` enum consolidation ([[fut-conformance-enum-consolidation]]).

## Testing

testkit contract (both adapters) + validator units + `loom_fixture_test` e2e:

1. **Round-trip:** `define_type` with each constraint kind → `get_type`/`list_types` return
   them unchanged on the memory fake **and** postgres; a back-compat type with no constraints
   still round-trips.
2. **Define-time rejection:** a `range` on a string prop (and a `length` on a numeric), and
   an invalid `pattern` regex, are rejected at `define_type` with a clear error.
3. **Validator units:** each kind (range below/above, length under/over, pattern
   match/no-match, one_of in/out of set) has a pass and a fail case; multiple violations on
   one row aggregate.
4. **Action enforcement:** a typed-insert violating a constraint → **422** with the
   violation, nothing written; a conforming insert succeeds; an ACL denial and a constraint
   violation are reported distinctly.
5. **Ingest enforcement:** a model-binding land with a violating row → `DoesNotConform`/422
   before materialize; a conforming batch lands; shape failure vs value failure are distinct.
6. **Composition:** `required` + value constraints together (a missing required prop and a
   range violation both reported).

## Risk

- **New write-path validation can wrongly reject** (false violations block legitimate
  writes); mitigated by per-kind unit tests (3) and the **single** value-level check driving
  both paths (no two diverging implementations). Empty constraints (the default) are a
  guaranteed no-op, so existing types/writes are unaffected.
- **User-supplied regex** is a ReDoS surface; mitigated by compiling+validating at
  define-time (invalid patterns never reach the write path) — complexity/size bounding is an
  explicit follow-on, not silently ignored.
- **Forward-only** means a newly-added constraint can coexist with already-violating rows;
  this is stated, not hidden, with [[fut-model-constraint-backfill]] as the remedy when
  retroactive enforcement is needed.
- Reuses the existing conformance 422 + `define_type` adapter machinery, so blast radius is
  the additive constraints field + two enforcement call-sites; the shape and ACL gates are
  untouched.
