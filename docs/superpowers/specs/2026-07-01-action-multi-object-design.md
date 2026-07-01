# Custom-logic actions slice 3 — multi-object / multi-step actions

- **Date:** 2026-07-01
- **Area:** ontology
- **Register items:** promotes [[fut-action-multi-object]] → mints [[road-action-multi-object]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

One named action mutates **several objects in one transaction**, and later objects can
reference earlier ones. `createOrderWithLines` inserts an `Order`, then N `LineItem`s whose
`orderId` is the **just-created order's identity**; either the whole action commits or none of
it does. This is the step from "an action writes one object" to "an action is a small,
atomic, governed transaction script" — still declarative, no arbitrary code.

## Current state

Today an `ActionDef` (`ontology.rs:208`) targets **one** type: `{name, target, parameters,
kind, assignments}`. `run_action` (`action.rs:321`) resolves that one target, gates it, and
dispatches to `run_insert`/`run_mutate`, each of which writes **one** object through
`action_engine.write_object`/`overwrite_table` → `iceberg_landing` in a single Postgres tx.
There is no way to write two related objects atomically, and no way for one written row to
reference another's generated identity — a caller must do two calls and stitch the ids
client-side, with no atomicity and a visible half-written state between them.

The commit machinery is closer than it looks: `append_parquet_snapshot`
(`iceberg_landing.rs:149`) already runs on a single `pool.begin()` tx and can register files
for a table + emit lineage + enqueue jobs together; the `Tx` seam (`transaction.rs:33`)
already composes multiple staged writes (`create_table`/`append_files`) in one unit of work.
The gap is an **action-level** multi-write path that threads N per-target writes through one
such unit and captures each step's identity for the next.

## Design

### The step model (single-step = today)

`ActionDef` gains an ordered `steps: Vec<ActionStep>`, where

```
ActionStep { target: TypeName, kind: ActionKind, parameters: Vec<ParamDef>,
             assignments: Vec<Assignment>, bind: Option<String> }
```

`bind` names the step's output so later steps can reference it (`@order`). A **single-step**
action is exactly today's shape — the migration lifts the existing flat
`{target, parameters, kind, assignments}` into one implicit step, so every existing action is
unchanged. Slice-1 `binds` and slice-2 `Assignment` (const|expr) live **inside** each step
unchanged.

### Cross-step references

Steps execute in **declared order**. After each step writes its object, its resolved row
(every property, keyed by name — importantly its declared **identity** property) is captured
into a step-scoped environment under the step's `bind` name. A later step's param default,
assignment, or expression (slice 2's `@ref` machinery, reused wholesale) may reference
`@order.id` / `@order.<prop>`. Reference resolution:

- **Define-time:** a `@bind.prop` must name an **earlier** step's `bind` and a real property
  of that step's target; forward or self references, and refs to an unbound step, are rejected.
  No cycles (order is linear).
- **Invocation-time:** the referenced value is read from the environment after the earlier
  step resolved; a step that produced no identity (e.g. a Delete) exposes only what it had.

This is the capability that makes multi-object more than a batch insert: the child rows are
wired to the parent the action just minted.

### Atomic multi-write

All steps commit in **one** unit of work. The action write path is extended from
"one object → `write_object`" to "N per-target row-sets → one landing transaction":

- The query-api action path resolves **every** step's row (params → binds → assignments/exprs
  → cross-step refs) and runs each step's governance **before** any write: the coarse
  `Action::Write` gate on each step's target, then per-step fine-grained ACL `write_filter`
  and [[road-model-constraints]] validation on that step's resolved row — all unchanged, just
  run per step.
- The resolved per-step writes are handed to an **extended engine write seam** that stages all
  of them (grouped by target table) plus the single action-level `LineageEvent` into one
  `iceberg_landing` transaction (generalizing `append_parquet_snapshot` to accept writes for
  more than one table in the same `pool.begin()`), and commits once. Any step failing
  conformance, ACL, constraints, or the write **rolls back the whole action** — no partial
  object graph is ever visible.
- Steps sharing a target table coalesce into a multi-row batch (`build_object_batches`,
  `serving.rs:109`, already builds N-row batches); distinct targets are distinct staged table
  writes in the one tx.

Identity types on the inline/COW tiers keep their existing rules per step (e.g. a Delete step
still rejects vector types via `ensure_cow_supported`); the multi-write path composes those
per-step guards, it does not relax them.

### Lineage

One action = one `RunId`; the emitted `LineageEvent`'s `outputs` list **every** step's target
`DatasetRef` (inputs stay `[]` for a from-params create), so provenance records the whole
object graph the action produced as a single run.

### Decided (not open)

- **Linear ordered steps**, resolved single-pass; no DAG, no conditional steps, no per-step
  loops/fan-out ([[fut-action-multi-object]] follow-ons if ever needed).
- **Single-step actions are byte-compatible** with today via the implicit-step migration.
- **One Tx, all-or-nothing**; per-step governance runs **before** the single commit.
- **Cross-step refs reuse slice 2's `@ref` resolver** — this slice adds the *step binding
  environment*, not a second expression engine. (It therefore reads most cleanly **after**
  [[road-action-computed-assignments]] lands, though the binding env alone — refs to a prior
  step's plain identity — could ship without full expressions if sequenced first.)

## Scope

In scope: the `steps: Vec<ActionStep>` model + `bind`; the implicit-step migration
(back-compat) + both adapters + testkit; cross-step reference capture + define-time/invocation
resolution; per-step governance (coarse + fine ACL, constraints) run before a single commit;
the extended multi-target landing transaction (N table writes + one lineage event, atomic);
the multi-target `LineageEvent.outputs`.

Out of scope: DAG / conditional / fan-out steps; loops; cross-action composition; distributed
/ cross-database transactions; enqueue-on-commit ([[fut-action-enqueue-downstream]], which can
attach to a multi-step action once both land); arbitrary sandboxed logic.

## Testing

testkit `Ontology` action contract (both adapters) + `loom_fixture_test` e2e:

1. **Round-trip:** a multi-step `ActionDef` (with `bind`s + cross-step refs) → `get_action`
   returns it unchanged on both adapters; a single-step (legacy-shaped) action round-trips
   identically (implicit-step back-compat).
2. **Define-time rejection:** a `@ref` to a later/self/unbound step, to a non-property of the
   bound step's target, or a double-bound property within a step — each rejected.
3. **Order + lines e2e:** `createOrderWithLines` inserts an Order and two LineItems whose
   `orderId` = the created order's identity; reads confirm all three landed and the FK wiring
   is correct.
4. **Atomic rollback:** a later step that fails (ACL 403, constraint 422, or a bad write)
   leaves **no** object from **any** step visible — the whole action rolled back.
5. **Per-step governance:** a denied column on step 2 returns 403 and nothing commits; a
   constraint violation on step 2 returns 422 and nothing commits — each identical to the
   single-object gate.
6. **Lineage:** the action's single `RunId` records **every** step's target in `outputs`.
7. **Mixed kinds:** a step that Updates/Deletes an existing object alongside an Insert step
   commits atomically; vector-type guards still reject where applicable.

## Risk

- **Largest structural change of the arc** — `ActionDef` grows a steps model and the write
  path grows a multi-target atomic commit. Bounded by: the implicit-step migration keeping
  every existing action unchanged (1), and per-step governance being the *existing* gates run
  N times, not new enforcement logic (5).
- **The multi-target landing transaction is the sharp edge** — generalizing
  `append_parquet_snapshot` to more than one table in one tx must preserve its atomicity and
  its lineage/job co-commit; the all-or-nothing rollback test (4) is the load-bearing check.
- **Cross-step resolution correctness** — the declared-order, no-forward-ref rule (shared with
  slice 2) keeps it a define-time property; (2) pins the rejection cases.
- **Sequencing:** cleanest after [[road-action-computed-assignments]] (shares the `@ref`
  resolver). If built first, scope the ref env to prior-step **identity/property values only**
  and fold expressions in when slice 2 lands.
