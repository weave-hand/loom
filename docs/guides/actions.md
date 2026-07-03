# Actions: defining and invoking governed typed writes

Actions are loom's **governed write path** into the ontology. An action is a
named, declarative mutation — no arbitrary code — that the query-api enforces
ACL and model constraints on before it commits. This guide covers defining an
action, invoking it over HTTP, and — the focus here — **multi-object /
multi-step actions**, where one action mutates several related objects in a
single atomic transaction.

If you only need the runtime capability summary, see
[`system-capabilities/control-plane.md`](../system-capabilities/control-plane.md)
(§ *Ontology and typed writes (actions)*). This document is the how-to.

## Mental model

An `ActionDef` is a **name** plus an ordered list of **steps**. Each step
mutates **one** object of one type:

```
ActionStep { target, kind, parameters, assignments, bind }
```

- `target` — the object type this step writes.
- `kind` — `Insert`, `Update`, or `Delete`.
- `parameters` — invocation inputs, mapped onto the target's properties.
- `assignments` — properties filled without a parameter: a constant, a computed
  expression, or a **cross-step reference** to an earlier step's value.
- `bind` — an optional name for this step's resolved row, so a **later** step can
  reference its properties (`@order.id`).

A **single-step** action (the common case — one Insert/Update/Delete) is exactly
the classic shape; you rarely think about steps at all. A **multi-step** action
is the new capability: steps execute in declared order, later steps can wire to
the identities earlier steps just minted, and **all steps commit in one
transaction — all or nothing**.

## Where actions are defined vs invoked

- **Defining** an action is a control-plane operation — `Ontology::define_action`,
  called in-process (at bootstrap/seed time or from a tool). There is **no HTTP
  endpoint to define actions** yet; the external ontology-definition wire is
  deferred. So the "define" examples below are Rust.
- **Invoking** an action is over HTTP: `POST /actions/{name}`. That is the
  runtime surface your clients use.

## Defining a single-step action (the foundation)

Use the fluent `ActionDefBuilder` via `ActionDef::build(name, target, kind)`:

```rust
use control_plane_core::{ActionDef, ActionKind, Ontology};

let create_widget = ActionDef::build("createWidget", "Widget", ActionKind::Insert)
    .param_req("id", "Long")          // required param → writes the `id` property
    .param("name", "String")          // optional param → writes `name`
    .assign("status", serde_json::json!("active"))   // constant default
    .done();

ontology.define_action(create_widget).await?;
```

Parameter and assignment building blocks (all append to the *current* step):

| Builder call | Effect |
|---|---|
| `.param(name, ty)` | optional param writing the property of the same name |
| `.param_req(name, ty)` | required param writing the property of the same name |
| `.param_bound(name, ty, required, binds)` | param renamed from the property it writes (`binds`) |
| `.assign(prop, json)` | constant value when no param supplies `prop` |
| `.assign_expr(prop, "expr")` | computed value over the action's inputs (closed grammar, e.g. `"qty * unitPrice"`) |
| `.assign_step_ref(prop, bind, refprop)` | cross-step reference (`prop = @bind.refprop`) — multi-step only |

Property types (`ty`) are the ontology logical types: `Long`, `String`, `Bool`,
`Double`, `IsoDate`, `IsoTimestamp`.

## Defining a multi-step action

Two builder calls unlock multi-step:

- **`.step(target, kind)`** opens a new step; subsequent `.param*`/`.assign*`
  calls apply to it.
- **`.bind(name)`** names the current step's output so later steps can reference it.

And **`.assign_step_ref(property, bind, refprop)`** wires a later step's property
to an earlier step's value.

### Example: `createOrderWithLines`

Insert an `Order`, then two `LineItem`s whose `orderId` is the order's
just-minted `id`:

Given an `Order` with identity `id`, and a `LineItem` with identity `id`, an
`orderId`, and an optional `sku`:

```rust
use control_plane_core::{ActionDef, ActionKind, Ontology};

let create_order = ActionDef::build("createOrderWithLines", "Order", ActionKind::Insert)
    .param_bound("oid", "Long", true, "id")   // param `oid` writes Order.id
    .bind("order")                             // name this step's row `order`
    // second step: a LineItem whose orderId = @order.id
    .step("LineItem", ActionKind::Insert)
    .param_bound("li1", "Long", true, "id")   // this line item's own identity
    .param("sku1", "String")                   // optional; writes LineItem.sku
    .assign_step_ref("orderId", "order", "id") // orderId = the order's minted id
    // third step: another LineItem
    .step("LineItem", ActionKind::Insert)
    .param_bound("li2", "Long", true, "id")
    .param("sku2", "String")
    .assign_step_ref("orderId", "order", "id")
    .done();

ontology.define_action(create_order).await?;
```

Each step must supply every **required** property of its target — here each
`LineItem` step provides its own `id` (a distinctly-named param, since one flat
body feeds all steps) and gets `orderId` from the cross-step reference, so no
`orderId` parameter is needed.

> **Equivalent struct form.** The builder is sugar over `ActionDef { name, steps:
> vec![ActionStep { target, kind, parameters, assignments, bind }, …] }`, where a
> cross-step ref is `Assignment::step_ref("orderId", "order", "id")`. Use whichever
> reads better; the tests use struct literals for dense multi-step topologies.

### Mixed kinds

A step can Update or Delete a **pre-existing** object alongside an Insert — they
commit atomically. For example, `fulfillOrder` inserts a shipping `LineItem` and
flips the existing order's status:

```rust
let fulfill = ActionDef::build("fulfillOrder", "LineItem", ActionKind::Insert)
    .param_req("id", "Long")
    .param_req("orderId", "Long")
    .step("Order", ActionKind::Update)
    .param_req("id", "Long")                       // identifies the order to update
    .assign("status", serde_json::json!("shipped"))
    .done();
```

## Invoking an action

`POST /actions/{name}` with a JSON **object** of parameters. One flat body
carries the params for **every** step.

```bash
curl -X POST http://localhost:8080/actions/createOrderWithLines \
  -H 'content-type: application/json' \
  -d '{ "oid": "500", "li1": "1", "sku1": "WIDGET-1", "li2": "2", "sku2": "WIDGET-2" }'
```

This inserts `Order 500` and two `LineItem`s (`1`, `2`) whose `orderId` is `500`
— atomically, under one run id.

- **Long values are JSON strings** (`"500"`), so 64-bit ids keep full precision.
  Other types use their natural JSON form (`"text"`, `true`, `3.14`,
  `"2026-07-03"`, `"2026-07-03T12:00:00"`).
- On success: **`201 Created`**, the response body is the affected object as JSON,
  and the **`X-Loom-Run-Id`** header carries the action's run id — use it to look
  up the action's lineage.

### One flat body, distinct per-step params

All steps share a single request body. Each step sees only the parameters it
declares; a body key that belongs to **no** step is rejected. So give each step's
params distinct names (`sku1`, `sku2` above) unless two steps genuinely share an
input. (A cross-step reference like `orderId` is filled by `assign_step_ref`, not
by a parameter, so the child steps need no `orderId` param.)

## What you get: guarantees

- **Atomic — all or nothing.** Every step is resolved and governed *before* any
  write; the writes then stage into one transaction that commits once. If any step
  fails conformance, ACL, a constraint, or the write, the **whole action rolls
  back** — no partial object graph is ever visible.
- **One run, full provenance.** The action emits a single `LineageEvent` under one
  `RunId` whose `outputs` list every step's target, so the whole object graph the
  action produced is one traceable run.
- **Per-step governance.** For each step, in order: the coarse `Action::Write`
  gate on the target, then fine-grained write/mutate ACL, then model-constraint
  validation — identical to the single-object gates, run once per step. A denial on
  step 2 returns before anything commits.

### Governance setup

Grant the caller's role `Write` on **each** step's target type (and `Read` if it
will read the results back). A missing grant on any step's target denies the whole
action.

## Status codes

| Code | Meaning |
|---|---|
| `201 Created` | committed; body = affected object, `X-Loom-Run-Id` header set |
| `400 Bad Request` | body is not a JSON object |
| `403 Forbidden` | coarse `Write` denied (bodyless), or fine-grained write denied (`{error, reason, column?}`) |
| `404 Not Found` | unknown action, or an Update/Delete target row not found |
| `422 Unprocessable Entity` | bad/missing params, model-constraint violations (`{violations: […]}`), or unsupported mutation |
| `500 Internal Server Error` | misconfigured action or backend fault |

## Rules and gotchas

**Define-time (rejected when you `define_action`):**

- A cross-step reference must name a **strictly earlier** step's `bind` and a
  **real property** of that step's target. Forward references, self references,
  references to an unbound step, and references to a non-property are all rejected.
- An `Update`/`Delete` step's target table **cannot** be written by any other step
  in the same action. (A multi-step Overwrite reads the table's *committed* state,
  so same-table multi-mutation could silently lose a sibling write — so it is
  rejected up front. Insert+Insert to the same table is fine; those coalesce.)
- Within one step, no property may be written twice (by two params, or a param and
  an assignment).

**Runtime:**

- Steps execute in **declared order**; a `Delete` step still rejects vector-typed
  targets (the copy-on-write read leg can't yet handle vectors).
- Multi-step `Update`/`Delete` uses whole-table copy-on-write (it rewrites the
  table's post-image), which is `O(table)` per mutating step — fine for modest
  tables, a known cost for large ones.

**Known limitation:**

- A multi-step action's HTTP response currently returns only the **first** step's
  object (e.g. the `Order`, not the `LineItem`s). The children are readable via a
  subsequent governed read, and lineage records the whole graph. Tracked as
  `iss-multi-object-action-response`.

## Related

- Reference: [`system-capabilities/control-plane.md`](../system-capabilities/control-plane.md)
  § *Ontology and typed writes (actions)*.
- Design: `docs/superpowers/specs/2026-07-01-action-multi-object-design.md`.
- Deferred extensions: `fut-action-step-control-flow` (DAG / conditional / fan-out
  / loops), `fut-action-cross-step-expr-refs` (`@bind.prop` inside expressions) in
  [`FUTURE.md`](../FUTURE.md).
</content>
