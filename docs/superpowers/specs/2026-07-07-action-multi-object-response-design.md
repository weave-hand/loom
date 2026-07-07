# Multi-object action response envelope — design

**Item:** `#iss-multi-object-action-response`

## Problem

A multi-step action commits every step atomically and records every step in
lineage, but its HTTP response returns only the **first** step's object. In
`run_multi_step` (`src/services/query-api/src/action.rs:1248`) the per-step loop
resolves and governs each step, yet it captures the affected object only for the
first one — `first_affected` is set once, guarded by `if first_affected.is_none()`
(`action.rs:1307`, `action.rs:1333-1338`) — and returns that single `ObjectRows`
(`action.rs:1370-1372`). `post_action` (`src/services/query-api/src/http.rs:922`)
then renders it via `objects_to_json` and unwraps the *first* element
(`http.rs:939-946`), so a caller invoking `createOrderWithLines` gets back the
`Order` and never sees the two `LineItem`s the same transaction created.

This is an **ergonomic gap, not a correctness one.** The children *are* written
(the atomic multi-target commit is intact) and the single action-level
`LineageEvent.outputs` already lists **every** step's target dataset
(`action.rs:1357-1366`), so provenance records the whole graph. The children are
also readable via a subsequent governed read. What's missing is only the inline
echo of the child rows in the action response. It was deferred because no caller
yet needed the children inline.

## Scope

In scope: a multi-object response envelope carrying **all** steps' affected rows,
each labelled by its step `bind` and `target`, built in the action layer and
serialized by `post_action`, plus its OpenAPI documentation.

**Non-goals (explicit):**

- **No change to lineage.** `LineageEvent.outputs` already lists every step's
  target (`action.rs:1357-1366`); this work only surfaces what lineage already
  records, in the HTTP response.
- **No change to the single-step action response.** A lone bind-less step is
  today's byte-compatible path (`run_action`, `action.rs:549-580`) and its 201
  body stays the bare affected object — see *Backward compatibility* below.
- **No re-read / serving round-trip.** The envelope echoes each step's *resolved
  write image* (the same `affected_object` the first-object response already
  uses), not a governed read-back — so masking/row-filter-on-read is out of scope
  and the trust model is unchanged from today's single-object echo.
- **No new step semantics.** Update/Delete steps are simply *included* in the
  envelope; their affected-row shape (full post-image) is unchanged.
- **No change to the `X-Loom-Run-Id` header** mechanism (`http.rs:950-951`).

## Design

### The envelope

`run_multi_step` already computes the affected `ObjectRows` for a step at
`action.rs:1335-1337` via `affected_object(target, cols, vals)` (`action.rs:653`)
— it just discards all but the first. The change is to collect one entry **per
step**, in declared order:

```rust
struct StepResult { bind: Option<String>, target: String, rows: ObjectRows }
```

The action layer returns these ordered step results (alongside the existing
`RunId`) instead of a lone `ObjectRows`. To keep the single-step path untouched,
`run_action` returns an outcome the handler can branch on — e.g.
`ActionOutcome::Single(ObjectRows)` from the single-step path and
`ActionOutcome::Multi(Vec<StepResult>)` from `run_multi_step`.

### Wire / JSON shape

`post_action` serializes each step's `ObjectRows` with the existing
`objects_to_json` (`src/services/query-api/src/render.rs:17`) and wraps the
per-step `objects` arrays into an ordered list:

```json
{
  "steps": [
    { "bind": "order", "target": "Order",    "objects": [ { "id": "500" } ] },
    { "bind": null,    "target": "LineItem", "objects": [ { "id": "1", "orderId": "500" } ] },
    { "bind": null,    "target": "LineItem", "objects": [ { "id": "2", "orderId": "500" } ] }
  ]
}
```

**Why an ordered list, not a `{bind: object}` map.** `bind` is optional
(`ActionStep.bind: Option<String>`, `ontology.rs:514-516`) and two unbound steps
can share a target (the two `LineItem` steps above). A map keyed on bind would
collapse them; keying on target would collide. An ordered array — labelled with
both `bind` (the caller-facing name when the step declared one) and `target`
(always present) — keeps every step individually addressable by declared
position while still exposing the bind key. This is the "keyed by bind/target"
envelope: `bind` is the semantic key, `target` the type label, array order the
disambiguator for unbound same-target steps.

Append steps sharing a target coalesce in the *write* path (`coalesce_appends`,
`action.rs:1353`), but the *response* stays one entry per declared step — the
step results are captured in the loop before coalescing, so each `LineItem`
remains its own envelope entry.

### Backward compatibility

- **Single-step actions:** `run_action`'s single-step branch is unchanged; the
  201 body remains the bare affected object and `post_action`'s existing
  render-and-unwrap-first stays for the `Single` outcome. Existing single-object
  callers see no difference.
- **Multi-step actions:** the response shape changes from "bare first object" to
  the `{steps: [...]}` envelope. This is safe: the current multi-step response is
  the very incompleteness this item fixes, and the item records there is *no*
  caller depending on the bare-first-object multi-step shape ("Deferred as no
  caller yet needs the children inline"). The change is therefore a strict
  improvement with no dependent contract to preserve.
- The `X-Loom-Run-Id` response header is emitted for both shapes, unchanged.

### OpenAPI

The `#[utoipa::path]` on `post_action` (`http.rs:908-921`) currently documents
the 201 as "created/affected object". Add a `ToSchema` struct for the envelope
(`ActionStepsBody { steps: Vec<ActionStepResult> }`) and update the 201
description to state that single-step actions return the bare object and
multi-step actions return the `steps` envelope. Register the schema so
`build_openapi()` picks it up (same pattern as the other action bodies).

## Testing

- **Extend the existing multi-object e2e** (`tests/action_multi_object_e2e.rs`,
  the `createOrderWithLines` case at ~L158-230, which today ignores the returned
  object as `_created` at `action.rs`-caller L203): assert the outcome carries
  three step results in order — `bind: "order"` / target `Order` with `id 500`,
  then two `LineItem` entries (`bind: null`) carrying `id` `1`/`2` and
  `orderId == "500"`. This proves the children and their cross-step FK wiring are
  echoed inline, matching what the read-back at L212-230 already confirms landed.
- **Handler-level test** over the query-api router using the `e2e-support`
  library (`//src/services/query-api:e2e-support`, `tests/e2e_support.rs` — its
  `get`/`StubAction`/`subject_with_role`/`grant_read` helpers): `POST
  /actions/createOrderWithLines` and assert the 201 body is the `{steps:[...]}`
  envelope with all three entries and the `X-Loom-Run-Id` header still present.
- **Single-step back-compat test:** a single bind-less Insert action still
  returns the bare affected object at 201 (no `steps` wrapper) — pin the
  unchanged shape so a future refactor can't silently wrap it.
- **OpenAPI test:** assert the envelope schema appears in `build_openapi()`
  (existing openapi-test pattern).
