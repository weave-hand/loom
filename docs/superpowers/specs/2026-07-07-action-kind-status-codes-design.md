# Kind-true action status codes — design

**Item:** `#iss-action-kind-status`

## Problem

`POST /actions/{action_name}` answers **`201 Created` for every action kind**.
`post_action`'s single success arm is `(StatusCode::CREATED, Json(one))`
regardless of the action's `ActionKind` (`query-api/src/http.rs:949`), so an
identity-targeted PATCH (`Update`) and a copy-on-write delete (`Delete`) both
report `201 Created` — a resource-creation status for operations that create
nothing.

The wart used to be invisible (only the handler knew the status). It is now
**public**: the generated OpenAPI documents the handler truthfully per
`#road-api-docs-coverage` — `action_op` emits a `201` response for Insert,
Update, *and* Delete (`openapi_gen.rs:358`), with a comment admitting the
single-arm shortcut (`openapi_gen.rs:314-316`) and a test that pins it
(`tests/openapi_gen.rs:360`). So every `/docs` reader now sees a delete
advertised as `201 Created`, and the static guide asserts the same untruth
(`docs/guides/actions.md:159,195`).

The three `ActionKind` variants are `Insert`, `Update`, `Delete`
(`control-plane/core/src/ontology.rs:371`); there is no `Create` variant —
`Insert` is loom's create.

## Scope

Make the action response status **kind-true**: `201 Created` for `Insert`,
`200 OK` for `Update` and `Delete`. Update the generated *and* static OpenAPI
together so both stay truthful, and record the wire change in release notes.

**Non-goals (explicit):**

- **No change to the response body shape.** All kinds still return the single
  affected object as JSON plus the `X-Loom-Run-Id` header — only the status
  line changes.
- **No other endpoint's status changes.** This touches `post_action` only.
- **No versioned API / content negotiation.** The change lands as a plain
  behavior change on the one wire, not behind an `Accept-Version` or `?v=`
  negotiation.
- **The 400/403/404/422/500 error statuses are unchanged** — only the success
  arm is kind-dependent.

## Design

**Status per variant** (the primary step's kind — the first step, which is the
object `run_action` already returns and `action_op` already documents as the
`2xx` body):

| `ActionKind` | Status | Rationale |
|---|---|---|
| `Insert` | `201 Created` | a new object is minted |
| `Update` | `200 OK` | an existing object is mutated in place |
| `Delete` | `200 OK` | an existing object is removed; body is its pre-deletion values |

**Threading the kind to the handler.** `run_action` returns
`(ObjectRows, RunId)` today (`action.rs:527`) and the handler has no kind. Add
the primary kind to that return — `(ObjectRows, RunId, ActionKind)` — sourced
from the primary step: the single-step path already binds `single_step.kind`
(`action.rs:576`), and the multi-step path uses `action.steps.first()` (the
same "primary" the generated doc's `action_op` keys on). `post_action` then
selects the status:

```rust
let status = match kind {
    ActionKind::Insert => StatusCode::CREATED,
    ActionKind::Update | ActionKind::Delete => StatusCode::OK,
};
let mut resp = (status, Json(one)).into_response();
```

(Re-resolving the `ActionDef` in `post_action` to read the kind is rejected:
`run_action` already loaded it, so a second ontology read is wasted work.)

**Generated OpenAPI (`action_op`, `openapi_gen.rs`).** Key the success
response's status string on the primary step's kind: `"201"` for `Insert`,
`"200"` for `Update`/`Delete`, carrying the existing per-kind `ok_desc`
("Updated object" / "Deleted object (pre-deletion values)"). Update the
stale doc comment (`openapi_gen.rs:314-316`) to state that the status is now
kind-true, not a single-arm shortcut. Multi-step actions key on their first
step's kind, same as the single-step case.

**Static docs.** `docs/guides/actions.md` — change the on-success line
(`:159`) and the status-code table (`:195`) to spell out `201 Created` for
inserts and `200 OK` for updates/deletes. Sweep
`docs/system-capabilities/query-api.md` for any action-status claim and align
it.

**Release note.** Add under a *Breaking* heading:

> **Action responses are now kind-true.** `POST /actions/{name}` returns
> `200 OK` for Update and Delete actions (previously `201 Created`); Insert
> actions still return `201 Created`. Clients that branch on the exact `201`
> status for updates/deletes must accept `200`. The response body and
> `X-Loom-Run-Id` header are unchanged.

**Backward incompatibility — accepted.** A client that matches on the literal
`201` for an update or delete will now see `200`. This is a deliberate,
one-time correction: the old status was semantically wrong, it is freshly
exposed in the public OpenAPI, and the platform is pre-1.0 with no stability
guarantee on the action wire. The mitigation is the release note plus the
now-truthful docs; no dual-status shim is worth carrying.

## Testing

- **HTTP e2e (per kind).** Extend the action e2e suite (reusing `e2e-support`)
  to assert `Insert → 201`, `Update → 200`, `Delete → 200` on the real router.
  `tests/action_run_id_http.rs:150` currently asserts `StatusCode::CREATED` on
  an insert — it stays green (insert is unchanged); add sibling update/delete
  cases asserting `StatusCode::OK`.
- **Multi-step.** Assert a multi-step action whose first step is `Insert` still
  answers `201` (primary-kind rule), guarding the "first step wins" choice.
- **OpenAPI snapshot.** Rewrite `tests/openapi_gen.rs:360`
  (`update_and_delete_actions_document_201_like_the_handler`) to assert Update
  and Delete document a `200` response and **no** `201` — the inverse of
  today's assertion — and that Insert still documents `201`. Rename it to drop
  "like_the_handler". Keep the assertion that the `2xx` body `$ref` still
  points at the target type's component schema.
