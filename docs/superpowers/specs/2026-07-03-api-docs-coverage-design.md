# API docs coverage — real action ops, per-type grouping, runtime-route docs

- **Date:** 2026-07-03
- **Area:** devx
- **Register items:** delivers [[road-api-docs-coverage]] (which subsumes the
  former `#fut-openapi-per-type-insert-route`)
- **Status:** implementation-ready

## North star

A developer opens `/docs` on query-api and sees the API as it actually is: one
section per ontology type holding that type's List operation, its real callable
actions (`POST /actions/{name}` with the action's true parameter schema), and
its outbound link traversals — plus sections documenting the login, session,
service-account, and admin (users/roles/grants/models) endpoints that already
exist but are invisible today. Nothing in the document names a route that
doesn't exist; "Try it out" never 405s.

## Problem

Three defects in the served OpenAPI document (operator-reported, 2026-07-03):

1. **Phantom insert path.** `openapi_gen.rs` emits a generated
   `POST /objects/{Type}` per type, but no such route exists — typed writes go
   through `POST /actions/{action_name}` (`query-api/src/http.rs:97`). The op's
   own description admits it. This was the recorded deferral
   `#fut-openapi-per-type-insert-route`: the generator's inputs are
   `(types, links)` only, so it couldn't name real action paths.
2. **Wrong grouping.** Generated ops tag `"objects"` / `"actions"` / `"links"`
   (`openapi_gen.rs:137,159,175`), so Scalar splits one type's operations
   across three unrelated sections. Each type should be its own section.
3. **Undocumented runtime surface.** The `/auth/*` (login, logout, password,
   service-accounts + tokens) and `/admin/*` (users, disable/enable, password
   reset, models, roles, grants) routes served by `service_runtime` carry no
   utoipa annotations at all — they exist in no service's `/openapi.json`.

Scope call (operator): this slice fixes 1–3 (the "A+B" scope). Genuinely new
endpoints (dataset listing, per-type schema read, ontology mutation, link
CRUD, grant list/revoke, user↔role assignment) are recorded as a FUTURE idea
(`#fut-api-management-crud`), spec'd separately.

## Design

### A1 — `list_actions` on the Ontology concern

The generator needs the ontology's actions. Add to the `Ontology` trait
(`control-plane/core/src/ontology.rs`, beside `define_action`/`get_action` at
:621–625), mirroring `list_types` exactly:

```rust
/// Page through every defined action, ordered by name.
async fn list_actions(&self, page: PageReq) -> Result<Page<ActionDef>>;
```

Implemented on **three** implementors: the memory adapter (map scan, name-
sorted), the postgres adapter (`select name from ontology.action order by
name` + per-name `get_action`, exactly `list_types`'s shape at
`postgres/src/ontology.rs:209`; `.sqlx` refreshed via
`tools/sqlx-prepare.sh`), and the **engine-wire proxy** `WireOntology`
(`query-api/src/wire_control_plane.rs:116`) — a trait method must land on all
implementors, and the ontology read surface stays symmetric over the wire
(the docs endpoint itself reads the direct control plane,
`query-api/src/serve.rs:76–83`), so the RPC gets the full `ListTypes` treatment
(`engine_control.proto:27,149–150`): a `ListActions` message pair with
`page_json` envelopes, the client macro method `gov_list_actions`, and the
engine-service handler beside `list_types` (`engine/src/service.rs:505`).
Like `list_types`, adapters return the full set in one page (`next: None`),
name-ordered; `PageReq` is accepted for future keyset paging. Contract-tested
in testkit's `ontology_contract` (runs against both store adapters).

### A2 — generator: real action ops, per-type tags

`ontology_openapi` gains the actions input:

```rust
pub fn ontology_openapi(
    types: &[ObjectType],
    links: &[LinkDef],
    actions: &[ActionDef],
) -> (Paths, BTreeMap<String, RefOr<Schema>>)
```

- **Delete `insert_op` and the phantom `POST /objects/{Type}`.** For each
  action whose `target` type is in the snapshot (same skew guard as links),
  emit `POST /actions/{action.name.0}`:
  - **Request schema from the action's `ParamDef`s** (not the type's read
    schema): each parameter → `property_schema(&p.ty, p.required)`; the
    OpenAPI `required` array lists the `required: true` parameter names.
  - **Summary/description by `ActionKind`**: Insert → "Insert a {Target}",
    Update → "Update a {Target} (identity-targeted PATCH)", Delete →
    "Delete a {Target} by identity".
  - **Responses**: every kind → `201` with `$ref` to the target's component
    schema — `post_action`'s single Ok arm responds `StatusCode::CREATED`
    regardless of kind (`http.rs:695`), and the document follows the handler
    (delete's body is the row's pre-deletion values). Kind-true statuses for
    Update/Delete are a registered follow-up, not this slice (no behavior
    changes). All kinds also document `400`, `403`, `404`, `422` in line with
    the static `post_action` annotation (`http.rs:654–666`).
  - An action path collides with nothing: the static doc documents the
    parameterized `POST /actions/{action_name}` template; generated concrete
    paths coexist the same way `/objects/{type_name}` coexists with
    `/objects/Customer` today.
- **Per-type tags**: generated list op, action ops, and link ops all tag the
  **type name** (`get_objects_op`/`action_op` → `target`; `link_op` → the
  `from` type). One Scalar section per type, holding List + its actions + its
  outbound traversals. Static parameterized routes keep their generic tags.
- `live_openapi` drains actions next to types (same `MAX_TYPE_PAGES`-bounded
  loop over `list_actions`); on a read error it logs and proceeds with the
  types/links it has (never fails the document).

### B — runtime doc fragments for `/auth/*` + `/admin/*`

The handlers and DTOs live in `service_runtime`, so the annotations must too:

- Annotate the runtime handlers with `#[utoipa::path(...)]` (the exact style
  of `query-api/src/http.rs:169–184`) and derive `ToSchema` on the request/
  response DTOs (`LoginReq`, `LoginResp`, `ChangePasswordReq`,
  `CreateAccountReq`, `MintTokenReq`, `CreateUserReq`, `CreateUserResp`,
  `ListUsersResp`/`UserView`, `ResetPasswordReq`, `CreateRoleReq`, `GrantReq`,
  `DefineModelReq`/`TableReq`/`PropReq`). Every documented op that sits behind
  `require_auth`/`require_admin` carries `security(("bearer_auth" = []))`;
  `POST /auth/login` alone is unauthenticated. Passwords appear only as
  write-only request fields; the two deliberate secret-bearing responses are
  `LoginResp` and `MintTokenResp` (the single moment each token is shown) —
  no other response DTO echoes a secret.
- Export one `#[derive(OpenApi)]` fragment per router family, matching the
  mount functions services already choose from:
  - `service_runtime::auth_openapi()` — `/auth/login`, `/auth/logout`,
    `/auth/password` (tag `auth`);
  - `service_runtime::service_account_openapi()` — `/auth/service-accounts*`
    (tag `service-accounts`);
  - `service_runtime::admin_openapi()` — `/admin/*` (tag `admin`).
- Each service merges exactly the fragments for the routers it mounts
  (utoipa's `OpenApi::merge`): query-api (`serve.rs:57–74`) merges all three
  into `build_openapi()`; ingest (`serve.rs:42–47`) merges auth +
  service-accounts (it mounts no admin router). The docs stay truthful
  per-service by construction; a future service that mounts admin routes
  merges the admin fragment.

### Status-code fidelity

Documented responses come from the handlers as they are (grounded 2026-07-03):
login `200`/`401`; logout `200`; password change `200`/`403`; create-account
`200` always (idempotent); mint-token `200` (body `{token, token_id}`) plus
`400` over-cap/zero TTL; revoke-token `200` plus `400` bad hex; create-user
`201`/`200`/`400`; disable/enable `200`; reset-password `200`/`404`;
create-role `201`;
list-roles `200`; grant `201`; define-model `201`. If implementation finds a
handler returning something else, the doc follows the handler — this slice
changes no runtime behavior.

## Testing

- **Contract**: testkit `list_actions` contract (both adapters) — ordering,
  keyset paging, empty.
- **Generator** (`tests/openapi_gen.rs`): update `generates_per_type_operations`
  — assert the phantom `POST /objects/{Type}` is **gone**, real
  `POST /actions/{name}` ops appear with param-derived request schemas and
  kind-correct status codes; new assertions that every generated op's tag is
  its type name; skew guard (action targeting an unknown type is skipped).
- **Liveness** (`live_doc_reflects_defined_types_without_restart` pattern):
  define a type + an action against the memory CP, assert the live doc carries
  `/actions/{name}`.
- **Route sets**: `documents_exactly_the_expected_routes` in query-api and
  ingest gain the merged runtime routes (query-api: +auth+service-accounts+
  admin; ingest: +auth+service-accounts, no `/admin/*`); a runtime-side test
  asserts each fragment documents exactly its router's routes, bearer scheme
  present, and no response schema carries a password/token field except
  mint-token's response.

## Non-goals

- New endpoints of any kind (`#fut-api-management-crud` — the C matrix:
  dataset listing, per-type schema read, ontology update/delete, link
  definition CRUD, an HTTP route for `define_action`, grant list/revoke,
  user↔role assignment, role deletion).
- Ontology ops in ingest's document (`#fut-ingest-ontology-openapi`) and
  per-subject catalog filtering (`#fut-openapi-per-subject-catalog`) — stay
  deferred.
- Removing the static parameterized templates (`/objects/{type_name}`,
  `/actions/{action_name}`) — they document the route shape for undiscovered
  types/actions and keep the doc useful when the ontology read fails.

## Register outcome

- Mint `road-api-docs-coverage` (this spec) and remove
  `#fut-openapi-per-type-insert-route` (subsumed: the phantom POST is deleted
  and replaced by real action ops).
- Add FUTURE `#fut-api-management-crud` recording the C matrix (prose above).
- The landing PR closes `road-api-docs-coverage`, documents the capability in
  `docs/system-capabilities/query-api.md` (docs surface) +
  `control-plane.md` (`list_actions`), and updates `ui.md`'s note about
  `/openapi.json` component schemas if stale.
