# Management API surface — CRUD the read/admin matrix gaps

- **Date:** 2026-07-03
- **Area:** devx
- **Register items:** delivers [[road-api-management-crud]] (promoted from
  `#fut-api-management-crud`)
- **Status:** implementation-ready

## North star

An operator (or the UI) can administer loom end-to-end over HTTP: browse the
datasets the platform holds and one dataset's live schema, read one ontology
type's full shape, define links and actions without dropping to SQL or seeds,
see and revoke what a role can do, assign and unassign a user's roles, and
delete roles/links/actions that are no longer wanted — every route documented
in the same OpenAPI fragments #344 established, so the docs UI stays complete
by construction.

## Operator decisions (2026-07-03)

- **Type delete + schema-evolution-aware update are OUT** — deferred with the
  ARCHITECTURE.md ontology-migration open question (dangling non-FK ACL
  grants/policies and surviving inbound links make naive delete unsafe; update
  needs a data-migration story). Redefinition via the existing
  `POST /admin/models` upsert (clears + re-inserts properties, no evolution
  guard) is the documented update path for now. A guarded `delete_type` is
  recorded as the follow-on `#fut-ontology-type-delete`.
- **One lane, one PR** for everything else.

## Surface (route → concern method; NEW marks trait additions)

Grounded 2026-07-03 (post #343/#344 main):

| Route | Method behind it | Status |
| --- | --- | --- |
| `GET /ontology/types/{name}` (query-api) | `Ontology::get_type` + `links` + `links_to` | exists — surface only |
| `GET /datasets` (query-api) | `Catalog::list_tables` | **NEW read** |
| `GET /datasets/{schema}/{table}` (query-api) | `Catalog::current_snapshot` + `schema` | exists — surface only |
| `POST /admin/links` | `Ontology::define_link` | exists — surface only |
| `DELETE /admin/links/{from}/{name}` | `Ontology::delete_link` | **NEW delete** |
| `POST /admin/actions` | `Ontology::define_action` | exists — surface only |
| `DELETE /admin/actions/{name}` | `Ontology::delete_action` | **NEW delete** |
| `GET /admin/roles/{role}/grants` | `Acl::list_grants` | **NEW read** |
| `DELETE /admin/roles/{role}/grants` | `Acl::revoke` | exists — surface only |
| `DELETE /admin/roles/{role}` | `Acl::delete_role` | **NEW delete** |
| `PUT /admin/users/{username}/roles/{role}` | `Acl::assign_role` | exists — surface only |
| `DELETE /admin/users/{username}/roles/{role}` | `Acl::unassign_role` | exists — surface only |
| `GET /admin/users/{username}/roles` | `Acl::roles_of` | **NEW read** |

## New trait methods

Each lands on the concern trait + memory + postgres adapters (`.sqlx`
refreshed), contract-tested in testkit next to its concern's existing
contract, mirroring the established shapes (`list_actions`, `revoke`):

- `Catalog::list_tables(&self, page: PageReq) -> Result<Page<TableRef>>` —
  every table the mirror catalog holds, `(schema, name)`-ordered, single full
  page (`next: None`), `page` accepted-for-future like every other list.
- `Acl::list_grants(&self, role: &RoleId, page: PageReq) -> Result<Page<Grant>>`
  — the role's **coarse** grant rows, deterministic order; new core DTO
  `Grant { action: Action, target: PolicyTarget, effect: Effect }` (serde,
  like the other governance types). `NotFound` on unknown role. Fine-grained
  `Policy` listing per role stays out (only the subject-scoped `policies_for`
  exists; add when a consumer needs it).
- `Acl::roles_of(&self, subject: &SubjectId, page: PageReq) -> Result<Page<RoleId>>`
  — the subject's direct role memberships, id-ordered (the inverse of
  `has_role`; today only `list_roles` (all roles) exists).
- `Acl::delete_role(&self, role: &RoleId) -> Result<()>` — idempotent; every
  reference cascades by schema (`role_member`, `role_grant`, `role_inherits`,
  `policy` all carry `on delete cascade` — grounded in migrations 0003/0007/
  0011), so no guard is needed. The postgres impl is a single delete; memory
  removes the role and its memberships/grants/policies/inheritance edges to
  match.
- `Ontology::delete_link(&self, from: &TypeName, name: &str) -> Result<()>` —
  idempotent delete by the link's `(name, from_type)` primary key. Removes the
  definition only; backing columns/join tables are physical data, untouched.
- `Ontology::delete_action(&self, name: &ActionName) -> Result<()>` —
  idempotent; steps/params/assignments cascade (migration 0030). Queued jobs
  referencing a deleted action fail at resolve time exactly like any unknown
  action (deterministic 404/abandon) — no new failure mode.

**Wire proxies:** `WireOntology`/`WireAcl` implement the new methods. Deletes
reject with the existing `read_only(...)` error (they are admin-plane writes,
same posture as `define_*`). The new *reads* are wired only if the serving
path needs them: `GET /ontology/types/{name}` runs on query-api's `AppState.cp`
(wire-capable already via `gov_get_type`/`gov_links`/`gov_links_to` — no new
RPC). The dataset + grant + role reads are served by handlers holding the
**direct** control plane (datasets on query-api's `direct`, exactly like the
GC-enqueue precedent; grants/roles inside the runtime admin router, which is
already direct) — so no new wire RPCs are needed anywhere in this lane. If the
wire `Catalog`/`Acl` proxies must implement the new reads for trait
completeness, they reject like the writes with a clear "not wired; served
direct" error. (Widening the wire surface for management reads is deferred —
`#fut-wire-governance-cache` territory.)

## Route contracts

All follow the established runtime/query-api conventions: bearer required
everywhere; `/admin/*` additionally behind `require_admin`; explicit request/
response DTOs with `ToSchema`; utoipa annotations on every handler; the #344
fragments and per-service `documents_exactly_the_expected_routes` tests grow
accordingly (the drift guard forces this).

- `GET /ontology/types/{name}` → 200
  `{ name, table: {schema, name}, identity, properties: [{name, ty, required}],
  links: [LinkView], links_to: [LinkView] }` (`LinkView { name, from, to,
  cardinality }`); 404 unknown type. Serves the UI's Schema tab
  (`#fut-object-explorer-tabs` prose already anticipates it).
- `GET /datasets` → 200 `{ datasets: [{schema, name}] }`.
- `GET /datasets/{schema}/{table}` → 200 `{ table: {schema, name},
  snapshot_id, snapshot_time, columns: [{name, ty, nullable}] }` from
  `current_snapshot` + `schema` at it; 404 unknown table.
- `POST /admin/links` body = the `LinkDef` serde shape (it is the wire-stable
  governance type) → 201; 400 on validation (unknown endpoint types map from
  `NotFound`/`Validation` exactly as `define_model` maps them today).
- `POST /admin/actions` body = the `ActionDef` serde shape (steps included —
  post-#343 this is the canonical authored form; a bespoke DTO would just
  restate it) → 201; 400 on validation (unknown target type, malformed steps).
- `DELETE /admin/links/{from}/{name}`, `DELETE /admin/actions/{name}`,
  `DELETE /admin/roles/{role}` → 200 always (idempotent deletes, matching
  `revoke`/`unassign_role` trait semantics; body `{deleted: <echo>}`).
- `GET /admin/roles/{role}/grants` → 200 `{ grants: [{action, target, effect}]
  }` with the `PolicyTarget` rendered in its serde form; 404 unknown role.
- `DELETE /admin/roles/{role}/grants` body = the existing `GrantReq`
  (`{action, type}`) → 200 (idempotent revoke); 400 on a bad `action` string
  (same branch as the POST).
- `PUT /admin/users/{username}/roles/{role}` → 200; 404 unknown user or role
  (maps `assign_role`'s `NotFound`). `DELETE` same shape → 200 idempotent
  (unknown user still 404 — the username→subject lookup precedes the call).
- `GET /admin/users/{username}/roles` → 200 `{ roles: [String] }`; 404 unknown
  user.

## Testing

- Testkit contracts for the six new trait methods (both store adapters):
  `list_tables` ordering + presence after a commit; `list_grants` content +
  unknown-role `NotFound` + reflects `revoke`; `roles_of` after
  assign/unassign; `delete_role` cascade observed via `has_role`/`list_grants`
  + idempotence; `delete_link`/`delete_action` idempotence + reads no longer
  return the definition + re-define after delete works.
- Runtime route tests (memory adapter) per handler: happy path, 404s,
  admin-gating (non-admin 403 via the middleware — covered by mounting, spot
  asserted once), and the fragment route-set/secret/bearer tests extended.
- query-api tests: type-detail handler (200 shape incl. links, 404), datasets
  list/detail against the fixture-landed tables, and `expected()` growth in
  the openapi drift tests.

## Non-goals

- Type delete / schema-evolution update (`#fut-ontology-type-delete`, new).
- Vector-index HTTP CRUD (define/list exist on the trait, unexposed — not in
  the operator's matrix; `#fut-api-management-crud` follow-on prose).
- Fine-grained `Policy` CRUD per role over HTTP (only coarse grants here).
- Role-inheritance HTTP surface (`add/remove_role_inheritance` exist,
  unexposed — same follow-on).
- Dataset writes/deletes over HTTP; per-subject filtering of any listing
  (`#fut-openapi-per-subject-catalog` unchanged).
- New wire RPCs for management reads (served direct; see Wire proxies above).

## Register outcome

- Promote `#fut-api-management-crud` → `road-api-management-crud`
  (`status:planned`, this spec); the landing PR closes it, removes the FUTURE
  entry, mints `#fut-ontology-type-delete` (guarded delete + migration-aware
  update, carrying the dangling-reference grounding above), and documents the
  capability in `docs/system-capabilities/` (query-api reads; control-plane
  new trait surface; build-and-test docs paragraph).
