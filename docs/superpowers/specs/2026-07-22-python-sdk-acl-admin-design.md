# Python SDK ACL Admin Surface (`client.admin.roles`) Design

> **Status:** planned (`road-python-sdk-acl-admin`, promoted 2026-07-22 from
> `fut-python-sdk-acl-admin`). Follow-on slice to the v1 SDK
> (`2026-07-21-python-sdk-v1-design.md`, shipped as `road-python-sdk-v1`).

## Problem

`loom-sdk` v1's `client.admin` namespace wraps only the ontology-definition
endpoints (`define_model`/`define_link`). loom's ACL is deny-by-default even for
the `admin` role — holding `ADMIN_ROLE` only gates the `/admin/*` routes
(`service_runtime::require_admin`); it carries no implicit grants — so a Python
caller standing up a fresh instance must drive the roles/grants endpoints with
raw `httpx`. The SDK's own e2e smoke test does exactly that
(`src/sdk/python/tests/e2e_smoke_test.py::_grant_acl`): raw
`POST /admin/roles`, `PUT /admin/users/{u}/roles/{r}`, and
`POST /admin/roles/{r}/grants` against the query-api base URL. That raw block is
the motivating evidence: bootstrapping governance from Python is a first-class
SDK job.

## Decision (operator, 2026-07-22): full roles+grants lifecycle, no policies

Scope decision taken in the planning session:

- **Wrap the whole roles + grants + user-role lifecycle** — writes *and* the
  verification reads (list roles, list a role's grants, list a user's roles) and
  the removals (delete role, revoke grant, unassign role). All nine endpoints
  exist today on `service_runtime::admin.rs`; the SDK adds no new server surface.
- **Row-level policies stay deferred** (`/admin/roles/{r}/policies` POST/GET/
  DELETE). Their request shape is much richer (`RowFilter` predicate trees:
  `Compare`/`And`/`Or`/`Not`, ranges, lengths) and nothing in the SDK's write
  path needs them to bootstrap an instance. A follow-on FUTURE item records this.
- **Hand-written, sans-IO, dual-shell** — same conventions as v1 (no codegen).

## Surface

One new sub-namespace on both shells, name matching the deferral prose:

```python
client.admin.roles.create(role) -> str          # POST   /admin/roles            (201, echoes role)
client.admin.roles.list() -> list[str]          # GET    /admin/roles            ({"roles": [...]})
client.admin.roles.delete(role) -> None         # DELETE /admin/roles/{role}     (idempotent, cascades)

client.admin.roles.assign(role, username) -> None    # PUT    /admin/users/{u}/roles/{r}  (idempotent)
client.admin.roles.assigned(username) -> list[str]   # GET    /admin/users/{u}/roles      ({"roles": [...]})
client.admin.roles.unassign(role, username) -> None  # DELETE /admin/users/{u}/roles/{r}  (idempotent)

client.admin.roles.grant(role, action, *, type=None, table=None) -> None    # POST   /admin/roles/{r}/grants (201)
client.admin.roles.grants(role) -> list[GrantEntry]                         # GET    /admin/roles/{r}/grants
client.admin.roles.revoke(role, action, *, type=None, table=None) -> None   # DELETE /admin/roles/{r}/grants (idempotent)
```

- All routes go to `query_url` (the `/admin/*` router lives on query-api), like
  the existing `client.admin.*` methods.
- `grant`/`revoke` take `action: str` (`"read"`/`"write"` wire tokens,
  passed through — the server 400s anything else) and an exactly-one-of target:
  `type="Customer"` **or** `table=("schema", "name")`. The exactly-one-of rule
  is enforced client-side with `ValueError` before any request is built,
  mirroring the server's 400 (`parse_target`); the tuple is serialized to the
  wire's `{"schema": ..., "name": ...}` object.
- `delete`/`revoke`/`unassign`/`assign` are idempotent server-side; the SDK
  surfaces them as `None`-returning calls (a 404 for an unknown user/role still
  raises `NotFoundError` via the existing error mapping).

### `GrantEntry` (new frozen dataclass in `models.py`)

The wire row is `{"action": str, "target": <PolicyTarget serde shape>, "effect":
str}` where target is `{"Type": "Widget"}` or `{"Table": {"schema": ...,
"name": ...}}`. The SDK flattens the enum shape into optional fields, reusing
the existing `TableRef`:

```python
@dataclasses.dataclass(frozen=True)
class GrantEntry:
    action: str                  # "read" | "write"
    effect: str                  # "allow" | "deny" wire token
    type: str | None             # set iff the target is a Type
    table: TableRef | None       # set iff the target is a Table
```

Exactly one of `type`/`table` is non-`None` per entry. The existing `_core.py`
parsers trust the wire (a missing key surfaces as `KeyError`); the tagged-enum
target decode is the one branch point — an unrecognized variant raises
`ValueError` naming the shape rather than guessing.

## Implementation shape (v1 conventions, restated as requirements)

- **`_core.py`** gains pure request builders + parsers: `create_role_request`,
  `parse_create_role`, `list_roles_request`, `parse_role_list` (shared by
  role-list and user-role-list — both return `{"roles": [...]}`),
  `delete_role_request`, `assign_role_request`, `user_roles_request`,
  `unassign_role_request`, `grant_request`, `parse_grants`, `revoke_request`,
  plus a shared `grant_payload(action, type, table)` doing the exactly-one-of
  validation. No I/O, no httpx.
- **`client.py` / `aclient.py`** each gain a `_RolesNamespace` exposed as
  `client.admin.roles`; methods are thin line-identical send-and-parse pairs,
  duplicated sync/async by design (the only place duplication is allowed).
- **`models.py`** gains `GrantEntry`.
- **Errors**: no new error types — the existing status mapping
  (401 `AuthError`, 403 `ForbiddenError`, 404 `NotFoundError`, 400
  `RequestError`) already covers this surface.

## Testing

- **Unit** (`tests/admin_test.py` or a sibling `acl_admin_test.py`, mock
  transport, both shells): request shape per method (path, verb, JSON body,
  auth header, query-api base), parser round-trips for `parse_role_list` /
  `parse_grants` (both target shapes), client-side `ValueError` on
  zero-or-two-of `type`/`table`.
- **E2e dogfood** (the acceptance criterion): `e2e_smoke_test.py::_grant_acl`
  drops its raw `httpx.AsyncClient` block and drives
  `client.admin.roles.create/assign/grant`; a follow-up read via `.grants()` /
  `.assigned()` asserts the writes landed. The raw-httpx workaround this item
  exists to delete must not survive the PR.
- RE-pinned like all python tests (`remote_execution = RE_TEST_PROPS`).

## Out of scope

- `/admin/roles/{r}/policies` (row-level security) — stays deferred; recorded
  as `fut-python-sdk-policy-admin` in FUTURE.
- `/admin/users` lifecycle (create/list/disable/enable/password) — user
  provisioning is a different concern from ACL bootstrap; not needed by the
  smoke test (`create-admin` CLI covers it). Defer until a caller needs it.
- Role inheritance edges — no server admin route exposes them today.
