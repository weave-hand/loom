# Admin-gated user provisioning — create / list / disable users

- **Date:** 2026-06-30
- **Area:** acl
- **Register items:** mints [[road-auth-user-management]]; records [[fut-auth-acl-provisioning-tx]]; reuses gate shared with [[road-auth-service-tokens]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

An operator can **provision users** through the API: the bootstrap admin creates a new
user (with an initial password and starting roles), lists who exists, and disables a user
who should no longer have access. This makes the bootstrap admin a *real* admin — not just
"the first account that can log in" — and gives loom a day-one identity-administration
surface on top of the [[road-auth-password-session]] foundation.

## Current state

[[road-auth-password-session]] (PR #195) shipped the `auth` concern: `Auth`
(`control-plane/core/src/auth.rs`) with `create_user`, `find_password_credential`,
`create_session`/`resolve_session`/`revoke_session`, `has_any_user`; `POST /auth/login`
(shared `service_runtime::login_routes`); and `require_auth` middleware that resolves a
bearer to a verified `Subject` (the sole `Unauthorized`/401 producer).

`service_runtime::bootstrap_admin` (`runtime/src/auth.rs:210`) seeds the first user iff
the store is empty — **subject id = username, and it grants nothing** ("role assignment
stays an operator ACL task"). Two gaps follow:

- **There is no admin notion in code.** No `require_admin`, no admin role, no admin
  capability. The bootstrap admin is just the first `auth.user` row. So "admin at launch"
  today means *can authenticate*, not *can administer*.
- **There is no way to create a second user** except re-seeding an empty store. No
  list, no disable. The `Acl` concern grants roles to subjects, but nothing **creates the
  subjects/credentials** an operator hands out.

The planned [[road-auth-service-tokens]] already assumes an admin gate exists ("admin-gated
management routes … gates on the **bootstrap admin** subject"). This slice **establishes
that gate** so both management surfaces share it.

Relevant seams: `Acl` (`control-plane/core/src/acl.rs`) has `define_subject`,
`define_role`, `assign_role` (idempotent), `grant`. The cross-concern `Tx`
(`control-plane/core/src/transaction.rs`) covers **catalog/queue/lineage only** — it has
no `auth` or `acl` write ops, and `ControlPlane` does not even expose `auth()`. So
auth-writes and acl-writes are **separate autocommit operations** today; there is no
single transaction spanning `create_user` + `assign_role`.

## Design

### The admin gate (new shared primitive)

A `service_runtime::require_admin` gate (extractor or middleware, layered after
`require_auth`): a request is admin **iff its verified `Subject` equals the configured
bootstrap-admin identity** (`LOOM_BOOTSTRAP_ADMIN_USERNAME`, the same value
`bootstrap_admin` seeds). A non-admin subject on an `/admin/*` route gets **403**
(authenticated but not authorized — distinct from `require_auth`'s 401). This is the
**single admin notion** the slice introduces; it is deliberately the *config-named
bootstrap admin*, not a first-class ACL capability — generalizing to "any subject granted
a manage-principals capability" is [[fut-auth-admin-capability]], reused identically by
[[road-auth-service-tokens]].

### Routes (shared `admin_routes`, mounted by query-api, all behind `require_admin`)

A shared `service_runtime::admin_routes` module (mirroring `login_routes`), mounted by
query-api (the governance service):

- **`POST /admin/users`** — body `{ username, password, roles: [RoleId..] }`. Creates the
  user, defines its ACL subject, assigns the listed roles (sequenced — see below). 201 on
  full success.
- **`GET /admin/users`** — paginated list of users (username, subject id, disabled state,
  created-at); never returns the password verifier.
- **`POST /admin/users/{username}/disable`** and **`POST /admin/users/{username}/enable`**
  — deactivate / reactivate a user.

### Initial credential — admin-set password

`POST /admin/users` takes an **initial password**, hashed server-side via the existing
`hash_password` and stored through `create_user` (exactly the bootstrap path). The user
logs in with it via the existing `/auth/login`. Password **reset / rotation / forced
change / lockout** are explicitly out — [[fut-auth-password-lifecycle]].

### Bundled roles — sequenced, idempotent, not cross-concern-atomic

The route provisions a *usable* user in one call by assigning starting roles. Because the
`Tx` seam has no auth+acl unit of work, this is a **sequence of autocommit operations**:

1. `create_user` (auth);
2. `define_subject` (acl, idempotent);
3. `assign_role` per listed role (acl, idempotent).

Contract:

- **Listed roles must pre-exist** (role creation is a separate admin/ACL op, `define_role`
  + `grant`); an unknown role is a 4xx that creates nothing new beyond what already ran.
- Because `define_subject`/`assign_role` are idempotent, a **retried identical POST is
  safe** after a partial failure: the only non-idempotent step is `create_user`, so the
  route treats "user already exists" on retry as continue-to-grant rather than a hard
  conflict (a create-or-complete-grants semantics), letting the admin recover a
  half-applied provision by re-POSTing.
- On a mid-sequence failure the response **names what completed** (user created? which
  roles assigned?) so the admin can finish via the ACL API.

True single-transaction provisioning (create + grant commit-or-rollback together) needs a
`Tx`-seam extension spanning auth+acl — recorded as [[fut-auth-acl-provisioning-tx]], not
built here.

### Disable semantics

A new **deactivation column** on `auth.user` (e.g. `disabled_at timestamptz NULL`;
migration). Disabling a user:

- **revokes all that user's sessions** immediately (so disable takes effect now, not at
  session expiry), and
- **login rejects a disabled user** (the login path checks the flag; a disabled user
  cannot obtain a new session).
- `resolve_session` additionally rejects a disabled user's bearer as **defense in depth**
  (covers any session minted in a race with the disable).

Enable clears the flag (does not resurrect revoked sessions — the user logs in afresh).
**Hard delete** is out (removing a subject that holds ACL grants / appears in lineage needs
cascade handling); disable is the safe revoke primitive.

### `Auth` trait additions

`list_users(page) -> Page<UserSummary>`, `set_user_disabled(username, bool)`, and the
disabled flag honored by the login path and `resolve_session`. Implemented on **both
adapters** (memory fake + postgres, with `.sqlx` refresh) and contract-tested in testkit.
`create_user` is reused as-is.

### Decided (not open)

- **Config-named bootstrap admin gate** (not a first-class capability) — consistent with
  [[road-auth-service-tokens]]; [[fut-auth-admin-capability]] generalizes both later.
- **Admin-set initial password** (not invite token) — reuses the bootstrap credential
  path; lifecycle is [[fut-auth-password-lifecycle]].
- **Roles bundled into create, sequenced + idempotent** (not a separate ACL-only step) —
  one call yields a usable user; cross-concern atomicity deferred to
  [[fut-auth-acl-provisioning-tx]].
- **Disable, not delete** — the safe revoke primitive.
- **403 (not 401) for a non-admin on `/admin/*`** — authenticated-but-unauthorized.

## Scope

In scope:

- `service_runtime::require_admin` gate (config-named bootstrap admin) + 403 path.
- Shared `admin_routes` (create / list / disable / enable), mounted by query-api behind
  the gate.
- `Auth` additions: `list_users`, `set_user_disabled`; disabled honored in login +
  `resolve_session`; the `auth.user` deactivation migration; both adapters + testkit
  contract.
- Bundled-role create (sequenced create→define_subject→assign_role, idempotent,
  pre-existing roles), with partial-failure reporting.

Out of scope:

- **First-class admin ACL capability** ([[fut-auth-admin-capability]]) — multi-admin via a
  manage-principals grant.
- **Cross-concern atomic provisioning** ([[fut-auth-acl-provisioning-tx]]) — a `Tx`-seam
  extension covering auth+acl.
- **Password lifecycle** ([[fut-auth-password-lifecycle]]) — reset, rotation, forced
  change, lockout, rate-limit.
- Hard user delete; invite/set-password-later token flow; self-service profile/password
  change; mounting `admin_routes` on ingest (query-api is the governance surface).
- Granting **data** access to the bootstrap admin — admin-of-auth is distinct from
  data-read grants; the admin still grants itself object access via the ACL API.

## Testing

`loom_fixture_test` (auth/acl require postgres) + testkit contract (both adapters):

1. **Admin gate:** the bootstrap admin reaches `/admin/*`; a non-admin authenticated
   subject gets **403**; an unauthenticated request gets **401** (the `require_auth`
   layer).
2. **Create + login:** `POST /admin/users` with a password → the new user can
   `POST /auth/login` and obtain a session; the verifier is never returned by any read.
3. **Bundled roles:** create with `roles:[r1,r2]` (pre-existing) → the user is assigned
   both (verified via `Acl`); a retried identical POST is a no-op success (idempotent);
   an unknown role is a 4xx.
4. **List:** `GET /admin/users` returns the seeded admin + created users with disabled
   state, paginated, no verifier.
5. **Disable:** disable a user → their existing session is rejected immediately
   (`resolve_session`) **and** a fresh `/auth/login` is denied; enable → login works again.
6. **Contract parity:** the new `Auth` methods behave identically on the memory fake and
   postgres (testkit), including the disabled-honoring login/resolve.

## Risk

- **Auth-surface change touching login/session** (the disabled flag gates authentication);
  mitigated by the immediate session-revoke + login-check + resolve-check belt-and-braces
  (test 5) and the contract parity test (6). The flag defaults NULL/active, so existing
  users are unaffected by the migration.
- **Non-atomic bundled provisioning** can leave a user with partial roles on failure;
  bounded by idempotent retry (re-POST completes it), pre-existing-role validation, and
  explicit partial-success reporting — with the atomic path deferred, not hand-waved
  ([[fut-auth-acl-provisioning-tx]]).
- **Single config-named admin** is a deliberate floor, not a limitation oversight —
  [[fut-auth-admin-capability]] is the planned generalization, shared with
  [[road-auth-service-tokens]] so the two management surfaces stay consistent.
- Reuses the proven `create_user`/`hash_password`/session machinery; the new routes are
  additive and gated, so non-admin and unauthenticated paths are unchanged.
