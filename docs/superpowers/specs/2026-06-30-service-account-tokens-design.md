# Service-account API tokens

- **Date:** 2026-06-30
- **Area:** acl
- **Register items:** promotes [[fut-auth-service-tokens]] → mints [[road-auth-service-tokens]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

A non-interactive **machine** caller (a worker, an ETL job, an external client of the
forthcoming SQL wire) authenticates with a **bearer API token** that resolves to an ACL
`SubjectId`, exactly like a human session — but issued and managed out-of-band (no
password login) and **rotation-friendly** (every token expires; mint-new-then-revoke-old).
This is the machine-identity slice of the [[fut-loom-auth]] umbrella, built on the
[[road-auth-password-session]] foundation.

## Current state

The auth concern (`control-plane/core/src/auth.rs`, migration `0017_auth.sql`) has:

- `auth.user` (subject_id, username) + `auth.password_credential` (subject_id,
  password_phc) — the password is stored *separately* from the user row;
- `auth.session` (subject_id, token_sha256, expires_at) — opaque server-side sessions;
- `Auth` trait: `create_user`, `find_password_credential`, `create_session`,
  `resolve_session(hash, now)`, `revoke_session`, `has_any_user`;
- `service_runtime`'s `require_auth` middleware: bearer token → `resolve_session` →
  `Subject(SubjectId)`, the single producer of `ControlPlaneError::Unauthorized` (401).

There is no machine credential: every principal is a username/password user, and every
bearer token is a login session.

## Design

### Identity — a distinct `service_account` entity

A service account is its **own** entity, parallel to `auth.user` (not a flavoured user):

```
auth.service_account(subject_id PK, name UNIQUE, created_at)
```

`create_service_account` ensures the ACL subject exists (the same way `create_user` does),
so the account is immediately a valid ACL principal — role/policy grants apply to its
`SubjectId` uniformly, no special-casing in the ACL path. It has **no**
`password_credential`, so it can never password-login. Keeping it a separate table (rather
than a `kind` flag on `auth.user`) keeps the human-login invariants (`username` +
`password_credential`) untouched and makes "list service accounts" / "list users" clean,
distinct queries.

### Tokens — a distinct `service_token` table, mandatory TTL

```
auth.service_token(
  token_sha256 PK,           -- SHA-256 of the issued opaque token (plaintext never stored)
  subject_id,                -- FK -> service_account.subject_id
  label,                     -- operator-facing name (e.g. "nightly-etl")
  created_at,
  expires_at NOT NULL,       -- mandatory TTL; no never-expiring tokens
  revoked_at NULL)
```

Distinct from `auth.session` so machine credentials have their own lifecycle: labelled,
independently listable, independently revocable, and never touched by a session/logout
sweep. Multiple live tokens per account are allowed (that **is** the rotation story — mint
a fresh token, cut over, revoke the old, all overlapping).

**Mandatory TTL.** Minting requires a `ttl`; `expires_at = now + ttl`, capped at a
configurable maximum (`LOOM_SERVICE_TOKEN_MAX_TTL`, sane default e.g. 90d) — a request over
the cap is rejected (400). No immortal tokens; rotation is required by construction.

`Auth` trait gains:

- `create_service_account(&NewServiceAccount) -> Result<()>` (`Conflict` on duplicate name);
- `create_service_token(subject, token_sha256, label, expires_at) -> Result<()>`;
- `resolve_service_token(token_sha256, now) -> Result<Option<SubjectId>>` — resolves iff
  `revoked_at IS NULL AND expires_at > now`;
- `revoke_service_token(token_sha256) -> Result<()>` (idempotent);
- `list_service_tokens(subject, PageReq)` + `list_service_accounts(PageReq)` for management
  (label/created/expiry/revoked metadata — never the token).

Both adapters (memory fake + postgres) implement them, run against the shared `Auth`
contract in testkit (mirroring the session contract). Postgres uses compile-time `query!`
(refresh `.sqlx` via `tools/sqlx-prepare.sh`).

### Middleware — resolve session OR service token

`require_auth` becomes: bearer token → try `resolve_session`; if absent, try
`resolve_service_token`; the first hit injects `Subject(SubjectId)`; neither → 401. A small
`resolve_bearer` helper sequences the two so there is still exactly **one** path that
produces `Unauthorized`. Downstream handlers and ACL are unchanged — a `Subject` is a
`Subject` whether it came from a login or a token. (Order is session-first because
interactive traffic dominates; both are constant-time hash lookups, so order is
performance-only.)

### Management surface — admin-gated

Management routes live in the shared `service_runtime` auth surface (mounted by the
services alongside `/auth/login`), behind the authn middleware **and** an admin gate:

- `POST /auth/service-accounts {name}` → create; returns the account.
- `POST /auth/service-accounts/{id}/tokens {label, ttl}` → mint; returns the **plaintext
  token exactly once** (only its SHA-256 is persisted), plus `expires_at`.
- `GET /auth/service-accounts` / `GET /auth/service-accounts/{id}/tokens` → list metadata.
- `DELETE /auth/service-accounts/{id}/tokens/{token_id}` → revoke.

**Admin gate (decided):** management is restricted to the **bootstrap admin** subject
seeded by [[road-auth-password-session]] (the only admin notion that exists today). A
first-class "auth-admin" ACL capability (a new `PolicyTarget`) is a deliberate follow-on,
not this slice — recorded as a deferred item so the gate can later widen without reworking
the routes.

## Scope

In scope:

- `auth.service_account` + `auth.service_token` tables (new migration) and the five `Auth`
  trait methods above on both adapters + the testkit contract.
- `require_auth` resolving session-or-service-token via `resolve_bearer`.
- The admin-gated management routes (create account, mint/list/revoke token) in
  `service_runtime`, with the `LOOM_SERVICE_TOKEN_MAX_TTL` cap.

Out of scope:

- Wiring tokens into the **external SQL wire** ([[road-external-sql-governed-catalog]] slice
  2) — this slice provides the credential; the wire consumes it later.
- A first-class auth-admin ACL capability (new `PolicyTarget`) — deferred follow-on; this
  slice gates on the bootstrap admin.
- Token *scoping* (a token narrower than its account's grants), per-token rate limiting, and
  the other [[fut-loom-auth]] factors (MFA, passkeys, SAML, password lifecycle).

## Testing

- **`Auth` contract (both adapters)** in testkit: create account → mint token → resolve
  (hash → subject); expired token (`expires_at <= now`) → `None`; revoked token → `None`;
  unknown hash → `None`; duplicate account name → `Conflict`; list returns metadata, never
  the token. (Fixture-backed postgres run via `loom_fixture_test`.)
- **Middleware** (`service_runtime`): a request bearing a valid service token resolves to
  the account's `Subject` and passes `require_auth`; an expired/revoked/unknown token → 401;
  a valid session still works (no regression). One path produces `Unauthorized`.
- **Management e2e** (over the auth router): admin mints a token (plaintext returned once,
  not re-derivable from a later list); a non-admin subject → 403 on every management route;
  minting over `LOOM_SERVICE_TOKEN_MAX_TTL` → 400; revoke makes a previously-accepted token
  401 on the next request (resolve reflects `revoked_at`).
- **ACL parity:** an ACL grant on a service account's subject governs its token's reads
  exactly as the same grant governs a user — a service token is not privileged.

## Risk

- New credential surface is security-sensitive; mitigated by storing only the token's
  SHA-256 (plaintext shown once, never persisted — mirrors the session model), mandatory
  expiry, and revocation reflected in `resolve_service_token` (tested). The token is a
  high-entropy opaque value (same generation as session tokens).
- The middleware change touches the single 401 path; mitigated by routing both kinds through
  one `resolve_bearer` helper and the no-session-regression test.
- Admin-gating on the bootstrap admin is intentionally minimal; the non-admin-403 test pins
  it, and the deferred auth-admin capability is recorded so widening it is additive.
