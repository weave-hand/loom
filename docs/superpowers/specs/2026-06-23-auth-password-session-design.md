# Password login, sessions, and the authentication seam

_Design spec. 2026-06-23._

## Context

loom has authorization but no authentication. Every HTTP handler derives the
request's principal by reading the **self-asserted** `X-Loom-Subject` header and
falling back to `"anonymous"`
(`src/services/query-api/src/http.rs:55-59`, repeated at lines 111, 182, 276,
337, 576). Whoever sets the header *is* that subject — there is no proof. The
ACL concern then evaluates grants/policies against that string
(`src/services/query-api/src/handler.rs:187`). The reserved
`ControlPlaneError::Unauthorized` variant
(`src/control-plane/core/src/error.rs:23-24`) documents this exact gap — "the
caller is not authorized … reserved for the Step-3 service auth layer" — and has
**no producer** anywhere in the tree. The `Subject` wrapper
(`src/services/query-api/src/handler.rs:26`) is explicitly a placeholder: "authn
is a later spec; carried from a request header."

This is the first slice of the `[[fut-loom-auth]]` umbrella. The operator's
intent is a full identity stack (passwords, sessions, OTP, passkeys, SAML), but
that is **five independent subsystems** and loom builds in tight vertical
slices. This spec covers only the **foundation that the others plug into**:
password login, server-side sessions, and the authentication middleware that
replaces header self-assertion with a *verified* `SubjectId`. TOTP, passkeys,
SAML, and service-account tokens are sequenced follow-ons
(`[[fut-auth-totp-mfa]]`, `[[fut-auth-passkeys]]`, `[[fut-auth-saml]]`,
`[[fut-auth-service-tokens]]`) that reuse this slice's credential store, session
model, and middleware.

The relationship to the existing ACL concern is the design's spine: **`auth`
answers "who are you" (authentication); `acl` keeps "what may you do"
(authorization), unchanged.** They join by `SubjectId`. A loom *user* is an ACL
subject that has credentials. ACL's trait, role closure, grants, and policies
need no modification — authentication merely populates a *trustworthy* subject
upstream of the unchanged `Acl::check`.

## Current state

- **`SubjectId(pub String)`** (`src/control-plane/core/src/acl.rs:21-22`) — "a
  user or service account." Opaque; carries no credentials. ACL owns
  `define_subject` and the `acl` schema.
- **Subject resolution** — every query-api route parses `X-Loom-Subject` →
  `Subject(SubjectId(..))` with an `"anonymous"` default. Ingest
  (`src/services/ingest/src/http.rs:101-192`) reads **no** subject at all (it is
  unauthenticated and unauthorized today).
- **`ControlPlaneError::Unauthorized`** — defined, compile-asserted
  (`src/control-plane/core/tests/error_display.rs:16`), never returned.
- **Five existing concerns** (queue, catalog, ontology, acl, lineage) establish
  the pattern this slice mirrors: a `core` trait, a `postgres` adapter over a
  loom-owned schema, a `memory` fake, and a `testkit` contract run against both
  backends.
- **No identity primitives exist** anywhere: no users, passwords, tokens,
  sessions, credentials, or IdP integration. (`src/control-plane/core/src/identity.rs`
  is dataset/type identity for lineage — unrelated.)
- **`service_runtime`** is shared by both service binaries and is the natural
  home for cross-cutting request middleware.

## Goals

1. A loom user can be created with a username + password; the password is stored
   only as an Argon2 verifier.
2. `POST /auth/login` exchanges username+password for an opaque **session
   token**; `POST /auth/logout` revokes it.
3. A shared **authentication middleware** resolves the bearer session token to a
   *verified* `SubjectId` and rejects an absent/invalid/expired token with
   **HTTP 401** (the first producer of `ControlPlaneError::Unauthorized`).
4. Handlers obtain the subject from the verified request context; the
   `X-Loom-Subject` self-assertion header is **removed**. ACL evaluation is
   unchanged but now runs on a trustworthy subject.
5. A bootstrap admin user is seeded from config so the system is reachable
   without a chicken-and-egg unauthenticated create-user route.

## Non-goals (explicit; deferred to follow-ons)

- **TLS** — the binaries are plain-HTTP (`[[fut-graceful-shutdown-tls]]`). Slice
  1 assumes a TLS-terminating front or dev-only use. Passwords/sessions over
  cleartext are insecure; this is an accepted pre-deployment gap, recorded here
  so the work agent does not silently ship it as production-ready.
- **TOTP / OTP, passkeys (WebAuthn), SAML** — separate credential kinds/challenges
  (`[[fut-auth-totp-mfa]]`, `[[fut-auth-passkeys]]`, `[[fut-auth-saml]]`).
- **Service-account API tokens** — the non-interactive machine credential
  (`[[fut-auth-service-tokens]]`).
- **Password lifecycle** — reset, email verification, rotation, account lockout,
  and login rate-limiting (`[[fut-auth-password-lifecycle]]`).
- **Session refresh / sliding expiry** — fixed TTL only
  (`[[fut-auth-session-refresh]]`).
- **Ingest authorization** — ingest receives the same authn middleware (a
  verified subject), but adding an ACL gate to the materialize path stays a
  separate concern (`[[fut-ingest-followups]]`).
- **A richer user profile** — beyond `(subject_id, username)`; no display name,
  email, or attributes this slice.

## Design

### The `auth` concern (a store, not a crypto engine)

A new control-plane concern `auth`, structured exactly like the existing five.
Like ACL — which stores policy but never interprets a `RowFilter` — the `auth`
trait **persists verifiers and sessions but performs no cryptography**. Hashing
and verification live in the service layer (below), so the trait stays a thin,
contract-testable store with no crypto dependency in `core`.

Proposed `core` trait (final method shapes are the work agent's to refine):

```rust
#[async_trait]
pub trait Auth {
    /// Create a user bound to `subject_id`, storing the Argon2 PHC verifier.
    /// Ensures the ACL subject exists. `Conflict` if the username is taken.
    async fn create_user(&self, user: &NewUser) -> Result<()>;

    /// Look up a username's subject + stored password verifier for login.
    /// Unknown username → `Ok(None)` (the caller must not distinguish
    /// "no such user" from "bad password" in its response).
    async fn find_password_credential(
        &self,
        username: &str,
    ) -> Result<Option<PasswordCredential>>;

    /// Persist a session: the SHA-256 of the issued token plus its expiry.
    async fn create_session(
        &self,
        subject: &SubjectId,
        token_sha256: &[u8; 32],
        expires_at: OffsetDateTime,
    ) -> Result<()>;

    /// Resolve a presented token hash to its subject, iff unexpired.
    /// Unknown/expired → `Ok(None)`.
    async fn resolve_session(
        &self,
        token_sha256: &[u8; 32],
        now: OffsetDateTime,
    ) -> Result<Option<SubjectId>>;

    /// Revoke a session (logout). Idempotent.
    async fn revoke_session(&self, token_sha256: &[u8; 32]) -> Result<()>;
}

pub struct NewUser {
    pub subject_id: SubjectId,
    pub username: String,
    pub password_phc: String, // Argon2 PHC string, computed service-side
}

pub struct PasswordCredential {
    pub subject_id: SubjectId,
    pub password_phc: String,
}
```

`create_user` ensures the ACL subject exists (it calls `define_subject` or
documents the ordering requirement) so the user is immediately a valid ACL
principal; role assignment stays an ACL operation.

### `auth` Postgres schema

A new loom-owned `auth` schema (a migration alongside the existing per-concern
schemas), roughly:

- `auth.user(subject_id text primary key, username text unique not null,
  created_at timestamptz not null default now())`
- `auth.password_credential(subject_id text primary key references auth.user,
  password_phc text not null, updated_at timestamptz not null default now())`
- `auth.session(token_sha256 bytea primary key, subject_id text not null
  references auth.user, expires_at timestamptz not null, created_at timestamptz
  not null default now())`

`subject_id` is the join to ACL's principal. The session primary key is the
token **hash**, never the raw token. Compile-time `sqlx` (with a refreshed
`.sqlx` cache per the repo's offline-build wiring) covers the new queries; the
memory fake mirrors the behavior for the contract suite.

### Crypto (service-side)

A small auth helper in the service layer (or `service_runtime`):

- **Passwords:** Argon2id. `create_user` hashes the password to a PHC string
  before calling the store; login verifies the presented password against the
  stored PHC. A constant-time verify path and a dummy-hash on unknown-username
  keep login timing uniform.
- **Session tokens:** a 256-bit secret from a CSPRNG, returned to the caller
  **once**. Only `SHA-256(token)` is stored. Tokens are high-entropy, so a fast
  hash is correct (unlike passwords, which need a slow KDF). Fixed TTL from
  config.

### Login / logout flow

- `POST /auth/login {username, password}` — **public** route. Fetch the
  credential; verify Argon2; on success mint a token, store its hash + TTL,
  return the raw token (response body and/or `Set-Cookie`; the work agent picks
  the carrier — bearer header is the baseline). On failure: a uniform
  `401`/`400` that does not reveal whether the username exists.
- `POST /auth/logout` — revoke the presented session. Authenticated route.

### Authentication middleware (the seam)

An axum layer in `service_runtime`, applied by both service binaries:

1. Read the session token (baseline: `Authorization: Bearer <token>`).
2. `SHA-256` it, `resolve_session(hash, now)`.
3. On hit → inject a verified `Subject(SubjectId)` into request extensions; an
   axum extractor hands it to handlers.
4. On miss/expired/absent (for a protected route) → return
   `ControlPlaneError::Unauthorized` → **HTTP 401**. This gives the reserved
   variant its first and only producer.

Public routes (`/auth/login`, health) bypass the gate. Every query-api handler
drops its `X-Loom-Subject` parse and reads the injected `Subject`; the header is
removed from the codebase. `Acl::check`/`policies_for` are called exactly as
before, now on a verified subject.

### Bootstrap

On boot, if `auth.user` is empty and `LOOM_BOOTSTRAP_ADMIN_USERNAME` /
`LOOM_BOOTSTRAP_ADMIN_PASSWORD` are set, seed that user (and ensure its ACL
subject). This breaks the chicken-and-egg without an unauthenticated create-user
endpoint. Absent config + empty table is a valid (locked-out) state the operator
resolves by setting the env. Creating *additional* users is an authenticated,
ACL-gated operation whose route shape is the work agent's call (it is mechanical
once the seam exists; this spec does not over-constrain it).

## Testing

- **`testkit` Auth contract** — a backend-agnostic suite (create user → find
  credential; create/resolve/revoke session; expiry boundary; unknown
  username/token → `None`; duplicate username → `Conflict`) run against **both**
  the `memory` fake and the `postgres` adapter, mirroring every existing
  concern's contract. Fixture-backed runs use `loom_fixture_test`.
- **Crypto unit tests** — Argon2 round-trip + wrong-password reject; token
  hashing; as `tests/<name>.rs` `rust_test` targets (the `no-inline-tests` hook
  forbids inline `#[test]`).
- **query-api e2e** (through `e2e_support`): login → token → governed read =
  `200`; missing token = `401`; bad password = `401`; revoked/expired session =
  `401`; verified subject still subject to ACL deny = `403`. `e2e_support::get`
  swaps `X-Loom-Subject` injection for obtaining and presenting a real session
  token, so the existing graph/object-set e2es exercise the authenticated path.
- **`.sqlx` cache** refreshed (`tools/sqlx-prepare.sh`) and the
  `sqlx-cache-check` test green for the new queries.

## Risks / decisions settled in brainstorming

- **Opaque server-side sessions over JWT** — chosen for revocability (logout,
  `[[fut-auth-password-lifecycle]]` lockout) and zero key management; the
  per-request DB lookup is acceptable at loom's scale and matches the
  stateful-control-plane grain.
- **Crypto in the service, store in the concern** — keeps `core` dependency-light
  and the trait contract-testable, consistent with ACL not interpreting filters.
- **Cleartext transport** — a real, recorded gap gated on
  `[[fut-graceful-shutdown-tls]]`; not silently shipped as production-ready.

## Register outcome

- Promote `[[fut-loom-auth]]` → `status:promoted` (umbrella).
- Mint ROADMAP `road-auth-password-session` (`area:acl status:planned`, this
  spec) linking back to `[[fut-loom-auth]]`.
- Record sequenced follow-ons in FUTURE: `[[fut-auth-totp-mfa]]`,
  `[[fut-auth-passkeys]]`, `[[fut-auth-saml]]`, `[[fut-auth-service-tokens]]`,
  `[[fut-auth-password-lifecycle]]`, `[[fut-auth-session-refresh]]`.
