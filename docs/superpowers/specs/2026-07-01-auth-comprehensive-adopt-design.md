# Comprehensive auth — adopt an established stack for the fuller identity layer

- **Date:** 2026-07-01
- **Area:** acl
- **Register items:** delivers [[road-auth-comprehensive]]; delivers against
  [[fut-auth-totp-mfa]], [[fut-auth-passkeys]], [[fut-auth-saml]],
  [[fut-auth-login-rate-limit]], [[fut-auth-password-policy]],
  [[fut-auth-session-refresh]]; depends on [[road-auth-password-lifecycle]]
- **Status:** umbrella spec (architecture + evaluation; each slice gets its own spec)

## North star

An operator can run loom as the identity authority for a real deployment:
users log in with a second factor (TOTP or a passkey), an enterprise can federate
its IdP, brute-force attempts are throttled at the edge, passwords obey a policy
and rotate, and sessions slide instead of expiring mid-work. loom **does not
hand-roll** the cryptographic ceremonies behind any of that — it **adopts
established, pure-Rust implementations** (WebAuthn via `passkey-rs`, TOTP, OIDC
token validation, GCRA rate limiting) and plugs each into the identity spine already in
place: the `auth` concern, opaque server-side sessions, the `require_auth`
middleware, and the `SubjectId`-keyed ACL. This spec is the umbrella: the
build-vs-adopt call, the per-capability candidate evaluation, the integration
architecture, the credential-kind schema, and a sequenced slicing plan. It writes
no implementation; each slice below becomes its own spec.

## Problem / goal

[[road-auth-password-session]] (PR #195) shipped password login, opaque sessions,
and the authn seam; [[road-auth-service-tokens]] (PR #271) added machine tokens;
[[road-auth-user-management]] (PR #272) added admin provisioning; and
[[road-auth-password-lifecycle]] adds self-service change / admin reset / lockout.
That is a solid **single-factor** foundation. The committed decision
([[road-auth-comprehensive]]) is to grow it into a **fuller identity layer** — and
to do so by **adopting** proven components rather than continuing to hand-roll
security-critical protocols. The acceptance surface is six deferred `fut-auth-*`
ideas: TOTP MFA, passkeys, SAML federation, per-IP login rate-limiting, password
policy (strength + forced rotation), and session refresh.

The goal of this umbrella is to pick a **coherent, buckifiable, self-hostable set
of crates** that satisfies all six against loom's hard constraints, and to define
how they attach to the existing seam **without touching the ACL model**.

## Build vs. adopt (adopt is chosen — recap why)

The decision is **adopt**, and it is already reflected in the codebase: password
hashing was never hand-rolled — `crypto.rs` adopts `argon2` + `sha2`. What
"comprehensive auth" adds are protocols where a hand-rolled implementation is a
liability, not a differentiator: WebAuthn attestation/assertion, RFC 6238 TOTP,
OIDC ID-token validation, and GCRA rate limiting. loom has no reason to own that
code.

The load-bearing distinction is **adopt a library (crate) vs. adopt a service
(IdP)**:

- **A full external IdP** (Keycloak, Authentik, Ory Kratos/Hydra) is a **deploy
  dependency, not a library.** It is a separate long-running service with its own
  datastore — it cannot be buckified as a reindeer crate, it breaks the
  single-binary-ish apko/Wolfi + Helm deploy, and it would **displace** loom's
  `SubjectId`-centric authorization rather than plug into it. Rejected on every
  hard constraint.
- **Best-in-class Rust crates**, one per ceremony, integrated into loom's existing
  `auth` concern: buckifiable via reindeer (crates.io), self-hosted by
  construction, and additive to the session/ACL spine. **This is the chosen path.**

So "adopt" means: adopt the *ceremony crates*; keep loom's *store, session model,
middleware, and ACL* as the integration spine. loom stays the authority; the
crates supply the hard math.

## Candidate evaluation (per capability)

| Capability | Options considered | Recommendation | Blast radius / notes |
| --- | --- | --- | --- |
| TOTP / OTP ([[fut-auth-totp-mfa]]) | `totp-rs`, `otpauth`, `libreauth` | **`totp-rs`** | Small, pure-Rust (`sha2`/`base32`). Emits the `otpauth://` provisioning URI; keep QR-image features **off** (avoids `image`/`qrcode`) — the client renders the URI. Minimal reindeer footprint. |
| Passkeys / WebAuthn ([[fut-auth-passkeys]]) | `webauthn-rs` (Kanidm), `passkey-rs` (1Password) | **`passkey-rs`** (pure Rust) — **decided** | Chosen for **zero native-dep risk**: `passkey-rs` is pure Rust, so it buckifies clean with no `openssl-sys`/`links`-crate exposure to the reindeer downgrade footgun. Trade-off: less battle-tested as a *server* RP library than `webauthn-rs`, so Slice C carries more integration + security-review work (RP-ID/origin binding, attestation policy, sign-count) — acceptable against the hermetic-build guarantee. |
| Federation ([[fut-auth-saml]]) | `openidconnect` (OIDC, pure Rust), `samael` (SAML2, native) | **`openidconnect`** (OIDC) — **decided** | Federation is delivered via **OIDC**, which is pure Rust and self-hostable — no native `xmlsec`/libxml2 dependency at all. OIDC covers the mainstream enterprise IdPs (Google, Okta, Azure AD, Auth0, Keycloak). SAML-specifically (`samael` + its native XML-DSIG stack) is **dropped from scope** unless a future consumer needs a SAML-only legacy IdP — at which point it is its own deferred slice. |
| Login rate-limit ([[fut-auth-login-rate-limit]]) | `tower_governor` (tower layer over `governor`), raw `governor` | **`tower_governor`** | Small, pure-Rust GCRA limiter as an axum/tower layer in `service_runtime`, keyed by real client IP. In-memory per-process for now; distributed/Redis-backed is deferred (Open Question — multi-replica Helm). |
| Password policy ([[fut-auth-password-policy]]) | `zxcvbn` (strength estimator), home-grown rules | **`zxcvbn`** for strength + **home-grown** forced-rotation | `zxcvbn` is pure Rust (bundles a frequency dictionary — modest binary-size cost). Forced rotation is just a `password_changed_at` column + a login-time max-age check; no crate. Breach-list (HIBP) needs network → deferred. |
| Session refresh ([[fut-auth-session-refresh]]) | home-grown on `auth.session` | **Home-grown** (no new crate) | Sliding expiry / refresh is a mechanics change to the existing opaque-session table + `resolve_session`; nothing to adopt. The cheapest capability. |

Net: **five pure-Rust crates** — `totp-rs`, `tower_governor`, `zxcvbn`,
`passkey-rs`, `openidconnect` — and **one home-grown** extension (session refresh).
**Every adopted crate is pure Rust; the batch takes no native `links`-crate
dependency**, so there is no `openssl-sys`/`xmlsec`/libxml2 exposure and no reindeer
downgrade-footgun surface. This is a deliberate outcome of the two library calls.

## Integration architecture

The architecture rests on one invariant that keeps the ACL model untouched:

> **The opaque session token stays the sole bearer credential, and
> `create_session` stays the sole way one is minted. Every new authentication
> method is just a new way to *earn* a session.** Once a session exists,
> `require_auth` → `resolve_bearer` → `Subject(SubjectId)` → `Acl::check` runs
> exactly as today, indistinguishable across factors. loom's authorization model
> is not modified by any slice.

Concretely, each capability attaches at a specific point **upstream** of the
unchanged seam:

- **TOTP / passkey MFA — a gate before `create_session`.** Today `login`
  (`runtime/src/auth.rs:128`) verifies the password and immediately calls
  `create_session`. MFA inserts a step: on a correct password, if the user holds a
  second-factor credential, mint a **short-lived, single-purpose "MFA-pending"
  challenge token** (stored server-side, not a real session) instead of a session.
  A second route (`POST /auth/mfa/verify`) checks the TOTP code or WebAuthn
  assertion against the pending challenge and, on success, calls the **existing**
  `create_session`. The session model and `require_auth` are unchanged.
- **Passkeys — two modes, same terminus.** WebAuthn works as a **second factor**
  (assertion after password, reusing the MFA challenge machinery) or as a
  **passwordless primary** (assertion is the sole factor). Both end at
  `create_session`. Registration (attestation) is an authenticated ceremony that
  stores a serialized `passkey-rs` credential for the subject.
- **OIDC federation — an alternate session origin.** The relying party runs the
  OIDC authorization-code flow (`openidconnect`), validates the returned ID token
  (issuer, audience, signature via the IdP's JWKS, nonce, expiry), maps the token's
  `sub` (issuer + subject) to a loom `SubjectId` via a federated-identity binding,
  and — if mapped — mints a session via `create_session`. Federation bypasses
  password + MFA (the IdP asserts the factors). JIT user provisioning on first login
  is an Open Question.
- **Rate-limit — an edge layer, no `auth` involvement.** A `tower_governor` layer
  in `service_runtime` in front of `/auth/login` (and the MFA-verify route),
  keyed by client IP, returning 429 over the cap. It sees no `SubjectId` and stores
  no credential — purely a request-shaping layer. Complements the per-**account**
  lockout from [[road-auth-password-lifecycle]] with per-**IP** protection.
- **Password policy — checks in the set/verify paths.** Strength (`zxcvbn`) is a
  set-time check in create-user / change-password / admin-reset; forced rotation is
  a login-time max-age check against `password_changed_at` that, when exceeded,
  issues a "must-change" challenge rather than a session (same challenge machinery
  as MFA).
- **Session refresh — inside `resolve_session`/a renew route.** Sliding expiry
  extends `expires_at` on use (bounded by an absolute max lifetime); or an explicit
  refresh route re-mints. All on the existing `auth.session` table.

Every arrow points at `create_session`; nothing points at `Acl`.

## Credential-kind schema

New credential kinds live in the loom-owned `auth` schema **alongside**
`auth.password_credential`, following the precedent set by service tokens
([[road-auth-service-tokens]]): **distinct per-kind tables keyed by `subject_id`,
not a `kind`-flag column on a shared row.** This keeps the password invariants
untouched, makes "list a subject's passkeys / TOTP enrollment / federated
identities" clean, distinct queries, and lets each crate persist its own opaque
credential state without a polymorphic blob. Sketch (final shapes are each slice's
to refine):

- `auth.totp_credential(subject_id PK → auth.user, secret_enc, confirmed_at,
  created_at)` — the shared secret (encrypted at rest; key management is a slice
  decision), confirmed only after a first successful code.
- `auth.webauthn_credential(credential_id PK, subject_id → auth.user,
  passkey_state jsonb, sign_count, label, created_at)` — one row per registered
  authenticator (a subject may hold several); `passkey_state` is the serialized
  `passkey-rs` credential.
- `auth.federated_identity(issuer, external_subject, subject_id → auth.user,
  created_at, PRIMARY KEY (issuer, external_subject))` — the OIDC `(iss, sub)` → loom
  subject binding.
- `auth.mfa_challenge(challenge_sha256 PK, subject_id, purpose, expires_at,
  created_at)` — the short-lived pending-MFA / must-change-password token; hashed,
  never the raw value, mirroring `auth.session`.
- Column adds on `auth.password_credential` / `auth.user` for policy + refresh:
  `password_changed_at`; a session `last_seen_at` / absolute-max column for sliding
  expiry.

The `Auth` trait gains per-kind store/list/resolve methods, each implemented on
**both** adapters (memory fake + postgres, `.sqlx` refreshed via
`tools/sqlx-prepare.sh`) and contract-tested in `testkit` — the exact pattern
every concern already follows. Crypto (TOTP verify, WebAuthn ceremony, OIDC
token validation) stays in the service layer; the trait remains a pure store, consistent
with `auth` today.

## Slicing plan

Each slice is independently shippable, lands its own spec, extends the `Auth`
contract on both adapters, and reuses the challenge machinery the prior slice
introduced. Sequenced cheapest-first / lowest-blast-radius-first.

- **Slice A — rate-limit + password policy + session refresh.** No new credential
  kind, no ceremony crate beyond `zxcvbn` + `tower_governor`. Delivers
  [[fut-auth-login-rate-limit]], [[fut-auth-password-policy]],
  [[fut-auth-session-refresh]]. Adds the `tower_governor` edge layer, the
  `zxcvbn` strength gate + `password_changed_at` forced-rotation "must-change"
  flow, and sliding-expiry sessions. Cheapest; proves the adopt-a-layer / adopt-a-
  small-crate path and establishes the "challenge instead of session" pattern.
  Depends on [[road-auth-password-lifecycle]].
- **Slice B — TOTP MFA.** First new credential kind (`auth.totp_credential`) and
  first true MFA challenge (`auth.mfa_challenge`): enroll → confirm → second-factor
  login via `POST /auth/mfa/verify` → `create_session`. Adopts `totp-rs`. Delivers
  [[fut-auth-totp-mfa]]. Establishes the two-step login the passkey second factor
  reuses.
- **Slice C — passkeys / WebAuthn.** Reuses Slice B's challenge machinery; adds the
  registration (attestation) and authentication (assertion) ceremonies and the
  `auth.webauthn_credential` store, in both second-factor and passwordless-primary
  modes. Adopts **`passkey-rs`** (pure Rust). Delivers [[fut-auth-passkeys]].
  Sequenced after TOTP because it is heavier (RP-ID/origin config, attestation
  policy, sign-count handling) and, with the less-battle-tested server RP path,
  carries the deepest integration + security-review load of the batch.
- **Slice D — OIDC federation.** Federated-identity mapping table
  (`auth.federated_identity` on `(issuer, subject)`) + OIDC authorization-code flow
  and ID-token validation + session mint, plus the JIT-provisioning decision.
  Adopts **`openidconnect`** (pure Rust). Delivers [[fut-auth-saml]] (the federation
  capability, via OIDC). Sequenced last as the most infrastructure-shaped slice
  (redirect flow, IdP registration/config, JWKS handling) — but with **no native
  dependency**, since SAML-specifically is out of scope.

## Constraints & risks

- **Hermetic build / reindeer blast radius.** Every crate must buckify from
  crates.io. With the two library calls made (`passkey-rs`, `openidconnect`), **all
  five adopted crates are pure Rust** — no native `links` crate enters the tree, so
  the batch is free of the `openssl-sys`/`xmlsec`/libxml2 exposure and the documented
  "`reindeer update` can downgrade native crates" footgun (CLAUDE.md). Still, run the
  **full** `buck2 test //src/...` after each `./tools/buckify.sh`, since a
  `reindeer update` re-resolves the whole graph and can move *other* crates. The
  binary-size cost of `zxcvbn`'s bundled dictionary is the main non-trivial add and
  is modest.
- **Security review per slice.** Adopting reviewed crates is the point, but
  *integration* is where the bugs live: MFA-challenge replay/expiry, WebAuthn
  RP-ID/origin binding and sign-count regression, OIDC ID-token validation
  (issuer/audience/nonce/JWKS-signature, authorization-code replay), rate-limit
  real-IP spoofing (`X-Forwarded-For` trust behind the TLS front). Each slice's spec
  carries an explicit review gate;
  none ships as "reviewed because the crate is."
- **Discipline the repo already enforces.** Each slice: no inline `#[test]`
  (sibling `tests/*.rs` `rust_test`, `loom_fixture_test` for postgres),
  `.sqlx` refresh + `sqlx-cache-check` green, `testkit` contract parity on both
  adapters, strict clippy. New credential structs must not derive a `Debug` that
  leaks secrets — the same class as [[fut-auth-credential-debug-redact]].
- **Cleartext transport** remains the standing pre-deploy gap
  ([[fut-graceful-shutdown-tls]]); MFA/passkeys/OIDC over plain HTTP assume a
  TLS-terminating front, recorded so no slice ships as production-ready. (OIDC in
  particular requires HTTPS redirect URIs at any real IdP.)

## Non-goals (explicit; stay deferred)

- **Email password reset / verification** ([[fut-auth-email-reset]]) —
  hard-blocked on mail infrastructure loom does not have.
- **Service-token scoping** ([[fut-auth-token-scoping]]) — gates on a real
  least-privilege need.
- **First-class auth-admin ACL capability** ([[fut-auth-admin-capability]]) — the
  new management routes keep gating on the config-named bootstrap admin, like the
  service-account and user-management surfaces.
- **A full external IdP** (Keycloak/Authentik/Ory) — rejected in Build-vs-adopt.
- **Distributed/Redis-backed rate limiting** — in-memory per-process only; a
  multi-replica shared limiter is deferred.
- **TLS** ([[fut-graceful-shutdown-tls]]) and **`Debug` redaction**
  ([[fut-auth-credential-debug-redact]]) — tracked separately.

## Resolved library decisions (2026-07-01)

Two calls were made with the operator when this spec landed, both toward **pure
Rust / zero native deps**:

- **Federation → OIDC via `openidconnect`** (not SAML/`samael`). SAML-specifically is
  dropped from scope unless a future SAML-only-IdP consumer surfaces.
- **Passkeys → `passkey-rs`** (not `webauthn-rs`), accepting the deeper server-RP
  integration work in exchange for no `openssl-sys` exposure.

These close what were the two biggest open questions; the remainder below are
integration/policy decisions for the individual slice specs.

## Open questions (the human-decision points)

1. **MFA enforcement policy.** Per-user opt-in, admin-mandated per user, or
   org-wide required? And at-login only vs step-up for sensitive actions.
2. **JIT provisioning on first federated login** — auto-create the loom
   user/subject (and with which starting roles?), or require the admin surface to
   pre-provision the subject before federation can bind to it?
3. **Passkeys: second-factor first, or passwordless-primary from the start?**
   Both share the ceremony but differ in the login flow and recovery story.
4. **Rate-limit source identity.** How is the real client IP derived behind the
   TLS-terminating front — which `X-Forwarded-For` hop is trusted, and how is that
   configured so the limiter cannot be trivially bypassed or weaponized?
5. **Credential-kind storage shape.** Confirm per-kind tables (recommended,
   consistent with service tokens) over a polymorphic `auth.credential(kind,
   material)` table.
6. **TOTP secret encryption at rest** — key source and rotation for
   `auth.totp_credential.secret_enc` (unlike password verifiers, a TOTP secret is
   symmetric and must be recoverable to verify).

## Register outcome

- [[road-auth-comprehensive]]: this umbrella spec fills its `spec:` slug; it stays
  `status:planned` until the slices land, then closes when Slices A–D are done.
- The six delivered ideas ([[fut-auth-totp-mfa]], [[fut-auth-passkeys]],
  [[fut-auth-saml]], [[fut-auth-login-rate-limit]], [[fut-auth-password-policy]],
  [[fut-auth-session-refresh]]) stay in FUTURE as the detailed surface each slice
  delivers against; each is promoted to a `road-*` item when its slice spec is cut.
  Note [[fut-auth-saml]] is satisfied as the **federation** capability via OIDC
  (Slice D); SAML-specifically stays deferred (no `road-*` yet) unless a SAML-only
  IdP consumer surfaces.
- Deferred and untouched: [[fut-auth-email-reset]], [[fut-auth-token-scoping]],
  [[fut-auth-admin-capability]], [[fut-auth-credential-debug-redact]].
