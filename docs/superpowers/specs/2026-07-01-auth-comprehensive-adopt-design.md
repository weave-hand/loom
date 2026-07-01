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
established, security-reviewed Rust implementations** (WebAuthn, TOTP, SAML/XML
signatures, GCRA rate limiting) and plugs each into the identity spine already in
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
SAML assertion parsing + XML-DSIG signature validation, and GCRA rate limiting.
loom has no reason to own that code.

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
| Passkeys / WebAuthn ([[fut-auth-passkeys]]) | `webauthn-rs` (Kanidm), `passkey-rs` (1Password) | **`webauthn-rs`** (server RP library) | The de-facto Rust RP library, battle-tested in Kanidm; handles registration (attestation) + authentication (assertion) + credential state serialization. **Risk: verify its crypto backend at buckify time** — if it pulls `openssl-sys` (a native `links` crate) it triggers the reindeer downgrade footgun and needs a fixup; `passkey-rs` is the pure-Rust fallback if the native dep is unacceptable (Open Question). |
| SAML federation ([[fut-auth-saml]]) | `samael` (SAML2 SP), **or pivot to OIDC** via `openidconnect` | **`samael`** as the SAML answer, **but flag OIDC as a lower-cost alternative** | `samael` depends on `libxml2`/`xmlsec` **native** libs for XML-DSIG — the single biggest blast radius here, and it interacts with the "deploy env provides system libs" model (the libxml2 fetch is already RE/test-only). `openidconnect` is **pure Rust** and may satisfy the real federation need at a fraction of the cost. **Biggest human decision — see Open Questions.** |
| Login rate-limit ([[fut-auth-login-rate-limit]]) | `tower_governor` (tower layer over `governor`), raw `governor` | **`tower_governor`** | Small, pure-Rust GCRA limiter as an axum/tower layer in `service_runtime`, keyed by real client IP. In-memory per-process for now; distributed/Redis-backed is deferred (Open Question — multi-replica Helm). |
| Password policy ([[fut-auth-password-policy]]) | `zxcvbn` (strength estimator), home-grown rules | **`zxcvbn`** for strength + **home-grown** forced-rotation | `zxcvbn` is pure Rust (bundles a frequency dictionary — modest binary-size cost). Forced rotation is just a `password_changed_at` column + a login-time max-age check; no crate. Breach-list (HIBP) needs network → deferred. |
| Session refresh ([[fut-auth-session-refresh]]) | home-grown on `auth.session` | **Home-grown** (no new crate) | Sliding expiry / refresh is a mechanics change to the existing opaque-session table + `resolve_session`; nothing to adopt. The cheapest capability. |

Net: **four small/pure crates** (`totp-rs`, `tower_governor`, `zxcvbn`,
`webauthn-rs` pending its backend check), **one native-heavy crate under review**
(`samael`, possibly replaced by pure-Rust `openidconnect`), and **one home-grown**
extension (session refresh).

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
  stores a serialized `webauthn-rs` credential for the subject.
- **SAML / federation — an alternate session origin.** The SP validates the
  IdP assertion (signature, conditions, audience), maps the assertion's
  NameID/attribute to a loom `SubjectId` via a federated-identity binding, and — if
  mapped — mints a session via `create_session`. Federation bypasses password + MFA
  (the IdP asserts the factors). JIT user provisioning on first login is an Open
  Question.
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
  `webauthn-rs` credential.
- `auth.federated_identity(idp, external_subject, subject_id → auth.user,
  created_at, PRIMARY KEY (idp, external_subject))` — the IdP NameID → loom subject
  binding.
- `auth.mfa_challenge(challenge_sha256 PK, subject_id, purpose, expires_at,
  created_at)` — the short-lived pending-MFA / must-change-password token; hashed,
  never the raw value, mirroring `auth.session`.
- Column adds on `auth.password_credential` / `auth.user` for policy + refresh:
  `password_changed_at`; a session `last_seen_at` / absolute-max column for sliding
  expiry.

The `Auth` trait gains per-kind store/list/resolve methods, each implemented on
**both** adapters (memory fake + postgres, `.sqlx` refreshed via
`tools/sqlx-prepare.sh`) and contract-tested in `testkit` — the exact pattern
every concern already follows. Crypto (TOTP verify, WebAuthn ceremony, SAML
validation) stays in the service layer; the trait remains a pure store, consistent
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
  modes. Adopts `webauthn-rs`. Delivers [[fut-auth-passkeys]]. Sequenced after TOTP
  because it is heavier (RP-ID/origin config, attestation policy) and gates on the
  crate's native-backend check.
- **Slice D — SAML (or OIDC) federation.** Federated-identity mapping table + the
  assertion/token validation + session mint, plus the JIT-provisioning decision.
  Adopts `samael` (or `openidconnect` if the pivot is taken). Delivers
  [[fut-auth-saml]]. Sequenced last: biggest blast radius (native `xmlsec`/libxml2)
  and gated on the SAML-vs-OIDC human decision.

## Constraints & risks

- **Hermetic build / reindeer blast radius.** Every crate must buckify from
  crates.io. The four small crates (`totp-rs`, `tower_governor`, `zxcvbn`,
  `webauthn-rs`-if-pure) are low-risk pure-Rust adds. The two danger points:
  (1) **`webauthn-rs`'s crypto backend** — if it pulls `openssl-sys`, that is a
  native `links` crate, subject to the documented "`reindeer update` can downgrade
  native crates" footgun (CLAUDE.md), needs a fixup, and must be verified with the
  **full** `buck2 test //src/...` after buckify. (2) **`samael`'s libxml2/xmlsec**
  native dependency — the largest, and it interacts with the "deploy env provides
  system libs" model (loom embeds only its own artifacts; libxml2 stays the deploy
  env's job, per the deploy-libs memory). Prefer the OIDC pivot if the federation
  need allows.
- **Security review per slice.** Adopting reviewed crates is the point, but
  *integration* is where the bugs live: MFA-challenge replay/expiry, WebAuthn
  RP-ID/origin binding and sign-count regression, SAML signature-wrapping / XXE /
  audience-restriction validation, rate-limit real-IP spoofing (`X-Forwarded-For`
  trust behind the TLS front). Each slice's spec carries an explicit review gate;
  none ships as "reviewed because the crate is."
- **Discipline the repo already enforces.** Each slice: no inline `#[test]`
  (sibling `tests/*.rs` `rust_test`, `loom_fixture_test` for postgres),
  `.sqlx` refresh + `sqlx-cache-check` green, `testkit` contract parity on both
  adapters, strict clippy. New credential structs must not derive a `Debug` that
  leaks secrets — the same class as [[fut-auth-credential-debug-redact]].
- **Cleartext transport** remains the standing pre-deploy gap
  ([[fut-graceful-shutdown-tls]]); MFA/passkeys/SAML over plain HTTP assume a
  TLS-terminating front, recorded so no slice ships as production-ready.

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

## Open questions (the human-decision points)

1. **SAML vs OIDC for federation.** `samael` (SAML2, native `xmlsec`/libxml2, big
   blast radius) vs `openidconnect` (pure Rust, self-hostable, far smaller). The
   FUTURE item names SAML, but OIDC may satisfy the actual enterprise-federation
   need at a fraction of the cost. **The single biggest call**; it decides Slice D's
   shape and whether loom takes a native XML dependency at all.
2. **`webauthn-rs` crypto backend.** Accept its native dep (likely `openssl-sys`)
   with a fixup, or take the pure-Rust `passkey-rs` fallback? Decides Slice C's
   blast radius.
3. **MFA enforcement policy.** Per-user opt-in, admin-mandated per user, or
   org-wide required? And at-login only vs step-up for sensitive actions.
4. **JIT provisioning on first federated login** — auto-create the loom
   user/subject (and with which starting roles?), or require the admin surface to
   pre-provision the subject before federation can bind to it?
5. **Passkeys: second-factor first, or passwordless-primary from the start?**
   Both share the ceremony but differ in the login flow and recovery story.
6. **Rate-limit source identity.** How is the real client IP derived behind the
   TLS-terminating front — which `X-Forwarded-For` hop is trusted, and how is that
   configured so the limiter cannot be trivially bypassed or weaponized?
7. **Credential-kind storage shape.** Confirm per-kind tables (recommended,
   consistent with service tokens) over a polymorphic `auth.credential(kind,
   material)` table.
8. **TOTP secret encryption at rest** — key source and rotation for
   `auth.totp_credential.secret_enc` (unlike password verifiers, a TOTP secret is
   symmetric and must be recoverable to verify).

## Register outcome

- [[road-auth-comprehensive]]: this umbrella spec fills its `spec:` slug; it stays
  `status:planned` until the slices land, then closes when Slices A–D are done.
- The six delivered ideas ([[fut-auth-totp-mfa]], [[fut-auth-passkeys]],
  [[fut-auth-saml]], [[fut-auth-login-rate-limit]], [[fut-auth-password-policy]],
  [[fut-auth-session-refresh]]) stay in FUTURE as the detailed surface each slice
  delivers against; each is promoted to a `road-*` item when its slice spec is cut.
- Deferred and untouched: [[fut-auth-email-reset]], [[fut-auth-token-scoping]],
  [[fut-auth-admin-capability]], [[fut-auth-credential-debug-redact]].
</content>
</invoke>
