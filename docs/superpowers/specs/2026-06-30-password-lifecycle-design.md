# Password lifecycle — self-service change, admin reset, account lockout

- **Date:** 2026-06-30
- **Area:** acl
- **Register items:** promotes [[fut-auth-password-lifecycle]] → mints [[road-auth-password-lifecycle]]; records [[fut-auth-email-reset]] + [[fut-auth-login-rate-limit]] + [[fut-auth-password-policy]]; depends on [[road-auth-user-management]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

A user can **rotate their own password**, an operator can **reset a forgotten one**, and
repeated failed logins **lock the account** — the password hygiene a real auth system needs,
without standing up email infrastructure. This is the lifecycle layer on the
[[road-auth-password-session]] foundation: create+verify becomes a credential with a life.

## Current state

[[road-auth-password-session]] (PR #195) ships **create + verify only**. The `Auth` concern
(`control-plane/core/src/auth.rs`) persists the Argon2 PHC verifier and sessions
(`create_user`, `find_password_credential`, `create_session`/`resolve_session`/
`revoke_session`); crypto lives in the service layer. Login is `POST /auth/login`
(`service_runtime::login_routes`). There is **no** password-change surface, **no** failed-
attempt tracking, and **no** lockout — a stolen-but-known password works forever and online
brute force is unthrottled.

[[road-auth-user-management]] (planned) adds admin-set **initial** passwords on user create
and the `require_admin` gate, but **explicitly deferred password reset to this slice**. So
this slice **builds on** that one: it reuses the `require_admin` admin surface for the reset
route and adds the self-service + lockout pieces.

Email-dependent flows (forgot-password, email verification) are out — loom has no mail
infrastructure; admin reset covers operator-driven recovery without it.

## Design

### `Auth` trait additions

- **`update_password(subject_id, new_phc)`** — replace a subject's stored PHC. Shared by the
  self-service change and the admin reset (the *verify-current* decision is made in the
  service layer before the call; the trait just persists, consistent with its
  no-cryptography contract).
- **Store-backed lockout** (a forward-only migration adds `failed_attempt_count`,
  `locked_until`, `last_failed_at` to `auth.user`):
  - **`record_failed_login(username, now)`** — increment the counter (resetting it if the
    window since `last_failed_at` elapsed) and set `locked_until = now + lockout_duration`
    once the count reaches the threshold.
  - **`reset_failed_logins(username)`** — clear counter + `locked_until` on a successful
    login.
  - the login read surfaces `locked_until` (extend `find_password_credential`'s result, or a
    sibling read) so the service can reject a locked account.

  All on both adapters (memory fake + postgres, `.sqlx` refresh) + testkit contract. Lockout
  is keyed by **username** and therefore protects **existing** accounts only — unknown-
  username brute force is the deferred rate-limiter's concern ([[fut-auth-login-rate-limit]]),
  noted not hidden.

### Self-service change — `POST /auth/password` (behind `require_auth`)

Body `{ current, new }`. The service:

1. verifies `current` against the authenticated subject's stored PHC (service-layer Argon2
   verify); mismatch → **403**, nothing changed;
2. hashes `new` and `update_password`s it;
3. **revokes the subject's *other* sessions** — the **current** session is preserved (the
   user is not logged out mid-action), but every other outstanding token for that subject is
   invalidated (a changed password should not leave old sessions live).

Mirrors `login_routes` placement (shared `service_runtime`). Password **strength/complexity**
checks are out ([[fut-auth-password-policy]]).

### Admin reset — `POST /admin/users/{username}/password` (behind `require_admin`)

Extends [[road-auth-user-management]]'s admin surface (completing the reset it deferred): an
admin sets a **new** password for any user with **no current-verify** (the operator-recovery
path), and **revokes *all* that user's sessions** (force re-login with the new credential).
This is the forgotten-password remedy that needs no email.

### Lockout enforcement (login path)

`POST /auth/login` becomes:

1. read the credential + `locked_until`; if `locked_until > now` → reject as locked (a clear
   but non-enumerating response — same posture as the generic bad-password failure, so a
   locked vs unknown account is not distinguishable beyond the lock signal);
2. verify the password; on **success** → `reset_failed_logins` + issue session; on **failure**
   → `record_failed_login(username, now)` (which locks at the threshold) + the existing
   generic failure.

Threshold + window + lock duration are service-layer config via the existing seam
(`LOOM_LOGIN_LOCKOUT_THRESHOLD`, `LOOM_LOGIN_LOCKOUT_WINDOW`, `LOOM_LOGIN_LOCKOUT_DURATION`,
sensible defaults e.g. 5 / 15m / 15m).

### Decided (not open)

- **Self-service change + admin reset + lockout** is the slice; email reset/verification,
  per-IP rate-limiting, and forced rotation/strength are deferred (below).
- **Store-backed lockout** (not in-memory) — correct across replicas and restarts; an
  attacker can't reset the counter via a pod bounce.
- **Self-service change preserves the current session, revokes the rest; admin reset revokes
  all** — the two have deliberately different session outcomes.
- **Admin reset reuses `require_admin`** ([[road-auth-user-management]]) — this slice depends
  on that one for the admin gate/route infrastructure.

## Scope

In scope:

- `Auth`: `update_password`, `record_failed_login`, `reset_failed_logins`, lockout state on
  the login read; the `auth.user` lockout migration; both adapters + testkit.
- `POST /auth/password` (self-service change, verify-current, revoke-others).
- `POST /admin/users/{username}/password` (admin reset, revoke-all) on the
  [[road-auth-user-management]] admin surface.
- Lockout enforcement + config in the login path.

Out of scope:

- **Forgot-password / email reset / email verification** ([[fut-auth-email-reset]]) — needs
  mail infrastructure; admin reset is the no-email remedy.
- **Per-IP / per-endpoint login rate-limiting** ([[fut-auth-login-rate-limit]]) — anti-brute-
  force across accounts / unknown usernames, distinct from per-account lockout.
- **Forced rotation (max-age) + password strength/complexity policy**
  ([[fut-auth-password-policy]]).
- MFA/passkeys/SAML/session-refresh (their own deferred items); CAPTCHA; breach-list checks.

## Testing

testkit `Auth` contract (both adapters) + `loom_fixture_test` e2e:

1. **update_password round-trip:** set a new PHC → the old verifier no longer matches, the
   new one does (memory + postgres).
2. **Lockout counter:** `record_failed_login` increments; at the threshold `locked_until` is
   set; `reset_failed_logins` clears it; a stale window resets the count rather than locking.
3. **Self-service change e2e:** wrong `current` → 403, unchanged; right `current` → new
   password logs in, the **old** fails, the caller's **other** sessions are revoked, its
   **current** session still works.
4. **Admin reset e2e:** admin sets a user's new password → the user logs in with it, **all**
   prior sessions are revoked; a non-admin is 403 (the `require_admin` gate).
5. **Lockout e2e:** N failed logins → the account is locked, and even the **correct** password
   is rejected while locked; after the lock window it logs in again; a successful login before
   the threshold resets the counter.
6. **No enumeration:** a locked existing account and an unknown username are not distinguishable
   beyond the lock signal (the generic-failure posture holds).

## Risk

- **Login-path change is security-critical** — a lockout bug could deny legitimate users or
  fail open. Mitigated by the contract + e2e tests (2, 5), configurable threshold/window, the
  preserved generic-failure response, and store-backed counters (correct under replicas).
- **Lockout as a DoS vector** (an attacker locks a victim by failing their logins) is inherent
  to account lockout; bounded by the time-boxed `locked_until` (auto-unlock) and is exactly
  why per-IP rate-limiting ([[fut-auth-login-rate-limit]]) is the complementary follow-on.
- **Session revocation on change/reset** is intentional; the differing outcomes
  (change-keeps-current vs reset-revokes-all) are pinned by tests 3–4.
- **Depends on [[road-auth-user-management]]** (admin gate) — a sequencing note for the work
  agent; the self-service + lockout pieces stand alone on [[road-auth-password-session]] if
  needed.
- Reuses the proven Argon2 verify + session machinery; the new surface is additive (one new
  self-service route, one admin route, lockout columns), so unauthenticated and existing login
  paths are otherwise unchanged.
