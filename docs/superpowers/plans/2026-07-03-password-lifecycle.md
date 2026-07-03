# Password Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add self-service password change, admin password reset, and per-account failed-login lockout to loom's `auth` concern — the password hygiene a real auth system needs, without email infrastructure.

**Architecture:** Extend the pure-store `Auth` trait (control-plane) with password-update, per-subject session revocation, and store-backed lockout primitives; implement them in both adapters (memory fake + postgres, with a new migration + regenerated `.sqlx` cache); add the crypto/HTTP behavior in the service layer (`service_runtime`) — a `POST /auth/password` self-service route behind `require_auth`, a `POST /admin/users/{username}/password` reset route behind `require_admin`, and lockout enforcement in the existing `POST /auth/login` handler. Lockout thresholds are service-layer config via the existing fail-loud env seam.

**Tech Stack:** Rust 2024, axum (0.7-style `:param` routes), sqlx compile-time `query!` (offline `.sqlx` cache), Argon2id (service-side), `time` crate, buck2 (`rust_test` for pure-logic, `loom_fixture_test` for postgres), testkit contract functions.

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-06-30-password-lifecycle-design.md` — every task traces to it.
- **Register item:** `road-auth-password-lifecycle` (ROADMAP.md). Branch: `work/road-auth-password-lifecycle`.
- **No inline `#[test]`** in `src/**.rs` outside a `tests/` dir — the `no-inline-tests` prek hook fails otherwise. All tests are `rust_test`/`loom_fixture_test` integration targets.
- **Panic-safety clippy is enforced on production code:** no `unwrap`/`expect`/`panic`/`todo`/`unimplemented`/`unreachable`/`indexing_slicing`/`dbg`. Use `?`, `is_some_and`, `map_or`, `try_from(..).unwrap_or(..)` (`unwrap_or` is allowed — it is not `unwrap`). Carry source errors (no `map_err(|_| ...)`). Test code is exempt for panic-safety via the `loom_rust_test`/`loom_fixture_test` wrappers.
- **Error → HTTP mapping is `status_for`** (`runtime/src/auth.rs:40`): `Unauthorized`→401, `NotFound`→404, `Conflict`→409, `Validation`→400, else 500. **There is no 422 in this codebase** — use 400/403/404 as specified below.
- **sqlx:** any changed/new `query!` in the postgres crate requires regenerating `src/control-plane/postgres/.sqlx/` (see Task 1, Step "Regenerate the sqlx cache"). The `sqlx-cache-check` fixture test enforces freshness.
- **Fixture tests route to RE in this root cloud session** (the buck2 shim injects the flag). Pure-logic `rust_test`s run on RE directly.
- **Commit style:** Conventional Commits (`feat:`, `test:`, `refactor:`) — enforced by the `conventional-commit` hook.

**Verification target set** (scoped — never a bare whole-tree build in cloud; use `-M none` for the lib build):

```
buck2 build -M none //src/control-plane/core:core //src/control-plane/memory:memory //src/control-plane/postgres:postgres //src/services/runtime:runtime //src/services/standalone:standalone //src/services/ingest:ingest //src/services/query-api:query-api
buck2 test //src/control-plane/memory:auth //src/control-plane/postgres:auth //src/control-plane/postgres:sqlx-cache-check \
  //src/services/runtime:auth-routes //src/services/runtime:admin-routes //src/services/runtime:ttl \
  //src/services/runtime:password-routes //src/services/runtime:lockout \
  //src/services/query-api:auth-e2e //src/services/query-api:admin-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

---

## File Structure

**Control plane (store layer):**
- `src/control-plane/core/src/auth.rs` — add `LockoutPolicy` type, `PasswordCredential.locked_until` field, 5 new `Auth` trait methods.
- `src/control-plane/core/src/lib.rs:28` — re-export `LockoutPolicy`.
- `src/control-plane/memory/src/auth.rs` — implement the 5 methods; add 3 lockout fields to `MemUser`.
- `src/control-plane/postgres/src/auth.rs` — implement the 5 methods with `query!`; extend `find_password_credential`'s SELECT.
- `src/control-plane/postgres/migrations/0027_auth_lockout.sql` — **new** lockout columns.
- `src/control-plane/postgres/.sqlx/` — regenerated cache (generated artifact, committed).
- `src/control-plane/testkit/src/lib.rs` — new `password_lifecycle_contract` fn.
- `src/control-plane/memory/tests/auth.rs`, `src/control-plane/postgres/tests/auth.rs` — call the new contract.

**Service layer:**
- `src/services/runtime/src/auth.rs` — `AuthState.lockout` field, `login` rewrite, `change_password` handler + `/auth/password` route, `login_lockout` config reader.
- `src/services/runtime/src/admin.rs` — `reset_password` handler + `/admin/users/:username/password` route.
- `src/services/runtime/src/lib.rs` — re-export `login_lockout`; wire `lockout` in `bootstrap`.
- `src/services/standalone/src/lib.rs` — `StandaloneTuning.lockout` field + AuthState wiring.
- Test-only AuthState construction sites (add `lockout: LockoutPolicy::default()`): `runtime/tests/{admin_routes,service_accounts,auth_middleware,auth_routes}.rs`, `ingest/tests/http_model.rs`, `query-api/tests/{e2e_support,auth_e2e,admin_e2e,http_wire_e2e}.rs`.

**New test files:**
- `src/services/runtime/tests/password_routes.rs` — self-service change matrix (memory, `rust_test`).
- `src/services/runtime/tests/lockout.rs` — lockout via `login_routes` (memory, `rust_test`).
- `src/services/runtime/BUCK` — two new `rust_test` targets.

---

## Task 1: Store layer — trait, adapters, migration, contract

This is the atomic "store" unit: adding trait methods breaks both adapters until implemented, so core + memory + postgres + migration + `.sqlx` + contract land together and the tree stays green. TDD driver = the new testkit contract, run against both adapters.

**Files:**
- Modify: `src/control-plane/core/src/auth.rs`, `src/control-plane/core/src/lib.rs:28`
- Modify: `src/control-plane/memory/src/auth.rs`
- Modify: `src/control-plane/postgres/src/auth.rs`
- Create: `src/control-plane/postgres/migrations/0027_auth_lockout.sql`
- Modify (generated): `src/control-plane/postgres/.sqlx/`
- Modify: `src/control-plane/testkit/src/lib.rs`
- Modify: `src/control-plane/memory/tests/auth.rs`, `src/control-plane/postgres/tests/auth.rs`

**Interfaces produced (relied on by Tasks 2–6):**

```rust
// control_plane_core (re-exported at crate root)
pub struct LockoutPolicy {
    pub threshold: u32,
    pub window: time::Duration,
    pub lockout_duration: time::Duration,
}
impl Default for LockoutPolicy { /* 5, 15 min, 15 min */ }

pub struct PasswordCredential {
    pub subject_id: SubjectId,
    pub password_phc: String,
    pub locked_until: Option<time::OffsetDateTime>,   // NEW field
}

// new Auth trait methods
async fn update_password(&self, subject: &SubjectId, new_phc: &str) -> Result<()>;
async fn password_phc_for_subject(&self, subject: &SubjectId) -> Result<Option<String>>;
async fn revoke_subject_sessions(&self, subject: &SubjectId, keep: Option<&[u8; 32]>) -> Result<()>;
async fn record_failed_login(&self, username: &str, now: OffsetDateTime, policy: LockoutPolicy) -> Result<()>;
async fn reset_failed_logins(&self, username: &str) -> Result<()>;
```

- [ ] **Step 1: Write the failing contract**

Add to `src/control-plane/testkit/src/lib.rs` (after `service_account_contract`). This exercises all five new methods + the `locked_until` surfacing, against any `Auth + Acl`:

```rust
/// Contract for the password-lifecycle `Auth` ops (update, per-subject session
/// revoke, failed-login lockout). `a` must be freshly empty.
pub async fn password_lifecycle_contract<A: Auth + Acl>(a: &A) {
    let sid = |s: &str| SubjectId(s.to_string());
    let h = |b: u8| -> [u8; 32] { [b; 32] };

    a.create_user(&NewUser {
        subject_id: sid("u-al"),
        username: "al".into(),
        password_phc: "phc-1".into(),
    })
    .await
    .unwrap();

    // --- update_password round-trip (keyed by subject) ---
    assert_eq!(
        a.password_phc_for_subject(&sid("u-al")).await.unwrap(),
        Some("phc-1".to_string())
    );
    a.update_password(&sid("u-al"), "phc-2").await.unwrap();
    assert_eq!(
        a.password_phc_for_subject(&sid("u-al")).await.unwrap(),
        Some("phc-2".to_string()),
        "update replaced the stored PHC"
    );
    // the login read reflects the new PHC too
    let cred = a.find_password_credential("al").await.unwrap().unwrap();
    assert_eq!(cred.password_phc, "phc-2");
    assert!(cred.locked_until.is_none(), "unlocked by default");
    // unknown subject → NotFound
    assert!(matches!(
        a.update_password(&sid("ghost"), "x").await,
        Err(ControlPlaneError::NotFound(_))
    ));
    assert!(
        a.password_phc_for_subject(&sid("ghost"))
            .await
            .unwrap()
            .is_none()
    );

    // --- revoke_subject_sessions (keep one / all) ---
    let now = OffsetDateTime::now_utc();
    let future = now + time::Duration::hours(1);
    a.create_session(&sid("u-al"), &h(1), future).await.unwrap();
    a.create_session(&sid("u-al"), &h(2), future).await.unwrap();
    a.create_session(&sid("u-al"), &h(3), future).await.unwrap();
    // keep h(2): the others are revoked, h(2) survives
    a.revoke_subject_sessions(&sid("u-al"), Some(&h(2)))
        .await
        .unwrap();
    assert!(a.resolve_session(&h(1), now).await.unwrap().is_none());
    assert_eq!(
        a.resolve_session(&h(2), now).await.unwrap(),
        Some(sid("u-al")),
        "kept session survives"
    );
    assert!(a.resolve_session(&h(3), now).await.unwrap().is_none());
    // revoke all
    a.revoke_subject_sessions(&sid("u-al"), None).await.unwrap();
    assert!(a.resolve_session(&h(2), now).await.unwrap().is_none());

    // --- lockout: threshold, window reset, clear-on-success ---
    let policy = LockoutPolicy {
        threshold: 3,
        window: time::Duration::minutes(10),
        lockout_duration: time::Duration::minutes(15),
    };
    let t0 = OffsetDateTime::now_utc();
    // two failures inside the window: not yet locked
    a.record_failed_login("al", t0, policy).await.unwrap();
    a.record_failed_login("al", t0 + time::Duration::seconds(1), policy)
        .await
        .unwrap();
    assert!(
        a.find_password_credential("al")
            .await
            .unwrap()
            .unwrap()
            .locked_until
            .is_none(),
        "below threshold: not locked"
    );
    // third failure reaches the threshold → locked, lock in the future
    let t_lock = t0 + time::Duration::seconds(2);
    a.record_failed_login("al", t_lock, policy).await.unwrap();
    let locked = a
        .find_password_credential("al")
        .await
        .unwrap()
        .unwrap()
        .locked_until
        .expect("locked at threshold");
    assert!(locked > t_lock, "lock expiry is in the future");

    // reset clears the lock + counter
    a.reset_failed_logins("al").await.unwrap();
    assert!(
        a.find_password_credential("al")
            .await
            .unwrap()
            .unwrap()
            .locked_until
            .is_none(),
        "reset cleared the lock"
    );

    // stale window: a failure far past the window resets the counter to 1, so a
    // single later failure does not lock.
    a.record_failed_login("al", t0, policy).await.unwrap();
    a.record_failed_login("al", t0 + time::Duration::seconds(1), policy)
        .await
        .unwrap();
    let stale = t0 + time::Duration::hours(2); // > window since last failure
    a.record_failed_login("al", stale, policy).await.unwrap();
    assert!(
        a.find_password_credential("al")
            .await
            .unwrap()
            .unwrap()
            .locked_until
            .is_none(),
        "stale window reset the count instead of locking"
    );

    // record/reset on an unknown username are no-ops (lockout protects existing
    // accounts only), never an error.
    a.record_failed_login("ghost", t0, policy).await.unwrap();
    a.reset_failed_logins("ghost").await.unwrap();
}
```

Wire it into both adapter test files.

`src/control-plane/memory/tests/auth.rs` — add:
```rust
#[tokio::test]
async fn memory_passes_password_lifecycle_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::password_lifecycle_contract(&cp).await;
}
```

`src/control-plane/postgres/tests/auth.rs` — add:
```rust
#[tokio::test]
async fn postgres_passes_password_lifecycle_contract() {
    let fixture = PgFixture::shared();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::password_lifecycle_contract(&cp).await;
}
```

The testkit already imports `Auth, Acl, NewUser, SubjectId, ControlPlaneError, OffsetDateTime, time`. Add `LockoutPolicy` to its `use control_plane_core::{...}` import.

- [ ] **Step 2: Run the memory contract to verify it fails to compile**

Run: `buck2 build //src/control-plane/testkit:testkit 2>&1 | tail -20`
Expected: FAIL — `no method named update_password`, `no LockoutPolicy`, `no field locked_until`.

- [ ] **Step 3: Add the core types and trait methods**

In `src/control-plane/core/src/auth.rs`:

Add the `LockoutPolicy` type near the top (after the imports; `time::Duration` is available — it is the same crate as `OffsetDateTime`):
```rust
/// Failed-login lockout parameters (service-layer config, passed to the store so
/// the increment-and-maybe-lock decision is atomic). `threshold` consecutive
/// failures within `window` lock the account for `lockout_duration`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LockoutPolicy {
    pub threshold: u32,
    pub window: time::Duration,
    pub lockout_duration: time::Duration,
}

impl Default for LockoutPolicy {
    fn default() -> Self {
        LockoutPolicy {
            threshold: 5,
            window: time::Duration::minutes(15),
            lockout_duration: time::Duration::minutes(15),
        }
    }
}
```

Extend `PasswordCredential` (add the field + doc):
```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PasswordCredential {
    pub subject_id: SubjectId,
    pub password_phc: String,
    /// When the account is locked (failed-login threshold reached), the instant the
    /// lock expires; `None` if not locked. The login path rejects while
    /// `locked_until > now`.
    pub locked_until: Option<OffsetDateTime>,
}
```

Add the five methods inside `pub trait Auth` (place them after `set_user_disabled`):
```rust
    /// Replace `subject`'s stored password verifier (and bump `updated_at`). Shared by
    /// the self-service change and the admin reset — the verify-current decision is a
    /// service-layer concern, consistent with this trait's no-cryptography contract.
    /// `NotFound` if the subject has no password credential.
    async fn update_password(&self, subject: &SubjectId, new_phc: &str) -> Result<()>;

    /// The stored PHC for `subject`, or `None` if the subject has no credential. Lets
    /// the self-service change verify the current password when the caller is
    /// identified by their session subject rather than a username.
    async fn password_phc_for_subject(&self, subject: &SubjectId) -> Result<Option<String>>;

    /// Revoke `subject`'s sessions. `keep = Some(hash)` preserves that one session
    /// (self-service change keeps the caller logged in); `None` revokes all (admin
    /// reset). Idempotent.
    async fn revoke_subject_sessions(
        &self,
        subject: &SubjectId,
        keep: Option<&[u8; 32]>,
    ) -> Result<()>;

    /// Record a failed login for `username` under `policy`: increment the
    /// failed-attempt counter (resetting it to 1 first if the time since
    /// `last_failed_at` exceeded `policy.window`), stamp `last_failed_at = now`, and
    /// set `locked_until = now + policy.lockout_duration` once the counter reaches
    /// `policy.threshold`. Atomic. No-op if the username is unknown — lockout is keyed
    /// by username and protects existing accounts only.
    async fn record_failed_login(
        &self,
        username: &str,
        now: OffsetDateTime,
        policy: LockoutPolicy,
    ) -> Result<()>;

    /// Clear `username`'s failed-attempt counter and lock, on a successful login.
    /// Idempotent; no-op if the username is unknown.
    async fn reset_failed_logins(&self, username: &str) -> Result<()>;
```

In `src/control-plane/core/src/lib.rs:28`, add `LockoutPolicy` to the `pub use auth::{...}` list:
```rust
pub use auth::{
    Auth, LockoutPolicy, NewServiceAccount, NewUser, PasswordCredential, ServiceAccount,
    ServiceToken, UserSummary,
};
```

- [ ] **Step 4: Implement the memory adapter**

In `src/control-plane/memory/src/auth.rs`:

Add three fields to `MemUser`:
```rust
struct MemUser {
    subject_id: String,
    password_phc: String,
    disabled: bool,
    created_at: OffsetDateTime,
    failed_attempt_count: u32,
    last_failed_at: Option<OffsetDateTime>,
    locked_until: Option<OffsetDateTime>,
}
```

In `create_user`, initialize them in the `MemUser { ... }` literal:
```rust
                failed_attempt_count: 0,
                last_failed_at: None,
                locked_until: None,
```

In `find_password_credential`, add `locked_until` to the mapped `PasswordCredential`:
```rust
            .map(|u| PasswordCredential {
                subject_id: SubjectId(u.subject_id.clone()),
                password_phc: u.password_phc.clone(),
                locked_until: u.locked_until,
            }))
```

Add the five methods to `impl Auth for MemoryControlPlane` (import `LockoutPolicy` in the `use control_plane_core::{...}` line):
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn update_password(&self, subject: &SubjectId, new_phc: &str) -> Result<()> {
        let mut auth = self.auth.lock();
        match auth.users.values_mut().find(|u| u.subject_id == subject.0) {
            Some(u) => {
                u.password_phc = new_phc.to_string();
                Ok(())
            }
            None => Err(ControlPlaneError::NotFound(format!(
                "credential for subject {}",
                subject.0
            ))),
        }
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn password_phc_for_subject(&self, subject: &SubjectId) -> Result<Option<String>> {
        let auth = self.auth.lock();
        Ok(auth
            .users
            .values()
            .find(|u| u.subject_id == subject.0)
            .map(|u| u.password_phc.clone()))
    }

    #[tracing::instrument(skip(self, keep), level = "debug")]
    async fn revoke_subject_sessions(
        &self,
        subject: &SubjectId,
        keep: Option<&[u8; 32]>,
    ) -> Result<()> {
        let mut auth = self.auth.lock();
        match keep {
            Some(k) => auth
                .sessions
                .retain(|hash, s| s.subject_id != subject.0 || hash == k),
            None => auth.sessions.retain(|_, s| s.subject_id != subject.0),
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn record_failed_login(
        &self,
        username: &str,
        now: OffsetDateTime,
        policy: LockoutPolicy,
    ) -> Result<()> {
        let mut auth = self.auth.lock();
        if let Some(u) = auth.users.get_mut(username) {
            let within_window = u.last_failed_at.is_some_and(|t| now - t <= policy.window);
            u.failed_attempt_count = if within_window {
                u.failed_attempt_count + 1
            } else {
                1
            };
            u.last_failed_at = Some(now);
            if u.failed_attempt_count >= policy.threshold {
                u.locked_until = Some(now + policy.lockout_duration);
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn reset_failed_logins(&self, username: &str) -> Result<()> {
        let mut auth = self.auth.lock();
        if let Some(u) = auth.users.get_mut(username) {
            u.failed_attempt_count = 0;
            u.last_failed_at = None;
            u.locked_until = None;
        }
        Ok(())
    }
```

- [ ] **Step 5: Run the memory contract — expect PASS**

Run: `buck2 test //src/control-plane/memory:auth > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (both `memory_passes_auth_contract` and `memory_passes_password_lifecycle_contract`).

- [ ] **Step 6: Write the postgres migration**

Create `src/control-plane/postgres/migrations/0027_auth_lockout.sql`:
```sql
-- Account lockout for online brute-force resistance (see Auth::record_failed_login).
-- Store-backed so the counter is correct across replicas and survives restarts; an
-- attacker cannot reset it via a pod bounce. All NULL/0 defaults, so existing users
-- are unaffected (unlocked, zero failures).
alter table auth.user
    add column failed_attempt_count integer not null default 0,
    add column last_failed_at       timestamptz,
    add column locked_until         timestamptz;
```

- [ ] **Step 7: Implement the postgres adapter**

In `src/control-plane/postgres/src/auth.rs`:

Extend `find_password_credential`'s query + mapping to surface `locked_until`:
```rust
        let row = sqlx::query!(
            "select u.subject_id, pc.password_phc, u.locked_until \
             from auth.user u \
             join auth.password_credential pc on pc.subject_id = u.subject_id \
             where u.username = $1 and u.disabled_at is null",
            username,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?;
        Ok(row.map(|r| PasswordCredential {
            subject_id: SubjectId(r.subject_id),
            password_phc: r.password_phc,
            locked_until: r.locked_until,
        }))
```

Add the five methods to `impl Auth for PgControlPlane` (import `LockoutPolicy` in the `use control_plane_core::{...}` line):
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn update_password(&self, subject: &SubjectId, new_phc: &str) -> Result<()> {
        let res = sqlx::query!(
            "update auth.password_credential set password_phc = $2, updated_at = now() \
             where subject_id = $1",
            &subject.0,
            new_phc,
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        if res.rows_affected() == 0 {
            return Err(ControlPlaneError::NotFound(format!(
                "credential for subject {}",
                subject.0
            )));
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn password_phc_for_subject(&self, subject: &SubjectId) -> Result<Option<String>> {
        let phc = sqlx::query_scalar!(
            "select password_phc from auth.password_credential where subject_id = $1",
            &subject.0,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?;
        Ok(phc)
    }

    #[tracing::instrument(skip(self, keep), level = "debug")]
    async fn revoke_subject_sessions(
        &self,
        subject: &SubjectId,
        keep: Option<&[u8; 32]>,
    ) -> Result<()> {
        match keep {
            Some(k) => {
                sqlx::query!(
                    "delete from auth.session where subject_id = $1 and token_sha256 <> $2",
                    &subject.0,
                    &k[..],
                )
                .execute(self.pool())
                .await
                .map_err(backend)?;
            }
            None => {
                sqlx::query!(
                    "delete from auth.session where subject_id = $1",
                    &subject.0,
                )
                .execute(self.pool())
                .await
                .map_err(backend)?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn record_failed_login(
        &self,
        username: &str,
        now: OffsetDateTime,
        policy: LockoutPolicy,
    ) -> Result<()> {
        // Interval math is done in Rust so the SQL binds only concrete instants —
        // no PgInterval. `new_count` is computed in the subquery from the CURRENT
        // stored count, and the UPDATE writes it back atomically (correct under
        // concurrency / replicas).
        let window_cutoff = now - policy.window;
        let locked_until = now + policy.lockout_duration;
        let threshold = i32::try_from(policy.threshold).unwrap_or(i32::MAX);
        sqlx::query!(
            "update auth.user u \
             set failed_attempt_count = nc.new_count, \
                 last_failed_at = $2, \
                 locked_until = case when nc.new_count >= $4 then $5 else u.locked_until end \
             from ( \
               select case \
                   when last_failed_at is null or last_failed_at < $3 then 1 \
                   else failed_attempt_count + 1 \
                 end as new_count \
               from auth.user where username = $1 \
             ) nc \
             where u.username = $1",
            username,
            now,
            window_cutoff,
            threshold,
            locked_until,
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn reset_failed_logins(&self, username: &str) -> Result<()> {
        sqlx::query!(
            "update auth.user \
             set failed_attempt_count = 0, locked_until = null, last_failed_at = null \
             where username = $1",
            username,
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }
```

**Note for the `record_failed_login` query:** if `cargo sqlx prepare` reports it cannot infer a param type (`could not determine data type of parameter $4`), add explicit casts: `nc.new_count >= $4::int4`, `last_failed_at < $3::timestamptz`, `then $5::timestamptz`. Try without casts first.

- [ ] **Step 8: Regenerate the `.sqlx` cache**

`tools/sqlx-prepare.sh` runs `initdb`, which refuses to run as root. In this root cloud session, run the cluster as a non-root user and `cargo sqlx prepare` as root over a shared socket. Save this as `/tmp/regen-sqlx.sh` and run it:

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
eval "$(./tools/env.sh)"
if [ ! -x .loom/bin/sqlx ]; then
  cargo install --root "$PWD/.loom" --version '^0.9' --no-default-features --features postgres,rustls sqlx-cli
fi
export PATH="$PWD/.loom/bin:$PATH"
BIN="$PWD/$(buck2 build //src/control-plane/postgres:postgres-bin --show-output 2>/dev/null | awk '{print $2}')"
XML="$PWD/$(buck2 build //src/control-plane/postgres:libxml2 --show-output 2>/dev/null | awk '{print $2}')"
export LD_LIBRARY_PATH="$BIN/lib:$XML"
id pgtest &>/dev/null || useradd -m pgtest
BASE="/tmp/loom-sqlx-regen"; rm -rf "$BASE"; mkdir -p "$BASE/pgdata" "$BASE/sock"
chown -R pgtest "$BASE"; chmod 0777 "$BASE/sock"
PORT=54399
su pgtest -s /bin/bash -c "export LD_LIBRARY_PATH='$LD_LIBRARY_PATH'; \
  '$BIN/bin/initdb' -D '$BASE/pgdata' -U postgres --auth=trust >'$BASE/initdb.log' 2>&1 && \
  '$BIN/bin/pg_ctl' -D '$BASE/pgdata' -o \"-p $PORT -k $BASE/sock -c listen_addresses=''\" -w -l '$BASE/pg.log' start && \
  '$BIN/bin/createdb' -h '$BASE/sock' -p $PORT -U postgres loom"
for f in src/control-plane/postgres/migrations/*.sql; do
  su pgtest -s /bin/bash -c "export LD_LIBRARY_PATH='$LD_LIBRARY_PATH'; \
    '$BIN/bin/psql' -h '$BASE/sock' -p $PORT -U postgres -d loom -v ON_ERROR_STOP=1 -q -f '$PWD/$f'"
done
export DATABASE_URL="postgres://postgres@localhost:$PORT/loom?host=$BASE/sock"
( cd src/control-plane/postgres && unset SQLX_OFFLINE && cargo sqlx prepare -- --lib )
su pgtest -s /bin/bash -c "export LD_LIBRARY_PATH='$LD_LIBRARY_PATH'; '$BIN/bin/pg_ctl' -D '$BASE/pgdata' -m immediate stop" || true
rm -rf "$BASE"
echo "sqlx cache regenerated"
```

Run: `bash /tmp/regen-sqlx.sh 2>&1 | tail -20`
Expected: `sqlx cache regenerated`, and `git status src/control-plane/postgres/.sqlx` shows new/changed query files.

- [ ] **Step 9: Build the postgres crate offline + run both fixture contracts**

Run:
```bash
buck2 build -M none //src/control-plane/postgres:postgres 2>&1 | tail -5
buck2 test //src/control-plane/postgres:auth //src/control-plane/postgres:sqlx-cache-check > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: build OK; both `postgres_passes_auth_contract`, `postgres_passes_password_lifecycle_contract`, and `sqlx-cache-check` PASS. (`sqlx-cache-check` proves the committed cache matches the live schema.)

- [ ] **Step 10: Commit**

```bash
git add src/control-plane/core src/control-plane/memory src/control-plane/postgres src/control-plane/testkit
git commit -m "feat(auth): password-update, per-subject session revoke, and lockout store primitives"
```

---

## Task 2: Service config + `AuthState.lockout` field

Add the fail-loud `login_lockout` env reader and thread a `LockoutPolicy` through `AuthState` (needed by Task 3's login rewrite). Update every construction site so the tree compiles.

**Files:**
- Modify: `src/services/runtime/src/auth.rs` (`AuthState` struct, `login_lockout` fn)
- Modify: `src/services/runtime/src/lib.rs` (`bootstrap`, re-export)
- Modify: `src/services/runtime/tests/ttl.rs` (new tests)
- Modify: `src/services/standalone/src/lib.rs` (`StandaloneTuning`)
- Modify test sites: `runtime/tests/{admin_routes,service_accounts,auth_middleware,auth_routes}.rs`, `ingest/tests/http_model.rs`, `query-api/tests/{e2e_support,auth_e2e,admin_e2e,http_wire_e2e}.rs`

**Interfaces produced:**
```rust
// service_runtime
pub struct AuthState { pub auth: Arc<dyn Auth + Send + Sync>, pub session_ttl: Duration, pub lockout: LockoutPolicy }
pub fn login_lockout(vars: &HashMap<String, String>) -> Result<LockoutPolicy, ConfigError>;
```

- [ ] **Step 1: Write the failing config test**

Add to `src/services/runtime/tests/ttl.rs`:
```rust
#[test]
fn lockout_defaults() {
    let p = service_runtime::login_lockout(&map(&[])).unwrap();
    assert_eq!(p.threshold, 5);
    assert_eq!(p.window, time::Duration::minutes(15));
    assert_eq!(p.lockout_duration, time::Duration::minutes(15));
}

#[test]
fn lockout_parses_overrides() {
    let p = service_runtime::login_lockout(&map(&[
        ("LOOM_LOGIN_LOCKOUT_THRESHOLD", "3"),
        ("LOOM_LOGIN_LOCKOUT_WINDOW", "60"),
        ("LOOM_LOGIN_LOCKOUT_DURATION", "120"),
    ]))
    .unwrap();
    assert_eq!(p.threshold, 3);
    assert_eq!(p.window, time::Duration::seconds(60));
    assert_eq!(p.lockout_duration, time::Duration::seconds(120));
}

#[test]
fn lockout_malformed_threshold_is_startup_error() {
    let vars = map(&[("LOOM_LOGIN_LOCKOUT_THRESHOLD", "lots")]);
    let err = service_runtime::login_lockout(&vars).unwrap_err();
    assert!(matches!(err, ConfigError::Invalid { ref var, .. }
        if var == "LOOM_LOGIN_LOCKOUT_THRESHOLD"));
}
```

Add `use time;` if not present (the file may need `time` in `deps`). Confirm `time` is a dep of the `ttl` test target; if not, add `"//third-party:time"` to the `ttl` `rust_test` deps in `src/services/runtime/BUCK` (target `name = "ttl"`).

- [ ] **Step 2: Run — verify it fails**

Run: `buck2 build //src/services/runtime:ttl 2>&1 | tail -10`
Expected: FAIL — `no function login_lockout`.

- [ ] **Step 3: Add the `login_lockout` reader**

In `src/services/runtime/src/auth.rs`, add `LockoutPolicy` to the `use control_plane_core::{...}` import, and add after `session_ttl`:
```rust
/// Fail-loud read of the failed-login lockout policy from the env snapshot:
/// `LOOM_LOGIN_LOCKOUT_THRESHOLD` (count, default 5), `LOOM_LOGIN_LOCKOUT_WINDOW`
/// and `LOOM_LOGIN_LOCKOUT_DURATION` (seconds, default 900 = 15 min each). Same
/// fallback semantics as [`session_ttl`] — a present-but-malformed value is a
/// startup error naming the key, never a silent fallback.
pub fn login_lockout(vars: &HashMap<String, String>) -> Result<LockoutPolicy, loom_config::ConfigError> {
    let threshold = loom_config::parse_var(vars, "LOOM_LOGIN_LOCKOUT_THRESHOLD", 5_u32)?;
    let window_secs = loom_config::parse_var(vars, "LOOM_LOGIN_LOCKOUT_WINDOW", 900_u64)?;
    let duration_secs = loom_config::parse_var(vars, "LOOM_LOGIN_LOCKOUT_DURATION", 900_u64)?;
    Ok(LockoutPolicy {
        threshold,
        window: time::Duration::seconds(i64::try_from(window_secs).unwrap_or(i64::MAX)),
        lockout_duration: time::Duration::seconds(i64::try_from(duration_secs).unwrap_or(i64::MAX)),
    })
}
```

Add the `lockout` field to `AuthState`:
```rust
#[derive(Clone)]
pub struct AuthState {
    pub auth: Arc<dyn Auth + Send + Sync>,
    pub session_ttl: Duration,
    pub lockout: LockoutPolicy,
}
```

- [ ] **Step 4: Re-export + wire `bootstrap`**

In `src/services/runtime/src/lib.rs`, add `login_lockout` to the `pub use auth::{...}` list (line ~6):
```rust
pub use auth::{
    AuthState, Subject, login_lockout, login_routes, protect, require_auth,
    service_account_routes, service_token_max_ttl, session_routes, session_ttl, status_for,
};
```

In `bootstrap` (lib.rs ~366), read the policy before the pool boots and add it to the `AuthState`:
```rust
    let session_ttl = auth::session_ttl(vars)?;
    let lockout = auth::login_lockout(vars)?;
    let max_ttl = auth::service_token_max_ttl(vars)?;
    let (pool, embedded) = build_pool_managed(&cfg).await?;
    let pg = Arc::new(control_plane(pool.clone(), cfg.lock_timeout));
    let auth = AuthState {
        auth: pg.clone(),
        session_ttl,
        lockout,
    };
```

- [ ] **Step 5: Update `StandaloneTuning`**

In `src/services/standalone/src/lib.rs`, add the field to `StandaloneTuning` (after `max_ttl`):
```rust
    pub lockout: control_plane_core::LockoutPolicy,
```
Populate it in `from_map` (after `max_ttl:`):
```rust
            lockout: service_runtime::login_lockout(vars)?,
```
And in `serve_composite`'s `AuthState` literal (line ~81), add:
```rust
        lockout: tuning.lockout,
```
Confirm `standalone`'s BUCK depends on `//src/control-plane/core`; it does (it already references `service_runtime`). If `control_plane_core` is not a direct dep, use `service_runtime`'s re-export instead: import path `service_runtime::login_lockout` is already used, and for the type write `control_plane_core::LockoutPolicy` only if that crate is a dep — otherwise add `pub use control_plane_core::LockoutPolicy;` to `service_runtime/src/lib.rs` and reference `service_runtime::LockoutPolicy`.

> To avoid the dependency question entirely, add this re-export to `src/services/runtime/src/lib.rs` and use `service_runtime::LockoutPolicy` everywhere in service/test code:
> ```rust
> pub use control_plane_core::LockoutPolicy;
> ```
> Place it near the other re-exports. Then `StandaloneTuning.lockout: service_runtime::LockoutPolicy`.

- [ ] **Step 6: Update every test-only AuthState construction site**

Add `lockout: service_runtime::LockoutPolicy::default(),` (or `LockoutPolicy::default()` with the appropriate import) to each `AuthState { ... }` literal in:
- `src/services/runtime/tests/admin_routes.rs:25`
- `src/services/runtime/tests/service_accounts.rs:19`
- `src/services/runtime/tests/auth_middleware.rs:22`
- `src/services/runtime/tests/auth_routes.rs:15`
- `src/services/ingest/tests/http_model.rs:103`
- `src/services/query-api/tests/e2e_support.rs:254,325,981`
- `src/services/query-api/tests/auth_e2e.rs:31`
- `src/services/query-api/tests/admin_e2e.rs:36,53`
- `src/services/query-api/tests/http_wire_e2e.rs:143`

Each test crate already imports `service_runtime`; use `service_runtime::LockoutPolicy::default()` (works once Step 5's re-export exists), so no new import line is needed.

- [ ] **Step 7: Build the affected crates + run ttl**

Run:
```bash
buck2 build -M none //src/services/runtime:runtime //src/services/standalone:standalone 2>&1 | tail -5
buck2 test //src/services/runtime:ttl > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: build OK; ttl PASS.

- [ ] **Step 8: Commit**

```bash
git add src/services/runtime src/services/standalone src/services/ingest src/services/query-api
git commit -m "feat(auth): login-lockout config seam + AuthState.lockout"
```

---

## Task 3: Login lockout enforcement

Rewrite the `login` handler to reject a locked account, record failures, and reset on success. New memory-backed `rust_test`.

**Files:**
- Modify: `src/services/runtime/src/auth.rs` (`login`)
- Create: `src/services/runtime/tests/lockout.rs`
- Modify: `src/services/runtime/BUCK` (new `lockout` target)

**Interfaces consumed:** `AuthState.lockout` (Task 2); `Auth::{find_password_credential (now returns locked_until), record_failed_login, reset_failed_logins}` (Task 1).

- [ ] **Step 1: Write the failing test**

Create `src/services/runtime/tests/lockout.rs`:
```rust
//! Login lockout: N failures lock the account, the correct password is rejected
//! while locked, a success before the threshold resets the counter, and the lock
//! auto-expires after the configured duration.
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use control_plane_core::{Auth, LockoutPolicy, NewUser, SubjectId};
use control_plane_memory::MemoryControlPlane;
use service_runtime::{AuthState, hash_password, login_routes};
use tower::ServiceExt;

fn state(cp: Arc<MemoryControlPlane>, lockout: LockoutPolicy) -> AuthState {
    AuthState {
        auth: cp,
        session_ttl: Duration::from_secs(3600),
        lockout,
    }
}

async fn seed(cp: &MemoryControlPlane, user: &str, pw: &str) {
    cp.create_user(&NewUser {
        subject_id: SubjectId(user.into()),
        username: user.into(),
        password_phc: hash_password(pw).unwrap(),
    })
    .await
    .unwrap();
}

async fn login(app: Router, user: &str, pw: &str) -> axum::http::StatusCode {
    let body = format!(r#"{{"username":"{user}","password":"{pw}"}}"#);
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/auth/login")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}

#[tokio::test]
async fn lockout_after_threshold_rejects_even_correct_password() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed(&cp, "al", "right").await;
    let policy = LockoutPolicy {
        threshold: 3,
        window: time::Duration::minutes(10),
        lockout_duration: time::Duration::hours(1),
    };
    // 3 wrong attempts → locked
    for _ in 0..3 {
        assert_eq!(
            login(login_routes(state(cp.clone(), policy)), "al", "wrong").await,
            axum::http::StatusCode::UNAUTHORIZED
        );
    }
    // correct password now rejected while locked (same generic 401)
    assert_eq!(
        login(login_routes(state(cp.clone(), policy)), "al", "right").await,
        axum::http::StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn success_before_threshold_resets_counter() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed(&cp, "al", "right").await;
    let policy = LockoutPolicy {
        threshold: 3,
        window: time::Duration::minutes(10),
        lockout_duration: time::Duration::hours(1),
    };
    // 2 failures, then a success (resets), then 2 more failures → still not locked
    for _ in 0..2 {
        login(login_routes(state(cp.clone(), policy)), "al", "wrong").await;
    }
    assert_eq!(
        login(login_routes(state(cp.clone(), policy)), "al", "right").await,
        axum::http::StatusCode::OK
    );
    for _ in 0..2 {
        login(login_routes(state(cp.clone(), policy)), "al", "wrong").await;
    }
    assert_eq!(
        login(login_routes(state(cp.clone(), policy)), "al", "right").await,
        axum::http::StatusCode::OK,
        "counter was reset by the earlier success, so 2 more failures do not lock"
    );
}

#[tokio::test]
async fn lock_auto_expires_after_duration() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed(&cp, "al", "right").await;
    let policy = LockoutPolicy {
        threshold: 2,
        window: time::Duration::minutes(10),
        lockout_duration: time::Duration::milliseconds(200),
    };
    for _ in 0..2 {
        login(login_routes(state(cp.clone(), policy)), "al", "wrong").await;
    }
    // locked now
    assert_eq!(
        login(login_routes(state(cp.clone(), policy)), "al", "right").await,
        axum::http::StatusCode::UNAUTHORIZED
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    // lock expired → correct password logs in
    assert_eq!(
        login(login_routes(state(cp.clone(), policy)), "al", "right").await,
        axum::http::StatusCode::OK
    );
}
```

Add the target to `src/services/runtime/BUCK` (mirror `auth-routes`):
```python
rust_test(
    name = "lockout",
    crate = "lockout",
    srcs = ["tests/lockout.rs"],
    crate_root = "tests/lockout.rs",
    edition = "2024",
    deps = [
        ":runtime",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:axum",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:tower",
    ],
)
```

- [ ] **Step 2: Run — verify it fails**

Run: `buck2 test //src/services/runtime:lockout > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL — the current `login` neither locks nor rejects a locked account (the lock tests fail).

- [ ] **Step 3: Rewrite the `login` handler**

Replace `login` in `src/services/runtime/src/auth.rs` with:
```rust
/// `POST /auth/login` — public. Verify the password, mint a session token, return
/// it once. Enforces per-account lockout: a locked account is rejected with the
/// same generic 401 as a bad password (no enumeration); each failure is recorded
/// and a success clears the counter. A uniform 401 on unknown user OR bad password
/// OR locked (with a dummy hash to keep timing uniform).
async fn login(State(st): State<AuthState>, axum::Json(req): axum::Json<LoginReq>) -> Response {
    let now = OffsetDateTime::now_utc();
    let cred = match st.auth.find_password_credential(&req.username).await {
        Ok(Some(c)) => c,
        // Unknown or disabled user: burn comparable time, then the same 401.
        Ok(None) => {
            drop(crate::hash_password(&req.password));
            return unauthorized();
        }
        Err(e) => return status_for(&e).into_response(),
    };

    // Locked: reject with the same generic 401 (burn a dummy hash so a locked
    // account is indistinguishable from a bad password by response and by timing).
    if cred.locked_until.is_some_and(|lu| lu > now) {
        drop(crate::hash_password(&req.password));
        return unauthorized();
    }

    if crate::verify_password(&req.password, &cred.password_phc) {
        if let Err(e) = st.auth.reset_failed_logins(&req.username).await {
            return status_for(&e).into_response();
        }
        let token = crate::generate_session_token();
        let hash = token_sha256(&token);
        #[expect(
            clippy::expect_used,
            reason = "session_ttl comes from Duration::from_secs(u64) which always fits in time::Duration (<292 years)"
        )]
        let expires = now
            + time::Duration::try_from(st.session_ttl).expect("session_ttl fits in time::Duration");
        match st.auth.create_session(&cred.subject_id, &hash, expires).await {
            Ok(()) => (StatusCode::OK, axum::Json(LoginResp { token })).into_response(),
            Err(e) => status_for(&e).into_response(),
        }
    } else {
        if let Err(e) = st.auth.record_failed_login(&req.username, now, st.lockout).await {
            return status_for(&e).into_response();
        }
        unauthorized()
    }
}
```

- [ ] **Step 4: Run — verify PASS**

Run: `buck2 test //src/services/runtime:lockout //src/services/runtime:auth-routes > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (lockout + the existing login tests still green).

- [ ] **Step 5: Commit**

```bash
git add src/services/runtime
git commit -m "feat(auth): enforce per-account login lockout"
```

---

## Task 4: Self-service password change — `POST /auth/password`

**Files:**
- Modify: `src/services/runtime/src/auth.rs` (`change_password` handler, `session_routes`)
- Create: `src/services/runtime/tests/password_routes.rs`
- Modify: `src/services/runtime/BUCK` (new `password-routes` target)

**Interfaces consumed:** `Auth::{password_phc_for_subject, update_password, revoke_subject_sessions}` (Task 1); `crate::{verify_password, hash_password}`; `bearer_token`, `token_sha256`, `Subject`, `protect`.

- [ ] **Step 1: Write the failing test**

Create `src/services/runtime/tests/password_routes.rs`:
```rust
//! Self-service password change: wrong `current` → 403 unchanged; right `current`
//! → the new password logs in, the old fails, other sessions are revoked, and the
//! caller's current session survives.
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{StatusCode, header::AUTHORIZATION};
use control_plane_core::{Auth, LockoutPolicy, NewUser, SubjectId};
use control_plane_memory::MemoryControlPlane;
use service_runtime::{AuthState, hash_password, session_routes, token_sha256};
use tower::ServiceExt;

fn state(cp: Arc<MemoryControlPlane>) -> AuthState {
    AuthState {
        auth: cp,
        session_ttl: Duration::from_secs(3600),
        lockout: LockoutPolicy::default(),
    }
}

async fn seed_user_session(cp: &MemoryControlPlane, user: &str, pw: &str, token: &str) {
    cp.create_user(&NewUser {
        subject_id: SubjectId(user.into()),
        username: user.into(),
        password_phc: hash_password(pw).unwrap(),
    })
    .await
    .unwrap();
    cp.create_session(
        &SubjectId(user.into()),
        &token_sha256(token),
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await
    .unwrap();
}

async fn change(app: Router, token: &str, body: &str) -> StatusCode {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/auth/password")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}

#[tokio::test]
async fn wrong_current_is_403_and_unchanged() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_user_session(&cp, "al", "orig", "tok-cur").await;
    let status = change(
        session_routes(state(cp.clone())),
        "tok-cur",
        r#"{"current":"WRONG","new":"fresh"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // password unchanged: original still verifies
    let cred = cp.find_password_credential("al").await.unwrap().unwrap();
    assert!(service_runtime::verify_password("orig", &cred.password_phc));
}

#[tokio::test]
async fn right_current_rotates_revokes_others_keeps_current() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_user_session(&cp, "al", "orig", "tok-cur").await;
    // a second, "other" session for the same subject
    cp.create_session(
        &SubjectId("al".into()),
        &token_sha256("tok-other"),
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await
    .unwrap();

    let status = change(
        session_routes(state(cp.clone())),
        "tok-cur",
        r#"{"current":"orig","new":"fresh"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let now = time::OffsetDateTime::now_utc();
    // current session preserved
    assert_eq!(
        cp.resolve_session(&token_sha256("tok-cur"), now)
            .await
            .unwrap(),
        Some(SubjectId("al".into()))
    );
    // other session revoked
    assert!(
        cp.resolve_session(&token_sha256("tok-other"), now)
            .await
            .unwrap()
            .is_none()
    );
    // new password verifies, old does not
    let cred = cp.find_password_credential("al").await.unwrap().unwrap();
    assert!(service_runtime::verify_password("fresh", &cred.password_phc));
    assert!(!service_runtime::verify_password("orig", &cred.password_phc));
}

#[tokio::test]
async fn unauthenticated_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let status = app_unauth(session_routes(state(cp))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

async fn app_unauth(app: Router) -> StatusCode {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/auth/password")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"current":"x","new":"y"}"#))
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}
```

Add the target to `src/services/runtime/BUCK` (mirror `auth-routes`, add `//third-party:serde_json` is not needed here; keep the same dep set as `auth-routes` minus serde_json/http-body-util — but `time` is used):
```python
rust_test(
    name = "password-routes",
    crate = "password_routes",
    srcs = ["tests/password_routes.rs"],
    crate_root = "tests/password_routes.rs",
    edition = "2024",
    deps = [
        ":runtime",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:axum",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:tower",
    ],
)
```

- [ ] **Step 2: Run — verify it fails**

Run: `buck2 test //src/services/runtime:password-routes > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|404" /tmp/t.log`
Expected: FAIL — `/auth/password` is not a route yet (404), so the 403/200 assertions fail.

- [ ] **Step 3: Add the `change_password` handler + route**

In `src/services/runtime/src/auth.rs`, add the request type and handler (near the login handlers). Note `axum::http::HeaderMap` is already used by `logout`:
```rust
#[derive(serde::Deserialize)]
struct ChangePasswordReq {
    current: String,
    new: String,
}

/// `POST /auth/password` — authenticated. Verify the caller's current password,
/// rotate to the new one, and revoke the caller's OTHER sessions (the current
/// session, identified by the presented bearer, is preserved). Wrong current → 403,
/// nothing changed. Password strength policy is out of scope.
async fn change_password(
    subject: Subject,
    State(st): State<AuthState>,
    headers: axum::http::HeaderMap,
    axum::Json(req): axum::Json<ChangePasswordReq>,
) -> Response {
    let phc = match st.auth.password_phc_for_subject(&subject.0).await {
        Ok(Some(p)) => p,
        // An authenticated subject with no credential should not happen; fail closed.
        Ok(None) => return unauthorized(),
        Err(e) => return status_for(&e).into_response(),
    };
    if !crate::verify_password(&req.current, &phc) {
        return (StatusCode::FORBIDDEN, "current password is incorrect").into_response();
    }
    let Ok(new_phc) = crate::hash_password(&req.new) else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "password hashing failed").into_response();
    };
    if let Err(e) = st.auth.update_password(&subject.0, &new_phc).await {
        return status_for(&e).into_response();
    }
    // Keep the current session, revoke the rest.
    let keep = bearer_token(&headers).map(|t| token_sha256(&t));
    if let Err(e) = st
        .auth
        .revoke_subject_sessions(&subject.0, keep.as_ref())
        .await
    {
        return status_for(&e).into_response();
    }
    StatusCode::OK.into_response()
}
```

Extend `session_routes` to mount the route:
```rust
pub fn session_routes(auth: AuthState) -> Router {
    protect(
        Router::new()
            .route("/auth/logout", axum::routing::post(logout))
            .route("/auth/password", axum::routing::post(change_password))
            .with_state(auth.clone()),
        auth,
    )
}
```

- [ ] **Step 4: Run — verify PASS**

Run: `buck2 test //src/services/runtime:password-routes > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/services/runtime
git commit -m "feat(auth): self-service password change route"
```

---

## Task 5: Admin password reset — `POST /admin/users/{username}/password`

**Files:**
- Modify: `src/services/runtime/src/admin.rs` (`reset_password` handler, `admin_routes`)
- Modify: `src/services/runtime/tests/admin_routes.rs` (new tests)

**Interfaces consumed:** `Auth::{update_password, revoke_subject_sessions}` (Task 1); `crate::hash_password`; `require_admin` gate; `SubjectId(username)` convention (as `admin::create_user` already uses).

- [ ] **Step 1: Write the failing test**

Add to `src/services/runtime/tests/admin_routes.rs`:
```rust
#[tokio::test]
async fn admin_reset_revokes_all_sessions_and_sets_new_password() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let admin_token = seed_admin_session(&cp, ADMIN).await;
    // victim user with a live session
    let victim_token = seed_session(&cp, "victim").await;

    let (status, _) = send(
        app(cp.clone()),
        post_json("/admin/users/victim/password", &admin_token, r#"{"new":"reset-pw"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // all the victim's prior sessions are revoked
    assert!(
        cp.resolve_session(&token_sha256(&victim_token), time::OffsetDateTime::now_utc())
            .await
            .unwrap()
            .is_none()
    );
    // the new password verifies
    let cred = cp.find_password_credential("victim").await.unwrap().unwrap();
    assert!(service_runtime::verify_password("reset-pw", &cred.password_phc));
}

#[tokio::test]
async fn admin_reset_unknown_user_is_404() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let admin_token = seed_admin_session(&cp, ADMIN).await;
    let (status, _) = send(
        app(cp),
        post_json("/admin/users/ghost/password", &admin_token, r#"{"new":"x"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn non_admin_reset_is_403() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let alice = seed_session(&cp, "alice").await; // not admin
    seed_session(&cp, "victim").await;
    let (status, _) = send(
        app(cp),
        post_json("/admin/users/victim/password", &alice, r#"{"new":"x"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}
```

Confirm `admin_routes.rs`'s existing imports cover `service_runtime::verify_password`; if not, add it to the `use service_runtime::{...}` line.

- [ ] **Step 2: Run — verify it fails**

Run: `buck2 test //src/services/runtime:admin-routes > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|404" /tmp/t.log`
Expected: FAIL — `/admin/users/:username/password` is not a route yet.

- [ ] **Step 3: Add the `reset_password` handler + route**

In `src/services/runtime/src/admin.rs`, add the request type and handler (near `disable_user`):
```rust
#[derive(serde::Deserialize)]
struct ResetPasswordReq {
    new: String,
}

/// `POST /admin/users/:username/password` — admin. Set a new password for any user
/// with NO current-verify (operator-driven recovery), and revoke ALL that user's
/// sessions (force re-login). `NotFound` (404) if the username is unknown. The
/// subject id equals the username, mirroring `create_user` on this surface.
async fn reset_password(
    State(st): State<AdminState>,
    Path(username): Path<String>,
    Json(req): Json<ResetPasswordReq>,
) -> Response {
    let Ok(new_phc) = hash_password(&req.new) else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "password hashing failed").into_response();
    };
    let subject = SubjectId(username);
    if let Err(e) = st.auth.update_password(&subject, &new_phc).await {
        return status_for(&e).into_response();
    }
    if let Err(e) = st.auth.revoke_subject_sessions(&subject, None).await {
        return status_for(&e).into_response();
    }
    StatusCode::OK.into_response()
}
```

Add the route to `admin_routes`'s inner router (after the disable/enable routes):
```rust
        .route("/admin/users/:username/password", post(reset_password))
```

- [ ] **Step 4: Run — verify PASS**

Run: `buck2 test //src/services/runtime:admin-routes > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (new reset tests + existing admin tests).

- [ ] **Step 5: Commit**

```bash
git add src/services/runtime
git commit -m "feat(auth): admin password reset route"
```

---

## Task 6: Postgres-backed e2e (self-service change, admin reset, lockout)

Prove the full HTTP + real-Postgres path for the headline flows via `loom_fixture_test`, extending the existing e2e files.

**Files:**
- Modify: `src/services/query-api/tests/auth_e2e.rs` (self-service change + lockout over postgres)
- Modify: `src/services/query-api/tests/admin_e2e.rs` (admin reset over postgres)

**Interfaces consumed:** the routes from Tasks 3–5; the fixture helpers already in each file (`setup_iceberg`, `app`, `seed_admin_session`, `hash_password`, `token_sha256`).

- [ ] **Step 1: Add the self-service-change e2e**

Read `src/services/query-api/tests/auth_e2e.rs` for its `app(cp, eng)` builder (it merges `login_routes`; ensure it also merges `session_routes` — if not, add `.merge(service_runtime::session_routes(auth.clone()))` to the local `app` helper so `/auth/password` is mounted). Then add:
```rust
#[tokio::test]
async fn self_service_change_over_postgres() {
    let fx = PgFixture::shared();
    let (cp, eng, _writer) = setup_iceberg(fx).await;
    let cp = Arc::new(cp);
    cp.create_user(&NewUser {
        subject_id: SubjectId("al".into()),
        username: "al".into(),
        password_phc: hash_password("orig").expect("hash"),
    })
    .await
    .unwrap();
    // log in to get a real session token
    let login_body = serde_json::json!({ "username": "al", "password": "orig" });
    let res = app(cp.clone(), eng.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/login")
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&login_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = http_body_util::BodyExt::collect(res.into_body())
        .await
        .unwrap()
        .to_bytes();
    let token = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();

    // change the password
    let body = serde_json::json!({ "current": "orig", "new": "fresh" });
    let res = app(cp.clone(), eng.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/password")
                .header("Authorization", format!("Bearer {token}"))
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // new password logs in, old does not
    for (pw, want) in [("fresh", StatusCode::OK), ("orig", StatusCode::UNAUTHORIZED)] {
        let b = serde_json::json!({ "username": "al", "password": pw });
        let res = app(cp.clone(), eng.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/auth/login")
                    .header("Content-Type", "application/json")
                    .body(Body::from(serde_json::to_vec(&b).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), want, "login with {pw}");
    }
}
```

Match the file's existing imports (`NewUser`, `SubjectId`, `hash_password`, `Request`, `Body`, `StatusCode`, `serde_json`, `tower::ServiceExt`); add any missing to the `use` lines and any missing dep to the `auth-e2e` BUCK target.

- [ ] **Step 2: Add the admin-reset e2e**

In `src/services/query-api/tests/admin_e2e.rs`, add (using its `app`, `seed_admin_session`, `seed_session` helpers):
```rust
#[tokio::test]
async fn admin_reset_over_postgres() {
    let fx = PgFixture::shared();
    let cp = Arc::new(fx.fresh_control_plane().await);
    let admin_token = seed_admin_session(&cp, "root").await;
    let victim_token = seed_session(&cp, "victim").await;

    let body = serde_json::json!({ "new": "reset-pw" });
    let res = app(cp.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/users/victim/password")
                .header("Authorization", format!("Bearer {admin_token}"))
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // victim's sessions revoked; new password verifies via a fresh login
    assert!(
        cp.resolve_session(&token_sha256(&victim_token), time::OffsetDateTime::now_utc())
            .await
            .unwrap()
            .is_none()
    );
    let login = serde_json::json!({ "username": "victim", "password": "reset-pw" });
    // NOTE: admin_e2e's `app` may not mount login_routes; if it doesn't, assert the
    // credential via cp.find_password_credential("victim") + verify_password instead:
    let cred = cp.find_password_credential("victim").await.unwrap().unwrap();
    assert!(service_runtime::verify_password("reset-pw", &cred.password_phc));
    let _ = login;
}
```
Prefer the `find_password_credential` + `verify_password` assertion (no dependency on whether `admin_e2e`'s `app` mounts `login_routes`). Add `service_runtime::verify_password` and `token_sha256` to the imports if absent.

- [ ] **Step 3: Run the e2e**

Run: `buck2 test //src/services/query-api:auth-e2e //src/services/query-api:admin-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/services/query-api
git commit -m "test(auth): postgres e2e for password change and admin reset"
```

---

## Task 7: Register close + docs

**Files:**
- Modify: `docs/ROADMAP.md` (close `road-auth-password-lifecycle`)
- Modify: `docs/FUTURE.md` (confirm `fut-auth-login-rate-limit`, `fut-auth-password-policy`, `fut-auth-email-reset` present as deferred; add if missing)

- [ ] **Step 1: Run `loom-docs-update`**

Use the `loom-docs-update` skill to close `road-auth-password-lifecycle` (`- [ ]`→`- [x]`, `status:planned`→`status:done`, add `pr:#N` once the PR exists) and record the spec's explicitly-deferred follow-ons (email reset, per-IP rate limiting, password policy) in FUTURE if not already present.

- [ ] **Step 2: Validate the registers**

Run: `bash tools/docs.sh validate 2>&1 | tail`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add docs
git commit -m "docs(auth): close road-auth-password-lifecycle"
```

---

## Final verification (whole feature)

Run the scoped build + test set from **Global Constraints**, plus `prek` and the metric gate:
```bash
buck2 run //tools:prek -- run --all-files 2>&1 | tail -20   # fixes trailing ws / EOF; commit any changes
```
Then the metric gate (part of the final review): `loom-complexity diff` and `loom-duplication diff` — report any NEW cc>15 / cognitive>15 / MI<20 / SLOC>100 hotspot or NEW cross-file duplication ≥20 lines, and fix or justify each in the PR body.

## Spec ↔ task traceability

- `Auth::update_password` → Task 1.
- Store-backed lockout (`record_failed_login` / `reset_failed_logins`, columns migration, both adapters + testkit) → Task 1.
- Login read surfaces `locked_until` (extend `PasswordCredential`) → Task 1.
- `POST /auth/password` self-service change (verify-current 403, revoke-others, keep-current) → Task 4.
- `POST /admin/users/{username}/password` admin reset (revoke-all, 404 unknown, 403 non-admin) → Task 5.
- Lockout enforcement in the login path + config seam (`LOOM_LOGIN_LOCKOUT_*`) → Tasks 2 & 3.
- Testing 1 (update round-trip) & 2 (lockout counter) → Task 1 contract. Testing 3 (self-service e2e) → Tasks 4 & 6. Testing 4 (admin reset e2e) → Tasks 5 & 6. Testing 5 (lockout e2e) → Tasks 3 & 6. Testing 6 (no enumeration: locked == generic 401) → Task 3 (`login` returns the same `unauthorized()` for locked, bad-password, and unknown).
