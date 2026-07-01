# Service-Account API Tokens Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give non-interactive machine callers a rotation-friendly bearer **API token** that resolves to an ACL `SubjectId` exactly like a human session, issued/managed out-of-band by the bootstrap admin.

**Architecture:** A service account is its own `auth.service_account` entity (an ACL subject with no password), and tokens live in a distinct `auth.service_token` table with a mandatory TTL. The control-plane `Auth` trait gains five persist-only methods (both adapters + testkit contract). `service_runtime`'s `require_auth` middleware resolves a bearer token as *session OR service token* through a single `resolve_bearer` helper (still one 401 path). Admin-gated management routes (create account, mint/list/revoke token) mount alongside `/auth/login` in both service binaries, capped by `LOOM_SERVICE_TOKEN_MAX_TTL`.

**Tech Stack:** Rust, buck2, axum, sqlx compile-time `query!` (postgres adapter), Argon2/SHA-256 crypto helpers (already in `service_runtime::crypto`), `time::OffsetDateTime`.

## Global Constraints

- **Tests are `rust_test` integration targets only** — put unit tests in a sibling `tests/<name>.rs` wired as its own target; never inline `#[cfg(test)]`. Fixture-backed (postgres) tests use `loom_fixture_test`, pure-logic tests use `rust_test`.
- **Postgres adapter uses compile-time `query!`/`query_scalar!`.** After changing any SQL, regenerate the committed cache with `bash tools/sqlx-prepare.sh` and commit `src/control-plane/postgres/.sqlx/`. The `sqlx-cache-check` test enforces freshness.
- **Clippy is strict** (pedantic + restriction; `unwrap_used`/`expect_used`/`indexing_slicing`/`panic` enforced on non-test code). Test code is exempted from panic-safety lints via the `loom_rust_test`/`loom_fixture_test` wrappers.
- **The trait persists but performs NO cryptography.** Token minting/hashing lives in `service_runtime::crypto`. The `Auth` trait only stores/reads the SHA-256 and metadata — plaintext token never reaches the trait.
- **Migration numbering:** the next migration is `0022_` (last on disk is `0021_vector_index_named.sql`). Migrations are **embedded at compile time** by `sqlx::migrate!("./migrations")` (`postgres/src/lib.rs:84`); the new file needs no explicit registration because the `postgres` target's `mapped_srcs = {f: f for f in glob(["migrations/*.sql"])}` (`postgres/BUCK`) captures it into the sandbox. If a build seems to ignore the new migration, rebuild `//src/control-plane/postgres:postgres` (recompilation re-embeds it) rather than looking for a runtime migrations dir.
- **Run the suite:** `buck2 test //src/...`. Don't pipe long `buck2 test` through `head`/`tail`; redirect to a file and grep it: `buck2 test … > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **Markdown lint:** any `.md` you touch must end with exactly one trailing newline and have no trailing whitespace.

---

## File Structure

**Task 1 — control-plane data layer:**
- Create: `src/control-plane/postgres/migrations/0022_service_accounts.sql`
- Modify: `src/control-plane/core/src/auth.rs` (new types + trait methods)
- Modify: `src/control-plane/memory/src/auth.rs` (impl)
- Modify: `src/control-plane/postgres/src/auth.rs` (impl)
- Modify: `src/control-plane/core/src/lib.rs` (re-export new types)
- Modify: `src/control-plane/testkit/src/lib.rs` (new `service_account_contract`)
- Modify: `src/control-plane/postgres/tests/auth.rs`, `src/control-plane/memory/tests/auth.rs` (call the contract)
- Modify: `src/control-plane/core/tests/auth_types.rs` (shape guard for new types)
- Regenerate: `src/control-plane/postgres/.sqlx/`

**Task 2 — middleware:**
- Modify: `src/services/runtime/src/auth.rs` (`resolve_bearer` + `require_auth`)
- Modify: `src/services/runtime/tests/auth_middleware.rs` (service-token cases)

**Task 3 — management routes + wiring:**
- Modify: `src/services/runtime/src/auth.rs` (`service_account_routes`, admin gate, DTOs, `service_token_max_ttl_from_env`)
- Modify: `src/services/runtime/src/lib.rs` (re-exports)
- Create: `src/services/runtime/tests/service_accounts.rs` (e2e route tests)
- Modify: `src/services/runtime/BUCK` (new test target)
- Modify: `src/services/query-api/src/main.rs`, `src/services/ingest/src/main.rs` (mount routes)

---

## Task 1: Control-plane data layer (types, trait, both adapters, contract, migration, sqlx)

This is one atomic task: adding methods to the `Auth` trait breaks both adapter `impl`s until they implement them, so the trait + memory + postgres + contract land together and leave `buck2 test //src/control-plane/...` green.

**Files:**
- Create: `src/control-plane/postgres/migrations/0022_service_accounts.sql`
- Modify: `src/control-plane/core/src/auth.rs`
- Modify: `src/control-plane/core/src/lib.rs:26`
- Modify: `src/control-plane/memory/src/auth.rs`
- Modify: `src/control-plane/postgres/src/auth.rs`
- Modify: `src/control-plane/testkit/src/lib.rs` (after `auth_contract`, ~line 1803)
- Modify: `src/control-plane/postgres/tests/auth.rs`
- Modify: `src/control-plane/memory/tests/auth.rs`
- Modify: `src/control-plane/core/tests/auth_types.rs`

**Interfaces:**
- Consumes: existing `SubjectId`, `Page`, `PageReq`, `Result`, `ControlPlaneError` from `control_plane_core`; the existing `Auth` trait, `MemoryControlPlane` (`self.auth: Arc<Mutex<AuthState>>`, `self.acl`), `PgControlPlane` (`self.pool()`, `backend`, `conflict_or_backend`).
- Produces (relied on by Tasks 2 & 3):
  - `NewServiceAccount { subject_id: SubjectId, name: String }`
  - `ServiceAccount { subject_id: SubjectId, name: String, created_at: OffsetDateTime }`
  - `ServiceToken { token_sha256: [u8; 32], subject_id: SubjectId, label: String, created_at: OffsetDateTime, expires_at: OffsetDateTime, revoked_at: Option<OffsetDateTime> }`
  - `Auth::create_service_account(&self, account: &NewServiceAccount) -> Result<()>`
  - `Auth::create_service_token(&self, subject: &SubjectId, token_sha256: &[u8; 32], label: &str, expires_at: OffsetDateTime) -> Result<()>`
  - `Auth::resolve_service_token(&self, token_sha256: &[u8; 32], now: OffsetDateTime) -> Result<Option<SubjectId>>`
  - `Auth::revoke_service_token(&self, token_sha256: &[u8; 32]) -> Result<()>`
  - `Auth::list_service_tokens(&self, subject: &SubjectId, page: PageReq) -> Result<Page<ServiceToken>>`
  - `Auth::list_service_accounts(&self, page: PageReq) -> Result<Page<ServiceAccount>>`

- [ ] **Step 1: Add the migration**

Create `src/control-plane/postgres/migrations/0022_service_accounts.sql`:

```sql
-- Service accounts: a machine identity that is an ACL subject (acl.subject) with
-- NO password_credential — it can never password-login. Parallel to auth.user; the
-- two never share a row. Tokens live in auth.service_token, keyed by the token HASH
-- (sha-256), never the raw token, exactly like auth.session. Every token has a
-- mandatory expiry; rotation = mint-new-then-revoke-old, overlapping allowed.
create table auth.service_account (
    subject_id text primary key,
    name       text unique not null,
    created_at timestamptz not null default now()
);

create table auth.service_token (
    token_sha256 bytea primary key,
    subject_id   text not null references auth.service_account (subject_id) on delete cascade,
    label        text not null,
    created_at   timestamptz not null default now(),
    expires_at   timestamptz not null,
    revoked_at   timestamptz
);

create index service_token_subject_id_idx on auth.service_token (subject_id);
```

- [ ] **Step 2: Add core types + trait methods**

In `src/control-plane/core/src/auth.rs`, add after `PasswordCredential` (line 29) — new types:

```rust
/// A service account to be created: an ACL subject and a unique operator-facing
/// name. It has NO password credential (cannot password-login). Machine identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewServiceAccount {
    pub subject_id: SubjectId,
    pub name: String,
}

/// A service account's stored metadata, returned by `list_service_accounts`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceAccount {
    pub subject_id: SubjectId,
    pub name: String,
    pub created_at: OffsetDateTime,
}

/// A service token's stored metadata, returned by `list_service_tokens`. Carries
/// the token's SHA-256 (its stable id — the raw token is never stored or returned),
/// its label, and its lifecycle timestamps. `revoked_at.is_some()` ⇒ revoked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceToken {
    /// SHA-256 of the issued token; the row's primary key and its addressable id.
    pub token_sha256: [u8; 32],
    pub subject_id: SubjectId,
    pub label: String,
    pub created_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
    pub revoked_at: Option<OffsetDateTime>,
}
```

Then add these methods to the `Auth` trait (after `has_any_user`, before the closing `}`). Import `Page`/`PageReq` at the top of the file (`use crate::page::{Page, PageReq};`):

```rust
    /// Create a service account bound to `account.subject_id`. Ensures the ACL
    /// subject exists (so the account is immediately a valid ACL principal, exactly
    /// like `create_user`). `Conflict` if the name is already taken. No password.
    async fn create_service_account(&self, account: &NewServiceAccount) -> Result<()>;

    /// Persist a service token: the SHA-256 of the issued token, its owning account,
    /// a label, and a mandatory expiry. `NotFound` if `subject` is not a service
    /// account. Multiple live tokens per account are allowed (rotation).
    async fn create_service_token(
        &self,
        subject: &SubjectId,
        token_sha256: &[u8; 32],
        label: &str,
        expires_at: OffsetDateTime,
    ) -> Result<()>;

    /// Resolve a presented token hash to its account's subject, iff it is neither
    /// revoked (`revoked_at IS NULL`) nor expired (`expires_at > now`). Otherwise
    /// `Ok(None)`.
    async fn resolve_service_token(
        &self,
        token_sha256: &[u8; 32],
        now: OffsetDateTime,
    ) -> Result<Option<SubjectId>>;

    /// Revoke a service token (idempotent; no-op if absent). A revoked token never
    /// resolves again.
    async fn revoke_service_token(&self, token_sha256: &[u8; 32]) -> Result<()>;

    /// List a service account's tokens (metadata only — never the raw token),
    /// including revoked/expired ones, newest-stable order.
    async fn list_service_tokens(
        &self,
        subject: &SubjectId,
        page: PageReq,
    ) -> Result<Page<ServiceToken>>;

    /// List all service accounts (metadata), stable order.
    async fn list_service_accounts(&self, page: PageReq) -> Result<Page<ServiceAccount>>;
```

In `src/control-plane/core/src/lib.rs:26`, extend the auth re-export:

```rust
pub use auth::{
    Auth, NewServiceAccount, NewUser, PasswordCredential, ServiceAccount, ServiceToken,
};
```

- [ ] **Step 3: Write the failing contract test in testkit**

In `src/control-plane/testkit/src/lib.rs`, extend the `use control_plane_core::{...}` import block (line 19) to include `NewServiceAccount` (that is the only new type the contract *names* — `ServiceAccount`/`ServiceToken` are used only as inferred return types, so importing them would be an unused-import warning). Add after `auth_contract` (which ends ~line 1803):

```rust
/// Contract for the service-account + service-token `Auth` ops. `a` must be freshly
/// empty. Bound on `Acl` too so we can prove `create_service_account` made the
/// subject a real ACL principal (role assignment succeeds).
pub async fn service_account_contract<A: Auth + Acl>(a: &A) {
    let sid = |s: &str| SubjectId(s.to_string());
    let h = |b: u8| -> [u8; 32] { [b; 32] };
    let now = OffsetDateTime::now_utc();
    let future = now + time::Duration::hours(1);

    // --- create_service_account ---
    a.create_service_account(&NewServiceAccount {
        subject_id: sid("svc-etl"),
        name: "nightly-etl".into(),
    })
    .await
    .unwrap();

    // create_service_account ensured the ACL subject: assigning a role succeeds
    // (it returns NotFound for an unknown subject), proving ACL parity with a user.
    a.define_role(&RoleId("r".into())).await.unwrap();
    a.assign_role(&sid("svc-etl"), &RoleId("r".into()))
        .await
        .unwrap();

    // duplicate name → Conflict
    let dup = a
        .create_service_account(&NewServiceAccount {
            subject_id: sid("svc-other"),
            name: "nightly-etl".into(),
        })
        .await;
    assert!(matches!(dup, Err(ControlPlaneError::Conflict(_))));

    // list_service_accounts returns the account metadata (name), never a token.
    let accounts = a.list_service_accounts(PageReq::unbounded()).await.unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts.items[0].name, "nightly-etl");
    assert_eq!(accounts.items[0].subject_id, sid("svc-etl"));

    // --- create_service_token / resolve ---
    a.create_service_token(&sid("svc-etl"), &h(1), "primary", future)
        .await
        .unwrap();
    assert_eq!(
        a.resolve_service_token(&h(1), now).await.unwrap(),
        Some(sid("svc-etl")),
        "a live token resolves to its account"
    );

    // minting for an unknown account → NotFound
    assert!(matches!(
        a.create_service_token(&sid("ghost"), &h(2), "x", future)
            .await,
        Err(ControlPlaneError::NotFound(_))
    ));

    // unknown token hash → None
    assert!(a.resolve_service_token(&h(9), now).await.unwrap().is_none());

    // expired (expires_at <= now) → None
    assert!(
        a.resolve_service_token(&h(1), now + time::Duration::hours(2))
            .await
            .unwrap()
            .is_none(),
        "an expired token does not resolve"
    );

    // rotation: a second live token overlaps the first.
    a.create_service_token(&sid("svc-etl"), &h(3), "rotated", future)
        .await
        .unwrap();
    assert_eq!(
        a.resolve_service_token(&h(3), now).await.unwrap(),
        Some(sid("svc-etl"))
    );

    // list_service_tokens returns BOTH, metadata only (label/expiry), never the raw
    // token — the type has no plaintext field. Scoped to the account's subject.
    let tokens = a
        .list_service_tokens(&sid("svc-etl"), PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(tokens.len(), 2, "both minted tokens are listed");
    let labels: HashSet<String> = tokens.items.iter().map(|t| t.label.clone()).collect();
    assert_eq!(
        labels,
        ["primary", "rotated"].iter().map(|s| s.to_string()).collect()
    );
    assert!(
        tokens.items.iter().all(|t| t.revoked_at.is_none()),
        "neither token is revoked yet"
    );

    // --- revoke (idempotent; reflected in resolve) ---
    a.revoke_service_token(&h(1)).await.unwrap();
    assert!(
        a.resolve_service_token(&h(1), now).await.unwrap().is_none(),
        "a revoked token does not resolve"
    );
    assert_eq!(
        a.resolve_service_token(&h(3), now).await.unwrap(),
        Some(sid("svc-etl")),
        "revoking one token leaves the other live"
    );
    a.revoke_service_token(&h(1)).await.unwrap(); // idempotent no-op

    // the revoked token still lists, now with revoked_at set.
    let after = a
        .list_service_tokens(&sid("svc-etl"), PageReq::unbounded())
        .await
        .unwrap();
    let revoked = after
        .items
        .iter()
        .find(|t| t.token_sha256 == h(1))
        .expect("revoked token still listed");
    assert!(revoked.revoked_at.is_some(), "revoked_at recorded");

    // tokens are scoped: an account with no tokens lists empty.
    a.create_service_account(&NewServiceAccount {
        subject_id: sid("svc-empty"),
        name: "empty".into(),
    })
    .await
    .unwrap();
    assert!(
        a.list_service_tokens(&sid("svc-empty"), PageReq::unbounded())
            .await
            .unwrap()
            .is_empty()
    );
}
```

- [ ] **Step 4: Wire the contract into both adapter tests**

Append to `src/control-plane/memory/tests/auth.rs`:

```rust
#[tokio::test]
async fn memory_passes_service_account_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::service_account_contract(&cp).await;
}
```

Append to `src/control-plane/postgres/tests/auth.rs`:

```rust
#[tokio::test]
async fn postgres_passes_service_account_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::service_account_contract(&cp).await;
}
```

- [ ] **Step 5: Run the memory contract to verify it FAILS to compile (methods missing)**

Run: `buck2 build //src/control-plane/memory:auth > /tmp/t.log 2>&1; grep -E "error|Tests finished|BUILD SUCCEEDED|FAIL" /tmp/t.log | head`
Expected: compile error — `create_service_account` etc. not found on `MemoryControlPlane`.

- [ ] **Step 6: Implement the memory adapter**

In `src/control-plane/memory/src/auth.rs`, extend the state structs and add the impl. Add `use control_plane_core::{..., NewServiceAccount, ServiceAccount, ServiceToken, Page, PageReq};` to the import. Add records + maps:

```rust
struct MemServiceAccount {
    subject_id: String,
    name: String,
    created_at: OffsetDateTime,
}

struct MemServiceToken {
    subject_id: String,
    label: String,
    created_at: OffsetDateTime,
    expires_at: OffsetDateTime,
    revoked_at: Option<OffsetDateTime>,
}

#[derive(Default)]
pub(crate) struct AuthState {
    /// username -> user
    users: HashMap<String, MemUser>,
    /// token sha-256 -> session
    sessions: HashMap<[u8; 32], MemSession>,
    /// subject_id -> service account
    service_accounts: HashMap<String, MemServiceAccount>,
    /// token sha-256 -> service token
    service_tokens: HashMap<[u8; 32], MemServiceToken>,
}
```

Add the six methods to `impl Auth for MemoryControlPlane`:

```rust
    #[tracing::instrument(skip(self, account), level = "debug")]
    async fn create_service_account(&self, account: &NewServiceAccount) -> Result<()> {
        let mut auth = self.auth.lock();
        if auth
            .service_accounts
            .values()
            .any(|a| a.name == account.name)
        {
            return Err(ControlPlaneError::Conflict(format!(
                "service account name {}",
                account.name
            )));
        }
        auth.service_accounts.insert(
            account.subject_id.0.clone(),
            MemServiceAccount {
                subject_id: account.subject_id.0.clone(),
                name: account.name.clone(),
                created_at: OffsetDateTime::now_utc(),
            },
        );
        drop(auth);
        // Ensure the ACL subject exists (so the account is a valid ACL principal).
        self.acl.lock().subjects_insert(&account.subject_id.0);
        Ok(())
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn create_service_token(
        &self,
        subject: &SubjectId,
        token_sha256: &[u8; 32],
        label: &str,
        expires_at: OffsetDateTime,
    ) -> Result<()> {
        let mut auth = self.auth.lock();
        if !auth.service_accounts.contains_key(&subject.0) {
            return Err(ControlPlaneError::NotFound(format!(
                "service account {}",
                subject.0
            )));
        }
        auth.service_tokens.insert(
            *token_sha256,
            MemServiceToken {
                subject_id: subject.0.clone(),
                label: label.to_string(),
                created_at: OffsetDateTime::now_utc(),
                expires_at,
                revoked_at: None,
            },
        );
        Ok(())
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn resolve_service_token(
        &self,
        token_sha256: &[u8; 32],
        now: OffsetDateTime,
    ) -> Result<Option<SubjectId>> {
        let auth = self.auth.lock();
        Ok(auth.service_tokens.get(token_sha256).and_then(|t| {
            (t.revoked_at.is_none() && t.expires_at > now)
                .then(|| SubjectId(t.subject_id.clone()))
        }))
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn revoke_service_token(&self, token_sha256: &[u8; 32]) -> Result<()> {
        if let Some(t) = self.auth.lock().service_tokens.get_mut(token_sha256)
            && t.revoked_at.is_none()
        {
            t.revoked_at = Some(OffsetDateTime::now_utc());
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_service_tokens(
        &self,
        subject: &SubjectId,
        _page: PageReq,
    ) -> Result<Page<ServiceToken>> {
        let auth = self.auth.lock();
        let mut items: Vec<ServiceToken> = auth
            .service_tokens
            .iter()
            .filter(|(_, t)| t.subject_id == subject.0)
            .map(|(hash, t)| ServiceToken {
                token_sha256: *hash,
                subject_id: SubjectId(t.subject_id.clone()),
                label: t.label.clone(),
                created_at: t.created_at,
                expires_at: t.expires_at,
                revoked_at: t.revoked_at,
            })
            .collect();
        items.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.token_sha256.cmp(&b.token_sha256))
        });
        Ok(Page::from_full(items))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_service_accounts(&self, _page: PageReq) -> Result<Page<ServiceAccount>> {
        let auth = self.auth.lock();
        let mut items: Vec<ServiceAccount> = auth
            .service_accounts
            .values()
            .map(|a| ServiceAccount {
                subject_id: SubjectId(a.subject_id.clone()),
                name: a.name.clone(),
                created_at: a.created_at,
            })
            .collect();
        items.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.subject_id.0.cmp(&b.subject_id.0))
        });
        Ok(Page::from_full(items))
    }
```

- [ ] **Step 7: Run the memory contract — expect PASS**

Run: `buck2 test //src/control-plane/memory:auth > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: `Tests finished: Pass 2. Fail 0.`

- [ ] **Step 8: Implement the postgres adapter**

In `src/control-plane/postgres/src/auth.rs`, extend imports:
`use control_plane_core::{Auth, ControlPlaneError, NewServiceAccount, NewUser, Page, PageReq, PasswordCredential, Result, ServiceAccount, ServiceToken, SubjectId};`

Add a FK-violation mapper next to `conflict_or_backend`:

```rust
/// Map a foreign-key violation (SQLSTATE 23503) to `NotFound`, anything else to `Backend`.
fn notfound_or_backend(e: sqlx::Error, what: &str) -> ControlPlaneError {
    if let sqlx::Error::Database(db) = &e
        && db.code().as_deref() == Some("23503")
    {
        return ControlPlaneError::NotFound(what.to_string());
    }
    ControlPlaneError::Backend(Box::new(e))
}
```

Add the six methods to `impl Auth for PgControlPlane`:

```rust
    #[tracing::instrument(skip(self, account), level = "debug")]
    async fn create_service_account(&self, account: &NewServiceAccount) -> Result<()> {
        // One transaction: ensure the ACL subject, then the service account.
        // A duplicate name aborts on the service_account insert (23505 -> Conflict).
        let mut tx = self.pool().begin().await.map_err(backend)?;
        sqlx::query!(
            "insert into acl.subject (id) values ($1) on conflict (id) do nothing",
            &account.subject_id.0,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sqlx::query!(
            "insert into auth.service_account (subject_id, name) values ($1, $2)",
            &account.subject_id.0,
            &account.name,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| conflict_or_backend(e, &format!("service account name {}", account.name)))?;
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn create_service_token(
        &self,
        subject: &SubjectId,
        token_sha256: &[u8; 32],
        label: &str,
        expires_at: OffsetDateTime,
    ) -> Result<()> {
        sqlx::query!(
            "insert into auth.service_token (token_sha256, subject_id, label, expires_at) \
             values ($1, $2, $3, $4)",
            &token_sha256[..],
            &subject.0,
            label,
            expires_at,
        )
        .execute(self.pool())
        .await
        .map_err(|e| notfound_or_backend(e, &format!("service account {}", subject.0)))?;
        Ok(())
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn resolve_service_token(
        &self,
        token_sha256: &[u8; 32],
        now: OffsetDateTime,
    ) -> Result<Option<SubjectId>> {
        let row = sqlx::query_scalar!(
            "select subject_id from auth.service_token \
             where token_sha256 = $1 and revoked_at is null and expires_at > $2",
            &token_sha256[..],
            now,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?;
        Ok(row.map(SubjectId))
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn revoke_service_token(&self, token_sha256: &[u8; 32]) -> Result<()> {
        sqlx::query!(
            "update auth.service_token set revoked_at = now() \
             where token_sha256 = $1 and revoked_at is null",
            &token_sha256[..],
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_service_tokens(
        &self,
        subject: &SubjectId,
        _page: PageReq,
    ) -> Result<Page<ServiceToken>> {
        let rows = sqlx::query!(
            "select token_sha256, subject_id, label, created_at, expires_at, revoked_at \
             from auth.service_token where subject_id = $1 \
             order by created_at, token_sha256",
            &subject.0,
        )
        .fetch_all(self.pool())
        .await
        .map_err(backend)?;
        let items = rows
            .into_iter()
            .map(|r| {
                // The bytea is always 32 bytes (every insert writes a [u8; 32]), but
                // convert fallibly — no panic path — to satisfy the panic-safety lints.
                let hash: [u8; 32] = r.token_sha256.as_slice().try_into().map_err(|_| {
                    ControlPlaneError::Backend("service_token.token_sha256 not 32 bytes".into())
                })?;
                Ok(ServiceToken {
                    token_sha256: hash,
                    subject_id: SubjectId(r.subject_id),
                    label: r.label,
                    created_at: r.created_at,
                    expires_at: r.expires_at,
                    revoked_at: r.revoked_at,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Page::from_full(items))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn list_service_accounts(&self, _page: PageReq) -> Result<Page<ServiceAccount>> {
        let rows = sqlx::query!(
            "select subject_id, name, created_at from auth.service_account \
             order by created_at, subject_id",
        )
        .fetch_all(self.pool())
        .await
        .map_err(backend)?;
        let items = rows
            .into_iter()
            .map(|r| ServiceAccount {
                subject_id: SubjectId(r.subject_id),
                name: r.name,
                created_at: r.created_at,
            })
            .collect();
        Ok(Page::from_full(items))
    }
```

`ControlPlaneError::Backend` takes a `Box<dyn Error + Send + Sync>`; `"...".into()` builds one from a `&str` via the `From<&str>` impl. This matches the existing `backend()` usage in the file.

- [ ] **Step 9: Regenerate the sqlx cache**

Run: `bash tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; echo "exit=$?"; tail -5 /tmp/sqlx.log`
Expected: `exit=0`; new `query-*.json` files appear under `src/control-plane/postgres/.sqlx/`. If the environment cannot boot postgres here, flag it for the orchestrator to run.

- [ ] **Step 10: Update the core type shape guard**

Append to `src/control-plane/core/tests/auth_types.rs` (do NOT touch the top-level `use` — this test imports what it needs locally, reusing the top-level `SubjectId`):

```rust
#[test]
fn service_account_types_construct() {
    use control_plane_core::{NewServiceAccount, ServiceAccount, ServiceToken};
    use time::OffsetDateTime;

    let na = NewServiceAccount {
        subject_id: SubjectId("svc".into()),
        name: "etl".into(),
    };
    assert_eq!(na.name, "etl");

    let now = OffsetDateTime::now_utc();
    let acct = ServiceAccount {
        subject_id: na.subject_id.clone(),
        name: na.name.clone(),
        created_at: now,
    };
    assert_eq!(acct.subject_id, na.subject_id);

    let tok = ServiceToken {
        token_sha256: [7u8; 32],
        subject_id: na.subject_id.clone(),
        label: "primary".into(),
        created_at: now,
        expires_at: now,
        revoked_at: None,
    };
    assert_eq!(tok.token_sha256, [7u8; 32]);
    assert!(tok.revoked_at.is_none());
}
```

The test uses `time::OffsetDateTime`, so add `//third-party:time` to the `auth-types` test target's `deps` in `src/control-plane/core/BUCK` (currently `deps = [":core"]`):

```python
    deps = [":core", "//third-party:time"],
```

- [ ] **Step 11: Run the whole control-plane suite — expect PASS**

Run: `buck2 test //src/control-plane/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: all pass, including `postgres:auth`, `memory:auth`, `core:auth-types`, and `postgres:sqlx-cache-check`.

- [ ] **Step 12: Commit**

```bash
git add src/control-plane docs/superpowers/plans
git commit -m "feat(auth): service-account + service-token control-plane layer

Add auth.service_account/auth.service_token tables (migration 0022), the
five Auth trait methods on both adapters, and the testkit
service_account_contract covering create/mint/resolve/revoke/list, TTL
expiry, rotation overlap, and ACL-principal parity."
```

---

## Task 2: Middleware — resolve session OR service token

**Files:**
- Modify: `src/services/runtime/src/auth.rs` (add `resolve_bearer`, call it from `require_auth`)
- Modify: `src/services/runtime/tests/auth_middleware.rs`

**Interfaces:**
- Consumes: `Auth::resolve_session`, `Auth::resolve_service_token` (Task 1), the existing `AuthState`, `bearer_token`, `token_sha256`, `Subject`, `require_auth`.
- Produces: unchanged public `require_auth` behavior (one 401 path) that now also accepts service tokens. No new public symbol required (`resolve_bearer` is a private helper).

- [ ] **Step 1: Write the failing middleware tests**

In `src/services/runtime/tests/auth_middleware.rs`, extend imports to include what you need (`control_plane_core::NewServiceAccount`) and add:

```rust
async fn seed_service_token(
    cp: &MemoryControlPlane,
    account: &str,
    name: &str,
    token: &str,
    expires: OffsetDateTime,
) {
    cp.create_service_account(&control_plane_core::NewServiceAccount {
        subject_id: SubjectId(account.into()),
        name: name.into(),
    })
    .await
    .unwrap();
    cp.create_service_token(&SubjectId(account.into()), &token_sha256(token), "t", expires)
        .await
        .unwrap();
}

#[tokio::test]
async fn valid_service_token_authenticates() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_service_token(
        &cp,
        "svc-etl",
        "etl",
        "svc-token-xyz",
        OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await;
    assert_eq!(
        bearer(app(cp), Some("svc-token-xyz")).await,
        StatusCode::OK,
        "a live service token passes require_auth like a session"
    );
}

#[tokio::test]
async fn expired_service_token_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_service_token(
        &cp,
        "svc-etl",
        "etl",
        "svc-stale",
        OffsetDateTime::now_utc() - time::Duration::hours(1),
    )
    .await;
    assert_eq!(
        bearer(app(cp), Some("svc-stale")).await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn revoked_service_token_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_service_token(
        &cp,
        "svc-etl",
        "etl",
        "svc-revoked",
        OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await;
    cp.revoke_service_token(&token_sha256("svc-revoked"))
        .await
        .unwrap();
    assert_eq!(
        bearer(app(cp), Some("svc-revoked")).await,
        StatusCode::UNAUTHORIZED
    );
}
```

- [ ] **Step 2: Run to verify the new tests FAIL**

Run: `buck2 test //src/services/runtime:auth-middleware > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: FAIL — `valid_service_token_authenticates` returns 401 (service tokens not yet accepted).

- [ ] **Step 3: Add `resolve_bearer` and call it from `require_auth`**

In `src/services/runtime/src/auth.rs`, add above `require_auth`:

```rust
/// Resolve a bearer token hash to a subject: try a login session first (interactive
/// traffic dominates), then a service token. Both are constant-time hash lookups, so
/// order is performance-only. This is the single sequencing point so there stays
/// exactly one path that produces `Unauthorized`.
async fn resolve_bearer(
    auth: &(dyn Auth + Send + Sync),
    hash: &[u8; 32],
    now: OffsetDateTime,
) -> Result<Option<SubjectId>, ControlPlaneError> {
    if let Some(sid) = auth.resolve_session(hash, now).await? {
        return Ok(Some(sid));
    }
    auth.resolve_service_token(hash, now).await
}
```

Change the body of `require_auth` to call it (replace the `st.auth.resolve_session(...)` match):

```rust
    let hash = token_sha256(&token);
    match resolve_bearer(st.auth.as_ref(), &hash, OffsetDateTime::now_utc()).await {
        Ok(Some(sid)) => {
            req.extensions_mut().insert(Subject(sid));
            next.run(req).await
        }
        Ok(None) => unauthorized(),
        Err(e) => status_for(&e).into_response(),
    }
```

(Ensure `ControlPlaneError` is already imported — it is, at line 17.)

- [ ] **Step 4: Run the middleware suite — expect PASS (no session regression)**

Run: `buck2 test //src/services/runtime:auth-middleware > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all pass — the three new service-token tests AND the four existing session tests.

- [ ] **Step 5: Commit**

```bash
git add src/services/runtime/src/auth.rs src/services/runtime/tests/auth_middleware.rs
git commit -m "feat(auth): require_auth resolves session or service token

Sequence resolve_session then resolve_service_token through one
resolve_bearer helper so a machine bearer token authenticates exactly like
a login session, with a single 401 path and no session regression."
```

---

## Task 3: Management routes — admin-gated create/mint/list/revoke + binary wiring

**Files:**
- Modify: `src/services/runtime/src/auth.rs` (DTOs, `ServiceAccountState`, admin gate, five handlers, `service_account_routes`, `service_token_max_ttl_from_env`)
- Modify: `src/services/runtime/src/lib.rs` (re-export `service_account_routes`, `service_token_max_ttl_from_env`)
- Create: `src/services/runtime/tests/service_accounts.rs`
- Modify: `src/services/runtime/BUCK` (new `service-accounts` test target)
- Modify: `src/services/query-api/src/main.rs`, `src/services/ingest/src/main.rs`

**Interfaces:**
- Consumes: `Auth::create_service_account`, `create_service_token`, `revoke_service_token`, `list_service_accounts`, `list_service_tokens` (Task 1); `resolve_bearer`/`require_auth` (Task 2); existing `AuthState`, `Subject`, `protect`, `generate_session_token`, `token_sha256`, `status_for`.
- Produces:
  - `pub fn service_account_routes(auth: AuthState, admin_subject: Option<SubjectId>, max_ttl: Duration) -> Router`
  - `pub fn service_token_max_ttl_from_env() -> Duration` (reads `LOOM_SERVICE_TOKEN_MAX_TTL` seconds, default 90 days)

- [ ] **Step 1: Add the management state, admin gate, DTOs, handlers, and router**

In `src/services/runtime/src/auth.rs`, add (after `session_routes`). Extend the top imports: `use control_plane_core::{Auth, ControlPlaneError, NewServiceAccount, NewUser, PageReq, SubjectId};` and add `use axum::extract::Path;`.

```rust
// ---------------------------------------------------------------------------
// Service-account management (admin-gated)
// ---------------------------------------------------------------------------

/// Shared state for the admin-gated service-account routes. Carries the authn store,
/// the bootstrap-admin subject the gate compares against (None ⇒ management is closed
/// to everyone), and the mandatory-TTL cap.
#[derive(Clone)]
struct ServiceAccountState {
    auth: Arc<dyn Auth + Send + Sync>,
    admin_subject: Option<SubjectId>,
    max_ttl: Duration,
}

/// 403 unless the verified subject is the configured bootstrap admin. This is the
/// only admin notion today; a first-class auth-admin ACL capability is deferred.
fn ensure_admin(subject: &Subject, st: &ServiceAccountState) -> Result<(), Response> {
    match &st.admin_subject {
        Some(admin) if *admin == subject.0 => Ok(()),
        _ => Err((StatusCode::FORBIDDEN, "service-account management is admin-only").into_response()),
    }
}

#[derive(serde::Deserialize)]
struct CreateAccountReq {
    name: String,
}

#[derive(serde::Serialize)]
struct AccountResp {
    subject_id: String,
    name: String,
}

#[derive(serde::Deserialize)]
struct MintTokenReq {
    label: String,
    /// Requested lifetime in seconds. `expires_at = now + ttl`, capped at max_ttl.
    ttl_secs: u64,
}

#[derive(serde::Serialize)]
struct MintTokenResp {
    /// The plaintext token — returned exactly once, never persisted or re-derivable.
    token: String,
    /// Hex SHA-256 of the token: its stable id, usable in the revoke URL.
    token_id: String,
    label: String,
    /// Expiry as a Unix timestamp (seconds).
    expires_at: i64,
}

#[derive(serde::Serialize)]
struct TokenMetaResp {
    token_id: String,
    label: String,
    created_at: i64,
    expires_at: i64,
    revoked_at: Option<i64>,
}

/// `POST /auth/service-accounts` — admin. Create a service account.
async fn create_account(
    subject: Subject,
    State(st): State<ServiceAccountState>,
    axum::Json(req): axum::Json<CreateAccountReq>,
) -> Response {
    if let Err(r) = ensure_admin(&subject, &st) {
        return r;
    }
    // subject_id == name keeps machine identities operator-legible, mirroring how the
    // bootstrap admin's subject_id equals its username.
    let account = NewServiceAccount {
        subject_id: SubjectId(req.name.clone()),
        name: req.name.clone(),
    };
    match st.auth.create_service_account(&account).await {
        Ok(()) => (
            StatusCode::OK,
            axum::Json(AccountResp {
                subject_id: req.name.clone(),
                name: req.name,
            }),
        )
            .into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// `GET /auth/service-accounts` — admin. List service accounts.
async fn list_accounts(subject: Subject, State(st): State<ServiceAccountState>) -> Response {
    if let Err(r) = ensure_admin(&subject, &st) {
        return r;
    }
    match st.auth.list_service_accounts(PageReq::unbounded()).await {
        Ok(page) => {
            let accounts: Vec<AccountResp> = page
                .items
                .into_iter()
                .map(|a| AccountResp {
                    subject_id: a.subject_id.0,
                    name: a.name,
                })
                .collect();
            (StatusCode::OK, axum::Json(serde_json::json!({ "accounts": accounts }))).into_response()
        }
        Err(e) => status_for(&e).into_response(),
    }
}

/// `POST /auth/service-accounts/{id}/tokens` — admin. Mint a token (plaintext once).
async fn mint_token(
    subject: Subject,
    State(st): State<ServiceAccountState>,
    Path(id): Path<String>,
    axum::Json(req): axum::Json<MintTokenReq>,
) -> Response {
    if let Err(r) = ensure_admin(&subject, &st) {
        return r;
    }
    // Mandatory-TTL cap: reject an over-cap (or zero) request — no immortal tokens.
    let ttl = Duration::from_secs(req.ttl_secs);
    if req.ttl_secs == 0 || ttl > st.max_ttl {
        return (
            StatusCode::BAD_REQUEST,
            format!(
                "ttl_secs must be in 1..={} (LOOM_SERVICE_TOKEN_MAX_TTL)",
                st.max_ttl.as_secs()
            ),
        )
            .into_response();
    }
    let Ok(ttl_time) = time::Duration::try_from(ttl) else {
        return (StatusCode::BAD_REQUEST, "ttl_secs too large").into_response();
    };
    let expires = OffsetDateTime::now_utc() + ttl_time;
    let token = crate::generate_session_token();
    let hash = token_sha256(&token);
    match st
        .auth
        .create_service_token(&SubjectId(id), &hash, &req.label, expires)
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            axum::Json(MintTokenResp {
                token,
                token_id: hex::encode(hash),
                label: req.label,
                expires_at: expires.unix_timestamp(),
            }),
        )
            .into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// `GET /auth/service-accounts/{id}/tokens` — admin. List a token's metadata.
async fn list_tokens(
    subject: Subject,
    State(st): State<ServiceAccountState>,
    Path(id): Path<String>,
) -> Response {
    if let Err(r) = ensure_admin(&subject, &st) {
        return r;
    }
    match st
        .auth
        .list_service_tokens(&SubjectId(id), PageReq::unbounded())
        .await
    {
        Ok(page) => {
            let tokens: Vec<TokenMetaResp> = page
                .items
                .into_iter()
                .map(|t| TokenMetaResp {
                    token_id: hex::encode(t.token_sha256),
                    label: t.label,
                    created_at: t.created_at.unix_timestamp(),
                    expires_at: t.expires_at.unix_timestamp(),
                    revoked_at: t.revoked_at.map(|r| r.unix_timestamp()),
                })
                .collect();
            (StatusCode::OK, axum::Json(serde_json::json!({ "tokens": tokens }))).into_response()
        }
        Err(e) => status_for(&e).into_response(),
    }
}

/// `DELETE /auth/service-accounts/{id}/tokens/{token_id}` — admin. Revoke a token,
/// addressed by its hex SHA-256 id (from the mint/list responses). Idempotent.
async fn revoke_token(
    subject: Subject,
    State(st): State<ServiceAccountState>,
    Path((_id, token_id)): Path<(String, String)>,
) -> Response {
    if let Err(r) = ensure_admin(&subject, &st) {
        return r;
    }
    let Ok(bytes) = hex::decode(&token_id) else {
        return (StatusCode::BAD_REQUEST, "token_id is not valid hex").into_response();
    };
    let Ok(hash): Result<[u8; 32], _> = bytes.try_into() else {
        return (StatusCode::BAD_REQUEST, "token_id must be a 32-byte sha-256").into_response();
    };
    match st.auth.revoke_service_token(&hash).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// Admin-gated service-account management routes, behind the authn gate. `auth`
/// supplies authn; `admin_subject` is the only principal allowed to manage (None ⇒
/// closed); `max_ttl` caps minted-token lifetime.
pub fn service_account_routes(
    auth: AuthState,
    admin_subject: Option<SubjectId>,
    max_ttl: Duration,
) -> Router {
    let mgmt = ServiceAccountState {
        auth: auth.auth.clone(),
        admin_subject,
        max_ttl,
    };
    protect(
        Router::new()
            .route(
                "/auth/service-accounts",
                axum::routing::post(create_account).get(list_accounts),
            )
            .route(
                "/auth/service-accounts/:id/tokens",
                axum::routing::post(mint_token).get(list_tokens),
            )
            .route(
                "/auth/service-accounts/:id/tokens/:token_id",
                axum::routing::delete(revoke_token),
            )
            .with_state(mgmt),
        auth,
    )
}

/// Read the service-token TTL cap from `LOOM_SERVICE_TOKEN_MAX_TTL` (seconds, default
/// 90 days). Mint requests over this are rejected (400).
pub fn service_token_max_ttl_from_env() -> Duration {
    std::env::var("LOOM_SERVICE_TOKEN_MAX_TTL")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(90 * 24 * 3600))
}
```

> Route path syntax: this repo is on **axum 0.7**, which uses the **colon** capture form (`:id`, `:token_id`) in `.route(...)` — see `query-api/src/http.rs:71` (`.route("/objects/:type_name", ...)`). Do NOT use `{id}` braces (that is axum 0.8, and the `{type_name}` you see in the OpenAPI `expected()` set is the utoipa *documentation* form, not the axum route form). The `Path<String>` / `Path<(String, String)>` extractors are unchanged.

- [ ] **Step 2: Re-export from `lib.rs`**

In `src/services/runtime/src/lib.rs`, extend the auth re-export (lines 6-9):

```rust
pub use auth::{
    AuthState, BootstrapError, Subject, bootstrap_admin, login_routes, protect, require_auth,
    service_account_routes, service_token_max_ttl_from_env, session_routes, session_ttl_from_env,
    status_for,
};
```

- [ ] **Step 3: Write the e2e route tests**

Create `src/services/runtime/tests/service_accounts.rs`:

```rust
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{StatusCode, header::AUTHORIZATION};
use control_plane_core::{Auth, SubjectId};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use service_runtime::{
    AuthState, service_account_routes, token_sha256,
};
use time::OffsetDateTime;
use tower::ServiceExt;

const ADMIN: &str = "root";
const MAX_TTL: Duration = Duration::from_secs(90 * 24 * 3600);

fn state(cp: Arc<MemoryControlPlane>) -> AuthState {
    AuthState {
        auth: cp,
        session_ttl: Duration::from_secs(3600),
    }
}

/// Seed a session token for `subject` and return the bearer value.
async fn session_for(cp: &MemoryControlPlane, subject: &str, token: &str) {
    cp.create_session(
        &SubjectId(subject.into()),
        &token_sha256(token),
        OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await
    .unwrap();
}

fn app(cp: Arc<MemoryControlPlane>) -> Router {
    service_account_routes(state(cp), Some(SubjectId(ADMIN.into())), MAX_TTL)
}

async fn send(app: Router, method: &str, uri: &str, bearer: Option<&str>, body: &str) -> (StatusCode, String) {
    let mut req = Request::builder().method(method).uri(uri);
    if !body.is_empty() {
        req = req.header("content-type", "application/json");
    }
    if let Some(b) = bearer {
        req = req.header(AUTHORIZATION, format!("Bearer {b}"));
    }
    let res = app
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test]
async fn admin_creates_account_and_mints_token_once() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    session_for(&cp, ADMIN, "admin-tok").await;

    // create account
    let (st, _) = send(app(cp.clone()), "POST", "/auth/service-accounts", Some("admin-tok"), r#"{"name":"etl"}"#).await;
    assert_eq!(st, StatusCode::OK);

    // mint token → plaintext returned once
    let (st, body) = send(
        app(cp.clone()),
        "POST",
        "/auth/service-accounts/etl/tokens",
        Some("admin-tok"),
        r#"{"label":"primary","ttl_secs":3600}"#,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let token = v["token"].as_str().unwrap();
    // the minted token resolves to the account
    assert_eq!(
        cp.resolve_service_token(&token_sha256(token), OffsetDateTime::now_utc())
            .await
            .unwrap(),
        Some(SubjectId("etl".into()))
    );
    // list never re-derives the plaintext
    let (st, list) = send(app(cp), "GET", "/auth/service-accounts/etl/tokens", Some("admin-tok"), "").await;
    assert_eq!(st, StatusCode::OK);
    assert!(!list.contains(token), "list must not leak the plaintext token");
}

#[tokio::test]
async fn non_admin_is_403_on_every_management_route() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    session_for(&cp, "not-admin", "user-tok").await;
    for (method, uri, body) in [
        ("POST", "/auth/service-accounts", r#"{"name":"x"}"#),
        ("GET", "/auth/service-accounts", ""),
        ("POST", "/auth/service-accounts/x/tokens", r#"{"label":"l","ttl_secs":10}"#),
        ("GET", "/auth/service-accounts/x/tokens", ""),
        ("DELETE", "/auth/service-accounts/x/tokens/00", ""),
    ] {
        let (st, _) = send(app(cp.clone()), method, uri, Some("user-tok"), body).await;
        assert_eq!(st, StatusCode::FORBIDDEN, "{method} {uri} must be admin-only");
    }
}

#[tokio::test]
async fn unauthenticated_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let (st, _) = send(app(cp), "GET", "/auth/service-accounts", None, "").await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn mint_over_max_ttl_is_400() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    session_for(&cp, ADMIN, "admin-tok").await;
    send(app(cp.clone()), "POST", "/auth/service-accounts", Some("admin-tok"), r#"{"name":"etl"}"#).await;
    let over = MAX_TTL.as_secs() + 1;
    let (st, _) = send(
        app(cp),
        "POST",
        "/auth/service-accounts/etl/tokens",
        Some("admin-tok"),
        &format!(r#"{{"label":"l","ttl_secs":{over}}}"#),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn revoke_makes_token_stop_resolving() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    session_for(&cp, ADMIN, "admin-tok").await;
    send(app(cp.clone()), "POST", "/auth/service-accounts", Some("admin-tok"), r#"{"name":"etl"}"#).await;
    let (_, body) = send(
        app(cp.clone()),
        "POST",
        "/auth/service-accounts/etl/tokens",
        Some("admin-tok"),
        r#"{"label":"primary","ttl_secs":3600}"#,
    )
    .await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let token = v["token"].as_str().unwrap().to_string();
    let token_id = v["token_id"].as_str().unwrap().to_string();
    // live before revoke
    assert!(
        cp.resolve_service_token(&token_sha256(&token), OffsetDateTime::now_utc())
            .await
            .unwrap()
            .is_some()
    );
    // revoke via the hex id
    let (st, _) = send(
        app(cp.clone()),
        "DELETE",
        &format!("/auth/service-accounts/etl/tokens/{token_id}"),
        Some("admin-tok"),
        "",
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(
        cp.resolve_service_token(&token_sha256(&token), OffsetDateTime::now_utc())
            .await
            .unwrap()
            .is_none(),
        "a revoked token no longer resolves"
    );
}

#[tokio::test]
async fn management_closed_when_no_admin_configured() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    session_for(&cp, ADMIN, "admin-tok").await;
    // admin_subject = None ⇒ nobody can manage, even the would-be admin.
    let app = service_account_routes(state(cp), None, MAX_TTL);
    let (st, _) = send(app, "GET", "/auth/service-accounts", Some("admin-tok"), "").await;
    assert_eq!(st, StatusCode::FORBIDDEN);
}
```

- [ ] **Step 4: Wire the test target in BUCK**

In `src/services/runtime/BUCK`, add after the `auth-routes` target (mirror its deps):

```python
rust_test(
    name = "service-accounts",
    crate = "service_accounts",
    srcs = ["tests/service_accounts.rs"],
    crate_root = "tests/service_accounts.rs",
    edition = "2024",
    deps = [
        ":runtime",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:axum",
        "//third-party:http-body-util",
        "//third-party:serde_json",
        "//third-party:time",
        "//third-party:tokio",
        "//third-party:tower",
    ],
)
```

- [ ] **Step 5: Run the runtime auth suites — expect PASS**

Run: `buck2 test //src/services/runtime:service-accounts //src/services/runtime:auth-middleware //src/services/runtime:auth-routes > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: all pass.

- [ ] **Step 6: Mount the routes in both binaries**

In `src/services/query-api/src/main.rs`, after the `bootstrap_admin` block (line ~63), compute the admin subject and mount. Replace the router-build chain (lines 69-79) so the merge includes the new routes:

```rust
    let admin_subject = std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME")
        .ok()
        .map(control_plane_core::SubjectId);
    let max_ttl = service_runtime::service_token_max_ttl_from_env();

    let app = service_runtime::protect(
        router(AppState {
            cp,
            serving,
            action_engine,
            default_limit: app_cfg.serving.default_limit,
        }),
        auth_state.clone(),
    )
    .merge(service_runtime::login_routes(auth_state.clone()))
    .merge(service_runtime::session_routes(auth_state.clone()))
    .merge(service_runtime::service_account_routes(
        auth_state,
        admin_subject,
        max_ttl,
    ));
```

(Note: `auth_state.clone()` on the `session_routes` line because `auth_state` is now also moved into `service_account_routes`.)

In `src/services/ingest/src/main.rs`, similarly replace lines 59-61:

```rust
    let admin_subject = std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME")
        .ok()
        .map(control_plane_core::SubjectId);
    let max_ttl = service_runtime::service_token_max_ttl_from_env();

    let app = service_runtime::protect(router(AppState { materializer, cp }), auth_state.clone())
        .merge(service_runtime::login_routes(auth_state.clone()))
        .merge(service_runtime::session_routes(auth_state.clone()))
        .merge(service_runtime::service_account_routes(
            auth_state,
            admin_subject,
            max_ttl,
        ));
```

`control_plane_core` is already a dep of both binaries (they import `ControlPlane`); if `SubjectId` isn't in scope, use the fully-qualified path as shown.

- [ ] **Step 7: Build both binaries — expect success**

Run: `buck2 build //src/services/query-api/... //src/services/ingest/... > /tmp/t.log 2>&1; grep -E "BUILD SUCCEEDED|error\[|FAILED" /tmp/t.log | head`
Expected: `BUILD SUCCEEDED`.

- [ ] **Step 8: Run clippy on the touched crates**

Run: `tools/clippy-all.sh > /tmp/clippy.log 2>&1; grep -E "warning|error|clean|FAIL" /tmp/clippy.log | head -20`
Expected: no warnings/errors introduced by the new code.

- [ ] **Step 9: Commit**

```bash
git add src/services/runtime src/services/query-api/src/main.rs src/services/ingest/src/main.rs
git commit -m "feat(auth): admin-gated service-account management routes

Add create-account / mint-token / list / revoke routes behind require_auth
and a bootstrap-admin gate, capped by LOOM_SERVICE_TOKEN_MAX_TTL. Plaintext
token returned exactly once; revoke addressed by the token's hex sha-256.
Mounted in the query-api and ingest binaries."
```

---

## Final verification (whole slice)

- [ ] Run the full suite: `buck2 test //src/... > /tmp/full.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/full.log`
  Expected: every target passes, including `postgres:sqlx-cache-check` (proves the `.sqlx` cache is fresh) and the postgres fixture `auth` contract.
- [ ] Run lint hooks: `buck2 run //tools:prek -- run --all-files > /tmp/lint.log 2>&1; grep -E "Passed|Failed|error" /tmp/lint.log` and commit any hook-applied fixes.
- [ ] Close the register item via `loom-docs-update`: flip `road-auth-service-tokens` to `- [x]`, set terminal status, add `pr:#N` and `promotes [[fut-auth-service-tokens]]` bookkeeping; record any newly-deferred follow-ons the spec names (auth-admin ACL capability, token scoping) if not already present.
