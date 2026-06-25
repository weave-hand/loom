# Password Login, Sessions, and the Authentication Seam — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new `auth` control-plane concern (credential + session store), service-side Argon2/SHA-256 crypto, an axum authentication middleware that resolves a bearer session token to a *verified* `SubjectId`, and wire both service binaries to it — replacing the self-asserted `X-Loom-Subject` header with proven identity.

**Architecture:** `auth` mirrors the five existing control-plane concerns exactly — a `core` trait (`Auth`), a `memory` fake, a `postgres` adapter over a loom-owned `auth` schema, and a `testkit` contract run against both backends. The trait is a pure store (no crypto). Hashing, token minting, the middleware, the `/auth/login`/`/auth/logout` routes, the `Subject` extractor, and bootstrap-admin seeding all live in the shared `service_runtime` crate so both binaries compose them identically. `auth` answers "who are you"; the unchanged `acl` concern keeps "what may you do"; they join by `SubjectId`.

**Tech Stack:** Rust 2024, buck2, axum 0.7, sqlx 0.9 compile-time queries, Argon2id (`argon2` crate), SHA-256 (`sha2`), `time::OffsetDateTime`, Postgres 17 (hermetic fixture), DuckDB (serving).

## Global Constraints

- **Tests are `rust_test` integration targets only** — NEVER inline `#[cfg(test)] mod tests`. The `no-inline-tests` prek hook fails if any first-party `src/**.rs` contains `#[test]`/`#[tokio::test]`. Put every test in a sibling `tests/<name>.rs` wired as its own target.
- **Fixture-backed tests (real Postgres/DuckDB) MUST use the `loom_fixture_test` macro** (`load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")`), never a bare `rust_test`, or they route to RE and fail as root. Pure-logic tests use bare `rust_test`.
- **Adding a third-party crate uses a MINIMAL-DRIFT lock update, NOT `cargo generate-lockfile`.** A whole-graph re-resolve silently downgrades `duckdb` 1.10503.1 → 1.10501.0 and breaks every DuckLake serving test at runtime. Workflow: edit `Cargo.toml`, run `eval "$(./tools/env.sh)"` then `cargo update -p argon2` (adds only the new crate + its deps), then **verify** `grep 'name = "duckdb"' -A1 Cargo.lock` still shows `1.10503.1` (if not, `cargo update -p duckdb --precise 1.10503.1`), then `./tools/buckify.sh`.
- **Run the full suite before claiming done:** `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`. NEVER pipe `buck2 test` through `tail`/`head` (it stalls). A green per-crate build is not enough — shared-dep regressions surface only in the full fixture sweep.
- **After any SQL change, regenerate the `.sqlx` cache:** `tools/sqlx-prepare.sh`, then commit `src/control-plane/postgres/.sqlx/`. The `sqlx-cache-check` fixture test enforces freshness.
- **Markdown lint:** any `.md` file ends with exactly one trailing newline, no trailing whitespace.
- **Crypto lives in the service layer, never in `core`** — the `Auth` trait persists verifiers and sessions but performs no cryptography (mirrors ACL not interpreting a `RowFilter`).
- **Transport is cleartext (no TLS)** — an accepted, recorded pre-deployment gap (`[[fut-graceful-shutdown-tls]]`); do not present this slice as production-ready.
- **The session primary key is the token HASH, never the raw token.** The raw 256-bit token is returned to the caller exactly once.
- **`#[tracing::instrument(skip(self), level = "debug")]`** on each adapter trait method (skip secret args, e.g. `skip(self, user)` / `skip(self, token_sha256)`), mirroring the ACL adapters.

---

### Task 1: Add the `argon2` third-party dependency

**Files:**
- Modify: `src/services/runtime/Cargo.toml` (add `argon2`)
- Modify (generated): `Cargo.lock`, `third-party/BUCK`

**Interfaces:**
- Produces: a `//third-party:argon2` buck target depending crates can list in `deps`. (`sha2`, `hex`, `rand_core`/`getrandom`, `subtle`, `time`, `serde`, `serde_json`, `async-trait`, `tower`, `http-body-util` already exist in `third-party/BUCK` and need no add.)

- [ ] **Step 1: Add the dependency to the runtime crate manifest**

In `src/services/runtime/Cargo.toml`, under `[dependencies]`, add:

```toml
argon2 = "0.5"
```

- [ ] **Step 2: Minimal-drift lock update (NOT generate-lockfile)**

```bash
eval "$(./tools/env.sh)"
cargo update -p argon2
grep -A1 'name = "duckdb"' Cargo.lock
```

Expected: the `duckdb` block shows `version = "1.10503.1"`. If it shows `1.10501.0`, run `cargo update -p duckdb --precise 1.10503.1` and re-check.

- [ ] **Step 3: Regenerate the buck rules**

```bash
./tools/buckify.sh
git diff --stat third-party/BUCK
```

Expected: `third-party/BUCK` gains `argon2` (and its transitive `password-hash`, `argon2`-internal deps). No unrelated `duckdb`/`libduckdb-sys` churn.

- [ ] **Step 4: Verify the target builds**

```bash
buck2 build //third-party:argon2 > /tmp/t.log 2>&1; tail -3 /tmp/t.log
```

Expected: BUILD SUCCEEDED.

- [ ] **Step 5: Commit**

```bash
git add src/services/runtime/Cargo.toml Cargo.lock third-party/BUCK
git commit -m "build(deps): vendor argon2 for password hashing"
```

---

### Task 2: The `Auth` core trait and domain types

**Files:**
- Create: `src/control-plane/core/src/auth.rs`
- Modify: `src/control-plane/core/src/lib.rs` (declare + re-export)
- Test: `src/control-plane/core/tests/auth_types.rs`
- Modify: `src/control-plane/core/BUCK` (add the `auth-types` test target)

**Interfaces:**
- Consumes: `SubjectId` (from `crate::acl`), `Result` (from `crate::error`), `time::OffsetDateTime`, `async_trait`.
- Produces — the public surface every later task binds to:
  - `trait Auth` with methods: `create_user(&self, user: &NewUser) -> Result<()>`, `find_password_credential(&self, username: &str) -> Result<Option<PasswordCredential>>`, `create_session(&self, subject: &SubjectId, token_sha256: &[u8; 32], expires_at: OffsetDateTime) -> Result<()>`, `resolve_session(&self, token_sha256: &[u8; 32], now: OffsetDateTime) -> Result<Option<SubjectId>>`, `revoke_session(&self, token_sha256: &[u8; 32]) -> Result<()>`, `has_any_user(&self) -> Result<bool>`.
  - `struct NewUser { subject_id: SubjectId, username: String, password_phc: String }`
  - `struct PasswordCredential { subject_id: SubjectId, password_phc: String }`

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/core/tests/auth_types.rs`:

```rust
//! Compile/shape guard for the auth domain types. Behavior is covered by the
//! testkit `auth_contract`; this just pins the public type surface.
use control_plane_core::{NewUser, PasswordCredential, SubjectId};

#[test]
fn new_user_and_credential_construct() {
    let u = NewUser {
        subject_id: SubjectId("alice".into()),
        username: "alice".into(),
        password_phc: "$argon2id$v=19$m=19456,t=2,p=1$abc$def".into(),
    };
    assert_eq!(u.subject_id, SubjectId("alice".into()));
    assert_eq!(u.username, "alice");

    let c = PasswordCredential {
        subject_id: u.subject_id.clone(),
        password_phc: u.password_phc.clone(),
    };
    assert_eq!(c.subject_id, u.subject_id);
    assert_eq!(c.password_phc, u.password_phc);
}
```

- [ ] **Step 2: Run it to verify it fails (does not compile)**

```bash
buck2 test //src/control-plane/core:auth-types > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log
```

Expected: failure — target unknown / unresolved imports (`NewUser`, `PasswordCredential` not exported).

- [ ] **Step 3: Write the trait and types**

Create `src/control-plane/core/src/auth.rs`:

```rust
//! The `auth` concern: loom's credential and session store. Like `acl` (which
//! stores policy but never interprets a `RowFilter`), this trait PERSISTS
//! password verifiers and sessions but performs NO cryptography — hashing,
//! verification, and token minting live in the service layer. The `auth` schema
//! is loom-owned. A loom *user* is an ACL subject that has credentials; the two
//! concerns join by [`SubjectId`].

use async_trait::async_trait;
use time::OffsetDateTime;

use crate::acl::SubjectId;
use crate::error::Result;

/// A user to be created: an ACL subject, a unique username, and the Argon2 PHC
/// verifier (computed service-side — the trait never sees the plaintext).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewUser {
    pub subject_id: SubjectId,
    pub username: String,
    /// Argon2 PHC string, computed service-side.
    pub password_phc: String,
}

/// A username's stored password verifier, returned for login.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PasswordCredential {
    pub subject_id: SubjectId,
    pub password_phc: String,
}

#[async_trait]
pub trait Auth {
    /// Create a user bound to `user.subject_id`, storing the Argon2 PHC verifier.
    /// Ensures the ACL subject exists (so the user is immediately a valid ACL
    /// principal; role assignment stays an ACL operation). `Conflict` if the
    /// username is already taken.
    async fn create_user(&self, user: &NewUser) -> Result<()>;

    /// Look up a username's subject + stored password verifier for login.
    /// Unknown username → `Ok(None)` (the caller must not distinguish
    /// "no such user" from "bad password" in its response).
    async fn find_password_credential(
        &self,
        username: &str,
    ) -> Result<Option<PasswordCredential>>;

    /// Persist a session: the SHA-256 of the issued token plus its expiry.
    /// Idempotent on the token hash.
    async fn create_session(
        &self,
        subject: &SubjectId,
        token_sha256: &[u8; 32],
        expires_at: OffsetDateTime,
    ) -> Result<()>;

    /// Resolve a presented token hash to its subject, iff unexpired
    /// (`expires_at > now`). Unknown/expired → `Ok(None)`.
    async fn resolve_session(
        &self,
        token_sha256: &[u8; 32],
        now: OffsetDateTime,
    ) -> Result<Option<SubjectId>>;

    /// Revoke a session (logout). Idempotent (no-op if absent).
    async fn revoke_session(&self, token_sha256: &[u8; 32]) -> Result<()>;

    /// True iff at least one user exists. Drives bootstrap ("seed admin if the
    /// user table is empty").
    async fn has_any_user(&self) -> Result<bool>;
}
```

In `src/control-plane/core/src/lib.rs`, add the module declaration next to `mod acl;` (keep alphabetical):

```rust
mod auth;
```

and add the re-export block near the `pub use acl::{...};` block:

```rust
pub use auth::{Auth, NewUser, PasswordCredential};
```

- [ ] **Step 4: Add the test target to BUCK**

In `src/control-plane/core/BUCK`, after the `error-display` `rust_test`, add:

```python
rust_test(
    name = "auth-types",
    crate = "auth_types",
    srcs = ["tests/auth_types.rs"],
    crate_root = "tests/auth_types.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [":core"],
)
```

(No change to the `:core` library target — its `glob(["src/**/*.rs"])` picks up `auth.rs` automatically.)

- [ ] **Step 5: Run the test to verify it passes**

```bash
buck2 test //src/control-plane/core:auth-types > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/core/src/auth.rs src/control-plane/core/src/lib.rs src/control-plane/core/tests/auth_types.rs src/control-plane/core/BUCK
git commit -m "feat(core): Auth concern trait + NewUser/PasswordCredential types"
```

---

### Task 3: The `testkit` Auth contract

**Files:**
- Modify: `src/control-plane/testkit/src/lib.rs` (add `auth_contract`)

**Interfaces:**
- Consumes: `Auth`, `NewUser`, `PasswordCredential`, `Acl`, `RoleId`, `SubjectId`, `ControlPlaneError` (from `control_plane_core`), `time::OffsetDateTime`.
- Produces: `pub async fn auth_contract<A: Auth + Acl>(a: &A)` — the backend-agnostic suite Tasks 4 and 5 run against the memory fake and the postgres adapter. `a` must be freshly empty.

- [ ] **Step 1: Add the contract function**

In `src/control-plane/testkit/src/lib.rs`, extend the `use control_plane_core::{...}` import to include `Auth, NewUser` (and `PasswordCredential` is not needed — it is returned, not constructed). Then add this function (place it after `acl_contract`):

```rust
/// Contract for the `Auth` ops. `a` must be freshly empty. Bound on `Acl` too so
/// we can prove `create_user` made the subject a real ACL principal.
pub async fn auth_contract<A: Auth + Acl>(a: &A) {
    let sid = |s: &str| SubjectId(s.to_string());
    let h = |b: u8| -> [u8; 32] { [b; 32] };

    // Fresh store: no users.
    assert!(!a.has_any_user().await.unwrap());

    // --- create_user ---
    a.create_user(&NewUser {
        subject_id: sid("u-alice"),
        username: "alice".into(),
        password_phc: "phc-alice".into(),
    })
    .await
    .unwrap();
    assert!(a.has_any_user().await.unwrap());

    // create_user ensured the ACL subject: assigning a role must succeed (it
    // returns NotFound for an unknown subject).
    a.define_role(&RoleId("r".into())).await.unwrap();
    a.assign_role(&sid("u-alice"), &RoleId("r".into()))
        .await
        .unwrap();

    // duplicate username → Conflict
    let dup = a
        .create_user(&NewUser {
            subject_id: sid("u-other"),
            username: "alice".into(),
            password_phc: "phc-other".into(),
        })
        .await;
    assert!(matches!(dup, Err(ControlPlaneError::Conflict(_))));

    // --- find_password_credential ---
    let cred = a.find_password_credential("alice").await.unwrap().unwrap();
    assert_eq!(cred.subject_id, sid("u-alice"));
    assert_eq!(cred.password_phc, "phc-alice");
    assert!(a.find_password_credential("ghost").await.unwrap().is_none());

    // --- sessions ---
    let now = OffsetDateTime::now_utc();
    let future = now + time::Duration::hours(1);
    a.create_session(&sid("u-alice"), &h(1), future).await.unwrap();

    // resolve while unexpired
    assert_eq!(
        a.resolve_session(&h(1), now).await.unwrap(),
        Some(sid("u-alice"))
    );
    // unknown token → None
    assert!(a.resolve_session(&h(9), now).await.unwrap().is_none());
    // expiry boundary: at/after expires_at → None
    assert!(
        a.resolve_session(&h(1), now + time::Duration::hours(2))
            .await
            .unwrap()
            .is_none()
    );

    // revoke is effective and idempotent
    a.revoke_session(&h(1)).await.unwrap();
    assert!(a.resolve_session(&h(1), now).await.unwrap().is_none());
    a.revoke_session(&h(1)).await.unwrap(); // no-op, no error
}
```

- [ ] **Step 2: Verify it builds (no runner yet)**

```bash
buck2 build //src/control-plane/testkit:testkit > /tmp/t.log 2>&1; tail -3 /tmp/t.log
```

Expected: BUILD SUCCEEDED (the contract compiles against the `core` trait; it has no runner until Tasks 4–5).

- [ ] **Step 3: Commit**

```bash
git add src/control-plane/testkit/src/lib.rs
git commit -m "test(testkit): backend-agnostic auth_contract"
```

---

### Task 4: The `memory` Auth fake

**Files:**
- Create: `src/control-plane/memory/src/auth.rs`
- Modify: `src/control-plane/memory/src/lib.rs` (module, struct field, `new`)
- Test: `src/control-plane/memory/tests/auth.rs`
- Modify: `src/control-plane/memory/BUCK` (add the `auth` test target)

**Interfaces:**
- Consumes: `auth_contract` (Task 3), the `Auth` trait (Task 2).
- Produces: `impl Auth for MemoryControlPlane`; a new `auth: Arc<Mutex<AuthState>>` field.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/memory/tests/auth.rs`:

```rust
#[tokio::test]
async fn memory_passes_auth_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::auth_contract(&cp).await;
}
```

Add the target to `src/control-plane/memory/BUCK` (after the `acl` `rust_test`):

```python
rust_test(
    name = "auth",
    crate = "auth",
    srcs = ["tests/auth.rs"],
    crate_root = "tests/auth.rs",
    edition = "2024",
    deps = [
        ":memory",
        "//src/control-plane/testkit:testkit",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run it to verify it fails**

```bash
buck2 test //src/control-plane/memory:auth > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log
```

Expected: failure — `MemoryControlPlane` does not implement `Auth`.

- [ ] **Step 3: Implement the fake**

Create `src/control-plane/memory/src/auth.rs`:

```rust
use std::collections::HashMap;

use async_trait::async_trait;
use control_plane_core::{
    Auth, ControlPlaneError, NewUser, PasswordCredential, Result, SubjectId,
};
use time::OffsetDateTime;

use crate::MemoryControlPlane;

struct MemUser {
    subject_id: String,
    password_phc: String,
}

struct MemSession {
    subject_id: String,
    expires_at: OffsetDateTime,
}

#[derive(Default)]
pub(crate) struct AuthState {
    /// username -> user
    users: HashMap<String, MemUser>,
    /// token sha-256 -> session
    sessions: HashMap<[u8; 32], MemSession>,
}

#[async_trait]
impl Auth for MemoryControlPlane {
    #[tracing::instrument(skip(self, user), level = "debug")]
    async fn create_user(&self, user: &NewUser) -> Result<()> {
        let mut auth = self.auth.lock().unwrap();
        if auth.users.contains_key(&user.username) {
            return Err(ControlPlaneError::Conflict(format!(
                "username {}",
                user.username
            )));
        }
        auth.users.insert(
            user.username.clone(),
            MemUser {
                subject_id: user.subject_id.0.clone(),
                password_phc: user.password_phc.clone(),
            },
        );
        drop(auth);
        // Ensure the ACL subject exists (so the user is a valid ACL principal).
        self.acl.lock().unwrap().subjects_insert(&user.subject_id.0);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn find_password_credential(
        &self,
        username: &str,
    ) -> Result<Option<PasswordCredential>> {
        let auth = self.auth.lock().unwrap();
        Ok(auth.users.get(username).map(|u| PasswordCredential {
            subject_id: SubjectId(u.subject_id.clone()),
            password_phc: u.password_phc.clone(),
        }))
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn create_session(
        &self,
        subject: &SubjectId,
        token_sha256: &[u8; 32],
        expires_at: OffsetDateTime,
    ) -> Result<()> {
        self.auth.lock().unwrap().sessions.insert(
            *token_sha256,
            MemSession {
                subject_id: subject.0.clone(),
                expires_at,
            },
        );
        Ok(())
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn resolve_session(
        &self,
        token_sha256: &[u8; 32],
        now: OffsetDateTime,
    ) -> Result<Option<SubjectId>> {
        let auth = self.auth.lock().unwrap();
        Ok(auth.sessions.get(token_sha256).and_then(|s| {
            (s.expires_at > now).then(|| SubjectId(s.subject_id.clone()))
        }))
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn revoke_session(&self, token_sha256: &[u8; 32]) -> Result<()> {
        self.auth.lock().unwrap().sessions.remove(token_sha256);
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn has_any_user(&self) -> Result<bool> {
        Ok(!self.auth.lock().unwrap().users.is_empty())
    }
}
```

The fake needs to insert into the ACL subject set. In `src/control-plane/memory/src/acl.rs`, add a small helper on `AclState` (next to its struct definition) so `auth.rs` does not need to touch private fields directly:

```rust
impl AclState {
    /// Insert a subject id (idempotent). Used by `create_user` to make a new
    /// user a valid ACL principal.
    pub(crate) fn subjects_insert(&mut self, id: &str) {
        self.subjects.insert(id.to_string());
    }
}
```

In `src/control-plane/memory/src/lib.rs`:
- add `mod auth;` next to `mod acl;`
- add `use crate::auth::AuthState;` next to `use crate::acl::AclState;`
- add the field to the `MemoryControlPlane` struct, after `acl: Arc<Mutex<AclState>>,`:

```rust
    auth: Arc<Mutex<AuthState>>,
```

- initialize it in `new()`, after the `acl:` line:

```rust
            auth: Arc::new(Mutex::new(AuthState::default())),
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
buck2 test //src/control-plane/memory:auth > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/memory/src/auth.rs src/control-plane/memory/src/lib.rs src/control-plane/memory/src/acl.rs src/control-plane/memory/tests/auth.rs src/control-plane/memory/BUCK
git commit -m "feat(memory): in-memory Auth fake passing auth_contract"
```

---

### Task 5: The `postgres` Auth adapter, schema migration, and `.sqlx` cache

**Files:**
- Create: `src/control-plane/postgres/migrations/0017_auth.sql`
- Create: `src/control-plane/postgres/src/auth.rs`
- Modify: `src/control-plane/postgres/src/lib.rs` (declare `mod auth;`)
- Create: `src/control-plane/postgres/tests/auth.rs`
- Modify: `src/control-plane/postgres/BUCK` (add the `auth` fixture test)
- Modify (generated): `src/control-plane/postgres/.sqlx/`

**Interfaces:**
- Consumes: `auth_contract` (Task 3), the `Auth` trait (Task 2), `PgControlPlane` (existing).
- Produces: `impl Auth for PgControlPlane` over the `auth` schema.

- [ ] **Step 1: Write the migration**

Create `src/control-plane/postgres/migrations/0017_auth.sql`:

```sql
-- The auth concern: credential + session store. A loom *user* is an ACL subject
-- (acl.subject) that has credentials; auth and acl join by subject_id. The
-- session primary key is the token HASH (sha-256), never the raw token.
create schema if not exists auth;

create table auth.user (
    subject_id text primary key,
    username   text unique not null,
    created_at timestamptz not null default now()
);

create table auth.password_credential (
    subject_id   text primary key references auth.user (subject_id) on delete cascade,
    password_phc text not null, -- Argon2 PHC string, computed service-side
    updated_at   timestamptz not null default now()
);

create table auth.session (
    token_sha256 bytea primary key,
    subject_id   text not null references auth.user (subject_id) on delete cascade,
    expires_at   timestamptz not null,
    created_at   timestamptz not null default now()
);
```

- [ ] **Step 2: Write the failing test**

Create `src/control-plane/postgres/tests/auth.rs`:

```rust
use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_auth_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::auth_contract(&cp).await;
}
```

Add the target to `src/control-plane/postgres/BUCK` (after the `acl` `loom_fixture_test`):

```python
loom_fixture_test(
    name = "auth",
    crate = "auth",
    srcs = ["tests/auth.rs"],
    crate_root = "tests/auth.rs",
    deps = [":postgres", "//src/control-plane/testkit:testkit", "//third-party:tokio"],
)
```

- [ ] **Step 3: Run it to verify it fails**

```bash
buck2 test //src/control-plane/postgres:auth > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log
```

Expected: failure — `PgControlPlane` does not implement `Auth`.

- [ ] **Step 4: Implement the adapter**

Create `src/control-plane/postgres/src/auth.rs`:

```rust
use async_trait::async_trait;
use control_plane_core::{
    Auth, ControlPlaneError, NewUser, PasswordCredential, Result, SubjectId,
};
use time::OffsetDateTime;

use crate::{backend, PgControlPlane};

/// Map a unique-violation (SQLSTATE 23505) to `Conflict`, anything else to `Backend`.
fn conflict_or_backend(e: sqlx::Error, what: &str) -> ControlPlaneError {
    if let sqlx::Error::Database(db) = &e
        && db.code().as_deref() == Some("23505")
    {
        return ControlPlaneError::Conflict(what.to_string());
    }
    ControlPlaneError::Backend(Box::new(e))
}

#[async_trait]
impl Auth for PgControlPlane {
    #[tracing::instrument(skip(self, user), level = "debug")]
    async fn create_user(&self, user: &NewUser) -> Result<()> {
        // One transaction: ensure the ACL subject, then the user + credential.
        // A duplicate username aborts on the auth.user insert (23505 -> Conflict).
        let mut tx = self.pool().begin().await.map_err(backend)?;
        sqlx::query!(
            "insert into acl.subject (id) values ($1) on conflict (id) do nothing",
            &user.subject_id.0,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        sqlx::query!(
            "insert into auth.user (subject_id, username) values ($1, $2)",
            &user.subject_id.0,
            &user.username,
        )
        .execute(&mut *tx)
        .await
        .map_err(|e| conflict_or_backend(e, &format!("username {}", user.username)))?;
        sqlx::query!(
            "insert into auth.password_credential (subject_id, password_phc) values ($1, $2)",
            &user.subject_id.0,
            &user.password_phc,
        )
        .execute(&mut *tx)
        .await
        .map_err(backend)?;
        tx.commit().await.map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn find_password_credential(
        &self,
        username: &str,
    ) -> Result<Option<PasswordCredential>> {
        let row = sqlx::query!(
            "select u.subject_id, pc.password_phc \
             from auth.user u \
             join auth.password_credential pc on pc.subject_id = u.subject_id \
             where u.username = $1",
            username,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?;
        Ok(row.map(|r| PasswordCredential {
            subject_id: SubjectId(r.subject_id),
            password_phc: r.password_phc,
        }))
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn create_session(
        &self,
        subject: &SubjectId,
        token_sha256: &[u8; 32],
        expires_at: OffsetDateTime,
    ) -> Result<()> {
        sqlx::query!(
            "insert into auth.session (token_sha256, subject_id, expires_at) \
             values ($1, $2, $3) \
             on conflict (token_sha256) do update set \
                 subject_id = excluded.subject_id, expires_at = excluded.expires_at",
            &token_sha256[..],
            &subject.0,
            expires_at,
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn resolve_session(
        &self,
        token_sha256: &[u8; 32],
        now: OffsetDateTime,
    ) -> Result<Option<SubjectId>> {
        let row = sqlx::query_scalar!(
            "select subject_id from auth.session \
             where token_sha256 = $1 and expires_at > $2",
            &token_sha256[..],
            now,
        )
        .fetch_optional(self.pool())
        .await
        .map_err(backend)?;
        Ok(row.map(SubjectId))
    }

    #[tracing::instrument(skip(self, token_sha256), level = "debug")]
    async fn revoke_session(&self, token_sha256: &[u8; 32]) -> Result<()> {
        sqlx::query!(
            "delete from auth.session where token_sha256 = $1",
            &token_sha256[..],
        )
        .execute(self.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn has_any_user(&self) -> Result<bool> {
        let exists = sqlx::query_scalar!("select exists (select 1 from auth.user)")
            .fetch_one(self.pool())
            .await
            .map_err(backend)?
            .unwrap_or(false);
        Ok(exists)
    }
}
```

In `src/control-plane/postgres/src/lib.rs`, add `mod auth;` next to `mod acl;`. (`backend` is a private `fn` in `lib.rs`; `auth.rs` imports it via `use crate::{backend, PgControlPlane};`. `pool()` is already a public accessor.)

- [ ] **Step 5: Generate the `.sqlx` cache**

```bash
./tools/sqlx-prepare.sh > /tmp/t.log 2>&1; tail -5 /tmp/t.log
git status --short src/control-plane/postgres/.sqlx
```

Expected: new `query-*.json` files appear under `.sqlx/` for the six auth queries.

- [ ] **Step 6: Run the contract + cache-freshness tests**

```bash
buck2 test //src/control-plane/postgres:auth //src/control-plane/postgres:sqlx-cache-check > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: both PASS.

- [ ] **Step 7: Commit**

```bash
git add src/control-plane/postgres/migrations/0017_auth.sql src/control-plane/postgres/src/auth.rs src/control-plane/postgres/src/lib.rs src/control-plane/postgres/tests/auth.rs src/control-plane/postgres/BUCK src/control-plane/postgres/.sqlx
git commit -m "feat(postgres): Auth adapter over the auth schema + .sqlx cache"
```

---

### Task 6: Service-side crypto helpers

**Files:**
- Create: `src/services/runtime/src/crypto.rs`
- Modify: `src/services/runtime/src/lib.rs` (declare module + re-export)
- Modify: `src/services/runtime/BUCK` (add `argon2`/`sha2`/`hex` deps; add the `crypto` test target)
- Test: `src/services/runtime/tests/crypto.rs`

**Interfaces:**
- Produces (all `pub`, re-exported from the crate root):
  - `fn hash_password(password: &str) -> Result<String, AuthError>` — Argon2id PHC string.
  - `fn verify_password(password: &str, phc: &str) -> bool` — constant-time; `false` on any parse/verify failure.
  - `fn generate_session_token() -> String` — 256-bit CSPRNG token, hex (64 chars).
  - `fn token_sha256(token: &str) -> [u8; 32]`.
  - `enum AuthError { Hash(String) }` (`thiserror`).

- [ ] **Step 1: Write the failing test**

Create `src/services/runtime/tests/crypto.rs`:

```rust
use service_runtime::{generate_session_token, hash_password, token_sha256, verify_password};

#[test]
fn argon2_round_trip_and_reject() {
    let phc = hash_password("correct horse").unwrap();
    assert!(phc.starts_with("$argon2"));
    assert!(verify_password("correct horse", &phc));
    assert!(!verify_password("wrong", &phc));
    assert!(!verify_password("correct horse", "not-a-phc-string"));
}

#[test]
fn distinct_salts_per_hash() {
    // Same password hashed twice → different PHC (random salt), both verify.
    let a = hash_password("pw").unwrap();
    let b = hash_password("pw").unwrap();
    assert_ne!(a, b);
    assert!(verify_password("pw", &a));
    assert!(verify_password("pw", &b));
}

#[test]
fn token_is_high_entropy_and_hash_is_stable() {
    let t1 = generate_session_token();
    let t2 = generate_session_token();
    assert_eq!(t1.len(), 64); // 32 bytes hex
    assert_ne!(t1, t2);
    // hashing is deterministic for a given token, differs across tokens
    assert_eq!(token_sha256(&t1), token_sha256(&t1));
    assert_ne!(token_sha256(&t1), token_sha256(&t2));
}
```

- [ ] **Step 2: Run it to verify it fails**

```bash
buck2 test //src/services/runtime:crypto > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|Unknown target" /tmp/t.log
```

Expected: failure — target/functions do not exist.

- [ ] **Step 3: Implement the crypto module**

Create `src/services/runtime/src/crypto.rs`:

```rust
//! Service-side cryptography for the auth slice. The control-plane `Auth` trait
//! is a pure store; all hashing/token minting lives here.
//!
//! - Passwords: Argon2id, stored as a PHC verifier string.
//! - Session tokens: a 256-bit CSPRNG secret returned to the caller once; only
//!   its SHA-256 is persisted. Tokens are high-entropy, so a fast hash is correct
//!   (unlike passwords, which need the slow KDF).

use argon2::password_hash::rand_core::{OsRng, RngCore};
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use sha2::{Digest, Sha256};

/// A crypto failure (e.g. hashing). Verification never errors — it returns `false`.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("password hashing failed: {0}")]
    Hash(String),
}

/// Hash a plaintext password to an Argon2id PHC string (random salt).
pub fn hash_password(password: &str) -> Result<String, AuthError> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| AuthError::Hash(e.to_string()))
}

/// Verify a plaintext password against a stored PHC string. `false` on any
/// parse or verification failure (never panics, never errors).
pub fn verify_password(password: &str, phc: &str) -> bool {
    match PasswordHash::new(phc) {
        Ok(parsed) => Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok(),
        Err(_) => false,
    }
}

/// Mint a fresh 256-bit session token, hex-encoded (64 chars). Returned to the
/// caller once; store only `token_sha256(&token)`.
pub fn generate_session_token() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// SHA-256 of the raw token string — the value persisted as the session key.
pub fn token_sha256(token: &str) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(token.as_bytes());
    h.finalize().into()
}
```

In `src/services/runtime/src/lib.rs`, add near the top (with the other module declarations, or create one if none exist):

```rust
mod crypto;
pub use crypto::{generate_session_token, hash_password, token_sha256, verify_password, AuthError};
```

In `src/services/runtime/BUCK`, add to the `runtime` `rust_library` `deps` (keep sorted): `"//third-party:argon2"`, `"//third-party:hex"`, `"//third-party:sha2"`. Then add the test target after the library:

```python
rust_test(
    name = "crypto",
    crate = "crypto",
    srcs = ["tests/crypto.rs"],
    crate_root = "tests/crypto.rs",
    edition = "2024",
    deps = [":runtime"],
)
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
buck2 test //src/services/runtime:crypto > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/services/runtime/src/crypto.rs src/services/runtime/src/lib.rs src/services/runtime/BUCK src/services/runtime/tests/crypto.rs
git commit -m "feat(runtime): Argon2 password + CSPRNG session-token crypto helpers"
```

---

### Task 7: The `Subject` extractor and `require_auth` middleware

**Files:**
- Create: `src/services/runtime/src/auth.rs`
- Modify: `src/services/runtime/src/lib.rs` (declare module + re-export)
- Modify: `src/services/runtime/BUCK` (add `core`-facing deps already present; add `time`, `async-trait`, `tower`, `http-body-util` test deps; add the `auth_middleware` test)
- Test: `src/services/runtime/tests/auth_middleware.rs`

**Interfaces:**
- Consumes: `Auth` (core), `SubjectId`, `ControlPlaneError`, the crypto helpers (Task 6).
- Produces (all `pub`, re-exported):
  - `struct Subject(pub SubjectId)` (derives `Clone`) + `impl FromRequestParts<S> for Subject` (rejection: 401).
  - `struct AuthState { auth: Arc<dyn Auth + Send + Sync>, session_ttl: Duration }` (derives `Clone`).
  - `async fn require_auth(State<AuthState>, Request, Next) -> Response` — the gate.
  - `fn protect(router: Router, auth: AuthState) -> Router` — applies the gate as a `route_layer`.
  - `fn status_for(e: &ControlPlaneError) -> StatusCode` — maps `Unauthorized → 401`, `NotFound → 404`, `Conflict → 409`, `Validation → 400`, else `500`.

- [ ] **Step 1: Write the failing test**

Create `src/services/runtime/tests/auth_middleware.rs`:

```rust
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{header::AUTHORIZATION, StatusCode};
use axum::routing::get;
use axum::Router;
use control_plane_core::{Auth, SubjectId};
use control_plane_memory::MemoryControlPlane;
use service_runtime::{protect, token_sha256, AuthState, Subject};
use time::OffsetDateTime;
use tower::ServiceExt;

async fn whoami(subject: Subject) -> String {
    subject.0 .0
}

fn app(cp: Arc<MemoryControlPlane>) -> Router {
    protect(
        Router::new().route("/whoami", get(whoami)),
        AuthState {
            auth: cp,
            session_ttl: Duration::from_secs(3600),
        },
    )
}

async fn bearer(app: Router, token: Option<&str>) -> StatusCode {
    let mut req = Request::builder().uri("/whoami");
    if let Some(t) = token {
        req = req.header(AUTHORIZATION, format!("Bearer {t}"));
    }
    app.oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn valid_token_authenticates() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = "deadbeef";
    cp.create_session(
        &SubjectId("alice".into()),
        &token_sha256(token),
        OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await
    .unwrap();
    assert_eq!(bearer(app(cp), Some(token)).await, StatusCode::OK);
}

#[tokio::test]
async fn missing_token_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    assert_eq!(bearer(app(cp), None).await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn unknown_token_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    assert_eq!(bearer(app(cp), Some("nope")).await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn expired_token_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let token = "stale";
    cp.create_session(
        &SubjectId("alice".into()),
        &token_sha256(token),
        OffsetDateTime::now_utc() - time::Duration::hours(1),
    )
    .await
    .unwrap();
    assert_eq!(bearer(app(cp), Some(token)).await, StatusCode::UNAUTHORIZED);
}
```

Add the test target to `src/services/runtime/BUCK` after the `crypto` test:

```python
rust_test(
    name = "auth-middleware",
    crate = "auth_middleware",
    srcs = ["tests/auth_middleware.rs"],
    crate_root = "tests/auth_middleware.rs",
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

- [ ] **Step 2: Run it to verify it fails**

```bash
buck2 test //src/services/runtime:auth-middleware > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|Unknown target" /tmp/t.log
```

Expected: failure — `Subject`/`AuthState`/`protect` do not exist.

- [ ] **Step 3: Implement the extractor + middleware**

Create `src/services/runtime/src/auth.rs`:

```rust
//! The authentication seam: a `Subject` extractor, the `require_auth` middleware
//! that resolves a bearer session token to a verified `SubjectId`, and the
//! `protect` combinator both binaries apply to their routers. This is the first
//! and only producer of `ControlPlaneError::Unauthorized` (→ HTTP 401).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::extract::{FromRequestParts, Request, State};
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Router;
use control_plane_core::{Auth, ControlPlaneError, SubjectId};
use time::OffsetDateTime;

use crate::token_sha256;

/// A verified request principal, injected into request extensions by
/// [`require_auth`] and handed to handlers by the [`Subject`] extractor.
#[derive(Clone, Debug)]
pub struct Subject(pub SubjectId);

/// Shared state for the auth routes + middleware.
#[derive(Clone)]
pub struct AuthState {
    pub auth: Arc<dyn Auth + Send + Sync>,
    pub session_ttl: Duration,
}

/// Map a control-plane error to an HTTP status. Centralised so the reserved
/// `Unauthorized` variant has exactly one producer path.
pub fn status_for(e: &ControlPlaneError) -> StatusCode {
    match e {
        ControlPlaneError::Unauthorized => StatusCode::UNAUTHORIZED,
        ControlPlaneError::NotFound(_) => StatusCode::NOT_FOUND,
        ControlPlaneError::Conflict(_) => StatusCode::CONFLICT,
        ControlPlaneError::Validation(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn unauthorized() -> Response {
    let e = ControlPlaneError::Unauthorized;
    (status_for(&e), e.to_string()).into_response()
}

/// Extract the bearer token from the `Authorization` header, if present.
fn bearer_token(parts_headers: &axum::http::HeaderMap) -> Option<String> {
    parts_headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.to_string())
}

#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for Subject {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Subject>()
            .cloned()
            .ok_or_else(unauthorized)
    }
}

/// Resolve the bearer session token to a verified `Subject`, inject it into
/// request extensions, and run the handler. Absent/invalid/expired → 401.
pub async fn require_auth(
    State(st): State<AuthState>,
    mut req: Request,
    next: Next,
) -> Response {
    let Some(token) = bearer_token(req.headers()) else {
        return unauthorized();
    };
    let hash = token_sha256(&token);
    match st.auth.resolve_session(&hash, OffsetDateTime::now_utc()).await {
        Ok(Some(sid)) => {
            req.extensions_mut().insert(Subject(sid));
            next.run(req).await
        }
        Ok(None) => unauthorized(),
        Err(e) => status_for(&e).into_response(),
    }
}

/// Apply the authn gate to every route in `router`.
pub fn protect(router: Router, auth: AuthState) -> Router {
    router.route_layer(axum::middleware::from_fn_with_state(auth, require_auth))
}
```

In `src/services/runtime/src/lib.rs`, add:

```rust
mod auth;
pub use auth::{protect, require_auth, status_for, AuthState, Subject};
```

In `src/services/runtime/BUCK`, add to the `runtime` `rust_library` `deps` (sorted): `"//third-party:async-trait"`, `"//third-party:time"`. (`axum`, `core`, `tokio`, `thiserror` are already present.)

- [ ] **Step 4: Run the test to verify it passes**

```bash
buck2 test //src/services/runtime:auth-middleware > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/services/runtime/src/auth.rs src/services/runtime/src/lib.rs src/services/runtime/BUCK src/services/runtime/tests/auth_middleware.rs
git commit -m "feat(runtime): Subject extractor + require_auth middleware (401 producer)"
```

---

### Task 8: The `/auth/login` and `/auth/logout` routes

**Files:**
- Modify: `src/services/runtime/src/auth.rs` (add handlers + route builders)
- Modify: `src/services/runtime/src/lib.rs` (re-export `login_routes`, `session_routes`)
- Modify: `src/services/runtime/BUCK` (add `serde`/`serde_json` deps; add the `auth_routes` test)
- Test: `src/services/runtime/tests/auth_routes.rs`

**Interfaces:**
- Consumes: `AuthState`, `protect`, the crypto helpers, the `Auth` store.
- Produces (re-exported):
  - `fn login_routes(auth: AuthState) -> Router` — un-gated `POST /auth/login`.
  - `fn session_routes(auth: AuthState) -> Router` — gated `POST /auth/logout`.
  - On success, login returns `200 {"token": "<hex>"}`; failure (unknown user or bad password) returns a uniform `401` that does not reveal whether the username exists.

- [ ] **Step 1: Write the failing test**

Create `src/services/runtime/tests/auth_routes.rs`:

```rust
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{header::AUTHORIZATION, StatusCode};
use axum::Router;
use control_plane_core::{Auth, NewUser, SubjectId};
use control_plane_memory::MemoryControlPlane;
use http_body_util::BodyExt;
use service_runtime::{
    hash_password, login_routes, session_routes, token_sha256, AuthState,
};
use tower::ServiceExt;

fn state(cp: Arc<MemoryControlPlane>) -> AuthState {
    AuthState { auth: cp, session_ttl: Duration::from_secs(3600) }
}

async fn seed_user(cp: &MemoryControlPlane, username: &str, password: &str) {
    cp.create_user(&NewUser {
        subject_id: SubjectId(username.into()),
        username: username.into(),
        password_phc: hash_password(password).unwrap(),
    })
    .await
    .unwrap();
}

async fn post_login(app: Router, body: &str) -> (StatusCode, String) {
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/login")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test]
async fn login_success_returns_token() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_user(&cp, "alice", "pw123").await;
    let (status, body) = post_login(
        login_routes(state(cp.clone())),
        r#"{"username":"alice","password":"pw123"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let token = v["token"].as_str().unwrap();
    // the minted token resolves to alice
    assert_eq!(
        cp.resolve_session(&token_sha256(token), time::OffsetDateTime::now_utc())
            .await
            .unwrap(),
        Some(SubjectId("alice".into()))
    );
}

#[tokio::test]
async fn login_bad_password_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_user(&cp, "alice", "pw123").await;
    let (status, _) = post_login(
        login_routes(state(cp)),
        r#"{"username":"alice","password":"wrong"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn login_unknown_user_is_401() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    let (status, _) = post_login(
        login_routes(state(cp)),
        r#"{"username":"ghost","password":"x"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn logout_revokes_session() {
    let cp = Arc::new(MemoryControlPlane::new(Duration::from_millis(300)));
    seed_user(&cp, "alice", "pw").await;
    let token = "tok-logout";
    cp.create_session(
        &SubjectId("alice".into()),
        &token_sha256(token),
        time::OffsetDateTime::now_utc() + time::Duration::hours(1),
    )
    .await
    .unwrap();

    let res = session_routes(state(cp.clone()))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/logout")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert!(cp
        .resolve_session(&token_sha256(token), time::OffsetDateTime::now_utc())
        .await
        .unwrap()
        .is_none());
}
```

Add to `src/services/runtime/BUCK` after the `auth-middleware` test:

```python
rust_test(
    name = "auth-routes",
    crate = "auth_routes",
    srcs = ["tests/auth_routes.rs"],
    crate_root = "tests/auth_routes.rs",
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

- [ ] **Step 2: Run it to verify it fails**

```bash
buck2 test //src/services/runtime:auth-routes > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|Unknown target" /tmp/t.log
```

Expected: failure — `login_routes`/`session_routes` do not exist.

- [ ] **Step 3: Implement the routes**

Append to `src/services/runtime/src/auth.rs` (add the needed imports to the existing `use` block at the top: `axum::extract::Json`, `axum::routing::post`, `axum::Json as RespJson` is the same `Json` — use `axum::Json`; `serde::{Deserialize, Serialize}`; and the crypto helpers `crate::{generate_session_token, hash_password, verify_password}`):

```rust
#[derive(serde::Deserialize)]
struct LoginReq {
    username: String,
    password: String,
}

#[derive(serde::Serialize)]
struct LoginResp {
    token: String,
}

/// `POST /auth/login` — public. Verify the password, mint a session token,
/// return it once. A uniform 401 on unknown user OR bad password (and a
/// dummy hash on unknown user keeps login timing uniform).
async fn login(
    State(st): State<AuthState>,
    axum::Json(req): axum::Json<LoginReq>,
) -> Response {
    match st.auth.find_password_credential(&req.username).await {
        Ok(Some(cred)) => {
            if crate::verify_password(&req.password, &cred.password_phc) {
                let token = crate::generate_session_token();
                let hash = token_sha256(&token);
                let expires = OffsetDateTime::now_utc()
                    + time::Duration::try_from(st.session_ttl)
                        .expect("session_ttl fits in time::Duration");
                match st.auth.create_session(&cred.subject_id, &hash, expires).await {
                    Ok(()) => (StatusCode::OK, axum::Json(LoginResp { token })).into_response(),
                    Err(e) => status_for(&e).into_response(),
                }
            } else {
                unauthorized()
            }
        }
        // Unknown user: burn comparable time, then the same 401 as a bad password.
        Ok(None) => {
            let _ = crate::hash_password(&req.password);
            unauthorized()
        }
        Err(e) => status_for(&e).into_response(),
    }
}

/// `POST /auth/logout` — authenticated. Revoke the presented session
/// (idempotent). The `Subject` extractor enforces that the caller is verified;
/// the token to revoke is re-read from the bearer header.
async fn logout(_subject: Subject, State(st): State<AuthState>, req: Request) -> Response {
    if let Some(token) = bearer_token(req.headers()) {
        let hash = token_sha256(&token);
        if let Err(e) = st.auth.revoke_session(&hash).await {
            return status_for(&e).into_response();
        }
    }
    StatusCode::OK.into_response()
}

/// Public auth routes (login). Mount un-gated.
pub fn login_routes(auth: AuthState) -> Router {
    Router::new()
        .route("/auth/login", axum::routing::post(login))
        .with_state(auth)
}

/// Authenticated session routes (logout), behind the authn gate.
pub fn session_routes(auth: AuthState) -> Router {
    protect(
        Router::new()
            .route("/auth/logout", axum::routing::post(logout))
            .with_state(auth.clone()),
        auth,
    )
}
```

> **Note for the implementer:** `logout` takes both the `Subject` extractor (a `FromRequestParts`) and `req: Request` (a `FromRequest`, consumes the body). In axum a handler may have at most one body-consuming extractor and it must be last; `Subject` is parts-only so this ordering is valid. If the compiler objects, replace `req: Request` with `headers: axum::http::HeaderMap` (also parts-only) and read the bearer from `&headers`.

In `src/services/runtime/src/lib.rs`, extend the auth re-export:

```rust
pub use auth::{login_routes, protect, require_auth, session_routes, status_for, AuthState, Subject};
```

In `src/services/runtime/BUCK`, add to the `runtime` library `deps` (sorted): `"//third-party:serde"`, `"//third-party:serde_json"`.

- [ ] **Step 4: Run the test to verify it passes**

```bash
buck2 test //src/services/runtime:auth-routes > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/services/runtime/src/auth.rs src/services/runtime/src/lib.rs src/services/runtime/BUCK src/services/runtime/tests/auth_routes.rs
git commit -m "feat(runtime): /auth/login + /auth/logout routes"
```

---

### Task 9: Bootstrap-admin seeding

**Files:**
- Modify: `src/services/runtime/src/auth.rs` (add `bootstrap_admin`)
- Modify: `src/services/runtime/src/lib.rs` (re-export)
- Test: `src/services/runtime/tests/bootstrap.rs`
- Modify: `src/services/runtime/BUCK` (add the `bootstrap` test)

**Interfaces:**
- Consumes: the `Auth` store, `hash_password`, `NewUser`, `SubjectId`.
- Produces: `async fn bootstrap_admin<A: Auth>(auth: &A, username: &str, password: &str) -> Result<(), BootstrapError>` — seeds the user iff `!auth.has_any_user()`. The admin's `subject_id` equals its `username`. `enum BootstrapError { Hash(AuthError), Store(ControlPlaneError) }`.

- [ ] **Step 1: Write the failing test**

Create `src/services/runtime/tests/bootstrap.rs`:

```rust
use std::time::Duration;

use control_plane_core::Auth;
use control_plane_memory::MemoryControlPlane;
use service_runtime::{bootstrap_admin, hash_password};

#[tokio::test]
async fn seeds_admin_into_empty_store() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    assert!(!cp.has_any_user().await.unwrap());
    bootstrap_admin(&cp, "root", "s3cret").await.unwrap();
    let cred = cp.find_password_credential("root").await.unwrap().unwrap();
    assert_eq!(cred.subject_id.0, "root");
}

#[tokio::test]
async fn is_noop_when_users_exist() {
    let cp = MemoryControlPlane::new(Duration::from_millis(300));
    cp.create_user(&control_plane_core::NewUser {
        subject_id: control_plane_core::SubjectId("existing".into()),
        username: "existing".into(),
        password_phc: hash_password("x").unwrap(),
    })
    .await
    .unwrap();
    // Should NOT create "root" because the store is non-empty.
    bootstrap_admin(&cp, "root", "s3cret").await.unwrap();
    assert!(cp.find_password_credential("root").await.unwrap().is_none());
}
```

Add to `src/services/runtime/BUCK`:

```python
rust_test(
    name = "bootstrap",
    crate = "bootstrap",
    srcs = ["tests/bootstrap.rs"],
    crate_root = "tests/bootstrap.rs",
    edition = "2024",
    deps = [
        ":runtime",
        "//src/control-plane/core:core",
        "//src/control-plane/memory:memory",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run it to verify it fails**

```bash
buck2 test //src/services/runtime:bootstrap > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|Unknown target" /tmp/t.log
```

Expected: failure — `bootstrap_admin` does not exist.

- [ ] **Step 3: Implement bootstrap**

Append to `src/services/runtime/src/auth.rs` (add `use control_plane_core::NewUser;` to the imports; `AuthError` is `crate::AuthError`):

```rust
/// Failure seeding the bootstrap admin.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error(transparent)]
    Hash(#[from] crate::AuthError),
    #[error(transparent)]
    Store(#[from] ControlPlaneError),
}

/// Seed `username`/`password` as the first user iff the store has no users yet.
/// The admin's subject id equals its username (role assignment stays an operator
/// ACL task). A non-empty store is a no-op (so a restart never re-seeds).
pub async fn bootstrap_admin<A: Auth>(
    auth: &A,
    username: &str,
    password: &str,
) -> Result<(), BootstrapError> {
    if auth.has_any_user().await? {
        return Ok(());
    }
    let password_phc = crate::hash_password(password)?;
    auth.create_user(&NewUser {
        subject_id: SubjectId(username.to_string()),
        username: username.to_string(),
        password_phc,
    })
    .await?;
    Ok(())
}
```

In `src/services/runtime/src/lib.rs`, extend the re-export:

```rust
pub use auth::{
    bootstrap_admin, login_routes, protect, require_auth, session_routes, status_for, AuthState,
    BootstrapError, Subject,
};
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
buck2 test //src/services/runtime:bootstrap > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/services/runtime/src/auth.rs src/services/runtime/src/lib.rs src/services/runtime/BUCK src/services/runtime/tests/bootstrap.rs
git commit -m "feat(runtime): bootstrap_admin seeds the first user from config"
```

---

### Task 10: Query-API — replace `X-Loom-Subject` with the verified `Subject` extractor

**Files:**
- Modify: `src/services/query-api/src/handler.rs` (remove the local `Subject`, import `service_runtime::Subject`)
- Modify: `src/services/query-api/src/http.rs` (drop all six `X-Loom-Subject` parses; use the extractor)
- Modify: `src/services/query-api/BUCK` (add `//src/services/runtime:runtime` to the `query-api` lib deps)

**Interfaces:**
- Consumes: `service_runtime::Subject` (Task 7).
- Produces: handlers that receive a verified `Subject` extractor instead of a header string. The `router(state: AppState)` signature is UNCHANGED (auth is layered by the composition in Tasks 11–12), so existing callers keep compiling.

- [ ] **Step 1: Point the handler module at the shared `Subject`**

In `src/services/query-api/src/handler.rs`, DELETE the local definition (line ~26):

```rust
pub struct Subject(pub SubjectId);
```

and add an import (near the other `control_plane_core` / use lines):

```rust
use service_runtime::Subject;
```

Leave every `subject: &Subject` parameter and `subject.0` access exactly as-is — they now refer to `service_runtime::Subject`, which has the identical `Subject(pub SubjectId)` shape.

- [ ] **Step 2: Replace the six header parses in `http.rs` with the extractor**

In `src/services/query-api/src/http.rs`, for EACH of the six handlers (`get_object`, `get_linked`, `get_linked_chain`, `get_graph`, `get_graph_path`, `post_action`):

1. Add `subject: Subject` to the handler's argument list (import `use service_runtime::Subject;` at the top of the file). A parts-only extractor, place it before any body extractor (`Bytes`/`Json`) — e.g. `post_action` becomes `async fn post_action(State(st): State<AppState>, Path(...): Path<...>, subject: Subject, body: Bytes) -> Response`.
2. DELETE the header-parse block:

```rust
    let subject = headers
        .get("X-Loom-Subject")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();
```

3. Where the handler previously built `Subject(SubjectId(subject))` (read paths) — use the extracted `subject` directly (it already IS a `Subject`). Where it passed a raw `subject: String` to a helper (`graph_respond`, `graph_union_respond`, `graph_tail_respond`, `post_action`'s `SubjectId(subject)`) — change those helpers/call-sites to take/pass `&Subject` (or `subject.0` for the `SubjectId`), removing the now-dead string plumbing.
4. If `headers: HeaderMap` is now unused in a handler, remove the parameter. If it is still used for another header, keep it.

Mechanically: after editing, there must be ZERO occurrences of `X-Loom-Subject` and ZERO `"anonymous"` defaults in `src/services/query-api/`:

```bash
grep -rn "X-Loom-Subject\|anonymous" src/services/query-api/src
```

Expected: no matches.

- [ ] **Step 3: Add the runtime dep**

In `src/services/query-api/BUCK`, add `"//src/services/runtime:runtime"` to the `query-api` `rust_library` `deps` (sorted). (No cycle: `runtime` does not depend on `query-api`.)

- [ ] **Step 4: Verify the crate builds and clippy is clean**

```bash
buck2 build //src/services/query-api:query-api '//src/services/query-api:query-api[clippy.txt]' > /tmp/t.log 2>&1; tail -5 /tmp/t.log
```

Expected: BUILD SUCCEEDED, empty clippy output. (E2E tests are updated in Task 11 — the `e2e-support` lib and the e2e test targets will not compile until then; that is expected at this step.)

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/src/handler.rs src/services/query-api/src/http.rs src/services/query-api/BUCK
git commit -m "refactor(query-api): verified Subject extractor replaces X-Loom-Subject header"
```

---

### Task 11: E2E support — present a real session token; add the authenticated read e2e

**Files:**
- Modify: `src/services/query-api/tests/e2e_support.rs` (compose the auth gate; swap header injection for a minted session token; add `session_token`)
- Modify: `src/services/query-api/BUCK` (the `e2e-support` lib already deps `runtime`/`time`; add an `auth` e2e test target)
- Create: `src/services/query-api/tests/auth_e2e.rs`

**Interfaces:**
- Consumes: `service_runtime::{protect, AuthState, generate_session_token, token_sha256, hash_password, login_routes}`, `control_plane_core::{Auth, NewUser}`.
- Produces: an updated `get(...)` (same signature) that authenticates internally, and `pub async fn session_token(cp: &PgControlPlane, subject: &str) -> String`. Every existing graph/object-set e2e keeps calling `get(cp, eng, uri, "alice")` unchanged.

- [ ] **Step 1: Add the `session_token` helper and rewrite `get`**

In `src/services/query-api/tests/e2e_support.rs`, add imports:

```rust
use control_plane_core::{Auth, ControlPlaneError, NewUser};
use service_runtime::{protect, token_sha256, AuthState};
use time::OffsetDateTime;
```

Add the helper:

```rust
/// Ensure `subject` has an auth.user (the session FK target) and mint a live
/// session token for it. Lets the existing e2e tests authenticate without
/// driving the password flow.
pub async fn session_token(cp: &PgControlPlane, subject: &str) -> String {
    let phc = service_runtime::hash_password("e2e-password").expect("hash");
    match cp
        .create_user(&NewUser {
            subject_id: SubjectId(subject.into()),
            username: subject.into(),
            password_phc: phc,
        })
        .await
    {
        Ok(()) | Err(ControlPlaneError::Conflict(_)) => {}
        Err(e) => panic!("create_user({subject}): {e}"),
    }
    let token = service_runtime::generate_session_token();
    let expires = OffsetDateTime::now_utc() + time::Duration::hours(1);
    cp.create_session(&SubjectId(subject.into()), &token_sha256(&token), expires)
        .await
        .expect("create_session");
    token
}
```

Rewrite `get` to authenticate (keep the signature):

```rust
pub async fn get(
    cp: Arc<PgControlPlane>,
    eng: Arc<EmbeddedDuckDb>,
    uri: &str,
    subject: &str,
) -> (StatusCode, serde_json::Value) {
    let token = session_token(&cp, subject).await;
    let app = protect(
        router(AppState {
            cp: cp.clone() as Arc<dyn ControlPlane>,
            serving: eng,
            action_engine: Arc::new(StubAction),
        }),
        AuthState {
            auth: cp.clone(),
            session_ttl: std::time::Duration::from_secs(3600),
        },
    );
    let res = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}
```

(`cp.clone()` for `AuthState.auth` coerces `Arc<PgControlPlane>` → `Arc<dyn Auth + Send + Sync>`.)

- [ ] **Step 2: Write the authenticated-path e2e**

Create `src/services/query-api/tests/auth_e2e.rs`. It drives the FULL composed router (gate + login route) and asserts the auth matrix from the spec:

```rust
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::Request;
use axum::http::{header::AUTHORIZATION, StatusCode};
use control_plane_core::{Auth, ControlPlane};
use control_plane_postgres::fixture::PgFixture;
use http_body_util::BodyExt;
use service_runtime::{login_routes, protect, token_sha256, AuthState};
use tower::ServiceExt;

use e2e_support::{grant_read, land, setup, subject_with_role, tref, StubAction};

// Build the full query-api app: protected object routes + public /auth/login.
// (Mirrors how the binary composes the router in Task 12.)
fn app(cp: Arc<control_plane_postgres::PgControlPlane>, eng: Arc<e2e_support::EmbeddedDuckDb>) -> axum::Router {
    let auth = AuthState { auth: cp.clone(), session_ttl: Duration::from_secs(3600) };
    protect(
        query_api::http::router(query_api::http::AppState {
            cp: cp as Arc<dyn ControlPlane>,
            serving: eng,
            action_engine: Arc::new(StubAction),
        }),
        auth.clone(),
    )
    .merge(login_routes(auth))
}
```

> **Implementer note:** reuse the exact seed/setup the other e2e tests use (`e2e_support::setup`) to create a `Customer`-style type, land a row, and `subject_with_role` + `grant_read` for an authorized subject. The four assertions to encode, each via `app(...).oneshot(...)`:
> 1. **login → token → read = 200**: seed a user with a known password (call `e2e_support::session_token` is the shortcut, OR drive `POST /auth/login` to get a token), present `Authorization: Bearer <token>` on the governed `GET /objects/<type>` → `200`.
> 2. **missing token = 401**: same GET with no `Authorization` header → `401`.
> 3. **bad password = 401**: `POST /auth/login` with a wrong password for a seeded user → `401`.
> 4. **revoked/expired = 401**: mint a session, `revoke_session` (or set `expires_at` in the past), present it → `401`.
> 5. **verified subject still ACL-denied = 403**: a subject with a valid session but NO `grant_read` on the type → authn passes (not 401) but `Acl::check` denies → `403`.

Encode each as its own `#[tokio::test]` against a fresh `PgFixture`. Use `e2e_support::session_token` to mint tokens for the authn-pass cases and `cp.revoke_session(&token_sha256(&token))` for the revoke case.

Add the test target to `src/services/query-api/BUCK` (mirror the existing e2e fixture tests — they use `loom_fixture_test` with `duckdb = True` and dep `:e2e-support`):

```python
loom_fixture_test(
    name = "auth-e2e",
    crate = "auth_e2e",
    srcs = ["tests/auth_e2e.rs"],
    crate_root = "tests/auth_e2e.rs",
    duckdb = True,
    deps = [
        ":query-api",
        ":e2e-support",
        "//src/services/runtime:runtime",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:axum",
        "//third-party:http-body-util",
        "//third-party:tokio",
        "//third-party:tower",
    ],
)
```

> **Implementer note:** confirm `query_api::http::{router, AppState}` and `EmbeddedDuckDb`/`StubAction` are reachable from the test (the e2e support lib already re-exports the needed items; extend its `pub use` if `EmbeddedDuckDb` or `query_api::http` is not visible). Match whatever the existing graph/object-set e2e tests import.

- [ ] **Step 3: Run the query-api e2e suite**

```bash
buck2 test //src/services/query-api:auth-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: PASS (5 cases). Then confirm the EXISTING e2e tests still pass through the rewritten `get`:

```bash
buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add src/services/query-api/tests/e2e_support.rs src/services/query-api/tests/auth_e2e.rs src/services/query-api/BUCK
git commit -m "test(query-api): authenticated e2e + session-token e2e harness"
```

---

### Task 12: Wire both service binaries

**Files:**
- Modify: `src/services/query-api/src/main.rs`
- Modify: `src/services/ingest/src/main.rs`

**Interfaces:**
- Consumes: `service_runtime::{protect, login_routes, session_routes, bootstrap_admin, AuthState}`, the concrete `PgControlPlane` for the `Auth` Arc.
- Produces: both binaries serve a router gated by `require_auth`, expose `/auth/login` + `/auth/logout`, and seed the bootstrap admin on boot.

- [ ] **Step 1: Add a shared env-parsing helper for auth config**

Both mains read three optional env vars directly (mirroring how they already read `LOOM_LANDING_BACKEND` etc. — NOT through `Config`, to avoid churning the `Config` struct and its tests):

- `LOOM_SESSION_TTL_SECS` (default `86400`)
- `LOOM_BOOTSTRAP_ADMIN_USERNAME` (optional)
- `LOOM_BOOTSTRAP_ADMIN_PASSWORD` (optional)

Add this free function to `src/services/runtime/src/auth.rs` and re-export it (so both binaries share one parser):

```rust
/// Read the session TTL from `LOOM_SESSION_TTL_SECS` (default 24h).
pub fn session_ttl_from_env() -> Duration {
    std::env::var("LOOM_SESSION_TTL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(86_400))
}
```

Re-export in `lib.rs`: add `session_ttl_from_env` to the `pub use auth::{...}` list.

- [ ] **Step 2: Wire query-api `main.rs`**

In `src/services/query-api/src/main.rs`, keep the concrete `PgControlPlane` Arc around so it can serve `Auth`. Replace the `cp` construction and the final `serve` section with:

```rust
    // Concrete PgControlPlane: serves both ControlPlane (read path) and Auth.
    let pg = Arc::new(service_runtime::control_plane(pool.clone(), cfg.lock_timeout));
    let cp: Arc<dyn ControlPlane> = pg.clone();

    // ... existing (serving, action_engine) backend match, using `cp` ...

    // Auth wiring.
    let auth_state = service_runtime::AuthState {
        auth: pg.clone(),
        session_ttl: service_runtime::session_ttl_from_env(),
    };
    if let (Ok(user), Ok(pass)) = (
        std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME"),
        std::env::var("LOOM_BOOTSTRAP_ADMIN_PASSWORD"),
    ) {
        service_runtime::bootstrap_admin(pg.as_ref(), &user, &pass).await?;
    }

    let app = service_runtime::protect(
        router(AppState { cp, serving, action_engine }),
        auth_state.clone(),
    )
    .merge(service_runtime::login_routes(auth_state.clone()))
    .merge(service_runtime::session_routes(auth_state));

    service_runtime::serve(cfg.bind_addr, app).await?;
    Ok(())
```

(`bootstrap_admin` returns `BootstrapError`; the `main` returns `Box<dyn Error>`, into which `BootstrapError` converts via `?`. Ensure `BootstrapError: std::error::Error` — it derives `thiserror::Error`, so it does.)

- [ ] **Step 3: Wire ingest `main.rs`**

Ingest gets the SAME authn gate (a verified subject) but no ACL gate on the materialize path (a deliberate non-goal). In `src/services/ingest/src/main.rs`, after the `materializer` is built and before `serve`, construct a `PgControlPlane` for auth (ingest already builds a `pool`; for the Iceberg backend the pool is moved into the materializer — clone it first):

```rust
    // Build the auth-serving control plane from the pool BEFORE it is moved into
    // an Iceberg materializer.
    let pg = Arc::new(service_runtime::control_plane(pool.clone(), cfg.lock_timeout));
    let auth_state = service_runtime::AuthState {
        auth: pg.clone(),
        session_ttl: service_runtime::session_ttl_from_env(),
    };
    if let (Ok(user), Ok(pass)) = (
        std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME"),
        std::env::var("LOOM_BOOTSTRAP_ADMIN_PASSWORD"),
    ) {
        service_runtime::bootstrap_admin(pg.as_ref(), &user, &pass).await?;
    }

    let app = service_runtime::protect(router(AppState { materializer }), auth_state.clone())
        .merge(service_runtime::login_routes(auth_state.clone()))
        .merge(service_runtime::session_routes(auth_state));
    service_runtime::serve(cfg.bind_addr, app).await?;
    Ok(())
```

> **Implementer note:** `pool` is consumed differently per backend in ingest's `main`. Clone it (`pool.clone()`) for the `pg` above so the existing materializer construction still owns its copy. If `cfg.lock_timeout` is already moved, capture it into a local before the backend match. Adjust only the wiring, not the materializer logic.

- [ ] **Step 4: Build both binaries**

```bash
buck2 build //src/services/query-api:query-api-bin //src/services/ingest:ingest-bin > /tmp/t.log 2>&1; tail -5 /tmp/t.log
```

Expected: BUILD SUCCEEDED.

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/src/main.rs src/services/ingest/src/main.rs src/services/runtime/src/auth.rs src/services/runtime/src/lib.rs
git commit -m "feat(services): gate both binaries with authn + bootstrap admin"
```

---

### Task 13: Update the over-the-wire and ingest e2e tests; full-suite green

**Files:**
- Modify: the over-the-wire HTTP e2e (`road-e2e-http-client`) test and any ingest fixture e2e that now hits a gated route.

**Interfaces:**
- Consumes: the composed auth router from Task 12 / the e2e `session_token` helper from Task 11.

- [ ] **Step 1: Find the tests broken by the authn gate**

```bash
grep -rln "spawn_http\|reqwest\|/datasets/\|ingest::http::router\|query_api::http::router" src/services --include=*.rs
```

The over-the-wire e2e builds both routers and drives them with a `reqwest` client; ingest fixture tests post to `/datasets/...`. Both routes are now gated → requests without a token get `401`.

- [ ] **Step 2: Make each broken test authenticate**

For every test that builds a service router and drives a protected route, apply the SAME composition the binary uses: wrap the service router with `service_runtime::protect(router, auth_state)` (and `.merge(login_routes(...))` if the test logs in), then seed a user + present a bearer token. The mechanical recipe:

```rust
// once per test, before driving protected routes:
let auth_state = service_runtime::AuthState { auth: cp.clone(), session_ttl: std::time::Duration::from_secs(3600) };
let app = service_runtime::protect(<service-router>, auth_state);
let token = /* mint a session for the test subject:
   service_runtime::generate_session_token() + cp.create_user(...) (ignore Conflict)
   + cp.create_session(&SubjectId(subject), &token_sha256(&token), now + 1h) */;
// then add to every request:
.header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))   // reqwest client
// or for tower::oneshot:
.header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"))
```

For the over-the-wire `spawn_http` test (both backends, both services), seed the admin via `LOOM_BOOTSTRAP_ADMIN_*` OR mint a session directly through the `PgControlPlane` the test already constructs, and attach the bearer header to its `reqwest` calls. Add the auth deps (`//src/services/runtime:runtime` is likely already present; add `//third-party:reqwest`'s header use is via the already-vendored alias) to that test's BUCK target if missing.

> **Implementer note:** keep the malformed-request (`4xx`) and governance-deny (`403`) assertions intact — they must still hold once authenticated (a verified-but-unauthorized subject is `403`, not `401`). Do not weaken the gate to make a test pass; give the test a real token.

- [ ] **Step 3: Run the FULL suite (the real gate)**

```bash
buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```

Expected: `Tests finished: ... 0 failed`. Investigate and fix any failure (a shared-dep regression would show as duckdb/serving failures in crates your diff did not touch — if so, re-assert the duckdb pin per the Global Constraints and re-buckify).

- [ ] **Step 4: Run clippy + prek on all files**

```bash
./tools/clippy-all.sh > /tmp/c.log 2>&1; tail -5 /tmp/c.log
buck2 run //tools:prek -- run --all-files > /tmp/p.log 2>&1; tail -15 /tmp/p.log
git add -A && git status --short
```

Expected: clippy clean; prek green (commit any in-place fixes the hooks make).

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test(e2e): authenticate the over-the-wire and ingest e2e paths"
```

---

### Task 14: Close the register item and record deferred follow-ons

**Files:**
- Modify: `docs/ROADMAP.md`, `docs/FUTURE.md` (via the `loom-docs-update` skill)

- [ ] **Step 1: Run the docs-update skill**

Invoke `loom-docs-update`. It will:
- Flip `road-auth-password-session` `[ ]→[x]`, set `status:done`, add `pr:#<this PR>`.
- Promote `[[fut-loom-auth]]` → `status:promoted` (umbrella) if not already.
- Ensure FUTURE records the sequenced follow-ons named in the spec: `[[fut-auth-totp-mfa]]`, `[[fut-auth-passkeys]]`, `[[fut-auth-saml]]`, `[[fut-auth-service-tokens]]`, `[[fut-auth-password-lifecycle]]`, `[[fut-auth-session-refresh]]`, plus newly-surfaced deferrals from THIS implementation: **ingest ACL gate** (`[[fut-ingest-followups]]`), **TLS** (`[[fut-graceful-shutdown-tls]]`).

- [ ] **Step 2: Validate the registers**

```bash
bash tools/docs.sh validate > /tmp/d.log 2>&1; cat /tmp/d.log
```

Expected: validation passes (ids/vocab/links/spec-slugs resolve).

- [ ] **Step 3: Commit**

```bash
git add docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(registers): close road-auth-password-session"
```

---

## Self-Review

**1. Spec coverage:**

| Spec requirement | Task |
| --- | --- |
| `auth` core trait (store, no crypto) + `NewUser`/`PasswordCredential` | 2 |
| `auth` Postgres schema (user/password_credential/session, hash PK) | 5 |
| memory fake + testkit contract (both backends) | 3, 4, 5 |
| Argon2id passwords + CSPRNG token + SHA-256 (service-side) | 6 |
| `POST /auth/login` → token; uniform 401; dummy-hash on unknown user | 8 |
| `POST /auth/logout` revokes (idempotent) | 8 |
| authentication middleware → verified `SubjectId`; 401 on absent/invalid/expired; first `Unauthorized` producer | 7 |
| handlers read injected `Subject`; `X-Loom-Subject` removed | 10 |
| bootstrap admin from config (empty-table guard) | 9, 12 |
| both binaries apply the middleware | 12 |
| `.sqlx` cache refreshed + `sqlx-cache-check` green | 5 |
| query-api e2e (login→200, missing→401, bad pw→401, revoked/expired→401, ACL-deny→403) | 11 |
| `e2e_support::get` swaps header for a real token | 11 |
| register outcome (promote umbrella, close item, record follow-ons) | 14 |
| non-goals (TLS, ingest ACL gate, TOTP/passkeys/SAML/service-tokens, password lifecycle, session refresh) — recorded, not built | 14 (FUTURE entries); not implemented anywhere |

No spec requirement is without a task.

**2. Placeholder scan:** Tasks 11 and 13 contain implementer notes rather than fully-literal test bodies for the e2e matrix and the broken-test sweep — this is deliberate: those tests reuse the existing `e2e_support` topology and an unknown set of pre-existing wire tests whose exact shape must be read at implementation time. Every NEW, self-contained unit (core types, memory fake, postgres adapter, crypto, middleware, login/logout, bootstrap) has complete literal code and exact assertions. The notes give the exact composition recipe (router wrapping + token minting) so there is no design ambiguity.

**3. Type consistency:** `Subject(pub SubjectId)` is defined once (Task 7, `service_runtime`) and consumed identically in query-api (Task 10) and the e2e harness (Task 11). `AuthState { auth: Arc<dyn Auth + Send + Sync>, session_ttl: Duration }` has the same shape at every construction site (Tasks 7, 8, 11, 12). `token_sha256(&str) -> [u8; 32]` and the adapter's `&[u8; 32]` params line up (Tasks 5, 6, 7). `create_user`/`find_password_credential`/`create_session`/`resolve_session`/`revoke_session`/`has_any_user` signatures are identical across the trait (Task 2), memory (Task 4), postgres (Task 5), and contract (Task 3).
