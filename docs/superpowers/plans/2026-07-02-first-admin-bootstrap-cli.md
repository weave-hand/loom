# First-admin bootstrap via host CLI — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `LOOM_BOOTSTRAP_ADMIN_*` env bootstrap with a one-time, out-of-band `loom create-admin` host CLI that seeds+seals the first admin, and give that admin an admin-gated HTTP surface to create models, roles, and coarse ACL grants.

**Architecture:** Add `Acl::has_role` and `Auth` sealing methods to the control-plane core (both adapters + testkit), an `acl.bootstrap` sealing table, a `create-admin` argv subcommand on the `loom` binary that runs a seed+seal transaction, an admin gate keyed on the reserved `admin` role, and four admin routes (`/admin/models`, `/admin/roles`, `/admin/roles/{role}/grants`, and the existing `/admin/users`).

**Tech Stack:** Rust, buck2, axum, sqlx (compile-time macros + committed `.sqlx`), Postgres, Argon2 (`service_runtime::hash_password`).

## Global Constraints

- Tests are `rust_test`/`loom_fixture_test` targets only — never inline `#[cfg(test)]` (the `no-inline-tests` prek hook fails the build otherwise). New unit tests go in `tests/<name>.rs` wired in the crate `BUCK`; fixture-backed tests use `loom_fixture_test`.
- Strict clippy (pedantic + restriction) on all production lib/bin code (incl. the CLI): no `unwrap`/`expect`/`indexing_slicing`/`panic`/`todo`; propagate with `?` and typed errors. Test code uses the `loom_rust_test`/`loom_fixture_test` wrappers which exempt the panic-safety lints.
- New control-plane SQL uses sqlx compile-time `query!`/`query_scalar!`; after changing SQL run `tools/sqlx-prepare.sh` and commit the `.sqlx` change. The `//src/control-plane/postgres:sqlx-cache-check` test enforces freshness.
- Both control-plane adapters (memory fake + postgres) implement every new trait method and are pinned by a `testkit` contract — no adapter divergence.
- The CLI has **no HTTP path**; it is argv-only on the `loom` binary.
- No standing ACL-bypass identity; the admin is governed like any subject for data reads (it self-grants).
- `buck2 test` output is never piped to `tail` — redirect to a file and grep it.
- The reserved admin role id is the string `admin`, exposed as `control_plane_core::ADMIN_ROLE`.

---

### Task 1: `Acl::has_role` (core trait + both adapters + contract)

**Files:**
- Modify: `src/control-plane/core/src/acl.rs` (add trait method + `ADMIN_ROLE` const)
- Modify: `src/control-plane/core/src/lib.rs` (re-export `ADMIN_ROLE`)
- Modify: `src/control-plane/postgres/src/acl.rs` (impl)
- Modify: `src/control-plane/memory/src/acl.rs` (impl)
- Modify: `src/control-plane/testkit/src/lib.rs` (`acl_contract` cases)
- Regenerate: `src/control-plane/postgres/.sqlx` (new query)

**Interfaces:**
- Produces: `Acl::has_role(&self, subject: &SubjectId, role: &RoleId) -> Result<bool>` — direct membership (not inheritance-transitive); unknown subject/role → `Ok(false)`, never an error. `pub const ADMIN_ROLE: &str = "admin";`

- [ ] **Step 1: Add the contract assertions to `acl_contract`**

In `src/control-plane/testkit/src/lib.rs`, inside `pub async fn acl_contract<A: Acl + Ontology>(a: &A)` (starts line ~1260), after the existing `assign_role(alice, reader)` setup (~line 1288), add:

```rust
// --- has_role: direct membership only ---
assert!(
    a.has_role(&sid("alice"), &rid("reader")).await.unwrap(),
    "alice was assigned reader"
);
assert!(
    !a.has_role(&sid("alice"), &rid("writer")).await.unwrap(),
    "alice not assigned writer yet"
);
assert!(
    !a.has_role(&sid("ghost"), &rid("reader")).await.unwrap(),
    "unknown subject → false, not error"
);
assert!(
    !a.has_role(&sid("alice"), &rid("no-such-role")).await.unwrap(),
    "unknown role → false, not error"
);
```

- [ ] **Step 2: Add the trait method (compile-fails the contract)**

In `src/control-plane/core/src/acl.rs`, add to the `Acl` trait (after `assign_role`, before `unassign_role`):

```rust
    /// `true` iff `subject` is directly assigned `role` (NOT inheritance-transitive).
    /// Unknown subject or role → `Ok(false)`, never an error. Used by the admin gate.
    async fn has_role(&self, subject: &SubjectId, role: &RoleId) -> Result<bool>;
```

And near the top-level items of the same file add:

```rust
/// The reserved role that gates the admin HTTP surface (`/admin/*`). Assigned to
/// the first admin by `loom create-admin`.
pub const ADMIN_ROLE: &str = "admin";
```

In `src/control-plane/core/src/lib.rs`, add `ADMIN_ROLE` to the `pub use acl::{…}` list.

- [ ] **Step 3: Run the contract build to confirm it fails**

Run: `buck2 build //src/control-plane/memory:memory 2>&1 | tail -5`
Expected: FAIL — `not all trait items implemented, missing: has_role` for `MemoryControlPlane`.

- [ ] **Step 4: Implement in the memory fake**

In `src/control-plane/memory/src/acl.rs`, inside `impl Acl for MemoryControlPlane`, add (place after `assign_role`):

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn has_role(&self, subject: &SubjectId, role: &RoleId) -> Result<bool> {
        Ok(self
            .acl
            .lock()
            .members
            .contains(&(subject.0.clone(), role.0.clone())))
    }
```

- [ ] **Step 5: Implement in the postgres adapter**

In `src/control-plane/postgres/src/acl.rs`, inside `impl Acl for PgControlPlane`, add (after `assign_role`):

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn has_role(&self, subject: &SubjectId, role: &RoleId) -> Result<bool> {
        Ok(sqlx::query_scalar!(
            "select exists (select 1 from acl.role_member \
             where subject_id = $1 and role_id = $2)",
            &subject.0,
            &role.0,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?
        .unwrap_or(false))
    }
```

- [ ] **Step 6: Regenerate the sqlx cache**

Run: `./tools/sqlx-prepare.sh 2>&1 | tail -5`
Expected: succeeds; `git status` shows a new/changed file under `src/control-plane/postgres/.sqlx/`.

- [ ] **Step 7: Run the ACL contract on both adapters**

Run: `buck2 test //src/control-plane/... 2>&1 > /tmp/t1.log; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS (the memory + postgres `acl-contract` fixture tests both exercise the new assertions).

- [ ] **Step 8: Commit**

```bash
git add src/control-plane/core src/control-plane/postgres src/control-plane/memory src/control-plane/testkit
git commit -m "feat(acl): Acl::has_role + ADMIN_ROLE const"
```

---

### Task 2: Bootstrap sealing state (Auth trait + migration + both adapters + contract)

**Files:**
- Modify: `src/control-plane/core/src/auth.rs` (two trait methods)
- Create: `src/control-plane/postgres/migrations/0026_bootstrap_seal.sql`
- Modify: `src/control-plane/postgres/src/auth.rs` (impl)
- Modify: `src/control-plane/memory/src/auth.rs` (impl + state field)
- Modify: `src/control-plane/testkit/src/lib.rs` (`auth_contract` cases)
- Regenerate: `src/control-plane/postgres/.sqlx`

**Interfaces:**
- Produces: `Auth::is_bootstrap_sealed(&self) -> Result<bool>` (true once sealed); `Auth::seal_bootstrap(&self) -> Result<()>` (insert-once; a second call returns `ControlPlaneError::Conflict`).

- [ ] **Step 1: Add the contract assertions**

In `src/control-plane/testkit/src/lib.rs`, inside `pub async fn auth_contract<A: Auth + Acl>(a: &A)` (starts ~line 1944), after the initial `assert!(!a.has_any_user()...)` block, add:

```rust
// --- bootstrap sealing: one-way ---
assert!(!a.is_bootstrap_sealed().await.unwrap(), "fresh CP is not sealed");
a.seal_bootstrap().await.unwrap();
assert!(a.is_bootstrap_sealed().await.unwrap(), "sealed after seal_bootstrap");
assert!(
    matches!(
        a.seal_bootstrap().await,
        Err(control_plane_core::ControlPlaneError::Conflict(_))
    ),
    "second seal is a Conflict, never a silent success"
);
```

- [ ] **Step 2: Add the trait methods**

In `src/control-plane/core/src/auth.rs`, add to the `Auth` trait (after `has_any_user`):

```rust
    /// `true` once the instance has been bootstrapped (an admin created + sealed).
    /// Bootstrap is a one-way state machine: there is deliberately no unseal method.
    async fn is_bootstrap_sealed(&self) -> Result<bool>;
    /// Mark the instance sealed. Insert-once: a second call returns `Conflict`.
    async fn seal_bootstrap(&self) -> Result<()>;
```

- [ ] **Step 3: Write the migration**

Create `src/control-plane/postgres/migrations/0026_bootstrap_seal.sql`:

```sql
-- One-way bootstrap seal: a single row records when the first admin was created.
-- Absence of the row = Uninitialized; presence = Sealed. There is no unseal path.
create table acl.bootstrap (
    id        smallint primary key default 1 check (id = 1),
    sealed_at timestamptz not null default now()
);
```

- [ ] **Step 4: Implement in the postgres adapter**

In `src/control-plane/postgres/src/auth.rs`, inside `impl Auth for PgControlPlane`, add (after `has_any_user`):

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn is_bootstrap_sealed(&self) -> Result<bool> {
        Ok(sqlx::query_scalar!("select exists (select 1 from acl.bootstrap where id = 1)")
            .fetch_one(&self.pool)
            .await
            .map_err(backend)?
            .unwrap_or(false))
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn seal_bootstrap(&self) -> Result<()> {
        let inserted = sqlx::query!(
            "insert into acl.bootstrap (id) values (1) on conflict (id) do nothing"
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?
        .rows_affected();
        if inserted == 0 {
            return Err(ControlPlaneError::Conflict("bootstrap already sealed".into()));
        }
        Ok(())
    }
```

(Confirm `backend` and `ControlPlaneError` are already imported in this file; `has_any_user` above uses the same `backend` mapper.)

- [ ] **Step 5: Implement in the memory fake**

In `src/control-plane/memory/src/auth.rs`, add a field to `AuthState` (line ~41 struct):

```rust
    /// Set once by `seal_bootstrap`; the one-way bootstrap seal.
    bootstrap_sealed: bool,
```

Then inside `impl Auth for MemoryControlPlane`, add (after `has_any_user`):

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn is_bootstrap_sealed(&self) -> Result<bool> {
        Ok(self.auth.lock().bootstrap_sealed)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    async fn seal_bootstrap(&self) -> Result<()> {
        let mut auth = self.auth.lock();
        if auth.bootstrap_sealed {
            return Err(ControlPlaneError::Conflict("bootstrap already sealed".into()));
        }
        auth.bootstrap_sealed = true;
        Ok(())
    }
```

(Confirm `ControlPlaneError` is imported in this file; other methods here return it.)

- [ ] **Step 6: Regenerate the sqlx cache (migration + new queries)**

Run: `./tools/sqlx-prepare.sh 2>&1 | tail -5`
Expected: succeeds (it applies `0026_bootstrap_seal.sql`, then re-prepares); `.sqlx` gains the two new query files.

- [ ] **Step 7: Run the control-plane suite**

Run: `buck2 test //src/control-plane/... 2>&1 > /tmp/t2.log; grep -E "Tests finished|FAIL" /tmp/t2.log`
Expected: PASS (both `auth-contract` fixtures exercise sealing; `sqlx-cache-check` sees the new schema+cache).

- [ ] **Step 8: Commit**

```bash
git add src/control-plane
git commit -m "feat(auth): one-way bootstrap seal (is_bootstrap_sealed/seal_bootstrap)"
```

---

### Task 3: `loom create-admin` CLI + remove env bootstrap

**Files:**
- Create: `src/services/runtime/src/create_admin.rs` (seed+seal logic)
- Modify: `src/services/runtime/src/lib.rs` (module + re-export; drop `bootstrap_admin` re-export)
- Modify: `src/services/runtime/src/auth.rs` (remove `bootstrap_admin` + `BootstrapError`)
- Modify: `src/services/standalone/src/main.rs` (argv dispatch + stdin password)
- Modify: `src/services/standalone/src/lib.rs` (delete the env-bootstrap block, lines ~56-62)
- Create: `src/services/standalone/tests/create_admin.rs` (`loom_fixture_test`)
- Modify: `src/services/standalone/BUCK` (new test target)
- Modify: `src/services/runtime/BUCK` (add `create_admin.rs` is part of the lib `srcs` glob — verify; add unit test target if the lib isn't globbed)

**Interfaces:**
- Consumes: `Acl::has_role`, `Acl::define_role`/`assign_role`/`define_subject`, `Auth::create_user`/`is_bootstrap_sealed`/`seal_bootstrap`, `ADMIN_ROLE`, `service_runtime::hash_password`.
- Produces: `service_runtime::create_admin::run_create_admin<CP: Auth + Acl + Sync>(cp: &CP, username: &str, password: &str) -> Result<(), CreateAdminError>` and `pub enum CreateAdminError { AlreadySealed, EmptyUsername, EmptyPassword, Hash, Backend(ControlPlaneError) }` (impl `std::error::Error` + `Display`).

- [ ] **Step 1: Write the failing fixture test**

Create `src/services/standalone/tests/create_admin.rs`. Mirror the fixture-config helper from `src/services/standalone/tests/composite_error_path.rs` for building a `PgControlPlane` over the fixture PG (a direct pool, no composite). Test body:

```rust
#[tokio::test]
async fn create_admin_seeds_then_seals() {
    let (cp, _guard) = fixture_control_plane().await; // PgControlPlane over fixture PG

    // First run: creates the admin, assigns ADMIN_ROLE, seals.
    service_runtime::create_admin::run_create_admin(&cp, "jack", "hunter2")
        .await
        .expect("first create-admin succeeds");

    assert!(cp.has_any_user().await.unwrap());
    assert!(cp.is_bootstrap_sealed().await.unwrap());
    assert!(
        cp.has_role(
            &control_plane_core::SubjectId("jack".into()),
            &control_plane_core::RoleId(control_plane_core::ADMIN_ROLE.into()),
        )
        .await
        .unwrap(),
        "jack holds the admin role"
    );

    // Second run: refused because sealed.
    let err = service_runtime::create_admin::run_create_admin(&cp, "mallory", "x")
        .await
        .expect_err("second create-admin is refused");
    assert!(matches!(
        err,
        service_runtime::create_admin::CreateAdminError::AlreadySealed
    ));

    // Empty password refused (on a fresh CP this would be EmptyPassword; here still refused).
    assert!(service_runtime::create_admin::run_create_admin(&cp, "x", "")
        .await
        .is_err());
}
```

Add the target to `src/services/standalone/BUCK` (mirror `composite-error-path`, i.e. `loom_fixture_test`), deps: `//src/control-plane/postgres:postgres`, `//src/control-plane/core:core`, `//src/services/runtime:runtime`, `//third-party:tokio`, `//third-party:tempfile`.

- [ ] **Step 2: Run it to confirm it fails**

Run: `buck2 build //src/services/standalone:create-admin 2>&1 | tail -5`
Expected: FAIL — `unresolved module or unlinked crate` / `run_create_admin` not found.

- [ ] **Step 3: Implement `run_create_admin`**

Create `src/services/runtime/src/create_admin.rs`:

```rust
//! First-admin bootstrap logic shared by the `loom create-admin` CLI. Seeds the
//! first admin and seals the instance in a single logical sequence; refuses once
//! sealed. No HTTP path — the caller is the host CLI only.
use control_plane_core::{Acl, ADMIN_ROLE, Auth, ControlPlaneError, NewUser, RoleId, SubjectId};

/// Failure modes of [`run_create_admin`].
#[derive(Debug, thiserror::Error)]
pub enum CreateAdminError {
    #[error("an admin already exists; re-bootstrap requires direct DB/host access")]
    AlreadySealed,
    #[error("username must not be empty")]
    EmptyUsername,
    #[error("password must not be empty")]
    EmptyPassword,
    #[error("password hashing failed")]
    Hash,
    #[error(transparent)]
    Backend(#[from] ControlPlaneError),
}

/// Create the first admin (`username`/`password`) and seal the instance. Refuses
/// with [`CreateAdminError::AlreadySealed`] if already sealed. Steps: guard →
/// create_user → define_subject → define_role(admin) → assign_role → seal.
pub async fn run_create_admin<CP: Auth + Acl + Sync>(
    cp: &CP,
    username: &str,
    password: &str,
) -> Result<(), CreateAdminError> {
    if username.trim().is_empty() {
        return Err(CreateAdminError::EmptyUsername);
    }
    if password.is_empty() {
        return Err(CreateAdminError::EmptyPassword);
    }
    if cp.is_bootstrap_sealed().await? {
        return Err(CreateAdminError::AlreadySealed);
    }
    let phc = crate::hash_password(password).map_err(|_| CreateAdminError::Hash)?;
    let subject = SubjectId(username.to_string());
    cp.create_user(&NewUser {
        subject_id: subject.clone(),
        username: username.to_string(),
        password_phc: phc,
    })
    .await?;
    cp.define_subject(&subject).await?;
    let admin_role = RoleId(ADMIN_ROLE.to_string());
    cp.define_role(&admin_role).await?;
    cp.assign_role(&subject, &admin_role).await?;
    cp.seal_bootstrap().await?;
    Ok(())
}
```

In `src/services/runtime/src/lib.rs` add `pub mod create_admin;` (near the other `mod` lines) — verify `create_admin.rs` is covered by the lib `srcs` (the runtime lib lists explicit `srcs`; if so, add `"src/create_admin.rs"` to it in `src/services/runtime/BUCK`).

- [ ] **Step 4: Remove the env bootstrap**

In `src/services/runtime/src/auth.rs`, delete `pub async fn bootstrap_admin(...)` and `pub enum BootstrapError`. In `src/services/runtime/src/lib.rs`, remove `bootstrap_admin` and `BootstrapError` from the `pub use auth::{…}` list.

In `src/services/standalone/src/lib.rs`, delete the block (lines ~56-62):

```rust
    if let (Ok(user), Ok(pass)) = (
        std::env::var("LOOM_BOOTSTRAP_ADMIN_USERNAME"),
        std::env::var("LOOM_BOOTSTRAP_ADMIN_PASSWORD"),
    ) {
        service_runtime::bootstrap_admin(pg.as_ref(), &user, &pass).await?;
    }
```

(Leave the `admin_subject`/`max_ttl` lines below it intact — `admin_subject` is used by `service_account_routes`; keep sourcing it from `LOOM_BOOTSTRAP_ADMIN_USERNAME` for now, or from `LOOM_SUPERUSER`-free default — do NOT remove that line in this step.)

- [ ] **Step 5: Run the fixture test to green**

Run: `buck2 test //src/services/standalone:create-admin 2>&1 > /tmp/t3.log; grep -E "Tests finished|FAIL|PASS" /tmp/t3.log`
Expected: PASS.

- [ ] **Step 6: Wire the CLI subcommand**

In `src/services/standalone/src/main.rs`, at the top of `main` (before `migrate_requested`), add argv dispatch:

```rust
    // Out-of-band first-admin bootstrap. `loom create-admin --username <u>` reads
    // the password from stdin (prompt to stderr); no network path.
    if std::env::args().nth(1).as_deref() == Some("create-admin") {
        return create_admin_cli().await;
    }
```

Add the helper (module-private) to `main.rs`:

```rust
async fn create_admin_cli() -> Result<(), BoxErr> {
    use std::io::Write;
    let mut username = None;
    let mut args = std::env::args().skip(2);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--username" => username = args.next(),
            "--password-stdin" => {} // password always read from stdin
            other => return Err(format!("unknown create-admin arg: {other}").into()),
        }
    }
    let username = username.ok_or_else(|| -> BoxErr { "--username is required".into() })?;

    // Read one line of password from stdin (works piped and interactive).
    let mut stderr = std::io::stderr();
    write!(stderr, "Password: ")?;
    stderr.flush()?;
    let mut password = String::new();
    std::io::stdin().read_line(&mut password)?;
    let password = password.trim_end_matches(['\n', '\r']).to_string();

    let env = service_runtime::env_map();
    let cfg = service_runtime::Config::from_map(&env)?;
    let pool = service_runtime::build_pool(&cfg.db).await?;
    let cp = service_runtime::control_plane(pool, cfg.lock_timeout);
    service_runtime::create_admin::run_create_admin(&cp, &username, &password).await?;
    println!("created admin '{username}' and sealed the instance");
    Ok(())
}
```

(`BoxErr` is the alias already declared at the top of `main.rs`.)

- [ ] **Step 7: Build the binary**

Run: `buck2 build //src/services/standalone:loom 2>&1 | tail -3`
Expected: BUILD SUCCEEDED.

- [ ] **Step 8: Run clippy on the touched crates**

Run: `buck2 build '//src/services/runtime:runtime[clippy.txt]' '//src/services/standalone:loom[clippy.txt]' 2>&1 | tail -3`
Expected: empty clippy output (clean).

- [ ] **Step 9: Commit**

```bash
git add src/services/runtime src/services/standalone
git commit -m "feat(deploy): loom create-admin CLI; remove LOOM_BOOTSTRAP_ADMIN_* env bootstrap"
```

---

### Task 4: Admin gate → `has_role`; `AdminState` refactor

**Files:**
- Modify: `src/services/runtime/src/admin.rs` (`AdminState`, `require_admin`)
- Modify: `src/services/query-api/src/serve.rs` (`AdminState` construction, ~lines 50-56)
- Modify: `src/services/runtime/tests/admin_routes.rs` (test harness: assign admin role)
- Modify: `src/services/query-api/tests/admin_e2e.rs` (same)

**Interfaces:**
- Consumes: `ControlPlane::acl()`, `Acl::has_role`, `ADMIN_ROLE`.
- Produces: `AdminState { auth: Arc<dyn Auth + Send + Sync>, cp: Arc<dyn ControlPlane> }` (drops `admin_username`; `cp` supplies `acl()` for the gate and `ontology()` for Task 5).

- [ ] **Step 1: Update the admin-routes test harness to grant the admin role**

The existing tests build an admin subject by matching `admin_username`. Change them to assign `ADMIN_ROLE`. In `src/services/runtime/tests/admin_routes.rs` (the `admin_routes(admin, auth)` setup ~line 55) and `src/services/query-api/tests/admin_e2e.rs` (~line 32): where the test currently sets `admin_username`, instead build `AdminState { auth, cp }` and, in the seed, `cp.define_subject(admin) → cp.define_role(RoleId("admin")) → cp.assign_role(admin, RoleId("admin"))`. Add an assertion that a subject WITHOUT the admin role receives 403 on an `/admin/*` route.

- [ ] **Step 2: Run to confirm failure**

Run: `buck2 build //src/services/runtime:admin-routes 2>&1 | tail -5`
Expected: FAIL — `AdminState` has no field `cp` / has field `admin_username`.

- [ ] **Step 3: Refactor `AdminState` + `require_admin`**

In `src/services/runtime/src/admin.rs`:

```rust
#[derive(Clone)]
pub struct AdminState {
    pub auth: Arc<dyn Auth + Send + Sync>,
    /// The direct (postgres-backed) control plane. Supplies `acl()` for the gate
    /// and role/grant writes, and `ontology()` for `define_type`.
    pub cp: Arc<dyn ControlPlane>,
}

/// Gate `/admin/*`: allow only a verified subject holding the reserved `admin`
/// role. Layered AFTER `require_auth` (which injects [`Subject`]); missing
/// `Subject` → 401, a non-admin → 403, a lookup error → 403 (fail closed).
pub async fn require_admin(State(st): State<AdminState>, req: Request, next: Next) -> Response {
    let Some(Subject(sid)) = req.extensions().get::<Subject>().cloned() else {
        return unauthorized();
    };
    match st.cp.acl().has_role(&sid, &RoleId(ADMIN_ROLE.to_string())).await {
        Ok(true) => next.run(req).await,
        Ok(false) => forbidden(),
        Err(_) => forbidden(),
    }
}
```

Update imports in `admin.rs`: add `control_plane_core::{Acl, ADMIN_ROLE, ControlPlane, RoleId}` (the `Acl` trait must be in scope to call `has_role`/`define_role`/`grant` on the `&dyn Acl` returned by `cp.acl()`); drop the `admin_username` doc field. In `create_user` and other handlers replace any `st.acl` / `st.auth` facet access: `st.auth` stays; replace `st.acl.<m>` with `st.cp.acl().<m>`.

- [ ] **Step 4: Update `serve.rs` construction**

In `src/services/query-api/src/serve.rs` (~lines 50-56), replace the `admin_username` + `admin_state` block with:

```rust
    let admin_state = service_runtime::AdminState {
        auth: auth.auth.clone(),
        cp: direct.clone(),
    };
```

Ensure `direct` is cloned here (it is later moved into `openapi_cp`; change that line to `let openapi_cp: Arc<dyn ControlPlane> = direct;` staying after this clone — `direct.clone()` above leaves `direct` usable). The `admin_subject`/`service_account_routes` lines are unchanged.

- [ ] **Step 5: Run the admin route tests**

Run: `buck2 test //src/services/runtime:admin-routes //src/services/query-api:admin-e2e 2>&1 > /tmp/t4.log; grep -E "Tests finished|FAIL" /tmp/t4.log`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/services/runtime src/services/query-api
git commit -m "feat(acl): admin gate keyed on the admin role (has_role); AdminState holds cp"
```

---

### Task 5: Admin governance routes — roles, grants, define model

**Files:**
- Modify: `src/services/runtime/src/admin.rs` (handlers + routes)
- Modify: `src/services/query-api/tests/admin_e2e.rs` (governance e2e)

**Interfaces:**
- Consumes: `ControlPlane::acl()`/`ontology()`, `Acl::{define_role, grant}`, `Ontology::define_type`, `ObjectType`/`PropertyDef`/`TableRef`/`TypeName`/`Action`/`PolicyTarget`/`Effect`.
- Produces routes (all under `require_admin`):
  - `POST /admin/roles` `{ "role": "reader" }` → 201
  - `POST /admin/roles/{role}/grants` `{ "action": "read"|"write", "type": "Widget" }` → 201 (unknown type → 400)
  - `POST /admin/models` `{ "name","table":{"schema","name"},"identity","properties":[{"name","ty","required"}] }` → 201

(No role-listing route this slice — YAGNI; a role is created then granted/assigned by name. `Acl` has no `list_roles`, and adding one is deferred.)

- [ ] **Step 1: Write the governance e2e (failing)**

In `src/services/query-api/tests/admin_e2e.rs`, add a test that (as the admin-role subject): `POST /admin/models` defining a `Widget` over an existing landed `main.widget`; `POST /admin/roles {role:"reader"}`; `POST /admin/roles/reader/grants {action:"read", type:"Widget"}`; creates a reader user via `POST /admin/users`; then asserts the reader can `GET /objects/Widget` and that a non-admin gets 403 on each new route, and a grant to an unknown type returns 400. (Reuse the existing `e2e_support` seed helpers for landing `main.widget`.)

- [ ] **Step 2: Run to confirm 404/failure**

Run: `buck2 test //src/services/query-api:admin-e2e 2>&1 > /tmp/t5.log; grep -E "FAIL|status" /tmp/t5.log | head`
Expected: FAIL (routes 404 / not implemented).

- [ ] **Step 3: Add request types + handlers**

In `src/services/runtime/src/admin.rs` add:

```rust
#[derive(serde::Deserialize)]
struct CreateRoleReq { role: String }

async fn create_role(State(st): State<AdminState>, Json(req): Json<CreateRoleReq>) -> Response {
    match st.cp.acl().define_role(&RoleId(req.role.clone())).await {
        Ok(()) => (StatusCode::CREATED, Json(serde_json::json!({ "role": req.role }))).into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}

#[derive(serde::Deserialize)]
struct GrantReq { action: String, r#type: String }

async fn grant(
    State(st): State<AdminState>,
    axum::extract::Path(role): axum::extract::Path<String>,
    Json(req): Json<GrantReq>,
) -> Response {
    let action = match req.action.as_str() {
        "read" => Action::Read,
        "write" => Action::Write,
        _ => return (StatusCode::BAD_REQUEST, "action must be read|write").into_response(),
    };
    let target = PolicyTarget::Type(TypeName(req.r#type.clone()));
    match st.cp.acl().grant(&RoleId(role), action, target, Effect::Allow).await {
        Ok(()) => (StatusCode::CREATED, "granted").into_response(),
        Err(e) => status_for(&e).into_response(), // unknown type → Validation → 400
    }
}

#[derive(serde::Deserialize)]
struct PropReq { name: String, ty: String, #[serde(default)] required: bool }
#[derive(serde::Deserialize)]
struct TableReq { schema: String, name: String }
#[derive(serde::Deserialize)]
struct DefineModelReq {
    name: String,
    table: TableReq,
    identity: Option<String>,
    properties: Vec<PropReq>,
}

async fn define_model(State(st): State<AdminState>, Json(req): Json<DefineModelReq>) -> Response {
    let otype = ObjectType {
        name: TypeName(req.name.clone()),
        table: TableRef { schema: req.table.schema, name: req.table.name },
        properties: req.properties.into_iter().map(|p| PropertyDef {
            name: p.name,
            ty: p.ty,
            required: p.required,
            constraints: control_plane_core::PropertyConstraints::default(),
        }).collect(),
        derived: vec![],
        identity: req.identity,
    };
    match st.cp.ontology().define_type(otype).await {
        Ok(()) => (StatusCode::CREATED, Json(serde_json::json!({ "name": req.name }))).into_response(),
        Err(e) => status_for(&e).into_response(),
    }
}
```

Add imports to `admin.rs`: `control_plane_core::{Acl, Action, Effect, ObjectType, Ontology, PolicyTarget, PropertyDef, TableRef, TypeName}` (`Acl` + `Ontology` traits in scope for the `cp.acl()`/`cp.ontology()` calls).

- [ ] **Step 4: Register the routes**

In `admin_routes` (`admin.rs` ~line 185) add to `inner`:

```rust
        .route("/admin/models", post(define_model))
        .route("/admin/roles", post(create_role))
        .route("/admin/roles/:role/grants", post(grant))
```

- [ ] **Step 5: Run the governance e2e green**

Run: `buck2 test //src/services/query-api:admin-e2e //src/services/runtime:admin-routes 2>&1 > /tmp/t5b.log; grep -E "Tests finished|FAIL" /tmp/t5b.log`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/services/runtime src/services/query-api
git commit -m "feat(acl): admin routes for roles, coarse grants, and define-model"
```

---

### Task 6: `dev-up.sh` + docs registers

**Files:**
- Modify: `tools/dev-up.sh`
- Modify: `docs/ROADMAP.md`

- [ ] **Step 1: Switch dev-up.sh to `create-admin`**

In `tools/dev-up.sh`, remove `LOOM_BOOTSTRAP_ADMIN_USERNAME`/`LOOM_BOOTSTRAP_ADMIN_PASSWORD` from the `exec env` list. Because the composite `exec`s and blocks, run the server in the background, wait for readiness, then create the admin. Replace the final `exec env … "$LOOM_BIN"` with:

```bash
echo "dev-up: starting loom on http://$QAPI_ADDR  (login: $ADMIN_USER / $ADMIN_PASS)"
echo "dev-up: UI served from $UI_DIR"
env "${common_env[@]}" \
  LOOM_PG_BIN_DIR="$PGROOT/bin" LOOM_PG_LD_LIBRARY_PATH="$PGLD" \
  LOOM_ENGINE_SOCKET="$DATA_PATH/engine.sock" \
  LOOM_QUERY_API_BIND_ADDR="$QAPI_ADDR" LOOM_INGEST_BIND_ADDR="$INGEST_ADDR" \
  LOOM_UI_DIR="$UI_DIR" "$LOOM_BIN" &
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null' EXIT INT TERM

# Wait for the query-api port, then bootstrap the first admin (idempotent: a
# second boot is refused because the instance is already sealed — that's fine).
host="${QAPI_ADDR%:*}"; port="${QAPI_ADDR##*:}"
for _ in $(seq 1 60); do
  (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null && { exec 3>&- 3<&-; break; }
  sleep 0.5
done
printf '%s' "$ADMIN_PASS" | env "${common_env[@]}" \
  LOOM_PG_BIN_DIR="$PGROOT/bin" LOOM_PG_LD_LIBRARY_PATH="$PGLD" \
  "$LOOM_BIN" create-admin --username "$ADMIN_USER" --password-stdin \
  || echo "dev-up: create-admin skipped (already sealed?)"

wait "$server_pid"
```

(`create-admin` connects to the running embedded PG via the shared `common_env` `LOOM_DB_HOST` socket; it does not boot its own PG.)

- [ ] **Step 2: Smoke-test dev-up end to end**

Run (background is fine): `rm -rf /tmp/loom-b && LOOM_DATA_PATH=/tmp/loom-b QAPI_ADDR=127.0.0.1:18080 tools/dev-up.sh > /tmp/devup.log 2>&1 &` then, after ~30s, `curl -s -m5 -X POST http://127.0.0.1:18080/auth/login -H 'content-type: application/json' -d '{"username":"admin","password":"admin"}' -w '\n%{http_code}\n'`.
Expected: `200` + a token. Then `kill %1`.

- [ ] **Step 3: Add the ROADMAP entry**

In `docs/ROADMAP.md` add (in the `acl` area grouping, matching the register grammar):

```
- [x] **First-admin bootstrap via host CLI** `{#road-first-admin-bootstrap area:acl status:done from:2026-07-02-first-admin-bootstrap-cli-design pr:- spec:2026-07-02-first-admin-bootstrap-cli-design}`
  Replaces the `LOOM_BOOTSTRAP_ADMIN_*` env bootstrap with a one-time out-of-band `loom create-admin` host CLI that seeds+seals the first admin (one-way `Uninitialized→Sealed` via `acl.bootstrap`); the admin is a normal identity holding the reserved `admin` role (`Acl::has_role` gate), with admin routes for models, roles, and coarse grants (`/admin/{models,roles,roles/:role/grants}`). No standing ACL-bypass superuser.
```

- [ ] **Step 4: Validate registers**

Run: `bash tools/docs.sh validate 2>&1 | tail -3`
Expected: `OK`.

- [ ] **Step 5: Commit**

```bash
git add tools/dev-up.sh docs/ROADMAP.md
git commit -m "chore(dev): dev-up.sh uses loom create-admin; ROADMAP road-first-admin-bootstrap"
```

---

## Notes for the final whole-branch review

- Confirm no `LOOM_BOOTSTRAP_ADMIN_*` references remain except the deliberate `admin_subject` sourcing in `standalone/src/lib.rs`/`serve.rs` for `service_account_routes` (or migrate that to a neutral default if trivial — flag, don't silently change).
- Confirm the admin gate fails **closed** on a `has_role` lookup error (403, not 500-open).
- Confirm the seed+seal ordering leaves an Uninitialized (retryable) instance on any mid-sequence failure — sealing is the last step.
- Confirm `.sqlx` cache committed and `sqlx-cache-check` green; run the full `buck2 test //src/...` once at the end (the trait additions touch every adapter + the fixture suite).
```
