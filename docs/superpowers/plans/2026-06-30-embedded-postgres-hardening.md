# Embedded Postgres slice-1 hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the four non-blocking hardening findings from the slice-1 review of the embedded-Postgres lifecycle (`src/services/managed-postgres`): owner-only dir perms, fast-fail readiness, a real fresh-init owner lock, and a graceful teardown that doesn't orphan backends.

**Architecture:** All four changes live in the single crate `src/services/managed-postgres/src/lib.rs` (the `EmbeddedPg` lifecycle type). They are additive and behaviour-preserving for the happy path — the existing `embedded_lifecycle` fixture test must stay green. New behaviour is proven by a new `loom_fixture_test` (`tests/embedded_hardening.rs`). No public API is added or changed; no new third-party dependency is introduced (the lock reuses the codebase's existing FFI-free `/proc`-liveness idiom rather than `flock`).

**Tech Stack:** Rust (edition 2024), `tokio::process`, `sqlx` (already deps), std `fs`/`os::unix`, buck2 (`cargo.rust_library` + `loom_fixture_test`).

## Global Constraints

- **Edition 2024.** Match the crate.
- **No inline `#[cfg(test)]` tests.** Every test is a sibling `tests/<name>.rs` wired as its own buck target. Fixture tests (anything that boots `initdb`/`postgres`) use the **`loom_fixture_test`** macro, never a bare `rust_test`. (Pure-logic tests use the `rust_test` wrapper.)
- **Strict clippy (pedantic + restriction) on production code.** No `unwrap`/`expect`/`panic`/`todo`/`indexing_slicing`/`dbg`/`get_unwrap`/`map_err_ignore`/`let_underscore_must_use` in `src/`. Discard best-effort `Result`s with `.ok();` (a statement) — never `let _ = <Result>`. Use `#[allow(lint, reason = "…")]` / `#[expect(lint, reason = "…")]` (reason required) for unavoidable cases. Test code is exempt from the panic-safety lints (the `loom_fixture_test` / `rust_test` wrappers inject the allows).
- **FFI-free preference.** The crate already avoids FFI: root detection reads `/proc/self/status`, the running-server check reads `postmaster.pid` + `/proc/<pid>`. The new owner lock follows the same idiom (an `O_EXCL` lockfile recording our pid, contended via `/proc/<pid>` liveness) rather than adding a `flock`/`libc`/`rustix` dependency.
- **No new third-party crate.** Do not edit `Cargo.toml` deps or run `buckify`.
- **Unix only.** `/proc`-based liveness is Linux; macOS falls through to existing fallbacks (consistent with the current code's comments). Do not add Windows handling.

---

## File map

- **Modify** `src/services/managed-postgres/src/lib.rs` — all four behaviours.
- **Create** `src/services/managed-postgres/tests/embedded_hardening.rs` — the new fixture test (perms, fast-fail, lock contention).
- **Modify** `src/services/managed-postgres/BUCK` — add the `embedded-hardening` `loom_fixture_test` target.
- **Modify** `docs/FUTURE.md` — close `fut-embedded-postgres-hardening` (`- [ ]`→`- [x]`, `status:done`, `pr:#N`) at PR time (Task 5).

---

### Task 1: Owner-only permissions on the data and socket dirs

Finding (1): with `-A trust` auth, a world-traversable socket dir lets any local user connect as the `postgres` superuser. Lock both the data dir and the socket dir to `0700`.

**Files:**
- Modify: `src/services/managed-postgres/src/lib.rs` (add `ensure_dir_secure`; replace the two `create_dir_all` calls in `start`)
- Create: `src/services/managed-postgres/tests/embedded_hardening.rs`
- Modify: `src/services/managed-postgres/BUCK` (new `embedded-hardening` target)

**Interfaces:**
- Produces: `fn ensure_dir_secure(dir: &Path) -> std::io::Result<()>` (private). Creates `dir` (and parents) if absent and clamps the **leaf**'s mode to `0o700`.

- [ ] **Step 1: Write the failing test** — create `tests/embedded_hardening.rs` with the perms assertion. (This file gains more tests in Tasks 2–3.)

```rust
//! Proves the slice-1 hardening contract: owner-only dir perms, fast-fail on a
//! dead postmaster, and the single-owner lock. Boots real `initdb`/`postgres`
//! via the buck `:postgres-bin` (fixture env), so it is a `loom_fixture_test`.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use managed_postgres::{EmbeddedPg, EmbeddedPgConfig, EmbeddedPgError};

fn cfg(data: &Path, sock: &Path) -> EmbeddedPgConfig {
    EmbeddedPgConfig {
        bin_dir: PathBuf::from(std::env::var("POSTGRES_BIN_DIR").expect("POSTGRES_BIN_DIR")),
        ld_library_path: std::env::var("POSTGRES_LD_LIBRARY_PATH").unwrap_or_default(),
        data_dir: data.to_path_buf(),
        socket_dir: sock.to_path_buf(),
        database: "loom".to_string(),
    }
}

fn mode_of(p: &Path) -> u32 {
    std::fs::metadata(p).expect("metadata").permissions().mode() & 0o777
}

#[tokio::test]
async fn data_and_socket_dirs_are_owner_only() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path().join("pgdata");
    let sock = tmp.path().join("pgrun");
    let pg = EmbeddedPg::start(cfg(&data, &sock)).await.expect("start");
    assert_eq!(mode_of(&data), 0o700, "data dir is owner-only");
    assert_eq!(mode_of(&sock), 0o700, "socket dir is owner-only");
    pg.shutdown().await.expect("shutdown");
}
```

- [ ] **Step 2: Add the `embedded-hardening` buck target** — append to `src/services/managed-postgres/BUCK`:

```python
loom_fixture_test(
    name = "embedded-hardening",
    crate = "embedded_hardening",
    srcs = ["tests/embedded_hardening.rs"],
    crate_root = "tests/embedded_hardening.rs",
    deps = [
        ":managed-postgres",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run the test to verify it fails** (the socket dir is `0755` today, so the assert fails — not a compile error, since `EmbeddedPgError` is already public):

Run: `buck2 test //src/services/managed-postgres:embedded-hardening -- --exact 'embedded_hardening::data_and_socket_dirs_are_owner_only' > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`
Expected: FAIL on `socket dir is owner-only` (actual `0o755`).

- [ ] **Step 4: Implement `ensure_dir_secure`** — add near the other free functions in `lib.rs` (after `check_not_already_running`):

```rust
/// Create `dir` (and any missing parents) and clamp the **leaf** to `0700`
/// (owner rwx only). With `-A trust` a world-traversable socket dir would let
/// any local user connect as the `postgres` superuser; `initdb` already wants
/// `0700` on the data dir, so this matches it and additionally locks the socket
/// dir. Only the leaf is re-permissioned — intermediate parents created on the
/// way keep their default mode. Idempotent: re-clamping an existing dir is a
/// no-op.
fn ensure_dir_secure(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(dir)?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
}
```

- [ ] **Step 5: Wire it into `start`** — in `EmbeddedPg::start`, replace:

```rust
        std::fs::create_dir_all(&cfg.data_dir)?;
        std::fs::create_dir_all(&cfg.socket_dir)?;
```

with:

```rust
        ensure_dir_secure(&cfg.data_dir)?;
        ensure_dir_secure(&cfg.socket_dir)?;
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `buck2 test //src/services/managed-postgres:embedded-hardening -- --exact 'embedded_hardening::data_and_socket_dirs_are_owner_only' > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`
Expected: PASS (1 test).

- [ ] **Step 7: Commit**

```bash
git add src/services/managed-postgres/src/lib.rs src/services/managed-postgres/tests/embedded_hardening.rs src/services/managed-postgres/BUCK
git commit -m "feat(managed-postgres): clamp pgdata/pgrun to 0700 (owner-only)"
```

---

### Task 2: Fast-fail readiness when the postmaster dies during startup

Finding (2): `wait_ready` polls for the full 15 s even when `postgres` has already exited (e.g. a bad config), masking the real cause. Poll the child handle so a dead postmaster fails immediately with its exit status.

**Files:**
- Modify: `src/services/managed-postgres/src/lib.rs` (new `ServerExited` variant; `wait_ready` takes `&mut self` and calls `try_wait`; `start` binds `pg` as `mut`)
- Modify: `src/services/managed-postgres/tests/embedded_hardening.rs` (add the fast-fail test)

**Interfaces:**
- Consumes: nothing new.
- Produces: `EmbeddedPgError::ServerExited(std::process::ExitStatus)`. `EmbeddedPg::wait_ready(&mut self)`.

- [ ] **Step 1: Write the failing test** — append to `tests/embedded_hardening.rs`:

```rust
#[tokio::test]
async fn dead_postmaster_fails_fast_not_after_timeout() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path().join("pgdata");
    let sock = tmp.path().join("pgrun");
    // Boot once to create the cluster, then shut down cleanly.
    EmbeddedPg::start(cfg(&data, &sock))
        .await
        .expect("init")
        .shutdown()
        .await
        .expect("shutdown");
    // Poison the config so the next `postgres` exits immediately on startup.
    let conf = data.join("postgresql.conf");
    let mut body = std::fs::read_to_string(&conf).expect("read conf");
    body.push_str("\nshared_buffers = 'definitely-not-a-size'\n");
    std::fs::write(&conf, body).expect("write conf");

    let t0 = std::time::Instant::now();
    let err = EmbeddedPg::start(cfg(&data, &sock))
        .await
        .expect_err("start must fail on a dead postmaster");
    let elapsed = t0.elapsed();
    assert!(
        matches!(err, EmbeddedPgError::ServerExited(_)),
        "expected ServerExited, got {err:?}"
    );
    // The readiness timeout is 15s; failing well under it proves the fast path.
    assert!(
        elapsed < std::time::Duration::from_secs(10),
        "should fail fast, took {elapsed:?}"
    );
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/services/managed-postgres:embedded-hardening -- --exact 'embedded_hardening::dead_postmaster_fails_fast_not_after_timeout' > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS|error\[" /tmp/t.log`
Expected: FAIL to **compile** — `EmbeddedPgError::ServerExited` does not exist yet.

- [ ] **Step 3: Add the `ServerExited` error variant** — in the `EmbeddedPgError` enum, after the `NotReady` variant:

```rust
    #[error("postgres exited during startup: {0}")]
    ServerExited(std::process::ExitStatus),
```

- [ ] **Step 4: Make `wait_ready` poll the child and take `&mut self`** — replace the whole `wait_ready` method body:

```rust
    async fn wait_ready(&mut self) -> Result<(), EmbeddedPgError> {
        let timeout = Duration::from_secs(15);
        for _ in 0..300 {
            // If the postmaster has already exited, fail fast with the real exit
            // status instead of polling a dead socket for the full timeout.
            if let Some(child) = self.server.as_mut() {
                if let Some(status) = child.try_wait()? {
                    return Err(EmbeddedPgError::ServerExited(status));
                }
            }
            if let Ok(mut conn) = self.maintenance_opts().connect().await {
                drop(conn.close().await);
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(EmbeddedPgError::NotReady(timeout))
    }
```

- [ ] **Step 5: Bind `pg` mutably in `start`** — in `EmbeddedPg::start`, change:

```rust
        let pg = EmbeddedPg {
```

to:

```rust
        let mut pg = EmbeddedPg {
```

(The subsequent `pg.wait_ready().await?;` now needs `&mut pg`; `ensure_database(&self)` and the `Ok(pg)` return are unaffected.)

- [ ] **Step 6: Run the test to verify it passes**

Run: `buck2 test //src/services/managed-postgres:embedded-hardening -- --exact 'embedded_hardening::dead_postmaster_fails_fast_not_after_timeout' > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/services/managed-postgres/src/lib.rs src/services/managed-postgres/tests/embedded_hardening.rs
git commit -m "feat(managed-postgres): fail fast when postmaster exits during startup"
```

---

### Task 3: Single-owner lock (fresh-init race) + graceful teardown

Finding (3): the `postmaster.pid` check misses two loom processes racing `initdb` on the **same empty** dir (no pidfile exists yet, so both pass). Add a real owner lock held for the cluster's life. Finding (4): a panic-drop SIGKILLs only the postmaster, orphaning backends — so make `Drop` stop the server gracefully.

The lock is an `O_EXCL` lockfile recording our pid (FFI-free, matching `check_not_already_running`). It lives **beside** the data dir (`<data_dir>.loomlock`), never inside it, because `initdb` requires an empty data dir. An RAII `OwnerLock` guard removes the lockfile on drop, so every early-return path in `start` (and the failure path of Task 2's `wait_ready`) reclaims it automatically.

**Files:**
- Modify: `src/services/managed-postgres/src/lib.rs` (add `OwnerLock`, `owner_lock_path`, `acquire_owner_lock`; add `_owner_lock` field; acquire it in `start`; add `impl Drop for EmbeddedPg`)
- Modify: `src/services/managed-postgres/tests/embedded_hardening.rs` (add the lock-contention test)

**Interfaces:**
- Consumes: `EmbeddedPgError::AlreadyLocked(PathBuf)` (already exists).
- Produces (all private): `struct OwnerLock { path: PathBuf }` with a `Drop` that removes the lockfile; `fn owner_lock_path(data_dir: &Path) -> PathBuf`; `fn acquire_owner_lock(data_dir: &Path) -> Result<OwnerLock, EmbeddedPgError>`. New field `EmbeddedPg._owner_lock: OwnerLock`. New `impl Drop for EmbeddedPg`.

- [ ] **Step 1: Write the failing test** — append to `tests/embedded_hardening.rs`:

```rust
#[tokio::test]
async fn second_owner_on_same_data_dir_is_rejected() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path().join("pgdata");
    let sock = tmp.path().join("pgrun");
    // First owner boots and holds the lock for its lifetime.
    let pg1 = EmbeddedPg::start(cfg(&data, &sock)).await.expect("start 1");
    // A second start on the SAME data dir must be rejected before it can race
    // initdb / a second postmaster.
    let err = EmbeddedPg::start(cfg(&data, &sock))
        .await
        .expect_err("second owner must be rejected");
    assert!(
        matches!(err, EmbeddedPgError::AlreadyLocked(_)),
        "expected AlreadyLocked, got {err:?}"
    );
    // Releasing the first owner frees the lock so a fresh start succeeds.
    pg1.shutdown().await.expect("shutdown 1");
    EmbeddedPg::start(cfg(&data, &sock))
        .await
        .expect("start after release")
        .shutdown()
        .await
        .expect("shutdown 2");
}
```

- [ ] **Step 2: Run the test to verify it fails** (today the second start adopts the cluster / races, so no `AlreadyLocked`):

Run: `buck2 test //src/services/managed-postgres:embedded-hardening -- --exact 'embedded_hardening::second_owner_on_same_data_dir_is_rejected' > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS|panic" /tmp/t.log`
Expected: FAIL (the second `start` does not return `AlreadyLocked`).

- [ ] **Step 3: Add the lock primitives** — add to `lib.rs` (near the other free functions; needs `use std::fs::OpenOptions; use std::io::Write;` — add these to the existing `use` block at the top):

```rust
/// Cross-process owner-lockfile path for a data dir. It lives *beside* the data
/// dir, not inside it: `initdb` refuses a non-empty data dir, so a lockfile
/// within would break a fresh init. For `/x/pgdata` the lock is
/// `/x/pgdata.loomlock`.
fn owner_lock_path(data_dir: &Path) -> PathBuf {
    let mut p = data_dir.as_os_str().to_os_string();
    p.push(".loomlock");
    PathBuf::from(p)
}

/// RAII single-owner lock for a data dir. The lockfile's existence (recording
/// our pid) *is* the lock; the guard removes it on drop so the dir is
/// reclaimable after a clean shutdown, an early-return error, or a panic.
struct OwnerLock {
    path: PathBuf,
}

impl Drop for OwnerLock {
    fn drop(&mut self) {
        // Best-effort: a leftover lockfile is reclaimed by the next start's
        // pid-liveness check, so a removal error is not worth surfacing.
        std::fs::remove_file(&self.path).ok();
    }
}

/// Acquire the single-owner lock for `data_dir`. Closes the `initdb`-race gap
/// the `postmaster.pid` check misses: two loom processes starting on the same
/// empty dir both pass the pid check (no pidfile yet) and would race `initdb`.
/// The lock is an `O_EXCL` create recording our pid; a contender whose recorded
/// pid is still alive (`/proc/<pid>`) loses with `AlreadyLocked`, while a lock
/// left by a crashed owner (pid dead) is reclaimed.
fn acquire_owner_lock(data_dir: &Path) -> Result<OwnerLock, EmbeddedPgError> {
    let path = owner_lock_path(data_dir);
    // At most a couple of iterations: a stale lock is removed once, then the
    // create either wins or finds a live holder. The bound guards against any
    // pathological flapping.
    for _ in 0..5 {
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut f) => {
                // Record our pid so a later contender can test our liveness.
                write!(f, "{}", std::process::id())?;
                f.flush()?;
                return Ok(OwnerLock { path });
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let holder_alive = std::fs::read_to_string(&path)
                    .ok()
                    .and_then(|s| s.trim().parse::<u32>().ok())
                    .is_some_and(|pid| Path::new(&format!("/proc/{pid}")).exists());
                if holder_alive {
                    return Err(EmbeddedPgError::AlreadyLocked(data_dir.to_path_buf()));
                }
                // Stale lock from a crashed owner — remove it and retry the create.
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
    Err(EmbeddedPgError::AlreadyLocked(data_dir.to_path_buf()))
}
```

- [ ] **Step 4: Add the `_owner_lock` field** — in the `EmbeddedPg` struct, add as the **last** field:

```rust
    /// Held for the cluster's life; its `Drop` releases the on-disk owner lock.
    #[allow(dead_code, reason = "RAII guard — only its Drop matters")]
    _owner_lock: OwnerLock,
```

- [ ] **Step 5: Acquire the lock in `start` and store it on the handle** — in `EmbeddedPg::start`, right after the two `ensure_dir_secure` calls and **before** `check_not_already_running`, insert:

```rust
        // Single-owner lock (covers the fresh-init race the pidfile check misses).
        // Acquired before initdb/spawn; the guard frees the lockfile on any
        // early-return below or when the handle is dropped.
        let owner_lock = acquire_owner_lock(&cfg.data_dir)?;
```

Then move `owner_lock` into the handle by adding it to the struct literal as the last field (alongside `database: cfg.database,`):

```rust
            _owner_lock: owner_lock,
```

- [ ] **Step 6: Add the graceful `Drop` for `EmbeddedPg`** — append after the `impl EmbeddedPg` blocks:

```rust
impl Drop for EmbeddedPg {
    fn drop(&mut self) {
        // `shutdown()` already stopped the server and took `server` (None here),
        // so this best-effort path only runs on an abnormal drop — a panic or an
        // early-return error during `start()`. Stop the postmaster *gracefully*
        // (immediate mode makes it terminate its backends; a bare SIGKILL via
        // kill_on_drop would orphan them), then fall back to SIGKILL if pg_ctl
        // can't. The `_owner_lock` field's own Drop frees the lockfile afterwards.
        if let Some(mut child) = self.server.take() {
            let mut cmd = std::process::Command::new(self.bin_dir.join("pg_ctl"));
            if !self.ld_library_path.is_empty() {
                cmd.env("LD_LIBRARY_PATH", &self.ld_library_path);
            }
            cmd.arg("stop")
                .arg("-D")
                .arg(&self.data_dir)
                .args(["-m", "immediate", "-w"]);
            let stopped = cmd.status().map(|s| s.success()).unwrap_or(false);
            if !stopped {
                // pg_ctl couldn't stop it (e.g. already exited) — best-effort kill.
                child.start_kill().ok();
            }
        }
    }
}
```

- [ ] **Step 7: Run the lock-contention test to verify it passes**

Run: `buck2 test //src/services/managed-postgres:embedded-hardening -- --exact 'embedded_hardening::second_owner_on_same_data_dir_is_rejected' > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`
Expected: PASS.

- [ ] **Step 8: Run the whole new fixture target + the existing lifecycle test** (proves no regression and the Drop path is exercised by the fast-fail test):

Run: `buck2 test //src/services/managed-postgres:embedded-hardening //src/services/managed-postgres:embedded-lifecycle //src/services/managed-postgres:db-name > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`
Expected: all PASS, 0 FAIL.

- [ ] **Step 9: Commit**

```bash
git add src/services/managed-postgres/src/lib.rs src/services/managed-postgres/tests/embedded_hardening.rs
git commit -m "feat(managed-postgres): single-owner lock + graceful teardown"
```

---

### Task 4: Whole-crate verification, clippy, and register close-out

**Files:**
- Modify: `docs/FUTURE.md` (close the item)

- [ ] **Step 1: Full crate test sweep**

Run: `buck2 test //src/services/managed-postgres/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`
Expected: all PASS, 0 FAIL.

- [ ] **Step 2: Clippy clean on the production library** (pedantic + restriction; must be empty):

Run: `buck2 build '//src/services/managed-postgres:managed-postgres[clippy.txt]' --show-output > /tmp/c.log 2>&1; out=$(grep -oE '[^ ]*clippy.txt' /tmp/c.log | head -1); echo "--- clippy.txt ---"; cat "buck-out/.../$out" 2>/dev/null || cat $(buck2 build '//src/services/managed-postgres:managed-postgres[clippy.txt]' --show-output 2>/dev/null | awk '{print $2}')`
Expected: empty clippy output (no warnings). If anything fires, fix it (e.g. add a `#[expect(lint, reason = "…")]` or restructure) and re-run.

- [ ] **Step 3: Confirm the dead-postmaster Drop path doesn't hang or double-stop** — already covered: `dead_postmaster_fails_fast_not_after_timeout` constructs an `EmbeddedPg`, lets `wait_ready` error, and drops it (running `Drop`); a hang/panic there would fail that test. No extra test needed.

- [ ] **Step 4: Close the register item** (done at PR time via `loom-docs-update`; the literal edit): in `docs/FUTURE.md`, change the `fut-embedded-postgres-hardening` line from

```
- [ ] **Embedded Postgres slice-1 hardening (perms, fast-fail, fresh-init lock)** `{#fut-embedded-postgres-hardening area:deploy status:deferred from:2026-06-28-embedded-postgres-lifecycle-design pr:- spec:2026-06-28-embedded-postgres-lifecycle-design}`
```

to (replace `#N` with the actual PR number):

```
- [x] **Embedded Postgres slice-1 hardening (perms, fast-fail, fresh-init lock)** `{#fut-embedded-postgres-hardening area:deploy status:done from:2026-06-28-embedded-postgres-lifecycle-design pr:#N spec:2026-06-28-embedded-postgres-lifecycle-design}`
```

- [ ] **Step 5: Validate the registers and lint markdown**

Run: `bash tools/docs.sh validate && buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; grep -iE "failed|error" /tmp/prek.log | head`
Expected: docs validate OK; prek hooks pass (commit any in-place fixes the hooks make).

- [ ] **Step 6: Commit**

```bash
git add docs/FUTURE.md
git commit -m "docs(future): close embedded-postgres slice-1 hardening"
```

---

## Self-Review

**Spec / findings coverage:**
- Finding (1) owner-only perms → Task 1 (`ensure_dir_secure`, `0700` on data + socket dirs; asserted by `data_and_socket_dirs_are_owner_only`). ✓
- Finding (2) fast-fail readiness → Task 2 (`ServerExited` + `wait_ready` `try_wait`; asserted by `dead_postmaster_fails_fast_not_after_timeout`). ✓
- Finding (3) fresh-init lock → Task 3 (`OwnerLock` + `acquire_owner_lock`, acquired before initdb; asserted by `second_owner_on_same_data_dir_is_rejected`). ✓
- Finding (4) orphaned backends on panic-drop → Task 3 (`impl Drop for EmbeddedPg` graceful `pg_ctl stop -m immediate`, SIGKILL fallback). ✓
- Slice-1 spec invariants preserved: idempotent init, restart-adoption, persistence, clean shutdown → existing `embedded_lifecycle` test stays green (Task 3 Step 8, Task 4 Step 1). ✓

**Placeholder scan:** every code step shows complete code; no TBD/TODO/"handle errors". ✓

**Type consistency:** `ensure_dir_secure(&Path)->io::Result<()>`, `owner_lock_path(&Path)->PathBuf`, `acquire_owner_lock(&Path)->Result<OwnerLock,EmbeddedPgError>`, `OwnerLock{path}`, `EmbeddedPgError::ServerExited(ExitStatus)`, `wait_ready(&mut self)` — names/signatures are referenced consistently across tasks. The `_owner_lock` field is set in `start`'s struct literal and declared on the struct. ✓

**Risks called out:**
- `Drop` runs a brief blocking `pg_ctl` on the abnormal path only (normal path uses `shutdown()`); acceptable since we're already tearing down. The `dead_postmaster…` test exercises this path.
- `/proc`-based pid liveness has the same pid-reuse caveat already accepted by `check_not_already_running` — consistent, not a new exposure.
- `_owner_lock` is never read → `#[allow(dead_code, reason=…)]`; using `allow` (not `expect`) avoids an unfulfilled-expectation error if the lint placement shifts.
