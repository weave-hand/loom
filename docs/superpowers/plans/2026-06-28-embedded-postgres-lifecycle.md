# Embedded Postgres Lifecycle (slice 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give loom an embedded-Postgres lifecycle so a single service can boot (or adopt) a persistent local cluster with no external Postgres server, reusing the entire `control-plane-postgres` adapter unchanged.

**Architecture:** A new buck-only crate `src/services/managed-postgres/` exposes an `EmbeddedPg` type that promotes the test fixture's ephemeral-cluster logic (`fixture.rs`) to a persistent runtime: refuse-if-root → file-lock the data dir → `initdb`-if-absent → spawn `postgres` on a unix socket → wait-ready → ensure the app database. `service_runtime` gains a `build_pool_managed` seam selected by `LOOM_PG_MODE=embedded` that starts `EmbeddedPg`, builds a `PgPool` from its socket, and runs the existing directory-based migrations. External Postgres stays the default, unchanged.

**Tech Stack:** Rust 2024, buck2, sqlx 0.9 (postgres, runtime-tokio), tokio (process + time), libc (geteuid + flock), the vendored `:postgres-bin` distribution, `loom_fixture_test` for the hermetic test.

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`. Each test is a sibling `tests/<name>.rs` wired as its own target. The `no-inline-tests` prek hook fails on any `#[test]`/`#[tokio::test]` in `src/**.rs`.
- **Fixture-backed tests (real `initdb`/`postgres`) MUST use the `loom_fixture_test` macro** (`load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")`), never a bare `rust_test`, or they route to RE and fail as root. Pure-logic tests use plain `rust_test` (RE-eligible).
- **`initdb`/`postgres` refuse to run as uid 0** — this is load-bearing, not incidental.
- **buck2 is the source of truth for builds/tests.** Run `buck2 test //src/...` (see the buck/inotify note below). Do not pipe `buck2 test` through `tail`/`head` — redirect to a file and grep it.
- **buck2 file-watcher caveat (this environment):** if `buck2` fails at startup with *"Error creating a FileWatcher … OS file watch limit reached"*, the box lacks watchman and `buck-out` exceeds the inotify watch limit. Builds/tests cannot run locally there; rely on CI/RE for the canonical gate and verify locally via the cargo path the fixture exposes (`POSTGRES_BIN_DIR` etc.). State this in any completion claim if buck2 could not be run.
- **First-party crates may be buck-only** (no `Cargo.toml`, not a workspace member) — `src/services/store-config/` is the precedent to mirror.
- Commit message convention: Conventional Commits (`feat:`/`refactor:`/`test:`/`docs:`), enforced by the `conventional-commit` hook.

---

## File Structure

- `src/services/managed-postgres/BUCK` — crate + test targets (mirror `store-config`, add a `loom_fixture_test`).
- `src/services/managed-postgres/src/lib.rs` — `EmbeddedPgConfig`, `EmbeddedPgError`, `EmbeddedPg` (lifecycle), `validate_db_name`. One file; the crate has one responsibility.
- `src/services/managed-postgres/tests/db_name.rs` — pure-logic unit test for `validate_db_name` (plain `rust_test`, RE).
- `src/services/managed-postgres/tests/embedded_lifecycle.rs` — the fixture-backed idempotency/persistence test (`loom_fixture_test`).
- `src/services/runtime/src/lib.rs` — add `EmbeddedSettings`, a `Config.embedded` field + parse, `RuntimeError` variants, and `build_pool_managed`.
- `src/services/runtime/BUCK` — add the `managed-postgres` dep; a `rust_test` for the new config parsing.

## Deviation from spec (intentional)

The spec listed an **embedded `sqlx::migrate!` runner** ("no migrations-on-disk") in slice 1. There is no precedent for compile-time file embedding under buck2 anywhere in this repo, and slice 1's test already receives `LOOM_MIGRATIONS_DIR` from the fixture. So slice 1 uses the **existing** directory-based `control_plane_postgres::run_migrations(pool, dir)`; the true no-disk embed moves to **slice 2**, where it is solved with the same extract-to-dir mechanism as the PG binaries. This keeps slice 1 low-risk and needs **zero** change to `control-plane-postgres`. (Update the spec's slice-1/slice-2 scope bullets to match.)

---

### Task 1: `managed-postgres` crate — config types, error type, db-name guard

Scaffolds the crate and the pure-logic surface (no process spawning yet), so the lifecycle task builds on a compiling crate with a tested validator.

**Files:**
- Create: `src/services/managed-postgres/src/lib.rs`
- Create: `src/services/managed-postgres/BUCK`
- Create: `src/services/managed-postgres/tests/db_name.rs`

**Interfaces:**
- Produces: `managed_postgres::EmbeddedPgConfig { bin_dir: PathBuf, ld_library_path: String, data_dir: PathBuf, socket_dir: PathBuf, database: String }` (derives `Clone, Debug, PartialEq, Eq`); `managed_postgres::EmbeddedPgError` (a `thiserror` enum); `managed_postgres::validate_db_name(&str) -> Result<(), EmbeddedPgError>`.

- [ ] **Step 1: Write the crate with config + error + validator (no lifecycle yet)**

Create `src/services/managed-postgres/src/lib.rs`:

```rust
//! Embedded Postgres lifecycle: boot (or adopt) a persistent cluster on a unix
//! socket and hand back sqlx connect options, so loom can run with no external
//! Postgres server. Promotes the test fixture's ephemeral-cluster logic to a
//! persistent runtime. `initdb`/`postgres` refuse to run as root by design.

use std::path::PathBuf;

/// Parameters for an embedded cluster. `bin_dir`/`ld_library_path` are a
/// configurable path in slice 1 (point them at the buck `:postgres-bin` output);
/// slice 2 fills them from an extracted asset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddedPgConfig {
    /// Postgres `bin/` directory (holds initdb, postgres, pg_ctl).
    pub bin_dir: PathBuf,
    /// LD_LIBRARY_PATH for the spawned binaries (dist libs + libxml2).
    pub ld_library_path: String,
    /// Persistent cluster data dir (e.g. `<LOOM_DATA_PATH>/pgdata`).
    pub data_dir: PathBuf,
    /// Unix-socket directory (e.g. `<LOOM_DATA_PATH>/pgrun`).
    pub socket_dir: PathBuf,
    /// Application database name. Validated as `[a-z_][a-z0-9_]*`.
    pub database: String,
}

#[derive(Debug, thiserror::Error)]
pub enum EmbeddedPgError {
    #[error("embedded Postgres cannot run as root — run loom as a normal user")]
    RunningAsRoot,
    #[error("another process already owns the data dir {0}")]
    AlreadyLocked(PathBuf),
    #[error("invalid database name {0:?}: must match [a-z_][a-z0-9_]*")]
    InvalidDbName(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("initdb failed: exit {0}")]
    Initdb(std::process::ExitStatus),
    #[error("postgres did not become ready within {0:?}")]
    NotReady(std::time::Duration),
    #[error("connect: {0}")]
    Connect(sqlx::Error),
    #[error("create database: {0}")]
    CreateDatabase(sqlx::Error),
    #[error("pg_ctl stop failed: exit {0}")]
    Stop(std::process::ExitStatus),
}

/// Guard the database name before it is spliced into `CREATE DATABASE` (which
/// cannot be parameterised). Conservative identifier subset; rejects anything
/// that could break out of the identifier position.
pub fn validate_db_name(name: &str) -> Result<(), EmbeddedPgError> {
    let mut chars = name.chars();
    let ok_first = matches!(chars.next(), Some(c) if c == '_' || c.is_ascii_lowercase());
    let ok_rest = chars.all(|c| c == '_' || c.is_ascii_lowercase() || c.is_ascii_digit());
    if ok_first && ok_rest && !name.is_empty() {
        Ok(())
    } else {
        Err(EmbeddedPgError::InvalidDbName(name.to_string()))
    }
}
```

- [ ] **Step 2: Write the BUCK file**

Create `src/services/managed-postgres/BUCK`:

```python
load("//src:loom_test.bzl", "rust_test")
load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")
load("@prelude//rust:cargo_package.bzl", "cargo")

cargo.rust_library(
    name = "managed-postgres",
    crate = "managed_postgres",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [
        "//third-party:libc",
        "//third-party:sqlx",
        "//third-party:thiserror",
        "//third-party:tokio",
        "//third-party:tracing",
    ],
    visibility = ["PUBLIC"],
)

rust_test(
    name = "db-name",
    crate = "db_name",
    srcs = ["tests/db_name.rs"],
    crate_root = "tests/db_name.rs",
    edition = "2024",
    deps = [":managed-postgres"],
)
```

(The `loom_fixture_test` load is added now so Task 2 only appends a target.)

- [ ] **Step 3: Write the failing pure-logic test**

Create `src/services/managed-postgres/tests/db_name.rs`:

```rust
use managed_postgres::validate_db_name;

#[test]
fn accepts_simple_lowercase_names() {
    assert!(validate_db_name("loom").is_ok());
    assert!(validate_db_name("loom_local_2").is_ok());
}

#[test]
fn rejects_empty_uppercase_and_punctuation() {
    assert!(validate_db_name("").is_err());
    assert!(validate_db_name("Loom").is_err());
    assert!(validate_db_name("loom; drop").is_err());
    assert!(validate_db_name("2loom").is_err());
}
```

- [ ] **Step 4: Run the test, expect a pass (logic is already implemented)**

Run: `buck2 test //src/services/managed-postgres:db-name > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`
Expected: `Tests finished: PASS`. (If buck2 cannot start — see the file-watcher caveat — run `cargo test -p` is not available for a buck-only crate; instead confirm via the lifecycle task's cargo path in Task 2.)

- [ ] **Step 5: Commit**

```bash
git add src/services/managed-postgres/
git commit -m "feat(managed-postgres): crate scaffold with config + db-name guard"
```

---

### Task 2: `EmbeddedPg` lifecycle + fixture-backed idempotency/persistence test

The core of the slice: start/adopt a persistent cluster, hand back connect options, shut down cleanly — proven by a restart-and-survive test.

**Files:**
- Modify: `src/services/managed-postgres/src/lib.rs`
- Modify: `src/services/managed-postgres/BUCK`
- Create: `src/services/managed-postgres/tests/embedded_lifecycle.rs`

**Interfaces:**
- Consumes: `EmbeddedPgConfig`, `EmbeddedPgError`, `validate_db_name` (Task 1).
- Produces: `EmbeddedPg` with `async fn start(EmbeddedPgConfig) -> Result<EmbeddedPg, EmbeddedPgError>`, `fn connect_options(&self) -> sqlx::postgres::PgConnectOptions`, `fn socket_dir(&self) -> &std::path::Path`, `async fn shutdown(self) -> Result<(), EmbeddedPgError>`.

- [ ] **Step 1: Add imports and the `EmbeddedPg` struct to `lib.rs`**

Add to the top of `src/services/managed-postgres/src/lib.rs` (after the existing `use`):

```rust
use std::os::fd::AsRawFd;
use std::path::Path;
use std::time::Duration;

use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, Connection, Executor, PgConnection, Row};
use tokio::process::{Child, Command};

/// A running, owned embedded Postgres. Prefer `shutdown()` for a clean
/// `pg_ctl stop -m fast`; `Drop` is a best-effort fast stop so a panic does not
/// leak a postmaster.
pub struct EmbeddedPg {
    server: Option<Child>,
    bin_dir: PathBuf,
    ld_library_path: String,
    data_dir: PathBuf,
    socket_dir: PathBuf,
    database: String,
    // flock held for the handle's life; released on Drop / process exit.
    _lock: std::fs::File,
}
```

- [ ] **Step 2: Add the private helpers (command builder, lock, initdb, spawn, readiness, ensure-db)**

Append to `lib.rs`:

```rust
/// Build a tokio `Command` for a pg binary with the shared-library search path.
fn pg_command(program: PathBuf, ld_library_path: &str) -> Command {
    let mut cmd = Command::new(program);
    if !ld_library_path.is_empty() {
        cmd.env("LD_LIBRARY_PATH", ld_library_path);
    }
    cmd
}

/// Acquire an exclusive, non-blocking advisory lock on `<data_dir>/loom-embedded.lock`.
/// The returned file must be kept alive for the lock to hold.
fn acquire_lock(data_dir: &Path) -> Result<std::fs::File, EmbeddedPgError> {
    let path = data_dir.join("loom-embedded.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)?;
    // SAFETY: flock on a valid open fd; LOCK_NB makes it return EWOULDBLOCK instead
    // of blocking when another loom already holds the lock.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        return Err(EmbeddedPgError::AlreadyLocked(data_dir.to_path_buf()));
    }
    Ok(file)
}

impl EmbeddedPg {
    async fn run_initdb(cfg: &EmbeddedPgConfig) -> Result<(), EmbeddedPgError> {
        let status = pg_command(cfg.bin_dir.join("initdb"), &cfg.ld_library_path)
            .arg("-D")
            .arg(&cfg.data_dir)
            .args(["--no-locale", "--encoding=UTF8", "-A", "trust", "-U", "postgres"])
            .status()
            .await?;
        if status.success() {
            Ok(())
        } else {
            Err(EmbeddedPgError::Initdb(status))
        }
    }

    fn spawn_postgres(cfg: &EmbeddedPgConfig) -> Result<Child, EmbeddedPgError> {
        // Unix socket only (no TCP). Durability stays ON (real data, not a test).
        let child = pg_command(cfg.bin_dir.join("postgres"), &cfg.ld_library_path)
            .arg("-D")
            .arg(&cfg.data_dir)
            .arg("-k")
            .arg(&cfg.socket_dir)
            .args(["-c", "listen_addresses="])
            .spawn()?;
        Ok(child)
    }

    /// Connect options for the `postgres` maintenance db over the socket.
    fn maintenance_opts(&self) -> PgConnectOptions {
        PgConnectOptions::new()
            .socket(&self.socket_dir)
            .username("postgres")
            .database("postgres")
    }

    async fn wait_ready(&self) -> Result<(), EmbeddedPgError> {
        let timeout = Duration::from_secs(15);
        for _ in 0..300 {
            if let Ok(mut conn) = self.maintenance_opts().connect().await {
                let _ = conn.close().await;
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(EmbeddedPgError::NotReady(timeout))
    }

    async fn ensure_database(&self) -> Result<(), EmbeddedPgError> {
        validate_db_name(&self.database)?;
        let mut conn = self
            .maintenance_opts()
            .connect()
            .await
            .map_err(EmbeddedPgError::Connect)?;
        let exists: bool = sqlx::query("select exists(select 1 from pg_database where datname = $1)")
            .bind(&self.database)
            .fetch_one(&mut conn)
            .await
            .map_err(EmbeddedPgError::CreateDatabase)?
            .get(0);
        if !exists {
            // db name is validated above; CREATE DATABASE cannot be parameterised.
            conn.execute(format!("create database {}", self.database).as_str())
                .await
                .map_err(EmbeddedPgError::CreateDatabase)?;
        }
        let _ = conn.close().await;
        Ok(())
    }
}
```

- [ ] **Step 3: Add `start`, `connect_options`, `socket_dir`, `shutdown`, and `Drop`**

Append to the `impl EmbeddedPg` block (or a second `impl`):

```rust
impl EmbeddedPg {
    /// Boot (or adopt) a persistent cluster and return a ready handle.
    pub async fn start(cfg: EmbeddedPgConfig) -> Result<EmbeddedPg, EmbeddedPgError> {
        // initdb/postgres reject uid 0; fail early with a clear message.
        // SAFETY: geteuid is always safe.
        if unsafe { libc::geteuid() } == 0 {
            return Err(EmbeddedPgError::RunningAsRoot);
        }
        validate_db_name(&cfg.database)?;
        std::fs::create_dir_all(&cfg.data_dir)?;
        std::fs::create_dir_all(&cfg.socket_dir)?;
        let lock = acquire_lock(&cfg.data_dir)?;

        // Idempotent init: PG_VERSION present ⇒ adopt the existing cluster.
        if !cfg.data_dir.join("PG_VERSION").exists() {
            Self::run_initdb(&cfg).await?;
        }
        let server = Self::spawn_postgres(&cfg)?;
        let pg = EmbeddedPg {
            server: Some(server),
            bin_dir: cfg.bin_dir,
            ld_library_path: cfg.ld_library_path,
            data_dir: cfg.data_dir,
            socket_dir: cfg.socket_dir,
            database: cfg.database,
            _lock: lock,
        };
        pg.wait_ready().await?;
        pg.ensure_database().await?;
        Ok(pg)
    }

    /// Connect options for the application database over the socket (feeds a `PgPool`).
    pub fn connect_options(&self) -> PgConnectOptions {
        PgConnectOptions::new()
            .socket(&self.socket_dir)
            .username("postgres")
            .database(&self.database)
    }

    /// The unix-socket directory the server listens on.
    pub fn socket_dir(&self) -> &Path {
        &self.socket_dir
    }

    /// Clean shutdown: `pg_ctl stop -m fast`, then release the lock (on `Drop`).
    pub async fn shutdown(mut self) -> Result<(), EmbeddedPgError> {
        // Take the child so Drop does not also try to signal it.
        let _ = self.server.take();
        let status = pg_command(self.bin_dir.join("pg_ctl"), &self.ld_library_path)
            .arg("stop")
            .arg("-D")
            .arg(&self.data_dir)
            .args(["-m", "fast", "-w"])
            .status()
            .await?;
        if status.success() {
            Ok(())
        } else {
            Err(EmbeddedPgError::Stop(status))
        }
    }
}

impl Drop for EmbeddedPg {
    fn drop(&mut self) {
        // Best-effort fast stop if shutdown() was not called: SIGINT to the
        // postmaster is Postgres' "fast shutdown". Ignore all errors.
        if let Some(child) = self.server.take()
            && let Some(pid) = child.id()
        {
            // SAFETY: kill with a known pid + signal; failure is ignored.
            unsafe { libc::kill(pid as i32, libc::SIGINT) };
        }
    }
}
```

- [ ] **Step 4: Append the `loom_fixture_test` target to BUCK**

Add to `src/services/managed-postgres/BUCK`:

```python
loom_fixture_test(
    name = "embedded-lifecycle",
    crate = "embedded_lifecycle",
    srcs = ["tests/embedded_lifecycle.rs"],
    crate_root = "tests/embedded_lifecycle.rs",
    deps = [
        ":managed-postgres",
        "//src/control-plane/postgres:postgres",
        "//third-party:sqlx",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 5: Write the failing fixture test**

Create `src/services/managed-postgres/tests/embedded_lifecycle.rs`:

```rust
//! Proves slice 1's contract: idempotent init, restart-adoption, data
//! persistence, and clean shutdown. Uses the buck `:postgres-bin` via the
//! fixture env (POSTGRES_BIN_DIR / POSTGRES_LD_LIBRARY_PATH / LOOM_MIGRATIONS_DIR).

use std::path::PathBuf;

use managed_postgres::{EmbeddedPg, EmbeddedPgConfig};
use sqlx::postgres::PgPoolOptions;

fn cfg(data: &std::path::Path, sock: &std::path::Path) -> EmbeddedPgConfig {
    EmbeddedPgConfig {
        bin_dir: PathBuf::from(std::env::var("POSTGRES_BIN_DIR").expect("POSTGRES_BIN_DIR")),
        ld_library_path: std::env::var("POSTGRES_LD_LIBRARY_PATH").unwrap_or_default(),
        data_dir: data.to_path_buf(),
        socket_dir: sock.to_path_buf(),
        database: "loom".to_string(),
    }
}

#[tokio::test]
async fn embedded_pg_is_idempotent_and_persistent() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data = tmp.path().join("pgdata");
    let sock = tmp.path().join("pgrun");
    let migrations = PathBuf::from(std::env::var("LOOM_MIGRATIONS_DIR").expect("LOOM_MIGRATIONS_DIR"));

    // 1. Fresh start → initdb ran, db created, migrations applied, write a sentinel.
    let pg = EmbeddedPg::start(cfg(&data, &sock)).await.expect("first start");
    assert!(data.join("PG_VERSION").exists(), "initdb ran");
    let pool = PgPoolOptions::new()
        .connect_with(pg.connect_options())
        .await
        .expect("connect 1");
    control_plane_postgres::run_migrations(&pool, &migrations)
        .await
        .expect("migrate 1");
    let count1: i64 = sqlx::query_scalar("select count(*) from _sqlx_migrations")
        .fetch_one(&pool)
        .await
        .expect("count migrations 1");
    assert!(count1 > 0, "migrations were applied");
    sqlx::query("insert into acl.subject (id) values ('sentinel')")
        .execute(&pool)
        .await
        .expect("insert sentinel");
    pool.close().await;
    pg.shutdown().await.expect("first shutdown");

    // 2. Restart on the SAME dir → adopt (no re-initdb), zero new migrations, data survives.
    let pg = EmbeddedPg::start(cfg(&data, &sock)).await.expect("second start");
    let pool = PgPoolOptions::new()
        .connect_with(pg.connect_options())
        .await
        .expect("connect 2");
    control_plane_postgres::run_migrations(&pool, &migrations)
        .await
        .expect("migrate 2");
    let count2: i64 = sqlx::query_scalar("select count(*) from _sqlx_migrations")
        .fetch_one(&pool)
        .await
        .expect("count migrations 2");
    assert_eq!(count1, count2, "no new migrations applied on restart");
    let sentinel: i64 = sqlx::query_scalar("select count(*) from acl.subject where id = 'sentinel'")
        .fetch_one(&pool)
        .await
        .expect("read sentinel");
    assert_eq!(sentinel, 1, "data survived restart");
    pool.close().await;
    pg.shutdown().await.expect("second shutdown");
}
```

- [ ] **Step 6: Run the lifecycle test, expect a pass**

Run: `buck2 test //src/services/managed-postgres:embedded-lifecycle > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS|error\\[" /tmp/t.log`
Expected: `Tests finished: PASS`.

If buck2 cannot start locally (file-watcher caveat), run it via the materialized toolchain + fixture env (the same path the repo's sqlx-prepare uses): export `POSTGRES_BIN_DIR`, `POSTGRES_LD_LIBRARY_PATH`, `LOOM_MIGRATIONS_DIR` from the `:postgres-bin`/`:libxml2`/`:migrations` outputs and `cargo test` the crate — but since it is buck-only, prefer landing the PR and confirming the `affected` CI job is green. State which path was used in the completion note.

- [ ] **Step 7: Commit**

```bash
git add src/services/managed-postgres/
git commit -m "feat(managed-postgres): EmbeddedPg lifecycle with idempotent persistent cluster"
```

---

### Task 3: `service_runtime` embedded seam (`build_pool_managed`)

Wire embedded mode into the runtime so a service opts in with `LOOM_PG_MODE=embedded`, leaving external Postgres the untouched default.

**Files:**
- Modify: `src/services/runtime/src/lib.rs`
- Modify: `src/services/runtime/BUCK`
- Create: `src/services/runtime/tests/embedded_config.rs`

**Interfaces:**
- Consumes: `managed_postgres::{EmbeddedPg, EmbeddedPgConfig, EmbeddedPgError}` (Task 2); existing `Config`, `DbConfig`, `build_pool`, `RuntimeError`, `control_plane_postgres::run_migrations`.
- Produces: `service_runtime::EmbeddedSettings { cfg: EmbeddedPgConfig, migrations_dir: PathBuf }`; `Config.embedded: Option<EmbeddedSettings>`; `async fn build_pool_managed(cfg: &Config) -> Result<(PgPool, Option<EmbeddedPg>), RuntimeError>`.

- [ ] **Step 1: Add the dep to `src/services/runtime/BUCK`**

In the `runtime` library target's `deps`, add (next to the existing `"//src/services/store-config:store-config"`):

```python
        "//src/services/managed-postgres:managed-postgres",
```

- [ ] **Step 2: Write the failing config-parse test**

Create `src/services/runtime/tests/embedded_config.rs`:

```rust
use std::collections::HashMap;

use service_runtime::Config;

fn base() -> HashMap<String, String> {
    // The minimal external-mode keys Config::from_map requires.
    [
        ("LOOM_BIND_ADDR", "127.0.0.1:8080"),
        ("LOOM_DB_HOST", "/var/run/pg"),
        ("LOOM_DB_PORT", "5432"),
        ("LOOM_DB_USER", "postgres"),
        ("LOOM_DB_PASSWORD", ""),
        ("LOOM_DB_NAME", "loom"),
        ("LOOM_DATA_PATH", "/tmp/loomdata"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

#[test]
fn external_mode_has_no_embedded_settings() {
    let cfg = Config::from_map(&base()).expect("parse external");
    assert!(cfg.embedded.is_none());
}

#[test]
fn embedded_mode_derives_pgdata_and_socket_under_data_path() {
    let mut vars = base();
    vars.insert("LOOM_PG_MODE".into(), "embedded".into());
    vars.insert("LOOM_PG_BIN_DIR".into(), "/opt/pg/bin".into());
    vars.insert("LOOM_MIGRATIONS_DIR".into(), "/opt/loom/migrations".into());
    let cfg = Config::from_map(&vars).expect("parse embedded");
    let e = cfg.embedded.expect("embedded settings present");
    assert_eq!(e.cfg.bin_dir, std::path::PathBuf::from("/opt/pg/bin"));
    assert_eq!(e.cfg.data_dir, std::path::PathBuf::from("/tmp/loomdata/pgdata"));
    assert_eq!(e.cfg.socket_dir, std::path::PathBuf::from("/tmp/loomdata/pgrun"));
    assert_eq!(e.cfg.database, "loom");
    assert_eq!(e.migrations_dir, std::path::PathBuf::from("/opt/loom/migrations"));
}
```

Add the target to `src/services/runtime/BUCK` (mirroring the existing `config` test target):

```python
rust_test(
    name = "embedded-config",
    crate = "embedded_config",
    srcs = ["tests/embedded_config.rs"],
    crate_root = "tests/embedded_config.rs",
    edition = "2024",
    deps = [":runtime"],
)
```

- [ ] **Step 3: Run the test, expect a compile failure**

Run: `buck2 test //src/services/runtime:embedded-config > /tmp/t.log 2>&1; grep -E "error\\[|cannot find|FAIL|no field" /tmp/t.log`
Expected: FAIL — `no field 'embedded' on type 'Config'`.

- [ ] **Step 4: Add `EmbeddedSettings`, the `Config.embedded` field, and parsing**

In `src/services/runtime/src/lib.rs`, add the struct near `DbConfig`:

```rust
/// Embedded-Postgres settings, present only when `LOOM_PG_MODE=embedded`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbeddedSettings {
    pub cfg: managed_postgres::EmbeddedPgConfig,
    /// Directory of `.sql` migrations applied after the cluster is ready.
    pub migrations_dir: PathBuf,
}
```

Add the field to `Config` (after `gc_retention`):

```rust
    /// Present when running an embedded (loom-managed) Postgres cluster.
    pub embedded: Option<EmbeddedSettings>,
```

In `Config::from_map`, after `data_path` is computed and before the `Ok(Config { ... })`, add:

```rust
        let embedded = if vars.get("LOOM_PG_MODE").map(String::as_str) == Some("embedded") {
            let bin_dir = PathBuf::from(req("LOOM_PG_BIN_DIR")?);
            let migrations_dir = PathBuf::from(req("LOOM_MIGRATIONS_DIR")?);
            Some(EmbeddedSettings {
                cfg: managed_postgres::EmbeddedPgConfig {
                    bin_dir,
                    ld_library_path: vars
                        .get("LOOM_PG_LD_LIBRARY_PATH")
                        .cloned()
                        .unwrap_or_default(),
                    data_dir: data_path.join("pgdata"),
                    socket_dir: data_path.join("pgrun"),
                    database: req("LOOM_DB_NAME")?,
                },
                migrations_dir,
            })
        } else {
            None
        };
```

Add `embedded,` to the `Ok(Config { ... })` literal.

- [ ] **Step 5: Run the config test, expect a pass**

Run: `buck2 test //src/services/runtime:embedded-config > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`
Expected: `Tests finished: PASS`.

- [ ] **Step 6: Add `RuntimeError` variants and `build_pool_managed`**

Add variants to the `RuntimeError` enum:

```rust
    #[error("embedded postgres: {0}")]
    Embedded(managed_postgres::EmbeddedPgError),
    #[error("migrate: {0}")]
    Migrate(control_plane_core::ControlPlaneError),
```

Add the function near `build_pool`:

```rust
/// Build a control-plane pool, owning an embedded Postgres cluster when configured.
/// External mode is identical to `build_pool` and returns `None`. In embedded mode
/// the returned `EmbeddedPg` must be kept alive for the process lifetime and
/// `shutdown()` on exit.
pub async fn build_pool_managed(
    cfg: &Config,
) -> Result<(PgPool, Option<managed_postgres::EmbeddedPg>), RuntimeError> {
    match &cfg.embedded {
        None => Ok((build_pool(&cfg.db).await?, None)),
        Some(e) => {
            let pg = managed_postgres::EmbeddedPg::start(e.cfg.clone())
                .await
                .map_err(RuntimeError::Embedded)?;
            let mut opts = PgPoolOptions::new();
            if let Some(n) = cfg.db.max_connections {
                opts = opts.max_connections(n);
            }
            let pool = opts
                .connect_with(pg.connect_options())
                .await
                .map_err(RuntimeError::Pool)?;
            control_plane_postgres::run_migrations(&pool, &e.migrations_dir)
                .await
                .map_err(RuntimeError::Migrate)?;
            Ok((pool, Some(pg)))
        }
    }
}
```

Confirm the imports at the top of `lib.rs` cover `control_plane_core::ControlPlaneError` and `control_plane_postgres::run_migrations` (add `use` lines if absent — `control_plane_postgres` is already a dep; `run_migrations` is a free function, so `control_plane_postgres::run_migrations` works fully-qualified).

- [ ] **Step 7: Build the runtime crate, expect success**

Run: `buck2 build //src/services/runtime:runtime > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\\[|error:" /tmp/b.log || echo OK`
Expected: builds clean (no `error[...]`).

- [ ] **Step 8: Commit**

```bash
git add src/services/runtime/
git commit -m "feat(runtime): build_pool_managed seam for embedded Postgres (LOOM_PG_MODE=embedded)"
```

---

### Task 4: Full-suite gate, clippy, and register update

**Files:**
- Modify: `docs/ROADMAP.md` (add the single-binary arc item), `docs/FUTURE.md` (PG-version-upgrade gap), and the spec's slice scope bullets.

- [ ] **Step 1: Run the full first-party suite**

Run: `buck2 test //src/... > /tmp/full.log 2>&1; grep -E "Tests finished|FAIL" /tmp/full.log`
Expected: `Tests finished` with no `FAIL`. (Confirms no regression in the runtime/control-plane crates from the new field + seam.) If buck2 cannot start (file-watcher caveat), say so and rely on the PR's CI `affected` job.

- [ ] **Step 2: Lint (clippy + format) the touched crates**

Run: `./tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -5 /tmp/clippy.log`
Expected: clean. Then `buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; tail -5 /tmp/prek.log` and commit any hook fixes.

- [ ] **Step 3: Update the documentation registers**

Use the `loom-docs-update` skill. Add a ROADMAP item for the single-binary arc (`area:` deploy or runtime) referencing `spec:2026-06-28-embedded-postgres-lifecycle` with `[[links]]` to the deferred slices 2/3; record the PG-version-upgrade gap in FUTURE. Sync the spec's slice-1/slice-2 scope bullets to match the migration deviation noted above.

- [ ] **Step 4: Commit**

```bash
git add docs/
git commit -m "docs(registers): track embedded-Postgres single-binary arc (slice 1 done)"
```

---

## Self-Review

**Spec coverage:**
- "new crate `managed-postgres` exposing `EmbeddedPg`" → Tasks 1–2. ✓
- "`service_runtime` seam selecting external vs embedded" → Task 3 (`Config.embedded` + `build_pool_managed`). ✓
- Lifecycle behaviours (root refusal, idempotent init, single-owner flock, start+wait-ready, clean shutdown, crash-recovery via released lock) → Task 2 `start`/`acquire_lock`/`wait_ready`/`shutdown`/`Drop`. ✓
- "embedded migration runner / no migrations-on-disk" → **intentionally deferred to slice 2** (see Deviation); slice 1 uses `run_migrations(dir)`. Spec scope bullet to be synced in Task 4. ✓ (documented gap, not silent)
- Persistent data layout under `LOOM_DATA_PATH` (`pgdata`/`pgrun`) → Task 3 parsing. ✓
- Fixture-backed idempotency/restart/persistence test → Task 2 Step 5. ✓
- Root-refusal as typed error → `EmbeddedPgError::RunningAsRoot`, returned first in `start`. ✓
- Risks (PG version skew, macOS socket-path length, latency, footprint) → carried in the spec; version-skew detection is a slice-1 nicety not yet implemented — **noted as a follow-up in Task 4's FUTURE entry** rather than silently dropped.

**Placeholder scan:** No TBD/TODO; every code step shows complete code; commands have expected output. ✓

**Type consistency:** `EmbeddedPgConfig`/`EmbeddedPgError`/`EmbeddedPg`/`validate_db_name` names match across Tasks 1–3; `build_pool_managed` return type `(PgPool, Option<EmbeddedPg>)` matches its consumers; `EmbeddedSettings { cfg, migrations_dir }` field names match the parse and the test. ✓

**Note on version-skew:** `EmbeddedPgError` does not include the `VersionSkew` variant from the spec sketch — slice 1 does not yet read/compare `PG_VERSION` contents (it only checks existence). This is deliberate scope-trimming; the upgrade story is the FUTURE item in Task 4. If you want the early typed error in slice 1, add a `PG_VERSION`-content check in `start` before `spawn_postgres`.
