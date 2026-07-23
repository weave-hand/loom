//! Embedded Postgres lifecycle: boot (or adopt) a persistent cluster on a unix
//! socket and hand back sqlx connect options, so loom can run with no external
//! Postgres server. Promotes the test fixture's ephemeral-cluster logic to a
//! persistent runtime. `initdb`/`postgres` refuse to run as root by design.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use sqlx::postgres::PgConnectOptions;
use sqlx::{AssertSqlSafe, ConnectOptions, Connection, Executor, Row};
use tokio::process::{Child, Command};

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
    #[error(
        "embedded Postgres cannot start: missing shared library {lib}. loom bundles \
         only its own Postgres artifacts, not system libraries — install it on the host \
         (see the Host prerequisites section of docs/deploy.md)"
    )]
    MissingSharedLibrary { lib: String },
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
    #[error("postgres exited during startup: {0}")]
    ServerExited(std::process::ExitStatus),
    #[error("connect: {0}")]
    Connect(sqlx::Error),
    #[error("create database: {0}")]
    CreateDatabase(sqlx::Error),
    #[error("pg_ctl stop failed: exit {0}")]
    Stop(std::process::ExitStatus),
    #[error(
        "embedded Postgres data dir is PostgreSQL {data_major} but the binary is \
         PostgreSQL {binary_major}; automated major-version upgrade of an existing \
         data dir is not supported — back up and re-initialise, or migrate the \
         cluster manually with pg_upgrade (see docs/deploy.md)"
    )]
    VersionMismatch { data_major: u32, binary_major: u32 },
}

/// Extract the missing library name from a dynamic-loader failure line, if the
/// stderr contains one. Matches the glibc loader message
/// `error while loading shared libraries: <lib>: cannot open shared object file`.
/// Pure so it is unit-testable without a host that actually lacks the library.
#[must_use]
pub fn classify_loader_error(stderr: &str) -> Option<String> {
    const MARKER: &str = "error while loading shared libraries: ";
    let start = stderr.find(MARKER)? + MARKER.len();
    let lib = stderr.get(start..)?.split([':', '\n']).next()?.trim();
    if lib.is_empty() {
        None
    } else {
        Some(lib.to_string())
    }
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

/// The Postgres *major* version a data dir was created by, from its `PG_VERSION`
/// file. PG 10+ writes just the major (e.g. `17`); pre-10 wrote `9.6`, whose
/// compatibility major is the first segment. Pure so it is unit-testable.
#[must_use]
pub fn pg_version_major(pg_version_file: &str) -> Option<u32> {
    let first_line = pg_version_file.lines().next()?.trim();
    first_line.split('.').next()?.parse().ok()
}

/// The Postgres *major* version of a `postgres --version` banner, e.g.
/// `postgres (PostgreSQL) 17.4` → `17`. Takes the first whitespace token whose
/// leading run is ASCII digits. Pure so it is unit-testable.
#[must_use]
pub fn parse_binary_major(version_output: &str) -> Option<u32> {
    version_output.split_whitespace().find_map(|token| {
        let digits: String = token.chars().take_while(char::is_ascii_digit).collect();
        digits.parse().ok()
    })
}

/// A running, owned embedded Postgres. Prefer `shutdown()` for a clean
/// `pg_ctl stop -m fast`; an abnormal `Drop` (panic / early-return during
/// `start`) stops the postmaster gracefully (`pg_ctl stop -m immediate`, which
/// reaps backends), falling back to SIGKILL (`kill_on_drop`) if that fails, so
/// a panic cannot leak a postmaster.
pub struct EmbeddedPg {
    server: Option<Child>,
    bin_dir: PathBuf,
    ld_library_path: String,
    data_dir: PathBuf,
    socket_dir: PathBuf,
    database: String,
    /// Held for the cluster's life; its `Drop` releases the on-disk owner lock.
    #[allow(dead_code, reason = "RAII guard — only its Drop matters")]
    _owner_lock: OwnerLock,
}

/// Build a tokio `Command` for a pg binary with the shared-library search path.
fn pg_command(program: PathBuf, ld_library_path: &str) -> Command {
    let mut cmd = Command::new(program);
    if !ld_library_path.is_empty() {
        cmd.env("LD_LIBRARY_PATH", ld_library_path);
    }
    cmd
}

/// Effective-uid root check, FFI-free. On Linux the effective uid is the 2nd
/// field of the `Uid:` line in `/proc/self/status`. On platforms without `/proc`
/// (macOS) this returns `false` and we fall back to `initdb`'s own root refusal.
fn is_effective_root() -> bool {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return false;
    };
    status
        .lines()
        .find_map(|l| l.strip_prefix("Uid:"))
        .and_then(|rest| rest.split_whitespace().nth(1)) // real *effective* saved fs
        .is_some_and(|euid| euid == "0")
}

/// Reject a second owner of the same data dir, FFI-free. Postgres writes its pid
/// on the first line of `<data_dir>/postmaster.pid`; if that pid is still alive
/// (`/proc/<pid>` exists on Linux), another server already owns the cluster. A
/// stale pidfile (process gone) is left for Postgres to clean on start.
fn check_not_already_running(data_dir: &Path) -> Result<(), EmbeddedPgError> {
    let Ok(contents) = std::fs::read_to_string(data_dir.join("postmaster.pid")) else {
        return Ok(()); // no pidfile ⇒ not running
    };
    let alive = contents
        .lines()
        .next()
        .and_then(|l| l.trim().parse::<u32>().ok())
        .is_some_and(|pid| Path::new(&format!("/proc/{pid}")).exists());
    if alive {
        Err(EmbeddedPgError::AlreadyLocked(data_dir.to_path_buf()))
    } else {
        Ok(())
    }
}

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

impl EmbeddedPg {
    /// Run `postgres -V` and convert a dynamic-loader failure into
    /// `MissingSharedLibrary`. Any other failure (or an I/O error spawning the
    /// probe) is swallowed so the normal init/spawn path reproduces today's error.
    async fn preflight_shared_libs(cfg: &EmbeddedPgConfig) -> Result<(), EmbeddedPgError> {
        let out = pg_command(cfg.bin_dir.join("postgres"), &cfg.ld_library_path)
            .arg("-V")
            .output()
            .await?;
        if out.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&out.stderr);
        if let Some(lib) = classify_loader_error(&stderr) {
            return Err(EmbeddedPgError::MissingSharedLibrary { lib });
        }
        Ok(())
    }

    /// On adopt, refuse a Postgres *major*-version skew between the existing data
    /// dir and the binary that would run it — a mismatch otherwise crashes the
    /// postmaster cryptically. Fail-open: if either major is unparseable we do not
    /// fabricate a mismatch (a false positive would brick a working deployment),
    /// leaving today's behaviour. Major-only: same major ⇒ compatible on-disk format.
    async fn check_version_compatibility(cfg: &EmbeddedPgConfig) -> Result<(), EmbeddedPgError> {
        let raw = std::fs::read_to_string(cfg.data_dir.join("PG_VERSION"))?;
        let Some(data_major) = pg_version_major(&raw) else {
            return Ok(());
        };
        let out = pg_command(cfg.bin_dir.join("postgres"), &cfg.ld_library_path)
            .arg("--version")
            .output()
            .await?;
        let banner = String::from_utf8_lossy(&out.stdout);
        let Some(binary_major) = parse_binary_major(&banner) else {
            return Ok(());
        };
        if data_major == binary_major {
            Ok(())
        } else {
            Err(EmbeddedPgError::VersionMismatch { data_major, binary_major })
        }
    }

    async fn run_initdb(cfg: &EmbeddedPgConfig) -> Result<(), EmbeddedPgError> {
        let out = pg_command(cfg.bin_dir.join("initdb"), &cfg.ld_library_path)
            .arg("-D")
            .arg(&cfg.data_dir)
            .args([
                "--no-locale",
                "--encoding=UTF8",
                "-A",
                "trust",
                "-U",
                "postgres",
            ])
            .output()
            .await?;
        if out.status.success() {
            return Ok(());
        }
        // Fallback root detection for platforms where is_effective_root() couldn't
        // tell (no /proc): initdb refuses root with a message naming "root".
        let stderr = String::from_utf8_lossy(&out.stderr);
        if stderr.contains("root") {
            return Err(EmbeddedPgError::RunningAsRoot);
        }
        Err(EmbeddedPgError::Initdb(out.status))
    }

    fn spawn_postgres(cfg: &EmbeddedPgConfig) -> Result<Child, EmbeddedPgError> {
        // Unix socket only (no TCP). Durability stays ON (real data, not a test).
        // kill_on_drop is the *fallback* leak-guard: `EmbeddedPg::Drop` first asks
        // pg_ctl for a graceful immediate stop (reaping backends); only if that
        // fails does tokio SIGKILL the postmaster on the child's drop. The durable
        // data dir recovers on next start either way.
        let child = pg_command(cfg.bin_dir.join("postgres"), &cfg.ld_library_path)
            .arg("-D")
            .arg(&cfg.data_dir)
            .arg("-k")
            .arg(&cfg.socket_dir)
            .args(["-c", "listen_addresses="])
            .kill_on_drop(true)
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

    async fn ensure_database(&self) -> Result<(), EmbeddedPgError> {
        validate_db_name(&self.database)?;
        let mut conn = self
            .maintenance_opts()
            .connect()
            .await
            .map_err(EmbeddedPgError::Connect)?;
        let exists: bool =
            sqlx::query("select exists(select 1 from pg_database where datname = $1)")
                .bind(&self.database)
                .fetch_one(&mut conn)
                .await
                .map_err(EmbeddedPgError::CreateDatabase)?
                .get(0);
        if !exists {
            // db name is validated above; CREATE DATABASE cannot be parameterised.
            conn.execute(AssertSqlSafe(format!("create database {}", self.database)))
                .await
                .map_err(EmbeddedPgError::CreateDatabase)?;
        }
        drop(conn.close().await);
        Ok(())
    }
}

impl EmbeddedPg {
    /// Boot (or adopt) a persistent cluster and return a ready handle.
    pub async fn start(cfg: EmbeddedPgConfig) -> Result<EmbeddedPg, EmbeddedPgError> {
        // initdb/postgres reject uid 0; fail early with a clear message (Linux).
        // Non-/proc platforms fall through to run_initdb's stderr-based detection.
        if is_effective_root() {
            return Err(EmbeddedPgError::RunningAsRoot);
        }
        validate_db_name(&cfg.database)?;
        // Cheap, side-effect-free preflight: a dynamic-loader failure (e.g. a
        // host missing libxml2.so.2) becomes a named error pointing at the deploy
        // docs, instead of a cryptic Initdb/ServerExited status later. Non-loader
        // failures fall through so the real init/spawn path surfaces its usual error.
        Self::preflight_shared_libs(&cfg).await?;
        ensure_dir_secure(&cfg.data_dir)?;
        ensure_dir_secure(&cfg.socket_dir)?;
        // Single-owner lock (covers the fresh-init race the pidfile check misses).
        // Acquired before initdb/spawn; the guard frees the lockfile on any
        // early-return below or when the handle is dropped.
        let owner_lock = acquire_owner_lock(&cfg.data_dir)?;
        // Single-owner guard against an already-running cluster on this data dir.
        check_not_already_running(&cfg.data_dir)?;

        // Idempotent init: PG_VERSION present ⇒ adopt the existing cluster.
        if cfg.data_dir.join("PG_VERSION").exists() {
            // Adopting: refuse a major-version skew before spawn (clear error, no
            // silent data loss). Automated pg_upgrade is out of scope.
            Self::check_version_compatibility(&cfg).await?;
        } else {
            Self::run_initdb(&cfg).await?;
        }
        let server = Self::spawn_postgres(&cfg)?;
        let mut pg = EmbeddedPg {
            server: Some(server),
            bin_dir: cfg.bin_dir,
            ld_library_path: cfg.ld_library_path,
            data_dir: cfg.data_dir,
            socket_dir: cfg.socket_dir,
            database: cfg.database,
            _owner_lock: owner_lock,
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

    /// Clean shutdown: `pg_ctl stop -m fast` (waits for the server to exit).
    pub async fn shutdown(mut self) -> Result<(), EmbeddedPgError> {
        let status = pg_command(self.bin_dir.join("pg_ctl"), &self.ld_library_path)
            .arg("stop")
            .arg("-D")
            .arg(&self.data_dir)
            .args(["-m", "fast", "-w"])
            .status()
            .await?;
        // The server has exited; dropping the child handle now is a no-op for
        // kill_on_drop (the process is already gone).
        drop(self.server.take());
        if status.success() {
            Ok(())
        } else {
            Err(EmbeddedPgError::Stop(status))
        }
    }
}

impl std::fmt::Debug for EmbeddedPg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddedPg")
            .field("bin_dir", &self.bin_dir)
            .field("data_dir", &self.data_dir)
            .field("socket_dir", &self.socket_dir)
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

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
