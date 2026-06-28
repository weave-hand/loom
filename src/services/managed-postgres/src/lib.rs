//! Embedded Postgres lifecycle: boot (or adopt) a persistent cluster on a unix
//! socket and hand back sqlx connect options, so loom can run with no external
//! Postgres server. Promotes the test fixture's ephemeral-cluster logic to a
//! persistent runtime. `initdb`/`postgres` refuse to run as root by design.

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

/// A running, owned embedded Postgres. Prefer `shutdown()` for a clean
/// `pg_ctl stop -m fast`; on `Drop` the child is killed (`kill_on_drop`) so a
/// panic cannot leak a postmaster.
pub struct EmbeddedPg {
    server: Option<Child>,
    bin_dir: PathBuf,
    ld_library_path: String,
    data_dir: PathBuf,
    socket_dir: PathBuf,
    database: String,
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

impl EmbeddedPg {
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
        // kill_on_drop: if the handle is dropped without shutdown() (e.g. a panic),
        // tokio SIGKILLs the child so no postmaster is leaked; the durable data dir
        // recovers on next start.
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

    async fn wait_ready(&self) -> Result<(), EmbeddedPgError> {
        let timeout = Duration::from_secs(15);
        for _ in 0..300 {
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
        std::fs::create_dir_all(&cfg.data_dir)?;
        std::fs::create_dir_all(&cfg.socket_dir)?;
        // Single-owner guard against an already-running cluster on this data dir.
        check_not_already_running(&cfg.data_dir)?;

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
