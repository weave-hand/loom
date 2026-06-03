//! Hermetic Postgres test fixture.
//!
//! Boots an ephemeral cluster from a buck2-provided server binary (located via
//! the `POSTGRES_BIN_DIR` env var, which the `rust_test` rule sets from the
//! `postgres-bin` http_archive) — one cluster per test process, listening on a
//! unix socket in a tempdir, torn down on `Drop`. No Docker, no external
//! database. `fresh_control_plane` hands back a [`PgControlPlane`] bound to a
//! brand-new database (with the temporary `_probe` table) for each test.
//!
//! This is test-support that happens to live in the library so other crates'
//! integration tests can reuse it; it is not for production use.

use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection};
use tempfile::TempDir;

use crate::PgControlPlane;

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A running ephemeral Postgres cluster. Killed and cleaned up on drop.
pub struct PgFixture {
    // Dropped last; keeps the data/socket dirs alive while the server runs.
    _data_dir: TempDir,
    socket_dir: TempDir,
    server: Child,
    bin: PathBuf,
    ld_library_path: String,
}

/// Build a `Command` for a pg binary, applying the shared-library search path so
/// the binaries find the dist libs and the bundled libxml2.
fn pg_command(program: PathBuf, ld_library_path: &str) -> Command {
    let mut cmd = Command::new(program);
    if !ld_library_path.is_empty() {
        cmd.env("LD_LIBRARY_PATH", ld_library_path);
    }
    cmd
}

impl PgFixture {
    /// Boot an ephemeral cluster on a unix socket. Requires `POSTGRES_BIN_DIR` to
    /// point at the server's `bin/` directory; honours `POSTGRES_LD_LIBRARY_PATH`
    /// for the spawned binaries' shared-library search path (the dist libs + the
    /// bundled libxml2). Panics on failure — test-only.
    pub fn start() -> Self {
        let bin = PathBuf::from(
            std::env::var("POSTGRES_BIN_DIR")
                .expect("POSTGRES_BIN_DIR must point at the postgres bin/ directory"),
        );
        let ld_library_path = std::env::var("POSTGRES_LD_LIBRARY_PATH").unwrap_or_default();
        let data_dir = tempfile::tempdir().expect("create data tempdir");
        let socket_dir = tempfile::tempdir().expect("create socket tempdir");

        let initdb_ok = pg_command(bin.join("initdb"), &ld_library_path)
            .arg("-D")
            .arg(data_dir.path())
            .args([
                "--no-locale",
                "--encoding=UTF8",
                "-A",
                "trust",
                "-U",
                "postgres",
            ])
            .status()
            .expect("run initdb")
            .success();
        assert!(initdb_ok, "initdb failed");

        let server = pg_command(bin.join("postgres"), &ld_library_path)
            .arg("-D")
            .arg(data_dir.path())
            .arg("-k")
            .arg(socket_dir.path())
            // unix socket only (no TCP), and disable durability for test speed.
            .args(["-c", "listen_addresses="])
            .args(["-c", "fsync=off"])
            .args(["-c", "full_page_writes=off"])
            .spawn()
            .expect("spawn postgres");

        let fixture = Self {
            _data_dir: data_dir,
            socket_dir,
            server,
            bin,
            ld_library_path,
        };
        fixture.wait_ready();
        fixture
    }

    fn wait_ready(&self) {
        // pg_isready against the socket dir; poll up to ~15s.
        for _ in 0..300 {
            let ready = pg_command(self.bin.join("pg_isready"), &self.ld_library_path)
                .arg("-h")
                .arg(self.socket_dir.path())
                .args(["-U", "postgres", "-q"])
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
            if ready {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("postgres did not become ready within ~15s");
    }

    fn opts(&self, db: &str) -> PgConnectOptions {
        // libpq/sqlx treat the socket dir as the host for a unix connection.
        PgConnectOptions::new()
            .socket(self.socket_dir.path())
            .username("postgres")
            .database(db)
    }

    /// Create a fresh, empty database (with the `_probe` table) and return a
    /// `PgControlPlane` connected to it. Isolated per call, so each test gets a
    /// clean slate.
    pub async fn fresh_control_plane(&self) -> PgControlPlane {
        let n = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        let db = format!("loom_test_{}_{}", std::process::id(), n);

        let mut admin = PgConnection::connect_with(&self.opts("postgres"))
            .await
            .expect("connect to admin database");
        admin
            .execute(format!("create database {db}").as_str())
            .await
            .expect("create database");
        admin.close().await.ok();

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(self.opts(&db))
            .await
            .expect("connect pool to fresh database");
        pool.execute("create table _probe (k text primary key, v bigint not null)")
            .await
            .expect("create _probe table");

        PgControlPlane::new(pool)
    }
}

impl Drop for PgFixture {
    fn drop(&mut self) {
        let _ = self.server.kill();
        let _ = self.server.wait();
        // TempDirs remove themselves on drop.
    }
}
