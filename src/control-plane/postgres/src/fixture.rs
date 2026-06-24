//! Hermetic Postgres test fixture.
//!
//! Boots an ephemeral cluster from a buck2-provided server binary (located via
//! the `POSTGRES_BIN_DIR` env var, which the `rust_test` rule sets from the
//! `postgres-bin` http_archive) — one cluster per test process, listening on a
//! unix socket in a tempdir, torn down on `Drop`. No Docker, no external
//! database. `fresh_control_plane` hands back a [`PgControlPlane`] bound to a
//! brand-new database for each test.
//!
//! This is test-support that happens to live in the library so other crates'
//! integration tests can reuse it; it is not for production use.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, Connection, Executor, PgConnection};
use tempfile::TempDir;

use crate::PgControlPlane;

// --- Iceberg seeder imports (used by `IcebergWriter` below) ---
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder as _, NamespaceIdent, TableCreation, TableIdent};
use sqlx::PgPool;

use crate::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};

static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Bounds the number of concurrently-alive fixture Postgres clusters across all
/// test processes/threads. `slots` marker files live under `dir`; an acquired
/// slot is an exclusive `flock` held for the cluster's lifetime. Test-support.
///
/// A full `buck2 test //src/...` sweep otherwise boots every `loom_fixture_test`
/// target's cluster in one window (dozens of live `postgres` processes, each
/// reserving SysV semaphore sets) and blows past the kernel's `SEMMNI` limit, so
/// `initdb` fails en masse (`iss-fixture-boot-contention`). Capping live clusters
/// keeps total semaphore sets far under the limit.
pub struct BootThrottle {
    dir: PathBuf,
    slots: usize,
}

/// An acquired boot slot. The `flock` is released when this `File` drops (or the
/// process exits), freeing the slot for another cluster.
pub struct SlotGuard {
    _file: File,
}

impl BootThrottle {
    /// Create a throttle of `slots` (min 1) marker files under `dir`.
    pub fn new(dir: PathBuf, slots: usize) -> Self {
        std::fs::create_dir_all(&dir).expect("create fixture slot dir");
        Self {
            dir,
            slots: slots.max(1),
        }
    }

    /// Block until a slot is free, then return a guard holding it. Polls each of
    /// the `K` marker files with a non-blocking exclusive `flock`; sleeps briefly
    /// when all are busy. Panics after a generous deadline (test-only) so a wedged
    /// suite fails loudly instead of hanging forever.
    ///
    /// Each attempt opens its **own** fd (own open file description), so two
    /// threads of the same process contend correctly — `flock` via distinct fds
    /// conflicts even within one process. Marker files are never deleted (empty,
    /// reused across runs), which avoids a create/unlink race.
    pub fn acquire(&self) -> SlotGuard {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            for i in 0..self.slots {
                let path = self.dir.join(format!("slot-{i}"));
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    // The marker file is only a lock target — never written to,
                    // never truncated (it stays empty and is reused across runs).
                    .truncate(false)
                    .open(&path)
                    .expect("open fixture slot file");
                match file.try_lock() {
                    Ok(()) => return SlotGuard { _file: file },
                    Err(TryLockError::WouldBlock) => continue, // taken — try next slot
                    Err(TryLockError::Error(e)) => panic!("flock slot {i}: {e}"),
                }
            }
            if Instant::now() >= deadline {
                panic!(
                    "could not acquire a pg fixture boot slot within 120s \
                     (LOOM_PG_FIXTURE_SLOTS too low, or slots leaked?)"
                );
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}

static THROTTLE: OnceLock<BootThrottle> = OnceLock::new();

/// The process-global fixture-boot throttle. `K` slots come from
/// `LOOM_PG_FIXTURE_SLOTS` (default 8); the slot dir from
/// `LOOM_PG_FIXTURE_SLOT_DIR` (default the fixed `/tmp/loom-pg-fixture-slots`, so
/// all test processes on the host share the same `K` slots — keying off a
/// per-action `$TMPDIR` would un-throttle the cross-target axis).
fn boot_throttle() -> &'static BootThrottle {
    THROTTLE.get_or_init(|| {
        let slots = std::env::var("LOOM_PG_FIXTURE_SLOTS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(8);
        let dir = std::env::var_os("LOOM_PG_FIXTURE_SLOT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp/loom-pg-fixture-slots"));
        BootThrottle::new(dir, slots)
    })
}

/// A running ephemeral Postgres cluster. Killed and cleaned up on drop.
pub struct PgFixture {
    // Dropped last; keeps the data/socket dirs alive while the server runs.
    _data_dir: TempDir,
    socket_dir: TempDir,
    server: Child,
    bin: PathBuf,
    ld_library_path: String,
    // Declared last so it drops AFTER the server is killed/reaped (Drop runs
    // fields in declaration order): the cluster's semaphores are freed before the
    // boot-slot lock is released, so the next waiter only proceeds once this
    // cluster is truly gone.
    _slot: SlotGuard,
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
        // Gate before initdb: bounds the number of live fixture clusters so the
        // whole-suite boot doesn't exhaust kernel SysV-semaphore resources
        // (`iss-fixture-boot-contention`). Held for the cluster's lifetime.
        let _slot = boot_throttle().acquire();
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
            _slot,
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

    /// The unix-socket directory the server listens on (for clients that build
    /// their own connection string, e.g. the DuckLake writer).
    pub fn socket_path(&self) -> &std::path::Path {
        self.socket_dir.path()
    }

    /// A libpq/sqlx connection URI for `db` over the fixture's unix socket — for
    /// clients that take a DSN string (e.g. the vendored Iceberg SQL catalog).
    pub fn pg_dsn(&self, db: &str) -> String {
        format!(
            "postgres://postgres@localhost/{db}?host={}",
            self.socket_dir.path().display()
        )
    }

    /// A fresh sqlx pool for an existing fixture database — for the read side of a
    /// test that also drives the vendored catalog over the same db via `pg_dsn`.
    pub async fn pool_for(&self, db: &str) -> PgPool {
        PgPoolOptions::new()
            .max_connections(5)
            .connect_with(self.opts(db))
            .await
            .expect("connect pool_for")
    }

    /// Create a fresh database (migrated) and return a `PgControlPlane` bound to it
    /// together with the database name, so test-support writers can target the same db.
    pub async fn fresh_db(&self) -> (PgControlPlane, String) {
        let n = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        let db = format!("loom_test_{}_{}", std::process::id(), n);

        let mut admin = PgConnection::connect_with(&self.opts("postgres"))
            .await
            .expect("connect to admin database");
        admin
            .execute(AssertSqlSafe(format!("create database {db}")))
            .await
            .expect("create database");
        admin.close().await.ok();

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(self.opts(&db))
            .await
            .expect("connect pool to fresh database");
        let migrations = std::env::var("LOOM_MIGRATIONS_DIR").expect("LOOM_MIGRATIONS_DIR");
        crate::run_migrations(&pool, std::path::Path::new(&migrations))
            .await
            .expect("run migrations");

        (
            crate::PgControlPlane::new(pool, std::time::Duration::from_millis(300)),
            db,
        )
    }

    /// Create a fresh, empty database and return a `PgControlPlane` connected to
    /// it. Isolated per call, so each test gets a clean slate.
    pub async fn fresh_control_plane(&self) -> PgControlPlane {
        self.fresh_db().await.0
    }
}

impl Drop for PgFixture {
    fn drop(&mut self) {
        let _ = self.server.kill();
        let _ = self.server.wait();
        // TempDirs remove themselves on drop.
    }
}

/// Test-support: drives real DuckLake (the pinned DuckDB CLI) to produce a
/// `ducklake.*` catalog in the hermetic Postgres, so the pg `Catalog` adapter has
/// real data to read. Reads `DUCKDB_BIN` (the CLI) and `DUCKDB_EXTENSION_DIR` (the
/// offline extension dir) from the environment, set by the `rust_test` rule.
pub struct DuckLakeWriter {
    duckdb_bin: PathBuf,
    extension_dir: String,
    socket: PathBuf,
    db: String,
    // Parquet data path; removed when the writer drops.
    _data_dir: TempDir,
}

impl DuckLakeWriter {
    pub fn new(socket: &std::path::Path, db: &str) -> Self {
        Self {
            duckdb_bin: PathBuf::from(
                std::env::var("DUCKDB_BIN").expect("DUCKDB_BIN must point at the duckdb CLI"),
            ),
            extension_dir: std::env::var("DUCKDB_EXTENSION_DIR")
                .expect("DUCKDB_EXTENSION_DIR must point at the offline extension dir"),
            socket: socket.to_path_buf(),
            db: db.to_string(),
            _data_dir: tempfile::tempdir().expect("create ducklake data tempdir"),
        }
    }

    /// Create `schema.table` (if absent) with `columns` as `(name, sql_type, nullable)`,
    /// then apply each entry of `batches` as one INSERT of that many rows (one Parquet
    /// data file per batch, inlining disabled). Returns the per-batch snapshot ids, in order.
    pub async fn seed(
        &self,
        schema: &str,
        table: &str,
        columns: &[(String, String, bool)],
        batches: &[usize],
    ) -> Vec<i64> {
        let col_defs: Vec<String> = columns
            .iter()
            .map(|(name, ty, nullable)| {
                format!("{name} {ty}{}", if *nullable { "" } else { " NOT NULL" })
            })
            .collect();

        let mut sql = String::new();
        sql.push_str(&format!(
            "SET extension_directory='{}';\n",
            self.extension_dir
        ));
        sql.push_str("LOAD ducklake;\nLOAD postgres_scanner;\n");
        sql.push_str(&format!(
            "ATTACH 'ducklake:postgres:dbname={} host={} user=postgres' AS lake (DATA_PATH '{}/', DATA_INLINING_ROW_LIMIT 0);\n",
            self.db,
            self.socket.display(),
            self._data_dir.path().display(),
        ));
        sql.push_str(&format!(
            "CREATE TABLE IF NOT EXISTS lake.{schema}.{table} ({});\n",
            col_defs.join(", ")
        ));
        for n in batches {
            sql.push_str(&format!(
                "INSERT INTO lake.{schema}.{table} SELECT {} FROM range({}) t(i);\n",
                Self::row_exprs(columns),
                n
            ));
        }

        let status = Command::new(&self.duckdb_bin)
            .arg("-c")
            .arg(&sql)
            .status()
            .expect("run duckdb");
        assert!(status.success(), "duckdb seeding failed");

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(self.opts())
            .await
            .expect("connect to read back snapshots");
        sqlx::query_scalar::<_, i64>(AssertSqlSafe(
            "select f.begin_snapshot from ducklake_data_file f \
             join ducklake_table t on f.table_id = t.table_id \
             join ducklake_schema s on t.schema_id = s.schema_id \
             where s.schema_name = $1 and t.table_name = $2 \
             order by f.data_file_id",
        ))
        .bind(schema)
        .bind(table)
        .fetch_all(&pool)
        .await
        .expect("read back data-file snapshots")
    }

    /// ATTACH the DuckLake catalog WITHOUT creating any table. A bare ATTACH
    /// against an empty database runs `InitializeDuckLake`, creating the 27
    /// `ducklake_*` tables, seeding snapshot 0, the `main` schema, and the
    /// `ducklake_metadata` rows — i.e. the same starting point a fresh DuckLake
    /// catalog has, with no `CREATE TABLE`/`INSERT`. loom's native `create_table`
    /// writer then operates against this bootstrap-only catalog.
    pub async fn bootstrap(&self) {
        let mut sql = String::new();
        sql.push_str(&format!(
            "SET extension_directory='{}';\n",
            self.extension_dir
        ));
        sql.push_str("LOAD ducklake;\nLOAD postgres_scanner;\n");
        sql.push_str(&format!(
            "ATTACH 'ducklake:postgres:dbname={} host={} user=postgres' AS lake (DATA_PATH '{}/', DATA_INLINING_ROW_LIMIT 0);\n",
            self.db,
            self.socket.display(),
            self._data_dir.path().display(),
        ));

        let status = Command::new(&self.duckdb_bin)
            .arg("-c")
            .arg(&sql)
            .status()
            .expect("run duckdb");
        assert!(status.success(), "duckdb bootstrap (bare ATTACH) failed");
    }

    /// Drop `schema.table` via the DuckDB CLI (DuckLake records the drop, setting
    /// `end_snapshot` on the table and its files/columns). Returns that drop snapshot.
    pub async fn drop_table(&self, schema: &str, table: &str) -> i64 {
        let mut sql = String::new();
        sql.push_str(&format!(
            "SET extension_directory='{}';\n",
            self.extension_dir
        ));
        sql.push_str("LOAD ducklake;\nLOAD postgres_scanner;\n");
        sql.push_str(&format!(
            "ATTACH 'ducklake:postgres:dbname={} host={} user=postgres' AS lake (DATA_PATH '{}/', DATA_INLINING_ROW_LIMIT 0);\n",
            self.db,
            self.socket.display(),
            self._data_dir.path().display(),
        ));
        sql.push_str(&format!("DROP TABLE lake.{schema}.{table};\n"));

        let status = Command::new(&self.duckdb_bin)
            .arg("-c")
            .arg(&sql)
            .status()
            .expect("run duckdb");
        assert!(status.success(), "duckdb drop failed");

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(self.opts())
            .await
            .expect("connect to read back drop snapshot");
        sqlx::query_scalar::<_, i64>(AssertSqlSafe(
            "select t.end_snapshot from ducklake_table t \
             join ducklake_schema s on t.schema_id = s.schema_id \
             where s.schema_name = $1 and t.table_name = $2 and t.end_snapshot is not null \
             order by t.end_snapshot desc limit 1",
        ))
        .bind(schema)
        .bind(table)
        .fetch_one(&pool)
        .await
        .expect("read back drop snapshot")
    }

    fn opts(&self) -> PgConnectOptions {
        PgConnectOptions::new()
            .socket(&self.socket)
            .username("postgres")
            .database(&self.db)
    }

    /// The `DATA_PATH` root used in the ATTACH (the Parquet data dir). DuckLake
    /// resolves a relative file path as `DATA_PATH + schema.path + table.path +
    /// file.path`; loom registers relative paths, so for table `main.t` a file
    /// `"f.parquet"` lives at `<data_path>/main/t/f.parquet`.
    pub fn data_path(&self) -> &std::path::Path {
        self._data_dir.path()
    }

    /// The ATTACH preamble (SET extension_directory + LOAD + ATTACH lake) shared
    /// by every duckdb-cli invocation against this catalog + data dir.
    fn preamble(&self) -> String {
        format!(
            "SET extension_directory='{}';\nLOAD ducklake;\nLOAD postgres_scanner;\n\
             ATTACH 'ducklake:postgres:dbname={} host={} user=postgres' AS lake \
             (DATA_PATH '{}/', DATA_INLINING_ROW_LIMIT 0);\n",
            self.extension_dir,
            self.db,
            self.socket.display(),
            self._data_dir.path().display(),
        )
    }

    /// Run arbitrary DuckDB SQL against the attached catalog (preamble prepended).
    /// Panics on a non-zero exit (test-only).
    pub async fn exec(&self, sql: &str) {
        let full = format!("{}{sql}", self.preamble());
        let status = Command::new(&self.duckdb_bin)
            .arg("-c")
            .arg(&full)
            .status()
            .expect("run duckdb");
        assert!(status.success(), "duckdb exec failed for SQL:\n{sql}");
    }

    /// Run a SQL statement that produces a single scalar and return its stdout,
    /// trimmed. Uses `-noheader -list` so stdout is just the value. Panics on a
    /// non-zero exit, surfacing duckdb's stderr (test-only).
    pub async fn query_scalar(&self, sql: &str) -> String {
        let full = format!("{}{sql}", self.preamble());
        let out = Command::new(&self.duckdb_bin)
            .args(["-noheader", "-list", "-c"])
            .arg(&full)
            .output()
            .expect("run duckdb");
        assert!(
            out.status.success(),
            "duckdb query failed for SQL:\n{sql}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stderr),
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// The current max `snapshot_id` in the DuckLake catalog, read directly from
    /// Postgres (the `ducklake_*` tables live in the pg backend, not as plain
    /// DuckDB-visible names). Used to assert snapshot-counter continuity.
    pub async fn max_snapshot_id(&self) -> i64 {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(self.opts())
            .await
            .expect("connect to read max snapshot id");
        sqlx::query_scalar::<_, Option<i64>>(AssertSqlSafe(
            "select max(snapshot_id) from ducklake_snapshot",
        ))
        .fetch_one(&pool)
        .await
        .expect("read max snapshot id")
        .expect("ducklake_snapshot has at least one row")
    }

    /// Count rows in a `ducklake_*` catalog table, read directly from Postgres.
    /// `table` must be a trusted literal (test-only). Used to assert that a
    /// rolled-back snapshot commit leaks zero catalog rows.
    pub async fn count_rows(&self, table: &str) -> i64 {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(self.opts())
            .await
            .expect("connect to count rows");
        sqlx::query_scalar::<_, i64>(AssertSqlSafe(format!("select count(*) from {table}")))
            .fetch_one(&pool)
            .await
            .expect("count rows")
    }

    /// Positional row expressions matching `columns`: integer columns count up from
    /// `i`, everything else is a constant cast to the column type.
    fn row_exprs(columns: &[(String, String, bool)]) -> String {
        columns
            .iter()
            .map(|(_, ty, _)| {
                if ty.to_ascii_lowercase().contains("int") {
                    "i".to_string()
                } else {
                    format!("CAST('x' AS {ty})")
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Test-only seeder for the Iceberg path. Creates real, spec-compliant Iceberg tables via the
/// vendored SQL catalog (exercising the catalog port end to end against the hermetic Postgres +
/// a `file://` warehouse), then writes real Parquet through the slice-2 writer chain
/// (`iceberg_writer::append_batches`). Each append's `update_table` projects the canonical
/// metadata into `iceberg_mirror.*`, so the read adapter serves exactly what was committed.
/// Sibling of `DuckLakeWriter`.
///
/// The catalog contracts assert file counts/paths and schema/type round-trips, all satisfied by
/// the real write→project→read path. `long`/`string` columns cover every type the contracts seed.
pub struct IcebergWriter {
    /// Shared with the read adapter — mirror rows land in the control plane's database.
    pool: PgPool,
    /// libpq DSN for the vendored `SqlCatalog` (it opens its own sqlx pool).
    pg_dsn: String,
    /// `file://` warehouse root; removed on drop.
    warehouse: TempDir,
}

/// Explicit column values for [`IcebergWriter::seed_arrays`], on plain Rust data (not
/// Arrow arrays) so callers in other crates do not cross the arrow-array version boundary.
/// One variant per loom logical type the graph fixtures need; extend as needed.
pub enum SeedCol<'a> {
    /// A non-null `long` column.
    Long(Vec<i64>),
    /// A nullable `long` column (`None` => SQL NULL — e.g. a dangling FK).
    NullableLong(Vec<Option<i64>>),
    /// A non-null `string` column.
    Str(Vec<&'a str>),
}

impl SeedCol<'_> {
    /// Build the arrow-array-57 column for this data (kept inside `control-plane-postgres`
    /// so the arrow-array version never leaks across a crate boundary).
    fn to_array(&self) -> ArrayRef {
        match self {
            SeedCol::Long(v) => Arc::new(Int64Array::from(v.clone())),
            SeedCol::NullableLong(v) => Arc::new(Int64Array::from(v.clone())),
            SeedCol::Str(v) => Arc::new(StringArray::from(v.clone())),
        }
    }
}

impl IcebergWriter {
    pub fn new(pool: PgPool, pg_dsn: String) -> Self {
        Self {
            pool,
            pg_dsn,
            warehouse: tempfile::tempdir().expect("warehouse temp dir"),
        }
    }

    fn iceberg_type(logical: &str) -> Type {
        match logical {
            "long" => Type::Primitive(PrimitiveType::Long),
            "integer" => Type::Primitive(PrimitiveType::Int),
            "double" => Type::Primitive(PrimitiveType::Double),
            "boolean" => Type::Primitive(PrimitiveType::Boolean),
            "string" => Type::Primitive(PrimitiveType::String),
            "date" => Type::Primitive(PrimitiveType::Date),
            "timestamp" => Type::Primitive(PrimitiveType::Timestamp),
            other => panic!("seed: unmapped logical type {other:?}"),
        }
    }

    async fn catalog(&self) -> SqlCatalog {
        let mut props = HashMap::new();
        props.insert(SQL_CATALOG_PROP_URI.to_string(), self.pg_dsn.clone());
        props.insert(
            SQL_CATALOG_PROP_WAREHOUSE.to_string(),
            format!("file://{}", self.warehouse.path().display()),
        );
        SqlCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load("loom", props)
            .await
            .expect("build vendored SqlCatalog")
    }

    /// Create `(ns, name)` in the vendored catalog from `columns` if it does not yet
    /// exist. `columns`: `(name, loom-logical-type, nullable)`. Shared by
    /// `seed` / `seed_arrays`.
    async fn ensure_table(
        &self,
        catalog: &SqlCatalog,
        ns: &str,
        name: &str,
        columns: &[(String, String, bool)],
    ) {
        let namespace = NamespaceIdent::new(ns.to_string());
        let _ = catalog.create_namespace(&namespace, HashMap::new()).await;
        let table_ident = TableIdent::new(namespace.clone(), name.to_string());
        if !catalog.table_exists(&table_ident).await.unwrap_or(false) {
            let fields: Vec<_> = columns
                .iter()
                .enumerate()
                .map(|(i, (cname, cty, nullable))| {
                    let id = (i + 1) as i32;
                    let f = if *nullable {
                        NestedField::optional(id, cname, Self::iceberg_type(cty))
                    } else {
                        NestedField::required(id, cname, Self::iceberg_type(cty))
                    };
                    Arc::new(f)
                })
                .collect();
            let schema = Schema::builder()
                .with_fields(fields)
                .build()
                .expect("build iceberg schema");
            let creation = TableCreation::builder()
                .name(name.to_string())
                .location(format!(
                    "file://{}/{}/{}",
                    self.warehouse.path().display(),
                    ns,
                    name
                ))
                .schema(schema)
                .build();
            catalog
                .create_table(&namespace, creation)
                .await
                .expect("create_table");
        }
    }

    /// Create the table (real Iceberg, via the catalog) if absent, then append one real Parquet
    /// file of that many rows per batch via the writer chain (each append projects the mirror).
    /// `columns`: `(name, loom-logical-type, nullable)`. Returns the per-batch loom snapshot ids.
    pub async fn seed(
        &self,
        ns: &str,
        name: &str,
        columns: &[(String, String, bool)],
        batches: &[usize],
    ) -> Vec<i64> {
        let catalog = self.catalog().await;
        self.ensure_table(&catalog, ns, name, columns).await;
        let table_ident = TableIdent::new(NamespaceIdent::new(ns.to_string()), name.to_string());

        // The mirror is projected inside the catalog's update_table during each append
        // (real Parquet via the writer chain), so the seeder no longer writes mirror rows
        // directly — it builds a real Arrow batch per requested row count and appends it.
        let table = catalog.load_table(&table_ident).await.expect("load_table");
        let current_schema = table.metadata().current_schema().clone();
        let arrow_schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(&current_schema).expect("arrow schema"),
        );

        let mut snapshots = Vec::new();
        for &rows in batches {
            let arrays: Vec<ArrayRef> = current_schema
                .as_struct()
                .fields()
                .iter()
                .map(|f| Self::column_array(f, rows))
                .collect();
            let batch = RecordBatch::try_new(arrow_schema.clone(), arrays).expect("record batch");
            // Reload so each append commits against the latest pointer.
            let table = catalog.load_table(&table_ident).await.expect("reload");
            crate::iceberg_writer::append_batches(&catalog, &table, vec![batch])
                .await
                .expect("append_batches");
            snapshots.push(self.latest_snapshot_id().await);
        }
        snapshots
    }

    /// Like `seed`, but appends ONE batch of *explicit* column values (real Parquet ->
    /// mirror projection), so tests can land arbitrary graph topologies (FK or join-table
    /// edges with specific values). Creates `(ns, name)` from `columns` if absent. `data`
    /// carries the column values in `columns` order as plain Rust (`SeedCol`) — NOT Arrow
    /// arrays, so callers in other crates never cross the arrow-array version boundary; the
    /// fixture builds the arrays here (arrow-array 57). The batch is built against the table's
    /// own Arrow schema so nullability/types match exactly. Returns the loom snapshot id.
    pub async fn seed_arrays(
        &self,
        ns: &str,
        name: &str,
        columns: &[(String, String, bool)],
        data: &[SeedCol<'_>],
    ) -> i64 {
        let catalog = self.catalog().await;
        self.ensure_table(&catalog, ns, name, columns).await;
        let table_ident = TableIdent::new(NamespaceIdent::new(ns.to_string()), name.to_string());
        let table = catalog.load_table(&table_ident).await.expect("load_table");
        let current_schema = table.metadata().current_schema().clone();
        let arrow_schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(&current_schema).expect("arrow schema"),
        );
        let arrays: Vec<ArrayRef> = data.iter().map(SeedCol::to_array).collect();
        let batch = RecordBatch::try_new(arrow_schema, arrays).expect("record batch");
        crate::iceberg_writer::append_batches(&catalog, &table, vec![batch])
            .await
            .expect("append_batches");
        self.latest_snapshot_id().await
    }

    /// Inline-append `rows` of `(id long, name string)` to `(ns, name)` via
    /// `inline_append` (mirror-only, no Parquet). `columns` is the table's logical
    /// schema. Returns the new loom snapshot id. Test-only.
    pub async fn inline(
        &self,
        ns: &str,
        name: &str,
        columns: &[(String, String, bool)],
        rows: &[(i64, &str)],
        run: uuid::Uuid,
    ) -> i64 {
        let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
        ]));
        let ids: Vec<i64> = rows.iter().map(|(i, _)| *i).collect();
        let names: Vec<&str> = rows.iter().map(|(_, n)| *n).collect();
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(arrow_array::Int64Array::from(ids)),
                std::sync::Arc::new(arrow_array::StringArray::from(names)),
            ],
        )
        .expect("inline batch");
        let specs: Vec<control_plane_core::ColumnSpec> = columns
            .iter()
            .map(|(n, t, nullable)| control_plane_core::ColumnSpec {
                name: n.clone(),
                ty: t.clone(),
                nullable: *nullable,
            })
            .collect();
        let lineage = control_plane_core::LineageEvent {
            run_id: control_plane_core::RunId(run),
            event_type: control_plane_core::EventType::Complete,
            event_time: time::OffsetDateTime::now_utc(),
            inputs: vec![],
            outputs: vec![],
            payload: serde_json::json!({ "source": "inline-test" }),
        };
        let table = control_plane_core::TableRef {
            schema: ns.into(),
            name: name.into(),
        };
        crate::iceberg_inline::inline_append(&self.pool, &table, &specs, &batch, lineage, None)
            .await
            .expect("inline_append")
            .0
    }

    /// Build a synthetic Arrow array of `rows` values for one column, typed to match its
    /// Iceberg primitive type. Only the types the catalog contracts seed (`long`, `string`)
    /// are supported, mirroring `iceberg_type`.
    fn column_array(field: &iceberg::spec::NestedField, rows: usize) -> ArrayRef {
        match field.field_type.as_ref() {
            Type::Primitive(PrimitiveType::Long) => {
                Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>()))
            }
            Type::Primitive(PrimitiveType::String) => Arc::new(StringArray::from(
                (0..rows).map(|i| format!("row{i}")).collect::<Vec<_>>(),
            )),
            other => panic!("seed: unsupported column type for real batch: {other:?}"),
        }
    }

    /// The most-recently allocated loom snapshot id. The seeder is single-writer, so right
    /// after an append (or a drop) this is the id that commit projected.
    async fn latest_snapshot_id(&self) -> i64 {
        let m: Option<i64> =
            sqlx::query_scalar("select max(snapshot_id) from iceberg_mirror.snapshot")
                .fetch_one(&self.pool)
                .await
                .expect("max snapshot");
        m.expect("at least one snapshot")
    }

    /// Attempt an inline append whose declared `columns` diverge from the live mirror
    /// schema, returning the error string. The batch itself stays `(id long, name string)`
    /// so only the declared schema diverges — exercising the detect+reject path. Test-only.
    pub async fn inline_expect_err(
        &self,
        ns: &str,
        name: &str,
        columns: &[(String, String, bool)],
    ) -> String {
        let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(arrow_array::Int64Array::from(vec![9i64])),
                std::sync::Arc::new(arrow_array::StringArray::from(vec!["z"])),
            ],
        )
        .expect("inline batch");
        let specs: Vec<control_plane_core::ColumnSpec> = columns
            .iter()
            .map(|(n, t, nullable)| control_plane_core::ColumnSpec {
                name: n.clone(),
                ty: t.clone(),
                nullable: *nullable,
            })
            .collect();
        let lineage = control_plane_core::LineageEvent {
            run_id: control_plane_core::RunId(uuid::Uuid::from_u128(99)),
            event_type: control_plane_core::EventType::Complete,
            event_time: time::OffsetDateTime::now_utc(),
            inputs: vec![],
            outputs: vec![],
            payload: serde_json::json!({ "source": "inline-evolution-test" }),
        };
        let table = control_plane_core::TableRef {
            schema: ns.into(),
            name: name.into(),
        };
        crate::iceberg_inline::inline_append(&self.pool, &table, &specs, &batch, lineage, None)
            .await
            .expect_err("expected schema-evolution rejection")
            .to_string()
    }

    /// Drop the table via the vendored catalog; the mirror is marked dropped in the same
    /// transaction (see `SqlCatalog::drop_table`). Returns the loom snapshot id at which it
    /// was dropped. The delete contract exercises the mirror's MVCC `end`-bound.
    pub async fn drop_table(&self, ns: &str, name: &str) -> i64 {
        let catalog = self.catalog().await;
        let ident = TableIdent::new(NamespaceIdent::new(ns.to_string()), name.to_string());
        catalog.drop_table(&ident).await.expect("drop_table");
        self.latest_snapshot_id().await
    }
}

/// A running ephemeral MinIO server. Killed and cleaned up on drop. Booted from the
/// vendored `minio` binary (path via `MINIO_BIN`); like `initdb` it won't run as root
/// on RE, so consumers must be `loom_fixture_test` targets.
pub struct MinioFixture {
    _data_dir: TempDir,
    server: std::process::Child,
    endpoint: String,
}

impl MinioFixture {
    /// Boot an ephemeral MinIO server. Requires `MINIO_BIN` to point at the minio
    /// binary. Panics on failure — test-only.
    pub fn start() -> Self {
        let bin = std::env::var("MINIO_BIN").expect("MINIO_BIN must point at the minio binary");
        let data_dir = tempfile::tempdir().expect("minio data tempdir");
        // Reserve a free port, then release it for minio to claim.
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
            l.local_addr().unwrap().port()
        };
        let addr = format!("127.0.0.1:{port}");
        let server = std::process::Command::new(&bin)
            .arg("server")
            .arg(data_dir.path())
            .args(["--address", &addr])
            .env("MINIO_ROOT_USER", "minioadmin")
            .env("MINIO_ROOT_PASSWORD", "minioadmin")
            // Quiet, no update checks, no console.
            .env("MINIO_UPDATE", "off")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn minio");
        let endpoint = format!("http://{addr}");
        let fixture = Self {
            _data_dir: data_dir,
            server,
            endpoint,
        };
        fixture.wait_ready(port);
        fixture
    }

    pub fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    pub fn access_key(&self) -> &str {
        "minioadmin"
    }

    pub fn secret_key(&self) -> &str {
        "minioadmin"
    }

    /// Poll readiness via a raw TCP connect to the port (avoids reqwest::blocking).
    /// Polls up to ~15s in 50ms steps.
    fn wait_ready(&self, port: u16) {
        let addr = format!("127.0.0.1:{port}");
        for _ in 0..300 {
            if std::net::TcpStream::connect(&addr).is_ok() {
                // Give MinIO a moment to finish binding after accepting TCP.
                std::thread::sleep(Duration::from_millis(200));
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("minio did not become ready within ~15s");
    }

    /// Create `bucket` via a SigV4-signed `PUT /{bucket}` (no `mc` binary).
    pub async fn create_bucket(&self, bucket: &str) {
        use hmac::{Hmac, Mac};
        use sha2::{Digest, Sha256};
        type HmacSha256 = Hmac<Sha256>;

        let region = "us-east-1";
        let service = "s3";
        let host = self.endpoint.strip_prefix("http://").unwrap().to_string();
        // Timestamps: YYYYMMDDtHHMMSSZ and YYYYMMDD (UTC). `time` is already a dep.
        let now = time::OffsetDateTime::now_utc();
        let amz_date = now
            .format(
                &time::format_description::parse("[year][month][day]T[hour][minute][second]Z")
                    .unwrap(),
            )
            .unwrap();
        let date = now
            .format(&time::format_description::parse("[year][month][day]").unwrap())
            .unwrap();

        let payload_hash = hex::encode(Sha256::digest(b""));
        let canonical_uri = format!("/{bucket}");
        let canonical_headers =
            format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_request = format!(
            "PUT\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );
        let scope = format!("{date}/{region}/{service}/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );

        let mac = |key: &[u8], msg: &str| {
            let mut m = HmacSha256::new_from_slice(key).unwrap();
            m.update(msg.as_bytes());
            m.finalize().into_bytes()
        };
        let k_date = mac(format!("AWS4{}", self.secret_key()).as_bytes(), &date);
        let k_region = mac(&k_date, region);
        let k_service = mac(&k_region, service);
        let k_signing = mac(&k_service, "aws4_request");
        let mut sig = HmacSha256::new_from_slice(&k_signing).unwrap();
        sig.update(string_to_sign.as_bytes());
        let signature = hex::encode(sig.finalize().into_bytes());

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key()
        );

        let resp = reqwest::Client::new()
            .put(format!("{}/{bucket}", self.endpoint))
            .header("Host", &host)
            .header("x-amz-content-sha256", &payload_hash)
            .header("x-amz-date", &amz_date)
            .header("Authorization", authorization)
            .send()
            .await
            .expect("PUT bucket");
        // 200 = created; MinIO returns 409 BucketAlreadyOwnedByYou on re-create.
        assert!(
            resp.status().is_success() || resp.status().as_u16() == 409,
            "create_bucket failed: {}",
            resp.status()
        );
    }
}

impl Drop for MinioFixture {
    fn drop(&mut self) {
        let _ = self.server.kill();
        let _ = self.server.wait();
        // TempDir removes itself on drop.
    }
}
