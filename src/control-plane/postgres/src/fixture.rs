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

use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, Connection, Executor, PgConnection};
use tempfile::TempDir;

use crate::PgControlPlane;

// --- Iceberg seeder imports (used by `IcebergWriter` below) ---
use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder as _, NamespaceIdent, TableCreation, TableIdent};
use sqlx::PgPool;

use crate::iceberg_mirror::{
    ProjectedColumn, ProjectedFile, ensure_table, mark_dropped, next_snapshot, project_columns,
    project_files,
};
use crate::iceberg_sql_catalog::{
    SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlCatalog, SqlCatalogBuilder,
};

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

/// Test-only seeder for the Iceberg read path. Creates real, spec-compliant Iceberg tables via
/// the vendored SQL catalog (exercising the catalog port end to end against the hermetic
/// Postgres + a `file://` warehouse), reads the schema Iceberg actually recorded, and projects
/// it — plus one synthetic data-file row per batch — into `iceberg_mirror.*` so the read adapter
/// can serve it. Sibling of `DuckLakeWriter`.
///
/// Slice 1 is the read path, so the seeder does NOT write real Parquet bytes (that needs the
/// `iceberg` writer chain bound to arrow/parquet 57, and is the write path's concern — slice 2).
/// The catalog contracts assert file counts/paths and schema/type round-trips, all of which a
/// real-schema + synthetic-file projection satisfies. `long`/`string` columns cover every type
/// the contracts seed.
pub struct IcebergWriter {
    /// Shared with the read adapter — mirror rows land in the control plane's database.
    pool: PgPool,
    /// libpq DSN for the vendored `SqlCatalog` (it opens its own sqlx pool).
    pg_dsn: String,
    /// `file://` warehouse root; removed on drop.
    warehouse: TempDir,
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

    /// Create the table (real Iceberg, via the catalog) if absent, then record each row-batch as
    /// its own loom snapshot adding one synthetic data file of that many rows.
    /// `columns`: `(name, loom-logical-type, nullable)`. Returns the per-batch loom snapshot ids.
    pub async fn seed(
        &self,
        ns: &str,
        name: &str,
        columns: &[(String, String, bool)],
        batches: &[usize],
    ) -> Vec<i64> {
        let catalog = self.catalog().await;
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

        // Read back the schema Iceberg recorded, so the mirror reflects real catalog metadata.
        let table = catalog.load_table(&table_ident).await.expect("load_table");
        let proj_cols: Vec<ProjectedColumn> = table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let iceberg_type = match field.field_type.as_ref() {
                    Type::Primitive(p) => p.to_string(),
                    other => panic!("seed: non-primitive column type {other:?}"),
                };
                ProjectedColumn {
                    order: (i + 1) as i64,
                    name: field.name.clone(),
                    iceberg_type,
                    nullable: !field.required,
                }
            })
            .collect();

        let mut snapshots = Vec::new();
        for &rows in batches {
            let mut conn = self.pool.acquire().await.expect("acquire");
            let at = next_snapshot(&mut conn, None).await.expect("next_snapshot");
            let tid = ensure_table(&mut conn, ns, name, at)
                .await
                .expect("ensure_table");
            if snapshots.is_empty() {
                project_columns(&mut conn, tid, at, &proj_cols)
                    .await
                    .expect("project_columns");
            }
            let file = ProjectedFile {
                path: format!("{ns}/{name}/data-{}.parquet", at.0),
                file_format: "parquet".to_string(),
                record_count: rows as i64,
                file_size_bytes: (rows as i64) * 16 + 100,
            };
            project_files(&mut conn, tid, at, std::slice::from_ref(&file))
                .await
                .expect("project_files");
            snapshots.push(at.0);
        }
        snapshots
    }

    /// Drop a table in the mirror (sets `end_snapshot`), returning the loom snapshot id at which
    /// it was dropped. The read path reads only the mirror, and the delete contract exercises the
    /// mirror's MVCC `end`-bound.
    pub async fn drop_table(&self, ns: &str, name: &str) -> i64 {
        let mut conn = self.pool.acquire().await.expect("acquire");
        let at = next_snapshot(&mut conn, None)
            .await
            .expect("next_snapshot for drop");
        mark_dropped(&mut conn, ns, name, at)
            .await
            .expect("mark_dropped");
        at.0
    }
}
