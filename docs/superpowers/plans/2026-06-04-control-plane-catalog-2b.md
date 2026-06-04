# Control-Plane Catalog — Phase 2b (real-DuckLake Postgres adapter) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Postgres `Catalog` adapter (pure sqlx, reading DuckLake's `public.ducklake_*` tables) pass the *same* `catalog_contract` the in-memory fake passes — with the catalog produced by **real DuckLake**, driven by a pinned DuckDB CLI in a hermetic test fixture.

**Architecture:** The shipped adapter is pure sqlx over the DuckLake catalog tables (no DuckDB in production). DuckDB appears only in the test fixture: a pinned `duckdb` CLI binary + the `ducklake`/`postgres_scanner` extensions, all materialized by buck2 (`http_file`) and loaded offline from a local `extension_directory`. A test-support `DuckLakeWriter` shells the CLI to `ATTACH 'ducklake:postgres:…'` against the hermetic Postgres and create a table + insert batches (with data inlining disabled so each batch writes one Parquet file), then reads back the per-batch snapshot ids via sqlx. The catalog contract is wired green via a local `CatalogSeed` newtype, exactly like the memory fake.

**Tech Stack:** Rust 2024, buck2, sqlx 0.8 (existing), the hermetic `PgFixture` (existing), DuckDB v1.5.3 CLI + `ducklake`/`postgres_scanner` extensions (new, test-only, `http_file`). No new Rust third-party crates.

---

## Background for the implementer

Phase 2a (merged) added the read-only `Catalog` trait + domain types in `core`, the `CatalogSeed` seam + `catalog_contract` in `testkit`, and the in-memory fake. This cycle (2b) adds the real Postgres adapter and the DuckLake-backed test substrate.

**Everything below was validated by a spike** against DuckDB v1.5.3 + the hermetic theseus Postgres. The key facts (also saved in the `ducklake-catalog-facts` memory):

- DuckLake's catalog tables live in Postgres schema **`public`**, table-name prefix `ducklake_` (e.g. `public.ducklake_snapshot`). Default `search_path` resolves unqualified `ducklake_snapshot` to `public`, so the adapter's SQL can use unqualified names.
- **MVCC range model:** rows in `ducklake_table`/`ducklake_data_file`/`ducklake_column`/`ducklake_schema` carry `begin_snapshot` and `end_snapshot`; a row is *live at* snapshot `s` when `begin_snapshot <= s AND (end_snapshot IS NULL OR end_snapshot > s)`. Snapshots are catalog-global monotonic `BIGINT` (`ducklake_snapshot.snapshot_id`).
- The extensions **load offline** from a local `extension_directory` laid out as `<dir>/v1.5.3/linux_amd64/<name>.duckdb_extension` — `LOAD` (not `INSTALL`) needs no network and no `-unsigned`.
- The DuckLake attach: `ATTACH 'ducklake:postgres:dbname=<db> host=<socket> user=postgres' AS lake (DATA_PATH '<dir>/', DATA_INLINING_ROW_LIMIT 0)`. **`DATA_INLINING_ROW_LIMIT 0` is load-bearing**: without it small inserts are inlined into `ducklake_inlined_data_*` and write NO `ducklake_data_file` row.
- **`column_type` is DuckLake-normalized** (`int64`/`varchar`, not the input `BIGINT`/`VARCHAR`), and **`column_order` is 1-based**. The 2a contract asserts `ty == "VARCHAR"`, which would fail on pg — Task 1 relaxes it.

The four adapter read queries were validated to return: `current_snapshot(main.events)`→ snapshot 3 (schema_version 1); `snapshots`→ [1,2,3] ascending; `files` → 1 at the first batch's snapshot, 2 at the second; `schema` → `[(1,id,int64,not-null),(2,name,varchar,nullable)]`.

Conventions (unchanged from prior cycles):
- pg tests run locally: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:<target>` (the fixture's `initdb`/`postgres` — and now `duckdb` — refuse to run / misbehave as root on RE workers).
- `cargo fmt --all` before commit; `tools/clippy-all.sh` clean; `is_none_or` not `map_or(true, …)`.
- Commit messages: Conventional Commits, body ending exactly with:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
- **DuckDB is pinned linux-x86_64 only** in this cycle (the catalog pg test is local-only and active dev/CI is linux). macOS support for the DuckLake seeder is a deliberate follow-up; do not block on it.

---

## File Structure

**Task 1 — relax the contract's type assertion (prep)**
- Modify: `src/control-plane/testkit/src/lib.rs` — treat `ColumnDef.ty` as opaque in `catalog_contract`.

**Task 2 — hermetic DuckDB substrate + DuckLake seeder (de-risk the fragile plumbing)**
- Modify: `src/control-plane/postgres/BUCK` — `http_file` the DuckDB CLI + two extensions; genrules to gunzip the CLI and build the `extension_directory`.
- Modify: `src/control-plane/postgres/src/fixture.rs` — expose the socket path + a `fresh_db` returning `(PgControlPlane, db_name)`; add a test-support `DuckLakeWriter`.
- Create: `src/control-plane/postgres/tests/ducklake_smoke.rs` — proves the binary/extensions/seeder produce the expected `ducklake_data_file` rows (read via raw sqlx), independent of the adapter.
- Modify: `src/control-plane/postgres/BUCK` — `ducklake-smoke` rust_test with the DuckDB env.

**Task 3 — pg Catalog adapter + green contract**
- Modify: `src/control-plane/postgres/src/lib.rs` — `impl Catalog for PgControlPlane` (the four queries).
- Create: `src/control-plane/postgres/tests/catalog.rs` — `CatalogSeed` newtype over `DuckLakeWriter`; run `catalog_contract`.
- Modify: `src/control-plane/postgres/BUCK` — `catalog` rust_test.

---

## Task 1: Relax the contract's column-type assertion

**Files:** Modify `src/control-plane/testkit/src/lib.rs`

The 2a `catalog_contract` asserts `sch.columns[1].ty == "VARCHAR"`. Real DuckLake stores `varchar`. Since `ty` is documented as opaque, the contract must not assert a literal — only that columns are present, ordered, and carry *some* non-empty type plus correct nullability.

- [ ] **Step 1: Relax the `ty` assertion**

In `src/control-plane/testkit/src/lib.rs`, find the schema assertions in `catalog_contract` (currently `assert_eq!(sch.columns[1].ty, "VARCHAR");`) and replace the exact-type check with an opacity check, keeping the name-order and nullability assertions:

```rust
    // schema at current: the two columns, in order, with non-empty (opaque) types
    // and correct nullability. `ty` is backend-spelled (DuckLake: `varchar`/`int64`;
    // the fake: whatever was seeded) so it is treated as opaque, never compared to a literal.
    let sch = catalog.schema(&t, cur.id).await.unwrap();
    assert_eq!(
        sch.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        vec!["id", "name"],
        "columns in order"
    );
    assert!(
        sch.columns.iter().all(|c| !c.ty.is_empty()),
        "every column has a (backend-spelled) type"
    );
    assert!(
        sch.columns[1].nullable && !sch.columns[0].nullable,
        "nullability preserved"
    );
```
(Remove the `assert_eq!(sch.columns[1].ty, "VARCHAR");` line. Leave everything else in the contract unchanged. Note: the contract already sorts/keys by `name`, not by the integer `order`, so the 1-based vs 0-based `column_order` difference between backends needs no change.)

- [ ] **Step 2: Confirm the memory fake still passes the relaxed contract**

```bash
cargo fmt --all
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/memory:catalog
tools/clippy-all.sh
```
Expected: `memory_passes_catalog_contract` still passes; clippy clean.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test(control-plane): treat Catalog column type as opaque in the contract"
```
(Append the Co-Authored-By trailer.)

---

## Task 2: Hermetic DuckDB substrate + DuckLake seeder

**Files:** Modify `src/control-plane/postgres/BUCK`, `src/control-plane/postgres/src/fixture.rs`; Create `src/control-plane/postgres/tests/ducklake_smoke.rs`

This task lands the fragile, buck-specific plumbing (DuckDB binary + offline extensions) and the seeder, and proves them with a focused smoke test before the adapter depends on them.

- [ ] **Step 1: Add the DuckDB CLI + extensions to the postgres BUCK**

In `src/control-plane/postgres/BUCK`, add the following (place near the existing `postgres-bin`/`libxml2` blocks). DuckDB is pinned linux-x86_64 only; the genrules decompress with `gzip` and are labelled `uses_xz` to keep them off RE (consistent with the `libxml2` genrule), since the catalog tests run locally anyway.

```python
# Pinned DuckDB CLI + the ducklake/postgres_scanner extensions, used ONLY by the
# catalog test fixture's DuckLakeWriter to produce a real ducklake.* catalog.
# Production code never touches DuckDB. linux-x86_64 only (tests run local-only).
DUCKDB_VERSION = "v1.5.3"

http_file(
    name = "duckdb-cli.gz",
    urls = ["https://github.com/duckdb/duckdb/releases/download/{}/duckdb_cli-linux-amd64.gz".format(DUCKDB_VERSION)],
    sha256 = "f05f3b448a9a1bc6e7ac27ff14dfe67bf5761b153c2002723365a456618ef35b",
)

# Gunzip + chmod the single-file CLI. `uses_xz` forces local (off RE).
genrule(
    name = "duckdb-cli",
    out = "duckdb",
    cmd = "gzip -dc $(location :duckdb-cli.gz) > $OUT && chmod +x $OUT",
    labels = ["uses_xz"],
    visibility = ["PUBLIC"],
)

http_file(
    name = "ducklake-ext.gz",
    urls = ["http://extensions.duckdb.org/{}/linux_amd64/ducklake.duckdb_extension.gz".format(DUCKDB_VERSION)],
    sha256 = "5fe971257a8aca6de035c49b29559982625b3d681b14777e1f2e3b8bd2f60a3d",
)

http_file(
    name = "postgres-scanner-ext.gz",
    urls = ["http://extensions.duckdb.org/{}/linux_amd64/postgres_scanner.duckdb_extension.gz".format(DUCKDB_VERSION)],
    sha256 = "42e9ba5c2a9f9f77f1415e4cd320295f9623d4c20e09b20687d384a06681e54c",
)

# DuckDB loads extensions offline from <dir>/<version>/<platform>/<name>.duckdb_extension.
# Build exactly that layout so the fixture can `SET extension_directory=` + `LOAD` with no network.
genrule(
    name = "duckdb-extensions",
    out = "extdir",
    cmd = " && ".join([
        "mkdir -p $OUT/{}/linux_amd64".format(DUCKDB_VERSION),
        "gzip -dc $(location :ducklake-ext.gz) > $OUT/{}/linux_amd64/ducklake.duckdb_extension".format(DUCKDB_VERSION),
        "gzip -dc $(location :postgres-scanner-ext.gz) > $OUT/{}/linux_amd64/postgres_scanner.duckdb_extension".format(DUCKDB_VERSION),
    ]),
    labels = ["uses_xz"],
    visibility = ["PUBLIC"],
)
```

- [ ] **Step 2: Expose connection info + add `DuckLakeWriter` to the fixture**

In `src/control-plane/postgres/src/fixture.rs`:

(a) Add a public accessor for the socket dir and refactor database creation so the db name is available to callers. Read the file first; it has a private `opts(&self, db)` and a `fresh_control_plane(&self)`. Add:

```rust
    /// The unix-socket directory the server listens on (for clients that build
    /// their own connection string, e.g. the DuckLake writer).
    pub fn socket_path(&self) -> &std::path::Path {
        self.socket_dir.path()
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
            .execute(format!("create database {db}").as_str())
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
```
Then make the existing `fresh_control_plane` delegate (so the queue test is unchanged):
```rust
    pub async fn fresh_control_plane(&self) -> PgControlPlane {
        self.fresh_db().await.0
    }
```

(b) Add the `DuckLakeWriter` test-support type at the end of the file. It shells the pinned DuckDB CLI (paths from env, set by the BUCK `rust_test`) to create a table + insert batches against the hermetic Postgres as the DuckLake catalog, then reads back the per-batch snapshot ids via sqlx. It does NOT depend on `testkit` (keeps the orphan rule clean — the `CatalogSeed` impl lives in the test crate).

```rust
/// Test-support: drives real DuckLake (the pinned DuckDB CLI) to produce a
/// `ducklake.*` catalog in the hermetic Postgres, so the pg `Catalog` adapter has
/// real data to read. Reads `DUCKDB_BIN` (the CLI) and `DUCKDB_EXTENSION_DIR` (the
/// offline extension dir) from the environment, set by the `rust_test` rule.
pub struct DuckLakeWriter {
    duckdb_bin: PathBuf,
    extension_dir: String,
    socket: PathBuf,
    db: String,
    // Parquet data path; dropped (removed) with the writer.
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
        sql.push_str(&format!("SET extension_directory='{}';\n", self.extension_dir));
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
        // One INSERT per batch -> one snapshot + one data file each.
        for n in batches {
            // Synthetic rows; only the count matters to the contract. Cast range to
            // the table's column types via a positional SELECT.
            sql.push_str(&format!(
                "INSERT INTO lake.{schema}.{table} SELECT * FROM (SELECT {} FROM range({}) t(i));\n",
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

        // Read back per-batch snapshots: with inlining off + append-only, data files
        // are created one-per-batch in id order; each file's begin_snapshot is its
        // batch's insert snapshot.
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(self.opts())
            .await
            .expect("connect to read back snapshots");
        let rows = sqlx::query_scalar::<_, i64>(
            "select f.begin_snapshot from ducklake_data_file f \
             join ducklake_table t on f.table_id = t.table_id \
             join ducklake_schema s on t.schema_id = s.schema_id \
             where s.schema_name = $1 and t.table_name = $2 \
             order by f.data_file_id",
        )
        .bind(schema)
        .bind(table)
        .fetch_all(&pool)
        .await
        .expect("read back data-file snapshots");
        rows
    }

    fn opts(&self) -> PgConnectOptions {
        PgConnectOptions::new()
            .socket(&self.socket)
            .username("postgres")
            .database(&self.db)
    }

    /// Build a positional row expression list matching `columns`: integers count up
    /// from `i`, everything else is a constant cast to the column type.
    fn row_exprs(columns: &[(String, String, bool)]) -> String {
        columns
            .iter()
            .map(|(_, ty, _)| {
                let t = ty.to_ascii_lowercase();
                if t.contains("int") {
                    "i".to_string()
                } else {
                    format!("CAST('x' AS {ty})")
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}
```
Add any missing imports at the top of `fixture.rs`: `std::path::PathBuf` (if not present) and ensure `Command`, `TempDir`, `PgConnectOptions`, `PgPoolOptions` are in scope (the file already uses `Command`, `TempDir`, and the sqlx connect options for the server — confirm and extend the `use` lines as needed).

- [ ] **Step 3: Write the smoke test**

Create `src/control-plane/postgres/tests/ducklake_smoke.rs` — proves the binary + offline extensions + seeder produce the expected catalog, using raw sqlx (no adapter yet):

```rust
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};

// The hermetic DuckDB CLI + offline extensions can drive DuckLake against the
// fixture's Postgres and produce one data file per (inlining-disabled) batch.
#[tokio::test]
async fn ducklake_writer_produces_catalog() {
    let fixture = PgFixture::start();
    let (_cp, db) = fixture.fresh_db().await;
    let writer = DuckLakeWriter::new(fixture.socket_path(), &db);

    let snaps = writer
        .seed(
            "main",
            "events",
            &[
                ("id".into(), "BIGINT".into(), false),
                ("name".into(), "VARCHAR".into(), true),
            ],
            &[10, 20],
        )
        .await;

    // Two batches => two data files => two distinct, ascending begin-snapshots.
    assert_eq!(snaps.len(), 2, "one snapshot per batch");
    assert!(snaps[0] < snaps[1], "snapshots ascending");
}
```

- [ ] **Step 4: Add the smoke-test target**

In `src/control-plane/postgres/BUCK`, add a `rust_test` wiring the full env (the existing pg env plus the two DuckDB vars). Note `crate_root` is `tests/ducklake_smoke.rs`:

```python
rust_test(
    name = "ducklake-smoke",
    crate = "ducklake_smoke",
    srcs = ["tests/ducklake_smoke.rs"],
    crate_root = "tests/ducklake_smoke.rs",
    edition = "2024",
    env = {
        "POSTGRES_BIN_DIR": "$(location :postgres-bin)/bin",
        "POSTGRES_LD_LIBRARY_PATH": "$(location :postgres-bin)/lib:$(location :libxml2)",
        "LOOM_MIGRATIONS_DIR": "$(location :migrations)/migrations",
        "DUCKDB_BIN": "$(location :duckdb-cli)",
        "DUCKDB_EXTENSION_DIR": "$(location :duckdb-extensions)",
    },
    deps = [":postgres", "//third-party:tokio"],
)
```

- [ ] **Step 5: Run the smoke test (de-risk the plumbing)**

```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:ducklake-smoke 2>&1 | tail -25
```
Expected: `ducklake_writer_produces_catalog` passes — the CLI runs, extensions load offline, DuckLake writes the catalog, and two batch snapshots come back. If extensions fail to load, verify the `:duckdb-extensions` genrule layout is `<extdir>/v1.5.3/linux_amd64/<name>.duckdb_extension`. If `duckdb` isn't found, check the `:duckdb-cli` genrule produced an executable and `DUCKDB_BIN` points at it.

- [ ] **Step 6: Format, lint, commit**

```bash
cargo fmt --all
tools/clippy-all.sh
git add -A
git commit -m "test(control-plane): hermetic DuckDB + DuckLakeWriter seeding the ducklake catalog"
```
(Append the Co-Authored-By trailer.)

---

## Task 3: Postgres Catalog adapter + green contract

**Files:** Modify `src/control-plane/postgres/src/lib.rs`; Create `src/control-plane/postgres/tests/catalog.rs`; Modify `src/control-plane/postgres/BUCK`

- [ ] **Step 1: Write the failing contract test (pg)**

Create `src/control-plane/postgres/tests/catalog.rs`. A local `PgSeeder` newtype adapts `CatalogSeed` to the `DuckLakeWriter`:

```rust
use async_trait::async_trait;
use control_plane_core::SnapshotId;
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use control_plane_testkit::{CatalogSeed, SeedSpec, SeededSnapshot, catalog_contract};

struct PgSeeder {
    writer: DuckLakeWriter,
}

#[async_trait]
impl CatalogSeed for PgSeeder {
    async fn seed(&self, spec: SeedSpec) -> Vec<SeededSnapshot> {
        let cols: Vec<(String, String, bool)> = spec
            .columns
            .into_iter()
            .map(|c| (c.name, c.ty, c.nullable))
            .collect();
        self.writer
            .seed(&spec.table.schema, &spec.table.name, &cols, &spec.row_batches)
            .await
            .into_iter()
            .map(|s| SeededSnapshot {
                snapshot: SnapshotId(s),
                files_added: 1,
            })
            .collect()
    }
}

#[tokio::test]
async fn postgres_passes_catalog_contract() {
    let fixture = PgFixture::start();
    let (cp, db) = fixture.fresh_db().await;
    let seeder = PgSeeder {
        writer: DuckLakeWriter::new(fixture.socket_path(), &db),
    };
    catalog_contract(&cp, &seeder).await;
}
```

- [ ] **Step 2: Run it to confirm it fails (no `Catalog` impl yet)**

```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:catalog 2>&1 | tail -20
```
Expected: build failure — `PgControlPlane` does not impl `Catalog` yet (and the `catalog` target isn't defined until Step 4).

- [ ] **Step 3: Implement `Catalog` for `PgControlPlane`**

In `src/control-plane/postgres/src/lib.rs`, add the impl. The queries read the unqualified `ducklake_*` tables (resolved to `public` by the default search_path) using the validated MVCC range filter. Add the catalog types to the existing `control_plane_core` import (`Catalog, ColumnDef, FileRef, Snapshot, SnapshotId, TableRef, TableSchema`); `OffsetDateTime`, `Row as _`, `backend` are already in scope.

```rust
#[async_trait]
impl Catalog for PgControlPlane {
    async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot> {
        let row = sqlx::query(
            "select sn.snapshot_id, sn.snapshot_time, sn.schema_version \
             from ducklake_snapshot sn \
             where exists ( \
                 select 1 from ducklake_table t join ducklake_schema s on t.schema_id = s.schema_id \
                 where s.schema_name = $1 and t.table_name = $2 \
                   and t.begin_snapshot <= sn.snapshot_id and (t.end_snapshot is null or t.end_snapshot > sn.snapshot_id)) \
             order by sn.snapshot_id desc limit 1",
        )
        .bind(&table.schema)
        .bind(&table.name)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| ControlPlaneError::NotFound(format!("{}.{}", table.schema, table.name)))?;
        Ok(row_to_snapshot(&row))
    }

    async fn snapshots(&self, table: &TableRef) -> Result<Vec<Snapshot>> {
        let rows = sqlx::query(
            "select sn.snapshot_id, sn.snapshot_time, sn.schema_version \
             from ducklake_snapshot sn \
             where exists ( \
                 select 1 from ducklake_table t join ducklake_schema s on t.schema_id = s.schema_id \
                 where s.schema_name = $1 and t.table_name = $2 \
                   and t.begin_snapshot <= sn.snapshot_id and (t.end_snapshot is null or t.end_snapshot > sn.snapshot_id)) \
             order by sn.snapshot_id",
        )
        .bind(&table.schema)
        .bind(&table.name)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        if rows.is_empty() {
            return Err(ControlPlaneError::NotFound(format!("{}.{}", table.schema, table.name)));
        }
        Ok(rows.iter().map(row_to_snapshot).collect())
    }

    async fn files(&self, table: &TableRef, at: SnapshotId) -> Result<Vec<FileRef>> {
        let tid = self.resolve_table(table, at).await?;
        let rows = sqlx::query(
            "select path, record_count, file_size_bytes from ducklake_data_file \
             where table_id = $1 and begin_snapshot <= $2 and (end_snapshot is null or end_snapshot > $2) \
             order by data_file_id",
        )
        .bind(tid)
        .bind(at.0)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows
            .iter()
            .map(|r| FileRef {
                path: r.get("path"),
                record_count: r.get("record_count"),
                file_size_bytes: r.get("file_size_bytes"),
            })
            .collect())
    }

    async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema> {
        let tid = self.resolve_table(table, at).await?;
        let rows = sqlx::query(
            "select column_order, column_name, column_type, nulls_allowed from ducklake_column \
             where table_id = $1 and begin_snapshot <= $2 and (end_snapshot is null or end_snapshot > $2) \
             order by column_order",
        )
        .bind(tid)
        .bind(at.0)
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(TableSchema {
            columns: rows
                .iter()
                .map(|r| ColumnDef {
                    order: r.get("column_order"),
                    name: r.get("column_name"),
                    ty: r.get("column_type"),
                    nullable: r.get("nulls_allowed"),
                })
                .collect(),
        })
    }
}

impl PgControlPlane {
    /// Resolve the `table_id` of `table` live at snapshot `at`, or `NotFound`.
    async fn resolve_table(&self, table: &TableRef, at: SnapshotId) -> Result<i64> {
        sqlx::query_scalar::<_, i64>(
            "select t.table_id from ducklake_table t join ducklake_schema s on t.schema_id = s.schema_id \
             where s.schema_name = $1 and t.table_name = $2 \
               and t.begin_snapshot <= $3 and (t.end_snapshot is null or t.end_snapshot > $3)",
        )
        .bind(&table.schema)
        .bind(&table.name)
        .bind(at.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?
        .ok_or_else(|| {
            ControlPlaneError::NotFound(format!("{}.{} @ {}", table.schema, table.name, at.0))
        })
    }
}

fn row_to_snapshot(row: &sqlx::postgres::PgRow) -> Snapshot {
    Snapshot {
        id: SnapshotId(row.get("snapshot_id")),
        time: row.get("snapshot_time"),
        schema_version: row.get("schema_version"),
    }
}
```

- [ ] **Step 4: Add the catalog test target**

In `src/control-plane/postgres/BUCK`, add the `catalog` rust_test (same env as `ducklake-smoke`, plus `testkit`/`core`/`async-trait`/`serde_json` deps for the contract + newtype):

```python
rust_test(
    name = "catalog",
    crate = "catalog",
    srcs = ["tests/catalog.rs"],
    crate_root = "tests/catalog.rs",
    edition = "2024",
    env = {
        "POSTGRES_BIN_DIR": "$(location :postgres-bin)/bin",
        "POSTGRES_LD_LIBRARY_PATH": "$(location :postgres-bin)/lib:$(location :libxml2)",
        "LOOM_MIGRATIONS_DIR": "$(location :migrations)/migrations",
        "DUCKDB_BIN": "$(location :duckdb-cli)",
        "DUCKDB_EXTENSION_DIR": "$(location :duckdb-extensions)",
    },
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//src/control-plane/testkit:testkit",
        "//third-party:async-trait",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 5: Run the contract — red→green**

```bash
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/postgres:catalog 2>&1 | tail -25
```
Expected: `postgres_passes_catalog_contract` passes — the pg adapter reads the real DuckLake catalog and satisfies the same contract as the fake.

- [ ] **Step 6: Full build, test, format, lint**

```bash
cargo fmt --all
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/control-plane/...
tools/clippy-all.sh
```
Expected: the whole control plane builds & passes (queue, worker, memory catalog, ducklake-smoke, pg catalog); clippy clean.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat(control-plane): Postgres Catalog adapter over DuckLake (passes the contract)"
```
(Append the Co-Authored-By trailer.)

---

## Final verification (after all tasks)

```bash
cargo fmt --all --check
env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...
tools/clippy-all.sh
buck2 run //tools:prek -- run --all-files
```

All green ⇒ ready to push and open a PR. Note for the PR body: 2b adds a test-only DuckDB CLI + extensions via `http_file` (linux-x86_64), so `affected`/`build-test` will materialize them on first run; the catalog/smoke tests are local-only (DuckDB, like Postgres, won't run as root on RE).

## Notes / gotchas (from the spike)

- **`DATA_INLINING_ROW_LIMIT 0` is mandatory** in the seeder — without it small batches inline and produce no `ducklake_data_file`, so `files()` returns nothing and the contract fails.
- **`column_type` is opaque** (`varchar`/`int64`) — Task 1 already relaxed the contract; don't reintroduce a literal type assertion.
- **Unqualified `ducklake_*`** in the adapter SQL resolves to `public` via the default search_path; loom's own `queue` schema (created by migrations in the same db) does not collide.
- **`path` is relative** in `ducklake_data_file` (`path_is_relative=true`); `FileRef.path` returns it as stored — resolving against the data root is a future concern, out of scope here.
- **Genrules are `uses_xz`-labelled** (forced local) because the catalog tests are local-only anyway and to avoid assuming `gzip` on RE; keep them off the common RE build path.
- The `duckdb-smoke` target exists to keep the fragile plumbing independently verifiable; do not delete it when the adapter lands — it localizes failures (binary/extension vs. adapter SQL).
```
