# Governed Object-Read Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up loom's first read service — `GET /objects/{type}` resolves an ontology type to its DuckLake table, compiles a minimal ACL row-filter + column projection into SQL, runs it on a DuckDB serving engine behind a swappable seam, and returns JSON rows.

**Architecture:** A new `src/services/query-api` crate consumes the control-plane library (`Ontology`, `Acl`, `Catalog` on `PgControlPlane`). A `ServingEngine` trait abstracts "run read-only SQL against loom's DuckLake catalog"; its first impl embeds DuckDB. A socket-free handler core (`read_object`) does resolve → policy → SQL-compile → execute → JSON; a thin axum layer exposes it over HTTP.

**Tech Stack:** Rust, buck2, axum (HTTP), DuckDB (`duckdb-rs` embedded, or the vendored DuckDB CLI as a subprocess fallback — decided by Task 1), the existing `control-plane-core`/`control-plane-postgres` crates, hermetic Postgres + DuckLake via `PgFixture`.

**Reference spec:** `docs/superpowers/specs/2026-06-10-query-governed-object-read-slice-design.md`.

**Conventions inherited from this repo (do not deviate):**
- Tests are `rust_test` integration targets only (no inline `#[cfg(test)]` runner under buck2). Run hermetic tests with `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...` (the bundled Postgres/DuckDB refuse to run as root on RE).
- **rustfmt is a check-only commit hook** — run `buck2 run //tools:rustfmt -- <files>` before every commit or the commit aborts silently.
- Lint: `tools/clippy-all.sh` and `buck2 run //tools:prek -- run --all-files`.
- Conventional Commits; every commit ends with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Adding a third-party crate: add it to the crate's `Cargo.toml` AND the workspace `Cargo.toml` members if new, run `buck2 run //tools:reindeer -- update` (refresh `Cargo.lock`), then `./tools/buckify.sh`, then depend on it as `//third-party:<crate>`. The `reindeer-check` hook fails if `Cargo.toml`/`Cargo.lock` and `third-party/BUCK` drift.
- Never weaken a test assertion to make it pass.

---

## File Structure

- `Cargo.toml` (workspace) — **Modify:** add `src/services/query-api` to `members`.
- `src/services/query-api/Cargo.toml` — **Create:** crate manifest.
- `src/services/query-api/BUCK` — **Create:** `rust_library` (the slice's logic) + `rust_binary` (the server) + `rust_test` targets.
- `src/services/query-api/src/lib.rs` — **Create:** module wiring + re-exports.
- `src/services/query-api/src/serving.rs` — **Create:** `ServingEngine` trait, `Rows`, `Row`, `SqlValue`, `ServingError`, and the chosen DuckDB impl (`EmbeddedDuckDb`).
- `src/services/query-api/src/sql.rs` — **Create:** pure SQL compilation — `compile_select` (ACL `RowFilter` + projection + request equality filters → SQL string + bound params). No I/O.
- `src/services/query-api/src/handler.rs` — **Create:** `read_object` core + `QueryError`, `Subject`, `ObjectQuery`. Socket-free.
- `src/services/query-api/src/http.rs` — **Create:** axum router + the `GET /objects/{type}` handler.
- `src/services/query-api/src/main.rs` — **Create:** binary entrypoint (bind + serve).
- `src/services/query-api/tests/serving_engine.rs` — **Create:** engine mechanics (fetch + param binding).
- `src/services/query-api/tests/governed_read.rs` — **Create:** the end-to-end governed-read test (the slice's oracle).
- `src/services/query-api/tests/http_smoke.rs` — **Create:** one axum route test via `tower::ServiceExt::oneshot` (no socket).

---

## Task 1: DuckDB serving-engine spike (go/no-go) — pick the impl

**Why first:** the one real unknown (spec §"Key risk") is whether `duckdb-rs` builds under buck2/reindeer at a DuckDB version that loads loom's vendored `ducklake` extension (ABI-locked to 1.5.3). This task answers it on the thinnest possible probe and **selects the `ServingEngine` impl strategy the rest of the plan uses**. The handler/SQL tasks depend only on the `ServingEngine` *trait*, so this choice stays local.

**Files:**
- Create: `src/services/query-api/Cargo.toml`, `src/services/query-api/BUCK`, `src/services/query-api/src/lib.rs`
- Modify: `Cargo.toml` (workspace members)
- Create: `src/services/query-api/tests/spike_duckdb.rs`

- [ ] **Step 1: Scaffold the crate (empty, compiling).**

`Cargo.toml`:
```toml
[package]
name = "query-api"
version = "0.1.0"
edition = "2024"

[dependencies]
control-plane-core = { path = "../../control-plane/core" }
control-plane-postgres = { path = "../../control-plane/postgres" }
async-trait = "0.1"
tokio = { version = "1", features = ["rt", "rt-multi-thread", "macros"] }

[dev-dependencies]
tokio = { version = "1", features = ["rt", "rt-multi-thread", "macros"] }
```

`src/lib.rs`:
```rust
//! loom query-api: the governed read service. A Rust HTTP chokepoint that resolves
//! ontology types, applies ACL policy by generating SQL, and runs it on a DuckDB
//! serving engine. See docs/superpowers/specs/2026-06-10-query-governed-object-read-slice-design.md.
```

Add `"src/services/query-api"` to the workspace `Cargo.toml` `members` list.

- [ ] **Step 2: Add the `duckdb` dep and regenerate buck rules.**

Add to `src/services/query-api/Cargo.toml` `[dependencies]`:
```toml
# Embedded DuckDB serving engine. `bundled` compiles DuckDB from source so no system
# lib is needed; Task 1 verifies the bundled version loads loom's 1.5.3 ducklake extension.
duckdb = { version = "1", features = ["bundled"] }
```
Then:
```bash
buck2 run //tools:reindeer -- update
./tools/buckify.sh
```
Expected: `third-party/BUCK` now defines `//third-party:duckdb` (+ its transitive crates). If `buckify.sh` errors on `duckdb`'s build script, that is signal for Step 4's decision, not a blocker to record.

- [ ] **Step 3: Write the spike test — embedded DuckDB reads loom's catalog.**

`src/services/query-api/tests/spike_duckdb.rs`:
```rust
//! GO/NO-GO spike: can an embedded duckdb-rs at our pinned version ATTACH loom's
//! DuckLake catalog (with the vendored 1.5.3 ducklake extension) and read it?
//! Mirrors the ATTACH preamble proven in postgres/tests/ducklake_interop.rs.

use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};

#[tokio::test(flavor = "multi_thread")]
async fn embedded_duckdb_attaches_loom_catalog() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await; // creates the 27 ducklake_* tables + `main` schema

    let ext_dir = std::env::var("DUCKDB_EXTENSION_DIR").expect("DUCKDB_EXTENSION_DIR");
    let data_dir = tempfile::tempdir().unwrap();
    let attach = format!(
        "SET extension_directory='{}';\nLOAD ducklake;\nLOAD postgres_scanner;\n\
         ATTACH 'ducklake:postgres:dbname={} host={} user=postgres' AS lake \
         (DATA_PATH '{}/', DATA_INLINING_ROW_LIMIT 0);",
        ext_dir,
        db,
        fx.socket_path().display(),
        data_dir.path().display(),
    );

    // Run on a blocking thread: duckdb-rs is synchronous.
    tokio::task::spawn_blocking(move || {
        let conn = duckdb::Connection::open_in_memory().expect("open duckdb");
        conn.execute_batch(&attach).expect("ATTACH loom catalog");
        let n: i64 = conn
            .query_row("SELECT count(*) FROM lake.information_schema.tables", [], |r| r.get(0))
            .expect("read catalog");
        assert!(n >= 0, "catalog readable");
    })
    .await
    .unwrap();
}
```

Add the `rust_test` target to `src/services/query-api/BUCK`:
```python
rust_test(
    name = "spike-duckdb",
    crate = "spike_duckdb",
    srcs = ["tests/spike_duckdb.rs"],
    crate_root = "tests/spike_duckdb.rs",
    edition = "2024",
    env = {
        "POSTGRES_BIN_DIR": "$(location //src/control-plane/postgres:postgres-bin)/bin",
        "POSTGRES_LD_LIBRARY_PATH": "$(location //src/control-plane/postgres:postgres-bin)/lib:$(location //src/control-plane/postgres:libxml2)",
        "LOOM_MIGRATIONS_DIR": "$(location //src/control-plane/postgres:migrations)/migrations",
        "DUCKDB_BIN": "$(location //src/control-plane/postgres:duckdb-cli)",
        "DUCKDB_EXTENSION_DIR": "$(location //src/control-plane/postgres:duckdb-extensions)",
    },
    deps = [
        ":query-api",
        "//src/control-plane/postgres:postgres",
        "//third-party:duckdb",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```
Add `tempfile = "3"` to `[dev-dependencies]` and re-run the reindeer/buckify flow if `//third-party:tempfile` is not yet visible (it is already used by postgres, so the alias exists).

- [ ] **Step 4: Run the spike — decide the impl.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/services/query-api:spike-duckdb`

- **PASS** → the `ServingEngine` impl is **`EmbeddedDuckDb` (duckdb-rs)**; proceed. Record the resolved DuckDB version `duckdb-rs` bundles in a comment in `serving.rs`.
- **FAIL because the bundled version ≠ 1.5.3 / the extension won't load** → try, in order: (a) pin `duckdb` to the crate version whose bundled DuckDB is 1.5.x and re-test; (b) switch `duckdb` off `bundled` and link loom's vendored 1.5.3 libs.
- **FAIL because `duckdb` won't build under buck2/reindeer at all, after a ~60-min timebox** → **fallback impl: `CliDuckDb`** — a `ServingEngine` that shells the vendored `//src/control-plane/postgres:duckdb-cli` exactly as `postgres/tests/ducklake_interop.rs` does (guaranteed 1.5.3, zero new build risk). Drop the `duckdb` dep, keep the seam identical, and note in `serving.rs` that `duckdb-rs` is a later optimization. **Do not silently work around a build failure — if you hit this branch, report it (BLOCKED→fallback) in your status.**

- [ ] **Step 5: Commit.**
```bash
buck2 run //tools:rustfmt -- src/services/query-api/tests/spike_duckdb.rs
git add Cargo.toml Cargo.lock third-party/BUCK src/services/query-api
git commit -m "spike(query-api): embedded DuckDB reads loom's DuckLake catalog

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: `ServingEngine` seam + the chosen DuckDB impl

**Files:**
- Create: `src/services/query-api/src/serving.rs`
- Modify: `src/services/query-api/src/lib.rs`, `src/services/query-api/BUCK`
- Create: `src/services/query-api/tests/serving_engine.rs`

- [ ] **Step 1: Write the failing test (engine mechanics + injection-safe binding).**

`src/services/query-api/tests/serving_engine.rs`:
```rust
//! ServingEngine mechanics: a plain SELECT returns typed rows, and caller values
//! are BOUND (a value containing a quote cannot break or inject into the SQL).

use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use query_api::serving::{EmbeddedDuckDb, ServingEngine, SqlValue};

async fn engine(fx: &PgFixture) -> EmbeddedDuckDb {
    let (_cp, db) = fx.fresh_db().await;
    DuckLakeWriter::new(fx.socket_path(), &db).bootstrap().await;
    // No catalog data needed for these mechanics tests; any dir satisfies DATA_PATH.
    EmbeddedDuckDb::attach(fx.socket_path(), &db, &std::env::temp_dir()).await.expect("attach")
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_rows_returns_typed_cells() {
    let fx = PgFixture::start();
    let eng = engine(&fx).await;
    let rows = eng.fetch_rows("SELECT 42 AS n, 'hi' AS s", &[]).await.unwrap();
    assert_eq!(rows.columns, vec!["n".to_string(), "s".to_string()]);
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], SqlValue::Int(42));
    assert_eq!(rows.rows[0][1], SqlValue::Text("hi".into()));
}

#[tokio::test(flavor = "multi_thread")]
async fn params_are_bound_not_interpolated() {
    let fx = PgFixture::start();
    let eng = engine(&fx).await;
    let evil = SqlValue::Text("x'; DROP TABLE lake.t; --".into());
    let rows = eng.fetch_rows("SELECT ? AS v", &[evil.clone()]).await.unwrap();
    assert_eq!(rows.rows[0][0], evil, "value round-trips as a literal, not SQL");
}
```

- [ ] **Step 2: Run it — verify it fails.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/services/query-api:serving-engine`
Expected: FAIL to compile (`query_api::serving` does not exist).

- [ ] **Step 3: Implement `serving.rs` (duckdb-rs impl shown; CliDuckDb if Task 1 chose the fallback).**

`src/services/query-api/src/serving.rs`:
```rust
//! The serving-engine seam: run read-only SQL against loom's DuckLake catalog.
//! One impl now (EmbeddedDuckDb); a Quack-client impl drops in later unchanged.

use async_trait::async_trait;

/// A backend-neutral scalar cell. Scalar-only by design (lists are expanded into
/// placeholders before binding — see sql::compile_select).
#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Text(String),
    Int(i64),
    Bool(bool),
    Null,
}

/// A result set: column names plus rows of cells (row-major, aligned to `columns`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Rows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ServingError {
    #[error("serving engine: {0}")]
    Engine(String),
}

#[async_trait]
pub trait ServingEngine: Send + Sync {
    /// Execute read-only `sql`, binding `params` positionally (`?` placeholders).
    async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError>;
}

/// Embedded DuckDB that has ATTACHed loom's DuckLake catalog read-only.
/// duckdb-rs is synchronous; calls run on a blocking thread. A fresh connection
/// per query keeps the slice simple (pool later).
pub struct EmbeddedDuckDb {
    attach_sql: String,
}

impl EmbeddedDuckDb {
    /// `data_path` must match the dir the writer used (relative file paths resolve
    /// under it). For data-free reads (e.g. SELECT 42) any existing dir works.
    pub async fn attach(
        socket: &std::path::Path,
        db: &str,
        data_path: &std::path::Path,
    ) -> Result<Self, ServingError> {
        let ext_dir = std::env::var("DUCKDB_EXTENSION_DIR")
            .map_err(|_| ServingError::Engine("DUCKDB_EXTENSION_DIR unset".into()))?;
        let attach_sql = format!(
            "SET extension_directory='{}';\nLOAD ducklake;\nLOAD postgres_scanner;\n\
             ATTACH 'ducklake:postgres:dbname={} host={} user=postgres' AS lake \
             (DATA_PATH '{}/', DATA_INLINING_ROW_LIMIT 0);",
            ext_dir, db, socket.display(), data_path.display(),
        );
        Ok(Self { attach_sql })
    }
}

#[async_trait]
impl ServingEngine for EmbeddedDuckDb {
    async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError> {
        let attach = self.attach_sql.clone();
        let sql = sql.to_string();
        let params = params.to_vec();
        tokio::task::spawn_blocking(move || run_sync(&attach, &sql, &params))
            .await
            .map_err(|e| ServingError::Engine(format!("join: {e}")))?
    }
}

fn run_sync(attach: &str, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError> {
    use duckdb::types::Value;
    let conn = duckdb::Connection::open_in_memory()
        .map_err(|e| ServingError::Engine(e.to_string()))?;
    conn.execute_batch(attach).map_err(|e| ServingError::Engine(e.to_string()))?;
    let mut stmt = conn.prepare(sql).map_err(|e| ServingError::Engine(e.to_string()))?;
    let bound: Vec<Value> = params.iter().map(to_duck).collect();
    let pref: Vec<&dyn duckdb::ToSql> = bound.iter().map(|v| v as &dyn duckdb::ToSql).collect();
    let mut q = stmt.query(pref.as_slice()).map_err(|e| ServingError::Engine(e.to_string()))?;

    let mut columns: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<SqlValue>> = Vec::new();
    while let Some(row) = q.next().map_err(|e| ServingError::Engine(e.to_string()))? {
        if columns.is_empty() {
            columns = row.as_ref().column_names().iter().map(|s| s.to_string()).collect();
        }
        let mut cells = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            let v: Value = row.get(i).map_err(|e| ServingError::Engine(e.to_string()))?;
            cells.push(from_duck(v));
        }
        rows.push(cells);
    }
    Ok(Rows { columns, rows })
}

fn to_duck(v: &SqlValue) -> duckdb::types::Value {
    use duckdb::types::Value;
    match v {
        SqlValue::Text(s) => Value::Text(s.clone()),
        SqlValue::Int(i) => Value::BigInt(*i),
        SqlValue::Bool(b) => Value::Boolean(*b),
        SqlValue::Null => Value::Null,
    }
}

fn from_duck(v: duckdb::types::Value) -> SqlValue {
    use duckdb::types::Value;
    match v {
        Value::Null => SqlValue::Null,
        Value::Boolean(b) => SqlValue::Bool(b),
        Value::TinyInt(i) => SqlValue::Int(i as i64),
        Value::SmallInt(i) => SqlValue::Int(i as i64),
        Value::Int(i) => SqlValue::Int(i as i64),
        Value::BigInt(i) => SqlValue::Int(i),
        Value::Text(s) => SqlValue::Text(s),
        other => SqlValue::Text(format!("{other:?}")),
    }
}
```
Add to `src/lib.rs`: `pub mod serving;`. Add `thiserror = "1"` to `Cargo.toml` deps + reindeer/buckify. (If Task 1 selected `CliDuckDb`, implement `ServingEngine` by invoking `//src/control-plane/postgres:duckdb-cli` with the preamble + `-json` output, parse stdout; bind params by escaping into literals in one centralized `escape()` — keep the `params_are_bound_not_interpolated` test, satisfied by correct escaping.)

- [ ] **Step 4: Add the `rust_test` + library targets to BUCK.**

In `src/services/query-api/BUCK` add the `rust_library` (if not already from Task 1) and the test target:
```python
rust_library(
    name = "query-api",
    crate = "query_api",
    srcs = glob(["src/**/*.rs"]),
    crate_root = "src/lib.rs",
    edition = "2024",
    deps = [
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:async-trait",
        "//third-party:duckdb",
        "//third-party:thiserror",
        "//third-party:tokio",
    ],
    visibility = ["PUBLIC"],
)

rust_test(
    name = "serving-engine",
    crate = "serving_engine",
    srcs = ["tests/serving_engine.rs"],
    crate_root = "tests/serving_engine.rs",
    edition = "2024",
    env = {
        "POSTGRES_BIN_DIR": "$(location //src/control-plane/postgres:postgres-bin)/bin",
        "POSTGRES_LD_LIBRARY_PATH": "$(location //src/control-plane/postgres:postgres-bin)/lib:$(location //src/control-plane/postgres:libxml2)",
        "LOOM_MIGRATIONS_DIR": "$(location //src/control-plane/postgres:migrations)/migrations",
        "DUCKDB_BIN": "$(location //src/control-plane/postgres:duckdb-cli)",
        "DUCKDB_EXTENSION_DIR": "$(location //src/control-plane/postgres:duckdb-extensions)",
    },
    deps = [
        ":query-api",
        "//src/control-plane/postgres:postgres",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 5: Run the test — verify it passes.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/services/query-api:serving-engine`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit.**
```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/serving.rs src/services/query-api/src/lib.rs src/services/query-api/tests/serving_engine.rs
git add -A
git commit -m "feat(query-api): ServingEngine seam + embedded DuckDB impl

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: SQL compilation (`sql.rs`) — ACL filter + projection → bound SQL

Pure, I/O-free, unit-tested as a `rust_test` with no fixture. This is where injection safety is enforced: identifiers come from trusted ontology/ACL metadata; all caller values become bound `?` params.

**Files:**
- Create: `src/services/query-api/src/sql.rs`
- Modify: `src/services/query-api/src/lib.rs`, `BUCK`
- Create: `src/services/query-api/tests/sql_compile.rs`

- [ ] **Step 1: Write the failing test.**

`src/services/query-api/tests/sql_compile.rs`:
```rust
use control_plane_core::{CompareOp, RowFilter, ScalarValue, TableRef};
use query_api::serving::SqlValue;
use query_api::sql::compile_select;

fn t() -> TableRef { TableRef { schema: "main".into(), name: "orders".into() } }

#[test]
fn projects_allowed_columns_and_quotes_identifiers() {
    let (sql, params) = compile_select(&t(), &["id".into(), "status".into()], &[], &[], 100);
    assert_eq!(sql, r#"SELECT "id", "status" FROM "main"."orders" LIMIT 100"#);
    assert!(params.is_empty());
}

#[test]
fn compiles_acl_compare_leaf_as_bound_param() {
    let f = RowFilter::Compare {
        property: "status".into(),
        op: CompareOp::Eq,
        value: ScalarValue::Text("open".into()),
    };
    let (sql, params) = compile_select(&t(), &["id".into()], std::slice::from_ref(&f), &[], 100);
    assert_eq!(sql, r#"SELECT "id" FROM "main"."orders" WHERE ("status" = ?) LIMIT 100"#);
    assert_eq!(params, vec![SqlValue::Text("open".into())]);
}

#[test]
fn compiles_and_or_not_tree() {
    let f = RowFilter::And(vec![
        RowFilter::Compare { property: "a".into(), op: CompareOp::Eq, value: ScalarValue::Int(1) },
        RowFilter::Or(vec![
            RowFilter::Compare { property: "b".into(), op: CompareOp::Gt, value: ScalarValue::Int(2) },
            RowFilter::Not(Box::new(RowFilter::Compare {
                property: "c".into(), op: CompareOp::IsNull, value: ScalarValue::Bool(true),
            })),
        ]),
    ]);
    let (sql, params) = compile_select(&t(), &["a".into()], std::slice::from_ref(&f), &[], 10);
    assert_eq!(
        sql,
        r#"SELECT "a" FROM "main"."orders" WHERE (("a" = ?) AND (("b" > ?) OR (NOT ("c" IS NULL)))) LIMIT 10"#
    );
    assert_eq!(params, vec![SqlValue::Int(1), SqlValue::Int(2)]);
}

#[test]
fn expands_in_list_into_placeholders() {
    let f = RowFilter::Compare {
        property: "region".into(),
        op: CompareOp::In,
        value: ScalarValue::List(vec![ScalarValue::Text("EU".into()), ScalarValue::Text("UK".into())]),
    };
    let (sql, params) = compile_select(&t(), &["id".into()], std::slice::from_ref(&f), &[], 10);
    assert_eq!(sql, r#"SELECT "id" FROM "main"."orders" WHERE ("region" IN (?, ?)) LIMIT 10"#);
    assert_eq!(params, vec![SqlValue::Text("EU".into()), SqlValue::Text("UK".into())]);
}

#[test]
fn ands_acl_filter_with_request_equality_filter() {
    let acl = RowFilter::Compare {
        property: "tenant".into(), op: CompareOp::Eq, value: ScalarValue::Text("acme".into()),
    };
    let eq = vec![("status".to_string(), SqlValue::Text("open".into()))];
    let (sql, params) = compile_select(&t(), &["id".into()], std::slice::from_ref(&acl), &eq, 10);
    assert_eq!(
        sql,
        r#"SELECT "id" FROM "main"."orders" WHERE ("tenant" = ?) AND ("status" = ?) LIMIT 10"#
    );
    assert_eq!(params, vec![SqlValue::Text("acme".into()), SqlValue::Text("open".into())]);
}
```

- [ ] **Step 2: Run it — verify it fails.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/services/query-api:sql-compile`
Expected: FAIL to compile (`query_api::sql` missing).

- [ ] **Step 3: Implement `sql.rs`.**

`src/services/query-api/src/sql.rs`:
```rust
//! Compile an ACL RowFilter tree + a column projection + request equality filters
//! into a single read-only SELECT. Identifiers (table, columns) come ONLY from
//! trusted ontology/ACL metadata and are double-quoted; every caller VALUE is a
//! bound `?` parameter (never interpolated) — this is the injection boundary.

use control_plane_core::{CompareOp, RowFilter, ScalarValue, TableRef};

use crate::serving::SqlValue;

fn quote_ident(id: &str) -> String {
    assert!(!id.contains('"'), "identifier must not contain a double quote: {id}");
    format!("\"{id}\"")
}

fn scalar(v: &ScalarValue, out: &mut Vec<SqlValue>) {
    match v {
        ScalarValue::Text(s) => out.push(SqlValue::Text(s.clone())),
        ScalarValue::Int(i) => out.push(SqlValue::Int(*i)),
        ScalarValue::Bool(b) => out.push(SqlValue::Bool(*b)),
        ScalarValue::List(_) => unreachable!("lists handled by In/NotIn arm"),
    }
}

fn op_sql(op: CompareOp) -> &'static str {
    match op {
        CompareOp::Eq => "=", CompareOp::Ne => "<>",
        CompareOp::Lt => "<", CompareOp::Le => "<=",
        CompareOp::Gt => ">", CompareOp::Ge => ">=",
        _ => unreachable!("In/NotIn/IsNull/IsNotNull handled separately"),
    }
}

fn filter_sql(f: &RowFilter, params: &mut Vec<SqlValue>) -> String {
    match f {
        RowFilter::Compare { property, op, value } => match op {
            CompareOp::In | CompareOp::NotIn => {
                let items = match value {
                    ScalarValue::List(xs) => xs,
                    _ => panic!("In/NotIn requires a list value"),
                };
                let mut placeholders = Vec::with_capacity(items.len());
                for it in items {
                    scalar(it, params);
                    placeholders.push("?");
                }
                let kw = if matches!(op, CompareOp::In) { "IN" } else { "NOT IN" };
                format!("({} {} ({}))", quote_ident(property), kw, placeholders.join(", "))
            }
            CompareOp::IsNull => format!("({} IS NULL)", quote_ident(property)),
            CompareOp::IsNotNull => format!("({} IS NOT NULL)", quote_ident(property)),
            _ => {
                scalar(value, params);
                format!("({} {} ?)", quote_ident(property), op_sql(*op))
            }
        },
        RowFilter::And(xs) => format!(
            "({})",
            xs.iter().map(|x| filter_sql(x, params)).collect::<Vec<_>>().join(" AND ")
        ),
        RowFilter::Or(xs) => format!(
            "({})",
            xs.iter().map(|x| filter_sql(x, params)).collect::<Vec<_>>().join(" OR ")
        ),
        RowFilter::Not(x) => format!("(NOT {})", filter_sql(x, params)),
    }
}

/// `allowed_cols` must be non-empty (caller enforces). `row_filters` and `eq_filters`
/// are ANDed together as conjuncts.
pub fn compile_select(
    table: &TableRef,
    allowed_cols: &[String],
    row_filters: &[RowFilter],
    eq_filters: &[(String, SqlValue)],
    limit: u32,
) -> (String, Vec<SqlValue>) {
    let mut params = Vec::new();
    let cols = allowed_cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let from = format!("{}.{}", quote_ident(&table.schema), quote_ident(&table.name));

    let mut conjuncts: Vec<String> = Vec::new();
    for f in row_filters {
        conjuncts.push(filter_sql(f, &mut params));
    }
    for (col, val) in eq_filters {
        conjuncts.push(format!("({} = ?)", quote_ident(col)));
        params.push(val.clone());
    }

    let mut sql = format!("SELECT {cols} FROM {from}");
    if !conjuncts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conjuncts.join(" AND "));
    }
    sql.push_str(&format!(" LIMIT {limit}"));
    (sql, params)
}
```
Add `pub mod sql;` to `src/lib.rs`.

- [ ] **Step 4: Add the test target + run.**

Add to `BUCK`:
```python
rust_test(
    name = "sql-compile",
    crate = "sql_compile",
    srcs = ["tests/sql_compile.rs"],
    crate_root = "tests/sql_compile.rs",
    edition = "2024",
    deps = [":query-api", "//src/control-plane/core:core"],
)
```
Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/services/query-api:sql-compile`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit.**
```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/sql.rs src/services/query-api/src/lib.rs src/services/query-api/tests/sql_compile.rs
git add -A
git commit -m "feat(query-api): SQL compilation for ACL filters + projection (bound params)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Handler core (`handler.rs`) — `read_object` end to end

Wires ontology resolve + ACL policy + sql compile + serving. The integration test is the slice's **oracle**: real Postgres, real DuckLake catalog, real Parquet, governed result.

**Files:**
- Create: `src/services/query-api/src/handler.rs`
- Modify: `src/services/query-api/src/lib.rs`, `BUCK`, `Cargo.toml`
- Create: `src/services/query-api/tests/governed_read.rs`

- [ ] **Step 1: Write the failing test (the oracle).**

`src/services/query-api/tests/governed_read.rs`:
```rust
//! THE governed-read oracle: an ontology type resolves to a DuckLake table; an ACL
//! policy (row filter + denied column) shapes the result; a request equality filter
//! narrows it. Seeds real rows via the snapshot-commit primitive + a DuckDB-written
//! Parquet (mirrors postgres/tests/ducklake_interop.rs::duckdb_scans_loom_appended_file).

use control_plane_core::{
    Acl, Action, ColumnSpec, ColumnStat, DataFile, ObjectType, Ontology, Policy, PolicyTarget,
    PropertyDef, RoleId, RowFilter, CompareOp, ScalarValue, SubjectId, TableRef, Tx, TypeName,
};
use control_plane_postgres::fixture::{DuckLakeWriter, PgFixture};
use query_api::handler::{read_object, ObjectQuery, QueryDeps, Subject};
use query_api::serving::{EmbeddedDuckDb, SqlValue};

#[tokio::test(flavor = "multi_thread")]
async fn governed_object_read() {
    let fx = PgFixture::start();
    let (cp, db) = fx.fresh_db().await;
    let writer = DuckLakeWriter::new(fx.socket_path(), &db);
    writer.bootstrap().await;

    let table = TableRef { schema: "main".into(), name: "orders".into() };

    // 1. loom natively creates the table (id, status, secret) and registers a Parquet.
    let mut tx = cp.begin().await.unwrap();
    tx.create_table(&table, &[
        ColumnSpec { name: "id".into(), ty: "int64".into(), nullable: false },
        ColumnSpec { name: "status".into(), ty: "varchar".into(), nullable: true },
        ColumnSpec { name: "secret".into(), ty: "varchar".into(), nullable: true },
    ]).await.unwrap();
    tx.commit().await.unwrap().expect("create snapshot");

    // 2. DuckDB writes the Parquet at the resolved relative path, loom registers it.
    let dir = writer.data_path().join("main").join("orders");
    std::fs::create_dir_all(&dir).unwrap();
    let abs = dir.join("o.parquet");
    writer.exec(&format!(
        "COPY (SELECT * FROM (VALUES \
           (1, 'open', 's1'), (2, 'closed', 's2'), (3, 'open', 's3')) \
           AS t(id, status, secret)) TO '{}' (FORMAT parquet);",
        abs.display()
    )).await;
    let bytes = std::fs::read(&abs).unwrap();
    let footer = {
        let l = &bytes[bytes.len() - 8..bytes.len() - 4];
        u32::from_le_bytes(l.try_into().unwrap()) as i64
    };
    let mut tx = cp.begin().await.unwrap();
    tx.append_files(&table, &[DataFile {
        path: "o.parquet".into(), path_is_relative: true, record_count: 3,
        file_size_bytes: bytes.len() as i64, footer_size: footer,
        column_stats: vec![
            ColumnStat { column_name: "id".into(), min: Some("1".into()), max: Some("3".into()), null_count: 0, value_count: 3, column_size_bytes: 24 },
            ColumnStat { column_name: "status".into(), min: Some("closed".into()), max: Some("open".into()), null_count: 0, value_count: 3, column_size_bytes: 24 },
            ColumnStat { column_name: "secret".into(), min: Some("s1".into()), max: Some("s3".into()), null_count: 0, value_count: 3, column_size_bytes: 24 },
        ],
    }]).await.unwrap();
    tx.commit().await.unwrap().expect("append snapshot");

    // 3. Ontology: type Order -> main.orders, with properties id/status/secret.
    cp.define_type(ObjectType {
        name: TypeName("Order".into()),
        properties: vec![
            PropertyDef { name: "id".into(), ty: "Long".into(), required: true },
            PropertyDef { name: "status".into(), ty: "String".into(), required: false },
            PropertyDef { name: "secret".into(), ty: "String".into(), required: false },
        ],
        table: table.clone(),
    }).await.unwrap();

    // 4. ACL: subject `analyst` in role `analysts`; policy on type Order denies `secret`
    //    and restricts rows to status = 'open'.
    let subj = SubjectId("analyst".into());
    let role = RoleId("analysts".into());
    cp.define_subject(&subj).await.unwrap();
    cp.define_role(&role).await.unwrap();
    cp.assign_role(&subj, &role).await.unwrap();
    cp.grant(&role, Action::Read, PolicyTarget::Type(TypeName("Order".into()))).await.unwrap();
    cp.set_policy(&role, Policy {
        target: PolicyTarget::Type(TypeName("Order".into())),
        row_filter: Some(RowFilter::Compare {
            property: "status".into(), op: CompareOp::Eq, value: ScalarValue::Text("open".into()),
        }),
        deny_columns: vec!["secret".into()],
    }).await.unwrap();

    // 5. Read it.
    let eng = EmbeddedDuckDb::attach(fx.socket_path(), &db, writer.data_path()).await.unwrap();
    let deps = QueryDeps { ontology: &cp, acl: &cp, serving: &eng };
    let rows = read_object(
        &ObjectQuery { type_name: "Order".into(), eq_filters: vec![] },
        &Subject(subj.clone()),
        &deps,
    ).await.unwrap();

    // secret projected out; only status='open' rows (ids 1 and 3) returned.
    assert_eq!(rows.columns, vec!["id".to_string(), "status".to_string()]);
    let ids: Vec<&SqlValue> = rows.rows.iter().map(|r| &r[0]).collect();
    assert_eq!(rows.rows.len(), 2, "ACL row filter kept only status=open");
    assert!(ids.contains(&&SqlValue::Int(1)) && ids.contains(&&SqlValue::Int(3)));
    assert!(!ids.contains(&&SqlValue::Int(2)), "closed row filtered out");

    // 6. A request equality filter narrows further.
    let rows2 = read_object(
        &ObjectQuery { type_name: "Order".into(), eq_filters: vec![("id".into(), SqlValue::Int(1))] },
        &Subject(subj),
        &deps,
    ).await.unwrap();
    assert_eq!(rows2.rows.len(), 1);
    assert_eq!(rows2.rows[0][0], SqlValue::Int(1));
}
```

- [ ] **Step 2: Run it — verify it fails.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/services/query-api:governed-read`
Expected: FAIL to compile (`query_api::handler` missing).

- [ ] **Step 3: Implement `handler.rs`.**

`src/services/query-api/src/handler.rs`:
```rust
//! The socket-free governed-read core. resolve type -> table + property columns;
//! load ACL policy -> row filter + denied columns; project allowed columns; compile
//! SQL with bound params; execute on the serving engine.

use control_plane_core::{
    Acl, Ontology, PageReq, PolicyTarget, RowFilter, SubjectId, TypeName,
};

use crate::serving::{Rows, ServingEngine, SqlValue};
use crate::sql::compile_select;

const DEFAULT_LIMIT: u32 = 1000;

/// The authenticated caller (authn is a later spec; carried from a request header).
pub struct Subject(pub SubjectId);

/// A read request: an ontology type plus optional equality filters on allowed columns.
pub struct ObjectQuery {
    pub type_name: String,
    pub eq_filters: Vec<(String, SqlValue)>,
}

/// Borrowed dependencies for one read.
pub struct QueryDeps<'a> {
    pub ontology: &'a (dyn Ontology + Sync),
    pub acl: &'a (dyn Acl + Sync),
    pub serving: &'a (dyn ServingEngine),
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("unknown object type: {0}")]
    UnknownType(String),
    #[error("forbidden")]
    Forbidden,
    #[error("filter column not permitted: {0}")]
    BadFilter(String),
    #[error(transparent)]
    ControlPlane(#[from] control_plane_core::ControlPlaneError),
    #[error(transparent)]
    Serving(#[from] crate::serving::ServingError),
}

pub async fn read_object(
    q: &ObjectQuery,
    subject: &Subject,
    deps: &QueryDeps<'_>,
) -> Result<Rows, QueryError> {
    let type_name = TypeName(q.type_name.clone());

    // resolve: type -> ObjectType (table + ordered properties).
    let object_type = deps
        .ontology
        .get_type(&type_name)
        .await
        .map_err(|_| QueryError::UnknownType(q.type_name.clone()))?;
    let target = PolicyTarget::Type(type_name.clone());

    // policy: gather row filters + denied columns across the subject's matching policies.
    let policies = deps
        .acl
        .policies_for(&subject.0, &target, PageReq::unbounded())
        .await?;
    let mut row_filters: Vec<RowFilter> = Vec::new();
    let mut denied: std::collections::HashSet<String> = std::collections::HashSet::new();
    for p in policies.items {
        if let Some(f) = p.row_filter {
            row_filters.push(f);
        }
        denied.extend(p.deny_columns);
    }

    // projection: type properties minus denied columns, preserving property order.
    let allowed: Vec<String> = object_type
        .properties
        .iter()
        .map(|p| p.name.clone())
        .filter(|n| !denied.contains(n))
        .collect();
    if allowed.is_empty() {
        return Err(QueryError::Forbidden);
    }

    // request equality filters must target an allowed (visible) column.
    for (col, _) in &q.eq_filters {
        if !allowed.contains(col) {
            return Err(QueryError::BadFilter(col.clone()));
        }
    }

    let (sql, params) =
        compile_select(&object_type.table, &allowed, &row_filters, &q.eq_filters, DEFAULT_LIMIT);
    Ok(deps.serving.fetch_rows(&sql, &params).await?)
}
```
Add `pub mod handler;` to `src/lib.rs`. (Note: `Subject` wraps `SubjectId`; an absent request header maps to a subject with no roles → empty policies → since this slice has no public grant model, that yields the type's full column list with no row filter. ACL deny/allow refinement is a later spec; the test always supplies a subject.)

- [ ] **Step 4: Add the test target + run.**

Add to `BUCK` (same env block as `serving-engine`, plus core dep):
```python
rust_test(
    name = "governed-read",
    crate = "governed_read",
    srcs = ["tests/governed_read.rs"],
    crate_root = "tests/governed_read.rs",
    edition = "2024",
    env = {
        "POSTGRES_BIN_DIR": "$(location //src/control-plane/postgres:postgres-bin)/bin",
        "POSTGRES_LD_LIBRARY_PATH": "$(location //src/control-plane/postgres:postgres-bin)/lib:$(location //src/control-plane/postgres:libxml2)",
        "LOOM_MIGRATIONS_DIR": "$(location //src/control-plane/postgres:migrations)/migrations",
        "DUCKDB_BIN": "$(location //src/control-plane/postgres:duckdb-cli)",
        "DUCKDB_EXTENSION_DIR": "$(location //src/control-plane/postgres:duckdb-extensions)",
    },
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:tokio",
    ],
)
```
Add `thiserror = "1"` to `Cargo.toml` if not added in Task 2; re-run reindeer/buckify if needed.
Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/services/query-api:governed-read`
Expected: PASS.

(The `data_path` argument to `EmbeddedDuckDb::attach` — added in Task 2 — is what makes the embedded engine resolve loom's relative file path to the Parquet the writer wrote; here it is `writer.data_path()`.)

- [ ] **Step 5: Commit.**
```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/handler.rs src/services/query-api/src/lib.rs src/services/query-api/tests/governed_read.rs src/services/query-api/src/serving.rs
git add -A
git commit -m "feat(query-api): governed read_object core (ontology + ACL -> SQL -> rows)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: axum HTTP layer + binary

**Files:**
- Create: `src/services/query-api/src/http.rs`, `src/services/query-api/src/main.rs`
- Modify: `src/services/query-api/src/lib.rs`, `Cargo.toml`, `BUCK`
- Create: `src/services/query-api/tests/http_smoke.rs`

- [ ] **Step 1: Add `axum` + `serde`/`serde_json` deps.**

Add to `Cargo.toml`:
```toml
axum = "0.7"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```
Run `buck2 run //tools:reindeer -- update && ./tools/buckify.sh`. Expected: `//third-party:axum`, `//third-party:serde`, `//third-party:serde_json` available.

- [ ] **Step 2: Write the failing route test (no socket; tower oneshot).**

`src/services/query-api/tests/http_smoke.rs`:
```rust
//! The axum route maps a GET into read_object and serializes Rows to JSON.
//! Uses tower::ServiceExt::oneshot so no socket is bound. A stub ServingEngine
//! keeps this test about HTTP wiring, not DuckDB (the DB path is covered by
//! governed-read).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use query_api::http::router;
use query_api::serving::{Rows, ServingEngine, ServingError, SqlValue};
use std::sync::Arc;
use tower::ServiceExt;

// minimal in-memory Ontology + Acl + ServingEngine stubs live in the test.
mod stubs; // see Step 3 note: inline the stubs in this file instead if a submodule is awkward.

#[tokio::test(flavor = "multi_thread")]
async fn get_objects_returns_json_rows() {
    let app = router(stubs::deps());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/objects/Order")
                .header("X-Loom-Subject", "analyst")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["columns"][0], "id");
    assert_eq!(json["rows"][0][0], 1);
}
```
Because the stubs are verbose, **inline them in `http_smoke.rs`** (drop the `mod stubs;`): implement tiny structs `StubOntology` (returns an `ObjectType` for "Order"), `StubAcl` (returns no policies), and `StubServing` (returns `Rows { columns: vec!["id"], rows: vec![vec![SqlValue::Int(1)]] }`), and a `deps()`/router wiring. Use `async_trait` impls returning canned values. (Full stub bodies: mirror the trait signatures from `control_plane_core::{Ontology, Acl}` and `query_api::serving::ServingEngine`, returning the canned data above; every other trait method may `unimplemented!()` since the route calls only `get_type`, `policies_for`, `fetch_rows`.)

- [ ] **Step 3: Implement `http.rs` + `main.rs`.**

`src/services/query-api/src/http.rs`:
```rust
//! Thin axum surface. All logic is in handler::read_object; this layer only maps
//! HTTP <-> the core and serializes Rows to JSON.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use control_plane_core::{Acl, Ontology, SubjectId};
use serde_json::json;

use crate::handler::{read_object, ObjectQuery, QueryError, Subject};
use crate::serving::{Rows, ServingEngine, SqlValue};

/// Shared, owned dependencies (the 'static analog of handler::QueryDeps).
#[derive(Clone)]
pub struct AppState {
    pub ontology: Arc<dyn Ontology + Send + Sync>,
    pub acl: Arc<dyn Acl + Send + Sync>,
    pub serving: Arc<dyn ServingEngine>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/objects/:type_name", get(get_object))
        .with_state(state)
}

async fn get_object(
    State(st): State<AppState>,
    Path(type_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let subject = headers
        .get("X-Loom-Subject")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();
    let eq_filters = params
        .into_iter()
        .map(|(k, v)| (k, SqlValue::Text(v)))
        .collect();
    let deps = crate::handler::QueryDeps {
        ontology: st.ontology.as_ref(),
        acl: st.acl.as_ref(),
        serving: st.serving.as_ref(),
    };
    match read_object(
        &ObjectQuery { type_name, eq_filters },
        &Subject(SubjectId(subject)),
        &deps,
    )
    .await
    {
        Ok(rows) => Json(rows_to_json(&rows)).into_response(),
        Err(QueryError::UnknownType(t)) => (StatusCode::NOT_FOUND, t).into_response(),
        Err(QueryError::Forbidden) => StatusCode::FORBIDDEN.into_response(),
        Err(QueryError::BadFilter(c)) => (StatusCode::BAD_REQUEST, c).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

fn rows_to_json(rows: &Rows) -> serde_json::Value {
    let cells = |r: &Vec<SqlValue>| -> Vec<serde_json::Value> {
        r.iter()
            .map(|c| match c {
                SqlValue::Text(s) => json!(s),
                SqlValue::Int(i) => json!(i),
                SqlValue::Bool(b) => json!(b),
                SqlValue::Null => serde_json::Value::Null,
            })
            .collect()
    };
    json!({
        "columns": rows.columns,
        "rows": rows.rows.iter().map(cells).collect::<Vec<_>>(),
    })
}
```
Note: `handler::QueryDeps` borrows `&dyn`; `AppState` owns `Arc<dyn …>`. Keep both — the route builds a `QueryDeps` from the `Arc`s per request. (Ensure the trait objects in `QueryDeps` are `&(dyn Ontology + Sync)` etc.; `Arc<dyn Ontology + Send + Sync>::as_ref()` coerces fine.)

`src/services/query-api/src/main.rs`:
```rust
//! query-api binary: builds the app state from a Postgres pool + embedded DuckDB
//! and serves the HTTP API. (Config plumbing is intentionally minimal for the slice.)

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("query-api: serving-tier wiring is a later spec; see the spec doc.");
    Ok(())
}
```
(The binary is a placeholder entrypoint for this slice — the testable surface is `router()` + `read_object`. Wiring a real pool + bind address is the serving-tier spec. Keep `main` trivial but compiling.)

Add `pub mod http;` to `src/lib.rs`.

- [ ] **Step 4: Add targets + run.**

Add to `BUCK`:
```python
rust_binary(
    name = "query-api-bin",
    crate = "query_api_bin",
    srcs = ["src/main.rs"],
    crate_root = "src/main.rs",
    edition = "2024",
    deps = [":query-api", "//third-party:tokio"],
    env = {"CARGO_PKG_VERSION": "0.1.0"},
    visibility = ["PUBLIC"],
)

rust_test(
    name = "http-smoke",
    crate = "http_smoke",
    srcs = ["tests/http_smoke.rs"],
    crate_root = "tests/http_smoke.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//third-party:async-trait",
        "//third-party:axum",
        "//third-party:http-body-util",
        "//third-party:serde_json",
        "//third-party:tokio",
        "//third-party:tower",
    ],
)
```
`http-body-util` and `tower` are needed only by the test; add them to `[dev-dependencies]` in `Cargo.toml` and re-run reindeer/buckify. Also add `tokio` to the library deps list and `axum`/`serde`/`serde_json` to the `query-api` `rust_library` deps.
Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/services/query-api:http-smoke`
Expected: PASS.

- [ ] **Step 5: Commit.**
```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/http.rs src/services/query-api/src/main.rs src/services/query-api/src/lib.rs src/services/query-api/tests/http_smoke.rs
git add -A
git commit -m "feat(query-api): axum GET /objects/{type} + JSON serialization

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: Full sweep, lint, final review, finish branch

**Files:** none (verification + finish).

- [ ] **Step 1: Full test sweep.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...`
Expected: all PASS, including the existing control-plane suites and the new `//src/services/query-api:{spike-duckdb, serving-engine, sql-compile, governed-read, http-smoke}`.

- [ ] **Step 2: Lint.**

Run:
```bash
tools/clippy-all.sh
buck2 run //tools:prek -- run --all-files
```
Expected: all PASS (rustfmt, clippy, `reindeer-check` — the `Cargo.toml`/`Cargo.lock`/`third-party/BUCK` are in sync — file checks).

- [ ] **Step 3: Confirm the slice against the spec.** Spot-check: ontology resolution drives the table; ACL row filter + `deny_columns` shape the result (governed-read proves both); values are bound not interpolated (serving-engine + sql-compile prove it); the `ServingEngine` seam is the only DuckDB touch-point (so the Quack-client swap is local). Note in your final message which `ServingEngine` impl Task 1 selected.

- [ ] **Step 4: Finish the branch.** Use superpowers:finishing-a-development-branch (verify tests pass → present options). Branch: `feat/query-governed-object-read-slice` (already carries the spec + roadmap commits).

---

## Self-review notes

- **Spec coverage:** `query-api` crate under `src/services/` (Task 1) · `ServingEngine` seam + embedded DuckDB (Tasks 1–2) · ontology-typed read (Task 4) · minimal ACL = row predicate + column projection (Tasks 3–4) · injection-safe bound params (Tasks 2–3) · plain-HTTP `GET /objects/{type}` + JSON (Task 5) · hermetic test reusing the snapshot-commit primitive to seed real rows (Task 4) · version/extension-ABI-lock risk made the go/no-go Task 1 with a CLI-subprocess fallback. Deferred items (Quack wire, serving tier, actions, full ACL, rich ontology, joins, authn) are not tasked — correct per spec §"What this slice is NOT".
- **Type consistency:** `ServingEngine::fetch_rows(&str, &[SqlValue]) -> Rows` is used identically in Tasks 2/3/4/5; `compile_select(&TableRef, &[String], &[RowFilter], &[(String, SqlValue)], u32)` is stable across Tasks 3–4; `read_object(&ObjectQuery, &Subject, &QueryDeps)` stable across Tasks 4–5; `EmbeddedDuckDb::attach(&Path, &str, &Path)` is the same 3-arg signature in Tasks 2, 3, and 4 (no churn).
