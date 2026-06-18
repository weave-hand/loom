# Iceberg DataFusion Serving Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A loom-native, DataFusion-backed `ServingEngine` that serves governed reads for file-backed Iceberg tables out of the `iceberg_mirror` projection, with no DuckDB in the path, selectable in the query-api binary by config.

**Architecture:** A new `DataFusionServingEngine` (in query-api) owns an `IcebergCatalog` (the mirror reader). On each `fetch_rows`, it enumerates live tables from the mirror, registers each table's live Parquet files (absolute `file://` paths) as a schema-qualified DataFusion `ListingTable`, inlines the query params via the existing injection-safe `inline_params`, runs the SQL through a fresh `SessionContext`, and maps the Arrow result back to the engine-neutral `Rows`. The read handler is unchanged — it already depends only on `ontology`/`acl`/`serving`, never on `cp.catalog()` — so only the binary's serving + action engines swap.

**Tech Stack:** Rust 2024, buck2, DataFusion 54, arrow 58, object_store 0.13, sqlx 0.9 compile-time queries, axum. Tests are `rust_test`/`loom_fixture_test` integration targets (NO inline `#[cfg(test)]`).

**Spec:** `docs/superpowers/specs/2026-06-17-iceberg-datafusion-serving-engine-design.md`

---

## Key facts the implementer must know (loom-specific)

- **No inline tests.** buck2 never runs inline `#[cfg(test)]`. Every test is a sibling `tests/<name>.rs` file wired as its own `rust_test` (pure logic) or `loom_fixture_test` (needs Postgres) target in the crate's `BUCK`. A prek hook fails the build if a first-party `src/**.rs` contains `#[test]`/`#[tokio::test]`.
- **Running tests.** `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`. NEVER pipe `buck2 test` through `tail`/`head` (it stalls). A single target: `buck2 test //src/services/query-api:<name> > /tmp/t.log 2>&1; cat /tmp/t.log`.
- **rustfmt is check-only.** Run `buck2 run //tools:rustfmt -- <files>` to format BEFORE committing, or the prek hook loops on a diff. `rustfmt.toml` pins edition 2024.
- **clippy.** `tools/clippy-all.sh` (or `buck2 build '//src/services/query-api:query-api[clippy.txt]'`).
- **sqlx compile-time queries.** A new `query!`/`query_scalar!` in `control-plane-postgres` requires regenerating the committed `.sqlx` cache: `tools/sqlx-prepare.sh`, then commit `src/control-plane/postgres/.sqlx/`. The `//src/control-plane/postgres:sqlx-cache-check` test enforces freshness.
- **`loom_fixture_test`** (loaded via `load("//src/control-plane/postgres:defs.bzl", "loom_fixture_test")`, already at the top of query-api's `BUCK`) pins the test to local execution and boots a hermetic Postgres. Use it for any test that touches the database. Set `duckdb = True` ONLY if the test needs the DuckDB CLI/extensions — the Iceberg path does NOT (it uses LocalFsStorage), so OMIT `duckdb` for new Iceberg tests (mirror `//src/control-plane/postgres:iceberg-writer`).

---

## File Structure

**Create:**
- `src/services/query-api/src/serving_datafusion.rs` — the whole new engine: `arrow_to_sqlvalue` cell mapping + `batches_to_rows`, `register_iceberg_table`, `DataFusionServingEngine` (`impl ServingEngine`), `UnsupportedActionEngine` (`impl ActionEngine`), `ServingBackend` enum + `parse_serving_backend`.
- `src/services/query-api/tests/datafusion_value_map.rs` — pure unit tests for `arrow_to_sqlvalue`/`batches_to_rows` (builds Arrow arrays directly; no DB).
- `src/services/query-api/tests/datafusion_register.rs` — `loom_fixture_test`: seed an Iceberg table, prove `register_iceberg_table` + a raw DataFusion `SELECT` reads it back (pins absolute-path handling).
- `src/services/query-api/tests/datafusion_serving.rs` — `loom_fixture_test`: seed an Iceberg table, prove `DataFusionServingEngine::fetch_rows` returns correct `Rows` for the exact SQL shape the handler emits.
- `src/services/query-api/tests/serving_backend_parse.rs` — pure unit tests for `parse_serving_backend`.
- `src/services/query-api/tests/unsupported_action.rs` — pure unit test that `UnsupportedActionEngine::insert_row` errors.

**Modify:**
- `src/control-plane/postgres/src/iceberg_catalog.rs` — add `IcebergCatalog::live_tables()`.
- `src/control-plane/postgres/.sqlx/` — regenerated cache (one new query).
- `src/control-plane/postgres/BUCK` — new `loom_fixture_test` for `live_tables` (`tests/iceberg_live_tables.rs`).
- `src/control-plane/postgres/tests/iceberg_live_tables.rs` — CREATE (live-tables test).
- `src/services/query-api/src/lib.rs` — add `pub mod serving_datafusion;`.
- `src/services/query-api/src/main.rs` — config-select the serving + action engines.
- `src/services/query-api/Cargo.toml` — add `datafusion`, `arrow`, `object_store`.
- `src/services/query-api/BUCK` — add lib deps; add the new test targets; add `//src/control-plane/postgres:postgres` to the binary.

**Unchanged (by design):** the `ServingEngine` trait, `serving.rs`, `handler.rs`, `http.rs`, `sql.rs`, all DuckDB engines, the ingest service.

---

## Task 1: `IcebergCatalog::live_tables()`

The engine must enumerate every table currently live in the mirror. `core::Catalog` is per-table and exposes no listing, so add a mirror-specific method on the concrete `IcebergCatalog`.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_catalog.rs`
- Create: `src/control-plane/postgres/tests/iceberg_live_tables.rs`
- Modify: `src/control-plane/postgres/BUCK`
- Modify: `src/control-plane/postgres/.sqlx/` (regen)

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/iceberg_live_tables.rs`. It seeds two Iceberg tables via the existing `IcebergWriter`, drops one, and asserts `live_tables()` returns exactly the live one. Mirror the setup in `src/control-plane/postgres/tests/iceberg_write_roundtrip.rs` for how to build the fixture + `IcebergWriter`.

```rust
//! IcebergCatalog::live_tables returns exactly the tables live in the mirror.
//! `rust_test` integration target (loom_fixture_test), not an inline module.

use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_core::TableRef;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_tables_lists_only_live() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![("id".to_string(), "long".to_string(), false)];
    writer.seed("sales", "orders", &cols, &[3]).await;
    writer.seed("sales", "shipments", &cols, &[2]).await;
    writer.drop_table("sales", "shipments").await;

    let catalog = IcebergCatalog::new(pool);
    let mut live = catalog.live_tables().await.expect("live_tables");
    live.sort_by(|a, b| {
        (a.schema.as_str(), a.name.as_str()).cmp(&(b.schema.as_str(), b.name.as_str()))
    });

    assert_eq!(
        live,
        vec![TableRef { schema: "sales".into(), name: "orders".into() }],
        "only the un-dropped table is live"
    );
}
```

Add the target to `src/control-plane/postgres/BUCK` (place it next to the other `iceberg-*` fixture tests, ~line 380):

```python
loom_fixture_test(
    name = "iceberg-live-tables",
    crate = "iceberg_live_tables",
    srcs = ["tests/iceberg_live_tables.rs"],
    crate_root = "tests/iceberg_live_tables.rs",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/control-plane/postgres:iceberg-live-tables > /tmp/t.log 2>&1; cat /tmp/t.log`
Expected: FAIL — compile error, `no method named live_tables`.

- [ ] **Step 3: Implement `live_tables()`**

In `src/control-plane/postgres/src/iceberg_catalog.rs`, add this method inside `impl IcebergCatalog` (next to `resolve_table`). A row is live when `end_snapshot is null`.

```rust
    /// Every table currently live in the mirror (those with no `end_snapshot`),
    /// as loom `TableRef`s (`table_namespace` -> schema, `table_name` -> name).
    /// The read engine registers each as a DataFusion table.
    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn live_tables(&self) -> Result<Vec<TableRef>> {
        let rows = sqlx::query!(
            "select table_namespace as \"schema!\", table_name as \"name!\" \
             from iceberg_mirror.table where end_snapshot is null \
             order by table_namespace, table_name"
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        Ok(rows
            .into_iter()
            .map(|r| TableRef { schema: r.schema, name: r.name })
            .collect())
    }
```

- [ ] **Step 4: Regenerate the sqlx cache**

Run: `./tools/sqlx-prepare.sh`
Then confirm a new `query-*.json` appeared: `git status --short src/control-plane/postgres/.sqlx/`
Expected: one added `.sqlx/query-*.json`.

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test //src/control-plane/postgres:iceberg-live-tables //src/control-plane/postgres:sqlx-cache-check > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (both).

- [ ] **Step 6: Format, lint, commit**

```bash
buck2 run //tools:rustfmt -- src/control-plane/postgres/src/iceberg_catalog.rs src/control-plane/postgres/tests/iceberg_live_tables.rs
git add src/control-plane/postgres/src/iceberg_catalog.rs src/control-plane/postgres/tests/iceberg_live_tables.rs src/control-plane/postgres/BUCK src/control-plane/postgres/.sqlx/
git commit -m "feat(iceberg): IcebergCatalog::live_tables for mirror enumeration"
```

---

## Task 2: query-api depends on DataFusion + the new module shell

Wire the new dependencies and an empty module so subsequent tasks have a compile target. Keep it minimal — just enough to build.

**Files:**
- Create: `src/services/query-api/src/serving_datafusion.rs` (module doc only)
- Modify: `src/services/query-api/src/lib.rs`
- Modify: `src/services/query-api/Cargo.toml`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Create the module file**

Create `src/services/query-api/src/serving_datafusion.rs`:

```rust
//! loom-native, DataFusion-backed serving engine for file-backed Iceberg tables.
//! Reads the `iceberg_mirror` projection (via `IcebergCatalog`), registers each
//! live table's Parquet files (absolute `file://` paths) as a DataFusion table,
//! and runs the governed/compiled SQL through DataFusion — no DuckDB in the path.
//! See docs/superpowers/specs/2026-06-17-iceberg-datafusion-serving-engine-design.md.
```

- [ ] **Step 2: Declare the module**

In `src/services/query-api/src/lib.rs`, add to the module list (alphabetical, after `serving`):

```rust
pub mod serving_datafusion;
```

- [ ] **Step 3: Add Cargo deps**

In `src/services/query-api/Cargo.toml`, under `[dependencies]`, add (match the versions ingest pins):

```toml
# loom-native serving engine (serving_datafusion.rs): DataFusion reads the Iceberg
# mirror's Parquet files directly — no DuckDB. arrow 58 / object_store 0.13 match
# datafusion 54 (same as the ingest/datafusion-io stack).
arrow = "58"
datafusion = { version = "54", default-features = false, features = ["parquet", "sql"] }
object_store = "0.13"
```

- [ ] **Step 4: Regenerate third-party rules and verify no drift**

Run: `./tools/buckify.sh`
Then: `git diff --stat third-party/BUCK Cargo.lock`
Expected: `third-party/BUCK` UNCHANGED (datafusion/arrow/object_store already exist as third-party rules — ingest uses them). `Cargo.lock` may gain only query-api's new dependency edges. **If `Cargo.lock` shows unrelated crates removed/downgraded (e.g. comfy-table, crossterm), revert it (`git checkout Cargo.lock`) and re-run `./tools/buckify.sh` — the edges reindeer needs are added without a full re-resolve.** Do NOT run `cargo generate-lockfile`.

- [ ] **Step 5: Add buck deps**

In `src/services/query-api/BUCK`, add to the `query-api` `rust_library` `deps` (keep sorted):

```python
        "//third-party:arrow",
        "//third-party:datafusion",
        "//third-party:object_store",
```

- [ ] **Step 6: Build to verify wiring**

Run: `buck2 build //src/services/query-api:query-api > /tmp/t.log 2>&1; grep -E "BUILD SUCCEEDED|FAIL|error" /tmp/t.log`
Expected: BUILD SUCCEEDED.

- [ ] **Step 7: Commit**

```bash
git add src/services/query-api/src/serving_datafusion.rs src/services/query-api/src/lib.rs src/services/query-api/Cargo.toml src/services/query-api/BUCK Cargo.lock
git commit -m "build(query-api): add datafusion/arrow/object_store deps + serving_datafusion module"
```

---

## Task 3: Arrow → `SqlValue` mapping

The DataFusion result is Arrow `RecordBatch`es; the engine returns the engine-neutral `Rows` (`columns: Vec<String>`, `rows: Vec<Vec<SqlValue>>`). This is the inverse of `serving.rs`'s `from_duck`. Pure logic — unit-testable with hand-built arrays, no DB.

**Files:**
- Modify: `src/services/query-api/src/serving_datafusion.rs`
- Create: `src/services/query-api/tests/datafusion_value_map.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/datafusion_value_map.rs`:

```rust
//! arrow_to_sqlvalue / batches_to_rows: Arrow result -> engine-neutral Rows.
//! `rust_test` integration target (pure logic, no DB).

use std::sync::Arc;

use arrow::array::{
    BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use query_api::serving::{Rows, SqlValue};
use query_api::serving_datafusion::batches_to_rows;

#[test]
fn maps_each_scalar_type_and_nulls() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
        Field::new("b", DataType::Boolean, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("d", DataType::Date32, true),
        Field::new("t", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(7), None])),
            Arc::new(Float64Array::from(vec![Some(1.5), None])),
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(StringArray::from(vec![Some("hi"), None])),
            // 1970-01-02 = 1 day after epoch.
            Arc::new(Date32Array::from(vec![Some(1), None])),
            // 1970-01-01T00:00:01 = 1_000_000 microseconds.
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1_000_000), None])),
        ],
    )
    .unwrap();

    let rows: Rows = batches_to_rows(vec![batch]);

    assert_eq!(rows.columns, vec!["i", "f", "b", "s", "d", "t"]);
    assert_eq!(rows.rows.len(), 2);
    assert_eq!(
        rows.rows[0],
        vec![
            SqlValue::Int(7),
            SqlValue::Double(1.5),
            SqlValue::Bool(true),
            SqlValue::Text("hi".into()),
            SqlValue::Date(time::macros::date!(1970 - 01 - 02)),
            SqlValue::Timestamp(time::macros::datetime!(1970 - 01 - 01 00:00:01)),
        ]
    );
    assert!(rows.rows[1].iter().all(|v| *v == SqlValue::Null), "row 2 is all nulls");
}

#[test]
fn empty_batches_yield_no_rows() {
    let rows = batches_to_rows(vec![]);
    assert!(rows.columns.is_empty());
    assert!(rows.rows.is_empty());
}
```

Add the target to `src/services/query-api/BUCK` (a plain `rust_test`, no fixture):

```python
rust_test(
    name = "datafusion-value-map",
    crate = "datafusion_value_map",
    srcs = ["tests/datafusion_value_map.rs"],
    crate_root = "tests/datafusion_value_map.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//third-party:arrow",
        "//third-party:time",
    ],
)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:datafusion-value-map > /tmp/t.log 2>&1; cat /tmp/t.log`
Expected: FAIL — `batches_to_rows` not found.

- [ ] **Step 3: Implement the mapping**

Append to `src/services/query-api/src/serving_datafusion.rs`:

```rust
use arrow::array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

use crate::serving::{Rows, SqlValue};

/// Flatten DataFusion result batches into the engine-neutral `Rows`. Columns come
/// from the first batch's schema (DataFusion preserves projection order, satisfying
/// the handler's column-order contract); an empty result yields empty `Rows`.
pub fn batches_to_rows(batches: Vec<RecordBatch>) -> Rows {
    let Some(first) = batches.first() else {
        return Rows::default();
    };
    let columns: Vec<String> = first
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut rows = Vec::new();
    for batch in &batches {
        for r in 0..batch.num_rows() {
            let mut cells = Vec::with_capacity(batch.num_columns());
            for c in 0..batch.num_columns() {
                cells.push(arrow_to_sqlvalue(batch.column(c), r));
            }
            rows.push(cells);
        }
    }
    Rows { columns, rows }
}

/// One Arrow cell -> `SqlValue`. Covers the scalar set loom serves; an unmapped
/// Arrow type falls back to a debug `Text` so a read never panics (mirrors the
/// DuckDB engine's `from_duck` fallback).
fn arrow_to_sqlvalue(array: &dyn Array, row: usize) -> SqlValue {
    if array.is_null(row) {
        return SqlValue::Null;
    }
    macro_rules! dc {
        ($ty:ty) => {
            array.as_any().downcast_ref::<$ty>().expect("arrow downcast")
        };
    }
    match array.data_type() {
        DataType::Utf8 => SqlValue::Text(dc!(StringArray).value(row).to_string()),
        DataType::LargeUtf8 => SqlValue::Text(dc!(LargeStringArray).value(row).to_string()),
        DataType::Boolean => SqlValue::Bool(dc!(BooleanArray).value(row)),
        DataType::Int8 => SqlValue::Int(dc!(Int8Array).value(row) as i64),
        DataType::Int16 => SqlValue::Int(dc!(Int16Array).value(row) as i64),
        DataType::Int32 => SqlValue::Int(dc!(Int32Array).value(row) as i64),
        DataType::Int64 => SqlValue::Int(dc!(Int64Array).value(row)),
        DataType::Float32 => SqlValue::Double(dc!(Float32Array).value(row) as f64),
        DataType::Float64 => SqlValue::Double(dc!(Float64Array).value(row)),
        DataType::Date32 => {
            let days = dc!(Date32Array).value(row);
            SqlValue::Date(time::macros::date!(1970 - 01 - 01) + time::Duration::days(days as i64))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = dc!(TimestampMicrosecondArray).value(row);
            let odt = time::OffsetDateTime::from_unix_timestamp_nanos(micros as i128 * 1_000)
                .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
            SqlValue::Timestamp(time::PrimitiveDateTime::new(odt.date(), odt.time()))
        }
        _ => SqlValue::Text(format!("{array:?}")),
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:datafusion-value-map > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Format and commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/serving_datafusion.rs src/services/query-api/tests/datafusion_value_map.rs
git add src/services/query-api/src/serving_datafusion.rs src/services/query-api/tests/datafusion_value_map.rs src/services/query-api/BUCK
git commit -m "feat(query-api): arrow_to_sqlvalue/batches_to_rows for the datafusion engine"
```

---

## Task 4: `register_iceberg_table` — absolute-path registration (the risk, pinned)

Register one Iceberg table's live files as a schema-qualified DataFusion table from the mirror's **absolute** `file://` paths. This is the riskiest unknown, so it gets its own integration test that seeds a table and SELECTs it back through raw DataFusion.

**Files:**
- Modify: `src/services/query-api/src/serving_datafusion.rs`
- Create: `src/services/query-api/tests/datafusion_register.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/datafusion_register.rs`:

```rust
//! register_iceberg_table registers a seeded Iceberg table's absolute-path Parquet
//! files so a raw DataFusion SELECT reads them back. loom_fixture_test (Postgres +
//! LocalFsStorage; no DuckDB).

use std::sync::Arc;

use control_plane_core::TableRef;
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use datafusion::prelude::SessionContext;
use query_api::serving::SqlValue;
use query_api::serving_datafusion::{batches_to_rows, register_iceberg_table};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registers_and_selects_back() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    // Two appends -> two Parquet files of 3 + 2 rows = 5 rows total.
    writer.seed("sales", "orders", &cols, &[3, 2]).await;

    let catalog = IcebergCatalog::new(pool);
    let table = TableRef { schema: "sales".into(), name: "orders".into() };

    let ctx = SessionContext::new();
    register_iceberg_table(&ctx, &catalog, &table)
        .await
        .expect("register");

    // Dogfood batches_to_rows (Task 3) to avoid fragile raw-Arrow casts.
    let df = ctx
        .sql("SELECT count(*) AS n FROM \"sales\".\"orders\"")
        .await
        .expect("sql");
    let rows = batches_to_rows(df.collect().await.expect("collect"));
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Int(5)]],
        "both appended files are registered and scanned"
    );

    // Keep the writer (its warehouse TempDir holds the files) alive until here.
    drop(writer);
}
```

Add the target (`loom_fixture_test`, needs DataFusion in the test to drive `ctx`):

```python
loom_fixture_test(
    name = "datafusion-register",
    crate = "datafusion_register",
    srcs = ["tests/datafusion_register.rs"],
    crate_root = "tests/datafusion_register.rs",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:datafusion",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:datafusion-register > /tmp/t.log 2>&1; cat /tmp/t.log`
Expected: FAIL — `register_iceberg_table` not found.

- [ ] **Step 3: Implement `register_iceberg_table`**

Append to `src/services/query-api/src/serving_datafusion.rs`:

```rust
use std::sync::Arc;

use control_plane_core::{PageReq, TableRef};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use datafusion::catalog::MemorySchemaProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use object_store::local::LocalFileSystem;

use crate::serving::ServingError;

/// Register `table`'s live data files (at its current snapshot) as a DataFusion
/// `ListingTable` under the schema-qualified name `"schema"."table"`, so the
/// compiled read SQL resolves it. Files are registered by their ABSOLUTE `file://`
/// paths as stored in the mirror (`iceberg_mirror.data_file.path`).
pub async fn register_iceberg_table(
    ctx: &SessionContext,
    catalog: &IcebergCatalog,
    table: &TableRef,
) -> Result<(), ServingError> {
    use control_plane_core::Catalog;

    // A non-prefixed local store for the absolute warehouse paths (a prefixed store
    // could not reach files outside data_path). Idempotent across calls on one ctx.
    ctx.register_object_store(
        ObjectStoreUrl::local_filesystem().as_ref(),
        Arc::new(LocalFileSystem::new()),
    );

    let snap = catalog.current_snapshot(table).await.map_err(to_serving)?;
    let files = catalog
        .files(table, snap.id, PageReq::unbounded())
        .await
        .map_err(to_serving)?;
    let urls: Vec<ListingTableUrl> = files
        .items
        .iter()
        .map(|f| ListingTableUrl::parse(&f.path))
        .collect::<Result<_, _>>()
        .map_err(to_serving)?;

    // Keep string/binary as canonical Utf8/Binary (not the *View variants) so
    // arrow_to_sqlvalue maps them — same choice as datafusion_io::scan_table.
    let format = ParquetFormat::default().with_force_view_types(false);
    let opts = ListingOptions::new(Arc::new(format));
    let cfg = ListingTableConfig::new_with_multi_paths(urls)
        .with_listing_options(opts)
        .infer_schema(&ctx.state())
        .await
        .map_err(to_serving)?;
    let provider = ListingTable::try_new(cfg).map_err(to_serving)?;

    // Ensure the schema exists in the default catalog, then register the table
    // schema-qualified so `"schema"."table"` references resolve.
    let cat = ctx
        .catalog("datafusion")
        .ok_or_else(|| ServingError::Engine("no default datafusion catalog".into()))?;
    if cat.schema(&table.schema).is_none() {
        cat.register_schema(&table.schema, Arc::new(MemorySchemaProvider::new()))
            .map_err(to_serving)?;
    }
    ctx.register_table(
        TableReference::partial(table.schema.clone(), table.name.clone()),
        Arc::new(provider),
    )
    .map_err(to_serving)?;
    Ok(())
}

/// Any error (mirror/Postgres, DataFusion, object_store, URL) -> opaque serving error.
fn to_serving<E: std::fmt::Display>(e: E) -> ServingError {
    ServingError::Engine(e.to_string())
}
```

Note: the import block in Step 3 of Task 3 and this step both add `use` lines — consolidate the `use std::sync::Arc;` so it appears once at the top of the file.

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:datafusion-register > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

If it FAILS on object-store resolution (DataFusion can't find the `file://` store), the registration above already registers a non-prefixed `LocalFileSystem`; re-read the error — a `ListingTableUrl::parse` failure means the mirror path is not a `file://` URL (inspect it: `buck2 ... ` then a quick `psql`-style check via the fixture), and adjust the parse to prefix `file://` if the stored path is a bare absolute path.

- [ ] **Step 5: Format, lint, commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/serving_datafusion.rs src/services/query-api/tests/datafusion_register.rs
tools/clippy-all.sh > /tmp/c.log 2>&1; grep -E "warning|error" /tmp/c.log || echo clean
git add src/services/query-api/src/serving_datafusion.rs src/services/query-api/tests/datafusion_register.rs src/services/query-api/BUCK
git commit -m "feat(query-api): register_iceberg_table — schema-qualified absolute-path scan"
```

---

## Task 5: `DataFusionServingEngine`

Tie it together: enumerate live tables, register each, inline params, run SQL, map results. Implements `ServingEngine`. Inherits the trait's default `dialect()` (`DuckDbDialect`) — the compiled SQL (quoted identifiers, `?` placeholders inlined, `LIMIT n`) is already valid DataFusion SQL, so no separate dialect is needed.

**Files:**
- Modify: `src/services/query-api/src/serving_datafusion.rs`
- Create: `src/services/query-api/tests/datafusion_serving.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/datafusion_serving.rs`. It exercises the engine through `fetch_rows` with the exact SQL shape the handler emits (`compile_select_with(&DuckDbDialect, ...)` produces this form).

```rust
//! DataFusionServingEngine::fetch_rows over a seeded Iceberg table returns correct
//! Rows for the handler's compiled-SQL shape. loom_fixture_test (Postgres +
//! LocalFsStorage; no DuckDB).

use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use query_api::serving::{ServingEngine, SqlValue};
use query_api::serving_datafusion::DataFusionServingEngine;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_rows_over_iceberg() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    writer.seed("sales", "orders", &cols, &[3]).await; // ids 0,1,2 ; names row0,row1,row2

    let engine = DataFusionServingEngine::new(IcebergCatalog::new(pool));

    // The compiled-read shape: quoted idents, a bound `?`, LIMIT.
    let sql = "SELECT \"id\", \"name\" FROM \"sales\".\"orders\" WHERE (\"id\" = ?) LIMIT 100";
    let rows = engine
        .fetch_rows(sql, &[SqlValue::Int(1)])
        .await
        .expect("fetch_rows");

    assert_eq!(rows.columns, vec!["id", "name"]);
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Int(1), SqlValue::Text("row1".into())]]
    );

    drop(writer);
}
```

Add the target:

```python
loom_fixture_test(
    name = "datafusion-serving",
    crate = "datafusion_serving",
    srcs = ["tests/datafusion_serving.rs"],
    crate_root = "tests/datafusion_serving.rs",
    deps = [
        ":query-api",
        "//src/control-plane/postgres:postgres",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:datafusion-serving > /tmp/t.log 2>&1; cat /tmp/t.log`
Expected: FAIL — `DataFusionServingEngine` not found.

- [ ] **Step 3: Implement the engine**

Append to `src/services/query-api/src/serving_datafusion.rs`:

```rust
use async_trait::async_trait;

use crate::serving::{inline_params, Rows, ServingEngine, SqlValue};

/// loom-native serving engine: serves governed reads for file-backed Iceberg
/// tables from the mirror via DataFusion. Holds only the mirror reader; the
/// `file://` object store and table registrations are built per query.
pub struct DataFusionServingEngine {
    catalog: IcebergCatalog,
}

impl DataFusionServingEngine {
    pub fn new(catalog: IcebergCatalog) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl ServingEngine for DataFusionServingEngine {
    async fn fetch_rows(&self, sql: &str, params: &[SqlValue]) -> Result<Rows, ServingError> {
        let ctx = SessionContext::new();
        // Register every live table so the compiled SQL's table refs resolve.
        // (Approach A: pre-register; the per-query mirror read is cheap. A schema
        // cache is a noted perf follow-up, not implemented here.)
        for table in self.catalog.live_tables().await.map_err(to_serving)? {
            register_iceberg_table(&ctx, &self.catalog, &table).await?;
        }
        // DataFusion has no positional bind slot here; inline params with the same
        // injection-safe renderer the Quack engine uses (`?` -> SQL literal).
        let inlined = inline_params(sql, params);
        let df = ctx.sql(&inlined).await.map_err(to_serving)?;
        let batches = df.collect().await.map_err(to_serving)?;
        Ok(batches_to_rows(batches))
    }
    // dialect(): inherit the trait default (DuckDbDialect). The compiled SQL it
    // produces is valid DataFusion SQL, so no override is needed.
}
```

`inline_params`, `Rows`, `SqlValue`, `ServingEngine` are `pub` in `crate::serving` (reuse, do not re-implement). Ensure imports are consolidated (single `use crate::serving::{...}` and single `use std::sync::Arc;`).

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:datafusion-serving > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Format, lint, commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/serving_datafusion.rs src/services/query-api/tests/datafusion_serving.rs
tools/clippy-all.sh > /tmp/c.log 2>&1; grep -E "warning|error" /tmp/c.log || echo clean
git add src/services/query-api/src/serving_datafusion.rs src/services/query-api/tests/datafusion_serving.rs src/services/query-api/BUCK
git commit -m "feat(query-api): DataFusionServingEngine over the Iceberg mirror"
```

---

## Task 6: `UnsupportedActionEngine`

The iceberg backend has no inline write path, so its `ActionEngine` rejects writes cleanly (the action endpoint then errors rather than misbehaving).

**Files:**
- Modify: `src/services/query-api/src/serving_datafusion.rs`
- Create: `src/services/query-api/tests/unsupported_action.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/unsupported_action.rs`:

```rust
//! UnsupportedActionEngine rejects writes (the iceberg backend is read-only).
//! `rust_test` integration target (pure logic, no DB).

use control_plane_core::TableRef;
use query_api::serving::{ActionEngine, ServingError, SqlValue};
use query_api::serving_datafusion::UnsupportedActionEngine;

#[tokio::test(flavor = "current_thread")]
async fn insert_row_is_rejected() {
    let engine = UnsupportedActionEngine;
    let table = TableRef { schema: "sales".into(), name: "orders".into() };
    let err = engine
        .insert_row(&table, &["id".to_string()], &[SqlValue::Int(1)])
        .await
        .expect_err("must reject");
    match err {
        ServingError::Engine(m) => assert!(m.contains("iceberg"), "message names the backend: {m}"),
    }
}
```

Add the target:

```python
rust_test(
    name = "unsupported-action",
    crate = "unsupported_action",
    srcs = ["tests/unsupported_action.rs"],
    crate_root = "tests/unsupported_action.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:unsupported-action > /tmp/t.log 2>&1; cat /tmp/t.log`
Expected: FAIL — `UnsupportedActionEngine` not found.

- [ ] **Step 3: Implement it**

Append to `src/services/query-api/src/serving_datafusion.rs`:

```rust
use crate::serving::ActionEngine;

/// The `ActionEngine` for the iceberg serving backend: there is no inline write
/// path (inlining is a DuckLake feature loom has not rebuilt), so writes are
/// rejected. The action endpoint surfaces this as an opaque error.
pub struct UnsupportedActionEngine;

#[async_trait]
impl ActionEngine for UnsupportedActionEngine {
    async fn insert_row(
        &self,
        _table: &TableRef,
        _columns: &[String],
        _values: &[SqlValue],
    ) -> Result<(), ServingError> {
        Err(ServingError::Engine(
            "actions unsupported on the iceberg serving backend".into(),
        ))
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:unsupported-action > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Format and commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/serving_datafusion.rs src/services/query-api/tests/unsupported_action.rs
git add src/services/query-api/src/serving_datafusion.rs src/services/query-api/tests/unsupported_action.rs src/services/query-api/BUCK
git commit -m "feat(query-api): UnsupportedActionEngine for the read-only iceberg backend"
```

---

## Task 7: Backend selection + binary wiring

A unit-testable parser for `LOOM_SERVING_BACKEND`, then wire `main.rs` to select the engines.

**Files:**
- Modify: `src/services/query-api/src/serving_datafusion.rs`
- Create: `src/services/query-api/tests/serving_backend_parse.rs`
- Modify: `src/services/query-api/src/main.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Write the failing parser test**

Create `src/services/query-api/tests/serving_backend_parse.rs`:

```rust
//! parse_serving_backend: env value -> ServingBackend. `rust_test` (pure logic).

use query_api::serving_datafusion::{parse_serving_backend, ServingBackend};

#[test]
fn defaults_to_ducklake_when_unset() {
    assert_eq!(parse_serving_backend(None), Ok(ServingBackend::DuckLake));
}

#[test]
fn parses_known_values_case_insensitively() {
    assert_eq!(parse_serving_backend(Some("ducklake")), Ok(ServingBackend::DuckLake));
    assert_eq!(parse_serving_backend(Some("iceberg")), Ok(ServingBackend::Iceberg));
    assert_eq!(parse_serving_backend(Some("ICEBERG")), Ok(ServingBackend::Iceberg));
}

#[test]
fn rejects_unknown() {
    assert!(parse_serving_backend(Some("delta")).is_err());
}
```

Add the target:

```python
rust_test(
    name = "serving-backend-parse",
    crate = "serving_backend_parse",
    srcs = ["tests/serving_backend_parse.rs"],
    crate_root = "tests/serving_backend_parse.rs",
    edition = "2024",
    deps = [":query-api"],
)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test //src/services/query-api:serving-backend-parse > /tmp/t.log 2>&1; cat /tmp/t.log`
Expected: FAIL — `parse_serving_backend` not found.

- [ ] **Step 3: Implement the parser**

Append to `src/services/query-api/src/serving_datafusion.rs`:

```rust
/// Which table-format backend the query-api binary serves reads from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServingBackend {
    /// DuckLake via embedded DuckDB (default; today's behavior).
    DuckLake,
    /// File-backed Iceberg via the loom-native DataFusion engine.
    Iceberg,
}

/// Parse `LOOM_SERVING_BACKEND`. Unset -> DuckLake. Case-insensitive.
pub fn parse_serving_backend(v: Option<&str>) -> Result<ServingBackend, String> {
    match v.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") | Some("ducklake") => Ok(ServingBackend::DuckLake),
        Some("iceberg") => Ok(ServingBackend::Iceberg),
        Some(other) => Err(format!(
            "LOOM_SERVING_BACKEND must be 'ducklake' or 'iceberg', got {other:?}"
        )),
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:serving-backend-parse > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Wire `main.rs`**

Replace the body of `src/services/query-api/src/main.rs` with the backend-selecting version. The DuckLake branch is byte-for-byte today's wiring; the iceberg branch builds the DataFusion engine over a cloned pool.

```rust
//! query-api binary: build the read AppState from env config via service_runtime —
//! a Postgres control plane plus a serving engine selected by LOOM_SERVING_BACKEND
//! (DuckLake-on-DuckDB by default, or the loom-native DataFusion engine over the
//! Iceberg mirror) — and serve the HTTP API.

use std::sync::Arc;

use control_plane_core::ControlPlane;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use query_api::http::{AppState, router};
use query_api::serving::{
    ActionEngine, EmbeddedDuckDb, EmbeddedDuckDbWriter, ServingEngine,
};
use query_api::serving_datafusion::{
    parse_serving_backend, DataFusionServingEngine, ServingBackend, UnsupportedActionEngine,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = service_runtime::Config::from_env()?;
    let pool = service_runtime::build_pool(&cfg.db).await?;
    let backend = parse_serving_backend(std::env::var("LOOM_SERVING_BACKEND").ok().as_deref())
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    // cp (ontology + ACL) is format-agnostic and identical for both backends.
    let cp: Arc<dyn ControlPlane> =
        Arc::new(service_runtime::control_plane(pool.clone(), cfg.lock_timeout));

    let (serving, action_engine): (Arc<dyn ServingEngine>, Arc<dyn ActionEngine>) = match backend {
        ServingBackend::DuckLake => (
            Arc::new(EmbeddedDuckDb::attach(&cfg.db.ducklake_libpq(), &cfg.data_path).await?),
            Arc::new(EmbeddedDuckDbWriter::attach(&cfg.db.ducklake_libpq(), &cfg.data_path).await?),
        ),
        ServingBackend::Iceberg => (
            Arc::new(DataFusionServingEngine::new(IcebergCatalog::new(pool))),
            Arc::new(UnsupportedActionEngine),
        ),
    };

    let app = router(AppState { cp, serving, action_engine });
    service_runtime::serve(cfg.bind_addr, app).await?;
    Ok(())
}
```

- [ ] **Step 6: Add the binary's new buck dep**

In `src/services/query-api/BUCK`, add to the `query-api-bin` `rust_binary` `deps`:

```python
        "//src/control-plane/postgres:postgres",
```

- [ ] **Step 7: Build the binary**

Run: `buck2 build //src/services/query-api:query-api-bin > /tmp/t.log 2>&1; grep -E "BUILD SUCCEEDED|FAIL|error" /tmp/t.log`
Expected: BUILD SUCCEEDED.

- [ ] **Step 8: Format, lint, commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/serving_datafusion.rs src/services/query-api/tests/serving_backend_parse.rs src/services/query-api/src/main.rs
tools/clippy-all.sh > /tmp/c.log 2>&1; grep -E "warning|error" /tmp/c.log || echo clean
git add src/services/query-api/src/serving_datafusion.rs src/services/query-api/tests/serving_backend_parse.rs src/services/query-api/src/main.rs src/services/query-api/BUCK
git commit -m "feat(query-api): select serving backend via LOOM_SERVING_BACKEND"
```

---

## Task 8: Full-suite green + roadmap update

**Files:**
- Modify: `docs/spike/ICEBERG_ROADMAP.md`

- [ ] **Step 1: Run the whole first-party suite**

Run: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS, 0 failures. (Confirms the new deps/tests didn't disturb anything; the `sqlx-cache-check` passes with the new query.)

- [ ] **Step 2: Run clippy across all first-party Rust**

Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -E "warning|error" /tmp/c.log || echo clean`
Expected: clean.

- [ ] **Step 3: Mark slice 3 done in the roadmap**

In `docs/spike/ICEBERG_ROADMAP.md`, move bullet 3 (service-binary wiring — read side) from "Left" to "Done", noting: loom-native DataFusion serving engine over the Iceberg mirror, selected by `LOOM_SERVING_BACKEND=iceberg`, reads file-backed tables only (inline/actions rejected; write/ingest wiring still deferred).

- [ ] **Step 4: Commit**

```bash
git add docs/spike/ICEBERG_ROADMAP.md
git commit -m "docs(iceberg): mark read-side service wiring (slice 3) done"
```

---

## Notes for the executor

- **Module growth:** `serving_datafusion.rs` accumulates across Tasks 3–7. Keep one consolidated import block at the top (the per-task code blocks show imports inline for readability; merge duplicate `use` lines as you go, especially `use std::sync::Arc;` and `use crate::serving::{...}`).
- **DataFusion API drift:** the exact paths for `MemorySchemaProvider`, `TableReference`, `ObjectStoreUrl::local_filesystem`, and `ListingTable*` are DataFusion 54 APIs. If a path doesn't resolve, find the type with `buck2 build //src/services/query-api:query-api[clippy.txt]` errors as a guide, or check how `src/services/datafusion-io/src/scan.rs` and `write.rs` import the same families. Do not change behavior — only import paths.
- **Why no `DataFusionDialect`:** the engine inlines params and the compiler quotes all identifiers, so `DuckDbDialect`'s output (`"id"`, `LIMIT n`, `?`→literal) is already valid DataFusion SQL. A separate identical dialect type would be dead weight; add one only if a real token diverges.
- **Out of scope (do not build):** inline data / on-the-fly tables, write/ingest backend selection, S3/object-store config, time-travel reads, per-column stats/pruning, a full governed-read (ontology+ACL) e2e over Iceberg (the `fetch_rows` test covers the engine; governance is format-agnostic and already tested over DuckLake).
```
