# Iceberg Inline Writes + Read Union Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** loom-native inline (small) writes for Iceberg tables — typed rows in the mirror, no object-storage Parquet, unioned with file-backed Parquet by the DataFusion serving engine at read time.

**Architecture:** A write primitive `inline_append` (postgres crate) stores rows in a per-table typed table `iceberg_mirror.inline_<table_id>` created on the fly, advancing a mirror snapshot and emitting lineage in one Postgres transaction. A reader `IcebergCatalog::inline_parquet` encodes a table's live inline rows to in-memory Parquet bytes (arrow/parquet 57, already in the crate). The slice-3 serving engine registers those bytes in an `InMemory` object store under `memory://` and adds the URL to the same `ListingTable` as the `file://` Parquet — the union is "more URLs".

**Tech Stack:** Rust 2024, buck2, sqlx 0.9 (runtime `AssertSqlSafe` for the dynamic per-table SQL), arrow-array/arrow-schema/parquet 57 (postgres crate), DataFusion 54 + object_store 0.13 (query-api), `loom_fixture_test`.

**Spec:** `docs/superpowers/specs/2026-06-18-iceberg-inline-writes-design.md`

---

## Key facts the implementer must know (loom-specific)

- **No inline tests.** Every test is a sibling `tests/<name>.rs` wired as its own `rust_test`/`loom_fixture_test` target. A prek hook fails the build on any `#[test]` inside `src/**.rs`.
- **Running tests:** `buck2 test //path:target > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`. NEVER pipe `buck2 test` to `tail`/`head`.
- **rustfmt is check-only:** run `buck2 run //tools:rustfmt -- <files>` before committing or the hook loops.
- **Dynamic SQL** uses sqlx 0.9's `AssertSqlSafe`: `sqlx::query(AssertSqlSafe(format!(...))).bind(..).execute(..)`. The inline tables have per-table schemas, so they CANNOT use compile-time `query!` (and aren't covered by the `.sqlx` cache check). The `table_id` interpolated into a table name is an internal i64 (never user input); column names come from the trusted schema.
- **Two arrow majors coexist:** the postgres crate uses arrow-array/arrow-schema/parquet **57** (`use arrow_array::...`, `use parquet57::...`); query-api uses arrow **58** (`use arrow::...`) + datafusion. Keep all RecordBatch construction for inline in the **postgres crate** so the query-api test never touches arrow-57.
- **`loom_fixture_test`** boots hermetic Postgres; OMIT `duckdb` for these tests (Iceberg path uses LocalFsStorage, no DuckDB).
- Reusable mirror helpers in `src/control-plane/postgres/src/iceberg_mirror.rs`: `next_snapshot(conn, Option<i64>)`, `ensure_table(conn, ns, name, at)`, `columns_exist(conn, tid)`, `project_columns(conn, tid, at, &[ProjectedColumn])`. Lineage: `crate::lineage::pg_emit(&mut *conn, &event)` (pub(crate)). The mirror write order is `next_snapshot → ensure_table → (if !columns_exist) project_columns → …` (see `project_mirror`).

---

## File Structure

**Create:**
- `src/control-plane/postgres/src/iceberg_inline.rs` — the inline write primitive, the `Cell` value enum + per-type arrow↔PG helpers, `inline_table_name`/`inline_ddl`, and `IcebergCatalog::inline_parquet`.
- `src/control-plane/postgres/tests/iceberg_inline.rs` — fixture tests for the write primitive + the parquet reader.
- `src/services/query-api/tests/datafusion_inline_union.rs` — fixture test for the read union.

**Modify:**
- `src/control-plane/postgres/src/iceberg_type.rs` — add `iceberg_physical_type` + `pg_type_for`.
- `src/control-plane/postgres/src/lib.rs` — `pub mod iceberg_inline;`.
- `src/control-plane/postgres/src/fixture.rs` — `IcebergWriter::inline()` helper (builds the arrow-57 batch + calls `inline_append`).
- `src/control-plane/postgres/BUCK` — new `iceberg-inline` test target.
- `src/services/query-api/src/serving_datafusion.rs` — union inline Parquet into `register_iceberg_table`.
- `src/services/query-api/BUCK` — new `datafusion-inline-union` test target.
- `docs/spike/ICEBERG_ROADMAP.md` — mark Slice A done.

**Unchanged by design:** the vendored `SqlCatalog`/`update_table` (inline is mirror-only), the `core::Catalog` trait, the DuckLake paths.

---

## Task 1: Type maps (`iceberg_physical_type`, `pg_type_for`)

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_type.rs`
- Create: `src/control-plane/postgres/tests/iceberg_inline_types.rs`
- Modify: `src/control-plane/postgres/BUCK`

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/iceberg_inline_types.rs`:

```rust
//! Forward logical-name -> Iceberg-type-name and logical-name -> Postgres-type maps
//! for the inline write path. `rust_test` (pure logic).

use control_plane_postgres::iceberg_type::{iceberg_physical_type, pg_type_for};

#[test]
fn iceberg_names_cover_the_scalar_set() {
    for (logical, iceberg) in [
        ("integer", "int"),
        ("long", "long"),
        ("double", "double"),
        ("boolean", "boolean"),
        ("string", "string"),
        ("date", "date"),
        ("timestamp", "timestamp"),
    ] {
        assert_eq!(iceberg_physical_type(logical), Some(iceberg), "{logical}");
    }
    assert_eq!(iceberg_physical_type("decimal"), None);
}

#[test]
fn pg_types_cover_the_scalar_set() {
    for (logical, pg) in [
        ("integer", "integer"),
        ("long", "bigint"),
        ("double", "double precision"),
        ("boolean", "boolean"),
        ("string", "text"),
        ("date", "date"),
        ("timestamp", "timestamp"),
    ] {
        assert_eq!(pg_type_for(logical), Some(pg), "{logical}");
    }
    assert_eq!(pg_type_for("decimal"), None);
}
```

Add the target to `src/control-plane/postgres/BUCK` (a plain `rust_test`, near the other type tests like `iceberg-type`):

```python
rust_test(
    name = "iceberg-inline-types",
    crate = "iceberg_inline_types",
    srcs = ["tests/iceberg_inline_types.rs"],
    crate_root = "tests/iceberg_inline_types.rs",
    deps = [":postgres"],
)
```

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test //src/control-plane/postgres:iceberg-inline-types > /tmp/t.log 2>&1; cat /tmp/t.log`
Expected: FAIL — `iceberg_physical_type`/`pg_type_for` not found.

- [ ] **Step 3: Implement the maps**

Append to `src/control-plane/postgres/src/iceberg_type.rs` (the logical names are `BaseType::canonical_name` values; matching on the string avoids a parse):

```rust
/// loom logical type name -> Iceberg primitive type name (write path, used to
/// project `iceberg_mirror.column` rows for an inline-only table). The inverse of
/// `logical_from_iceberg`. `None` for a name loom has no Iceberg mapping for.
pub fn iceberg_physical_type(logical: &str) -> Option<&'static str> {
    match logical.trim().to_ascii_lowercase().as_str() {
        "integer" => Some("int"),
        "long" => Some("long"),
        "double" => Some("double"),
        "boolean" => Some("boolean"),
        "string" => Some("string"),
        "date" => Some("date"),
        "timestamp" => Some("timestamp"),
        _ => None,
    }
}

/// loom logical type name -> Postgres column type for the per-table inline storage
/// (`iceberg_mirror.inline_<table_id>`). `None` for an unsupported name.
pub fn pg_type_for(logical: &str) -> Option<&'static str> {
    match logical.trim().to_ascii_lowercase().as_str() {
        "integer" => Some("integer"),
        "long" => Some("bigint"),
        "double" => Some("double precision"),
        "boolean" => Some("boolean"),
        "string" => Some("text"),
        "date" => Some("date"),
        "timestamp" => Some("timestamp"),
        _ => None,
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `buck2 test //src/control-plane/postgres:iceberg-inline-types > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Format and commit**

```bash
buck2 run //tools:rustfmt -- src/control-plane/postgres/src/iceberg_type.rs src/control-plane/postgres/tests/iceberg_inline_types.rs
git add src/control-plane/postgres/src/iceberg_type.rs src/control-plane/postgres/tests/iceberg_inline_types.rs src/control-plane/postgres/BUCK
git commit -m "feat(iceberg): forward logical->iceberg/postgres type maps for inline"
```

---

## Task 2: `inline_append` write primitive (+ fixture helper)

**Files:**
- Create: `src/control-plane/postgres/src/iceberg_inline.rs`
- Modify: `src/control-plane/postgres/src/lib.rs`
- Modify: `src/control-plane/postgres/src/fixture.rs`
- Create: `src/control-plane/postgres/tests/iceberg_inline.rs`
- Modify: `src/control-plane/postgres/BUCK`

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/iceberg_inline.rs`:

```rust
//! inline_append: a mirror-only typed inline write. loom_fixture_test (Postgres;
//! no DuckDB).

use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use sqlx::Row;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_append_writes_rows_snapshot_and_lineage() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    // Seed a real Parquet table first so the mirror table/columns exist.
    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    writer.seed("sales", "orders", &cols, &[3]).await;

    // Inline-append two rows; assert it advanced the snapshot and emitted lineage.
    let run = uuid::Uuid::new_v4();
    let snap = writer
        .inline("sales", "orders", &cols, &[(100, "row100"), (101, "row101")], run)
        .await;
    assert!(snap > 0, "inline write returned a snapshot id");

    // The per-table inline table exists and holds the two rows at this snapshot.
    let tid: i64 = sqlx::query_scalar(
        "select table_id from iceberg_mirror.table \
         where table_namespace='sales' and table_name='orders' and end_snapshot is null",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select count(*) from iceberg_mirror.inline_{tid} where begin_snapshot = {snap}"
    )))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n, 2, "two inline rows at the new snapshot");

    // Lineage event was emitted for the run.
    let events: i64 = sqlx::query("select count(*) from lineage.event where run_id = $1")
        .bind(run)
        .fetch_one(&pool)
        .await
        .map(|r| r.get(0))
        .unwrap();
    assert_eq!(events, 1, "one lineage event for the inline write");
}
```

Add the target to `src/control-plane/postgres/BUCK` (next to the other `iceberg-*` fixture tests):

```python
loom_fixture_test(
    name = "iceberg-inline",
    crate = "iceberg_inline",
    srcs = ["tests/iceberg_inline.rs"],
    crate_root = "tests/iceberg_inline.rs",
    deps = [
        ":postgres",
        "//third-party:sqlx",
        "//third-party:uuid",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test //src/control-plane/postgres:iceberg-inline > /tmp/t.log 2>&1; cat /tmp/t.log`
Expected: FAIL — `IcebergWriter::inline` not found.

- [ ] **Step 3: Create the inline module with the `Cell` enum + primitive**

Create `src/control-plane/postgres/src/iceberg_inline.rs`:

```rust
//! loom-native inline writes for Iceberg tables. Small writes land as typed rows in
//! a per-table `iceberg_mirror.inline_<table_id>` table (created on the fly), NOT a
//! Parquet object — a mirror-only commit (snapshot + rows + lineage in one tx). The
//! serving engine unions them with the table's Parquet files at read time via
//! `IcebergCatalog::inline_parquet`. External Iceberg clients don't see inline rows
//! until a future flush. See the slice-A design doc.

use arrow_array::{
    Array, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use control_plane_core::{ColumnSpec, ControlPlaneError, LineageEvent, Result, SnapshotId, TableRef};
use sqlx::{AssertSqlSafe, PgConnection, PgPool, Postgres, query::Query, postgres::PgArguments};

use crate::backend;
use crate::iceberg_mirror::{
    ProjectedColumn, columns_exist, ensure_table, next_snapshot, project_columns,
};
use crate::iceberg_type::{iceberg_physical_type, pg_type_for};
use crate::lineage::pg_emit;

/// The Postgres name of a table's inline storage. `table_id` is an internal i64.
pub fn inline_table_name(table_id: i64) -> String {
    format!("iceberg_mirror.inline_{table_id}")
}

/// One typed inline cell — the bridge between an arrow-57 array and a Postgres bind.
#[derive(Clone, Debug)]
pub(crate) enum Cell {
    I32(Option<i32>),
    I64(Option<i64>),
    F64(Option<f64>),
    Bool(Option<bool>),
    Str(Option<String>),
    Date(Option<time::Date>),
    Ts(Option<time::PrimitiveDateTime>),
}

/// Pull cell `(col, row)` out of an arrow-57 batch, typed per the logical column.
fn cell_from_arrow(batch: &RecordBatch, col: usize, row: usize, logical: &str) -> Result<Cell> {
    let a = batch.column(col);
    let null = a.is_null(row);
    macro_rules! dc {
        ($ty:ty) => {
            a.as_any().downcast_ref::<$ty>().expect("inline arrow downcast")
        };
    }
    Ok(match logical {
        "integer" => Cell::I32((!null).then(|| dc!(Int32Array).value(row))),
        "long" => Cell::I64((!null).then(|| dc!(Int64Array).value(row))),
        "double" => Cell::F64((!null).then(|| dc!(Float64Array).value(row))),
        "boolean" => Cell::Bool((!null).then(|| dc!(BooleanArray).value(row))),
        "string" => Cell::Str((!null).then(|| dc!(StringArray).value(row).to_string())),
        "date" => Cell::Date((!null).then(|| {
            time::macros::date!(1970 - 01 - 01) + time::Duration::days(dc!(Date32Array).value(row) as i64)
        })),
        "timestamp" => Cell::Ts((!null).then(|| {
            let micros = dc!(TimestampMicrosecondArray).value(row);
            let odt = time::OffsetDateTime::from_unix_timestamp_nanos(micros as i128 * 1_000)
                .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
            time::PrimitiveDateTime::new(odt.date(), odt.time())
        })),
        other => {
            return Err(ControlPlaneError::Backend(
                format!("inline: unsupported column type {other:?}").into(),
            ));
        }
    })
}

/// Bind a cell as the next positional parameter. Binds OWNED values (copied/cloned)
/// so the cell's borrow doesn't have to outlive the query — `Option<T>` binds NULL.
fn bind_cell<'q>(
    q: Query<'q, Postgres, PgArguments>,
    cell: &Cell,
) -> Query<'q, Postgres, PgArguments> {
    match cell {
        Cell::I32(v) => q.bind(*v),
        Cell::I64(v) => q.bind(*v),
        Cell::F64(v) => q.bind(*v),
        Cell::Bool(v) => q.bind(*v),
        Cell::Str(v) => q.bind(v.clone()),
        Cell::Date(v) => q.bind(*v),
        Cell::Ts(v) => q.bind(*v),
    }
}

/// `CREATE TABLE IF NOT EXISTS` DDL for a table's inline storage from its schema.
fn inline_ddl(table_id: i64, columns: &[ColumnSpec]) -> Result<String> {
    let mut cols = String::new();
    for c in columns {
        let pg = pg_type_for(&c.ty).ok_or_else(|| {
            ControlPlaneError::Backend(format!("inline: no pg type for {:?}", c.ty).into())
        })?;
        // Column names come from the trusted schema; quote to preserve case.
        cols.push_str(&format!(", \"{}\" {}", c.name.replace('"', "\"\""), pg));
    }
    Ok(format!(
        "create table if not exists {} (\
           loom_row_id bigserial primary key, \
           begin_snapshot bigint not null, \
           end_snapshot bigint{cols})",
        inline_table_name(table_id),
    ))
}

/// Land `batch` for `table` as inline rows: a mirror-only commit (snapshot + typed
/// rows + lineage) in ONE Postgres transaction. No object storage, no Iceberg
/// metadata. `columns` is the table's logical schema (authoritative). Returns the
/// new loom snapshot id.
pub async fn inline_append(
    pool: &PgPool,
    table: &TableRef,
    columns: &[ColumnSpec],
    batch: &RecordBatch,
    lineage: LineageEvent,
) -> Result<SnapshotId> {
    let mut tx = pool.begin().await.map_err(backend)?;
    // Transaction derefs to PgConnection; the helpers take `&mut PgConnection`.
    let conn: &mut PgConnection = &mut tx;
    // (If the deref-coercion in the line above ever fails to infer, write
    //  `&mut *tx` explicitly — same thing.)

    // 1. Snapshot (no Iceberg backing) + ensure mirror table/columns exist.
    let at = next_snapshot(conn, None).await?;
    let tid = ensure_table(conn, &table.schema, &table.name, at).await?;
    if !columns_exist(conn, tid).await? {
        let pcols = columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                Ok(ProjectedColumn {
                    order: i as i64,
                    name: c.name.clone(),
                    iceberg_type: iceberg_physical_type(&c.ty)
                        .ok_or_else(|| {
                            ControlPlaneError::Backend(
                                format!("inline: no iceberg type for {:?}", c.ty).into(),
                            )
                        })?
                        .to_string(),
                    nullable: c.nullable,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        project_columns(conn, tid, at, &pcols).await?;
    }

    // 2. Ensure inline storage exists (transactional DDL).
    sqlx::query(AssertSqlSafe(inline_ddl(tid, columns)?))
        .execute(&mut *conn)
        .await
        .map_err(backend)?;

    // 3. Insert each row with the new begin_snapshot.
    let col_list = columns
        .iter()
        .map(|c| format!("\"{}\"", c.name.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    for row in 0..batch.num_rows() {
        let placeholders = (0..columns.len())
            .map(|i| format!("${}", i + 2)) // $1 = begin_snapshot
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "insert into {} (begin_snapshot, {col_list}) values ($1, {placeholders})",
            inline_table_name(tid),
        );
        let mut q = sqlx::query(AssertSqlSafe(sql)).bind(at.0);
        let cells = columns
            .iter()
            .enumerate()
            .map(|(c, spec)| cell_from_arrow(batch, c, row, &spec.ty))
            .collect::<Result<Vec<_>>>()?;
        for cell in &cells {
            q = bind_cell(q, cell);
        }
        q.execute(&mut *conn).await.map_err(backend)?;
    }

    // 4. Lineage, atomic with the rows.
    pg_emit(&mut *conn, &lineage).await?;

    tx.commit().await.map_err(backend)?;
    Ok(at)
}
```

Add `pub mod iceberg_inline;` to `src/control-plane/postgres/src/lib.rs` (after `pub mod iceberg_catalog;`).

- [ ] **Step 4: Add the `IcebergWriter::inline` fixture helper**

In `src/control-plane/postgres/src/fixture.rs`, add this method inside `impl IcebergWriter` (near `seed`). It builds an arrow-57 batch for `(long id, string name)` rows and calls `inline_append`. The imports `Int64Array`, `StringArray`, `RecordBatch`, `Arc`, `Schema`/`Field` (arrow-schema 57) are already used by the seeder; reuse them.

```rust
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
        let table = control_plane_core::TableRef { schema: ns.into(), name: name.into() };
        crate::iceberg_inline::inline_append(&self.pool, &table, &specs, &batch, lineage)
            .await
            .expect("inline_append")
            .0
    }
```

If `fixture.rs` lacks any of `serde_json` / `time` / `arrow_schema` imports, add them (the crate already depends on all three).

- [ ] **Step 5: Run to verify it passes**

Run: `buck2 test //src/control-plane/postgres:iceberg-inline > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS.

- [ ] **Step 6: Format, lint, commit**

```bash
buck2 run //tools:rustfmt -- src/control-plane/postgres/src/iceberg_inline.rs src/control-plane/postgres/src/fixture.rs src/control-plane/postgres/src/lib.rs src/control-plane/postgres/tests/iceberg_inline.rs
buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' > /tmp/cl.log 2>&1; grep -iE "warning|error" /tmp/cl.log || echo clean
git add src/control-plane/postgres/
git commit -m "feat(iceberg): inline_append — mirror-only typed inline write primitive"
```

---

## Task 3: `IcebergCatalog::inline_parquet` reader

Encode a table's live inline rows to in-memory Parquet bytes (arrow/parquet 57), so the serving engine can union them as a `memory://` file.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs`
- Modify: `src/control-plane/postgres/tests/iceberg_inline.rs`

- [ ] **Step 1: Write the failing test**

Append to `src/control-plane/postgres/tests/iceberg_inline.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_parquet_encodes_live_rows() {
    use control_plane_postgres::iceberg_catalog::IcebergCatalog;
    use parquet57::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    writer.seed("sales", "orders", &cols, &[3]).await;
    let snap = writer
        .inline("sales", "orders", &cols, &[(100, "row100"), (101, "row101")], uuid::Uuid::new_v4())
        .await;

    let catalog = IcebergCatalog::new(pool);
    let table = control_plane_core::TableRef { schema: "sales".into(), name: "orders".into() };
    let bytes = catalog
        .inline_parquet(&table, control_plane_core::SnapshotId(snap))
        .await
        .expect("inline_parquet")
        .expect("Some bytes when inline rows exist");

    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .unwrap()
        .build()
        .unwrap();
    let total: usize = reader.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(total, 2, "two inline rows encoded to parquet");

    // A table that was never inline-written yields None.
    let other = control_plane_core::TableRef { schema: "sales".into(), name: "orders".into() };
    let none = catalog
        .inline_parquet(&other, control_plane_core::SnapshotId(1))
        .await
        .expect("inline_parquet at snapshot 1");
    assert!(none.is_none(), "no inline rows live at the seed snapshot");
}
```

Add `//third-party:parquet-57` and `//third-party:bytes` to the `iceberg-inline` test target's `deps` in `src/control-plane/postgres/BUCK`.

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test //src/control-plane/postgres:iceberg-inline > /tmp/t.log 2>&1; grep -E "error\[|FAIL|not found" /tmp/t.log | head`
Expected: FAIL — `inline_parquet` not found.

- [ ] **Step 3: Implement `inline_parquet`**

Append to `src/control-plane/postgres/src/iceberg_inline.rs`. It resolves the table's live `table_id`, returns `None` if the inline table is absent or has no live rows at `at`, else builds an arrow-57 batch and encodes Parquet bytes.

```rust
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::ArrayRef;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use control_plane_core::SnapshotId;
use parquet57::arrow::ArrowWriter;
use sqlx::Row;

use crate::iceberg_catalog::IcebergCatalog;
use crate::iceberg_mirror::live_table_id;

/// Arrow field for a logical column (canonical, non-`*View` so it matches the
/// file-Parquet side the slice-3 engine reads).
fn arrow_field(name: &str, logical: &str, nullable: bool) -> Result<Field> {
    let dt = match logical {
        "integer" => DataType::Int32,
        "long" => DataType::Int64,
        "double" => DataType::Float64,
        "boolean" => DataType::Boolean,
        "string" => DataType::Utf8,
        "date" => DataType::Date32,
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        other => {
            return Err(ControlPlaneError::Backend(
                format!("inline read: unsupported type {other:?}").into(),
            ));
        }
    };
    Ok(Field::new(name, dt, nullable))
}

impl IcebergCatalog {
    /// Encode `table`'s live inline rows at `at` to Parquet bytes (one row group),
    /// or `None` if there is no inline storage or no live inline rows. The serving
    /// engine drops these into an in-memory object store and unions them with the
    /// table's `file://` Parquet.
    pub async fn inline_parquet(
        &self,
        table: &TableRef,
        at: SnapshotId,
    ) -> Result<Option<Vec<u8>>> {
        let mut conn = self.pool.acquire().await.map_err(backend)?;
        let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name).await? else {
            return Ok(None);
        };

        // Inline storage may not exist (table never had an inline write).
        let exists: Option<String> = sqlx::query_scalar(AssertSqlSafe(format!(
            "select to_regclass('{}')::text",
            inline_table_name(tid)
        )))
        .fetch_one(&mut *conn)
        .await
        .map_err(backend)?;
        if exists.is_none() {
            return Ok(None);
        }

        // The column schema at `at` (logical types, in order).
        use control_plane_core::Catalog;
        let schema = self.schema(table, at).await?;
        let col_list = schema
            .columns
            .iter()
            .map(|c| format!("\"{}\"", c.name.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");

        let rows = sqlx::query(AssertSqlSafe(format!(
            "select {col_list} from {} \
             where begin_snapshot <= {} and (end_snapshot is null or end_snapshot > {}) \
             order by loom_row_id",
            inline_table_name(tid),
            at.0,
            at.0,
        )))
        .fetch_all(&mut *conn)
        .await
        .map_err(backend)?;
        if rows.is_empty() {
            return Ok(None);
        }

        // Build one arrow-57 array per column from the PG rows.
        let fields = schema
            .columns
            .iter()
            .map(|c| arrow_field(&c.name, &c.ty, c.nullable))
            .collect::<Result<Vec<_>>>()?;
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());
        for (i, c) in schema.columns.iter().enumerate() {
            arrays.push(column_array(&rows, i, &c.ty)?);
        }
        let arrow_schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)
            .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;

        let mut buf: Vec<u8> = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, arrow_schema, None)
            .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
        w.write(&batch)
            .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
        w.close()
            .map_err(|e| ControlPlaneError::Backend(e.to_string().into()))?;
        Ok(Some(buf))
    }
}

/// Build an arrow-57 array for column `i` (typed `logical`) from PG rows.
fn column_array(rows: &[sqlx::postgres::PgRow], i: usize, logical: &str) -> Result<ArrayRef> {
    macro_rules! get {
        ($ty:ty) => {
            rows.iter()
                .map(|r| r.try_get::<Option<$ty>, _>(i))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(backend)?
        };
    }
    Ok(match logical {
        "integer" => {
            let mut b = Int32Builder::new();
            for v in get!(i32) { b.append_option(v); }
            Arc::new(b.finish())
        }
        "long" => {
            let mut b = Int64Builder::new();
            for v in get!(i64) { b.append_option(v); }
            Arc::new(b.finish())
        }
        "double" => {
            let mut b = Float64Builder::new();
            for v in get!(f64) { b.append_option(v); }
            Arc::new(b.finish())
        }
        "boolean" => {
            let mut b = BooleanBuilder::new();
            for v in get!(bool) { b.append_option(v); }
            Arc::new(b.finish())
        }
        "string" => {
            let mut b = StringBuilder::new();
            for v in get!(String) { b.append_option(v); }
            Arc::new(b.finish())
        }
        "date" => {
            let mut b = Date32Builder::new();
            let epoch = time::macros::date!(1970 - 01 - 01);
            for v in get!(time::Date) {
                b.append_option(v.map(|d| (d - epoch).whole_days() as i32));
            }
            Arc::new(b.finish())
        }
        "timestamp" => {
            let mut b = TimestampMicrosecondBuilder::new();
            for v in get!(time::PrimitiveDateTime) {
                b.append_option(v.map(|t| {
                    (t.assume_utc() - time::OffsetDateTime::UNIX_EPOCH)
                        .whole_microseconds()
                        .try_into()
                        .unwrap_or(i64::MAX)
                }));
            }
            Arc::new(b.finish())
        }
        other => {
            return Err(ControlPlaneError::Backend(
                format!("inline read: unsupported type {other:?}").into(),
            ));
        }
    })
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `buck2 test //src/control-plane/postgres:iceberg-inline > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS (both tests in the file).

- [ ] **Step 5: Format, lint, commit**

```bash
buck2 run //tools:rustfmt -- src/control-plane/postgres/src/iceberg_inline.rs src/control-plane/postgres/tests/iceberg_inline.rs
buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' > /tmp/cl.log 2>&1; grep -iE "warning|error" /tmp/cl.log || echo clean
git add src/control-plane/postgres/src/iceberg_inline.rs src/control-plane/postgres/tests/iceberg_inline.rs src/control-plane/postgres/BUCK
git commit -m "feat(iceberg): IcebergCatalog::inline_parquet — encode live inline rows"
```

---

## Task 4: Read union in the DataFusion serving engine

Union a table's inline Parquet (from `inline_parquet`) with its `file://` Parquet by registering the bytes in an `InMemory` object store and adding the `memory://` URL to the same `ListingTable`.

**Files:**
- Modify: `src/services/query-api/src/serving_datafusion.rs`
- Create: `src/services/query-api/tests/datafusion_inline_union.rs`
- Modify: `src/services/query-api/BUCK`

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/datafusion_inline_union.rs`:

```rust
//! fetch_rows unions a table's Parquet files with its inline rows. loom_fixture_test
//! (Postgres + LocalFsStorage; no DuckDB).

use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use query_api::serving::{ServingEngine, SqlValue};
use query_api::serving_datafusion::DataFusionServingEngine;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_rows_unions_file_and_inline() {
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    writer.seed("sales", "orders", &cols, &[3]).await; // file rows id 0,1,2
    writer
        .inline("sales", "orders", &cols, &[(100, "row100")], uuid::Uuid::new_v4())
        .await; // inline row id 100

    let engine = DataFusionServingEngine::new(IcebergCatalog::new(pool));
    let sql = "SELECT \"id\", \"name\" FROM \"sales\".\"orders\" ORDER BY \"id\"";
    let rows = engine.fetch_rows(sql, &[]).await.expect("fetch_rows");

    // 3 file rows (0,1,2) + 1 inline row (100), unioned.
    let ids: Vec<i64> = rows
        .rows
        .iter()
        .map(|r| match &r[0] {
            SqlValue::Int(n) => *n,
            other => panic!("id not int: {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec![0, 1, 2, 100], "file rows + inline row unioned");
    assert_eq!(
        rows.rows.last().unwrap()[1],
        SqlValue::Text("row100".into()),
        "the inline row's name reads back"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_rows_inline_only_table() {
    // A table that only ever had inline writes (no Parquet files) still reads back —
    // exercises the "no file:// URLs, only memory://" registration path.
    let fx = PgFixture::start();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);

    let writer = IcebergWriter::new(pool.clone(), dsn);
    let cols = vec![
        ("id".to_string(), "long".to_string(), false),
        ("name".to_string(), "string".to_string(), false),
    ];
    writer
        .inline("events", "audit", &cols, &[(7, "seven")], uuid::Uuid::new_v4())
        .await;

    let engine = DataFusionServingEngine::new(IcebergCatalog::new(pool));
    let rows = engine
        .fetch_rows(
            "SELECT \"id\", \"name\" FROM \"events\".\"audit\" WHERE (\"id\" = ?)",
            &[SqlValue::Int(7)],
        )
        .await
        .expect("fetch_rows");
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Int(7), SqlValue::Text("seven".into())]],
        "inline-only table reads back, with a filter spanning the (single) source"
    );
}
```

Add the target to `src/services/query-api/BUCK`:

```python
loom_fixture_test(
    name = "datafusion-inline-union",
    crate = "datafusion_inline_union",
    srcs = ["tests/datafusion_inline_union.rs"],
    crate_root = "tests/datafusion_inline_union.rs",
    deps = [
        ":query-api",
        "//src/control-plane/postgres:postgres",
        "//third-party:uuid",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run to verify it fails**

Run: `buck2 test //src/services/query-api:datafusion-inline-union > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|assertion|error\[" /tmp/t.log | head`
Expected: FAIL — the inline row (100) is missing from `ids` (only `[0,1,2]`), because the engine doesn't union inline data yet.

- [ ] **Step 3: Union inline Parquet in `register_iceberg_table`**

In `src/services/query-api/src/serving_datafusion.rs`, add imports for the in-memory store and a `Path`:

```rust
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
```

Then in `register_iceberg_table`, after the `file://` `urls` vector is built and before `ListingTableConfig`, insert the inline union. The function already has `ctx`, `catalog`, `table`, `snap`, and `urls` in scope:

```rust
    // Union inline rows (mirror-only typed rows) with the file Parquet: encode them
    // to in-memory Parquet and add a memory:// URL to the same ListingTable.
    let mut urls = urls; // make mutable
    if let Some(bytes) = catalog
        .inline_parquet(table, snap.id)
        .await
        .map_err(to_serving)?
    {
        let mem = Arc::new(InMemory::new());
        let key = format!("inline/{}_{}_{}.parquet", table.schema, table.name, snap.id.0);
        mem.put(&ObjPath::from(key.clone()), bytes.into())
            .await
            .map_err(to_serving)?;
        let mem_url = ObjectStoreUrl::parse("memory://").map_err(to_serving)?;
        ctx.register_object_store(mem_url.as_ref(), mem);
        urls.push(ListingTableUrl::parse(format!("memory:///{key}")).map_err(to_serving)?);
    }
```

Notes for the implementer:
- `urls` was previously `let urls: Vec<ListingTableUrl> = ...`. Either change it to `let mut urls` at its definition and drop the `let mut urls = urls;` shim, or keep the shim — both compile; prefer changing the original binding to `mut` and removing the shim.
- `object_store` is already a query-api dependency (slice 3). `InMemory`/`memory://` need no new dep.
- The `memory://` store is registered per query on the fresh `SessionContext`, alongside the `file://` store already registered.

- [ ] **Step 4: Run to verify it passes**

Run: `buck2 test //src/services/query-api:datafusion-inline-union > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|assertion|error\[" /tmp/t.log | head`
Expected: PASS (`ids == [0,1,2,100]`).

Also re-run the slice-3 engine test to confirm no regression for the file-only path:

Run: `buck2 test //src/services/query-api:datafusion-serving //src/services/query-api:datafusion-register > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Format, lint, commit**

```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/serving_datafusion.rs src/services/query-api/tests/datafusion_inline_union.rs
buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/cl.log 2>&1; grep -iE "warning|error" /tmp/cl.log || echo clean
git add src/services/query-api/src/serving_datafusion.rs src/services/query-api/tests/datafusion_inline_union.rs src/services/query-api/BUCK
git commit -m "feat(query-api): union inline rows with file Parquet in the serving engine"
```

---

## Task 5: Full-suite green + roadmap update

**Files:**
- Modify: `docs/spike/ICEBERG_ROADMAP.md`

- [ ] **Step 1: Run the whole first-party suite**

Run: `buck2 test //src/... > /tmp/full.log 2>&1; grep -E "Tests finished|FAIL|Build failure" /tmp/full.log | tail -3`
Expected: PASS, 0 failures.

- [ ] **Step 2: Clippy across all first-party Rust**

Run: `tools/clippy-all.sh > /tmp/c.log 2>&1; grep -iE "warning|error" /tmp/c.log || echo clean`
Expected: clean.

- [ ] **Step 3: Update the roadmap**

In `docs/spike/ICEBERG_ROADMAP.md`, add a "Slice A — inline writes + read union" entry under Done (loom-native inline writes: typed rows in `iceberg_mirror.inline_<id>`, mirror-only commit with atomic lineage, unioned with file Parquet by the DataFusion engine via in-memory Parquet; external Iceberg clients see inline data only after a future flush). Note the remaining inline follow-ups (flush/compaction; threshold + ingest-binary wiring = Slice B).

- [ ] **Step 4: Commit**

```bash
git add docs/spike/ICEBERG_ROADMAP.md
git commit -m "docs(iceberg): mark inline writes + read union (slice A) done"
```

---

## Notes for the executor

- **Arrow major hygiene:** all inline RecordBatch construction lives in the **postgres** crate (arrow 57 / parquet 57). query-api only ever handles opaque Parquet bytes — never construct an arrow batch in query-api for inline.
- **sqlx type bindings:** the `time` crate types (`time::Date`, `time::PrimitiveDateTime`) bind/decode against Postgres `date`/`timestamp` via sqlx's `time` feature (already enabled). `Option<T>` binds NULL.
- **No migration:** the per-table `inline_<id>` tables are runtime DDL under the existing `iceberg_mirror` schema; no `.sqlx` cache entry is generated (these are `AssertSqlSafe` runtime queries, not `query!`).
- **Out of scope (do not build):** the inline-vs-Parquet threshold + `LandingBackend`/ingest wiring (Slice B); flush/compaction; a vendored PG TableProvider; schema evolution of inline tables; types beyond the scalar set the maps cover.
- **DataFusion API note:** if `memory://` registration/URL parsing fights you, mirror the `file://` pattern already in `register_iceberg_table` (`ObjectStoreUrl` + `register_object_store` + `ListingTableUrl::parse`). The InMemory store must be registered on the same `ctx` before the `ListingTable` is built.
```
