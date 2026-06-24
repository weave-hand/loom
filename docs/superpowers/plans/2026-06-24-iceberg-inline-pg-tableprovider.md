# Inline reads via a Postgres `TableProvider` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve un-flushed Iceberg inline rows directly from Postgres through a DataFusion `TableProvider` that pushes filter/limit (and projection) into a per-query `SELECT`, eliminating the Arrow→Parquet→Arrow round-trip in `IcebergCatalog::inline_parquet`.

**Architecture:** A new `PgTableProvider` (in `src/services/query-api/src/pg_table_provider.rs`) holds a `sqlx::PgPool`, a relation name (`iceberg_mirror.inline_<tid>`), the table's authoritative arrow-58 schema, the parallel loom logical-type list, and a fixed **base predicate** that encodes the per-query MVCC snapshot filter. Its `scan` builds `SELECT <proj> FROM <rel> WHERE <base> [AND <pushed filters>] [LIMIT n]` (filter fragments via DataFusion's `Unparser::expr_to_sql`), runs it over the pool, decodes the rows to one arrow-58 `RecordBatch`, and returns it wrapped in an in-memory `MemTable` scan. `register_iceberg_table` constructs this provider per query (snapshot baked into the base predicate) in place of the `inline_parquet`→`InMemory`→`ListingTable` block. `inline_parquet` is then deleted and its callers migrated to the surviving `inline_live_batch`.

**Tech Stack:** Rust 2024, DataFusion 54, arrow 58 (query-api side), sqlx (`PgPool`), buck2.

## Global Constraints

- **DataFusion is pinned at 54**, arrow at **58** in the query-api crate (`src/services/query-api/Cargo.toml:20-21`, `default-features = false, features = ["parquet", "sql"]` — the `sql` feature provides `datafusion::sql::unparser`). The postgres crate is arrow **57**; do NOT try to share arrow types across that boundary.
- **No inline `#[cfg(test)]` tests.** Every test is a sibling `tests/<name>.rs` wired as its own target in the crate `BUCK`. The `no-inline-tests` prek hook fails the build otherwise.
- **Fixture-backed tests (hermetic Postgres/DuckDB) MUST use the `loom_fixture_test` macro**, not bare `rust_test`, or they route to remote execution and fail as root. Pure-logic tests use `rust_test`.
- **Dynamic SQL uses `sqlx::AssertSqlSafe`**, consistent with the existing precedent in `iceberg_inline.rs`: the base predicate is built from a trusted integer snapshot id, the relation from an internal table id, and pushed filter fragments come from DataFusion's `Unparser` over the already-compiled governed query (loom already inlines params before `ctx.sql`).
- **Markdown lint:** any `.md` touched must end with exactly one trailing newline and have no trailing whitespace (`end-of-file-fixer` / `trim trailing whitespace` hooks).
- **Run the full suite before finishing:** `buck2 test //src/...` (a green per-crate build is not enough — `inline_parquet`'s deletion touches postgres, worker, and engine test crates). Redirect to a file and grep; never pipe `buck2 test` through `tail`.

## Deliberate divergence from the spec (read before Task 1)

The spec (`docs/superpowers/specs/2026-06-22-iceberg-inline-pg-tableprovider-design.md`) calls for **copying** upstream `datafusion-contrib/datafusion-table-providers` files (`SqlTable<T,P>`, `SqlExec`, the postgres binding) and adapting them. This plan instead builds a **purpose-built, query-api-local provider** because the three mandated adaptations gut the upstream code:

1. **Adaptation #2 (sqlx, not bb8/tokio-postgres)** removes the entire generic `DbConnectionPool<T,P>` / `DbConnection` / sync-async-bridge layer — the bulk of the upstream surface — leaving nothing of the pool machinery to "adapt."
2. **The arrow-major boundary** (postgres = arrow 57, query-api = arrow 58) makes the spec's "reuse `column_array`/`arrow_field`" impossible across crates; the row→Arrow decode must be reimplemented arrow-58-native in query-api regardless.
3. **Upstream's `scan_to_sql` is itself just `LogicalPlanBuilder` + `Unparser`** — DataFusion's own SQL generation. We use the same DataFusion primitive (`Unparser::expr_to_sql` per pushed filter, the exact call upstream's physical filter-pushdown path uses), so we vendor the *technique*, not a generic abstraction we'd immediately gut.

This delivers every stated goal of the spec — no Parquet round-trip, filter/projection/limit pushdown into the inline `SELECT`, a reusable PG `TableProvider`, and the base-predicate MVCC hook — with far less dead generic surface and full DF-54/arrow-58 nativeness. `PostgresTableWriter` (the write side) remains explicitly out of scope, as the spec requires.

---

### Task 1: `PgTableProvider` skeleton + `build_scan_sql` (pure-logic, fully unit-tested)

Create the provider type and the pure SQL-building function it delegates to. `build_scan_sql` takes no pool, so it is unit-testable without Postgres — this validates the `Unparser::expr_to_sql` API shape and the base-predicate splice **before** any fixture work.

**Files:**
- Create: `src/services/query-api/src/pg_table_provider.rs`
- Modify: `src/services/query-api/src/lib.rs` (declare `pub mod pg_table_provider;`)
- Test: `src/services/query-api/tests/pg_scan_sql.rs` (new)
- Modify: `src/services/query-api/BUCK` (new `rust_test` target `pg-scan-sql`)

**Interfaces:**
- Produces:
  - `pub struct PgTableProvider { pool: sqlx::PgPool, relation: String, schema: datafusion::arrow::datatypes::SchemaRef, logical_types: Vec<String>, base_filter: Option<String> }`
  - `pub fn new(pool: sqlx::PgPool, relation: String, schema: SchemaRef, logical_types: Vec<String>, base_filter: Option<String>) -> PgTableProvider`
  - `pub(crate) fn build_scan_sql(relation: &str, schema: &arrow::datatypes::Schema, base_filter: Option<&str>, projection: Option<&Vec<usize>>, filters: &[datafusion::logical_expr::Expr], limit: Option<usize>) -> String`
  - `fn quote_ident(name: &str) -> String` (renders `"name"`, doubling embedded `"`).
  - The `TableProvider` impl is added in Task 2 (this task leaves the struct + `build_scan_sql` only, plus `new`).

- [ ] **Step 1: Write the failing test**

`src/services/query-api/tests/pg_scan_sql.rs`:

```rust
//! Pure-logic tests for the inline PG TableProvider's SQL generation
//! (`build_scan_sql`): projection, limit, the base (MVCC) predicate, and pushed
//! filter fragments via DataFusion's Unparser. No Postgres needed.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::{col, lit};
use query_api::pg_table_provider::build_scan_sql;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ])
}

const BASE: &str = "begin_snapshot <= 42 and (end_snapshot is null or end_snapshot > 42)";

#[test]
fn all_columns_with_base_predicate() {
    let sql = build_scan_sql("iceberg_mirror.inline_7", &schema(), Some(BASE), None, &[], None);
    assert_eq!(
        sql,
        format!(
            "SELECT \"id\", \"name\" FROM iceberg_mirror.inline_7 WHERE {BASE}"
        )
    );
}

#[test]
fn projection_limits_select_list() {
    // Project only column 1 ("name").
    let sql = build_scan_sql(
        "iceberg_mirror.inline_7",
        &schema(),
        Some(BASE),
        Some(&vec![1]),
        &[],
        Some(5),
    );
    assert_eq!(
        sql,
        format!("SELECT \"name\" FROM iceberg_mirror.inline_7 WHERE {BASE} LIMIT 5")
    );
}

#[test]
fn pushed_filter_is_anded_after_base() {
    // id > 50  ->  unparser renders `"id" > 50`, ANDed after the base predicate.
    let filters = vec![col("id").gt(lit(50_i64))];
    let sql = build_scan_sql("iceberg_mirror.inline_7", &schema(), Some(BASE), None, &filters, None);
    assert_eq!(
        sql,
        format!(
            "SELECT \"id\", \"name\" FROM iceberg_mirror.inline_7 WHERE {BASE} AND (\"id\" > 50)"
        )
    );
}

#[test]
fn empty_projection_selects_constant() {
    // COUNT(*)-style: DataFusion may project zero columns.
    let sql = build_scan_sql(
        "iceberg_mirror.inline_7",
        &schema(),
        Some(BASE),
        Some(&vec![]),
        &[],
        None,
    );
    assert_eq!(sql, format!("SELECT 1 FROM iceberg_mirror.inline_7 WHERE {BASE}"));
}

#[test]
fn no_base_filter_no_where() {
    let sql = build_scan_sql("iceberg_mirror.inline_7", &schema(), None, None, &[], None);
    assert_eq!(sql, "SELECT \"id\", \"name\" FROM iceberg_mirror.inline_7");
}

// Keep an unused Arc import from being a hard error if a later refactor drops it.
#[allow(dead_code)]
fn _arc_marker() -> Option<Arc<()>> {
    None
}
```

- [ ] **Step 2: Add the BUCK target and run the test to verify it fails to compile**

Add to `src/services/query-api/BUCK` (near the other pure-logic `rust_test`s):

```python
rust_test(
    name = "pg-scan-sql",
    crate = "pg_scan_sql",
    srcs = ["tests/pg_scan_sql.rs"],
    crate_root = "tests/pg_scan_sql.rs",
    edition = "2024",
    deps = [
        ":query-api",
        "//third-party:arrow",
        "//third-party:datafusion",
    ],
)
```

Run: `buck2 test //src/services/query-api:pg-scan-sql > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[|cannot find" /tmp/t.log`
Expected: FAIL — `build_scan_sql` / `pg_table_provider` module not found.

- [ ] **Step 3: Create the module with the struct, `new`, and `build_scan_sql`**

`src/services/query-api/src/pg_table_provider.rs`:

```rust
//! A DataFusion `TableProvider` that serves a Postgres relation directly over
//! loom's `sqlx::PgPool`, pushing projection/filter/limit into a per-scan
//! `SELECT`. Built for Iceberg inline rows (`iceberg_mirror.inline_<tid>`), where
//! a fixed *base predicate* carries the per-query MVCC snapshot filter, but the
//! type is relation-agnostic and reusable for any "DataFusion scans Postgres"
//! need. See docs/superpowers/specs/2026-06-22-iceberg-inline-pg-tableprovider-design.md.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::PostgreSqlDialect;
use sqlx::{AssertSqlSafe, PgPool, Row};

use crate::serving::ServingError;

/// Serves a Postgres relation as a DataFusion table. `scan` generates
/// `SELECT <proj> FROM <relation> WHERE <base_filter> [AND <pushed>] [LIMIT n]`,
/// runs it over `pool`, and decodes the result to one arrow-58 batch.
#[derive(Debug, Clone)]
pub struct PgTableProvider {
    pool: PgPool,
    /// The Postgres relation, e.g. `iceberg_mirror.inline_7`. Trusted (built from
    /// an internal table id); spliced into SQL via `AssertSqlSafe`.
    relation: String,
    /// The authoritative arrow-58 schema the provider presents (table schema).
    schema: SchemaRef,
    /// loom logical type per column, parallel to `schema.fields()` — drives the
    /// PG-row → arrow decode (the seven logical types in `arrow_field`).
    logical_types: Vec<String>,
    /// A fixed predicate always ANDed into the scan's WHERE (the MVCC snapshot
    /// filter for inline rows). `None` for an unfiltered scan.
    base_filter: Option<String>,
}

impl PgTableProvider {
    pub fn new(
        pool: PgPool,
        relation: String,
        schema: SchemaRef,
        logical_types: Vec<String>,
        base_filter: Option<String>,
    ) -> Self {
        Self {
            pool,
            relation,
            schema,
            logical_types,
            base_filter,
        }
    }
}

/// Quote a SQL identifier (double embedded `"`), matching `iceberg_inline.rs`.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Build the scan SQL. Pure (no pool) so it is unit-testable. Projection picks the
/// SELECT list (empty projection -> `SELECT 1` for COUNT(*)-style scans); the base
/// predicate and each unparseable-free pushed filter are ANDed into one WHERE; a
/// filter the unparser cannot render is skipped (the provider reports `Inexact`,
/// so DataFusion re-applies it — correctness over completeness).
pub(crate) fn build_scan_sql(
    relation: &str,
    schema: &Schema,
    base_filter: Option<&str>,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> String {
    let select_list = match projection {
        Some(idx) if idx.is_empty() => "1".to_string(),
        Some(idx) => idx
            .iter()
            .map(|&i| quote_ident(schema.field(i).name()))
            .collect::<Vec<_>>()
            .join(", "),
        None => schema
            .fields()
            .iter()
            .map(|f| quote_ident(f.name()))
            .collect::<Vec<_>>()
            .join(", "),
    };

    let unparser = Unparser::new(&PostgreSqlDialect {});
    let mut conds: Vec<String> = Vec::new();
    if let Some(bf) = base_filter {
        conds.push(bf.to_string());
    }
    for f in filters {
        if let Ok(sql) = unparser.expr_to_sql(f) {
            conds.push(format!("({sql})"));
        }
    }
    let where_clause = if conds.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conds.join(" AND "))
    };
    let limit_clause = limit.map(|n| format!(" LIMIT {n}")).unwrap_or_default();
    format!("SELECT {select_list} FROM {relation}{where_clause}{limit_clause}")
}
```

> NOTE for the implementer: the imports `RecordBatch`, `RecordBatchOptions`, `ArrayRef`, `DataType`, `TimeUnit`, `Any`, `async_trait`, `Session`, `TableProvider`, `TableProviderFilterPushDown`, `TableType`, `ExecutionPlan`, `MemTable`, `AssertSqlSafe`, `Row`, `ServingError` are used by Task 2; add them now (as above) or expect `unused_import` until Task 2 lands — prefer adding them now since Task 2 immediately follows. If clippy flags unused imports between tasks, that is expected and resolved by Task 2.

- [ ] **Step 4: Declare the module**

In `src/services/query-api/src/lib.rs`, add alongside the other `pub mod` lines (after `pub mod params;`, keeping alphabetical-ish order with the neighbors):

```rust
pub mod pg_table_provider;
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test //src/services/query-api:pg-scan-sql > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS (5 tests). If `Unparser` renders a filter differently than `("id" > 50)` (e.g. without parens or with a different spacing), adjust the `pushed_filter_is_anded_after_base` assertion to the exact rendered string — the parens come from our `format!("({sql})")` wrapper, so only the inner `"id" > 50` is unparser-controlled.

- [ ] **Step 6: Commit**

```bash
git add src/services/query-api/src/pg_table_provider.rs src/services/query-api/src/lib.rs src/services/query-api/tests/pg_scan_sql.rs src/services/query-api/BUCK
git commit -m "feat(query-api): inline PG TableProvider skeleton + scan-SQL builder"
```

---

### Task 2: Implement `TableProvider` + wire it into `register_iceberg_table`

Add the `TableProvider` impl (run SQL over the pool, decode to an arrow-58 batch, wrap in a `MemTable` scan) and replace the `inline_parquet`→`InMemory`→`ListingTable` block. The existing fixture tests `datafusion_inline_union` and `datafusion_serving` are the behavior-preservation guard.

**Files:**
- Modify: `src/services/query-api/src/pg_table_provider.rs` (add decode fn + `TableProvider` impl)
- Modify: `src/services/query-api/src/serving_datafusion.rs:126-152` (replace inline branch) and its imports
- Test (guard, unchanged): `src/services/query-api/tests/datafusion_inline_union.rs`, `tests/datafusion_serving.rs`

**Interfaces:**
- Consumes (from Task 1): `PgTableProvider`, `build_scan_sql`.
- Consumes (loom, all already `pub`): `control_plane_postgres::iceberg_inline::{inline_table_name, has_live_inline_rows}`, `control_plane_postgres::iceberg_mirror::live_table_id`, `IcebergCatalog.pool` (public field).
- Produces:
  - `fn pg_rows_to_arrays(rows: &[sqlx::postgres::PgRow], logical_types: &[String]) -> Result<Vec<ArrayRef>, ServingError>` (arrow-58 decode, mirrors `iceberg_inline::column_array` per the seven logical types).
  - The `#[async_trait] impl TableProvider for PgTableProvider`.

- [ ] **Step 1: Write the failing guard check**

There is no new test in this task — the *existing* `datafusion_inline_union` fixture test is the failing/guard test. First confirm it currently passes (baseline), then it must still pass after the rewrite.

Run: `buck2 test //src/services/query-api:datafusion-inline-union > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (baseline — 2 tests). Record this; the rewrite must keep it green.

- [ ] **Step 2: Add the arrow-58 row decoder and the `TableProvider` impl**

Append to `src/services/query-api/src/pg_table_provider.rs`:

```rust
/// Decode `rows` to one arrow-58 array per column, typed by `logical_types[i]`
/// (positional — the SELECT list order). Mirrors `iceberg_inline::column_array`,
/// but arrow-58-native (the postgres crate is arrow-57; types do not cross that
/// boundary). sqlx decodes to plain Rust types, so only the arrow side differs.
fn pg_rows_to_arrays(
    rows: &[sqlx::postgres::PgRow],
    logical_types: &[String],
) -> Result<Vec<ArrayRef>, ServingError> {
    use arrow::array::builder::{
        BooleanBuilder, Date32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
        TimestampMicrosecondBuilder,
    };

    let map_err = |e: sqlx::Error| ServingError::Engine(e.to_string());
    macro_rules! get {
        ($ty:ty, $i:expr) => {
            rows.iter()
                .map(|r| r.try_get::<Option<$ty>, _>($i))
                .collect::<Result<Vec<_>, _>>()
                .map_err(map_err)?
        };
    }

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(logical_types.len());
    for (i, logical) in logical_types.iter().enumerate() {
        let array: ArrayRef = match logical.as_str() {
            "integer" => {
                let mut b = Int32Builder::new();
                for v in get!(i32, i) {
                    b.append_option(v);
                }
                Arc::new(b.finish())
            }
            "long" => {
                let mut b = Int64Builder::new();
                for v in get!(i64, i) {
                    b.append_option(v);
                }
                Arc::new(b.finish())
            }
            "double" => {
                let mut b = Float64Builder::new();
                for v in get!(f64, i) {
                    b.append_option(v);
                }
                Arc::new(b.finish())
            }
            "boolean" => {
                let mut b = BooleanBuilder::new();
                for v in get!(bool, i) {
                    b.append_option(v);
                }
                Arc::new(b.finish())
            }
            "string" => {
                let mut b = StringBuilder::new();
                for v in get!(String, i) {
                    b.append_option(v);
                }
                Arc::new(b.finish())
            }
            "date" => {
                let mut b = Date32Builder::new();
                let epoch = time::macros::date!(1970 - 01 - 01);
                for v in get!(time::Date, i) {
                    b.append_option(v.map(|d| (d - epoch).whole_days() as i32));
                }
                Arc::new(b.finish())
            }
            "timestamp" => {
                let mut b = TimestampMicrosecondBuilder::new();
                for v in get!(time::PrimitiveDateTime, i) {
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
                return Err(ServingError::Engine(format!(
                    "inline PG provider: unsupported logical type {other:?}"
                )));
            }
        };
        arrays.push(array);
    }
    Ok(arrays)
}

impl PgTableProvider {
    /// Run `sql`, decode the projected columns (`proj_logicals`, in SELECT order)
    /// into one batch with `proj_schema`. For an empty SELECT list (`SELECT 1`),
    /// `proj_schema` is empty and the batch carries only the row count.
    async fn fetch_batch(
        &self,
        sql: String,
        proj_schema: SchemaRef,
        proj_logicals: &[String],
    ) -> Result<RecordBatch, ServingError> {
        let rows = sqlx::query(AssertSqlSafe(sql))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;

        if proj_schema.fields().is_empty() {
            // Empty projection (e.g. COUNT(*)): a 0-column batch with the row count.
            let opts = RecordBatchOptions::new().with_row_count(Some(rows.len()));
            return RecordBatch::try_new_with_options(proj_schema, vec![], &opts)
                .map_err(|e| ServingError::Engine(e.to_string()));
        }
        let arrays = pg_rows_to_arrays(&rows, proj_logicals)?;
        RecordBatch::try_new(proj_schema, arrays).map_err(|e| ServingError::Engine(e.to_string()))
    }
}

#[async_trait]
impl TableProvider for PgTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        // Inexact: we push what unparses into the SQL, but DataFusion must still
        // re-apply every predicate (an unparseable filter is silently skipped in
        // `build_scan_sql`, so the SQL is never *more* restrictive than asked).
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Empty projection is pushed as `SELECT 1` (handled in build_scan_sql); we
        // then materialize a 0-column batch and let MemTable surface the row count.
        let sql = build_scan_sql(
            &self.relation,
            self.schema.as_ref(), // &SchemaRef -> &Schema explicitly (avoid deref-coercion thrash)
            self.base_filter.as_deref(),
            projection,
            filters,
            limit,
        );

        // Projected schema + parallel logical types for the decode.
        let (proj_schema, proj_logicals): (SchemaRef, Vec<String>) = match projection {
            Some(idx) if idx.is_empty() => (Arc::new(Schema::empty()), Vec::new()),
            Some(idx) => {
                let s = self
                    .schema
                    .project(idx)
                    .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
                let l = idx.iter().map(|&i| self.logical_types[i].clone()).collect();
                (Arc::new(s), l)
            }
            None => (self.schema.clone(), self.logical_types.clone()),
        };

        let batch = self
            .fetch_batch(sql, proj_schema.clone(), &proj_logicals)
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // The SQL already applied projection/filter/limit, so the MemTable scan
        // adds nothing on top.
        let mem = MemTable::try_new(proj_schema, vec![vec![batch]])?;
        mem.scan(state, None, &[], None).await
    }
}
```

> NOTE: `ServingError` must be `std::error::Error + Send + Sync + 'static` for `DataFusionError::External(Box::new(e))`. Verify `serving::ServingError` derives `thiserror::Error` (it does — `#[error("serving engine: {0}")] Engine(String)`), which gives `std::error::Error`; it is `Send + Sync` because it wraps only a `String`. If the `ArrowError` variant constructor signature differs in DF 54 (e.g. no second `None` arg), adjust to the compiler's suggestion — map any arrow `project` error to a `DataFusionError`.

- [ ] **Step 3: Replace the inline branch in `register_iceberg_table`**

In `src/services/query-api/src/serving_datafusion.rs`, replace lines 126-152 (the `inline_provider` block from the `// Inline rows (mirror-only typed rows)...` comment through the closing `};`) with:

```rust
    // Inline rows (mirror-only typed rows) are served DIRECTLY from Postgres via
    // a PG TableProvider that pushes filter/limit/projection into a per-query
    // SELECT — no Arrow->Parquet->Arrow round-trip. The snapshot is baked into a
    // base predicate so MVCC visibility matches `inline_live_batch`.
    let inline_provider =
        build_inline_provider(catalog, table, &schema, &table_schema.columns, snap.id).await?;
```

Then change the `inline_provider` consumption. The match at (old) lines 160-174 builds the union; `inline_provider` is now `Option<PgTableProvider>` instead of `Option<ListingTable>`. The arms already do `ctx.read_table(Arc::new(i))` and `Arc::new(i)`, which work unchanged because `PgTableProvider: TableProvider`. No edit to the match body is needed — only the type of `i` changes, inferred automatically.

Add this helper function near `register_iceberg_table` (e.g. immediately after it, before `listing_table`):

```rust
/// Build the inline PG provider for `table` at `at`, or `None` when there is no
/// inline storage or no live inline rows (preserving the prior `inline_parquet`
/// `None` behavior). `schema` is the table's authoritative arrow schema (already
/// built by the caller); `cols` are the mirror column defs (for logical types).
async fn build_inline_provider(
    catalog: &IcebergCatalog,
    table: &TableRef,
    schema: &SchemaRef,
    cols: &[control_plane_core::ColumnDef],
    at: control_plane_core::SnapshotId,
) -> Result<Option<crate::pg_table_provider::PgTableProvider>, ServingError> {
    use control_plane_postgres::iceberg_inline::{has_live_inline_rows, inline_table_name};
    use control_plane_postgres::iceberg_mirror::live_table_id;

    let mut conn = catalog.pool.acquire().await.map_err(to_serving)?;
    let Some(tid) = live_table_id(&mut conn, &table.schema, &table.name)
        .await
        .map_err(to_serving)?
    else {
        return Ok(None);
    };
    if !has_live_inline_rows(&mut conn, table, at)
        .await
        .map_err(to_serving)?
    {
        return Ok(None);
    }
    drop(conn);

    // MVCC base predicate over the inline storage's snapshot columns. `at.0` is a
    // trusted integer; spliced via AssertSqlSafe in the provider (iceberg_inline
    // precedent).
    let base = format!(
        "begin_snapshot <= {0} and (end_snapshot is null or end_snapshot > {0})",
        at.0
    );
    let logical_types: Vec<String> = cols.iter().map(|c| c.ty.clone()).collect();
    Ok(Some(crate::pg_table_provider::PgTableProvider::new(
        catalog.pool.clone(),
        inline_table_name(tid),
        schema.clone(),
        logical_types,
        Some(base),
    )))
}
```

- [ ] **Step 4: Drop now-unused imports in `serving_datafusion.rs`**

The `InMemory`, `ObjPath`, and `ObjectStoreExt` imports and the `memory://` registration are no longer used by the inline path. Check whether they are used elsewhere in the file (`grep -n 'InMemory\|ObjPath\|ObjectStoreExt\|memory://' src/services/query-api/src/serving_datafusion.rs`). If the only uses were in the deleted block, remove these imports (lines 37, 39, 40):

```rust
use object_store::ObjectStoreExt;
// keep object_store::local::LocalFileSystem (still used for file:// store)
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
```

Keep `object_store::local::LocalFileSystem` (still registered for file-backed paths). `listing_table` (lines 196-208) is now unused if nothing else calls it — `grep -n 'listing_table(' src/services/query-api/src/serving_datafusion.rs`; if the only caller was the deleted inline block, delete the `listing_table` fn too (and its now-unused `ListingTable`/`ListingOptions`/`ListingTableConfig`/`ParquetFormat` imports IF not used by `IcebergMirrorTableProvider::try_new`, which DOES use them — so keep those). Let the compiler/clippy guide which imports to drop; do not remove an import still referenced by `IcebergMirrorTableProvider`.

- [ ] **Step 5: Build, then run the behavior-guard tests**

Run:
```
buck2 build //src/services/query-api:query-api > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[|warning: unused" /tmp/b.log
buck2 test //src/services/query-api:datafusion-inline-union //src/services/query-api:datafusion-serving //src/services/query-api:pg-scan-sql > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: build SUCCEEDED with no unused-import warnings; all three test targets PASS. The inline-union test passing proves the new provider serves the same rows the `inline_parquet`→`ListingTable` path did.

- [ ] **Step 6: clippy + commit**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log` (empty == clean).

```bash
git add src/services/query-api/src/pg_table_provider.rs src/services/query-api/src/serving_datafusion.rs
git commit -m "feat(query-api): serve Iceberg inline rows via PG TableProvider (no Parquet round-trip)"
```

---

### Task 3: Delete `inline_parquet`; migrate its callers to `inline_live_batch`

`inline_parquet` is now unused by production code. Delete it and its `ArrowWriter` import; migrate the one decoding test caller to `inline_live_batch` and rename the assertion-only callers (which only check `Option::is_some/is_none`, valid for both signatures).

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` (delete the `inline_parquet` method — lines 445-462, the doc-comment + fn ending at the `}` before the final `impl` close; remove `use parquet57::arrow::ArrowWriter;` line 22). Delete **by method name**, not a hard line range — line numbers drift.
- Modify: `src/control-plane/postgres/tests/iceberg_inline.rs:62-115` (rewrite the decode test)
- Modify: `src/control-plane/postgres/tests/iceberg_flush.rs` (lines 116-119, 229-232, 259-262, 318-321, 337-340 — rename only)
- Modify: `src/services/worker/tests/e2e.rs` (lines 198-201, 285 — rename only)
- Modify: `src/services/engine/tests/wire.rs` (lines 319-322 — rename only)

**Interfaces:**
- `inline_live_batch(&self, table, at) -> Result<Option<(i64, Vec<i64>, RecordBatch)>>` survives (used by `iceberg_flush.rs:73`) and is the migration target. `.is_some()`/`.is_none()` work identically to the old `Option<Vec<u8>>`.

- [ ] **Step 1: Delete `inline_parquet` and its import**

In `src/control-plane/postgres/src/iceberg_inline.rs`:
- Delete the entire `inline_parquet` method — anchor on the method name / its doc comment (`/// Encode `table`'s live inline rows at `at` to Parquet bytes`) through the closing `}` before the final `impl` block close. (It is around lines 445-462, but line numbers drift — delete by name.)
- Delete `use parquet57::arrow::ArrowWriter;` (line 22).

- [ ] **Step 2: Rewrite the decoding test caller**

Replace `src/control-plane/postgres/tests/iceberg_inline.rs` lines 61-115 (the `inline_parquet_encodes_live_rows` test) with:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_live_batch_reconstructs_live_rows() {
    use control_plane_postgres::iceberg_catalog::IcebergCatalog;

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
        .inline(
            "sales",
            "orders",
            &cols,
            &[(100, "row100"), (101, "row101")],
            uuid::Uuid::new_v4(),
        )
        .await;

    let catalog = IcebergCatalog::new(pool);
    let table = control_plane_core::TableRef {
        schema: "sales".into(),
        name: "orders".into(),
    };
    let (_tid, row_ids, batch) = catalog
        .inline_live_batch(&table, control_plane_core::SnapshotId(snap))
        .await
        .expect("inline_live_batch")
        .expect("Some batch when inline rows exist");
    assert_eq!(batch.num_rows(), 2, "two live inline rows reconstructed");
    assert_eq!(row_ids.len(), 2, "two row ids returned");

    // A table that was never inline-written yields None at its seed snapshot.
    let other = control_plane_core::TableRef {
        schema: "sales".into(),
        name: "orders".into(),
    };
    let none = catalog
        .inline_live_batch(&other, control_plane_core::SnapshotId(1))
        .await
        .expect("inline_live_batch at snapshot 1");
    assert!(none.is_none(), "no inline rows live at the seed snapshot");
}
```

This removes the `parquet57`/`bytes` reader usage. After editing, check the test file's other tests for any remaining `parquet57`/`bytes::Bytes` references (`grep -n 'parquet57\|bytes::' src/control-plane/postgres/tests/iceberg_inline.rs`); if none remain, the `iceberg-inline` test target's `parquet57`/`bytes` deps are unused (harmless in buck, but remove them in Step 5 for cleanliness if present).

- [ ] **Step 3: Rename the assertion-only callers**

In each of these, replace `.inline_parquet(` with `.inline_live_batch(` and the `.expect("inline_parquet")` / `.expect("inline...")` message text may stay or be updated — the `.is_some()`/`.is_none()` assertions are unchanged:
- `src/control-plane/postgres/tests/iceberg_flush.rs`: lines 117, 230, 260, 319, 338.
- `src/services/worker/tests/e2e.rs`: lines 199, 285.
- `src/services/engine/tests/wire.rs`: line 320.

Use a targeted sed-free Edit per occurrence (they appear in distinct contexts). Example for `iceberg_flush.rs:116-120`:

```rust
    let inline = ice
        .inline_live_batch(&table, cur.id)
        .await
        .expect("inline_live_batch");
    assert!(inline.is_none(), "inline rows retired at current snapshot");
```

- [ ] **Step 4: Build the affected crates**

Run:
```
buck2 build //src/control-plane/postgres:postgres //src/services/worker:worker //src/services/engine:engine > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error\[|warning: unused" /tmp/b.log
```
Expected: SUCCEEDED. If `unused_import` fires for `parquet57` anywhere in `iceberg_inline.rs`, ensure `ArrowWriter` was the only `parquet57` use in that file (`grep -n parquet57 src/control-plane/postgres/src/iceberg_inline.rs`) — there should be none left; the `parquet57` named_dep on the *library* target is still used by other modules, so do not remove it from `BUCK`.

- [ ] **Step 5: Run the migrated/guard tests**

Run:
```
buck2 test //src/control-plane/postgres:iceberg-inline //src/control-plane/postgres:iceberg-flush //src/services/worker:e2e //src/services/engine:wire > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: all PASS. (Target names: verify with `grep -nE 'name = "iceberg-inline"|name = "iceberg-flush"' src/control-plane/postgres/BUCK` and the worker/engine BUCKs; adjust the labels above to the real target names if they differ.)

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_inline.rs src/control-plane/postgres/tests/iceberg_inline.rs src/control-plane/postgres/tests/iceberg_flush.rs src/services/worker/tests/e2e.rs src/services/engine/tests/wire.rs
git commit -m "refactor(iceberg): delete inline_parquet; migrate callers to inline_live_batch"
```

---

### Task 4: New regression tests — mixed UNION, time-travel MVCC, pushdown

Add a `loom_fixture_test` covering the three behaviors the spec calls out: a table with **both** inline rows and a Parquet file serves the correct `UNION ALL`; a **time-travel read at an older snapshot** returns only rows live then (the base-predicate MVCC filter); and a **WHERE/LIMIT pushdown** smoke check returns the correct subset.

**Files:**
- Create: `src/services/query-api/tests/inline_pg_provider_e2e.rs`
- Modify: `src/services/query-api/BUCK` (new `loom_fixture_test` target)

**Interfaces:**
- Consumes: `query_api::serving_datafusion::DataFusionServingEngine` (its `fetch_rows` is the read entry), `control_plane_postgres::iceberg_catalog::IcebergCatalog`, the test `IcebergWriter` seed/inline helpers used by `datafusion_inline_union.rs`.

- [ ] **Step 1: Read the existing fixture test to mirror its setup**

Read `src/services/query-api/tests/datafusion_inline_union.rs` in full and reuse its exact fixture/seed/inline helper imports and patterns (`PgFixture`, `IcebergWriter`, `DataFusionServingEngine`, the SQL shape with quoted idents). Do NOT invent new helpers; copy the working setup.

- [ ] **Step 2: Write the failing test**

`src/services/query-api/tests/inline_pg_provider_e2e.rs` (adapt the helper imports/paths to exactly match `datafusion_inline_union.rs` after reading it in Step 1):

```rust
//! End-to-end regression for the inline PG TableProvider: file+inline UNION,
//! time-travel MVCC via the base predicate, and WHERE/LIMIT pushdown.

// <-- Mirror the EXACT use-statements and fixture/helper constructors from
//     tests/datafusion_inline_union.rs (PgFixture, IcebergWriter, the
//     DataFusionServingEngine constructor, TableRef, SnapshotId, etc.).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unions_file_and_inline_rows() {
    // Seed >=1 Parquet file row AND >=1 live inline row in the same table, then
    // SELECT * and assert the row count == file_rows + inline_rows, and that an
    // inline-only id is present. (This mirrors
    // datafusion_inline_union::fetch_rows_unions_file_and_inline but is kept here
    // as the post-rewrite guard.)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn time_travel_excludes_rows_added_after_snapshot() {
    // NOTE: `IcebergWriter::inline(...)` RETURNS the snapshot id of the write (see
    // the Task 3 rewrite of `inline_live_batch_reconstructs_live_rows`, which does
    // `let snap = writer.inline(...).await;` and uses `SnapshotId(snap)`). Use that
    // return value to get snap_a/snap_b — no separate current_snapshot read needed.
    // 1. Inline-append row A -> snap_a = the returned snapshot id.
    // 2. Inline-append row B -> snap_b = the returned snapshot id (> snap_a).
    // 3. Read the table at snap_a via the serving engine: expect ONLY row A
    //    (row B's begin_snapshot > snap_a, so the base predicate excludes it).
    // 4. Read at snap_b: expect BOTH A and B.
    // The serving engine resolves `current_snapshot` per query; to read AS OF an
    // older snapshot, construct a DataFusionServingEngine path that registers the
    // table at snap_a. If `fetch_rows` always uses current_snapshot, drive
    // register_iceberg_table directly with snap_a (it is `pub`), build a
    // SessionContext, and run `SELECT ... ORDER BY id` — asserting the row set.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pushdown_where_and_limit_return_correct_rows() {
    // Seed several inline rows (ids 1..=5). Run `SELECT "id" FROM t WHERE "id" > 3
    // ORDER BY "id"` and assert {4,5}; run with LIMIT 1 and assert exactly one row.
    // Correctness holds regardless of pushdown (DataFusion re-applies Inexact
    // filters), so this guards both the pushed SQL and the union/decoding path.
}
```

> IMPLEMENTER GUIDANCE: For `time_travel_excludes_rows_added_after_snapshot`, prefer driving `query_api::serving_datafusion::register_iceberg_table(&ctx, &catalog, &table)` — but note it registers at `current_snapshot`, NOT an arbitrary `at`. To read as-of an older snapshot you need a registration at `snap_a`. Two options, pick the one that compiles cleanly:
> (a) If `register_iceberg_table` only supports current snapshot, assert MVCC at the **catalog** layer instead: call `catalog.inline_live_batch(&table, snap_a)` and `(&table, snap_b)` and assert the batch row counts (1 vs 2). This directly exercises the same base-predicate `begin_snapshot/end_snapshot` filter the provider uses, is deterministic, and needs no engine plumbing. **This is the recommended form** — it isolates the MVCC guarantee without depending on engine snapshot-selection wiring that is out of this slice's scope.
> (b) Only if as-of engine reads already exist, use them.
> Choose (a) unless as-of engine reads are trivially available.

- [ ] **Step 3: Add the BUCK target**

Add to `src/services/query-api/BUCK` (model on the `datafusion-inline-union` target — same deps):

```python
loom_fixture_test(
    name = "inline-pg-provider-e2e",
    crate = "inline_pg_provider_e2e",
    srcs = ["tests/inline_pg_provider_e2e.rs"],
    crate_root = "tests/inline_pg_provider_e2e.rs",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:tokio",
        "//third-party:uuid",
    ],
)
```

> Match the deps to whatever `datafusion_inline_union.rs` actually imports after Step 1 (it may need `//third-party:arrow` or others). Copy that target's `deps` list verbatim and add only what the new asserts require.

- [ ] **Step 4: Run the new test**

Run: `buck2 test //src/services/query-api:inline-pg-provider-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS (3 tests).

- [ ] **Step 5: Full suite + clippy + commit**

Run:
```
buck2 test //src/... > /tmp/full.log 2>&1; grep -E "Tests finished|FAIL" /tmp/full.log
./tools/clippy-all.sh > /tmp/clippy.log 2>&1; tail -5 /tmp/clippy.log
buck2 run //tools:rustfmt -- --check src/services/query-api/src/pg_table_provider.rs > /tmp/fmt.log 2>&1; cat /tmp/fmt.log
```
Expected: full suite PASS (no FAIL), clippy clean, fmt clean. If fmt reports diffs, run `buck2 run //tools:rustfmt -- src/services/query-api/src/pg_table_provider.rs` and re-commit.

```bash
git add src/services/query-api/tests/inline_pg_provider_e2e.rs src/services/query-api/BUCK
git commit -m "test(query-api): inline PG provider e2e — union, time-travel MVCC, pushdown"
```

---

## Documentation register update (at finish)

This work resolves `iss-iceberg-inline-reparse` and realizes `road-df-postgres-tableprovider`. At branch finish, run `loom-docs-update` to:
- Close `road-df-postgres-tableprovider` (`- [ ]`→`- [x]`, status `done`, add `pr:#N`).
- Mark `iss-iceberg-inline-reparse` fixed.
- Record any newly-deferred follow-ups surfaced here: projection-pushdown refinement is already implemented (kept), but note the **deliberate divergence** (purpose-built provider, not a verbatim upstream vendor) and that `PostgresTableWriter` / DataFusion PG *writes* remain deferred (the spec's noted follow-up), and the `(table_id, snapshot_id)` read cache remains deferred.

## Self-Review (completed by plan author)

**Spec coverage:**
- §"What gets vendored" / 3 adaptations → Task 1 (scan-SQL via `Unparser`, base predicate) + Task 2 (sqlx pool, arrow-58 decode = adaptation #2 + the arrow-boundary reality). DF 52→54 (#1) is subsumed: we target DF 54 natively. The "Deliberate divergence" section documents the conscious choice not to copy upstream verbatim, with rationale tied to each adaptation.
- §"Wiring the serving engine" → Task 2 Step 3 (replace inline branch; the `match (file_provider, inline_provider)` union is unchanged, as the spec states).
- §"Delete `inline_parquet`" → Task 3.
- §"Testing": behavior preserved (Task 2 guards via `datafusion_inline_union`/`datafusion_serving`); migrate `inline_parquet` callers (Task 3 — including the two extra callers in `iceberg_inline.rs` the spec missed); new regression for mixed union + time-travel MVCC + pushdown (Task 4).
- §"Scope boundary": read/scan only, inline side only — honored (no writer, file side untouched).

**Placeholder scan:** Task 4's test bodies are intentionally behavior-described (not full code) because they must mirror `datafusion_inline_union.rs`'s exact, unread-by-the-author helper imports; Step 1 forces reading that file first, and the recommended MVCC form (catalog-layer `inline_live_batch` assertion) is fully specified. All production code (Tasks 1-3) is complete.

**Type consistency:** `build_scan_sql` signature is identical in Task 1 (definition) and Task 2 (call). `PgTableProvider::new` arg order (pool, relation, schema, logical_types, base_filter) matches the `build_inline_provider` call. `inline_live_batch` return tuple `(i64, Vec<i64>, RecordBatch)` matches the Task 3 destructure `(_tid, row_ids, batch)`.
