# Iceberg per-column stats + predicate pushdown — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the loom-native Iceberg DataFusion serving engine per-file column statistics so it skips whole Parquet files a query's predicates cannot match (file skipping), eliminating the per-file footer-read + scan for non-matching files.

**Architecture:** Record per-file/per-column min/max/null-count in a new `iceberg_mirror.data_file_column_stat` table at write time (computed from the Parquet footer via the table's Iceberg `FileIO`); surface those stats through a concrete `IcebergCatalog::files_with_stats`; and replace the plain DataFusion `ListingTable` in `register_iceberg_table` with a custom `IcebergMirrorTableProvider` whose `scan` drops files a `PruningPredicate` proves cannot match before building the Parquet scan. Pruning is a pure performance optimization — files with no/insufficient stats are always kept, and DataFusion still re-applies the predicate per row (filters are pushed down `Inexact`), so results never change.

**Tech Stack:** Rust, DataFusion 54, Parquet 57 (iceberg 0.9 side) / 58 (query-api side), iceberg 0.9, sqlx 0.9 compile-time macros, Postgres, buck2.

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`/`#[test]` in `src/**.rs` (the `no-inline-tests` prek hook fails otherwise). Each test is a sibling `tests/<name>.rs` wired as its own target in the crate's `BUCK`.
- **Fixture-backed tests use the `loom_fixture_test` macro**, not a bare `rust_test` (they boot Postgres/DuckDB which refuse to run as root on RE).
- **Compile-time SQL.** Any new/changed `sqlx::query!`/`query_scalar!` in the postgres crate requires regenerating the committed `.sqlx` cache via `tools/sqlx-prepare.sh` and committing it. The `//src/control-plane/postgres:sqlx-cache-check` test enforces freshness.
- **Arrow/parquet version seam (critical).** `src/control-plane/postgres` uses **arrow/parquet 57** (iceberg 0.9; `parquet` is renamed `parquet57` in its `Cargo.toml`/`BUCK`). `src/services/datafusion-io` and `src/services/query-api` use **arrow/parquet 58 + datafusion 54** (bare `parquet`/`arrow`/`datafusion`). The parquet `Statistics` enum is a *different type* in each major, so `datafusion_io::write::file_stats_from_bytes` **cannot** be called from the postgres crate. The stats-merge logic is therefore **ported** against `parquet57` inside the postgres crate (Task 2). This is a forced consequence of the version split, not a deviation of convenience — record it in the new module's doc comment.
- **Iceberg-only.** Do not modify the DuckLake serving path, the shared `core::Catalog` trait, or `core::FileRef`. New surface (`FileWithStats`, `files_with_stats`) is concrete on `IcebergCatalog`.
- **Stat type taxonomy.** Bounds only for the `core::snapshot::StatValue` primitives mapped to clean iceberg types: `int`→`I32`, `long`→`I64`, `double`→`F64`, `boolean`→`Bool`, `string`→`Str`. Any other iceberg column type (`date`, `timestamp`, `float`, decimal, …) carries **no** min/max bound on read and is never pruned on (safe default). Null-counts are recorded for every column.
- **Don't pipe `buck2 test` through `tail`/`head`.** Redirect to a file: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- Markdown files (incl. this plan) end with exactly one trailing newline and no trailing whitespace (`end-of-file-fixer` / `trim trailing whitespace` hooks).

---

## File Structure

- **Create** `src/control-plane/postgres/migrations/0016_iceberg_mirror_file_column_stats.sql` — the `data_file_column_stat` table.
- **Create** `src/control-plane/postgres/src/iceberg_stats.rs` — port of the Parquet-footer → `core::ColumnStat` merge against `parquet57`, plus the `StatValue`↔text codec used by the write/read paths.
- **Modify** `src/control-plane/postgres/src/lib.rs` — declare `mod iceberg_stats;` (or `pub mod`, matching the existing `iceberg_*` modules' visibility).
- **Modify** `src/control-plane/postgres/src/iceberg_mirror.rs` — `ProjectedFile.column_stats`; `added_files_of` computes stats; `project_files` returns the inserted `data_file_id` and inserts stat rows.
- **Modify** `src/control-plane/postgres/src/iceberg_catalog.rs` — `FileWithStats` struct + `files_with_stats` read method.
- **Modify** `src/services/query-api/src/serving_datafusion.rs` — `IcebergMirrorTableProvider`; replace the file-backed `ListingTable` in `register_iceberg_table` with it.
- **Create** test files (one `rust_test`/`loom_fixture_test` target each): `tests/iceberg_mirror_provider.rs` (query-api, Task 1), `tests/iceberg_column_stats_unit.rs` (postgres, Task 2 — pure-logic) and `tests/iceberg_column_stats.rs` (postgres, Task 2 — fixture), `tests/iceberg_files_with_stats.rs` (postgres, Task 3), `tests/iceberg_pruning_e2e.rs` (query-api, Task 4).
- **Modify** `src/control-plane/postgres/BUCK` and `src/services/query-api/BUCK` — wire the new test targets.
- **Refresh** `src/control-plane/postgres/.sqlx/` (Tasks 2 & 3).

---

## Task 1: De-risk — `IcebergMirrorTableProvider` with file-level pruning

Proves the custom provider end to end **before** any data-side wiring (the spec's mandated first task). The DataFusion 54 APIs (`PruningPredicate`, `ScalarValue`-typed `ColumnStatistics`, assembling a `ParquetSource`/`FileScanConfig`/`DataSourceExec` over a chosen file set) are the unknown; resolve them here on a trivial two-file table.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_catalog.rs` (add the `FileWithStats` struct only — the `files_with_stats` *method* is Task 3)
- Modify: `src/services/query-api/src/serving_datafusion.rs` (add `IcebergMirrorTableProvider`)
- Test: `src/services/query-api/tests/iceberg_mirror_provider.rs`
- Modify: `src/services/query-api/BUCK` (new `rust_test` target — pure-logic, **not** a fixture test: it writes local Parquet, no Postgres)

**Interfaces:**
- Produces: `control_plane_postgres::iceberg_catalog::FileWithStats { path: String, record_count: i64, file_size_bytes: i64, column_stats: Vec<control_plane_core::snapshot::ColumnStat> }` (public).
- Produces: `query_api::serving_datafusion::IcebergMirrorTableProvider` implementing `datafusion::catalog::TableProvider`, with:
  - `pub async fn try_new(ctx: &SessionContext, files: Vec<FileWithStats>) -> Result<Self, ServingError>` — infers the arrow schema from the file set (same `ParquetFormat::default().with_force_view_types(false)` inference `listing_table` uses), stores `schema: SchemaRef` + `files`.
  - `fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>>` → `Inexact` for every filter.
  - `async fn scan(&self, state, projection, filters, limit) -> Result<Arc<dyn ExecutionPlan>>` — prunes files, then builds a `DataSourceExec` over survivors.
- Produces: `pub(crate) fn prune_files<'a>(schema: &SchemaRef, filters: &[Expr], files: &'a [FileWithStats]) -> Vec<&'a FileWithStats>` — the pure pruning decision, unit-testable without execution. A file is **kept** if: it has no usable stats for the predicate's columns, OR the `PruningPredicate` cannot prove it non-matching. Dropped only when proven non-matching.

**Implementation notes for the engineer (resolve the exact APIs here):**
- DataFusion 54 replaced `ParquetExec` with `DataSourceExec` wrapping a `ParquetSource` via `FileScanConfig`. Look at `datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder, ParquetSource}` and `datafusion::datasource::source::DataSourceExec`. The object store is the local filesystem store already registered in `register_iceberg_table` (`ObjectStoreUrl::local_filesystem()`); each `FileWithStats.path` is an absolute `file://`-style warehouse path — reuse `ListingTableUrl::parse` to derive the `object_store::path::Path`, or build `PartitionedFile::new(path, size)` directly.
- Build a `PruningPredicate` from the conjunction of `filters` against `self.schema`: `let phys = datafusion::physical_expr::create_physical_expr(&conjunction(filters), &df_schema, &ExecutionProps::new())?; PruningPredicate::try_new(phys, self.schema.clone())?`. Then evaluate it against a `PruningStatistics` implementation built from one file's `ColumnStat`s. The simplest robust route: implement `prune_files` by constructing, per file, a DataFusion `Statistics`/`ColumnStatistics` and feeding a small `PruningStatistics` adapter — OR use `PruningPredicate::prune` over a batched `PruningStatistics` covering all files at once and keep the rows it marks `true`/unknown. Either is acceptable; the test pins behavior, not the mechanism.
- `ColumnStat` → typed `ScalarValue` min/max: map `StatValue` to `ScalarValue` by the arrow column type (`Int32`→`ScalarValue::Int32`, `Int64`→`Int64`, `Float64`→`Float64`, `Boolean`→`Boolean`, `Utf8`→`Utf8`). A column with `min`/`max == None` → unknown bound (`ScalarValue::Null` of the column type) so the pruner cannot prune on it.
- If a `PruningPredicate` cannot be constructed for a given filter set (unsupported expr), **keep all files** (return them unpruned) — never fail the scan over a pruning limitation.

- [ ] **Step 1: Add the `FileWithStats` struct (postgres).**

In `src/control-plane/postgres/src/iceberg_catalog.rs`, add near the top (after imports):

```rust
use control_plane_core::snapshot::ColumnStat;

/// A live data file plus its per-column stats, for the pruning-aware serving
/// provider. Concrete to Iceberg — the shared `Catalog`/`FileRef` must not grow a
/// stats field (DuckLake uses them too). `column_stats` is empty for files written
/// before per-column stats landed (always kept by the pruner).
#[derive(Clone, Debug)]
pub struct FileWithStats {
    pub path: String,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub column_stats: Vec<ColumnStat>,
}
```

Confirm `control_plane_core` re-exports `snapshot::ColumnStat` (it is `control_plane_core::ColumnStat` per `core/src/lib.rs`; use whichever path the crate exposes — check with `grep -n "ColumnStat" src/control-plane/core/src/lib.rs`).

- [ ] **Step 2: Write the failing pruning unit test.**

Create `src/services/query-api/tests/iceberg_mirror_provider.rs`. It writes two real Parquet files to a temp dir with disjoint `id` ranges, builds `FileWithStats` with matching stats, and asserts the prune decision. Use arrow/parquet 58 (the query-api crate's arrow).

```rust
//! De-risk: IcebergMirrorTableProvider prunes whole files a predicate can't match,
//! always keeps no-stats files, and returns correct rows over the survivors.
//! Pure-logic test (local Parquet, no Postgres) — plain rust_test.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use control_plane_core::snapshot::{ColumnStat, StatValue};
use control_plane_postgres::iceberg_catalog::FileWithStats;
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::SessionContext;
use object_store::local::LocalFileSystem;
use datafusion::execution::object_store::ObjectStoreUrl;
use parquet::arrow::ArrowWriter;
use query_api::serving::SqlValue;
use query_api::serving_datafusion::{batches_to_rows, prune_files, IcebergMirrorTableProvider};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// Write a one-file Parquet at `path` with the given ids/names.
fn write_parquet(path: &std::path::Path, ids: Vec<i64>, names: Vec<&str>) {
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(file, schema(), None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn stats(path: &str, size: i64, min: i64, max: i64) -> FileWithStats {
    FileWithStats {
        path: path.to_string(),
        record_count: 1,
        file_size_bytes: size,
        column_stats: vec![ColumnStat {
            column_name: "id".into(),
            null_count: 0,
            column_size_bytes: 0,
            min: Some(StatValue::I64(min)),
            max: Some(StatValue::I64(max)),
        }],
    }
}

#[test]
fn prune_drops_nonmatching_keeps_nostats() {
    let s = schema();
    let file_a = stats("/w/a.parquet", 10, 1, 5); // id in [1,5]
    let file_b = stats("/w/b.parquet", 10, 100, 200); // id in [100,200]
    let file_c = FileWithStats {
        path: "/w/c.parquet".into(),
        record_count: 1,
        file_size_bytes: 10,
        column_stats: vec![], // no stats -> always kept
    };
    let files = vec![file_a, file_b, file_c];
    // WHERE id = 3 -> only file_a can match; file_c kept (no stats); file_b dropped.
    let kept = prune_files(&s, &[col("id").eq(lit(3i64))], &files);
    let kept_paths: Vec<&str> = kept.iter().map(|f| f.path.as_str()).collect();
    assert!(kept_paths.contains(&"/w/a.parquet"), "matching file kept");
    assert!(kept_paths.contains(&"/w/c.parquet"), "no-stats file always kept");
    assert!(!kept_paths.contains(&"/w/b.parquet"), "non-matching file dropped");
}
```

- [ ] **Step 3: Run it, expect a compile failure (symbols absent).**

Run: `buck2 test //src/services/query-api:iceberg-mirror-provider 2>&1 | tee /tmp/t.log; grep -E "error\[|Tests finished|FAIL" /tmp/t.log`
Expected: build error — `prune_files` / `IcebergMirrorTableProvider` undefined. (Add the BUCK target first — Step 4 — so the target exists.)

- [ ] **Step 4: Wire the test target in `src/services/query-api/BUCK`.**

Mirror the existing `datafusion-register` target (a `rust_test`, not fixture). Add:

```python
rust_test(
    name = "iceberg-mirror-provider",
    crate = "iceberg_mirror_provider",
    srcs = ["tests/iceberg_mirror_provider.rs"],
    crate_root = "tests/iceberg_mirror_provider.rs",
    deps = [
        ":query-api",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//third-party:arrow",
        "//third-party:datafusion",
        "//third-party:object_store",
        "//third-party:parquet",
        "//third-party:tokio",
    ],
)
```

Check the exact `rust_test` arg style and `parquet` target name against a neighboring query-api target (`grep -n "rust_test\|//third-party:parquet" src/services/query-api/BUCK`). If `parquet` isn't yet a query-api dep, add it (datafusion 54 already pulls parquet 58 transitively, so the alias exists).

- [ ] **Step 5: Implement `prune_files` + `IcebergMirrorTableProvider`.**

In `src/services/query-api/src/serving_datafusion.rs` add the provider and the pure prune helper (see the Implementation notes above for the DataFusion-54 specifics). Skeleton:

```rust
use control_plane_postgres::iceberg_catalog::FileWithStats;
use control_plane_core::snapshot::StatValue;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

pub struct IcebergMirrorTableProvider {
    schema: SchemaRef,
    files: Vec<FileWithStats>,
}

impl IcebergMirrorTableProvider {
    pub async fn try_new(
        ctx: &SessionContext,
        files: Vec<FileWithStats>,
    ) -> Result<Self, ServingError> {
        // infer schema from the file set exactly like `listing_table` does
        // (ParquetFormat::default().with_force_view_types(false)); store it.
        // ...
    }
}

/// Keep a file unless its stats prove it cannot match the conjunction of `filters`.
/// No usable stats, or an un-prunable predicate -> kept. Never fails.
pub(crate) fn prune_files<'a>(
    schema: &SchemaRef,
    filters: &[Expr],
    files: &'a [FileWithStats],
) -> Vec<&'a FileWithStats> {
    // build a PruningPredicate from `filters` over `schema`; on any construction
    // failure return files.iter().collect() (keep all). Evaluate per file via a
    // PruningStatistics built from its ColumnStats; keep where prune() yields
    // true/unknown. See Implementation notes.
}

#[async_trait]
impl TableProvider for IcebergMirrorTableProvider {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn schema(&self) -> SchemaRef { self.schema.clone() }
    fn table_type(&self) -> TableType { TableType::Base }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let kept = prune_files(&self.schema, filters, &self.files);
        // build FileScanConfig(ParquetSource) over `kept` paths, forwarding
        // projection + limit, against the local-filesystem object store; wrap in
        // DataSourceExec. See Implementation notes for the exact DF-54 builders.
    }
}
```

Helper `stat_to_scalar(&StatValue, &DataType) -> ScalarValue` lives here too (Task 4 reuses it).

- [ ] **Step 6: Run the prune unit test, expect PASS.**

Run: `buck2 test //src/services/query-api:iceberg-mirror-provider > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS.

- [ ] **Step 7: Add an execution test — correct rows over survivors.**

Append to `tests/iceberg_mirror_provider.rs` a `#[tokio::test]` that writes two real Parquet files (via `write_parquet` into a `tempfile::tempdir()`), builds `FileWithStats` with their true min/max, constructs the provider via `try_new`, registers it on a `SessionContext` (register the local-filesystem object store first, as `register_iceberg_table` does), and runs `SELECT id FROM t WHERE id = <value-only-in-file-A>`; assert the returned rows are exactly file A's matching rows via `batches_to_rows`. This proves the pruned plan still produces correct results. Add `tempfile` to the test target's `deps` (`//third-party:tempfile`).

- [ ] **Step 8: Run the full test file, expect PASS.**

Run: `buck2 test //src/services/query-api:iceberg-mirror-provider > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS (both tests).

- [ ] **Step 9: Clippy + commit.**

```bash
buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log
git add src/control-plane/postgres/src/iceberg_catalog.rs src/services/query-api/src/serving_datafusion.rs src/services/query-api/tests/iceberg_mirror_provider.rs src/services/query-api/BUCK
git commit -m "feat(iceberg): de-risk IcebergMirrorTableProvider file pruning"
```

---

## Task 2: Storage + write path — compute and persist per-column stats

Adds the `data_file_column_stat` table, ports the Parquet-footer stats merge against `parquet57`, and wires `added_files_of`/`project_files` so every written Iceberg data file (append **and** flush, which both funnel through these) gets stats in the same transaction as the snapshot commit.

**Files:**
- Create: `src/control-plane/postgres/migrations/0016_iceberg_mirror_file_column_stats.sql`
- Create: `src/control-plane/postgres/src/iceberg_stats.rs`
- Modify: `src/control-plane/postgres/src/lib.rs` (declare the module)
- Modify: `src/control-plane/postgres/src/iceberg_mirror.rs` (`ProjectedFile.column_stats`, `added_files_of`, `project_files`)
- Test: `src/control-plane/postgres/tests/iceberg_column_stats.rs` (`loom_fixture_test`)
- Modify: `src/control-plane/postgres/BUCK`
- Refresh: `src/control-plane/postgres/.sqlx/`

**Interfaces:**
- Consumes: `core::snapshot::{ColumnStat, StatValue}`; `parquet57` (already a postgres dep, named_dep `parquet57`).
- Produces: `iceberg_stats::column_stats_from_parquet(bytes: iceberg::Bytes, column_names: &[String]) -> Result<Vec<ColumnStat>>` — merges typed min/max across all row groups for each column index, mirroring `datafusion_io::write::file_stats_from_bytes` but against `parquet57::file::statistics::Statistics`. `column_names[i]` is the name recorded for row-group column `i` (Iceberg writes columns in schema order, so pass `columns_of(table)`-ordered names). **Takes `iceberg::Bytes` (re-exported `bytes::Bytes`) by value** so it can feed `SerializedFileReader::new` directly (`bytes::Bytes: parquet57::file::reader::ChunkReader`) — this avoids adding a `bytes` *library* dep to the postgres crate (a `Vec<u8>`/`&[u8]` is not a `ChunkReader`, and `iceberg` is already a library dep). The flush/append path passes the `Bytes` straight from `InputFile::read().await`.
- Produces: `iceberg_stats::stat_to_text(v: &StatValue) -> String` and `iceberg_stats::stat_from_text(text: &str, iceberg_type: &str) -> Option<StatValue>` — the text codec the read path (Task 3) re-types with.
- Produces: `ProjectedFile { path, file_format, record_count, file_size_bytes, column_stats: Vec<ColumnStat> }`.

- [ ] **Step 1: Write the migration.**

Create `src/control-plane/postgres/migrations/0016_iceberg_mirror_file_column_stats.sql`:

```sql
-- Per-file, per-column statistics for the Iceberg mirror, mirroring DuckLake's
-- ducklake_file_column_stats. NOT MVCC-versioned: stats are immutable for an
-- immutable data file and their lifecycle follows the data_file row. min/max are
-- stored as text and re-typed on read via the column's iceberg type (only the
-- StatValue primitives carry bounds; other types store NULL min/max).
create table iceberg_mirror.data_file_column_stat (
    data_file_id      bigint not null references iceberg_mirror.data_file(data_file_id),
    column_name       text   not null,
    null_count        bigint not null,
    column_size_bytes bigint not null,
    min_value         text,
    max_value         text,
    primary key (data_file_id, column_name)
);
```

Confirm the `data_file` PK column name (`grep -n "data_file_id\|create table iceberg_mirror.data_file" src/control-plane/postgres/migrations/0012_iceberg_mirror.sql`).

- [ ] **Step 2: Write the stats-merge module with its unit test first.**

Create `src/control-plane/postgres/tests/iceberg_column_stats_unit.rs` — a pure `rust_test` (no fixture) proving the merge + codec against a hand-written Parquet built with `parquet57`/`arrow-array`(57):

```rust
//! Unit: column_stats_from_parquet merges typed min/max across row groups, and the
//! StatValue<->text codec round-trips per iceberg type. parquet57/arrow-57.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::snapshot::StatValue;
use control_plane_postgres::iceberg_stats::{
    column_stats_from_parquet, stat_from_text, stat_to_text,
};
use parquet57::arrow::ArrowWriter;
use std::sync::Arc;

#[test]
fn merges_min_max_and_null_counts() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![3i64, 7, 1])),
            Arc::new(StringArray::from(vec![Some("b"), None, Some("a")])),
        ],
    )
    .unwrap();
    let mut buf = Vec::new();
    {
        let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    let names = vec!["id".to_string(), "name".to_string()];
    let stats = column_stats_from_parquet(iceberg::Bytes::from(buf), &names).unwrap();
    let id = stats.iter().find(|s| s.column_name == "id").unwrap();
    assert_eq!(id.min, Some(StatValue::I64(1)));
    assert_eq!(id.max, Some(StatValue::I64(7)));
    assert_eq!(id.null_count, 0);
    let name = stats.iter().find(|s| s.column_name == "name").unwrap();
    assert_eq!(name.min, Some(StatValue::Str("a".into())));
    assert_eq!(name.max, Some(StatValue::Str("b".into())));
    assert_eq!(name.null_count, 1);
}

#[test]
fn codec_round_trips_clean_types_and_drops_others() {
    assert_eq!(stat_to_text(&StatValue::I64(42)), "42");
    assert_eq!(stat_from_text("42", "long"), Some(StatValue::I64(42)));
    assert_eq!(stat_from_text("7", "int"), Some(StatValue::I32(7)));
    assert_eq!(stat_from_text("a", "string"), Some(StatValue::Str("a".into())));
    assert_eq!(stat_from_text("true", "boolean"), Some(StatValue::Bool(true)));
    // date/timestamp/other -> no bound (never pruned on)
    assert_eq!(stat_from_text("19000", "date"), None);
    assert_eq!(stat_from_text("123", "timestamp"), None);
}
```

- [ ] **Step 3: Add the unit test target + run to see it fail.**

Add to `src/control-plane/postgres/BUCK` (a plain `rust_test`, mirror the existing `iceberg-type` target which is pure-logic):

```python
rust_test(
    name = "iceberg-column-stats-unit",
    crate = "iceberg_column_stats_unit",
    srcs = ["tests/iceberg_column_stats_unit.rs"],
    crate_root = "tests/iceberg_column_stats_unit.rs",
    named_deps = {"parquet57": "//third-party:parquet57"},
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:arrow-array",
        "//third-party:arrow-schema",
        "//third-party:iceberg",
    ],
)
```

(`iceberg` is in `deps` so the test can name `iceberg::Bytes`.)

Run: `buck2 test //src/control-plane/postgres:iceberg-column-stats-unit 2>&1 | tee /tmp/t.log; grep -E "error\[|FAIL|Tests finished" /tmp/t.log`
Expected: build error (`iceberg_stats` module / symbols absent).

- [ ] **Step 4: Implement `src/control-plane/postgres/src/iceberg_stats.rs`.**

Port the merge from `datafusion_io::write::file_stats_from_bytes` against `parquet57` (use `parquet57::file::reader::SerializedFileReader`, `parquet57::file::statistics::Statistics`, `parquet57::file::reader::FileReader`). Reproduce the `min_stat`/`max_stat`/`stat_partial_cmp` helpers for `parquet57`'s `Statistics` enum, producing `core::snapshot::{ColumnStat, StatValue}`. The module doc comment must state *why* this duplicates `datafusion-io` (the parquet 57/58 version seam — see Global Constraints).

```rust
//! Per-column Parquet-footer stats for the Iceberg mirror, computed against
//! parquet57 (iceberg 0.9's parquet). This DUPLICATES the merge logic in
//! datafusion_io::write::file_stats_from_bytes by necessity: that crate is on
//! parquet 58 and the parquet `Statistics` enum is a distinct type per major, so
//! the function cannot be shared across the version boundary. Output is the
//! version-neutral core::snapshot::{ColumnStat, StatValue}.

use control_plane_core::snapshot::{ColumnStat, StatValue};
use control_plane_core::{ControlPlaneError, Result};
use iceberg::Bytes; // re-exported bytes::Bytes; impl parquet57 ChunkReader
use parquet57::file::reader::{FileReader, SerializedFileReader};
use parquet57::file::statistics::Statistics;

// min_stat / max_stat / stat_partial_cmp: same shape as datafusion-io's, over
// parquet57::file::statistics::Statistics. ByteArray -> Str via `.as_utf8()`.

pub fn column_stats_from_parquet(bytes: Bytes, column_names: &[String]) -> Result<Vec<ColumnStat>> {
    let reader = SerializedFileReader::new(bytes)
        .map_err(|e| ControlPlaneError::Backend(Box::new(e)))?;
    let meta = reader.metadata();
    let mut out = Vec::with_capacity(column_names.len());
    for (i, name) in column_names.iter().enumerate() {
        let mut null_count = 0i64;
        let mut column_size_bytes = 0i64;
        let mut min: Option<StatValue> = None;
        let mut max: Option<StatValue> = None;
        for rg in meta.row_groups() {
            let col = rg.column(i);
            column_size_bytes += col.compressed_size();
            if let Some(stats) = col.statistics() {
                null_count += stats.null_count_opt().unwrap_or(0) as i64;
                // merge min/max as in datafusion-io
            }
        }
        out.push(ColumnStat { column_name: name.clone(), null_count, column_size_bytes, min, max });
    }
    Ok(out)
}

pub fn stat_to_text(v: &StatValue) -> String {
    match v {
        StatValue::Bool(b) => b.to_string(),
        StatValue::I32(x) => x.to_string(),
        StatValue::I64(x) => x.to_string(),
        StatValue::F32(x) => x.to_string(),
        StatValue::F64(x) => x.to_string(),
        StatValue::Str(s) => s.clone(),
    }
}

/// Re-type a stored bound by the column's iceberg type. Only the clean primitive
/// types carry bounds; date/timestamp/float/other -> None (never pruned on).
pub fn stat_from_text(text: &str, iceberg_type: &str) -> Option<StatValue> {
    match iceberg_type.trim().to_ascii_lowercase().as_str() {
        "int" => text.parse().ok().map(StatValue::I32),
        "long" => text.parse().ok().map(StatValue::I64),
        "double" => text.parse().ok().map(StatValue::F64),
        "boolean" => text.parse().ok().map(StatValue::Bool),
        "string" => Some(StatValue::Str(text.to_string())),
        _ => None,
    }
}
```

No new dep: `iceberg::Bytes` is already reachable (the crate depends on `iceberg`), and `bytes::Bytes` already implements `parquet57::file::reader::ChunkReader`, so `SerializedFileReader::new(bytes)` compiles without adding `//third-party:bytes` to the library. Declare `pub mod iceberg_stats;` in `lib.rs` (match neighbor `iceberg_*` visibility). Run the unit test → expect PASS.

- [ ] **Step 5: Add `column_stats` to `ProjectedFile` and compute it in `added_files_of`.**

In `iceberg_mirror.rs`: add `pub column_stats: Vec<control_plane_core::ColumnStat>` to `ProjectedFile`. In `added_files_of`, after building each `ProjectedFile`, read the file bytes and compute stats. The column names are the table schema field names in order:

```rust
let names: Vec<String> = columns_of(table).into_iter().map(|c| c.name).collect();
// inside the per-file loop, after computing path/record_count/etc:
let bytes = table
    .file_io()
    .new_input(df.file_path())
    .map_err(iceberg_err)?
    .read()
    .await
    .map_err(iceberg_err)?; // iceberg::Bytes
let column_stats = crate::iceberg_stats::column_stats_from_parquet(bytes, &names)?;
files.push(ProjectedFile { path, file_format, record_count, file_size_bytes, column_stats });
```

(`new_input` returns `iceberg::Result<InputFile>`; `.read()` returns `iceberg::Result<Bytes>` — map both with `iceberg_err`. Compute `names` once before the manifest loop, not per file.)

- [ ] **Step 6: Make `project_files` insert stat rows in the same transaction.**

Change the `data_file` insert to `returning data_file_id`, then insert one `data_file_column_stat` row per `ColumnStat` using `stat_to_text` for the bounds:

```rust
let data_file_id = sqlx::query_scalar!(
    "insert into iceberg_mirror.data_file \
     (table_id, path, file_format, record_count, file_size_bytes, begin_snapshot) \
     values ($1, $2, $3, $4, $5, $6) returning data_file_id as \"id!\"",
    table_id, f.path, f.file_format, f.record_count, f.file_size_bytes, at.0,
)
.fetch_one(&mut *conn).await.map_err(backend)?;

for s in &f.column_stats {
    let min = s.min.as_ref().map(crate::iceberg_stats::stat_to_text);
    let max = s.max.as_ref().map(crate::iceberg_stats::stat_to_text);
    sqlx::query!(
        "insert into iceberg_mirror.data_file_column_stat \
         (data_file_id, column_name, null_count, column_size_bytes, min_value, max_value) \
         values ($1, $2, $3, $4, $5, $6)",
        data_file_id, s.column_name, s.null_count, s.column_size_bytes, min, max,
    )
    .execute(&mut *conn).await.map_err(backend)?;
}
```

Fix the only other `ProjectedFile { .. }` literal — the seeder path — if any constructs it without `column_stats` (grep `ProjectedFile {`). The single producer is `added_files_of`, so likely none.

- [ ] **Step 7: Regenerate the `.sqlx` cache.**

```bash
bash tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; tail -5 /tmp/sqlx.log
git add src/control-plane/postgres/.sqlx
```

- [ ] **Step 8: Write the fixture test asserting persisted stats.**

Create `src/control-plane/postgres/tests/iceberg_column_stats.rs` (a `loom_fixture_test`). Seed a multi-file table with **explicit** values via `IcebergWriter::seed_arrays` so min/max are known, then query `iceberg_mirror.data_file_column_stat` directly and assert the recorded bounds/null-counts. Mirror the harness in `tests/iceberg_flush.rs` (PgFixture + IcebergWriter). Example assertion core:

```rust
// seed one file: id long = [10, 4, 7], name string = ["m","a","z"]
writer.seed_arrays("sales", "orders", &cols,
    &[SeedCol::Long(vec![10, 4, 7]), SeedCol::Str(vec!["m", "a", "z"])]).await;
// then over the pool:
let rows = sqlx::query!(
    "select column_name, null_count, min_value, max_value \
     from iceberg_mirror.data_file_column_stat order by column_name")
    .fetch_all(&pool).await.unwrap();
// assert id: min '4' max '10' null 0; name: min 'a' max 'z' null 0
```

Use `AssertSqlSafe`/runtime queries in the test (tests don't go through the compile-time cache). Check how other fixture tests run ad-hoc SQL against `pool` (e.g. `grep -n "sqlx::query" src/control-plane/postgres/tests/iceberg_flush.rs`).

- [ ] **Step 9: Wire the fixture test target + run.**

Add a `loom_fixture_test` to `src/control-plane/postgres/BUCK` mirroring `iceberg-flush` (it needs `parquet57`, `arrow-array`, `arrow-schema`, `iceberg`, `core`, `tokio`, `sqlx`, `tempfile`, `uuid`, `time`).

Run: `buck2 test //src/control-plane/postgres:iceberg-column-stats //src/control-plane/postgres:iceberg-column-stats-unit > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS.

- [ ] **Step 10: Regression — existing mirror/flush tests stay green; clippy; commit.**

```bash
buck2 test //src/control-plane/postgres/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log
git add src/control-plane/postgres/migrations/0016_iceberg_mirror_file_column_stats.sql \
  src/control-plane/postgres/src/iceberg_stats.rs src/control-plane/postgres/src/lib.rs \
  src/control-plane/postgres/src/iceberg_mirror.rs src/control-plane/postgres/BUCK \
  src/control-plane/postgres/tests/iceberg_column_stats.rs \
  src/control-plane/postgres/tests/iceberg_column_stats_unit.rs src/control-plane/postgres/.sqlx
git commit -m "feat(iceberg): persist per-file column stats on the write path"
```

---

## Task 3: Read channel — `IcebergCatalog::files_with_stats`

Surfaces the persisted stats to the serving engine via one query joining `data_file ⋈ data_file_column_stat`, re-typing bounds by each column's iceberg type.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_catalog.rs` (`files_with_stats` method)
- Test: `src/control-plane/postgres/tests/iceberg_files_with_stats.rs` (`loom_fixture_test`)
- Modify: `src/control-plane/postgres/BUCK`
- Refresh: `src/control-plane/postgres/.sqlx/`

**Interfaces:**
- Consumes: `FileWithStats` (Task 1), `iceberg_stats::stat_from_text` (Task 2), `resolve_table` (existing).
- Produces: `IcebergCatalog::files_with_stats(&self, table: &TableRef, at: SnapshotId) -> Result<Vec<FileWithStats>>`.

- [ ] **Step 1: Write the failing fixture test.**

Create `src/control-plane/postgres/tests/iceberg_files_with_stats.rs` (`loom_fixture_test`). Seed an explicit two-file table, call `files_with_stats` at the current snapshot, and assert each file's `column_stats` carries the re-typed `StatValue` bounds (e.g. `id` file-A min `StatValue::I64(..)`). Mirror `datafusion_register.rs` for resolving the current snapshot:

```rust
let snap = catalog.current_snapshot(&table).await.unwrap();
let files = catalog.files_with_stats(&table, snap.id).await.unwrap();
assert_eq!(files.len(), 2);
let a = &files[0];
let id = a.column_stats.iter().find(|s| s.column_name == "id").unwrap();
assert_eq!(id.min, Some(control_plane_core::snapshot::StatValue::I64(/*known*/)));
```

- [ ] **Step 2: Add the target + run to see it fail.**

Add a `loom_fixture_test` `iceberg-files-with-stats` to `src/control-plane/postgres/BUCK` (same deps as `iceberg-flush` + `core`).
Run: `buck2 test //src/control-plane/postgres:iceberg-files-with-stats 2>&1 | tee /tmp/t.log; grep -E "error\[|FAIL|Tests finished" /tmp/t.log`
Expected: build error (`files_with_stats` absent).

- [ ] **Step 3: Implement `files_with_stats`.**

In `iceberg_catalog.rs`, add the method. One query left-joins the live data files at `at` to their stats and the column's iceberg type (from `iceberg_mirror.column`), then groups in Rust:

```rust
pub async fn files_with_stats(
    &self,
    table: &TableRef,
    at: SnapshotId,
) -> Result<Vec<FileWithStats>> {
    let tid = self.resolve_table(table, at).await?;
    let rows = sqlx::query!(
        "select f.data_file_id as \"data_file_id!\", f.path as \"path!\", \
                f.record_count as \"record_count!\", f.file_size_bytes as \"file_size_bytes!\", \
                cs.column_name as \"column_name?\", cs.null_count as \"null_count?\", \
                cs.column_size_bytes as \"column_size_bytes?\", \
                cs.min_value as \"min_value?\", cs.max_value as \"max_value?\", \
                c.column_type as \"column_type?\" \
         from iceberg_mirror.data_file f \
         left join iceberg_mirror.data_file_column_stat cs on cs.data_file_id = f.data_file_id \
         left join iceberg_mirror.column c \
           on c.table_id = f.table_id and c.column_name = cs.column_name \
              and c.begin_snapshot <= $2 and (c.end_snapshot is null or c.end_snapshot > $2) \
         where f.table_id = $1 and f.begin_snapshot <= $2 \
               and (f.end_snapshot is null or f.end_snapshot > $2) \
         order by f.data_file_id",
        tid, at.0,
    )
    .fetch_all(&self.pool)
    .await
    .map_err(backend)?;
    // group rows by data_file_id (in order); for each stat row with a column_type,
    // build a ColumnStat using stat_from_text(min_value, column_type) etc.
    // a file with no stat rows -> column_stats: vec![].
    Ok(/* grouped */)
}
```

Note: `null_count`/`column_size_bytes` come back as `Option` from the LEFT JOIN; treat a `NULL` (no stat row) as the file-has-no-stats case (skip). For a present stat row whose `column_type` doesn't yield a bound, `min`/`max` are `None` but `null_count` is still recorded.

- [ ] **Step 4: Regenerate `.sqlx`, run the test.**

```bash
bash tools/sqlx-prepare.sh > /tmp/sqlx.log 2>&1; tail -5 /tmp/sqlx.log
git add src/control-plane/postgres/.sqlx
buck2 test //src/control-plane/postgres:iceberg-files-with-stats > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log
```
Expected: PASS.

- [ ] **Step 5: Clippy + commit.**

```bash
buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log
git add src/control-plane/postgres/src/iceberg_catalog.rs src/control-plane/postgres/BUCK \
  src/control-plane/postgres/tests/iceberg_files_with_stats.rs
git commit -m "feat(iceberg): files_with_stats read channel for pruning"
```

---

## Task 4: Wire the provider into serving + end-to-end pruning

Replaces the file-backed `ListingTable` in `register_iceberg_table` with `IcebergMirrorTableProvider` fed by `files_with_stats`. The inline-rows provider is unchanged (no stats; always scanned). Proves governed reads prune files and that pruning never changes the governed result set.

**Files:**
- Modify: `src/services/query-api/src/serving_datafusion.rs` (`register_iceberg_table`)
- Test: `src/services/query-api/tests/iceberg_pruning_e2e.rs` (`loom_fixture_test`)
- Modify: `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `IcebergMirrorTableProvider::try_new` (Task 1), `IcebergCatalog::files_with_stats` (Task 3).

- [ ] **Step 1: Swap the file provider in `register_iceberg_table`.**

Replace the `let file_provider = …` block: instead of mapping `catalog.files(...)` → `ListingTableUrl` → `listing_table`, call `catalog.files_with_stats(table, snap.id)` and build `IcebergMirrorTableProvider::try_new(ctx, files_with_stats)` when non-empty. Keep the inline-rows branch (`inline_parquet`) exactly as-is — it stays a `listing_table` over the `memory://` store. The `(file_provider, inline_provider)` match arms then union a `Arc<IcebergMirrorTableProvider>` (file side) with the inline `ListingTable` as today. `IcebergMirrorTableProvider` must therefore be `Arc`-wrappable as `Arc<dyn TableProvider>` (it implements `TableProvider`). For the union path, `ctx.read_table(Arc::new(file_provider))` requires `Arc<dyn TableProvider>` — wrap accordingly.

- [ ] **Step 2: Verify existing serving e2es still pass (regression first).**

Run: `buck2 test //src/services/query-api/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all existing iceberg serving tests (`datafusion-register`, `datafusion-serving`, `datafusion-inline-union`, …) PASS — the provider is a drop-in for `ListingTable`. Fix any fallout before adding the new test.

- [ ] **Step 3: Write the end-to-end pruning test.**

Create `src/services/query-api/tests/iceberg_pruning_e2e.rs` (`loom_fixture_test`). Reuse the `register_iceberg_table` harness from `datafusion_register.rs`. Seed a table spanning **two files with disjoint `id` ranges** (via `IcebergWriter::seed_arrays`, two appends). Then:
  1. **Correctness:** `register_iceberg_table` + `SELECT id FROM "s"."t" WHERE id = <value-only-in-file-A>`; assert exactly file A's rows come back (via `batches_to_rows`).
  2. **Pruning happened:** assert file B was skipped. Deterministic route given our `scan()` does the dropping: build the provider directly over `files_with_stats`, call `prune_files(&schema, &[col("id").eq(lit(v))], &files)`, and assert it returns exactly file A. (This keeps the assertion off fragile DataFusion metric APIs while proving the skip.)
  3. **Governance invariance:** run the same query with an additional caller filter that the governed compile would AND in (e.g. `WHERE id = v AND id > 0`); assert the result set is identical to the ungoverned `WHERE id = v` over the same data — pruning never changes governed results.

- [ ] **Step 4: Wire the target + run.**

Add a `loom_fixture_test` `iceberg-pruning-e2e` to `src/services/query-api/BUCK`, mirroring the `datafusion-register` fixture target's deps (it needs `:query-api`, `core`, `postgres`, `arrow`, `datafusion`, `object_store`, `tokio`, plus `//third-party:parquet` if `prune_files` lit construction needs it).

Run: `buck2 test //src/services/query-api:iceberg-pruning-e2e > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: PASS.

- [ ] **Step 5: Full regression + clippy + commit.**

```bash
buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
buck2 build '//src/services/query-api:query-api[clippy.txt]' > /tmp/c.log 2>&1; cat /tmp/c.log
git add src/services/query-api/src/serving_datafusion.rs \
  src/services/query-api/tests/iceberg_pruning_e2e.rs src/services/query-api/BUCK
git commit -m "feat(iceberg): prune files in serving via IcebergMirrorTableProvider"
```

- [ ] **Step 6: Close the register item (loom-docs-update).**

In `docs/ROADMAP.md`, flip `road-iceberg-percolumn-stats` `- [ ]`→`- [x]`, set `status:done`, add `pr:#N` after the PR exists. Record any newly-deferred follow-ups in `docs/FUTURE.md` (backfill of stats for pre-existing files; footer-only/at-write-time stats read; Iceberg-manifest bound decoding; pruning-aware cost estimates) if not already present. Run `bash tools/docs.sh validate` and commit.

---

## Self-Review

**Spec coverage:**
- §1 Storage `data_file_column_stat` → Task 2 Step 1. ✓
- §2 Write path (`ProjectedFile.column_stats`, `added_files_of` computes via FileIO, `project_files` inserts in-tx, append+flush both funnel through) → Task 2 Steps 5–6 (both paths go through `added_files_of`/`project_files` per `project_mirror`). ✓
- §3 Read channel `files_with_stats` (concrete, not widening `Catalog`/`FileRef`) → Task 3. ✓
- §4 `IcebergMirrorTableProvider` (`Inexact` pushdown, `scan` builds `Statistics`/`PruningPredicate`, drops proven-non-matching, keeps no-stats, builds `ParquetSource`/`DataSourceExec`) → Tasks 1 & 4. ✓
- §5 Scope/backfill/missing-stats default (new writes only, Iceberg-only, clean StatValue types only, missing→kept) → Global Constraints + Task 1 Step 2 (no-stats kept) + Task 2 codec (non-clean types→no bound). ✓
- De-risk first (provider proven on two-file table before data-side) → Task 1. ✓
- Testing: stats computation (Task 2 Steps 2/8), provider pruning de-risk (Task 1), e2e serving + governance invariance (Task 4 Step 3), regression (Tasks 2/4). ✓
- Files touched list → all covered; the one addition beyond the spec is `iceberg_stats.rs` (forced by the parquet 57/58 seam) and the unit test, both justified inline.

**Placeholder scan:** No "TBD"/"handle errors"/"similar to". The two DataFusion-54 builder specifics (`scan`'s `DataSourceExec` assembly and the `PruningPredicate` construction) are explicitly framed as the de-risk unknown the engineer resolves in Task 1 — the plan gives the exact crate paths to start from and pins behavior with tests, which is the correct treatment for an acknowledged API-discovery task, not a placeholder for routine code.

**Type consistency:** `FileWithStats` fields identical across Tasks 1/3/4. `column_stats_from_parquet(bytes, column_names)`, `stat_to_text`, `stat_from_text(text, iceberg_type)` signatures match between Task 2 (producer) and Tasks 2/3 (consumers). `ProjectedFile.column_stats: Vec<ColumnStat>` consistent. `prune_files(&SchemaRef, &[Expr], &[FileWithStats]) -> Vec<&FileWithStats>` identical in Tasks 1 & 4.
