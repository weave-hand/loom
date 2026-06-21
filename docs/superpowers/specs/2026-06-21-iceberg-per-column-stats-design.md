# Iceberg per-column stats + predicate pushdown — design

> Register item: `road-iceberg-percolumn-stats` (area:iceberg). Promoted from
> `fut-iceberg-percolumn-stats`. Unblocks the read-parity half of
> `fut-replace-ducklake-decision`.

## Goal

Give the Iceberg DataFusion serving engine per-file column statistics
(min / max / null-count) so it **skips whole Parquet files** that cannot satisfy
a query's predicates — eliminating the per-file footer read + scan for
non-matching files. This closes the serving-performance gap (no predicate
pushdown / file skipping) that today makes the loom-native Iceberg serving path
slower than it needs to be, and is the read-side prerequisite for evaluating
Iceberg as a DuckLake replacement.

## Safety property (governance-safe by construction)

loom's governed read compiles ACL row-filters + caller filters into the query's
`WHERE` clause. The pruner only ever **removes** files it can prove cannot match
that full `WHERE`; it never adds rows and never changes results. Predicate
pushdown is therefore a pure performance optimization that **cannot** expose
denied rows or alter correctness:

- A file with no stats → kept (scanned as today).
- A file whose stats overlap the predicate → kept.
- A file whose stats prove no row can match → dropped.

Worst case equals current behavior. Pruning respects governance automatically
because the governed row-filter is part of the predicate the pruner evaluates.

## Background (verified)

- **The stats vocabulary already exists.** `core::snapshot::ColumnStat`
  (`column_name`, `null_count`, `column_size_bytes`, `min: Option<StatValue>`,
  `max: Option<StatValue>`) and `StatValue` (bool/i32/i64/f32/f64/str) are the
  types the **DuckLake** path already uses. `core::snapshot::DataFile` carries a
  `column_stats: Vec<ColumnStat>`.
- **The computation already exists.** `datafusion_io::write::file_stats_from_bytes`
  reads a Parquet footer and produces typed `ColumnStat`s (min/max merged across
  row groups, null counts, column sizes). It is the proven path DuckLake's write
  path uses (`services/ingest/src/materialize.rs`). The Iceberg mirror simply
  never wired it.
- **The Iceberg mirror has no stats.** `iceberg_mirror.data_file`
  (`migrations/0012_iceberg_mirror.sql`) stores only
  `path / file_format / record_count / file_size_bytes / begin_snapshot /
  end_snapshot`.
- **The Iceberg write/projection path** is `iceberg_mirror.rs`:
  `ProjectedFile` → `project_files` (insert) and `added_files_of` (enumerate the
  snapshot's new data files from the manifest via the table's `FileIO`). Both the
  slice-2 append path and the flush path (`iceberg_flush::append_parquet_snapshot`)
  funnel through these.
- **The read channel** is `IcebergCatalog::files` →
  `core::catalog::FileRef { path, record_count, file_size_bytes }`, consumed by
  the serving engine.
- **The serving engine** (`query-api/src/serving_datafusion.rs`,
  `register_iceberg_table` + `listing_table`) registers each table's live files as
  a plain DataFusion `ListingTable` from file URLs **with no statistics** — so
  DataFusion opens every live Parquet file and prunes only at row-group level.

## Design

### 1. Storage — `iceberg_mirror.data_file_column_stat`

Mirrors DuckLake's `ducklake_file_column_stats`. Not MVCC-versioned: stats are
immutable for an immutable data file and their lifecycle follows the `data_file`
row.

```sql
-- migration 00NN_iceberg_mirror_file_column_stats.sql
create table iceberg_mirror.data_file_column_stat (
    data_file_id      bigint not null references iceberg_mirror.data_file(data_file_id),
    column_name       text   not null,
    null_count        bigint not null,
    column_size_bytes bigint not null,
    min_value         text,   -- NULL = unknown/absent stat for this column
    max_value         text,   -- re-typed on read via the column's iceberg type
    primary key (data_file_id, column_name)
);
```

`min_value` / `max_value` are stored as text and re-typed on read using the
column's known iceberg type (the same `StatValue` taxonomy DuckLake uses). A
column absent from this table for a file → no usable bound → that file is never
pruned on that column (safe default).

### 2. Write path (compute + project)

- `ProjectedFile` (`iceberg_mirror.rs`) gains `column_stats: Vec<ColumnStat>`
  (the existing `core::snapshot::ColumnStat`).
- `added_files_of` — for each data file it enumerates from the manifest, read the
  file's bytes via the table's `FileIO` and run
  `datafusion_io::write::file_stats_from_bytes` to produce typed `ColumnStat`s.
  This is the single point that reuses loom's proven, typed stats computation
  rather than decoding Iceberg's binary manifest bounds.
- `project_files` writes the `data_file` row as today, then inserts one
  `data_file_column_stat` row per `ColumnStat`, in the **same transaction** as the
  rest of the mirror projection (atomic with the snapshot commit).
- Both the append path and the flush path funnel through
  `added_files_of` / `project_files`, so both kinds of written file get stats with
  no per-call special-casing.

*Optimization (not this slice):* reading only the Parquet footer, or computing
stats at write time from the in-memory batches instead of re-reading the file, is
a follow-up. Footers are small and files are local today.

### 3. Read channel — `IcebergCatalog::files_with_stats`

The serving engine holds a concrete `&IcebergCatalog`, so add a concrete method
rather than widening the shared `Catalog` trait / `FileRef` (which DuckLake also
uses and must not grow):

```rust
pub struct FileWithStats {
    pub path: String,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub column_stats: Vec<ColumnStat>,   // empty when a file predates the feature
}

// IcebergCatalog — concrete, Iceberg-only
pub async fn files_with_stats(
    &self, table: &TableRef, at: SnapshotId,
) -> Result<Vec<FileWithStats>>
```

One query joins `data_file` ⋈ `data_file_column_stat` for the live files at `at`,
re-typing `min_value`/`max_value` via each column's iceberg type. The existing
`files()` (returning `FileRef`) stays untouched for non-pruning callers.

### 4. The `IcebergMirrorTableProvider` (the core)

Replaces the plain `ListingTable` in `register_iceberg_table`. A custom DataFusion
`TableProvider`:

- **Holds**: the table schema (from the mirror columns) + the
  `Vec<FileWithStats>` for the current snapshot.
- **`supports_filters_pushdown`** → `Inexact` for all filters. We prune by file,
  but DataFusion must still re-apply the predicate per row — pruning is never
  trusted to be exact.
- **`scan(filters, projection, limit)`**:
  1. Build a DataFusion `Statistics` per file from its `ColumnStat`s (min/max →
     typed `ScalarValue`, null-count) as `ColumnStatistics`, typed via the
     column's iceberg type.
  2. Construct a `PruningPredicate` from the conjunction of `filters` against the
     table schema, evaluate it against each file's `Statistics`, and drop files it
     proves cannot match. Files with empty/missing stats are **kept**.
  3. Build a `ParquetSource` / `FileScanConfig` (`DataSourceExec`) over only the
     surviving file paths, forwarding `projection` / `limit`. DataFusion still
     reads each surviving file's footer for its own row-group pruning — so this is
     whole-file skipping **on top of** existing row-group pruning.

### 5. Scope, backfill, and the missing-stats default

- **New writes only.** Files written after this lands get stats; pre-existing
  `data_file` rows have none and are **always kept** by the pruner (correct, just
  unpruned). A bulk **backfill** (re-read footers for existing live files) is
  deferred to a follow-up FUTURE item, not this slice.
- **Iceberg-only.** The DuckLake serving path and the shared `Catalog` / `FileRef`
  are not modified.
- **Stat types.** Only the `StatValue` primitives (bool/i32/i64/f32/f64/str) get
  bounds; any other column carries no min/max and is never pruned on. Truncated or
  absent footer stats → no bound → kept.
- **Out of scope:** row-group-level tuning (DataFusion already does it),
  Iceberg-manifest bound decoding, S3 / footer-only read optimization, and
  stats-driven cost estimates beyond pruning.

## De-risk (first plan task)

The exact DataFusion 54 APIs are the unknown: `PruningPredicate::try_new` +
`PruningStatistics`, building `Statistics` / `ColumnStatistics` with typed
`ScalarValue` min/max, and assembling a `ParquetSource` `DataSourceExec` over a
chosen file set. The plan's **first task** proves the provider end to end on a
trivial two-file table (a predicate skips one file, asserted via the execution
plan / scan metrics, and a no-stats file is always included) **before** the full
wiring — exactly like the engine-wire codegen de-risk. If a DataFusion API fights
the approach, the controller adjusts before the data-side work is built on top.

## Testing

- **Stats computation** (postgres crate, `loom_fixture_test`): land a multi-file
  table via the real write path; assert `data_file_column_stat` rows carry the
  expected min/max/null-count per column, cross-checked against the known input
  batches.
- **Provider pruning de-risk** (task 1): build the provider over two hand-made
  files with disjoint min/max ranges; a predicate matching only one file; assert
  the executed plan touches exactly one file (via DataFusion metrics / `EXPLAIN`),
  and that a no-stats file is always included.
- **End-to-end serving** (query-api e2e, `loom_fixture_test`): seed an
  Iceberg-backed table spanning multiple files, issue a governed read whose filter
  matches one file; assert correct rows **and** that pruning occurred (fewer files
  scanned). Include a governed row-filter case proving pruning never changes the
  governed result set.
- **Regression:** the existing Iceberg serving e2es stay green — the provider is a
  drop-in replacement for `ListingTable`.

## Files touched

- Create: `src/control-plane/postgres/migrations/00NN_iceberg_mirror_file_column_stats.sql`
- Modify: `src/control-plane/postgres/src/iceberg_mirror.rs` (`ProjectedFile`,
  `added_files_of`, `project_files`)
- Modify: `src/control-plane/postgres/src/iceberg_catalog.rs` (`FileWithStats`,
  `files_with_stats`)
- Modify: `src/services/query-api/src/serving_datafusion.rs`
  (`IcebergMirrorTableProvider`, replace `listing_table` for Iceberg)
- Depend: `datafusion_io::write::file_stats_from_bytes` (reused),
  `core::snapshot::{ColumnStat, StatValue}`
- Refresh: committed `.sqlx` cache (new queries), `third-party/BUCK` only if a new
  dep is needed (none expected — datafusion + parquet already vendored).

## Out of scope (deferred)

- Backfill of stats for files written before this slice.
- Iceberg-manifest bound decoding (we compute from the Parquet footer instead).
- Footer-only / at-write-time stats reads (S3 efficiency).
- Pruning-aware cost estimates / join ordering beyond file skipping.
