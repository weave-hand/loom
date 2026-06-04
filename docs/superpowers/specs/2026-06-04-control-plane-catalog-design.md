# Design: Control-Plane Catalog (Phase 2)

> **Status:** approved design for the control plane's second concern — the DuckLake
> **read** surface. Sits under the umbrella roadmap
> (`2026-06-03-control-plane-roadmap-design.md`). Built in two cycles (2a, 2b);
> each gets its own implementation plan + PR.

## Goal

A read-only view over DuckLake's catalog (`ducklake.*` in Postgres), exposed
through the control-plane traits and satisfied by both the in-memory fake and the
Postgres adapter via one contract suite. loom **reads** the catalog and never
writes it — DuckLake (the DuckDB client) owns those tables. The `Catalog` trait
answers: what is the current snapshot of a table, what snapshots exist, which
Parquet files belong to a snapshot, and what is a table's column schema at a
snapshot.

This is the first **read-only** concern, which forces a structural difference
from the queue: there is no trait method to create catalog state, so the contract
suite arranges state through a separate, test-only **seeding seam** rather than
through the trait under test.

## Scope & staging

Built in two cycles, designed together here:

- **Phase 2a — trait + fake + contract (no DuckDB):** the `Catalog` trait and
  domain types in `core`; the `catalog_contract` suite in `testkit`, written
  against a `CatalogSeed` seeding seam; the in-memory fake adapter and its seeder
  pass it. Zero DuckDB, fully hermetic, establishes the read surface and the seam.
- **Phase 2b — real-DuckLake Postgres substrate:** the Postgres `Catalog` adapter
  (pure sqlx reads of `ducklake.*`) plus a pg `CatalogSeed` that drives **real
  DuckLake** — a pinned DuckDB CLI binary (`http_archive`) with the `ducklake` and
  `postgres` extensions provided locally for hermeticity — to produce `ducklake.*`
  rows in the hermetic Postgres and Parquet in a tempdir. The pg adapter passes the
  *same* contract suite.

## Why a seeding seam (the read-only wrinkle)

The queue's contract seeded state through the trait's own `enqueue`, so one
self-contained suite ran on both adapters. `Catalog` has no write op — loom does
not write `ducklake.*`. State must therefore be arranged out-of-band, and *how* it
is arranged differs per backend (build the fake's maps directly vs. run real
DuckLake). The contract suite stays identical by depending on a small test-only
trait that each adapter's test implements:

```rust
// test-support (in testkit). Implemented per adapter in its test crate.
pub struct SeedColumn { pub name: String, pub ty: String, pub nullable: bool }
pub struct SeedSpec {
    pub table: TableRef,            // schema + name
    pub columns: Vec<SeedColumn>,
    pub row_batches: Vec<usize>,    // each batch => one snapshot adding one data file of N rows
}
pub struct SeededSnapshot { pub snapshot: SnapshotId, pub files_added: usize }

#[async_trait]
pub trait CatalogSeed {
    /// Create the table if absent and apply each row-batch as its own snapshot,
    /// producing catalog state to read back. Returns the snapshots created, in order.
    async fn seed(&self, spec: SeedSpec) -> Vec<SeededSnapshot>;
}
```

- **Memory seeder:** constructs `Snapshot`/`FileRef`/`TableSchema` entries directly
  in the fake's state.
- **Postgres seeder (2b):** runs DuckLake (DuckDB CLI) to `CREATE TABLE` + `INSERT`
  per batch, then reads the produced `snapshot_id`s back from `ducklake_snapshot`
  via sqlx.

`CatalogSeed` is strictly test-support — production code never references it. This
keeps the *fidelity* property: in 2b the catalog rows under test are exactly what
DuckLake emitted, not a fiction loom authored.

> **YAGNI note:** the seam is intentionally minimal — append-only batches, no
> deletes/updates/schema-evolution in the seeder. That is enough to exercise every
> read op. Richer seeding (column add/drop across snapshots) is added only if a
> later concern needs to read it.

## The DuckLake catalog schema we read (grounding)

DuckLake keeps its catalog in Postgres as `ducklake_*` tables using an **MVCC
range** model: rows carry `begin_snapshot` and `end_snapshot`, and a row is live
"at" snapshot `s` when `begin_snapshot <= s AND (end_snapshot IS NULL OR
end_snapshot > s)`. Snapshots are **catalog-global** (a monotonic `BIGINT`
`snapshot_id`), not per-table. The subset loom reads (pinned to the DuckLake
catalog version we target):

- **`ducklake_snapshot`** — `snapshot_id BIGINT PK`, `snapshot_time TIMESTAMPTZ`,
  `schema_version BIGINT`, `next_catalog_id`, `next_file_id`. → snapshot identity,
  ordering, history.
- **`ducklake_schema`** — `schema_id`, `schema_name`, `begin/end_snapshot`. →
  resolve a schema name.
- **`ducklake_table`** — `table_id`, `table_name`, `schema_id`,
  `begin/end_snapshot`, `path`. → resolve `TableRef{schema,name}` → `table_id`
  live at a snapshot.
- **`ducklake_data_file`** — `data_file_id`, `table_id`, `begin/end_snapshot`,
  `path`, `record_count BIGINT`, `file_size_bytes BIGINT`. → the Parquet files of a
  table live at a snapshot.
- **`ducklake_column`** — `table_id`, `column_order`, `column_name`,
  `column_type VARCHAR`, `nulls_allowed BOOLEAN`, `begin/end_snapshot`. → the
  table's columns at a snapshot.

The exact `ducklake.*` schema/table-name prefix and version are pinned in 2b
against the DuckLake version the CLI seeder produces; 2a's fake mimics the same
read semantics (snapshot ranges) so both adapters answer identically.

## Trait surface (`core`)

Domain types live in `core`; all ops `async`, returning `Result<_, ControlPlaneError>`.

```rust
/// A DuckLake catalog-global snapshot id (monotonic).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SnapshotId(pub i64);

/// schema.table within the DuckLake catalog.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TableRef { pub schema: String, pub name: String }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub id: SnapshotId,
    pub time: OffsetDateTime,
    pub schema_version: i64,
}

/// A Parquet file backing a table at a snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRef {
    pub path: String,
    pub record_count: i64,
    pub file_size_bytes: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    pub order: i64,
    pub name: String,
    pub ty: String,        // DuckLake column_type text, opaque to loom for now
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchema { pub columns: Vec<ColumnDef> }

#[async_trait]
pub trait Catalog {
    /// The latest snapshot at which `table` is live. `NotFound` if the table does
    /// not exist at the catalog's current snapshot.
    async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot>;
    /// All snapshots at which `table` is live, oldest first.
    async fn snapshots(&self, table: &TableRef) -> Result<Vec<Snapshot>>;
    /// The Parquet files live for `table` at snapshot `at`.
    async fn files(&self, table: &TableRef, at: SnapshotId) -> Result<Vec<FileRef>>;
    /// `table`'s column schema at snapshot `at`, in column order.
    async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema>;
}
```

Notes / decisions:
- `files`/`schema` take an explicit `TableRef` + `SnapshotId` (rather than the
  roadmap sketch's bare `SnapshotId`) because DuckLake snapshots are catalog-global
  — a snapshot alone doesn't name a table, and reads are "this table *at* that
  snapshot." This pins the provisional roadmap signature.
- `ty` stays an opaque DuckLake type string for now (a typed loom type system is an
  ontology/P3 concern, not catalog). Non-goal to parse it here.
- Catalog is **not** part of `Tx`: it is a read surface. (The transactional seam
  stays as-is from P0/P1; catalog reads don't participate.)

## The Postgres adapter (2b) is pure sqlx

The shipped `Catalog` adapter has **zero DuckDB dependency** — `ducklake.*` are
Postgres tables, read with sqlx using the snapshot-range filter above (e.g.
`current_snapshot` = resolve `table_id` live at `max(snapshot_id)`, else
`NotFound`). DuckDB appears **only** in the 2b test fixture (the writer), so "loom
reads, DuckLake writes" holds even in the dependency graph.

### Hermetic DuckLake fixture (2b)

Mirrors the existing `postgres-bin` pattern (see
`prefers-hermetic-buck2-test-deps`):

- A pinned **DuckDB CLI** binary via `http_archive` per platform (buck2 build
  input, not ambient).
- The **`ducklake` and `postgres` DuckDB extensions** are normally auto-downloaded
  from `extensions.duckdb.org` at runtime — not hermetic. We `http_file` the pinned
  `.duckdb_extension(.gz)` files per (DuckDB-version, platform) and load them from a
  local `extension_directory` / by path (with unsigned/local loading enabled),
  the same shape as the libxml2 workaround. **De-risk first in 2b** — extension
  signature/version-matching is the one fragile spot; spike it before building the
  adapter.
- The seeder boots DuckDB pointed at the hermetic Postgres as the DuckLake catalog
  and a tempdir as the data path: `ATTACH 'ducklake:postgres:host=<socket>
  dbname=<db>' AS lake (DATA_PATH '<tmp>')`, then `CREATE TABLE`/`INSERT` per batch.
  Each statement set produces real `ducklake.*` rows + Parquet. The seeder then
  reads back snapshot ids via sqlx.
- Spawned only in the fixture, as an ephemeral process; killed on `Drop`. As with
  the pg tests, runs locally (`--local-only`) if the RE sandbox blocks the spawn.

## Testing

Contract behaviors (`testkit`, run against both adapters via `CatalogSeed`):

- **current_snapshot** returns the latest snapshot at which the table is live;
  after seeding N batches, it equals the last produced snapshot.
- **snapshots** returns the table's snapshot history oldest-first; length matches
  the batches that touched the table.
- **files** at the latest snapshot lists all live Parquet files (one per batch in
  the minimal seeder); at an *earlier* snapshot it lists only the files live then
  (proves the `begin/end_snapshot` range filter — a later batch's file is absent).
- **schema** at a snapshot returns the columns in `column_order` with names, type
  strings, and nullability matching the seed spec.
- **not found**: `current_snapshot`/`snapshots`/`files`/`schema` for a table that
  does not exist (or a snapshot before the table existed) → `ControlPlaneError::NotFound`.
- (2b only, fidelity) the fields read back equal what DuckLake actually wrote
  (record counts, file paths under the data dir, column types as DuckLake spells
  them).

The fake suite is fast and hermetic (2a); the pg suite uses the DuckLake fixture
(2b). Contract assertions are on values/variants, never on backend-specific
messages.

## Non-goals (this phase)

- **Writing `ducklake.*`** — owned by the DuckLake client; loom reads only.
- **Querying table *data*** (rows/Parquet contents) — catalog is *metadata* only.
  No DuckDB in production, no data scans here; that is the services' DataFusion
  path, not the control plane.
- **`rsql`/`rsql_drivers` and the `duckdb` FFI crate** — evaluated and rejected for
  this opinionated, test-only seeding need (see `opinionated-not-pluggable`); the
  CLI binary is the writer, sqlx does all reads.
- **Parsing DuckLake column types into a loom type system** — `ty` stays an opaque
  string; typing is an ontology (P3) concern.
- **GC of orphaned Parquet / compaction** — catalog-adjacent, deferred (roadmap
  open question), its own later cycle.
- **Multi-writer ingest / DuckLake concurrency** — a *write* concern; reads see a
  consistent snapshot, so it is deferred to when ingest lands. Not resolved here.
- **Schema evolution in the seeder** (column add/drop across snapshots) — minimal
  append-only seeding only, until a concern needs to read evolution.
```
