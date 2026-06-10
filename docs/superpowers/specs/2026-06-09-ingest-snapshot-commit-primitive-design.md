# Design: transactional snapshot-commit primitive (Step 3, ingest — part 1)

> **Status:** approved design (2026-06-09). First sub-project of **Step 3 (ingest)**.
> The Tx-seam decision (`2026-06-07-tx-seam-decision-design.md`) parked the catalog
> write leg here; this spec defines it. De-risked by a two-thread spike on DuckLake's
> commit model (documented + empirical against loom's pinned DuckDB 1.5.3) and an
> evaluation of `datafusion-contrib/datafusion-ducklake`.

## Goal

Give loom the **headline atomic unit**: commit a DuckLake snapshot **+** emit a lineage
event **+** enqueue downstream work in **one Postgres transaction**. loom becomes a
**native DuckLake writer** — it writes the `ducklake_*` catalog rows itself via sqlx,
never through the DuckDB engine — so the snapshot commit shares loom's transaction with
the lineage and queue writes. This closes "the gap between the pitch and the build."

## Scope: this is part 1 of the ingest effort

The ingest *service* is large; this spec is only its load-bearing primitive. Decomposition:

- **Part 1 (this spec):** the transactional snapshot-commit primitive in the
  control-plane library — `create_table` + `append_files` on the `Tx` seam, the native
  `ducklake_*` writer, the atomic unit, pessimistic serialization, and a DuckDB-engine
  fidelity guardrail.
- **Later specs:** the ingest service shell (Quack endpoint, the binary, object-store +
  DataFusion wiring that produces Parquet); the **read path** (Query/Transform/scheduled)
  via `datafusion-ducklake`; schema evolution; delete/compaction; orphaned-Parquet GC.

## Why loom-native (the decision, and the roads not taken)

The spike established the central constraint and the options:

- **Two transaction domains.** loom drives `queue`/`lineage`/`acl` through **sqlx**; a
  DuckLake snapshot is normally committed by the **DuckDB engine** on its own pooled
  Postgres connection. They are not one Postgres transaction by default, and DuckDB
  exposes **no** "use this connection/transaction" hook — so a *shared-transaction*
  approach is infeasible.
- **But the catalog is writable by anyone.** DuckLake's catalog is an **open, versioned
  (v1.0), backward-compatible spec** that explicitly invites third-party writers. The
  empirical probe against loom's pinned **DuckDB 1.5.3** confirmed the catalog is **plain
  Postgres rows with no foreign keys and no sequences** (all columns nullable except
  PKs); a hand-written `INSERT` into `ducklake_snapshot` succeeded. A DuckLake commit is
  **one Postgres transaction** (an aborted write left zero residue).
- So **loom-native** (loom writes `ducklake_*` via sqlx, in its own transaction) is the
  **only** strategy that delivers true single-transaction atomicity. Chosen.

`datafusion-contrib/datafusion-ducklake` was evaluated as a shortcut and **rejected for
the write path** for two reasons: (1) its `MetadataWriter` commits its **own** internal
transactions per op (never exposes one), so it cannot carry loom's lineage+enqueue
atomically without an invasive fork; and (2) its Postgres backend uses a **multicatalog
layout** (`ducklake_catalog`, `ducklake_catalog_snapshot_map`,
`ducklake_catalog_schema_map`, `cat_{id}`-scoped paths) that diverges from the DuckDB
single-catalog layout. It is, however, the chosen **read** engine for a later spec
(see "Read/write asymmetry").

Alternatives explicitly not chosen: *outbox/reconcile* (DuckDB or crate commits the
snapshot, loom records lineage+enqueue after, a reconciler closes the gap) — gives up the
atomic guarantee; *shared-transaction* — infeasible (no DuckDB hook); *adopt+fork the
crate* — couples loom to a v0.0.x fork and its multicatalog layout.

## Architecture & boundary

**Register-only.** The library never touches Parquet or object storage. The ingest
service (which has DataFusion + an object-store client) writes the Parquet file and hands
the primitive *metadata*; the library does the atomic catalog+lineage+enqueue transaction.
So `core`/`postgres` gain **no** DataFusion/S3 dependency.

Call site:

```rust
// --- ingest SERVICE (DataFusion + object store) ---
let written = datafusion.write_parquet(batches, &object_store).await?;
//   written -> { path, record_count, file_size_bytes, footer_size, per-column min/max/null_count }

// --- control-plane LIBRARY primitive (sqlx only) ---
let mut tx = cp.begin().await?;
tx.create_table(&table, &columns).await?;     // optional; idempotent if table exists
tx.append_files(&table, &[written]).await?;   // stage ducklake_* data rows
tx.emit(lineage_for(/* snapshot */)).await?;  // existing Tx method
tx.enqueue(downstream_job).await?;            // existing Tx method
let snapshot = tx.commit().await?;            // ONE Postgres transaction
```

## API — additions to `core`

New domain types (in a new `core/src/snapshot.rs`, re-exported from `lib.rs`):

```rust
/// A column definition for create_table. `ty` is a DuckLake type string ("int64",
/// "varchar", …) — the same dialect stored in ducklake_column.column_type.
pub struct ColumnSpec { pub name: String, pub ty: String, pub nullable: bool }

/// Per-column statistics for one data file (values serialized as strings, matching
/// ducklake_file_column_stats).
pub struct ColumnStat {
    pub column_name: String,
    pub min: Option<String>,
    pub max: Option<String>,
    pub null_count: i64,
}

/// A Parquet file the caller has already written to object storage.
pub struct DataFile {
    pub path: String,
    pub path_is_relative: bool,   // true => resolved against ducklake_metadata.data_path
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub footer_size: i64,
    pub column_stats: Vec<ColumnStat>,
}
```

New **flat methods on the `Tx` seam** (per the seam decision; staged until `commit`):

```rust
trait Tx {
    // existing: commit, rollback, enqueue, emit
    /// Create a physical DuckLake table (idempotent: no-op if it already exists live).
    /// Staged; applied as part of the snapshot at commit.
    async fn create_table(&mut self, table: &TableRef, columns: &[ColumnSpec]) -> Result<()>;
    /// Register already-written Parquet data files as part of the snapshot. Staged.
    async fn append_files(&mut self, table: &TableRef, files: &[DataFile]) -> Result<()>;
}
```

`commit(self: Box<Self>) -> Result<Option<SnapshotId>>` changes its return type from `()`
to `Option<SnapshotId>` (`SnapshotId` already exists in `catalog.rs`): `Some` when the
transaction staged a catalog op and therefore produced a new snapshot, `None` for the
existing emit/enqueue-only transactions that produce no snapshot. This is a breaking change
to `Tx::commit`'s signature; existing call sites (`worker`, `testkit`) that do
`tx.commit().await?` keep compiling (they simply ignore the returned `Option`). All adapters
return the same shape.

Autocommit (non-`Tx`) variants are **not** added — ingest always wants the atomic unit
(YAGNI). The `Catalog` trait stays read-only; writes live on `Tx`.

## Commit mechanics (postgres)

The `Tx` accumulates staged ops in memory; the `ducklake_*` writes and id allocation
happen **inside `commit()`** so ids are allocated as late as possible:

1. **Serialize:** `SELECT pg_advisory_xact_lock(<catalog key>)` — a transaction-scoped
   advisory lock, auto-released on commit/rollback. This serializes loom's own writers
   (no OCC retry/jitter needed). The key is a fixed constant for the single catalog.
2. **Read head counters:** read the latest `ducklake_snapshot` row → current
   `max(snapshot_id)`, `next_catalog_id`, `next_file_id`.
3. **Allocate + write rows** (`snapshot_id = max+1`; object/file ids from the counters):
   - **create_table** (if staged and not already live): `ducklake_schema` (if a new
     schema), `ducklake_table`, one `ducklake_column` per column, `ducklake_schema_versions`.
   - **append_files**: one `ducklake_data_file` per file, `ducklake_file_column_stats`
     (one per column per file), `ducklake_table_stats` (upsert of record_count /
     next_row_id / file_size_bytes).
   - **always**: one `ducklake_snapshot` (with advanced `next_catalog_id`/`next_file_id`)
     and one `ducklake_snapshot_changes` (the `changes_made` log, e.g.
     `created_table:"main"."t"`, `inserted_into_table:<table_id>`).
4. **Write staged lineage + queue rows** (the existing `emit`/`enqueue` logic).
5. **COMMIT** — snapshot, lineage, and enqueue become visible together or not at all.

The exact per-operation write sequence — target columns, positional value tuples, counter
rules, `changes_made` strings, and stats encoding — is documented in
`2026-06-09-ducklake-single-catalog-write-recipe.md`, **grounded in the pinned extension
source** (`duckdb/ducklake@e6a3bd0a` = DuckDB 1.5.3, spec v1.0) with `file:line` citations
and cross-checked against an empirical transcript. The implementer works from that recipe.
All SQL uses
compile-time `query!` against the committed `.sqlx` cache (the `sqlx-prepare.sh` harness
already attaches a real DuckLake catalog, so the new write queries validate; the cache and
the `sqlx-cache-check` test extend naturally).

## Catalog layout: DuckDB-compatible single-catalog

loom writes the **single-catalog** layout the empirical probe observed from DuckDB — the
base `ducklake_*` tables only, **no** `ducklake_catalog*` multicatalog tables, **no**
`cat_{id}` path scoping. This preserves **DuckDB-engine interop** and keeps loom's existing
read-side `Catalog` concern (which already queries this layout) unchanged. The catalog spec
version loom targets (**v1.0**, paired with DuckDB 1.5.3) is pinned in a constant.

Data inlining stays **disabled** (`DATA_INLINING_ROW_LIMIT 0`, already loom's setting): all
data lands as Parquet referenced by `ducklake_data_file`, never inlined in the catalog. This
also keeps loom's output readable by the future read engine without inline support.

## Concurrency & multi-writer

Pessimistic serialization via the advisory lock means concurrent loom commits briefly wait
rather than race+retry — and **multi-writer is safe by construction**: any number of ingest
processes serialize on the same lock in the same Postgres. This resolves the roadmap's open
"multi-writer ingest" question at the primitive level (topology becomes a deployment
choice, not a correctness one). If a DuckDB-engine writer ever raced loom, loom (holding the
lock and committing) wins and DuckDB hits its own PK-collision retry; in loom's world the
native writer is *the* writer, so this is an edge case.

## Fidelity guardrail (the make-or-break test)

The central risk of a native writer is that loom's rows diverge from what the DuckLake
engine expects. The gate: a postgres-only test where loom's primitive does
`create_table` + `append_files`, then the **pinned DuckDB CLI** (`:duckdb-cli` +
`:duckdb-extensions`, via the existing `DuckLakeWriter`/`ducklake-smoke` fixture pattern)
`ATTACH`es the catalog and **`SELECT`s** — asserting it reads back exactly what loom wrote
**and can append its own snapshot on top** (proving counters/PK/versioning are correct).
This interop test is what catches catalog drift on a DuckDB version bump.

## Read/write asymmetry (recorded; resolved in a later spec)

- **Write (this spec):** loom-native sqlx writer, single-catalog DuckDB-compatible layout.
- **Read (later — Query/Transform/scheduled):** `datafusion-contrib/datafusion-ducklake`,
  a DataFusion-native DuckLake engine (read+write, Postgres+S3, filter pushdown). loom
  uses only its **read** side. Known reconciliation for that spec: the crate's Postgres
  backend is **multicatalog** while loom writes **single-catalog**, so the read path needs
  the crate's single-catalog/DuckDB-compatible mode or a fork (the crate's source shows
  awareness of an "existing single-catalog Postgres catalog populated by DuckDB" — confirm
  then). loom keeping inlining disabled sidesteps the crate's documented "data inlining is
  not read" limitation.

## Adapters & testing

- **postgres:** the native writer. New `postgres/src/snapshot.rs` holds the `ducklake_*`
  row-builders; the staging + advisory-lock + commit logic lives in `PgTx`
  (`transaction.rs`). New write SQL added to the `.sqlx` cache via `sqlx-prepare.sh`.
- **memory:** mirrors the semantics against its in-memory catalog model (the one
  `Catalog::snapshots`/`files` already reads), so the shared conformance suite runs against
  both adapters.
- **Conformance (`testkit`):** the atomic unit — create+append+emit+enqueue commit together;
  rollback leaves nothing (no snapshot, no lineage, no job); the snapshot is then visible
  via the existing `Catalog` reads. Plus the postgres-only DuckDB-interop guardrail above.

## Verification

- `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...` green; conformance covers
  both adapters; the DuckDB-interop test passes (loom-written snapshot read by the pinned
  DuckDB, and appended onto).
- `tools/clippy-all.sh` clean; `prek run --all-files` green (incl. `reindeer-check` and
  `sqlx-cache-check` after the cache regen).
- A rollback test proves atomicity: an error after staging leaves zero `ducklake_*`,
  lineage, or queue rows.

## Scope / non-goals

- **In:** `create_table` + `append_files` on `Tx`; the native single-catalog `ducklake_*`
  writer; the atomic unit; pessimistic serialization; the DuckDB-interop guardrail; both
  adapters + conformance.
- **Out (later specs):** the ingest service shell (Quack endpoint, binary, object-store +
  DataFusion Parquet writing); the `datafusion-ducklake` read path; schema evolution;
  delete/compaction; orphaned-Parquet GC; autocommit (non-`Tx`) catalog writes; multi-
  catalog support.

## Open risks

- **Commit-fidelity drift** on a DuckDB bump — mitigated by the interop guardrail + the
  pinned spec-version constant.
- **Reproducing DuckLake semantics** (counter advances, `begin/end_snapshot` row
  versioning, `changes_made` strings, stats rows) is exacting — **mitigated** by the
  source-grounded recipe (`2026-06-09-ducklake-single-catalog-write-recipe.md`, citing
  `ducklake@e6a3bd0a`) plus the DuckDB-engine interop test as the executable oracle. The
  source read already corrected several details a black-box capture got wrong (3-column
  `ducklake_schema_versions`, per-table `column_id`, `value_count` = non-null count), so the
  plan follows the recipe rather than re-deriving.
- **Read-path layout reconciliation** (single- vs multi-catalog) is deferred but real — it
  shapes the later read-path spec; flagged so it is not a surprise.
