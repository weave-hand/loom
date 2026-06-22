# Inline reads via a vendored Postgres `TableProvider` — Design

> Realizes `fut-df-postgres-tableprovider` (promoted to `road-df-postgres-tableprovider`)
> and fixes `iss-iceberg-inline-reparse`. The loom-native Iceberg serving engine
> rebuilds an in-memory Parquet artifact for un-flushed inline rows on **every**
> read: `IcebergCatalog::inline_parquet` reconstructs the rows from Postgres,
> `ArrowWriter`-encodes them to bytes, drops them into a fresh `InMemory` object
> store, and a `ListingTable` re-parses the Parquet footer to infer a schema the
> reconstruction already produced exactly
> (`src/services/query-api/src/serving_datafusion.rs:116-139`). This slice serves
> inline rows **directly from Postgres** through a DataFusion `TableProvider`, so
> there is no Arrow→Parquet→Arrow round-trip and projection/filter/limit push down
> into the inline `SELECT`. The provider is reusable for any future
> "DataFusion scans Postgres" need (transforms/compaction), which is why this is a
> ROADMAP capability, not just a bugfix.

## Background — why a real provider, not MemTable

Inline rows live in real Postgres tables: `inline_append` writes typed rows into
`iceberg_mirror.inline_<table_id>` (`src/control-plane/postgres/src/iceberg_inline.rs`).
The serving engine reconstructs them per query (`inline_live_batch` → Arrow batch),
then today encodes that batch to Parquet (`inline_parquet`) only because the inline
provider was modeled as a `ListingTable` over an `InMemory` store. A pure-MemTable
fix would drop the Parquet hop but still rematerialize the whole live set per query
with no pushdown. Serving Postgres *as* a DataFusion table removes the bespoke
materializer and pushes projection/filter/limit into PG — and gives loom a general
PG `TableProvider` it has wanted since the inline-writes design (which deferred it
only because the published crate lagged loom's DataFusion version, not on merit).

## What gets vendored

From [`datafusion-contrib/datafusion-table-providers`](https://github.com/datafusion-contrib/datafusion-table-providers):

- `core/src/sql/sql_provider_datafusion/` — the generic `SqlTable<T, P>`
  `TableProvider` and its `SqlExec` plan, which generate `SELECT … FROM <rel>
  WHERE <pushed filters> LIMIT <n>` at scan time (`scan_to_sql`) with
  projection/filter/limit pushdown.
- `core/src/postgres.rs` — the Postgres binding (`PostgresTableFactory`, schema
  reflection, type mapping).

These are **copied in-tree as vendored first-party source** (not a reindeer/crates
import) because the published crate targets DataFusion 52 while loom is on
**54.0.0** — vendoring lets us adapt and edit. Target placement: a new module/crate
under `src/` owned by the query-api serving layer (it is a DataFusion-side concern);
the work plan picks the exact crate boundary. The linked `core/src/postgres/write.rs`
(`PostgresTableWriter`) may come along with the module but is **out of scope here**
(see below).

Three adaptations are required, and are the substance of the work:

### 1. DataFusion 52 → 54

Bring the vendored `SqlTable`/`SqlExec`/postgres binding to DataFusion 54 APIs
(`TableProvider`, `ExecutionPlan`, pushdown result enums — the source notes
version-specific `SortOrderPushdownResult` handling). Mechanical but must compile
and pass against loom's pinned 54.

### 2. Connection pool: adapt to sqlx (not bb8/tokio-postgres)

Upstream pools via `bb8_postgres` + `tokio_postgres`. loom is uniformly **sqlx**
(`PgPool` on `IcebergCatalog`). Rather than introduce a second Postgres driver and a
second pool against the same database, implement the provider's connection-pool
abstraction (`DbConnectionPool` / `DbConnection` and the row→Arrow path it drives)
over loom's existing `sqlx::PgPool`. loom already has the typed PG-row→Arrow
conversion to reuse for the result decoding (`column_array`/`arrow_field` in
`iceberg_inline.rs` cover the seven logical types: integer/long/double/boolean/
string/date/timestamp). No new PG driver dependency.

### 3. MVCC visibility: a base-predicate hook

Inline rows are snapshot-versioned. `inline_live_batch` filters
`begin_snapshot <= at AND (end_snapshot IS NULL OR end_snapshot > at)`. The upstream
provider scans a **bare table name** (`SqlTable::new`/`new_with_schema`) with **no
custom-query/view source**, so a raw scan of `inline_<tid>` would expose end-capped
and future-snapshot rows — a correctness bug, not a perf one.

Because we are vendoring, the fix is a small, owned edit to the copied `SqlTable`:
add a **fixed base-predicate** the provider always ANDs into `scan_to_sql` (e.g.
`SqlTable::new_with_schema(...).with_base_filter("begin_snapshot <= 42 AND
(end_snapshot IS NULL OR end_snapshot > 42)")`). `register_iceberg_table` already
runs per query with the current `snap.id` resolved, so it constructs the provider
per query with the snapshot baked into the base predicate; pushed-down query filters
compose on top of it. The base predicate is built from a trusted integer snapshot id
(not user input), consistent with the `AssertSqlSafe` dynamic-SQL precedent already
in `iceberg_inline.rs`.

(Alternatives rejected: a per-snapshot Postgres `VIEW` can't carry the per-query
`at`; a GUC/`current_setting` view needs the `SET` and `SELECT` on the same pooled
session, which the pooled provider does not guarantee. The base-predicate hook is
the least machinery and keeps the snapshot filter where it already conceptually
lives.)

## Wiring the serving engine

In `register_iceberg_table` (`serving_datafusion.rs`):

- Replace the inline branch (the `inline_parquet` → `InMemory` store →
  `listing_table` block, lines 116-139) with: construct the vendored PG provider
  over `iceberg_inline::inline_table_name(tid)` using `new_with_schema` from loom's
  `self.schema(table, at)` and the base-predicate for `snap.id`. Resolve `tid` via
  the existing `live_table_id`/catalog path. If there is no inline storage or no
  live rows, `inline_provider = None` exactly as today.
- The downstream `match (file_provider, inline_provider)` union (`:147-161`) is
  **unchanged** — the PG provider is a `TableProvider`, so the file-only /
  inline-only / `UNION` arms all work as-is.
- Schema: `new_with_schema` uses loom's canonical Arrow types (`Utf8`/`Int32`/…),
  matching the file provider's `ParquetFormat::with_force_view_types(false)` side;
  `DataFrame::union` widens any nullability difference, so the served union is
  identical to today's, row for row.

Then delete `IcebergCatalog::inline_parquet` (`iceberg_inline.rs:398-415`) and its
now-unused `parquet57::arrow::ArrowWriter` import. `inline_live_batch` stays — the
flush path (`iceberg_flush.rs:73`) still uses it, and it is the source of the
row→Arrow conversion the vendored sqlx pool path reuses.

## Scope boundary

- **Read/scan only.** The write side (`PostgresTableWriter`) is not wired:
  `inline_append` keeps its sqlx write path. Routing inline writes or governed
  actions through a DataFusion PG writer is a noted follow-up enabled by this
  vendoring, not part of fixing the reparse defect.
- **Inline side only.** The file side keeps `IcebergMirrorTableProvider` (its
  per-column-stats pruning is unrelated to inline). Only the inline union branch
  changes.

## Testing

- **Behavior preserved.** Existing inline serving tests stay green (same rows
  served). Migrate the `inline_parquet` test callers to `inline_live_batch` (which
  survives), since `inline_parquet` is deleted:
  `src/control-plane/postgres/tests/iceberg_flush.rs`,
  `src/services/worker/tests/e2e.rs`, `src/services/engine/tests/wire.rs`.
- **New regression (`loom_fixture_test`, query-api inline serving e2e):** a table
  with **both** inline rows and at least one Parquet data file serves the correct
  `UNION ALL`; and a **time-travel read at an older snapshot** returns only the rows
  live at that snapshot — exercising the base-predicate MVCC filter, the part most
  likely to regress with a table-scan provider. Add a pushdown smoke check (a
  `WHERE`/`LIMIT` over the inline side returns correct rows) so the pushed-down
  `scan_to_sql` path is covered.
- **Success criterion.** Green tests prove behavior + MVCC correctness; the perf win
  (no `ArrowWriter` encode, no Parquet footer re-inference, projection/limit pushed
  into PG) is structural — proven by the deletion of `inline_parquet` and the new
  scan path, verifiable on review. No runtime metric/observability seam is added.

## Out of scope / follow-ups

- **No snapshot-keyed read cache.** Each read still queries Postgres (correctness:
  must see freshly-committed inline rows). Memoizing by `(table_id, snapshot_id)` is
  a separable follow-up only if repeated identical-snapshot latency is shown to
  matter.
- **DataFusion PG *writes*** (`PostgresTableWriter`) — enabled by the vendoring,
  unused here.
- **Wider type coverage / federation pushdown** beyond loom's seven logical types
  and the basic filter/projection/limit pushdown — not pursued here.
- See `[[iss-iceberg-inline-visibility]]` (the related, accepted bounded-staleness
  item — external clients still see inline rows only after a flush; unaffected by
  this change) and `[[road-iceberg-inline-flush]]` (the flush vertical that bounds
  inline-row lifetime).
