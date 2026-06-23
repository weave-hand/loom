# Design: Iceberg additive schema evolution (add-column + detect/reject)

> **Status:** approved design (2026-06-23). Turns today's **silent** Iceberg-mirror
> schema divergence into correct **additive** schema evolution (appended nullable
> columns), rejecting every non-additive change with a clear error. Iceberg backend
> only. Promotes `fut-iceberg-schema-evolution` → `road-iceberg-schema-evolution`.

## Problem

loom mirrors Iceberg table metadata in Postgres under `iceberg_mirror`. A table's
columns are projected **once**, at table creation: `write_mirror`
(`iceberg_sql_catalog/catalog.rs`) calls `columns_exist(table_id)` and skips
`project_columns` if any column row already exists
(`iceberg_mirror.rs`). The reserved `iceberg_mirror.snapshot.schema_version` column
is hardcoded to `0` and read by nobody.

Consequence: a second land whose schema differs from the first (e.g. an extra
column) updates the **Iceberg** metadata (`ice_schema(columns)` rebuilds the schema
and the catalog accepts it) but the **mirror** silently keeps the original columns —
`columns_exist` returns `true`, so the new column is never projected. Reads go
through `IcebergCatalog::schema`, which queries the mirror, and therefore return a
**stale schema that omits the new column**, with no error anywhere. The mirror and
the data files diverge silently.

## Goal

Make the common evolution — **appending one or more nullable columns** — a
first-class, MVCC-correct operation, and make every other schema change a hard,
typed error instead of silent corruption. After this slice:

- A land that appends nullable columns evolves the table; reads at the current
  snapshot return the unified (superset) schema, null-filling the new column for
  rows from pre-evolution files.
- A land that drops / renames / retypes / reorders a column, or adds a
  non-nullable or non-appended column, is **rejected** and the whole commit
  (snapshot + lineage + files) aborts. The mirror never diverges from the data.
- `schema_version` stops being dead — it records which schema generation each
  snapshot was written at.

Iceberg backend only. DuckLake schema evolution is the separate
`fut-schema-evolution-coverage`.

## Design

### 1. Write path — schema reconciliation (replaces the "project once" guard)

In the append/land path (`do_update_table` → `write_mirror`, today guarded by
`columns_exist`), replace the once-guard with a **reconciliation** step. Load the
live mirror columns at the parent snapshot (the same MVCC query
`IcebergCatalog::schema` uses) and compare them, in order, to the incoming write's
columns by `(name, type, nullability)`:

- **identical** — no column rows written (today's common case, unchanged).
- **purely additive** — the incoming columns equal the live columns (same order,
  type, nullability) **plus** one or more **new nullable columns appended at the
  end** → project **only** the new columns via `project_columns` with
  `begin_snapshot = <new snapshot>`. Existing column rows are untouched (they stay
  live, `end_snapshot IS NULL`), so the mirror now holds the union, MVCC-stamped at
  the snapshot where each column appeared.
- **anything else** — a live column dropped, renamed, type-changed, reordered, or a
  new column that is **not nullable** or **not appended at the end** → return a
  typed `SchemaEvolutionUnsupported`-class error. Because reconciliation runs inside
  the same Postgres transaction as the snapshot/lineage/file commit, the error
  aborts the entire commit atomically.

The classification is a pure function over `(live_columns, incoming_columns)` —
unit-testable in isolation, and the single place the additive-vs-unsupported policy
lives.

### 2. `schema_version` population

Maintain a per-table **schema generation** and stamp it on each new
`iceberg_mirror.snapshot` row, replacing the hardcoded `0`:

- On table creation the generation is the initial value (e.g. `1`).
- A purely-additive land **bumps** the generation; the new snapshot carries the
  bumped value.
- A land with no schema change carries the table's current generation.

The generation is derived from the mirror (e.g. count of distinct
`begin_snapshot` values in `iceberg_mirror.column` for the table, or a max+1 over
prior snapshots' `schema_version`) — no new table needed. This gives the
snapshot → schema-generation binding the reserved column was always for, and is the
hook a later time-travel-correct read slice will resolve against.

**No migration is required:** `schema_version` already exists (`bigint NOT NULL
DEFAULT 0`); this slice simply starts writing a meaningful value. No `field_id`
column is added (see Non-goals).

### 3. Read path — mirror-authoritative superset + null-fill

After an additive evolution the table's Parquet files are **mixed**: pre-evolution
files lack the new column, post-evolution files have it. The read must present the
**superset** schema and null-fill the new column for the older files.

`IcebergCatalog::schema(table, at)` already returns the MVCC columns as-of `at`, so
at the current snapshot it now includes the added column. The change is in the
DataFusion serving path (`IcebergMirrorTableProvider` /
`DataFusionServingEngine`, `serving_datafusion.rs`):

- Treat the **mirror schema as the authoritative table schema** instead of inferring
  it from Parquet footers (`ListingTableConfig::infer_schema()`). The mirror is the
  source of truth; footer inference is what makes a mixed file set ambiguous.
- Configure the Parquet scan so columns absent from a given file are **null-filled**
  to match the unified schema (DataFusion supports projecting a table schema over
  files that are missing columns; the provider supplies the mirror-derived
  `SchemaRef` and relies on this null-fill).

Result: a current-snapshot read of an evolved table returns the superset schema,
with the value for rows from post-evolution files and `NULL` for rows from
pre-evolution files. File-skipping via per-column stats
(`road-iceberg-percolumn-stats`) is unaffected — a file with no stats for the new
column is simply never pruned on it.

### 4. Inline-accumulation path — detect + reject parity

The inline write path (`iceberg_inline.rs`) also projects columns once. To prevent
silent divergence there too, it gains the **same reconciliation check**, but only
the **detect+reject** half: identical → no-op, anything else (including additive)
→ the typed error. Additive evolution on the inline path is deferred (the
common-case land path is the Parquet path). This keeps both write paths honest
about schema without doubling the additive implementation.

## Non-goals (residual deferrals)

- **`field_id` persistence and rename / drop / type-promote / reorder.** These need
  Iceberg field-id identity in the mirror (a new `field_id` column + reading
  Iceberg's schema list). Deferred to a **full-evolution** slice; this slice's
  name/order/type model is sufficient for append-only additive changes and for
  rejecting the rest.
- **Time-travel as-of-schema reconstruction.** `iceberg_read.rs` builds the Arrow
  schema from `tbl.metadata().current_schema()`, so a time-travel read at a snapshot
  *before* a column was added still uses the current (wider) schema. Left as a
  documented follow-up; `schema_version` is the binding it will resolve against.
- **Additive evolution on the inline path** (detect+reject only there, per §4).
- **DuckLake schema evolution** — separate `fut-schema-evolution-coverage`.
- **Type widening / promotion semantics** (e.g. int→long) — a non-additive change,
  rejected here.

## Testing

A `loom_fixture_test` on the existing Iceberg fixture harness (real Postgres +
object store):

1. **Additive happy path** — land a base table `(a long, b string)`; land a second
   batch `(a long, b string, c long-nullable)`. Assert: the current-snapshot read
   returns the superset schema; `c` is the landed value for second-batch rows and
   `NULL` for first-batch rows; `iceberg_mirror.column` holds three live rows with
   `c.begin_snapshot` = the second snapshot; `schema_version` on the second snapshot
   is bumped relative to the first.
2. **Non-additive rejection** — from the base table, attempt a second land that
   drops `b` (and separately: changes `a`'s type; adds a non-nullable column).
   Assert each is rejected with the typed `SchemaEvolutionUnsupported` error, the
   commit aborts, and the mirror columns + latest snapshot are unchanged.
3. **Classifier unit test** — the pure `(live, incoming) → {identical, additive,
   unsupported}` function over a table of cases (append-nullable ✓, append-required
   ✗, drop ✗, rename ✗, retype ✗, reorder ✗, middle-insert ✗).
4. **Inline parity** — a divergent inline write is rejected (detect+reject).

No new third-party dependency; no migration. Green via `buck2 test //src/...`.

## Risks

- **DataFusion null-fill behavior for missing Parquet columns** must be confirmed
  for the provider's scan configuration; if a given DataFusion version does not
  null-fill a table-schema column absent from a file, the provider must adapt the
  per-file projection explicitly. The additive happy-path test is the guard.
- **Classifier strictness** — being conservative (reject anything not provably a
  pure nullable append) is the safe default; loosening it later is additive, while
  silently mis-accepting a change reintroduces divergence.
- **Two write paths** (Parquet land vs inline) must share one classifier so their
  notion of "schema change" can't drift.
