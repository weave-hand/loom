# Iceberg Inline Writes + Read Union — Design

> Slice A of the Iceberg write story. Prerequisite for wiring ingest into Iceberg
> (Slice B), which needs a way to land small writes without producing a tiny
> Parquet object. See `docs/spike/ICEBERG_ROADMAP.md`.

## Goal

Give loom-native, **inline** small writes for Iceberg tables: rows land in the
mirror (Postgres) as typed rows — **no object-storage Parquet file, no Iceberg
metadata change** — and loom's DataFusion serving engine **unions** them with the
table's Parquet data files at read time. This is the DuckLake `DATA_INLINING`
capability rebuilt loom-natively, which Iceberg has no native equivalent for.

## Why

A naive Iceberg landing writes one Parquet file per request, so a small write
produces a tiny Parquet object — the small-file problem, and worse than the
DuckLake ingest path. DuckLake gets inlining for free (it stores small inserts as
typed rows in the catalog DB and unions them on read, flushing to Parquet later).
loom must rebuild that to make the Iceberg ingest path (Slice B) good.

## Decisions (settled in brainstorming)

- **Inline visibility: loom's read engine only.** Inline rows live in loom's
  mirror; external Iceberg clients don't see them until a future flush writes a
  real Parquet data file. Bounded staleness for third-party readers is acceptable
  for now. The mirror is the source of truth, beyond pure Iceberg.
- **Storage: per-table typed inline tables** (the DuckLake model) —
  `iceberg_mirror.inline_<table_id>` with typed columns created on the fly from the
  table's schema. Faithful and flush-friendly. Costs dynamic DDL + a logical→PG
  type map + runtime (non-`query!`) SQL, following the `fixture.rs` `AssertSqlSafe`
  precedent.
- **Read union: dep-free, reconstruct → ephemeral in-memory Parquet → file-level
  union.** The serving engine reads inline rows via its existing sqlx pool, builds
  an Arrow batch, encodes it to Parquet bytes in an `InMemory` object store, and
  adds those `memory://` URLs to the same `ListingTable` it already builds. No new
  dependency. (A vendored DataFusion Postgres `TableProvider` was considered and
  deferred — the published crate targets DataFusion 53 while loom is on 54, and a
  native PG provider buys nothing the read path needs yet. Revisit for
  transforms/compaction.)
- **Lineage: atomic, trivially.** The inline write is mirror-only (one Postgres
  transaction, never through `update_table`), so it emits its `LineageEvent` on the
  same transaction via the existing `pg_emit`.

## Architecture

### Inline storage — `iceberg_mirror.inline_<table_id>`

Created on the first inline write to a table:

```sql
CREATE TABLE IF NOT EXISTS iceberg_mirror.inline_<table_id> (
  loom_row_id    bigserial PRIMARY KEY,
  begin_snapshot bigint NOT NULL,
  end_snapshot   bigint,            -- reserved for a future flush; null = live
  <col_1> <pg_type_1>,
  <col_2> <pg_type_2>,
  ...
);
```

- Columns and their Postgres types come from the table's mirror schema via a new
  `pg_type_for(logical) -> &str` map (logical → `bigint`/`integer`/`double precision`/
  `boolean`/`text`/`date`/`timestamp`).
- MVCC by `begin_snapshot`/`end_snapshot`, identical to the existing mirror tables.
  Append-only for now, so `end_snapshot` stays null.
- `<table_id>` is the mirror's internal `iceberg_mirror.table.table_id`.
- No migration: these tables are per-loom-table and created at runtime under the
  existing `iceberg_mirror` schema.

### Inline write primitive — `inline_append`

`inline_append(pool, table: &TableRef, columns: &[ColumnSpec], batch, lineage) ->
Result<SnapshotId>`, in **one Postgres transaction**:

1. Ensure the mirror `iceberg_mirror.table` + `column` rows exist for `table`;
   project them from `columns` if this is the first write (so an inline-only table
   is a live, schema-bearing mirror table the read engine can resolve).
2. Resolve `table_id`; ensure `iceberg_mirror.inline_<table_id>` exists (transactional
   DDL from `columns` via `pg_type_for`).
3. Allocate a snapshot: `nextval('iceberg_mirror.snapshot_seq')` and insert a
   synthetic `iceberg_mirror.snapshot` row (no Iceberg backing) so `current_snapshot`
   advances. `snapshot_time = now()`, `schema_version` = the table's current.
4. INSERT the batch's rows into `inline_<table_id>` with `begin_snapshot` = the new id.
5. `pg_emit(&mut *tx, &lineage)`.

No object storage, no Iceberg metadata, no `update_table`. Returns the new snapshot id.

### Read union — extend `register_iceberg_table` (query-api)

When registering a live table for a query, in addition to the `file://` Parquet
URLs the slice-3 engine already collects:

1. If `iceberg_mirror.inline_<table_id>` exists (checked via `to_regclass`), `SELECT`
   the columns of its live rows at the snapshot (`begin_snapshot <= at AND
   (end_snapshot IS NULL OR end_snapshot > at)`), ordered by `loom_row_id`.
2. Reconstruct an Arrow `RecordBatch` from the typed rows, using the table's mirror
   column schema (PG column → Arrow array, the inverse of `pg_type_for`).
3. Encode the batch to Parquet bytes, store under a synthetic key in an `InMemory`
   object store registered on the `SessionContext` under `memory://`.
4. Add the `memory://` URL(s) to the `ListingTable`'s path list alongside the
   `file://` ones.

The union is "more URLs"; the rest of registration/execution is unchanged. The
schema is consistent because both sides derive from the same table schema. Handles
file-only, inline-only, and mixed tables.

## Data flow

```
inline write:  batch + schema + lineage
   └─(one pg tx)→ ensure mirror table/columns
                → ensure inline_<id> (DDL)
                → alloc snapshot (+ synthetic snapshot row)
                → INSERT typed rows (begin_snapshot)
                → pg_emit(lineage)
                → SnapshotId

read:  fetch_rows(sql) → for each live table:
          file:// Parquet URLs  ─┐
          inline rows → Arrow → Parquet bytes → memory:// URLs ─┤→ one ListingTable
       → run governed SQL over the union → Rows
```

## Error handling

- All inline-write steps share one transaction; any failure rolls the whole write
  back (including the transactional DDL), so the mirror never half-commits.
- Read reconstruction errors (e.g. an unmapped PG type) surface as the engine's
  opaque `ServingError::Engine`, consistent with the slice-3 path.
- A table with neither Parquet files nor inline rows registers as empty rather than
  erroring (a live table should always have at least one).

## Testing

- **Integration (`loom_fixture_test`, no DuckDB):** seed a table with a Parquet
  append via the slice-2 `IcebergWriter`, then `inline_append` a small batch, then
  read through `DataFusionServingEngine::fetch_rows` and assert the result **unions**
  the file rows and the inline rows. Assert the inline write advanced
  `current_snapshot` and wrote a `lineage.event` row for the run id. Add an
  inline-only table case (no Parquet) and a filter that spans both sources.
- **Unit:** `pg_type_for` over each logical type; the PG-row → Arrow reconstruction
  for each supported type (including nulls).

All tests are `rust_test`/`loom_fixture_test` integration targets (no inline
`#[cfg(test)]`).

## Out of scope (later slices)

- The inline-vs-Parquet **threshold** and the `LandingBackend`/ingest-binary wiring
  (**Slice B**) — Slice A delivers the primitive + read union, driven directly by
  tests.
- **Flush/compaction** of inline rows into a real Parquet data file (which also
  restores external-Iceberg visibility).
- A vendored DataFusion Postgres `TableProvider` (future, for transforms/compaction).
- External-Iceberg-client visibility of inline data (accepted staleness until flush).
- Schema evolution of inline tables.
