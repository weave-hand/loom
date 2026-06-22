# Iceberg overwrite/replace write mode

_Design spec. 2026-06-22._

## Context

The Iceberg write path is **append-only**: every write (`iceberg_writer::append_batches`,
`append_parquet_snapshot`, the flush path) uses `fast_append`. DuckLake has
`Tx::replace_files` — replace a table's live data files in one snapshot,
expiring the prior files at the new snapshot (time travel preserved). Iceberg
has no analogue, so transform overwrite output and any re-land cannot target
Iceberg (`[[fut-iceberg-overwrite]]`).

This is the **second** parity gap on the path to **Iceberg-default**
(`[[fut-replace-ducklake-decision]]`), and a dependency of the third
(`[[road-iceberg-transform-writes]]`): transform's overwrite mode stages files
via `Tx::replace_files`, which needs an Iceberg replace primitive to commit
against. This slice adds that primitive. It does **not** wire transform (that is
the next slice) and does **not** flip any default.

## Current state

- **DuckLake replace** (`src/control-plane/postgres/src/snapshot.rs:99-134`):
  in the commit, for each replaced table — expire all live `ducklake_data_file`
  rows at the new snapshot, reset table stats, clear column stats, then write the
  new files. Change segments `deleted_from_table` + `inserted_into_table`
  (`snapshot.rs:212-217`). Contract test:
  `src/control-plane/postgres/tests/snapshot_replace.rs:28-68` — replace expires
  old, current snapshot lists only new, prior snapshot still lists old.
- **Iceberg mirror primitives** (`src/control-plane/postgres/src/iceberg_mirror.rs`):
  - `project_files` (`:125`) — insert new `iceberg_mirror.data_file` rows (with
    per-column stats) at `begin_snapshot = at`.
  - `mark_dropped` (`:190`) — end-cap **all** live `column` + `data_file` rows at
    `at` (used for table drop). The data-file end-cap is exactly the half a replace
    needs.
- **Iceberg reads resolve through the mirror**: `IcebergCatalog::files`/`files_with_stats`
  return only live (non-end-capped) rows, so a mirror-level end-cap is the source
  of read truth for loom-governed reads.
- **Iceberg metadata commit** (`iceberg_writer.rs` + the `CommitExtrasCatalog`
  end-cap path, `iceberg_sql_catalog/catalog.rs:453-467`): pointer-CAS + mirror
  projection + optional inline end-cap, all in one Postgres transaction.

## Decision — mirror-faithful overwrite

Add an Iceberg **overwrite snapshot** operation parallel to
`append_parquet_snapshot`. In one Postgres transaction:

1. Allocate the new loom snapshot id.
2. **End-cap every currently-live `iceberg_mirror.data_file` row** for the table
   at the new snapshot — the `mark_dropped` data-file update, but leaving the
   `table` and `column` rows live (the table is replaced, not dropped). A small
   shared helper (e.g. `end_cap_live_data_files(conn, table_id, at)`) extracted
   from `mark_dropped`'s data-file leg, used by both.
3. **`project_files`** the new files at the same snapshot — they become the live
   set, carrying their per-column footer stats (so the pruner works on replaced
   data immediately, no backfill).
4. **Iceberg metadata side**: append the new files as an Iceberg snapshot
   (`fast_append`), as today — the *overwrite* semantics live in the mirror
   end-cap (decision below).
5. **Emit lineage** with `deleted_from_table` + `inserted_into_table` change
   segments, mirroring DuckLake's replace, atomically in the same transaction.

Old files keep their `begin_snapshot < at`, `end_snapshot = at`, so older
snapshots still time-travel to them — identical to DuckLake's contract.

### Why mirror-faithful (not Iceberg-native overwrite)

loom-governed reads go through the mirror, so the mirror end-cap fully determines
what a loom read sees. Driving a *real* Iceberg overwrite/replace action so the
raw Iceberg catalog metadata is also faithful to **external** clients is heavier,
depends on iceberg-rust's overwrite-action maturity, and buys correctness loom
does not yet consume (no external raw-metadata readers are in scope). The raw
metadata still showing replaced files until GC is the **same accepted gap class
as `[[iss-iceberg-inline-visibility]]`** — recorded as a follow-up alongside
`[[fut-iceberg-gc]]`, not closed here.

## Surface

A function in the postgres iceberg crate, callable the way `append_parquet_snapshot`
is (the next slice's transform-Tx impl and any re-land path call it):

```rust
// src/control-plane/postgres/src/iceberg_landing.rs (or a sibling)
async fn overwrite_parquet_snapshot(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    batches: Vec<RecordBatch>,
    lineage: Option<&LineageEvent>,
) -> Result<SnapshotId, _>
```

Symmetry note: `append_parquet_snapshot` writes Parquet from batches and projects
them; `overwrite_parquet_snapshot` does the same plus the live-file end-cap. They
share the Parquet-write + project + stats path; only the end-cap step differs.
(The next slice may also need a variant that registers **already-written**
`DataFile`s rather than writing from batches — that belongs to the transform-Tx
design, not here; this slice's surface writes from batches, matching the existing
append entrypoint.)

## Error handling

- The whole operation is one Postgres transaction: end-cap + project + Iceberg
  pointer-CAS + lineage commit or roll back together. A mid-operation failure
  leaves the prior live set intact (no partial replace).
- Replacing a table with **zero** new files is a valid truncation: end-cap all
  live files, project nothing, emit `deleted_from_table` only (matches DuckLake,
  whose change-segment code guards the empty-insert case).
- Replacing a table that has **no** live files (never appended) is an append:
  the end-cap matches zero rows, project writes the new set.

## Testing

`rust_test` integration targets via `loom_fixture_test` (hermetic Postgres +
object store), mirroring `snapshot_replace.rs`:

- **Replace contract** (the core test): create an Iceberg table, append
  `a.parquet` (N rows); overwrite with `b.parquet` (M rows); assert
  `IcebergCatalog::files` at the current snapshot lists only `b.parquet`/M rows,
  and the prior snapshot still lists `a.parquet`/N rows (time travel preserved).
  This is the Iceberg twin of `snapshot_replace.rs`.
- **Stats after replace**: the replaced file's per-column stats are present in
  `data_file_column_stat` and the serving pruner skips it where predicates allow
  (reuse the pruning assertions from the per-column-stats e2e).
- **Truncate**: overwrite with zero files end-caps all live files; current
  snapshot lists none; prior snapshot still time-travels.
- **Atomicity**: a contrived failure during the overwrite leaves the original
  live set unchanged (no orphaned end-caps, no half-projected new files).
- **Lineage**: the overwrite emits `deleted_from_table` (+ `inserted_into_table`
  when non-empty), asserted against the lineage read — same shape DuckLake emits.

## Scope boundary

- **In:** the `overwrite_parquet_snapshot` primitive (mirror end-cap + project +
  Iceberg append + lineage, atomic), the shared `end_cap_live_data_files` helper
  extracted from `mark_dropped`, and the tests above.
- **Out (deferred, tracked):** wiring transform to call it
  (`[[road-iceberg-transform-writes]]`, the next slice); the default flip
  (`[[fut-replace-ducklake-decision]]`); Iceberg-native (raw-metadata-faithful)
  overwrite; physical reclamation of end-capped Parquet (`[[fut-iceberg-gc]]`);
  registering already-written `DataFile`s vs writing from batches (decided in the
  transform-Tx slice).

## Acceptance criteria

1. An Iceberg table can be overwritten: the new files become the sole live set,
   prior files remain reachable by time travel — identical contract to DuckLake
   `replace_files` (`snapshot_replace.rs`), now passing for Iceberg.
2. The end-cap + project + Iceberg metadata + lineage commit in one Postgres
   transaction; failure leaves the prior live set intact.
3. Replaced files carry per-column stats; the serving pruner operates on them.
4. Truncate (overwrite with zero files) and overwrite-of-empty both behave per
   the DuckLake contract.
5. `buck2 test //src/...` is green; append-only behavior and all defaults are
   unchanged (overwrite is a new, separately-invoked path).
