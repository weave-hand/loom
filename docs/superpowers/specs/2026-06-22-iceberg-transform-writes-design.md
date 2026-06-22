# Transform output to Iceberg — a polymorphic `Tx`

_Design spec. 2026-06-22._

## Context

This is the **third** parity gap on the path to **Iceberg-default**
(`[[fut-replace-ducklake-decision]]`): the transform worker can only write
DuckLake output. With governed writes (`[[road-iceberg-actionengine]]`) and
overwrite mode (`[[road-iceberg-overwrite-mode]]`) planned, transform is the last
production write path pinned to DuckLake.

Transform is already **backend-agnostic in its own code**
(`src/services/transform/src/run.rs:75-175`): it writes its Parquet via
`write_dataset` and stages the commit through the format-neutral `Tx` trait —
`cp.begin() → create_table → append_files | replace_files → emit(lineage) →
commit` (`run.rs:166-174`). The blocker is not in transform: the **only** `Tx`
impl is `PgTx`, whose commit (`src/control-plane/postgres/src/snapshot.rs`)
writes `ducklake_*` rows, and Iceberg writes bypass `Tx` entirely
(`iceberg_writer.rs` `fast_append`). `PgControlPlane::begin()`
(`src/control-plane/postgres/src/lib.rs:93`) unconditionally returns the DuckLake
`PgTx`.

**Decision (operator, 2026-06-22): make `Tx` genuinely polymorphic.** Rather than
a transform-local write port, add an **Iceberg-backed `ControlPlane`/`Tx`** so
`cp.begin()` routes the *same* staged operations to Iceberg. Transform's write
code is then unchanged — it only depends on which `ControlPlane` is injected at
boot. This realizes the format-neutrality the `Tx` trait already *declares*
(`src/control-plane/core/src/transaction.rs:32-69`) and is reusable by any future
`Tx` consumer. It is the larger lift, chosen deliberately over the localized
seam.

This slice does **not** flip any default and does **not** migrate ingest/actions
onto the polymorphic `Tx` (they keep their current write seams); it makes `Tx`
polymorphic and proves it end-to-end through transform.

## Current state

- **`Tx` trait** (`core/src/transaction.rs:32-69`): `create_table`,
  `append_files`, `replace_files`, `compact_files`, `enqueue`, `emit`, `commit`.
  Declared format-neutral; only the DuckLake impl exists.
- **`PgTx`** stages ops (`postgres/src/transaction.rs:11-72`) and applies them in
  `commit_snapshot` (`snapshot.rs`) against `ducklake_*` tables.
- **Iceberg write functions** (one PG tx each, via `CommitExtrasCatalog` carrying
  lineage + inline end-cap — `iceberg_sql_catalog/catalog.rs:453-467`):
  - `append_parquet_snapshot` — write Parquet from batches + `fast_append` + mirror
    project + lineage.
  - `overwrite_parquet_snapshot` — the same plus end-cap all live data files
    (`[[road-iceberg-overwrite-mode]]`).
  - `iceberg_mirror::project_files` (`:125`) / `end_cap_live_data_files` — the
    mirror primitives those compose.
- **Transform already produces written `DataFile`s** (`run.rs:143-164`,
  `write_dataset`) — path, record_count, file_size, per-column stats in object
  storage. It does **not** re-write them.

## Design

### 1. Register-already-written-files Iceberg entrypoints

Transform's files are already on object storage, so the Iceberg `Tx` must
**register** them, not re-write batches. Add (in the postgres iceberg crate) the
register variants the `overwrite-mode` spec deferred to here:

```rust
async fn register_files(
    tx: &mut PgConnection,         // the IcebergTx's held transaction
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    files: &[DataFile],            // already written by the caller
    mode: WriteMode,               // Append | Overwrite
    at: SnapshotId,
) -> Result<()>
```

- **Append**: build Iceberg `DataFile` descriptors from the loom `DataFile`s,
  `fast_append().add_data_files(...)` on the Iceberg metadata, and
  `project_files` the same set into the mirror at `at`.
- **Overwrite**: `end_cap_live_data_files(table_id, at)` then `project_files` the
  new set (the `[[road-iceberg-overwrite-mode]]` primitive, over already-written
  files) + the Iceberg append.

**Mirror-faithful, consistent with the prior two slices.** loom reads resolve
through the mirror (paths + per-column stats), so registering loom-written Parquet
makes loom-governed reads correct. Full Iceberg-native field-id/raw-metadata
faithfulness for *external* clients is the same accepted gap class as
`[[iss-iceberg-inline-visibility]]` / mirror-faithful overwrite — noted, not
closed here.

### 2. `IcebergControlPlane` + `IcebergTx`

A `ControlPlane` whose `catalog()` is the existing `IcebergCatalog` (reads, already
built) and whose `begin()` returns an `IcebergTx` holding one `sqlx::Transaction`
and staging the same op set as `PgTx`:

| `Tx` method | `IcebergTx` behavior |
|---|---|
| `create_table` | ensure Iceberg table + mirror exist (idempotent), via the Iceberg catalog |
| `append_files` | stage `(table, files, Append)` |
| `replace_files` | stage `(table, files, Overwrite)` |
| `emit` | stage the lineage event (lineage schema is backend-neutral) |
| `enqueue` | stage a queue insert (queue schema is backend-neutral) |
| `commit` | allocate the snapshot; apply staged `create_table`/file registrations via `register_files` + Iceberg pointer-CAS + mirror project + staged lineage/enqueue, **all in the one held PG transaction**; return `SnapshotId` |
| `compact_files` | **out of scope** this slice — return unsupported (Iceberg compaction is `[[fut-iceberg-gc]]`-adjacent, deferred) |

The atomicity model already exists: Iceberg writes commit pointer-CAS + mirror +
lineage in one PG tx via `CommitExtrasCatalog`. `IcebergTx::commit` generalizes
that wrapper to carry the staged op set, so a transform output (create_table +
append/overwrite + lineage) lands or rolls back atomically — the same guarantee
`PgTx` gives for DuckLake.

### 3. Boot selection in transform

`src/services/transform/src/main.rs:17` constructs the control plane. Select the
backend at boot — reuse the existing convention (`LOOM_TRANSFORM_BACKEND`,
mirroring `LOOM_LANDING_BACKEND`/`LOOM_SERVING_BACKEND`; unset → DuckLake): build
either `PgControlPlane` (today) or `IcebergControlPlane` and inject it. `run.rs`
is untouched — the polymorphic `Tx` is the whole point.

## Data flow

```
transform job dequeued
  -> run_transform(cp, store, req):           (run.rs — UNCHANGED)
       scan inputs (cp.catalog(), backend-agnostic)
       run SQL, collect batches
       write_dataset -> Vec<DataFile>          (already written to object store)
       tx = cp.begin()                          (IcebergTx when cp = IcebergControlPlane)
       tx.create_table(output, columns)
       match output_mode {
         Append    => tx.append_files(output, &files),
         Overwrite => tx.replace_files(output, &files),   (overwrite primitive)
       }
       tx.emit(lineage)
       tx.commit()  -> SnapshotId
            one PG tx: register files + Iceberg pointer-CAS + mirror project + lineage
```

## Error handling

- `commit` is one PG transaction: all staged ops or none. A registration/CAS/lineage
  failure rolls back — no orphaned mirror rows, no half-applied overwrite.
- A transform output with a type outside the canonical scalar set fails at
  `write_dataset`/projection as today (`[[fut-datafusion-type-coverage]]`),
  before any Iceberg commit.
- `compact_files` on `IcebergTx` returns an explicit unsupported error (no transform
  path calls it; documented).

## Testing

`rust_test` integration targets via `loom_fixture_test` (hermetic Postgres + object
store). Reuse the transform e2e fixtures (`overwrite_e2e.rs`, `output_mode.rs`).

- **Append e2e (Iceberg backend):** run a transform with `LOOM_TRANSFORM_BACKEND=iceberg`
  and `output_mode=append`; assert the output table is readable through the Iceberg
  catalog/serving engine with the expected rows, lineage emitted, snapshot advanced.
- **Overwrite e2e (Iceberg backend):** the Iceberg twin of `overwrite_e2e.rs` —
  overwrite replaces live contents; a prior snapshot still time-travels to the old
  contents (exercises `[[road-iceberg-overwrite-mode]]` through the `Tx` seam).
- **Atomicity:** a contrived commit failure leaves no output snapshot and no
  partial mirror state (output table unchanged).
- **`IcebergTx` unit/contract:** the staged-op → applied-effect mapping for
  create_table/append/replace/emit, and `compact_files` returns unsupported.
- **DuckLake unchanged:** the existing transform e2e suite still passes with the
  default backend (no `LOOM_TRANSFORM_BACKEND`).

## Scope boundary

- **In:** `IcebergControlPlane` + `IcebergTx` (create_table, append_files,
  replace_files, emit, enqueue, commit — atomic in one PG tx); the
  `register_files` append/overwrite entrypoints over already-written `DataFile`s;
  transform boot selection; the tests above.
- **Out (deferred, tracked):** the default flip
  (`[[fut-replace-ducklake-decision]]`); migrating ingest/actions onto the
  polymorphic `Tx` (they keep their current seams — this slice only proves `Tx`
  through transform); `IcebergTx::compact_files` (deferred,
  `[[fut-iceberg-gc]]`-adjacent); Iceberg-native raw-metadata/field-id
  faithfulness for external clients (`[[iss-iceberg-inline-visibility]]` class);
  wider type coverage (`[[fut-datafusion-type-coverage]]`).

## Acceptance criteria

1. With `LOOM_TRANSFORM_BACKEND=iceberg`, a transform writes its output to Iceberg
   in **append** mode; the result is readable through the Iceberg catalog/serving
   engine with lineage emitted — `run.rs` unchanged.
2. **Overwrite** mode replaces the table's live contents and preserves time travel,
   driving `[[road-iceberg-overwrite-mode]]` through `Tx::replace_files`.
3. `cp.begin()` is polymorphic: the same transform code commits to DuckLake or
   Iceberg depending only on the injected `ControlPlane`.
4. `IcebergTx::commit` is atomic — file registration + Iceberg pointer-CAS + mirror
   projection + lineage land in one Postgres transaction or roll back together.
5. `buck2 test //src/...` is green; the DuckLake transform path and all defaults are
   unchanged.
