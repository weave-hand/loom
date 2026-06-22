# Iceberg `ActionEngine` — governed object writes on the Iceberg backend

_Design spec. 2026-06-22._

## Context

loom serves two table-format backends behind boot-time seams. DuckLake is the
default; Iceberg coexists, selected by `LOOM_LANDING_BACKEND` (ingest) and
`LOOM_SERVING_BACKEND` (query-api). The strategic direction is **Iceberg-default,
DuckLake kept** — flip the boot defaults to Iceberg once Iceberg reaches
production parity, keeping DuckLake selectable as a fallback and as the test
differential oracle (see `[[fut-replace-ducklake-decision]]`). Three production
gaps block that flip: governed action writes, transform output, and overwrite
mode. This spec closes the **first** of them.

Today the query-api Iceberg serving backend wires `UnsupportedActionEngine`
(`src/services/query-api/src/serving_datafusion.rs:403`), which rejects every
governed action write with an opaque error. So a deployment that serves reads
from Iceberg cannot service the typed-insert action endpoint at all — the
serving default cannot flip to Iceberg while actions are unsupported. This is
the gap `[[fut-iceberg-actionengine]]` names: "the `ActionEngine` trait's reason
for being — a second write backend behind the inline-write seam."

This slice is **insert-only** (matching actions part-1; update/delete stay
deferred to `[[fut-update-delete-actions]]`) and does **not** flip any default
(that is the later umbrella step, gated on transform-writes + overwrite also
landing).

## The seam, as it stands

`ActionEngine` is a format-neutral trait
(`src/services/query-api/src/serving_datafusion.rs:197`):

```rust
async fn write_object(
    &self,
    table: &TableRef,
    columns: &[String],
    values: &[SqlValue],
    logical_types: &[String],
    event: LineageEvent,
) -> Result<SnapshotId, ServingError>;
```

The DuckLake impl `DuckLakeActionWriter`
(`src/services/query-api/src/serving.rs:219`) builds a one-row batch with
`build_object_batch(columns, values, logical_types)` and calls
`ingest::materialize::land_ducklake(...)` — a one-row Parquet file plus
`create_table` (idempotent) + `append_files` + `emit(lineage)` + `commit`, all
in a single Postgres transaction. Row and lineage land or roll back together.

The Iceberg landing path already exposes the symmetric atomic entrypoint
`control_plane_postgres::iceberg_landing::land`
(used by ingest's `IcebergMaterializer`,
`src/services/ingest/src/landing.rs:104`):

```rust
iceberg_land(
    &pool, &catalog, table, columns /* &[ColumnSpec] */,
    ipc_body /* &[u8] */, inline_byte_limit, flush_byte_threshold, lineage,
) -> Result<SnapshotId, _>
```

It decodes the IPC body in arrow-57 (the established cross-major boundary inside
the postgres crate), routes by in-memory byte size between `inline_append`
(mirror-only typed rows, atomic) and real Parquet, and emits lineage atomically
in the same Postgres transaction.

## Decision — route actions through the inline-write seam

The Iceberg `ActionEngine` builds the same one-row batch and forwards it to
`iceberg_landing::land`. A single action row is far below `inline_byte_limit`, so
it lands as a **mirror-only inline row**: one Postgres transaction committing the
row and its lineage together, drained to real Parquet later by the existing
flush vertical (`[[road-iceberg-flush-consumer]]`).

This is approach **A** of three considered:

- **A — inline-write seam (chosen).** Reuse `iceberg_land`; the row inlines.
  Atomic (single PG tx with lineage), low-latency, zero new commit logic. Exactly
  the "second write backend behind the inline-write seam" `[[fut-iceberg-actionengine]]`
  describes. The transactional database commit *is the point* — the action row and
  its lineage event are durable together or not at all.
- **B — force per-action Parquet.** Mirror DuckLake's "atomicity over latency,
  compaction handles small files" choice; every action becomes a one-row Parquet
  file immediately visible to external Iceberg clients. Rejected: costs a Parquet
  + footer write per interactive action and multiplies small files, for an
  external-visibility property the inline path already reaches via flush.
- **C — bespoke single-row Iceberg path.** A new lower-level helper taking a
  `RecordBatch` directly, skipping the IPC encode/decode. Rejected: duplicates the
  atomic-commit logic and diverges from the landing seam for a marginal round-trip
  saving.

**Inherited semantics.** Inline rows are invisible to *external* Iceberg clients
until a flush (`[[iss-iceberg-inline-visibility]]`, an accepted bounded-staleness
tradeoff) and are re-parsed from Postgres on each read until flushed
(`[[iss-iceberg-inline-reparse]]`). Actions inherit these exactly as the Iceberg
*landing* path already does — this slice introduces no new staleness or cost
semantics, it reuses the landing path's.

## Architecture

**New `IcebergActionWriter`** in `src/services/query-api/src/serving_datafusion.rs`,
replacing `UnsupportedActionEngine`. It holds the same dependencies as ingest's
`IcebergMaterializer`:

```rust
pub struct IcebergActionWriter {
    catalog: Arc<SqlCatalog>,
    pool: PgPool,
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
}
```

`write_object` is a thin adapter:

1. `let (schema, batch, specs) = build_object_batch(columns, values, logical_types)?;`
   — reuse the existing one-row batch builder. (It currently lives in the
   serving/DuckLake module; if it is not already reachable from
   `serving_datafusion.rs`, lift it to a shared location — e.g. a small
   `action_batch` module — so both action writers call one implementation rather
   than duplicating it. This is the only refactor in scope, and only if needed.)
2. Encode `batch` to an Arrow IPC stream (arrow-58 `StreamWriter`), producing the
   `ipc_body` bytes the Iceberg entrypoint decodes in arrow-57 — the same wire
   format ingest already crosses.
3. `iceberg_landing::land(&self.pool, &self.catalog, table, &specs, &ipc_body,
   self.inline_byte_limit, self.flush_byte_threshold, event)`.
4. Map the `SnapshotId` through; map any error to opaque `ServingError::Engine`.

**Wiring.** `src/services/query-api/src/main.rs`'s Iceberg branch already
constructs the `DataFusionServingEngine` from the catalog + pool + inline/flush
env. Construct `IcebergActionWriter` from the same handles instead of
`UnsupportedActionEngine`, and delete `UnsupportedActionEngine`.

**ACL is unchanged.** Write enforcement happens in the action handler *before* the
engine is called (`[[road-write-enforcement]]`); the engine only persists. A
denied action never reaches `write_object`, on either backend.

## Data flow

```
POST /actions/{type}/{action}            (governed action endpoint)
  -> handler: ACL write-enforcement (unchanged, pre-engine)
  -> ActionEngine::write_object(table, columns, values, logical_types, event)
       IcebergActionWriter:
         build_object_batch  -> (schema, one-row batch, ColumnSpecs)
         encode IPC (arrow-58)
         iceberg_landing::land  -> inline_append (row < limit)
              one Postgres tx: insert inline row + emit(lineage) + commit
         -> SnapshotId
  -> 200 { snapshot_id }
```

Read-back through the Iceberg serving engine immediately unions the new inline
row with file-backed Parquet (the existing Slice-A union), so the written object
is visible to governed reads on the same backend without waiting for a flush.

## Error handling

- Engine/landing failures (Postgres, IPC encode, mirror commit) map to
  `ServingError::Engine(...)` — the opaque shape the action endpoint already
  surfaces, identical to the DuckLake writer's mapping.
- Type coverage is the canonical scalar set `build_object_batch` already supports;
  an unsupported logical type fails the batch build before any write, exactly as on
  the DuckLake path. Wider types remain `[[fut-datafusion-type-coverage]]`.
- A row that (implausibly) exceeds `inline_byte_limit` routes to the Parquet
  branch of `iceberg_land` transparently — no special-casing, still atomic.

## Testing

All tests are `rust_test` integration targets (per the repo's no-inline-tests
rule), routed local via `loom_fixture_test` where they boot hermetic
Postgres/object-store.

- **e2e (primary):** mirror the DuckLake `action_e2e.rs` against the Iceberg
  serving backend, reusing `//src/services/query-api:e2e-support`. Drive a
  governed typed-insert action through the HTTP action endpoint with
  `LOOM_SERVING_BACKEND=iceberg`, then read the object back through the Iceberg
  DataFusion serving engine and assert: the row is present with correct typed
  values, a lineage event was emitted for the write, and the returned
  `SnapshotId` advances. The read-back oracle is **loom's own Iceberg engine**
  (inline+file union) — external-DuckDB visibility is explicitly out of scope
  (inline rows are invisible pre-flush by `[[iss-iceberg-inline-visibility]]`).
- **ACL:** an action denied by write-enforcement returns the governed denial and
  performs no write on the Iceberg backend — same assertion the DuckLake action
  e2e makes, proving enforcement is engine-independent.
- **Atomicity:** assert the row and its lineage event are both present after a
  successful write (they commit in one tx); a contrived landing failure leaves
  neither (no orphaned row, no orphaned lineage). This is the property the whole
  slice exists to provide.
- **Unit:** `IcebergActionWriter::write_object` builds the expected one-row batch
  and IPC body from representative `(columns, values, logical_types)` — a pure
  shaping test with no fixture.

## Scope boundary

- **In:** `IcebergActionWriter` implementing `ActionEngine` via the inline-write
  seam; query-api Iceberg-branch wiring; removal of `UnsupportedActionEngine`;
  the tests above. Insert-only, canonical scalars.
- **Out (deferred, tracked):** the default flip to Iceberg
  (`[[fut-replace-ducklake-decision]]`, gated on transform-writes + overwrite);
  update/delete actions (`[[fut-update-delete-actions]]`); wider type coverage
  (`[[fut-datafusion-type-coverage]]`); external-client visibility of inline rows
  (`[[iss-iceberg-inline-visibility]]`); inline re-parse cost
  (`[[iss-iceberg-inline-reparse]]`); physical GC (`[[fut-iceberg-gc]]`).

## Acceptance criteria

1. `LOOM_SERVING_BACKEND=iceberg` deployments accept governed typed-insert
   actions; the row is immediately readable through the Iceberg serving engine.
2. The write commits the row and its lineage event in one Postgres transaction —
   both land or neither does.
3. ACL write-enforcement governs Iceberg-backed actions identically to DuckLake.
4. `UnsupportedActionEngine` is removed; no action path returns "actions
   unsupported on the iceberg serving backend".
5. `buck2 test //src/...` is green; no DuckLake-backed behavior changes (the
   DuckLake action path and all defaults are untouched).
