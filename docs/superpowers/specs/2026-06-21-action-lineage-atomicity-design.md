# Action lineage atomicity — loom-owned atomic write + run_id handle — Design

> Closes `iss-action-lineage-atomicity`. Actions part-1 (`2026-06-15-actions-part1-design.md`)
> deliberately wrote the inserted row via DuckDB's `DATA_INLINING` path for low
> single-row latency. That path has DuckDB own and commit the DuckLake snapshot on
> its own connection *before* control returns; loom then looks up the snapshot and
> emits lineage best-effort on a separate connection. A crash in that gap leaves a
> snapshot with no lineage event (the documented "dangling slice"), and the event's
> `run_id` is a throwaway UUID never surfaced to the caller, so it isn't queryable.
> This slice makes the write and its lineage one atomic unit and surfaces the
> `run_id`.

## Goal

Make an ontology action's row write and its lineage event commit atomically — both
land or neither does — and return the action's `run_id` to the caller so its
lineage is findable. Realizes the `ARCHITECTURE.md` intent that "an action's
snapshot, its lineage event, and any downstream job enqueue remain one atomic
unit … via the Rust snapshot-commit primitive."

## Approach (chosen)

Route the action write through loom's **own** snapshot-commit primitive (the
`Tx` seam — `create_table` + `append_files` + `emit` + `commit` in one Postgres
transaction) instead of DuckDB's inline write. This is the same primitive ingest
uses (`materialize::land_ducklake`) and the same atomic shape the Iceberg backend
already has (`inline_append(…, lineage) -> SnapshotId`). The deliberate tradeoff:
the part-1 "inline row, no Parquet file" low-latency property is **retired** in
favor of atomicity — each action write produces one small Parquet file + a
DataFusion write. Actions are interactive, low-frequency governed write-backs, so
this is acceptable; small-file accumulation is handled by loom's existing
compaction.

Rejected alternatives: a **reconciliation sweep** (keeps inline writes but the
crash window persists and back-filled events lose action context); a **loom-owned
DuckLake-inline write** re-implementing DuckDB's internal inline format in loom SQL
(keeps latency + atomicity but couples loom to DuckLake internals — fragile,
large).

## The `ActionEngine` seam reshape

Today (opaque, non-atomic):

```rust
async fn insert_row(&self, table: &TableRef, columns: &[String], values: &[SqlValue])
    -> Result<(), ServingError>;
```

Becomes an **atomic write that bundles the lineage event and returns the snapshot**:

```rust
async fn write_object(
    &self,
    table: &TableRef,
    columns: &[String],
    values: &[SqlValue],
    logical_types: &[String],   // to build the one-row Arrow schema
    event: LineageEvent,
) -> Result<SnapshotId, ServingError>;
```

The caller builds the `LineageEvent` up front (so it owns the `run_id`) and hands
it to the engine, which writes the row **and** the event in one commit. This
mirrors the Iceberg `inline_append(…, lineage) -> SnapshotId` contract, unifying
both backends under one atomic write seam. The trait stays Arrow-free (takes
`logical_types`, not a `RecordBatch`); each impl builds Arrow internally.

## The DuckLake implementation

A new `DuckLakeActionWriter { cp: Arc<dyn ControlPlane>, store: Arc<dyn ObjectStore> }`
replaces `EmbeddedDuckDbWriter` on the action path. Its `write_object`:

1. Builds a **one-row Arrow `RecordBatch`** + `Schema` + `[ColumnSpec]` from
   `(columns, values, logical_types)` — Arrow `DataType` and DuckLake physical type
   derived from each logical type via `control_plane_core::resolve_logical`. The
   incoming `(columns, values, logical_types)` cover the target type's **full**
   property set (see `run_action` rework — properties the action does not set are
   passed as `SqlValue::Null`), so the written Parquet file's schema matches the
   table and `append_files` is consistent. (Part-1's DuckDB `INSERT` let unspecified
   columns default to NULL implicitly; the Parquet path makes that explicit.)
2. Calls **`ingest::land_ducklake(&*cp, store.clone(), table, schema, &columns,
   &[batch], &file_prefix, event)`** — the proven path that writes a Snappy Parquet
   file, then `create_table` (idempotent; the target table already exists) +
   `append_files` (with computed DuckLake `DataFile` stats) + `emit(event)` +
   `commit`, all in one Postgres transaction, returning the new `SnapshotId`.
3. Maps `IngestError` → `ServingError::Engine`.

`file_prefix` is `action-<run_id>` (unique per action). Reusing `land_ducklake`
avoids re-deriving DuckLake per-file stats and inherits the atomic commit; the
per-action DataFusion write is the accepted cost (a lightweight direct
arrow→parquet writer is a noted future optimization, not in this slice).

`query-api` gains a library dependency on `//src/services/ingest:ingest`
(acyclic — ingest does not depend on query-api). `EmbeddedDuckDbWriter`, the
`DATA_INLINING` attach variant, and `INLINE_ROW_LIMIT` are **removed** (dead once
actions move off the inline path). The read engine `EmbeddedDuckDb` is unchanged —
it already reads Parquet-backed DuckLake tables.

## `run_action` rework

`src/services/query-api/src/action.rs`, after the conformance check and the
fine-grained write policy:

1. Mint `run_id = RunId(Uuid::new_v4())` and build
   `LineageEvent { run_id, event_type: Complete, event_time: now, inputs: vec![],
   outputs: vec![DatasetRef::from(&action.target)], payload: json!({ "action":
   action_name }) }`. (`inputs` stays empty — a create-from-params action has no
   upstream datasets. The old post-hoc `snapshot_id` payload field is dropped: the
   event now commits *with* the snapshot, so their linkage is structural, not a
   best-effort breadcrumb.)
2. Expand the row to the target type's **full property set**: for each property
   (in declared order), use the action's parsed value if it set that column, else
   `SqlValue::Null`. This `(columns, values, logical_types)` triple — the full
   schema, not just the action's params — is what's written, so the Parquet file
   matches the table. (Part-1 relied on DuckDB filling unspecified columns with NULL;
   the loom-owned Parquet write must include them.) The fine-grained write policy
   still gates on the *set* (non-null) columns, unchanged.
3. Call `deps.action_engine.write_object(&target.table, &columns, &values,
   &logical_types, event).await?` (propagates `ServingError` → `ActionError::Serving`).
4. Delete the old steps: the `catalog().current_snapshot()` lookup and the
   best-effort `lineage().emit()` (with its dangling-slice comment) are gone — the
   engine now owns the atomic emit.
5. Return **`(ObjectRows, RunId)`** (a small `ActionOutcome { rows, run_id }` struct,
   or a tuple). The `ObjectRows` projects only the action-provided columns (as
   part-1 returns today); the full-schema expansion in step 2 is for the write only.

`UnsupportedActionEngine` (the Iceberg-backend placeholder) is updated to the new
`write_object` signature and still returns an error — wiring the Iceberg
`inline_append` as a real action backend stays the deferred `fut-iceberg-actionengine`.

## The run_id handle (queryability)

`post_action` (`src/services/query-api/src/http.rs`) sets an **`X-Loom-Run-Id`**
response header carrying the `run_id` on the 201 Created. The created-object JSON
body is unchanged (non-invasive). A caller can then locate the action's lineage via
`Lineage::events_for(run_id)`.

## Atomicity guarantee & error handling

The `Tx` rolls back on any failure (object-store write, Postgres), so a failed
action write leaves **no** snapshot, **no** lineage, **no** partial state — it
surfaces as `ServingError` → the existing opaque 500. There is no best-effort emit
and no dangling window. (`Tx` cross-concern atomicity is already contract-tested by
`tx_atomic_rollback_contract`.)

## Binary wiring

`query-api` `main.rs` constructs `DuckLakeActionWriter::new(cp.clone(), store)`
instead of `EmbeddedDuckDbWriter::attach(...)`. The control plane (`cp`) and object
store are already built for the read/serving path, so no new configuration is
needed; the writer shares them.

## Testing

All `rust_test` integration targets (no inline `#[cfg(test)]`); fixture-backed via
`loom_fixture_test`.

- **e2e (DuckLake fixture)** — the headline test: invoke an action over `run_action`
  (or the router), then assert **both** (a) the row reads back through the governed
  read path, **and** (b) a lineage event exists for the returned `run_id`
  (`Lineage::events_for`) whose `outputs` name the target type's dataset. This is
  exactly the assertion part-1 could not make (it skipped lineage as the dangling
  slice). Update the existing `action_e2e` accordingly (it currently documents that
  it intentionally does *not* assert lineage).
- **handler (in-memory)** — `run_action` over `MemoryControlPlane` + a recording
  stub `ActionEngine`: assert the returned `run_id` matches the `LineageEvent`
  handed to the engine, and that lineage is delivered to the engine (the atomic
  seam) rather than via a separate `Lineage::emit` call.
- **http** — a 201 response carries the `X-Loom-Run-Id` header equal to the action's
  `run_id`.
- **seam update** — port the part-1 action tests to `write_object`; retire the
  `EmbeddedDuckDbWriter`-specific `action_engine` test (the writer is removed).

## Out of scope

- **Iceberg action backend** (`fut-iceberg-actionengine`) — wiring `inline_append`
  as a real `ActionEngine`. `UnsupportedActionEngine` stays.
- **Update/delete actions** (`fut-update-delete-actions`).
- **A lightweight non-DataFusion single-row writer** — a latency optimization over
  `land_ducklake`, deferred until per-action latency is shown to matter.

## Files

- Modify: `src/services/query-api/src/serving.rs` — reshape `ActionEngine`; add
  `DuckLakeActionWriter`; remove `EmbeddedDuckDbWriter` + `INLINE_ROW_LIMIT`.
- Modify: `src/services/query-api/src/serving_datafusion.rs` — `UnsupportedActionEngine`
  to the new signature.
- Modify: `src/services/query-api/src/action.rs` — `run_action` rework; return
  `(ObjectRows, RunId)`.
- Modify: `src/services/query-api/src/http.rs` — `X-Loom-Run-Id` header on 201.
- Modify: `src/services/query-api/src/main.rs` — construct `DuckLakeActionWriter`.
- Modify: `src/services/query-api/BUCK` — add the `ingest` dep to `:query-api`;
  test-target deps.
- Modify/Create: `src/services/query-api/tests/action_e2e.rs` (assert lineage),
  the handler + http tests; remove `tests/action_engine.rs`.
- Modify: `docs/ISSUES.md` — close `iss-action-lineage-atomicity` (`[x] status:fixed`).
