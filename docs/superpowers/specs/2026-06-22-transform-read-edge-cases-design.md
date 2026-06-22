# Transform read-path edge cases — Design

> Closes `iss-transform-read-edge-cases`. `run_transform`
> (`…/services/transform/src/run.rs`) has two input-resolution edge cases that are
> mis-classified by the worker's retry policy:
> (1) a missing input that surfaces at `Catalog::files` (rather than
> `current_snapshot`) becomes `TransformError::ControlPlane` → **Retry**, so a
> permanently-absent input is retried forever instead of abandoned; and
> (2) an input table that exists but has **zero files** at its snapshot is passed
> to `scan_table` with an empty file list, which fails inside DataFusion's
> `infer_schema` → `TransformError::Scan` → **Retry**, instead of yielding an empty
> input the SQL can run over. This slice maps both to their correct, deterministic
> behavior.

## Background — how transform errors are classified

`retry_policy` (`…/transform/src/handler.rs:158`) maps `TransformError` to a
`RetryPolicy`:

- **Abandon** (deterministic, never succeeds on retry): `UnknownInput`,
  `AmbiguousInput`, `DoesNotConform`, `DataFusion`, `Infer`, `NoSnapshot`.
- **Retry** (transient): `ControlPlane`, `Scan`, `Write`.

`run_transform` step 1 resolves each input: `current_snapshot(table)` (maps
`NotFound → UnknownInput`), then `files(table, snap)?` (a bare `?`, so any error —
including `NotFound` — becomes `ControlPlane`), then `scan_table(.., &files.items)`.
The two bugs live in that block.

## Edge case 1 — missing input at `files`/`schema` → `UnknownInput` (Abandon)

`current_snapshot` already classifies a missing table as `UnknownInput`, but the
subsequent `files` call (and the new `schema` call from edge 2) let a `NotFound`
fall through to `ControlPlane` → Retry. A `NotFound` from any of the three input
reads means the table is not live at that snapshot (e.g. dropped between reads) —
**deterministically a bad input**, not a transient fault. Map all three the same
way.

Extract the existing inline mapping into a tiny helper and apply it at every input
read:

```rust
fn unknown_input(table: &TableRef, e: ControlPlaneError) -> TransformError {
    match e {
        ControlPlaneError::NotFound(_) =>
            TransformError::UnknownInput(table.schema.clone(), table.name.clone()),
        other => TransformError::ControlPlane(other),
    }
}
```

Applied to `current_snapshot` (unchanged behavior — just refactored to the helper),
`files`, and the `schema` read added below. Net effect: a missing input is
`UnknownInput` → **Abandon** regardless of which read surfaces it.

## Edge case 2 — empty input → empty relation, not a scan failure

When `files.items` is empty, do not call `scan_table` (its `infer_schema` over zero
paths fails). Instead register an **empty relation with the table's declared
schema**, so the transform SQL runs over an empty input (e.g. `SELECT *` → empty
result; `SELECT count(*)` → one row of `0`) and commits normally.

The schema comes from the catalog, not the (absent) files:
`Catalog::schema(table, at) -> TableSchema` (already on the trait) returns the
column defs at the snapshot. Build an Arrow schema from them and register an empty
`MemTable`.

New datafusion-io helpers (siblings of the existing read-path code, reusing its
type taxonomy):

- `infer.rs` — the inverse of the existing `arrow_logical_type`, covering exactly
  the same five types (YAGNI: the set `infer_columns` supports), erroring on
  anything else so the classification stays deterministic:

  ```rust
  /// loom logical type name -> Arrow DataType. Inverse of `arrow_logical_type`;
  /// the same five types `infer_columns` round-trips. `None` for unmapped types.
  pub fn logical_arrow_type(ty: &str) -> Option<DataType> {
      match ty {
          "boolean" => Some(DataType::Boolean),
          "integer" => Some(DataType::Int32),
          "long"    => Some(DataType::Int64),
          "double"  => Some(DataType::Float64),
          "string"  => Some(DataType::Utf8),
          _ => None,
      }
  }

  /// Build an Arrow schema from loom column specs. Errors `InferError::Unsupported`
  /// on a type outside the supported set — which maps to `TransformError::Infer`
  /// → Abandon (deterministic), NOT Scan/Retry.
  pub fn logical_arrow_schema(columns: &[ColumnSpec]) -> Result<SchemaRef, InferError>;
  ```

  Using `InferError` (not `ScanError`) is deliberate: an empty input whose schema
  carries an unsupported column type is a deterministic limitation (Abandon), and
  `TransformError::Infer` is already classified Abandon — symmetric with how a
  *non-empty* transform output of an unsupported type fails via `infer_columns`.
  (Widening the supported type set is the separate `fut-datafusion-type-coverage`.)

- `scan.rs` — register an empty table from a ready Arrow schema:

  ```rust
  /// Register an EMPTY DataFusion table named `name` with `schema` (zero rows) —
  /// the empty-input analog of `scan_table`. Uses `TableReference::bare` to
  /// preserve the registration name verbatim (same as `scan_table`).
  pub fn register_empty_table(
      ctx: &SessionContext, name: &str, schema: SchemaRef,
  ) -> Result<(), ScanError> {
      let provider = MemTable::try_new(schema, vec![])?; // zero batches
      ctx.register_table(TableReference::bare(name), Arc::new(provider))?;
      Ok(())
  }
  ```

`run_transform` step 1, per input, becomes:

```rust
let snapshot = cp.catalog().current_snapshot(input.table).await
    .map_err(|e| unknown_input(input.table, e))?;
let files = cp.catalog().files(input.table, snapshot.id, PageReq::unbounded()).await
    .map_err(|e| unknown_input(input.table, e))?;
if files.items.is_empty() {
    let ts = cp.catalog().schema(input.table, snapshot.id).await
        .map_err(|e| unknown_input(input.table, e))?;
    let columns: Vec<ColumnSpec> = ts.columns.into_iter()
        .map(|c| ColumnSpec { name: c.name, ty: c.ty, nullable: c.nullable }).collect();
    let schema = datafusion_io::logical_arrow_schema(&columns)?;   // InferError -> Abandon
    datafusion_io::register_empty_table(&ctx, input.register_as, schema)?;
} else {
    scan_table(&ctx, store.clone(), input.register_as, input.table, &files.items).await?;
}
```

Nullability and column order come straight from `TableSchema` (column-ordered),
so the empty relation's schema matches what a non-empty scan of the same table
would expose — the SQL behaves identically whether the input happens to be empty.

## What this does NOT change

- The retry-policy table itself (`handler.rs`) is **unchanged** — the fix is that
  `run_transform` now produces the *correct error variant*, which the existing
  policy already classifies correctly (`UnknownInput`/`Infer` → Abandon).
- The non-empty scan path (`scan_table`) is untouched.
- No new logical types are supported; the empty-input schema builder covers exactly
  the five types the transform read/write path already round-trips.
- Output/commit/lineage/conformance logic is unchanged.

## Testing

All tests are `rust_test` integration targets (no inline `#[cfg(test)]`).

- **Edge 1 — missing input at `files` → `UnknownInput`** (`transform`
  `tests/run_unknown_input.rs`, a **plain `rust_test`** — no DB fixture):
  a minimal `ControlPlane` double whose `catalog()` returns a fake `Catalog` with
  `current_snapshot → Ok(snapshot)` and `files → Err(NotFound)` (the drop-race the
  real fixture can't deterministically produce); its other accessors
  (`ontology`/`acl`/`lineage`/`queue`/`begin`) and the fake catalog's
  `schema`/`snapshots` are `unreachable!()` (never reached — the error precedes
  them). Drive `run_transform` with one input over an `InMemory` object store and
  assert the result is `Err(TransformError::UnknownInput(..))` (so the policy
  Abandons it). Pin the classification too: assert
  `retry_policy(&TransformError::UnknownInput("s".into(),"t".into()), 0)` is
  `RetryPolicy::Abandon` (a one-line guard; currently true).

- **Edge 2 — empty input runs the transform** (`transform` `tests/transform_e2e.rs`
  or a sibling `loom_fixture_test`, Postgres + DuckLake): create an input table
  with a schema but **no files** (e.g. `create_table` then no append — a
  current_snapshot exists, `files` is empty, `schema` resolves), then
  `run_transform` a SQL that reads it. Assert two cases:
  - `SELECT count(*) AS n FROM input` commits a snapshot whose single row is `0`
    (the empty input was registered as an empty relation, not a scan error);
  - `SELECT * FROM input` commits an empty output (zero record_count across files)
    rather than returning `Err`.
  Confirm neither returns a `Scan`/Retry error.

- **`logical_arrow_schema` unit** (`datafusion-io` `tests/…`, plain `rust_test`):
  the five supported types map to their Arrow `DataType`s in column order with
  nullability preserved; an unsupported type yields `InferError::Unsupported`.
  (Round-trip against `infer_columns` for the supported set.)

- **Existing transform e2e / conform / compact / output-mode tests stay green.**

## Files

- Modify: `src/services/transform/src/run.rs` — add the `unknown_input` helper and
  apply it to the `current_snapshot`/`files`/`schema` reads; branch on
  `files.items.is_empty()` to register an empty relation.
- Modify: `src/services/datafusion-io/src/infer.rs` — add `logical_arrow_type` and
  `logical_arrow_schema`.
- Modify: `src/services/datafusion-io/src/scan.rs` — add `register_empty_table`
  (needs `MemTable`; it is in the already-depended `datafusion` crate). Re-export
  both new fns from `datafusion-io`'s `lib.rs`.
- Create: `src/services/transform/tests/run_unknown_input.rs` (+ a plain `rust_test`
  target) and the `datafusion-io` `logical_arrow_schema` unit test (+ target);
  extend `src/services/transform/tests/transform_e2e.rs` (existing
  `loom_fixture_test`) with the empty-input cases.
- Modify: `docs/ISSUES.md` — close `iss-transform-read-edge-cases`
  (`[x] status:fixed pr:#<n>`); it points at this design.
- Core (`src/control-plane/core/`) is **untouched** (`Catalog::schema` already
  exists); no dependency/lockfile change.
