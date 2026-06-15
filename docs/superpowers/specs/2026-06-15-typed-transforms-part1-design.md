# Design: Typed transforms — part 1 (Object-Model `Type(s) → Type`)

> **Status:** approved design (2026-06-15). The next slice of loom's **Transform workers** pillar
> and the first time loom *derives data in the ontology's own vocabulary*. It wraps `resolve` +
> conformance validation around the physical `run_transform` primitive shipped in
> [transform workers part 1](2026-06-06-loom-roadmap.md): a job names input **types**, an output
> **type**, and a SQL query written in type terms; a worker resolves each input type to its
> DuckLake table, runs the SQL, **validates the result conforms to the output type's property
> contract**, and commits a new snapshot + lineage — atomically.

## Goal

Make loom run **typed transforms**: a job names input ontology **type(s)**, one output **type**,
and a SQL query that references the inputs **by type name**; a worker resolves each input type to
its backing DuckLake table, registers it in DataFusion under the type name, runs the SQL,
**validates that the result exactly conforms to the output type's declared properties**, and — only
if it conforms — writes the result as a new snapshot of the output type's backing table with
lineage, atomically.

End-to-end: **enqueue a typed transform → worker dequeues → resolves input types → computes →
validates against the output Object Model → new snapshot + lineage → the result reads back through
query-api as that typed object.**

This is the Object-Model layer the physical primitive was built to carry. Its genuinely-new piece
is **conformance**: the typed layer guarantees a derived dataset actually matches the Object Model
it claims to produce, before anything is committed.

## North star (context, not part-1)

A transform is ultimately **Object Model(s) in → Object Model(s) out**, with two authoring models
(SQL **and** programmatic). This slice delivers `Type(s) → Type` (multi-input, single-output) over
SQL. Multi-*output*, programmatic authoring, and overwrite/incremental are explicit follow-on
slices that compose the same primitives.

## Decisions (settled in brainstorming)

- **A — Output type pre-exists.** The output `ObjectType` (properties + intended backing
  `TableRef`) is declared up front via `ontology.define_type`; the transform *materializes
  instances* of it. The type's backing table need **not** exist yet — `run_transform`'s idempotent
  `create_table` brings it into being on first run. Type *authoring* is a separate action, not
  folded into the transform.
- **B — SQL references inputs by type name.** Each input type is resolved and registered in
  DataFusion under its **type name**, so SQL reads e.g. `SELECT ... FROM Customer JOIN "Order" ...`.
  Object-Model ergonomics; no physical table names leak into authored SQL.
- **C — Multi-input → single output type.** N input types joined by one SQL statement → one result
  set → one output type. Mirrors the physical `run_transform` (single-output) exactly. Multi-output
  is a clean follow-on.
- **D — Exact-match conformance.** The result columns must be **exactly** the output type's
  properties: every property has a same-named result column whose inferred physical type
  `satisfies` its logical type; a `required` property's column must be non-nullable; and there are
  **no extra columns**. Guarantees every run produces an identical, append-compatible schema and a
  faithful materialization of the Object Model.

## What this slice IS

- A new typed primitive **`run_typed_transform`** in the `transform` crate.
- Two **shared seams on `run_transform`** so the physical and typed paths run through one
  orchestration: a `register_as` input mapping (enabling type-name SQL) and an optional output
  **conformance contract**.
- A **`conform` module**: exact-match validation of an inferred result schema against an
  `ObjectType`'s properties, reusing core's `satisfies` affinity check.
- A new queue **job kind `"typed-transform"`**, its handler, and worker dispatch alongside the
  existing physical `"transform"` kind.
- **Graph-connected lineage** with the type-level relationship recorded in the event payload.

## What this slice is NOT

- **No multi-output types** (one job → several output types). Follow-on.
- **No programmatic transforms** (registered Rust / logical-plan authoring). Follow-on; swaps only
  the compute step.
- **No type authoring inside the transform** — the output type pre-exists (Decision A).
- **No overwrite / incremental** output — append only, inherited from `run_transform`.
- **No first-class type-named lineage nodes** — see §5; deferred as an additive enhancement,
  consistent with `identity.rs`.
- **No consolidation** of the two violation enums (`ingest::BindViolation` and this slice's
  conformance `Violation`) into core — noted follow-up.

## Design

### 1. The typed primitive — `run_typed_transform`

```rust
pub async fn run_typed_transform(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    run_id: &str,
    inputs: &[TypeName],
    output: &TypeName,
    sql: &str,
) -> Result<SnapshotId, TypedTransformError>;
```

Flow:
1. **Resolve inputs.** For each input `TypeName`: `cp.ontology().resolve(type)` → `TableRef`
   (`ControlPlaneError::NotFound` → `TypedTransformError::UnknownType`). Build a
   `TransformInput { table, register_as: <type name> }` per input.
2. **Resolve output contract.** `cp.ontology().get_type(output)` → `ObjectType { properties,
   table }` (NotFound → `UnknownType`). `properties` is the conformance contract; `table` is the
   output backing table.
3. **Build lineage** (§5): inputs → output, graph-connected via the resolved backing tables, with
   the input/output **type names + SQL** in the payload.
4. **Delegate** to `run_transform` with the input specs, `output.table`, `sql`,
   `conform: Some(&properties)`, and the lineage. Map any `TransformError` through
   `TypedTransformError::Transform`.

The typed primitive owns **no** scan/SQL/write/commit logic of its own — it resolves types, builds
the contract + lineage, and calls the shared orchestration. That keeps the derivation atomic
(validation aborts before any write) without duplicating the skeleton.

### 2. Shared seams on `run_transform`

Two additive changes let physical and typed paths share one orchestration:

**(a) `register_as` input mapping.** `TransformRequest.inputs` becomes
`&'a [TransformInput<'a>]`:

```rust
pub struct TransformInput<'a> {
    pub table: &'a TableRef,
    pub register_as: &'a str, // the DataFusion-visible table name for this input
}
```

The scan loop registers each input under `register_as` (was: `input.name`). The existing
same-name guard now keys on `register_as` (two inputs registering under the same SQL name is still
`AmbiguousInput`). The **physical** path passes `register_as = &input.table.name` (behavior
unchanged); the **typed** path passes the type name.

**(b) Optional conformance contract.** `TransformRequest` gains
`pub conform: Option<&'a [PropertyDef]>`. In `run_transform`, **after** `infer_columns(&schema)`
and **before** `write_dataset`: if `conform.is_some()`, run `check_conformance` (§3); on violation
return `TransformError::DoesNotConform(Vec<Violation>)` immediately — **no `write_dataset`, no
`Tx`**. When `None` (physical path), behavior is exactly as today.

`TransformError` gains one variant: `DoesNotConform(Vec<conform::Violation>)` (deterministic →
Abandon). All existing variants and call-site behavior are otherwise unchanged.

### 3. Conformance check — `transform::conform`

```rust
pub enum Violation {
    MissingColumn { property: String, logical: String },
    TypeMismatch { property: String, logical: String, physical: String },
    UnknownLogicalType { property: String, logical: String },
    NullabilityViolation { property: String }, // required property over a nullable column
    UnexpectedColumn { column: String },        // result column with no matching property
}

/// Exact-match: result columns must be precisely the type's properties.
pub fn check_conformance(
    result: &[ColumnSpec],          // inferred from the SQL result (datafusion_io::infer_columns)
    properties: &[PropertyDef],     // the output ObjectType's contract
) -> Result<(), Vec<Violation>>;
```

Rules (collect **all** violations, never short-circuit — mirrors `ingest::bind`):
- Every `property` has a same-named `result` column, else `MissingColumn`.
- For a matched column, `satisfies(property.ty, column.ty)`:
  - `Ok(true)` → fine; `Ok(false)` → `TypeMismatch`; `Err(UnknownLogicalType)` →
    `UnknownLogicalType`.
- A matched column for a `required` property must have `nullable == false`, else
  `NullabilityViolation`.
- Every `result` column must correspond to some property, else `UnexpectedColumn` (the exact-match
  half — this is what `ingest::bind`'s view-semantics does *not* enforce).

Pure logic; no I/O. `Violation` is local to the `transform` crate — a deliberate parallel to
`ingest::BindViolation`; consolidating both into `control-plane-core` is a noted follow-up, kept
out of this slice to avoid touching the ingest path.

### 4. Handler + binary dispatch

- **Payload** (`"typed-transform"` job kind):
  ```jsonc
  { "inputs": ["Customer", "Order"], "output": "OrderEnriched", "sql": "SELECT ..." }
  ```
  i.e. `{ inputs: Vec<String>, output: String, sql: String }` — type names, not `{schema,name}`.
- **`typed_transform_handler(cp, store, job)`**: deserialize → map `Vec<String>`/`String` to
  `TypeName`s → `run_typed_transform` → map outcome to `JobFailure` (§6). Malformed payload →
  Abandon, as in the physical handler.
- **Binary dispatch.** The worker loop runs one handler closure for a set of kinds. The closure
  branches on `job.kind`: `"transform"` → existing `transform_handler`, `"typed-transform"` →
  `typed_transform_handler`. `worker.run(&["transform", "typed-transform"], shutdown, dispatch)`.
  No new binary, no HTTP surface — still queue-driven.

### 5. Lineage — graph-connected, type-level in the payload

`identity.rs` already states the intent: *"An ontology type reaches its dataset through
`ObjectType.table -> DatasetId`; a type-level variant is an additive change if type-level lineage
lands."* So for this slice:

- **`inputs`/`outputs` are table-identity `DatasetRef`s** — built from the resolved backing
  `TableRef`s via the existing `From<&TableRef> for DatasetRef` (loom namespace, `schema.name`).
  This keeps the provenance graph **connected** end-to-end: ingest landing, physical transforms,
  and typed transforms all reference the same dataset identities, so `upstream`/`downstream` answer
  correctly across all three.
- **The type-level relationship lives in `payload`**: the input type names, the output type name,
  and the SQL. This satisfies "lineage records input types → output type" while preserving graph
  connectivity.
- A **first-class type-named `DatasetRef`** (its own namespace, name = type) is the *additive*
  enhancement `identity.rs` anticipates — explicitly **deferred**, because emitting type-named
  nodes today would fragment the graph from the table-named nodes ingest already emits.

`EventType::Complete`, a fresh `RunId`, mirroring the physical handler.

### 6. Error → retry policy

`TypedTransformError`:
```rust
pub enum TypedTransformError {
    UnknownType(String),                 // resolve/get_type NotFound
    Transform(TransformError),           // includes DoesNotConform(..)
}
```
Map to `JobFailure`:
- **Deterministic → `Abandon`**: `UnknownType`; `Transform(DoesNotConform)`; and the existing
  deterministic `TransformError` arms (`UnknownInput`, `AmbiguousInput`, `DataFusion`, `Infer`,
  `NoSnapshot`).
- **Transient → `Retry { delay }`**: `Transform(ControlPlane | Scan | Write)`, with the existing
  bounded `2^attempts` backoff.

The handler owns the classification; the worker's `catch_unwind` still backstops a panicking
handler (→ Abandon).

### 7. Testing

- **`conform` unit tests** (pure, `tests/conform.rs`): exact-match pass; `MissingColumn`;
  `TypeMismatch`; `UnknownLogicalType`; `NullabilityViolation`; `UnexpectedColumn`; and a
  multi-violation case proving collection (not short-circuit).
- **`run_transform` regression**: the existing physical transform e2e is the guard that the
  `register_as` + `conform: None` refactor left physical behavior unchanged.
- **Typed e2e fixture** (`loom_fixture_test`, Postgres + DuckDB):
  1. Land two input tables via ingest `materialize` — `main.customers(id, region)` and
     `main.orders(id, customer_id, amount)`.
  2. `ontology.define_type` for `Customer` and `Order` (bound to those tables), **and** for the
     output type `OrderEnriched` — its properties (e.g. `id: Long`, `region: String`,
     `amount: Double`) and an intended backing `TableRef` (`main.order_enriched`) **whose table
     does not exist yet**.
  3. Enqueue a `"typed-transform"` job with **type-name** join SQL producing exactly those columns.
  4. Run the worker for one job.
  5. **Assert:** the output table has a new snapshot; rows are the correct join; the lineage event
     references the input and output **dataset identities** (graph-connected) with the **type
     names + SQL in its payload**; and the result **reads back through query-api as `OrderEnriched`
     typed objects** (the Object Model round-trips end to end).
  6. **Negative case:** a second job whose SQL omits a required property (or adds an extra column)
     fails with `DoesNotConform` and **commits nothing** (output table unchanged / still absent).

### File structure

- **Create:** `src/services/transform/src/conform.rs` (validation), `src/services/transform/src/typed.rs`
  (`run_typed_transform`, `TypedTransformError`); tests `tests/conform.rs`, `tests/typed_transform_e2e.rs`.
- **Modify:** `src/services/transform/src/run.rs` (`TransformInput`, `register_as` scan,
  `conform` hook, `TransformError::DoesNotConform`); `src/services/transform/src/handler.rs`
  (`typed_transform_handler`); `src/services/transform/src/main.rs` (kind dispatch);
  `src/services/transform/src/lib.rs` (module wiring + exports); `src/services/transform/BUCK`
  (new modules, test targets).
- **Modify:** `docs/superpowers/specs/2026-06-06-loom-roadmap.md` (typed transforms part-1
  delivered).

## Follow-ups (later slices)

- **Multi-output typed transforms** (`Type(s) → Type(s)`): multi-statement payloads, per-output
  conformance, atomic multi-table commit.
- **Programmatic transforms**: a registered-plan authoring model swapping only the compute step.
- **First-class type-named lineage** (the additive `identity.rs` variant) — once the graph model
  decides how type-named and table-named nodes coexist.
- **Overwrite / incremental** output (with the compaction / file-supersession slice).
- **Consolidate violation vocabularies** (`ingest::BindViolation` + `transform::conform::Violation`)
  into `control-plane-core`.

## Roadmap

Lands under Step 3 → **Transform workers**, typed part-1 — the first Object-Model-level derivation,
turning the physical primitive into one that speaks the ontology's vocabulary and guarantees its
outputs conform to the Object Models they claim to produce.
