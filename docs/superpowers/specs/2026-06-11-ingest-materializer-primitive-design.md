# Design: ingest materializer primitive (Step 3, ingest — part 2a)

> **Status:** approved design (2026-06-11). Second sub-project of **Step 3 (ingest)**,
> building directly on the part-1 snapshot-commit primitive
> (`2026-06-09-ingest-snapshot-commit-primitive-design.md`). This spec defines loom's
> **landing-edge** ingest: turning abstract data into a registered DuckLake snapshot.

## Goal

Give loom the **landing materializer**: take data in hand (Arrow `RecordBatch`es), write a
Parquet file, extract the catalog statistics, put the file in object storage, and register
it as a DuckLake **snapshot + lineage** via the part-1 primitive — optionally validated
against a declared model first. This is the piece that makes loom *actually persist data
DuckDB can read back*, closing the gap between the committed catalog primitive and a running
ingest path.

loom stays a **native DuckLake writer**: the materializer produces the Parquet and the
`DataFile` metadata, then hands it to part-1's `Tx::create_table` + `append_files` + `emit` +
`commit` as **one atomic Postgres transaction**. The control-plane library keeps its
register-only boundary — Parquet and object storage live only in this new service crate.

## Two layers: inference at the landing edge, the ontology authoritative at retrieval

The design separates **where data lands** from **how it is retrieved**, so schema inference
and ontology authority never conflict — they sit at different stages:

- **Landing (this slice).** Abstract data → loom infers a physical DuckLake schema (Arrow →
  DuckLake types) → Parquet → snapshot + lineage. The output is a *backed dataset*: a real
  DuckLake table with an inferred schema. **No ontology type is required to land data.**
- **Model (the ontology — authoritative, later slice).** The typed object model the query
  front-end retrieves *through*. Landed data is not queryable-as-a-type until **bound** to a
  model. That binding — promoting a landed dataset into an authoritative `ObjectType` the FE
  serves — is the **next** sub-project and is explicitly out of scope here.

This mirrors the Foundry shape: raw **datasets** → **ontology objects**. Inference is
necessary at the landing edge; the ontology stays authoritative at the retrieval edge.

## The optional model-conformance gate ("this data is this model")

To make space for governed, typed ingest without pulling the full binding into this slice,
the materializer takes an **optional model-conformance gate** at its front edge:

- **No model supplied** → land abstract data with the **inferred** schema (raw dataset).
- **Model supplied** ("this data *is* `Customer`") → **validate the batch against the model
  before anything is written**, then create the table from the **model's** columns. The model
  is authoritative; inference is not consulted for the physical type.

The gate takes a plain **`ModelShape`** value (column names + DuckLake types + required), **not
the ontology** — so the materializer crate stays ontology-free. The next slice derives a
`ModelShape` from an `ObjectType`. This slice ships the *seam* plus a **minimal conformance
check** (required columns present, types compatible); richer on-the-fly constraints (ranges,
regex, nullability beyond required, coercion) are a documented extension point, not built.

## Architecture & boundary

**New crate `src/services/ingest/`** (mirrors `src/services/query-api/`). Depends on:
- `control-plane-core` — the `Tx` seam and the `ColumnSpec` / `ColumnStat` / `DataFile` /
  `TableRef` / lineage types.
- `control-plane-postgres` — the concrete `ControlPlane` to `begin()` a transaction.

The control-plane crates gain **no** Parquet / object-store dependency; those live only in
this crate, preserving the part-1 register-only boundary.

**Library only this slice** — no network endpoint, no service binary wiring, no DataFusion.
The materializer is a function over Arrow batches. (The ingest endpoint + binary, and
DataFusion compute, are later sub-projects.)

### Pipeline

```
abstract data (Arrow batches) ─┐
target TableRef ───────────────┤
optional &ModelShape ──────────┘
        │
        ▼
  [ gate.validate ]  ── reject (DoesNotConform) BEFORE any write
        │
        ▼
  schema: infer (no model) | model.columns (model wins)
        │
        ▼
  write Parquet + extract DataFile stats        ← load-bearing fidelity unit
        │
        ▼
  object_store.put (LocalFileSystem now; S3 later)
        │
        ▼
  Tx: create_table(schema, idempotent) + append_files(DataFile)
      + emit(lineage: output dataset = TableRef) + commit → SnapshotId
```

## Components

Five focused, independently testable units.

### 1. `infer` — Arrow schema → DuckLake columns (pure)

```rust
fn infer_columns(schema: &arrow::datatypes::Schema) -> Result<Vec<ColumnSpec>, InferError>
```

A table-driven Arrow-`DataType` → DuckLake-type-string map (`Int64`→`"int64"`,
`Utf8`/`LargeUtf8`→`"varchar"`, `Boolean`→`"boolean"`, `Float64`→`"double"`, …). Returns
`InferError::Unsupported(DataType)` on an unmapped Arrow type rather than guessing. Nullable
flag taken from the Arrow field. Used **only** on the un-modeled landing path.

### 2. `gate` — the optional model-conformance seam

```rust
pub struct ColumnShape { pub name: String, pub ty: String, pub required: bool } // ty = DuckLake string
pub struct ModelShape  { pub columns: Vec<ColumnShape> }
pub struct Violation   { pub column: String, pub reason: ViolationReason }
pub enum ViolationReason { MissingRequired, TypeMismatch { expected: String, found: String } }

fn validate(shape: &ModelShape, batch: &arrow::datatypes::Schema) -> Result<(), Vec<Violation>>;
```

Minimal check this slice: every `required` column is present in the batch, and each present
column's inferred DuckLake type matches the model's. `Violation` / `ViolationReason` are the
documented extension point for later constraints (ranges, regex, coercion). When a model is
supplied, the **model's** `columns` (not the inferred schema) become the physical schema for
`create_table`.

### 3. `write` — Arrow batches → Parquet + stats (load-bearing fidelity unit)

```rust
pub struct WrittenParquet {
    pub bytes: Vec<u8>,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub footer_size: i64,
    pub column_stats: Vec<ColumnStat>,   // core::ColumnStat
}
fn write_parquet(schema, batches) -> Result<WrittenParquet, WriteError>;
```

Writes with `parquet::arrow::ArrowWriter`, then reads back the Parquet `FileMetaData` to
extract exactly what `append_files` demands: per-column `min` / `max` / `null_count` /
`value_count` (= non-null count) / `column_size_bytes`, plus `footer_size` and the total
`file_size_bytes`. Stats are **string-encoded to match DuckLake's VARCHAR stat dialect** —
the encoding the part-1 recipe (`2026-06-09-ducklake-single-catalog-write-recipe.md`) already
pins; the interop guardrail is the executable oracle that confirms the encoding round-trips
through the DuckDB engine. Compression is **Snappy** (see Dependencies).

### 4. `store` — object-store put

```rust
pub struct StoredPath { pub path: String, pub path_is_relative: bool }
async fn put(store: &dyn object_store::ObjectStore, data_path: &str, bytes: Vec<u8>)
    -> Result<StoredPath, StoreError>;
```

`object_store` crate; `LocalFileSystem` for now (no credentials → hermetic tests). Generates
the data file's path under the catalog's `data_path` and returns it with `path_is_relative`
set for resolution against `ducklake_metadata.data_path`. S3 (the `aws` feature) is a later
slice.

### 5. `materialize` — the orchestrator

```rust
pub async fn materialize(
    cp: &dyn ControlPlane,
    store: &dyn object_store::ObjectStore,
    table: &TableRef,
    batches: &[arrow::record_batch::RecordBatch],
    gate: Option<&ModelShape>,
    lineage_src: &LineageSource,        // minimal: enough to emit an output-dataset event
) -> Result<SnapshotId, IngestError>;
```

Sequences gate → schema-selection → `write` → `put` → one atomic Tx
(`create_table` + `append_files` + `emit` + `commit`). Owns the put-then-commit ordering and
maps every step's failure to a typed `IngestError`.

## The authoritative-schema rule

The single rule that reconciles "inference is allowed" with "the ontological model is
authoritative":

- **No model** → `infer_columns` → `create_table` from the **inferred** columns.
- **Model supplied** → `validate` the batch → `create_table` from the **model's** columns.

Inference governs only un-modeled landing; the moment a caller says "this data is this model,"
the model wins for the physical schema. Lineage emitted by `materialize` references the landed
`TableRef` as its output dataset, so a future binding step has a first-class table to point a
type at — making "at some point it goes into a model" a pure addition, not a rework.

## Error handling

`IngestError`, fail-fast, no partial state:

- **Gate rejection** → `DoesNotConform(Vec<Violation>)` *before* any write.
- **Unsupported Arrow type** (un-modeled path) → `UnsupportedType` before any write.
- **Write / put failure** → `Write` / `Store`; `commit` is never reached → zero catalog rows.
- **Commit** is part-1's atomic unit: snapshot + `ducklake_*` + lineage land together or not
  at all.
- **Orphaned Parquet — the one accepted imperfection.** Ordering is **put → commit** (the path
  is needed to register the file). If `put` succeeds and `commit` then fails, the Parquet bytes
  are orphaned: the catalog never references them. This is the *lesser evil* — the alternative
  ordering would leave a catalog row pointing at a missing file, corrupting reads. Orphans are
  harmless and reclaimable; **orphaned-Parquet GC is already a deferred roadmap concern**, so
  this slice documents the orphan and does not address it.

## Testing

- **Unit:** the infer map (every supported Arrow type → DuckLake string; unsupported → error);
  stats extraction (a known batch → expected `min`/`max`/`null_count`/`value_count`/sizes);
  gate accept/reject (missing-required, type-mismatch).
- **Interop guardrail (make-or-break, same shape as part-1).** `materialize` a batch into a
  hermetic DuckLake-on-Postgres catalog (`PgFixture` + a `LocalFileSystem` temp dir for data),
  then the **pinned DuckDB CLI** (`:duckdb-cli` + `:duckdb-extensions`) `ATTACH`es the catalog
  and `SELECT`s — asserting it reads back **exactly** the rows loom landed **and can append its
  own snapshot on top** (proving Parquet + stats + counters are engine-faithful). This catches
  catalog/format drift on a DuckDB bump. Runs as a `loom_fixture_test(duckdb = True)` so the
  test command routes local (real `duckdb`/`postgres` refuse to run as root on RE).
- **Atomicity:** force a `commit` failure after `put` → assert no snapshot/lineage rows exist
  (the orphaned Parquet is the only residue, as documented).
- Whole suite via `buck2 test //src/...`; fixture tests use `loom_fixture_test`, never a bare
  `rust_test`.

## Dependencies

Imported via `./tools/buckify.sh` + a lockfile refresh:

- **`arrow 58.3.0`** — already buckified; depend on it directly.
- **`parquet 58.3.0`** — **`default-features = false`, features `["arrow", "snap"]`.**
  Deliberately *off*: `zstd` / `lz4` / `brotli`. Snappy is pure-Rust (`snap`), so loom avoids
  the `*-sys` C build-script crates entirely — sidestepping the RE native-dependency risk hit
  with bundled DuckDB. DuckDB reads Snappy Parquet without issue.
- **`object_store`** — **local feature only** (no `aws` / `gcp` / `azure`, which pull
  `reqwest` / `hyper`); the cloud features turn on in a later slice.
- **Build on RE before merge** to confirm no native-dependency surprise, per the standing rule.

**No DataFusion.** The materializer is `arrow` + `parquet` + `object_store` + the part-1
primitive; DataFusion is deferred until there is actual compute (transforms) to justify its
weight.

## Verification

- `buck2 test //src/...` green; the interop guardrail passes (loom-landed snapshot read by the
  pinned DuckDB, and appended onto); the atomicity test passes.
- `tools/clippy-all.sh` clean; `buck2 run //tools:prek -- run --all-files` green (incl.
  `reindeer-check` after the new deps land).
- The new third-party rules build on **RE**, not just locally.

## Scope / non-goals

- **In:** the `src/services/ingest` crate; the five units (`infer`, `gate`, `write`, `store`,
  `materialize`); the optional model-conformance gate (seam + minimal check); the
  authoritative-schema rule; the `parquet` + `object_store` imports (C-free); the DuckDB
  read-back interop guardrail; the atomicity test.
- **Out (later sub-projects):** the **dataset→model binding** (promote a landed dataset into an
  authoritative `ObjectType` the FE serves); the ingest **network endpoint** + service binary +
  config/pool wiring; **DataFusion** compute; **S3** object store; rich gate constraints
  (ranges/regex/coercion); schema evolution; delete/compaction; orphaned-Parquet GC.

## Open risks

- **Parquet/stats fidelity** — loom's `parquet`-crate output and string-encoded stats must be
  what the DuckLake engine expects. Mitigated by the source-grounded part-1 recipe and the
  DuckDB read-back interop test as the executable oracle.
- **Native-dependency drift on RE** — mitigated by the C-free dep set (Snappy-only Parquet,
  local-only object_store) and an explicit RE build before merge.
- **Dataset→model binding is deferred but real** — it is the next slice and shapes how a landed
  dataset becomes a typed, queryable object; flagged here so the seam (`ModelShape`, lineage
  referencing the landed `TableRef`) is built to make that addition clean rather than a rework.
