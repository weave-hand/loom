# Design: Transform workers — part 1 (queue-driven SQL transform)

> **Status:** approved design (2026-06-14). The first slice of loom's third service pillar —
> **Transform workers** — and the first time loom *derives* data rather than only landing and
> serving it. A queue-driven worker reads existing DuckLake table(s) with DataFusion, runs a SQL
> computation, and commits the result as a new DuckLake snapshot + lineage, atomically. It builds
> on everything already shipped: the `control-plane-worker` loop, the queue, the snapshot-commit
> `Tx`, and the DataFusion write path from the ingest slice.

## Goal

Make loom run **transforms**: a job names input table(s), an output table, and a SQL query; a
worker resolves the inputs' current DuckLake snapshots, has DataFusion read their Parquet, runs
the SQL, and writes the result as a new snapshot of the output table — emitting lineage
(inputs → output) in the same transaction. End-to-end: **enqueue a transform → worker dequeues →
computes → new snapshot + lineage → result is readable.**

This is the load-bearing primitive. Its genuinely-new piece is the **read** path — DataFusion
reading a DuckLake table (today only the query-api reads, and it does so via embedded DuckDB).
Everything downstream (typed/object-model transforms, programmatic transforms) layers on top
without new engine work.

## North star (context, not part-1)

A transform is ultimately **Object Model(s) in → Object Model(s) out** — inputs and outputs named
as ontology *types*, with two authoring models (SQL **and** programmatic). That is reachable by
composing primitives loom already has: resolve an input type → its table (`Ontology::resolve`),
run the compute, and bind the output table back to a type (`bind`). Part-1 builds the physical
engine underneath that; the typed and programmatic layers are explicit follow-on slices.

## What this slice IS

- A shared **`datafusion-io`** library: the DuckLake↔DataFusion IO layer — `write_dataset`
  (moved out of `ingest`) plus a new **`scan_table`** read path.
- A **`transform`** service: `run_transform` (the primitive), a worker handler, and a runnable
  binary on `service_runtime`.
- **Physical `TableRef` SQL transforms**, **multi-input** (joins), **append** output semantics,
  with **lineage (inputs → output)** committed atomically with the snapshot.

## What this slice is NOT

- **No object-model/typed transforms** (`Type → Type`) — that is the next slice (`resolve` +
  `bind` wrapped around this primitive).
- **No programmatic transforms** (registered Rust / logical-plan API) — a later slice that swaps
  only the compute step.
- **No overwrite / incremental output** — append only. Clean re-run (overwrite) needs file
  supersession, the already-deferred compaction item in `docs/FUTURE.md`.
- **No Ballista**, no DAG/scheduling/auto-enqueue-downstream, no transform-authoring auth.
- A transform reads **raw** DuckLake tables and bypasses ACL/ontology by design — it is trusted
  *pipeline code*; governance reapplies when the output is later read through query-api.

## Design

### 1. Crate structure

**New library `src/services/datafusion-io/`** (crate `datafusion_io`) — the DuckLake↔DataFusion
IO unit, depended on by both `ingest` and `transform`:
- **Moved from `ingest`** (verbatim behavior; renamed where ingest-specific): `write_dataset`,
  `WrittenFile`, `estimate_partitions`, `file_stats_from_bytes`, `WriteError`, and `WriteConfig`
  (renamed from `IngestWriteConfig`); and `infer_columns` (from `ingest::infer`) with its
  `InferError`.
- **New: `scan_table`** (§3).

**`ingest` refactor** (mechanical): depend on `datafusion_io`; replace its `write`/`infer` module
internals with re-exports / direct use of the moved symbols; update `IngestWriteConfig` →
`WriteConfig` at its call site (`materialize.rs`); move the relocated unit tests
(`tests/write.rs`) to `datafusion-io`, leaving ingest's behavior unchanged (the ingest fixture
tests are the regression guard).

**New service `src/services/transform/`** (crate `transform`): `run_transform`, the worker
handler, `main.rs`.

### 2. The transform primitive — `run_transform`

```rust
pub struct TransformRequest<'a> {
    pub inputs: &'a [TableRef],
    pub output: &'a TableRef,
    pub sql: &'a str,
    /// Built by the caller (it knows the datasource namespace); inputs → output.
    pub lineage: LineageEvent,
}

pub async fn run_transform(
    cp: &dyn ControlPlane,
    store: Arc<dyn ObjectStore>,
    req: TransformRequest<'_>,
) -> Result<SnapshotId, TransformError>;
```

Flow:
1. Build a per-call DataFusion `SessionContext`; register the loom object store.
2. For each `input` in `req.inputs`: `catalog.current_snapshot(input)` → `catalog.files(input,
   snapshot)` → `scan_table(...)` registers it as a DataFusion table named `input.name`.
3. `ctx.sql(req.sql)` → collect → result `Vec<RecordBatch>` + Arrow `Schema`.
4. `infer_columns(schema)` → `Vec<ColumnSpec>` (the output's physical columns).
5. `write_dataset(store, "<out.schema>/<out.name>/<run-id>", schema, batches, WriteConfig::default())`
   → `Vec<WrittenFile>` → `Vec<DataFile>`.
6. One `Tx`: `create_table(output, columns)` (idempotent) + `append_files(output, data_files)` +
   `emit(req.lineage)` + `commit` → `SnapshotId`.

`<run-id>` is a caller-unique prefix (a UUID), mirroring ingest's `file_prefix`.

### 3. The read path — `scan_table` (the new infrastructure)

```rust
pub async fn scan_table(
    ctx: &SessionContext,
    store: Arc<dyn ObjectStore>,
    name: &str,            // the SQL-visible table name (the input's name)
    table: &TableRef,
    files: &[FileRef],     // from catalog.files(table, snapshot)
) -> Result<(), ScanError>;
```

It reconstructs each `FileRef.path` to its object-store key and registers the Parquet files as a
single DataFusion table under `name` (a `ListingTable`/Parquet source over the resolved file
list, on the registered loom object store), so `ctx.sql` can reference `name`.

**Key implementation risk / Decision A — path reconstruction.** `FileRef.path` is
DuckLake-relative. Part-1 reconstructs the object-store key as **`"<table.schema>/<table.name>/"
+ FileRef.path`**, consistent with how the ingest write path lays files out
(`data_path/<schema>/<table>/<prefix>/part.parquet`, registering `DataFile.path =
"<prefix>/part.parquet"`). The end-to-end interop test (§6) is the oracle: it lands real tables
via ingest and reads them back through `scan_table`, so a wrong reconstruction fails loudly. If
DuckLake's stored `schema.path`/`table.path` diverge from that convention, adjust the
reconstruction (and, if necessary, surface the path pieces through the catalog) — this is a
conscious verification, not an unchecked assumption.

### 4. Worker handler + binary

- **Handler** `transform_handler(job: Job) -> Result<(), JobFailure>`: deserialize `job.payload`
  (JSON) into `{ inputs: Vec<TableRef>, output: TableRef, sql: String }`, build the
  `LineageEvent` (inputs → output, `EventType::Complete`, a fresh `RunId`), call `run_transform`,
  and map the outcome (§5). The job `kind` is `"transform"`.
- **Binary** `transform` (`main.rs`): from `service_runtime` config build the control-plane pool
  + the object store, construct `Worker::new(queue, worker_id, lease)`, and
  `worker.run(&["transform"], shutdown, transform_handler).await`. No HTTP surface — it is
  queue-driven. (`service_runtime` already provides the pool/store/config; this binary adds the
  worker-loop wiring rather than an axum router.)

### 5. Error → retry policy

`TransformError` variants map to `JobFailure`:
- **Deterministic** (retrying cannot help) → `RetryPolicy::Abandon`: malformed payload, SQL parse
  error, unknown/missing input table, schema-inference failure.
- **Transient** (may succeed later) → `RetryPolicy::Retry { delay }` with a bounded backoff
  derived from `job.attempts`: control-plane/DB errors, object-store IO, commit conflicts.

The handler owns this classification. The worker's existing `catch_unwind` still backstops a
panicking handler (→ `Abandon`).

### 6. Testing

- **`datafusion-io`:** unit test for `scan_table` (write Parquet to an in-memory/local store →
  register → `ctx.sql("SELECT …")` returns the rows); the relocated `write_dataset` unit tests
  come along unchanged.
- **`transform`:** payload-parse + error-mapping unit tests (pure); and the **load-bearing
  fixture e2e** (Postgres + DuckDB, mirroring the ingest interop oracle):
  1. Land two input tables via ingest `materialize` — e.g. `main.customers(id, region)` and
     `main.orders(id, customer_id, amount)`.
  2. Enqueue a `"transform"` job whose SQL joins them into `main.orders_enriched`.
  3. Run the worker for one job (drive `Worker::run` with an immediate shutdown after one job, or
     call `run_transform` directly *and* a worker-loop variant).
  4. Assert: the output table has a new snapshot; the joined rows are correct; a lineage event
     records `inputs = {customers, orders} → output = orders_enriched`; and the result reads back
     (via `catalog.files` + a DuckDB read of the output, the interop fidelity check).

### File structure

- Create: `src/services/datafusion-io/{BUCK, src/lib.rs, src/write.rs, src/infer.rs, src/scan.rs, tests/…}`
- Modify: `src/services/ingest/{BUCK, src/lib.rs, src/write.rs, src/infer.rs, src/materialize.rs, tests/…}` (extract + redirect)
- Create: `src/services/transform/{BUCK, src/lib.rs, src/main.rs, src/run.rs, src/handler.rs, tests/…}`
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md` (Transform workers part-1 delivered)

## Decisions

- **A — path reconstruction** (§3): `"<schema>/<table>/" + FileRef.path`, verified by the e2e oracle.
- **B — append output semantics**: each run `append_files` to the output table; re-runs
  accumulate. Overwrite/incremental is deferred (compaction/file-supersession).
- **C — read is ungoverned**: transforms read raw tables (trusted pipeline code); ACL/ontology
  reapply on the eventual governed read of the output. Transform-authoring auth is future.

## Follow-ups (later slices)

- **Object-model-typed transforms** (`Type(s) → Type(s)`): resolve input types → tables, run this
  primitive, bind the output table to a type.
- **Programmatic transforms**: a registered-plan authoring model swapping only the compute step.
- **Overwrite / incremental** output (with the compaction/file-supersession slice).
- DAG / transactional enqueue-downstream (the `Tx::enqueue` seam is already present), Ballista
  escalation, transform-authoring auth.

## Roadmap

Lands under Step 3 → **Transform workers**, part-1. It also brings online the long-built but
hitherto-unused `control-plane-worker` loop and the `Tx::enqueue`/lineage seams in a real service.
