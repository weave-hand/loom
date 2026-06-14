# Ingest Slice 3 — DataFusion Ingestion Compute Path

**Date:** 2026-06-14
**Status:** Design (approved for planning)
**Track:** Step 3 → Ingest service shell → DataFusion compute path
**Predecessors:** snapshot-commit primitive (part 1), landing materializer (part 2a),
dataset→model binding (part 2b), ingest service shell (binary + runtime, slices 1 & 2).

## Summary

Replace the direct `arrow::ArrowWriter` write in `src/services/ingest/src/write.rs` with
a **DataFusion-driven write**. Arrow batches flow through a per-call DataFusion
`SessionContext`, get repartitioned to a size-estimated number of partitions, and are
written by DataFusion's Parquet sink as **N Snappy files directly to object storage**.
Per-file DuckLake `DataFile` stats — now with **min/max merged across all row groups** —
are extracted from each file's footer and registered in a single `append_files` call
inside the existing atomic snapshot+lineage transaction.

This makes DataFusion the ingestion engine — the architecture's stated role for it
(`ARCHITECTURE.md`: "bulk ingestion runs on DataFusion … high-throughput Parquet
writes") — and delivers size-targeted, parallel multi-file output. The compute itself is
**identity today** (no casts/projection); the deliverable is the engine seam plus
high-throughput partitioned writes, with a home for future compute.

The DuckDB read-back interop test (`tests/ducklake_interop.rs`) remains the executable
fidelity oracle: real DuckDB must read the DataFusion-produced multi-file table and append
its own snapshot, or the slice is not done.

## Goals

- DataFusion owns the ingest write: `SessionContext` → repartition → `ParquetSink`.
- **Size-targeted multi-file output.** File count derives from an estimated compressed
  size against a configurable target file size (DuckLake/Iceberg-idiomatic).
- **DataFusion writes directly to object storage** (not in-memory buffers), via the same
  `object_store` crate loom already uses.
- **Per-file stats with cross-row-group min/max merge**, preserving DuckDB/DuckLake
  pruning on real multi-row-group files.
- One `append_files` registering all N files; the snapshot+lineage commit stays atomic.
- Interop oracle stays green.

## Non-goals (this slice)

- Real transforms/coercion (model-schema casts, projection) — identity compute only; the
  seam is the deliverable. Coercion is a candidate follow-up.
- S3/remote object-store backend (`LocalFileSystem` only, as today).
- The networked DataFusion endpoint / Quack wire.
- Compaction, delete, schema evolution, orphaned-Parquet GC.
- Distributed execution (Ballista).

## Key constraint: dependency alignment

**DataFusion 54.0.0 pins `arrow ^58.3.0` and `parquet 58.3.0`** — an exact match for
loom's currently vendored arrow/parquet `58.3.0`. Therefore this slice does **not** force
a repo-wide arrow bump (which would ripple through query-api, control-plane, and the
DuckDB interop). We add DataFusion 54 and it aligns. If a future arrow bump is needed, it
becomes its own cross-cutting slice.

## Architecture & components

### `write` module (rewritten) — `src/services/ingest/src/write.rs`

New public surface:

```rust
pub struct IngestWriteConfig {
    pub target_file_size_bytes: u64,   // default ~128 MiB
    pub max_files: usize,              // upper clamp on partition count
    pub compression_factor: f64,       // in-memory → estimated-compressed, default ~0.3
}
impl Default for IngestWriteConfig { /* sane defaults above */ }

/// A written Parquet file plus the metadata `append_files` registers.
pub struct WrittenFile {
    pub path: String,             // relative DataFile path, e.g. "<prefix>/part-0.parquet"
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub footer_size: i64,
    pub column_stats: Vec<ColumnStat>,
}

pub async fn write_dataset(
    store: Arc<dyn ObjectStore>,
    dir_prefix: &str,             // "<schema>/<table>/<file_prefix>"
    schema: Arc<Schema>,
    batches: &[RecordBatch],
    config: &IngestWriteConfig,
) -> Result<Vec<WrittenFile>, WriteError>;
```

`WrittenFile` is the renamed, per-file, path-bearing successor to today's
`WrittenParquet`.

### Flow inside `write_dataset`

1. Build a per-call `SessionContext`. Register `store` into its `RuntimeEnv` under a loom
   object-store URL (e.g. `loom://data/`).
2. Register `batches` as an in-memory `MemTable`.
3. **Estimate** compressed bytes:
   `est = (Σ RecordBatch::get_array_memory_size()) * compression_factor`.
   `partitions = clamp(ceil(est / target_file_size_bytes), 1, max_files)`.
   This is a **pure function**, unit-tested without I/O.
4. `df.repartition(Partitioning::RoundRobinBatch(partitions))?` then `write_parquet` to
   `dir_prefix` with Snappy compression. DataFusion writes one file per partition → N
   files. Repartition is deterministic given input batch order.
5. **List** `dir_prefix` to discover the written files (do not assume DataFusion's file
   names; list + sort).
6. For each file, read **only the Parquet footer** via an object-store suffix range-get
   (`ParquetMetaDataReader`) — no full-file read-back — and extract `record_count`,
   `file_size_bytes`, `footer_size`, and per-column stats.
7. **Merge min/max across all row groups** per column: typed fold over `Statistics`, same
   type coverage as today (`Boolean`, `Int32`, `Int64`, `Float`, `Double`, `ByteArray`
   as UTF-8); other types stay `None`. `null_count`, `value_count`, and
   `column_size_bytes` accumulate across row groups as today.

### `materialize.rs`

- Drop the `write_parquet` → `store::put` two-step. Call `write_dataset`, which writes
  directly to storage.
- `MaterializeRequest.file_name: &str` becomes **`file_prefix: &str`** — a caller-unique
  token used as a subdirectory under `<schema>/<table>/`, preserving cross-call
  uniqueness now that one call emits N files. `dir_prefix = "<schema>/<table>/<file_prefix>"`.
- Each `WrittenFile.path` is relative (`"<file_prefix>/part-N.parquet"`); DuckLake
  resolves `data_path + schema.path + table.path + file.path`, and `file.path` may contain
  a `/`.
- One `append_files` with the full `Vec<DataFile>` (the API already takes a slice).
- The store/runtime handle shifts from `&dyn ObjectStore` to `Arc<dyn ObjectStore>`
  (DataFusion's `RuntimeEnv` needs an owned `Arc`). Propagate through `materialize`'s
  signature and the callers (http/runtime).

### `store.rs`

`put` is retired — DataFusion writes directly. The module is slimmed: `StoreError` is
folded into `WriteError`; the object-store→DataFusion registration lives in `write`.
`tests/store.rs` and the `store` test target are removed. (`StoredPath`'s relative-path
semantics move into `write_dataset`'s path construction.)

## Error handling

`WriteError` absorbs `datafusion::error::DataFusionError` and `object_store::Error`
alongside the existing `parquet::errors::ParquetError`. `IngestError::Store` is removed (or
re-pointed at `WriteError`); `lib.rs`'s error doc is updated.

The "no partial catalog state" guarantee is unchanged. The ordering becomes
**write-then-commit**: a commit failure after the DataFusion write orphans the N Parquet
files (documented in `materialize`/`lib.rs`, same shape as the prior single-file orphan
note). GC stays a deferred concern.

## Build / dependency work (plan step 0 — primary risk)

1. Add `datafusion = "54"` to `src/services/ingest/Cargo.toml`.
2. `cargo generate-lockfile` (or `buck2 run //tools:reindeer -- update`).
3. `./tools/buckify.sh` to regenerate `third-party/BUCK`. **Expect new build-script
   fixups** in DataFusion's large dependency tree; budget for buckify iteration. The
   `reindeer-check` hook keeps generated rules in sync.
4. Add `//third-party:datafusion` to the ingest `rust_library` deps in
   `src/services/ingest/BUCK`.

This is the heaviest and riskiest step (large transitive tree, possible new fixups,
RE/build-script interactions). It lands first and independently.

## Testing

All ingest tests follow the repo rule: integration `rust_test` targets only, no inline
`#[cfg(test)]`. Fixture-backed (DuckDB) tests use `loom_fixture_test`.

- **`tests/write.rs`** (plain `rust_test` + tokio + an in-memory or `LocalFileSystem`
  object store — no DuckDB, RE-safe): force ≥2 files via a small `target_file_size`;
  assert Σ`record_count` over files equals input rows, every file is well-formed (`PAR1`),
  per-file stats are sane. Force **multiple row groups within one file** (small row-group
  size) and assert **file-level min/max spans the row groups** (the merge). The target
  becomes async with `tokio`/`object_store` deps.
- **New pure unit test** for the size-estimate → partition-count function (clamp bounds,
  empty input → 1 file, large input → `max_files`).
- **`tests/materialize.rs`:** exercise the multi-file path with a `file_prefix`; assert N
  `DataFile`s are registered when the input is forced across multiple files.
- **`tests/ducklake_interop.rs`** (the oracle): DuckDB reads the DataFusion-produced
  multi-file table and appends its own snapshot. Must stay green — the fidelity gate.
- **`tests/runtime_land.rs` / `tests/http_land.rs`:** update for the `Arc<dyn ObjectStore>`
  and `file_prefix` signature changes.

## Roadmap update

Update `docs/superpowers/specs/2026-06-06-loom-roadmap.md`: the ingest "Later" bullet's
"DataFusion endpoint" / compute-path item is partially delivered — the DataFusion
ingestion compute path (size-targeted partitioned writes) is in; the networked endpoint
remains.

## Risks & mitigations

1. **DataFusion 54 vendoring via reindeer** (large tree, build scripts, fixups) — *primary
   risk*. Mitigate: land step 0 first and independently; iterate buckify; expect new
   fixups.
2. **DataFusion Parquet output fidelity vs DuckLake expectations** — mitigated by the
   interop oracle, which is the acceptance gate.
3. **Compression-factor estimate is crude** → file sizes drift from target. Accepted and
   documented; refine (sampling, per-type width) in a later slice.
4. **Min/max merge type coverage** — scoped to the types already supported; others stay
   `None` (pruning disabled for them), unchanged from today.
5. **Non-deterministic file names** from DataFusion — never hardcode names; list + sort.
6. **`Arc<dyn ObjectStore>` signature churn** across http/runtime callers — mechanical,
   contained to ingest.

## Judgment calls (settled during brainstorm)

- **`file_name` → `file_prefix` (per-call subdirectory)** for cross-call uniqueness of N
  files. Alternative considered: flat `<file_prefix>-part-N.parquet` names with no subdir.
- **`store.rs::put` retired entirely** rather than kept as a single-file fallback.
- **Split policy = target file size** (estimate + repartition), the most DuckLake-idiomatic
  of the considered options (vs fixed parallelism or rows-per-file).
