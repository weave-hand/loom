# Iceberg Adapter — Roadmap

> Status snapshot as of 2026-06-17. Captures what the Iceberg table-format
> adapter is, what's shipped, and what's deferred. This is a spike/roadmap
> note, not a spec — each remaining slice gets its own
> `docs/superpowers/specs/` design before implementation.

## What it is

An Iceberg table-format adapter that **coexists** with DuckLake. It serves
loom's `core::Catalog` from a loom-owned `iceberg_mirror.*` Postgres projection
(fast reads, DuckLake parity) over a **vendored** Iceberg SQL catalog — loom
owns `update_table`, which is what makes atomic pointer+mirror commits possible.

Original decisions: adopt iceberg-rust · coexist (maybe replace later) ·
pointer+mirror storage · read-path first · vendor the catalog.

## Done

### Slice 1 — read path (PR #76)
- Vendored `iceberg-catalog-sql`, ported to sqlx 0.9; loom-owned.
- `iceberg_mirror.*` MVCC projection schema + `IcebergCatalog impl core::Catalog`
  (`current_snapshot` / `snapshots` / `files` / `schema`) + type mapping.
- Validated by the backend-agnostic `catalog_contract` + `catalog_delete_contract`
  — the same proofs DuckLake passes. Test seeder wrote *synthetic* file metadata.

### Slice 2 — write path (PR #78)
- Real Parquet via the iceberg writer chain.
- **Atomic** pointer-CAS + mirror projection in one Postgres transaction (mirror
  derived from canonical staged metadata).
- Concurrency-safe snapshot ids (Postgres sequence).
- Seeder now drives the real write → project → read path, so the contracts
  validate **real** writes. **Append-only.**

### Slice 3 — loom-native read serving (DataFusion, no DuckDB)
- `DataFusionServingEngine` (`query-api`): a `ServingEngine` that enumerates live
  tables from the mirror (`IcebergCatalog::live_tables`), registers each table's
  live Parquet files (absolute `file://` paths) as a schema-qualified DataFusion
  `ListingTable`, inlines params, runs the governed/compiled SQL through
  DataFusion, and maps Arrow → `Rows`. **No DuckDB in the path** — the first
  loom-native serving engine, the thing the mirror was built to enable.
- Selected in the binary by `LOOM_SERVING_BACKEND=iceberg` (default `ducklake`
  keeps the DuckDB path untouched); `cp` (ontology/ACL) stays `PgControlPlane` —
  the handler never calls `cp.catalog()`.
- **Reads file-backed tables only.** Actions/inline writes are rejected
  (`UnsupportedActionEngine`) — inlining is a DuckLake-only feature loom hasn't
  rebuilt. Spec/plan: `docs/superpowers/{specs,plans}/2026-06-17-iceberg-datafusion-serving-engine*`.

## Left (deferred)

1. **Per-column stats + pruning read path** — the mirror stores only
   `record_count` / `file_size`; no lower/upper bounds or null counts, so no
   predicate pushdown / file skipping.
2. **Overwrite/replace** — append-only today; the Iceberg analogue of DuckLake's
   `replace_files` (transform overwrite output mode).
3. **Write/ingest service wiring** — the read side shipped in slice 3, but the
   *write* path is still a **library** component: a real HTTP ingest can't target
   Iceberg yet (needs a DuckLake-vs-Iceberg landing backend selection in the
   ingest binary, analogous to query-api's `LOOM_SERVING_BACKEND`).
4. **Multi-writer perf** — the atomic commit holds the Postgres tx open across
   object-store manifest reads; fine single-writer, needs optimizing for
   throughput.

### Implicit / longer-term (not yet specced)

5. **Schema evolution** — slice 2 projects columns once; the mirror's
   `schema_version` is reserved but unused.
6. **Real object store (S3/MinIO)** — tests use `file://` / LocalFsStorage; the
   vendored catalog supports S3 FileIO but loom hasn't exercised it.
7. **The coexist → replace call** — "maybe replace DuckLake depending on
   ergonomics" is a future evaluation, not a slice.

## Where to go next

Both pillars are built (read + write), so the question is **depth vs reach**:

- **Reach → slice 3: service-binary wiring (#3).** Turns the library into
  something a running service actually uses — the first point where Iceberg is
  end-to-end usable, not just contract-proven. Highest "make it real" value.
- **Depth → stats + pruning (#1)** makes Iceberg reads performant, or
  **overwrite (#2)** brings the write path to transform parity.

Recommendation: **#3 (service wiring)** — without it, the adapter is correct but
unreachable from outside.
