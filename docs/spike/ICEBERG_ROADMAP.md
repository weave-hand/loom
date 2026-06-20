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

### Slice A — inline writes + read union (loom-native inlining)
- `inline_append` (`postgres`): small writes land as typed rows in a per-table
  `iceberg_mirror.inline_<table_id>` table created on the fly — **no object-storage
  Parquet, no Iceberg metadata**. A mirror-only commit: synthetic snapshot + typed
  rows + lineage, atomic in one Postgres transaction.
- `IcebergCatalog::inline_parquet` encodes a table's live inline rows to in-memory
  Parquet bytes (arrow/parquet 57); the slice-3 serving engine registers them under
  a `memory://` store and **unions** them with the table's `file://` Parquet (a
  DataFusion `UNION ALL` view — a `ListingTable` can't span two object stores).
- This is the DuckLake `DATA_INLINING` capability rebuilt loom-natively. **External
  Iceberg clients see inline rows only after a future flush** (bounded staleness,
  accepted). Spec/plan: `docs/superpowers/{specs,plans}/2026-06-18-iceberg-inline-writes*`.
- **Remaining inline follow-up:** flush/compaction of inline → Parquet (also
  restores external visibility).

### Slice B — landing backend + ingest wiring

- The ingest binary now selects a landing backend at boot via
  `LOOM_LANDING_BACKEND` (default `ducklake`, today's path untouched; `iceberg`
  enables the loom-native path), mirroring query-api's `LOOM_SERVING_BACKEND`. A
  `LandingMaterializer` port (`DuckLakeMaterializer` / `IcebergMaterializer`) is
  injected into the HTTP `AppState`; gate + schema resolution + lineage happen in
  the handler before dispatch.
- The Iceberg backend routes by **in-memory byte size** (`LOOM_INLINE_BYTE_LIMIT`,
  default 16 MiB): small requests `inline_append` (mirror-only rows), large
  requests write real Parquet. Both emit lineage atomically.
- **Atomic lineage on the Parquet path:** the Slice-2 `append_batches` emitted no
  lineage. Slice B adds `SqlCatalog::do_update_table(commit, Option<&LineageEvent>)`
  (the trait `update_table` delegates with `None`) and a per-call
  `LineageEmittingCatalog` decorator (`append_batches_with_lineage`) so the lineage
  row shares the pointer-CAS + mirror-projection tx and rolls back with a lost CAS.
- The arrow-57 landing logic (IPC decode, routing, both write branches,
  create-if-absent) lives in the postgres crate's `iceberg_landing` module; the
  ingest `IcebergMaterializer` is a thin forwarder passing the raw IPC body, so the
  arrow-57/58 cross-major boundary stays inside the postgres crate. Spec/plan:
  `docs/superpowers/{specs,plans}/2026-06-18-iceberg-landing-backend*`.

### Inline flush/compaction

- `flush_table` (`iceberg_flush`): a library primitive that drains a table's live
  inline rows into a **real Iceberg Parquet snapshot** (so external Iceberg clients
  see them and the read engine stops reconstructing them) and **end-caps** the
  inline rows at the same snapshot — `end_snapshot = S`, atomic, exactly-once,
  time-travel-correct. Generalizes the Slice B commit decorator to `CommitExtras
  { lineage, end_cap }`: the new `data_file` (`begin_snapshot = S`), the inline
  end-cap, and a **compaction lineage event** (`in == out == table`) all land in
  the one `do_update_table` tx. Serialized per table by a transaction-scoped
  advisory lock. Inline rows are end-capped, not deleted (time-travel preserved;
  physical GC deferred). Spec/plan:
  `docs/superpowers/{specs,plans}/2026-06-19-iceberg-inline-flush*`.
- **Remaining follow-ups:** the *consumer* side of triggering (a worker draining
  the queue and calling `flush_table`) and *physical GC* of end-capped inline rows
  + orphaned Parquet. The *producer* side of triggering has landed — see below.

## Left (deferred)

1. **Per-column stats + pruning read path** — the mirror stores only
   `record_count` / `file_size`; no lower/upper bounds or null counts, so no
   predicate pushdown / file skipping.
2. **Overwrite/replace** — append-only today; the Iceberg analogue of DuckLake's
   `replace_files` (transform overwrite output mode).
3. **Flush triggering + GC** — the *producer* has landed (Spec 1): a byte-size
   trigger on `inline_append` enqueues a `flush_table` job when a table's live
   inline bytes cross `LOOM_FLUSH_BYTE_THRESHOLD` (per-table override via
   `iceberg_mirror.inline_trigger.threshold`), debounced and reset on flush. The
   *consumer* — a worker that drains the queue and calls `flush_table` — is Spec 2,
   the engine-wire flush vertical (`docs/spike/engine-wire-transport.md`), not yet
   built. End-capped inline rows + orphaned Parquet are still never physically
   reclaimed (GC deferred). Spec/plan:
   `docs/superpowers/{specs,plans}/2026-06-19-inline-flush-trigger*`.
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

Read, write, and compaction are all in place as primitives; reads and writes are
end-to-end reachable from running services (`LOOM_SERVING_BACKEND` /
`LOOM_LANDING_BACKEND`). The open questions are **depth** and **automation**:

- **Per-column stats + pruning (#1)** makes Iceberg reads performant (predicate
  pushdown / file skipping), or **overwrite/replace (#2)** brings the write path
  to transform parity.
- **Flush triggering (#3)** turns `flush_table` from a primitive into a policy —
  a threshold/worker/endpoint that decides *when* to compact — plus the GC that
  reclaims end-capped rows.
