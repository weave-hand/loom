# Design: Iceberg as a second table-format adapter — read-path slice

> **Type:** design spec. **Date:** 2026-06-16.
> **Builds on:** `docs/superpowers/specs/2026-06-16-ducklake-format-seams-design.md` (the
> `core` seams that made a second table-format adapter slot in) and the spike
> `docs/spikes/2026-06-12-ducklake-trait-boundary.md` (which named Iceberg as the
> forcing function).
> **Decision:** build the Iceberg adapter the seams were prep for, **read path first**,
> coexisting with DuckLake behind the existing `core` ports.

## Why now

The two seam PRs (#70 table-format metadata boundary, #71 SQL-dialect seam) reshaped
`core` against Iceberg's real model **on paper, building zero Iceberg code**. They were
explicit prep for this adapter. `FUTURE.md` records the driver: *"Iceberg `ActionEngine`
impl — the trait's reason for being."* The seams spec listed the two loom-specific
Iceberg-parity features as future adapter work: **an Iceberg catalog in Postgres for fast
reads** and **inline writes**. This spec designs the first of the three slices that
deliver them.

## The four framing decisions (resolved with the maintainer)

1. **Adopt `iceberg` 0.9.1** (Apache iceberg-rust) for all spec-compliant metadata
   machinery — manifests, manifest-lists, `metadata.json`, Avro encoding, atomic
   `Transaction` commit, the Parquet/logical `writer` module, and `FileIO` (memory /
   `file://` / S3). loom builds **only** the two non-default pieces. We do **not**
   hand-roll Iceberg metadata the way loom hand-writes DuckLake catalog rows — Iceberg's
   format is far more complex and the crate is the right dependency.
2. **Coexist** with DuckLake behind the existing `core` ports. DuckLake stays and remains
   the green differential oracle throughout. "Replace DuckLake outright" is a deliberately
   deferred, ergonomics-driven future call — not this work.
3. **Pointer + mirrored metadata** (DuckLake-parity fast reads). Canonical Iceberg metadata
   lives in object store (written via `iceberg`, spec-compliant, externally
   interoperable). A loom-owned Postgres projection serves `core::Catalog` entirely from
   PG so reads never walk object-store manifests. (Not standard pointer-only Iceberg, which
   the maintainer explicitly rejected as not fast-reads.)
4. **loom vendors and owns the SQL catalog (revised — see below).** Originally this design
   adopted the `iceberg-catalog-sql` crate as the pointer owner. Spiking the dependency
   revealed two blockers: it pins **sqlx 0.8** (loom is on 0.9, so two sqlx versions in the
   tree) and it **owns its own connection** — `update_table` runs on its internal pool and
   never accepts an external executor, so the pointer compare-and-swap can **never share a
   Postgres transaction** with loom's mirror write. Since loom wants atomic pointer+mirror
   (so a reader never sees the pointer ahead of the mirror), loom **vendors the
   `iceberg-catalog-sql` SQL-catalog module (~1k lines: `async-trait`, `iceberg`, `sqlx`,
   `strum`), ports it to loom's sqlx 0.9, and owns it.** loom's `update_table` then performs
   the pointer compare-and-swap **and** the mirror upsert in one sqlx-0.9 transaction. The
   tree stays on a single sqlx version. `iceberg-catalog-sql` is **not** a dependency — only
   `iceberg` is. The vendored catalog still follows the JDBC table layout, so external
   engines can attach it. (This is loom's standing *read-the-pinned-source, own-it* pattern,
   not a black-box dependency.)

**Slice order:** read path first (this spec). It builds the `core::Catalog` impl, the PG
mirror, and the type/stat decode, validated against a *real* canonical Iceberg table that
`iceberg` itself seeds. Slices 2 (write path) and 3 (inline writes) are scoped in §8.

## Architecture — three layers, clean ownership

| Layer | Owner | What it holds | Used at |
|---|---|---|---|
| **Pointer** | loom (vendored SQL catalog, sqlx 0.9) | `table → current metadata.json location`, in JDBC-layout tables in loom's Postgres. loom owns `update_table`, so the pointer compare-and-swap can share a transaction with the mirror upsert. | write/commit (slice 2); seeding (slice 1) |
| **Mirror** | loom (`iceberg_mirror.*` schema) | read-optimized projection of snapshots / file lists / column stats / schema — what the pointer catalog does **not** store. The fast-reads layer. | read (this slice) |
| **Read port** | loom (`iceberg_catalog.rs`) | `impl core::Catalog`, serving from the mirror. Pure Postgres queries — structurally identical to the DuckLake `catalog.rs`. | read (this slice) |

> **Slice-1 note:** the read path needs neither the persistent pointer nor transaction
> coupling — it reads only the mirror. The vendored catalog appears in slice 1 *only* as the
> seeder's way to drive real `iceberg` writes into the `file://` warehouse. The
> transaction-coupling modification to `update_table` (folding the mirror upsert into the
> pointer-swap transaction) is a **slice-2** refinement; slice 1 projects the mirror separately
> in the seeder.

### The key structural insight — why read-path-first is cheap

At **read** time the adapter never touches `iceberg` or the object store: it is pure
Postgres queries against the mirror, the same shape as `postgres/src/catalog.rs` today.
`iceberg` and the vendored catalog are needed only to *produce* canonical metadata. So in
slice 1 they appear **only in the test seeder**: it commits a canonical table to a temp
`file://` warehouse, and a **mirror-projection** function reads that table's metadata and
populates the `iceberg_mirror.*` rows. The `core::Catalog` impl under test is just SQL.

### The mirror is a rebuildable cache — and, because loom owns the catalog, atomically updatable

The mirror is a pure **projection of the canonical catalog state**, so **canonical is the
single source of truth and the mirror is always reconstructable** from it (load table → walk
manifests via `FileIO` → re-project). That gives two safety nets, not one:

- **Rebuildable.** Fast reads are a **materialized-view cache** over the JDBC catalog; a
  divergent mirror can always be re-derived from canonical metadata (a reconcile pass). Same
  risk *class* as the "action lineage atomicity" dangling slice in `FUTURE.md`, but the
  derived-ness makes any gap benign.
- **Atomic (the reason loom vendors the catalog).** Because loom owns `update_table`, slice 2
  performs the pointer compare-and-swap **and** the mirror upsert in **one sqlx-0.9
  transaction** — a reader never sees the pointer ahead of the mirror. The reconcile path
  remains as defense-in-depth, but the common path is atomic. (An adopted `iceberg-catalog-sql`
  could not do this — it owns its own sqlx-0.8 connection and never shares a transaction.)

## The mirror-projection function (real, shared with slice 2)

`iceberg_mirror.rs` exposes the projection `canonical Iceberg table → iceberg_mirror.* rows`:
given a loaded `iceberg::table::Table`, read `table.metadata()` for the schema and snapshot
list and scan the snapshot's manifests via `table.file_io()` for data files (+ per-column
stats in slice 2). This is **real adapter code, not throwaway** — slice 2's transactional
write path reuses it to refresh the mirror inside the `update_table` transaction. Slice 1
builds and proves it via the seeder.

## Component / file layout (slice 1)

```
src/control-plane/postgres/
  migrations/0012_iceberg_mirror.sql   NEW — the iceberg_mirror.* projection schema
  src/iceberg_sql_catalog/             NEW — vendored iceberg-catalog-sql, ported to sqlx 0.9 (loom-owned)
  src/iceberg_catalog.rs               NEW — impl core::Catalog over iceberg_mirror.* (mirror of catalog.rs)
  src/iceberg_type.rs                  NEW — logical_from_iceberg() (mirror of ducklake_type.rs)
  src/iceberg_mirror.rs                NEW — projection: iceberg::Table → mirror rows (REAL, shared w/ slice 2)
  src/fixture.rs                       MOD — add IcebergWriter seeder (sibling to DuckLakeWriter)
  src/lib.rs                           MOD — declare the new modules
  tests/iceberg_catalog.rs             NEW — iceberg_passes_catalog_contract() via the testkit contract
  BUCK                                 MOD — new loom_fixture_test target; new //third-party deps
  Cargo.toml                           MOD — add `iceberg = "0.9"`, `strum`; then ./tools/buckify.sh
src/control-plane/testkit/             UNCHANGED — catalog_contract is already backend-agnostic; Iceberg reuses it as-is
```

The DuckLake adapter is the template for the loom-side files: `iceberg_catalog.rs` mirrors
`catalog.rs`'s query-the-mirror-then-map-types shape; `iceberg_type.rs` mirrors
`ducklake_type.rs`; `IcebergWriter` is a sibling of `DuckLakeWriter`. `iceberg_sql_catalog/`
is a vendored copy of `iceberg-catalog-sql`'s ~1k-line module, ported sqlx 0.8 → 0.9 and owned
by loom (the slice-2 `update_table` transaction-coupling edit lives here).

## The mirror schema (`iceberg_mirror.*`)

Named `iceberg_mirror` to avoid any collision with the vendored catalog's JDBC pointer tables
(`iceberg_tables` / `iceberg_namespace_properties`). MVCC-versioned by snapshot like DuckLake.
Sketch (final columns settled in the plan against the `iceberg` 0.9 API):

- `iceberg_mirror.snapshot(table_namespace, table_name, snapshot_id, sequence_number,
  committed_at, schema_id)` — serves `current_snapshot` / `snapshots`.
- `iceberg_mirror.data_file(table_namespace, table_name, snapshot_id, end_snapshot,
  file_path, file_format, record_count, file_size_bytes)` — serves `files`; `begin/end`
  snapshot give MVCC liveness.
- `iceberg_mirror.column(table_namespace, table_name, schema_id, field_id, name, type,
  nullable, ordinal)` — serves `schema`.
- `iceberg_mirror.column_stat(table_namespace, table_name, snapshot_id, file_path,
  field_id, null_count, column_size, lower_bound, upper_bound)` — typed-stat projection,
  bounds decoded from Iceberg's typed-binary form into `StatValue`-shaped values.

The `core::Catalog` read methods join these on `(namespace, name, snapshot)` with
`begin_snapshot <= at AND (end_snapshot IS NULL OR end_snapshot > at)` MVCC filters — the
exact pattern `catalog.rs` uses against `ducklake_*`.

## Type / stat decode (`iceberg_type.rs`)

The exact inverse of WS1's `ducklake_type.rs`, against **Iceberg primitive type names**.
Same closed-vocabulary, no-implicit-widening discipline; an unknown physical type returns
`None` and `Catalog::schema()` surfaces it as an explicit error rather than leaking a raw
Iceberg type string into `core`.

```
logical_from_iceberg("int")        -> Some(BaseType::Integer)
logical_from_iceberg("long")       -> Some(BaseType::Long)
logical_from_iceberg("double")     -> Some(BaseType::Double)
logical_from_iceberg("boolean")    -> Some(BaseType::Boolean)
logical_from_iceberg("string")     -> Some(BaseType::String)
logical_from_iceberg("date")       -> Some(BaseType::Date)
logical_from_iceberg("timestamp")  -> Some(BaseType::Timestamp)
_                                  -> None
```

`Catalog::schema()` maps physical → logical via this, then `.canonical_name()` — the
byte-identical pattern to `catalog.rs:114`. (Per-column stats are a slice-2 concern — the read
port `FileRef` carries none — so slice 1 does not decode or store them.)

## Fixture / seeder — hermetic, no new external binary

`iceberg` and the vendored catalog are pure Rust over `FileIO`, so seeding is **in-process** —
no DuckDB-CLI-style external binary, unlike the DuckLake fixture. `IcebergWriter` (sibling
to `DuckLakeWriter` in `fixture.rs`):

1. Builds loom's vendored SQL catalog (sqlx 0.9) pointed at the hermetic Postgres, with a
   `file://` warehouse in a temp dir (`FileIO` local storage).
2. Creates the namespace + table via the catalog (`create_namespace` / `create_table`),
   exercising the vendored catalog end to end; the vendored catalog manages its JDBC pointer
   tables and writes the canonical `metadata.json`.
3. Reads back the schema Iceberg actually recorded (`load_table().metadata().current_schema()`)
   and projects it — plus one synthetic data-file row per batch — into `iceberg_mirror.*`.

A `PgSeeder`-equivalent impls the testkit `CatalogSeed` trait (logical types in, the same
contract surface as DuckLake's seeder). The **schema** the mirror serves is genuine Iceberg
catalog metadata (so the logical↔Iceberg type round-trip is validated against a real table),
and the vendored catalog is exercised for real. **Slice 1 does not write real Parquet bytes:**
that needs the `iceberg` writer chain bound to **arrow/parquet 57** (a distinct major from the
arrow 58 the services use, whose version-suffixed buck targets are not publicly visible), and
producing real data files is the **write path's** concern — slice 2. The catalog contracts
assert file *counts*/*paths* and schema/type round-trips, all of which a real-schema +
synthetic-file projection satisfies; slice 2 replaces the synthetic files with real committed
`DataFile`s via the writer chain.

## Testing strategy

- **Reuse `catalog_contract` verbatim.** It is already backend-agnostic — the memory fake
  and the DuckLake adapter both pass it. `iceberg_passes_catalog_contract()` proves the
  Iceberg `core::Catalog` impl satisfies the **same** contract: the strongest possible
  parity signal with near-zero new assertion code. The only new test-harness piece is the
  seeder.
- **Round-trip fidelity.** Because the seeder writes canonical metadata via `iceberg` and
  the contract reads it back through the mirror, the test is a mirror-projection round-trip
  check (what `iceberg` wrote == what the mirror serves).
- **Pure-logic unit tests** (sibling `tests/*.rs`, per the no-inline-tests rule):
  `logical_from_iceberg` round-trip + unknown→`None`.
- **DuckLake suite untouched and green** — the coexistence proof.

## Slice decomposition (the full adapter; only slice 1 is built now)

1. **Slice 1 — read path** (this spec, to plan depth): vendor + port the SQL catalog to sqlx
   0.9, the `iceberg_mirror.*` schema, `iceberg_catalog.rs`, `iceberg_type.rs`,
   `iceberg_mirror.rs` projection, the `IcebergWriter` seeder, and the contract test.
2. **Slice 2 — snapshot-commit write path:** a `ControlPlane`/`Tx` impl commits canonical
   metadata via the vendored catalog + `iceberg::Transaction`, with the mirror upsert folded
   into the catalog's `update_table` transaction (**atomic pointer+mirror**; the rebuildable
   reconcile path is the backstop). The ingest materializer can then target Iceberg.
3. **Slice 3 — inline writes:** an `ActionEngine` impl for DuckLake-parity inline small
   writes (the second non-default feature, and the trait's stated reason for being).

## Risks & mitigations

- **Dep-tree size + native build on RE.** `iceberg` pulls a large transitive tree (opendal,
  parquet, avro). Per the *verify-native-deps-on-re* lesson, anything native must build on
  **RE**, not merely under `--local-only`. **Mitigation:** after `./tools/buckify.sh`, do a
  clean RE build of the postgres crate **before** wiring tests; if it breaks, fix via a
  prelude submodule bump, not a fork. (Spiked: `iceberg` 0.9.1 resolves with `time` pinned
  at 0.3.47 intact.)
- **Vendoring port (sqlx 0.8 → 0.9, `strum`).** The vendored SQL-catalog module must be
  ported from sqlx 0.8 (`any` driver) to loom's sqlx 0.9 Postgres. **Mitigation:** vendor as
  its own early task and prove it builds + creates/loads a table against the hermetic
  Postgres before wiring the seeder. Keep the JDBC table layout so the port is mechanical.
  loom owns the file, so the slice-2 transaction-coupling edit to `update_table` is a local
  change.
- **Catalog table migration coordination.** The vendored catalog creates its JDBC tables;
  decide whether via loom's sqlx migrator (preferred — deterministic ordering) or on first
  use. The `iceberg_mirror` schema name keeps the mirror and the catalog's pointer tables
  disjoint.
- **`iceberg` 0.9 API drift.** The writer/transaction/`FileIO`/`Catalog`-trait surfaces are
  pinned to 0.9.1; the plan resolves exact signatures against the published crate, not from
  memory.
- **Async runtime.** Pin `iceberg`'s Tokio feature to match loom.

## Non-goals (this slice)

- **No write path, no inline writes** — slices 2 and 3.
- **No second SQL dialect.** WS2 shipped the seam; Iceberg still serves Parquet through
  DataFusion/DuckDB. The dialect stays `DuckDbDialect`.
- **No S3/MinIO in tests** — `file://` warehouse only, hermetic.
- **No `SnapshotId` change** and no change to the three port traits' method shapes.
- **No replacement of DuckLake** — coexistence only; the replace decision is deferred.

## Plan structure

This spec yields one implementation plan now — **slice 1 (read path)** — green on its own
against the existing DuckLake/memory suites. Slices 2 and 3 get their own spec→plan cycles
when taken up.
