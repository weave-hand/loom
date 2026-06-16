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
4. **The pointer is iceberg-rust's, not loom's.** `iceberg-catalog-sql` 0.9.1 (the Apache
   SQL/JDBC catalog, sqlx-based, Postgres `$1..$N` bind style) owns `table → current
   metadata.json location` in its own JDBC tables. loom does not hand-roll pointer
   correctness, compare-and-swap, or atomic commit — `iceberg` does. External engines
   (Spark / PyIceberg / Trino) can attach the same catalog.

**Slice order:** read path first (this spec). It builds the `core::Catalog` impl, the PG
mirror, and the type/stat decode, validated against a *real* canonical Iceberg table that
`iceberg` itself seeds. Slices 2 (write path) and 3 (inline writes) are scoped in §8.

## Architecture — three layers, clean ownership

| Layer | Owner | What it holds | Used at |
|---|---|---|---|
| **Pointer** | `iceberg-catalog-sql::SqlCatalog` (iceberg-rust) | `table → current metadata.json location`, in its own JDBC tables in loom's Postgres. Canonical, interoperable, atomic commit handled by the crate. | write/commit (slice 2); seeding (slice 1) |
| **Mirror** | loom (`iceberg_mirror.*` schema) | read-optimized projection of snapshots / file lists / column stats / schema — what SqlCatalog does **not** store. The fast-reads layer. | read (this slice) |
| **Read port** | loom (`iceberg_catalog.rs`) | `impl core::Catalog`, serving from the mirror. Pure Postgres queries — structurally identical to the DuckLake `catalog.rs`. | read (this slice) |

### The key structural insight — why read-path-first is cheap

At **read** time the adapter never touches `iceberg` or the object store: it is pure
Postgres queries against the mirror, the same shape as `postgres/src/catalog.rs` today.
`iceberg`/`iceberg-catalog-sql` are needed only to *produce* canonical metadata. So in
slice 1 they appear **only in the test seeder**: it commits a canonical table to a temp
`file://` warehouse, and a **mirror-projection** function reads that table's metadata and
populates the `iceberg_mirror.*` rows. The `core::Catalog` impl under test is just SQL.

### The mirror is a rebuildable cache (defuses slice 2's hardest problem)

The mirror is a pure **projection of the canonical SqlCatalog state**, so **canonical is
the single source of truth and the mirror is always reconstructable** from it (load table →
walk manifests via `FileIO` → re-project). Consequences:

- Fast reads are a **materialized-view cache** over the JDBC catalog, not a second source
  of truth.
- Slice 2's pointer+mirror write needs **no true two-phase atomicity**. A crash between
  SqlCatalog's pointer commit and the mirror write is self-healing: a reconcile/rebuild
  pass re-derives the mirror from canonical metadata. (Same risk *class* as the existing
  "action lineage atomicity" dangling slice in `FUTURE.md`, but here the derived-ness makes
  the gap benign.)

## The mirror-projection function (real, shared with slice 2)

`iceberg_mirror.rs` exposes the projection `canonical Iceberg table → iceberg_mirror.* rows`:
given a `SqlCatalog::load_table(ident)` result, read `table.metadata()` for the schema and
snapshot list, scan the snapshot's manifests via `FileIO` for data files + per-column stats,
decode Iceberg's typed-binary lower/upper bounds into `StatValue`-shaped values, and write
the mirror rows. This is **real adapter code, not throwaway** — slice 2's transactional
write path reuses it to refresh the mirror after a commit. Slice 1 builds and proves it via
the seeder; slice 2 wires it into `Tx`.

## Component / file layout (slice 1)

```
src/control-plane/postgres/
  migrations/0012_iceberg_mirror.sql   NEW — the iceberg_mirror.* projection schema
  src/iceberg_catalog.rs               NEW — impl core::Catalog over iceberg_mirror.* (mirror of catalog.rs)
  src/iceberg_type.rs                  NEW — logical_from_iceberg() + StatValue decode (mirror of ducklake_type.rs)
  src/iceberg_mirror.rs                NEW — projection: SqlCatalog table metadata + manifests → mirror rows (REAL, shared w/ slice 2)
  src/fixture.rs                       MOD — add IcebergWriter seeder (sibling to DuckLakeWriter)
  src/lib.rs                           MOD — declare the new modules
  tests/iceberg_catalog.rs             NEW — iceberg_passes_catalog_contract() via the testkit contract
  BUCK                                 MOD — new loom_fixture_test target; new //third-party deps
  Cargo.toml                           MOD — add `iceberg = "0.9"`, `iceberg-catalog-sql = "0.9"`; then ./tools/buckify.sh
src/control-plane/testkit/             UNCHANGED — catalog_contract is already backend-agnostic; Iceberg reuses it as-is
```

The DuckLake adapter is the template throughout: `iceberg_catalog.rs` mirrors
`catalog.rs`'s query-the-mirror-then-map-types shape; `iceberg_type.rs` mirrors
`ducklake_type.rs`; `IcebergWriter` is a sibling of `DuckLakeWriter`.

## The mirror schema (`iceberg_mirror.*`)

Named `iceberg_mirror` to avoid any collision with SqlCatalog's own JDBC tables
(`iceberg_tables` / `iceberg_namespace_properties`), which the crate auto-creates and owns.
MVCC-versioned by snapshot like DuckLake. Sketch (final columns settled in the plan against
the `iceberg`/`iceberg-catalog-sql` 0.9 API):

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
byte-identical pattern to `catalog.rs:114`. Stats decode from Iceberg's typed-binary
lower/upper bounds into the mirror's `StatValue` form (the projection's job).

## Fixture / seeder — hermetic, no new external binary

`iceberg` + `iceberg-catalog-sql` are pure Rust over `FileIO`, so seeding is **in-process** —
no DuckDB-CLI-style external binary, unlike the DuckLake fixture. `IcebergWriter` (sibling
to `DuckLakeWriter` in `fixture.rs`):

1. Builds a `SqlCatalog` pointed at the hermetic Postgres (Postgres `$1..$N` bind style)
   with a `file://` warehouse in a temp dir (`FileIO` local storage).
2. Creates the namespace + table and commits the seeded rows via `iceberg`'s `Transaction`
   + Parquet writer. SqlCatalog auto-creates its own JDBC pointer tables on first use.
3. Calls `iceberg_mirror` to project the committed table's metadata into the
   `iceberg_mirror.*` rows.

A `PgSeeder`-equivalent impls the testkit `CatalogSeed` trait (logical types in, the same
contract surface as DuckLake's seeder). Because the seeder writes via `iceberg`, the read
path is validated against genuinely spec-compliant metadata, not a loom-fabricated mirror.

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
  `logical_from_iceberg` round-trip + unknown→`None`; `StatValue` decode from typed-binary
  bounds.
- **DuckLake suite untouched and green** — the coexistence proof.

## Slice decomposition (the full adapter; only slice 1 is built now)

1. **Slice 1 — read path** (this spec, to plan depth): `iceberg-catalog-sql` integration,
   `iceberg_mirror.*` schema, `iceberg_catalog.rs`, `iceberg_type.rs`, `iceberg_mirror.rs`
   projection, the `IcebergWriter` seeder, and the contract test.
2. **Slice 2 — snapshot-commit write path:** a `ControlPlane`/`Tx` impl commits canonical
   metadata via `SqlCatalog` + `iceberg::Transaction`, then refreshes the mirror (reusing
   the §projection). Because the mirror is rebuildable, the mirror refresh is best-effort
   with a reconcile path rather than a two-phase commit. The ingest materializer can then
   target Iceberg.
3. **Slice 3 — inline writes:** an `ActionEngine` impl for DuckLake-parity inline small
   writes (the second non-default feature, and the trait's stated reason for being).

## Risks & mitigations

- **Dep-tree size + native build on RE.** `iceberg` **and** `iceberg-catalog-sql` pull a
  large transitive tree (opendal, parquet, avro). Per the *verify-native-deps-on-re*
  lesson, anything native must build on **RE**, not merely under `--local-only`.
  **Mitigation:** after `./tools/buckify.sh`, do a clean RE build of the postgres crate
  **before** wiring tests; if it breaks, fix via a prelude submodule bump, not a fork.
- **SqlCatalog migration coordination.** SqlCatalog creates its JDBC tables on first use,
  *not* via loom's sqlx migrator. **Mitigation:** verify there is no race or ordering
  conflict with `fresh_db()`'s migration run; the `iceberg_mirror` schema name keeps loom's
  migration and SqlCatalog's tables disjoint.
- **`iceberg` 0.9 API drift.** The writer/transaction/`FileIO`/`SqlCatalog` builder
  surfaces are pinned to 0.9.1; the plan resolves exact signatures against the published
  crate, not from memory.
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
