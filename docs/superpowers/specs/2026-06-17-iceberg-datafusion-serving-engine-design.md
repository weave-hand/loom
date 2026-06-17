# Iceberg DataFusion Serving Engine — Design

> Slice 3 of the Iceberg adapter (after read-path PR #76, write-path PR #78).
> See `docs/spike/ICEBERG_ROADMAP.md` for the slice map.

## Goal

Serve loom's governed object reads for **Iceberg-backed tables from loom's own
engine (DataFusion)** instead of gluing to embedded DuckDB. This is the thing the
`iceberg_mirror` projection was built to enable: loom reading its own catalog
projection, registering the live Parquet files as DataFusion tables, and running
the existing governed/compiled SQL through DataFusion — with **no DuckDB anywhere
in the path**.

## Why (strategic intent)

query-api's only `ServingEngine` impls today are DuckDB-based: `EmbeddedDuckDb`
(ATTACH-ing the DuckLake catalog) and `QuackServingEngine` (forwarding to a remote
DuckDB). The Iceberg adapter + mirror exist to get loom off that DuckDB glue and
onto its own engine. This slice delivers the first loom-native serving engine.

`ServingEngine::dialect()` already anticipates this: its doc says "a future
non-DuckDB engine overrides this." This slice is that engine.

## Scope

**In scope:**
- A loom-native, DataFusion-backed `ServingEngine` reading file-backed Iceberg
  tables via the mirror.
- Wiring it into the query-api binary, selected by config.

**Out of scope (explicitly):**
- **Inline data.** Inlining is a DuckLake/DuckDB feature (`DATA_INLINING`) that
  loom got for free; the Iceberg path has no inline concept. Iceberg writes are
  append-only Parquet, so an Iceberg table's contents are *entirely* its live data
  files — nothing in Postgres to union in, no "create tables on the fly."
  Rebuilding inline writes for the loom engine is a separate, later slice. A
  governed read that depends on inline-written rows is simply not served by this
  engine.
- **All write/ingest wiring.** The ingest binary stays on DuckLake. No
  `LandingBackend`/backend-selected write path in this slice.
- **The DuckLake serving path.** `EmbeddedDuckDb`/`QuackServingEngine` are
  untouched; DuckLake reads stay on DuckDB.
- **Per-column stats / predicate pushdown / pruning** (deferred from slice 2).
- **Time-travel reads.** The engine serves each table at its latest
  (`current_snapshot`).
- **Governed write-back actions** on the Iceberg backend (no inline write path —
  see `UnsupportedActionEngine` below).

## Architecture

One new module in query-api: `src/serving_datafusion.rs`, holding
`DataFusionServingEngine` — a third `ServingEngine` impl alongside the DuckDB ones
in `serving.rs`. It owns:

- an `IcebergCatalog` (concrete, not `dyn Catalog`) — the mirror reader. Concrete
  because the engine needs to enumerate live tables, which the `core::Catalog`
  trait does not expose.
- an `Arc<dyn ObjectStore>` — the same store the data files live in.

New query-api dependencies: `datafusion-io` (brings DataFusion + the
`scan_table` building block and `LOOM_STORE_URL`) and `object_store`.
`control-plane-postgres` is already a dependency, so `IcebergCatalog` is in reach.

**No change** to the `ServingEngine` trait, the read handler, the SQL compiler, or
the DuckDB impls. The slice is isolated to one new engine type, one small mirror
helper, and a one-line binary swap. (This is the chosen "Approach A": the engine
owns the catalog and pre-registers live tables, rather than extending the seam to
pass referenced tables from the handler — that would touch the trait, both DuckDB
impls, and every handler call site for a perf win we don't need yet.)

## New mirror helper

`IcebergCatalog::live_tables(&self) -> Result<Vec<TableRef>>` — returns every
table currently live in the mirror (i.e. `iceberg_mirror.table` rows with
`end_snapshot is null`), as `(table_namespace -> schema, table_name -> name)`.
Read-only, mirror-local; no change to the `core::Catalog` trait.

## Data flow: `fetch_rows(sql, params)`

1. Build a fresh `SessionContext`.
2. Enumerate live tables via `IcebergCatalog::live_tables()`.
3. For each live table: resolve its `current_snapshot`, fetch its live `files` at
   that snapshot, and register it as a DataFusion `ListingTable` under a
   **schema-qualified** name (`"schema"."table"`), so the compiled SQL's
   `"schema"."table"` references resolve. (`scan_table` registers a *bare* name
   today — see "File-path reconstruction" for why the engine uses its own
   registration rather than `scan_table` verbatim.)
4. **Parameters:** reuse the existing, tested `inline_params` (the Quack path's
   injection-safe `?`→literal renderer, with `''`-doubling as the escape boundary)
   to inline `params` into `sql`, then `ctx.sql(&inlined)`. This sidesteps
   DataFusion's own parameter binding and reuses code that is already the escaping
   boundary. DataFusion accepts the same literal forms the renderer emits
   (`DATE 'YYYY-MM-DD'`, `TIMESTAMP '…'`, `TRUE`/`FALSE`, `'text'`).
5. `.collect()` the result `RecordBatch`es and map Arrow → `Rows`.

Registering all live tables per query is acceptable at slice scale (the mirror
read is cheap Postgres; the cost is Parquet footer schema inference). A schema
cache keyed by `(table, snapshot)` is a noted perf follow-up, **not** in this
slice (YAGNI).

### Snapshot semantics

Each table is registered at its own latest `current_snapshot`. A multi-table query
(link traversal) registers each table at its latest — matching the DuckLake
`ATTACH` path, which also reads latest. Time-travel is out of scope.

## Dialect

A thin `DataFusionDialect` implementing `SqlDialect`:
- `quote_ident(id)` → `"id"` (double-quote, reject embedded `"`), identical to
  `DuckDbDialect` so the compiled SQL's quoted identifiers preserve case.
- `LIMIT n`.

Today it is behaviorally identical to `DuckDbDialect`; it exists as its own type so
future DataFusion/DuckDB SQL divergence has a home. `DataFusionServingEngine::dialect()`
returns it.

## Result mapping

`arrow_to_sqlvalue` — the inverse of the DuckDB `from_duck`, covering the scalar
set loom serves:

| Arrow type | `SqlValue` |
|---|---|
| Utf8 / LargeUtf8 | `Text` |
| Int8/16/32/64 | `Int` |
| Float32/64 | `Double` |
| Boolean | `Bool` |
| Date32 | `Date` |
| Timestamp(Microsecond) | `Timestamp` |
| Null | `Null` |

A defensive `Text(format!("{…}"))` fallback for any unmapped Arrow type so a read
never panics on an unexpected column type — mirroring the existing DuckDB mapping's
fallback. Column order follows the projection (DataFusion preserves it), satisfying
the handler's "serving engine returned columns out of the projected order"
assertion.

## Binary wiring

query-api `main.rs` selects the serving engine from a new env var
`LOOM_SERVING_BACKEND`:

- unset or `ducklake` (default) → `EmbeddedDuckDb` + `EmbeddedDuckDbWriter`
  (today's behavior, unchanged).
- `iceberg` → `DataFusionServingEngine` over `IcebergCatalog::new(pool)` +
  `service_runtime::local_store(&cfg.data_path)`, paired with an
  `UnsupportedActionEngine`.

Parsing is a tiny unit-testable helper in query-api; `service_runtime`'s `Config`
is untouched (only query-api needs this knob this slice).

`UnsupportedActionEngine` is a new `ActionEngine` impl whose `insert_row` returns
`ServingError::Engine("actions unsupported on the iceberg serving backend")`. The
action endpoint therefore errors cleanly on the iceberg backend rather than
silently misbehaving — consistent with "file-backed reads only, no inline writes."

## Error handling

- Mirror/Postgres errors and DataFusion errors map to
  `ServingError::Engine(String)` (the existing serving error type). The handler
  already maps `ServingError` to an opaque HTTP error, so no internal detail leaks.
- A query referencing a table not live in the mirror surfaces as a DataFusion
  "table not found" → `ServingError::Engine` (the read handler's governance has
  already resolved the type → table; an absent table is a genuine fault).

## Testing

- **Integration (`loom_fixture_test`)**: seed Iceberg tables via the slice-2
  `IcebergWriter::seed` (real Parquet + atomic mirror commit), build
  `DataFusionServingEngine` over that `IcebergCatalog` + a `LocalFileSystem` store
  rooted at the same data path, and run governed reads — asserting the `Rows`
  match expectations (row count AND values). Cover a single-table object read and,
  if the fixture supports it, a link traversal (multi-table registration).
- **Unit**: `arrow_to_sqlvalue` over each mapped Arrow type + the defensive
  fallback; `DataFusionDialect` quoting/limit; the `LOOM_SERVING_BACKEND` parse
  helper.

All tests are `rust_test` integration targets (no inline `#[cfg(test)]`),
fixture-backed ones via `loom_fixture_test`.

## Open detail / risk — verify first in the plan

**File-path reconstruction.** `datafusion-io::scan_table` reconstructs object keys
as `<LOOM_STORE_URL>/<schema>/<table>/<path>` — the DuckLake *table-relative*
layout. Iceberg manifests commonly store **absolute** data-file paths
(`file:///…`/`s3://…`). The mirror's `iceberg_mirror.data_file.path` therefore may
be absolute, not table-relative. The engine's registration must resolve the
mirror's stored paths to correct object-store URLs, handling absolute-vs-relative —
likely a dedicated scan variant rather than reusing `scan_table` verbatim.

**This is the first thing the plan verifies**: seed one Iceberg table, inspect what
`iceberg_mirror.data_file.path` actually contains, and shape the registration to
match before building anything else on top.
