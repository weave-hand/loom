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
- **Recursive-CTE graph reads (`/graph`).** This engine is unproven against the
  `WITH RECURSIVE` reachability SQL emitted by `compile_graph_reach` /
  `compile_graph_reach_union` (the `/graph` part-1/2/3 compilers). The shape is
  SQL-standard linear recursion (single self-reference, distinct `UNION`, joins in
  the recursive term, an outer `IN (SELECT id FROM reach …)`) and DataFusion 54
  supports all of it (recursive CTEs default-on since 37.0.0), so no compiler change
  is expected — but no test runs a recursive CTE through `DataFusionServingEngine`,
  and a real check must land the graph into the `iceberg_mirror` rather than DuckLake.
  **Follow-up for this arc:** add a recursive-CTE-over-DataFusion integration test
  (mirror-landed self-link graph → `DataFusionServingEngine`, asserting the same
  reachable sets as the DuckDB graph e2es `graph-reach-e2e` / `graph-union-e2e`).
  See `docs/FUTURE.md` (the `/graph` surface section).

## Architecture

One new module in query-api: `src/serving_datafusion.rs`, holding
`DataFusionServingEngine` — a third `ServingEngine` impl alongside the DuckDB ones
in `serving.rs`. It owns **only** an `IcebergCatalog` (concrete, not `dyn Catalog`)
— the mirror reader. Concrete because the engine needs to enumerate live tables,
which the `core::Catalog` trait does not expose.

It does **not** take an injected object store. The mirror records **absolute**
`file://` data-file paths (`iceberg_mirror.data_file.path` = the Iceberg
`data_file.file_path()`), so the engine registers a non-prefixed
`object_store::local::LocalFileSystem::new()` under the `file://` scheme in each
query's `SessionContext` and registers tables by their absolute path. (S3/object
stores are a later concern — that's where an injected/configured store would slot
in.)

New query-api dependencies: `//third-party:datafusion`, `//third-party:object_store`,
`//third-party:arrow`. `control-plane-postgres` is already a dependency, so
`IcebergCatalog` is in reach. (We do **not** depend on `datafusion-io` — its
`scan_table` reconstructs *table-relative* keys for the DuckLake layout, the wrong
convention here; the engine does its own absolute-path registration, mirroring
`scan_table`'s `ParquetFormat::default().with_force_view_types(false)` so string
columns stay canonical `Utf8`.)

**No change** to the `ServingEngine` trait, the read handler, the SQL compiler, or
the DuckDB impls. The read handler's `QueryDeps` uses only `ontology`, `acl`, and
`serving` — it never calls `cp.catalog()` — so ontology/ACL stay on the shared
`PgControlPlane` (format-agnostic) and only the serving engine swaps. The slice is isolated to one new engine type, one small mirror
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
   `"schema"."table"` references resolve. (The engine uses its own absolute-path
   registration — see "File-path convention" — not `scan_table`, which registers a
   *bare*, table-relative name.)
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

The engine **inherits the `ServingEngine` trait's default `dialect()`**
(`DuckDbDialect`) — no new dialect type. Because the engine inlines params (so the
placeholder is always `?`) and the SQL compiler quotes every identifier, the SQL
`DuckDbDialect` emits — `"id"` quoting, `LIMIT n`, `?`-then-inlined literals — is
already valid DataFusion SQL. A separate `DataFusionDialect` would be byte-for-byte
identical and is therefore omitted (YAGNI); add one only if a real token ever
diverges between DuckDB and DataFusion.

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
- `iceberg` → `DataFusionServingEngine::new(IcebergCatalog::new(pool.clone()))`,
  paired with an `UnsupportedActionEngine`. `cp` (the `PgControlPlane` for
  ontology/ACL) is built and used exactly as today — `PgPool` is `Clone`, so the
  pool is shared between `cp` and the engine's `IcebergCatalog`.

Parsing is a tiny unit-testable helper in query-api (`parse_serving_backend`);
`service_runtime`'s `Config` is untouched (only query-api needs this knob this
slice).

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

## File-path convention (resolved during planning)

`iceberg_mirror.data_file.path` stores the Iceberg `data_file.file_path()` verbatim
(see `iceberg_mirror::added_files_of`), which is an **absolute** URI — for the
LocalFsStorage path the slice exercises, `file:///<warehouse>/<ns>/<name>/…/loom-<uuid>.parquet`.
The warehouse is the writer's own location, unrelated to any `data_path`.

Consequences, baked into the design above:
- The engine registers each file by its **absolute** path as a
  `ListingTableUrl::parse(path)`, NOT via `scan_table`'s `<schema>/<table>/<rel>`
  reconstruction.
- The engine registers a **non-prefixed** `LocalFileSystem::new()` under `file://`
  (a `new_with_prefix(data_path)` store could not reach the absolute warehouse
  paths).
- The plan's first product task still ends with an integration test that registers
  a *seeded* table and `SELECT`s it back, so the absolute-path handling is proven
  empirically before the engine is assembled on top.
