# Iceberg Landing Backend — Design

> Slice B of the Iceberg adapter. Wires a DuckLake-vs-Iceberg **landing
> backend** selection into the ingest service binary, mirroring how the
> query-api binary already selects a **serving** backend via
> `LOOM_SERVING_BACKEND`. Closes roadmap item #3 (write/ingest service wiring)
> and the inline-vs-Parquet threshold follow-up from Slice A.

## Goal

Make the ingest service able to land data through the Iceberg adapter, selected
at boot by `LOOM_LANDING_BACKEND` (default `ducklake`, today's path untouched).
Within the Iceberg backend, route each request by size: small requests inline
(mirror-only typed rows, no Parquet), large requests write real Parquet — both
emitting lineage atomically, exactly like the DuckLake path does today.

## Why now

The Iceberg read path shipped in Slice 3 (`LOOM_SERVING_BACKEND=iceberg`) and the
inline write primitive shipped in Slice A, but the *write* path is still a
library component: a running ingest service can only land to DuckLake. This
slice turns the library into something a service actually uses — the first point
where Iceberg ingest is end-to-end reachable.

## Background: the seams that already exist

- **Serving-side template** (`query-api`): `enum ServingBackend { DuckLake, Iceberg }`
  + `parse_serving_backend(LOOM_SERVING_BACKEND)`; `main` matches on it to build
  the chosen `Arc<dyn ServingEngine>`. The control plane (`cp`) is built once and
  shared. Slice B copies this shape on the ingest side.
- **DuckLake landing** (`ingest::materialize::materialize`): gate → schema-select
  (`infer_columns` or model) → `write_dataset` (DataFusion Parquet) → one atomic
  `cp` tx (`create_table` + `append_files` + `emit` + `commit`). Returns a
  `SnapshotId`.
- **Iceberg Parquet write** (`control_plane_postgres::iceberg_writer::append_batches`):
  drives Arrow batches through the iceberg writer chain to real Parquet, commits
  a `fast_append` via `tx.commit(catalog)`. The mirror is projected inside the
  vendored `SqlCatalog::update_table`. **Emits no lineage today.**
- **Iceberg inline write** (`control_plane_postgres::iceberg_inline::inline_append`):
  lands `batch` as typed rows in `iceberg_mirror.inline_<table_id>`, one Postgres
  tx, **emits lineage atomically via `pg_emit`**.
- **Type helpers** (reuse, do not reinvent): `datafusion_io::infer_columns`
  (Arrow `Schema` → `Vec<ColumnSpec>`), `iceberg::arrow::schema_to_arrow_schema`,
  `iceberg_type::iceberg_physical_type`.
- **Vendored catalog construction**: `SqlCatalogBuilder::default()
  .with_storage_factory(Arc::new(LocalFsStorageFactory)).load(name, props)` with
  props `uri` (Postgres DSN) and `warehouse` (`file://…`).

## Cross-major Arrow boundary (forces where the Iceberg work lives)

The ingest crate uses the **arrow 58** umbrella (`//third-party:arrow`); its
handler decodes the IPC body into arrow-58 `RecordBatch`es and the DuckLake path
(`datafusion_io`) is arrow-58. But `inline_append` / `append_batches` live in the
postgres crate, which is pinned to **arrow 57** (`arrow-array`/`arrow-schema` +
`parquet57`) because iceberg 0.9.1 requires it. arrow-58 and arrow-57
`RecordBatch` are distinct, incompatible types.

To avoid dragging a second arrow major (and an IPC bridge) into the ingest
crate, **the Iceberg landing logic lives in a new `iceberg_landing` module in the
postgres crate** (arrow-57 native). It owns: IPC decode (arrow-57), byte-size
routing, the inline-vs-Parquet branches, and create-if-absent. The ingest
`IcebergMaterializer` is a thin forwarder: it passes the **raw IPC body bytes**
(which the handler already has), the resolved `Vec<ColumnSpec>` (a
`control_plane_core` type — arrow-version-agnostic), the lineage event, and the
byte limit. The arrow-57 surface never appears in the ingest crate. (The handler
still decodes arrow-58 for the gate + the DuckLake path; the Iceberg path decodes
the same bytes again in arrow-57 — a second cheap decode, no cross-major
conversion. Writing the original decoded batch matches today's DuckLake
behaviour: the gate only validates, it does not project.)

## Architecture

### 1. Landing backend selection (ingest crate)

A new `enum LandingBackend { DuckLake, Iceberg }` with
`parse_landing_backend(v: Option<&str>) -> Result<LandingBackend, String>`,
exact mirror of `parse_serving_backend` (unset/empty/`"ducklake"` → DuckLake,
`"iceberg"` → Iceberg, anything else → `Err`). Read in `ingest/main.rs` right
after `Config::from_env()`.

### 2. The `LandingMaterializer` port

```rust
#[async_trait]
pub trait LandingMaterializer: Send + Sync {
    /// Land `batches` for `table` and return the new snapshot id. The gate has
    /// already passed and `columns` is the resolved physical schema.
    async fn land(&self, req: LandRequest<'_>) -> Result<SnapshotId, IngestError>;
}
```

`LandRequest` carries the backend-agnostic, already-validated inputs:

```rust
pub struct LandRequest<'a> {
    pub table: &'a TableRef,
    pub schema: Arc<Schema>,          // arrow-58 schema of the batches (DuckLake path)
    pub columns: &'a [ColumnSpec],    // resolved physical schema (model or inferred)
    pub batches: &'a [RecordBatch],   // arrow-58 batches (DuckLake path)
    pub ipc_body: &'a [u8],           // raw Arrow IPC body (Iceberg path; decoded in arrow-57)
    pub file_prefix: &'a str,
    pub lineage: LineageEvent,
}
```

The DuckLake impl uses `schema`/`batches`; the Iceberg impl uses `ipc_body` (see
the cross-major boundary note above). Both share `table`/`columns`/`lineage`.

**Gate + schema resolution move up into the handler** (they are backend-agnostic
and must run once, before dispatch): the handler validates the model gate,
computes `columns` (`infer_columns` or from the model), builds the
`LineageEvent`, then calls `state.materializer.land(req)`. This is a small
refactor of today's `materialize()` — its gate/schema-select head moves to the
handler; its write/commit tail becomes `DuckLakeMaterializer::land`.

Two implementations:

- **`DuckLakeMaterializer { cp, store }`** — exactly today's `materialize` tail:
  `write_dataset` → `cp.begin()` → `create_table` + `append_files` + `emit` +
  `commit`. Behaviour-preserving.
- **`IcebergMaterializer { catalog: Arc<SqlCatalog>, pool: PgPool, inline_byte_limit: usize }`**
  — a thin forwarder: `land` calls
  `control_plane_postgres::iceberg_landing::land(&self.pool, &*self.catalog, req.table, req.columns, req.ipc_body, self.inline_byte_limit, req.lineage)`.
  All arrow-57 work (decode, routing, both write branches) is inside that
  postgres-crate function (below).

`AppState` gains `materializer: Arc<dyn LandingMaterializer>` and drops the raw
`cp`/`store` from the handler's direct use (they move into `DuckLakeMaterializer`;
`cp` stays in `AppState` only if still needed for non-landing concerns — it is
not, so `AppState` becomes `{ materializer }`).

### 3. Iceberg routing by byte size (`iceberg_landing::land`, postgres crate)

```rust
// control_plane_postgres::iceberg_landing
pub async fn land(
    pool: &PgPool,
    catalog: &SqlCatalog,
    table: &TableRef,
    columns: &[ColumnSpec],
    ipc_body: &[u8],
    inline_byte_limit: usize,
    lineage: LineageEvent,
) -> Result<SnapshotId> {
    let (schema57, batches57) = decode_ipc_57(ipc_body)?;       // arrow-57 decode
    let bytes: usize = batches57.iter().map(|b| b.get_array_memory_size()).sum();
    if bytes <= inline_byte_limit {
        let batch = concat_batches(&schema57, &batches57)?;     // arrow-57
        inline_append(pool, table, columns, &batch, lineage).await   // atomic lineage (Slice A)
    } else {
        land_parquet(pool, catalog, table, columns, batches57, lineage).await // §4 + §5
    }
}
```

- `get_array_memory_size()` is in-memory (uncompressed) size — an over-estimate
  vs encoded Parquet, but deterministic and monotonic, which is what routing
  needs. `LOOM_INLINE_BYTE_LIMIT` is read in `main` (default **16 MiB =
  16_777_216**; tunable). The default favours inlining: a single small request
  (a handful of rows, kilobytes in memory) inlines, and only a sizeable bulk
  load writes Parquet. The exact default is a knob, not a contract. Note the
  read-side cost: inline rows are reconstructed and re-encoded to in-memory
  Parquet on every query through the union view (Slice A), so until the deferred
  flush/compaction lands, a higher limit means more PG-resident data re-encoded
  per read — which makes that follow-up more valuable.
- `inline_append` takes a single `RecordBatch`; `iceberg_landing::land`
  concatenates the decoded batches with `arrow_select::concat::concat_batches`
  (arrow-57) before calling it. (The inline contract requires positional column
  alignment; `columns` is derived from the same wire schema, so the order
  matches.)

### 4. Atomic lineage on the Iceberg Parquet path

`append_batches` commits through iceberg's `Transaction::commit(catalog)`, which
wraps a retry/rebase loop around `catalog.update_table(table_commit)` — the
`TableCommit` is built internally and not exposed, so we can neither extract it
nor reimplement the loop cleanly. Instead we inject lineage **at the catalog
boundary** with a per-call decorator:

1. **Refactor `SqlCatalog::update_table`** body into
   `pub(crate) async fn do_update_table(&self, commit: TableCommit, lineage: Option<&LineageEvent>) -> Result<Table>`.
   The trait `update_table` delegates with `None`. After `project_mirror(&mut tx, …)`
   and before `tx.commit()`, when `lineage` is `Some(ev)`, call
   `pg_emit(&mut *tx, ev)` — same tx as the pointer CAS + mirror projection, so
   the lineage event is atomic with the snapshot it describes and rolls back with
   it on a lost CAS.
2. **`LineageEmittingCatalog<'a>`** — a thin newtype `{ inner: &'a SqlCatalog,
   lineage: &'a LineageEvent }` that `impl iceberg::Catalog` by delegating every
   method to `inner`, **except** `update_table`, which calls
   `inner.do_update_table(commit, Some(self.lineage))`. (`Transaction::commit`'s
   retry loop calls `load_table` and `update_table`; all 14 trait methods are
   delegated so the wrapper is a valid `dyn Catalog`.)
3. **`append_batches_with_lineage(catalog, table, batches, lineage)`** in
   `iceberg_writer` — identical to `append_batches` but the final commit is
   `tx.commit(&LineageEmittingCatalog { inner: catalog, lineage })`. The lineage
   is re-presented on each retry attempt and only persists on the winning,
   committed attempt.

`land_parquet` (postgres crate) then: ensures namespace + table exist
(create-if-absent, schema from `columns`), loads the `Table`, builds the
arrow-57 batches, and calls `append_batches_with_lineage`, returning the mirror
snapshot id.

### 5. Table create-if-absent (Iceberg Parquet path)

Unlike DuckLake's idempotent `cp.create_table`, the Iceberg path must create the
namespace and table on first landing. `land_parquet` does:
`namespace_exists`/`create_namespace`, then `table_exists`/`create_table`
(building an iceberg `Schema` from `columns` via `iceberg_physical_type`), then
`load_table`. Idempotent: existing namespace/table are loaded, not recreated.
This mirrors the test seeder sequence
(`create_namespace` → `create_table` → `load_table` → append).

### 6. Snapshot id returned

Both Iceberg branches return the loom **mirror** snapshot id (the
`iceberg_mirror` `next_snapshot` value), so the HTTP response shape
(`{ snapshot_id, dataset }`) is identical across all three backends. The inline
path already returns it; the Parquet path reads it back after commit via
`IcebergCatalog::new(pool.clone()).current_snapshot(table).await?.id`.

## Data flow (Iceberg backend)

```
POST /datasets/{schema}/{table}  (Arrow IPC)
  → decode IPC, parse headers (gate, run-id)
  → validate gate; resolve columns (infer_columns | model)   [handler, once]
  → build LineageEvent
  → materializer.land(LandRequest)
       IcebergMaterializer (thin) → iceberg_landing::land (postgres, arrow-57):
         decode IPC (arrow-57); bytes = Σ get_array_memory_size
         ≤ limit → inline_append(pool, …, lineage)            [1 pg tx, lineage ✓]
         > limit → create-if-absent → append_batches_with_lineage
                     → tx.commit(LineageEmittingCatalog)
                       → do_update_table: CAS + project_mirror + pg_emit  [1 pg tx, lineage ✓]
  → { snapshot_id, dataset }
```

## Error handling

- Gate failure → 422 with violations (unchanged; now in the handler).
- Unsupported column type (`infer_columns` / `iceberg_physical_type` `None`) →
  400 (unchanged for infer; the Parquet path maps a missing iceberg type to a
  400-class `IngestError`).
- Backend faults (catalog, Postgres, object store) → 500 opaque (unchanged
  governance posture: no internal detail echoed).
- CAS conflict inside `do_update_table` rolls back mirror + lineage together and
  surfaces a retryable error to iceberg's backoff (unchanged for the non-lineage
  path; lineage simply rolls back with the tx).

## Testing

All tests are `rust_test` / `loom_fixture_test` targets (no inline `#[cfg(test)]`).

1. **`parse_landing_backend`** (pure unit, RE): unset/empty/`ducklake`/`iceberg`/
   error cases. Mirror `parse_serving_backend`'s test.
2. **Byte-size routing** (pure unit, RE): a small batch routes inline, a large
   batch routes Parquet — assert via a fake/spy `IcebergMaterializer` split, or
   assert the boundary helper `route(bytes, limit)` directly. Boundary: `== limit`
   → inline (≤).
3. **`iceberg_landing::land` inline routing + lineage** (fixture, local, postgres
   crate): a small IPC body lands inline; assert the rows are readable via the
   mirror and a lineage event exists for the run (`events_for`).
4. **`iceberg_landing::land` Parquet routing + atomic lineage** (fixture, local,
   postgres crate): an over-limit IPC body; assert (a) the Parquet snapshot is
   live in the mirror, (b) a lineage event with the right output dataset exists,
   (c) the returned snapshot id matches the mirror's current snapshot. Load-bearing
   for §4 — without the wrapper, lineage would be absent.
5. **`append_batches_with_lineage` atomicity** (fixture, local, postgres crate):
   a successful append emits exactly one lineage event in the same tx; reuse the
   existing `iceberg_write_roundtrip` harness + assert lineage.
6. **DuckLake landing unchanged** (existing ingest materialize/http tests): the
   refactor to `DuckLakeMaterializer` is behaviour-preserving; existing tests must
   still pass against the new trait seam.
7. **`parse_landing_backend`** (unit, RE, ingest crate): unset/empty/`ducklake`/
   `iceberg`/error cases.
8. **Iceberg backend end-to-end via HTTP** (fixture, local, ingest crate): build
   an `IcebergMaterializer`-backed `AppState`, POST a small Arrow IPC body, assert
   200 + `snapshot_id`, and that the mirror + lineage reflect it. Proves the
   thin-forwarder wiring (`ipc_body` threading, byte limit) end to end.

## Files

**postgres crate (arrow-57 native):**

- **Modify** `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` —
  extract `do_update_table(commit, lineage: Option<&LineageEvent>)`; trait
  `update_table` delegates with `None`; emit `pg_emit(&mut *tx, ev)` after
  `project_mirror` when `Some`.
- **Modify** `src/control-plane/postgres/src/iceberg_writer.rs` — add
  `append_batches_with_lineage` + `LineageEmittingCatalog`.
- **Create** `src/control-plane/postgres/src/iceberg_landing.rs` — `land(...)`
  (IPC decode arrow-57, byte routing, inline/parquet), `land_parquet`
  (create-if-absent + `append_batches_with_lineage` + snapshot read-back),
  `decode_ipc_57`. Export from `lib.rs`.
- **Modify** `src/control-plane/postgres/BUCK` — `iceberg_writer.rs`/landing tests
  (the postgres lib already has arrow-57 + iceberg deps); add an `arrow-ipc`
  (arrow-57) third-party dep if not already present for `decode_ipc_57`.

**ingest crate (arrow-58):**

- **Create** `src/services/ingest/src/landing.rs` — `LandingBackend`,
  `parse_landing_backend`, `LandingMaterializer` trait, `LandRequest`,
  `DuckLakeMaterializer`, `IcebergMaterializer` (thin forwarder).
- **Modify** `src/services/ingest/src/materialize.rs` — split gate/schema head
  (→ handler) from write tail (→ `DuckLakeMaterializer`). May fold into
  `landing.rs`.
- **Modify** `src/services/ingest/src/http.rs` — gate + column resolution + lineage
  build in `land`; pass `ipc_body: &body`; `AppState { materializer }`; dispatch
  to `materializer.land`.
- **Modify** `src/services/ingest/src/main.rs` — read `LOOM_LANDING_BACKEND` +
  `LOOM_INLINE_BYTE_LIMIT`, construct the chosen materializer (Iceberg builds
  `SqlCatalog` from DSN + `file://` warehouse via `SqlCatalogBuilder`).
- **Modify** `src/services/ingest/src/lib.rs` — export `landing`.
- **Modify** `src/services/ingest/BUCK` — ingest lib gains deps on
  `//src/control-plane/postgres:postgres`, `//third-party:iceberg`,
  `//third-party:sqlx` (for `PgPool`); new `rust_test`/`loom_fixture_test`
  targets.

**docs:**

- **Modify** `docs/spike/ICEBERG_ROADMAP.md` — mark Slice B done.

## Out of scope (deferred)

- Flush/compaction of inline rows → Parquet (restores external Iceberg-client
  visibility). Still a follow-up.
- Per-column stats / pruning, overwrite/replace, multi-writer perf, S3 warehouse
  — unchanged from the roadmap's deferred list.
- A real PG `TableProvider` for transforms — explicitly deferred earlier.
