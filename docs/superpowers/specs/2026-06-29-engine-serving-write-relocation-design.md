# Relocate governed writes to engine-serving (slice 1 of zero-postgres query-api)

- **Date:** 2026-06-29
- **Area:** iceberg
- **Register items:** promotes [[fut-engine-serving-write-relocation]] → [[road-engine-serving-write-relocation]]; mints follow-on [[fut-query-api-wire-control-plane]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

query-api becomes a **pure wire client** — zero `control_plane_postgres` dependency,
library *and* binary. Reads already cross the engine wire (Flight SQL); this arc
completes the picture for writes and governance, unifying all DataFusion/Iceberg I/O
behind the engine and leaving query-api as a thin, governance-enforcing HTTP front.

That end-state is reached in **two slices**. This spec builds **slice 1** (writes) and
commits **slice 2** (governance reads) as the named follow-on.

## Why two slices

A `buck2 cquery` audit shows query-api's **library** target only reaches the concrete
`control-plane/postgres` crate through a single file, `src/serving_datafusion.rs`
(the `IcebergActionWriter`, which uses `iceberg_sql_catalog::SqlCatalog` and
`iceberg_landing` directly). Everywhere else — including ACL and ontology — query-api
already depends on the **`core` `ControlPlane` trait**; only `main.rs` (the binary
composition root) constructs the concrete `PgControlPlane`.

So the decoupling splits cleanly along the lib/bin line:

- **Slice 1 (this spec):** relocate the writer → query-api's **library** is
  postgres-free.
- **Slice 2 (follow-on [[fut-query-api-wire-control-plane]]):** a wire-backed
  `ControlPlane` so query-api reads ACL/ontology over the wire → the **binary** drops
  postgres too.

The tension forcing the split: "keep ACL enforcement in query-api" + "zero-postgres"
means query-api must still *read* policy/ontology without a postgres dependency — a
separate, larger problem (governance transport) from relocating the writer. Folding
both into one plan is out of scope here.

## Current state

`IcebergActionWriter` (`src/services/query-api/src/serving_datafusion.rs`) implements
query-api's `ActionEngine` trait. It holds `Arc<SqlCatalog>` + `PgPool` and routes:

- `write_object` → `iceberg_landing::land` (governed typed-insert; one inline row +
  lineage committed in one Postgres transaction, drained to Parquet later by the flush
  vertical);
- `overwrite_table` → `iceberg_landing::overwrite_parquet_snapshot` (UPDATE/DELETE
  copy-on-write).

`query-api/src/main.rs` constructs it and injects it as `Arc<dyn ActionEngine>`; the
query-api handler enforces ACL **before** calling the `ActionEngine`.

The engine wire (`engine_control.proto`, the `EngineControl` gRPC service on the
internal UDS) already owns the "engine performs the Postgres-backed Iceberg I/O"
pattern: `FlushTable`, `GcTable`, `CompactTable`, `BuildVectorIndex`. `engine-serving`
already depends on `control-plane/postgres`. Governed writes fit this surface exactly.

## Slice 1 design

### Relocate the writer

Move `IcebergActionWriter` and its `iceberg_landing::{land, overwrite_parquet_snapshot}`
calls from query-api into **engine-serving**. The `inline_byte_limit` /
`flush_byte_threshold` knobs move with it (the engine constructs the writer from its
own config), so query-api no longer carries write-tuning config. `encode_ipc_stream`
(pure Arrow IPC serialization, no postgres) **stays query-api-side** — the one-row
batch is built and encoded by the client before it crosses the wire.

### Wire surface — two new EngineControl unary RPCs

Add to `engine_control.proto`, mirroring `FlushTable`/`CompactTable`:

```proto
rpc WriteObject    (WriteObjectRequest)    returns (WriteObjectResponse);
rpc OverwriteTable (OverwriteTableRequest) returns (OverwriteTableResponse);
```

- `WriteObjectRequest`: `schema`, `name` (the `TableRef`), and `ipc` (the one-row
  Arrow IPC stream body). `WriteObjectResponse`: whatever the current `write_object`
  returns (e.g. an affected-row/lineage signal).
- `OverwriteTableRequest`: `schema`, `name`, plus the overwrite predicate +
  replacement payload the copy-on-write path needs. `OverwriteTableResponse`: the new
  snapshot id (optional, mirroring `CompactTableResponse`).

The engine binary implements both by decoding the IPC and calling the relocated
`IcebergActionWriter`. The row+lineage transaction stays atomic, unchanged, inside the
engine.

### query-api becomes a thin write client

query-api's `ActionEngine` impl becomes a wire client over the existing `EngineControl`
channel (alongside `engine_client.rs`'s read client): authorize → build one-row batch
→ `encode_ipc_stream` → `WriteObject`/`OverwriteTable` RPC → map the response/`Status`
back to the handler's `ServingError`/HTTP. `main.rs` stops constructing the concrete
`SqlCatalog`/`PgPool`-backed writer.

### Governance boundary (unchanged)

ACL enforcement stays in the query-api handler, **pre-wire**: query-api authorizes,
then sends a *pre-authorized* write to the engine. The engine is a governance-free I/O
executor — exactly mirroring the read path, where query-api compiles governed SQL and
the engine executes it blindly. A denied write never reaches the engine, so there is no
observable behavior change for clients and no policy/control-plane dependency is added
to the engine beyond what it already has.

## Error handling

Relocated writer errors map to a gRPC `Status` at the engine boundary; the query-api
client maps `Status` back to its existing `ServingError`/HTTP response shape. ACL
denial remains a query-api-side rejection (HTTP 403), never a wire round-trip.

## Testing

The existing governed-write end-to-end suite is the behavioral proof — it must stay
green, now driving through the engine wire:

- `src/services/query-api/tests/action_e2e.rs`
- `src/services/query-api/tests/update_delete_e2e.rs`
- `src/services/query-api/tests/update_delete_governance_e2e.rs`
- `src/services/query-api/tests/update_delete_tiers_e2e.rs`

Add an engine-level wire test for `WriteObject`/`OverwriteTable` (mirroring the
existing `src/services/engine/tests/wire.rs` / `flight_sql.rs`): drive the RPC against a
live engine over the UDS and assert the row + lineage committed.

All new tests are `rust_test` integration targets (never inline `#[cfg(test)]`),
fixture-backed ones via `loom_fixture_test`.

**Decoupling assertion (the slice's measurable outcome):**
`buck2 cquery 'deps(//src/services/query-api:query-api)'` no longer lists
`//src/control-plane/postgres:postgres`. (Optionally encode this as a small
buck-query-based check so the boundary cannot silently regress.)

## Scope

In scope (slice 1): relocate `IcebergActionWriter` + inline-write seam to
engine-serving; the two `EngineControl` RPCs + their proto/codegen; query-api's thin
write client; removing `control_plane_postgres` from query-api's **library** deps
(`BUCK` + `Cargo.toml`); the wire test and the green e2e suite.

Out of scope (slice 2, [[fut-query-api-wire-control-plane]]): a wire-backed
`ControlPlane` for ACL/ontology reads; removing postgres from the query-api **binary**;
any change to how reads or governance metadata are fetched.

## Risk

- Behavior is preserved: the same writer code runs, just engine-side; the governance
  boundary and the atomic row+lineage transaction are unchanged. The e2e suite — which
  already covers insert, update, delete, governance denial, and the inline/flush tiers
  — is the regression net.
- New surface is the two RPCs and the IPC round-trip for writes; both reuse established
  engine-wire patterns (unary control RPCs; Arrow IPC payloads already used by reads).
- The internal-only UDS means the new write RPCs carry no untrusted-client exposure
  (consistent with the existing wire surface).
