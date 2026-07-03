# Tx trait interface segregation + Auth on the ControlPlane facade

- **Date:** 2026-07-03
- **Status:** approved
- **Register item:** `fut-tx-trait-segregation` (promoting to ROADMAP)

## Problem

Two core-trait blemishes, found by the pillar-idioms audit:

1. **`core::Tx` bundles two surfaces.** The backend-neutral unit of work
   (`enqueue`/`emit`/`commit`/`rollback`) and the table-format staging surface
   (`create_table`/`append_files`/`replace_files`/`compact_files`) live on one trait
   (`src/control-plane/core/src/transaction.rs`). `PgTx`
   (`src/control-plane/postgres/src/transaction.rs`) stubs all four staging methods with a
   runtime `ControlPlaneError::Validation` ("carries no table-format writer") — the type
   system says any `Tx` can stage files; reality says only `IcebergTx`
   (`postgres/src/iceberg_control_plane.rs`) and `MemoryTx` (`memory/src/transaction.rs`)
   can. Every new table-format method adds another dead stub, and `fut-wider-tx-composition`
   would multiply that surface if it lands first.
2. **`Auth` is a facade orphan.** It is a sixth concern with a core trait and both adapter
   impls (`postgres/src/auth.rs`, `memory/src/auth.rs`), but `ControlPlane` exposes only
   `catalog`/`ontology`/`acl`/`lineage`/`queue`. Services must hold the concrete adapter to
   authenticate: `service_runtime::bootstrap` builds `AuthState` from its concrete
   `Arc<PgControlPlane>` (`services/runtime/src/lib.rs`), and anything holding only
   `Arc<dyn ControlPlane>` (e.g. query-api's `WireControlPlane`) cannot reach `Auth` at all.

## Design

### 1. Split `Tx` / `TableTx: Tx`, and `ControlPlane` / `TableControlPlane: ControlPlane`

In `core/src/transaction.rs`:

- **`Tx`** keeps the backend-neutral unit of work: `commit(self: Box<Self>) ->
  Result<Option<SnapshotId>>`, `rollback`, `enqueue`, `emit`. `commit`'s
  `Option<SnapshotId>` return **stays on `Tx`** — splitting commit itself would force a
  second terminal method and upcast churn for zero safety gain; `PgTx` keeps returning
  `Ok(None)` (accepted residue, note it on the doc comment, which must stop naming
  `create_table`/`append_files`).
- **`TableTx: Tx`** gets the four staging methods verbatim: `create_table`,
  `append_files`, `replace_files`, `compact_files`.
- **`TableControlPlane: ControlPlane`** adds `async fn begin_table(&self) ->
  Result<Box<dyn TableTx + Send>>`. This is required because consumers reach a `Tx` only
  through `ControlPlane::begin() -> Box<dyn Tx + Send>`; with staging methods gone from
  `Tx`, table-writing consumers need a begin that yields the wider object. `begin()` keeps
  its signature and stays on the base facade.
- Export both new traits from `core/src/lib.rs`.

**Who implements what:**

- `PgTx`: **`Tx` only.** Delete the four stubs and the `no_table_format()` helper.
  `PgControlPlane` does **not** implement `TableControlPlane`.
- `IcebergTx` / `MemoryTx`: split the existing single `impl Tx` into `impl Tx` (unit of
  work) + `impl TableTx` (staging). `IcebergControlPlane` / `MemoryControlPlane` implement
  `TableControlPlane`, moving today's `begin()` body into `begin_table()`; `begin()`
  becomes `Ok(self.begin_table().await?)` — the `Box<dyn TableTx + Send>` →
  `Box<dyn Tx + Send>` dyn-upcasting coercion is stable and loom's nightly has it.
- `WireControlPlane` (query-api): unchanged — plain `ControlPlane`, `begin()` keeps its
  `read_only` error (a deliberate read-only-plane guard, not a capability stub).
- The transform test stub in `services/transform/tests/run_unknown_input.rs` grows the
  matching `TableTx`/`TableControlPlane` impls (test code is lint-exempt).

**Object safety / generics:** every consumer uses `Tx` as a boxed trait object (`begin()`
returns `Box<dyn Tx + Send>`; there are no generic `impl Tx` parameters anywhere under
`src/`), so the split is purely a dyn-vtable question. Supertrait methods (including
`commit(self: Box<Self>)`) are callable directly on `Box<dyn TableTx + Send>`; no caller
needs an explicit upcast except the adapter `begin()` delegation above.

**Consumers, mapped (the only production caller of staging methods is transform):**

- `services/transform/src/run.rs` (`run_transform`), `typed.rs` (`run_typed_transform`),
  `handler.rs` (both handlers): change `cp: &dyn ControlPlane` to
  `cp: &dyn TableControlPlane`, and `cp.begin()` to `cp.begin_table()`. No production
  binary wires these yet (transform workers are unbuilt), so the ripple is transform's own
  tests (`transform_e2e*`, `iceberg_backend_e2e`, `transform_chain_e2e`,
  `run_unknown_input`) — all already pass Iceberg or Memory planes.
- `testkit` (`src/control-plane/testkit/src/lib.rs`): the staging contracts —
  `snapshot_commit_contract`, `snapshot_replace_contract`, `snapshot_write_order_contract`,
  `snapshot_compact_contract`, and `tx_atomic_rollback_contract` (stages a conflicting
  compaction) — retighten their bound to `TableControlPlane` and call `begin_table()`.
  They already run only against Memory (`memory/tests/{snapshot,tx}.rs`) and
  `IcebergControlPlane` (`postgres/tests/iceberg_control_plane.rs`), never `PgControlPlane`
  — confirming the split matches actual usage. `queue_contract`, `lineage_contract`,
  `tx_isolation_contract` stage no files and keep the plain `ControlPlane` bound (they DO
  run against `PgControlPlane`).
- `datafusion-io` only mentions `append_files` in docs; no code change.

### 2. Add `auth()` to the `ControlPlane` facade

**Decision: add it.** Investigation found no reason for the asymmetry — `Auth` has the
same shape as the other five concerns (core trait, both adapter impls, a testkit
`auth_contract`), and the gap forces `bootstrap` and query-api wiring to pass a second
`Arc` alongside the facade. One accessor on `ControlPlane`:

```rust
/// The authentication surface (sessions, service tokens, credentials).
fn auth(&self) -> &(dyn Auth + Send + Sync);
```

One line per adapter: `PgControlPlane` → `self`; `MemoryControlPlane` → `self`;
`IcebergControlPlane` → `self.pg.auth()`; `WireControlPlane` → `self.direct.auth()`
(auth is Postgres-backed, exactly like its `queue()`/`lineage()` delegation).
Extend `control_plane_facade_contract` with an `auth()` probe (e.g. resolving an unknown
session hash returns `Ok(None)` through the facade). Rewiring `AuthState`/`bootstrap` to
consume the accessor is optional and NOT required by this slice — `AuthState.auth:
Arc<dyn Auth>` remains a fine narrow dependency; the accessor closes the facade gap.

### 3. Migration mechanics

Single PR, compile-driven: (1) split the core traits + add `auth()`; (2) fix the four
adapters (delete the `PgTx` stubs and `no_table_format()`; split the Iceberg/Memory
impls; add the four `auth()` one-liners); (3) retighten transform signatures and testkit
bounds; (4) fix tests/stubs until `buck2 test //src/...` is green. No data or wire-format
change; no register-schema change beyond promoting the item.

## Acceptance criteria

- Compile-time: `PgTx` no longer implements any table-staging method; the four runtime
  `Validation` stubs and `no_table_format()` are deleted from
  `postgres/src/transaction.rs`.
- `TableTx: Tx` and `TableControlPlane: ControlPlane` exist in core and are exported;
  `IcebergTx`/`MemoryTx` implement `TableTx`; transform compiles against
  `&dyn TableControlPlane`.
- `auth()` is reachable on all four `ControlPlane` impls and covered by
  `control_plane_facade_contract`.
- Full suite green: `buck2 test //src/...` (fixture slots capped per the `-j 8` note).

## Out of scope

- `fut-wider-tx-composition` itself (ontology/ACL writes inside a `Tx`) — this split is
  its prerequisite, not its implementation.
- The `Ontology`-side segregation: when `fut-vector-index-drop` lands, `Ontology` should
  grow a `VectorIndexRegistry` sub-trait rather than more methods. Stated here as a
  future note only; not designed in this spec.
- Making `WireControlPlane::begin()` type-safe (read-only-plane modelling) and any
  `AuthState`/`bootstrap` rewiring beyond the accessor.
