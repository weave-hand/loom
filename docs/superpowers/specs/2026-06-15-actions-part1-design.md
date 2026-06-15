# Design: Actions — part 1 (named typed write-backs, insert-only)

> **Status:** approved design (2026-06-15). The first slice of loom's fourth capability —
> **Actions**, the ontology's typed, governed **write-backs** — and the first time loom *mutates*
> object data on behalf of a user rather than only landing, deriving, and serving it. A named
> ontology **action** inserts a new typed object instance through the governed HTTP front door,
> executed as a **low-latency inline write** through the DuckDB/DuckLake serving layer, with a
> first live enforcement of the `Action::Write` ACL.

## Goal

Make loom run **actions**: a named, ontology-defined operation that a subject invokes over HTTP to
**create a new typed object**. Invoking an action resolves it to its target ontology type, enforces
a `Write` ACL on that type, validates typed parameters against the type's property contract, and
**inserts one new row inline** into the target's DuckLake table — low-latency, no per-write Parquet
file — committing a new DuckLake snapshot. The created object is immediately readable through the
existing governed read path.

End-to-end: **POST an action with typed params → governed (Write-ACL) → inline insert → new
DuckLake snapshot → the object reads back through `read_object`.**

This brings the **write-back** pillar online (land → derive → serve → **write**) and is the first
enforcement of the long-defined-but-unused `Action::Write`. Its genuinely-new infrastructure is the
**inline write seam**: a write-capable serving abstraction that inserts a single row inline via
DuckDB/DuckLake (not as a Parquet file), behind a trait so a future Iceberg backend swaps in.

## North star (context, not part-1)

Actions are ultimately **named, parameterized, typed mutations** ("approve order") — including
**updates** and multi-step/custom logic, governed at the HTTP API with loom owning an atomic
snapshot + lineage + enqueue commit. Part-1 builds the **named-action machinery + the inline write
seam + Write-ACL enforcement** for the **insert** case; updates, custom logic, and strict atomicity
are explicit follow-on slices that reuse this foundation.

## Decisions (settled in brainstorming)

- **A — Insert/create-only.** Part-1 actions CREATE new typed objects. The append-only
  snapshot-commit model suffices; no `Tx` update/delete leg, no compaction. Update/delete actions
  are a follow-on gated on the deferred row-supersession/compaction work.
- **B — Through-DuckDB inline writes, behind a trait.** The write executes via the
  DuckDB/DuckLake serving layer with **data inlining enabled** (`DATA_INLINING_ROW_LIMIT > 0`, vs
  the read path's `0`), so a single-row insert lands **inline in the catalog DB** — low-latency, no
  Parquet file — and DuckDB reconciles inlined rows with Parquet on read. The write path sits
  **behind an `ActionEngine` trait** so a future **Iceberg** backend replaces only that
  implementation. (loom does NOT use its native `materialize`/`write_dataset` → `Tx::append_files`
  path here — that is correct for bulk ingest/transform, wrong for low-latency single-row writes.)
- **C — Best-effort lineage (documented dangling slice).** DuckDB owns the inline-write
  transaction (it creates the DuckLake snapshot on its own connection); loom emits the lineage event
  on its own connection **after** the write. This is NOT strictly atomic — a crash in the gap can
  leave a snapshot without a lineage event — but lineage is append-only and backfillable, so part-1
  accepts the dangling window and **documents it loudly** for a future compaction/reconciliation
  pass to close. Strict atomicity is a follow-on.
- **D — Named `ActionDef` in the ontology.** Actions are a new ontology concept (a named,
  type-targeted, parameterized operation), invoked via `POST /actions/{name}` — not a generic
  `POST /objects/{type}`. Part-1 supports one action kind: "insert a new instance of the target
  type from the parameters."

## What this slice IS

- A new ontology **`ActionDef`** concept (`define_action`/`get_action`) — core types + trait, a
  postgres `ontology.action` table, the memory fake, and a testkit contract.
- A new **`ActionEngine`** write trait + an `EmbeddedDuckDbWriter` impl (inline INSERT via
  DuckDB/DuckLake) in query-api's serving module — the Iceberg-swap seam.
- A governed **`POST /actions/{action_name}`** endpoint + handler in query-api: resolve →
  **`Action::Write` ACL** → typed-param validation → inline insert → best-effort lineage → return
  the created object as typed JSON.
- **Typed parameter parsing** (the inverse of the read-side typed-JSON serialization).

## What this slice is NOT

- **No update/delete actions** — insert/create only (Decision A); gated on compaction.
- **No custom-logic / multi-step actions** — part-1's only action kind is "typed insert"; params
  map to the target type's properties. Actions whose params differ from properties or that run
  bespoke logic are a follow-on.
- **No strict snapshot+lineage atomicity** — best-effort, documented (Decision C).
- **No fine-grained write column policy** — part-1 governance is coarse `Write`-on-type. (Row
  filters / deny-write-column are a follow-on; the ACL surface already models them.)
- **No Iceberg `ActionEngine` impl** — only the DuckLake/DuckDB one; the trait is the seam.
- **No Quack/external wire** — same plain-HTTP front door as the rest of query-api.

## Design

### 1. Ontology — the `ActionDef` concept (net-new)

In `control-plane-core` (`ontology.rs`):

```rust
pub struct ActionName(pub String);

/// A typed input to an action. `ty` is the ontology's logical vocabulary (like PropertyDef.ty).
pub struct ParamDef { pub name: String, pub ty: String, pub required: bool }

/// A named ontology operation. Part-1 semantics: insert one new instance of `target`,
/// taking a value for each parameter.
pub struct ActionDef { pub name: ActionName, pub target: TypeName, pub parameters: Vec<ParamDef> }
```

New `Ontology` trait methods:
- `async fn define_action(&self, action: ActionDef) -> Result<()>` — upsert by name.
- `async fn get_action(&self, name: &ActionName) -> Result<ActionDef>` — `NotFound` if absent.
- (`list_actions` deferred — not needed for part-1.)

**Storage** (postgres adapter): a new `ontology.action` table — `name` (PK), `target_type`, and
`parameters JSONB` (the ordered `Vec<ParamDef>`). One migration; mirror the existing ontology
adapter style. The **memory fake** gets a matching `HashMap`, and the **testkit** gets a
define/get round-trip contract test (run against both adapters).

**Note:** part-1 does not require `parameters` to mirror `target`'s properties at `define_action`
time (validation happens on invoke, §3). Keeping them independent is what lets later action kinds
diverge params from properties; for the insert kind the invoke-time conformance check ties them
together.

### 2. The write seam — `ActionEngine` (the Iceberg-swap point)

In query-api (`serving` module, beside `ServingEngine`):

```rust
#[async_trait]
pub trait ActionEngine: Send + Sync {
    /// Insert one row into `table` (DuckLake) INLINE — low-latency, no Parquet data file —
    /// and return the new DuckLake snapshot id. A future Iceberg backend replaces only this.
    async fn insert_row(&self, table: &TableRef, columns: &[String], values: &[SqlValue])
        -> Result<SnapshotId, ActionEngineError>;
}
```

Impl **`EmbeddedDuckDbWriter`**: ATTACH the DuckLake catalog with **inlining enabled**
(`DATA_INLINING_ROW_LIMIT > 0`), run a parameterized `INSERT INTO <schema>.<table> (cols...) VALUES
(?...)` binding `values` positionally, then obtain the **new snapshot id**. Constructed like
`EmbeddedDuckDb` (a postgres conn string + data path).

**Key implementation risk / Decision E — the inline-write mechanism.** Three points must be
confirmed empirically (the §6 e2e is the oracle; the plan front-loads a small spike before building
the rest):
1. **Inlining actually inlines** — with `DATA_INLINING_ROW_LIMIT > 0`, a single-row INSERT lands
   inline in the catalog DB and does **not** write a Parquet data file (assert no new
   `ducklake_data_file` / no new `.parquet` for the write).
2. **Snapshot-id read-back** — after the INSERT, obtain the snapshot it produced. Preferred: loom
   reads it via the existing catalog path (`cp.catalog().current_snapshot(table)` against the same
   Postgres catalog DuckLake just wrote) rather than parsing DuckDB internals — clean and
   adapter-native. The trait still returns `SnapshotId`; how the impl gets it is internal. (If
   `current_snapshot` proves racy/insufficient, fall back to a DuckLake snapshot query inside the
   writer's connection — decide in the spike.)
3. **Reads reconcile inlined rows** — the existing read engine (`EmbeddedDuckDb`, ATTACH with
   `DATA_INLINING_ROW_LIMIT 0`) returns inline-written rows. `0` disables the *reader's own*
   inline-writes; it must not hide *reading* inlined data. Confirm in the spike; if reads miss
   inline data, adjust the read ATTACH config (a small, contained change).

### 3. HTTP endpoint + handler — `POST /actions/{action_name}`

Request body: a JSON object of parameter name → value. Handler flow (query-api):
1. Parse subject from the `X-Loom-Subject` header (as the read handlers do); parse `action_name`.
2. `ontology.get_action(name)` → `ActionDef` (`404` UnknownAction).
3. `ontology.get_type(action.target)` → `ObjectType` (target type, its `table`, its `properties`).
4. **ACL:** `acl.check(subject, Action::Write, &PolicyTarget::Type(target))` == `Allow`, else `403`
   — the first live enforcement of `Action::Write`.
5. **Validate params** (§4): every `required` parameter present; each value parsed to a `SqlValue`
   per its logical type; the resulting row **conforms** to the target type's property contract
   (reuse the logical-type `satisfies` vocabulary; missing/mistyped → `400`).
6. **Insert:** `action_engine.insert_row(&target.table, &columns, &values)` → `SnapshotId`.
7. **Best-effort lineage** (§5) — emit; on failure, log and continue (do not fail the action).
8. Respond `201` with the created object rendered as typed JSON (reuse `objects_to_json`'s value
   rendering).

Wired into the existing axum router (`http.rs`) as a `post(...)` route alongside the GET routes.
Errors map like the read handlers (`404`/`403`/`400`/`500`).

### 4. Typed parameter parsing

The inverse of the read-side typed-JSON serialization: a small `params` module parses the JSON body
into `Vec<(column, SqlValue)>` keyed by the action's `ParamDef`s — Long from a JSON string, Double
from a number, Boolean from a bool, String/aliases from a string, Date/Timestamp from ISO strings —
reusing the logical-type/`json_repr` vocabulary. Unknown params, missing-required params, and
type-mismatched values are `400` with a clear message. Pure logic, unit-tested.

### 5. Lineage — best-effort, type-named (Decision C)

After a successful insert, emit on loom's own connection:
`LineageEvent { run_id: fresh, event_type: Complete, outputs: vec![DatasetRef::from(&target)],
inputs: vec![], payload: json!({ "action": name, "snapshot_id": ..., "params": <summary> }) }`.
`outputs` reuses the **type-named** `DatasetRef` (`From<&TypeName>` / `TypeId`, namespace
`loom:type`) shipped in the typed-transforms slice, so actions join the Object-Model lineage graph.
A failure to emit is logged, not fatal — the **documented dangling slice**: a snapshot may briefly
lack its lineage event; a future compaction/reconciliation pass closes it. This relaxation is called
out in `docs/FUTURE.md`.

### 6. Testing

- **`ActionDef` round-trip** — `define_action`/`get_action` testkit contract, run against the
  memory fake and the postgres adapter (fixture).
- **Typed-param parsing** unit tests — happy path + each rejection (unknown/missing/mistyped).
- **Write-ACL enforcement** — a subject with no `Write` grant on the target type gets `403`; a
  granted subject succeeds. (Unit/handler-level with a fake or fixture.)
- **The inline-write spike** (early, de-risking) — a focused fixture test proving Decision E:
  inlining writes no Parquet file, the snapshot id is obtainable, and an inline-written row reads
  back. This gates building the handler on top.
- **Action e2e fixture** (Postgres + DuckDB): define a bound type (e.g. `Customer`) + an insert
  action (`createCustomer`) → grant `Write` → `POST /actions/createCustomer` with typed params →
  assert the response is the created object, the object **reads back through `read_object`**
  (write→read round-trip with inlining), a **lineage event** records the type-named output, and the
  write **inlined** (no new Parquet data file).

### File structure

- **Create (core):** `ActionName`/`ParamDef`/`ActionDef` + the `Ontology` action methods in
  `src/control-plane/core/src/ontology.rs`; exports in `lib.rs`; a testkit contract.
- **Modify (postgres):** `src/control-plane/postgres/` — a migration for `ontology.action`, the
  adapter methods (+ `.sqlx` cache), and the memory fake in `control-plane/memory`.
- **Create (query-api):** the `ActionEngine` trait + `EmbeddedDuckDbWriter` in the `serving`
  module; a `params` parsing module; the action handler (`handler.rs`/new `action.rs`) and the
  `POST /actions/{name}` route in `http.rs`; the binary wires the writer alongside the reader.
- **Tests:** the spike fixture, the action e2e fixture, param-parsing + Write-ACL unit tests, the
  ActionDef contract.
- **Modify (docs):** `docs/FUTURE.md` (the dangling-lineage + strict-atomicity follow-up, update
  actions, fine-grained write column policy); `docs/superpowers/specs/2026-06-06-loom-roadmap.md`
  (Actions part-1 delivered).

## Decisions

- **A — insert-only** (no update/delete; no compaction).
- **B — through-DuckDB inline writes behind the `ActionEngine` trait** (Iceberg-swappable; not
  loom-native Parquet).
- **C — best-effort, type-named lineage** (documented dangling slice; not strictly atomic).
- **D — named `ActionDef`** ontology concept; `POST /actions/{name}`.
- **E — the inline-write mechanism** (§2) is verified by the spike + e2e oracle: inlining writes no
  Parquet file; snapshot-id obtained via the catalog read-back; reads reconcile inlined rows.

## Follow-ups (later slices)

- **Update/delete actions** (with the deferred row-supersession/compaction slice).
- **Custom-logic / multi-step actions** (params ≠ properties; computed columns; enqueue downstream).
- **Strict snapshot+lineage+enqueue atomicity** (close the dangling window — loom-owned DuckLake
  write or a reconciliation pass at compaction).
- **Fine-grained write governance** (row filters / deny-write-columns on `Write`).
- **The Iceberg `ActionEngine` impl** (the trait's reason for being).

## Roadmap

Lands under Step 3 → **Actions**, part-1 — the write-back pillar's load-bearing primitive, bringing
`Action::Write` into live enforcement and establishing the inline write seam that updates, custom
actions, and an Iceberg backend all build on. Completes the platform's verb set: land, derive,
serve, **write**.
