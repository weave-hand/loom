# Type↔table lineage layer-join — a binding edge across the seam

- **Date:** 2026-07-01
- **Area:** lineage
- **Register items:** builds [[road-type-table-lineage-join]] (promotes [[fut-type-table-lineage-join]])
- **Status:** spec (ready for a work agent to plan + build)
- **Depends on:** [[road-lineage-read-maturation]] (the transitive-closure read machinery this
  composes with — already landed). Consistent with [[road-dataset-naming-bridge]] for the
  `TableRef`/`TypeName` → `DatasetRef` mapping (this spec consumes that mapping, it does not
  redefine it).

## North star

A single provenance read that starts at a **type** node reaches the **table** node that
backs it — and the table's ingest ancestry — in one graph walk, and vice versa. Today loom
records provenance in two disjoint layers: typed transforms emit **type-named** nodes
(`TypeId::dataset_ref()`, namespace `loom:type`), while ingest landings and physical
transforms emit **table-named** nodes (`DatasetId::dataset_ref()`, namespace `loom`). The
`2026-06-15-typed-transforms-part1` slice deliberately deferred joining them — the backing
table ref is kept only in the typed event's *payload*, so the linkage is traceable by a
human reading JSON but is **invisible to the closure walk**. This slice adds the missing
edge so `upstream`/`downstream` cross the type↔table seam automatically.

## Problem

The lineage graph is per-event input/output co-membership over `lineage.event_dataset`
(`{event_id, direction, namespace, name}`); the matured closure reads walk it with a
depth-bounded `WITH RECURSIVE` CTE (postgres) / visited-set BFS (memory), output→input for
`upstream`, input→output for `downstream`.

Consider a typed transform that derives type `Y` from type `X`, where `X` is bound to table
`main.customers`, itself landed by ingest from `s3://…`:

- ingest emits a **table-named** event: `output = loom/main.customers` (its upstreams are the
  `s3` sources).
- the typed transform emits a **type-named** event: `input = loom:type/X`,
  `output = loom:type/Y`.

`upstream(loom:type/Y)` reaches `loom:type/X` and **stops** — nothing in `event_dataset`
produces `X` as a *type-named* output, because `X` was produced under its *table* name.
`loom/main.customers` and its `s3` ancestry sit in a layer the walk cannot reach. The two
layers are structurally disjoint; the only thing bridging them (the payload backing ref) is
opaque to the graph.

## Approach

Emit a **binding edge**: a distinguished lineage *event* that co-references the type node
and its backing table node, so the seam is a first-class edge in the very graph the closure
already walks. Reusing an ordinary `LineageEvent` (not a new relation, not a new
`EventType`) is the headline simplicity win — **the closure crosses it with zero CTE / BFS
changes**.

- **Shape:** `inputs = [table DatasetRef]`, `outputs = [type DatasetRef]`. The physical rows
  of the table are *consumed to constitute* the type, so the table is **upstream** of the
  type. This is the direction that makes `upstream(type)` reach the table and `downstream(table)`
  reach the type (see *Data flow*).
- **EventType:** `Complete` (a completed fact; no new enum variant — keeps the contract
  stable). A payload marker `{"loom.kind": "type-table-binding"}` distinguishes it for any
  future filter/GC without affecting traversal.
- **Node naming:** the two refs are exactly the ones the emit sites already use —
  `DatasetId::from(&table).dataset_ref()` (namespace `loom`, name `schema.table`) and
  `TypeId::from(&name).dataset_ref()` (namespace `loom:type`, name `TypeName`). When
  [[road-dataset-naming-bridge]] lands a deployment-context-aware mapping, the binding edge
  picks it up for free — it constructs its refs through the same `identity.rs` seam.

### Emit site: inside `define_type`

The edge is emitted **inside `define_type`**, in each adapter, atomically with the type
upsert — **not** in the ingest `bind()` service function. Rationale:

- **Universal.** `ObjectType.table` is always present, so `define_type` always knows the
  backing table. Every type↔table binding is recorded regardless of whether it arrived via
  `bind()` (dataset→model promotion), a raw `define_type` (engine typed-transform setup,
  tests), or a future path. `bind()` delegates to `define_type`, so it gets the edge for
  free — **no `bind()` signature change**.
- **Atomic.** postgres `define_type` already opens its own transaction and upserts
  `ontology.object_type` + rewrites `property`/`derived_property`. The binding event is
  emitted in that **same transaction** via the existing `pg_emit` free function (a
  `pub(crate)` helper taking any `PgExecutor`). A committed type definition therefore always
  carries its binding edge — no crash window between "type defined" and "edge emitted". This
  is the same blessed cross-concern-within-one-adapter write the P5 lineage spec introduced
  (`Tx::emit` committing lineage + queue as one unit); here ontology + lineage commit as one
  unit. It is **not** cross-crate coupling — `PgControlPlane` implements every concern and
  `pg_emit` lives in the same crate.
- **No catalog dependency.** `define_type` does not verify the table exists in the catalog
  (`bind()` already does, and lineage refs are deliberately opaque — a dangling ref is legal,
  exactly as external datasets are). So `define_type` gains a lineage write but no catalog
  read.

### Idempotency

`define_type` is an **upsert** (re-binding the same type must not accrete duplicate edges).
Two layers of protection:

1. **Source guard (primary).** Before the upsert, read the type's *current* backing table
   (postgres: `SELECT table_schema, table_name FROM ontology.object_type WHERE name = $1`
   inside the same tx; memory: look up the existing `ObjectType` in the map). Emit the
   binding event **only when** the type is new **or** its backing table changed. An unchanged
   re-`define_type` emits nothing new.
2. **Read-side set semantics (defense in depth).** Even if a duplicate edge slips through
   (e.g. a concurrent first-bind race), the closure's `UNION` (postgres) / visited-set
   (memory) collapses parallel edges into one graph — correctness never depends on the guard,
   only event-table tidiness does.

**Rebinding to a different table** appends a *new* edge without retracting the old one
(lineage is append-only). The type then shows both bindings in its ancestry, which is
*historically accurate* provenance. Edge retraction/tombstoning is out of scope (see
*Non-goals* / *Open questions*).

## Components / interfaces

No `core` trait signature changes. The work is adapter-internal plus a testkit contract.

- **`control-plane-core` (`identity.rs`)** — already provides `From<&TableRef>` /
  `From<&TypeName>` for `DatasetRef` and the `DatasetId`/`TypeId` wrappers. A small helper —
  `fn type_table_binding_event(ty: &ObjectType) -> LineageEvent` — could live in `core`
  (pure logic, no I/O) so both adapters build the identical event (marker payload, direction,
  `EventType::Complete`, fresh `RunId`). Placing it in `core` keeps the two adapters from
  drifting.
- **`control-plane-postgres` (`ontology.rs::define_type`)** — after the `object_type` upsert,
  within the existing `tx`: if the pre-read shows a new/changed binding, call
  `pg_emit(&mut *tx, &type_table_binding_event(&ty))`. Requires a pre-`SELECT` of the prior
  `(table_schema, table_name)`. New SQL → refresh `.sqlx` via `tools/sqlx-prepare.sh`.
- **`control-plane-memory` (`ontology.rs::define_type`)** — after inserting into the
  `types` map (compare against the prior entry's `table` first), push the binding event onto
  `lineage.events`. Acquire the `ontology` lock, compute the change decision, then the
  `lineage` lock (fixed lock order to avoid deadlock with any other path that takes both).
- **`control-plane-testkit`** — a new `type_table_binding_contract` (see *Testing*), run by
  both adapters' existing lineage test targets.

## Data flow — how the closure crosses the seam

With the binding event `{input: loom/main.customers, output: loom:type/X}` present, and the
transform event `{input: loom:type/X, output: loom:type/Y}`, and the ingest event
`{input: s3/…, output: loom/main.customers}`:

- **`upstream(loom:type/Y, depth=N)`** walks **output→input**: hop 1 finds the transform
  event (Y is an output) → yields `X`; hop 2 finds the **binding event** (X is an output) →
  yields `loom/main.customers`; hop 3 finds the ingest event (the table is an output) →
  yields `s3/…`. The type-layer read now reaches the physical table and its external source
  in one walk.
- **`downstream(loom/main.customers, depth=N)`** walks **input→output**: hop 1 finds the
  binding event (the table is an input) → yields `loom:type/X`; hop 2 finds the transform
  event → yields `loom:type/Y`. The table-layer read now reaches every typed descendant.

The binding edge **costs one hop** of the depth budget when a walk crosses the seam — worth
noting for callers sizing `depth`, but the `LINEAGE_MAX_DEPTH` cap is unchanged. No CTE, BFS,
pagination, or cursor code changes: the edge is just another `event_dataset` row pair.

## Error handling

- The in-tx `pg_emit` shares `define_type`'s failure path — any emit error rolls the whole
  `define_type` back (atomicity), surfaced as the existing `ControlPlaneError::Backend`.
- The binding event references an opaque `DatasetRef`; a table that is absent from the
  catalog produces a *dangling* upstream node, which is legal (matches external-dataset
  behaviour) — no validation, no error. `DatasetRef` existence validation is a separate,
  deferred concern ([[fut-lineage-datasetref-validation]]).
- The source guard's pre-read failing is a normal `Backend` error, rolled back like any other
  step.

## Testing

`rust_test` integration targets only (no inline tests). A control-plane change gets the full
trilogy: **testkit contract → memory fake → postgres parity** (postgres via
`loom_fixture_test`, memory pure-logic).

New `type_table_binding_contract<CP: Ontology + Lineage + Catalog>(cp)`:

1. **Edge emitted + crosses the seam.** `define_type` a type `X` bound to `main.customers`;
   `emit` an ingest edge `s3/raw → loom/main.customers` and a transform edge
   `loom:type/X → loom:type/Y`. Assert `upstream(loom:type/Y, depth=3)` contains
   `loom/main.customers` **and** `s3/raw` (proves the walk crossed the binding), and
   `downstream(loom/main.customers, depth=2)` contains `loom:type/X` and `loom:type/Y`.
2. **Idempotent re-define.** Call `define_type(X→main.customers)` twice; assert the binding
   edge appears **once** in the graph / that no second binding event was appended (query
   `events_for` or assert the upstream set is unchanged and single-sourced).
3. **Rebind to a new table** appends a second edge; assert the type's upstream now includes
   **both** `main.customers` and the new table (append-only history), and the old edge is not
   retracted.
4. **Direction.** Assert the table is *upstream* of the type and the type is *downstream* of
   the table (guards against an accidental input/output swap).
5. **Depth accounting.** `upstream(Y, depth=1)` returns only `X` (the seam is not yet
   crossed); `depth=2` reaches the table — pins the "seam costs one hop" contract.

Also **audit `ontology_contract`**: it calls `define_type` heavily but asserts nothing about
lineage, so the new side effect is additive; add one assertion there only if a regression
guard is wanted. The existing `lineage_closure_contract` / `lineage_pagination_contract`
build their graphs via `cp.emit` (never `define_type`), so they are unaffected.

## Non-goals

- **Edge retraction / tombstoning.** Rebinding appends; it never removes the prior edge.
  Append-only is loom's lineage model; a retraction primitive is out of scope.
- **New `EventType::Binding` variant or a dedicated `lineage.type_binding` relation.** The
  edge is an ordinary event so the closure needs no changes; a distinguished representation is
  an explicit rejected alternative (see *Open questions*).
- **External HTTP surface.** This is a control-plane capability; exposing it is
  [[road-lineage-http-read]]'s job (which already walks the same closure).
- **Per-node ACL filtering / min-depth annotation / run-lifecycle stitching** — separate
  deferred items ([[road-lineage-acl-filtering]], [[fut-lineage-stitching]]).
- **Redefining the `TableRef`/`TypeName` → `DatasetRef` mapping** — owned by
  [[road-dataset-naming-bridge]]; consumed here, not restated.

## Open questions

- **Edge representation — event vs. relation.** This spec chooses a reused `LineageEvent`
  (zero closure changes, natural fit). A dedicated `lineage.type_binding(type, schema, name)`
  relation with a unique key would give *structural* idempotency (upsert, no source guard) but
  forces every closure query to `UNION` a second relation in both adapters. Chosen against for
  the CTE simplicity; revisit if binding events prove to bloat `event_dataset` materially.
- **`RunId` determinism.** A fresh `RunId` per binding event is simplest; a deterministic
  UUIDv5 of `(type, table)` would make an accidental duplicate literally identical (and enable
  a future `ON CONFLICT`-style dedup). Deferred — the source guard already prevents dups.
- **Should raw `define_type` without a live catalog table still emit?** This spec says yes
  (dangling refs are legal). If [[fut-lineage-datasetref-validation]] later forbids dangling
  loom-namespaced refs, `define_type` would need a catalog read — revisit then.
- **Rebind history vs. "current backing" reads.** Append-only means a rebound type shows both
  tables upstream. If a "current binding only" read is ever needed, that is a filtered read
  over the marker payload, not a change to emission — noted for a future consumer.
