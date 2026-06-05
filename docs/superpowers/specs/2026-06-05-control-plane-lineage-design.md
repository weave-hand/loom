# Design: Control-Plane Lineage (Phase 5)

> **Status:** approved design for the control plane's fifth and final roadmap concern —
> lineage. Sits under the umbrella roadmap (`2026-06-03-control-plane-roadmap-design.md`).
> Built in **one cycle** (like queue/ontology/acl): a loom-owned schema, append-mostly,
> no external substrate — trait → contract → fake → pg adapter + migration.
>
> **What makes P5 different:** it is the **first real cross-concern transaction**. `Tx`
> gains `emit`, so a lineage event and a queue enqueue commit (or roll back) as one unit.

## Goal

Lineage is loom's **provenance record**: every snapshot-producing operation (ingest,
transform) emits an OpenLineage event describing a *run*, its input datasets, and its
output datasets. The `Lineage` trait stores those events (append-mostly) and answers two
kinds of question — *what happened in this run* (`events_for`) and *how do datasets relate*
(`upstream`/`downstream`) — satisfied by both the in-memory fake and the Postgres adapter
via one contract suite.

Because lineage shares the database with the catalog and the queue, the architecture's
headline property finally pays off: the emitted event, the downstream job enqueue, and (in
a real service) the snapshot commit can be **one atomic transaction** — no "lineage drift"
between what happened and what was recorded. P5 wires the first half of that for real:
`Tx::emit` + `Tx::enqueue` in a single unit of work.

## The transaction seam (the distinguishing decision)

The shipped `Tx` (in `core/src/transaction.rs`) is the simple object-based form, not the
roadmap's provisional closure/aggregator sketch:

```rust
trait ControlPlane { async fn begin(&self) -> Result<Box<dyn Tx + Send>>; }
trait Tx { async fn commit(...); async fn rollback(...); async fn enqueue(&mut self, NewJob) -> Result<JobId>; }
```

`Tx` carries only the **write ops that participate in atomic units** — so far just
`enqueue`. P5 adds exactly one method:

```rust
pub trait Tx: Send {
    async fn commit(self: Box<Self>) -> Result<()>;
    async fn rollback(self: Box<Self>) -> Result<()>;
    async fn enqueue(&mut self, job: NewJob) -> Result<JobId>;
    async fn emit(&mut self, event: LineageEvent) -> Result<()>;   // NEW
}
```

**Decision: a flat method, not a per-concern aggregator.** This matches the existing seam
exactly, is a one-method delta, and avoids building a generic composition framework for
concerns that don't exist. `Tx` having two write ops (`enqueue`, `emit`) is not yet a
smell. The roadmap's accessor sketch (`tx.lineage().emit(...)`) was always flagged
provisional; we decline it. (Likewise, `ControlPlane` has no per-concern accessor methods —
concern traits are `impl`'d directly on the adapter struct; that is unchanged.)

As with the queue, lineage exposes **both** a standalone autocommit `Lineage::emit` (its own
transaction) and the transactional `Tx::emit`. A shared executor-generic helper backs both
in the pg adapter (mirroring `pg_insert`).

## The dataset identity (the key modelling point)

`DatasetRef { namespace: String, name: String }` — OpenLineage's own dataset identity,
deliberately **decoupled** from `TableRef`/`TypeName`. Lineage is fundamentally an *interop*
surface (OpenLineage), so its node identity is the interop identity, not loom's internal
vocabulary. This lets the graph span everything a run touches:

- a physical DuckLake table (`{namespace: "ducklake", name: "main.orders"}`),
- an ontology type (`{namespace: "ontology", name: "Customer"}`),
- an **external** dataset with no loom catalog entry at all (a source Kafka topic, an S3
  path, another system's table) — provenance frequently starts outside loom.

Callers map a `TableRef`/`TypeName` to a `DatasetRef` by convention. This is
**store-don't-validate** (consistent with the prior cycles): `emit` does not check a dataset
exists in the catalog or ontology.

## The graph model (per-event co-membership, one hop)

Each emitted event carries `inputs` and `outputs`. The graph is a **flat projection of every
event's own input/output sets** — for a given event, every input is an upstream of every
output. No run-grouping, no lifecycle reasoning:

- `upstream(d)` = distinct `input` datasets of any event whose `output` set contains `d`.
- `downstream(d)` = distinct `output` datasets of any event whose `input` set contains `d`.

Both are **one hop** (direct edges). Rationale: it's a flat, non-recursive query in both
adapters; "correct" is unambiguous; and it sidesteps cycle-handling entirely. In practice
loom's own emitters (ingest/transform) emit a single terminal event carrying both inputs and
outputs, so per-event co-membership *is* the run for our producers.

`run_id`, `event_type`, and `event_time` are stored on every event (for audit and a future
run-grouped graph), but the graph queries do not use them.

Transitive provenance / closure (with its cycle-guard), and run-grouped lifecycle stitching
(inputs on `START`, outputs on `COMPLETE` across events of one run), are **deferred** —
recorded in `docs/FUTURE.md`.

## Trait surface (`core`)

Domain types live in `core` (`lineage.rs`); all ops `async`, returning
`Result<_, ControlPlaneError>`. Runtime-free (no tokio). No serde derive is needed on these
types: the envelope fields go to columns, and `payload` is already a `serde_json::Value`.

```rust
use uuid::Uuid;
use time::OffsetDateTime;

/// An OpenLineage run identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RunId(pub Uuid);

/// OpenLineage dataset identity. Decoupled from TableRef/TypeName so the graph can
/// span physical tables, ontology types, and external datasets alike.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DatasetRef {
    pub namespace: String,
    pub name: String,
}

/// OpenLineage run-lifecycle event type. Stored; opaque to loom's logic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventType {
    Start,
    Running,
    Complete,
    Abort,
    Fail,
}

/// A lineage event: a typed envelope (the fields loom indexes/queries) plus the full
/// OpenLineage event stored opaquely.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LineageEvent {
    pub run_id: RunId,
    pub event_type: EventType,
    pub event_time: OffsetDateTime,
    pub inputs: Vec<DatasetRef>,
    pub outputs: Vec<DatasetRef>,
    pub payload: serde_json::Value,
}

#[async_trait]
pub trait Lineage {
    /// Record an event (append-only). Its own transaction.
    async fn emit(&self, event: LineageEvent) -> Result<()>;
    /// All events for a run, in emit order (the audit read). Empty if the run is unknown.
    async fn events_for(&self, run: &RunId) -> Result<Vec<LineageEvent>>;
    /// One hop: datasets that fed directly into a run producing `dataset`
    /// (order unspecified). Empty if `dataset` is unknown.
    async fn upstream(&self, dataset: &DatasetRef) -> Result<Vec<DatasetRef>>;
    /// One hop: datasets produced directly by a run consuming `dataset`
    /// (order unspecified). Empty if `dataset` is unknown.
    async fn downstream(&self, dataset: &DatasetRef) -> Result<Vec<DatasetRef>>;
}
```

Decisions pinned here:
- **`emit` is the write op; the contract self-seeds through it** (queue/ontology/acl pattern).
- **`events_for` exists** so the typed envelope and the opaque `payload` are observable
  through the trait (closing the audit-read loop and making the `jsonb` round-trip
  contract-testable). Without it, `payload`/`run_id`/`event_type`/`event_time` would be
  write-only and unverifiable.
- **Store, do not validate** — `emit` does not parse `payload` (opaque OpenLineage) and does
  not check datasets exist in the catalog/ontology.
- **Both autocommit and transactional `emit`** — `Lineage::emit` (own tx) and `Tx::emit`
  (ambient unit of work).
- **Single-tenant**; no pagination on reads (deferred).

## Schema: `lineage` (new migration `migrations/0004_lineage.sql`)

```
lineage.event
  event_id    bigserial   primary key
  run_id      uuid        not null
  event_type  text        not null          -- 'start'|'running'|'complete'|'abort'|'fail'
  event_time  timestamptz not null
  payload     jsonb       not null

lineage.event_dataset                        -- per-event inputs/outputs; powers the graph
  event_id   bigint not null references lineage.event (event_id) on delete cascade
  direction  text   not null                 -- 'input' | 'output'
  ordinal    int    not null                 -- preserves emit order within (event, direction)
  namespace  text   not null
  name       text   not null
  primary key (event_id, direction, ordinal)
```

- **`emit` (and `Tx::emit`):** insert the `event` row (`returning event_id`), then insert its
  `input` rows (ordinals `0..`) and `output` rows (ordinals `0..`). The standalone form runs
  in an internal `self.pool.begin()` transaction (atomic event+datasets, like `define_type`);
  the `Tx` form issues the same statements against the ambient `sqlx::Transaction`. A shared
  `pg_emit<E: sqlx::PgExecutor>(ex, &event)` helper backs both (mirrors `pg_insert`).
- **`event_type`** maps to/from `'start'|'running'|'complete'|'abort'|'fail'` via small
  helpers (`event_type_to_str`/`_from_str`, like `cardinality_*`). `from_str` is needed
  because `events_for` reads events back.
- **`upstream(d)`:** `select distinct ein.namespace, ein.name from lineage.event_dataset eout
  join lineage.event_dataset ein on ein.event_id = eout.event_id and ein.direction='input'
  where eout.direction='output' and eout.namespace=$1 and eout.name=$2`. `downstream` swaps
  input/output.
- **`events_for(run)`:** events by `event_id`, then each event's inputs/outputs by `ordinal`
  (N+1, like `list_types`).
- The in-memory fake stores events in a `Vec` behind a `Mutex` (no migration). `MemoryTx`
  gains a staged-events buffer applied on commit alongside staged jobs, under one lock.

## Testing

Self-seeding contract `lineage_contract<CP: ControlPlane + Lineage + Queue>(cp)` (run against
both adapters). The `Queue` + `ControlPlane` bounds are required for the cross-concern
atomicity test — the defining P5 behavior.

- **emit → events_for round-trip:** emit an event; `events_for(run)` returns it with
  `run_id`/`event_type`/`event_time`/`inputs`/`outputs`/`payload` intact. `event_time` is
  built at **microsecond precision** so the `timestamptz` round-trip is exact (a noted
  gotcha — Postgres `timestamptz` is µs, `OffsetDateTime` is ns). Multiple events for one run
  come back in emit order; unknown run → empty vec.
- **graph (one hop, co-membership):** emit a transform event with `inputs [A,B]`,
  `outputs [C]`. Then `upstream(C) == {A,B}`, `downstream(A) == {C}`, `downstream(B) == {C}`,
  `downstream(C) == {}` (no event consumes C), `upstream(A) == {}`. Unknown dataset → empty.
  (Compare as sets; order is unspecified.)
- **cross-concern atomicity (headline):** open a `Tx`; `emit(event)` + `enqueue(job)`; then
  **`rollback`** → `events_for(run)` is empty AND no job is dequeued. Repeat with **`commit`**
  → the event is in `events_for` AND the job dequeues. First test exercising two concerns in
  one unit of work.

Assertions are on values/variants, never on backend-specific messages.

## Non-goals (this phase — see `docs/FUTURE.md`)

- **Transitive provenance / closure** (and its cycle-guard) — `upstream`/`downstream` are one
  hop; multi-hop ancestry is a later cycle.
- **Run-grouped lifecycle stitching** — graph is per-event co-membership; stitching a run's
  `START` inputs to its `COMPLETE` outputs across events is deferred.
- **OpenLineage payload validation** — `payload` is opaque; loom does not parse or validate
  it (the typed envelope is supplied by the caller, not derived from the payload).
- **Dataset existence validation** — `emit` does not check datasets against the
  catalog/ontology.
- **Pagination / filtering** on `events_for` and the graph reads.
- **Tenancy** — single-tenant; partitioning deferred.
- **Wider `Tx` composition** — only `enqueue` and `emit` participate; catalog/ontology/acl
  writes are not on `Tx` (no consumer yet).
- **The snapshot-commit third leg** — the full "snapshot + lineage + enqueue" atomic unit
  needs a transactional catalog write, which the catalog (read-only this far) does not expose;
  P5 wires the lineage+queue half.
