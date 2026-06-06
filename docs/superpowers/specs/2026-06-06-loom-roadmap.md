# loom Roadmap (2026-06-06)

> Supersedes the original control-plane roadmap, now archived as
> `2026-06-03-control-plane-roadmap-design.md.old` — kept as the **as-built record
> of Step 1** (concern statuses, durable decisions, divergences from the original
> sketch). Three steps: the delivered control-plane library, hardening it per the
> critical review, then the services that consume it.
>
> Methodology unchanged: each numbered item is its own **spec → plan →
> subagent-driven implement → PR** cycle. `main` stays green.

---

## Step 1 — Control-plane library  ✅ DELIVERED

Five concerns as ports & adapters, one backend-agnostic contract per concern run
against an in-memory fake **and** real Postgres: **queue** (+ `worker`, `await_jobs`),
**catalog** (DuckLake read surface), **ontology**, **acl**, **lineage**. Cross-concern
`Tx` seam wired for `emit` + `enqueue`.

Detail: the archived as-built doc, the per-concern specs (`2026-06-04…` /
`2026-06-05…`), and `docs/FUTURE.md`. **Known shortcomings are catalogued in
`2026-06-06-control-plane-critical-review.md`** and become Step 2.

---

## Step 2 — Harden the control plane

Source of truth: `2026-06-06-control-plane-critical-review.md`. **Step 2a must land
before any Step 3 service consumes the library** (they're correctness/contract gaps
a consumer would inherit). 2b can trail. 2c is feature debt, pulled forward only when
a Step 3 consumer needs it.

### 2a — Must-fix before services depend on it

1. **Worker heartbeat.** `Worker::run` never renews its lease, so a handler outliving
   `lock_timeout` is reclaimed and **double-executed**; `Queue::heartbeat` is dead
   from the worker's side. Spawn a heartbeat task for the in-flight job + add a
   slow-handler test asserting single execution. *(Correctness; small.)*
2. **Tx isolation/concurrency contract.** Write the missing third Tx property
   (concurrent txns don't see each other's uncommitted state) into `testkit`, run on
   both adapters. This also forces the fix to the **non-atomic memory `Tx::commit`**
   (apply all staged buffers under one lock, or the contract fails). *(Correctness +
   fidelity; small.)*
3. **Catalog MVCC delete/evolve contract.** Exercise the `end`-snapshot half of the
   range predicate (drop/supersede a table, schema evolution across snapshots,
   query-before-existence) — the riskiest duplicated logic, currently tested only on
   the append path. *(Test gap; small.)*
4. **Typed qualified identity.** Introduce a shared newtype + namespacing convention
   for the cross-concern references that are currently bare strings (acl
   `RowFilter.property`, lineage `DatasetRef`) so the coupling to `TypeName`/
   `TableRef`/`ColumnDef` is visible. Validation can stay deferred; the goal is to end
   "coupled in reality, uncoupled in the compiler." *(Coupling; medium.)*
5. **Decide the `Tx` seam's future.** Either adopt a concern-agnostic staged-op model
   or keep the flat seam and add the **catalog write leg** — but resolve the useless
   `dyn ControlPlane` (only `begin()`) either way. Gates the headline
   "snapshot + lineage + enqueue atomic" feature. *(Extensibility/architecture;
   medium — do the design before service work.)*

### 2b — Trailing hardening

- `SKIP LOCKED` concurrency test (N workers / M jobs, each claimed once).
- Handler-panic policy in `Worker` (`catch_unwind` → Abandon/Retry) + test.
- Split the adapter monoliths per concern (`postgres/src/{queue,catalog,ontology,
  acl,lineage}.rs`; same for `memory`) to mirror `core`.
- Pagination/cursor convention on list reads (`list_types`, `events_for`,
  `policies_for`, `snapshots`, `files`, graph) — decide it now so it's additive.
- `tracing` spans + basic metrics around the adapter SQL.
- `.sqlx` offline metadata so pg queries are compile-time-checked.
- proptest/arbitrary round-trips for `RowFilter` and the lineage envelope.
- Wire or drop the unused `ControlPlaneError::{Conflict, Unauthorized}` variants.

### 2c — Deferred features (from `docs/FUTURE.md`)

Pull forward only when a consumer needs it:
- **Transactional catalog write** — prerequisite for Ingest/Transform to get the
  atomic snapshot+lineage+enqueue (couples to 2a#5).
- Transitive lineage closure + cycle guard; run-grouped lifecycle stitching.
- Deletion / GC / retention everywhere; orphaned-Parquet GC.
- ACL deny-override + column masking; role hierarchy.
- Multi-tenancy (`tenant_id` partitioning).

---

## Step 3 — The layers above (consume the library)

Per `ARCHITECTURE.md`. Each is its own brainstorm → spec → plan cycle; they sit *on
top of* the control-plane library and should not start until Step 2a is in.

- **Quack-over-DataFusion server shim** — the wire protocol every service exposes;
  translate inbound Quack queries into DataFusion plans. (Quack is beta — pin a
  DuckDB version; treat protocol bumps as breaking.)
- **Query API service** — ontology `resolve` → physical table, ACL `policies_for` →
  DataFusion plan rewrite (row filter pushdown + column projection), plan + serve.
- **Ingest service** — write Parquet, commit a DuckLake snapshot, emit lineage; wants
  the transactional catalog write (2c).
- **Transform workers** — built on `control-plane-worker`: consume the queue, run
  DataFusion, write snapshots, emit lineage + enqueue downstream atomically; optional
  Ballista escalation.

---

## Where we are

Step 1 complete; `main` green. Recommended immediate next move: **Step 2a #1 and #2**
(small, correctness, and they protect every future consumer), then the **2a#5 `Tx`
design** before any Step 3 service is specced.
