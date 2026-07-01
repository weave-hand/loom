# Custom-logic actions slice 4 — enqueue-downstream (write-then-derive)

- **Date:** 2026-07-01
- **Area:** ontology
- **Register items:** promotes [[fut-action-enqueue-downstream]] → mints [[road-action-enqueue-downstream]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

On commit, a governed action **atomically enqueues a downstream job** — a governed write
triggers derived work in the **same unit of work**. `createOrder` writes the `Order` and, iff
that write commits, enqueues a transform keyed by the new order's identity. This is the
write-then-derive primitive: the queued job is never visible to a worker unless the write it
depends on actually landed, and vice-versa.

## Current state

The atomic-enqueue plumbing already exists at the commit layer: `append_parquet_snapshot`
(`iceberg_landing.rs:149`) already takes a `jobs: &[NewJob]` slice and enqueues each within
the **same** `pool.begin()` transaction that registers the data files and emits lineage
(`iceberg_landing.rs:396`), and the `Tx::enqueue` seam (`transaction.rs:41`, "visible to
workers only if the transaction commits") is its trait-level statement. Ingest's inline path
already rides this to enqueue FLUSH jobs.

What is missing is the **action-level** wiring: an `ActionDef` cannot *declare* a downstream
job, and the query-api action write seam (`action_engine.write_object`, `action.rs:500` →
engine `IcebergActionWriter::write_object` → `land`) does not carry any job down to that
`jobs` slice. So the atomicity exists but no action can request it. This slice closes only
that gap — declaration + threading — not a new commit primitive.

## Design

### The declaration — job kind + templated payload

`ActionDef` gains an optional ordered `downstream: Vec<JobTemplate>`:

```
JobTemplate { kind: String, payload: PayloadTemplate }
```

- **`kind`** is a job kind string (e.g. the transform/FLUSH kinds the worker dispatches on),
  validated at define time against a known-kinds allowlist so a typo is a misconfiguration,
  not a silently-undispatched job.
- **`payload`** is a JSON object whose leaf values may be literals or **references** to the
  action's params and to the **written identity** of the target object (`@self.id` /
  `@self.<prop>`) — reusing slice 2's ([[road-action-computed-assignments]]) `@ref`
  resolution. So the enqueued job can carry the id of the object the action just created
  (`{ orderId: @self.id }`).

### Write path — thread the resolved jobs into the existing tx

`run_insert`/`run_mutate` already resolve the written row and know its identity. After
governance passes, the action path resolves each `JobTemplate` into a concrete `NewJob`
(payload refs filled from the params + the resolved identity) and threads the resulting
`&[NewJob]` through the write seam:

- extend `action_engine.write_object`/`overwrite_table` (and the engine-wire
  `IcebergActionWriter`) to carry an optional `jobs: &[NewJob]`;
- pass them straight into `land` → `append_parquet_snapshot(…, jobs)`, the **existing** enqueue
  point.

No new commit primitive, no new transaction: the job rides the write's `pool.begin()` and is
therefore atomic by construction — commit-or-neither. On rollback, no row, no lineage, no job.

### Define-time validation

`define_action` conformance gains: every `JobTemplate.kind` is in the allowlist; every payload
`@ref` resolves (a real param, or `@self.<prop>` naming a real property of the target); a
`@self.id` reference requires the target to declare an identity property (else the job could
never be keyed). Type-checking of payload leaves is light (JSON), but references must resolve.

### Ordering with the other slices

- If [[road-action-computed-assignments]] is present, payload refs reuse its resolver directly;
  if this slice ships first, scope payload refs to **plain param + `@self.<identity/prop>`**
  substitution (no arithmetic) and widen to full expressions when slice 2 lands.
- If [[road-action-multi-object]] is present, `downstream` attaches at the **action** level and
  its `@ref`s may name any step's `bind` (`{ orderId: @order.id }`); with single-object
  actions only `@self` is available.

### Decided (not open)

- **Declared job(s) with templated payload**, not a static fixed job and not a general
  multi-listener hook system — the bounded `Tx::enqueue` seam, exposed to the ontology.
- **Reuse the existing atomic-enqueue point** (`append_parquet_snapshot(jobs)`); this slice
  adds declaration + threading only.
- **Job kind allowlist at define time** — no free-form kinds that a worker won't dispatch.
- Applies to **Insert + Update + Delete** (all can declare downstream work; `@self.id` is the
  affected object's identity in every case).
- **No conditional enqueue** in this slice — a declared job always enqueues on commit.

## Scope

In scope: the `downstream: Vec<JobTemplate>` model (`kind` + payload template) + migration +
both adapters + testkit; define-time validation (kind allowlist, payload `@ref` resolution,
identity requirement); resolving templates to `NewJob`s on the write path and threading them
through `write_object`/`overwrite_table` → `land` → the existing `append_parquet_snapshot`
enqueue; e2e proof of atomicity (commit ⇒ job present; rollback ⇒ no job).

Out of scope: conditional / predicate-gated enqueue; a general event/hook bus with multiple
listeners; cross-object or cross-action triggers; new job kinds or worker handlers (this slice
enqueues **existing** kinds); scheduling/delay ([[fut-scheduled-jobs]]).

## Testing

testkit `Ontology` action contract (both adapters) + `loom_fixture_test` e2e through the
action + queue:

1. **Round-trip:** an `ActionDef` with `downstream` templates → `get_action` returns it
   unchanged on both adapters; an action without `downstream` round-trips identically
   (back-compat).
2. **Define-time rejection:** an unknown job `kind`, a payload `@ref` to an unknown
   param/property, and `@self.id` on a target with no identity property — each rejected.
3. **Enqueue-on-commit e2e:** a `createOrder` with `downstream=[{kind, {orderId:@self.id}}]`
   writes the order **and** leaves exactly one job on the queue whose payload carries the new
   order's identity; a worker `dequeue` sees it.
4. **Atomicity — rollback ⇒ no job:** force the write to fail after resolution (e.g. a
   constraint 422 or ACL 403) and assert **no** job was enqueued — the job never escapes a
   rolled-back write.
5. **Atomicity — commit ⇒ job visible only after commit:** the job is not dequeueable until the
   action's transaction commits (rides `Tx::enqueue` semantics).
6. **Update/Delete downstream:** an Update and a Delete action each enqueue their declared job
   keyed by the affected identity.

## Risk

- **Smallest of the three** — the atomic-enqueue point already exists and is proven by ingest;
  the change is declaration + threading a `&[NewJob]` through the write seam. Blast radius is
  the `JobTemplate` model + payload resolution + the seam signature.
- **The load-bearing property is atomicity**, and it is inherited, not newly built: the job
  goes through the same `pool.begin()` as the write. Tests (4)/(5) pin commit-or-neither.
- **A templated payload referencing `@self.id`** is the one bit of real logic; it reuses slice
  2's resolver (or a scoped substitution if sequenced first), and the identity-required check
  (2) prevents an unkeyable job.
- **Kind allowlist** keeps a misconfigured action from enqueuing a job no worker dispatches —
  a define-time reject, not a silent dead-letter.
