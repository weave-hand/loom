# Design: Control-Plane Roadmap (as-built)

> **Status: DELIVERED (2026-06-06).** All five concerns shipped. This was the
> umbrella design; it has been compacted to reflect what was actually built. The
> provisional trait sketches have been replaced by the as-built surfaces and the
> notable divergences from the original plan. Per-concern detail lives in each
> concern's own spec; a critical read of the result is in
> `2026-06-06-control-plane-critical-review.md`; deferred work is in `docs/FUTURE.md`.

## Goal (unchanged)

loom's **control plane** — the typed, transactional access layer over the Postgres
schemas in `ARCHITECTURE.md` (`ducklake.*`, `ontology`, `queue`, `lineage`, `acl`).
A **library** the three services (Ingest, Transform, Query API) consume, not a
service itself.

## What shipped

Ports & adapters with one backend-agnostic contract suite per concern, run against
**both** an in-memory fake and real Postgres. The methodology held for all five
concerns: trait (in `core`) → contract (in `testkit`) → fake passes → pg adapter +
migration passes the *same* suite.

### Crate layout (as built)

```
src/control-plane/
  core/      control-plane-core      traits, domain types, ControlPlaneError. No I/O, no tokio.
  testkit/   control-plane-testkit   generic contract fns over the traits; backend-agnostic.
  memory/    control-plane-memory    in-memory fake (Arc<Mutex<…>>); fast tests + local dev.
  postgres/  control-plane-postgres  sqlx adapter + migrations/ + hermetic PgFixture.
  worker/    control-plane-worker    generic Worker<Q: Queue> dequeue→handle→complete/fail loop.
```

`worker/` was added beyond the original four crates (Phase 1b). Dependencies:
`core` ← everything; `testkit` ← `core`; adapters ← `core` (+ `testkit` as dev-dep);
`worker` ← `core`.

### Concern status

| Phase | Concern | Status | Key spec |
| ----- | ------- | ------ | -------- |
| 0  | foundations (crate split, `ControlPlaneError`, `Tx` seam, hermetic pg fixture) | ✅ | `2026-06-03-control-plane-phase-0*` |
| 1  | **queue** (+ worker, `await_jobs`) | ✅ | `2026-06-04-control-plane-queue-design` |
| 2  | catalog (DuckLake read surface) | ✅ | `2026-06-04-control-plane-catalog-design` |
| 3  | ontology (types/properties/links, `resolve`) | ✅ | `2026-06-04-control-plane-ontology-design` |
| 4  | acl (subjects/roles/grants + row/col policy) | ✅ | `2026-06-04-control-plane-acl-design` |
| 5  | lineage (events + one-hop graph; first cross-concern `Tx`) | ✅ | `2026-06-05-control-plane-lineage-design` |

## Durable decisions (still true)

- **Error model:** one `ControlPlaneError` in `core` (`NotFound`/`Conflict`/
  `Unauthorized`/`Serialization`/`Backend`); contracts assert on **variants, never
  messages**, so both backends satisfy one suite. (Note: `Conflict`/`Unauthorized`
  are defined but currently unused — `Acl::check` returns a `Decision`, not an error.)
- **Async + object safety** via `async_trait`; adapters on `tokio`; `core` runtime-free.
- **Hermetic Postgres fixture:** the server binary is a buck2 `http_archive` input
  (theseus-rs), booted as an ephemeral per-process cluster, fresh DB per test. Runs in
  the default CI path; pg/duckdb tests are `--local-only` (refuse to run as root on RE).
- **No `rsql`/driver abstraction** — rejected as a generic layer whose value isn't a
  goal (opinionated, not pluggable). DuckLake is driven only by a pinned DuckDB CLI in
  the catalog *test fixture*; production code never touches DuckDB.

## Divergences from the original sketch (worth knowing)

- **The `Tx` seam is object-based, not the closure aggregator.** Shipped:
  `ControlPlane::begin() -> Box<dyn Tx + Send>` with a flat `Tx { commit, rollback,
  enqueue, emit }`. The sketched `transaction<F,T>(closure)` + per-concern accessors
  (`fn queue(&self) -> &dyn Queue`, …) were **dropped** — the finicky-lifetime risk
  resolved by going object-based. Consequence: `dyn ControlPlane` exposes only
  `begin()`, and only `enqueue`/`emit` are transactional (see review §2).
- **No standalone Tx "probe op," and no isolation test.** Tx semantics are covered
  inline (commit/rollback in `queue_contract`, cross-concern emit+enqueue in
  `lineage_contract`). The promised concurrent-isolation test was never written
  (review §1).
- **catalog:** `files`/`schema` are keyed by `(TableRef, SnapshotId)`, and
  `current_snapshot` was added; the read surface is MVCC-range based.
- **ontology:** `actions()` dropped (deferred); write ops (`define_type`/
  `define_link`) added so the concern is loom-owned and the contract self-seeds.
- **acl** is much larger than the 2-method sketch: full write surface (subjects,
  roles, membership, grants, policy + inverses) and a recursive boolean `RowFilter`
  tree stored as `jsonb`, not just `check`/`policies_for`.
- **lineage:** added `events_for` (audit read-back); `DatasetRef` is a generic
  OpenLineage `{namespace, name}`.
- **sqlx offline metadata (`.sqlx`) was NOT adopted** — the pg adapter uses the
  runtime query API throughout (SQL is unchecked at compile time; review §2).

## Open questions → outcome

- Queue durability vs latency → **resolved (P1):** `LISTEN/NOTIFY` + polling-fallback
  `await_jobs`. (Gap: the shipped `Worker` never heartbeats — review §1.)
- Multi-writer ingest / DuckLake concurrency → **still open** (catalog is read-only).
- Ontology authoring & migration → **partial (P3):** control plane is the write API;
  physical-table migration still open.
- ACL pushdown completeness → **reframed (P4):** P4 stores/returns predicates as a
  structured tree; what's pushable is the Query API's call.
- GC of orphaned Parquet → **still open** (`docs/FUTURE.md`).
- Tenancy → **still open**, single-tenant everywhere (`docs/FUTURE.md`).
- Transactional snapshot commit (the third atomic leg) → **not built**; catalog has no
  write op (review §4).

## Non-goals (unchanged)

The Quack-over-DataFusion server shim, the three services, and DataFusion plan
rewriting (the control plane is the library beneath them); writing `ducklake.*`
directly (DuckLake client owns it); a fully typed OpenLineage model (payload stays
opaque); Ballista, GC, and multi-tenancy mechanics.

## Where to look next

`2026-06-06-control-plane-critical-review.md` — the prioritized "what to fix" list
(heartbeating worker, Tx isolation contract, catalog MVCC-delete contract, typed
cross-concern identity, and the `Tx` seam's future). `docs/FUTURE.md` — deferred
features. The layers *above* this library (services, Quack shim) are the natural next
project and will want their own brainstorm.
