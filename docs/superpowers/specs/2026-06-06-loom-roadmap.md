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

### 2a — Must-fix before services depend on it  ✅ COMPLETE

1. **Worker heartbeat.** ✅ (PR #13) `Worker::new` takes the lease and heartbeats the
   in-flight job at `lease/3` (best-effort), so a handler outliving `lock_timeout` is no
   longer reclaimed and double-executed. Regression test added.
2. **Tx isolation/concurrency contract.** ✅ (PR #14) Deterministic
   `tx_isolation_contract` (uncommitted invisible until commit; rollback invisible) on
   both adapters; the **non-atomic memory `Tx::commit`** is fixed to hold both locks in one
   critical section.
3. **Catalog MVCC delete contract.** ✅ (PR #15) `CatalogSeed::drop_table` + a
   `catalog_delete_contract` exercising the `end > s` bound (false at the drop snapshot,
   true with a non-null `end` in the live past) and `begin <= s` false (before-existence),
   on both adapters (pg drives a real DuckLake `DROP`). Schema-evolution + file-supersession
   deferred to `docs/FUTURE.md`.
4. **Typed qualified identity.** ✅ (PR #16, re-scoped) Brainstorming found a core typed
   bridge premature: OpenLineage already fixes the dataset shape (`{namespace, name}`, which
   `DatasetRef` matches) and its namespace is datasource/deployment-derived — so the
   `TableRef`/`TypeName` → `DatasetRef` mapping is a Step 3 service concern, not core
   constants. Resolved to **reserve the seams**: `ControlPlaneError` is now `#[non_exhaustive]`
   (validation variant additive later), `DatasetRef` documents the OpenLineage convention, and
   `docs/FUTURE.md` records that cross-concern validation needs no new core seam (adapters
   co-locate concerns via `&self`) and can use **cross-schema FK constraints** for
   transactional referential integrity without an app read.
5. **`Tx` seam decision.** ✅ (decided; `2026-06-07-tx-seam-decision-design.md`) The seam
   **stays flat**; the **catalog write leg is deferred to the ingest worker (Step 3)** (how
   loom commits a snapshot is an ingest concern, inseparable from the multi-writer question);
   `dyn ControlPlane` is left minimal. No code change — re-open the flat-vs-aggregator call
   only if a fourth transactional concern proves it insufficient.

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

Per `ARCHITECTURE.md` (**revised**: DuckDB is the serving engine and speaks Quack
natively; a Rust HTTP API is the governance chokepoint that compiles ontology + ACL into
SQL; DataFusion is scoped to ingestion/transforms — there is **no** Quack-over-DataFusion
shim). Each piece is its own brainstorm → spec → plan cycle, sits *on top of* the
control-plane library, and should not start until Step 2a is in. Each large service is
decomposed into a load-bearing **part 1** primitive first, mirroring how ingest was done.

- **Ingest** —
  - *Part 1 — snapshot-commit primitive* ✅ DELIVERED (PR #32;
    `2026-06-09-ingest-snapshot-commit-primitive-design.md`). loom is a native DuckLake
    single-catalog writer: snapshot + lineage + enqueue commit in one Postgres
    transaction, proven against the pinned DuckDB engine.
  - *Part 2a — landing materializer* ✅ DELIVERED
    (`2026-06-11-ingest-materializer-primitive-design.md`). Arrow batches → inferred
    DuckLake schema → Snappy Parquet in object storage → the part-1 snapshot+lineage
    commit, with an optional model-conformance gate. Proven by a DuckDB read-back
    interop guardrail. Library-only (no DataFusion/endpoint).
  - *Later:* dataset→model binding; the ingest service shell (binary, object store,
    DataFusion endpoint); schema evolution; delete/compaction; orphaned-Parquet GC.
- **Query / read path** —
  - *Part 1 — governed object-read slice* (specced, in progress;
    `2026-06-10-query-governed-object-read-slice-design.md`). `GET /objects/{type}` →
    resolve the ontology type to a physical DuckLake table → inject a minimal ACL row
    predicate + column projection → execute on an embedded DuckDB (`duckdb-rs`) behind a
    `ServingEngine` seam → JSON rows. Plain-HTTP client surface; the Quack wire deferred.
  - *Later:* the serving *tier* over Quack (separate `quack_serve`'d DuckDB; the seam's
    Quack-client impl); the client-facing Quack endpoint; full ACL (deny-override,
    masking, roles); rich ontology (links, derived properties); multi-type queries/joins;
    authentication.
- **Actions** — the ontology's typed write-backs, governed at the HTTP API and executed
  *through* the serving layer, with loom owning the catalog-commit transaction (the
  snapshot-commit primitive) so snapshot + lineage + enqueue stay atomic. Own spec.
- **Transform workers** — built on `control-plane-worker`: consume the queue, run
  DataFusion, write snapshots, emit lineage + enqueue downstream atomically; optional
  Ballista escalation.

---

## Where we are

Step 1 complete; **Step 2a complete** (all five items — PRs #13–#16 + the #5 decision
record); **Step 2b** partially landed (pagination convention, proptest round-trips, and
the dead-variant cleanup via PR #22; `.sqlx` compile-time queries done). `main` green.

**Step 3 is underway.** Ingest **part 1** (the snapshot-commit primitive) and **part 2a**
(the landing materializer) are both delivered. Query **part 1** (the governed
object-read slice) is specced (`2026-06-10-query-governed-object-read-slice-design.md`)
and in implementation.

Recommended next move: implement the query read slice (its plan's first task is a
`duckdb-rs`/extension-version spike), then pick up the remaining Step 2b trailing
hardening opportunistically (per-concern adapter split and `tracing` are the
highest-leverage).
