# Issues register

_As of edc81cb._

Known defects, gaps, and footguns in code that has already shipped — each `open`
until `fixed` or `wontfix`. Deferred *capabilities* live in
[`FUTURE.md`](FUTURE.md); committed work in [`ROADMAP.md`](ROADMAP.md). Grammar:
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

## query

- [ ] **quote_ident panics on embedded quote** `{#iss-quote-ident-panic area:query status:open from:link-traversal pr:- spec:2026-06-14-query-governed-link-traversal-design}`
  `sql.rs::quote_ident` `assert!`s an identifier contains no `"`; combined with deferred [[fut-define-link-validation]], a backing column containing a `"` panics the request thread instead of erroring cleanly. Harden to return a `CompileError` (or escape `"`→`""`).
- [ ] **Literal comma inside `in:` operand** `{#iss-literal-comma-in-in area:query status:open from:comparison-set-operators pr:- spec:2026-06-16-filter-comparison-operators-design}`
  The `in:` operator splits on commas, so an operand containing a comma cannot be expressed; needs an escaping or alternate-delimiter convention.
- [ ] **Multi-file DuckLake LIMIT mis-read** `{#iss-multi-file-limit-misread area:query status:open from:cross-cutting pr:- spec:-}`
  A DuckLake table backed by >1 Parquet file read with a pushed-down `LIMIT` can reconstruct column values incorrectly (observed: int64 `id` corrupted by a `+ (other_id << 8)` pattern) — an upstream DuckDB/DuckLake bug. loom sidesteps it for single-file transform outputs; large multi-file outputs remain exposed. Follow-up: upstream repro + a loom-side guard.
- [ ] **Serving faults not logged server-side** `{#iss-serving-faults-not-logged area:query status:open from:tracing-pass pr:- spec:2026-06-07-tracing-instrumentation-design}`
  `query-api` returns an opaque 500 for backend/serving faults (correctly, to avoid leaking SQL/table detail) but drops the error `e` entirely. Log `e` server-side once a tracing subscriber is wired in the binary.

## lineage

- [ ] **Action lineage atomicity gap** `{#iss-action-lineage-atomicity area:lineage status:open from:actions-part1 pr:- spec:2026-06-15-actions-part1-design}`
  Action writes emit lineage best-effort on a separate connection after the DuckDB inline write, so a crash in the gap leaves a snapshot without its event. The event also carries no inputs and `run_action` doesn't surface its `run_id`. Close via a loom-owned DuckLake write (or reconciliation) plus a correlatable action-lineage handle.

## ontology

- [x] **Action param/property conformance unchecked** `{#iss-action-param-conformance area:ontology status:fixed from:actions-part1 pr:#106 spec:2026-06-20-actions-param-property-conformance-design}`
  Fixed (PR #106): `run_action` now validates the resolved `ActionDef` against its target `ObjectType` before the insert — every parameter must name a property of a compatible logical type, every required property must be covered — surfacing `ActionError::Misconfigured` (HTTP 500 naming each violation) instead of an opaque insert-time error. Invoke-time only; `define_action` fail-fast remains deferred.

## acl

- [ ] **Role-inheritance cycle check not atomic** `{#iss-acl-role-cycle-atomic area:acl status:open from:acl-role-hierarchy pr:- spec:2026-06-11-acl-role-hierarchy-design}`
  `add_role_inheritance` does the cycle check and the edge insert as two round-trips, so two concurrent calls inserting opposite edges of a cycle could both pass. Safe single-writer; wrap in a SERIALIZABLE tx (or lock) if concurrent edge writes land.

## transform

- [ ] **Transform read-path edge cases** `{#iss-transform-read-edge-cases area:transform status:open from:transform-workers pr:#57 spec:2026-06-14-transform-workers-part1-design}`
  Two `run_transform` edge cases: a missing input surfacing at `Catalog::files` is classified transient (Retry) instead of `UnknownInput` (Abandon); and `scan_table` over an empty file list errors inside DataFusion (Retry) rather than yielding an empty input. Tidy when the typed-transform slice builds on the primitive.

## iceberg

- [ ] **Recursive-CTE /graph reads unverified on DataFusion engine** `{#iss-recursive-cte-iceberg area:iceberg status:open from:graph-traversal pr:#82 spec:2026-06-17-iceberg-datafusion-serving-engine-design}`
  The `/graph` reachability compilers emit `WITH RECURSIVE` SQL run today only against the DuckDB/DuckLake engine. The shape is standard linear recursion DataFusion 54 supports (no compiler change expected) but unproven — no test runs a recursive CTE through `DataFusionServingEngine`. Add a recursive-CTE-over-DataFusion test landing a self-link graph into `iceberg_mirror`. (`WITH RECURSIVE`/`UNION` is a hardcoded literal, not routed through `SqlDialect`.)
- [ ] **Iceberg commit holds PG tx across object-store reads** `{#iss-iceberg-tx-objectstore area:iceberg status:open from:iceberg-write-path pr:#78 spec:2026-06-17-iceberg-adapter-write-path-design}`
  The atomic commit holds the Postgres transaction open across object-store manifest reads — fine single-writer, but a multi-writer throughput/perf follow-up.
- [ ] **Inline rows invisible to external clients** `{#iss-iceberg-inline-visibility area:iceberg status:open from:iceberg-inline-writes pr:#86 spec:2026-06-18-iceberg-inline-writes-design}`
  Inline writes land mirror-only (no object-storage Parquet / Iceberg metadata), so external Iceberg clients see inline rows only after a flush (bounded staleness, accepted). The flush primitive restores external visibility.
- [ ] **Iceberg inline read rebuilds Parquet every query** `{#iss-iceberg-inline-reparse area:iceberg status:open from:iceberg-landing-backend pr:#90 spec:2026-06-18-iceberg-landing-backend-design}`
  Inline rows are reconstructed from Postgres into ephemeral in-memory Parquet on every query through the union view, so the read cost is paid per read until a flush — making the flush follow-up more valuable. See [[road-iceberg-inline-flush]].
- [ ] **Flush idempotency under at-least-once redelivery is unasserted** `{#iss-flush-at-least-once-idempotency area:iceberg status:open from:engine-wire-flush-vertical pr:- spec:2026-06-20-engine-wire-flush-vertical-design}`
  The worker's `Worker` loop heartbeats the lease on a timer, and each heartbeat is now a remote gRPC round-trip to the engine over the UDS; a slow or partitioned socket can miss heartbeats, lapse the lease, and let a second worker re-dispatch the same `flush_table` job (at-least-once delivery). `flush_table` is a transaction-scoped advisory-locked snapshot-commit, so a double-run is *expected* to be a safe no-op — but nothing in the flush vertical asserts that idempotency under concurrent/duplicate dispatch. Add a test that runs two concurrent flushes of the same table over the wire and asserts exactly-once effect. Related to [[road-iceberg-flush-consumer]].

## cross-cutting

- [ ] **Memory Tx::commit not atomic across concerns** `{#iss-memory-tx-not-atomic area:cross-cutting status:open from:critical-review pr:- spec:2026-06-06-tx-isolation-contract-design}`
  The memory fake's `Tx::commit` applies jobs under one lock then events under another — two critical sections, so a concurrent reader can observe jobs-applied-but-events-not. The pg adapter gets true all-or-nothing from one `sqlx::Transaction`, so the fake doesn't faithfully model the atomic guarantee. Apply all staged buffers under one lock, or document the weaker guarantee.
- [ ] **Dataset/target existence validation missing** `{#iss-existence-validation area:cross-cutting status:open from:critical-review pr:- spec:-}`
  Lineage `emit`, ACL `grant`/`set_policy`, and ontology `resolve` store without validating the referenced dataset/table/type exists. No new core seam needed (adapters co-locate concerns; `ControlPlaneError` is `#[non_exhaustive]`); same-database refs can use cross-schema FK constraints, but the in-memory fake must replicate the check and external `DatasetRef`s are un-FK-able.
