# Issues register

_As of edc81cb._

Known defects, gaps, and footguns in code that has already shipped — each `open`
until `fixed` or `wontfix`. Deferred *capabilities* live in
[`FUTURE.md`](FUTURE.md); committed work in [`ROADMAP.md`](ROADMAP.md). Grammar:
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

## query

- [x] **quote_ident panics on embedded quote** `{#iss-quote-ident-panic area:query status:fixed from:link-traversal pr:#110 spec:2026-06-14-query-governed-link-traversal-design}`
  Fixed (PR #110): `sql.rs::quote_ident` no longer `assert!`s — it escapes `"`→`""` per SQL identifier rules, so a backing column containing a `"` (reachable while [[fut-define-link-validation]] is deferred) renders as a valid quoted identifier instead of panicking the request thread. Return type stays `String`, so no ripple across call sites.
- [ ] **Literal comma inside `in:` operand** `{#iss-literal-comma-in-in area:query status:open from:comparison-set-operators pr:- spec:2026-06-21-filter-set-operand-comma-escaping-design}`
  The `in:` operator splits on commas, so an operand containing a comma cannot be expressed; needs an escaping or alternate-delimiter convention.
- [x] **Multi-file DuckLake LIMIT mis-read** `{#iss-multi-file-limit-misread area:query status:fixed from:cross-cutting pr:#131 spec:2026-06-21-multi-file-limit-guard-design}`
  Fixed (PR #131): governed reads on the DuckDB serving engine now emit a stable `ORDER BY <key>` barrier before the pushed-down `LIMIT` at every compiler `LIMIT` site, which compiles to DuckDB's TopN and stops the `LIMIT` being pushed into the multi-file Parquet scan — the corruption site. The order key is the queried type's `identity` when visible, else its projected (unmasked) columns, always a subset of the SELECT list (DISTINCT-safe). Gated by a new `SqlDialect::limit_needs_order_barrier()` (`true` for `DuckDbDialect`); the new `DataFusionDialect` keeps the bare `LIMIT` (no such bug). A multi-file `loom_fixture_test` (`multi-file-limit-guard`) reproduced the corruption deterministically with the barrier off (id `65537`) and asserts uncorrupted ids with it on. Upstream DuckDB repro + the workaround's removal are tracked in [[fut-multi-file-limit-upstream]].
- [x] **Serving faults not logged server-side** `{#iss-serving-faults-not-logged area:query status:fixed from:tracing-pass pr:#140 spec:2026-06-21-serving-fault-logging-design}`
  Fixed: installed `service_runtime::init_tracing()` in both service binaries (env-driven via `RUST_LOG`, default `info`, idempotent `try_init`). Added `pub fn internal_error(context, e)` to `query-api/src/http.rs` that logs the fault detail via `tracing::error!` server-side and returns the opaque `"internal error"` 500 body unchanged. Applied at the four opaque-500 arms (`read_object`, `chain_error`, `graph_error`, `post_action`). Pinned by `serving_fault_logging` test with `#[traced_test]`: asserts the opaque body contains no leaked detail and `logs_contain` sees the fault. Drops the standing `TODO(serving-tier)` comment.

## lineage

- [x] **Action lineage atomicity gap** `{#iss-action-lineage-atomicity area:lineage status:fixed from:actions-part1 pr:#123 spec:2026-06-21-action-lineage-atomicity-design}`
  Action writes emit lineage best-effort on a separate connection after the DuckDB inline write, so a crash in the gap leaves a snapshot without its event. The event also carries no inputs and `run_action` doesn't surface its `run_id`. Fixed: action writes now route through loom's own snapshot-commit primitive (`DuckLakeActionWriter` → `ingest::materialize::land_ducklake`), committing the row and its `LineageEvent` in one Postgres transaction, and `run_action` returns the `run_id` (surfaced on the `X-Loom-Run-Id` response header).

## ontology

- [x] **Action param/property conformance unchecked** `{#iss-action-param-conformance area:ontology status:fixed from:actions-part1 pr:#106 spec:2026-06-20-actions-param-property-conformance-design}`
  Fixed (PR #106): `run_action` now validates the resolved `ActionDef` against its target `ObjectType` before the insert — every parameter must name a property of a compatible logical type, every required property must be covered — surfacing `ActionError::Misconfigured` (HTTP 500 naming each violation) instead of an opaque insert-time error. Invoke-time only; `define_action` fail-fast remains deferred.

## acl

- [x] **Role-inheritance cycle check not atomic** `{#iss-acl-role-cycle-atomic area:acl status:fixed from:acl-role-hierarchy pr:#125 spec:2026-06-11-acl-role-hierarchy-design}`
  Fixed (PR #125): `add_role_inheritance` now runs its existence check, recursive-CTE cycle check, and edge insert in one transaction guarded by a transaction-scoped advisory lock (`pg_advisory_xact_lock`, the same pattern as `snapshot.rs`'s catalog lock, on a distinct key). Concurrent opposite-edge inserts serialize on that lock, so the loser observes the winner's committed edge through the same cycle CTE and is rejected with `Conflict` rather than both committing and forming a cycle. The lock auto-releases on commit/rollback (incl. drop-on-panic). A fixture-backed concurrency test (`acl-inheritance-concurrency`) asserts two racing `A->B`/`B->A` calls yield exactly one commit and one conflict.

## transform

- [ ] **Transform read-path edge cases** `{#iss-transform-read-edge-cases area:transform status:open from:transform-workers pr:#57 spec:2026-06-22-transform-read-edge-cases-design}`
  Two `run_transform` edge cases: a missing input surfacing at `Catalog::files` is classified transient (Retry) instead of `UnknownInput` (Abandon); and `scan_table` over an empty file list errors inside DataFusion (Retry) rather than yielding an empty input. Tidy when the typed-transform slice builds on the primitive.

## iceberg

- [x] **Recursive-CTE /graph reads unverified on DataFusion engine** `{#iss-recursive-cte-iceberg area:iceberg status:fixed from:graph-traversal pr:#82,#118 spec:2026-06-17-iceberg-datafusion-serving-engine-design}`
  Fixed (PR #118): verifying the recursive-CTE `/graph` SQL on DataFusion revealed it did NOT run as emitted — DataFusion ignores the `reach(id, depth)` CTE column-name list, so the anchor's `0` literal stayed named `Int64(0)` and the recursive `r.depth` reference failed (`Schema error: No field named r.depth`). The three graph compilers (`compile_graph_reach`, `compile_graph_reach_union`, `recursive_reach_cte`) now alias the anchor/recursive columns explicitly (`SELECT s.id AS id, 0 AS depth …`), which both DuckDB and DataFusion accept. The new `recursive-cte-over-datafusion` `loom_fixture_test` lands self-link graphs into `iceberg_mirror` (real Parquet, via the new `IcebergWriter::seed_arrays`) and asserts the same reachable sets as the DuckDB `graph-reach-e2e` / `graph-union-e2e` (FK cycle → `[1,2,3]`; FK ∪ join-table → `[2,3,4]`). The DuckDB graph e2es remain green. (`WITH RECURSIVE`/`UNION` is still a hardcoded literal, not routed through `SqlDialect`.)
- [x] **Iceberg commit holds PG tx across object-store reads** `{#iss-iceberg-tx-objectstore area:iceberg status:fixed from:iceberg-write-path pr:#78,#138 spec:2026-06-22-iceberg-tx-objectstore-scope-design}`
  Fixed (PR #138): `do_update_table` now hoists the sole in-tx object-store read (`added_files_of` — manifest list + manifests + Parquet footers) plus `columns_of` and the staged snapshot id **before** `begin()`, and `project_mirror` was replaced by a FileIO-free `write_mirror(tx, ident, staged_snap, &[ProjectedColumn], &[ProjectedFile])` that takes precomputed inputs only (no `&Table`/`FileIO`, so it is type-level incapable of reading object storage in-tx). The transaction body is now pure local PG (CAS → mirror writes → optional end-cap → optional lineage → commit); `next_snapshot` stays in-tx. Atomicity unchanged — manifests are immutable/content-addressed and already persisted, so the pre-tx read yields identical rows and the CAS still guards the pointer (39/39 iceberg fixture tests green before and after). See [[fut-iceberg-cas-conflict-retry]].
- [ ] **Inline rows invisible to external clients** `{#iss-iceberg-inline-visibility area:iceberg status:open from:2026-06-18-iceberg-inline-writes-design pr:#86 spec:-}`
  Inline writes land mirror-only (no object-storage Parquet / Iceberg metadata), so external Iceberg clients see inline rows only after a flush (bounded staleness, accepted). The flush primitive restores external visibility.
- [ ] **Iceberg inline read rebuilds Parquet every query** `{#iss-iceberg-inline-reparse area:iceberg status:open from:2026-06-18-iceberg-landing-backend-design pr:#90 spec:-}`
  Inline rows are reconstructed from Postgres into ephemeral in-memory Parquet on every query through the union view, so the read cost is paid per read until a flush — making the flush follow-up more valuable. See [[road-iceberg-inline-flush]].
- [x] **Flush idempotency under at-least-once redelivery is unasserted** `{#iss-flush-at-least-once-idempotency area:iceberg status:fixed from:engine-wire-flush-vertical pr:#134 spec:2026-06-22-flush-idempotency-test-design}`
  Fixed (PR #134): Adds regression test `duplicate_dispatch_flush_is_idempotent_over_the_wire` that dispatches two concurrent flush_table RPCs for the same table and asserts exactly-once effect (rows written exactly once, inline rows retired exactly once). The worker's `Worker` loop heartbeats the lease on a timer, and each heartbeat is now a remote gRPC round-trip to the engine over the UDS; a slow or partitioned socket can miss heartbeats, lapse the lease, and let a second worker re-dispatch the same `flush_table` job (at-least-once delivery). `flush_table` is a transaction-scoped advisory-locked snapshot-commit, so a double-run is *expected* to be a safe no-op — and the test confirms this holds under concurrent/duplicate dispatch. Related to [[road-iceberg-flush-consumer]].

## cross-cutting

- [x] **Memory Tx::commit not atomic across concerns** `{#iss-memory-tx-not-atomic area:cross-cutting status:fixed from:critical-review pr:#112 spec:2026-06-06-tx-isolation-contract-design}`
  Fixed (PR #112): `Tx::commit` now holds `rows`+`lineage`+`catalog` across the whole apply (consistent lock order; readers each take one lock, so no deadlock) and validates the fallible compaction check before any mutation — so a failed commit rolls back everything across queue, lineage, and catalog, matching the pg single-`sqlx::Transaction`. (The earlier jobs-vs-events leg was closed by [[road-tx-isolation-contract]]; this closes the catalog leg + the partial-commit-on-conflict path.) `tx_atomic_rollback_contract` asserts it on both adapters.
- [ ] **Dataset/target existence validation missing** `{#iss-existence-validation area:cross-cutting status:open from:critical-review pr:- spec:2026-06-21-existence-validation-design}`
  Lineage `emit`, ACL `grant`/`set_policy`, and ontology `resolve` store without validating the referenced dataset/table/type exists. No new core seam needed (adapters co-locate concerns; `ControlPlaneError` is `#[non_exhaustive]`); same-database refs can use cross-schema FK constraints, but the in-memory fake must replicate the check and external `DatasetRef`s are un-FK-able.

## devx

- [x] **Claim not reaped promptly after its PR merges** `{#iss-claim-reap-on-merge area:devx status:fixed from:work-checkout pr:#115 spec:2026-06-21-work-item-planning-checkout-design}`
  Fixed (PR #115): `_pr_state` now returns a `gone` state when no open `work/<id>` PR exists but one ever did (`--state all` after the open check), and `docs.sh claims --reap` reaps a `gone` (merged/closed) claim immediately, ignoring the grace window — which now gates only the genuine pre-PR `none` case. `unknown` (gh unavailable) is still never reaped. Covered by a `pr-gone.sh`-stubbed test.

## test

- [ ] **Hermetic-Postgres fixture boot storm exhausts kernel resources** `{#iss-fixture-boot-contention area:test status:open from:postgres-fixture pr:- spec:2026-06-22-fixture-boot-throttle-design}`
  A full `buck2 test //src/...` boots every `loom_fixture_test` target's Postgres cluster at once (~60 `initdb` in one window) and they fail en masse at initdb's bootstrap backend (`fixture.rs:88`, `initdb failed`) — kernel-resource exhaustion (SysV semaphore sets; Linux `SEMMNI`=128) under mass-concurrent cluster boot. Single targets and small batches pass, so it manifests only on the whole-suite sweep (and intermittently trips the pre-push `buck2-test` hook with a shifting failure set). Fix: a cross-process+cross-thread `flock` slot semaphore in `PgFixture::start` bounding live clusters to `K` (default 8, `LOOM_PG_FIXTURE_SLOTS`), no build/dependency change.
