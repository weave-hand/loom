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

- ✅ `SKIP LOCKED` concurrency test (N workers / M jobs, each claimed once) —
  `queue_concurrency_contract`, run by both adapters.
- ✅ Handler-panic policy in `Worker` (`catch_unwind` → `fail(.., Abandon)`) + test.
- ✅ Split the adapter monoliths per concern — `postgres/src/{queue,catalog,ontology,acl,
  lineage,snapshot,transaction}.rs` and the `memory` equivalents already mirror `core`.
- ✅ Pagination/cursor convention on list reads (PR #22).
- ✅ `tracing` spans around the adapter SQL — every concern's trait methods now carry a
  `#[tracing::instrument]` span (`acl`/`queue` from the start; `catalog`/`snapshot`/`lineage`
  via `2026-06-14-control-plane-tracing-pass-design.md`).
- ✅ `.sqlx` offline metadata so pg queries are compile-time-checked.
- ✅ proptest/arbitrary round-trips for `RowFilter` and the lineage envelope (PR #22).
- ✅ `ControlPlaneError::{Conflict, Unauthorized}` resolved: `Conflict` is **wired** (both ACL
  adapters raise it on a uniqueness race, asserted in the contract); `Unauthorized` is **kept
  as a documented reservation** for the Step-3 service auth layer.

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
  - *Part 2b — dataset→model binding* ✅ DELIVERED
    (`2026-06-12-ingest-dataset-model-binding-design.md`). A validated promotion: bind a
    landed dataset to an ontology type, checking the physical schema satisfies the declared
    type (new core logical-type vocabulary: base scalars + affinities + semantic aliases)
    before define_type — so a bound type is guaranteed-serveable. Proven by an accept/reject
    matrix and a materialize→bind→read e2e.
  - *Part 3 — DataFusion ingestion compute path* ✅ DELIVERED
    (`2026-06-14-ingest-datafusion-compute-path-design.md`). The write path now runs on
    DataFusion (SessionContext -> size-estimated repartition -> ParquetSink), writing N
    size-targeted Snappy files directly to object storage with per-file DuckLake stats
    (min/max merged across row groups). Proven by the DuckDB multi-file interop oracle.
  - *Later:* the networked DataFusion endpoint; footer-only stats reads; in-batch
    splitting (RoundRobinBatch is per-batch today); schema evolution; delete/compaction;
    orphaned-Parquet GC.
- **Query / read path** —
  - *Part 1 — governed object-read slice* (specced, in progress;
    `2026-06-10-query-governed-object-read-slice-design.md`). `GET /objects/{type}` →
    resolve the ontology type to a physical DuckLake table → inject a minimal ACL row
    predicate + column projection → execute on an embedded DuckDB (`duckdb-rs`) behind a
    `ServingEngine` seam → JSON rows. Plain-HTTP client surface; the Quack wire deferred.
  - *Part 2 — typed JSON serialization* ✅ DELIVERED
    (`2026-06-12-query-typed-json-serialization-design.md`). The read path now returns
    typed objects (`{ "objects": [...] }`) rendered by each property's logical type: the
    core vocabulary gains a `JsonRepr` classification (logical↔JSON axis), the serving
    layer stops collapsing Double/Date/Timestamp to debug strings, and `Long` renders as a
    JSON string to keep int64 precision past 2⁵³. Proven by a render matrix and the
    strengthened materialize→bind→read e2e. (Surfaced + fixed a latent affinity bug: the
    DuckLake physical name for a 64-bit float is `float64`, not `double`.)
  - *Part 3 — governed link traversal* ✅ DELIVERED
    (`2026-06-14-query-governed-link-traversal-design.md`). Links are now resolvable
    (`LinkBacking`: foreign-key + join-table) and a governed traversal read
    (`read_linked_objects`, `GET /objects/:from/links/:link`) joins source → target with
    both-ends governance (Read on both types, source + target row filters, target projection)
    and `DISTINCT` dedup. The first relational read; part-1 of richer read capability.
  - *Part 4 — derived properties (slice B)* ✅ DELIVERED
    (`2026-06-15-derived-properties-design.md`). An object type can declare
    aggregate-over-link derived properties (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`), served through
    `read_object` next to physical properties as governed correlated subqueries over a link.
    Both-ends governed: the subject needs Read on the linked type, the linked type's row-filters
    apply inside the subquery, and a derived prop is omitted (like a denied column) when the
    linked type or aggregated column is unreadable — never an error. Read-time only. Proven by
    an e2e.
  - *Part 5 — governed multi-hop traversal (slice C, part-1)* ✅ DELIVERED
    (`2026-06-15-query-multi-hop-traversal-design.md`). An ordered chain of links
    (`GET /objects/{from}/links?path=l1,l2`) is served as a `SELECT DISTINCT` chain of governed
    INNER JOINs, reusing the per-hop FK/join-table join shapes from slice A. Governed at **every**
    hop: `Read` is required on each type in the chain (source, every intermediate, final target),
    and each type's row-filters are AND'd inside the join — generalizing slice A's both-ends rule
    to N-ends, so a caller can only reach final targets through intermediate rows the policy permits.
    Depth-capped (4 hops), deduped final-target projection (`DISTINCT` over the visible columns),
    source-filter only. The single-hop route forwards into the chain handler as the `N=1` case
    (one compiler). Proven by an e2e.
  - *Part 6 — target / intermediate filters (slice C part-2)* ✅ DELIVERED
    (`2026-06-16-query-target-intermediate-filters-design.md`). A traversal caller can now
    filter **any** type in a chain (source, every intermediate, final target) by typed equality,
    addressed by a `<linkname>.<column>` query key (bare = source). Each per-hop filter is
    coerced to its own type's logical type (via `filter::coerce_filter`) and visibility-checked
    against its own type's governed projection; the N-ends Read governance is unchanged, so
    caller filters only narrow within already-permitted visibility. Draws the relational
    (`/links`) vs deferred graph (`/graph`) boundary: a per-hop filter on a link that repeats in
    the path is rejected (the graph case). Proven by compiler unit tests, a pure resolver unit,
    and a chain e2e.
  - *Part 7 — comparison / set operators on filters* ✅ DELIVERED
    (`2026-06-16-query-comparison-set-operators-design.md`). Caller filters across `read_object`,
    traversal, and chains (at every position) now express the full `CompareOp` surface — `ne`, `lt`,
    `le`, `gt`, `ge`, `in`, `nin`, `isnull`, `isnotnull` — via a value-prefixed `op:operand` grammar
    (bare value = `eq`; `eq:` escape; ranges as repeated keys). Operands stay typed (`SqlValue`,
    reusing `coerce_filter` per operand) so `Double`/`Date`/`Timestamp` filtering is preserved; one
    `caller_predicate_sql` renderer reuses the ACL `CompareOp`/`op_sql` machinery. Proven by
    `coerce_predicate` units, compiler-render units, and read/chain e2es.
  - *Part 8 — inverse-direction hops (slice C part-3)* ✅ DELIVERED
    (`2026-06-16-inverse-direction-hops-design.md`). Every link traversable backwards
    (`target <--link-- source`), governed at every hop, single- and multi-hop; via a new
    inbound-adjacency query (`Ontology::links_to`) + `LinkBacking::reversed()`, with the
    SQL chain compiler unchanged. Ambiguous inbound link name → deterministic 400.
  - *Later:* the serving *tier* over Quack (separate `quack_serve`'d DuckDB; the seam's
    Quack-client impl); the client-facing Quack endpoint; full ACL (deny-override,
    masking, roles); rich ontology (links, derived properties); multi-type queries/joins;
    authentication.
- **Actions** —
  - *Part 1 — governed typed insert* ✅ DELIVERED
    (`2026-06-15-actions-part1-design.md`). Named ontology `ActionDef` (targets an
    object type, carries typed parameters), invoked via `POST /actions/{name}`. First
    live enforcement of `Action::Write` (deny-by-default) at the governed HTTP front
    door. Writes execute as a **low-latency DuckLake inline write** behind an
    `ActionEngine` trait (Iceberg-swappable seam); no Parquet file per insert — DuckDB
    reconciles inline + Parquet rows on read. Insert-only: creates a new typed object,
    validated against the target type's property contract; the created object reads back
    through the existing governed read path. Best-effort type-named lineage (documented
    dangling slice). Proven by a fixture e2e.
  - *Later:* update/delete actions (gated on row-supersession/compaction); custom-logic
    / multi-step actions (params differing from properties, or enqueue-downstream);
    fine-grained write governance (row-filter / deny-column on `Write`); Iceberg
    `ActionEngine` impl.
  - *Fine-grained write governance — part 1 (control plane)* ✅ DELIVERED
    (`2026-06-16-acl-action-scoped-policies-design.md`). `acl.policy` is now action-scoped:
    `set_policy`/`clear_policy`/`policies_for` key on `(role, action, target)`, so a role holds
    independent read and write policies (parity with the already-action-scoped `role_grant`). The
    read path is explicitly `Read`-scoped (unchanged).
  - *Fine-grained write governance — part 2 (service enforcement)* ✅ DELIVERED
    (`2026-06-16-write-enforcement-design.md`). `run_action` loads the subject's `Write` policy and
    rejects an insert that sets a denied column or produces a row failing the policy's `row_filter`
    (a new pure in-memory three-valued evaluator, `write_filter.rs`, with full read-parity
    coercion). Fail-closed; a generic 403 with a logged reason. Deny-column counts only columns the
    action actually SETS (an omitted optional, materialized as NULL, is not "setting" it).
    `mask_columns` is ignored on writes (read-render only). The write front door now reaches parity
    with the read-side ACL. Proven by a fixture e2e.
- **Transform workers** —
  - *Part 1 — queue-driven SQL transform* ✅ DELIVERED
    (`2026-06-14-transform-workers-part1-design.md`). A worker (on `control-plane-worker`)
    reads input DuckLake table(s) with DataFusion (the new shared `datafusion-io` `scan_table`),
    runs a SQL query, and commits the result as a new snapshot + lineage (inputs → output),
    atomically. Physical `TableRef` in/out, multi-input, append semantics. Proven by an e2e
    that joins two landed tables off the queue and reads the output back through DuckDB.
  - *Part 2 — typed transforms part-1 (`Type(s) → Type`)* ✅ DELIVERED
    (`2026-06-15-typed-transforms-part1-design.md`). A `"typed-transform"` job names input
    ontology **type(s)** and an output **type**; the worker resolves each type to its DuckLake
    table, runs the SQL in type terms, validates the result **exactly conforms** to the output
    type's property contract, and commits the new snapshot. First-class type-named lineage: a
    new `TypeId` identity in control-plane-core so `upstream(OutputType)` returns the input
    types. Output reads through query-api as the typed object (governed read-back). Proven by
    an e2e fixture.
  - *Later:* programmatic (registered-plan) transforms; watermark/incremental output; a wider output type set (the write/infer path is canonical scalars only today);
    DAG / transactional enqueue-downstream; optional Ballista escalation.

---

## Where we are

Step 1 complete; **Step 2a complete** (all five items — PRs #13–#16 + the #5 decision
record); **Step 2b now closed** — the `tracing` instrumentation pass over the
catalog/snapshot/lineage adapter methods
(`2026-06-14-control-plane-tracing-pass-design.md`) was its last open item; the
concurrency/panic-policy/per-concern-split work and the pagination, proptest, `.sqlx`,
and dead-variant items had already shipped. **Step 2 is complete**, leaving **Step 3
(services) as the sole active track.** `main` green.

**Step 3 is underway.** Ingest **part 1** (the snapshot-commit primitive), **part 2a**
(the landing materializer), and **part 2b** (dataset→model binding) are all delivered.
Query **part 1** (the governed object-read slice), **part 2** (typed JSON
serialization, `2026-06-12-query-typed-json-serialization-design.md`), and **part 3**
(governed link traversal, `2026-06-14-query-governed-link-traversal-design.md`) are
delivered — landed data now binds to an ontology type, serves as typed objects through
the governed front door, and resolves links between types as a both-ends-governed
relational read. The **Transform worker (part 1)** — the queue-driven SQL transform that
reads DuckLake table(s) with DataFusion and commits the result as a new snapshot + lineage
atomically — is now delivered too, so all three service pillars have a load-bearing
primitive: ingest land, query serve, transform derive.

The external SQL wire (Quack / Postgres-wire / Flight SQL) is deliberately deferred as
a distribution/ergonomics concern, gated on a real external consumer *and* a design for
governance over arbitrary SQL — neither of which is in hand. **Typed transforms part-1**
(`Type(s) → Type` via `resolve` + `bind`, with conformance validation and type-named
lineage) is now also delivered, bringing the transform track to parity with ingest and
query. **Actions part-1** is now delivered too, bringing the **write-back** verb online
(`POST /actions/{name}` — governed typed insert enforcing `Action::Write`, inline DuckLake
write, validated against the type contract, reads back through the governed read path):
the platform's four core verbs — **land → derive → serve → write** — are all live.
Richer reads now span aggregate-over-link **derived properties** (slice B) and **multi-hop
traversal** (slice C part-1 — forward, source-filtered, deduped link chaining, governed at every
hop). The remaining slice-C part is object-set inputs keyed on identity. Candidate next
slices: object-set inputs, programmatic
transforms, compaction (reuses `replace_files`), and watermark/incremental output.
**Overwrite output mode** (`2026-06-17-overwrite-output-mode-design.md`) is now delivered:
a transform can set `output_mode = overwrite` to replace its output table's live contents
(vs the default append), backed by a new DuckLake-faithful `Tx::replace_files` primitive
that expires the prior files at the new snapshot — older snapshots still time-travel to
them — and resets table stats. Compaction (reusing `replace_files`) and watermark-tracked
incremental output remain deferred.
**Typed input filters** are now delivered too
(`2026-06-16-query-typed-input-filters-design.md`): query-param equality filters now coerce
to the column's declared ontology logical type (via the `json_repr_of`/`JsonRepr` taxonomy,
reusing the action-param coercion pattern), so `Long`/`Double`/`Boolean`/`Date`/`Timestamp`
filters work across `read_object`, single-hop traversal, and multi-hop chains; an uncoercible
value → 400 (equality-only — comparison operators are a later slice). The remaining smaller
query follow-ups are a schema sidecar and tz timestamps.
**Target / intermediate filters** (slice C part-2,
`2026-06-16-query-target-intermediate-filters-design.md`) are now delivered too: every type a
traversal touches is caller-filterable (typed, governed per type), not just the source, and the
relational-vs-graph boundary is drawn (a future `/graph` surface owns cyclic/self-link
traversal). The remaining slice-C part is object-set inputs keyed on identity.
**Comparison / set operators** (`2026-06-16-query-comparison-set-operators-design.md`) complete the
filter arc: every caller filter, at every read path and chain position, now expresses the full
`CompareOp` surface (ranges via repeated keys), not just equality — real analytical filtering on the
governed read path.

**Inverse-direction hops** (slice-C part-3,
`2026-06-16-inverse-direction-hops-design.md`) are now delivered: every link is
traversable backwards (`target <--link-- source`), governed at every hop, across the
single-hop (`?direction=inverse`) and multi-hop (`~`-prefixed `path` elements, freely
mixed with forward hops) read paths. The control plane gained an inbound-adjacency query
(`Ontology::links_to`) and `LinkBacking::reversed()`; the SQL chain compiler is unchanged
(an inverse hop is just a reversed backing + origin type). An inbound link name that is
not unique for the target type is a deterministic 400 (`AmbiguousLink`). The remaining
slice-C part is object-set inputs keyed on identity.

**Selective compaction** (`2026-06-17-compaction-design.md`) is now delivered: a
table's sub-threshold Parquet files can be coalesced into fewer size-targeted ones
(large files left in place) via a new partial-supersede control-plane primitive
(`Tx::compact_files`) and a `compact_table` service function. The primitive expires a
named subset of live files at the new snapshot and writes coalesced replacements,
adjusting table stats by delta (vs `replace_files`, which expires all files); older
snapshots still time-travel to the originals. A concurrent-compaction race is rejected
by an exact expire-count assertion. Library primitive only — a queue job and operator
endpoint remain deferred, as does watermark-tracked incremental output.

**Object identity + source→target association** (slice-C,
`2026-06-17-object-identity-association-design.md`) is now delivered: `ObjectType`
gained a first-class `identity: Option<String>` naming its primary-key property —
persisted in the ontology and validated at bind (a declared identity must name a
required property, else `BadIdentity`). Consuming it, a governed traversal can now
return the **edge list** — source↔target identity pairs — instead of the
`DISTINCT`-collapsed target set, via a `?shape=association` flag on the existing chain
routes (`/objects/:from/links` and `/objects/:from/links/:link`). Output is
`{"associations":[{"from":<id>,"to":<id>}]}`, governed both-ends exactly like the
chain read plus one rule: the source and final-target must each have a declared,
caller-visible identity (else `NoIdentity` → 400). The remaining slice-C follow-up is
**object-set inputs keyed on identity** (`?ids=1,2,3` → an `in:` predicate on the
source identity column), plus the longer-standing `/graph` surface for cyclic /
self-link traversal.

**Packaging / deploy (landed, MVP).** The ingest + query-api binaries now ship as
reproducible apko/Wolfi OCI images and a Helm chart (in their own `deploy//` cell,
image/Helm rules consumed from the `jomcgi/homelab` repo as a buck2 git external
cell). Chart: CNPG-bundled Postgres control plane, a
deployer-chosen StorageClass for the shared object-store PVC, default-deny
NetworkPolicy with no ingress, and an optional Gateway API `HTTPRoute`. A
`release` workflow pushes immutable `sha-<short>` images on every merge and
republishes the chart as the moving `bleeding-edge` channel (digest-pinned to
them); versioned `vX.Y.Z` image releases are manual dispatch, and a versioned
chart publishes when its `Chart.yaml` version is bumped. See `docs/deploy.md`. Open
follow-ups: a schema-migration Job (the chart provisions PG but doesn't migrate),
and replacing the `LocalFileSystem` PVC with a real S3/MinIO object store (which
removes the RWX-for-multi-pod constraint).
