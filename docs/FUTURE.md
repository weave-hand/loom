# Future work register

_As of edc81cb._

Deliberately-deferred capabilities and tech debt — the "later, if a consumer
needs it" pile. Each notes the concern it came from and why it was deferred.
These are *not* committed roadmap items (see [`ROADMAP.md`](ROADMAP.md)); known
defects in shipped code are in [`ISSUES.md`](ISSUES.md). Grammar:
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

## lineage

- [ ] **Transitive provenance closure + cycle guard** `{#fut-lineage-closure area:lineage status:deferred from:phase-5 pr:- spec:2026-06-05-control-plane-lineage-design}`
  `upstream`/`downstream` return one hop. Full ancestry/descendancy (`upstream_closure`, or a `depth` param) needs a cycle guard (re-runs can cycle) and a depth/visited bound. Kept out of P5 so one-hop queries stay flat. See [[fut-lineage-stitching]].
- [ ] **Run-grouped lifecycle stitching** `{#fut-lineage-stitching area:lineage status:deferred from:phase-5 pr:- spec:2026-06-05-control-plane-lineage-design}`
  The graph is per-event co-membership. OpenLineage allows a run to split inputs on START and outputs on COMPLETE across events sharing a `run_id`; stitching those is deferred (loom's emitters emit one terminal event).
- [ ] **OpenLineage payload validation** `{#fut-openlineage-validation area:lineage status:deferred from:phase-5 pr:- spec:2026-06-05-control-plane-lineage-design}`
  `payload` is stored opaquely as `jsonb`; a validating/parsing layer that derives the typed envelope from the payload is future work.
- [ ] **Pagination/filtering on lineage reads** `{#fut-lineage-pagination area:lineage status:deferred from:phase-5 pr:- spec:-}`
  `events_for` and the graph reads are unbounded.
- [ ] **Type↔table lineage layer-join** `{#fut-type-table-lineage-join area:lineage status:deferred from:typed-transforms pr:#59 spec:2026-06-15-typed-transforms-part1-design}`
  Typed transforms emit type-named lineage nodes; ingest/physical transforms emit table-named nodes. The two layers don't auto-join yet (backing refs kept in the payload). A binding edge at `define_type`/bind would connect them.
- [ ] **TableRef/TypeName → DatasetRef naming bridge** `{#fut-dataset-naming-bridge area:lineage status:deferred from:critical-review pr:- spec:-}`
  An OpenLineage-conformant `{namespace, name}` mapping needs deployment context (physical storage location), so it belongs to the Step-3 services, not `core` constants.

## catalog

- [ ] **Transactional catalog write** `{#fut-transactional-catalog-write area:catalog status:deferred from:phase-5 pr:- spec:2026-06-07-tx-seam-decision-design}`
  Prerequisite for the full atomic snapshot+lineage+enqueue commit; the read-only catalog doesn't expose a transactional write. Owned by the ingest worker, joins the flat `Tx` seam when ingest defines it.
- [ ] **Schema-evolution test coverage** `{#fut-schema-evolution-coverage area:catalog status:deferred from:phase-2 pr:- spec:2026-06-07-catalog-delete-contract-design}`
  The delete contract covers the `end`-bound via `DROP` but not `ALTER TABLE` add/drop column across snapshots. Adds an `ALTER` op to the `CatalogSeed` seam; deferred until column-level time travel matters.
- [ ] **Queue-driven compaction job + incremental output** `{#fut-compaction-job area:catalog status:deferred from:phase-2 pr:- spec:2026-06-17-compaction-design}`
  The compaction library primitive (`compact_files`/`compact_table`) is wired but unscheduled; a queue-driven job / operator endpoint and watermark-tracked incremental (append-delta) output remain. Compacted files get fresh row-ids — revisit if row-level deletes land.

## ontology

- [ ] **Physical-column validation at define_link** `{#fut-define-link-validation area:ontology status:deferred from:link-traversal pr:- spec:2026-06-14-query-governed-link-traversal-design}`
  `define_link` stores backing column names without checking they exist (keeps the ontology write path decoupled from a catalog read). A bad column surfaces at traversal time; authoring-time validation against `Catalog::schema` is deferred. See [[iss-quote-ident-panic]].
- [ ] **Define-time derived-property validation** `{#fut-define-time-derived-validation area:ontology status:deferred from:derived-properties pr:- spec:2026-06-15-derived-properties-design}`
  Part-1 resolves the named link at read time and omits the property if missing; there is no authoring-time validation of a derived property's link/target/column/result-type at `define_type`.
- [ ] **Define-time chain/link validation** `{#fut-define-time-chain-validation area:ontology status:deferred from:multi-hop-traversal pr:- spec:2026-06-15-query-multi-hop-traversal-design}`
  Multi-hop resolves the chain at read time, so a broken chain (unknown link, or a link whose `from` is not the current type) surfaces then as a 400, not at authoring.
- [ ] **Update/delete actions** `{#fut-update-delete-actions area:ontology status:deferred from:actions-part1 pr:- spec:2026-06-15-actions-part1-design}`
  Actions part-1 is insert-only; mutating existing objects is gated on the deferred row-supersession/compaction work.
- [ ] **Custom-logic / multi-step actions** `{#fut-custom-logic-actions area:ontology status:deferred from:actions-part1 pr:- spec:2026-06-15-actions-part1-design}`
  Part-1's only action kind is "typed insert"; actions whose params differ from properties, run bespoke logic, or enqueue downstream are a follow-on.
- [ ] **Ontology versioning** `{#fut-ontology-versioning area:ontology status:deferred from:to-be-planned pr:- spec:-}`
  Version the ontology over time.
- [ ] **Migrations for ontology** `{#fut-ontology-migrations area:ontology status:deferred from:to-be-planned pr:- spec:-}`
  Support migrations for evolving the ontology.
- [ ] **Segmented reads by ontology version** `{#fut-segmented-reads area:ontology status:deferred from:to-be-planned pr:- spec:-}`
  Segment/partition reads by ontology version.
- [ ] **Model constraints** `{#fut-model-constraints area:ontology status:deferred from:to-be-planned pr:- spec:-}`
  Support constraints on models.

## query

- [ ] **Object-identity dedup for traversal** `{#fut-object-identity-dedup area:query status:deferred from:link-traversal pr:- spec:2026-06-17-object-identity-association-design}`
  Many-to-many traversal dedups with `SELECT DISTINCT` over the visible projection (the target key may be ACL-denied). `ObjectType.identity` now exists; reworking dedup to key on the visible identity is the remaining follow-up.
- [ ] **Scalar/expression derived properties** `{#fut-scalar-derived-props area:query status:deferred from:derived-properties pr:- spec:2026-06-15-derived-properties-design}`
  Own-column computations are excluded from part-1 (aggregate-over-link only); they need a whitelisted, injection-safe, governable SQL-expression surface.
- [ ] **Derived properties on traversal/chain output** `{#fut-derived-on-traversal area:query status:deferred from:derived-properties pr:- spec:2026-06-15-derived-properties-design}`
  Part-1 serves derived properties on the primary `read_object` only; projecting them onto link-traversal and multi-hop chain output is a follow-on.
- [ ] **Multi-hop aggregates and derived-on-derived** `{#fut-multi-hop-aggregates area:query status:deferred from:derived-properties pr:- spec:2026-06-15-derived-properties-design}`
  Derived-properties part-1 is single-link and non-nested. Aggregating over a chain, or a derived property referencing another, is deferred.
- [ ] **Derived-property materialization** `{#fut-derived-materialization area:query status:deferred from:derived-properties pr:- spec:2026-06-15-derived-properties-design}`
  Part-1 computes derived properties at read time as correlated subqueries; materializing them for hot paths is a follow-on.
- [ ] **Derived props as filter/sort targets** `{#fut-derived-filter-sort area:query status:deferred from:derived-properties pr:- spec:2026-06-16-query-comparison-set-operators-design}`
  Caller predicates validate against physical columns only; making derived (aggregate) properties filterable/sortable is deferred.
- [ ] **Richer filter error body** `{#fut-richer-filter-error area:query status:deferred from:typed-input-filters pr:- spec:2026-06-16-query-typed-input-filters-design}`
  An uncoercible value reuses `BadFilter(col)` (body = column name); reporting the expected type plus the offending value widens the error contract, deferred.
- [ ] **422 for body-bearing endpoints** `{#fut-422-body-endpoints area:query status:deferred from:typed-input-filters pr:- spec:2026-06-16-query-typed-input-filters-design}`
  Typed filters are URI params on a body-less GET (correctly 400). Whether `POST /actions`'s `BadParams` should become a 422 is a separate question.
- [ ] **or-combined caller predicates** `{#fut-or-predicates area:query status:deferred from:comparison-set-operators pr:- spec:2026-06-16-query-comparison-set-operators-design}`
  All caller predicates are ANDed; a disjunction grammar (OR across predicates) is deferred.
- [ ] **between:lo,hi sugar** `{#fut-between-sugar area:query status:deferred from:comparison-set-operators pr:- spec:2026-06-16-query-comparison-set-operators-design}`
  Ranges are two predicates (`ge`+`le`) via repeated keys; a dedicated `between` operator is sugar only.
- [ ] **Text-pattern matching operators** `{#fut-text-pattern-ops area:query status:deferred from:comparison-set-operators pr:- spec:2026-06-16-query-comparison-set-operators-design}`
  No `like`/`ilike`/`contains` `CompareOp` exists; a separate slice would add text-pattern matching with safe rendering.
- [ ] **Rename eq_filters field** `{#fut-rename-eq-filters area:query status:deferred from:comparison-set-operators pr:- spec:2026-06-16-query-comparison-set-operators-design}`
  The request field is still `eq_filters` though it carries the full operator grammar; a rename to `filters`/`predicates` is a cosmetic follow-up touching http.rs + e2es.
- [ ] **Inverse links inside graph path** `{#fut-inverse-in-path area:query status:deferred from:graph-path-cycle pr:- spec:2026-06-18-graph-path-cycle-design}`
  Each path link in `/graph` is followed forward; mixing backward hops into a cyclic path (e.g. `~memberOf,hasMember`) is a follow-on.
- [ ] **Graph-aware filter addressing** `{#fut-graph-filter-addressing area:query status:deferred from:target-intermediate-filters pr:- spec:-}`
  Per-occurrence/positional filter addressing that resolves the repeated-link ambiguity `/links` rejects, deferred to the `/graph` surface.
- [ ] **Min-depth annotation on reachable objects** `{#fut-min-depth-annotation area:query status:deferred from:graph-reachability pr:- spec:-}`
  Annotating reachable objects with the minimum hop count at which they were first reached; the deduped set carries no depth label.
- [ ] **Shortest-path / /tree surface** `{#fut-shortest-path-tree area:query status:deferred from:graph-reachability pr:- spec:-}`
  A surface serving the path itself (not just the reachable set) for shortest-path, spanning-tree, or hierarchical views.
- [ ] **Weighted edges** `{#fut-weighted-edges area:query status:deferred from:graph-reachability pr:- spec:-}`
  Edge-weight–aware traversal (min-cost reachability) requiring weight columns on the join-table backing.
- [ ] **Remaining /graph parts** `{#fut-graph-remaining area:query status:deferred from:roadmap-where-we-are pr:- spec:-}`
  Umbrella for the deferred `/graph` work: [[fut-inverse-in-path]], [[fut-min-depth-annotation]], [[fut-shortest-path-tree]], [[fut-weighted-edges]].
- [ ] **External SQL wire (Quack / Postgres-wire / Flight SQL)** `{#fut-external-sql-wire area:query status:deferred from:roadmap-where-we-are pr:- spec:2026-06-11-serving-tier-quack-design}`
  loom-as-a-Quack-*server* (external DuckDB clients `ATTACH` loom) is deferred — a distribution/ergonomics concern gated on a real external consumer and a design for governance over arbitrary SQL. A Quack-*client* `ServingEngine` exists.
- [ ] **HTTP serving-engine selection (Embedded vs Quack)** `{#fut-serving-engine-selection area:query status:deferred from:serving-tier pr:- spec:2026-06-11-serving-tier-quack-design}`
  Wiring serving-engine selection (`EmbeddedDuckDb` vs Quack-client) into the HTTP service, plus production serving-process supervision, is a later slice.
- [ ] **Multi-type queries / joins & schema sidecar** `{#fut-query-multitype-joins area:query status:deferred from:roadmap-where-we-are pr:- spec:-}`
  Rich ontology reads: multi-type queries/joins, a schema sidecar, and timezone-aware timestamps are deferred query follow-ups.

## acl

- [ ] **ACL deny-override + column masking + role hierarchy** `{#fut-acl-deny-masking-roles area:acl status:deferred from:roadmap-step2c pr:- spec:2026-06-10-acl-deny-override-design}`
  Full ACL semantics — deny-override, column masking on reads, role hierarchy — beyond the delivered baseline. (`mask_columns` is currently ignored on writes.)
- [ ] **Structured action write-denial reason** `{#fut-structured-write-denial area:acl status:deferred from:write-enforcement pr:#72 spec:2026-06-16-write-enforcement-design}`
  `run_action` surfaces a logs-only generic 403 on a denied write; surfacing a structured reason (which column / which row-filter failed) is an open follow-up.
- [ ] **loom user model and authentication** `{#fut-loom-auth area:acl status:deferred from:to-be-planned pr:- spec:-}`
  A loom user/identity model and authentication (the `Unauthorized` reservation is the seam).

## ingest

- [ ] **Ingest follow-ups (endpoint, splitting, schema evolution, GC)** `{#fut-ingest-followups area:ingest status:deferred from:roadmap-step3 pr:- spec:2026-06-14-ingest-datafusion-compute-path-design}`
  Deferred ingest work: the networked DataFusion endpoint, footer-only stats reads, in-batch splitting (RoundRobinBatch is per-batch today), schema evolution, delete/compaction, and orphaned-Parquet GC.

## transform

- [ ] **Programmatic / registered-plan transforms** `{#fut-programmatic-transforms area:transform status:deferred from:transform-workers pr:#57 spec:2026-06-14-transform-workers-part1-design}`
  Transforms only run SQL today; a registered-plan authoring model (swapping the compute step) and multi-output typed transforms (atomic multi-table commit) are follow-ups.
- [ ] **Transform-authoring authorization** `{#fut-transform-authoring-auth area:transform status:deferred from:transform-workers pr:#57 spec:2026-06-14-transform-workers-part1-design}`
  Transforms read raw tables as trusted pipeline code; governing who may author/run a transform is future work.
- [ ] **Transform follow-ups (incremental, DAG, Ballista, wider types)** `{#fut-transform-followups area:transform status:deferred from:roadmap-step3 pr:- spec:-}`
  Watermark/incremental output, DAG/transactional enqueue-downstream, optional Ballista escalation, and a wider output type set (canonical scalars only today). See [[fut-datafusion-type-coverage]].
- [ ] **Wider DataFusion/DuckLake type coverage** `{#fut-datafusion-type-coverage area:transform status:deferred from:cross-cutting pr:- spec:-}`
  `datafusion-io` supports only a canonical scalar set; other Arrow types (timestamps, dates, decimals, unsigned/8/16-bit ints) error `InferError::Unsupported` and the job Abandons. Extend as pipelines need it.
- [ ] **Scheduled jobs** `{#fut-scheduled-jobs area:transform status:deferred from:to-be-planned pr:- spec:-}`
  Support scheduled (cron-like) jobs.

## iceberg

- [ ] **Overwrite/replace write mode** `{#fut-iceberg-overwrite area:iceberg status:deferred from:iceberg-roadmap pr:- spec:-}`
  The Iceberg write path is append-only; the analogue of DuckLake's `replace_files` (transform overwrite parity) is deferred.
- [ ] **Physical GC of end-capped inline rows** `{#fut-iceberg-gc area:iceberg status:deferred from:iceberg-roadmap pr:- spec:2026-06-19-inline-flush-trigger-design}`
  End-capped inline rows and orphaned flush/replaced Parquet are never physically reclaimed (end-capped, not deleted, to preserve time-travel). A GC pass is outstanding.
- [ ] **Per-column stats + predicate pushdown** `{#fut-iceberg-percolumn-stats area:iceberg status:deferred from:iceberg-roadmap pr:- spec:-}`
  The mirror stores only `record_count`/`file_size` (no bounds/null counts), so there is no predicate pushdown or file skipping. Per-column stats would make Iceberg reads performant.
- [ ] **Iceberg schema evolution** `{#fut-iceberg-schema-evolution area:iceberg status:deferred from:iceberg-roadmap pr:- spec:-}`
  Slice 2 projects columns once; the mirror's `schema_version` is reserved but unused. Schema evolution is implicit longer-term work.
- [ ] **Schema cache for Iceberg serving engine** `{#fut-iceberg-schema-cache area:iceberg status:deferred from:iceberg-roadmap pr:#82 spec:2026-06-17-iceberg-datafusion-serving-engine-design}`
  `DataFusionServingEngine` registers all live tables per query (PG read + footer inference); a cache keyed by `(table, snapshot)` is a noted perf follow-up (YAGNI for now).
- [ ] **DataFusion Postgres TableProvider** `{#fut-df-postgres-tableprovider area:iceberg status:deferred from:iceberg-inline-writes pr:#86 spec:2026-06-18-iceberg-inline-writes-design}`
  The inline-read union rebuilds rows into ephemeral in-memory Parquet rather than a native PG `TableProvider` (the published crate targets DataFusion 53; loom is on 54). Revisit for transforms/compaction.
- [ ] **Real object store (S3/MinIO) for Iceberg** `{#fut-iceberg-real-object-store area:iceberg status:deferred from:iceberg-roadmap pr:- spec:-}`
  Tests use `file://`/LocalFsStorage; the vendored catalog supports S3 FileIO but loom hasn't exercised it.
- [ ] **Replace-DuckLake-with-Iceberg decision** `{#fut-replace-ducklake-decision area:iceberg status:deferred from:iceberg-roadmap pr:#76 spec:2026-06-16-iceberg-adapter-read-path-design}`
  The Iceberg adapter coexists with DuckLake (the green differential oracle). "Replace DuckLake outright" is a deferred, ergonomics-driven decision still owed — not the adapter work.
- [ ] **Iceberg ActionEngine impl** `{#fut-iceberg-actionengine area:iceberg status:deferred from:actions-part1 pr:- spec:2026-06-15-actions-part1-design}`
  The `ActionEngine` trait's reason for being — a second write backend behind the inline-write seam, for deployments that prefer Iceberg over DuckLake inline writes — is not yet implemented.
- [ ] **Arrow Flight data plane over the engine-wire** `{#fut-engine-wire-flight area:iceberg status:deferred from:engine-wire-flush-vertical pr:#108 spec:2026-06-20-engine-wire-flush-vertical-design}`
  The flush vertical is control-plane only — no bulk data crosses the wire. The `DoGet`/`DoPut` Arrow Flight data lane (the arrow-major transport that lets the engine serve/accept bulk rows over the same UDS) arrives with the read/write engine-wire vertical, not flush. See [[road-iceberg-flush-consumer]].
- [ ] **Persistent-stream AwaitJobs** `{#fut-awaitjobs-stream area:iceberg status:deferred from:engine-wire-flush-vertical pr:#108 spec:2026-06-20-engine-wire-flush-vertical-design}`
  `AwaitJobs` is a unary long-poll bridged to `PgControlPlane::await_jobs` (one LISTEN per call). A server-streaming form that holds a single listener across waits is an efficiency optimization, deferred until the wire carries enough job traffic to justify it.
- [ ] **Multiple engines / pooling / TLS / auth on the engine socket** `{#fut-engine-wire-multi-tls area:iceberg status:deferred from:engine-wire-flush-vertical pr:#108 spec:2026-06-20-engine-wire-flush-vertical-design}`
  The vertical is one engine, one UDS, local trust. Multiple engines, connection pooling, TLS, and authentication on the socket are deferred until loom runs the engine/worker across a trust boundary (cf. the existing [[fut-graceful-shutdown-tls]] for the HTTP services).

## deploy

- [ ] **Deploy follow-ups (migration Job, real object store)** `{#fut-deploy-followups area:deploy status:deferred from:roadmap-where-we-are pr:- spec:-}`
  A schema-migration Job (the chart provisions PG but doesn't migrate) and replacing the LocalFileSystem PVC with a real S3/MinIO store (removing the RWX-for-multi-pod constraint).
- [ ] **Graceful shutdown, signal handling, TLS** `{#fut-graceful-shutdown-tls area:deploy status:deferred from:service-runtime pr:- spec:2026-06-13-service-runtime-and-binaries-design}`
  The binaries ship as a minimal `serve` with no graceful shutdown/signal handling, no TLS, and no connection-pool tuning knobs.
- [ ] **S3 / remote object store for binaries** `{#fut-binaries-s3 area:deploy status:deferred from:service-runtime pr:- spec:2026-06-13-service-runtime-and-binaries-design}`
  `service_runtime` wires LocalFileSystem only; S3/remote object store is a later store slice.
- [ ] **Config and deployment ergonomics** `{#fut-config-deploy-ergonomics area:deploy status:deferred from:to-be-planned pr:- spec:-}`
  Broad improvements to configuration and deployment ergonomics.

## test

- [ ] **Socket round-trip integration test for binaries** `{#fut-socket-roundtrip-test area:test status:deferred from:service-runtime pr:- spec:2026-06-13-service-runtime-and-binaries-design}`
  The end-to-end socket round-trip test (needs an HTTP-client dep) is deferred; the binaries are exercised via config unit tests and the ingest land fixture test.

## quality

- [ ] **Unify the coercion taxonomy** `{#fut-coercion-taxonomy area:quality status:deferred from:typed-input-filters pr:- spec:2026-06-16-query-typed-input-filters-design}`
  `params::parse_value` and `filter::coerce_filter` duplicate the short `JsonRepr` repr-match; sharing one taxonomy helper would remove the duplication (input shapes `Value` vs `&str` differ enough it wasn't worth it yet).
- [ ] **Consolidate BindViolation / conformance Violation enums** `{#fut-conformance-enum-consolidation area:quality status:deferred from:typed-transforms pr:#59 spec:2026-06-15-typed-transforms-part1-design}`
  `ingest::BindViolation` and the typed-transform `Violation` are deliberate parallels; consolidating both into control-plane-core is a noted follow-up.

## devx

- [ ] **Autogenerate API specs from ontology** `{#fut-autogen-api-specs area:devx status:deferred from:to-be-planned pr:- spec:-}`
  Automatically generate API specifications from the ontology definition.
- [ ] **Python bindings** `{#fut-python-bindings area:devx status:deferred from:to-be-planned pr:- spec:-}`
  Provide Python bindings for loom.
- [ ] **loom-codehealth-reflect routine** `{#fut-codehealth-reflect area:devx status:deferred from:code-health pr:#91 spec:2026-06-19-code-health-remediation-design}`
  A future skill that mines accumulated remediation-PR outcomes and register trends for higher-level patterns, proposing batched knowledge updates. Sequenced after the fix skills produce PR history.
- [ ] **Migrate loom-stpa to vendored jq** `{#fut-stpa-vendored-jq area:devx status:deferred from:code-health pr:#91 spec:2026-06-19-code-health-routines-design}`
  The new code-health routines render via `//tools:jq`; migrating `loom-stpa` to that same vendored jq (instead of host jq) is a follow-up.

## cross-cutting

- [ ] **Deletion / GC / retention everywhere** `{#fut-gc-retention area:cross-cutting status:deferred from:roadmap-step2c pr:- spec:-}`
  Deletion, GC, and retention across concerns, including orphaned-Parquet GC.
- [ ] **Multi-tenancy (tenant_id partitioning)** `{#fut-multi-tenancy area:cross-cutting status:deferred from:roadmap-step2c pr:- spec:-}`
  Every concern is single-tenant; a `tenant_id` threaded through schemas and lookups is deferred until a deployment needs it.
- [ ] **Wider Tx composition** `{#fut-wider-tx-composition area:cross-cutting status:deferred from:critical-review pr:- spec:2026-06-07-tx-seam-decision-design}`
  `Tx` carries only `enqueue` and `emit`; the seam stays flat (a new op is added as a flat method when needed). Re-open only if a fourth transactional concern proves it insufficient.
- [ ] **metrics crate / counters & histograms** `{#fut-metrics-crate area:cross-cutting status:deferred from:tracing-pass pr:- spec:2026-06-07-tracing-instrumentation-design}`
  The tracing pass wired spans/events only; a `metrics` crate with counters/histograms is deferred to the binaries (libraries have no subscriber).
- [ ] **Muntjac integration** `{#fut-muntjac-integration area:cross-cutting status:deferred from:to-be-planned pr:- spec:-}`
  Integrate with Muntjac.
