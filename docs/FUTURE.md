# Future work register

_As of edc81cb._

Deliberately-deferred capabilities and tech debt — the "later, if a consumer
needs it" pile. Each notes the concern it came from and why it was deferred.
These are *not* committed roadmap items (see [`ROADMAP.md`](ROADMAP.md)); known
defects in shipped code are in [`ISSUES.md`](ISSUES.md). Grammar:
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

## lineage

- [ ] **Transitive provenance closure + cycle guard** `{#fut-lineage-closure area:lineage status:deferred from:2026-06-05-control-plane-lineage-design pr:- spec:-}`
  `upstream`/`downstream` return one hop. Full ancestry/descendancy (`upstream_closure`, or a `depth` param) needs a cycle guard (re-runs can cycle) and a depth/visited bound. Kept out of P5 so one-hop queries stay flat. See [[fut-lineage-stitching]].
- [ ] **Run-grouped lifecycle stitching** `{#fut-lineage-stitching area:lineage status:deferred from:2026-06-05-control-plane-lineage-design pr:- spec:-}`
  The graph is per-event co-membership. OpenLineage allows a run to split inputs on START and outputs on COMPLETE across events sharing a `run_id`; stitching those is deferred (loom's emitters emit one terminal event).
- [ ] **OpenLineage payload validation** `{#fut-openlineage-validation area:lineage status:deferred from:2026-06-05-control-plane-lineage-design pr:- spec:-}`
  `payload` is stored opaquely as `jsonb`; a validating/parsing layer that derives the typed envelope from the payload is future work.
- [ ] **Pagination/filtering on lineage reads** `{#fut-lineage-pagination area:lineage status:deferred from:phase-5 pr:- spec:-}`
  `events_for` and the graph reads are unbounded.
- [ ] **Type↔table lineage layer-join** `{#fut-type-table-lineage-join area:lineage status:deferred from:2026-06-15-typed-transforms-part1-design pr:#59 spec:-}`
  Typed transforms emit type-named lineage nodes; ingest/physical transforms emit table-named nodes. The two layers don't auto-join yet (backing refs kept in the payload). A binding edge at `define_type`/bind would connect them.
- [ ] **TableRef/TypeName → DatasetRef naming bridge** `{#fut-dataset-naming-bridge area:lineage status:deferred from:critical-review pr:- spec:-}`
  An OpenLineage-conformant `{namespace, name}` mapping needs deployment context (physical storage location), so it belongs to the Step-3 services, not `core` constants.
- [ ] **Lineage DatasetRef validation** `{#fut-lineage-datasetref-validation area:lineage status:deferred from:2026-06-21-existence-validation-design pr:- spec:-}`
  Carved out of [[iss-existence-validation]]: validate the `DatasetRef`s a `LineageEvent` references on `emit`. Needs an internal-vs-external namespace convention first — a `DatasetRef` is deliberately opaque and may name an external dataset, so validating naively would reject legitimate external lineage. Relates to [[fut-dataset-naming-bridge]].

## catalog

- [ ] **Transactional catalog write** `{#fut-transactional-catalog-write area:catalog status:deferred from:2026-06-07-tx-seam-decision-design pr:- spec:-}`
  Prerequisite for the full atomic snapshot+lineage+enqueue commit; the read-only catalog doesn't expose a transactional write. Owned by the ingest worker, joins the flat `Tx` seam when ingest defines it.
- [ ] **Schema-evolution test coverage** `{#fut-schema-evolution-coverage area:catalog status:deferred from:2026-06-07-catalog-delete-contract-design pr:- spec:-}`
  The delete contract covers the `end`-bound via `DROP` but not `ALTER TABLE` add/drop column across snapshots. Adds an `ALTER` op to the `CatalogSeed` seam; deferred until column-level time travel matters.
- [ ] **Catalog-table reference validation** `{#fut-catalog-reference-validation area:catalog status:deferred from:2026-06-21-existence-validation-design pr:- spec:-}`
  Carved out of [[iss-existence-validation]]: validate `define_type`'s backing `TableRef` and ACL `PolicyTarget::Table` targets against the DuckLake catalog. Needs a `Catalog::exists(&TableRef)` read seam — the catalog is DuckDB-owned and external to loom's Postgres, so it is not cheaply checkable today and the type-only slice deliberately left `Table` targets unvalidated.
- [x] **Queue-driven compaction job + incremental output** `{#fut-compaction-job area:catalog status:promoted from:2026-06-17-compaction-design pr:- spec:-}`
  Promoted to committed work — see [[road-compaction-job]]. Operator-triggered, engine-wire compaction (zero-pool worker streams files over Arrow Flight, commits `compact_files` over a new `EngineControl::CompactTable` RPC). Watermark-tracked **incremental (append-delta)** output remains a deferred follow-on (slice 1 is full-rewrite of the small-file set). Compacted files get fresh row-ids — revisit if row-level deletes land.

## ontology

- [x] **Physical-column validation at define_link** `{#fut-define-link-validation area:ontology status:promoted from:2026-06-14-query-governed-link-traversal-design pr:- spec:2026-06-23-define-time-ontology-validation-design}`
  Promoted to committed work — see [[road-define-time-ontology-validation]]. `define_link` stores backing column names without checking they exist (keeps the ontology write path decoupled from a catalog read). A bad column surfaces at traversal time; authoring-time validation against `Catalog::schema` is the committed slice (a `bind_link` seam mirroring `bind`). See [[iss-quote-ident-panic]].
- [x] **Define-time derived-property validation** `{#fut-define-time-derived-validation area:ontology status:promoted from:2026-06-15-derived-properties-design pr:- spec:2026-06-23-define-time-ontology-validation-design}`
  Promoted to committed work — see [[road-define-time-ontology-validation]]. Part-1 resolves the named link at read time and omits the property if missing; the committed slice validates a derived property's link/target/column/result-type at `define_type` by extending the existing `bind` conformance gate.
- [x] **Define-time chain/link validation** `{#fut-define-time-chain-validation area:ontology status:dropped from:2026-06-15-query-multi-hop-traversal-design pr:- spec:-}`
  Dropped (2026-06-23): subsumed by [[road-define-time-ontology-validation]]. Once every link is column-validated and endpoint-typed at define time, any chain of *defined* links is structurally sound by construction; ad-hoc multi-hop chains are caller-supplied query paths with no stored artifact to validate at authoring time, so their read-time `BadChain`/`UnknownLink` 400 is the correct surface and remains. There is no separate define-time chain artifact to validate.
- [ ] **Update/delete actions** `{#fut-update-delete-actions area:ontology status:deferred from:2026-06-15-actions-part1-design pr:- spec:-}`
  Actions part-1 is insert-only; mutating existing objects is gated on the deferred row-supersession/compaction work.
- [ ] **Custom-logic / multi-step actions** `{#fut-custom-logic-actions area:ontology status:deferred from:2026-06-15-actions-part1-design pr:- spec:-}`
  Part-1's only action kind is "typed insert"; actions whose params differ from properties, run bespoke logic, or enqueue downstream are a follow-on.
- [ ] **Ontology versioning** `{#fut-ontology-versioning area:ontology status:deferred from:to-be-planned pr:- spec:-}`
  Version the ontology over time.
- [ ] **Migrations for ontology** `{#fut-ontology-migrations area:ontology status:deferred from:to-be-planned pr:- spec:-}`
  Support migrations for evolving the ontology.
- [ ] **Segmented reads by ontology version** `{#fut-segmented-reads area:ontology status:deferred from:to-be-planned pr:- spec:-}`
  Segment/partition reads by ontology version.
- [ ] **Model constraints** `{#fut-model-constraints area:ontology status:deferred from:to-be-planned pr:- spec:-}`
  Support constraints on models.
- [ ] **DuckLake vector storage** `{#fut-vector-ducklake area:ontology status:deferred from:2026-06-23-vector-column-type-design pr:- spec:-}`
  [[road-vector-column-type]] scopes the `vector(N)` type to the Iceberg path (`list<float>`). DuckLake/DuckDB storage of the same logical type (as `FLOAT[N]`, the DuckDB fixed-size array) is deferred — the Grimoire consumer is Iceberg-only, and the Iceberg-default direction makes a DuckLake vector path low-priority. `ducklake_physical_type` returns unsupported for `BaseType::Vector` until this lands.
- [ ] **Semantic description fields across the ontology** `{#fut-ontology-semantic-descriptions area:ontology status:deferred from:to-be-planned pr:- spec:-}`
  Optional human-readable description/documentation strings on ontology entities — object types, properties, and **links** — carried through the ontology store (a nullable column on each `define_*` row) and surfaced on reads. Foundry-style semantic metadata: nothing in the engine consumes it, so it is pure annotation today, but it is the substrate for [[fut-autogen-api-specs]] (richer generated API docs), a future ontology-browsing UI, and LLM/semantic search over the schema. A link carries its own description (the relationship's meaning), distinct from its endpoint types'. Deferred until a consumer needs it: it touches the `core` type/property/link domain structs, the postgres ontology schema (new nullable columns + a migration), and the JSON read serialization, all for metadata no current read path uses.

## query

- [ ] **Object-identity dedup for traversal** `{#fut-object-identity-dedup area:query status:deferred from:2026-06-17-object-identity-association-design pr:- spec:-}`
  Many-to-many traversal dedups with `SELECT DISTINCT` over the visible projection (the target key may be ACL-denied). `ObjectType.identity` now exists; reworking dedup to key on the visible identity is the remaining follow-up.
- [ ] **Scalar/expression derived properties** `{#fut-scalar-derived-props area:query status:deferred from:2026-06-15-derived-properties-design pr:- spec:-}`
  Own-column computations are excluded from part-1 (aggregate-over-link only); they need a whitelisted, injection-safe, governable SQL-expression surface.
- [ ] **Derived properties on traversal/chain output** `{#fut-derived-on-traversal area:query status:deferred from:2026-06-15-derived-properties-design pr:- spec:-}`
  Part-1 serves derived properties on the primary `read_object` only; projecting them onto link-traversal and multi-hop chain output is a follow-on.
- [ ] **Multi-hop aggregates and derived-on-derived** `{#fut-multi-hop-aggregates area:query status:deferred from:2026-06-15-derived-properties-design pr:- spec:-}`
  Derived-properties part-1 is single-link and non-nested. Aggregating over a chain, or a derived property referencing another, is deferred.
- [ ] **Derived-property materialization** `{#fut-derived-materialization area:query status:deferred from:2026-06-15-derived-properties-design pr:- spec:-}`
  Part-1 computes derived properties at read time as correlated subqueries; materializing them for hot paths is a follow-on.
- [ ] **Derived props as filter/sort targets** `{#fut-derived-filter-sort area:query status:deferred from:2026-06-16-query-comparison-set-operators-design pr:- spec:-}`
  Caller predicates validate against physical columns only; making derived (aggregate) properties filterable/sortable is deferred.
- [ ] **Richer filter error body** `{#fut-richer-filter-error area:query status:deferred from:2026-06-16-query-typed-input-filters-design pr:- spec:-}`
  An uncoercible value reuses `BadFilter(col)` (body = column name); reporting the expected type plus the offending value widens the error contract, deferred.
- [ ] **422 for body-bearing endpoints** `{#fut-422-body-endpoints area:query status:deferred from:2026-06-16-query-typed-input-filters-design pr:- spec:-}`
  Typed filters are URI params on a body-less GET (correctly 400). Whether `POST /actions`'s `BadParams` should become a 422 is a separate question.
- [ ] **or-combined caller predicates** `{#fut-or-predicates area:query status:deferred from:2026-06-16-query-comparison-set-operators-design pr:- spec:-}`
  All caller predicates are ANDed; a disjunction grammar (OR across predicates) is deferred.
- [ ] **between:lo,hi sugar** `{#fut-between-sugar area:query status:deferred from:2026-06-16-query-comparison-set-operators-design pr:- spec:-}`
  Ranges are two predicates (`ge`+`le`) via repeated keys; a dedicated `between` operator is sugar only.
- [ ] **Text-pattern matching operators** `{#fut-text-pattern-ops area:query status:deferred from:2026-06-16-query-comparison-set-operators-design pr:- spec:-}`
  No `like`/`ilike`/`contains` `CompareOp` exists; a separate slice would add text-pattern matching with safe rendering.
- [ ] **Rename eq_filters field** `{#fut-rename-eq-filters area:query status:deferred from:2026-06-16-query-comparison-set-operators-design pr:- spec:-}`
  The request field is still `eq_filters` though it carries the full operator grammar; a rename to `filters`/`predicates` is a cosmetic follow-up touching http.rs + e2es.
- [ ] **Inverse links inside graph path** `{#fut-inverse-in-path area:query status:deferred from:2026-06-18-graph-path-cycle-design pr:- spec:-}`
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
- [ ] **External SQL wire (Quack / Postgres-wire / Flight SQL)** `{#fut-external-sql-wire area:query status:deferred from:2026-06-11-serving-tier-quack-design pr:- spec:-}`
  loom-as-a-Quack-*server* (external DuckDB clients `ATTACH` loom) is deferred — a distribution/ergonomics concern gated on a real external consumer and a design for governance over arbitrary SQL. A Quack-*client* `ServingEngine` exists.
- [ ] **HTTP serving-engine selection (Embedded vs Quack)** `{#fut-serving-engine-selection area:query status:deferred from:2026-06-11-serving-tier-quack-design pr:- spec:-}`
  Wiring serving-engine selection (`EmbeddedDuckDb` vs Quack-client) into the HTTP service, plus production serving-process supervision, is a later slice.
- [ ] **Multi-type queries / joins & schema sidecar** `{#fut-query-multitype-joins area:query status:deferred from:roadmap-where-we-are pr:- spec:-}`
  Rich ontology reads: multi-type queries/joins, a schema sidecar, and timezone-aware timestamps are deferred query follow-ups.
- [ ] **Upstream DuckDB multi-file LIMIT fix + workaround removal** `{#fut-multi-file-limit-upstream area:query status:deferred from:multi-file-limit-guard pr:- spec:2026-06-21-multi-file-limit-guard-design}`
  loom works around an upstream DuckDB/DuckLake bug — a pushed-down `LIMIT` over a multi-file Parquet scan corrupts column values — with a serving-side `ORDER BY` barrier ([[iss-multi-file-limit-misread]]). File the upstream bug with a minimal multi-file + `LIMIT` reproduction; when loom's pinned `duckdb` crate (currently `1.10503.1`, bundled) is bumped to a fixed version, remove the barrier and its `SqlDialect::limit_needs_order_barrier()` gate. A DuckDB `SET`/`PRAGMA` disabling the offending optimization, if found, is an acceptable alternative removal path.

## acl

- [x] **ACL deny-override + column masking + role hierarchy** `{#fut-acl-deny-masking-roles area:acl status:dropped from:2026-06-10-acl-deny-override-design pr:- spec:-}`
  Dropped (2026-06-22): already implemented — this deferred idea predates the work that shipped it. Deny-override (`Effect` enum + deny-wins `check`, spec `2026-06-10-acl-deny-override-design`), column masking on reads (`Policy.mask_columns` → `'***'` SQL emission, spec `2026-06-11-acl-column-masking-design`), and role hierarchy (`add/remove_role_inheritance` + recursive-CTE closure + atomic cycle check, see [[iss-acl-role-cycle-atomic]]) are all live and contract-tested. `mask_columns` being ignored on **writes** is by design (a read-render concept, `write_filter.rs`), not a gap.
- [x] **Structured action write-denial reason** `{#fut-structured-write-denial area:acl status:promoted from:2026-06-16-write-enforcement-design pr:#72 spec:-}`
  Promoted to committed work — see [[road-structured-write-denial]]. `run_action` already computes a precise `WriteVerdict` (DenyColumn/DenyRow) but discards it to a bodyless 403; surface a caller-scoped structured reason (column name + column-vs-row-filter distinction, predicate kept server-side).
- [x] **loom user model and authentication** `{#fut-loom-auth area:acl status:promoted from:to-be-planned pr:- spec:2026-06-23-auth-password-session-design}`
  Promoted to committed work — see [[road-auth-password-session]]. The umbrella for loom's identity stack. Today every handler trusts a self-asserted `X-Loom-Subject` header (`http.rs:55-59`); the `Unauthorized` reservation is the seam. Decomposed (2026-06-23) into a sequenced track: the **password + session + authn-middleware foundation** is the committed first slice ([[road-auth-password-session]]); the remaining factors plug into its credential store and session model — see [[fut-auth-totp-mfa]], [[fut-auth-passkeys]], [[fut-auth-saml]], [[fut-auth-service-tokens]], [[fut-auth-password-lifecycle]], [[fut-auth-session-refresh]].
- [ ] **TOTP / OTP second factor** `{#fut-auth-totp-mfa area:acl status:deferred from:2026-06-23-auth-password-session-design pr:- spec:-}`
  An MFA challenge layered onto the [[road-auth-password-session]] login flow (a TOTP enrollment + verify step gating session issuance). Reuses that slice's session model; deferred until the password foundation lands.
- [ ] **Passkeys (WebAuthn)** `{#fut-auth-passkeys area:acl status:deferred from:2026-06-23-auth-password-session-design pr:- spec:-}`
  A second credential *kind* alongside passwords (WebAuthn registration + assertion) plugged into the [[road-auth-password-session]] credential store + session model. Deferred follow-on.
- [ ] **SAML bridge** `{#fut-auth-saml area:acl status:deferred from:2026-06-23-auth-password-session-design pr:- spec:-}`
  Federated identity: map a verified SAML assertion to a loom `SubjectId` and issue a session. Builds on [[road-auth-password-session]]'s session model; deferred follow-on.
- [ ] **Service-account API tokens** `{#fut-auth-service-tokens area:acl status:deferred from:2026-06-23-auth-password-session-design pr:- spec:-}`
  The non-interactive machine credential (a long-lived bearer token kind) for services/workers, alongside the human session path. Reuses the [[road-auth-password-session]] credential store + authn middleware; deferred until machine identity is needed across a trust boundary (cf. [[fut-engine-wire-multi-tls]]).
- [ ] **Password lifecycle (reset / rotation / lockout / rate-limit)** `{#fut-auth-password-lifecycle area:acl status:deferred from:2026-06-23-auth-password-session-design pr:- spec:-}`
  Password reset, email verification, rotation, account lockout, and login rate-limiting — deliberately out of the [[road-auth-password-session]] foundation, which ships create + verify only.
- [ ] **Session refresh / sliding expiry** `{#fut-auth-session-refresh area:acl status:deferred from:2026-06-23-auth-password-session-design pr:- spec:-}`
  [[road-auth-password-session]] sessions have a fixed TTL; sliding renewal / refresh tokens are a deferred efficiency/ergonomics follow-on.

## ingest

- [ ] **Ingest follow-ups (endpoint, splitting, schema evolution, GC)** `{#fut-ingest-followups area:ingest status:deferred from:2026-06-14-ingest-datafusion-compute-path-design pr:- spec:-}`
  Deferred ingest work: the networked DataFusion endpoint, footer-only stats reads, in-batch splitting (RoundRobinBatch is per-batch today), schema evolution, delete/compaction, and orphaned-Parquet GC.

## transform

- [ ] **Programmatic / registered-plan transforms** `{#fut-programmatic-transforms area:transform status:deferred from:2026-06-14-transform-workers-part1-design pr:#57 spec:-}`
  Transforms only run SQL today; a registered-plan authoring model (swapping the compute step) and multi-output typed transforms (atomic multi-table commit) are follow-ups.
- [ ] **Transform-authoring authorization** `{#fut-transform-authoring-auth area:transform status:deferred from:2026-06-14-transform-workers-part1-design pr:#57 spec:-}`
  Transforms read raw tables as trusted pipeline code; governing who may author/run a transform is future work.
- [ ] **Transform follow-ups (incremental, DAG, Ballista, wider types)** `{#fut-transform-followups area:transform status:deferred from:roadmap-step3 pr:- spec:-}`
  Watermark/incremental output, DAG/transactional enqueue-downstream, optional Ballista escalation, and a wider output type set (canonical scalars only today). See [[fut-datafusion-type-coverage]].
- [ ] **Wider DataFusion/DuckLake type coverage** `{#fut-datafusion-type-coverage area:transform status:deferred from:cross-cutting pr:- spec:-}`
  `datafusion-io` supports only a canonical scalar set; other Arrow types (timestamps, dates, decimals, unsigned/8/16-bit ints) error `InferError::Unsupported` and the job Abandons. Extend as pipelines need it.
- [ ] **Scheduled jobs** `{#fut-scheduled-jobs area:transform status:deferred from:to-be-planned pr:- spec:-}`
  Support scheduled (cron-like) jobs.

## iceberg

- [x] **Overwrite/replace write mode** `{#fut-iceberg-overwrite area:iceberg status:promoted from:iceberg-roadmap pr:- spec:-}`
  Promoted to committed work — see [[road-iceberg-overwrite-mode]]. Mirror-faithful replace (end-cap all live data files + project new, atomic, time-travel-preserving) matching DuckLake's `replace_files`. Second parity gap on the path to [[fut-replace-ducklake-decision]]; a dependency of the transform-output-to-Iceberg slice (planned next).
- [x] **Multi-writer CAS-conflict retry/backoff** `{#fut-iceberg-cas-conflict-retry area:iceberg status:promoted from:2026-06-22-iceberg-tx-objectstore-scope-design pr:- spec:2026-06-23-iceberg-cas-conflict-retry-design}`
  Promoted to committed work — see [[road-iceberg-cas-conflict-retry]]. The optimistic pointer CAS in `do_update_table` surfaces a lost race as a retryable `CatalogCommitConflicts` error, but no writer retries it — so under contention some appends fail rather than eventually committing (bumping `concurrent_appends_keep_the_mirror_consistent`'s `N` 4→8 turned it red, which is why the bump was reverted). A bounded reload-and-re-stage retry with backoff in the `append_batches*` family (and an N=8 contention test) is the committed slice; production-readiness for multi-writer S3 ([[road-iceberg-real-object-store]]) on the path to [[fut-replace-ducklake-decision]].
- [x] **Physical GC of end-capped inline rows** `{#fut-iceberg-gc area:iceberg status:promoted from:2026-06-19-inline-flush-trigger-design pr:- spec:2026-06-23-iceberg-gc-end-capped-rows-design}`
  Promoted to committed work — see [[road-iceberg-gc]]. End-capped inline rows and end-capped/replaced `data_file` Parquet are never physically reclaimed (end-capped, not deleted, to preserve time-travel). The committed slice 1 reclaims those **mirror-driven** dead bytes under an age-based retention horizon (operator-triggered per-table `gc_table`). The third source — truly **orphaned** Parquet from write-then-commit failures, which needs an object-store listing sweep + a write-race grace — stays deferred as a follow-on.
- [x] **Per-column stats + predicate pushdown** `{#fut-iceberg-percolumn-stats area:iceberg status:promoted pr:- spec:2026-06-21-iceberg-per-column-stats-design}`
  The mirror stores only `record_count`/`file_size` (no bounds/null counts), so there is no predicate pushdown or file skipping. Per-column stats would make Iceberg reads performant. Promoted to committed work — see [[road-iceberg-percolumn-stats]].
- [x] **Backfill of per-column stats for pre-existing files** `{#fut-iceberg-stats-backfill area:iceberg status:dropped from:2026-06-21-iceberg-per-column-stats-design pr:- spec:-}`
  Dropped (2026-06-22): loom is pre-deployment, so there is no pre-existing live data lacking stats to backfill. Every file written from [[road-iceberg-percolumn-stats]] onward already carries stats, and there are no earlier production files. Re-open only if a deployment predates the stats work.
- [ ] **Iceberg-manifest bound decoding** `{#fut-iceberg-manifest-bounds area:iceberg status:deferred from:2026-06-21-iceberg-per-column-stats-design pr:- spec:-}`
  Stats are computed by re-reading the Parquet footer at write time; the bounds Iceberg already records in its manifest entries are not decoded. Reading manifest bounds directly (avoiding a footer re-read) is a deferred efficiency follow-up to [[road-iceberg-percolumn-stats]].
- [ ] **Footer-only / at-write-time stats reads** `{#fut-iceberg-footer-write-time-stats area:iceberg status:deferred from:2026-06-21-iceberg-per-column-stats-design pr:- spec:-}`
  The write path reads each file's full bytes via `FileIO` to compute stats — fine for local files, wasteful against S3. A footer-only range read (or capturing stats inline as the writer flushes the file) is deferred until a real object store ([[fut-iceberg-real-object-store]]) makes the round-trip cost matter.
- [ ] **Pruning-aware cost estimates / join ordering** `{#fut-iceberg-pruning-cost-estimates area:iceberg status:deferred from:2026-06-21-iceberg-per-column-stats-design pr:- spec:-}`
  `IcebergMirrorTableProvider` skips files but does not surface per-column statistics into DataFusion's `Statistics` for cost-based planning (cardinality estimates, join ordering). Feeding the recorded bounds/null counts into the optimizer beyond file skipping is a deferred follow-up to [[road-iceberg-percolumn-stats]].
- [x] **Iceberg schema evolution** `{#fut-iceberg-schema-evolution area:iceberg status:promoted from:iceberg-roadmap pr:- spec:-}`
  Promoted to [[road-iceberg-schema-evolution]] (additive add-column + detect/reject slice): appended nullable columns evolve the mirror MVCC-correctly and populate the reserved `schema_version`; every non-additive change (drop/rename/retype/reorder/non-nullable add) is rejected instead of silently dropped. `field_id` persistence + rename/drop/type-promote and time-travel as-of-schema reconstruction remain deferred to a full-evolution slice ([[fut-iceberg-full-schema-evolution]], [[fut-iceberg-time-travel-schema]]).
- [ ] **Full Iceberg schema evolution (field_id persistence + real-metadata evolution)** `{#fut-iceberg-full-schema-evolution area:iceberg status:deferred from:2026-06-23-iceberg-schema-evolution-design pr:- spec:-}`
  The additive slice ([[road-iceberg-schema-evolution]]) is deliberately **mirror-authoritative**: an additive land writes superset Parquet and projects the new nullable columns into `iceberg_mirror.*`, but does **not** mutate the real Iceberg metadata schema (no `AddSchema`/`SetCurrentSchema`) — iceberg-rust 0.9.1 exposes no atomic schema-evolution+append transaction action. Consequence: loom-governed reads see the new column, but **external `ATTACH` clients reading the raw Iceberg table do not** — the same accepted gap class as [[iss-iceberg-inline-visibility]]. A full-evolution slice would persist Iceberg `field_id` identity in the mirror (a new `field_id` column + reading Iceberg's schema list), evolve the real metadata schema, and unlock rename / drop / type-promote / reorder (all rejected today). Deferred until external-client visibility or non-additive changes are needed.
- [ ] **Additive schema evolution on the inline path** `{#fut-iceberg-additive-inline area:iceberg status:deferred from:2026-06-23-iceberg-schema-evolution-design pr:- spec:-}`
  The inline accumulation path (`inline_append`) currently does **detect + reject only**: an identical re-append is a no-op and any divergence (additive *or* non-additive) returns the typed `schema evolution unsupported` error. Additive evolution there is deferred — the common-case land path is the Parquet path ([[road-iceberg-schema-evolution]]), and doubling the additive implementation onto the inline path wasn't worth it for this slice. A later slice could let inline accumulation absorb appended nullable columns directly instead of forcing a flush. Until that lands, an additive Parquet land is **rejected while live inline rows exist** (the caller must let the flush job run first), so the mirror's projected columns and the physical `inline_<tid>` table can never diverge — a divergence would otherwise break inline reconstruction on both the read and flush paths.
- [ ] **Time-travel as-of-schema reconstruction** `{#fut-iceberg-time-travel-schema area:iceberg status:deferred from:2026-06-23-iceberg-schema-evolution-design pr:- spec:-}`
  `iceberg_read.rs` builds the Arrow schema from `tbl.metadata().current_schema()`, so a time-travel read at a snapshot *before* a column was added still uses the current (wider) schema rather than the schema as-of that snapshot. The additive slice ([[road-iceberg-schema-evolution]]) populates `iceberg_mirror.snapshot.schema_version` as the per-table schema generation precisely so a later as-of-schema read can resolve the correct generation against it; wiring that resolution into the read path is the deferred follow-up.
- [ ] **Schema cache for Iceberg serving engine** `{#fut-iceberg-schema-cache area:iceberg status:deferred from:2026-06-17-iceberg-datafusion-serving-engine-design pr:#82 spec:-}`
  `DataFusionServingEngine` registers all live tables per query (PG read + footer inference); a cache keyed by `(table, snapshot)` is a noted perf follow-up (YAGNI for now).
- [ ] **Converge the tree on one arrow major (iceberg → arrow 58)** `{#fut-iceberg-arrow58-converge area:iceberg status:deferred from:2026-06-22-engine-wire-compaction-flight-design pr:#157 spec:-}`
  loom straddles two arrow/parquet majors: the **iceberg** stack is on arrow/parquet **57** (`iceberg 0.9.1`, the latest crates.io release — pulls `arrow-array 57.3.1`/`parquet 57.3.1`), while **DataFusion 54** + **DuckDB** + `ingest`/`query-api` are on arrow/parquet **58**. iceberg is the laggard; the fix is to converge *up* (iceberg → 58), not down (downgrading DataFusion to an older arrow-57 release is a real feature regression and DuckDB removal doesn't free DataFusion's arrow pin). The arrow-58 migration is **already merged on iceberg-rust `main`** (its `Cargo.toml` pins `arrow = "58"`/`parquet = "58"`); it is simply **not in a published crate yet** (latest release `0.9.1`, 2026-05-06, is still arrow 57). **Trigger:** iceberg-rust publishes the post-0.9.1 release carrying the arrow-58 migration — then bump `//third-party:iceberg` and, in lockstep, `arrow-flight` and the first-party arrow-57 crates (`control-plane-postgres`, `engine`, `worker`) from 57 → 58, absorbing both the arrow major API churn (`RecordBatch`/`Schema`/parquet-reader sites; the Flight encode/decode; the stats footer reader) and the iceberg `0.9 → 0.x` API delta, gated on the full `buck2 test //src/...` + the `.sqlx` cache + the `duckdb 1.10503.1` pin guard. **Payoff:** one arrow major everywhere — the per-boundary 57↔58 batch conversions vanish, the `//third-party:arrow-*-57`/`-58` target families collapse to one, the `parquet-57`-vs-default-`parquet` footgun goes away, and the iceberg side shares types directly with the DataFusion serving path (helps an eventual [[fut-replace-ducklake-decision]] / DuckDB-removal story). Surfaced building [[road-engine-wire-flight]], which had to pin `arrow-flight` to 57 to match the iceberg chain.
- [x] **DataFusion Postgres TableProvider** `{#fut-df-postgres-tableprovider area:iceberg status:promoted from:2026-06-18-iceberg-inline-writes-design pr:#86 spec:-}`
  Promoted to [[road-df-postgres-tableprovider]] — committed to as the fix for [[iss-iceberg-inline-reparse]] (serve inline rows directly from Postgres instead of rebuilding ephemeral Parquet per query). The original deferral was a DataFusion-version mismatch (the published `datafusion-table-providers` crate targets DF 52; loom is on 54), surmountable by vendoring + adapting the provider rather than importing it.
- [x] **Real object store (S3/MinIO) for Iceberg** `{#fut-iceberg-real-object-store area:iceberg status:promoted from:iceberg-roadmap pr:- spec:2026-06-23-iceberg-real-object-store-design}`
  Promoted to [[road-iceberg-real-object-store]] — committed to as production-readiness for the Iceberg engine. Tests use `file://`/LocalFsStorage; the vendored catalog/`iceberg` `FileIO` supports S3 but loom never wired or exercised it. The slice makes the `StorageFactory` + warehouse URI env-driven (scheme-selected) and proves the S3 path with a hermetic MinIO round-trip. Iceberg only; DuckLake's object store stays `file://`.
- [ ] **Replace-DuckLake-with-Iceberg decision** `{#fut-replace-ducklake-decision area:iceberg status:deferred from:2026-06-16-iceberg-adapter-read-path-design pr:#76 spec:-}`
  The Iceberg adapter coexists with DuckLake (the green differential oracle). Direction set (2026-06-22): **Iceberg-default, DuckLake kept** — flip the `LOOM_LANDING_BACKEND`/`LOOM_SERVING_BACKEND` boot defaults to Iceberg, keeping DuckLake selectable as a fallback and as the test oracle (not "replace outright"). Three production parity gaps gate the flip, sequenced as their own slices: governed action writes ([[road-iceberg-actionengine]]), overwrite/replace mode ([[road-iceberg-overwrite-mode]]), and transform output to Iceberg ([[road-iceberg-transform-writes]]). This umbrella tracks the final default-flip once those land.
- [x] **Iceberg ActionEngine impl** `{#fut-iceberg-actionengine area:iceberg status:promoted from:2026-06-15-actions-part1-design pr:- spec:-}`
  Promoted to committed work — see [[road-iceberg-actionengine]]. The `ActionEngine` trait's reason for being: a second write backend behind the inline-write seam, routing governed object writes through `iceberg_landing::land` so the row and its lineage commit in one Postgres transaction. First parity gap on the path to [[fut-replace-ducklake-decision]].
- [x] **Arrow Flight data plane over the engine-wire** `{#fut-engine-wire-flight area:iceberg status:promoted from:2026-06-20-engine-wire-flush-vertical-design pr:#108 spec:-}`
  Promoted to committed work — see [[road-engine-wire-flight]]. The flush vertical is control-plane only — no bulk data crosses the wire. The Arrow Flight data lane (engine serves table/file rows as Arrow streams to zero-pool worker clients over the same wire) is introduced as the data path for [[road-compaction-job]], its first consumer; broader uses (transform bulk reads, exports) remain follow-on. See [[road-iceberg-flush-consumer]].
- [ ] **Persistent-stream AwaitJobs** `{#fut-awaitjobs-stream area:iceberg status:deferred from:2026-06-20-engine-wire-flush-vertical-design pr:#108 spec:-}`
  `AwaitJobs` is a unary long-poll bridged to `PgControlPlane::await_jobs` (one LISTEN per call). A server-streaming form that holds a single listener across waits is an efficiency optimization, deferred until the wire carries enough job traffic to justify it.
- [ ] **Multiple engines / pooling / TLS / auth on the engine socket** `{#fut-engine-wire-multi-tls area:iceberg status:deferred from:2026-06-20-engine-wire-flush-vertical-design pr:#108 spec:-}`
  The vertical is one engine, one UDS, local trust. Multiple engines, connection pooling, TLS, and authentication on the socket are deferred until loom runs the engine/worker across a trust boundary (cf. the existing [[fut-graceful-shutdown-tls]] for the HTTP services).

## deploy

- [ ] **Deploy follow-ups (migration Job, real object store)** `{#fut-deploy-followups area:deploy status:deferred from:roadmap-where-we-are pr:- spec:-}`
  A schema-migration Job (the chart provisions PG but doesn't migrate) and replacing the LocalFileSystem PVC with a real S3/MinIO store (removing the RWX-for-multi-pod constraint).
- [ ] **Graceful shutdown, signal handling, TLS** `{#fut-graceful-shutdown-tls area:deploy status:deferred from:2026-06-13-service-runtime-and-binaries-design pr:- spec:-}`
  The binaries ship as a minimal `serve` with no graceful shutdown/signal handling, no TLS, and no connection-pool tuning knobs.
- [ ] **S3 / remote object store for binaries** `{#fut-binaries-s3 area:deploy status:deferred from:2026-06-13-service-runtime-and-binaries-design pr:- spec:-}`
  `service_runtime` wires LocalFileSystem only; S3/remote object store is a later store slice.
- [ ] **Config and deployment ergonomics** `{#fut-config-deploy-ergonomics area:deploy status:deferred from:to-be-planned pr:- spec:-}`
  Broad improvements to configuration and deployment ergonomics.

## test

- [x] **Socket round-trip integration test for binaries** `{#fut-socket-roundtrip-test area:test status:promoted from:2026-06-13-service-runtime-and-binaries-design pr:- spec:-}`
  Promoted to [[road-e2e-http-client]], which adds the over-the-wire e2e layer (real `TcpListener` via `service_runtime::serve` + a `reqwest` client) the deferral was waiting on — the HTTP-client dep is now justified (`reqwest` is already vendored). The actual-binary-*subprocess* variant (exec the built binary to also cover `main.rs` + `Config::from_env`) remains out of that slice and is the residual deferral, tracked as [[fut-binary-subprocess-smoke]].
- [ ] **Actual-binary subprocess smoke** `{#fut-binary-subprocess-smoke area:test status:deferred from:2026-06-23-e2e-http-client-design pr:- spec:-}`
  The over-the-wire e2e ([[road-e2e-http-client]]) spawns the routers in-process via `service_runtime::serve`, covering `serve()` + the routers over a real socket. Booting the built `ingest-bin`/`query-api-bin` as a subprocess to also cover `main.rs` + `Config::from_env` over a spawned process stays deferred — those paths are covered today by the `runtime` config unit test + the `runtime_land` fixture.

## quality

- [ ] **Unify the coercion taxonomy** `{#fut-coercion-taxonomy area:quality status:deferred from:2026-06-16-query-typed-input-filters-design pr:- spec:-}`
  `params::parse_value` and `filter::coerce_filter` duplicate the short `JsonRepr` repr-match; sharing one taxonomy helper would remove the duplication (input shapes `Value` vs `&str` differ enough it wasn't worth it yet).
- [ ] **Consolidate BindViolation / conformance Violation enums** `{#fut-conformance-enum-consolidation area:quality status:deferred from:2026-06-15-typed-transforms-part1-design pr:#59 spec:-}`
  `ingest::BindViolation` and the typed-transform `Violation` are deliberate parallels; consolidating both into control-plane-core is a noted follow-up.

## devx

- [ ] **Autogenerate API specs from ontology** `{#fut-autogen-api-specs area:devx status:deferred from:to-be-planned pr:- spec:-}`
  Automatically generate API specifications from the ontology definition.
- [ ] **Python bindings** `{#fut-python-bindings area:devx status:deferred from:to-be-planned pr:- spec:-}`
  Provide Python bindings for loom.
- [ ] **loom-codehealth-reflect routine** `{#fut-codehealth-reflect area:devx status:deferred from:2026-06-19-code-health-remediation-design pr:#91 spec:-}`
  A future skill that mines accumulated remediation-PR outcomes and register trends for higher-level patterns, proposing batched knowledge updates. Sequenced after the fix skills produce PR history.
- [ ] **Migrate loom-stpa to vendored jq** `{#fut-stpa-vendored-jq area:devx status:deferred from:2026-06-19-code-health-routines-design pr:#91 spec:-}`
  The new code-health routines render via `//tools:jq`; migrating `loom-stpa` to that same vendored jq (instead of host jq) is a follow-up.

## cross-cutting

- [ ] **Deletion / GC / retention everywhere** `{#fut-gc-retention area:cross-cutting status:deferred from:roadmap-step2c pr:- spec:-}`
  Deletion, GC, and retention across concerns, including orphaned-Parquet GC.
- [ ] **Multi-tenancy (tenant_id partitioning)** `{#fut-multi-tenancy area:cross-cutting status:deferred from:roadmap-step2c pr:- spec:-}`
  Every concern is single-tenant; a `tenant_id` threaded through schemas and lookups is deferred until a deployment needs it.
- [ ] **Wider Tx composition** `{#fut-wider-tx-composition area:cross-cutting status:deferred from:2026-06-07-tx-seam-decision-design pr:- spec:-}`
  `Tx` carries only `enqueue` and `emit`; the seam stays flat (a new op is added as a flat method when needed). Re-open only if a fourth transactional concern proves it insufficient.
- [ ] **metrics crate / counters & histograms** `{#fut-metrics-crate area:cross-cutting status:deferred from:2026-06-07-tracing-instrumentation-design pr:- spec:-}`
  The tracing pass wired spans/events only; a `metrics` crate with counters/histograms is deferred to the binaries (libraries have no subscriber).
- [ ] **Muntjac integration** `{#fut-muntjac-integration area:cross-cutting status:deferred from:to-be-planned pr:- spec:-}`
  Integrate with Muntjac.
