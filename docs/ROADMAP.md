# Roadmap register

_As of edc81cb._

Committed and sequenced work — the build plan and its as-built record. `status:
done` items are shipped (kept as the slice-by-slice history); `planned`
items are committed-but-unshipped. Deferred ideas live in
[`FUTURE.md`](FUTURE.md); known defects in [`ISSUES.md`](ISSUES.md). Grammar:
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

## cross-cutting

- [x] **Control-plane library (five concerns)** `{#road-control-plane-library area:cross-cutting status:done from:roadmap-step1 pr:- spec:2026-06-04-control-plane-queue-design}`
  Queue+worker, catalog (DuckLake read surface), ontology, acl, lineage as ports & adapters, each with one backend-agnostic contract run against an in-memory fake and real Postgres. Cross-concern `Tx` seam wired for `emit` + `enqueue`.
- [x] **Worker heartbeat** `{#road-worker-heartbeat area:cross-cutting status:done from:roadmap-step2a pr:#13 spec:2026-06-06-worker-heartbeat-design}`
  `Worker::new` leases and heartbeats the in-flight job at `lease/3`, so a handler outliving `lock_timeout` is no longer reclaimed and double-executed.
- [x] **Tx isolation/concurrency contract** `{#road-tx-isolation-contract area:cross-cutting status:done from:roadmap-step2a pr:#14 spec:2026-06-06-tx-isolation-contract-design}`
  Deterministic `tx_isolation_contract` on both adapters; the non-atomic memory `Tx::commit` fixed to hold both locks in one critical section. See [[iss-memory-tx-not-atomic]].
- [x] **Typed qualified identity (seam reservation)** `{#road-typed-qualified-identity area:cross-cutting status:done from:roadmap-step2a pr:#16 spec:-}`
  Re-scoped to reserve seams: `ControlPlaneError` is `#[non_exhaustive]`, `DatasetRef` documents the OpenLineage convention, cross-concern validation recorded as needing no new core seam. See [[fut-dataset-naming-bridge]], [[iss-existence-validation]].
- [x] **Tx seam decision (stays flat)** `{#road-tx-seam-decision area:cross-cutting status:done from:roadmap-step2a pr:- spec:2026-06-07-tx-seam-decision-design}`
  Decision record: the `Tx` seam stays flat, the catalog write leg is deferred to the ingest worker, `dyn ControlPlane` stays minimal. See [[fut-wider-tx-composition]], [[fut-transactional-catalog-write]].
- [x] **SKIP LOCKED concurrency test** `{#road-skip-locked-test area:cross-cutting status:done from:roadmap-step2b pr:- spec:-}`
  `queue_concurrency_contract` (N workers / M jobs, each claimed once) run by both adapters.
- [x] **Handler-panic policy in Worker** `{#road-handler-panic-policy area:cross-cutting status:done from:roadmap-step2b pr:- spec:2026-06-07-queue-worker-robustness-design}`
  `Worker` wraps handlers in `catch_unwind` and fails the job `Abandon` on panic, plus a test.
- [x] **Split adapter monoliths per concern** `{#road-adapter-split area:cross-cutting status:done from:roadmap-step2b pr:- spec:2026-06-07-adapter-file-split-design}`
  postgres and memory adapters split into per-concern modules mirroring `core`.
- [x] **Pagination/cursor convention on lists** `{#road-pagination-cursor area:cross-cutting status:done from:roadmap-step2b pr:#22 spec:-}`
  Pagination/cursor convention added to control-plane list reads.
- [x] **tracing spans around adapter SQL** `{#road-tracing-spans area:cross-cutting status:done from:roadmap-step2b pr:- spec:2026-06-14-control-plane-tracing-pass-design}`
  Every concern's trait methods carry a `#[tracing::instrument]` span. See [[fut-metrics-crate]].
- [x] **.sqlx offline metadata** `{#road-sqlx-offline area:cross-cutting status:done from:roadmap-step2b pr:- spec:2026-06-08-sqlx-compile-time-queries-design}`
  Committed `.sqlx` offline metadata so postgres queries are compile-time-checked.

## catalog

- [x] **Catalog MVCC delete contract** `{#road-catalog-mvcc-delete area:catalog status:done from:roadmap-step2a pr:#15 spec:2026-06-07-catalog-delete-contract-design}`
  `CatalogSeed::drop_table` + a `catalog_delete_contract` exercising the `end>s`/`begin<=s` MVCC bounds on both adapters (pg drives a real DuckLake `DROP`). See [[fut-schema-evolution-coverage]].
- [x] **Selective compaction (library primitive)** `{#road-selective-compaction area:catalog status:done from:roadmap-where-we-are pr:- spec:2026-06-17-compaction-design}`
  `Tx::compact_files` + a `compact_table` service function coalesce a table's sub-threshold Parquet files, adjusting stats by delta and rejecting a concurrent-compaction race. Library primitive only. See [[road-compaction-job]].
- [ ] **Queue-driven compaction job (engine-wire)** `{#road-compaction-job area:catalog status:planned from:2026-06-17-compaction-design pr:- spec:2026-06-22-engine-wire-compaction-flight-design}`
  Schedule the [[road-selective-compaction]] primitive as an operator-triggered queue job in the engine-wire model. An operator HTTP endpoint enqueues a `compact_table` job; a **zero-pool** worker dequeues it, streams the small files over Arrow Flight ([[road-engine-wire-flight]]), rewrites them coalesced to object store, and commits `compact_files` over a new `EngineControl::CompactTable` RPC — engine commits, worker computes, time travel preserved. Depends on [[road-engine-wire-flight]]. Promoted from [[fut-compaction-job]]. Out: watermark/incremental (append-delta) output, automatic threshold triggering, orphaned-Parquet GC ([[fut-iceberg-gc]]).

## test

- [x] **proptest round-trips for RowFilter/lineage** `{#road-proptest-roundtrips area:test status:done from:roadmap-step2b pr:#22 spec:-}`
  proptest/arbitrary round-trip tests for `RowFilter` and the lineage envelope.
- [x] **ControlPlaneError Conflict/Unauthorized resolved** `{#road-cp-error-conflict area:acl status:done from:roadmap-step2b pr:- spec:-}`
  `Conflict` wired (both ACL adapters raise it on a uniqueness race); `Unauthorized` kept as a documented reservation for the Step-3 service auth layer.

## ingest

- [x] **Part 1 — snapshot-commit primitive** `{#road-ingest-snapshot-commit area:ingest status:done from:roadmap-step3 pr:#32 spec:2026-06-09-ingest-snapshot-commit-primitive-design}`
  loom as a native DuckLake single-catalog writer: snapshot + lineage + enqueue commit in one Postgres transaction, proven against the pinned DuckDB engine.
- [x] **Part 2a — landing materializer** `{#road-ingest-materializer area:ingest status:done from:roadmap-step3 pr:- spec:2026-06-11-ingest-materializer-primitive-design}`
  Arrow → inferred DuckLake schema → Snappy Parquet → the part-1 commit, with an optional model-conformance gate. Proven by a DuckDB read-back guardrail.
- [x] **Part 2b — dataset→model binding** `{#road-ingest-model-binding area:ingest status:done from:roadmap-step3 pr:- spec:2026-06-12-ingest-dataset-model-binding-design}`
  Validated promotion binding a landed dataset to an ontology type, checking the physical schema satisfies the declared type before `define_type`.
- [x] **Part 3 — DataFusion compute path** `{#road-ingest-datafusion-path area:ingest status:done from:roadmap-step3 pr:- spec:2026-06-14-ingest-datafusion-compute-path-design}`
  The write path runs on DataFusion (SessionContext → size-estimated repartition → ParquetSink), writing N size-targeted Snappy files with per-file DuckLake stats. See [[fut-ingest-followups]].

## query

- [x] **Part 1 — governed object-read slice** `{#road-query-object-read area:query status:done from:roadmap-step3 pr:- spec:2026-06-10-query-governed-object-read-slice-design}`
  `GET /objects/{type}` resolves the ontology type to a DuckLake table, injects a minimal ACL row predicate + column projection, executes on embedded DuckDB behind a `ServingEngine`, returns JSON rows.
- [x] **Part 2 — typed JSON serialization** `{#road-query-typed-json area:query status:done from:roadmap-step3 pr:- spec:2026-06-12-query-typed-json-serialization-design}`
  The read path returns typed objects rendered by each property's logical type (`JsonRepr`); `Long` rendered as a JSON string to keep int64 precision.
- [x] **Part 3 — governed link traversal** `{#road-query-link-traversal area:query status:done from:roadmap-step3 pr:- spec:2026-06-14-query-governed-link-traversal-design}`
  `LinkBacking` (FK + join-table) and a governed `read_linked_objects` (`GET /objects/:from/links/:link`) joining source→target with both-ends governance + DISTINCT dedup. See [[fut-object-identity-dedup]], [[fut-define-link-validation]].
- [x] **Part 4 — derived properties** `{#road-query-derived-properties area:query status:done from:roadmap-step3 pr:- spec:2026-06-15-derived-properties-design}`
  Aggregate-over-link derived properties (COUNT/SUM/AVG/MIN/MAX) served through `read_object` as governed correlated subqueries; omitted when the linked type/column is unreadable. See [[fut-scalar-derived-props]], [[fut-derived-materialization]], [[fut-derived-filter-sort]].
- [x] **Part 5 — multi-hop traversal** `{#road-query-multi-hop area:query status:done from:roadmap-step3 pr:- spec:2026-06-15-query-multi-hop-traversal-design}`
  An ordered chain (`?path=l1,l2`) as a `SELECT DISTINCT` chain of governed INNER JOINs, governed at every hop, depth-capped at 4.
- [x] **Part 6 — target/intermediate filters** `{#road-query-target-filters area:query status:done from:roadmap-step3 pr:- spec:2026-06-16-query-target-intermediate-filters-design}`
  A traversal caller can filter any type in a chain by typed equality, addressed `<linkname>.<column>`, visibility-checked. See [[fut-graph-filter-addressing]].
- [x] **Part 7 — comparison/set operators** `{#road-query-comparison-ops area:query status:done from:roadmap-step3 pr:- spec:2026-06-16-query-comparison-set-operators-design}`
  Caller filters express the full `CompareOp` surface (ne/lt/le/gt/ge/in/nin/isnull/isnotnull) via a `op:operand` grammar, reusing the ACL `CompareOp` renderer. See [[fut-or-predicates]], [[fut-between-sugar]], [[fut-text-pattern-ops]], [[fut-rename-eq-filters]].
- [x] **Part 8 — inverse-direction hops** `{#road-query-inverse-hops area:query status:done from:roadmap-step3 pr:- spec:2026-06-16-inverse-direction-hops-design}`
  Every link traversable backwards via `Ontology::links_to` + `LinkBacking::reversed()`, governed at every hop; ambiguous inbound name → 400.
- [x] **Object identity + source→target association** `{#road-query-object-identity area:query status:done from:roadmap-where-we-are pr:- spec:2026-06-17-object-identity-association-design}`
  `ObjectType.identity` names a validated primary-key property; `?_shape=association` returns the edge list (identity pairs), governed both-ends.
- [x] **Object-set inputs + reserved control-param namespace** `{#road-query-object-set-inputs area:query status:done from:roadmap-where-we-are pr:- spec:2026-06-18-object-set-inputs-design}`
  `?_ids=1,2,3` scopes any read to a set by declared identity; all control params use a reserved `_` prefix and bind rejects `_`-prefixed property names.
- [x] **/graph part-1 — bounded recursive reachability** `{#road-graph-reachability area:query status:done from:roadmap-where-we-are pr:- spec:2026-06-18-graph-reachability-design}`
  `GET /objects/:type/graph/:link?depth=N` serves the deduped set reachable via 1..N hops of a self-link, backed by a depth-bounded `WITH RECURSIVE` CTE. See [[iss-recursive-cte-iceberg]].
- [x] **/graph part-2 — repeated path-cycle** `{#road-graph-path-cycle area:query status:done from:roadmap-where-we-are pr:- spec:2026-06-18-graph-path-cycle-design}`
  `?path=l1,…,lK&depth=N` follows a multi-link path forming a cycle up to N times; part-1 is the 1-element case. See [[fut-inverse-in-path]].
- [x] **/graph part-3 — multi-edge union reachability** `{#road-graph-multi-edge area:query status:done from:roadmap-where-we-are pr:- spec:2026-06-19-graph-multi-edge-design}`
  `?links=l1,…,lN&depth=N` follows any one of a set of self-links per step (union over edge types); `?path=`/`?links=` mutually exclusive.
- [x] **/graph part-B — recursive-core + relational-tail** `{#road-graph-recursive-tail area:query status:done from:roadmap-where-we-are pr:- spec:2026-06-19-graph-recursive-core-relational-tail-design}`
  A `*`-suffixed self-link recursive core followed by a forward relational tail to the projected type. See [[fut-graph-remaining]].

## ontology

- [x] **Actions part 1 — governed typed insert** `{#road-actions-typed-insert area:ontology status:done from:roadmap-step3 pr:- spec:2026-06-15-actions-part1-design}`
  Named `ActionDef` invoked via `POST /actions/{name}`; first live `Action::Write` enforcement. Insert-only DuckLake inline write behind an `ActionEngine` trait, validated against the type contract. See [[fut-update-delete-actions]], [[fut-custom-logic-actions]], [[iss-action-param-conformance]], [[iss-action-lineage-atomicity]].

## acl

- [x] **Action-scoped ACL policies** `{#road-action-scoped-policies area:acl status:done from:roadmap-step3 pr:- spec:2026-06-16-acl-action-scoped-policies-design}`
  `acl.policy` keyed on `(role, action, target)` so a role holds independent read and write policies; the read path is Read-scoped.
- [x] **Write enforcement (service)** `{#road-write-enforcement area:acl status:done from:roadmap-step3 pr:#72 spec:2026-06-16-write-enforcement-design}`
  `run_action` rejects an insert that sets a denied column or produces a row failing the policy's `row_filter` (new `write_filter.rs` evaluator). Fail-closed generic 403. See [[road-structured-write-denial]].
- [x] **Structured action write-denial reason** `{#road-structured-write-denial area:acl status:done from:2026-06-16-write-enforcement-design pr:#146 spec:2026-06-22-structured-write-denial-design}`
  `run_action` already computes a precise `WriteVerdict` (DenyColumn/DenyRow) but discards it to a bodyless 403 ([[road-write-enforcement]]). Surface a **caller-scoped** structured 403 body — `{error:"write_denied", reason:"column"|"row_filter", column?}` — naming the offending (caller-supplied) column and the column-vs-row-filter distinction, while keeping the row-filter predicate/policy id server-side (deliberate confidentiality). Query-api only; the verdict already exists, this stops discarding it. Promoted from [[fut-structured-write-denial]].

## transform

- [x] **Part 1 — queue-driven SQL transform** `{#road-transform-sql area:transform status:done from:roadmap-step3 pr:#57 spec:2026-06-14-transform-workers-part1-design}`
  A worker reads input DuckLake tables with DataFusion (`datafusion-io scan_table`), runs SQL, and commits the result as a new snapshot + lineage atomically. See [[iss-transform-read-edge-cases]], [[fut-transform-authoring-auth]].
- [x] **Part 2 — typed transforms (Type(s)→Type)** `{#road-transform-typed area:transform status:done from:roadmap-step3 pr:#59 spec:2026-06-15-typed-transforms-part1-design}`
  A typed-transform job names input/output ontology types; the worker runs SQL in type terms, validates exact conformance, and commits with type-named lineage (`TypeId`). See [[fut-programmatic-transforms]], [[fut-type-table-lineage-join]], [[fut-conformance-enum-consolidation]], [[fut-transform-followups]].
- [x] **Overwrite output mode** `{#road-overwrite-output-mode area:transform status:done from:roadmap-where-we-are pr:- spec:2026-06-17-overwrite-output-mode-design}`
  `output_mode = overwrite` replaces a table's live contents via a new DuckLake-faithful `Tx::replace_files` (prior files expire at the new snapshot; older snapshots still time-travel).

## iceberg

- [x] **Slice 1 — read path** `{#road-iceberg-read-path area:iceberg status:done from:iceberg-roadmap pr:#76 spec:2026-06-16-iceberg-adapter-read-path-design}`
  Vendored `iceberg-catalog-sql` (loom-owned, sqlx 0.9) + an `iceberg_mirror.*` MVCC projection and `IcebergCatalog impl core::Catalog`, validated by the same `catalog_contract`/`catalog_delete_contract` DuckLake passes.
- [x] **Slice 2 — write path** `{#road-iceberg-write-path area:iceberg status:done from:iceberg-roadmap pr:#78 spec:2026-06-17-iceberg-adapter-write-path-design}`
  Real Parquet via the iceberg writer chain; atomic pointer-CAS + mirror projection in one Postgres tx; concurrency-safe snapshot ids. Append-only. See [[iss-iceberg-tx-objectstore]], [[fut-iceberg-overwrite]].
- [x] **Slice 3 — loom-native DataFusion serving** `{#road-iceberg-datafusion-serving area:iceberg status:done from:iceberg-roadmap pr:#82 spec:2026-06-17-iceberg-datafusion-serving-engine-design}`
  `DataFusionServingEngine` registers each mirror table's live Parquet as a DataFusion `ListingTable` and runs governed SQL — no DuckDB in the path. See [[fut-iceberg-schema-cache]], [[fut-iceberg-percolumn-stats]].
- [x] **Slice A — inline writes + read union** `{#road-iceberg-inline-writes area:iceberg status:done from:iceberg-roadmap pr:#86 spec:2026-06-18-iceberg-inline-writes-design}`
  `inline_append` lands small writes as typed rows in a per-table inline table (mirror-only atomic commit); the serving engine unions inline rows (in-memory Parquet) with `file://` Parquet via UNION ALL. See [[iss-iceberg-inline-visibility]], [[iss-iceberg-inline-reparse]], [[fut-df-postgres-tableprovider]].
- [x] **Slice B — landing backend + ingest wiring** `{#road-iceberg-landing-backend area:iceberg status:done from:iceberg-roadmap pr:#90 spec:2026-06-18-iceberg-landing-backend-design}`
  The ingest binary selects a landing backend at boot (`LOOM_LANDING_BACKEND`); the Iceberg backend routes by byte size between `inline_append` and real Parquet, both emitting lineage atomically.
- [x] **Inline flush/compaction primitive** `{#road-iceberg-inline-flush area:iceberg status:done from:iceberg-roadmap pr:#96 spec:2026-06-19-iceberg-inline-flush-design}`
  `flush_table` drains live inline rows into a real Iceberg Parquet snapshot and end-caps them at the same snapshot — atomic, exactly-once, time-travel-correct, serialized per table by an advisory lock. See [[fut-iceberg-gc]].
- [x] **Inline flush trigger (producer)** `{#road-iceberg-inline-flush-trigger area:iceberg status:done from:iceberg-roadmap pr:#103 spec:2026-06-19-inline-flush-trigger-design}`
  A byte-size trigger on `inline_append` enqueues a `flush_table` job when live inline bytes cross `LOOM_FLUSH_BYTE_THRESHOLD`, debounced and reset on flush.
- [x] **Inline flush consumer worker (engine-wire flush vertical)** `{#road-iceberg-flush-consumer area:iceberg status:done from:iceberg-roadmap pr:#108 spec:2026-06-20-engine-wire-flush-vertical-design}`
  Delivered. A new `engine` process owns Postgres and serves a tonic `EngineControl` over a unix socket; a new **zero-pool** `worker` binary (`src/services/worker/`, no Postgres in its dep closure) connects via `GrpcQueueClient`, runs the generic `control_plane_worker::Worker` loop, and drains `flush_table` jobs by calling `flush_table` over the wire. Control-plane only — flush moves no bulk data. Proven end to end by a fixture e2e: `inline_append` past threshold → producer enqueues → worker dequeues → flush over the wire → table file-backed. See [[road-engine-wire-flight]], [[fut-engine-wire-multi-tls]], [[fut-awaitjobs-stream]], [[iss-flush-at-least-once-idempotency]].
- [ ] **Arrow Flight data plane (engine-wire)** `{#road-engine-wire-flight area:iceberg status:planned from:2026-06-20-engine-wire-flush-vertical-design pr:- spec:2026-06-22-engine-wire-compaction-flight-design}`
  Add an Arrow Flight server to the `engine` (alongside `EngineControl`) so a zero-pool worker can stream a table's live file rows out as Arrow batches — no Postgres, no object-store creds on the worker. A `Ticket` names `{schema, name, files}`; the engine resolves against the mirror, reads object store, and streams. The bulk data plane the flush vertical deliberately omitted. Introduced as the data path of [[road-compaction-job]] (its first consumer); broader consumers (transform bulk reads, exports) remain follow-on. Promoted from [[fut-engine-wire-flight]].
- [x] **Per-column stats + predicate pushdown** `{#road-iceberg-percolumn-stats area:iceberg status:done from:iceberg-roadmap pr:#132 spec:2026-06-21-iceberg-per-column-stats-design}`
  Delivered. The write path records per-file/per-column min/max + null counts in `iceberg_mirror.data_file_column_stat` (computed from the Parquet footer against parquet57, in the same tx as the snapshot commit, for both append and flush); `IcebergCatalog::files_with_stats` surfaces them; and `IcebergMirrorTableProvider` replaces the serving `ListingTable`, skipping files a `PruningPredicate` proves cannot match (no-stats/un-prunable → always kept, so governed results never change). The path off DuckDB's serving performance gap. Promoted from [[fut-iceberg-percolumn-stats]]; unblocks the [[fut-replace-ducklake-decision]] read-parity question. Deferred follow-ups: [[fut-iceberg-manifest-bounds]], [[fut-iceberg-footer-write-time-stats]], [[fut-iceberg-pruning-cost-estimates]].
- [x] **Iceberg ActionEngine (governed object writes)** `{#road-iceberg-actionengine area:iceberg status:done from:2026-06-16-iceberg-adapter-read-path-design pr:#151 spec:2026-06-22-iceberg-actionengine-design}`
  Delivered. First parity gap on the path to **Iceberg-default** ([[fut-replace-ducklake-decision]]). The query-api `UnsupportedActionEngine` is replaced by an `IcebergActionWriter` that builds the one-row action batch (shared `build_object_batch`), encodes it to an Arrow IPC stream (`encode_ipc_stream`), and routes it through `iceberg_landing::land` (the inline-write seam), so a governed typed-insert commits the row and its lineage in one Postgres transaction and is immediately readable through the Iceberg serving engine. Insert-only, canonical scalars; no default flip in this slice. Promoted from [[fut-iceberg-actionengine]]. Inherits [[iss-iceberg-inline-visibility]]/[[iss-iceberg-inline-reparse]] semantics from the landing path.
- [ ] **Iceberg overwrite/replace write mode** `{#road-iceberg-overwrite-mode area:iceberg status:planned from:iceberg-roadmap pr:- spec:2026-06-22-iceberg-overwrite-mode-design}`
  Second parity gap on the path to **Iceberg-default** ([[fut-replace-ducklake-decision]]). Add an `overwrite_parquet_snapshot` primitive: end-cap all live `iceberg_mirror.data_file` rows at the new snapshot (leaving table/columns live) + project the new files + Iceberg append + lineage, all in one Postgres transaction — mirror-faithful replace matching DuckLake's `Tx::replace_files` contract (the Iceberg twin of `snapshot_replace.rs`). Time travel preserved; replaced files keep per-column stats. Promoted from [[fut-iceberg-overwrite]]; a dependency of [[road-iceberg-transform-writes]]. External raw-metadata faithfulness deferred (same class as [[iss-iceberg-inline-visibility]], reclaimed by [[fut-iceberg-gc]]).
- [ ] **Transform output to Iceberg (polymorphic `Tx`)** `{#road-iceberg-transform-writes area:iceberg status:planned from:iceberg-roadmap pr:- spec:2026-06-22-iceberg-transform-writes-design}`
  Third parity gap on the path to **Iceberg-default** ([[fut-replace-ducklake-decision]]). Transform is already backend-agnostic (writes Parquet, stages via the format-neutral `Tx`); the blocker is that the only `Tx` impl commits DuckLake rows. Add an `IcebergControlPlane`/`IcebergTx` so `cp.begin()` is polymorphic — `create_table`/`append_files`/`replace_files`/`emit`/`commit` register transform's already-written `DataFile`s into Iceberg (append via `fast_append`+mirror project; overwrite via [[road-iceberg-overwrite-mode]]) atomically in one Postgres tx. Transform's `run.rs` is unchanged; the backend is selected at boot (`LOOM_TRANSFORM_BACKEND`). Depends on [[road-iceberg-overwrite-mode]]. Out: default flip, migrating ingest/actions onto the polymorphic `Tx`, `compact_files` ([[fut-iceberg-gc]]-adjacent), external raw-metadata faithfulness ([[iss-iceberg-inline-visibility]] class).
- [ ] **DataFusion Postgres TableProvider (inline serving)** `{#road-df-postgres-tableprovider area:iceberg status:planned from:2026-06-18-iceberg-inline-writes-design pr:- spec:2026-06-22-iceberg-inline-pg-tableprovider-design}`
  Serve un-flushed inline rows directly from Postgres through a DataFusion `TableProvider`, removing the per-query Arrow→Parquet→Arrow round-trip — the fix for [[iss-iceberg-inline-reparse]]. Vendor `datafusion-table-providers`' generic `SqlTable` + Postgres binding in-tree (the published crate is on DF 52; loom is on 54), adapted three ways: DF 52→54; the connection pool reimplemented over loom's existing sqlx `PgPool` (no second PG driver); and a base-predicate hook on the vendored `SqlTable` so the per-query snapshot filter (`begin_snapshot <= at AND (end_snapshot IS NULL OR end_snapshot > at)`) is always ANDed in — the upstream provider scans a bare table name, so MVCC visibility must be injected. `register_iceberg_table` builds the provider per query over `inline_<tid>` via `new_with_schema`; the existing `UNION` with the file provider is unchanged; `inline_parquet` + its `ArrowWriter` are deleted (`inline_live_batch` stays for flush). Read-only here — the vendored write path is unused. Promoted from [[fut-df-postgres-tableprovider]].
- [ ] **Real object store (S3/MinIO) for Iceberg** `{#road-iceberg-real-object-store area:iceberg status:planned from:iceberg-roadmap pr:- spec:2026-06-23-iceberg-real-object-store-design}`
  Production-readiness for the Iceberg engine: make it usable against S3-compatible object storage and prove it. The `iceberg` `FileIO` already speaks `s3://`; loom only ever wired `LocalFsStorageFactory` + a `file://` warehouse and never exercised S3. Make the `StorageFactory` + warehouse URI **env-driven, scheme-selected** in `service_runtime` (`LOOM_WAREHOUSE_URI` scheme picks local vs S3; standard `AWS_*` creds/endpoint, path-style implied for MinIO), add a `build_storage_factory` seam the three service `main.rs` files call (read/write/mirror code untouched — paths are already opaque URLs), and prove the S3 path with a **hermetic MinIO** `loom_fixture_test` (vendor a pinned MinIO binary like `:postgres-bin`; land+read an Iceberg table over `s3://`, assert the mirror's data-file paths are `s3://…`). Iceberg only — DuckLake's `ObjectStore` stays `file://`. Promoted from [[fut-iceberg-real-object-store]]; a precondition for non-toy Iceberg deployments under [[fut-replace-ducklake-decision]].

## deploy

- [x] **Packaging / deploy MVP (apko + Helm)** `{#road-deploy-mvp area:deploy status:done from:roadmap-where-we-are pr:- spec:-}`
  The ingest + query-api binaries ship as reproducible apko/Wolfi OCI images and a Helm chart (CNPG Postgres, object-store PVC, default-deny NetworkPolicy, optional Gateway API HTTPRoute) in a `deploy//` cell, with a release workflow. See `docs/deploy.md`. See [[fut-deploy-followups]], [[fut-graceful-shutdown-tls]], [[fut-binaries-s3]].

## devx

- [x] **Work-item planning & checkout** `{#road-work-checkout area:devx status:done from:work-checkout pr:- spec:2026-06-21-work-item-planning-checkout-design}`
  `tools/docs.sh claim/release/claims` give an atomic `refs/claim/<id>` mutex over register items (PR-lifecycle release, spec-exists gate); `loom-work-plan` and `loom-work-checkout` skills document the plan→checkout pipeline. Retired the `in-progress` roadmap status.
