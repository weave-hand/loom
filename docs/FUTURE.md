# Future work / deferred decisions

Deliberately-deferred capabilities and tech debt, recorded so they aren't lost. Each item
notes the concern it came from and why it was deferred. These are *not* committed roadmap
items — they're the "later, if a consumer needs it" pile.

## Lineage (Phase 5)

- **Transitive provenance / closure.** `Lineage::upstream`/`downstream` return **one hop**
  (direct edges). Full ancestry/descendancy ("everything this dataset ultimately derives
  from") is deferred. When added (e.g. `upstream_closure`, or a `depth` parameter), it needs
  a **cycle guard** — lineage graphs can cycle via re-runs — and a depth/visited bound. Kept
  out of P5 so the one-hop queries stay flat and non-recursive and "correct" stays
  unambiguous.
- **Run-grouped lifecycle stitching.** The graph is computed from each event's own
  `inputs`/`outputs` (per-event co-membership). OpenLineage allows a run to report inputs on
  its `START` event and outputs on its `COMPLETE` event across multiple events sharing a
  `run_id`; stitching those into edges is deferred. loom's own emitters (ingest/transform)
  emit a single terminal event carrying both, so co-membership suffices for now. Add stitching
  if an external emitter reports split lifecycles.
- **OpenLineage payload validation.** `payload` is stored opaquely (`jsonb`); loom neither
  parses nor validates it, and the typed envelope is supplied by the caller rather than
  derived from the payload. A validating/parsing layer is future work.
- **Pagination / filtering** on `events_for` and the graph reads. Currently unbounded.
- **The snapshot-commit third leg of the atomic unit.** P5 makes `emit` + `enqueue` atomic via
  `Tx`. The full architecture goal — snapshot commit + lineage + enqueue in one transaction —
  also needs a **transactional catalog write**, which the catalog does not expose (it has been
  read-only). **Decided (Step 2a #5):** this is an **ingest-worker (Step 3) concern** — *how*
  loom commits a snapshot is inseparable from the ingest service design and the open
  multi-writer/DuckLake-concurrency question, so it's owned there and joins the flat `Tx` seam
  as one more method when ingest defines it. See `2026-06-07-tx-seam-decision-design.md`.

## Catalog (Phase 2)

- **Schema-evolution test coverage.** The catalog MVCC delete contract (Step 2a #3)
  exercises the `end`-snapshot bound via `DROP TABLE` (and query-before-existence for the
  `begin` bound). It does **not** cover schema *evolution* — `ALTER TABLE` add/drop
  column, asserting `schema(T, old_snapshot)` differs from `schema(T, new_snapshot)` as a
  table's columns drift across snapshots. `DROP` already exercises the column `end`-bound,
  so this is a lower-risk path about *which* columns change; it adds an `ALTER` op to the
  `CatalogSeed` seam and DuckDB-CLI surface. Add it when a consumer (or a bug) makes
  column-level time travel matter.
- **File supersession / compaction.** The delete contract covers a dropped table, not
  files being *replaced* (compaction) — superseded data files gaining an `end_snapshot`
  while the table stays live. Not deterministically CLI-drivable today; deferred.

## Ontology & read path (Step 3)

From the governed link-traversal slice (`2026-06-14-query-governed-link-traversal-design.md`),
which delivered part-1 of richer read capability.

- **Object-identity dedup for traversal.** Many-to-many traversal dedups with
  `SELECT DISTINCT` over the **visible projection**, not a raw key — deliberately, because the
  target key column may itself be ACL-denied and deduping on a key would expose it (and a
  subject can't distinguish two targets with identical visible columns anyway). True
  object-identity dedup (two distinct targets sharing a visible projection kept separate) needs
  a **visible primary key** on the type. That promotes a per-type primary-key concept, which
  ties into the **derived/aggregate-properties slice (B)** — fold it in there.
- **Authoring-time physical-column validation at `define_link`.** A link's backing names
  physical columns (`from_column`/`to_column`, plus the mapping-table columns for join-table
  backings) but `define_link` stores them **without** checking they exist in the backing tables
  — keeping the ontology write path decoupled from a catalog-schema read. A bad column surfaces
  as an error at traversal time, not at authoring. Validating against `Catalog::schema` at
  authoring time is deferred (it's the same class as the cross-cutting "dataset/target existence
  validation" item below, and would layer onto whatever lands there).
- **`quote_ident` panics on a `"` in an identifier.** `sql.rs::quote_ident` `assert!`s that an
  identifier contains no double-quote (pre-existing for `compile_select`; the join-table backing
  widens the trusted-metadata surface to five identifiers per link). Today these come only from
  governed ontology authoring (trusted), so it's not an injection hole — but combined with the
  deferred column validation above, a maliciously/accidentally-authored backing column with a
  `"` would panic the request thread rather than erroring cleanly. Harden `quote_ident` to
  return a `CompileError` (or escape `"`→`""`) when authoring validation lands.
- **The remaining relational-read slices.** Part-1 (this slice) is single-link,
  source-filter→target traversal. Still to come: **(B) derived / aggregate properties**
  (`Customer.order_count` — aggregates over a traversed link); **(C) multi-hop / object-set
  traversal** (chaining links, starting from a saved object set, inverse-direction traversal,
  target-side filtering, and returning the source→target association). Both build on the
  resolvable-link + governed-join primitive delivered here.

## Cross-cutting

- **Wider DataFusion <-> DuckLake type coverage.** `datafusion-io`'s `infer_columns` /
  `write_dataset` support only a canonical scalar set (Boolean, Int32/64, Float64, Utf8/LargeUtf8).
  A transform (or ingest) whose data carries other Arrow types — timestamps, dates, decimals,
  unsigned/8/16-bit ints, the `Int32`/`Int64` a SQL aggregate may produce — currently errors
  (`InferError::Unsupported`) and the transform job Abandons. Extend the type map (and the
  DuckLake column-type strings) as real pipelines need it. (`scan_table` already pins
  `ParquetFormat::with_force_view_types(false)` so scanned strings stay canonical `Utf8`.)
- **Transform read-path robustness (minor).** Two `run_transform` edge cases noted in the
  part-1 review, outside its exercised scope: a missing input surfacing at `Catalog::files`
  (rather than `current_snapshot`) is classified transient (Retry) instead of `UnknownInput`
  (Abandon); and `scan_table` over an *empty* file list errors inside DataFusion (Retry) rather
  than yielding an empty input. Tidy when the typed-transform slice builds on the primitive.
- **Dataset/target existence validation.** Lineage `emit`, ACL `grant`/`set_policy`, and
  ontology `resolve` all **store without validating** that the referenced dataset / table /
  type exists in the catalog or ontology. Cross-concern referential validation is deferred
  across the board. When taken up:
  - **No new core seam is needed** — the adapters already co-locate every concern on one
    struct (`PgControlPlane`/`MemoryControlPlane` `impl` all five traits), so a validating
    `set_policy` can consult the ontology via its own `&self`. `ControlPlaneError` is
    `#[non_exhaustive]`, so a dedicated validation variant is additive.
  - **Same-database references can use cross-schema FK constraints** instead of an
    application read (e.g. `acl.policy.target_type → ontology.object_type`). Within one
    Postgres transaction, earlier writes are visible to later FK checks, so this gives
    *transactional* referential integrity — including for a type + policy defined in the
    **same `Tx`** — without solving read-your-writes-in-`Tx` at the app level. Caveats:
    (a) the in-memory fake has no FK engine, so it must replicate the check against its
    staged buffers to stay contract-faithful (the read concern returns, fake-only);
    (b) lineage `DatasetRef` can name **external** datasets with no catalog row, so it
    categorically cannot be FK'd — its validation, if any, stays advisory; (c) cross-schema
    FKs couple schemas we deliberately kept isolated — a tradeoff to weigh.
- **`TableRef`/`TypeName` → `DatasetRef` naming bridge.** Per the OpenLineage naming spec a
  dataset's namespace is datasource-derived (`s3://bucket`, `postgres://host:port`) and its
  name dot-qualified (`database.schema.table`). That mapping needs deployment context (the
  physical storage location), so it belongs to the Step 3 services, not `core` constants —
  `DatasetRef` already conforms to the minimal `{namespace, name}` shape. (This is why the
  "typed cross-concern identity" hardening item collapsed to docs + the `#[non_exhaustive]`
  reservation rather than a core typed bridge.)
- **Tenancy.** Every concern is single-tenant. Multi-tenant partitioning (a `tenant_id`
  threaded through the schemas and lookups) is deferred until a deployment needs it.
- **Wider `Tx` composition.** `Tx` carries only `enqueue` and `emit`. **Decided (Step 2a #5):**
  the seam **stays flat** — a new transactional op is added as a flat method when a concern
  needs it; per-concern sub-handles / a staged-op model are *not* adopted now (only 2–3 ops in
  sight). Re-open only if a fourth transactional concern (beyond queue, lineage, and the
  ingest catalog write) proves flat insufficient. `dyn ControlPlane` is likewise left
  minimal (only `begin()`; no per-concern accessors) until a consumer needs them. See
  `2026-06-07-tx-seam-decision-design.md`.
