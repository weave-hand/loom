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
- **File supersession / compaction.** The file-supersession *mechanism* — superseded data
  files gaining an `end_snapshot` while the table stays live — is delivered as the
  `Tx::replace_files` primitive behind transform **overwrite output mode**
  (`2026-06-17-overwrite-output-mode-design.md`): an overwrite expires the prior files at
  the new snapshot and writes the new ones, time travel preserved. **Selective (size-threshold)
  compaction** is now delivered too (`2026-06-17-compaction-design.md`): a new partial-supersede
  primitive `Tx::compact_files` expires a *named subset* of a table's live files and writes
  coalesced replacements (adjusting table stats by delta, leaving other files untouched), and a
  `compact_table` service function reads only the sub-threshold files, rewrites them via
  `write_dataset`, and swaps them through `compact_files` — time travel preserved. Compacted files
  get **fresh row-ids**, which is sound today because loom has no merge-on-read delete vectors;
  revisit this if row-level deletes land. Still deferred: a **queue-driven compaction job /
  operator endpoint** (the library primitive is wired but unscheduled) and **watermark-tracked
  incremental** (stateful append-delta) output.

## Ontology & read path (Step 3)

From the governed link-traversal slice (`2026-06-14-query-governed-link-traversal-design.md`),
which delivered part-1 of richer read capability.

- **Object-identity dedup for traversal.** Many-to-many traversal dedups with
  `SELECT DISTINCT` over the **visible projection**, not a raw key — deliberately, because the
  target key column may itself be ACL-denied and deduping on a key would expose it (and a
  subject can't distinguish two targets with identical visible columns anyway). True
  object-identity dedup (two distinct targets sharing a visible projection kept separate) needs
  a **visible primary key** on the type. That per-type primary-key concept now exists —
  `ObjectType.identity` landed in `2026-06-17-object-identity-association-design.md` (and powers
  source→target association). Reworking traversal dedup to key on the visible identity (rather
  than the whole visible projection) is the remaining follow-up here.
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
- **The remaining relational-read slices.** Part-1 (link traversal) is single-link,
  source-filter→target traversal. **(B) derived / aggregate properties**
  (`Customer.order_count` — aggregates over a traversed link) is now **DELIVERED**
  (`2026-06-15-derived-properties-design.md`): aggregate-over-link derived properties served
  through `read_object` as governed correlated subqueries (see the follow-ups below). **(C)
  multi-hop traversal part-1** (chaining links — `Customer → Order → LineItem`) is now also
  **DELIVERED** (`2026-06-15-query-multi-hop-traversal-design.md`): a `SELECT DISTINCT` chain of
  governed INNER JOINs, governed at every hop (see the follow-ups below). Inverse-direction
  traversal, target-side filtering, source→target association (with first-class object
  identity, `2026-06-17-object-identity-association-design.md`), and object-set inputs keyed on
  identity (`2026-06-18-object-set-inputs-design.md`) are now all delivered. All build on the
  resolvable-link + governed-join primitive delivered here.

From the derived-properties part-1 slice (`2026-06-15-derived-properties-design.md`), which
delivered aggregate-over-link derived properties (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`) on
`read_object` as governed correlated subqueries:

- **Scalar/expression derived properties.** Own-column computations are deliberately excluded
  from part-1 (aggregate-over-link only); they form a separate slice bearing a whitelisted
  SQL-expression surface (injection-safe, governable).
- **Derived properties on traversal output (`read_linked_objects`).** Part-1 serves derived
  properties on the primary `read_object` only; the link-traversal read is unchanged. Projecting
  derived properties onto traversal output is a follow-on.
- **Multi-hop aggregates and derived-on-derived.** Part-1 is single-link, non-nested. Aggregating
  over a chain of links, or a derived property that references another derived property (nested),
  is deferred.
- **Materialization.** Part-1 computes derived properties at read time as correlated subqueries.
  Materializing them (precomputed, refreshed) for hot paths is a follow-on.
- **Define-time validation.** Part-1 resolves the named link at READ time and omits the derived
  property if the link/target/column is missing — there is no authoring-time validation of a
  derived property's link/target/column/result-type at `define_type`. Same class as the deferred
  "authoring-time physical-column validation at `define_link`" item above.
- **Derived props as filter/sort targets.** Part-1 projects derived properties but they are not
  filterable/sortable (eq-filters validate against physical columns only). Making them filter/sort
  targets is a follow-on.

From the multi-hop traversal part-1 slice (`2026-06-15-query-multi-hop-traversal-design.md`), which
delivered forward, source-filtered, deduped link chaining (`GET /objects/{from}/links?path=l1,l2`) as
a `SELECT DISTINCT` chain of governed INNER JOINs, governed at every hop:

- **Inverse-direction hops** (slice-C part-3) — DELIVERED
  (`docs/superpowers/specs/2026-06-16-inverse-direction-hops-design.md`): backward link
  traversal, governed at every hop, single- and multi-hop, via `Ontology::links_to` +
  `LinkBacking::reversed()`. `/graph` part-1 (bounded recursive self-link reachability) is
  now DELIVERED; remaining `/graph` parts (multi-link / heterogeneous paths, graph-aware
  filter addressing, min-depth annotation, `/tree`, weighted edges) are recorded in the
  **Graph traversal** item below.
- **Caller-supplied target / intermediate filters.** ✅ DELIVERED
  (`2026-06-16-query-target-intermediate-filters-design.md`): per-hop typed equality filters on
  any type in a chain, addressed by `<linkname>.<column>` (bare = source), governed per type.
- **Object-set inputs.** ✅ DELIVERED — see **Object-set inputs keyed on identity** below.
- **Source→target association.** ✅ DELIVERED
  (`2026-06-17-object-identity-association-design.md`): a governed traversal can now return the
  **edge list** — source↔target identity pairs — instead of just the deduped target set, via a
  `?shape=association` flag on the existing chain routes. Output is compact id-pairs
  (`{"associations":[{"from":<id>,"to":<id>}]}`), governed both-ends like the chain read plus a
  declared, caller-visible identity required on the source and final target (else `NoIdentity` →
  400). This landed the **object identity** concept it ties into: `ObjectType` gained a
  first-class `identity: Option<String>` (the PK property), validated at bind. Follow-up still
  open: **object-set inputs keyed on identity** (see below).
- **Object-set inputs keyed on identity.** ✅ DELIVERED
  (`2026-06-18-object-set-inputs-design.md`). `?_ids=1,2,3` scopes any read — plain
  `/objects/:type`, single/multi-hop traversal, or association — to a set of source objects by
  their declared identity (an `In` predicate via `identity_in_predicate`, wired into `read_object`
  and `resolve_chain`). No declared identity → 400; denied/masked identity or uncoercible value →
  400; present-but-empty `?_ids=` → 400. Bundled in the same slice: all query control params now
  use a reserved `_` prefix (`_path`, `_direction`, `_shape`, `_ids`), and bind-time rejects any
  property or derived-property name beginning with `_` (`BindViolationReason::ReservedName`) —
  making control-param/column collisions impossible by construction. `/graph` part-1
  (bounded recursive self-link reachability) is now DELIVERED; remaining `/graph` parts are
  recorded in the **Graph traversal** item below.
- **Define-time chain/link validation.** Validate link continuity and physical columns at authoring
  time (shared with slice A's deferred `define_link` column validation). Part-1 resolves the chain at
  **read** time, so a broken chain (unknown link, a link whose `from` is not the current type)
  surfaces then as a `400` rather than at authoring.
- **Derived properties on chain output.** Serve slice B's aggregates on traversal/chain output.
  Part-1 chain output is the physical columns of the **final target** only; projecting derived
  properties onto it is a follow-on (the same class as the "derived properties on traversal output"
  item in the derived-properties block above).

From the typed-input-filters slice (`2026-06-16-query-typed-input-filters-design.md`), which
made query-param equality filters coerce to the column's declared ontology logical type (via the
`json_repr_of`/`JsonRepr` taxonomy) across `read_object`, single-hop traversal, and multi-hop
chains:

- **Comparison / set operators.** This slice is equality-only — the flat `col = value` filter
  shape is unchanged. Range and set predicates (`>`, `<`, `>=`, `<=`, `in`, ranges) are a later
  slice; they need a richer filter grammar (operator + value) on top of the typed coercion landed
  here, so they're deferred.
- **Richer filter error body.** An uncoercible value reuses `BadFilter(col)` (body = the column
  name). Reporting the *expected* type plus the offending value would make the 400 self-explanatory,
  but it widens the error contract — deferred to keep this slice's surface minimal.
- **`422` for body-bearing endpoints.** Typed filters are URI params on a body-less `GET`, so an
  uncoercible value correctly stays `400` (there is no request body to be unprocessable). Whether
  `POST /actions`'s `BadParams` (whose typed params *do* live in a request body) should become a
  `422` is a separate question to reconsider on its own, not part of this slice.
- **Unify the coercion taxonomy.** `params::parse_value` (coerces a JSON `Value` for action
  params) and `filter::coerce_filter` (coerces a `&str` for query-param filters) both duplicate the
  short `JsonRepr` repr-match. Sharing one taxonomy helper across both would remove the duplication;
  deferred because the input shapes (`Value` vs `&str`) differ enough that the common core wasn't
  worth extracting under this slice.

From the target/intermediate-filters slice (`2026-06-16-query-target-intermediate-filters-design.md`),
which made every type in a traversal chain caller-filterable (typed, governed per position) and
drew the relational/graph boundary:

- **Graph traversal — `/graph` surface.** The relational `/links` chain is a fixed, acyclic set
  of INNER JOINs over DuckLake tables — distinct link names per path, so link-name filter
  addressing is unambiguous. Genuine graph traversal (self-links, cycles, friend-of-friend,
  hierarchies, variable-length / recursive paths) lives on the separate `/graph` surface, with its
  own execution (recursive CTEs, cycle guards) and graph-aware filter addressing that resolves the
  repeated-link case `/links` rejects. Plain self-traversal still *works* on `/links` (no
  regression) — only a per-hop *filter* on a repeated link is refused.

  **Part 1 — bounded recursive self-link reachability is DELIVERED**
  (`2026-06-18-graph-reachability-design.md`): `GET /objects/:type/graph/:link?depth=N` serves the
  deduped set of objects reachable from a seed set via 1..N hops of a self-link (`from == to ==
  :type`), governed at the seed, every recursive expansion, and the final projection (`WITH
  RECURSIVE`, depth-bounded termination, deduped by the declared identity). The graph counterpart
  to the relational `/links` arc.

  Remaining `/graph` parts (deferred):
  - **Multi-link / heterogeneous paths.** Recursion across links whose `from` and `to` differ, or
    chaining multiple self-links in a single graph walk.
  - **Graph-aware filter addressing.** Per-occurrence or positional filter addressing that resolves
    the repeated-link ambiguity `/links` rejects (a path that visits the same link name twice
    cannot address per-hop filters by link name alone).
  - **Min-depth annotation.** Annotating reachable objects with the minimum hop count at which
    they were first reached (currently the deduped set carries no depth label).
  - **Shortest-path / `/tree` surface.** A separate surface (or a `/tree` split-out) serving the
    path itself — not just the reachable set — for shortest-path, spanning-tree, or hierarchical
    views. Not committed yet.
  - **Weighted edges.** Edge-weight–aware traversal (e.g. min-cost reachability), requiring
    weight columns on the join-table backing.
- **Comparison / set operators on per-hop filters.** Equality-only here, inheriting the
  typed-input-filters comparison-operators follow-up; a shared richer filter grammar would cover
  source and per-hop filters at once.

From the comparison/set-operators slice (`2026-06-16-query-comparison-set-operators-design.md`), which
gave caller filters the full `CompareOp` surface (`op:operand` grammar, ranges via repeated keys):

- **`or`-combined caller predicates.** All caller predicates are ANDed (matching the equality
  filters they generalize). A disjunction grammar (OR across caller predicates) is deferred.
- **`between:lo,hi` sugar.** Ranges are two predicates (`ge` + `le`) via repeated keys; a dedicated
  `between` operator is sugar only, deferred.
- **Literal comma inside an `in` operand.** `in:` splits on commas, so an operand containing a comma
  cannot be expressed — needs an escaping / alternate-delimiter convention.
- **Text-pattern matching** (`like`/`ilike`/`contains`). No such `CompareOp` variant exists today; a
  separate slice (and a new operator + safe rendering) would add it.
- **Predicates on derived (aggregate) properties.** Caller predicates validate against physical
  columns only; making derived properties filterable is shared with the derived-properties
  "filter/sort targets" follow-up.
- **Rename `ObjectQuery.eq_filters` / `LinkQuery`/`ChainQuery` filter fields.** The request-struct
  field is still named `eq_filters` (and the wire keys "equality filters") though it now carries the
  full operator grammar. A rename to `filters`/`predicates` for clarity is a cosmetic follow-up
  (touches http.rs + the read e2es).

## Actions (Step 3)

From the actions part-1 slice (`2026-06-15-actions-part1-design.md`), which delivered
governed typed insert (`POST /actions/{name}`, `Action::Write` enforcement, inline DuckLake
write behind an `ActionEngine` trait).

- **Action lineage atomicity (dangling slice).** Action writes emit lineage best-effort on a
  separate connection *after* the DuckDB inline write (which owns its own transaction and
  creates the DuckLake snapshot); a crash in the gap leaves a snapshot without its lineage
  event. Close via a loom-owned DuckLake write or a compaction/reconciliation pass. Also:
  the action's lineage event currently carries no inputs and `run_action` doesn't surface its
  `run_id`, so the event isn't easily queryable — a correlatable action-lineage handle is part
  of this follow-up.
- **Action parameter ↔ property conformance.** Part-1 validates the request body against the
  `ActionDef`'s declared parameters (`parse_params`), but does NOT cross-check at invoke time that
  those parameters mirror the target type's properties (same names, `satisfies` logical types, all
  required properties covered). So a misconfigured `ActionDef` (a param naming a column the type
  lacks, or omitting a required property) surfaces as an opaque insert-time 500 rather than a clear
  error. `define_action` trusts the author to mirror the type (consistent with the design's choice
  to keep params/properties independent at define time). Follow-up: enforce conformance — at
  `define_action` time (fail fast) or in `run_action` before the insert.
- **Update/delete actions.** Part-1 is insert-only; mutating existing objects is gated on the
  deferred row-supersession/compaction work.
- **Custom-logic / multi-step actions.** Part-1's only action kind is "typed insert" (params
  map to the target type's properties); actions whose params differ from properties or that
  run bespoke logic or enqueue downstream are a follow-on.
- **Fine-grained write governance.** ✅ DELIVERED (both parts). Part 1 (control plane,
  `2026-06-16-acl-action-scoped-policies-design.md`): `acl.policy` is action-scoped, so read and
  write policies are independent. Part 2 (service enforcement,
  `2026-06-16-write-enforcement-design.md`): `run_action` loads the `Write` policy and enforces
  deny-write-column + row-filter-on-insert via a pure in-memory three-valued `RowFilter` evaluator
  (`write_filter.rs`), fail-closed, with full read-parity coercion. `mask_columns` on a `Write`
  policy is **ignored** (masking is read-only) — confirmed. Deny-column counts only columns the
  action actually SETS (an omitted optional, materialized as NULL by `parse_params`, does not
  count). Open follow-up: surface a *structured* denial reason (which column / row-filter failure)
  instead of the current logs-only generic 403.
- **Iceberg `ActionEngine` impl.** The trait's reason for being — a second write backend behind
  the inline-write seam, for deployments that prefer Iceberg over DuckLake inline writes.

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
- **Multi-file DuckLake `LIMIT` mis-read (upstream DuckDB/DuckLake).** When a DuckLake table
  is backed by more than one Parquet data file, a read with a pushed-down `LIMIT` (as query-api
  issues) can reconstruct column values incorrectly (observed: int64 `id` values corrupted by a
  `+ (other_id << 8)` pattern). loom currently sidesteps this for small transform outputs by
  writing a single Parquet file (`datafusion-io::write_dataset` pins small results to one file
  via `minimum_parallel_output_files = estimate_partitions`), but legitimately large
  transform/ingest outputs still write multiple files and remain exposed. Follow-up: build a
  minimal upstream repro and file it against DuckDB/DuckLake; consider a loom-side guard (e.g.
  avoid `LIMIT` pushdown over multi-file tables, or compaction) until fixed.
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
