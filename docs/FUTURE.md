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
  read-only). Wiring a catalog write op into `Tx` is future work.

## Cross-cutting

- **Dataset/target existence validation.** Lineage `emit`, ACL `grant`/`set_policy`, and
  ontology `resolve` all **store without validating** that the referenced dataset / table /
  type exists in the catalog or ontology. Cross-concern referential validation is deferred
  across the board.
- **Tenancy.** Every concern is single-tenant. Multi-tenant partitioning (a `tenant_id`
  threaded through the schemas and lookups) is deferred until a deployment needs it.
- **Wider `Tx` composition.** `Tx` carries only `enqueue` and `emit`. If a third concern ever
  needs transactional writes, revisit whether the flat-method seam should become a
  per-concern-handle aggregator (the roadmap's original, provisional sketch).
