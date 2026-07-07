# Issues register

_As of 4861433b._

Known defects, gaps, and footguns in code that has already shipped — open items
only (resolved defects are recorded in git history, and the shipped behaviour in
[`system-capabilities/`](system-capabilities/README.md)). Deferred *capabilities* live in
[`FUTURE.md`](FUTURE.md); committed work in [`ROADMAP.md`](ROADMAP.md). Grammar:
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

## ontology

- [ ] **A multi-step action's response returns only the first step's object** `{#iss-multi-object-action-response area:ontology status:open from:2026-07-01-action-multi-object-design pr:- spec:2026-07-07-action-multi-object-response-design}`
  `run_multi_step` (`#road-action-multi-object`, #343) commits every step's write atomically and its `LineageEvent.outputs` lists every step's target, but the HTTP action response (`ObjectRows`) carries only the **first** step's affected object — a caller invoking `createOrderWithLines` gets back the `Order`, not the `LineItem`s. The created child objects are readable via a subsequent governed read (and lineage records the whole graph), so this is an ergonomic gap, not a correctness one; the fix is a multi-object response envelope (all steps' affected rows keyed by bind/target) that the `post_action` handler serializes. Deferred as no caller yet needs the children inline.

- [ ] **Deleting a link silently strands derived properties that traverse it** `{#iss-delete-link-derived-dangle area:ontology status:open from:2026-07-03-api-management-crud-design pr:- spec:2026-07-07-derived-property-link-validation-design}`
  `ontology.derived_property.link_name` carries no FK to `ontology.link`, and the governed read path deliberately omits a derived property whose link is missing (`query-api/src/handler.rs:317`) — so `Ontology::delete_link` (and its `DELETE /admin/links/{from}/{name}` route) can make a bind-validated derived column silently disappear from reads with no warning to the admin. Pre-existing degradation mode (a raw `define_type` never validated links either), but link deletion is the first first-class route into it. Fix candidates: a referrer guard on the delete (409 listing derived properties naming the link — needs a derived-property-by-link read), or a warning in the delete response. Same family, second surface (PR #374): `define_type` also never validates a derived property's **agg column or result `ty`** — the read path guards a missing link, a gone target type, and a denied target column (`query-api/src/handler.rs:300-336`), but a resolving link whose `agg` names a nonexistent target column compiles `SUM(sub."nope")` (quoted, injection-safe) and fails in DataFusion at query time — an opaque 500 for every reader of the type. `POST /admin/models` `derived` support makes a typo'd `column` a one-request operator footgun; fix shape: define-time validation of `agg` column existence (and numeric-ness for sum/avg) against the link's target type where resolvable, or the read path degrading the bad derived column to omission like the missing-link case.

## query

- [ ] **Action invocation responds `201 Created` for Update and Delete kinds** `{#iss-action-kind-status area:query status:open from:2026-07-03-api-docs-coverage-design pr:- spec:2026-07-07-action-kind-status-codes-design}`
  `post_action`'s single Ok arm is `(StatusCode::CREATED, ...)` regardless of `ActionKind` (`query-api/src/http.rs:695`), so an identity-targeted PATCH and a delete both answer `201 Created`. The generated OpenAPI documents the handler truthfully (`#road-api-docs-coverage`), so the wart is now visible to every docs reader. Fixing it is a wire behavior change (existing clients may match on 201) — decide kind-true statuses (200 for Update/Delete) deliberately, update the static + generated docs together, and note it in release notes.
- [ ] **/search can serve superseded or tombstoned cold hits after an inline-shadow mutation** `{#iss-search-cold-superseded-hits area:query status:open from:2026-07-03-overwrite-vector-rebuild-design pr:- spec:2026-07-07-search-cold-suppression-design}`
  After a COW slice-1 inline-shadow UPDATE/DELETE (`#road-cow-inline-shadow`, #331), the cold Puffin index still holds the pre-mutation vector (a stale duplicate hit) or a tombstoned identity; the hot/cold merge adds the new row version but does not *suppress* the superseded cold entry, and the query-api row-filter post-filter drops it only when row filters happen to exist (`handler.rs:607`). Cold-suppression belongs to slice 2's compaction consolidation (`fut-cow-inline-shadow`), whose fold-into-new-base commit is a replace-shaped snapshot routing through the shared `rebuild_jobs_for` seam. Surfaced by the `2026-07-03-overwrite-vector-rebuild-design` investigation.
