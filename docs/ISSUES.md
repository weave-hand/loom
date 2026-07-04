# Issues register

_As of 4861433b._

Known defects, gaps, and footguns in code that has already shipped — open items
only (resolved defects are recorded in git history, and the shipped behaviour in
[`system-capabilities/`](system-capabilities/README.md)). Deferred *capabilities* live in
[`FUTURE.md`](FUTURE.md); committed work in [`ROADMAP.md`](ROADMAP.md). Grammar:
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

## ontology

- [ ] **A multi-step action's response returns only the first step's object** `{#iss-multi-object-action-response area:ontology status:open from:2026-07-01-action-multi-object-design pr:- spec:-}`
  `run_multi_step` (`#road-action-multi-object`, #343) commits every step's write atomically and its `LineageEvent.outputs` lists every step's target, but the HTTP action response (`ObjectRows`) carries only the **first** step's affected object — a caller invoking `createOrderWithLines` gets back the `Order`, not the `LineItem`s. The created child objects are readable via a subsequent governed read (and lineage records the whole graph), so this is an ergonomic gap, not a correctness one; the fix is a multi-object response envelope (all steps' affected rows keyed by bind/target) that the `post_action` handler serializes. Deferred as no caller yet needs the children inline.

- [ ] **Deleting a link silently strands derived properties that traverse it** `{#iss-delete-link-derived-dangle area:ontology status:open from:2026-07-03-api-management-crud-design pr:- spec:-}`
  `ontology.derived_property.link_name` carries no FK to `ontology.link`, and the governed read path deliberately omits a derived property whose link is missing (`query-api/src/handler.rs:317`) — so `Ontology::delete_link` (and its `DELETE /admin/links/{from}/{name}` route) can make a bind-validated derived column silently disappear from reads with no warning to the admin. Pre-existing degradation mode (a raw `define_type` never validated links either), but link deletion is the first first-class route into it. Fix candidates: a referrer guard on the delete (409 listing derived properties naming the link — needs a derived-property-by-link read), or a warning in the delete response.

## query

- [ ] **Action invocation responds `201 Created` for Update and Delete kinds** `{#iss-action-kind-status area:query status:open from:2026-07-03-api-docs-coverage-design pr:- spec:-}`
  `post_action`'s single Ok arm is `(StatusCode::CREATED, ...)` regardless of `ActionKind` (`query-api/src/http.rs:695`), so an identity-targeted PATCH and a delete both answer `201 Created`. The generated OpenAPI documents the handler truthfully (`#road-api-docs-coverage`), so the wart is now visible to every docs reader. Fixing it is a wire behavior change (existing clients may match on 201) — decide kind-true statuses (200 for Update/Delete) deliberately, update the static + generated docs together, and note it in release notes.
- [ ] **/search can serve superseded or tombstoned cold hits after an inline-shadow mutation** `{#iss-search-cold-superseded-hits area:query status:open from:2026-07-03-overwrite-vector-rebuild-design pr:- spec:-}`
  After a COW slice-1 inline-shadow UPDATE/DELETE (`#road-cow-inline-shadow`, #331), the cold Puffin index still holds the pre-mutation vector (a stale duplicate hit) or a tombstoned identity; the hot/cold merge adds the new row version but does not *suppress* the superseded cold entry, and the query-api row-filter post-filter drops it only when row filters happen to exist (`handler.rs:607`). Cold-suppression belongs to slice 2's compaction consolidation (`fut-cow-inline-shadow`), whose fold-into-new-base commit is a replace-shaped snapshot routing through the shared `rebuild_jobs_for` seam. Surfaced by the `2026-07-03-overwrite-vector-rebuild-design` investigation.

## transform

- [ ] **Lost terminal FinishRunFailed leaves a run stuck Running** `{#iss-transform-run-stuck-running area:transform status:open from:2026-07-04-transform-ergonomics-design pr:#351 spec:-}`
  The worker's run-failure reporting is best-effort: if the job is abandoned (terminal) and the `FinishRunFailed` RPC itself fails, the run record permanently reads `Running` while execution has terminally ended (`report_run_failure`, `src/services/worker/src/transform.rs` — warn-logged, deliberately non-masking). Retryable failures self-heal on the retry's re-mark; only the terminal-moment RPC loss strands the record. Fix shape: a reconciliation sweep (runs `Running` with no live queue job → `Failed("reporting lost")`), or make abandon-side reporting synchronous-with-retry. Fold in the small worker cleanup flagged by the final review (dedup the 10-line `mark_run_running` block across the two handlers, drop `handle_typed_transform_inner`'s redundant `run_id` param, refresh `handle_typed_transform`'s doc comment).

- [ ] **Corrupt transform body row poisons the schedule claim batch** `{#iss-transform-claim-poison-row area:transform status:open from:2026-07-04-transform-ergonomics-design pr:- spec:-}`
  `claim_due_schedules` (postgres, `src/control-plane/postgres/src/transforms.rs`) decodes each claimed row's `body` with `de_body(...)?` — a single row whose body no longer deserializes (schema-stale after a future `TransformBody` change, or corrupt) errors the whole batch, rolls back the transaction, and repeats every tick: it stays due, sorts earliest, and starves ALL schedules. Unreachable through today's API (define stores a validated serialized def). Fix shape: per-row skip-and-warn (or advance-and-warn) on decode failure instead of `?`.
