# Issues register

_As of 4861433b._

Known defects, gaps, and footguns in code that has already shipped — open items
only (resolved defects are recorded in git history, and the shipped behaviour in
[`system-capabilities/`](system-capabilities/README.md)). Deferred *capabilities* live in
[`FUTURE.md`](FUTURE.md); committed work in [`ROADMAP.md`](ROADMAP.md). Grammar:
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

## ontology

- [ ] **`mode=stream` (log) against an existing CDC table is silently accepted** `{#iss-stream-log-vs-cdc-declare area:ontology status:open from:2026-07-07-stream-pk-cdc-tables-design pr:- spec:2026-07-07-stream-pk-cdc-tables-design}`
  `reconcile_stream_mode` (`src/control-plane/postgres/src/stream.rs`) rejects a `mode=cdc` request against a table of a different kind, but has NO symmetric guard for the reverse: a `mode=stream&buckets=N` (log) write whose bucket count matches an already-declared **CDC** table falls through the `(Some(n), Some(m))` arm — whose kind-check is gated on `StreamDecl::Cdc` — and is accepted, leaving the registry `kind='cdc'` (rows still hash-bucket, not log-bucket). The asymmetry is essentially forced by slice-2's byte-identical-Log-arm constraint (adding a kind-check to the Log arm would perturb the log/batch path the constraint pins). Reachability is obscure: CDC is declarable only via `POST /models/{type}?mode=cdc`, while `mode=stream` arrives via `POST /datasets/{schema}/{table}`, so both surfaces must target the same physical table. Fix shape: a kind-aware reject in the Log-declare arm that leaves the pure log/batch (no pre-existing CDC row) path untouched, or a single normalized declaration seam both endpoints share.

## query

- [ ] **/search can serve superseded or tombstoned cold hits after an inline-shadow mutation** `{#iss-search-cold-superseded-hits area:query status:open from:2026-07-03-overwrite-vector-rebuild-design pr:- spec:2026-07-07-search-cold-suppression-design}`
  After a COW slice-1 inline-shadow UPDATE/DELETE (`#road-cow-inline-shadow`, #331), the cold Puffin index still holds the pre-mutation vector (a stale duplicate hit) or a tombstoned identity; the hot/cold merge adds the new row version but does not *suppress* the superseded cold entry, and the query-api row-filter post-filter drops it only when row filters happen to exist (`handler.rs:607`). Cold-suppression belongs to slice 2's compaction consolidation (`fut-cow-inline-shadow`), whose fold-into-new-base commit is a replace-shaped snapshot routing through the shared `rebuild_jobs_for` seam. Surfaced by the `2026-07-03-overwrite-vector-rebuild-design` investigation.

## ui

- [ ] **Transforms drawer Run/Delete swallow non-401 errors; Runs tab won't refetch from itself** `{#iss-ui-transforms-drawer-errors area:ui status:open from:2026-07-07-transforms-admin-surface-design pr:- spec:-}`
  The Transforms surface's drawer action paths lack an error surface: `TransformDrawer` has no error slot, so a 400/404/network failure on **Run saved** or **Delete** is silent (a failed delete looks like a no-op). A `401` still fails closed to logout, and the editor-form Define / Run-ad-hoc paths correctly surface `Rejected(body)` via the form's server-error line — only the drawer actions are affected. Fix: add an `action_error: Option<AttrValue>` prop to `TransformDrawer`, render it beside the action buttons, and set it (reusing the surface's `server_error` state) in the `on_run`/`on_delete` `Err(_)` arms. Related edge: the runs-history effect is keyed on `(selection, active_tab)`, so a **Run** fired while the Runs tab is already active clears `tf_runs` but does not re-fire the fetch (the normal flow — Run from the Definition tab flips the tab and refetches — works); fold a runs-generation bump into the Run success path. Both are low-frequency admin-only paths surfaced by this spec's whole-branch review. Sibling of `#iss-ui-swallowed-fetch-errors`.
