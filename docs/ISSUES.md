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

- [ ] **`/search` over a vector type with a live inline row 500s on the first merge-view query** `{#iss-search-vector-merge-view-nullable area:query status:open from:2026-07-07-search-cold-suppression-design pr:- spec:-}`
  `engine-serving`'s `build_merge_view` (`src/services/engine-serving/src/serving.rs`) UNION-ALLs the file tier (non-null `vector(N)` column) with the inline tier and projects back to the mirror schema, which declares that column **non-nullable**; a tombstone/inline delta makes the column nullable in the union, so the **first** execution of the merged view for such a type throws `Invalid argument error: Column '<vec>' is declared as non-nullable but contains null values` (500) — subsequent identical queries succeed (process-global DataFusion schema-inference nondeterminism). Latent until now: no test combined a live inline row with a `vector(N)` column, and `/search`'s survivor post-filter only reached the merge view for row-filtered types — now, after the `#iss-search-cold-superseded-hits` fix, for every identity-bearing type. Surfaced by that fix's e2e (`vector_search_cold_suppression_e2e.rs`, both cases `#[ignore]`'d pending this). Fix shape: make the merged view's projected `vector(N)` nullability consistent with the union (widen to nullable, or coerce), and eliminate the first-query nondeterminism; then un-ignore the two e2e cases.

## ui

- [ ] **Transforms drawer Run/Delete swallow non-401 errors; Runs tab won't refetch from itself** `{#iss-ui-transforms-drawer-errors area:ui status:open from:2026-07-07-transforms-admin-surface-design pr:- spec:-}`
  The Transforms surface's drawer action paths lack an error surface: `TransformDrawer` has no error slot, so a 400/404/network failure on **Run saved** or **Delete** is silent (a failed delete looks like a no-op). A `401` still fails closed to logout, and the editor-form Define / Run-ad-hoc paths correctly surface `Rejected(body)` via the form's server-error line — only the drawer actions are affected. Fix: add an `action_error: Option<AttrValue>` prop to `TransformDrawer`, render it beside the action buttons, and set it (reusing the surface's `server_error` state) in the `on_run`/`on_delete` `Err(_)` arms. Related edge: the runs-history effect is keyed on `(selection, active_tab)`, so a **Run** fired while the Runs tab is already active clears `tf_runs` but does not re-fire the fetch (the normal flow — Run from the Definition tab flips the tab and refetches — works); fold a runs-generation bump into the Run success path. Both are low-frequency admin-only paths surfaced by this spec's whole-branch review. Sibling of `#iss-ui-swallowed-fetch-errors`.
