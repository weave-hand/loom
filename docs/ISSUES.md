# Issues register

_As of 4861433b._

Known defects, gaps, and footguns in code that has already shipped — open items
only (resolved defects are recorded in git history, and the shipped behaviour in
[`system-capabilities/`](system-capabilities/README.md)). Deferred *capabilities* live in
[`FUTURE.md`](FUTURE.md); committed work in [`ROADMAP.md`](ROADMAP.md). Grammar:
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

## query

- [ ] **/search can serve superseded or tombstoned cold hits after an inline-shadow mutation** `{#iss-search-cold-superseded-hits area:query status:open from:2026-07-03-overwrite-vector-rebuild-design pr:- spec:-}`
  After a COW slice-1 inline-shadow UPDATE/DELETE (`#road-cow-inline-shadow`, #331), the cold Puffin index still holds the pre-mutation vector (a stale duplicate hit) or a tombstoned identity; the hot/cold merge adds the new row version but does not *suppress* the superseded cold entry, and the query-api row-filter post-filter drops it only when row filters happen to exist (`handler.rs:607`). Cold-suppression belongs to slice 2's compaction consolidation (`fut-cow-inline-shadow`), whose fold-into-new-base commit is a replace-shaped snapshot routing through the shared `rebuild_jobs_for` seam. Surfaced by the `2026-07-03-overwrite-vector-rebuild-design` investigation.
