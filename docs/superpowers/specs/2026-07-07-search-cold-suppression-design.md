# Interim `/search` cold-hit suppression — always run the survivor post-filter — design

**Item:** `#iss-search-cold-superseded-hits`

## Problem

`/search` can serve superseded or tombstoned **cold** hits after a COW slice-1
inline-shadow mutation (`#road-cow-inline-shadow`, #331). A governed
UPDATE/DELETE on an identity-bearing vector type writes an O(change) inline
delta (row-version or tombstone) rather than rebuilding the Puffin index; the
pre-mutation vector stays in the cold index. At query time the engine's
`vector_search` scores the cold Puffin index and the hot inline delta and
combines them with `merge_topk` (`engine-serving/src/vector_search.rs:27`),
which merely concatenates and stable-sorts by distance — it does **not**
identity-dedup or suppress. So the cold index still contributes:

- **DELETE** — a tombstoned identity surfaces as a live hit (returning
  deleted data — a governance leak).
- **UPDATE** — the stale pre-mutation vector surfaces as a duplicate hit
  alongside the fresh hot-scored version of the same identity.

query-api's `vector_search` post-filter (`handler.rs:627-674`) already re-queries
the live merged serving view — which hides tombstoned and superseded rows via
its precedence-rank winner (`_loom_rn = 1`) then `_loom_tomb = false` filter
(`engine-serving/src/serving.rs:249-257`) — and retains only surviving hit
identities (`handler.rs:673`). But it is gated behind an early return:
`if g.row_filters.is_empty() { return Ok(hits); }` (`handler.rs:630-632`; the
issue cites the older `handler.rs:607`). So the suppression runs **only when the
type happens to carry a fine-grained row filter**; a type with no row policy
returns the raw engine hits, tombstones and stale duplicates included.

## Scope

Interim mitigation only: make the existing survivor post-filter run
**unconditionally** for identity-bearing vector types, so stale/tombstoned cold
hits are dropped now regardless of whether a row filter exists. Concretely:

- Lift the `row_filters.is_empty()` early return so the survivor re-query runs
  on every `/search` over an identity-bearing type.
- Identity-dedup the retained hits (keep one hit per surviving identity, the
  nearest in the engine's distance order), so an UPDATE's stale-vector
  duplicate collapses to a single hit — the survivor set-membership check alone
  removes tombstones but not same-identity duplicates.

**Non-goals (explicit):**

- **Durable cold-entry removal** — physically dropping the superseded/tombstoned
  vector from the Puffin index at compaction — stays deferred to slice-2
  compaction consolidation (`fut-cow-inline-shadow`), whose fold-into-new-base
  commit is a replace-shaped snapshot that rebuilds the index. This spec is the
  interim query-time mitigation; it does not close that item. (Referenced as a
  code span, not a cross-link, because it stays open.)
- **No change to the Puffin write path** — `iceberg_inline::write_inline_delta`,
  index build, and `merge_topk` are untouched; suppression stays a query-api
  post-filter.
- **No ANN / index-kind changes** and no change to the engine's cold/hot merge
  or MVCC snapshot anchoring.
- **Identity-less vector types are out of scope** — they keep the additive
  union (`serving.rs:121-122`) and cannot be inline-shadowed, so they accrue no
  stale cold entries; they return raw hits exactly as today (see Design).

## Design

**Where it runs today vs. where it must run.** The post-filter block
(`handler.rs:627-674`) is reached only past the `row_filters.is_empty()` early
return. The interim removes that early return so the block always executes for
an identity-bearing type. The `identity_governed()` fail-closed guard
(`handler.rs:627-629`) is unchanged and stays *ahead* of any post-filter work —
a policy that denies/masks the identity column still 403s.

**Carve-out for identity-less types.** Inline-shadow requires a declared
identity (the merged dedup is identity-keyed; identity-less types keep the
additive union and are never shadowed). So when `g.otype.identity` is `None`,
there can be no superseded/tombstoned cold entry and the post-filter is skipped
(return raw hits) — this preserves today's behavior for that class and avoids
turning the existing `NoIdentity` guard (`handler.rs:633`) into a spurious
internal error now that the block runs unconditionally. The post-filter runs
**iff** the type declares an identity.

**What identifies a superseded/tombstoned identity.** The survivor re-query
(`compile_select_with` over `g.otype.table`, scoped by
`identity_in_predicate` to the candidate hit ids — `handler.rs:644-665`) reads
the engine's live **merged view**, which already resolves MVCC:
precedence-rank winner then `_loom_tomb = false`. A tombstoned identity yields
no surviving row → its cold hit is dropped by `hits.retain(...)`
(`handler.rs:673`). For an UPDATE the identity still survives, so retain keeps
it — the added identity-dedup then collapses the (cold stale, hot fresh) pair to
one hit. With no row filters present, `SelectInputs.row_filters` is simply empty
and the compiled query degenerates to `SELECT <identity> WHERE <identity IN
candidates>` over the merged view — pure suppression, no policy scoping.

**Correctness argument.** The hot path already contributes the current
row-version of every mutated identity (scored against the probe). The merged
view is the single source of truth for "which identities are live at Q." Keeping
only hits whose identity survives that view, deduped to one per identity, yields
exactly {live identities among the k candidates} — tombstoned ids removed,
stale-vector duplicates collapsed. The interim cannot *improve* a surviving
identity's reported distance if the stale cold copy sorted nearer than the hot
copy (the nearer of the two is kept); eliminating that residual requires the
durable cold-entry removal that slice-2 owns. The set of returned identities,
however, is correct.

**Performance.** The post-filter adds one bounded engine round-trip on every
`/search` over an identity-bearing type — a `SELECT identity WHERE identity IN
(≤ k ids)` over the merged view, `k` capped by the per-request `MAX_SEARCH_K`
(`http.rs:31`). Compared with the kNN scan it is cheap and O(k). For an
unmutated table it is pure overhead (every candidate survives), but bounded and
small; slice-2's durable removal lets a later change reintroduce a fast path
that skips the re-query when the index covers the current snapshot.

## Testing

E2e `/search` tests (`src/services/query-api`, reusing `e2e-support` and the
inline-shadow serving stub the COW e2e suites use), each **with no row filter on
the type** so the reproduction depends solely on the unconditional post-filter:

1. **Superseded UPDATE.** Seed an identity-bearing vector type, land a row whose
   vector is a near neighbor of the probe, then drive a governed UPDATE that
   changes the vector (inline row-version). `/search` returns the identity
   **exactly once** — the stale cold duplicate is suppressed. Red-first:
   pre-fix it appears twice.
2. **Tombstoned DELETE.** Same seed, then a governed DELETE (inline tombstone).
   `/search` **omits** the deleted identity entirely. Red-first: pre-fix the
   tombstoned cold hit is returned.
3. **Row-filter path unchanged.** A type that *does* carry a row filter still
   suppresses (regression guard that lifting the early return didn't alter the
   existing behavior).
4. **Identity-less type unaffected.** A vector type with no declared identity
   still returns its raw additive hits (no post-filter, no spurious
   `NoIdentity` internal error).
5. `buck2 test //src/services/query-api/...` green; clippy/prek clean. No `.sqlx`
   change expected (the survivor query reuses `compile_select_with`).
