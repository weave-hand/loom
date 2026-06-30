# Scalable copy-on-write — slice 1: inline-shadow merge-on-read + CAS guard

- **Date:** 2026-06-30
- **Area:** ontology
- **Register items:** carves slice 1 of [[fut-cow-inline-shadow]]; consumes [[fut-cow-cas-guard]]; mints [[road-cow-inline-shadow]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

A governed UPDATE/DELETE costs **O(change)**, not O(table). Instead of rewriting the whole
table's Parquet on every mutation ([[road-update-delete-actions]]), a mutation writes a tiny
**inline delta** — an UPDATE writes a new inline **row-version**, a DELETE writes an inline
**tombstone** — and the engine read path **merges on read**, letting the latest inline
version *shadow* the file-tier row by identity (and a tombstone hide it). The write is
guarded by per-identity **compare-and-swap** so a concurrent mutation can't be lost. This is
slice 1 of the scalable-COW destination ([[fut-cow-inline-shadow]]); tombstone-aware
**compaction consolidation** and **identity-change/upsert** are slices 2 and 3.

## Current state

[[road-update-delete-actions]] does **whole-table** COW: `action_writer::write_object`
(`engine-serving/src/action_writer.rs:57`) reads the full table (inline ∪ files), applies
the single-object change in memory, and calls `iceberg_landing::overwrite_parquet_snapshot`
to rewrite every file. Two costs:

- **Write amplification:** O(table) bytes rewritten per single-row mutation.
- **Lost-update race ([[fut-cow-cas-guard]]):** the read→modify→overwrite window is not
  atomic — a concurrent writer that commits between the read and the overwrite is silently
  clobbered.

The serving read path (`engine-serving/src/serving.rs:111`) unions the file tier with the
inline tier via `DataFrame::union` — **additive UNION ALL**. This is correct today only
because inline rows are *new* objects (no identity collision with file rows). Inline MVCC
columns are `loom_row_id` / `begin_snapshot` / `end_snapshot`
(`iceberg_mirror.inline_<tid>`).

## Design

### Tombstone + version model (inline tier)

Add one column to the inline tier: `loom_tombstone boolean NOT NULL DEFAULT false`. The
inline tier now carries three row kinds, all keyed by the type's **identity** column (the
merge key):

- **append** — a brand-new object (today's behaviour), `loom_tombstone=false`, no prior
  identity.
- **version** — a mutation of an existing object: a new inline row with the **same identity
  value**, a later `begin_snapshot`, `loom_tombstone=false`, carrying the full post-mutation
  row.
- **tombstone** — a delete: a new inline row with the identity value, `loom_tombstone=true`,
  data columns null.

Versions and tombstones are **shadow deltas** (they shadow a base/earlier row by identity);
appends are not. A row is a shadow delta iff it is a tombstone or its identity already exists
in the base/file tier — known at write time (the mutation targets an existing object).

**Identity is required.** Shadow-COW needs a merge key, so it applies only to types with a
declared `identity`. Identity-less types keep whole-table COW (unchanged). Vector-bearing
types stay rejected (`ensure_cow_supported`, [[fut-cow-arrow-native]]) — orthogonal.

### Merge-on-read (engine read path)

Replace the additive union with an **identity-dedup merge** when the type has an identity:

- Across (file ∪ inline) rows for a given identity, the **winner** is the row with the
  greatest `begin_snapshot` (file-tier rows rank below any inline delta — deltas are written
  strictly after the base, so inline always shadows file for the same identity; among inline
  rows the max `begin_snapshot` wins).
- If the winner is a **tombstone**, the identity is **omitted** entirely (the file row is
  hidden too).
- Otherwise the winner's row is emitted.

Concretely: file rows whose identity is **not** present among live inline rows pass through;
identities present inline are represented **only** by their latest non-tombstone inline
version. The identity column is threaded into the serving build (`build_inline_provider` /
the union site) from the `ObjectType` — the read path becomes identity-aware where today it
is identity-agnostic. MVCC visibility (`begin_snapshot <= q AND (end_snapshot is null OR
end_snapshot > q)`) is unchanged; the dedup runs over the already-visible set so time-travel
stays correct.

Governance is unchanged: the merge runs **below** the governed `GovernedTableProvider` /
`compile_select_with` layer, so row filters and column masks apply to the merged result
exactly as before.

### O(change) mutation write (replaces overwrite for identity types)

`write_object` for an identity-bearing type stops calling `overwrite_parquet_snapshot` and
instead, in one mirror commit (snapshot + lineage, as today):

- **UPDATE(id, delta):** resolve the object's current live version (latest inline version or
  the file row), apply the caller's field delta (the existing PATCH semantics + the
  fine-grained column/row-filter ACL check on the affected row — preserved verbatim), and
  insert a **version** inline row.
- **DELETE(id):** insert a **tombstone** inline row for `id`.

No file rewrite; the commit writes one inline row.

### CAS guard (per-identity optimistic concurrency) — [[fut-cow-cas-guard]]

The read→commit window is closed by a **compare-and-swap on the identity's current
version**, bundled here because the new write path is where it belongs (build it safe, don't
harden it later):

- the mutation records the `begin_snapshot` (version id) of the live row it read for `id`;
- the inline-delta insert is **conditional**: commit only if no inline row for `id` with a
  greater `begin_snapshot` exists at commit time (the latest live version is still the one we
  read);
- if it advanced, **abort** and retry on the fresh latest (bounded retry, mirroring the
  compaction conflict→retry loop). A lost update becomes a retried update.

This is per-object (not whole-table) optimism — strictly stronger and cheaper than the
whole-table snapshot CAS the original [[fut-cow-cas-guard]] envisioned.

### Flush interaction (required guard)

The existing byte-trigger flush (`flush_table`) drains live inline rows into Parquet. Flushed
**naively, a shadow delta becomes a duplicate file row** (the old file row + the flushed new
version share an identity) — corruption. Until slice 2's tombstone-aware consolidation,
slice 1 **suppresses the automatic flush for tables carrying shadow deltas** (a table that
has taken a mutation). Shadow deltas accumulate inline and merge-on-read; consolidation is
deferred (the accepted "inline grows until slice 2" tradeoff). Append-only tables with no
mutations flush exactly as today.

## Scope

In scope (slice 1):

- `loom_tombstone` column + the append/version/tombstone model on the inline tier.
- Identity-aware **merge-on-read** in engine-serving (replaces additive union for identity
  types; identity-less types unchanged).
- O(change) mutation write (UPDATE→version, DELETE→tombstone) replacing
  `overwrite_parquet_snapshot` for identity types, preserving the existing PATCH + per-row
  ACL semantics.
- Per-identity **CAS guard** with bounded retry.
- Suppressing automatic flush for shadow-delta-bearing tables.

Out of scope:

- **Slice 2 ([[fut-cow-inline-shadow]]):** tombstone-aware **compaction consolidation**
  (fold inline deltas into a new base Parquet snapshot, clear them, re-enable flush) — the
  unbounded-inline-growth resolution.
- **Slice 3:** identity-change / upsert ([[fut-cow-identity-change]]).
- **File-granular COW** ([[fut-cow-file-granular]]) — a different O(touched-files) approach,
  not this path.
- Identity-less types (keep whole-table COW); vector types ([[fut-cow-arrow-native]]);
  per-session check-in ([[fut-cow-session-checkin]]); Iceberg-native delete-files (blocked on
  iceberg-rust).

## Testing

`loom_fixture_test` end-to-end through the governed action path + the serving read:

1. **O(change) update reads correctly:** seed a file-resident object, UPDATE it → the read
   returns the **new** values for that identity (inline version shadows the file row), all
   other rows unchanged, and **no file rewrite** occurred (assert the file set is unchanged /
   one inline row added).
2. **Delete via tombstone:** DELETE a file-resident object → the identity is absent from
   reads (tombstone hides the file row); other rows intact; no file rewrite.
3. **Latest version wins:** two sequential UPDATEs of the same id → the read returns the
   second version (max `begin_snapshot`).
4. **Time-travel intact:** a read as-of a snapshot *before* the mutation still sees the
   original file value (MVCC visibility unchanged).
5. **CAS guard:** two concurrent UPDATEs of the same id → one commits, the other aborts and
   retries on the fresh version (no lost update); final state reflects both applied in
   order. A concurrent mutation of a *different* id never conflicts.
6. **Governance preserved:** the per-row column/row-filter ACL denial on the affected row
   behaves exactly as in [[road-update-delete-actions]] (a denied mutation 403s, nothing
   written); a masked/denied column never leaks through the merged read.
7. **Identity-less fallback:** a type with no identity still uses whole-table COW (unchanged).

## Risk

- **Correctness-critical read-path change** (merge-on-read can hide/expose rows). Mitigated
  by the dedup running over the already-MVCC-visible set (time-travel test), the file-ranks-
  below-inline invariant, and tests 1–4 pinning each shadow/tombstone case. The governed
  layer sits above the merge, so ACL is unaffected (test 6).
- **CAS correctness** is the subtle concurrency core; pinned by test 5 and the bounded-retry
  pattern already proven in compaction.
- **Flush/consolidation gap:** suppressing flush risks unbounded inline growth — explicitly
  accepted and bounded to slice 2; the suppression itself prevents the corruption that would
  otherwise occur, and append-only tables are unaffected.
- Whole-table COW remains for identity-less types, so the change is additive for the path it
  does not cover.
