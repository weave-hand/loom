# Overwrite path vector-index rebuild — `rebuild_jobs_for` shared with flush

**Status:** approved
**Date:** 2026-07-03
**Register item:** `iss-overwrite-vector-index-staleness` (docs/ISSUES.md)
**Inputs:** `iceberg_flush.rs` rebuild enqueue (PR #240), `iceberg_landing.rs::overwrite_parquet_snapshot`, engine-serving `action_writer`, COW slice 1 (PR #331), `vector_search.rs` hot/cold merge.

## Problem

PR #240 made `iceberg_flush::flush_table` enqueue one `build_vector_index` job
per declared index atomically with the flush snapshot (`iceberg_flush.rs:117-151`),
deduped against pending jobs via `CommitExtras.jobs` →
`commit_mirror::apply_commit_extras` → `queue::pg_insert_if_absent` (insert only
if no `state='available'` job with the same `(kind, payload)` exists).

The sibling overwrite/replace commit — `iceberg_landing::overwrite_parquet_snapshot`,
reached from engine-serving's `IcebergActionWriter::overwrite_table` (governed
UPDATE/DELETE whole-table copy-on-write, `Tx::replace_files`) — passes
`CommitExtras { lineage, overwrite: true, ..Default }` with an **empty job set**.
After a governed UPDATE/DELETE on a vector-indexed type, the rewritten rows are
file-backed (cold) but absent from the Puffin index built at the older
`covered_snapshot`, and they are not inline, so `inline_delta_batch` never scores
them: they fall out of k-NN / `/search` until a manual rebuild. The zero-file
truncate branch (`overwrite_truncate`) is worse — it is a mirror-only transaction
with no `CommitExtras` seam at all, so a delete-all leaves the entire cold index
serving tombstoned identities.

Today the gap is latent end-to-end: `ensure_cow_supported`
(`query-api/src/action.rs:658`) 400s UPDATE/DELETE on any type with a `vector(N)`
column. But the engine-side writer is guard-free by design ("pre-authorized"),
so the primitive is exercisable — and `fut-cow-arrow-native` lifts the guard.

## Design

**One helper, two call sites.** Extract the flush path's job construction
(`iceberg_flush.rs:120-137`) into `control-plane/postgres/src/vector_index.rs`,
next to `declared_vector_index_names` (which it wraps):

```rust
/// One `build_vector_index` NewJob per index declared on the ontology type
/// backing `table`; empty Vec when the table has no type or no indexes.
pub(crate) async fn rebuild_jobs_for(pool: &PgPool, table: &TableRef)
    -> Result<Vec<NewJob>>;
```

Payload stays `BuildVectorIndexJob { schema, name, index_name }` with kind
`BUILD_VECTOR_INDEX_JOB_KIND` — byte-identical to the flush path's, so the
pending-dedup keys collide across paths (a flush-enqueued pending job also
suppresses an overwrite-enqueued one, and vice versa; that is correct — one
rebuild at the newer snapshot covers both commits).

**Enqueuing paths:**

- `flush_table` — swaps its inline construction for `rebuild_jobs_for`
  (behavior-preserving refactor; the existing `flush_vector_rebuild.rs` suite
  pins it).
- `overwrite_parquet_snapshot`, non-empty branch — passes
  `jobs: &rebuild_jobs_for(pool, table).await?` in its `CommitExtras`, riding
  the existing atomic seam (`apply_commit_extras`).
- `overwrite_truncate` — calls `queue::pg_insert_if_absent` per job inside its
  own transaction, after the end-caps and lineage emit. Same dedup, same
  atomicity (the CTE's `pg_notify` is buffered until commit). A rebuild over an
  empty table is well-defined: `build_vector_index` re-binds at the new
  `covered_snapshot` with `row_count` reflecting the surviving (zero) rows.

**Dedup semantics** (unchanged from #240): pending-only, keyed `(kind, payload)`
on `state='available'`. A *running* build does not suppress a new enqueue —
deliberate, since the running build reads at the older snapshot and would
otherwise strand the newer data uncovered.

**Inline-shadow path (COW slice 1, #331): no rebuild — deliberately.** A
governed UPDATE/DELETE on an identity-bearing type routes to `write_delta` →
`iceberg_inline::write_inline_delta` (O(change) row-version/tombstone), not to
`overwrite_parquet_snapshot`. The new row version *is* covered without a
rebuild: `vector_search`'s hot path (`inline_delta_batch`) scores every inline
row with `begin_snapshot > covered_snapshot` live at Q and merges via
`merge_topk`. Enqueuing an O(table) rebuild per O(change) delta would defeat
slice 1's point; the dedup would only bound the queue, not the build cost. What
the hot/cold merge does **not** cover is suppression of the *superseded* cold
entry — the Puffin index still holds the pre-mutation vector (stale duplicate
hit) or a tombstoned identity (the query-api row-filter post-filter drops it
only when row filters happen to exist, `handler.rs:607`). That is a
cold-suppression defect, not an enqueue defect; it belongs to slice 2's
compaction consolidation (`fut-cow-inline-shadow`), whose fold-into-new-base
commit is itself a replace-shaped snapshot and will route through this same
`rebuild_jobs_for` seam. File it as its own ISSUES entry when slice 2 is
specced; do not solve it here.

## Acceptance criteria

Red-first, fixture tests in `//src/control-plane/postgres` (mirror
`flush_vector_rebuild.rs`, via `loom_fixture_test`):

1. **Overwrite enqueues exactly the declared rebuilds.** Seed a type with two
   declared indexes, land rows, then drive a governed UPDATE/DELETE through the
   engine-serving writer seam (`IcebergActionWriter::overwrite_table`, below
   the query-api vector guard): exactly two `build_vector_index` jobs appear in
   `queue.jobs` with `state='available'`, payloads naming each index. Written
   first and observed to fail (today: zero jobs).
2. **Truncate branch enqueues too.** The same assertion for an empty-IPC
   (delete-all) overwrite through `overwrite_truncate`.
3. **No duplicate pending jobs.** A second overwrite while the first job is
   still `available` inserts nothing (count stays 1 per index); after the job
   is consumed, a third overwrite enqueues again.
4. **Flush behavior unchanged.** The existing `flush_vector_rebuild.rs` suite
   stays green over the extracted helper.
5. Full `buck2 test //src/...` green; `.sqlx` refresh only if new compile-time
   queries are added (none expected — `rebuild_jobs_for` reuses existing ones).

## Out of scope

- Lifting `ensure_cow_supported` (`fut-cow-arrow-native`) — this slice makes
  the primitive correct so that item's acceptance criterion becomes a checkbox.
- Stale-cold-entry suppression on the inline-shadow path (superseded vectors /
  tombstoned identities still in the Puffin index) — slice 2 compaction
  consolidation territory, per the Design note above.
- Compaction (`iceberg_compact.rs`) rebuild enqueue — explicitly deferred by
  the #240 spec; unchanged here.
- ANN index kinds, rebuild throttling/coalescing beyond pending-dedup, and
  incremental (delta) index maintenance (`fut-puffin-vector-index-ann`).
