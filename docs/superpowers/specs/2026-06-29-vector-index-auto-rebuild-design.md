# Vector index auto-rebuild on flush (freshness-on-staleness)

- **Date:** 2026-06-29
- **Area:** query
- **Register items:** carves a slice out of [[fut-puffin-vector-index-ann]]; mints [[road-vector-index-auto-rebuild]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

A declared vector index stays **fresh automatically**: rows become searchable
without an operator manually re-enqueuing a build. This is one carved slice of the
larger [[fut-puffin-vector-index-ann]] basket — the rest (Vamana/DiskANN, distributed
build, cold-index ACL pruning, sidecar GC, wider element types) stays deferred.

## Problem — flush opens a k-NN visibility gap

The cold Puffin index binds to a `covered_snapshot` (`iceberg_mirror.vector_index`).
Engine-side k-NN (`engine-serving/src/vector_search.rs`) returns
**cold ∪ hot-delta**, where the hot delta is the *live inline* rows born after the
index's `covered_snapshot`:

```sql
-- inline_delta_batch (vector_index.rs)
where begin_snapshot > covered_snapshot
  and begin_snapshot <= q
  and (end_snapshot is null or end_snapshot > q)   -- still live at q
```

A **flush** (`iceberg_flush::flush_table`) drains live inline rows into a cold Parquet
snapshot `F` and **end-caps** them (`end_snapshot = F`). After that, for any query at
`q ≥ F`:

- the row fails `end_snapshot > q` → **dropped from the hot delta**; and
- the cold Puffin index was built at the older `covered_snapshot < F` → **the row is
  not in the cold leg either.**

So between a flush and the next build, just-flushed vectors **silently disappear from
k-NN results**. Builds are **explicit-enqueue only today** (no code path auto-enqueues
`build_vector_index`; `BUILD_VECTOR_INDEX_JOB_KIND` is produced only by operators/tests),
so nothing closes the gap on its own. The hot/cold merge keeps *inline* rows correct
([[fut-inline-vector-hot-delta]], PR #209) but cannot cover rows that have left the
inline tier.

This is the freshness analogue of [[iss-iceberg-inline-visibility]]'s bounded staleness:
the flush is the event that creates staleness, so the flush must also schedule the
repair.

## Design

**On flush, enqueue a rebuild of every vector index the flush just staled.**

### Trigger: in the flush primitive, atomic with the end-cap

`iceberg_flush::flush_table` already runs a per-table, advisory-locked transaction that
end-caps the inline rows and commits the new snapshot. In that same transaction, after
the rows are end-capped and before commit:

1. Resolve the table's declared vector indexes
   (`ontology.vector_index_definition` rows for the table — the same set a build
   resolves).
2. For each, enqueue one `build_vector_index` job (`BUILD_VECTOR_INDEX_JOB_KIND`,
   payload `BuildVectorIndexJob { schema, name, index_name }`) via the existing
   `queue::pg_insert` on the flush's connection — so the rebuild is scheduled **atomically
   with the snapshot that staled the index** (a flush that commits can never forget to
   schedule its rebuild, and one that rolls back enqueues nothing).

This mirrors the established inline-flush trigger (`iceberg_inline.rs:300-312`), which
enqueues a `flush_table` job in the same transaction as the inline write that crossed the
threshold.

### Dedup — no rebuild pile-up

A burst of flushes (or an already-pending build) must not queue redundant rebuilds.
Before inserting, skip the enqueue if a `build_vector_index` job for the same
`(schema, name, index_name)` is **already pending or running** (a predicate query over
the jobs table by `kind` + payload fields). This is the queue-level equivalent of the
inline trigger's `enqueued` guard; a coalesced rebuild always rebuilds against the
*latest* snapshot when it finally runs, so collapsing duplicates loses no freshness.

### No threshold knob (and why)

Because the gap is a **correctness/visibility** gap, not a cost optimization, the
trigger fires whenever a flush end-caps rows for a table that has ≥1 declared vector
index — there is no "rebuild only past N rows" threshold (that would leave sub-threshold
flushes' rows invisible). Coalescing via the dedup guard, not a threshold, is what bounds
rebuild frequency. (A debounce/threshold knob is a possible later refinement, tracked in
the basket, not built here.)

### Unchanged

- The build itself: `build_vector_index` (worker → engine `BuildVectorIndex` RPC) and
  the `vector_index` mirror row it writes are untouched — this slice only schedules it.
- The query path: cold ∪ hot merge is unchanged. After the rebuild lands, `covered_snapshot`
  advances to `F`, the (now empty) inline delta shrinks, and results are whole again.
- No proto/wire change; no new job kind; no ontology change.

## Staleness window

Like inline external visibility, freshness is restored after a **bounded** delay — here
the build job's enqueue-to-completion latency — rather than synchronously at flush. The
flush stays fast (it does not build the index inline); the rebuild runs as a normal
queued job. This bounded-staleness tradeoff is the same one loom already accepts for
inline→external visibility, and is the right call (a synchronous in-flush rebuild would
make every flush pay full index-build cost).

## Scope

In scope:

- The rebuild-enqueue (with dedup) inside `iceberg_flush::flush_table`, atomic with the
  end-cap, for each declared vector index on the flushed table.
- A jobs-table predicate to detect an already-pending/running `build_vector_index` for a
  given `(schema, name, index_name)`.
- The tests below; the `.sqlx` cache refresh for any new compile-time query
  (`tools/sqlx-prepare.sh`).

Out of scope (stay in [[fut-puffin-vector-index-ann]]):

- Threshold/debounce tuning, Vamana/DiskANN, distributed/disaggregated build, cold-index
  ACL pruning, Puffin-sidecar GC, non-`f32` element types / extra metrics.
- Rebuild on **compaction** (the `gc_table`/compact path) — only inline→cold *flush*
  stales the index in the hot-delta model; compaction rewrites already-cold, already-
  indexed files. If a future audit shows compaction also moves indexed rows, fold it in
  then.
- Synchronous in-flush rebuild (rejected above).

## Testing

A `loom_fixture_test` integration test (hermetic Postgres + warehouse + a live engine
for the build RPC), per loom's testing rules (`rust_test` target, never inline
`#[cfg(test)]`). Mirror the existing vector-search engine-serving fixtures.

1. **The failing-without-this-slice case (freshness):** declare a vector index; land &
   build it; land more vector rows **inline**; **flush** the table; run k-NN at the
   post-flush snapshot and assert a just-flushed row is **missing** (demonstrates the gap
   on `main`). Then, after the auto-enqueued `build_vector_index` job is drained, assert
   the same k-NN now returns that row — fresh again. *(Write this test first; it is the
   red test the slice turns green.)*
2. **Auto-enqueue:** assert exactly one `build_vector_index` job is enqueued for the
   index when the flush commits (and that a flush of a table with no declared index
   enqueues none).
3. **Dedup:** two flushes with a build still pending enqueue **one** build job, not two.
4. **Atomicity:** a flush that finds no live inline rows (no-op) enqueues no rebuild.

## Risk

- The enqueue rides the flush's existing transaction, so it inherits the flush's
  atomicity — no new failure mode (a rebuild is scheduled iff the staling flush
  committed). The pattern is copied from the proven inline-flush trigger.
- Extra load is one coalesced rebuild per burst of flushes per index — bounded by the
  dedup guard; the build itself already exists and is rate-limited by the queue/worker.
- Behavior-preserving for tables without vector indexes (the declared-index set is empty
  → no enqueue, byte-identical flush).
