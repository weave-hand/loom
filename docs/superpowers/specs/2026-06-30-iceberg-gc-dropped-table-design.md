# Iceberg GC — dropped-table reclaim

- **Date:** 2026-06-30
- **Area:** iceberg
- **Register items:** promotes [[fut-iceberg-gc-dropped-table]] → mints [[road-iceberg-gc-dropped-table]]; records [[fut-iceberg-gc-orphan-sweep]]
- **Status:** spec (ready for a work agent to plan + build)

## North star

GC reclaims the bytes of a **dropped** table, not just a live one. After
`DROP`-ing an object type's table, its end-capped `data_file` Parquet and its physical
`inline_<tid>` table are reclaimed under the same age-based retention horizon that
[[road-iceberg-gc]] applies to live tables — closing the one mirror-driven leak that slice
1 left open.

## Current state

[[road-iceberg-gc]] (PR #194) ships `gc_table(catalog, pool, table, retention)`
(`control-plane/postgres/src/iceberg_gc.rs`): under a per-table advisory lock it resolves
the horizon `H = youngest snapshot fully aged past LOOM_GC_RETENTION_SECS`, deletes
`iceberg_mirror` rows with `end_snapshot <= H`, and deletes their Parquet via the `FileIO`
`delete_file` seam **commit-then-delete** (a failed object delete degrades to a deferred
orphan, never a dangling reference). It is reachable via `EngineControl::GcTable` /
`POST /maintenance/gc/{schema}/{table}`.

The gap: `gc_locked` resolves the table with `live_table_id(schema, name)` and **no-ops
when that returns `None`** (`iceberg_gc.rs:97`). A dropped table — `mark_dropped`
(`iceberg_mirror.rs:253`) end-caps its `table`/`column`/`data_file` rows, leaving **no live
row** — therefore keeps its end-capped `data_file` Parquet and its `inline_<tid>` table
**forever**: `gc_table` is a clean no-op on it.

A `(namespace, name)` can map to **several** `table_id`s over a drop/recreate history; only
the most recent may be live (or none). Reclaiming the dead incarnations needs resolution
across those historical `table_id`s, which `live_table_id` deliberately does not do.

## Design

Extend `gc_table(schema, name)` to reclaim **both** the live table's aged-out dead bytes
(today's behaviour) **and** every **dropped incarnation** of `(schema, name)` — no new
endpoint or RPC; the existing `POST /maintenance/gc/{schema}/{table}` and
`EngineControl::GcTable` cover it.

Under the same advisory lock and horizon `H`:

1. **Live reclaim (unchanged).** If `live_table_id` resolves, run the existing
   end-capped-row + Parquet reclaim for that `table_id`.
2. **Dropped reclaim (new).** Resolve the **dropped** `table_id`s for `(schema, name)`: every
   `iceberg_mirror.table` row ever bound to `(schema, name)` that is **end-capped** (dropped)
   and is **not** the currently-live `table_id`. For each:
   - delete its `data_file` rows with `end_snapshot <= H` and their Parquet (the same
     commit-then-delete `delete_file` path — so the dropped table's files reclaim exactly as
     a live table's replaced files do);
   - once **all** of a dropped `table_id`'s `data_file` rows are reclaimed (i.e. the drop
     snapshot itself has aged past `H`, so no in-window time-travel read can reach it),
     physically `DROP TABLE iceberg_mirror.inline_<tid>` (it can never gain a live row again)
     and delete its now-empty `table`/`column` mirror rows.

Time-travel safety is identical to slice 1: a `data_file` row is only deleted once
`end_snapshot <= H`, i.e. proven invisible to every retention-window read; a dropped table
whose drop snapshot is still **within** the window keeps its files until it ages out (a
partial reclaim that completes on a later GC run). The `inline_<tid>` drop and the
`table`/`column`-row delete are gated on **full** reclaim so no metadata vanishes while any
of its data could still be time-travelled.

### Decided (not open)

- **Surface:** extend `gc_table(schema, name)` rather than add a `gc_dropped_table` op —
  reuses the endpoint/RPC/lock/horizon, and `gc_table` on a never-recreated dropped name
  (today a no-op) now reclaims it. An **all-dropped sweep** (discovering dropped names
  without the operator naming them) is a follow-on, paired with scheduled GC
  ([[fut-scheduled-jobs]]).
- **Retention horizon is shared** — one `H` from `LOOM_GC_RETENTION_SECS`; dropped tables
  are not reclaimed more aggressively than live ones (a time-travel read as-of before a
  recent drop must still resolve).
- **Commit-then-delete** is preserved (a failed Parquet delete degrades to a deferred orphan
  for the [[fut-iceberg-gc-orphan-sweep]] sweep, never a dangling mirror reference).

## Scope

In scope:

- `dropped_table_ids(schema, name)` resolution (end-capped `table` rows for the name minus
  the live one) and the dropped-incarnation reclaim loop in `gc_locked`.
- Physical `DROP TABLE inline_<tid>` + `table`/`column` mirror-row delete, gated on full
  `data_file` reclaim of that `table_id`.
- Reuse of the existing horizon, advisory lock, `delete_file` commit-then-delete, and the
  GcTable RPC / maintenance endpoint (no new surface).

Out of scope:

- **Orphaned-Parquet sweep** ([[fut-iceberg-gc-orphan-sweep]]) — object-store-listing-driven
  reclaim of files no mirror row references (write-then-commit failures + slice-1/this-slice
  deferred-orphan degradations). Needs a LIST + reference diff + write-race grace; riskier
  (data-loss blast radius), its own slice.
- All-dropped / scheduled sweep ([[fut-scheduled-jobs]]); DuckLake GC; retention metrics
  ([[fut-metrics-crate]]).

## Testing

`loom_fixture_test` against the Iceberg backend:

1. **Dropped table reclaimed:** land a table (file-backed + some inline rows), drop it, advance
   time past retention, `gc_table(schema, name)` → its `data_file` Parquet is deleted, its
   `inline_<tid>` table is gone, and its `table`/`column` mirror rows are removed; the object
   store no longer holds the files.
2. **Within-window drop is preserved:** drop a table, `gc_table` **before** the drop snapshot
   ages past `H` → nothing deleted (a time-travel read as-of before the drop still resolves);
   a later GC after aging completes the reclaim.
3. **Drop/recreate isolation:** create `(s,t)`, drop it, recreate `(s,t)` (new `table_id`),
   land into the live one, age past retention, `gc_table(s,t)` → the **dropped** incarnation's
   bytes are reclaimed while the **live** incarnation's current files are untouched.
4. **Live-table behaviour unchanged:** the existing slice-1 `gc_table` tests still pass (live
   reclaim is behaviour-preserving; the dropped loop is additive).
5. **Idempotent:** a second `gc_table` on a fully-reclaimed dropped name is a clean no-op.

## Risk

- **Data-loss-adjacent** (deletes Parquet + drops physical tables); mitigated by gating every
  delete on `end_snapshot <= H` (the slice-1 invariant, proven invisible to in-window reads),
  gating the `inline_<tid>` drop + metadata-row delete on **full** reclaim, and test 2/3
  pinning within-window and drop/recreate isolation. Commit-then-delete keeps a failed object
  delete from ever orphaning a mirror reference.
- The historical-`table_id` resolution is the new logic; bounded to "end-capped `table` rows
  for `(ns,name)` minus the live one" and pinned by the drop/recreate test.
- Reuses the proven lock/horizon/delete_file machinery, so the blast radius is the dropped
  branch only; live GC and all non-GC paths are untouched.
