# Physical GC of end-capped Iceberg rows (slice 1) — design

_2026-06-23. Work item: [[road-iceberg-gc]] (promoted from [[fut-iceberg-gc]])._

## Problem

The Iceberg backend never physically reclaims dead bytes. Every retirement is an
**end-cap, not a delete**: a row's `end_snapshot` is set so older time-travel
reads still see it, but the row (and, for `data_file` rows, the Parquet object it
points at) lives forever. Three sources accumulate:

1. **End-capped inline rows** — `flush_table` drains live `inline_<table_id>` rows
   into a real Parquet snapshot and end-caps them at the same snapshot
   (`iceberg_sql_catalog/catalog.rs` ~487–501, the `extras.end_cap` path).
2. **End-capped `data_file` rows + their Parquet** — overwrite/replace and drop
   end-cap live data files (`iceberg_mirror.rs::end_cap_live_data_files`,
   ~192–206; called from `write_mirror(.., overwrite=true)` and `mark_dropped`).
   The retired `data_file` row carries the physical `path`, and the Parquet object
   remains in the store.
3. **Orphaned Parquet** — a write-then-commit failure leaves a written Parquet
   object referenced nowhere in the mirror (documented as GC-deferred in
   `services/ingest/src/materialize.rs` and `lib.rs`).

This slice reclaims sources **(1) and (2)** — the mirror-driven, query-discoverable
dead bytes — under an **age-based retention horizon**. Source (3), which needs an
object-store *listing* sweep and a write-race grace, is explicitly deferred (see
**Out of scope**).

There are no `*gc*`/`*orphan*`/`*retention*`/`*vacuum*` tests today; this is net-new.

## Goal

An operator-triggered, per-table `gc_table(schema, name)` that physically deletes
the mirror rows (and their Parquet objects) that aged out of the time-travel
retention window, **without ever breaking an in-window time-travel read** and
without leaving a dangling mirror→file reference.

## Retention model: age-based horizon

GC reclaims bytes by deleting end-capped rows, which **withdraws the time-travel
guarantee** for snapshots below a horizon. The horizon is **age-based**, mirroring
Iceberg's `expire_snapshots` semantics.

### Snapshot commit-time (schema change)

`iceberg_mirror.snapshot` records `snapshot_id`, `iceberg_snapshot_id`,
`schema_version` — **no commit timestamp**, so today there is no snapshot→wall-clock
mapping. Add it:

```sql
-- new migration: 0017_iceberg_snapshot_committed_at.sql
ALTER TABLE iceberg_mirror.snapshot
  ADD COLUMN committed_at timestamptz NOT NULL DEFAULT now();
```

`now()` at row creation is the commit time (the snapshot row is inserted inside the
committing Postgres transaction). Existing rows backfill to migration time, which is
harmless — loom is pre-deployment, so there is no real historical data whose age
matters (consistent with the [[fut-iceberg-stats-backfill]] reasoning). Regenerate
the `.sqlx` cache with `tools/sqlx-prepare.sh` and commit it.

### Horizon resolution

Given a retention duration `R` (engine config `LOOM_GC_RETENTION`, proposed default
`7d`), define the **horizon snapshot `H`** as the youngest snapshot that has fully
aged out of the window:

```
H = max(snapshot_id) WHERE committed_at < now() - R
```

If no snapshot has aged out, `H` is undefined and GC reclaims nothing (a no-op, not
an error). Every snapshot `> H` stays fully time-travelable.

### Safety invariant

> A row is reclaimable **iff `end_snapshot IS NOT NULL AND end_snapshot <= H`**.

Proof sketch: the MVCC read predicate is
`begin_snapshot <= at AND (end_snapshot IS NULL OR end_snapshot > at)`. An
end-capped row with `end_snapshot = E` is visible only for `at < E`. The oldest
`at` any in-window reader may supply is a snapshot `> H`. So if `E <= H`, no
in-window `at` satisfies `at < E` — the row is invisible to every guaranteed read
and is safe to delete. Live rows (`end_snapshot IS NULL`) are never touched.

## Reclaim primitive — `gc_table`

New module `src/control-plane/postgres/src/iceberg_gc.rs`, exposing
`gc_table(catalog, pool, table)`:

1. **Serialize per table.** Take `pg_advisory_xact_lock(lock_key(schema, name))` —
   the **same key** `iceberg_flush.rs` uses (reuse `lock_key`) — so GC never races a
   concurrent flush/overwrite on the table. Xact-scoped, auto-released on drop/panic.
2. **Resolve the horizon `H`** (a no-op return if undefined).
3. **Collect reclaimable Parquet paths**: select `path` from `iceberg_mirror.data_file`
   where `table_id = $t AND end_snapshot IS NOT NULL AND end_snapshot <= H`.
4. **Delete mirror rows in one transaction**:
   - `DELETE FROM iceberg_mirror.data_file_column_stat WHERE data_file_id IN (<those>)`
     (stats lifecycle follows the data file; delete first to respect any FK).
   - `DELETE FROM iceberg_mirror.data_file WHERE table_id = $t AND end_snapshot <= H`.
   - `DELETE FROM iceberg_mirror.inline_<table_id> WHERE end_snapshot IS NOT NULL AND end_snapshot <= H`.
   - Commit.
5. **Delete Parquet objects post-commit** (ordering — see below): for each collected
   path, call a new `FileIO` **delete seam** (`delete_file(path)`). Failures are
   **logged and left as orphans**, not retried into a hard failure.

Return a small summary (counts of data-file rows, inline rows, and objects deleted)
for observability and test assertions.

### Delete/commit ordering: commit-then-delete

You cannot atomically delete an object-store file and commit a Postgres tx, so the
ordering defines the failure mode. This slice uses **commit-then-delete** (mirror
rows first, then objects):

- Mirror is the source of truth. Once a `data_file` row is gone, nothing references
  the object; if the subsequent object delete fails, the file degrades to exactly
  the **orphaned-Parquet class already deferred** — never data loss, never a
  dangling mirror→file reference.
- The rejected alternative (delete-then-commit) risks a crash leaving mirror rows
  pointing at deleted objects, so a read just inside the horizon could 404. A
  tombstone two-phase is robust but over-engineered for slice 1.

### Object-store delete seam

The Iceberg code wires `FileIO` (`iceberg_sql_catalog/catalog.rs`: `fileio` field,
`FileIOBuilder::new(factory).build()`) and reads via `new_input(path)`, but **never
deletes**. Add a thin `delete_file(path)` wrapper over the `iceberg` crate's
`FileIO` delete API. This is the only new object-store capability; read/write paths
are untouched. Works against both `file://` (local fixture) and `s3://` (once
[[road-iceberg-real-object-store]] lands) since paths are opaque URLs.

## Execution model (engine-wire)

Consistent with [[road-iceberg-flush-consumer]] and [[road-compaction-job]]:

- New `GC_JOB_KIND = "gc_table"` and `GcJob { schema, name }` in `control_plane_core`
  (mirroring `flush.rs`).
- An **operator HTTP endpoint** enqueues a per-table `gc_table` job (`NewJob` →
  `pg_insert`, the inline-trigger pattern).
- A **zero-pool worker** dequeues it (`handle_gc`, mirroring `handle_flush`) and
  calls a new **`EngineControl::GcTable` RPC**; the **engine** (which owns Postgres
  and object-store creds) runs `gc_table`. The worker stays zero-pool.
- Retention `R` is **engine config** (`LOOM_GC_RETENTION`), not a per-job field —
  the job only names the table.
- **No Arrow Flight**: GC only deletes, no bulk data crosses the wire (unlike
  compaction).

Why operator-triggered and not automatic: the age-based horizon makes a
GC-right-after-write pointless (freshly end-capped rows are inside the window).
Reclamation only becomes possible after `R` elapses, so the trigger is decoupled
from write events. A scheduled all-tables sweep is the natural next step but depends
on unbuilt [[fut-scheduled-jobs]] (deferred).

## Testing

A `loom_fixture_test` (`tests/iceberg_gc.rs`, `loom_fixture_test` macro — it boots
hermetic Postgres + a `file://` object store):

- **Happy path.** Land a table; overwrite (and/or flush) it so rows+files are
  end-capped at snapshot `E`; make `E` aged-out by **injecting an old `committed_at`**
  on its snapshot row (deterministic — no wall-clock sleeps); run `gc_table`. Assert:
  (a) the end-capped `data_file` / `data_file_column_stat` / inline rows are **gone**;
  (b) the Parquet objects are **deleted** from the fixture store;
  (c) a **live read still returns current data**;
  (d) a time-travel read **at an in-window snapshot is unchanged**.
- **Negative — in-window protection.** Rows end-capped at a snapshot `> H` are
  **not** touched (rows present, objects present).
- **No-op.** `gc_table` on a table with no aged-out snapshots reclaims nothing and
  succeeds.
- **Lock coexistence.** `gc_table` and a concurrent `flush_table` on the same table
  serialize (no interleaving corruption) — reuse the existing concurrency harness
  shape.

## Out of scope (deferred follow-ups)

- **Orphaned-Parquet sweep** (write-then-commit orphans). Needs object-store
  *listing* + an age grace to avoid deleting an in-flight uncommitted write. Stays a
  follow-up under the GC umbrella (a new FUTURE item linked from [[road-iceberg-gc]]).
- **Scheduled / automatic all-tables GC** — depends on [[fut-scheduled-jobs]].
- **DuckLake-backend GC** — this slice is Iceberg-only.
- **Reclaiming aged-out `iceberg_mirror.snapshot`/`table`/`column` metadata rows** —
  tiny; left live this slice.
- **Env-tunable beyond a single duration, retention metrics** ([[fut-metrics-crate]]),
  and lineage-level retention policy ([[fut-gc-retention]]).

## Touched surface

- `src/control-plane/postgres/migrations/0017_iceberg_snapshot_committed_at.sql` (new)
- `src/control-plane/postgres/.sqlx/` (regenerated)
- `src/control-plane/postgres/src/iceberg_gc.rs` (new) + `lib.rs` wiring
- `src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs` (FileIO `delete_file` seam)
- `src/control-plane/core/src/` (new `gc.rs`: `GC_JOB_KIND`, `GcJob`)
- `engine` service: `EngineControl::GcTable` RPC + handler
- `src/services/worker/src/handler.rs`: `handle_gc`
- operator HTTP endpoint to enqueue `gc_table`
- `src/control-plane/postgres/tests/iceberg_gc.rs` (new `loom_fixture_test`) + BUCK target
