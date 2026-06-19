# Inline Flush Trigger (Enqueue) — Design

> Spec 1 of the flush-automation split. Turns the `flush_table` primitive
> (`iceberg_flush.rs`) from "callable" into "automatically triggered" by emitting
> a `flush_table` **job** when a table accumulates enough live inline bytes. This
> spec covers **only the producer** — the byte-threshold trigger and the enqueue.
> The consumer (a worker draining the queue and calling `flush_table`) is Spec 2,
> the engine-wire flush vertical, and is independent of this spec.

## Goal

Inline writes (`inline_append`) accumulate loom-native rows that the read engine
reconstructs on every query until a flush compacts them. Nothing flushes
automatically today. This spec makes a write the **event**: each `inline_append`
adds the batch's in-memory byte size to a per-table running total of *live inline
bytes since last flush*; when that total crosses a threshold, the same
transaction enqueues a `flush_table` job. Crossing is debounced (one pending job
per table) and the counter resets when a flush caps the rows.

Success criterion, provable without a worker: after enough inline writes to a
table, exactly one `flush_table` job for that table exists in `queue.jobs` with
the correct payload; below threshold, none.

## Background (verified)

- **The primitive** — `flush_table(catalog, pool, table, run_id) ->
  Result<Option<SnapshotId>>` (`iceberg_flush.rs`) drains live inline rows into a
  real Parquet snapshot and end-caps them, atomically, via the `CommitExtras`
  commit tx in `SqlCatalog::do_update_table`.
- **The queue** — `control_plane_core::Queue` with a `PgQueue` adapter
  (`postgres/src/queue.rs`). `enqueue(NewJob)` writes a `queue.jobs` row and fires
  `pg_notify('loom_queue:' || kind)` atomically; `dequeue` claims with `FOR UPDATE
  SKIP LOCKED`. `NewJob { kind: String, payload: serde_json::Value, .. }`.
  `enqueue` today takes `&self` on its own connection.
- **The write path** — `inline_append` (`postgres/src/iceberg_inline.rs`) inserts
  rows into the per-table `iceberg_mirror.inline_<table_id>` table inside one
  transaction, allocating a synthetic snapshot + lineage. It already resolves the
  stable `table_id`. It receives the decoded arrow-57 `RecordBatch`es, so it can
  compute their in-memory size (`RecordBatch::get_array_memory_size`).
- **No counter exists today** — live inline bytes/rows are knowable only by
  scanning `inline_<table_id>`. This spec adds the counter so triggering is O(1)
  per write, no scan.
- **Byte sizing precedent** — Slice B already computes an Arrow in-memory byte
  size for the inline-vs-Parquet routing (`LOOM_INLINE_BYTE_LIMIT`); the trigger
  reuses the same measure (`get_array_memory_size`), not a new metric.

## Architecture

### New state: `iceberg_mirror.inline_trigger`

A dedicated per-table trigger row, keyed by the **stable** `table_id` (not the
MVCC-versioned `iceberg_mirror.table` rows), created on a table's first inline
write:

```sql
CREATE TABLE iceberg_mirror.inline_trigger (
    table_id   BIGINT  PRIMARY KEY,
    live_bytes BIGINT  NOT NULL DEFAULT 0,   -- live inline bytes since last flush
    threshold  BIGINT,                        -- NULL => use the global env default
    enqueued   BOOLEAN NOT NULL DEFAULT false -- a flush job is already pending
);
```

- `threshold` is the per-table override; the global default is the ingest config
  value `LOOM_FLUSH_BYTE_THRESHOLD` (**default 64 MiB** — 4× the 16 MiB inline
  routing limit, so a table accrues several inline batches before compacting).
  Resolution is `COALESCE(threshold, $global)`.
- `enqueued` is the debounce: at most one pending flush job per table.
- It is a **static** table (unlike the dynamically-named `inline_<table_id>`
  tables), so its queries are sqlx **compile-time** `query!` and it ships in a
  migration.

### Transactional enqueue: reuse the existing `pg_insert`

No new method is needed. `queue.rs` already exposes a transaction-capable
enqueue:

```rust
// postgres/src/queue.rs (existing)
pub(crate) async fn pg_insert<'e, E: sqlx::PgExecutor<'e>>(
    ex: E, job: &NewJob,
) -> Result<JobId>;   // INSERT INTO queue.jobs … + pg_notify, in one statement
```

It is generic over any `PgExecutor`, so `&mut *conn` (the `inline_append`
transaction) satisfies it directly. `NOTIFY` issued inside a transaction
correctly fires on commit. (The `Queue` trait already documents this path —
"for transactional enqueue, use `Tx::enqueue`"; `pg_insert` is the shared
helper underneath.) So the trigger enqueues with `crate::queue::pg_insert(&mut
*conn, &job)` — no new public surface.

### Write-path change: `inline_append`

After inserting the inline rows, inside the **same** transaction:

1. Compute `batch_bytes = Σ batch.get_array_memory_size()` over the appended
   batches.
2. Upsert the trigger row and bump the counter, returning the effective
   threshold and the debounce flag:

   ```sql
   INSERT INTO iceberg_mirror.inline_trigger (table_id, live_bytes)
   VALUES ($table_id, $batch_bytes)
   ON CONFLICT (table_id)
   DO UPDATE SET live_bytes = inline_trigger.live_bytes + EXCLUDED.live_bytes
   RETURNING live_bytes, COALESCE(threshold, $global) AS effective, enqueued;
   ```
3. If `live_bytes >= effective AND NOT enqueued`:
   - `enqueue_tx(conn, NewJob { kind: "flush_table", payload: {"schema": …,
     "name": …} })`
   - `UPDATE iceberg_mirror.inline_trigger SET enqueued = true WHERE table_id =
     $table_id`

   All on the same connection/transaction as the row insert. On rollback, the
   rows, the counter bump, the flag, and the job all vanish together.

The global threshold reaches `inline_append` via the existing config plumbing
(an added field on the materializer/landing config sourced from
`LOOM_FLUSH_BYTE_THRESHOLD`).

### Flush-side reset (standalone, both paths)

After a flush, the counter must reset and the trigger must re-arm.
`flush_table` runs a standalone statement against the table's `inline_trigger`
row:

```sql
UPDATE iceberg_mirror.inline_trigger
SET live_bytes = 0, enqueued = false
WHERE table_id = $table_id;   -- no-op if the row is absent
```

This is a **standalone `UPDATE`, not folded into the `CommitExtras` cap tx** —
keeping the reset out of the vendored-catalog commit path (one fewer reason to
touch `do_update_table`). The only gap it introduces is a crash *between* the cap
commit and the reset, which **self-heals**: the job wasn't completed, so it is
reclaimed and re-run, and the re-run takes the `None` path (rows already capped)
which issues the same reset. Combined with at-least-once job processing
(Spec 2's worker), the flag cannot stay stuck. Reset
is to **zero, not decrement-by-flushed-bytes**: a write landing between a flush's
row-capture and its commit is not in the capped set, so zeroing slightly
*undercounts* those bytes — which only *delays* their next flush. Undercount is
the safe bias (extra latency, never a double-serve or runaway flushing); it is
documented, not tracked exactly.

**Every `flush_table` call must leave the trigger disarmed**, including the
no-op path. If a job runs but finds no live rows (`flush_table` returns `None` —
e.g. a manual flush already drained the table), there is no commit tx to carry
the reset, so `enqueued` would stick `true` and — because writes don't re-enqueue
while a job is "pending" — that table would never flush again. So on the `None`
path `flush_table` issues a **standalone** `UPDATE iceberg_mirror.inline_trigger
SET live_bytes = 0, enqueued = false WHERE table_id = $table_id`. Invariant:
after *any* `flush_table` call, the table's `inline_trigger` is `live_bytes=0,
enqueued=false` — atomically with the cap on the `Some` path, via the standalone
`UPDATE` on the `None` path. This keeps the debounce hygiene wholly inside Spec 1
(the consumer needs no flag bookkeeping).

### Job shape

- `kind = "flush_table"`.
- `payload = { "schema": "<schema>", "name": "<name>" }` — exactly what `flush_table`
  needs to rebuild a `TableRef`. The consumer (Spec 2) mints its own `RunId`.

## Why it is correct

- **No lost trigger.** The enqueue shares the write's transaction, so a job
  cannot be lost unless the write itself rolls back. No periodic safety sweep is
  needed.
- **No orphan job.** Same reason in reverse — a job can't survive a rolled-back
  write.
- **Idempotent/harmless duplicates.** `flush_table` is a no-op when nothing is
  live and self-serializes via its advisory lock, so a stale or duplicate job
  does no harm; the `enqueued` flag suppresses duplicates while one is pending
  regardless.

## Error handling

| Situation | Behavior |
|---|---|
| Write commits below threshold | counter accrues; no job; `enqueued` stays false |
| Write crosses threshold | exactly one job enqueued; `enqueued=true`; atomic with the rows |
| Write rolls back | counter bump, flag, and job all vanish |
| Repeated crossings while a job is pending | `enqueued` flag suppresses re-enqueue |
| Flush commits rows (`Some`) | `live_bytes=0`, `enqueued=false`, atomic with the end-cap |
| Flush finds nothing live (`None`) | standalone `UPDATE` still sets `live_bytes=0`, `enqueued=false` — flag can't stick |
| Write between a flush's capture and commit | not capped (stays live); counter zeroed ⇒ slight undercount ⇒ flushed next round |

## Testing (all `loom_fixture_test`, postgres crate)

1. **Sub-threshold accrues, no job.** Several small `inline_append`s under the
   threshold → `queue.jobs` has no `flush_table` job; `inline_trigger.live_bytes`
   equals the summed batch sizes; `enqueued=false`.
2. **Crossing enqueues exactly one.** Append past the threshold → exactly one
   `flush_table` job with payload `{schema,name}` matching the table;
   `enqueued=true`.
3. **Per-table override beats global.** Set a table `threshold` below the global,
   cross it with a write that's under the global → a job is enqueued (proves the
   `COALESCE`).
4. **Debounce.** Two threshold-crossing writes while a job is pending → still
   exactly one job.
5. **Atomicity on rollback.** Force `inline_append` to fail after the bump → no
   counter change and no job (assert `queue.jobs` empty and no `inline_trigger`
   row / unchanged).
6. **Flush resets state.** After a real `flush_table` (`Some`), assert
   `live_bytes=0` and `enqueued=false` for the table, observed in a committed
   read.
7. **No-op flush disarms the flag.** Arm a table (`enqueued=true`), drain it so a
   second `flush_table` returns `None`, and assert the `None` call still leaves
   `enqueued=false` (the flag can't stick).
8. **Slice B / flush preserved.** Existing `iceberg_flush` tests stay green
   through the added reset statement.

## Files

- **Create** a migration under `postgres/migrations/` adding
  `iceberg_mirror.inline_trigger`.
- **`postgres/src/queue.rs`** — no change; the existing generic `pg_insert` is
  the transactional enqueue.
- **Modify** `postgres/src/iceberg_mirror.rs` — add the static-schema trigger
  helpers (`bump_inline_trigger`, `arm_inline_trigger`, `reset_inline_trigger`),
  compile-time `query!` in the module's existing style.
- **Modify** `postgres/src/iceberg_inline.rs` — add a `flush_threshold:
  Option<i64>` param to `inline_append`; when `Some`, compute `batch_bytes`
  (`RecordBatch::get_array_memory_size`), bump the trigger, and on crossing
  `pg_insert` a `flush_table` job + arm. `None` preserves today's behavior.
- **Modify** `postgres/src/iceberg_flush.rs` — standalone `inline_trigger` reset
  on both flush paths (`Some`: after the commit; `None`: resolve the `table_id`
  and reset). No `catalog.rs` / `CommitExtras` change.
- **Modify** the ingest/landing config (`services/runtime` + the
  `IcebergMaterializer` path) — read `LOOM_FLUSH_BYTE_THRESHOLD` (default 64 MiB)
  and pass it to `inline_append`.
- **Modify** `postgres/src/lib.rs` / `BUCK` — a new `inline-flush-trigger`
  `loom_fixture_test` target.
- **Regenerate** `postgres/.sqlx` via `tools/sqlx-prepare.sh` (new compile-time
  queries against `inline_trigger`).
- **Modify** `docs/spike/ICEBERG_ROADMAP.md` — note the trigger producer landed;
  the consumer is Spec 2.

## Out of scope (deferred)

- **The consumer** — the worker/engine that drains `flush_table` jobs and calls
  `flush_table`. That is **Spec 2 (the engine-wire flush vertical)**:
  `docs/spike/engine-wire-transport.md`.
- **Physical GC** of end-capped inline rows / orphaned Parquet.
- **Age- or count-based triggers**, a generic trigger registry (the
  `inline_trigger` table is flush-specific by design), and multi-table batch
  flush.
- **Encoding the threshold per-table via an API** — the `threshold` column is
  set out-of-band for now; a governed surface to set it is later work.

## Branch

Stacks on `main` after PR #96 (the `flush_table` primitive). Touches
`iceberg_inline.rs`, `queue.rs`, and the flush commit tx; no overlap with the
engine-wire work, so Spec 1 and Spec 2 can proceed independently.
