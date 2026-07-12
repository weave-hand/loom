# MV watermark-aware GC Design

> **Status:** design (direction). This spec makes `road-mv-watermark-aware-gc`
> build-ready (promoted 2026-07-12 from `#fut-mv-watermark-aware-gc`). A
> separate work agent writes the implementation plan from it and builds it.

## Problem

`gc_table`'s horizon is purely wall-clock age: `H = max(snapshot_id) WHERE
snapshot_time < now() - retention` (`postgres/src/iceberg_gc.rs:100-110`),
reclaiming end-capped rows/files with `end_snapshot <= H`. It knows nothing
about **MV read positions** (`stream.mv_watermark`,
`migrations/0042_mv_watermark.sql`). If a reclaim path removes source
offsets a lagging micro-batch MV has not yet consumed, the MV's next delta
silently starts above its committed watermark: the delta comes up short, the
watermark CAS advances over the hole (or a superseding run Conflict-abandons
— `worker/src/stream_mv.rs:253-258`), and those events are lost to the MV
with no error at the source. Today the only protection is operational:
"retention must exceed the slowest MV's lag".

### Reachability — honest accounting

With today's reclaim paths the harm is **narrow but real, and about to
widen**:

- The common flush path is safe by construction: flushing end-caps inline
  rows only after the same offsets land in **live** files, which age-based GC
  never touches (`end_snapshot IS NULL` is never reclaimable).
- The armed cases are (a) **dropped-source reclaim** (a deliberate operator
  act — acceptable to remain loud-but-lossy), and (b) every **future**
  end-capping path over stream tables: stream small-file compaction
  (`#fut-stream-smallfile-compaction`), CDC changelog retention for MV
  sources (`#fut-mv-cdc-source`), and any replay/truncation surface.

The guard therefore lands as a **durable invariant on `gc_locked`** — every
current and future reclaim path inherits it — not as a patch to one caller.

## Context — what ships today (verified)

- **Watermarks.** `stream.mv_watermark(mv, source_table_id, bucket,
  next_offset)` (`0042_mv_watermark.sql:6-12`); `mv` is the OUTPUT's
  qualified name, `next_offset` the next unprocessed `loom_offset`. Reads
  and the CAS advance live in `postgres/src/stream.rs:552-632`. **No
  aggregate query across MVs exists yet.**
- **Which MVs read source T.** `mv_watermark.source_table_id` already keys
  the resolved source, so the floor query needs no transform-body decoding.
  The gap: an MV **registered but never yet run** has no watermark row —
  registration is a `transforms.transform` row with a `MicroBatch{source,…}`
  / `MicroBatchJoin{source,…}` body (`core/src/transforms.rs:62-83`,
  `pg_fire_data_triggers` / `data_triggered_defs`,
  `postgres/src/transforms.rs:139-148,682-696`). Such an MV must be treated
  as floor-0 (it will read from offset 0).
- **GC selection.** `reclaimable_paths` / `delete_data_files` /
  `delete_end_capped_inline_rows` all select on `end_snapshot <= H`
  (`iceberg_gc.rs:187-228`). Per-file column stats (min/max per column,
  including `loom_offset`) are recorded by the stats pass and readable via
  `files_with_stats`.
- **Failure today**: nothing holds GC back; the loss is silent (C7 shape:
  short delta → CAS advance or Conflict-abandon).

## Design

### The invariant

> For a table that is a micro-batch MV **source**, GC may not reclaim a row
> or file containing a `(bucket, offset)` at or above the **MV floor** —
> `min(next_offset)` per bucket across (i) all `mv_watermark` rows for the
> source and (ii) an implicit 0 for every registered-but-unrun MV reading it.

### Mechanism (inside `gc_locked`)

1. **Floor query** (new, one round trip):

   ```sql
   select bucket, min(next_offset) as floor
   from stream.mv_watermark
   where source_table_id = $tid
   group by bucket
   ```

   plus a registered-MV existence check over `transforms.transform` bodies
   whose resolved source is this table (reuse the resolution
   `data_triggered_defs`/`pg_fire_data_triggers` already perform). If a
   registered MV has **no** watermark rows at all → global floor 0.
2. **Guarded selection.** For a stream-declared table with a non-empty
   floor set, tighten the reclaim predicates: a candidate end-capped
   **file** is reclaimable only if its `loom_offset` max stat is strictly
   below `min(floor)` across all buckets (per-file stats are not
   per-bucket; taking the min across buckets is conservative — it only ever
   *holds* files longer). A candidate end-capped **inline** row is
   reclaimable only if `(loom_bucket, loom_offset)` is below that bucket's
   floor (rows are per-bucket precise). Files without offset stats (never
   true for stream tables, whose framing is stamped at write) are held, not
   reclaimed — fail-safe.
3. **Non-source tables and non-stream tables**: floor set empty → predicates
   unchanged, byte-identical behavior.
4. **Dropped sources**: the dropped-incarnation full reclaim deliberately
   **bypasses** the floor (operator dropped the source; the MV is dead by
   definition). The reclaim logs a warning naming the lagging MVs it
   strands. This keeps drop-GC convergent instead of wedging on a dead MV
   forever.
5. **Observability**: when the floor holds anything back, the `GcSummary`
   gains a `held_by_mv_floor` count and a `tracing` event names the slowest
   `mv` per bucket — the operator's lead to a wedged/lagging MV.

### Why floor-at-GC and not GC-aware MVs

The alternative (MVs detect the hole and fail) already half-exists (short
delta → eventual Conflict) and is the wrong direction: it makes data loss
detectable, not impossible. Holding reclaim until consumption is the same
contract Kafka retention-by-consumer-lag / Paimon consumer-id snapshot
pinning converge on, and `#fut-stream-consumer-offsets` will want the same
floor for subscriber cursors later — the mechanism generalizes (a second
floor source union'd in).

### Liveness trade-off (accepted)

A dead-but-registered MV pins its source's end-capped tail indefinitely.
Accepted for v1: the pin is visible (`held_by_mv_floor` + logs), the escape
hatches are deleting the MV registration or dropping the output (which
removes its watermark rows / registration), and unbounded silent data loss
is strictly worse than bounded visible disk. A max-hold override knob is
deliberately deferred until a real deployment needs it.

## Non-regression

- Tables with no MV readers (the overwhelming majority, incl. every existing
  fixture) take the empty-floor fast path — behavior byte-identical, existing
  GC suite green unchanged.
- The floor only ever *shrinks* the reclaim set; it can never delete more.
- New SQL is compile-time `query!` → `tools/sqlx-prepare.sh` + committed
  `.sqlx`.

## Testing

Fixture tests extending `postgres/tests/iceberg_gc.rs` + a worker e2e:

- **Lagging MV holds the tail** — source with flushed + end-capped rows past
  retention; one MV consumed to offset k; GC reclaims strictly below k
  (file-granular: only files wholly below the min floor), `held_by_mv_floor`
  counts the rest.
- **Caught-up MV releases** — advance the watermark past the tail; next GC
  reclaims what was held.
- **Registered-but-unrun MV pins everything** — define an MV over the
  source, never run it; GC reclaims none of the source's end-capped stream
  rows.
- **Multiple MVs → min wins** — two MVs at different offsets; floor is the
  slower one.
- **Non-source regression** — a plain table's GC output is byte-identical
  with the guard code present.
- **Dropped source bypasses** — drop the source; full reclaim proceeds, the
  warning names the stranded MV.
- **e2e (worker)** — lagging MV + aggressive retention + GC run + MV run:
  the MV's delta is complete (no hole), then catches up and GC converges.

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `#[expect(lint, reason)]` locally.
- `tools/sqlx-prepare.sh` + commit `.sqlx` after SQL changes.
- `buck2 run //tools:prek -- run --all-files` before every commit.
- Tests are `rust_test`/`loom_fixture_test` targets only.
- GC's commit-then-delete ordering and advisory-lock discipline
  (`iceberg_gc.rs:30-33,68-88`) are unchanged — the floor tightens
  *selection*, not the protocol.

## Out of scope (deferred)

- **Subscriber-cursor floors** — rides `#fut-stream-consumer-offsets` when
  server-side consumer offsets exist; the floor mechanism is built to union
  a second source.
- **Max-hold override / TTL on the floor** — add when a deployment hits the
  dead-MV pin in practice.
- **CDC changelog floors** — micro-batch MVs read log sources only today
  (`mv_delta.rs:72-78`); extend the floor to changelog tables alongside
  `#fut-mv-cdc-source`.

## Acceptance

1. A lagging MV's unread source offsets survive GC (file- and inline-tier),
   proven by the hold/release/e2e tests.
2. Non-MV-source tables GC byte-identically.
3. Held reclaim is observable (summary count + logs naming the laggard).
4. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `gc_locked` / `reclaimable_paths` / `delete_data_files` /
  `delete_end_capped_inline_rows` (`postgres/src/iceberg_gc.rs:90-228`);
  `stream.mv_watermark` (`0042_mv_watermark.sql`); `pg_mv_watermarks`
  (`stream.rs:552-567`); `data_triggered_defs` / MV body resolution
  (`postgres/src/transforms.rs`); `files_with_stats` offset stats.
- Produces: the per-source floor query + registered-MV floor-0 rule; the
  guarded reclaim predicates in `gc_locked`; `GcSummary.held_by_mv_floor`;
  the dropped-source bypass + warning.
