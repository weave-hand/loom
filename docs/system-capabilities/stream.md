# Stream engine capabilities

This document describes what loom's stream engine can do today: declared
append-only **log tables** and identity-bound **PK/CDC tables**, the
framing/bucketing/offset machinery both share, CDC event emission on the
mutation path, the dual (base + changelog) Iceberg table model, LastRow
compaction, and the CDC-aware serving reads that keep `GET /objects` correct
across the whole write→flush→consolidate lifecycle. It is distilled from the
two shipped slices' design specs and their landed commits; open work is listed
at the end.

_As of 4740865d._

## Declaration: log vs. CDC, and the shared registry

A dataset opts into stream framing at write time via a `mode` query parameter,
recorded in a new `stream.stream_table` control-plane registry row keyed on
the Iceberg mirror `table_id` (`src/control-plane/postgres/src/stream.rs`).
Two flavors share one registry, distinguished by a `kind` column
(`log` | `cdc`, migration `0037_stream_table_cdc_columns.sql`):

- **Log tables** — `?mode=stream&buckets=N` on `POST /datasets/{schema}/{table}`
  (ingest). No identity requirement; appends only.
- **CDC/PK tables** — `?mode=cdc&buckets=N` on `POST /models/{type}` (the
  dataset→model bind surface). The bound type **must declare an identity
  property**, else `400` — a CDC table keys its buckets and its LastRow merge
  on that identity, and there is nothing to key without one. The identity
  column name is recorded as `bucket_key` on the registry row.

Both flavors go through one reconciliation function,
`reconcile_stream_mode` (`stream.rs:52`), driven by a `StreamDecl` enum
(`None` | `Log(n)` | `Cdc{buckets, bucket_key}`) so the inline and
direct-write Parquet paths cannot diverge. Declaration is **immutable** and
validated before any row is written:

- A requested `buckets < 1` is rejected as `Validation`, never a raw DB CHECK
  failure.
- Redeclaring an existing table with a different bucket count is a
  `Conflict`; converting a pre-existing **batch** table to `stream`/`cdc` is a
  `Validation` error (no retroactive conversion).
- A `mode=cdc` request against a table already declared a **different kind**
  (e.g. an existing `log` table) is rejected — but the converse is not: see
  `#iss-stream-log-vs-cdc-declare` in Known gaps.
- A concurrent first-writer race is resolved by an `ON CONFLICT DO NOTHING`
  insert followed by a re-read; the loser's request is validated against
  whatever the winner actually recorded.

Declaration runs **inside the write's own transaction**, so it commits iff the
write does. For a fresh CDC declaration it also creates and registers the
changelog table in the same transaction (see *Dual Iceberg tables* below).

## Framing columns and bucket assignment

Every inline table carries three universal framing columns
(`iceberg_inline.rs`, following the `loom_tombstone` precedent): a
not-null `loom_change_kind` (`+I` / `-U` / `+U` / `-D`, default `+I`), and
nullable `loom_bucket`/`loom_offset`, populated only for declared stream
tables. These are inline-physical bookkeeping columns — never part of a
table's `ColumnSpec` — so a single **`is_reserved` filter** (the `loom_`
prefix) already excludes them from every logical-schema derivation site
(`PgTableProvider`, `IcebergMirrorTableProvider`, `GET /datasets` schema
reporting): ordinary reads never see them.

Bucket assignment differs by flavor, computed once per batch in
`inline_append`:

- **Log tables:** `bucket = row_index % bucket_count` — a within-batch
  round-robin with no cross-batch balancing cursor (a documented v1
  simplification, `#fut-stream-partitioning`).
- **CDC tables:** `bucket = stable_hash(identity_cell) % bucket_count`,
  reusing the same deterministic hash family as the inline CAS's
  `advisory_key_for_id`. Every event for one identity — its `+I`, each
  `(−U,+U)` pair, its eventual `−D` — lands in the **same bucket**, so a
  key's whole change history stays ordered together and LastRow compaction
  can fold per identity correctly. A `stream_cdc_bucket` fixture test proves
  two separate appends for one identity land in the same bucket with gapless
  offsets.

For each touched bucket, `pg_allocate_offset(&mut *tx, tid, bucket, count)`
(`stream.rs:174`) reserves a contiguous offset run **inside the write's own
transaction** — offsets are assigned iff the write commits; a rollback frees
the run. Ordering is guaranteed **per bucket only**, not globally.

## CDC event emission on the mutation path

The identity-targeted inline-shadow mutation path (`run_mutate`/`run_insert`
in query-api's action router, landed by `road-cow-inline-shadow`) already
captured the identity, the CAS version token, and — for updates — the prior
row image. On a table whose `stream_kind` is `cdc`, `write_delta` emits the
full Flink-style change sequence instead of a single row:

- **Insert → `+I`.** The inserted row, stamped with bucket + offset exactly
  as a log-table append.
- **Update → an adjacent `(−U, +U)` pair.** `write_delta` gained a CDC
  variant that writes **two** inline rows: a **`−U` before-image** (the
  prior row `run_mutate` already holds) and the existing **`+U` after-image**.
  Both carry the mutation's own `begin_snapshot`/version token (so the `−U`
  does not perturb CAS) and receive **consecutive offsets in the identity's
  bucket**, `−U` first — a retract-capable consumer sees a well-formed
  retract/append pair.
- **Delete → `−D` with the full prior image.** Non-CDC identity tables keep
  today's NULL-column tombstone (byte-identical); a CDC table's `−D` carries
  the complete prior row so the changelog event is self-contained, with
  `loom_tombstone=true` still set so it hides the base row in merge-on-read.

The before-image itself is threaded end-to-end as a new, optional
`WriteDeltaRequest.before_ipc`/`before_columns_json` pair on the
`EngineControl` wire (`engine_control.proto`) and the `ActionEngine` trait —
present only for CDC updates/deletes, empty (absent) otherwise, so non-CDC
call sites are unaffected.

**The `−U` row is a changelog-only event, never current state.** Three
separate read predicates independently exclude it — `read_max_version` and
`inline_live_batch` (`and (loom_change_kind is null or loom_change_kind <>
'-U')`), and, discovered later by an end-to-end CDC lifecycle test, the
`GET /objects` serving merge view's base predicate (a `(-U, +U)` pair shares
one `begin_snapshot`, so a snapshot-only tie-break resolved to scan order and
briefly served the stale `-U` image — fixed by extending the same exclusion
to that third site). A dedicated `inline_live_batch_full` read exists
alongside the filtered ones, used only by the changelog flush path and by
consolidation's fold (which needs every event, including `−U`, but never lets
one win).

## Dual Iceberg tables

A declared CDC table is **two** Iceberg tables, one registry row
(`changelog_table_id` pointing at the second):

- **Current-state base** — the same table `ensure_iceberg_table` always
  created; unchanged merge-on-read; carries only `+I`/`+U`/`−D` deltas.
- **Changelog** — `<schema>.<name>__changelog`, created **at declaration**
  (not lazily) with `include_framing=true`, so its physical schema is *user
  columns + `loom_change_kind`/`loom_bucket`/`loom_offset`*. Append-only;
  holds **every** event including `−U`. `is_reserved` keeps its framing out
  of any logical schema, same as slice 1. The user-facing changelog read is
  the subscribe/tail feed (see **Subscribe / tail feed** below).

**Flush dual-writes both tables in one Postgres transaction.**
`flush_locked` (`iceberg_flush.rs`), on a `kind='cdc'` table, partitions the
captured live inline rows by `loom_change_kind`, appends the current-state
subset to the base and the full event set to the changelog via
`append_batches_on_tx` on one caller-owned transaction, and retries the whole
attempt on a lost CAS — so both mirror snapshots advance together or not at
all. Inline rows are end-capped exactly once, on the base append. Non-CDC
flush is byte-identical to before (no partition step, no second table). The
prior has_shadow suppression that kept a shadowed cow-inline-shadow table out
of the byte-triggered flush enqueue is **lifted for CDC** — its dual-write
flush is delta-aware and safe — while non-CDC identity tables keep it.

**Overwrite preserves framing for a declared stream table.**
`overwrite_parquet_snapshot`'s `include_framing` flag — previously hardcoded
`false` — is now derived from the stream registry (`pg_stream_bucket_count`
on the resolved table id): `true` for any declared log/CDC table (so a COW
overwrite doesn't silently drop `loom_*` from the physical schema mid-life),
`false` (byte-identical) for a plain batch table.

## Compaction: `consolidate_stream`

A **new, distinct worker job kind** (`stream_consolidate`, separate from
`flush_table` and the Parquet-coalescing `compact_table`, which only merges
small files and never folds deltas) drives engine-side LastRow compaction
(`src/services/engine-serving/src/consolidate.rs`):

- Reads the CDC base's physical framed rows — live Parquet files **union**
  any still-live inline tail (so a consolidate that races ahead of a flush
  still folds correctly; `inline_live_batch_full` keeps `−U` rows in the read
  but they never win the fold, since their adjacent `+U` always carries a
  greater `loom_offset`).
- Folds by **`loom_offset` descending per identity** (`row_number() over
  (partition by <identity> order by loom_offset desc)`, `_rn = 1`), dropping
  a `−D`-tombstoned winner so a deleted identity does not resurrect.
- Rewrites the base via `overwrite_parquet_snapshot` (framing-preserving,
  same primitive the COW UPDATE/DELETE path uses), clears `has_shadow`, and
  disarms the consolidate trigger (below).
- **Never touches the changelog** — it is the durable, always-resumable log;
  its retention rides the age-based `gc_table` pass, not compaction.
- A non-CDC table, or a CDC table with no snapshot yet, is a documented
  no-op returning snapshot id `0`.

**Enqueue** mirrors the byte-trigger flush pattern: `write_inline_delta`
accrues CDC delta-row counts in a sibling `iceberg_mirror.consolidate_trigger`
table and enqueues one `stream_consolidate` job, armed-once, on crossing
`EngineTuning::consolidate_delta_threshold` (env
`LOOM_CONSOLIDATE_DELTA_THRESHOLD`, default 128). The worker side
(`stream_consolidate_job.rs`) is a thin trigger — a
`handle_stream_consolidate` handler reusing `run_wire_job`'s parse-then-RPC
shape, dispatching straight to `EngineControl::ConsolidateStream`. Two
production-wiring gaps were caught and fixed in follow-up commits: the
threshold was initially wired only through test constructors
(`IcebergActionWriter::write_delta` hardcoded `None`) — fixed by threading
`with_consolidate_delta_threshold`/`EngineTuning::consolidate_delta_threshold`
into `engine::run`; and `consolidate_stream` initially read-then-overwrote the
base with **no lock**, so a flush committing in that window could be
end-capped by the overwrite without its rows ever entering the fold (silent
data loss) — fixed by taking the same per-table advisory lock
(`iceberg_flush::lock_table`) flush/GC already share across the whole
read+fold+overwrite window; `pg_advisory_xact_lock` blocks rather than
skip/retries, exactly mirroring `flush_table`'s own contention behavior
against a concurrent flush/GC.

## Merge engines

A declared CDC table folds its current-state base per-identity by a per-table,
**immutable** `merge_engine` (`stream.stream_table.merge_engine`, migration
`0040`; default `last_row`). Both fold sites — compaction
(`consolidate_stream`) and merge-on-read (`build_merge_view`) — render the same
engine to their `ROW_NUMBER() ... ORDER BY` window, so the choice is honored
identically on read and after a consolidate. A `-D` winner drops the identity
under **all** engines; the durable changelog is engine-agnostic (it keeps every
event regardless).

| Engine | Winner = rank 1 (`PARTITION BY identity ORDER BY …`) | Needs a version col |
| --- | --- | --- |
| `last_row` (default) | `loom_offset DESC` — byte-identical to the pre-engine fold | no |
| `first_row` | `loom_offset ASC` — "first write wins"; later `+U`/`-D` events for a key are ignored for current-state (they still land in the changelog) | no |
| `versioned` | `"<version_col>" DESC, loom_offset DESC` — highest domain version wins; offset tie-breaks (last-write-within-version). Handles out-of-order arrival | yes |

The engine is chosen at CDC bind time: `?merge_engine=<engine>` on
`POST /models/{type}` (alongside `?mode=cdc&buckets=N`), defaulting to
`last_row`. **Declaration-time validation** (`reconcile_stream_mode`, the seam
every declarer — HTTP or direct `land_cdc` — passes through) rejects
`merge_engine=versioned` unless the bound type declares a `version` property
(`ObjectType.version`, an ordinary user column, migration `0039`) of an
orderable logical type (`Integer` / `Long` / `Timestamp`); a redeclare with a
different engine is a `Conflict`. The version column is resolved live at the
fold sites via `version_for_table` (mirroring `identity_for_table`), never
copied onto the registry.

**`Versioned` stays correct across consolidate cycles.** A consolidate
physically rewrites the base to the highest-version winner; a *later* event
carrying a *lower* version still loses on the next read, because the folded
winner's version is preserved in the base. (The `last_row` default renders the
exact `loom_offset DESC` expressions in use before this work, so non-CDC and
non-versioned paths are byte-identical.)

The *aggregate-class* engines — Aggregation (per-column sum/max/min/count) and
PartialUpdate (last-non-null field merge), which *combine* rows rather than pick
one and need per-column merge-policy on the ontology model — are deferred
(`#fut-stream-merge-aggregate`).

## Reads stay correct across the whole lifecycle

Current-state reads (`GET /objects`, link traversal, `GET /datasets`) are
**merge-on-read**, generalized in `build_serving_provider`/`build_merge_view`
(`src/services/engine-serving/src/serving.rs`) around a `Precedence` enum:

- `Precedence::Snapshot` — the original non-CDC behavior, deduping by
  `begin_snapshot`/`loom_tombstone`, byte-identical to before this work.
- `Precedence::Offset` — CDC identity tables, deduping by `loom_offset`
  descending per identity, dropping a `loom_change_kind = '-D'` winner.

This matters because a CDC base can legitimately hold **multiple physical
rows per identity plus `−D` tombstones** in the file tier itself (dual-write
flush lands the whole `+I/+U/−D` delta history, not just the latest row per
identity — that's compaction's job, and it runs asynchronously on a
threshold, not synchronously with flush). An identity-table branch of
`build_serving_provider` originally returned the **raw, undeduped** file
provider for this shape, so `GET /objects` in the window after a flush but
before the next consolidation duplicated rows per identity and could
resurrect a `−D`-deleted id. A regression test (`stream_cdc_read_mid_window`)
pins the fix: reads in that window are deduped and tombstone-correct
regardless of whether consolidation has run yet. `ontology::stream_meta_for_table`
is the predicate that routes a CDC table into the engine-aware `Offset`
precedence (fetching kind + `merge_engine` in one lookup); framing columns are
exposed to the fold internally but
never leak into the projected output.

## Subscribe / tail feed

A CDC table's durable changelog is readable as a **governed, offset-resumable,
ordered change-event feed**: `GET /objects/{type}/changes` streams
newline-delimited JSON (NDJSON), one change event per line
(`{bucket, offset, change_kind, fields, cursor}`), ordered by
`(loom_bucket, loom_offset)` and driven to sub-second freshness by a
`pg_notify` wakeup fired inside the CDC inline-write commit transaction.

- **The feed is a plain disjoint `UNION ALL`** of the changelog Iceberg files
  ∪ the base table's live inline tail. Because flush appends to the changelog
  **and** end-caps the same inline rows on one transaction (`flush_locked_cdc`),
  an event is inline **XOR** in files at any instant — so the union needs **no
  dedup and no flush watermark** for correctness. The one invented primitive,
  `changelog_feed_scan` (`src/services/engine-serving/src/feed.rs`), is that
  ordered, `LIMIT`-bounded union; `−U` before-images are included (the changelog
  contract is full events).
- **The cursor is client-held, opaque, and unsigned.** `?cursor=` is a
  `base64url` blob of the per-bucket resume map plus the type it was minted for
  (rejected on a type mismatch); `earliest` boots from offset 0, `latest` joins
  the tail (per-bucket `peek_offset`). Each emitted line carries a `cursor`
  positioned **after** that event, so a consumer reconnects from its last-seen
  line. The server holds **no per-consumer state** — N consumers are N
  independent streams over the shared changelog; a tampered cursor only corrupts
  the consumer's own position (it carries no authority). `?max_events=N` bounds a
  catch-up read; unset is an endless tail.
- **Governance is enforced at connect and per batch.** Every connect re-runs
  `resolve_governed` (coarse Read deny-before-existence → row/column policy) —
  identical to `GET /objects/{type}` — and each scanned page is wrapped in
  `GovernedTableProvider` before the ordered read, so a subject never sees an
  event, or a column, it may not read, on any batch (`−U/+U/+D/−D` of an
  ACL-filtered identity are all filtered; a masked column reads as the mask
  marker on every event). Policy is resolved at connect; a mid-stream policy
  change takes effect on the next reconnect. `?fields=` intersects the governed
  columns; `loom_*` framing surfaces only as the envelope `bucket`/`offset`/
  `change_kind`, never as a `fields` key.
- **Freshness** rides a `pg_notify('loom_changelog:{base_tid}')` fired inside
  the CDC inline-write commit (buffered until commit, like the queue's enqueue
  notify), with a poll-fallback timer bounding a missed notify (`await_changelog`,
  mirroring `await_jobs`).
- **Transport-agnostic contract.** The `ChangeEvent` record and cursor live in
  `control-plane/core`; NDJSON is the HTTP framing. The feed is served today by
  the **in-process engine only** — the three `ServingEngine` feed methods default
  to `Unsupported`, so the route answers `501` on the production wire deployment
  until the engine-wire hop lands (`#fut-stream-subscribe-wire`).

## Known gaps

- `#fut-stream-merge-aggregate` — only the *replace-class* merge engines
  (LastRow / FirstRow / Versioned) are built; the *aggregate-class* engines
  (Aggregation per-column sum/max/min/count + PartialUpdate last-non-null field
  merge) are deferred — see *Merge engines*.
- `#road-stream-framing-write-paths` (promoted from `#fut-stream-framing-write-paths`
  2026-07-09) — the multi-step-action (`write_steps`) and transform-commit
  (`IcebergTx::commit`) write paths still drop framing; only the single-table
  inline/flush/overwrite paths documented above stamp and preserve it.
- `#fut-stream-partitioning` — log-table bucketing is within-batch
  round-robin with no cross-batch balancing cursor, and CDC bucketing has no
  richer/two-level sharding beyond a fixed `hash(identity) % bucket_count`.
- `#fut-stream-arrow-log` — no Arrow log on object storage; the changelog is
  Iceberg/Parquet only.
- `#fut-stream-subscribe-wire` — the subscribe feed (see **Subscribe / tail
  feed**) is served by the in-process engine only; the production wire client
  answers `501` until an engine-wire Flight `do_get` + long-poll RPC implement
  the seam.
- `#fut-stream-feed-pruning` — the feed's per-bucket resume predicate filters
  above `GovernedTableProvider`'s full-table inner scan; pushing it into the
  mirror provider's stat-pruning scan would skip already-consumed changelog
  files by Parquet stats.
- `#fut-stream-consumer-offsets` — the subscribe cursor is client-held and the
  server is stateless; a server-side `__consumer_offsets` checkpoint registry
  (and the durability-based flush watermark it enables) is deferred.
- `#fut-stream-log-table-subscribe` — subscribe serves CDC tables; a log
  (non-CDC) table's tail feed (reading its offset-framed base rows directly, no
  changelog union) is a small follow-on.
- `#road-stream-continuous` — continuous/standing queries (slice 4) are not
  built.
- `#road-stream-joins` — stream joins / the delta-join analog (slice 5) are
  not built.
- `#iss-stream-log-vs-cdc-declare` — a `mode=stream` (log) declaration
  against an already-CDC table is silently accepted (only the converse,
  `mode=cdc` against an existing non-CDC kind, is rejected).

Two residuals noted during review, not yet tracked as separate register
items: `consolidate_stream`'s pre-lock metadata reads (`stream_meta`,
`live_table_id`) are not re-validated once the advisory lock is held —
benign today since a CDC table's kind/identity never change post-declaration,
but would matter if a future change let declaration race live traffic; and
the consolidate-lock regression test proves the lock is taken and blocks
sequentially, but does not fixture a genuine concurrent flush-vs-consolidate
race (structural correctness only, not an interleaving test).
