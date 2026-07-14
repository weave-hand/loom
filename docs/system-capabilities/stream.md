# Stream engine capabilities

This document describes what loom's stream engine can do today: declared
append-only **log tables** and identity-bound **PK/CDC tables**, the
framing/bucketing/offset machinery both share, CDC event emission on the
mutation path, the dual (base + changelog) Iceberg table model, LastRow
compaction, the CDC-aware serving reads that keep `GET /objects` correct
across the whole write→flush→consolidate lifecycle, and materialized views as
micro-batch **standing queries** over a log source. It is distilled from the
three shipped slices' design specs and their landed commits; open work is
listed at the end.

_As of 3cd6c37e._

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
  `Validation` error (no retroactive conversion). The guard's `pre_existing`
  witness is derived from `ensure_table_witnessed`'s own return (`created:
  bool`), not from a read taken before the ensure call: `ensure_table` silently
  resolves a lost unique-index race to the WINNER's `table_id`, so a pre-read
  could call another transaction's brand-new table "not existing" and let a
  losing stream declare convert it. Both `reconcile_stream_mode` call sites
  (`iceberg_inline::inline_append`, `iceberg_landing::land_parquet_stream`) now
  derive `pre_existing` this way, so a stream declare that loses the mirror-row
  create race sees the winner's table as pre-existing and is rejected rather
  than converting it.
- A request against a table already declared a **different kind** is rejected as
  `Validation`, in **either** direction (#432): `mode=cdc` against an existing
  `log` table, and `mode=stream` (log) against an existing `cdc` table — the
  latter even when the bucket counts match, which is the only case where the
  count check alone would have let it through. Because both HTTP surfaces and
  the micro-batch MV commit path share this seam, the same guard also refuses an
  MV whose declared **log** output names a pre-existing CDC table (a commit that
  would otherwise stamp log-framing into CDC storage). A *different* bucket count
  still reports the count `Conflict` first, in both directions.
- A concurrent first-writer race is resolved by an `ON CONFLICT DO NOTHING`
  insert followed by a re-read; the loser's request is validated against
  whatever the winner actually recorded — and that re-read (`pg_stream_meta`,
  no new SQL) now checks **everything** the count-equal redeclare arm already
  checked, not just the bucket count: a disagreeing `kind` is a `Validation`
  worded distinctly ("declared concurrently with a different stream kind") so
  it stays distinguishable from the redeclare arm's own kind rejection, and a
  disagreeing `merge_engine` or `bucket_key` (for a `Cdc` request) is a
  `Conflict`/`Validation` respectively, mirroring the redeclare arm's guards.
  The redeclare arm itself gained the matching `bucket_key` guard it was
  missing (it already checked `kind` and `merge_engine`), so both arms are now
  symmetric. All of this validation runs, and can reject, **before** the CDC
  sub-branch's changelog writes (`ensure_table`/`pg_set_changelog_table_id`) —
  a rejected declare performs no writes at all, where previously a losing CDC
  declare could stamp `changelog_table_id` onto the winner's log row before
  being rejected. Because `ensure_table_witnessed` serializes the mirror-row
  create via a partial unique index, no two production transactions can
  actually contend on this path any more — it is defense-in-depth, reached
  only by tests driving the seam directly with a synthetic `tid`.

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
(`src/services/engine-serving/src/consolidate.rs`). The RPC/job-kind name
(`ConsolidateStream`/`stream_consolidate`) is unchanged, but the entry point
is now `consolidate_table`, which dispatches per table kind: a declared CDC
table takes the fold below; a non-CDC identity table with a live shadow
(`has_shadow`) takes the sibling `Precedence::Snapshot` (COW) fold documented
in `docs/system-capabilities/engine.md`'s consolidation section; any other
table is a no-op. The two folds share the trigger, the dispatch entry point,
and the wire names — only the fold and its precedence differ.

The CDC fold itself:

- Reads the CDC base's physical framed rows — live Parquet files **union**
  any still-live inline tail (so a consolidate that races ahead of a flush
  still folds correctly; `inline_live_batch_full` keeps `−U` rows in the read
  but they never win the fold, since their adjacent `+U` always carries a
  greater `loom_offset`).
- Reads **only the tiers that exist** (#440). The file leg is taken only when
  the base actually has live Parquet files, and the fold's `union all` is
  assembled from the registered tiers — because `read_files_as_batches` calls
  `catalog.load_table` *before* it looks at its path list, and a CDC base has
  no `iceberg_tables` row until its first Parquet write (a CDC declare
  pre-creates only the *changelog* table). Since the enqueue trigger is the
  inline delta-row count, which needs no flush, a base that has only ever been
  inline-appended reaches the fold with an empty file list: it now consolidates
  on the inline tier alone, and the fold's own overwrite creates the base's
  Iceberg table (`append_parquet_snapshot` → `ensure_iceberg_table`). A base
  whose every identity folds to a `−D` yields zero rows and short-circuits to
  the mirror-only `overwrite_truncate`, which is likewise safe with no
  `iceberg_tables` row.
- A base with **neither** tier is a no-op that still clears `has_shadow` **and
  the consolidate trigger** (#440). The trigger is armed by the enqueue and
  cleared only by a completed consolidate, and the enqueue condition is
  `delta_count >= effective && !enqueued` — so a consolidate that errored or
  returned early without clearing latched `enqueued = true` permanently, and
  that table could never enqueue another `stream_consolidate` even after a
  later flush would have made it succeed.
- Folds by **`loom_offset` descending per identity** (`row_number() over
  (partition by <identity> order by loom_offset desc)`, `_rn = 1`), dropping
  a `−D`-tombstoned winner so a deleted identity does not resurrect.
- Rewrites the base via `overwrite_parquet_snapshot_consuming`
  (framing-preserving, the same targeted primitive the COW consolidation fold
  uses): the commit carries an `InlineEndCap` naming exactly the row ids the
  fold read and folded, so a mutation committing mid-consolidation is left
  live rather than end-capped unfolded — the drive-by fix for
  `iss-consolidate-stream-lost-write` (the CDC arm previously blanket-capped
  every live inline row via plain `overwrite_parquet_snapshot`, which could
  silently drop such a race). Also clears `has_shadow` and disarms the
  consolidate trigger (below).
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
  `control-plane/core`; NDJSON is the HTTP framing. The feed is served on the
  production wire, not just in-process: three unary `EngineControl` RPCs —
  `ChangelogLatest` (the subscribability probe), `ChangelogFeed` (one bounded
  governed page), and `AwaitChangelog` (the long-poll wake) — implement the
  three `ServingEngine` feed methods on `EngineServingClient`
  (`src/services/query-api/src/engine_client.rs`), so `GET
  /objects/{type}/changes` no longer answers `501` in production. A page
  crosses the wire as `page_json` (a serialized `ChangeFeedPage`), the same
  `*_json` convention the governance-read RPCs already use; the engine clamps
  `limit` server-side (`MAX_FEED_LIMIT = 1024`,
  `src/services/engine/src/service.rs`). Governance is enforced **engine-side**
  from a caller-resolved `WirePolicy` (row filters + denied + masked,
  `engine-wire/src/client.rs`) — the subject identity never crosses the wire,
  only the already-resolved policy does.
- **The consistency contract.** One feed page is read at ONE pinned pair of
  snapshots: `current_snapshots_pair` (`postgres/src/iceberg_catalog.rs`) reads
  the changelog snapshot and the base snapshot in a single SQL statement, so
  both come from one Postgres MVCC snapshot. The inline tier is as-of by
  construction (`mvcc_live_pred`, `postgres/src/iceberg_inline.rs`); the file
  tier is pinned explicitly to the same pair before either tier is read
  (`engine-serving/src/feed.rs`). **Do not refactor this back into two
  independent `current_snapshot` calls** — that reintroduces the torn read a
  flush landing between the two reads used to open (previously
  `iss-stream-feed-torn-read`): the file tier goes stale mid-page while inline
  advances past it, leaving a hole neither tier covers, and the per-event
  `next` fold silently skips it forever for that consumer.
- **Log (append-only) tables subscribe too, via a kind-agnostic dispatch (#429).**
  An append-only log table carries the same offset framing but has no changelog
  sibling — the base rows *are* the log — so `changelog_feed_scan` keys off
  `StreamMeta.kind` (resolved by `stream_meta_for`, `postgres/src/stream.rs`) and
  routes a `Log` table to `log_feed_scan_at`, which reads the base table's **own**
  files ∪ inline tail (`build_base_file_tier` mirroring the changelog
  `build_file_tier`, `mv_delta_scan`'s union shape) with `change_kind` the stored
  constant `'+I'`. A `Log` table pins a **single** base snapshot (no changelog
  sibling to tear), while CDC keeps the pinned-pair path unchanged; both arms
  share one `union_govern_read` tail (union → `GovernedTableProvider` → ordered
  resume read → decode), so governance, framing exemption, cursor, and NDJSON
  contract are identical. The positions probe (`changelog_positions_latest`) and
  the inline-append `pg_notify` wakeup were relaxed from `Cdc`-only to `Cdc | Log`;
  because the kind dispatch lives engine-side in the `ChangelogFeed` RPC, log
  tables serve over the production wire with **no wire/client/handler change**.

## Continuous / standing queries (materialized views)

Shipped as `road-stream-continuous` (#418).

**Materialized views are a fourth `TransformBody` kind, not a new subsystem.**
`TransformBody::MicroBatch { source: TableRef, output: TableRef, buckets: i32,
sql: String }` (serde tag `"microbatch"`, `core/src/transforms.rs:62`) rides
the transforms concern wholesale: registration is `POST /admin/transforms`
with the new body, `on_input_commit: true` and/or a cron `schedule` picks the
trigger, and list/get/delete/run-history/manual-run/ad-hoc all work unchanged
— `TransformDef`'s body enum is the only extension point (no `/admin/views`
sugar surface yet — see *Deferred from this slice* below). `to_job`
(`transforms.rs:75`) emits a new queue kind, `STREAM_MV_JOB_KIND = "stream_mv"`
(`core/src/stream_mv_job.rs`), carrying `StreamMvJob { source, output,
buckets, sql, run_id }`; `TriggerNode::resolve` gains a `MicroBatch` arm
(`inputs = [source]`, `output = Some(output)`) so define-time cycle rejection
and the commit-seam matcher cover MV→MV chains for free — a source commit
fires a data-triggered MV def exactly like any other transform, debounced the
same way. `validate_transform_def` rejects `buckets < 1`, an empty `sql`, and
`source == output` (a self-consuming MV) at define time; whether `source` is a
**declared log stream table** is validated later, at delta-read time, as a
deterministic worker abandon — not at define time (the memory adapter has no
`TableRef → table_id` mirror to resolve against, and loom's registers
routinely accept defs whose physical tables land later). **v1 sources are
declared log tables only** — CDC sources are deferred (see below); MV outputs
are themselves declared log tables, so composition stays closed.

**The watermark and its CAS are the whole exactly-once story.** A new
`stream.mv_watermark` table (migration `0042_mv_watermark.sql`, PK `(mv,
source_table_id, bucket)`) tracks, per micro-batch standing query and source
bucket, the next offset to read. It is keyed by `mv_key(output)` —
`"{schema}.{name}"` of the **output**, not the def name (`core/src/stream.rs:151`)
— so the watermark survives a def rename/redefine exactly when the output
table is kept (which is exactly when resuming is correct), and ad-hoc
(nameless) micro-batch runs work with no special case. An absent row reads as
offset `0`: a fresh MV's first micro-batch processes the source's whole
existing log, so bootstrap and steady state are one code path. The core
`MvWatermarks` trait (`mv_watermarks`/`advance_mv_watermark`,
`stream.rs:173`/`:181`) is implemented by the memory fake, a postgres adapter
(`pg_mv_watermarks`/`pg_advance_mv_watermark`, `postgres/src/stream.rs:552`/
`:575` — executor-generic over `PgExecutor`, runtime `AssertSqlSafe` like
`version_for_table`, no `.sqlx` regen needed), and a shared testkit contract.
The advance is a plain CAS: `update ... set next_offset = $to where ... and
next_offset = $from` (insert-where-absent when `from = 0`); **zero rows
affected is a `Conflict`**, and because the advance runs inside the same
Postgres transaction as the output commit (below), a lost CAS rolls the whole
commit back — no partial effect ever lands. The gapless per-bucket offset
substrate (slice 0) is what makes the CAS bounds derivable rather than
tracked: offsets in a bucket commit in order, so for every bucket with delta
rows `min(loom_offset) == watermark` and `max(loom_offset) + 1` is the new
watermark — the worker computes `WatermarkAdvance { bucket, from, to }`
straight off the fetched delta's framing (`framing_bounds`,
`worker/src/stream_mv.rs`), no separate bookkeeping RPC.

**The delta read is the framed files∪inline read, offset-filtered.** A new
internal Flight ticket, `MvDeltaTicket { mv, schema, name }`
(`engine-wire/src/flight.rs`, `deny_unknown_fields` keeps it disjoint from
every other ticket shape via its required `mv` field), dispatches to
`mv_delta_scan` (`engine-serving/src/mv_delta.rs:49`) — the same
`files_with_stats` + `read_files_as_batches` ∪ `inline_live_batch_full` union
`consolidate_stream` reads, under the same per-table advisory lock
(`lock_table`) so a flush racing the read cannot double-read or drop rows. Each
tier is registered **only if it exists** (#436): `read_files_as_batches` bottoms
out in `catalog.load_table`, which fails for a table that has never been flushed
to Iceberg — and *every* micro-batch MV output is inline-only until its
byte-threshold flush fires, so calling it unconditionally made a fresh MV output
unreadable as a delta **source**. A downstream MV chained onto one therefore
wedged until the upstream happened to flush. Now an absent file tier is skipped,
an absent inline tier is skipped, and a source with neither returns an empty
batch under the authoritative framed schema — mirroring `build_serving_provider`'s
tier union (`#iss-serving-empty-table-not-found`). Chained MVs compose with no
flush in between, which the slice-5 composability e2e now asserts directly (its
explicit `flush_table` workaround is deleted).
It resolves the source table, requires a declared `kind = 'log'` stream table
(anything else — an unknown table or a CDC/batch source — is a deterministic
`"mv delta: ..."`-prefixed error the worker maps to an abandon, never a
retry), reads the watermark map, and runs one DataFusion pass: project user
columns plus `loom_change_kind`/`loom_bucket`/`loom_offset`, filter
`OR`-per-bucket `loom_bucket = b AND loom_offset >= from_b` (`from_b = 0` for
buckets absent from the map), order by `(loom_bucket, loom_offset)`. The
framing columns ride the wire deliberately — this is an internal data-plane
read, not a logical one — and the worker strips them (`strip_framing`) before
registering the delta under the source's table name in a fresh DataFusion
session; the MV's authored SQL never sees `loom_*` columns.

**The commit is one transaction: land, advance, mark succeeded.** After
running the standing query's SQL over the stripped delta, the worker calls a
single `EngineControl::CommitMicroBatch` RPC. On the engine side,
`inline_append_mv` (`postgres/src/iceberg_inline.rs:820`, a thin wrapper
widening `inline_append_decl` with an `Option<&MvCommit>` extra) does, on ONE
Postgres transaction:

1. Inline-lands the result under `StreamDecl::Log(buckets)` — the output is
   declared a log stream table in the same tx as its first write (existing
   `reconcile_stream_mode` guards apply: a pre-existing batch table as output
   is `Validation`, a bucket-count mismatch is `Conflict`); every row is
   stamped `+I` with a fresh `(bucket, offset)`.
2. Fires the byte-trigger flush arm and `pg_fire_data_triggers` for the
   output table — **this is composability**: an MV's output commit is itself
   a commit-that-writes-new-data, so a second MV (or any other data-triggered
   transform) reading the first's output fires automatically, with no special
   MV-to-MV wiring.
3. `pg_advance_mv_watermark` CAS-advances every touched bucket; a lost CAS
   aborts the whole transaction.
4. `pg_mark_run_succeeded` stamps the driving `TransformRun`, exactly as
   `CommitTransform` does.
5. Emits a lineage event: inputs `[source]`, outputs `[output]`, payload
   `{"sql", "mv", "offsets": {bucket: {"from", "to"}}}` — the consumed offset
   range is durable provenance.

**Superseded runs are a named, benign outcome.** Because the queue is
at-least-once, a retried or duplicate micro-batch run re-reads the delta from
the *committed* watermark; if a concurrent run already covered it, the CAS
returns `Conflict`, the whole commit transaction rolls back (no duplicate
output, no partial watermark move), and the worker abandons the job with
`"superseded: watermark advanced concurrently (a newer run covers this
delta)"` — deterministic, never retried, and correct: the covering run already
produced the rows, and debounce guarantees any still-unprocessed tail already
has a queued run.

**Empty output has two distinct, both-atomic shapes.** A truly empty delta
(a debounced spurious wakeup — no rows at all) commits empty `ipc` and empty
advances: the engine lands nothing, declares nothing, and just marks the run
succeeded (`snapshot_id` absent) — a cheap no-op. A **filtering MV** — a
non-empty delta whose SQL keeps zero rows — is a different case handled
explicitly, not folded into the no-op: it still carries the (non-empty)
per-bucket `WatermarkAdvance`s derived from the delta's framing, with empty
`ipc`. The engine's `commit_micro_batch` branches on `ipc.is_empty()`
independently of whether advances are present: with advances it still
CAS-advances the watermark and marks the run atomically (else the consumed
delta would reprocess forever on every subsequent trigger); with no advances
it only closes the run. Either way nothing is landed or declared when `ipc` is
empty.

**Delta-decomposability is the correctness contract, not an implementation
detail.** v1 micro-batching is honestly framed as **approximate** continuous
query semantics — re-execution over deltas, not true incremental operators. It
converges to the batch-equivalent result exactly for queries where
`Q(Δ₁ ∪ … ∪ Δₙ) = Q(Δ₁) ∪ … ∪ Q(Δₙ)`: row-wise map / filter / projection.
Whole-history aggregations are **not** batch-equivalent under this contract —
they would need retract-correct (`−U`/`+U`-emitting) MV output and
per-operator state, which is exactly what's deferred (see below). The
worker-fixture convergence test drives an MV through multiple micro-batches
(including a flush between them) and asserts the result matches the same SQL
run once over the whole source.

**Because MV outputs are declared log stream tables, they are subscribable for
free.** This slice imports no subscribe code and depends on none — an MV's
output is a plain declared log stream table, so it gets subscribability
structurally: its inline rows carry `loom_change_kind = '+I'`, `loom_bucket`,
and gapless `loom_offset` from `0`, and flushed Parquet keeps that framing via
the existing `include_framing` derivation. **Subscribe / tail feed** (above)
serves both CDC tables' changelogs and log tables' offset-framed base rows
(#429), so an MV output — a plain declared log stream table — is subscribable
directly (reading its base rows, no changelog union) with zero additional work
in this slice.

**Retention: the MV read-position floor, and what it does not cover.**
`gc_table` is no longer watermark-unaware — for a table that is an MV **source**
its reclaim is bounded by the per-bucket **`mv_floor`** (the minimum committed
`stream.mv_watermark` `next_offset` across every MV reading the source, an absent
row — including a registered-but-never-run MV — counting as `0`), so a lagging
MV's end-capped source bytes are **held on disk** instead of being destroyed
while it is behind; the hold is counted (`GcSummary.held_by_mv_floor`) and a
warning names the per-bucket laggard, and deleting the MV's transform def deletes
its watermark rows (releasing the floor) as the escape hatch. See engine's **GC**
section for the full guard.

**That GC tier is byte-retention defense, not hole-freedom** — read the
distinction before relying on it. GC only reclaims **end-capped** rows
(`end_snapshot <= H`), whereas a micro-batch delta reads **live** rows at the
**current** snapshot (`mv_delta_scan`): an end-capped row is already invisible to
the MV before GC touches it, so `gc_table` could never have taken a row an MV
could still read. The starvation is created by whichever path **end-caps** offsets
the MV has not consumed — those rows leave the MV's delta immediately, and no
GC-tier guard can bring them back.

**The end-cap side now ships too (#442).** Every end-cap primitive in the mirror
requires an explicit **`EndCapIntent`** (`postgres/src/mv_floor.rs`) and calls
`guard_end_cap` on the caller's transaction: `Reframing` (the same rows are
re-projected at the same `(bucket, offset)` — flush, plain-coalesce compaction —
no floor consult), `Removing` (the offsets leave the live set — the floor is
consulted and a blocked removal refused, with the stable message prefix `mv-floor
refuses end-cap:`; this is the **default**, so a future retention path inherits the
guard structurally), and `Destroying { reason }` (deliberate destruction, bypassed
on purpose and logged — the catalog drop). On top of that seam, **three refusals**
remove the lossy paths outright:

- an **overwrite of a declared stream table is refused**
  (`pg_refuse_stream_target` on `overwrite_parquet_snapshot` and its consuming
  twin) — previously a delete-all (`overwrite_truncate`) end-capped every live file
  *and* every live inline row with no stream check, destroying a log table's whole
  offset range; the CDC consolidate fold, the only legitimate overwriter of a
  framed base, keeps its own private door (`overwrite_stream_base`);
- a **typed UPDATE/DELETE against a declared log table is refused**
  (`write_inline_delta` → `pg_stream_meta_for_typed_write`) — previously it wrote an
  unframed delta row, set `has_shadow`, and handed the table to the COW arm, which
  folds an offset-framed event log **by identity**;
- a **micro-batch MV over a CDC source is refused in both directions** — at
  registration (`define_transform`) and at declaration (`reconcile_stream_mode`);
  `mv_delta_scan` reads log sources only, so such an MV could never run and would
  pin its source at offset `0` forever (the residual concurrent interleave is
  `#iss-mv-cdc-declare-register-race`).

With those in place, **no production path can `Removing`-end-cap a declared log
stream table** — the refusals are the fix; the seam is the type-level constraint
future retention paths inherit. The CDC fold *is* a `Removing` end-cap, so it can
meet a floor: it pre-checks, and on a block it **skips and re-arms** (warn naming
the laggard MV, clear the consolidate trigger, `Ok(0)`) rather than erroring or
abandoning. Should an end-cap ever take offsets an MV has not read, the failure is
still loud rather than silent: the next micro-batch's delta starts **above** the
committed watermark, the derived CAS `from` no longer matches the stored
`next_offset`, and the commit **Conflict-aborts the run**. Retention should still
exceed the slowest MV's lag — an operational caveat shared with the subscribe
feed's own retention story (see `#fut-stream-consumer-offsets`).

**Manual repair: a legacy shadowed log table.** A declared log table carrying
`has_shadow` can only be one typed-mutated *before* the refusal above landed. It
**neither folds nor flushes** until an operator intervenes — `consolidate_table`'s
defensive `StreamKind::Log` arm warns and skips (never folds by identity) and
deliberately leaves the flag set, which keeps the non-CDC flush suppressed. Repair
by hand: inspect the unframed delta rows on `inline_<tid>` (NULL
`loom_bucket`/`loom_offset`), decide their fate, then clear the flag (`delete from
iceberg_mirror.shadow_flag where table_id = <tid>`).

Deferred from this slice, named so the register close-out can track them as
their own items:

- **True incremental operators / retract-correct aggregate MVs** —
  whole-history aggregations need `−U`/`+U`-emitting MV output (a
  CDC-flavored output table) and per-operator state; folds into the broader
  `#fut-stream-merge-aggregate` replace-vs-aggregate-engine split above.
- **CDC-table sources** — a CDC source's delta is its changelog (files ∪ full
  inline including `−U`); exposing event kinds to the MV SQL vocabulary is a
  follow-on. v1 sources are log tables only.
- **Multi-source MVs / stream-stream joins** — shipped as slice 5; see
  **Stream joins / delta-join analog** below.
- **Backfill/replay control** — no watermark-reset API or `from`-offset
  registration; v1 always starts at offset `0` and resumes from the committed
  watermark, so a rebuild means a new output table.
- **Parquet spill for oversized micro-batch outputs** — v1 output always
  lands on the inline tier (micro-batches are delta-sized by construction); a
  framed direct-Parquet output spill path is a follow-on.
- **Stats-pruned / streaming delta scans** — `mv_delta_scan` reads live files
  whole, the same posture as `consolidate_stream`; pruning by `loom_offset`
  file stats and streaming (non-collecting) execution are follow-ons under
  `#fut-transform-followups`.
- **Watermark-aware GC** — shipped: `gc_locked` holds reclaim of an MV source at
  the per-bucket `mv_floor`, and the end-cap-side half shipped too (#442) — the
  `EndCapIntent` seam plus the three write-path refusals (see the retention
  section above).
- **`/admin/views` sugar surface** — MVs register through the ordinary
  transforms admin surface in v1; a dedicated, MV-shaped admin surface is
  deferred.

## Stream joins / delta-join analog (slice 5)

A `TransformBody::MicroBatchJoin` (#420) is a micro-batch MV whose SQL joins a
source stream's watermarked **delta** against a second table's **folded current
state** — Fluss's delta-join shape on loom's grain — emitted as an enriched `+I`
log through slice-4's `CommitMicroBatch`, unchanged. It is *one new fetch* on the
slice-4 loop: no new RPC, proto, migration, job kind, or `.sqlx` change. The
watermark advances on the **source** only (the enrich state is read, never
consumed), so exactly-once effect, composability, and convergence are inherited
from the continuous-query slice.

- **Two flavors.** `MicroBatchJoin { source, enrich, on: Option<LookupOn>,
  output, buckets, sql }`. `on: None` is a **state-join** — the worker fetches
  B's full folded current state and registers it beside A's delta. `on: Some` is
  a **lookup-join** (the delta-join analog) — the worker extracts A-delta's
  distinct `source_col` values and fetches only B's rows whose `enrich_col` is in
  that set. Over `MAX_LOOKUP_KEYS` (10 000) distinct keys it falls back to the
  full-state fetch (a superset is always correct). Join state lives in storage,
  not the worker.
- **The enrich read.** `mv_enrich_scan` (engine-serving) serves a table's folded
  current state through `build_serving_provider` (CDC merge applied per the
  declared engine; files ∪ inline for log/plain tables) — a **logical** read, so
  `loom_*` framing is hidden. A `Some(col, keys)` key predicate becomes a typed
  `col IN (…)` filter above the merge view, coercing each JSON scalar to the
  column's Arrow type (Int32/Int64/Utf8 in v1). Carried by an `MvEnrichTicket`
  (`enrich_schema`/`enrich_name`/`key`/`keys`) on the extensible `EngineTicket`
  decode chain and `FlightTableClient::fetch_mv_enrich`; the worker registers the
  result beside the delta and runs the SQL. The ticket **is** the future seam a
  dedicated PK index (`#fut-stream-pk-index`) re-backs without touching the
  worker or SQL contract.
- **Live-but-empty enrich.** A join against a live-but-empty enrich table serves
  its declared schema with zero rows: `do_get_mv_enrich` sends the schema
  unconditionally (`FlightDataEncoderBuilder::with_schema`, since arrow-flight's
  encoder drops zero-row batches), `fetch_mv_enrich` surfaces it as
  `(SchemaRef, batches)`, and the worker registers an empty table — the INNER
  join yields zero rows, the run **succeeds**, and the watermark advances
  (instead of a terminal abandon).
- **Triggers & cycles.** `TriggerNode::resolve` lists both `source` and `enrich`
  as inputs, so an enrich commit debounce-wakes the MV (no-op through the
  empty-delta path if the source has nothing new) and an enrich-edge cycle is
  rejected at define time by the existing cycle check. Join-MV outputs compose
  into downstream plain MVs.
- **Error taxonomy.** Deterministic refusals (unknown enrich table, missing/
  non-coercible key) carry a stable `"mv enrich:"` message prefix → engine
  `failed_precondition` → worker **abandon**; transient wire/serving faults stay
  unprefixed → **retry** with backoff. Mirrors slice-4's `"mv delta:"` mechanism.
- **Correctness contract.** Processing-time enrichment: each source event joins
  B's state *as of that micro-batch's execution* (Flink lookup-join / Fluss
  delta-join semantics), not an event-time temporal join — a B update enriches
  **subsequent** batches only; already-emitted rows are never retracted.
  Keyed ≡ full-fetch for the supported equijoin pattern (`on` is a documented
  contract, not parsed against the SQL). Retract-correct bilateral joins
  (`#fut-stream-incremental-join`), a dedicated PK index (`#fut-stream-pk-index`),
  event-time/temporal joins, delta×delta windowed joins, N-way enrichment, and
  self-enrichment (`source == enrich`) stay deferred.

## Known gaps

- `#fut-stream-merge-aggregate` — only the *replace-class* merge engines
  (LastRow / FirstRow / Versioned) are built; the *aggregate-class* engines
  (Aggregation per-column sum/max/min/count + PartialUpdate last-non-null field
  merge) are deferred — see *Merge engines*.
- `#fut-stream-partitioning` — log-table bucketing is within-batch
  round-robin with no cross-batch balancing cursor, and CDC bucketing has no
  richer/two-level sharding beyond a fixed `hash(identity) % bucket_count`.
- `#fut-stream-arrow-log` — no Arrow log on object storage; the changelog is
  Iceberg/Parquet only.
- `#fut-stream-feed-pruning` — the feed's per-bucket resume predicate filters
  above `GovernedTableProvider`'s full-table inner scan; pushing it into the
  mirror provider's stat-pruning scan would skip already-consumed changelog
  files by Parquet stats.
- `#fut-stream-consumer-offsets` — the subscribe cursor is client-held and the
  server is stateless; a server-side `__consumer_offsets` checkpoint registry
  (and the durability-based flush watermark it enables) is deferred.
- `#fut-stream-bulk-append-notify` — the subscribe wakeup `pg_notify` fires
  only on the **inline** append path (both CDC and log); a direct-to-Parquet
  bulk landing (a batch over `inline_byte_limit`) commits no notify, so a
  blocked `await_changelog` catches those writes only on its poll-fallback
  timer, not sub-second.
- `#iss-iceberg-create-outside-tx-framing` — `ensure_iceberg_table`'s
  create-if-absent runs outside and before the landing transaction, so a
  batch land and a stream-declaring land racing to create the same brand-new
  table can leave the winner's (batch) mirror row pointing at a table whose
  physical Iceberg schema carries the loser's framing columns; the mirror-row
  race itself is correctly guarded (see *Declaration* above), only the
  Iceberg-create race is not.
- `#iss-mv-register-below-reclaimed-floor` — a newly registered MV floors at
  offset `0` even if the source's low offsets are already gone, and registration
  is not serialized against GC's per-table lock (a race #442 narrowed — GC's
  floor read now runs inside its own transaction — but did not close).
- `#iss-mv-cdc-declare-register-race` — the MV/CDC mutual exclusion is guarded
  from both sides, but the guards share no lock, so a concurrent `?mode=cdc`
  write and `define_transform` can still interleave into an MV that can never
  run over a CDC source.
- `#iss-mv-floor-holds-pre-declaration-files` — data files written before
  `declare_stream` carry no `loom_offset` stat and are held forever by the
  floor's fail-safe, inflating `held_by_mv_floor`.

Two residuals noted during review, not yet tracked as separate register
items: `consolidate_stream`'s pre-lock metadata reads (`stream_meta`,
`live_table_id`) are not re-validated once the advisory lock is held —
benign today since a CDC table's kind/identity never change post-declaration,
but would matter if a future change let declaration race live traffic; and
the consolidate-lock regression test proves the lock is taken and blocks
sequentially, but does not fixture a genuine concurrent flush-vs-consolidate
race (structural correctness only, not an interleaving test).
