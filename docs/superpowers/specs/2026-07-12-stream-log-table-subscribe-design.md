# Log-table (non-CDC) subscribe Design

> **Status:** design (direction). This spec makes
> `road-stream-log-table-subscribe` build-ready (promoted 2026-07-12 from
> `#fut-stream-log-table-subscribe`). A separate work agent writes the
> implementation plan from it and builds it.

## Problem

v1 subscribe (`#road-stream-subscribe`) serves **CDC** tables, which own a
separate `__changelog` sibling (`StreamMeta.changelog_table_id`,
`core/src/stream.rs:79`). Log (append-only, non-CDC) tables carry the same
offset framing but no changelog — the events *are* the data — and the feed
probe refuses them: `changelog_positions_latest` returns `None` unless
`meta.kind == Cdc` (`postgres/src/stream.rs:651`), which the handler maps to
`400 "not backed by a declared CDC table"` (`query-api/src/http.rs:634-651`).
A consumer cannot tail an append-only stream table.

## Context — what ships today (verified)

- **Log rows are already an ordered log.** Every inline table carries the
  framing columns (`loom_change_kind default '+I'`, `loom_bucket`,
  `loom_offset` — DDL `iceberg_inline.rs:285-297`); log-stream inserts stamp
  `(begin_snapshot, loom_bucket, loom_offset, …)` (`iceberg_inline.rs:645`),
  and flushed Parquet keeps the framing (`framing_column_specs`,
  `iceberg_landing.rs:659-699`). A log table never emits `-U`/`+U`/`-D` —
  `loom_change_kind` is constantly `'+I'`.
- **The offset-ordered base-table read already exists.** `mv_delta_scan`
  (`engine-serving/src/mv_delta.rs:49-201`) is exactly the shape: files
  (`files_with_stats` → `read_files_as_batches`) ∪ live inline
  (`inline_live_batch_full`), per-bucket offset predicate
  `(loom_bucket=b AND loom_offset>=from) OR …`, ordered
  `(loom_bucket, loom_offset)`. It requires `meta.kind == StreamKind::Log`
  (`mv_delta.rs:72`) — the mirror image of the feed's Cdc-only guard.
  (`build_serving_provider` is the *folded current-state* reader — wrong tool
  for an ordered tail.)
- **The feed pipeline above the scan is kind-agnostic**: cursor spec, NDJSON
  contract, `ChangeFeedPage`/`next` fold, poll loop
  (`subscribe.rs:99-175`), governance via `GovernedTableProvider` with
  framing columns exempt from masking (`feed.rs:40-46`).
- **Await primitive**: pg `await_changelog` LISTENs on `loom_changelog:{tid}`
  (`stream.rs:520-546`) — confirm at plan time whether log-table inline
  appends NOTIFY that channel; if not, the append path gains the notify (the
  flush/CDC path already has it).

## Design

One capability, three seams:

1. **Positions probe.** Relax `changelog_positions_latest`
   (`stream.rs:651`) to answer for `Log` tables too: per-bucket latest
   positions read from the base table's own max offsets (files ∪ inline —
   the same per-bucket `peek_offset` bookkeeping the write path maintains).
   Return shape unchanged (`Some(BTreeMap<bucket, next>)`), so the handler's
   probe/400 logic just stops refusing log tables.
2. **Feed scan.** In `changelog_feed_scan` (or a sibling
   `log_feed_scan` it dispatches to on `StreamDecl::Log`): when the table is
   a log table, read the **base table directly** — files ∪ inline with the
   resume predicate and `(loom_bucket, loom_offset)` order, following
   `mv_delta_scan`'s union shape — instead of the changelog∪inline union.
   No XOR concern: there is one storage home per event (flush moves rows
   from inline to files at the same offsets; the consistent-snapshot pinning
   from `2026-07-12-stream-subscribe-wire-design` Part A applies identically
   and prevents the same flush race). `change_kind` serves as the stored
   `'+I'`.
3. **Kind-agnostic dispatch.** The dispatch keys off `StreamMeta.kind`
   engine-side, inside the `ChangelogFeed` unary `EngineControl` RPC (shipped
   by `road-stream-subscribe-wire`, since closed — there is no Flight ticket;
   see spec deviation 2 in `2026-07-12-stream-subscribe-wire-design`), so both
   the in-process engine and the production wire serve log tables with no
   client/handler change. `GET /objects/{type}/changes` is unchanged.

Sequencing: independent of the wire item — this lands in the engine-serving
scan layer; whichever merges second inherits the other (the `ChangelogFeed`
RPC calls the same scan; the scan's pinning comes from the wire spec's Part A
if that lands first, else this item picks up live reads and the wire item
retrofits the pins).

## Non-regression

- CDC feeds are byte-identical (the Log arm is new dispatch, not a changed
  path).
- `mv_delta_scan` is untouched (the feed follows its shape; it does not
  refactor it — any shared-helper extraction is the implementer's judgment
  call if it falls out naturally).
- New SQL → `tools/sqlx-prepare.sh` + committed `.sqlx`.

## Testing

- **Log tail e2e** (fixture): land offset-framed rows into a declared log
  table (inline-only, then across a flush), subscribe from empty cursor —
  all events in `(bucket, offset)` order, `change_kind == "+I"`, cursors
  resume correctly mid-stream and across the flush boundary.
- **Positions probe** — `changelog_positions_latest` on a log table returns
  the per-bucket next offsets; on an undeclared table still `None` (400
  preserved).
- **Governance** — masked/denied columns enforced on the log feed; framing
  columns always present.
- **Await** — a blocked `await_changelog` on a log table wakes on an inline
  append.
- **CDC regression** — existing subscribe suite green, unchanged.

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `#[expect(lint, reason)]` locally.
- `tools/sqlx-prepare.sh` + commit `.sqlx` after SQL changes.
- `buck2 run //tools:prek -- run --all-files` before every commit.
- Tests are `rust_test`/`loom_fixture_test` targets only.

## Out of scope (deferred)

- Aggregate/merge-engine event shapes — log tables are `'+I'`-only by
  definition.
- Offset-pruning pushdown (`#fut-stream-feed-pruning`), consumer offsets
  (`#fut-stream-consumer-offsets`).

## Acceptance

1. A declared log table serves `GET /objects/{type}/changes` end-to-end
   (tail, resume, long-poll), in-process and — once the wire item lands —
   over the production wire.
2. CDC subscribe behavior is unchanged.
3. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `changelog_positions_latest` (`postgres/src/stream.rs:638-659`),
  `StreamMeta`/`StreamKind` (`core/src/stream.rs`), `mv_delta_scan`'s union
  shape (`engine-serving/src/mv_delta.rs:93-201`), `changelog_feed_scan` /
  feed pipeline (`engine-serving/src/feed.rs`, `query-api/src/subscribe.rs`),
  `await_changelog` (`stream.rs:520-546`).
- Produces: the Log arm of the positions probe; the log-table feed scan
  (base-table files∪inline ordered read) dispatched by `StreamMeta.kind`;
  the append-path NOTIFY if missing.
