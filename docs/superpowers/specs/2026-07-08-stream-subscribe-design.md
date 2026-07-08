# Stream engine — Subscribe / tail feed (slice 3) Design

> **Status:** design (direction). This spec makes `road-stream-subscribe`
> build-ready as its own per-slice spec (it currently points at the umbrella
> `2026-07-06-stream-engine-design`). A separate work agent writes the
> implementation plan from it and builds it.

## Goal

A governed, offset-resumable, ordered **change-event feed** over a declared CDC
table's durable changelog — the user-facing read path the slice-2b CDC machinery
made possible but left unread ("no user-facing changelog read yet"). A consumer
opens a streaming `GET /objects/{type}/changes` connection at a client-held
resume **cursor** and receives newline-delimited JSON (NDJSON) change events
`(+I / −U / +U / −D, row image)` ordered by `(bucket, offset)`, driven to
sub-second freshness by a Postgres `LISTEN/NOTIFY` wakeup.

The feed is the union of the **changelog Iceberg files** and the **live inline
tail** — and because slice-2b flush appends to the changelog and end-caps the
same inline rows *on one transaction*, an event is inline **XOR** in files at
any instant, so the union is a plain disjoint `UNION ALL` (no dedup, **and no
per-bucket flush watermark is needed for correctness** — the watermark the
umbrella spec mentions is deferred to later GC/ack work).

**Design principle — the event + cursor contract is the logical layer; NDJSON is
its HTTP framing.** The event model and the client-held cursor are specified
once, transport-agnostic, so a future **gRPC duplex** service wraps the same
contract (adding server-side acks / flow control / mid-stream query changes —
the "extra functionality" the stateless HTTP transport cannot do) as a transport
swap plus a capability add, not a redesign.

## Context — what ships today (slice 2b)

A declared CDC table is two Iceberg tables: an append-only **changelog** (every
event incl. `−U`) and a current-state **base** (`+I/+U/−D` only). Every event
carries framing — `loom_change_kind` (`+I`/`−U`/`+U`/`−D`), `loom_bucket`,
`loom_offset` — where `loom_offset` is gapless and monotonic per
`(table, bucket)`, allocated transactionally by `BucketOffsets::allocate_offset`
(`core/src/stream.rs:33`). The changelog table is `<schema>.<name>__changelog`
(`changelog_table_ref`, `iceberg_landing.rs:667`), pointed at by the soft
pointer `StreamMeta.changelog_table_id` (`core/src/stream.rs:30`), created at
CDC declaration with `include_framing=true`.

The feed reuses, unchanged: the offset allocator; the changelog resolution;
`inline_live_batch_full` (`iceberg_inline.rs:1391`, the live inline tail, keeps
`−U`, carries `loom_bucket`/`loom_offset`); the pruning-aware
`IcebergMirrorTableProvider` and the heterogeneous file∪inline union shape
`build_merge_view` already uses (`serving.rs:288`); the governed chokepoint
`resolve_governed` → `load_policy` → `GovernedTableProvider`
(`engine-serving/governed.rs:174`); and the `LISTEN/NOTIFY`-inside-commit
pattern from the job queue (`pg_notify('loom_queue:{kind}', '')` inside the
enqueue CTE, `queue.rs:10`; the `await_jobs` `PgListener`+`timeout` waiter,
`queue.rs:152`).

## The simplifying insight

`flush_locked_cdc` (`iceberg_flush.rs:193`) appends the base + changelog Parquet
files and **end-caps the same inline rows** (`InlineEndCap`, sets `end_snapshot`)
**on one caller transaction**. An outside reader therefore sees an event either
as a live inline row (not yet flushed) **or** as a changelog-file row (flushed,
end-capped) — **never both, never neither**. Consequences:

- The changelog feed is a plain `UNION ALL` of (changelog files) ∪ (base live
  inline tail). **No dedup.**
- **No per-bucket flush watermark is required for correctness.** The "flush
  watermark" the umbrella spec frames the union read around is only needed later
  — for trimming the inline tail by durability (today's age-based `gc_table` stays)
  and for explicit consumer durability acks. It is **deferred out of this slice.**

This leaves exactly **one** substrate primitive to invent: the **ordered
changelog-from-resume-offset scan** (files ∪ inline, `(bucket, offset)`-ordered,
bounded). Everything else is reuse.

## Architecture

### Endpoint & route

`GET /objects/{type}/changes` — a governed query-api route, registered in
`http.rs:router` (`http.rs:94`) alongside the object-read, gated by
`resolve_governed` (`governed.rs:59`) exactly like `GET /objects/{type}`: coarse
Read deny-before-existence, then `load_policy` for the row/column policy.

### Cursor — client-held, opaque, stateless server

`?cursor=<opaque>` encodes the per-bucket resume map and the type, as an
**unsigned** `base64url` blob, e.g. `{ t: <type>, b: { <bucket>: <next_offset> } }`.
Unsigned by design: a tampered cursor only corrupts the consumer's own resume
position; it carries **no authority**. Special values: `?cursor=earliest` boots
from offset 0 across all buckets; `?cursor=latest` boots from the current
high-water (`BucketOffsets::peek_offset` per bucket — join-the-tail).

- **The server is stateless.** No per-consumer state is held after the stream
  closes. The cursor is the only resume contract.
- **Multiple consumers are therefore free.** N consumers = N independent streams,
  each with its own client-held cursor, served from the shared changelog. No
  cross-consumer coordination.
- **Reconnect re-gates.** Every connect re-runs `resolve_governed`; a cursor can
  never bypass governance. Corollary (to document): policy is resolved at
  connect; a mid-stream policy change takes effect on the next reconnect.

### Transport — chunked NDJSON, contract is transport-agnostic

The connection stays open and emits newline-delimited JSON event objects
(`{ "bucket", "offset", "change_kind", "fields": { … } }`), one per line. The
**event record and the cursor are specified as the logical contract**; NDJSON is
its HTTP framing. A future gRPC duplex service emits the same event record as a
protobuf `ChangeEvent` message over a bidirectional stream and adds
server-driven acks/flow-control — a transport swap + capability add on top of
this contract, not a redesign.

Driven to freshness by:
- a `LISTEN/NOTIFY` wakeup on channel `loom_changelog:{base_table_id}`, fired
  *inside the CDC inline-write commit tx* (mirroring `pg_notify('loom_queue:…')`
  in `queue.rs:10` — fire-and-forget, buffered until commit, cannot break the
  write); and
- a poll-fallback timer (the `await_jobs` `PgListener` + `timeout(poll_interval)`
  shape, `queue.rs:152`), bounding the wait if a notify is missed.

Each emitted event carries its own `(bucket, offset)`, so a consumer always knows
its own progress; on disconnect (TCP half-close) it reconnects with a cursor
built from its last-seen offsets. The server's scan loop `select!`s on
*client-disconnect* vs *next-wakeup*, so a closed connection ends the stream
promptly (no orphaned long-poll); a per-batch `LIMIT` bounds memory (natural
backpressure — never reads the whole log in one pass).

## The ordered changelog feed scan (the invented primitive)

`changelog_feed_scan(catalog, base_table, cursor, limit) -> (batch, next_cursor)`
— the one new substrate piece. For the cursor's `{bucket → next_offset}`:

1. **Changelog files** — scan `<schema>.<name>__changelog` through the
   pruning-aware `IcebergMirrorTableProvider`, predicate
   `loom_bucket ∈ cursor-buckets AND loom_offset ≥ next_offset[bucket]`,
   projected to `[user_cols, loom_change_kind, loom_bucket, loom_offset]`. The
   offset predicate prunes files by stats (same provider
   `build_serving_provider` uses, `serving.rs:74`).
2. **Inline tail** — the **base** table's live inline rows via
   `inline_live_batch_full` (the fresh tail lives on the base's inline tier;
   flush reads it from there and dual-appends), filtered to the same
   bucket/offset predicate.
3. **Disjoint UNION ALL** — DataFusion unions the Iceberg provider and the base
   `PgTableProvider` (the heterogeneous union `build_merge_view` already does).
   No dedup (inline XOR files by flush atomicity).
4. **Order + bound** — `ORDER BY (loom_bucket, loom_offset)`, `LIMIT` per batch
   (max-events / max-bytes). Emit those rows as NDJSON lines, advance the
   in-stream cursor by the emitted offsets, return to the wakeup loop.
5. **`−U` is emitted** — the changelog contract is full events incl. `−U`
   before-images, so a retract-capable consumer stays correct.

`changelog_feed_scan` is wrapped in `GovernedTableProvider` (below) before
execution, so governance is enforced by the providers, not by the scan SQL.

## Governance on the stream

- **Gate at connect** — `resolve_governed` once: coarse Read gate
  deny-before-existence → `load_policy` → `GovernedType` (row filters, denied /
  masked columns). Identical to `GET /objects/{type}`.
- **Per-batch enforcement** — each batch's union provider (changelog files ∪ base
  inline) is wrapped in `GovernedTableProvider` (`engine-serving/governed.rs:174`)
  with the resolved `TablePolicy`: denied columns masked/dropped via
  `ProjectionExec`, ACL row-filters pushed as `FilterExec` conjuncts
  (`GovernedTableProvider::scan`, `governed.rs:211`). A subject never sees an
  event, or a column, it may not read — on **every** batch. Governance is on the
  row *image*, applied consistently across `+I/−U/+U/−D` for one key: if a key is
  ACL-filtered, all its events are filtered.
- **Column projection** — `?fields=` intersects with the governed `allowed()` set.

## Non-regression

Almost entirely **additive**: a new route, a new handler, a new read primitive,
and a new notify channel. The **single touch point on existing code** is one
fire-and-forget statement — `pg_notify('loom_changelog:{base_tid}', '')` inside
the CDC inline-write commit tx (`iceberg_inline.rs` write path, alongside the
existing byte-trigger enqueue). A failed notify cannot break the write (it is a
commit side-effect; the rows are visible on commit regardless). Existing CDC e2e
(`stream_cdc_e2e`, `stream_cdc_consolidate`, the slice-2b suite) stays green and
byte-identical.

## Testing

Tests are `rust_test` / `loom_fixture_test` integration targets only — never
inline `#[cfg(test)]`. New fixture tests use the `loom_fixture_test` macro and
wire their own target in `src/services/query-api/BUCK`, mirroring an existing
sibling; they reuse `//src/services/query-api:e2e-support`.

- **Full stream across a flush boundary** — subscribe from `earliest`; write
  `+I/−U/+U/−D`, flush mid-stream; assert the NDJSON stream is the complete,
  correctly-ordered event sequence (changelog files ∪ inline tail), ordered by
  `(bucket, offset)`, `−U` included.
- **Resume / disjointness** — consume to offset `X`, disconnect, reconnect with
  the cursor at `X` → receives exactly events `≥ X`, no gap, no dup (proves the
  inline XOR files disjointness).
- **Multi-consumer** — two concurrent streams from different cursors →
  independent and each correct.
- **Governance** — a restricted subject sees masked denied columns and zero
  events for ACL-filtered rows; a reconnect re-gates.
- **Freshness** — an un-flushed inline write appears on the stream within the
  notify/poll window.
- **Wakeup latency** — a blocked stream emits promptly after a write commits
  (notify path), not only on the poll timer.
- **`latest` cursor** — boots from `peek_offset` (join-the-tail).

## Global constraints (loom-specific, carry into the plan)

- **Tests are `rust_test` / `loom_fixture_test` integration targets only.** New
  fixture tests MUST use `loom_fixture_test`
  (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test`. The
  `no-inline-tests` prek hook fails the build on any inline `#[test]`.
- **After changing any `query!`/`query_scalar!` SQL** (none expected unless the
  notify is implemented as a compile-time query), run `tools/sqlx-prepare.sh` and
  commit `.sqlx/`; `sqlx-cache-check` enforces freshness. (A runtime
  `sqlx::query(AssertSqlSafe(...))` for the `pg_notify` statement — as the
  queue's enqueue uses — avoids the cache entirely.)
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files`.
  Markdown files end with exactly one trailing newline, no trailing whitespace.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/
  `indexing_slicing`/`panic`/`todo` in production lib/bin code. `#[expect(lint,
  reason = "...")]` for a justified local exception. Test code is exempted from
  the panic-safety lints via `loom_rust_test`/`loom_fixture_test`.
- **The notify is fire-and-forget inside the commit tx.** It must never be on a
  code path that can fail the write; mirror `queue.rs:10`'s `pg_notify`-in-CTE
  shape exactly.
- **Framing stays hidden from logical reads** — the feed's emitted event projects
  user columns only; `loom_*` framing is internal to the scan and ordering.
  `?fields=` and governance intersect on user columns only.

## Non-goals (deferred)

- **Server-side consumer-offset storage** (`__consumer_offsets`) — v1 cursor is
  client-held; the server is stateless. A consumer-checkpoint registry (so a
  consumer can ask "where was I?" without tracking it itself) is a deferred
  follow-on.
- **The flush watermark** — not needed for correctness (disjoint union). Relevant
  later for inline-tail GC trimming (age-based `gc_table` stays today) and
  explicit durability acks.
- **gRPC duplex service** — the NDJSON event + cursor contract is shaped to port
  to it (future server-side acks / flow control / mid-stream query changes);
  this slice ships the HTTP/NDJSON transport only.
- **Log-table (non-CDC) subscribe** — v1 is CDC tables (which own a changelog
  table via `changelog_table_id`). Log tables have offsets but no separate
  changelog (the events *are* the data); their subscribe is a small follow-on.
- **Mid-stream policy re-evaluation** — policy resolved at connect; a policy
  change mid-stream takes effect on reconnect (documented).
- **Authenticated SSE / browser transport** — NDJSON chosen for
  service-to-service (header-auth-friendly); an SSE/browser surface is a
  follow-on if browser consumers appear.

## Interfaces (names the plan consumes)

- Consumes: `resolve_governed`/`load_policy`/`GovernedType`
  (`query-api/src/governed.rs:59`); the router `http.rs:router` (`http.rs:94`);
  `compile_select_with`/`select_where_conjuncts` (`query-api/src/sql.rs:458`);
  `GovernedTableProvider`/`policy_for`/`TablePolicy`/`execute_governed_sql_stream`
  (`engine-serving/src/governed.rs:174`/`:152`/`:143`/`:289`);
  `build_serving_provider`/`IcebergMirrorTableProvider`/`build_merge_view`
  (`engine-serving/src/serving.rs:74`/`:288`); `changelog_table_ref`
  (`iceberg_landing.rs:667`); `StreamMeta.changelog_table_id` /
  `set_changelog_table_id` (`core/src/stream.rs:30`/`:59`);
  `inline_live_batch_full` (`iceberg_inline.rs:1391`);
  `BucketOffsets::allocate_offset`/`peek_offset` (`core/src/stream.rs:33`);
  `pg_notify`-in-commit + `await_jobs` (`queue.rs:10`/`:152`); the axum app
  `serve_with_shutdown` (`runtime/src/lib.rs:518`).
- Produces (later plan tasks rely on these EXACT names/types):
  - `GET /objects/{type}/changes?cursor=&fields=` route + handler in `query-api`
    (`http.rs`), returning a chunked NDJSON `Response` over an axum `Body` fed by
    a `Stream<Bytes>`.
  - An opaque cursor type `SubscribeCursor { t: String, b: HashMap<i32, i64> }`
    with `earliest`/`latest` sentinels, `to_opaque()`/`from_opaque()` base64url
    (unsigned), in `query-api`.
  - `pub async fn changelog_feed_scan(catalog, base_table, cursor, limit) ->
    Result<(Vec<ChangeEvent>, SubscribeCursor)>` in `engine-serving`
    (the ordered disjoint union: changelog files `WHERE loom_bucket/loom_offset`
    ∪ base `inline_live_batch_full` filtered, `ORDER BY (loom_bucket,
    loom_offset)`, `LIMIT`).
  - `pg_notify('loom_changelog:{base_tid}', '')` inside the CDC inline-write
    commit tx (`iceberg_inline.rs`), mirroring `queue.rs:10`.
  - A `ChangeEvent { bucket, offset, change_kind, fields: HashMap<String, Value> }`
    record (the transport-agnostic logical event; NDJSON-serialized on the HTTP
    path, protobuf-ready for the future gRPC path).
