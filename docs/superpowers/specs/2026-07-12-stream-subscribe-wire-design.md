# Stream subscribe on the production wire Design

> **Status:** design (direction). This spec makes `road-stream-subscribe-wire`
> build-ready (promoted 2026-07-12 from `#fut-stream-subscribe-wire`), and —
> by operator decision — **folds in the fix for `iss-stream-feed-torn-read`**,
> which closes with this item. A separate work agent writes the implementation
> plan from it and builds it.

## Problem

Two gaps, one seam:

1. **The feed is 501 in production.** `#road-stream-subscribe` ships
   `GET /objects/{type}/changes` served only by the in-process test engine.
   The three `ServingEngine` feed methods default to
   `Err(ServingError::Unsupported)` (`query-api/src/serving.rs:331-361`), and
   the production wire client `EngineServingClient`
   (`query-api/src/engine_client.rs`) does not override them, so the handler's
   probe maps `Unsupported` → `501 NOT_IMPLEMENTED`
   (`query-api/src/http.rs:643-649`).
2. **The feed read is torn.** `changelog_feed_scan`
   (`engine-serving/src/feed.rs:330-397`) reads the changelog snapshot
   (`current_snapshot(&clog)`, `feed.rs:118`) and the base snapshot
   (`current_snapshot(base)`, `feed.rs:354`) as two independent pooled
   queries. The slice-2b XOR invariant (an event is inline **xor** in
   changelog files) holds on any single DB state; two states can tear. The
   harmful window: a flush **and** a follow-on inline write both land between
   the reads — the file tier is stale `[0..k)`, inline holds non-contiguous
   `[k+i..)`, the union has a hole at `[k, k+i)`, and the per-event
   `next.insert(bucket, offset+1)` fold advances past it, silently dropping
   events for that consumer.

The wire hop owns the serving-side read boundary, so the consistency fix and
the transport land together.

## Decision record

**Operator decision, 2026-07-12 (committed):** fold the torn-read fix into
this item rather than shipping transport over a known-torn read. Part A
(consistency) lands first as its own commit — it fixes the in-process engine
too — then Part B (transport).

## Context — what ships today (verified)

- **Feed substrate.** `changelog_feed_scan(catalog, base, serving_store,
  positions: &BTreeMap<i32,i64>, limit, policy: &TablePolicy) ->
  ChangeFeedPage` (`feed.rs:330-397`): disjoint UNION of the changelog file
  tier (`build_file_tier`, `feed.rs:113`) and the base inline tail
  (`build_inline_tier`, `feed.rs:148`, via `inline_live_batch_full`);
  governance wrapped **engine-side** via `GovernedTableProvider`
  (`feed.rs:372`) with the three `loom_*` framing columns never masked
  (`feed.rs:40-46`); resume predicate + `(loom_bucket, loom_offset)` order +
  limit; decode folds `next[bucket] = offset+1` (`decode_page`, `feed.rs:174`).
- **Types.** `ChangeEvent { bucket, offset, change_kind, fields }` /
  `ChangeFeedPage { events, next }` (`core/src/stream.rs:92-108`);
  `ChangeFeedPolicy { row_filters, denied, masked }`
  (`query-api/src/serving.rs:107-112`), converted to the engine-serving
  `TablePolicy` at the call boundary.
- **Handler flow** (`http.rs:634-681`): probe `changelog_latest` (501/400
  mapping), boot positions from the cursor, stream via `ndjson_feed_stream`
  (`subscribe.rs:99-175`): page → emit NDJSON lines with per-event opaque
  cursors → on empty page `await_changelog(FEED_POLL_INTERVAL)`.
- **Wire patterns to mirror.**
  - Tickets: `EngineTicket` JSON arms with `#[serde(deny_unknown_fields)]`
    disjointness (`engine-wire/src/flight.rs:213-230`, decode fall-through
    `:276-327`); `MvDeltaTicket` (`flight.rs:121-144`) is the closest — an
    internal data-plane ticket already streaming framed `loom_*` rows.
  - Long-poll: `AwaitJobs` unary RPC (`proto/engine_control.proto:9,57-58`;
    server `engine/src/service.rs:128-138`; client
    `engine-wire/src/client.rs:688-698` — **client deadline = server timeout
    + 2s**, `client.rs:693-695`).
  - Client-side `ServingEngine` impl template: `vector_search`
    (`engine_client.rs:60-91`) — build ticket, one `do_get`, map error class.
  - The pg long-poll primitive already exists: `await_changelog(pool, table,
    timeout)` (`postgres/src/stream.rs:520-546`, LISTEN on
    `loom_changelog:{tid}` + poll fallback), and the positions probe
    `changelog_positions_latest` (`stream.rs:638-659`).
  - Codegen is proto-edit-only (`engine-wire/BUCK:6-32` `:pb-gen` genrule; no
    build.rs).
- **Reference in-process wiring** (what the wire client must reproduce):
  `InProcessServingEngine` (`query-api/tests/e2e_support.rs:200-234`)
  delegates the three methods to `changelog_positions_latest` /
  `changelog_feed_scan` / `await_changelog`.
- **Catalog read seam.** The `Catalog` trait's reads are independent pooled
  queries; there is **no** repeatable-read/tx parameter
  (`core/src/catalog.rs:67-96`; `current_snapshot` is a standalone
  `fetch_optional`, `iceberg_catalog.rs:206-227`). But the downstream reads
  are already snapshot-parameterized (`schema(table, at)`,
  `files_with_stats(table, snap_id)`), and inline rows are MVCC-versioned
  (`begin_snapshot`/`end_snapshot` columns, `iceberg_inline.rs:285-297`).

## Design

### Part A — consistent-snapshot feed read (closes `iss-stream-feed-torn-read`)

The tear exists because the two `current_snapshot` reads see two DB states.
Fix by **pinning both snapshots atomically**, then reading everything as-of
the pins:

1. **Atomic dual-snapshot read.** A new postgres helper
   `current_snapshots_pair(pool, base, clog) -> (Option<Snapshot>,
   Option<Snapshot>)` issuing **one SQL statement** over
   `iceberg_mirror.snapshot` for both tables — one statement ⇒ one Postgres
   MVCC snapshot ⇒ the pair is mutually consistent by construction. (No
   transaction machinery, no isolation-level plumbing, no `Catalog` trait
   change beyond the one new read.)
2. **Thread the pinned ids.** `build_file_tier` reads
   `schema(clog, clog_pin)` + `files_with_stats(clog, clog_pin)` (already
   snapshot-parameterized); the base schema read uses `base_pin`
   (`feed.rs:359` already does given the pin).
3. **As-of inline tier.** `build_inline_tier` currently reads the *live*
   inline set (`inline_live_batch_full`). Add an as-of variant
   (`inline_batch_at(tid, pin)`) selecting rows where
   `begin_snapshot <= pin AND (end_snapshot IS NULL OR end_snapshot > pin)` —
   expressible today because inline rows carry MVCC columns. A flush that
   commits after the pins then changes neither tier: its changelog files have
   `begin_snapshot > clog_pin` (invisible to the file tier) and its
   end-capping of inline rows has `end_snapshot > base_pin` (rows still
   visible to the inline tier).

The XOR invariant now holds because both tiers are evaluated at one
consistent pair of pins. `changelog_feed_scan`'s signature is unchanged; the
fix is internal. The in-process engine and every existing feed test inherit
it.

### Part B — the wire transport

Three additions, each mirroring an established pattern verbatim:

1. **`ChangelogFeedTicket`** — a new `EngineTicket` JSON arm
   (`deny_unknown_fields`; a required unique field, e.g.
   `changelog_feed: true`-style tag or the `positions` field name, keeps the
   fall-through decode disjoint):

   ```rust
   pub struct ChangelogFeedTicket {
       pub schema: String,
       pub name: String,
       pub positions: BTreeMap<i32, i64>,
       pub limit: u64,
       pub policy: WirePolicy,   // row_filters + denied + masked, the
                                 // ChangeFeedPolicy fields serialized
   }
   ```

   Engine `do_get` arm: resolve the table, convert policy, run
   `changelog_feed_scan`, stream the resulting framed batches (user columns +
   `loom_change_kind`/`loom_bucket`/`loom_offset`). The client folds
   `ChangeFeedPage` from the batches exactly as `decode_page` does — an empty
   stream returns the caller's positions unchanged. Client method:
   `FlightTableClient::fetch_changelog_feed`, mirroring `vector_search`'s
   status-preserving error mapping.
2. **`ChangelogLatest` unary RPC** on `EngineControl` (proto-only edit):
   `ChangelogLatestRequest { schema, name }` →
   `ChangelogLatestResponse { present: bool, positions: map<int32,int64> }`,
   bridging `changelog_positions_latest` (`stream.rs:638-659`). `present:
   false` = not a subscribable table (query-api maps to its existing 400).
3. **`AwaitChangelog` unary long-poll RPC**: `AwaitChangelogRequest { schema,
   name, timeout_ms }` → empty response, bridging pg `await_changelog`
   (`stream.rs:520-546`); client sets its gRPC deadline to
   `timeout + 2s` (the `AwaitJobs` detail, `client.rs:693-695`).

Then `EngineServingClient` overrides the three `ServingEngine` feed methods
using 2, 1, 3 respectively — reproducing `InProcessServingEngine`'s wiring
over the wire. `GET /objects/{type}/changes`, `ndjson_feed_stream`, the
cursor contract, and the 501 mapping in `http.rs` are all **unchanged**; the
501 simply stops firing on the production deployment.

### Governance note

Policy crosses the wire inside the ticket (as `VectorSearchTicket` and the
governed SQL path already do): query-api resolves the subject's
`ChangeFeedPolicy` exactly as today (`http.rs:665-669`) and the engine
enforces it via `GovernedTableProvider` before the ordered read — enforcement
stays engine-side; the wire carries the *resolved* policy, not the subject.

## Non-regression

- Part A changes no signatures; every existing feed/subscribe test runs
  against the pinned read and must stay green.
- Part B adds RPCs/tickets; existing wire surfaces untouched. The
  `EngineTicket` fall-through decode must keep every existing arm decoding
  identically (the `deny_unknown_fields` disjointness test suite covers new
  arms).
- New SQL (`current_snapshots_pair`, `inline_batch_at`) is compile-time
  `query!` → `tools/sqlx-prepare.sh` + committed `.sqlx`.

## Testing

- **Torn-read regression (the core new test).** Deterministically interleave:
  pin-read A, then flush + one inline write, then complete the feed read —
  assert no hole (events `[k, k+i)` present) and no duplicates. Structure the
  scan so the pin step is injectable/observable (e.g. the helper takes the
  pins; a test drives the interleave through the advisory-lock or a
  two-phase call), rather than racing threads.
- **As-of inline visibility unit tests** — `inline_batch_at` at pins
  before/after a flush end-cap.
- **Wire e2e** (worker or query-api fixture, mirroring the existing
  subscribe e2e but over `EngineServingClient` against a real engine
  socket): subscribe → land CDC writes → events stream with correct cursors;
  resume from a cursor; `await_changelog` wakes on a write and times out
  clean; masked/denied columns absent (governance over the wire); non-CDC
  table → 400; and the previously-501 path now 200s.
- **Ticket disjointness** — the new arm round-trips and every prior ticket
  still decodes to its own arm.

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `#[expect(lint, reason)]` locally.
- `tools/sqlx-prepare.sh` + commit `.sqlx` after SQL changes.
- `buck2 run //tools:prek -- run --all-files` before every commit.
- Tests are `rust_test`/`loom_fixture_test` targets only.
- Proto changes regenerate via the `:pb-gen` genrule — never hand-edit
  generated stubs.

## Out of scope (deferred)

- **Log-table (non-CDC) subscribe** — sibling item
  `road-stream-log-table-subscribe` (specced in parallel,
  `2026-07-12-stream-log-table-subscribe-design`); the ticket dispatch is
  kind-agnostic so that item slots in engine-side.
- **Offset-pruning through governance** — `#fut-stream-feed-pruning`.
- **Server-side consumer offsets / ack** — `#fut-stream-consumer-offsets`.
- **Streaming duplex transport** (server-push instead of long-poll pages) —
  rides `#fut-awaitjobs-stream`'s streaming form if it lands.

## Acceptance

1. `GET /objects/{type}/changes` serves a live NDJSON feed on the
   production-wire deployment (no 501), with cursors, long-poll, and
   governance intact.
2. The torn-read interleave test proves no event loss/duplication under a
   concurrent flush + write.
3. `iss-stream-feed-torn-read` closes with this item.
4. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `changelog_feed_scan` / `build_file_tier` / `build_inline_tier` /
  `decode_page` (`engine-serving/src/feed.rs`); `ChangeEvent`/`ChangeFeedPage`
  (`core/src/stream.rs:92-108`); `changelog_positions_latest` /
  `await_changelog` (`postgres/src/stream.rs:520-659`); `EngineTicket` /
  `FlightTableClient` (`engine-wire/src/flight.rs`); `AwaitJobs` pattern
  (`client.rs:688-698`); `ServingEngine` defaults
  (`query-api/src/serving.rs:299-369`); `EngineServingClient`
  (`query-api/src/engine_client.rs`); `ndjson_feed_stream`
  (`query-api/src/subscribe.rs:99-175`).
- Produces:
  - `current_snapshots_pair` (atomic dual-snapshot read) and `inline_batch_at`
    (as-of inline read) in `control_plane_postgres`.
  - Pinned-snapshot internals of `changelog_feed_scan` (signature unchanged).
  - `ChangelogFeedTicket` arm + `FlightTableClient::fetch_changelog_feed`.
  - `EngineControl::ChangelogLatest` + `EngineControl::AwaitChangelog` RPCs
    and wire-client methods.
  - `EngineServingClient` overrides for `changelog_latest` /
    `changelog_feed` / `await_changelog`.
